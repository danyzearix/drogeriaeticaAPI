// src/workers/sync-master-to-wc.js
import 'dotenv/config';
import crypto from 'node:crypto';
import { dbQuery } from '../lib/db.js';
import pkg from '@woocommerce/woocommerce-rest-api';
const WooCommerceRestApi = pkg.default;

function ts() { return new Date().toISOString(); }
function log(...a) { console.log(ts(), ...a); }

const wc = new WooCommerceRestApi({
  url: process.env.WC_URL,
  consumerKey: process.env.WC_KEY,
  consumerSecret: process.env.WC_SECRET,
  version: 'wc/v3',
  queryStringAuth: true,
});

// -------- util --------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const hashRow = (r) =>
  crypto.createHash('sha256')
    .update([r.PRECIO, r.EXISTENCIA, r.Estado].join('|'))
    .digest('hex');

const toWoo = (r) => ({
  sku: String(r.SKU),
  name: r.NOM_PROD || String(r.SKU),
  regular_price: String(r.PRECIO ?? ''),
  manage_stock: true,
  stock_quantity: Number(r.EXISTENCIA ?? 0),
  status: r.Estado === 'A' ? 'publish' : 'draft',
});

// -------- CATEGORÍAS (nuevo) --------
// Cache por nombre (lowercase) -> { id, parent }
const catCacheByName = new Map();
let catCacheLoaded = false;

async function loadCategoryCache() {
  if (catCacheLoaded) return;
  let page = 1;
  while (true) {
    const { data } = await wc.get('products/categories', { per_page: 100, page });
    if (!Array.isArray(data) || data.length === 0) break;
    for (const c of data) {
      const key = (c.name || '').trim().toLowerCase();
      if (!key) continue;
      catCacheByName.set(key, { id: c.id, parent: c.parent });
    }
    if (data.length < 100) break;
    page++;
  }
  catCacheLoaded = true;
}

async function ensureCategory(name, parentId = 0) {
  const key = (name || '').trim().toLowerCase();
  if (!key) return null;

  // en cache
  const cached = catCacheByName.get(key);
  if (cached) return cached.id;

  // buscar por API (por si no vino en el primer cache)
  const { data: found } = await wc.get('products/categories', { search: name, per_page: 100 });
  const exact = (found || []).find(c => (c.name || '').trim().toLowerCase() === key);
  if (exact) {
    catCacheByName.set(key, { id: exact.id, parent: exact.parent });
    return exact.id;
  }

  // crear
  const payload = parentId ? { name, parent: parentId } : { name };
  const { data: created } = await wc.post('products/categories', payload);
  catCacheByName.set(key, { id: created.id, parent: created.parent });
  return created.id;
}

// Asegura CATEGORIA -> SUBCATEGORIA y devuelve array de IDs
async function ensureCategoryPath(parentName, childName) {
  await loadCategoryCache();
  const ids = [];
  const parent = (parentName || '').trim();
  const child  = (childName  || '').trim();

  let parentId = 0;
  if (parent) {
    parentId = await ensureCategory(parent, 0);
    if (parentId) ids.push(parentId);
  }
  if (child) {
    const childId = await ensureCategory(child, parentId || 0);
    if (childId) ids.push(childId);
  }
  return ids;
}

// Construye el payload final incluyendo categorías
async function buildPayload(r) {
  let categories = [];
  try {
    // OJO: asegúrate de seleccionar CATEGORIA y SUBCATEGORIA en fetchBatch()
    const catIds = await ensureCategoryPath(r.CATEGORIA, r.SUBCATEGORIA);
    categories = catIds.map(id => ({ id }));
  } catch (e) {
    log(`[CAT] Error asegurando categorías para SKU ${r.SKU}:`, e?.response?.status || e.message);
    categories = [];
  }

  const base = toWoo(r);
  return { ...base, categories };
}

// -------- DB ops --------
async function fetchBatch(limit = 50) {
  log(`[DB] Leyendo lote de Master LIMIT ${limit}...`);
  const [rows] = await dbQuery(
    `SELECT SKU, NOM_PROD, EXISTENCIA, PRECIO, Estado, Fecha, CATEGORIA, SUBCATEGORIA
     FROM Master
     WHERE CAMBIOS='S'
     ORDER BY Fecha ASC
     LIMIT ?`,
    [limit]
  );
  log(`[DB] Lote obtenido: ${rows.length} filas`);
  return rows;
}

async function markProcessedMany(skus = []) {
  if (!skus.length) return;
  log(`[DB] Marcando CAMBIOS='N' para ${skus.length} SKU...`);
  await dbQuery(`UPDATE Master SET CAMBIOS='N' WHERE SKU IN (?)`, [skus]);
  log('[DB] Marcado OK');
}

async function getLastHashes(skus = []) {
  if (!skus.length) return new Map();
  log(`[DB] Consultando ledger para ${skus.length} SKU...`);
  const [rows2] = await dbQuery(
    `SELECT sku, last_hash FROM wc_sync_ledger WHERE sku IN (?)`,
    [skus]
  );
  const m = new Map();
  rows2.forEach(r => m.set(String(r.sku), r.last_hash));
  log(`[DB] Ledger devuelto: ${rows2.length} filas`);
  return m;
}

async function upsertLedgerMany(rows = [], mapHash = {}) {
  if (!rows.length) return;
  const values = rows.map(r => [
    String(r.SKU), r.Fecha, r.PRECIO, r.EXISTENCIA,
    r.Estado === 'A' ? 'publish' : 'draft',
    mapHash[String(r.SKU)]
  ]);
  log(`[DB] Upsert ledger para ${values.length} SKU...`);
  await dbQuery(
    `INSERT INTO wc_sync_ledger (sku, last_fecha, last_precio, last_exist, last_status, last_hash)
     VALUES ?
     ON DUPLICATE KEY UPDATE
       last_fecha=VALUES(last_fecha),
       last_precio=VALUES(last_precio),
       last_exist=VALUES(last_exist),
       last_status=VALUES(last_status),
       last_hash=VALUES(last_hash),
       updated_at=NOW()`,
    [values]
  );
  log('[DB] Ledger actualizado OK');
}

// -------- Woo helpers (reintentos + ids bajo demanda) --------
async function wcBatch({ create = [], update = [] }) {
  if (!create.length && !update.length) return;
  const MAX_ATTEMPTS = 5;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      log(`[WC] Enviando batch (create=${create.length}, update=${update.length}) intento=${attempt}...`);
      const t0 = Date.now();
      await wc.post('products/batch', { create, update });
      log(`[WC] Batch OK en ${Date.now() - t0} ms`);
      return;
    } catch (err) {
      const status = err?.response?.status;
      const body = err?.response?.data;
      const retriable = status === 429 || status >= 500;
      log(`[WC] ERROR batch status=${status} retriable=${retriable} intento=${attempt} body=${JSON.stringify(body).slice(0,300)}`);
      if (!retriable || attempt === MAX_ATTEMPTS) {
        throw new Error(`Woo batch failed status=${status} body=${JSON.stringify(body).slice(0,400)}`);
      }
      const backoff = Math.min(1000 * 2 ** attempt, 8000);
      log(`[WC] Backoff ${backoff} ms y reintentamos...`);
      await sleep(backoff);
    }
  }
}

const CONCURRENCY = 5;

async function findWooIdBySku(sku) {
  log(`[WC] Buscando id por SKU=${sku}...`);
  const t0 = Date.now();
  const { data: list } = await wc.get('products', { sku, status: 'any' });
  const id = (Array.isArray(list) && list.length) ? list[0].id : null;
  log(`[WC] SKU=${sku} => id=${id} (${Date.now() - t0} ms)`);
  return id;
}

async function resolveMissingIds(skuToId, missingSkus) {
  log(`[WC] Resolviendo IDs de ${missingSkus.length} SKU (concurrencia=${CONCURRENCY})...`);
  const queue = [...missingSkus];
  let active = 0;
  return await new Promise((resolve) => {
    const found = new Map();
    const next = async () => {
      if (!queue.length && active === 0) return resolve(found);
      while (active < CONCURRENCY && queue.length) {
        const sku = queue.shift();
        active++;
        findWooIdBySku(sku)
          .then(id => { if (id) { found.set(sku, id); skuToId.set(sku, id); } })
          .catch(e => log(`[WC] ERROR resolviendo id de ${sku}:`, e?.message || e))
          .finally(() => { active--; next(); });
      }
    };
    next();
  });
}

// -------- loop suave con backoff + recuperación --------
const skuToId = new Map();
const BATCH_SIZE = 50;
let delayMs = 1500;
const MIN_DELAY = 1500;
const MAX_DELAY = 30000;

async function cycle() {
  log('--- CICLO INICIO ---');
  const rows = await fetchBatch(BATCH_SIZE);

  if (!rows.length) {
    log(`Sin pendientes. Dormimos ${delayMs} ms`);
    await sleep(delayMs);
    delayMs = Math.min(delayMs * 2, MAX_DELAY);
    log('--- CICLO FIN (sin trabajo) ---');
    return;
  }
  delayMs = MIN_DELAY;

  const skus = rows.map(r => String(r.SKU));
  log(`Lote recibido: ${rows.length} filas, primeros SKU: ${skus.slice(0,5).join(', ')}...`);

  // idempotencia
  const lastHashes = await getLastHashes(skus);
  const currentHashes = {};
  const toProcess = rows.filter(r => {
    const h = hashRow(r);
    currentHashes[String(r.SKU)] = h;
    return lastHashes.get(String(r.SKU)) !== h;
  });
  log(`Idempotencia: a procesar=${toProcess.length}, saltados=${rows.length - toProcess.length}`);

  if (!toProcess.length) {
    await markProcessedMany(skus); // limpieza
    log('Nada que aplicar, solo limpieza de banderas.');
    log('--- CICLO FIN ---');
    return;
  }

  // resolver IDs faltantes
  const unknown = toProcess
    .map(r => String(r.SKU))
    .filter(sku => !skuToId.has(sku));
  if (unknown.length) {
    log(`IDs desconocidos en el batch: ${unknown.length}`);
    await resolveMissingIds(skuToId, unknown);
  }

  // separar create/update (con categorías)
  const creates = [];
  const updates = [];
  for (const r of toProcess) {
    const payload = await buildPayload(r); // ⬅️ aquí añadimos categorías
    const id = skuToId.get(String(r.SKU));
    if (id) updates.push({ id, ...payload });
    else creates.push(payload);
  }
  log(`Preparado batch => create=${creates.length}, update=${updates.length}`);

  // enviar lote
  await wcBatch({ create: creates, update: updates });

  // después de crear nuevos, cachear IDs
  if (creates.length) {
    log('Resolviendo IDs de recién creados para cache local...');
    for (const c of creates) {
      await sleep(50);
      try {
        const id = await findWooIdBySku(c.sku);
        if (id) skuToId.set(c.sku, id);
      } catch (e) {
        log(`[WC] No se pudo cachear ID de ${c.sku}:`, e?.message || e);
      }
    }
  }

  // ledger + marcar como procesados
  await upsertLedgerMany(toProcess, currentHashes);
  await markProcessedMany(toProcess.map(r => String(r.SKU)));

  log(`FIN de batch: total=${rows.length} aplicados=${toProcess.length} (create=${creates.length}, update=${updates.length})`);
  await sleep(1000); // pausa entre tandas
  log('--- CICLO FIN ---');
}

(async function main(){
  log('Worker iniciado. WC_URL=', process.env.WC_URL);
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await cycle();
    } catch (e) {
      log('CYCLE ERROR:', e.code || e.message);
      await sleep(5000);
    }
  }
})();


