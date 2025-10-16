import mysql from 'mysql2/promise';

let pool;

function ts(){ return new Date().toISOString(); }
function log(...a){ console.log(ts(), '[DB]', ...a); }

function createPool() {
  log('Creando pool MySQL (TLS activado)…');
  return mysql.createPool({
    host: process.env.DB_HOST,              // srv1448.hstgr.io
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 4,                     // bajamos presión
    queueLimit: 0,
    connectTimeout: 12000,
    enableKeepAlive: true,
    keepAliveInitialDelay: 8000,
    // Hostinger suele aceptar TLS sin CA pública:
    ssl: { rejectUnauthorized: false, minVersion: 'TLSv1.2' },
  });
}

export function getPool() {
  if (!pool) pool = createPool();
  return pool;
}

export function resetPool() {
  if (pool) { try { pool.end(); } catch {} log('Pool anterior cerrado.'); }
  pool = createPool();
}

const RETRIABLE = new Set([
  'ETIMEDOUT','ECONNRESET','EPIPE','PROTOCOL_CONNECTION_LOST','ER_CON_COUNT_ERROR'
]);

export async function dbQuery(sql, params) {
  let attempt = 0;
  while (true) {
    attempt++;
    const t0 = Date.now();
    try {
      const p = getPool();
      const res = await p.query(sql, params);
      log(`OK query (${Date.now()-t0} ms) [attempt ${attempt}]`);
      return res;
    } catch (e) {
      const code = e?.code || 'UNKNOWN';
      const ms = Date.now()-t0;
      const retriable = RETRIABLE.has(code);
      log(`ERROR query (${ms} ms) code=${code} attempt=${attempt} retriable=${retriable}`);
      if (!retriable || attempt >= 5) throw e;
      resetPool();
      const wait = Math.min(1000*attempt, 5000);
      log(`Backoff ${wait} ms y reintentamos…`);
      await new Promise(r=>setTimeout(r, wait));
    }
  }
}
