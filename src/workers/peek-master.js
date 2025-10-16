import 'dotenv/config';
import { pool } from '../lib/db.js';

async function main() {
  const [rows] = await pool.query(
    `SELECT SKU, NOM_PROD, EXISTENCIA, PRECIO, Estado, Fecha
     FROM Master
     WHERE CAMBIOS='S'
     ORDER BY Fecha DESC
     LIMIT 20`
  );
  console.table(rows);
  if (!rows.length) {
    console.log('No hay productos marcados con CAMBIOS="S" por ahora.');
  }
  process.exit(0);
}

main().catch(err => {
  console.error('ERROR peek-master:', err);
  process.exit(1);
});
