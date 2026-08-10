const { Pool } = require('pg');

const db5 = new Pool({
  connectionString: process.env.DATABASE_URL_DB5,
  max: 10, // 10 connections is enough for social
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

db5.on('connect', () => console.log('[DB5] Connected to Neon Global Social DB'));
db5.on('error', (err) => console.error('[DB5 POOL ERROR]', err));

module.exports = { db5 };
