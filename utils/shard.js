// utils/shard.js
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const { Pool } = require('pg');

// ========== 3x PRISMA SHARDS ==========
const dbUrls = [
  process.env.DATABASEURL1,
  process.env.DATABASEURL2,
  process.env.DATABASEURL3
];

const prismaClients = {
  db1: new PrismaClient({ datasources: { db: { url: dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db1" } } }),
  db2: new PrismaClient({ datasources: { db: { url: dbUrls[1] || dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db2" } }),
  db3: new PrismaClient({ datasources: { db: { url: dbUrls[2] || dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db3" } } }),
};

Object.entries(prismaClients).forEach(([name, client]) => {
  client.$connect()
   .then(() => console.log(`[Prisma Success] Connected cleanly to ${name}`))
   .catch((err) => console.error(`[Prisma Warning] Shard ${name} offline on start.`, err.message));
});

// ========== 3x REDIS SHARDS ==========
const redisUrls = [
  process.env.REDISURL1,
  process.env.REDISURL2,
  process.env.REDISURL3
].map(u => (u && u.trim())? u.trim() : 'redis://127.0.0.1:6379');

const redisClients = {
  redis1: new Redis(redisUrls[0], { maxRetriesPerRequest: 1, retryStrategy: (times) => Math.min(times * 50, 2000) }),
  redis2: new Redis(redisUrls[1] || redisUrls[0], { maxRetriesPerRequest: 1, retryStrategy: (times) => Math.min(times * 50, 2000) }),
  redis3: new Redis(redisUrls[2] || redisUrls[0], { maxRetriesPerRequest: 1, retryStrategy: (times) => Math.min(times * 50, 2000) }),
};

Object.entries(redisClients).forEach(([name, client]) => {
  client.on('error', (err) => console.error(`[Redis Error] Shard ${name}: ${err.message}`));
  client.on('connect', () => console.log(`[Redis Connected] Shard ${name} established.`));
});

// ========== 1x PROFILE POSTGRES POOL ==========
const profilePool = new Pool({
  connectionString: process.env.DATABASEURL4,
  max: 10
});
profilePool.on('error', (err) => console.error('[ProfileDB Error]', err.message));

// ========== HELPERS ==========
function getShardIndex(id) {
  if (!id) return 0;
  return parseInt(id, 36) % 3;
}

function getDbShard(userId) {
  const idx = getShardIndex(userId);
  if (idx === 1) return { client: prismaClients.db2, name: 'db2' };
  if (idx === 2) return { client: prismaClients.db3, name: 'db3' };
  return { client: prismaClients.db1, name: 'db1' };
}

function getRedisShard(userId) {
  const idx = getShardIndex(userId);
  if (idx === 1) return redisClients.redis2;
  if (idx === 2) return redisClients.redis3;
  return redisClients.redis1;
}

module.exports = { prismaClients, redisClients, getDbShard, getRedisShard, profilePool };
