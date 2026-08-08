
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
  db2: new PrismaClient({ datasources: { db: { url: dbUrls[1] || dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db2" } } }),
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
].map(u => (u && u.trim()) ? u.trim() : 'redis://127.0.0.1:6379');

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
const crypto = require('crypto');

function getShardIndex(id) {
  if (!id) return 0;
  // MD5 hash the id, take first 8 chars, convert to int, then %3
  const hash = crypto.createHash('md5').update(id).digest('hex');
  return parseInt(hash.substring(0, 8), 16) % 3;
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

// ========== TRANSACTION CONCURRENCY ENGINE ==========
async function processWalletTransaction({ userId, action, isCreator, meta = {} }) {
  if (!userId) return;
  const redis = getRedisShard(userId);
  const db = getDbShard(userId);

  let lockAcquired = false;
  try {
    const lock = await redis.set(`lock:${userId}`, '1', 'EX', 3, 'NX').catch(() => 'DYNAMIC_PASS');
    if (!lock) return;
    lockAcquired = true;

    const user = await db.client.user.findUnique({ where: { id: userId } }).catch(() => null);
    if (!user) return;

    const walletType = user.monetizeFlag ? 'CASH' : 'FREE';
    let pointsToAdd = 0;

    switch (action) {
      case 'LIKE': pointsToAdd = isCreator ? 10 : 1; break;
      case 'COMMENT': pointsToAdd = isCreator ? 15 : 3; break;
      case 'VIEW_REEL': pointsToAdd = isCreator ? 2 : 0; break; 
      case 'READ_NOVEL': pointsToAdd = 10; break;
      case 'READ_STORY': pointsToAdd = 10; break;
      case 'REFERRAL_BONUS': pointsToAdd = 1000; break;
      case 'GIFT': pointsToAdd = meta.points || 0; break; 
    }
    if (pointsToAdd === 0) return;

    if (walletType === 'CASH') {
      const today = new Date().toISOString().split('T')[0];
      const capKey = `cap:${userId}:${today}`;
      const current = parseFloat(await redis.get(capKey).catch(() => '0') || '0');
      if (current >= 10000) return;
      if (current + pointsToAdd > 10000) pointsToAdd = 10000 - current;
      await redis.incrbyfloat(capKey, pointsToAdd).catch(() => {});
      await redis.expire(capKey, 90000).catch(() => {});
    }

    if (!isCreator) {
      const limitKey = `limit:${userId}:${action.toLowerCase()}`;
      const count = await redis.incr(limitKey).catch(() => 0);
      if (action === 'LIKE' && count > 50) return;
      if (action === 'COMMENT' && count > 30) return;
      await redis.expire(limitKey, 86400).catch(() => {});
    }

    await db.client.$transaction([
      db.client.pointsLedger.create({
        data: { userId, amount: pointsToAdd, type: walletType, action, referenceId: meta.refId || '' }
      }),
      db.client.user.update({
        where: { id: userId },
        data: {
          freeCredits: walletType === 'FREE' ? { increment: pointsToAdd } : undefined,
          cashBalance: walletType === 'CASH' ? { increment: pointsToAdd } : undefined,
        }
      })
    ]);
  } catch (err) {
    console.error(`[Transaction Intercept] Error: ${err.message}`);
  } finally {
    if (lockAcquired) {
      await redis.del(`lock:${userId}`).catch(() => {});
    }
  }
}

module.exports = { 
  prismaClients, 
  redisClients, 
  getDbShard, 
  getRedisShard, 
  profilePool, 
  processWalletTransaction // <-- EXPORT IT HERE
};


