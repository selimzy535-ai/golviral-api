const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const axios = require('axios');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// --- INIT ---
const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(cors({ origin: '*' }));
app.use(helmet());
app.use(morgan('combined'));

const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_shard_key_2026';
const APP_BASE_URL = process.env.APP_BASE_URL || 'https://v45-reels-app.pwa';

// ========== 3x SHARDING ==========
const prismaClients = {
  db1: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_1 } }),
  db2: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_2 } }),
  db3: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_3 } }),
};

const redisClients = {
  redis1: new Redis(process.env.REDIS_URL_1),
  redis2: new Redis(process.env.REDIS_URL_2),
  redis3: new Redis(process.env.REDIS_URL_3),
};

const b2Clients = {
  b2a: new S3Client({ endpoint: process.env.B2_ENDPOINT_A, credentials: { accessKeyId: process.env.B2_KEY_ID_A, secretAccessKey: process.env.B2_APPLICATION_KEY_A }, region: process.env.B2_REGION_A }),
  b2b: new S3Client({ endpoint: process.env.B2_ENDPOINT_B, credentials: { accessKeyId: process.env.B2_KEY_ID_B, secretAccessKey: process.env.B2_APPLICATION_KEY_B }, region: process.env.B2_REGION_B }),
  b2c: new S3Client({ endpoint: process.env.B2_ENDPOINT_C, credentials: { accessKeyId: process.env.B2_KEY_ID_C, secretAccessKey: process.env.B2_APPLICATION_KEY_C }, region: process.env.B2_REGION_C }),
};

// ========== v4.5 SHARDING: NUMERIC % 3 ==========
function getShardIndex(userId) {
  // userId is string → convert to int via base36
  return parseInt(userId, 36) % 3;
}

function getDbShard(userId) {
  const idx = getShardIndex(userId);
  if (idx === 0) return { client: prismaClients.db1, name: 'db1' };
  if (idx === 1) return { client: prismaClients.db2, name: 'db2' };
  return { client: prismaClients.db3, name: 'db3' };
}

function getRedisShard(userId) {
  const idx = getShardIndex(userId);
  if (idx === 0) return redisClients.redis1;
  if (idx === 1) return redisClients.redis2;
  return redisClients.redis3;
}

function getB2Shard(postId) {
  const idx = parseInt(postId, 36) % 3; // FIXED: b2Clients not b2clients
  if (idx === 0) return { client: b2Clients.b2a, bucket: process.env.B2_BUCKET_A };
  if (idx === 1) return { client: b2Clients.b2b, bucket: process.env.B2_BUCKET_B };
  return { client: b2Clients.b2c, bucket: process.env.B2_BUCKET_C };
}

async function findUserAcrossShards(field, value) {
  const shards = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (let i = 0; i < shards.length; i++) {
    const user = await shards[i].user.findUnique({ where: { [field]: value } });
    if (user) return { user, db: shards[i] };
  }
  return null;
}

// ========== 10s BUFFER ==========
let interactionBuffer = [];

// ========== EMAIL ==========
async function sendMailNotification(email, subject, text) {
  const mailOptions = { from: process.env.EMAIL_FROM, to: email, subject, text };
  try {
    const brevoTransport = nodemailer.createTransport({
      host: 'smtp-relay.brevo.com',
      port: 587,
      auth: { user: process.env.BREVO_USER, pass: process.env.BREVO_PASS }
    });
    await brevoTransport.sendMail(mailOptions);
  } catch {
    await axios.post('https://api.resend.com/emails', {
      from: process.env.EMAIL_FROM,
      to: [email],
      subject,
      text
    }, { headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}` } }).catch(() => {});
  }
}

// ========== AUTH ==========
async function verifyTurnstile(req, res, next) {
  const token = req.body.turnstileToken;
  if (!token) return res.status(400).json({ error: 'Captcha required' });
  const outcome = await axios.post('https://challenges.cloudflare.com/turnstile/v0/siteverify', null, {
    params: { secret: process.env.TURNSTILE_SECRET_KEY, response: token }
  });
  if (!outcome.data.success) return res.status(403).json({ error: 'Captcha failed' });
  next();
}

function authenticateToken(req, res, next) {
  const token = req.headers['authorization']?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token required' });
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Invalid token' });
    req.user = user;
    next();
  });
}

// ========== v4.5 WALLET ENGINE ==========
async function processWalletTransaction({ userId, action, isCreator, meta = {} }) {
  const redis = getRedisShard(userId);
  const dbConfig = getDbShard(userId);
  const lockKey = `lock:${userId}`;
  const lock = await redis.set(lockKey, '1', 'EX', 2, 'NX');
  if (!lock) throw new Error('Concurrent lock');

  try {
    const user = await dbConfig.client.user.findUnique({ where: { id: userId } });
    if (!user) throw new Error('User not found');

    const isMonetized = user.monetizeFlag;
    let pointsToAdd = 0;
    const walletType = isMonetized? 'CASH' : 'FREE';

    // v4.5 split rewards
    switch (action) {
      case 'LIKE':
        pointsToAdd = isCreator? 5 : 1;
        break;
      case 'COMMENT':
        pointsToAdd = isCreator? 10 : 3;
        break;
      case 'VIEW_REEL':
        pointsToAdd = isCreator? 0.5 : 0;
        break;
      case 'READ_NOVEL':
      case 'READ_STORY':
        pointsToAdd = isCreator? 10 : 10; // both get 10pts v4.5
        break;
      case 'REFERRAL_BONUS':
        pointsToAdd = 1000;
        break;
    }

    if (pointsToAdd === 0) return;

    // v4.5: CHECK CAP BEFORE DB WRITE
    if (walletType === 'CASH') {
      const today = new Date().toISOString().split('T')[0];
      const capKey = `cap:${userId}:${today}`;
      const current = parseFloat(await redis.get(capKey) || '0');
      if (current >= 10000) return;
      if (current + pointsToAdd > 10000) {
        pointsToAdd = 10000 - current;
      }
      await redis.incrbyfloat(capKey, pointsToAdd);
      await redis.expire(capKey, 90000);
    }

    // v4.5: DAILY LIMITS FOR ACTORS ONLY
    if (!isCreator) {
      const limitKey = `limit:${userId}:${action.toLowerCase()}`;
      const count = await redis.incr(limitKey);
      if (action === 'LIKE' && count > 50) return;
      if (action === 'COMMENT' && count > 30) return;
      await redis.expire(limitKey, 86400);
    }

    await dbConfig.client.$transaction([
      dbConfig.client.pointsLedger.create({
        data: { userId, amount: pointsToAdd, type: walletType, action, referenceId: meta.refId || '' }
      }),
      dbConfig.client.user.update({
        where: { id: userId },
        data: {
          freeCredits: walletType === 'FREE'? { increment: pointsToAdd } : undefined,
          cashBalance: walletType === 'CASH'? { increment: pointsToAdd } : undefined,
        }
      })
    ]);

  } finally {
    await redis.del(lockKey);
  }
}

// ========== ROUTES ==========
app.post('/api/auth/signup', verifyTurnstile, async (req, res) => {
  const { username, email, password, referralCode } = req.body;
  const existing = await findUserAcrossShards('email', email);
  if (existing) return res.status(400).json({ error: 'Email taken' });

  const hashed = await bcrypt.hash(password, 12);
  const userId = crypto.randomBytes(8).toString('hex');
  const db = getDbShard(userId);

  const user = await db.client.user.create({
    data: {
      id: userId,
      username,
      email,
      password: hashed,
      freeCredits: 1500, // v4.5 signup bonus
      monetizeFlag: false,
      freeFarmingStopped: false
    }
  });

  if (referralCode) {
    const refUser = await findUserAcrossShards('id', referralCode);
    if (refUser) {
      await db.client.referral.create({
        data: { referrerId: referralCode, refereeId: userId, status: 'PENDING' }
      });
    }
  }

  const token = jwt.sign({ userId, username }, JWT_SECRET, { expiresIn: '30d' });
  res.status(201).json({ token, userId, profileLink: `${APP_BASE_URL}/u/${userId}` });
});
// ========== ADMIN ENDPOINTS v4.5 ==========
function verifyAdminKey(req, res, next) {
  const key = req.headers['x-admin-key'];
  if (!key || key !== process.env.ADMIN_SECRET_KEY) {
    return res.status(403).json({ error: 'Admin key required' });
  }
  next();
}

// 1. Get all pending payouts
app.get('/api/admin/payouts', verifyAdminKey, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const payouts = await db.payoutQueue.findMany({ 
      where: { status: 'PENDING' },
      include: { user: { select: { username: true, email: true } }
    });
    all.push(...payouts);
  }
  res.json(all);
});

// 2. Reject payout + refund cash
app.post('/api/admin/payouts/reject', verifyAdminKey, async (req, res) => {
  const { payoutId, userId, reason } = req.body;
  const db = getDbShard(userId);
  
  const payout = await db.payoutQueue.findUnique({ where: { id: payoutId } });
  if (!payout) return res.status(404).json({ error: 'Payout not found' });

  await db.$transaction([
    db.user.update({ 
      where: { id: userId }, 
      data: { cashBalance: { increment: payout.amountPoints } } 
    }),
    db.payoutQueue.update({ 
      where: { id: payoutId }, 
      data: { status: 'REJECTED', reason } 
    })
  ]);

  const user = await db.user.findUnique({ where: { id: userId } });
  await sendMailNotification(user.email, 'Payout Rejected', `Reason: ${reason}. ${payout.amountPoints}pts refunded.`);

  res.json({ message: 'Payout rejected + refunded' });
});

// Your existing approve endpoint
app.post('/api/admin/payouts/approve', verifyAdminKey, async (req, res) => {
  const { payoutId, userId } = req.body;
  const db = getDbShard(userId);
  await db.payoutQueue.update({ where: { id: payoutId }, data: { status: 'APPROVED' } });
  res.json({ success: true });
});
app.post('/api/post/create-intent', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { fileExtension, contentType } = req.body;
  const db = getDbShard(userId);
  const redis = getRedisShard(userId);

  const lock = await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX');
  if (!lock) return res.status(423).json({ error: 'Busy' });

  try {
    const user = await db.client.user.findUnique({ where: { id: userId } });
    if (user.freeCredits < 25) return res.status(400).json({ error: 'Need 25 credits' });

    const today = new Date().toISOString().split('T')[0];
    const postsKey = `posts:${userId}:${today}`;
    const postsToday = parseInt(await redis.get(postsKey) || '0');
    if (postsToday >= 3) return res.status(429).json({ error: '3 posts/day limit' });

    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { decrement: 25 } });

    const postId = crypto.randomBytes(8).toString('hex');
    const b2 = getB2Shard(postId);
    const key = `media/${postId}.${fileExtension}`;

    const cmd = new PutObjectCommand({ Bucket: b2.bucket, Key: key, ContentType: contentType });
    const url = await getSignedUrl(b2.client, cmd, { expiresIn: 3600 });

    await db.client.post.create({
      data: { id: postId, userId, mediaUrl: key, status: 'PENDING_WORKER', b2Shard: getShardIndex(postId) }
    });

    await redis.incr(postsKey);
    await redis.expire(postsKey, 86400);

    // Queue worker
    await redis.lpush('video-integrity', JSON.stringify({ postId, userId, key }));

    res.json({ postId, presignedUrl: url, bucket: b2.bucket, objectKey: key });
  } finally {
    await redis.del(`lock:${userId}`);
  }
});
app.get('/api/wallet', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const db = getDbShard(userId);
  const redis = getRedisShard(userId);
  const today = new Date().toISOString().split('T')[0];

  const user = await db.user.findUnique({ where: { id: userId } });
  const todayEarned = parseFloat(await redis.get(`cap:${userId}:${today}`) || '0');
  const refs = await db.referral.count({ where: { referrerId: userId, status: 'QUALIFIED' } });
  const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);

  res.json({
    freeCredits: user.freeCredits,
    cashBalance: user.cashBalance,
    todayEarnings: todayEarned,
    dailyCapProgress: `${todayEarned}/10000`,
    daysToMonetize: Math.max(0, 7 - days),
    refsLeft: Math.max(0, 5 - refs),
    monetized: user.monetizeFlag
  });
});
app.post('/api/worker/callback', async (req, res) => {
  const { postId, status, userId } = req.body;
  const db = getDbShard(userId);

  if (status === 'REJECTED') {
    const redis = getRedisShard(userId);
    await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX');
    await db.client.$transaction([
      db.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: 25 } }),
      db.client.post.update({ where: { id: postId }, data: { status: 'REJECTED' } })
    ]);
    await redis.del(`lock:${userId}`);
    return res.json({ message: 'Refunded 25pts' });
  }

  await db.client.post.update({ where: { id: postId }, data: { status: 'ACTIVE' } });
  res.json({ message: 'Approved' });
});

// v4.5: Buffer endpoints
app.post('/api/view', (req, res) => {
  interactionBuffer.push({...req.body, type: 'VIEW', timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

app.post('/api/interact', authenticateToken, (req, res) => {
  interactionBuffer.push({...req.body, actorId: req.userId, timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

app.post('/api/read-session', authenticateToken, (req, res) => {
  interactionBuffer.push({...req.body, userId: req.user.userId, type: 'READ', timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

// ========== 10s CRON v4.5 ==========
cron.schedule('*/10 *', async () => {
  if (interactionBuffer.length === 0) return;
  const batch = [...interactionBuffer];
  interactionBuffer = [];

  for (const item of batch) {
    try {
      if (item.type === 'VIEW') {
        const redis = getRedisShard(item.userId);
        const added = await redis.pfadd(`view:${item.postId}`, item.viewerId || item.viewerIp);
        if (added === 1) {
          await processWalletTransaction({ userId: item.userId, action: 'VIEW_REEL', isCreator: true, meta: { refId: item.postId } });
        }
      } else if (item.type === 'LIKE' || item.type === 'COMMENT') {
        await processWalletTransaction({ userId: item.userId, action: item.type, isCreator: true, meta: { refId: item.postId } });
        await processWalletTransaction({ userId: item.actorId, action: item.type, isCreator: false, meta: { refId: item.postId } });
      } else if (item.type === 'READ') {
        const redis = getRedisShard(item.userId);
        const coolKey = `cool:read:${item.userId}:${item.contentId}`;
        if (!await redis.get(coolKey)) {
          const delay = item.contentType === 'NOVEL'? 120 : 180;
          await redis.set(coolKey, '1', 'EX', delay);
          await processWalletTransaction({ userId: item.authorId, action: `READ_${item.contentType}`, isCreator: true, meta: { refId: item.contentId } });
          await processWalletTransaction({ userId: item.userId, action: `READ_${item.contentType}`, isCreator: false, meta: { refId: item.contentId } });
        }
      }
    } catch {}
  }
});

// ========== MONETIZE CRON 12am v4.5 ==========
cron.schedule('0 0 *', async () => {
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const users = await db.user.findMany({ where: { monetizeFlag: false } });
    for (const user of users) {
      const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);
      const refs = await db.referral.count({ where: { referrerId: user.id, status: 'QUALIFIED' } });
      if (days >= 7 && refs >= 5) {
        await db.user.update({ where: { id: user.id }, data: { monetizeFlag: true, freeFarmingStopped: true } });
        await sendMailNotification(user.email, 'Monetization Unlocked', 'You now earn cash!');
      }
    }
  }
});

// ========== DELETE CRON 3am v4.5 ==========
cron.schedule('0 3 *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  const clusters = [
    { db: prismaClients.db1, b2: b2Clients.b2a, bucket: process.env.B2_BUCKET_A },
    { db: prismaClients.db2, b2: b2Clients.b2b, bucket: process.env.B2_BUCKET_B },
    { db: prismaClients.db3, b2: b2Clients.b2c, bucket: process.env.B2_BUCKET_C }
  ];
  for (const c of clusters) {
    const posts = await c.db.post.findMany({ where: { createdAt: { lt: cutoff } });
    for (const p of posts) {
      await c.b2.send(new (require('@aws-sdk/client-s3').DeleteObjectCommand)({ Bucket: c.bucket, Key: p.mediaUrl }));
    }
    await c.db.post.deleteMany({ where: { createdAt: { lt: cutoff } });
  }
});

app.listen(PORT, () => console.log(`v4.5 API running on ${PORT}`));
