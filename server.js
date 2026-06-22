const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const { S3Client, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const axios = require('axios');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// ========== INIT ==========
const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(cors({ origin: '*' }));
app.use(helmet());
app.use(morgan('combined'));

const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_shard_key_2026';
const APP_BASE_URL = process.env.APP_BASE_URL || 'https://v45-reels-app.pwa';

// ========== 3x SHARDING CLIENTS ==========
const prismaClients = {
  db1: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_1 || "postgresql://mock:fallback@localhost:5432/db1" } } }),
  db2: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_2 || "postgresql://mock:fallback@localhost:5432/db2" } } }),
  db3: new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL_3 || "postgresql://mock:fallback@localhost:5432/db3" } } }),
};

// ========== 3x REDIS CLIENTS (ULTRA-SAFE FALLBACK) ==========
const primaryRedisUrl = process.env.REDIS_URL_1 && process.env.REDIS_URL_1.trim() 
  ? process.env.REDIS_URL_1 
  : 'redis://127.0.0.1:6379';

// Explicitly check if strings are valid, otherwise forcefully clamp to primary
const r2Url = process.env.REDIS_URL_2 && process.env.REDIS_URL_2.trim() ? process.env.REDIS_URL_2 : primaryRedisUrl;
const r3Url = process.env.REDIS_URL_3 && process.env.REDIS_URL_3.trim() ? process.env.REDIS_URL_3 : primaryRedisUrl;

const redisClients = {
  redis1: new Redis(primaryRedisUrl),
  redis2: new Redis(r2Url),
  redis3: new Redis(r3Url),
};

// Catch and handle background connection errors gracefully
Object.entries(redisClients).forEach(([shardName, client]) => {
  client.on('error', (err) => {
    console.error(`[Redis Error] Shard ${shardName} connection failed:`, err.message);
  });
});
const b2Clients = {
  b2a: new S3Client({ endpoint: process.env.B2_ENDPOINT_A, credentials: { accessKeyId: process.env.B2_KEY_ID_A, secretAccessKey: process.env.B2_APPLICATION_KEY_A }, region: process.env.B2_REGION_A }),
  b2b: new S3Client({ endpoint: process.env.B2_ENDPOINT_B, credentials: { accessKeyId: process.env.B2_KEY_ID_B, secretAccessKey: process.env.B2_APPLICATION_KEY_B }, region: process.env.B2_REGION_B }),
  b2c: new S3Client({ endpoint: process.env.B2_ENDPOINT_C, credentials: { accessKeyId: process.env.B2_KEY_ID_C, secretAccessKey: process.env.B2_APPLICATION_KEY_C }, region: process.env.B2_REGION_C }),
};

// ========== SHARDING HELPERS v4.5 ==========
function getShardIndex(id) {
  return parseInt(id, 36) % 3;
}

function getDbShard(userId) {
  const idx = getShardIndex(userId);
  return idx === 0? { client: prismaClients.db1 } : idx === 1? { client: prismaClients.db2 } : { client: prismaClients.db3 };
}

function getRedisShard(userId) {
  const idx = getShardIndex(userId);
  return idx === 0? redisClients.redis1 : idx === 1? redisClients.redis2 : redisClients.redis3;
}

function getB2Shard(postId) {
  const idx = getShardIndex(postId);
  return idx === 0? { client: b2Clients.b2a, bucket: process.env.B2_BUCKET_A } :
         idx === 1? { client: b2Clients.b2b, bucket: process.env.B2_BUCKET_B } :
         { client: b2Clients.b2c, bucket: process.env.B2_BUCKET_C };
}

async function findUserAcrossShards(field, value) {
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const user = await db.user.findUnique({ where: { [field]: value } });
    if (user) return { user, db };
  }
  return null;
}

// ========== 10s BUFFER ==========
let interactionBuffer = [];

// ========== EMAIL ENGINE ==========
async function sendMailNotification(email, subject, text) {
  const mailOptions = { from: process.env.EMAIL_FROM, to: email, subject, text };
  try {
    const brevo = nodemailer.createTransport({
      host: 'smtp-relay.brevo.com',
      port: 587,
      auth: { user: process.env.BREVO_USER, pass: process.env.BREVO_PASS }
    });
    await brevo.sendMail(mailOptions);
  } catch {
    await axios.post('https://api.resend.com/emails', {
      from: process.env.EMAIL_FROM, to: [email], subject, text
    }, { headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}` } }).catch(() => {});
  }
}

// ========== MIDDLEWARES ==========
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

function verifyAdminKey(req, res, next) {
  const key = req.headers['x-admin-key'];
  if (!key || key!== process.env.ADMIN_SECRET_KEY) {
    return res.status(403).json({ error: 'Admin key required' });
  }
  next();
}

// ========== WALLET ENGINE v4.5 ==========
async function processWalletTransaction({ userId, action, isCreator, meta = {} }) {
  const redis = getRedisShard(userId);
  const db = getDbShard(userId);
  const lock = await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX');
  if (!lock) throw new Error('Concurrent lock');

  try {
    const user = await db.client.user.findUnique({ where: { id: userId } });
    if (!user) return;

    const walletType = user.monetizeFlag? 'CASH' : 'FREE';
    let pointsToAdd = 0;

    switch (action) {
      case 'LIKE': pointsToAdd = isCreator? 5 : 1; break;
      case 'COMMENT': pointsToAdd = isCreator? 10 : 3; break;
      case 'VIEW_REEL': pointsToAdd = isCreator? 0.5 : 0; break;
      case 'READ_NOVEL': pointsToAdd = 10; break;
      case 'READ_STORY': pointsToAdd = 10; break;
      case 'REFERRAL_BONUS': pointsToAdd = 1000; break;
    }
    if (pointsToAdd === 0) return;

    // Daily cap check BEFORE DB
    if (walletType === 'CASH') {
      const today = new Date().toISOString().split('T')[0];
      const capKey = `cap:${userId}:${today}`;
      const current = parseFloat(await redis.get(capKey) || '0');
      if (current >= 10000) return;
      if (current + pointsToAdd > 10000) pointsToAdd = 10000 - current;
      await redis.incrbyfloat(capKey, pointsToAdd);
      await redis.expire(capKey, 90000);
    }

    // Daily limits for actors only
    if (!isCreator) {
      const limitKey = `limit:${userId}:${action.toLowerCase()}`;
      const count = await redis.incr(limitKey);
      if (action === 'LIKE' && count > 50) return;
      if (action === 'COMMENT' && count > 30) return;
      await redis.expire(limitKey, 86400);
    }

    await db.client.$transaction([
      db.client.pointsLedger.create({
        data: { userId, amount: pointsToAdd, type: walletType, action, referenceId: meta.refId || '' }
      }),
      db.client.user.update({
        where: { id: userId },
        data: {
          freeCredits: walletType === 'FREE'? { increment: pointsToAdd } : undefined,
          cashBalance: walletType === 'CASH'? { increment: pointsToAdd } : undefined,
        }
      })
    ]);
  } finally {
    await redis.del(`lock:${userId}`);
  }
}
// Base Gateway Root Route
app.get('/', (req, res) => {
  res.status(200).json({
    status: "online",
    platform: "GolViral API Gateway",
    version: "4.5",
    timestamp: new Date()
  });
});
// ========== AUTH ROUTES ==========
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
      freeCredits: 1500,
      cashBalance: 0,
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

app.post('/api/auth/login', async (req, res) => {
  const { email, password } = req.body;
  const context = await findUserAcrossShards('email', email);
  if (!context) return res.status(401).json({ error: 'Invalid credentials' });

  const match = await bcrypt.compare(password, context.user.password);
  if (!match) return res.status(401).json({ error: 'Invalid credentials' });

  const token = jwt.sign({ userId: context.user.id, username: context.user.username }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, userId: context.user.id, profileLink: `${APP_BASE_URL}/u/${context.user.id}` });
});

app.post('/api/auth/forgot-password', async (req, res) => {
  const { email } = req.body;
  const context = await findUserAcrossShards('email', email);
  if (!context) return res.json({ message: 'If account exists, code sent' });

  const otp = Math.floor(100000 + Math.random() * 900000).toString();
  const redis = getRedisShard(context.user.id);
  await redis.set(`otp:${email}`, otp, 'EX', 900);

  await sendMailNotification(email, 'Password Reset Code', `Your OTP: ${otp}. Valid 15min.`);
  res.json({ message: 'If account exists, code sent' });
});

app.post('/api/auth/reset-password', async (req, res) => {
  const { email, otp, newPassword } = req.body;
  const context = await findUserAcrossShards('email', email);
  if (!context) return res.status(400).json({ error: 'Invalid request' });

  const redis = getRedisShard(context.user.id);
  const savedOtp = await redis.get(`otp:${email}`);
  if (savedOtp!== otp) return res.status(400).json({ error: 'Invalid or expired OTP' });

  const hash = await bcrypt.hash(newPassword, 12);
  await context.db.user.update({ where: { email }, data: { password: hash } });
  await redis.del(`otp:${email}`);
  res.json({ message: 'Password updated' });
});

// ========== MEDIA ROUTES ==========
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

    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { decrement: 25 } } });

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
    await redis.lpush('video-integrity', JSON.stringify({ postId, userId, key }));

    res.json({ postId, presignedUrl: url, bucket: b2.bucket, objectKey: key });
  } finally {
    await redis.del(`lock:${userId}`);
  }
});

app.post('/api/worker/callback', async (req, res) => {
  const { postId, status, userId } = req.body;
  const db = getDbShard(userId);

  if (status === 'REJECTED') {
    const redis = getRedisShard(userId);
    await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX');
    await db.client.$transaction([
      db.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: 25 } } }),
      db.client.post.update({ where: { id: postId }, data: { status: 'REJECTED' } })
    ]);
    await redis.del(`lock:${userId}`);
    return res.json({ message: 'Refunded 25pts' });
  }

  await db.client.post.update({ where: { id: postId }, data: { status: 'ACTIVE' } });
  res.json({ message: 'Approved' });
});

// ========== INTERACTION BUFFER ROUTES ==========
app.post('/api/view', (req, res) => {
  interactionBuffer.push({...req.body, type: 'VIEW', timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

app.post('/api/interact', authenticateToken, (req, res) => {
  interactionBuffer.push({...req.body, actorId: req.user.userId, timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

app.post('/api/read-session', authenticateToken, (req, res) => {
  interactionBuffer.push({...req.body, userId: req.user.userId, type: 'READ', timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

// ========== WALLET ROUTE ==========
app.get('/api/wallet', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const db = getDbShard(userId);
  const redis = getRedisShard(userId);
  const today = new Date().toISOString().split('T')[0];

  const user = await db.client.user.findUnique({ where: { id: userId } });
  const todayEarned = parseFloat(await redis.get(`cap:${userId}:${today}`) || '0');
  const refs = await db.client.referral.count({ where: { referrerId: userId, status: 'QUALIFIED' } });
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

// ========== ADMIN ROUTES ==========
app.get('/api/admin/payouts', verifyAdminKey, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const payouts = await db.payoutQueue.findMany({
      where: { status: 'PENDING' },
      include: { user: { select: { username: true, email: true } } }
    });
    all.push(...payouts);
  }
  res.json(all);
});

app.post('/api/admin/payouts/approve', verifyAdminKey, async (req, res) => {
  const { payoutId, userId } = req.body;
  const db = getDbShard(userId);
  await db.client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'APPROVED' } });
  res.json({ success: true });
});

app.post('/api/admin/payouts/reject', verifyAdminKey, async (req, res) => {
  const { payoutId, userId, reason } = req.body;
  const db = getDbShard(userId);

  const payout = await db.client.payoutQueue.findUnique({ where: { id: payoutId } });
  if (!payout) return res.status(404).json({ error: 'Payout not found' });

  await db.client.$transaction([
    db.client.user.update({ where: { id: userId }, data: { cashBalance: { increment: payout.amountPoints } } }),
    db.client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'REJECTED', reason } })
  ]);

  const user = await db.client.user.findUnique({ where: { id: userId } });
  await sendMailNotification(user.email, 'Payout Rejected', `Reason: ${reason}. ${payout.amountPoints}pts refunded.`);
  res.json({ message: 'Payout rejected + refunded' });
});

// ========== CRONS v4.5 ==========
// 10s buffer processor
cron.schedule('*/10 * * * * *', async () => {
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

// Monetize unlock 12am daily
cron.schedule('0 0 * * *', async () => {
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

// 15-day auto delete 3am daily
cron.schedule('0 3 * * *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  const clusters = [
    { db: prismaClients.db1, b2: b2Clients.b2a, bucket: process.env.B2_BUCKET_A },
    { db: prismaClients.db2, b2: b2Clients.b2b, bucket: process.env.B2_BUCKET_B },
    { db: prismaClients.db3, b2: b2Clients.b2c, bucket: process.env.B2_BUCKET_C }
  ];
  for (const c of clusters) {
    const posts = await c.db.post.findMany({ where: { createdAt: { lt: cutoff } } });
    for (const p of posts) {
      await c.b2.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: p.mediaUrl })).catch(() => {});
    }
    await c.db.post.deleteMany({ where: { createdAt: { lt: cutoff } } });
  }
});

app.listen(PORT, () => console.log(`v4.5 API running on ${PORT}`));
