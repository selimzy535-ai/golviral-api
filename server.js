require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const Redis = require('ioredis');
const cron = require('node-cron');
const axios = require('axios');
const { Queue } = require('bullmq');
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { PrismaClient } = require('@prisma/client');

const app = express();
app.use(express.json());
app.use(cors({ origin: 'https://golviral.com' }));

// Global System Architecture Variables
let GLOBAL_B2_OVERFLOW = false;
const JWT_SECRET = process.env.JWT_SECRET || 'golviral-sec-v4.4';
const PORT = process.env.PORT || 3000;

// --- MULTI-SHARD DATABASE ARCHITECTURE ---
const prisma1 = new PrismaClient({ datasources: { db1: { url: process.env.DATABASE_URL_1 } } });
const prisma2 = new PrismaClient({ datasources: { db2: { url: process.env.DATABASE_URL_2 } } });
const prisma3 = new PrismaClient({ datasources: { db3: { url: process.env.DATABASE_URL_3 } } });

function getPrismaShard(id) {
  if (!id) return prisma1;
  const hash = id.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const rem = hash % 3;
  if (rem === 1) return prisma2;
  if (rem === 2) return prisma3;
  return prisma1;
}

// --- REDIS ARCHITECTURE (UPSTASH PROXIES) ---
const redis1 = new Redis(process.env.REDIS_URL_1); // Auth, Locks, Counters
const redis2 = new Redis(process.env.REDIS_URL_2); // High-frequency HyperLogLog metrics

// --- BULLMQ PROCESSING PIPELINE ---
const mediaQueue = new Queue('media-processing', { connection: new Redis(process.env.REDIS_URL_1) });

// --- BACKBLAZE B2 OBJECT STORAGE ENGINE ---
const b2Clients = {
  A: new S3Client({ endpoint: process.env.B2_ENDPOINT_A, credentials: { accessKeyId: process.env.B2_KEY_ID_A, secretAccessKey: process.env.B2_APPLICATION_KEY_A }, region: process.env.B2_REGION_A }),
  B: new S3Client({ endpoint: process.env.B2_ENDPOINT_B, credentials: { accessKeyId: process.env.B2_KEY_ID_B, secretAccessKey: process.env.B2_APPLICATION_KEY_B }, region: process.env.B2_REGION_B }),
  C: new S3Client({ endpoint: process.env.B2_ENDPOINT_C, credentials: { accessKeyId: process.env.B2_KEY_ID_C, secretAccessKey: process.env.B2_APPLICATION_KEY_C }, region: process.env.B2_REGION_C })
};

const b2Buckets = { A: process.env.B2_BUCKET_NAME_A, B: process.env.B2_BUCKET_NAME_B, C: process.env.B2_BUCKET_NAME_C };

function selectB2Shard(userId) {
  const hash = userId.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const rem = hash % 3;
  return rem === 1 ? 'B' : rem === 2 ? 'C' : 'A';
}

// --- 10-SECOND MEMORY BUFFER ENGINE (CRITICAL UPSTASH PROTECTION) ---
let writeBuffer = [];

setInterval(async () => {
  if (writeBuffer.length === 0) return;
  const batch = writeBuffer.splice(0, writeBuffer.length);
  
  try {
    const viewLogs = batch.filter(item => item.type === 'VIEW');
    const likes = batch.filter(item => item.type === 'LIKE');
    const comments = batch.filter(item => item.type === 'COMMENT');

    // Aggregate Views via Shards
    for (const view of viewLogs) {
      const shard = getPrismaShard(view.data.postId);
      await shard.viewLog.create({ data: view.data });
      await adjustPoints(view.data.userId, 10, 'FREE', 'VIEW_ENGAGEMENT');
    }

    // Aggregate Likes
    for (const like of likes) {
      const shard = getPrismaShard(like.data.postId);
      await shard.like.create({ data: like.data });
    }

    // Aggregate Comments
    for (const comment of comments) {
      const shard = getPrismaShard(comment.data.postId);
      await shard.comment.create({ data: comment.data });
    }
  } catch (error) {
    console.error("Buffer flusher database exception encountered. Rolled back buffer batch elements.", error);
    writeBuffer.unshift(...batch);
  }
}, 10000);

// --- ATOMIC POINTS ENGINE ---
async function adjustPoints(userId, amount, walletType, reason) {
  const lockKey = `lock:points:${userId}`;
  const locked = await redis1.set(lockKey, 'locked', 'NX', 'EX', 5);
  if (!locked) return false;

  try {
    const shard = getPrismaShard(userId);
    const user = await shard.user.findUnique({ where: { id: userId } });
    if (!user) return false;

    const todayStr = new Date().toISOString().split('T')[0];
    const capKey = `points:cap:${userId}:${todayStr}:${walletType}`;
    const currentDailyPoints = parseInt(await redis1.get(capKey) || '0');

    if (currentDailyPoints + amount > 10000) {
      return false; // Strict v4.4 limit hit
    }

    if (walletType === 'FREE') {
      await shard.user.update({ where: { id: userId }, data: { freeCredits: user.freeCredits + amount } });
    } else {
      await shard.user.update({ where: { id: userId }, data: { creatorCredits: user.creatorCredits + amount } });
    }

    await shard.pointsLedger.create({ data: { userId, amount, type: walletType, reason } });
    await redis1.incrby(capKey, amount);
    await redis1.expire(capKey, 90000);
    return true;
  } finally {
    await redis1.del(lockKey);
  }
}

// --- CLOUDFLARE TURNSTILE + BREVO VALIDATION ENGINE ---
async function verifyTurnstile(token) {
  try {
    const response = await axios.post('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
      secret: process.env.TURNSTILE_SECRET_KEY,
      response: token
    });
    return response.data.success;
  } catch { return false; }
}

async function verifyOTP(email, code) {
  const cachedCode = await redis1.get(`otp:${email}`);
  return cachedCode === code;
}

// --- SECURITY INTERACTION GATEWAY MIDDLEWARE ---
async function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.sendStatus(401);

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.sendStatus(403);
    req.user = user;
    next();
  });
}

// --- AUTHENTICATION PIPELINES ---
app.post('/api/auth/signup', async (req, res) => {
  const { email, password, turnstileToken, otpCode, referrerId } = req.body;
  if (!await verifyTurnstile(turnstileToken) || !await verifyOTP(email, otpCode)) {
    return res.status(400).json({ error: 'Security validation failed' });
  }

  const shard = getPrismaShard(email);
  const existingUser = await shard.user.findUnique({ where: { email } });
  if (existingUser) return res.status(400).json({ error: 'User exists' });

  const passwordHash = await bcrypt.hash(password, 10);
  const user = await shard.user.create({
    data: { email, passwordHash, freeCredits: 1500, referredBy: referrerId }
  });

  if (referrerId) {
    const refShard = getPrismaShard(referrerId);
    await refShard.referral.create({ data: { referrerId, refereeId: user.id } });
    await adjustPoints(referrerId, 250, 'FREE', 'REFERRAL_SIGNUP');
  }

  const token = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET);
  res.json({ token, userId: user.id });
});

app.post('/api/auth/login', async (req, res) => {
  const { email, password, turnstileToken } = req.body;
  if (!await verifyTurnstile(turnstileToken)) return res.status(400).json({ error: 'Invalid captcha' });

  const shard = getPrismaShard(email);
  const user = await shard.user.findUnique({ where: { email } });
  if (!user || !await bcrypt.compare(password, user.passwordHash)) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const token = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET);
  res.json({ token, userId: user.id });
});

// --- MEDIA PUBLISHING WORKFLOW ---
app.post('/api/posts/upload-intent', authenticateToken, async (req, res) => {
  if (GLOBAL_B2_OVERFLOW) return res.status(503).json({ error: 'Storage allocation limit reached' });

  const userId = req.user.id;
  const { type } = req.body; // "reel" or "explore"
  const dayKey = `user:postcount:${userId}:${new Date().toISOString().split('T')[0]}`;
  
  const dailyCount = await redis1.incr(dayKey);
  await redis1.expire(dayKey, 90000);
  if (dailyCount > 3) return res.status(429).json({ error: 'Daily upload cap reached' });

  const shard = getPrismaShard(userId);
  const user = await shard.user.findUnique({ where: { id: userId } });
  
  if (user.freeCredits < 25) return res.status(402).json({ error: 'Insufficient credits (25 required)' });
  await shard.user.update({ where: { id: userId }, data: { freeCredits: user.freeCredits - 25 } });

  const b2Shard = selectB2Shard(userId);
  const fileId = `${uuidv4()}.mp4`;
  const bucketName = b2Buckets[b2Shard];

  const presignedUrl = await getSignedUrl(
    b2Clients[b2Shard],
    new PutObjectCommand({ Bucket: bucketName, Key: fileId, ContentType: 'video/mp4' }),
    { expiresIn: 900 }
  );

  const post = await shard.mediaPost.create({
    data: { userId, mediaUrl: fileId, thumbUrl: '', type, status: 'PENDING', shard: b2Shard }
  });

  await mediaQueue.add('verify-integrity', { postId: post.id, userId, fileId, shard: b2Shard });

  res.json({ uploadUrl: presignedUrl, postId: post.id });
});

// --- PLATFORM INTERACTION AGGREGATION ENDPOINTS ---
app.post('/api/engagement/view', authenticateToken, async (req, res) => {
  const { postId } = req.body;
  const userId = req.user.id;

  const hllKey = `hll:view:${postId}`;
  const isUnique = await redis2.pfadd(hllKey, userId);
  
  if (isUnique === 1) {
    writeBuffer.push({ type: 'VIEW', data: { postId, userId } });
  }
  res.sendStatus(202);
});

app.post('/api/engagement/like', authenticateToken, async (req, res) => {
  const { postId } = req.body;
  const userId = req.user.id;
  const limitKey = `user:likes:${userId}:${new Date().toISOString().split('T')[0]}`;

  const currentLikes = await redis1.incr(limitKey);
  await redis1.expire(limitKey, 90000);
  if (currentLikes > 50) return res.status(429).json({ error: 'Daily interaction threshold reached' });

  writeBuffer.push({ type: 'LIKE', data: { postId, userId } });
  res.sendStatus(202);
});

// --- WORKER FEEDBACK API INTERFACE ---
app.post('/api/internal/worker-callback', async (req, res) => {
  const { postId, status, thumbUrl, refundReason } = req.body;
  const shard = getPrismaShard(postId);
  
  const post = await shard.mediaPost.findUnique({ where: { id: postId } });
  if (!post) return res.status(404).json({ error: 'Target entity not discovered' });

  if (status === 'APPROVED') {
    await shard.mediaPost.update({ where: { id: postId }, data: { status: 'APPROVED', thumbUrl } });
  } else {
    await shard.mediaPost.update({ where: { id: postId }, data: { status: 'REJECTED' } });
    const userShard = getPrismaShard(post.userId);
    const user = await userShard.user.findUnique({ where: { id: post.userId } });
    await userShard.user.update({ where: { id: post.userId }, data: { freeCredits: user.freeCredits + 25 } });
  }
  res.sendStatus(200);
});

// --- AUTOMATED NETWORK MANAGEMENT (CRON ENGINE) ---
cron.schedule('0 0 * * *', async () => {
  // Purge content aged beyond 15 days across data layers
  const expirationThreshold = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  
  for (const shard of [prisma1, prisma2, prisma3]) {
    const expiredPosts = await shard.mediaPost.findMany({ where: { createdAt: { lt: expirationThreshold } } });
    for (const post of expiredPosts) {
      try {
        // Purge physical B2 asset structural arrays safely
        const client = b2Clients[post.shard];
        await client.send(new DeleteObjectCommand({ Bucket: b2Buckets[post.shard], Key: post.mediaUrl }));
      } catch (err) { console.error("Asset physical cleanup skipped", err); }
    }
    await shard.mediaPost.deleteMany({ where: { createdAt: { lt: expirationThreshold } } });
  }
});

// Global storage monitor checks B2 arrays dynamically
cron.schedule('*/30 * * * *', async () => {
  // Total target capacity metrics tracking logic goes here
  // Sets GLOBAL_B2_OVERFLOW = true dynamically if size exceeding metrics are reached
});

app.listen(PORT, () => console.log(`Golviral Functional Core API online via Engine Cluster port ${PORT}`));

function uuidv4() {
  return require('crypto').randomUUID();
}
