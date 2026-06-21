const express = require('express');
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const cron = require('node-cron');
const crypto = require('crypto');

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

// ========== 3x DB + 3x REDIS SHARDING ==========
const prisma1 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL1 });
const prisma2 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL2 });
const prisma3 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL3 });

const redis1 = new Redis(process.env.REDIS_URL1);
const redis2 = new Redis(process.env.REDIS_URL2);
const redis3 = new Redis(process.env.REDIS_URL3);

const getPrisma = (userId) => {
  const shard = userId % 3;
  if (shard === 0) return prisma1;
  if (shard === 1) return prisma2;
  return prisma3;
};

const getRedis = (userId) => {
  const shard = userId % 3;
  if (shard === 0) return redis1;
  if (shard === 1) return redis2;
  return redis3;
};

// ========== 3x B2 S3 CLIENTS ==========
const b2Clients = [
  new S3Client({
    endpoint: process.env.B2_ENDPOINT_A,
    region: 'us-west-000',
    credentials: { accessKeyId: process.env.B2_KEY_ID_A, secretAccessKey: process.env.B2_APPLICATION_KEY_A }
  }),
  new S3Client({
    endpoint: process.env.B2_ENDPOINT_B,
    region: 'us-west-000',
    credentials: { accessKeyId: process.env.B2_KEY_ID_B, secretAccessKey: process.env.B2_APPLICATION_KEY_B }
  }),
  new S3Client({
    endpoint: process.env.B2_ENDPOINT_C,
    region: 'us-west-000',
    credentials: { accessKeyId: process.env.B2_KEY_ID_C, secretAccessKey: process.env.B2_APPLICATION_KEY_C }
  })
];

const getB2Client = (userId) => b2Clients[userId % 3];
const getB2Bucket = (userId) => {
  const shard = userId % 3;
  if (shard === 0) return process.env.B2_BUCKET_A;
  if (shard === 1) return process.env.B2_BUCKET_B;
  return process.env.B2_BUCKET_C;
};

// ========== 10s MEMORY BUFFER ==========
const viewBuffer = [];
const likeBuffer = [];
const commentBuffer = [];

setInterval(async () => {
  if (viewBuffer.length > 0) {
    const batch = viewBuffer.splice(0);
    // PFADD dedupe per shard
    for (const item of batch) {
      const redis = getRedis(item.creatorId);
      const added = await redis.pfadd(`view:${item.postId}`, item.viewerId);
      if (added === 1) {
        // Credit creator +0.5pts
        await creditPoints(item.creatorId, 0.5, 'view');
      }
    }
  }
  // Same logic for likeBuffer + commentBuffer with split rewards
}, 10000);

// ========== AUTH MIDDLEWARE ==========
const auth = async (req, res, next) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.userId = decoded.id;
    next();
  } catch {
    res.status(401).json({ error: 'Unauthorized' });
  }
};

// ========== POINTS HELPER v4.5 ==========
async function creditPoints(userId, amount, type) {
  const redis = getRedis(userId);
  const prisma = getPrisma(userId);
  const lock = await redis.set(`LOCK:points:${userId}`, '1', 'EX', 2, 'NX');
  if (!lock) return;

  try {
    const user = await prisma.user.findUnique({ where: { id: userId } });
    const now = new Date();
    const daysSince = Math.floor((now - user.createdAt) / 86400000);
    const monetized = user.monetizedAt && user.freeFarmingStopped;

    // Daily cap 10k check
    const todayKey = `daily:${userId}:${now.toISOString().split('T')[0]}`;
    const todayEarned = parseFloat(await redis.get(todayKey) || '0');
    if (todayEarned + amount > 10000) {
      amount = Math.max(0, 10000 - todayEarned);
    }

    if (amount <= 0) return;

    const walletType = monetized? 'cash' : 'free';
    const field = monetized? 'cashBalance' : 'freeCredits';

    await prisma.user.update({
      where: { id: userId },
      data: { [field]: { increment: amount } }
    });

    await prisma.pointsLedger.create({
      data: { userId, amount, type, walletType, createdAt: now }
    });

    await redis.incrbyfloat(todayKey, amount);
    await redis.expire(todayKey, 86400);
  } finally {
    await redis.del(`LOCK:points:${userId}`);
  }
}

// ========== ROUTES ==========

// POST CREATE v4.5: Deduct 25pts upfront, refund on reject
app.post('/post/create', auth, async (req, res) => {
  const { type, caption } = req.body;
  const userId = req.userId;
  const redis = getRedis(userId);
  const prisma = getPrisma(userId);

  // Check 3 posts/day limit
  const todayKey = `posts:${userId}:${new Date().toISOString().split('T')[0]}`;
  const postsToday = parseInt(await redis.get(todayKey) || '0');
  if (postsToday >= 3) return res.status(429).json({ error: 'Daily post limit reached' });

  // Deduct 25pts upfront
  const user = await prisma.user.findUnique({ where: { id: userId } });
  if (user.freeCredits < 25) return res.status(400).json({ error: 'Insufficient credits' });

  await prisma.user.update({ where: { id: userId }, data: { freeCredits: { decrement: 25 } });
  await redis.incr(todayKey);
  await redis.expire(todayKey, 86400);

  // Create post record
  const post = await prisma.media_post.create({
    data: { userId, type, caption, status: 'pending', b2Shard: userId % 3 }
  });

  // Generate presigned B2 URL
  const b2Client = getB2Client(userId);
  const bucket = getB2Bucket(userId);
  const key = `posts/${post.id}/${Date.now()}.mp4`;

  const command = new PutObjectCommand({ Bucket: bucket, Key: key, ContentType: 'video/mp4' });
  const uploadUrl = await getSignedUrl(b2Client, command, { expiresIn: 3600 });

  await prisma.media_post.update({ where: { id: post.id }, data: { b2Key: key } });

  // Queue worker job for integrity + compress
  await redis.lpush('video-integrity', JSON.stringify({ postId: post.id, userId, key }));

  res.json({ postId: post.id, uploadUrl, key });
});

// LIKE v4.5: Creator +5pts, Liker +1pt
app.post('/post/:id/like', auth, async (req, res) => {
  const postId = parseInt(req.params.id);
  const likerId = req.userId;

  const prisma = getPrisma(likerId);
  const post = await prisma.media_post.findUnique({ where: { id: postId } });
  if (!post) return res.status(404).json({ error: 'Post not found' });

  // Push to 10s buffer for batch processing
  likeBuffer.push({ postId, creatorId: post.userId, likerId });
  res.json({ ok: true });
});

// COMMENT v4.5: Creator +10pts, Commenter +3pts
app.post('/post/:id/comment', auth, async (req, res) => {
  const postId = parseInt(req.params.id);
  const commenterId = req.userId;
  const { text } = req.body;

  const prisma = getPrisma(commenterId);
  const post = await prisma.media_post.findUnique({ where: { id: postId } });
  if (!post) return res.status(404).json({ error: 'Post not found' });

  await prisma.comment.create({ data: { postId, userId: commenterId, text } });
  commentBuffer.push({ postId, creatorId: post.userId, commenterId });
  res.json({ ok: true });
});

// WORKER CALLBACK: Post approved/rejected
app.post('/post/complete', auth, async (req, res) => {
  const { postId, status } = req.body;
  const prisma = getPrisma(req.userId);

  await prisma.media_post.update({ where: { id: postId }, data: { status } });

  if (status === 'rejected') {
    // Refund 25pts
    await prisma.user.update({ where: { id: req.userId }, data: { freeCredits: { increment: 25 } });
  }
  res.json({ ok: true });
});

// MONETIZATION CRON 12am
cron.schedule('0 0 *', async () => {
  for (const prisma of [prisma1, prisma2, prisma3]) {
    const users = await prisma.user.findMany({
      where: { monetizedAt: null, referralCount: { gte: 5 } }
    });
    for (const user of users) {
      const days = Math.floor((new Date() - user.createdAt) / 86400000);
      if (days >= 7) {
        await prisma.user.update({
          where: { id: user.id },
          data: { monetizedAt: new Date(), freeFarmingStopped: true }
        });
      }
    }
  }
});

// DELETE CRON 3am - 15 day cleanup
cron.schedule('0 3 *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  // Delete old posts + B2 objects from all 3 shards
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API running on ${PORT}`));
