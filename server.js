require('dotenv').config();
const express = require('express');
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const cron = require('node-cron');

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(cors({ origin: process.env.CORS_ORIGIN }));

// 1. DB + REDIS + B2 SETUP
const prisma1 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL1 });
const prisma2 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL2 });
const prisma3 = new PrismaClient({ datasourceUrl: process.env.DATABASE_URL3 });

const redis1 = new Redis(process.env.REDIS_URL1);
const redis2 = new Redis(process.env.REDIS_URL2);

function getDb(userId) {
  const shard = userId % 3;
  if (shard === 0) return prisma1;
  if (shard === 1) return prisma2;
  return prisma3;
}

function getB2Client(userId) {
  const shard = userId % 3;
  if (shard === 0) {
    return {
      client: new S3Client({
        endpoint: process.env.B2_ENDPOINT_A,
        credentials: { accessKeyId: process.env.B2_KEY_ID_A, secretAccessKey: process.env.B2_APPLICATION_KEY_A },
        region: 'us-west-000'
      }),
      bucket: process.env.B2_BUCKET_A
    };
  }
  if (shard === 1) {
    return {
      client: new S3Client({
        endpoint: process.env.B2_ENDPOINT_B,
        credentials: { accessKeyId: process.env.B2_KEY_ID_B, secretAccessKey: process.env.B2_APPLICATION_KEY_B },
        region: 'us-west-000'
      }),
      bucket: process.env.B2_BUCKET_B
    };
  }
  return {
    client: new S3Client({
      endpoint: process.env.B2_ENDPOINT_C,
      credentials: { accessKeyId: process.env.B2_KEY_ID_C, secretAccessKey: process.env.B2_APPLICATION_KEY_C },
      region: 'us-west-000'
    }),
    bucket: process.env.B2_BUCKET_C
  };
}

// 2. 10-SECOND MEMORY BUFFER ENGINE
const buffer = [];
setInterval(async () => {
  if (buffer.length === 0) return;
  const batch = buffer.splice(0, buffer.length);
  
  try {
    const viewOps = [];
    const likeAgg = {};
    const commentAgg = {};

    for (const item of batch) {
      if (item.type === 'view') {
        const added = await redis2.pfadd(`view:${item.postId}`, item.viewerId);
        if (added === 1) {
          await addPoints(item.creatorId, 0.5, 'view');
        }
      }
      if (item.type === 'like') {
        likeAgg[item.postId] = (likeAgg[item.postId] || 0) + 1;
        await addPoints(item.creatorId, 1, 'like');
      }
      if (item.type === 'comment') {
        commentAgg[item.postId] = (commentAgg[item.postId] || 0) + 1;
        await addPoints(item.creatorId, 2, 'comment');
      }
    }

    // Bulk DB writes
    // Implementation depends on Prisma models
    console.log('Flushed buffer:', batch.length);
  } catch (e) {
    console.error('Buffer flush failed:', e);
    buffer.unshift(...batch); // Restore on failure
  }
}, 10000);

// 3. POINTS LOGIC
async function addPoints(userId, delta, reason) {
  const user = await prisma1.user.findUnique({ where: { id: userId } });
  if (!user) return;

  const dailyKey = `daily:${userId}:${new Date().toISOString().split('T')[0]}`;
  const dailyEarned = parseFloat(await redis1.get(dailyKey) || 0);
  if (dailyEarned + delta > 10000) return;

  const isMonetized = user.monetizedAt !== null && user.freeFarmingStopped === true;
  
  if (isMonetized) {
    await prisma3.pointsLedger.create({
      data: { userId, amount: delta, type: 'cash', reason }
    });
  } else {
    await prisma3.pointsLedger.create({
      data: { userId, amount: delta, type: 'free', reason }
    });
  }
  await redis1.incrbyfloat(dailyKey, delta);
  await redis1.expire(dailyKey, 86400);
}

// 4. AUTH MIDDLEWARE
function auth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });
  try {
    req.user = jwt.verify(token, process.env.JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
}

// 5. OTP SEND WITH FALLBACK
async function sendOTP(email, code) {
  const fetch = require('node-fetch');
  try {
    await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sender: { email: process.env.OTP_FROM_EMAIL },
        to: [{ email }],
        subject: 'GolViral OTP',
        textContent: `Your OTP: ${code}`
      })
    });
  } catch {
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: process.env.OTP_FROM_EMAIL,
        to: email,
        subject: 'GolViral OTP',
        text: `Your OTP: ${code}`
      })
    });
  }
}

// 6. ROUTES
app.post('/auth/signup', async (req, res) => {
  const { username, email, password, turnstileToken } = req.body;
  // Verify turnstileToken here with Cloudflare API
  const hash = await bcrypt.hash(password, 10);
  const user = await prisma1.user.create({
    data: { username, email, password: hash, freeCredits: 1500 }
  });
  const token = jwt.sign({ id: user.id }, process.env.JWT_SECRET);
  res.json({ token });
});

app.post('/auth/login', async (req, res) => {
  const { username, password, turnstileToken } = req.body;
  const user = await prisma1.user.findUnique({ where: { username } });
  if (!user || !await bcrypt.compare(password, user.password)) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  const token = jwt.sign({ id: user.id }, process.env.JWT_SECRET);
  res.json({ token });
});

app.post('/post/create', auth, async (req, res) => {
  const userId = req.user.id;
  const { type, caption } = req.body;
  
  const todayKey = `posts:${userId}:${new Date().toISOString().split('T')[0]}`;
  const count = parseInt(await redis1.get(todayKey) || 0);
  if (count >= 3) return res.status(429).json({ error: '3 posts/day limit' });
  
  const user = await getDb(userId).user.findUnique({ where: { id: userId } });
  if (user.freeCredits < 25) return res.status(402).json({ error: 'Not enough 25pts' });
  
  await prisma3.pointsLedger.create({ data: { userId, amount: -25, type: 'free', reason: 'post_fee' } });
  await redis1.incr(todayKey);
  await redis1.expire(todayKey, 86400);
  
  const { client, bucket } = getB2Client(userId);
  const key = `${userId}/${Date.now()}.${type === 'video' ? 'mp4' : 'jpg'}`;
  const command = new PutObjectCommand({ Bucket: bucket, Key: key, ContentType: type === 'video' ? 'video/mp4' : 'image/jpeg' });
  const uploadUrl = await getSignedUrl(client, command, { expiresIn: 300 });
  
  const post = await getDb(userId).mediaPost.create({
    data: { userId, type, caption, b2Key: key, status: 'pending' }
  });
  
  res.json({ postId: post.id, uploadUrl });
});

app.post('/view', auth, async (req, res) => {
  const { postId, creatorId } = req.body;
  buffer.push({ type: 'view', postId, viewerId: req.user.id, creatorId, ts: Date.now() });
  res.status(202).json({ ok: true });
});

app.post('/like', auth, async (req, res) => {
  const { postId } = req.body;
  const post = await getDb(req.user.id).mediaPost.findUnique({ where: { id: postId } });
  buffer.push({ type: 'like', postId, userId: req.user.id, creatorId: post.userId });
  res.status(202).json({ ok: true });
});

app.get('/feed', auth, async (req, res) => {
  const posts1 = await prisma1.mediaPost.findMany({ where: { status: 'approved' }, take: 20, orderBy: { createdAt: 'desc' } });
  const posts2 = await prisma2.mediaPost.findMany({ where: { status: 'approved' }, take: 20, orderBy: { createdAt: 'desc' } });
  const posts3 = await prisma3.mediaPost.findMany({ where: { status: 'approved' }, take: 20, orderBy: { createdAt: 'desc' } });
  const posts = [...posts1, ...posts2, ...posts3].sort((a, b) => b.createdAt - a.createdAt).slice(0, 20);
  res.json(posts);
});

app.get('/wallet/balance', auth, async (req, res) => {
  const userId = req.user.id;
  const ledgers = await prisma3.pointsLedger.findMany({ where: { userId } });
  const freeCredits = ledgers.filter(l => l.type === 'free').reduce((a, b) => a + b.amount, 0);
  const cashBalance = ledgers.filter(l => l.type === 'cash').reduce((a, b) => a + b.amount, 0);
  const dailyKey = `daily:${userId}:${new Date().toISOString().split('T')[0]}`;
  const dailyEarned = parseFloat(await redis1.get(dailyKey) || 0);
  const user = await prisma1.user.findUnique({ where: { id: userId } });
  res.json({ freeCredits, cashBalance, dailyEarned, daysToMonetize: 7, refsLeft: 5 });
});

// 7. CRON JOBS
cron.schedule('0 3 *', async () => {
  console.log('Running 15-day cleanup');
  // Delete old media + B2 files from all 3 buckets
});

cron.schedule('0 0 *', async () => {
  console.log('Checking monetization eligibility');
  // Set monetizedAt + freeFarmingStopped
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API running on ${PORT}`));
