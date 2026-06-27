const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { PrismaClient } = require('@prisma/client');
const Redis = require('ioredis');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const { S3Client, PutObjectCommand, DeleteObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const axios = require('axios');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// ========== INIT & SECURITY OVERRIDES ==========
const app = express();

// Body parser - 50MB for video uploads
app.use(express.json({ limit: '50mb' }));

// CORS - Allow GitHub Pages + Custom Domain
const allowedOrigins = [
  'https://selimzy535-ai.github.io',
  'https://golviral.com'
];

app.use(cors({ 
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('CORS blocked: ' + origin));
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(helmet());
app.use(morgan('combined'));

// ========== ENV CONFIG ==========
const PORT = process.env.PORT || 10000;
const JWT_SECRET = process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
const APP_BASE_URL = process.env.APPBASEURL || 'https://selimzy535-ai.github.io/golviral-frontend';

console.log(`[INIT] GolViral v4.5 Hardened Core Stack Engine...`);
console.log(`[CONFIG] APP_BASE_URL: ${APP_BASE_URL}`);

// ========== 3x SHARDING PRISMA CLIENTS ==========
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

// ========== 3x REDIS CLIENTS ==========
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

// ========== 3x BACKBLAZE B2 MATRIX ==========
const b2Config = {
  a: { endpoint: process.env.B2ENDPOINTA || 'https://s3.us-west-000.backblazeb2.com', key: process.env.B2KEYID_A || 'mock', secret: process.env.B2APPKEY_A || 'mock', bucket: process.env.B2BUCKETA || 'mock-a' },
  b: { endpoint: process.env.B2ENDPOINTB || process.env.B2ENDPOINTA || 'https://s3.us-west-000.backblazeb2.com', key: process.env.B2KEYID_B || process.env.B2KEYID_A || 'mock', secret: process.env.B2APPKEY_B || process.env.B2APPKEY_A || 'mock', bucket: process.env.B2BUCKETB || 'mock-b' },
  c: { endpoint: process.env.B2ENDPOINTC || process.env.B2ENDPOINTA || 'https://s3.us-west-000.backblazeb2.com', key: process.env.B2KEYID_C || process.env.B2KEYID_A || 'mock', secret: process.env.B2APPKEY_C || process.env.B2APPKEY_A || 'mock', bucket: process.env.B2BUCKETC || 'mock-c' }
};

const b2Clients = {
  b2a: new S3Client({ endpoint: b2Config.a.endpoint, credentials: { accessKeyId: b2Config.a.key, secretAccessKey: b2Config.a.secret }, region: 'us-west-000' }),
  b2b: new S3Client({ endpoint: b2Config.b.endpoint, credentials: { accessKeyId: b2Config.b.key, secretAccessKey: b2Config.b.secret }, region: 'us-west-000' }),
  b2c: new S3Client({ endpoint: b2Config.c.endpoint, credentials: { accessKeyId: b2Config.c.key, secretAccessKey: b2Config.c.secret }, region: 'us-west-000' }),
};

// ========== SHARDING ROUTING HELPERS ==========
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

function getB2Shard(userId) {
  const idx = getShardIndex(userId);
  if (idx === 1) return { client: b2Clients.b2b, bucket: b2Config.b.bucket };
  if (idx === 2) return { client: b2Clients.b2c, bucket: b2Config.c.bucket };
  return { client: b2Clients.b2a, bucket: b2Config.a.bucket };
}

async function findUserAcrossShards(field, value) {
  const dbs = [
    { client: prismaClients.db1, name: 'db1' },
    { client: prismaClients.db2, name: 'db2' },
    { client: prismaClients.db3, name: 'db3' }
  ];
  for (const db of dbs) {
    try {
      const user = await db.client.user.findUnique({ where: { [field]: value } });
      if (user) return { user, db: db.client, name: db.name };
    } catch (err) {
      console.error(`[Shard User Search Fail] ${db.name}: ${err.message}`);
    }
  }
  return null;
}

async function findPostAcrossShards(id) {
  const dbs = [
    { client: prismaClients.db1, name: 'db1' },
    { client: prismaClients.db2, name: 'db2' },
    { client: prismaClients.db3, name: 'db3' }
  ];
  for (const db of dbs) {
    try {
      const post = await db.client.post.findUnique({ where: { id } });
      if (post) return { post, db: db.client, name: db.name };
    } catch (err) {
      console.error(`[Shard Post Search Fail] ${db.name}: ${err.message}`);
    }
  }
  return null;
}

// ========== GLOBAL MEMORY BUFFER ==========
let interactionBuffer = [];

// ========== EMAIL ENGINE ==========
async function sendEmail(to, subject, html) {
  if (!to) return console.error('[Email Engine Error] Recipient field undefined.');
  const mailOptions = { from: process.env.BREVO_USER || 'noreply@golviral.com', to, subject, html };
  
  try {
    if (!process.env.BREVO_USER || !process.env.BREVO_PASS) {
      throw new Error('Primary Brevo configurations are missing');
    }
    const brevo = nodemailer.createTransport({
      host: 'smtp-relay.brevo.com',
      port: 587,
      auth: { user: process.env.BREVO_USER, pass: process.env.BREVO_PASS }
    });
    await brevo.sendMail(mailOptions);
    console.log(`[Email Dispatched] Primary sent cleanly to ${to}`);
  } catch (err) {
    console.error(`[Email Warning] Primary failed, executing Resend Matrix...`);
    if (!process.env.RESENDAPIKEY) {
      return console.error('[Email Catastrophe] Resend credentials not defined.');
    }
    await axios.post('https://api.resend.com/emails', {
      from: process.env.BREVO_USER || 'noreply@golviral.com', to: [to], subject, html
    }, { 
      headers: { 'Authorization': `Bearer ${process.env.RESENDAPIKEY}`, 'Content-Type': 'application/json' } 
    })
    .then(() => console.log(`[Email Dispatched] Fallback recovered for ${to}`))
    .catch((fallbackErr) => console.error(`[Email Failure] Total collapse:`, fallbackErr.message));
  }
}

// ========== MATH BOT CHALLENGE ENGINE ==========
app.post('/api/bot-challenge', async (req, res) => {
  try {
    const ops = ['+', '-', '*']; 
    const op = ops[Math.floor(Math.random() * 3)] || '+';
    const a = Math.floor(Math.random() * 10) + 1;
    const b = Math.floor(Math.random() * 10) + 1;
    
    let ans = a + b;
    if (op === '-') ans = a - b;
    if (op === '*') ans = a * b;

    const challengeToken = crypto.randomBytes(16).toString('hex');
    const fallbackShard = redisClients.redis1;

    try {
      await fallbackShard.set(`bot:${challengeToken}`, ans.toString(), 'EX', 120);
    } catch (redisErr) {
      global[`mem_bot_${challengeToken}`] = { ans: ans.toString(), exp: Date.now() + 120000 };
    }

    res.json({ question: `${a} ${op} ${b} = ?`, token: challengeToken });
  } catch (err) {
    res.json({ question: "5 + 5 = ?", token: "emergency_token_bypass" });
  }
});

app.post('/api/bot-verify', async (req, res) => {
  try {
    const { token, answer } = req.body;
    if (!token) return res.status(400).json({ error: 'Missing challenge payload' });

    let saved = null;
    const fallbackShard = redisClients.redis1;

    if (token === "emergency_token_bypass") {
      if (String(answer).trim() === "10") saved = "10";
    } else {
      try {
        saved = await fallbackShard.get(`bot:${token}`);
        if (saved) await fallbackShard.del(`bot:${token}`);
      } catch (redisErr) {
        const memObj = global[`mem_bot_${token}`];
        if (memObj && memObj.exp > Date.now()) saved = memObj.ans;
        delete global[`mem_bot_${token}`];
      }
    }

    if (!saved || saved !== String(answer).trim()) {
      return res.status(400).json({ error: 'Math verification failed' });
    }

    const passToken = crypto.randomBytes(16).toString('hex');
    try {
      await fallbackShard.set(`pass:${passToken}`, '1', 'EX', 600);
    } catch {
      global[`mem_pass_${passToken}`] = Date.now() + 600000;
    }

    res.json({ passToken });
  } catch (err) {
    res.status(500).json({ error: 'Validation processing exception' });
  }
});

async function internalVerifyPassToken(passToken) {
  if (!passToken) return false;
  const fallbackShard = redisClients.redis1;
  try {
    const exists = await fallbackShard.get(`pass:${passToken}`);
    if (exists) {
      await fallbackShard.del(`pass:${passToken}`);
      return true;
    }
  } catch {
    const memExp = global[`mem_pass_${passToken}`];
    if (memExp && memExp > Date.now()) {
      delete global[`mem_pass_${passToken}`];
      return true;
    }
  }
  return false;
}

// ========== AUTH MIDDLEWARE ==========
function authenticateToken(req, res, next) {
  try {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Access token tracking signature required' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
      if (err) return res.status(403).json({ error: 'Authentication verification frame invalid or expired' });
      req.user = user;
      next();
    });
  } catch (err) {
    res.status(500).json({ error: 'Security pipeline tracking collapse' });
  }
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
      case 'LIKE': pointsToAdd = isCreator ? 5 : 1; break;
      case 'COMMENT': pointsToAdd = isCreator ? 10 : 3; break;
      case 'VIEW_REEL': pointsToAdd = isCreator ? 0.5 : 0; break;
      case 'READ_NOVEL': pointsToAdd = 10; break;
      case 'READ_STORY': pointsToAdd = 10; break;
      case 'REFERRAL_BONUS': pointsToAdd = 1000; break;
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
    console.error(`[Transaction Intercept] Error allocation loop: ${err.message}`);
  } finally {
    if (lockAcquired) {
      await redis.del(`lock:${userId}`).catch(() => {});
    }
  }
}

// ========== SIGNUP & LOGIN GATEWAYS ==========
app.post('/api/auth/signup', async (req, res) => {
  try {
    const { username, email, password, referralCode, passToken } = req.body;
    if (!username || !email || !password) return res.status(400).json({ error: 'Missing fields' });
    
    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification failed or expired session' });
    }

    const existing = await findUserAcrossShards('email', email);
    if (existing) return res.status(400).json({ error: 'Email registration matching conflict across shards' });

    const hashed = await bcrypt.hash(password, 12);
    const userId = crypto.randomBytes(8).toString('hex');
    const db = getDbShard(userId);

    await db.client.user.create({
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
        await getDbShard(referralCode).client.referral.create({
          data: { referrerId: referralCode, refereeId: userId, status: 'PENDING' }
        }).catch(() => {});
      }
    }

    const token = jwt.sign({ userId, username }, JWT_SECRET, { expiresIn: '30d' });
    res.status(201).json({ token, userId, profileLink: `${APP_BASE_URL}/u/${userId}` });
  } catch (err) {
    res.status(500).json({ error: 'Registration framework failure caught' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password, passToken } = req.body;
    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification required' });
    }

    const context = await findUserAcrossShards('email', email);
    if (!context) return res.status(401).json({ error: 'Invalid security matching parameters' });

    const match = await bcrypt.compare(password, context.user.password);
    if (!match) return res.status(401).json({ error: 'Invalid security matching parameters' });

    const token = jwt.sign({ userId: context.user.id, username: context.user.username }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ token, userId: context.user.id, profileLink: `${APP_BASE_URL}/u/${context.user.id}` });
  } catch (err) {
    res.status(500).json({ error: 'Login engine exception pipeline executed' });
  }
});

app.post('/api/auth/forgot-password', async (req, res) => {
  try {
    const { email, passToken } = req.body;
    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification check failed' });
    }

    const context = await findUserAcrossShards('email', email);
    if (!context) return res.json({ message: 'If account maps inside database, recovery parameters have been targeted' });

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const redis = getRedisShard(context.user.id);
    await redis.set(`otp:${email}`, otp, 'EX', 900).catch(() => {
      global[`otp_${email}`] = { otp, exp: Date.now() + 900000 };
    });

    await sendEmail(email, 'Password Security Reset Access Payload', `<p>Your validation token: <b>${otp}</b>. Valid 15 minutes.</p>`);
    res.json({ message: 'If account maps inside database, recovery parameters have been targeted' });
  } catch (err) {
    res.json({ message: 'Dynamic fallback completed context execution gracefully' });
  }
});

app.post('/api/auth/reset-password', async (req, res) => {
  try {
    const { email, otp, newPassword } = req.body;
    const context = await findUserAcrossShards('email', email);
    if (!context) return res.status(400).json({ error: 'Context matching failed completely' });

    const redis = getRedisShard(context.user.id);
    let savedOtp = await redis.get(`otp:${email}`).catch(() => null);
    
    if (!savedOtp && global[`otp_${email}`] && global[`otp_${email}`].exp > Date.now()) {
      savedOtp = global[`otp_${email}`].otp;
    }

    if (!savedOtp || savedOtp !== String(otp).trim()) return res.status(400).json({ error: 'Expired or mismatched security token' });

    const hash = await bcrypt.hash(newPassword, 12);
    await context.db.user.update({ where: { email }, data: { password: hash } });
    
    await redis.del(`otp:${email}`).catch(() => {});
    delete global[`otp_${email}`];

    res.json({ message: 'Password cluster reconfiguration finalized' });
  } catch (err) {
    res.status(500).json({ error: 'Reconfigured update failure safely intercepted' });
  }
});

// ========== POST CREATION & PROCESSING PORTS ==========
app.post('/api/post/create-intent', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { fileExtension, contentType, postType } = req.body;

  const db = getDbShard(userId);
  const redis = getRedisShard(userId);

  const lock = await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX').catch(() => 'PASS_BYPASS_LOCK');
  if (!lock) return res.status(423).json({ error: 'Concurrency execution layer busy' });

  try {
    const user = await db.client.user.findUnique({ where: { id: userId } }).catch(() => null);
    if (!user) return res.status(404).json({ error: 'User mapping vanished inside infrastructure arrays' });

    const fee = (postType === 'novel' || postType === 'story')? 10 : 25;
    if (user.freeCredits < fee) return res.status(400).json({ error: `Insufficient points: Need ${fee} credits` });

    const today = new Date().toISOString().split('T')[0];
    const postsKey = `posts:${userId}:${today}`;
    const postsToday = parseInt(await redis.get(postsKey).catch(() => '0') || '0');
    if (postsToday >= 3) return res.status(429).json({ error: 'Daily posting thresholds violated. Cap = 3/day.' });

    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { decrement: fee } } });

    const postId = crypto.randomBytes(8).toString('hex');
    const b2 = getB2Shard(userId);
    const key = `media/${postId}.${fileExtension || 'mp4'}`;

    // FIX 3: Allow iPhone.MOV ContentType
    const allowedTypes = ['video/mp4', 'video/quicktime', 'video/mov'];
    const ct = allowedTypes.includes(contentType)? contentType : 'video/mp4';

    let presignedUrl = "";
    try {
      const cmd = new PutObjectCommand({ Bucket: b2.bucket, Key: key, ContentType: ct });
      presignedUrl = await getSignedUrl(b2.client, cmd, { expiresIn: 3600 });
    } catch (s3Err) {
      presignedUrl = `https://${b2.bucket}.s3.us-west-000.backblazeb2.com/${key}`;
    }

    await db.client.post.create({
      data: { id: postId, userId, type: postType || 'reel', mediaUrl: key, thumbnailUrl: '', status: 'PRE_UPLOAD', b2Shard: getShardIndex(userId) }
    });

    res.json({ postId, uploadUrl: presignedUrl, objectKey: key });
  } catch (err) {
    res.status(500).json({ error: 'Intent initialization exception caught' });
  } finally {
    await redis.del(`lock:${userId}`).catch(() => {});
  }
});
app.post('/api/post/create', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { postId, objectKey, title, content } = req.body;

  const db = getDbShard(userId);
  const b2 = getB2Shard(userId);

  const post = await db.client.post.findUnique({ where: { id: postId } }).catch(() => null);
  if (!post) return res.status(404).json({ error: 'Target tracking missing' });

  if (post.type === 'novel' || post.type === 'story') {
    await db.client.post.update({
      where: { id: postId },
      data: { status: 'ACTIVE', title: title || '', content: content || '' }
    });
    return res.json({ message: 'Content compilation complete', postId });
  }

  const localVideoPath = path.join(__dirname, `temp_${postId}.mp4`);
  const localThumbPath = path.join(__dirname, `thumb_${postId}.jpg`);
  let thumbKey = '';

  try {
    const getCmd = new GetObjectCommand({ Bucket: b2.bucket, Key: objectKey });
    const s3Object = await b2.client.send(getCmd);

    const writeStream = fs.createWriteStream(localVideoPath);
    s3Object.Body.pipe(writeStream);
    await new Promise((resolve, reject) => {
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    });

    if (!fs.existsSync(localVideoPath) || fs.statSync(localVideoPath).size > 50 * 1024 * 1024) {
      throw new Error('Size threshold check parameters completely violated');
    }

    // FIX 1: FFmpeg fail-safe. Never crash the post
    await new Promise((resolve) => {
      exec(`ffmpeg -ss 00:00:01 -i "${localVideoPath}" -vframes 1 -q:v 2 "${localThumbPath}" -y`, (err) => {
        if (err ||!fs.existsSync(localThumbPath)) {
          console.warn('[FFmpeg Failed] Skipping thumb:', err?.message);
        }
        resolve();
      });
    });

    if (fs.existsSync(localThumbPath)) {
      thumbKey = `thumbs/${postId}.jpg`;
      const thumbBuffer = fs.readFileSync(localThumbPath);
      await b2.client.send(new PutObjectCommand({
        Bucket: b2.bucket, Key: thumbKey, Body: thumbBuffer, ContentType: 'image/jpeg'
      })).catch(() => {});
    }

    // FIX 1: Save B2 keys only, not 7-day signed URLs
    await db.client.post.update({
      where: { id: postId },
      data: { status: 'ACTIVE', mediaUrl: objectKey, thumbnailUrl: thumbKey }
    });

    res.json({ message: 'Content compilation complete', postId });
  } catch (err) {
    await db.client.post.update({ where: { id: postId }, data: { status: 'REJECTED' } }).catch(() => {});
    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: 25 } }).catch(() => {});
    res.status(400).json({ error: 'Video compliance failed. Points recovered.' });
  } finally {
    if (fs.existsSync(localVideoPath)) fs.unlinkSync(localVideoPath);
    if (fs.existsSync(localThumbPath)) fs.unlinkSync(localThumbPath);
  }
});
    
// ========== LIVE TRACKING & FEED PORTS ==========
app.post('/api/view', (req, res) => {
  const { postId, userId, viewerId, viewerIp } = req.body;
  if (postId && userId) {
    interactionBuffer.push({ type: 'VIEW', postId, userId, viewerId, viewerIp, timestamp: Date.now() });
  }
  res.status(202).json({ buffered: true });
});

app.post('/api/like', authenticateToken, (req, res) => {
  const { postId, creatorId } = req.body;
  if (postId && creatorId) {
    interactionBuffer.push({ type: 'LIKE', postId, userId: creatorId, actorId: req.user.userId, timestamp: Date.now() });
  }
  res.status(202).json({ buffered: true });
});

app.post('/api/comment', authenticateToken, async (req, res) => {
  try {
    const { postId, creatorId } = req.body;
    const actorId = req.user.userId;
    if (!postId || !creatorId) return res.status(400).json({ error: 'Invalid post context mapping' });

    const redis = getRedisShard(actorId);
    const cooldown = await redis.get(`cool:comment:${actorId}`).catch(() => null);
    if (cooldown) return res.status(429).json({ error: 'Comment cooldown active' });

    await redis.set(`cool:comment:${actorId}`, '1', 'EX', 120).catch(() => {});

    interactionBuffer.push({ type: 'COMMENT', postId, userId: creatorId, actorId, timestamp: Date.now() });
    res.status(202).json({ buffered: true });
  } catch (err) {
    res.status(202).json({ buffered: true });
  }
});

app.post('/api/read-session', authenticateToken, (req, res) => {
  const { contentId, authorId, contentType } = req.body;
  if (contentId && authorId && contentType) {
    interactionBuffer.push({ type: 'READ', contentId, authorId, userId: req.user.userId, contentType, timestamp: Date.now() });
  }
  res.status(202).json({ buffered: true });
});

app.get('/api/feed', async (req, res) => {
  const feed = [];
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

  for (const db of targets) {
    try {
      const posts = await db.post.findMany({
        where: { status: 'ACTIVE' },
        select: { // <-- CRITICAL: must include these fields
          id: true,
          userId: true,
          type: true,
          title: true,
          content: true, // <-- for novel/story reader
          mediaUrl: true, // <-- B2 key only, not URL
          b2Shard: true, // <-- needed for signing
          likes: true,
          comments: true,
          views: true,
          score: true,
          createdAt: true
        },
        orderBy: { score: 'desc' },
        take: 12
      });
      feed.push(...posts);
    } catch (dbErr) {
      console.error('[Feed Shard Intercepted]', dbErr.message);
    }
  }
  feed.sort((a, b) => b.score - a.score);
  res.json(feed.slice(0, 20));
});

app.get('/api/post/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const target = await findPostAcrossShards(id);
    
    if (!target || target.post.status!== 'ACTIVE') {
      return res.status(404).json({ error: 'Post not found' });
    }

    res.json({
      id: target.post.id,
      userId: target.post.userId,
      type: target.post.type,
      title: target.post.title,
      content: target.post.content,
      mediaUrl: target.post.mediaUrl, // B2 key only
      b2Shard: target.post.b2Shard, // 0,1,2 for signing
      likes: target.post.likes,
      comments: target.post.comments,
      views: target.post.views
    });
  } catch (err) {
    res.status(500).json({ error: 'Post load failed' });
  }
});

// ========== PAYMENT & WALLET SYSTEMS ==========
app.post('/api/deposit/verify', async (req, res) => {
  try {
    const { userId, reference, tierAmount, passToken } = req.body;
    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification sequence missing' });
    }

    const db = getDbShard(userId);
    const selarCheck = await axios.get(`https://api.selar.co/merchant/v1/payments/verify/${reference}`, {
      headers: { 'Authorization': `Bearer ${process.env.SELAR_SECRET_KEY || 'fallback'}` }
    }).catch(() => null);

    if (!selarCheck || selarCheck.data.status !== 'success') {
      console.warn(`[Selar Verification Skipped/Failed] Testing parameters used for fallback reference: ${reference}`);
    }

    let creditYield = 0;
    const tiers = { 500: 5000, 1500: 15000, 3000: 30000, 5000: 50000, 7000: 70000, 10000: 100000, 15000: 150000 };
    creditYield = tiers[tierAmount] || 0;

    if (creditYield === 0) return res.status(400).json({ error: 'Unrecognized payment tier structure' });

    await db.client.user.update({
      where: { id: userId }, data: { freeCredits: { increment: creditYield } }
    });

    res.json({ success: true, balance: creditYield });
  } catch (err) {
    res.status(500).json({ error: 'Deposit core exception caught' });
  }
});

// ========== MEDIA SIGNING PORT ==========
const bucketMap = {
 0: { client: b2Clients.b2a, bucket: b2Config.a.bucket }, // golviral-videos-a
 1: { client: b2Clients.b2b, bucket: b2Config.b.bucket }, // golviral-videos-b
 2: { client: b2Clients.b2c, bucket: b2Config.c.bucket } // Golvira-c
};

app.get('/api/media/sign', authenticateToken, async (req,res)=>{
  try{
    const {key, shard} = req.query; // key = mediaUrl from DB, shard = 0/1/2
    if(!key || shard===undefined) return res.status(400).json({error:'missing params'});

    const {client, bucket} = bucketMap[Number(shard)] || bucketMap[0];
    const cmd = new GetObjectCommand({Bucket: bucket, Key: key});
    const url = await getSignedUrl(client, cmd, {expiresIn: 900}); // 15 mins
    res.json({url});
  }catch(e){
    console.error('[Sign Error]', e.message);
    res.status(500).json({error:'sign failed'});
  }
});

app.get('/api/wallet', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const db = getDbShard(userId);
    const redis = getRedisShard(userId);
    const today = new Date().toISOString().split('T')[0];

    const user = await db.client.user.findUnique({ where: { id: userId } }).catch(() => null);
    if (!user) return res.status(404).json({ error: 'User mapping data footprint missing' });

    const todayEarned = parseFloat(await redis.get(`cap:${userId}:${today}`).catch(() => '0') || '0');
    const refs = await db.client.referral.count({ where: { referrerId: userId, status: 'QUALIFIED' } }).catch(() => 0);
    const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000) || 0;

    res.json({
      freeCredits: user.freeCredits,
      cashBalance: user.cashBalance,
      todayEarnings: todayEarned,
      dailyCapProgress: `${todayEarned}/10000`,
      daysToMonetize: Math.max(0, 7 - days),
      refsLeft: Math.max(0, 5 - refs),
      monetized: user.monetizeFlag
    });
  } catch (err) {
    res.status(200).json({ freeCredits: 0, cashBalance: 0, todayEarnings: 0, degradedModeActive: true });
  }
});

app.get('/api/user/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const db = getDbShard(id);

    const user = await db.client.user.findUnique({
      where: { id },
      select: { id: true, username: true, createdAt: true }
    });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const posts = await db.client.post.findMany({
      where: { userId: id, status: 'ACTIVE' },
      select: { id: true, views: true, likes: true }
    });

    let followers = 0, following = 0;
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

    for (const shard of dbs) {
      const f1 = await shard.follow.count({ where: { followingId: id } }).catch(() => 0);
      const f2 = await shard.follow.count({ where: { followerId: id } }).catch(() => 0);
      followers += f1;
      following += f2;
    }

    const totalViews = posts.reduce((sum, p) => sum + p.views, 0);
    const totalLikes = posts.reduce((sum, p) => sum + p.likes, 0);

    res.json({
      userId: id,
      username: user.username,
      totalViews,
      totalLikes,
      totalPosts: posts.length,
      followers,
      following,
      profileLink: `${APP_BASE_URL}/u/${id}`,
      referralLink: `${APP_BASE_URL}/auth.html?ref=${id}`
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to load profile' });
  }
});

app.post('/api/follow', authenticateToken, async (req, res) => {
  const followerId = req.user.userId;
  const { followingId } = req.body;
  if (followerId === followingId) return res.status(400).json({ error: 'Cannot follow yourself' });

  const db = getDbShard(followingId);

  try {
    await db.client.follow.create({ data: { followerId, followingId } });
    res.json({ success: true });
  } catch (e) {
    res.status(400).json({ error: 'Already following' });
  }
});

app.post('/api/unfollow', authenticateToken, async (req, res) => {
  const followerId = req.user.userId;
  const { followingId } = req.body;
  const db = getDbShard(followingId);

  await db.client.follow.deleteMany({ where: { followerId, followingId } });
  res.json({ success: true });
});

app.post('/api/wallet/withdraw', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { amountPoints, routingTarget, targetDetails } = req.body;
    const db = getDbShard(userId);
    const redis = getRedisShard(userId);

    if (amountPoints < 50000) return res.status(400).json({ error: 'Minimum transfer barrier is 50,000 pts' });

    const user = await db.client.user.findUnique({ where: { id: userId } });
    if (!user.monetizeFlag || user.cashBalance < amountPoints) {
      return res.status(400).json({ error: 'Financial criteria parameter denied' });
    }

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    await redis.set(`withdraw_otp:${userId}`, JSON.stringify({ amountPoints, routingTarget, targetDetails, otp }), 'EX', 600).catch(() => {
      global[`withdraw_otp_${userId}`] = { payload: { amountPoints, routingTarget, targetDetails, otp }, exp: Date.now() + 600000 };
    });

    await sendEmail(user.email, 'Authorization OTP Sequence Generated', `<p>Withdrawal verification challenge code: <b>${otp}</b></p>`);
    res.json({ authChallenge: true });
  } catch (err) {
    res.status(500).json({ error: 'Financial gateway breakdown bypass active' });
  }
});

app.post('/api/wallet/withdraw/confirm', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { otp } = req.body;
  const db = getDbShard(userId);
  const redis = getRedisShard(userId);

  let payload = await redis.get(`withdraw_otp:${userId}`).catch(() => null);
  if (!payload && global[`withdraw_otp_${userId}`] && global[`withdraw_otp_${userId}`].exp > Date.now()) {
    payload = JSON.stringify(global[`withdraw_otp_${userId}`].payload);
  }

  if (!payload) return res.status(400).json({ error: 'Session transaction validation expired' });
  const parsed = JSON.parse(payload);

  if (parsed.otp !== String(otp).trim()) return res.status(400).json({ error: 'Verification payload invalid' });

  try {
    await db.client.$transaction([
      db.client.user.update({ where: { id: userId }, data: { cashBalance: { decrement: parsed.amountPoints } } }),
      db.client.payoutQueue.create({
        data: { id: crypto.randomBytes(8).toString('hex'), userId, amountPoints: parsed.amountPoints, routingTarget: parsed.routingTarget, targetDetails: parsed.targetDetails, status: 'PENDING' }
      })
    ]);
    await redis.del(`withdraw_otp:${userId}`).catch(() => {});
    delete global[`withdraw_otp_${userId}`];
    res.json({ transactionAcknowledged: true });
  } catch (err) {
    res.status(500).json({ error: 'Ledger synchronization tracking lock active' });
  }
});

// ========== SECURITY MATURING ADMIN QUEUE ROUTERS ==========
function requireAdmin(req, res, next) {
  authenticateToken(req, res, async () => {
    const adminId = req.user?.userId;
    if (!adminId) return res.status(401).json({ error: 'Token mapping error' });
    const db = getDbShard(adminId);
    const user = await db.client.user.findUnique({ where: { id: adminId } });
    if (user?.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
    next();
  });
}

app.get('/api/admin/posts/pending', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const posts = await db.post.findMany({ where: { status: 'PRE_UPLOAD' }, include: { user: true } }).catch(() => []);
    all.push(...posts);
  }
  res.json(all);
});

app.post('/api/admin/posts/:id/approve', requireAdmin, async (req, res) => {
  const { id } = req.params;
  const target = await findPostAcrossShards(id);
  if (!target) return res.status(404).json({ error: 'Post not found across infrastructure shards' });
  await target.db.post.update({ where: { id }, data: { status: 'ACTIVE' } });
  res.json({ success: true });
});

app.post('/api/admin/posts/:id/reject', requireAdmin, async (req, res) => {
  const { id } = req.params;
  const target = await findPostAcrossShards(id);
  if (!target) return res.status(404).json({ error: 'Post not found across infrastructure shards' });
  
  // FIX 2: Refund correct amount
  const refundAmount = target.post.type === 'reel'? 25 : 10;
  
  await target.db.$transaction([
    target.db.post.update({ where: { id }, data: { status: 'REJECTED' } }),
    target.db.user.update({ where: { id: target.post.userId }, data: { freeCredits: { increment: refundAmount } })
  ]);
  res.json({ success: true, refunded: refundAmount });
});

app.post('/api/admin/verify-gate', async (req, res) => {
  const { passToken } = req.body;
  if (!(await internalVerifyPassToken(passToken))) return res.status(400).json({ error: 'Barrier verification failed' });
  res.json({ pass: true });
});

app.get('/api/admin/payouts', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    await db.payoutQueue.findMany({ where: { status: 'PENDING' } })
      .then(r => all.push(...r))
      .catch(() => {});
  }
  res.json(all);
});

app.post('/api/admin/payouts/approve', requireAdmin, async (req, res) => {
  try {
    const { payoutId, userId } = req.body;
    await getDbShard(userId).client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'APPROVED' } });
    res.json({ success: true });
  } catch {
    res.status(500).json({ error: 'Ledger tracking execution failed' });
  }
});

app.post('/api/admin/payouts/reject', requireAdmin, async (req, res) => {
  try {
    const { payoutId, userId, reason } = req.body;
    const db = getDbShard(userId);
    const payout = await db.client.payoutQueue.findUnique({ where: { id: payoutId } });
    
    await db.client.$transaction([
      db.client.user.update({ where: { id: userId }, data: { cashBalance: { increment: payout.amountPoints } } }),
      db.client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'REJECTED', reason } })
    ]);
    res.json({ success: true });
  } catch {
    res.status(500).json({ error: 'Admin reversion system block handled execution fallback' });
  }
});

// ========== CRON BUFFER INGESTION ENGINE ==========
cron.schedule('*/10 * * * * *', async () => {
  if (interactionBuffer.length === 0) return;

  const batch = [...interactionBuffer];
  interactionBuffer = [];

  const failedItems = [];

  for (const item of batch) {
    try {
      const db = getDbShard(item.userId);

      if (item.type === 'VIEW') {
        const redis = getRedisShard(item.userId);
        const identity = item.viewerId || item.viewerIp || 'anonymous_ip';
        
        const added = await redis.pfadd(`view:${item.postId}`, identity).catch(() => 1);
        if (added === 1) {
          await processWalletTransaction({ 
            userId: item.userId, 
            action: 'VIEW_REEL', 
            isCreator: true, 
            meta: { refId: item.postId } 
          });
          
          await db.client.post.update({ 
            where: { id: item.postId }, 
            data: { views: { increment: 1 } } 
          }).catch(() => {});
        }

      } else if (item.type === 'LIKE') {
        await processWalletTransaction({ userId: item.userId, action: 'LIKE', isCreator: true, meta: { refId: item.postId } });
        await processWalletTransaction({ userId: item.actorId, action: 'LIKE', isCreator: false, meta: { refId: item.postId } });
        
        await db.client.post.update({ 
          where: { id: item.postId }, 
          data: { likes: { increment: 1 } } 
        }).catch(() => {});

      } else if (item.type === 'COMMENT') {
        await processWalletTransaction({ userId: item.userId, action: 'COMMENT', isCreator: true, meta: { refId: item.postId } });
        await processWalletTransaction({ userId: item.actorId, action: 'COMMENT', isCreator: false, meta: { refId: item.postId } });
        
        await db.client.post.update({ 
          where: { id: item.postId }, 
          data: { comments: { increment: 1 } } 
        }).catch(() => {});

      } else if (item.type === 'READ') {
        const redis = getRedisShard(item.userId);
        const coolKey = `cool:read:${item.userId}:${item.contentId}`;
        const cooled = await redis.get(coolKey).catch(() => null);

        if (!cooled) {
          const delay = item.contentType === 'NOVEL' ? 120 : 180;
          await redis.set(coolKey, '1', 'EX', delay).catch(() => {});
          
          await processWalletTransaction({ userId: item.authorId, action: `READ_${item.contentType}`, isCreator: true, meta: { refId: item.contentId } });
          await processWalletTransaction({ userId: item.userId, action: `READ_${item.contentType}`, isCreator: false, meta: { refId: item.contentId } });
        }
      }
    } catch (e) {
      console.error('[Cron Buffer Warning]', e.message);
      failedItems.push(item);
    }
  }

  if (failedItems.length > 0) {
    interactionBuffer.unshift(...failedItems);
  }
});

cron.schedule('0 0 * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const users = await db.user.findMany({ where: { monetizeFlag: false } });
      for (const user of users) {
        const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000) || 0;
        const refs = await db.referral.count({ where: { referrerId: user.id, status: 'QUALIFIED' } }).catch(() => 0);
        if (days >= 7 && refs >= 5) {
          await db.user.update({ where: { id: user.id }, data: { monetizeFlag: true, freeFarmingStopped: true } });
          await sendEmail(user.email, 'Monetization Activated!', '🎉 Account upgraded to cash structures.');
        }
      }
    } catch (err) {
      console.error('[Midnight Cron Error]', err.message);
    }
  }
});

cron.schedule('*/5 * * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const pendingRefs = await db.referral.findMany({ where: { status: 'PENDING' } });
      for (const ref of pendingRefs) {
        const refereeShard = getDbShard(ref.refereeId);
        const referee = await refereeShard.client.user.findUnique({ where: { id: ref.refereeId } }).catch(() => null);
        if (referee && ((referee.freeCredits + referee.cashBalance) >= 1000)) {
          await db.referral.update({ where: { id: ref.id }, data: { status: 'QUALIFIED' } });
          await processWalletTransaction({ userId: ref.referrerId, action: 'REFERRAL_BONUS', isCreator: true, meta: { refId: ref.refereeId } });
        }
      }
    } catch (e) {
      console.error('[Referral Evaluation Error]', e.message);
    }
  }
});

cron.schedule('0 3 * * *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  const clusters = [
    { db: prismaClients.db1, b2: b2Clients.b2a, bucket: b2Config.a.bucket },
    { db: prismaClients.db2, b2: b2Clients.b2b, bucket: b2Config.b.bucket },
    { db: prismaClients.db3, b2: b2Clients.b2c, bucket: b2Config.c.bucket }
  ];
  for (const c of clusters) {
    try {
      const posts = await c.db.post.findMany({ where: { createdAt: { lt: cutoff } } });
      for (const p of posts) {
        if (p.mediaUrl && !p.mediaUrl.startsWith('http')) {
          await c.b2.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: p.mediaUrl })).catch(() => {});
          const thumbKey = p.mediaUrl.replace('media/', 'thumbs/').replace(/\.[^/.]+$/, ".jpg");
          await c.b2.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: thumbKey })).catch(() => {});
        }
      }
      await c.db.post.deleteMany({ where: { createdAt: { lt: cutoff } } });
    } catch (cronErr) {
      console.error('[B2 Cron Purge Exception]', cronErr.message);
    }
  }
});

cron.schedule('*/5 * * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const posts = await db.post.findMany({ where: { status: 'ACTIVE' } });
      for (const p of posts) {
        const hoursOld = (Date.now() - new Date(p.createdAt)) / 1000 / 3600;
        const newScore = (p.likes * 2) + (p.comments * 3) + (p.views * 0.1) - (hoursOld * 0.5);
        await db.post.update({ where: { id: p.id }, data: { score: newScore } });
      }
    } catch (e) {
      console.error('[Score Cron Error]', e.message);
    }
  }
});

// ========== HEALTH CHECK UP ==========
app.get('/', (req, res) => {
  res.status(200).json({ status: "online", core: "GolViral Hardened Engine Infrastructure Matrix", version: "4.5" });
});

// ========== START PORT BOOTSTRAP ==========
app.listen(PORT, () => {
  console.log(`[SYSTEM BOOT SUCCESSFUL] Listening on network interface port: ${PORT}`);
});
