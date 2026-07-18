
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
const webpush = require('web-push'); // npm i web-push

// ========== ENV CONFIG ==========
const PORT = process.env.PORT || 10000;
const JWT_SECRET = process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
const APP_BASE_URL = process.env.APPBASEURL || 'https://selimzy535-ai.github.io/golviral-frontend';

console.log(`[INIT] GolViral v5.1 Hardened Core Stack Engine...`);
console.log(`[CONFIG] APP_BASE_URL: ${APP_BASE_URL}`);

// ========== INIT & SECURITY OVERRIDES ==========
const app = express();
const http = require('http');
const { Server } = require('socket.io');
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: ['https://selimzy535-ai.github.io', 'https://golviral.com'],
    credentials: true
  }
});

// Map userId to socketId for DM routing
const onlineUsers = new Map();

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

// SET VAPID KEYS ONCE. Replace with your real keys
webpush.setVapidDetails(
  'mailto:carl56590@gmail.com',
  process.env.VAPID_PUBLIC_KEY || 'BEl0YourPublicKeyHere',
  process.env.VAPID_PRIVATE_KEY || 'YourPrivateKeyHere'
);

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

// Dynamic Monetization Check Helper
async function isUserMonetized(userId) {
  const db = getDbShard(userId);
  const user = await db.client.user.findUnique({ where: { id: userId } });
  if (!user) return false;
  if (user.monetizeFlag) return true;

  const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);
  let followers = 0;
  const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const shard of dbs) {
    followers += await shard.follow.count({ where: { followingId: userId } }).catch(() => 0);
  }

  if (days >= 7 && followers >= 10) {
    await db.client.user.update({
      where: { id: userId },
      data: { monetizeFlag: true, freeFarmingStopped: true }
    }).catch(() => {});
    return true;
  }
  return false;
}

// ========== SOCKET HANDSHAKE & EVENTS ==========
io.use(async (socket, next) => {
  const token = socket.handshake.auth.token;
  if (!token) return next(new Error("Unauthorized"));
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    socket.userId = decoded.userId;
    next();
  } catch (e) {
    next(new Error("Invalid token"));
  }
});

io.on('connection', (socket) => {
  console.log(`[WS] User connected: ${socket.userId}`);
  onlineUsers.set(socket.userId, socket.id);
  socket.join(socket.userId);

  // 1. SEND MESSAGE REALTIME
  socket.on('send_message', async ({ receiverId, text }) => {
    const db = getDbShard(receiverId);
    const senderDb = getDbShard(socket.userId);

    const sender = await senderDb.client.user.findUnique({where:{id:socket.userId}});
    const receiver = await db.client.user.findUnique({where:{id:receiverId}});

    // DM is Free only if the user (or target) is monetized, otherwise they must have paid (dmUnlocked === true)
    const senderEligible = (await isUserMonetized(socket.userId)) || sender.dmUnlocked;
    const receiverEligible = (await isUserMonetized(receiverId)) || receiver.dmUnlocked;

    if(!senderEligible || !receiverEligible){
      return socket.emit('error_msg', {error: "Both users must unlock DM or have monetization active (7 days + 10 followers)"});
    }

    // Save to DB
    const msg = await db.client.message.create({
      data: {
        id: crypto.randomBytes(8).toString('hex'),
        senderId: socket.userId,
        receiverId,
        text
      }
    });

    const receiverSocketId = onlineUsers.get(receiverId);
    if(receiverSocketId){
      io.to(receiverId).emit('receive_message', msg);
    }

 
sendNotification(receiverId, 'DM', 'New Message', `Message from ${senderUser.username}`);
    
    socket.emit('receive_message', msg);
  });

  socket.on('typing', ({receiverId}) => {
    io.to(receiverId).emit('user_typing', {from: socket.userId});
  });

  socket.on('disconnect', () => {
    onlineUsers.delete(socket.userId);
    console.log(`[WS] User disconnected: ${socket.userId}`);
  });
});

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

    // Upgraded coeffecients for V5.1
    switch (action) {
      case 'LIKE': pointsToAdd = isCreator ? 10 : 1; break;
      case 'COMMENT': pointsToAdd = isCreator ? 15 : 3; break;
      case 'VIEW_REEL': pointsToAdd = isCreator ? 0.2 : 0; break; 
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
      if (current >= 10000) return; // Cap at 10000pts limit 
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
  const { fileExtension, contentType, postType, caption, externalLink } = req.body;

  const db = getDbShard(userId);
  const redis = getRedisShard(userId);

  const lock = await redis.set(`lock:${userId}`, '1', 'EX', 2, 'NX').catch(() => 'PASS_BYPASS_LOCK');
  if (!lock) return res.status(423).json({ error: 'Concurrency execution layer busy' });

  try {
    const user = await db.client.user.findUnique({ where: { id: userId } }).catch(() => null);
    if (!user) return res.status(404).json({ error: 'User mapping vanished inside infrastructure arrays' });

    const fee = (postType === 'novel' || postType === 'story' || postType === 'store') ? 10 : 25;
    if (user.freeCredits < fee) return res.status(400).json({ error: `Insufficient points: Need ${fee} credits` });

    // V5.1 Constraint - Maximum 3 posts limit per day: exactly 1 reel, 1 novel, 1 story (no duplication of same type)
    const startOfToday = new Date();
    startOfToday.setHours(0,0,0,0);
    const postsToday = await db.client.post.findMany({
      where: {
        userId,
        createdAt: { gte: startOfToday }
      },
      select: { type: true }
    });

    if (postsToday.length >= 3) {
      return res.status(429).json({ error: 'Daily posting limit of 3 posts reached.' });
    }

    const normalizedType = (postType === 'story' || postType === 'store') ? 'story' : postType;
    const alreadyHasType = postsToday.some(p => {
      const existingType = (p.type === 'story' || p.type === 'store') ? 'story' : p.type;
      return existingType === normalizedType;
    });

    if (alreadyHasType) {
      return res.status(429).json({ error: `You have already posted a ${normalizedType} today. Only 1 reel, 1 novel, and 1 story is allowed per day.` });
    }

    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { decrement: fee } } });

    const postId = crypto.randomBytes(8).toString('hex');
    const b2 = getB2Shard(userId);

    // ===== iPhone.MOV Support =====
    const allowedTypes = ['video/mp4', 'video/quicktime', 'video/mov'];
    const ct = allowedTypes.includes(contentType) ? contentType : 'video/mp4';

    let ext = fileExtension;
    if (!ext) {
      ext = ct === 'video/quicktime' ? 'mov' : 'mp4';
    }
    const key = `media/${postId}.${ext}`;

    let presignedUrl = "";
    try {
      const cmd = new PutObjectCommand({ Bucket: b2.bucket, Key: key, ContentType: ct });
      presignedUrl = await getSignedUrl(b2.client, cmd, { expiresIn: 3600 });
    } catch (s3Err) {
      console.error('[B2 Sign Fail]', s3Err.message);
      return res.status(500).json({ error: 'B2 presign failed' });
    }

    // Save with caption & externalLink 
    await db.client.post.create({
      data: { 
        id: postId, 
        userId, 
        type: postType || 'reel', 
        mediaUrl: key, 
        thumbnailUrl: '', 
        status: 'PRE_UPLOAD', 
        b2Shard: getShardIndex(userId),
        caption: caption || '',
        externalLink: externalLink || '',
        isBoosted: false
      }
    });

    res.json({ postId, uploadUrl: presignedUrl, objectKey: key });
  } catch (err) {
    console.error('[Intent Error]', err.message);
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

  if (post.type === 'novel' || post.type === 'story' || post.type === 'store') {
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

    await new Promise((resolve) => {
      exec(`ffmpeg -ss 00:00:01 -i "${localVideoPath}" -vframes 1 -q:v 2 "${localThumbPath}" -y`, (err) => {
        if (err || !fs.existsSync(localThumbPath)) {
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

    await db.client.post.update({
      where: { id: postId },
      data: { status: 'ACTIVE', mediaUrl: objectKey, thumbnailUrl: thumbKey }
    });

    res.json({ message: 'Content compilation complete', postId });
  } catch (err) {
    await db.client.post.update({ where: { id: postId }, data: { status: 'REJECTED' } }).catch(() => {});
    await db.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: 25 } } }).catch(() => {});
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
    const { postId, creatorId, text } = req.body;
    const actorId = req.user.userId; 
    if (!postId || !creatorId || !text || text.trim().length < 2) {
      return res.status(400).json({ error: 'Invalid comment payload' });
    }

    const redis = getRedisShard(actorId);
    const cooldown = await redis.get(`cool:comment:${actorId}`).catch(() => null);
    if (cooldown) return res.status(429).json({ error: 'Comment cooldown active' });

    await redis.set(`cool:comment:${actorId}`, '1', 'EX', 120).catch(() => {});

    const db = getDbShard(creatorId);
    await db.client.comment.create({
      data: { postId, userId: actorId, text: text.trim().slice(0, 500) }
    });

    const actorDb = getDbShard(actorId);
    const actorUser = await actorDb.client.user.findUnique({where:{id:actorId}});
    sendNotification(creatorId, 'COMMENT', 'New Comment', `${actorUser.username} commented: ${text.slice(0,40)}`);
    
    interactionBuffer.push({ type: 'COMMENT', postId, userId: creatorId, actorId, timestamp: Date.now() });
    res.status(201).json({ success: true });
  } catch (err) {
    console.error('[Comment Error]', err.message);
    res.status(500).json({ error: 'Comment failed' });
  }
});

app.get('/api/comments/:postId', async (req, res) => {
  try {
    const { postId } = req.params;
    const { page = 1, limit = 20 } = req.query;

    const target = await findPostAcrossShards(postId);
    if (!target) return res.status(404).json({ error: 'Post not found' });

    const comments = await target.db.comment.findMany({
      where: { postId },
      select: {
        id: true, text: true, createdAt: true,
        user: { select: { id: true, username: true } }
      },
      orderBy: { createdAt: 'desc' },
      skip: (Number(page) - 1) * Number(limit),
      take: Number(limit)
    });

    res.json({ comments, total: comments.length });
  } catch (err) {
    res.status(500).json({ error: 'Failed to load comments' });
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
        select: {
          id: true,
          userId: true,
          type: true,
          title: true,
          content: true,
          mediaUrl: true,
          b2Shard: true,
          likes: true,
          comments: true,
          views: true,
          score: true,
          createdAt: true,
          isBoosted: true,
          boostExpiresAt: true,
          externalLink: true,
          caption: true
        },
        take: 24
      });
      
      const processedPosts = posts.map(p => {
        const currentlyBoosted = p.isBoosted && p.boostExpiresAt && new Date(p.boostExpiresAt) > new Date();
        return {
          ...p,
          isBoosted: !!currentlyBoosted,
          // Link only clickable if currently boosted
          externalLink: currentlyBoosted ? p.externalLink : null
        };
      });

      feed.push(...processedPosts);
    } catch (dbErr) {
      console.error('[Feed Shard Intercepted]', dbErr.message);
    }
  }

  // Sorting: Boosted posts first, then by score descending
  feed.sort((a, b) => {
    if (a.isBoosted && !b.isBoosted) return -1;
    if (!a.isBoosted && b.isBoosted) return 1;
    return b.score - a.score;
  });

  res.json(feed.slice(0, 30));
});

app.get('/api/post/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const target = await findPostAcrossShards(id);
    
    if (!target || target.post.status !== 'ACTIVE') {
      return res.status(404).json({ error: 'Post not found' });
    }

    const currentlyBoosted = target.post.isBoosted && target.post.boostExpiresAt && new Date(target.post.boostExpiresAt) > new Date();

    res.json({
      id: target.post.id,
      userId: target.post.userId,
      type: target.post.type,
      title: target.post.title,
      content: target.post.content,
      mediaUrl: target.post.mediaUrl,
      b2Shard: target.post.b2Shard,
      likes: target.post.likes,
      comments: target.post.comments,
      views: target.post.views,
      caption: target.post.caption || '',
      isBoosted: !!currentlyBoosted,
      externalLink: currentlyBoosted ? target.post.externalLink : null
    });
  } catch (err) {
    res.status(500).json({ error: 'Post load failed' });
  }
});

// ========== PAYMENT & WALLET SYSTEMS ==========
const GIFT_PACKS = {
  RUBY: { ngn: 5000, points: 200, giftsTotal: 100 },
  GOLD: { ngn: 10000, points: 500, giftsTotal: 100 },
  DIAMOND: { ngn: 15000, points: 1000, giftsTotal: 100 }
};

app.post('/api/deposit/init', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { tierAmount } = req.body; 

    // Buy Points tiers via Selar
    const tiers = {
      1500: 15000, 
      7000: 70000 
    };

    const points = tiers[tierAmount];
    if (!points) return res.status(400).json({ error: 'Invalid tier amount. Only 1500 or 7000 allowed' });

    const token = crypto.randomBytes(16).toString('hex');
    const db = getDbShard(userId);

    await db.client.deposit.create({
      data: {
        id: crypto.randomBytes(8).toString('hex'),
        userId,
        amountNaira: tierAmount,
        points,
        token,
        status: 'PENDING',
        meta: 'DEPOSIT', 
        expiresAt: new Date(Date.now() + 30 * 60 * 1000)
      }
    });

    res.json({
      selarLink: `https://selar.co/m/YOUR_STORE_SLUG/${tierAmount}`,
      token
    });
  } catch (err) {
    console.error('[Deposit Init Error]', err.message);
    res.status(500).json({ error: 'Deposit init failed' });
  }
});

app.post('/api/dm/init', authenticateToken, async (req, res) => {
  const { userId } = req.user; 
  const token = crypto.randomBytes(16).toString('hex');
  const db = getDbShard(userId);
  await db.client.deposit.create({ data: { id: crypto.randomBytes(8).toString('hex'), userId, amountNaira: 3000, points: 0, token, status: 'PENDING', meta: 'DM_UNLOCK', expiresAt: new Date(Date.now() + 30 * 60 * 1000) } });
  res.json({ selarLink: `https://selar.co/m/YOUR_STORE_SLUG/3000`, token });
});

app.post('/api/gift/init', authenticateToken, async (req, res) => {
  const { userId } = req.user; const { giftType } = req.body;
  const pack = GIFT_PACKS[giftType];
  if(!pack) return res.status(400).json({error:"Invalid gift"});
  const token = crypto.randomBytes(16).toString('hex'); const db = getDbShard(userId);
  await db.client.deposit.create({ data: { id: crypto.randomBytes(8).toString('hex'), userId, amountNaira: pack.ngn, points: pack.points, token, status: 'PENDING', meta: `GIFT_${giftType}`, expiresAt: new Date(Date.now() + 30 * 60 * 1000) } });
  res.json({ selarLink: `https://selar.co/m/YOUR_STORE_SLUG/${pack.ngn}`, token });
});

// V5.1 Boost Initiation Engine
app.post('/api/boost/init', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { postId, tierAmount } = req.body; // 3500, 7500, 17000 Naira

    const validTiers = {
      3500: 1,   // 1 Day Boost
      7500: 3,   // 3 Days Boost
      17000: 7   // 7 Days Boost
    };

    if (!validTiers[tierAmount]) {
      return res.status(400).json({ error: 'Invalid boost tier amount. Choose 3500, 7500, or 17000' });
    }

    const postContext = await findPostAcrossShards(postId);
    if (!postContext) return res.status(404).json({ error: 'Post not found across network shards' });

    if (postContext.post.userId !== userId) {
      return res.status(403).json({ error: 'Cannot boost another user\'s post' });
    }

    const token = crypto.randomBytes(16).toString('hex');
    const db = getDbShard(userId);

    await db.client.deposit.create({
      data: {
        id: crypto.randomBytes(8).toString('hex'),
        userId,
        amountNaira: tierAmount,
        points: 0,
        token,
        status: 'PENDING',
        meta: `BOOST_${postId}_${tierAmount}`,
        expiresAt: new Date(Date.now() + 30 * 60 * 1000) // 30min checkout hold
      }
    });

    res.json({
      selarLink: `https://selar.co/m/YOUR_STORE_SLUG/${tierAmount}`,
      token
    });
  } catch (err) {
    console.error('[Boost Init Error]', err.message);
    res.status(500).json({ error: 'Boost deployment initialization failed' });
  }
});

app.post('/api/payment/verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { tx_ref, token, passToken } = req.body;

    if (!(await internalVerifyPassToken(passToken))) return res.status(400).json({ error: 'Math verification failed' });
    if (!tx_ref || !token) return res.status(400).json({ error: 'Missing tx_ref or token' });

    const db = getDbShard(userId);

    const deposit = await db.client.deposit.findFirst({
      where: { userId, token, status: 'PENDING', expiresAt: { gt: new Date() } }
    });

    if (!deposit) return res.status(400).json({ error: 'Invalid or expired ticket' });

    const usedTx = await db.client.deposit.findFirst({ where: { reference: tx_ref, status: 'SUCCESS' } });
    if (usedTx) return res.status(400).json({ error: 'Payment already redeemed' });

    const ops = [
      db.client.deposit.update({ where: { id: deposit.id }, data: { reference: tx_ref, status: 'SUCCESS' } })
    ];

    let resp = { success: true };

    if (deposit.meta === "DM_UNLOCK") {
      ops.push(db.client.user.update({ where: { id: userId }, data: { isVerified: true, dmUnlocked: true } }));
      resp.unlocked = "DM";
    } else if (deposit.meta?.startsWith("GIFT_")) {
      const giftType = deposit.meta.split('_')[1];
      const pack = GIFT_PACKS[giftType];
      if(!pack) return res.status(400).json({error:"Invalid gift"});
      ops.push(db.client.gift.create({ data:{ id: crypto.randomBytes(8).toString('hex'), buyerId: userId, giftType, price: deposit.amountNaira, pointsPerGift: pack.points, giftsSent: 0, giftsTotal: pack.giftsTotal, expiresAt: new Date(Date.now() + 30*24*60*60*1000) } }));
      resp.gift = giftType;
    } else if (deposit.meta?.startsWith("BOOST_")) {
      const parts = deposit.meta.split('_');
      const targetPostId = parts[1];
      const tierAmount = parseInt(parts[2]);

      const validTiers = { 3500: 1, 7500: 3, 17000: 7 };
      const durationDays = validTiers[tierAmount] || 1;
      const expireDate = new Date(Date.now() + durationDays * 24 * 60 * 60 * 1000);

      const targetPost = await findPostAcrossShards(targetPostId);
      if (targetPost) {
        ops.push(
          targetPost.db.post.update({
            where: { id: targetPostId },
            data: { isBoosted: true, boostExpiresAt: expireDate }
          })
        );
        resp.boosted = targetPostId;
        resp.expiresAt = expireDate;
      }
    } else {
      ops.push(
        db.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: deposit.points } } }),
        db.client.pointsLedger.create({ data: { userId, amount: deposit.points, type: 'FREE', action: 'DEPOSIT', referenceId: tx_ref } })
      );
      resp.credited = deposit.points;
    }

    await db.client.$transaction(ops);
    res.json(resp);
  } catch (err) {
    console.error('[Payment Verify Error]', err.message);
    res.status(500).json({ error: 'Payment verification failed' });
  }
});

// ========== ADMIN PORTS ==========
app.get('/api/admin/deposits', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const deposits = await db.deposit.findMany({
      orderBy: { createdAt: 'desc' },
      take: 100,
      include: { user: { select: { username: true, email: true } } }
    }).catch(() => []);
    all.push(...deposits);
  }
  res.json(all);
});

// ========== MEDIA SIGNING PORT ==========
const bucketMap = {
 0: { client: b2Clients.b2a, bucket: b2Config.a.bucket },
 1: { client: b2Clients.b2b, bucket: b2Config.b.bucket },
 2: { client: b2Clients.b2c, bucket: b2Config.c.bucket }
};

app.get('/api/media/sign', authenticateToken, async (req,res)=>{
  try{
    const {key, shard} = req.query;
    if(!key || shard===undefined) return res.status(400).json({error:'missing params'});

    const {client, bucket} = bucketMap[Number(shard)] || bucketMap[0];
    const cmd = new GetObjectCommand({Bucket: bucket, Key: key});
    const url = await getSignedUrl(client, cmd, {expiresIn: 900});
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

    let followers = 0;
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
    for (const shard of dbs) followers += await shard.follow.count({ where: { followingId: userId } }).catch(() => 0);

    res.json({
      freeCredits: user.freeCredits,
      cashBalance: user.cashBalance,
      todayEarnings: todayEarned,
      dailyCapProgress: `${todayEarned}/10000`,
      daysToMonetize: Math.max(0, 7 - days),
      refsLeft: Math.max(0, 5 - refs),
      monetized: user.monetizeFlag,
      followersProgress: `${followers}/10`, 
      daysProgress: `${days}/7` 
    });
  } catch (err) {
    res.status(200).json({ freeCredits: 0, cashBalance: 0, todayEarnings: 0, degradedModeActive: true });
  }
});

app.get('/api/user/:id', authenticateToken, async (req, res) => {
  try {
    const { id: targetId } = req.params;
    const meId = req.user.userId; 
    const db = getDbShard(targetId);

    const user = await db.client.user.findUnique({
      where: { id: targetId },
      select: { 
        id: true, 
        username: true, 
        createdAt: true, 
        isVerified: true,  
        dmUnlocked: true   
      }
    });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const posts = await db.client.post.findMany({
      where: { userId: targetId, status: 'ACTIVE' },
      select: { id: true, views: true, likes: true }
    });

    let followers = 0, following = 0, isFollowing = false;
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

    for (const shard of dbs) {
      followers += await shard.follow.count({ where: { followingId: targetId } }).catch(() => 0);
      following += await shard.follow.count({ where: { followerId: targetId } }).catch(() => 0);
      if(!isFollowing && meId !== targetId){
        const rel = await shard.follow.findFirst({ 
          where: { followerId: meId, followingId: targetId } 
        }).catch(() => null);
        if(rel) { isFollowing = true; }
      }
    }

    const totalViews = posts.reduce((sum, p) => sum + p.views, 0);
    const totalLikes = posts.reduce((sum, p) => sum + p.likes, 0);
    const monetized = await isUserMonetized(targetId);

    res.json({
      userId: targetId,
      username: user.username,
      isVerified: user.isVerified || monetized,     
      dmUnlocked: user.dmUnlocked || monetized,     
      totalViews,                                      
      totalLikes,                                      
      totalPosts: posts.length,        
      followers,                                       
      following,                                       
      isFollowing,                                     
      profileLink: `${APP_BASE_URL}/u/${targetId}`,
      referralLink: `${APP_BASE_URL}/auth.html?ref=${targetId}`
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

    const followerDb = getDbShard(followerId);
    const followerUser = await followerDb.client.user.findUnique({where:{id:followerId}});
    sendNotification(followingId, 'FOLLOW', 'New Follower', `${followerUser.username} started following you`);
    res.status(400).json({ error: 'Already following' });
    
    res.json({ success: true });
  } catch (e) {
    
  }
});

// ========== FIX 1: UNFOLLOW TYPO ==========
app.post('/api/unfollow', authenticateToken, async (req, res) => {
  const followerId = req.userId;
  const { followingId } = req.body; // FIXED: was Carrot
  const db = getDbShard(followingId);
  await db.client.follow.deleteMany({ where: { followerId, followingId } });
  res.json({ success: true });
});

// ========== V5.1 REDESIGNED WITHDRAWAL GATEWAY ==========
app.post('/api/wallet/withdraw', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { amountPoints, method, routingTarget, targetDetails } = req.body; // method: 'BANK' or 'USDT'
    const db = getDbShard(userId);
    const redis = getRedisShard(userId);

    // Minimum limit constraints
    if (amountPoints < 50000) {
      return res.status(400).json({ error: 'Minimum withdrawal dynamic baseline is 50,000 pts (₦5000)' });
    }

    // USDT specific validation matrix
    if (method === 'USDT') {
      if (amountPoints !== 120000 && amountPoints !== 12000000) {
        return res.status(400).json({ error: 'USDT withdrawals are only allowed for exact tier bounds: 120,000 pts ($10) or 12,000,000 pts ($100)' });
      }
      // TRC20 Address compliance check (Starts with T, alphanumerical base)
      const trc20Regex = /^T[A-Za-z1-9]{33}$/;
      if (!trc20Regex.test(targetDetails?.trim())) {
        return res.status(400).json({ error: 'Only USDT TRC20 addresses are allowed' });
      }
    } 
    // Local bank specific validation matrix
    else if (method === 'BANK') {
      if (!targetDetails || !targetDetails.bankName || !targetDetails.accountNumber || !targetDetails.accountName) {
        return res.status(400).json({ error: 'Nigeria Bank withdrawals require bankName, accountNumber, and accountName' });
      }
    } else {
      return res.status(400).json({ error: 'Invalid withdrawal vector. Selection must be BANK or USDT' });
    }

    // Limit check: Max 1 payout submission permitted every 7 days (7 day hold check)
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const recentPayout = await db.client.payoutQueue.findFirst({
      where: {
        userId,
        createdAt: { gte: sevenDaysAgo }
      }
    });

    if (recentPayout) {
      return res.status(429).json({ error: 'Withdrawal locked. Limit 1 withdrawal transaction per week.' });
    }

    const user = await db.client.user.findUnique({ where: { id: userId } });
    if (!user.monetizeFlag || user.cashBalance < amountPoints) {
      return res.status(400).json({ error: 'Financial criteria parameter denied: Insufficient cash balance or unmonetized account status' });
    }

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const payload = { amountPoints, method, routingTarget, targetDetails, otp };

    await redis.set(`withdraw_otp:${userId}`, JSON.stringify(payload), 'EX', 600).catch(() => {
      global[`withdraw_otp_${userId}`] = { payload, exp: Date.now() + 600000 };
    });

    await sendEmail(user.email, 'Authorization OTP Sequence Generated', `<p>Withdrawal verification challenge code: <b>${otp}</b></p>`);
    res.json({ authChallenge: true });
  } catch (err) {
    console.error('[Withdraw Init Error]', err.message);
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
        data: { 
          id: crypto.randomBytes(8).toString('hex'), 
          userId, 
          amountPoints: parsed.amountPoints, 
          routingTarget: parsed.routingTarget || parsed.method, 
          targetDetails: typeof parsed.targetDetails === 'object' ? JSON.stringify(parsed.targetDetails) : parsed.targetDetails, 
          status: 'PENDING' 
        }
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
  
  const refundAmount = target.post.type === 'reel' ? 25 : 10;
  
  await target.db.$transaction([
    target.db.post.update({ where: { id }, data: { status: 'REJECTED' } }),
    target.db.user.update({ where: { id: target.post.userId }, data: { freeCredits: { increment: refundAmount } } }) 
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

    const amount = payout.amountPoints / 10;
    sendNotification(userId, 'WITHDRAW', 'Withdrawal Approved ✅', `Your ₦${amount} withdrawal is approved and processing`);
    
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

// ========== V5.1 NEW SUPPORT TICKETING MATRIX ==========
app.post('/api/support/send', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { subject, message } = req.body;
    if (!subject || !message) return res.status(400).json({ error: 'Missing subject or message body' });

    const db = getDbShard(userId);
    const ticketId = crypto.randomBytes(8).toString('hex');

    await db.client.supportTicket.create({
      data: {
        id: ticketId,
        userId,
        subject: subject.trim(),
        message: message.trim(),
        status: 'PENDING',
        reply: ''
      }
    });

    res.status(201).json({ success: true, ticketId });
  } catch (err) {
    res.status(500).json({ error: 'Technical support pipeline dispatch failure' });
  }
});

app.get('/api/support/my', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const db = getDbShard(userId);

    const tickets = await db.client.supportTicket.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' }
    });

    res.json(tickets);
  } catch (err) {
    res.status(500).json({ error: 'Could not fetch support timeline logs' });
  }
});

app.get('/api/admin/support', requireAdmin, async (req, res) => {
  try {
    const all = [];
    for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
      const tickets = await db.supportTicket.findMany({
        where: { status: 'PENDING' },
        orderBy: { createdAt: 'desc' }
      }).catch(() => []);
      all.push(...tickets);
    }
    res.json(all);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve active ticket streams' });
  }
});

app.post('/api/admin/support/reply', requireAdmin, async (req, res) => {
  try {
    const { ticketId, userId, reply } = req.body;
    if (!ticketId || !userId || !reply) return res.status(400).json({ error: 'Missing ticket transaction credentials' });

    const db = getDbShard(userId);
    await db.client.supportTicket.update({
      where: { id: ticketId },
      data: { reply: reply.trim(), status: 'RESOLVED' }
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to process admin support verification step' });
  }
});

// ========== SOCKET.IO & V4.6 FALLBACK MESSAGING ==========
app.post('/api/message/send', authenticateToken, requireDMUnlock, async (req,res)=>{
  const {receiverId, text} = req.body;
  const senderId = req.user.userId;
  const db = getDbShard(receiverId);
  await db.client.message.create({data:{id:crypto.randomBytes(8).toString('hex'), senderId, receiverId, text}});
  res.json({sent:true})
})

app.get('/api/messages/:userId', authenticateToken, async (req,res)=>{
  const me = req.user.userId; 
  const other = req.params.userId;
  const db = getDbShard(me);
  const msgs = await db.client.message.findMany({
    where:{ OR:[{senderId:me, receiverId:other},{senderId:other, receiverId:me}] },
    orderBy:{createdAt:'asc'}, take:100
  });
  res.json(msgs)
})

app.post('/api/gift/send', authenticateToken, async (req,res)=>{
  const {receiverId} = req.body;
  const senderId = req.user.userId;
  const db = getDbShard(senderId);

  const gift = await db.client.gift.findFirst({where:{buyerId:senderId, expiresAt:{gt:new Date()}, giftsSent:{lt:100}}});
  if(!gift) return res.status(400).json({error:"No active gift pack"});

  await db.client.$transaction([
    db.client.gift.update({where:{id:gift.id}, data:{giftsSent:{increment:1}}}),
    processWalletTransaction({userId:receiverId, action:'GIFT', isCreator:true, meta:{points:gift.pointsPerGift, refId:gift.id}})
  ])
  sendNotification(receiverId, 'GIFT', 'Gift Received! 🎁', `You received ${gift.giftType} gift! +${gift.pointsPerGift} pts`);
  res.json({success:true, pointsSent:gift.pointsPerGift})
})

// ========== HELPER: SEND NOTIFICATION ==========
async function sendNotification(userId, type, title, body, data = {}) {
  const db = getDbShard(userId);
  
  // 1. Save to DB for bell icon
  await db.client.notification.create({
    data: { userId, type, title, body, data }
  }).catch(()=>{});

  // 2. Send web push if user subscribed
  const subs = await db.client.pushSubscription.findMany({ where: { userId } }).catch(()=>[]);
  for(const sub of subs){
    const pushSubscription = { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } };
    webpush.sendNotification(pushSubscription, JSON.stringify({title, body, data})).catch(async () => {
      // Delete dead subscription
      await db.client.pushSubscription.delete({ where: { id: sub.id } }).catch(()=>{});
    });
  }
}

// ========== ENDPOINT 1: PROFILE POSTS ==========
app.get('/api/user/:id/posts', async (req, res) => {
  try {
    const { id: targetId } = req.params;
    const { page = 1, limit = 12 } = req.query;
    const db = getDbShard(targetId);
    
    const posts = await db.client.post.findMany({
      where: { userId: targetId, status: { in: ['ACTIVE', 'ARCHIVED'] } },
      orderBy: { createdAt: 'desc' },
      skip: (Number(page) - 1) * Number(limit),
      take: Number(limit)
    });

    const processed = posts.map(p => ({
      ...p,
      mediaUrl: p.status === 'ARCHIVED' ? null : p.mediaUrl,
      thumbnailUrl: p.status === 'ARCHIVED' ? null : p.thumbnailUrl
    }));
    res.json(processed);
  } catch (err) { res.status(500).json({ error: 'Failed to load user posts' }); }
});

// ========== ENDPOINT 2: EXPLORE ==========
app.get('/api/explore', async (req, res) => {
  try {
    const { page = 1, limit = 20 } = req.query;
    const feed = [];
    const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

    for (const db of targets) {
      const posts = await db.post.findMany({
        where: { status: 'ACTIVE', type: 'reel', views: { gte: 50 }, createdAt: { gte: sevenDaysAgo } },
        take: 30
      }).catch(()=>[]);
      feed.push(...posts);
    }

    feed.sort((a, b) => {
      const aBoost = a.isBoosted && a.boostExpiresAt && new Date(a.boostExpiresAt) > new Date();
      const bBoost = b.isBoosted && b.boostExpiresAt && new Date(b.boostExpiresAt) > new Date();
      if (aBoost && !bBoost) return -1;
      if (!aBoost && bBoost) return 1;
      return b.score - a.score;
    });
    res.json(feed.slice((page-1)*limit, page*limit));
  } catch (err) { res.status(500).json({ error: 'Failed to load explore' }); }
});

// ========== ENDPOINT 3: SEARCH USERS ==========
app.get('/api/search/users', authenticateToken, async (req, res) => {
  try {
    const { q } = req.query;
    if (!q || q.trim().length < 2) return res.json([]);
    const searchTerm = q.trim();
    const allUsers = [];
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
    
    for (const db of dbs) {
      const users = await db.user.findMany({
        where: { username: { contains: searchTerm, mode: 'insensitive' } },
        select: { id: true, username: true, isVerified: true },
        take: 20
      }).catch(()=>[]);
      
      for(const u of users){
        let followers = 0;
        for(const shard of dbs) followers += await shard.follow.count({ where: { followingId: u.id } }).catch(() => 0);
        allUsers.push({...u, followers})
      }
    }
    allUsers.sort((a,b) => b.followers - a.followers);
    res.json(allUsers.slice(0, 20));
  } catch (err) { res.status(500).json({ error: 'Search failed' }); }
});

// ========== ENDPOINT 4-7: PUSH SYSTEM ==========
app.post('/api/push/subscribe', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { subscription } = req.body; // {endpoint, keys:{p256dh, auth}}
  const db = getDbShard(userId);
  await db.client.pushSubscription.upsert({
    where: { endpoint: subscription.endpoint },
    update: { p256dh: subscription.keys.p256dh, auth: subscription.keys.auth },
    create: { userId, endpoint: subscription.endpoint, p256dh: subscription.keys.p256dh, auth: subscription.keys.auth }
  });
  res.json({success: true});
});

app.post('/api/push/unsubscribe', authenticateToken, async (req, res) => {
  const { endpoint } = req.body;
  const db = getDbShard(req.userId);
  await db.client.pushSubscription.delete({ where: { endpoint } }).catch(()=>{});
  res.json({success: true});
});

app.get('/api/notifications', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { page = 1, limit = 20 } = req.query;
  const db = getDbShard(userId);
  const notifs = await db.client.notification.findMany({ where: { userId }, orderBy: { createdAt: 'desc' }, skip: (page-1)*limit, take: Number(limit) });
  res.json(notifs);
});

app.post('/api/notifications/read/:id', authenticateToken, async (req, res) => {
  const db = getDbShard(req.userId);
  await db.client.notification.update({ where: { id: req.params.id }, data: { isRead: true } }).catch(()=>{});
  res.json({success: true});
});

// ========== CHORE SYSTEM SCHEDULER CRON SERVICES ==========

// 1. Cron Buffer Ingestion Engine (Every 10 seconds)
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
            if([100, 1000, 10000, 100000].includes(p.views + 1)){
           sendNotification(item.userId, 'VIRAL', 'Your Reel is Blowing Up! 🔥', `Your reel hit ${p.views + 1} views!`);
    
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

// requireDMUnlock middleware checks monetization OR dmUnlocked status
async function requireDMUnlock(req,res,next){
  const userId = req.user.userId; 
  const monetized = await isUserMonetized(userId);
  if(monetized) return next();

  const db = getDbShard(userId);
  const u = await db.client.user.findUnique({where:{id:userId}});
  if(!u?.dmUnlocked) return res.status(403).json({error:"Unlock DM for 3000 or gain monetization (7 days + 10 followers)"});
  next();
}

// 2. Nightly Monetization Evaluation (00:00 Daily)
cron.schedule('0 0 * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const users = await db.user.findMany({ where: { monetizeFlag: false } });
      for (const user of users) {
        const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);
        let followers = 0;
        for(const shard of targets) followers += await shard.follow.count({ where: { followingId: user.id } }).catch(()=>0);
        if (days >= 7 && followers >= 10) { 
          await db.user.update({ where: { id: user.id }, data: { monetizeFlag: true, freeFarmingStopped: true } });
          sendNotification(user.id, 'MONETIZE', 'Congrats! You\'re Earning 💰', 'You hit 7 days + 10 followers. Earnings now go to Cash.');
          await sendEmail(user.email, 'Monetization Activated!', `You hit 7 days + 10 followers. Earnings now go to Cash.`);
        }
      }
    } catch (err) { console.error('[Midnight Cron Error]', err.message); }
  }
});

// 3. Referral Evaluation Engine (Every 5 minutes)
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

// 4. B2 Media Clean up & Deletion (Every Day at 3:00 AM) - Skip Boosted Posts
// 4. B2 Media Archive & Cleanup (Every Day at 3:00 AM)
cron.schedule('0 3 * * *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
  const clusters = [
    { db: prismaClients.db1, b2: b2Clients.b2a, bucket: b2Config.a.bucket },
    { db: prismaClients.db2, b2: b2Clients.b2b, bucket: b2Config.b.bucket },
    { db: prismaClients.db3, b2: b2Clients.b2c, bucket: b2Config.c.bucket }
  ];
  for (const c of clusters) {
    try {
      const posts = await c.db.post.findMany({ 
        where: { createdAt: { lt: cutoff }, status: 'ACTIVE', OR: [{ isBoosted: false }, { isBoosted: null }] } 
      });
      for (const p of posts) {
        if (p.mediaUrl && !p.mediaUrl.startsWith('http')) {
          await c.b2.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: p.mediaUrl })).catch(() => {});
          const thumbKey = p.mediaUrl.replace('media/', 'thumbs/').replace(/\.[^/.]+$/, ".jpg");
          await c.b2.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: thumbKey })).catch(() => {});
        }
        await c.db.post.update({ where: { id: p.id }, data: { status: 'ARCHIVED', mediaUrl: null, thumbnailUrl: null } }).catch(() => {});
      }
    } catch (cronErr) { console.error('[B2 Cron Archive Exception]', cronErr.message); }
  }
});

// 5. Score Recalculator Engine (Every 5 minutes)
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

// 6. Boost Expiration Cron System (Every 1 hour)
cron.schedule('0 * * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const expired = await db.post.updateMany({
        where: {
          isBoosted: true,
          boostExpiresAt: { lt: new Date() }
        },
        data: {
          isBoosted: false
        }
      });
      if (expired.count > 0) {
        console.log(`[Boost Engine] Expired ${expired.count} posts from matrix index.`);
      }
    } catch (err) {
      console.error('[Boost Expiration Process Error]', err.message);
    }
  }
});

// ========== HEALTH CHECK UP ==========
app.get('/', (req, res) => {
  res.status(200).json({ status: "online", core: "GolViral Hardened Engine Infrastructure Matrix", version: "5.1" });
});

// ========== START PORT BOOTSTRAP ==========
server.listen(PORT, () => { 
  console.log(`[SYSTEM BOOT SUCCESSFUL] WS + HTTP Listening on port: ${PORT}`);
});
