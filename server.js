// ========== 1. ALL IMPORTS & REQUIRES ==========
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
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
const adminRoutes = require('./routes/admin'); 
const { router: profileRoutes, requireFaceVerified, requireIdVerified } = require('./routes/profile');
const webpush = require('web-push'); // npm i web-push
const multer = require('multer'); // npm i multer
const { prismaClients, redisClients, getDbShard, getRedisShard, profilePool, processWalletTransaction } = require('./utils/shard');
// ========== 2. ENV CONFIG & CONSTANTS ==========
const PORT = process.env.PORT || 10000;
const JWT_SECRET = process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
const APP_BASE_URL = process.env.APPBASEURL || 'https://selimzy535-ai.github.io/golviral-frontend';
const { db5 } = require('./utils/db5');
// CORS - Allow GitHub Pages + Custom Domain
const allowedOrigins = [
  'https://selimzy535-ai.github.io',
  'https://golviral.com'
];

console.log(`[INIT] GolViral v5.1 Hardened Core Stack Engine...`);
console.log(`[CONFIG] APP_BASE_URL: ${APP_BASE_URL}`);

// ========== 3. APP & SERVER INITIALIZATION ==========
const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: ['https://selimzy535-ai.github.io', 'https://golviral.com'],
    credentials: true
  }
});

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }); // 10MB// ========== 4. MIDDLEWARE CONFIGURATION ==========
// Body parser - 50MB for video uploads
app.use(express.json({ limit: '50mb' }));

app.use(cors({ 
  origin: allowedOrigins, // use array directly, faster
  credentials: true, // must be false with specific origins
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-key'] // ADDED
}));

app.options('*', cors()); // iPhone preflight fix

app.use(helmet());
app.use(morgan('combined'));

// MOUNT ADMIN ROUTES HERE - NOT AT THE BOTTOM
app.use('/api/admin', adminRoutes);
app.use('/api/profile', profileRoutes);
// ========== 5. GLOBAL MEMORY & STATE MAPS ==========
// Map userId to socketId for DM routing
const onlineUsers = new Map();
let interactionBuffer = [];

// ========== 6. 3x SHARDING PRISMA CLIENTS ==========

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

// HARD REQUIRE: No fallback. Server will crash on boot if keys missing
if(!process.env.VAPID_PUBLIC_KEY ||!process.env.VAPID_PRIVATE_KEY){
  throw new Error('[FATAL] VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY must be set in.env');
}

webpush.setVapidDetails(
  'mailto:carl56590@gmail.com',
  process.env.VAPID_PUBLIC_KEY,
  process.env.VAPID_PRIVATE_KEY
);
console.log('[Push] VAPID initialized with key:', process.env.VAPID_PUBLIC_KEY.slice(0,10)+'...');

// ========== 7. HELPER FUNCTIONS & ROUTING HELPERS ==========
function getShardIndex(id) {
  if (!id) return 0;
  // MD5 hash the id, take first 8 chars, convert to int, then %3
  const hash = crypto.createHash('md5').update(id).digest('hex');
  return parseInt(hash.substring(0, 8), 16) % 3;
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
  const result = await db5.query(`SELECT * FROM posts WHERE id = $1 LIMIT 1`, [id]);
  if (result.rows.length === 0) return null;
  return { post: result.rows[0], db: db5, name: 'db5' };
}

async function getTotalFollowers(userId) {
  const result = await db5.query(`SELECT COUNT(*) FROM follows WHERE "followingId" = $1`, [userId]);
  return parseInt(result.rows[0].count);
}

async function getTotalFollowing(userId) {
  const result = await db5.query(`SELECT COUNT(*) FROM follows WHERE "followerId" = $1`, [userId]);
  return parseInt(result.rows[0].count);
}

// Dynamic Monetization Check Helper
async function isUserMonetized(userId) {
  const db = getDbShard(userId);
  const user = await db.client.user.findUnique({ where: { id: userId } });
  if (!user) return false;
  if (user.monetizeFlag) return true;

  const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);
  const followers = await getTotalFollowers(userId); // FIXED

  // NEW: CHECK FACE VERIFY TOO
  const {rows} = await profilePool.query(`SELECT face_verified FROM profiles WHERE user_id=$1`, [userId]);
  if(!rows[0]?.face_verified) return false;

  if (days >= 7 && followers >= 10) {
    await db.client.user.update({
      where: { id: userId },
      data: { monetizeFlag: true, freeFarmingStopped: true }
    }).catch(() => {});
    return true;
  }
  return false;
}
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

// ========== 8. SOCKET HANDSHAKE & EVENTS ==========
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

// 1. SEND MESSAGE REALTIME - 72H EPHEMERAL + DUAL SHARD SAVE
socket.on('send_message', async ({ receiverId, text }) => {
    const senderDb = getDbShard(socket.userId);
    const receiverDb = getDbShard(receiverId);

    const sender = await senderDb.client.user.findUnique({where:{id:socket.userId}});
    const receiver = await receiverDb.client.user.findUnique({where:{id:receiverId}});
    if(!sender ||!receiver) return socket.emit('error_msg', {error: "User not found"});

    const senderEligible = (await isUserMonetized(socket.userId)) || sender.dmUnlocked;
    const receiverEligible = (await isUserMonetized(receiverId)) || receiver.dmUnlocked;

    if(!senderEligible ||!receiverEligible){
      return socket.emit('error_msg', {error: "Both users must unlock DM or have monetization active (7 days + 10 followers)"});
    }

    const msgId = crypto.randomBytes(8).toString('hex');
    const msgData = { id: msgId, senderId: socket.userId, receiverId, text };

    // SAVE TO BOTH SHARDS TO AVOID FK ERROR
    const ops = [senderDb.client.message.create({ data: msgData })];
    if(senderDb.name!== receiverDb.name){
      ops.push(receiverDb.client.message.create({ data: msgData }));
    }
    await Promise.all(ops).catch(e => console.error("DM Save Error", e));

    const receiverSocketId = onlineUsers.get(receiverId);
    if(receiverSocketId){
      io.to(receiverId).emit('receive_message', msgData);
    }

    sendNotification(receiverId, 'DM', 'New Message', `Message from ${sender.username}`);
    socket.emit('receive_message', msgData);
});

  socket.on('typing', ({receiverId}) => {
    io.to(receiverId).emit('user_typing', {from: socket.userId});
  });

  socket.on('disconnect', () => {
    onlineUsers.delete(socket.userId);
    console.log(`[WS] User disconnected: ${socket.userId}`);
  });
});

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

// ========== SIGNUP & LOGIN GATEWAYS ==========
app.post('/api/auth/signup', async (req, res) => {
  try {
    const { username, email, password, referralCode, passToken } = req.body;
    if (!username || !email || !password) return res.status(400).json({ error: 'Missing fields' });
    
    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification failed or expired session' });
    }

    const emailNorm = email.toLowerCase().trim();
    const usernameNorm = username.toLowerCase().trim();

    const existingEmail = await findUserAcrossShards('email', emailNorm);
    if (existingEmail) return res.status(400).json({ error: 'This email is already registered' });

    const existingUsername = await findUserAcrossShards('username', usernameNorm);
    if (existingUsername) return res.status(400).json({ error: 'This username is already taken' });

    const hashed = await bcrypt.hash(password, 12);
    const userId = crypto.randomBytes(8).toString('hex');
    const db = getDbShard(userId);

    await db.client.user.create({
      data: {
        id: userId,
        username: usernameNorm,
        email: emailNorm,
        password: hashed,
        role: "user", // from your schema default
        freeCredits: 1500,
        cashBalance: 0,
        monetizeFlag: false,           // ADDED BACK
        freeFarmingStopped: false,     // ADDED BACK
        isVerified: false,
        dmUnlocked: false
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

    const token = jwt.sign({ userId, username: usernameNorm }, JWT_SECRET, { expiresIn: '30d' });
    res.status(201).json({ token, userId, profileLink: `${APP_BASE_URL}/u/${userId}` });
  } catch (err) {
    if (err.code === 'P2002') {
      return res.status(400).json({ error: 'Email or Username already exists' });
    }
    console.error(err);
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

app.post('/api/post/create-intent', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { fileExtension, contentType, postType, caption, title, externalLink } = req.body; // ADDED title

  const userDb = getDbShard(userId);
  const redis = getRedisShard(userId);

  const lock = await redis.set(`lock:${userId}`, '1', 'EX', 5, 'NX').catch(() => 'PASS_BYPASS_LOCK');
  if (!lock) return res.status(423).json({ error: 'Please wait, processing previous request' });

  try {
    // 1. Check credits
    const user = await userDb.client.user.findUnique({ where: { id: userId } });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const fee = (postType === 'novel' || postType === 'story' || postType === 'store') ? 10 : 25;
    if (user.freeCredits < fee) return res.status(400).json({ error: `Insufficient points: Need ${fee} credits` });

    // 2. Daily limit check
    const startOfToday = new Date();
    startOfToday.setHours(0, 0, 0, 0);
    const { rows: countRows } = await db5.query(
      `SELECT COUNT(*) FROM posts WHERE "userId"=$1 AND type=$2 AND "createdAt" >= $3`,
      [userId, postType, startOfToday]
    );
    const countToday = parseInt(countRows[0]?.count || 0, 10);
    const DAILY_LIMIT = 15;
    if (countToday >= DAILY_LIMIT) {
      return res.status(429).json({ error: `Daily ${postType} limit reached: ${DAILY_LIMIT}` });
    }

    // 3. Deduct credits
    await userDb.client.user.update({
      where: { id: userId },
      data: { freeCredits: { decrement: fee } }
    });

    // 4. Generate B2 key ONLY - NO PRESIGNED URL
    const postId = crypto.randomBytes(8).toString('hex');
    const shardIndex = getShardIndex(userId);

    let key = '';

    if (postType === 'story' || postType === 'store') {
      if (contentType) {
        const allowedTypes = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
        if (!allowedTypes.includes(contentType)) return res.status(400).json({ error: `Story only accepts images` });
        let ext = fileExtension || 'jpg';
        if (contentType.includes('png')) ext = 'png';
        if (contentType.includes('webp')) ext = 'webp';
        if (contentType.includes('gif')) ext = 'gif';
        key = `uploads/raw/${userId}/${postId}.${ext}`; // MATCH CDN PATH
      }

    } else if (postType === 'reel') {
      const allowedTypes = ['video/mp4', 'video/quicktime', 'video/mov'];
      if (contentType && !allowedTypes.includes(contentType)) return res.status(400).json({ error: `Reel only accepts video` });
      let ext = fileExtension || 'mp4';
      if (contentType && contentType.includes('quicktime')) ext = 'mov';
      key = `uploads/raw/${userId}/${postId}.${ext}`; // MATCH CDN PATH

    } else {
      // NOVEL: TEXT ONLY
      key = '';
    }

    // 5. INSERT INTO db5 - ADDED title
    await db5.query(`
      INSERT INTO posts(id, "userId", type, title, "mediaUrl", status, "b2Shard", caption, "externalLink", likes, comments, views, score)
      VALUES($1,$2,$3,$4,$5,'PRE_UPLOAD',$6,$7,$8,0,0,0,0)
    `, [postId, userId, postType, title || '', key, shardIndex, caption || '', externalLink || '']);

    console.log(`[INTENT OK] user:${userId} type:${postType} postId:${postId}`);
    res.json({ postId, uploadUrl: '', objectKey: key }); // uploadUrl empty because CDN handles upload

  } catch (err) {
    console.error('[Intent Error]', err.message);
    // REFUND ON ERROR (Fixed missing closing parenthesis before .catch)
    const userDb = getDbShard(userId);
    const refund = (postType === 'novel' || postType === 'story' || postType === 'store') ? 10 : 25;
    await userDb.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: refund } } }).catch(() => {});
    res.status(500).json({ error: 'Failed to create post intent' });
  } finally {
    await redis.del(`lock:${userId}`).catch(() => {});
  }
});

app.post('/api/post/create', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { postId, objectKey, title, content } = req.body;

  console.log(`[CREATE START] user:${userId} postId:${postId}`);

  try {
    const { rows } = await db5.query(`SELECT * FROM posts WHERE id=$1 AND "userId"=$2`, [postId, userId]);
    const post = rows[0];
    if (!post) return res.status(404).json({ error: 'Post not found' });
    if (post.status!== 'PRE_UPLOAD') return res.status(400).json({ error: 'Post already processed' });

    // 1. TEXT POSTS: Publish immediately
    if (post.type === 'novel') {
      await db5.query(`UPDATE posts SET status='ACTIVE', title=$1, content=$2 WHERE id=$3`, [title || '', content || '', postId]);
      return res.json({ message: 'Content published', postId });
    }

    // 2. ALL MEDIA POSTS: reel, story, store. Just mark as waiting for admin
    // DO NOT CALL CDN HERE. Admin will trigger it from /api/cdn/approve
    await db5.query(`UPDATE posts SET status='PENDING_APPROVAL', title=$1, content=$2 WHERE id=$3`, [title || '', content || '', postId]);

    console.log(`[CREATE SUCCESS] Waiting for  Approval: ${postId}`);
    res.json({ message: 'Uploaded! Waiting for approval.', postId });

 } catch (err) {
    console.error(`[CREATE ERROR] postId:${postId}`, err.message);
    const userDb = getDbShard(userId);
    await db5.query(`UPDATE posts SET status='REJECTED' WHERE id=$1`, [postId]).catch(() => {});
    const refund = (post.type === 'novel' || post.type === 'story' || post.type === 'store')? 10 : 25;
    await userDb.client.user.update({ where: { id: userId }, data: { freeCredits: { increment: refund } } }).catch(() => {});
    res.status(500).json({ error: 'Failed to publish post. Credits refunded.' });
  }
});

// ========== CDN FINALIZE CALLBACK FROM golviral-cdn ==========

app.post('/api/post/cdn-finalize', async (req, res) => {
  try {
    const { postId, status, userId, file_id, botId, type, title, caption, cdnUrl, b2Url } = req.body;
    if (!postId ||!status ||!userId) return res.status(400).json({ error: 'Missing fields' });

    const userDb = getDbShard(userId);
    console.log(`[CDN CALLBACK] postId:${postId} status:${status} file_id:${file_id} cdnUrl:${cdnUrl}`);

    // Build the real CDN URL. cdn-server should send it. If not, build from b2Url
    const finalCdnUrl = cdnUrl || `https://f005.backblazeb2.com/file/${process.env.B2_BUCKET_A}/${b2Url}`;

    // 1. WORKER FINISHED PROCESSING - SAVE MEDIA NOW
    if (status === 'PENDING_APPROVAL') {
      await db5.query(`
        UPDATE posts SET 
          status='PENDING_APPROVAL', 
          file_id=$1, 
          "botId"=$2, 
          cdn_url=$3,
          "mediaUrl"=$3, 
          type=$4, title=$5, caption=$6 
        WHERE id=$7
      `, [file_id, botId, finalCdnUrl, type || 'reel', title || '', caption || '', postId]);
      return res.json({ success: true });
    }

    // 2. ADMIN APPROVED
    if (status === 'READY') {
      await db5.query(`
        UPDATE posts SET 
          status='ACTIVE', 
          file_id=$1, 
          "botId"=$2, 
          cdn_url=$3,
          "mediaUrl"=$3, 
          type=$4, title=$5, caption=$6 
        WHERE id=$7
      `, [file_id, botId, finalCdnUrl, type || 'reel', title || '', caption || '', postId]);
      return res.json({ success: true });
    }

    // 3. ADMIN REJECTED OR FAILED
    if (status === 'REJECTED' || status === 'FAILED') {
      const { rows } = await db5.query(`SELECT type FROM posts WHERE id=$1`, [postId]);
      const postType = rows[0]?.type || 'reel';
      await db5.query(`UPDATE posts SET status='REJECTED' WHERE id=$1`, [postId]);

      const refundAmount = (postType === 'novel' || postType === 'story' || postType === 'store')? 10 : 25;
      await userDb.client.user.update({
        where: { id: userId },
        data: { freeCredits: { increment: refundAmount } }
      }).catch(() => {});

      return res.json({ success: true, refunded: refundAmount });
    }

    res.status(400).json({ error: 'Invalid status' });
  } catch (err) {
    console.error('[CDN FINALIZE ERROR]', err.message);
    res.status(500).json({ error: 'Failed to finalize post' });
  }
});

app.post('/api/view', (req, res) => {
  const { postId, userId, viewerId, viewerIp } = req.body;
  if (postId && userId) {
    interactionBuffer.push({ type: 'VIEW', postId, userId, viewerId, viewerIp, timestamp: Date.now() });
  }
  res.status(202).json({ buffered: true });
});

app.post('/api/like', authenticateToken, async (req, res) => {
  const { postId, creatorId } = req.body;
  const actorId = req.user.userId;
  await db5.query(`
    INSERT INTO likes(id, "postId", "userId", "creatorId")
    VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING
  `, [crypto.randomBytes(8).toString('hex'), postId, actorId, creatorId]);

  await db5.query(`UPDATE posts SET likes = likes + 1 WHERE id = $1`, [postId]);
  interactionBuffer.push({ type: 'LIKE', postId, userId: creatorId, actorId, timestamp: Date.now() });
  res.status(202).json({ buffered: true });
});

app.post('/api/comment', authenticateToken, async (req, res) => {
  try {
    const { postId, creatorId, text } = req.body;
    const actorId = req.user.userId;
    if (!postId ||!creatorId ||!text || text.trim().length < 2) {
      return res.status(400).json({ error: 'Invalid comment payload' });
    }

    const redis = getRedisShard(actorId);
    const cooldown = await redis.get(`cool:comment:${actorId}`).catch(() => null);
    if (cooldown) return res.status(429).json({ error: 'Comment cooldown active' });
    await redis.set(`cool:comment:${actorId}`, '1', 'EX', 120).catch(() => {});

    const commentId = crypto.randomBytes(8).toString('hex');
    await db5.query(
      `INSERT INTO comments(id, "postId", "userId", text) VALUES($1,$2,$3,$4)`,
      [commentId, postId, actorId, text.trim().slice(0, 500)]
    );

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
    const page = Number(req.query.page) || 1;
    const limit = Number(req.query.limit) || 20;
    const offset = (page - 1) * limit;

    // 1. Check post exists in db5
    const { rows: postRows } = await db5.query(`SELECT id FROM posts WHERE id=$1`, [postId]);
    if (postRows.length === 0) return res.status(404).json({ error: 'Post not found' });

    // 2. Get comments from db5 with pagination
    const { rows: comments } = await db5.query(`
      SELECT id, "userId", text, "createdAt"
      FROM comments
      WHERE "postId"=$1
      ORDER BY "createdAt" DESC
      LIMIT $2 OFFSET $3
    `, [postId, limit, offset]);

    // 3. Get total count
    const { rows: countRows } = await db5.query(
      `SELECT COUNT(*) FROM comments WHERE "postId"=$1`,
      [postId]
    );
    const total = parseInt(countRows[0].count || 0);

    if (comments.length === 0) return res.json({ comments: [], total: 0 });

    // 4. Get usernames from db1,2,3
    const userIds = [...new Set(comments.map(c => c.userId))];
    let allUsers = [];
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

    for(const db of dbs){
      try {
        const users = await db.user.findMany({
          where: { id: { in: userIds } },
          select: { id: true, username: true }
        });
        allUsers.push(...users);
      } catch(e){}
    }
    const userMap = new Map(allUsers.map(u => [u.id, u]));

    // 5. Attach username
    const finalComments = comments.map(c => ({
      id: c.id,
      text: c.text,
      createdAt: c.createdAt,
      userId: c.userId,
      user: userMap.get(c.userId) || { id: c.userId, username: 'User' }
    }));

    res.json({ comments: finalComments, total });
  } catch (err) {
    console.error('[Get Comments Error]', err.message);
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
  try {
    const now = new Date().toISOString();
    const { rows } = await db5.query(`
      SELECT
        id, "userId", type, title, content, "b2Shard", caption, file_id, "botId",
        likes, comments, views, score, "createdAt",
        CASE WHEN "isBoosted" = true AND "boostExpiresAt" > $1 THEN true ELSE false END as "isBoosted"
      FROM posts
      WHERE status='ACTIVE'
      ORDER BY CASE WHEN "isBoosted" = true AND "boostExpiresAt" > $1 THEN 1 ELSE 0 END DESC, score DESC
      LIMIT 30
    `, [now]);
    res.json(rows);
  } catch(err) {
    res.status(500).json({ error: 'Failed to load feed' });
  }
});

app.get('/api/post/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await db5.query(`SELECT * FROM posts WHERE id = $1 LIMIT 1`, [id]);
    const post = rows[0];
    
    if (!post || post.status !== 'ACTIVE') {
      return res.status(404).json({ error: 'Post not found' });
    }

    const currentlyBoosted = post.isBoosted && post.boostExpiresAt && new Date(post.boostExpiresAt) > new Date();

    res.json({
      id: post.id,
      userId: post.userId,
      type: post.type,
      title: post.title,
      content: post.content,
      caption: post.caption || '',
      // CDN fields - IMPORTANT
      file_id: post.file_id, 
      botId: post.botId,
      b2Shard: post.b2Shard,
      // Old B2 field - keep for fallback
      mediaUrl: post.mediaUrl,
      
      likes: post.likes,
      comments: post.comments,
      views: post.views,
      score: post.score,
      createdAt: post.createdAt,
      isBoosted: !!currentlyBoosted,
      externalLink: currentlyBoosted ? post.externalLink : null
    });
  } catch (err) {
    console.error('[Get Post Error]', err.message);
    res.status(500).json({ error: 'Post load failed' });
  }
});

// ========== PAYMENT & WALLET SYSTEMS ==========
const GIFT_PACKS = {
  RUBY: { ngn: 5000, points: 200, giftsTotal: 100 },
  GOLD: { ngn: 10000, points: 500, giftsTotal: 100 },
  DIAMOND: { ngn: 15000, points: 1000, giftsTotal: 100 }
};


app.post('/api/gift/init', authenticateToken, async (req, res) => {
  const { userId } = req.user;
  const { giftType } = req.body;
  const pack = GIFT_PACKS[giftType];
  if(!pack) return res.status(400).json({error:"Invalid gift"});

  const redis = getRedisShard(userId);
  // LOCK: same user + same gift type = 60 sec block
  const lockKey = `lock:gift:${userId}:${giftType}`;
  const hasLock = await redis.set(lockKey, '1', 'EX', 60, 'NX').catch(()=>null);
  if(!hasLock) return res.status(429).json({error: "Please wait, processing your last request"});

  const giftLinks = {
    RUBY: SELAR_LINKS.GIFT_RUBY,
    GOLD: SELAR_LINKS.GIFT_GOLD,
    DIAMOND: SELAR_LINKS.GIFT_DIAMOND
  }

  const db = getDbShard(userId);
  // Extra check: if PENDING ticket already exists for same type in last 2 mins, reuse it
  const existing = await db.client.deposit.findFirst({
    where: { userId, status: 'PENDING', meta: `GIFT_${giftType}`, expiresAt: {gt: new Date()} },
    orderBy: { createdAt: 'desc' }
  });
  if(existing){
    await redis.del(lockKey).catch(()=>{});
    return res.json({ selarLink: `${giftLinks[giftType]}?custom_field_1=${existing.token}`, token: existing.token });
  }

  const token = crypto.randomBytes(16).toString('hex');
  await db.client.deposit.create({
    data: { id: crypto.randomBytes(8).toString('hex'), userId, amountNaira: pack.ngn, points: pack.points, token, status: 'PENDING', meta: `GIFT_${giftType}`, expiresAt: new Date(Date.now() + 30 * 60 * 1000) }
  });

  await redis.del(lockKey).catch(()=>{});
  res.json({ selarLink: `${giftLinks[giftType]}?custom_field_1=${token}`, token });
});

app.post('/api/boost/init', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { postId, tierAmount } = req.body;

    const validTiers = {
      3500: { days: 1, link: SELAR_LINKS.BOOST_1D },
      7500: { days: 3, link: SELAR_LINKS.BOOST_3D },
      17000: { days: 7, link: SELAR_LINKS.BOOST_7D }
    };
    const tier = validTiers[tierAmount];
    if (!tier) return res.status(400).json({ error: 'Invalid boost tier' });

    const redis = getRedisShard(userId);
    // LOCK: same user + same post + same tier = 60 sec block
    const lockKey = `lock:boost:${userId}:${postId}:${tierAmount}`;
    const hasLock = await redis.set(lockKey, '1', 'EX', 60, 'NX').catch(()=>null);
    if(!hasLock) return res.status(429).json({error: "Please wait, processing your last request"});

    const db = getDbShard(userId);
    // Check if same post already has PENDING boost in last 2 mins
    const existing = await db.client.deposit.findFirst({
      where: { userId, status: 'PENDING', meta: `BOOST_${postId}_${tierAmount}`, expiresAt: {gt: new Date()} },
      orderBy: { createdAt: 'desc' }
    });
    if(existing){
      await redis.del(lockKey).catch(()=>{});
      return res.json({ selarLink: `${tier.link}?custom_field_1=${existing.token}&custom_field_2=${postId}`, token: existing.token });
    }

    const token = crypto.randomBytes(16).toString('hex');
    await db.client.deposit.create({
      data: {
        id: crypto.randomBytes(8).toString('hex'),
        userId, amountNaira: tierAmount, points: 0,
        token, status: 'PENDING', meta: `BOOST_${postId}_${tierAmount}`,
        expiresAt: new Date(Date.now() + 30 * 60 * 1000)
      }
    });

    await redis.del(lockKey).catch(()=>{});
    res.json({ selarLink: `${tier.link}?custom_field_1=${token}&custom_field_2=${postId}`, token });
  } catch (err) {
    res.status(500).json({ error: 'Boost init failed' });
  }
});


app.post('/api/payment/verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { tx_ref, token, passToken } = req.body;

    if (!(await internalVerifyPassToken(passToken))) {
      return res.status(400).json({ error: 'Math verification failed' });
    }
    if (!tx_ref || !token) {
      return res.status(400).json({ error: 'Missing tx_ref or token' });
    }

    const db = getDbShard(userId);

    // 1. Check if this tx_ref was already used ANYWHERE. Prevents double spend
    const alreadyUsed = await db.client.deposit.findFirst({
      where: { reference: tx_ref, status: 'SUCCESS' }
    });
    if (alreadyUsed) {
      return res.json({ success: true, message: 'Payment already processed' }); // 200, not 400
    }

    // 2. Find the pending deposit ticket
    const deposit = await db.client.deposit.findFirst({
      where: { userId, token, status: 'PENDING', expiresAt: { gt: new Date() } }
    });

    if (!deposit) return res.status(400).json({ error: 'Invalid or expired ticket' });

    const ops = [
      // Lock the deposit so it can't be used again
      db.client.deposit.update({
        where: { id: deposit.id },
        data: { reference: tx_ref, status: 'SUCCESS' }
      })
    ];

    let resp = { success: true };

    // 3. Route based on meta type
    if (deposit.meta === 'DM_UNLOCK') {
      ops.push(
        db.client.user.update({
          where: { id: userId },
          data: { isVerified: true, dmUnlocked: true }
        })
      );
      resp.unlocked = 'DM';

    } else if (deposit.meta?.startsWith('GIFT_')) {
      const giftType = deposit.meta.split('_')[1];
      const pack = GIFT_PACKS[giftType];
      if (!pack) return res.status(400).json({ error: 'Invalid gift' });

      ops.push(
        db.client.gift.create({
          data: {
            id: crypto.randomBytes(8).toString('hex'),
            buyerId: userId,
            giftType,
            price: deposit.amountNaira,
            pointsPerGift: pack.points,
            giftsSent: 0,
            giftsTotal: pack.giftsTotal,
            expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000)
          }
        })
      );
      resp.gift = giftType;

    } else if (deposit.meta?.startsWith('BOOST_')) {
      const parts = deposit.meta.split('_');
      const targetPostId = parts[1];
      const tierAmount = parseInt(parts[2], 10);

      const validTiers = { 3500: 1, 7500: 3, 17000: 7 };
      const durationDays = validTiers[tierAmount] || 1;
      const expireDate = new Date(Date.now() + durationDays * 24 * 60 * 60 * 1000);

      const targetPost = await findPostAcrossShards(targetPostId);
      if (targetPost) {
        await db5.query(
          `UPDATE posts SET "isBoosted"=true, "boostExpiresAt"=$1 WHERE id=$2`,
          [expireDate, targetPostId]
        );
      }
      resp.boosted = targetPostId;
      resp.expiresAt = expireDate;

    } else {
      // 4. DEFAULT: BUY POINTS. Use increment so retry is safe
      ops.push(
        db.client.user.update({
          where: { id: userId },
          data: { freeCredits: { increment: deposit.points } }
        }),
        db.client.pointsLedger.create({
          data: {
            userId,
            amount: deposit.points,
            type: 'FREE',
            action: 'DEPOSIT',
            referenceId: tx_ref
          }
        })
      );
      resp.credited = deposit.points;
    }

    // 5. Run all ops in 1 transaction. If 1 fails, all rollback
    await db.client.$transaction(ops);
    res.json(resp);

  } catch (err) {
    console.error('[Payment Verify Error]', err.message);
    // Handle race condition: 2 requests at same time
    if (err.code === 'P2002') {
      return res.json({ success: true, message: 'Payment already processed' });
    }
    res.status(500).json({ error: 'Payment verification failed' });
  }
});


// ========== MEDIA SIGNING PORT ==========
const bucketMap = {
 0: { client: b2Clients.b2a, bucket: b2Config.a.bucket },
 1: { client: b2Clients.b2b, bucket: b2Config.b.bucket },
 2: { client: b2Clients.b2c, bucket: b2Config.c.bucket }
};

app.get('/api/media/sign', authenticateToken, async (req,res)=>{
  try{
    const {postId} = req.query;
    if(!postId) return res.status(400).json({error:'postId required'});

    const { rows } = await db5.query(`SELECT file_id, "botId", type FROM posts WHERE id=$1`, [postId]);
    const post = rows[0];
    if(!post?.file_id || post?.botId === null){
      return res.status(404).json({error:'Media not ready'});
    }

    // Call CDN to get fresh TG URL
    const cdnRes = await axios.get(`${process.env.CDN_URL}/api/cdn/refresh?file_id=${post.file_id}&botId=${post.botId}`, {
      headers: { 'x-api-key': process.env.CDN_API_KEY }
    });
    res.json({ url: cdnRes.data.url });
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

    // NEW: Count both types
    const refsQualified = await db.client.referral.count({ where: { referrerId: userId, status: 'QUALIFIED' } }).catch(() => 0);
    const refsPending = await db.client.referral.count({ where: { referrerId: userId, status: 'PENDING' } }).catch(() => 0);

    const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000) || 0;
    const followers = await getTotalFollowers(userId);

    res.json({
      freeCredits: user.freeCredits,
      cashBalance: user.cashBalance,
      todayEarnings: todayEarned,
      dailyCapProgress: `${todayEarned}/10000`,
      daysToMonetize: Math.max(0, 7 - days),

      refsQualified, // Already paid 1000 each
      refsPending, // Will pay 1000 each after KYC
      estimatedPending: refsPending * 1000, // Show "fake" balance like old

      refsLeft: Math.max(0, 5 - refsQualified), // Slots left to earn
      monetized: user.monetizeFlag,
      followersProgress: `${followers}/10`,
      daysProgress: `${days}/7`
    });
  } catch (err) {
    console.error('[Wallet Error]', err.message);
    res.status(200).json({ freeCredits: 0, cashBalance: 0, todayEarnings: 0, degradedModeActive: true });
  }
});

app.get('/api/user/:id', authenticateToken, async (req, res) => {
  try {
    const { id: targetId } = req.params;
    const meId = req.userId;
    const db = getDbShard(targetId);

    const user = await db.client.user.findUnique({
      where: { id: targetId },
      select: { id: true, username: true, email: true, createdAt: true, isVerified: true, dmUnlocked: true } // ADDED EMAIL
    });
    if (!user) return res.status(404).json({ error: 'User not found' });

    // 1. Get totals from db5 where posts actually live
    const { rows: stats } = await db5.query(
      `SELECT
        COALESCE(SUM(views),0) as views,
        COALESCE(SUM(likes),0) as likes,
        COUNT(*) as total
       FROM posts
       WHERE "userId"=$1 AND status='ACTIVE'`,
      [targetId]
    );

    const totalViews = parseInt(stats[0].views);
    const totalLikes = parseInt(stats[0].likes);
    const totalPosts = parseInt(stats[0].total);

    // 2. Followers/Following count from db5
    const followers = await getTotalFollowers(targetId);
    const following = await getTotalFollowing(targetId);

    // 3. Check if ME follows TARGET
    const { rows: followRows } = await db5.query(
      `SELECT 1 FROM follows WHERE "followerId"=$1 AND "followingId"=$2 LIMIT 1`,
      [meId, targetId]
    );
    const isFollowing = followRows.length > 0;

    // 4. GET FOLLOWERS LIST + FOLLOWING LIST
    const { rows: followerRows } = await db5.query(
      `SELECT "followerId" FROM follows WHERE "followingId"=$1 LIMIT 100`, // cap at 100
      [targetId]
    );
    const followerIds = followerRows.map(r => r.followerId);

    const { rows: followingRows } = await db5.query(
      `SELECT "followingId" FROM follows WHERE "followerId"=$1 LIMIT 100`, // cap at 100
      [targetId]
    );
    const followingIds = followingRows.map(r => r.followingId);

    // Fetch usernames for both lists from all 3 shards
    let followersList = [];
    let followingList = [];
    const allIds = [...new Set([...followerIds,...followingIds])];
    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

    for(const db of dbs){
      try {
        const users = await db.user.findMany({
          where: { id: { in: allIds } },
          select: { id: true, username: true, isVerified: true }
        });
        followersList.push(...users.filter(u => followerIds.includes(u.id)));
        followingList.push(...users.filter(u => followingIds.includes(u.id)));
      } catch(e){}
    }
    // Dedupe in case user exists in 2 shards
    followersList = [...new Map(followersList.map(u => [u.id, u])).values()];
    followingList = [...new Map(followingList.map(u => [u.id, u])).values()];

    const monetized = await isUserMonetized(targetId);
    const profile = await profilePool.query(`SELECT bio FROM profiles WHERE user_id=$1`, [targetId]).catch(()=>({rows:[]}));

    const isMe = meId === targetId;

    res.json({
      userId: targetId,
      username: user.username,
      email: isMe? user.email : null, // ONLY SHOW EMAIL TO SELF
      bio: profile.rows[0]?.bio || "",
      isVerified: user.isVerified || monetized,
      dmUnlocked: user.dmUnlocked || monetized,
      totalViews,
      totalLikes,
      totalPosts, // <-- POST TOTAL
      followers,
      following,
      isFollowing,
      followersList, // <-- ARRAY OF {id, username, isVerified}
      followingList, // <-- ARRAY OF {id, username, isVerified}
      profileLink: `${APP_BASE_URL}/u/${targetId}`,
      referralLink: `${APP_BASE_URL}/auth.html?ref=${targetId}`
    });
  } catch (err) {
    console.error('[User Profile Error]', err.message);
    res.status(500).json({ error: 'Failed to load profile' });
  }
});


app.post('/api/follow', authenticateToken, async (req, res) => { 
  try {
    // 1. Safely extract followerId from req.user
    const followerId = req.user?.userId || req.user?.id || req.userId; 
    const { followingId } = req.body; 

    if (!followerId) return res.status(401).json({ error: 'Unauthorized user identity' });
    if (!followingId) return res.status(400).json({ error: 'followingId required' });
    if (followerId === followingId) return res.status(400).json({ error: 'Cannot follow yourself' }); 

    // 2. Validate target user exists
    const target = await findUserAcrossShards('id', followingId); 
    if (!target) return res.status(404).json({ error: 'User not found' }); 

    // 3. Insert into db5 follows table safely
    const result = await db5.query(
      `INSERT INTO follows ("followerId", "followingId") 
       VALUES ($1, $2) 
       ON CONFLICT ("followerId", "followingId") DO NOTHING 
       RETURNING *`, 
      [followerId, followingId]
    ); 

    // 4. Send notification only if a new row was created
    if (result.rowCount > 0) {
      const followerDb = getDbShard(followerId); 
      const followerUser = await followerDb.client.user.findUnique({ where: { id: followerId } }).catch(() => null); 
      
      if (followerUser) { 
        await sendNotification(
          followingId, 
          'FOLLOW', 
          'New Follower', 
          `${followerUser.username} started following you`
        ).catch((err) => console.error('[Follow Notification Error]', err.message)); 
      } 
      return res.status(201).json({ success: true, message: 'Followed' });
    } else {
      return res.status(200).json({ success: true, message: 'Already following' });
    }

  } catch (err) {
    console.error('[Follow Error]', err.message);
    res.status(500).json({ error: 'Follow failed' });
  }
}); 

app.post('/api/unfollow', authenticateToken, async (req, res) => { 
  try {
    // 1. Safely extract followerId from req.user
    const followerId = req.user?.userId || req.user?.id || req.userId; 
    const { followingId } = req.body; 

    if (!followerId) return res.status(401).json({ error: 'Unauthorized user identity' });
    if (!followingId) return res.status(400).json({ error: 'followingId required' });

    // 2. Remove follow relation from db5
    const result = await db5.query(
      `DELETE FROM follows WHERE "followerId"=$1 AND "followingId"=$2 RETURNING *`, 
      [followerId, followingId]
    ); 

    if (result.rowCount > 0) {
      return res.json({ success: true, message: 'Unfollowed' });
    } else {
      return res.json({ success: true, message: 'Was not following' });
    }

  } catch (err) {
    console.error('[Unfollow Error]', err.message);
    res.status(500).json({ error: 'Unfollow failed' });
  }
});


// ========== V5.2 WITHDRAWAL GATEWAY WITH KYC ==========
app.post('/api/wallet/withdraw', authenticateToken, requireFaceVerified, requireIdVerified, async (req, res) => {
  try {
    const { userId } = req.user;
    const { amountPoints, method, routingTarget, targetDetails } = req.body; // method: 'BANK' or 'USDT'
    const numericPoints = Number(amountPoints);

    const db = getDbShard(userId);
    const redis = getRedisShard(userId);

    // 1. CHECK MONETIZATION: 7 days + 10 followers
    const user = await db.client.user.findUnique({ where: { id: userId } });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const days = Math.floor((Date.now() - new Date(user.createdAt).getTime()) / 86400000);
    const followers = await getTotalFollowers(userId);

    if (!user.monetizeFlag && (days < 7 || followers < 10)) {
      return res.status(403).json({ 
        error: `Monetization required. You need 7 days and 10 followers. Current: ${days} days, ${followers} followers` 
      });
    }

    // Auto-flip monetizeFlag if they qualify now
    if (!user.monetizeFlag) {
      await db.client.user.update({
        where: { id: userId },
        data: { monetizeFlag: true, freeFarmingStopped: true }
      });
    }

    // 2. MINIMUM LIMIT
    if (isNaN(numericPoints) || numericPoints < 50000) {
      return res.status(400).json({ error: 'Minimum withdrawal is 50,000 pts (₦5000)' });
    }

    // 3. BALANCE CHECK - MUST BE CASH
    if (user.cashBalance < numericPoints) {
      return res.status(400).json({ error: 'Insufficient cash balance. Free credits cannot be withdrawn' });
    }

    // 4. USDT specific validation
    if (method === 'USDT') {
      if (numericPoints !== 120000 && numericPoints !== 12000000) {
        return res.status(400).json({ error: 'USDT withdrawals only: 120,000 pts ($10) or 12,000,000 pts ($100)' });
      }

      // Safely resolve address whether sent as string or object
      const usdtAddress = typeof targetDetails === 'string' 
        ? targetDetails 
        : (targetDetails?.address || '');

      const trc20Regex = /^T[A-Za-z1-9]{33}$/;
      if (!usdtAddress || !trc20Regex.test(usdtAddress.trim())) {
        return res.status(400).json({ error: 'Only USDT TRC20 addresses allowed' });
      }
    } 
    // 5. BANK specific validation
    else if (method === 'BANK') {
      if (
        !targetDetails || 
        typeof targetDetails !== 'object' || 
        !targetDetails.bankName || 
        !targetDetails.accountNumber || 
        !targetDetails.accountName
      ) {
        return res.status(400).json({ error: 'Bank withdrawals require bankName, accountNumber, and accountName' });
      }
    } else {
      return res.status(400).json({ error: 'Invalid method. Must be BANK or USDT' });
    }

    // 6. LIMIT: 1 withdrawal per 7 days
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const recentPayout = await db.client.payoutQueue.findFirst({
      where: { userId, createdAt: { gte: sevenDaysAgo } }
    });

    if (recentPayout) {
      return res.status(429).json({ error: 'Withdrawal locked. 1 withdrawal per week only' });
    }

    // 7. SEND OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const payload = { amountPoints: numericPoints, method, routingTarget, targetDetails, otp };

    await redis.set(`withdraw_otp:${userId}`, JSON.stringify(payload), 'EX', 600).catch(() => {
      global[`withdraw_otp_${userId}`] = { payload, exp: Date.now() + 600000 };
    });

    await sendEmail(user.email, 'Withdrawal OTP - GolViral', `<p>Your withdrawal code: <b>${otp}</b>. Valid 10 minutes.</p>`);
    res.json({ authChallenge: true, message: 'OTP sent to email' });

  } catch (err) {
    console.error('[Withdraw Init Error]', err.message);
    res.status(500).json({ error: 'Withdrawal gateway error' });
  }
});

app.get('/api/test-push', authenticateToken, async (req,res)=>{
  const { userId } = req.user;
  console.log('[TEST PUSH] Using VAPID:', process.env.VAPID_PUBLIC_KEY.slice(0,10)+'...');
  
  try {
    await sendNotification(userId, 'TEST', 'VAPID Test', 'If you see this, key is valid');
    res.json({sent: true, key: process.env.VAPID_PUBLIC_KEY.slice(0,10)+'...'});
  } catch(e){
    console.error('[TEST PUSH ERROR]', e.message);
    res.status(500).json({error: e.message});
  }
})

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
// ========== SOCKET.IO & V4.6 FALLBACK MESSAGING ==========
app.post('/api/message/send', authenticateToken, requireDMUnlock, async (req,res)=>{
  const {receiverId, text} = req.body;
  const senderId = req.user.userId;

  const senderDb = getDbShard(senderId);
  const receiverDb = getDbShard(receiverId);
  const msgId = crypto.randomBytes(8).toString('hex');
  const msgData = {id: msgId, senderId, receiverId, text};

  const ops = [senderDb.client.message.create({data: msgData})];
  if(senderDb.name!== receiverDb.name){
    ops.push(receiverDb.client.message.create({data: msgData}));
  }
  await Promise.all(ops);

  res.json({sent:true})
})

// ========== GET DM HISTORY BETWEEN 2 USERS - CHECKS BOTH SHARDS ==========
app.get('/api/messages/:userId', authenticateToken, async (req,res)=>{
  try {
    const me = req.user.userId;
    const other = req.params.userId;

    // We need to check both sender's shard and receiver's shard
    const meDb = getDbShard(me).client;
    const otherDb = getDbShard(other).client;
    const dbsToCheck = [meDb];
    if(meDb!== otherDb) dbsToCheck.push(otherDb); // only add if different shard

    let allMsgs = [];

    for(const db of dbsToCheck){
      try {
        const msgs = await db.message.findMany({
          where:{
            OR:[
              {senderId:me, receiverId:other},
              {senderId:other, receiverId:me}
            ]
          },
          orderBy:{createdAt:'asc'},
          take:200 // get last 200
        });
        allMsgs.push(...msgs);
      }catch(e){
        console.error('[Msg History Shard Error]', e.message)
      }
    }

    // Dedupe messages by ID in case they exist in both shards
    const uniqueMsgs = [...new Map(allMsgs.map(item => [item.id, item])).values()];

    // Sort again after dedupe
    uniqueMsgs.sort((a,b) => new Date(a.createdAt) - new Date(b.createdAt));

    res.json(uniqueMsgs);

  } catch (err) {
    console.error('[Get Messages Error]', err.message);
    res.status(500).json({error: 'Failed to load messages'});
  }
})
// ========== GET ALL CONVERSATIONS - CHECK ALL 3 SHARDS ==========
app.get('/api/messages', authenticateToken, async (req,res)=>{
  const me = req.user.userId;
  const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  
  let allMsgs = [];
  
  // 1. Get all messages from all 3 shards
  for(const db of dbs){
    try {
      const msgs = await db.message.findMany({
        where: { OR:[{senderId:me},{receiverId:me}] },
        orderBy:{createdAt:'desc'},
        take: 100
      });
      allMsgs.push(...msgs);
    } catch(e){ console.error('[Msg Shard Error]', e.message) }
  }

  if(allMsgs.length === 0) return res.json({chats: []});

  // 2. Sort all messages globally and get unique users
  allMsgs.sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt));
  const otherIds = [...new Set(allMsgs.map(m => m.senderId === me ? m.receiverId : m.senderId))];

  // 3. Get user data from all shards in 1 go
  let allUsers = [];
  for(const db of dbs){
    try {
      const users = await db.user.findMany({
        where: { id: { in: otherIds } },
        select: { id: true, username: true }
      });
      allUsers.push(...users);
    } catch(e){}
  }
  const userMap = new Map(allUsers.map(u => [u.id, u]));

  // 4. Build chat list with only last message per user
  const chats = [];
  for(const m of allMsgs){
    const otherId = m.senderId === me ? m.receiverId : m.senderId;
    if(!chats.find(c => c.userId === otherId)){
      chats.push({
        userId: otherId,
        username: userMap.get(otherId)?.username || 'User',
        lastMessage: m.text,
        lastTime: m.createdAt
      })
    }
  }
  res.json({chats});
});
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
  await db.client.notification.create({ data: { userId, type, title, body, data } }).catch(()=>{});

  const subs = await db.client.pushSubscription.findMany({ where: { userId } }).catch(()=>[]);
  if(subs.length === 0) return;

  for(const sub of subs){
    const pushSubscription = { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } };
    webpush.sendNotification(pushSubscription, JSON.stringify({title, body, data}))
  .then(() => console.log(`[Push OK] VAPID key is VALID. Sent to ${userId}`))
  .catch(async (err) => {
      console.error(`[Push FAIL] VAPID key is FAKE/WRONG`);
      console.error(`Status: ${err.statusCode} | Body: ${err.body}`); // 400 = bad key
      if(err.statusCode === 410 || err.statusCode === 404){
        await db.client.pushSubscription.delete({ where: { id: sub.id } }).catch(()=>{});
      }
    });
  }
}

// ========== ENDPOINT 1: PROFILE POSTS ==========
app.get('/api/user/:id/posts', authenticateToken, async (req, res) => { 
  try {
    const { id: targetId } = req.params; 
    const { page = 1, limit = 12 } = req.query; 
    
    const pageNum = Math.max(1, parseInt(page) || 1);
    const limitNum = Math.min(50, parseInt(limit) || 12);
    const offset = (pageNum - 1) * limitNum;

    const { rows } = await db5.query(
      `SELECT id, "userId", type, title, caption, cdn_url, file_id, "botId", views, likes, "createdAt" 
       FROM posts 
       WHERE "userId"=$1 
         AND status='ACTIVE' 
       ORDER BY "createdAt" DESC 
       LIMIT $2 OFFSET $3`, 
      [targetId, limitNum, offset] 
    ); 

    res.json(rows);
  } catch(err) { 
    console.error('[User Posts Error]', err.message);
    res.status(500).json({error: 'Failed to load user posts'}) 
  } 
});

// ========== ENDPOINT 2: EXPLORE ==========
app.get('/api/explore', async (req, res) => {
  try {
    const { page = 1, limit = 20 } = req.query;
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    
    const { rows } = await db5.query(`
      SELECT * FROM posts 
      WHERE status='ACTIVE' 
        AND type='reel' 
        AND views >= 50 
        AND "createdAt" >= $1
      ORDER BY 
        CASE WHEN "isBoosted" = true AND "boostExpiresAt" > NOW() THEN 1 ELSE 0 END DESC, 
        score DESC
      LIMIT $2 OFFSET $3
    `, [sevenDaysAgo, limit, (page-1)*limit]);

    res.json(rows);
  } catch (err) { 
    console.error('[Explore Error]', err.message);
    res.status(500).json({ error: 'Failed to load explore' }); 
  }
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
        // FIX: Get followers from db5 where follows table actually is
        const { rows } = await db5.query(`SELECT COUNT(*) FROM follows WHERE "followingId"=$1`, [u.id]);
        const followers = parseInt(rows[0].count);
        
        allUsers.push({...u, followers})
      }
    }
    
    // Dedupe users in case same username exists on 2 shards
    const uniqueUsers = [...new Map(allUsers.map(item => [item.id, item])).values()];
    
    uniqueUsers.sort((a,b) => b.followers - a.followers);
    res.json(uniqueUsers.slice(0, 20));
    
  } catch (err) { 
    console.error('[Search Users Error]', err.message);
    res.status(500).json({ error: 'Search failed' }); 
  }
});

// ========== ENDPOINT 4-7: PUSH SYSTEM ==========
app.post('/api/push/subscribe', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user; // Correct
    const { subscription } = req.body; // {endpoint, keys:{p256dh, auth}}
    
    if(!subscription || !subscription.endpoint) {
      return res.status(400).json({error: 'Invalid subscription'});
    }
    
    const db = getDbShard(userId);
    await db.client.pushSubscription.upsert({
      where: { endpoint: subscription.endpoint },
      update: { 
        p256dh: subscription.keys.p256dh, 
        auth: subscription.keys.auth,
        userId: userId // in case user logged in on new device
      },
      create: { 
        userId, 
        endpoint: subscription.endpoint, 
        p256dh: subscription.keys.p256dh, 
        auth: subscription.keys.auth 
      }
    });
    
    console.log(`[PUSH] Saved subscription for ${userId}`);
    res.json({success: true});
  } catch(e) {
    console.error('[PUSH SUB ERROR]', e);
    res.status(500).json({error: e.message});
  }
});

app.post('/api/push/unsubscribe', authenticateToken, async (req, res) => {
  try {
    const { endpoint } = req.body;
    const { userId } = req.user; // BUG FIX: was req.userId
    const db = getDbShard(userId);
    await db.client.pushSubscription.delete({ where: { endpoint } }).catch(()=>{});
    res.json({success: true});
  } catch(e) {
    res.status(500).json({error: e.message});
  }
});

app.get('/api/notifications', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user; // Correct
    const { page = 1, limit = 20 } = req.query;
    const db = getDbShard(userId);
    const notifs = await db.client.notification.findMany({ 
      where: { userId }, 
      orderBy: { createdAt: 'desc' }, 
      skip: (page-1)*limit, 
      take: Number(limit) 
    });
    res.json(notifs);
  } catch(e) {
    res.status(500).json({error: e.message});
  }
});

app.post('/api/notifications/read/:id', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user; // BUG FIX: was req.userId
    const db = getDbShard(userId);
    await db.client.notification.update({ 
      where: { id: req.params.id, userId: userId }, // added userId check for security
      data: { isRead: true } 
    }).catch(()=>{});
    res.json({success: true});
  } catch(e) {
    res.status(500).json({error: e.message});
  }
});
// 3. EXPORT FOR profile.js + ADMIN.JS TO USE
module.exports = {
  prismaClients,
  findPostAcrossShards,
  sendNotification,
  getDbShard
};
// ========== CHORE SYSTEM SCHEDULER CRON SERVICES ==========

// 1. Interaction Buffer Batch Processor (Every 30 Seconds)
cron.schedule('*/30 * * * * *', async () => {
  if (interactionBuffer.length === 0) return;

  const batch = [...interactionBuffer];
  interactionBuffer = [];

  const failedItems = [];
  const MILESTONES = [100, 1000, 10000, 100000];

  for (const item of batch) {
    try {
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

          await db5.query(`UPDATE posts SET views = views + 1 WHERE id=$1`, [item.postId]);
          const { rows } = await db5.query(`SELECT views, "userId", title, type FROM posts WHERE id=$1`, [item.postId]);
          const p = rows[0];

          if (p && MILESTONES.includes(p.views)) {
            const milestoneKey = `milestone:${item.postId}`;
            const alreadySent = await redis.sismember(milestoneKey, p.views).catch(() => 0);
            if (!alreadySent) {
              await redis.sadd(milestoneKey, p.views).catch(() => {});
              await redis.expire(milestoneKey, 30 * 24 * 60 * 60).catch(() => {});
              
              // Safe slice on optional title
              const safeTitle = (p.title || '').slice(0, 20);
              const viewDisplay = p.views >= 1000 ? `${p.views / 1000}K` : p.views;

              await sendNotification(
                p.userId,
                'VIRAL',
                `🔥 ${viewDisplay} Views!`,
                `Your ${p.type} "${safeTitle}" just hit ${p.views.toLocaleString()} views!`
              ).catch(() => {});
            }
          }
        }

      } else if (item.type === 'LIKE') {
        await processWalletTransaction({ userId: item.userId, action: 'LIKE', isCreator: true, meta: { refId: item.postId } });
        await processWalletTransaction({ userId: item.actorId, action: 'LIKE', isCreator: false, meta: { refId: item.postId } });
        await db5.query(`UPDATE posts SET likes = likes + 1 WHERE id=$1`, [item.postId]);

      } else if (item.type === 'COMMENT') {
        await processWalletTransaction({ userId: item.userId, action: 'COMMENT', isCreator: true, meta: { refId: item.postId } });
        await processWalletTransaction({ userId: item.actorId, action: 'COMMENT', isCreator: false, meta: { refId: item.postId } });
        await db5.query(`UPDATE posts SET comments = comments + 1 WHERE id=$1`, [item.postId]);

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

// 2. Nightly Monetization Evaluation (00:00 Daily)
cron.schedule('0 0 * * *', async () => {
  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const users = await db.user.findMany({ where: { monetizeFlag: false } });
      for (const user of users) {
        const days = Math.floor((Date.now() - new Date(user.createdAt)) / 86400000);
        const followers = await getTotalFollowers(user.id);

        if (days >= 7 && followers >= 10) {
          await db.user.update({ where: { id: user.id }, data: { monetizeFlag: true, freeFarmingStopped: true } });
          
          // Added missing await
          await sendNotification(user.id, 'MONETIZE', "Congrats! You're Earning 💰", 'You hit 7 days + 10 followers. Earnings now go to Cash.');
          await sendEmail(user.email, 'Monetization Activated!', 'You hit 7 days + 10 followers. Earnings now go to Cash.');
        }
      }
    } catch (err) {
      console.error('[Midnight Cron Error]', err.message);
    }
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

// 4. B2 Media Archive & Cleanup (Every Day at 3:00 AM)
cron.schedule('0 3 * * *', async () => {
  const cutoff = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);

  // Map b2Shard index to client + bucket
  const bucketMap = {
    0: { client: b2Clients.b2a, bucket: b2Config.a.bucket },
    1: { client: b2Clients.b2b, bucket: b2Config.b.bucket },
    2: { client: b2Clients.b2c, bucket: b2Config.c.bucket }
  };

  try {
    // 1. Get all old posts from db5
    const { rows: posts } = await db5.query(`
      SELECT id, "mediaUrl", "b2Shard" FROM posts
      WHERE "createdAt" < $1 
        AND status='ACTIVE' 
        AND ("isBoosted"=false OR "isBoosted" IS NULL)
    `, [cutoff]);

    // 2. Delete from B2 and archive in db5
    for (const p of posts) {
      const b2 = bucketMap[p.b2Shard] || bucketMap[0]; // fallback to bucket A

      if (p.mediaUrl && !p.mediaUrl.startsWith('http')) {
        await b2.client.send(new DeleteObjectCommand({ Bucket: b2.bucket, Key: p.mediaUrl })).catch(() => {});

        const thumbKey = p.mediaUrl.replace('media/', 'thumbs/').replace(/\.[^/.]+$/, '.jpg');
        await b2.client.send(new DeleteObjectCommand({ Bucket: b2.bucket, Key: thumbKey })).catch(() => {});
      }

      await db5.query(`
        UPDATE posts 
        SET status='ARCHIVED', "mediaUrl"=null, "thumbnailUrl"=null 
        WHERE id=$1
      `, [p.id]).catch(() => {});
    }

    if (posts.length > 0) {
      console.log(`[B2 CRON] Archived ${posts.length} posts`);
    }

  } catch (cronErr) {
    console.error('[B2 Cron Archive Exception]', cronErr.message);
  }
});

// 5. Score Recalculator Engine (Every 5 minutes)
cron.schedule('*/5 * * * *', async () => {
  try {
    const { rows: posts } = await db5.query(`SELECT id, likes, comments, views, "createdAt" FROM posts WHERE status='ACTIVE'`);
    for (const p of posts) {
      const hoursOld = (Date.now() - new Date(p.createdAt)) / 1000 / 3600;
      const newScore = (p.likes * 2) + (p.comments * 3) + (p.views * 0.1) - (hoursOld * 0.5);
      await db5.query(`UPDATE posts SET score=$1 WHERE id=$2`, [newScore, p.id]);
    }
  } catch (e) {
    console.error('[Score Cron Error]', e.message);
  }
});

// 6. Boost Expiration Cron System (Every 1 hour)
cron.schedule('0 * * * *', async () => {
  try {
    const result = await db5.query(`
      UPDATE posts SET "isBoosted"=false
      WHERE "isBoosted"=true AND "boostExpiresAt" < NOW()
    `);
    if (result.rowCount > 0) {
      console.log(`[Boost Engine] Expired ${result.rowCount} posts from matrix index.`);
    }
  } catch (err) {
    console.error('[Boost Expiration Process Error]', err.message);
  }
});

// 7. 72H EPHEMERAL DM DELETION (Every 1 hour)
cron.schedule('0 * * * *', async () => {
  const cutoffTime = new Date(Date.now() - 72 * 60 * 60 * 1000);
  let deletedTotal = 0;

  const targets = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
  for (const db of targets) {
    try {
      const deleted = await db.message.deleteMany({
        where: { createdAt: { lt: cutoffTime } }
      }).catch(() => ({ count: 0 }));
      deletedTotal += deleted.count;
    } catch (err) {
      console.error('[DM Cleanup Cron Error]', err.message);
    }
  }
  if (deletedTotal > 0) console.log(`[DM CRON] Deleted ${deletedTotal} messages >72h`);
});


// ========== HEALTH CHECK UP ==========
app.get('/', (req, res) => {
  res.status(200).json({ status: "online", core: "GolViral Hardened Engine Infrastructure Matrix", version: "5.1" });
});

// ========== START PORT BOOTSTRAP ==========
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[SYSTEM BOOT SUCCESSFUL] WS + HTTP Listening on port: ${PORT}`);
});
