const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');
const { PrismaClient } = require('@prisma/client'); // ADDED

// ========== KILL CIRCULAR DEPENDENCY: MAKE OWN DB ==========
const dbUrls = [
  process.env.DATABASEURL1,
  process.env.DATABASEURL2,
  process.env.DATABASEURL3
];

const prismaClients = { // ADDED: own clients
  db1: new PrismaClient({ datasources: { db: { url: dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db1" } }),
  db2: new PrismaClient({ datasources: { db: { url: dbUrls[1] || dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db2" } }),
  db3: new PrismaClient({ datasources: { db: { url: dbUrls[2] || dbUrls[0] || "postgresql://mock:fallback@127.0.0.1:5432/db3" } }),
};

Object.entries(prismaClients).forEach(([name, client]) => {
  client.$connect().catch((err) => console.error(`[Admin Prisma] Shard ${name} offline.`, err.message));
});

// ========== ONLY IMPORT WHAT WE NEED FROM SERVER ==========
const { sendNotification } = require('../server'); // ONLY this

const JWT_SECRET = process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
const ADMIN_SECRET = process.env.ADMIN_SECRET_KEY; // ADD THIS TO RENDER ENV

// ========== COPIED HELPERS TO KILL CIRCLE ==========
function findPostAcrossShards(id) { // ADDED: own copy
  const dbs = [ {client: prismaClients.db1}, {client: prismaClients.db2}, {client: prismaClients.db3} ];
  return (async () => {
    for (const db of dbs) {
      try { const post = await db.client.post.findUnique({ where: { id } }); if (post) return { post, db: db.client }; }
      catch (err) {}
    } return null;
  })();
}

function getDbShard(userId) {
  const idx = parseInt(userId, 36) % 3;
  if (idx === 1) return { client: prismaClients.db2 };
  if (idx === 2) return { client: prismaClients.db3 };
  return { client: prismaClients.db1 };
}

function requireAdmin(req, res, next) {
  try {
    const adminKey = req.headers['x-admin-key'];
    if (ADMIN_SECRET && adminKey === ADMIN_SECRET) {
      console.log(`[ADMIN ACCESS] Secret Key used`);
      req.userId = 'admin-secret-key';
      return next();
    }
    const token = req.headers['authorization']?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Admin Secret Key or Login required' });
    const user = jwt.verify(token, JWT_SECRET);
    req.userId = user.userId;
    const db = getDbShard(user.userId);
    return db.client.user.findUnique({ where: { id: user.userId } }).then(u => {
      if (u?.role!== 'admin') return res.status(403).json({ error: 'Admin only' });
      next();
    }).catch(() => res.status(500).json({ error: "DB error" }));
  } catch { res.status(403).json({ error: 'Invalid credentials' }); }
}

// ========== 1. MASTER USER CONTROL: LIST, DELETE, MONETIZE ==========
router.post('/users/manage', requireAdmin, async (req, res) => {
  try {
    const adminId = req.userId;
    const { action, userId, page = 1, limit = 50, search = '' } = req.body;

    if (!action) return res.status(400).json({ error: 'Missing action field' });
    console.log(`[ADMIN ACTION] Admin:${adminId} Action:${action} Target:${userId || 'N/A'}`);

    const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];

    if (action === 'list') {
      const allUsers = [];
      for (const db of dbs) {
        const users = await db.client.user.findMany({
          where: search ? {
            OR: [
              { username: { contains: search, mode: 'insensitive' } },
              { email: { contains: search, mode: 'insensitive' } }
            ]
          } : {},
          select: {
            id: true, username: true, email: true, createdAt: true,
            monetizeFlag: true, freeCredits: true, cashBalance: true,
            isVerified: true, dmUnlocked: true, role: true
          },
          orderBy: { createdAt: 'desc' },
          take: Number(limit) * 3
        }).catch(() => []);

        for (const u of users) {
          let followers = 0;
          for (const shard of dbs) {
            followers += await shard.client.follow.count({ where: { followingId: u.id } }).catch(() => 0);
          }
          allUsers.push({ ...u, followers });
        }
      }
      allUsers.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
      const start = (Number(page) - 1) * Number(limit);
      return res.json({ users: allUsers.slice(start, start + Number(limit)), total: allUsers.length });
    }

    if (!userId) return res.status(400).json({ error: 'Missing userId for this action' });
    const db = getDbShard(userId);
    const targetUser = await db.client.user.findUnique({ where: { id: userId } });
    if (!targetUser) return res.status(404).json({ error: 'User not found on any shard' });

    if (action === 'delete') {
      if (userId === adminId) return res.status(400).json({ error: 'Cannot delete yourself' });
      await db.client.$transaction([
        db.client.post.deleteMany({ where: { userId } }),
        db.client.comment.deleteMany({ where: { userId } }),
        db.client.message.deleteMany({ where: { OR: [{ senderId: userId }, { receiverId: userId }] } }),
        db.client.follow.deleteMany({ where: { OR: [{ followerId: userId }, { followingId: userId }] } }),
        db.client.notification.deleteMany({ where: { userId } }),
        db.client.pointsLedger.deleteMany({ where: { userId } }),
        db.client.deposit.deleteMany({ where: { userId } }),
        db.client.supportTicket.deleteMany({ where: { userId } }),
        db.client.pushSubscription.deleteMany({ where: { userId } }),
        db.client.user.delete({ where: { id: userId } })
      ]);
      return res.json({ success: true, message: `User ${targetUser.username} deleted` });
    }

    if (action === 'monetize') {
      await db.client.user.update({ where: { id: userId }, data: { monetizeFlag: true, freeFarmingStopped: true } });
      return res.json({ success: true, message: `User ${targetUser.username} is now monetized` });
    }

    if (action === 'unmonetize') {
      await db.client.user.update({ where: { id: userId }, data: { monetizeFlag: false, freeFarmingStopped: false } });
      return res.json({ success: true, message: `User ${targetUser.username} monetization removed` });
    }

    return res.status(400).json({ error: 'Invalid action. Use: list, delete, monetize, unmonetize' });
  } catch (err) {
    console.error('[ADMIN MANAGE ERROR]', err.message);
    res.status(500).json({ error: 'Admin operation failed' });
  }
});

// ========== 2. DEPOSITS ==========
router.get('/deposits', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const deposits = await db.client.deposit.findMany({
      orderBy: { createdAt: 'desc' },
      take: 100,
      include: { user: { select: { username: true, email: true } } }
    }).catch(() => []);
    all.push(...deposits);
  }
  res.json(all);
});

// ========== 3. POST MODERATION ==========
router.get('/posts/pending', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    const posts = await db.client.post.findMany({ where: { status: 'PRE_UPLOAD' }, include: { user: true } }).catch(() => []);
    all.push(...posts);
  }
  res.json(all);
});

router.post('/posts/:id/approve', requireAdmin, async (req, res) => {
  const { id } = req.params;
  const target = await findPostAcrossShards(id);
  if (!target) return res.status(404).json({ error: 'Post not found across infrastructure shards' });
  await target.db.post.update({ where: { id }, data: { status: 'ACTIVE' } });
  res.json({ success: true });
});

router.post('/posts/:id/reject', requireAdmin, async (req, res) => {
  const { id } = req.params;
  const target = await findPostAcrossShards(id);
  if (!target) return res.status(404).json({ error: 'Post not found across infrastructure shards' });

  const refundAmount = target.post.type === 'reel' ? 25 : 10;
  const userDb = getDbShard(target.post.userId);

  await target.db.post.update({ where: { id }, data: { status: 'REJECTED' } });
  await userDb.client.user.update({ where: { id: target.post.userId }, data: { freeCredits: { increment: refundAmount } } });
  sendNotification(target.post.userId, 'POST', 'Post Rejected', `Your post was rejected. ${refundAmount} credits refunded`);

  res.json({ success: true, refunded: refundAmount });
});

// ========== 4. PAYOUTS ==========
router.get('/payouts', requireAdmin, async (req, res) => {
  const all = [];
  for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
    await db.client.payoutQueue.findMany({ where: { status: 'PENDING' } })
      .then(r => all.push(...r)).catch(() => {});
  }
  res.json(all);
});

router.post('/payouts/approve', requireAdmin, async (req, res) => {
  try {
    const { payoutId, userId } = req.body;
    const db = getDbShard(userId);
    const payout = await db.client.payoutQueue.findUnique({ where: { id: payoutId } });
    if (!payout) return res.status(404).json({ error: 'Payout not found' });

    await db.client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'APPROVED' } });
    const amount = payout.amountPoints / 10;
    sendNotification(userId, 'WITHDRAW', 'Withdrawal Approved ✅', `Your ₦${amount} withdrawal is approved and processing`);
    res.json({ success: true });
  } catch (e) {
    console.error('[Payout Approve Error]', e.message);
    res.status(500).json({ error: 'Ledger tracking execution failed' });
  }
});

router.post('/payouts/reject', requireAdmin, async (req, res) => {
  try {
    const { payoutId, userId, reason } = req.body;
    const db = getDbShard(userId);
    const payout = await db.client.payoutQueue.findUnique({ where: { id: payoutId } });
    if (!payout) return res.status(404).json({ error: 'Payout not found' });

    await db.client.$transaction([
      db.client.user.update({ where: { id: userId }, data: { cashBalance: { increment: payout.amountPoints } } }), // FIXED: Closing brace added
      db.client.payoutQueue.update({ where: { id: payoutId }, data: { status: 'REJECTED', reason } })
    ]);
    sendNotification(userId, 'WITHDRAW', 'Withdrawal Rejected ❌', `Reason: ${reason}`);
    res.json({ success: true });
  } catch (err) {
    console.error('[Payout Reject Error]', err.message);
    res.status(500).json({ error: 'Admin reversion system block handled execution fallback' });
  }
});

// ========== 5. SUPPORT TICKETS ==========
router.get('/support', requireAdmin, async (req, res) => {
  try {
    const all = [];
    for (const db of [prismaClients.db1, prismaClients.db2, prismaClients.db3]) {
      const tickets = await db.client.supportTicket.findMany({
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

router.post('/support/reply', requireAdmin, async (req, res) => {
  try {
    const { ticketId, userId, reply } = req.body;
    if (!ticketId || !userId || !reply) return res.status(400).json({ error: 'Missing ticket transaction credentials' });
    const db = getDbShard(userId);
    await db.client.supportTicket.update({
      where: { id: ticketId },
      data: { reply: reply.trim(), status: 'RESOLVED' }
    });
    sendNotification(userId, 'SUPPORT', 'Support Reply', `Admin replied: ${reply.slice(0, 50)}`);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to process admin support verification step' });
  }
});

module.exports = router;
