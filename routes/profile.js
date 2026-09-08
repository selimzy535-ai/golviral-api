const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');
const axios = require('axios');
const { profilePool, prismaClients, getDbShard, processWalletTransaction } = require('../utils/shard');
const db4 = profilePool;

const CDN_URL = process.env.CDN_URL || process.env.MAIN_CDN_URL;
const CDN_API_KEY = process.env.CDN_API_KEY;

const avatarCache = new Map(); // userId -> {url, exp}

// HELPERS
async function getProfile(userId) {
  const { rows } = await db4.query(
    `SELECT user_id, bio, face_hash, face_verified, id_hash, id_verified, id_verified_at,
            id_photo_temp, id_status, created_at, updated_at,
            avatar_file_id, avatar_bot_id, avatar_updated_at
     FROM profiles WHERE user_id=$1`, [userId]
  );
  return rows[0];
}

async function getAvatarUrl(profile) {
  if (!profile?.avatar_file_id) return null;

  // cache 10 mins
  const cached = avatarCache.get(profile.user_id);
  if (cached && cached.exp > Date.now()) return cached.url;

  if (!CDN_URL ||!CDN_API_KEY) return null;
  try {
    const r = await axios.get(`${CDN_URL}/api/cdn/refresh`, {
      params: { file_id: profile.avatar_file_id, botId: profile.avatar_bot_id || 0 },
      headers: { 'x-api-key': CDN_API_KEY },
      timeout: 5000
    });
    const url = r.data.url;
    if (url) avatarCache.set(profile.user_id, { url, exp: Date.now() + 10*60*1000 });
    return url;
  } catch (e) {
    console.log('[Avatar Refresh Fail]', e.message);
    return null;
  }
}

function authenticateToken(req, res, next) {
  const JWT_SECRET = process.env.JWT_SECRET || process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
  try {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Access token required' });
    jwt.verify(token, JWT_SECRET, (err, user) => {
      if (err) return res.status(403).json({ error: 'Token invalid or expired' });
      req.user = user;
      next();
    });
  } catch (err) { res.status(500).json({ error: 'Auth error' }); }
}

async function requireFaceVerified(req, res, next) {
  try {
    const profile = await getProfile(req.user.userId);
    if (!profile?.face_verified) return res.status(403).json({ error: "Face verification required" });
    next();
  } catch (err) { res.status(500).json({ error: 'Verification check failed' }); }
}
async function requireIdVerified(req, res, next) {
  try {
    const profile = await getProfile(req.user.userId);
    if (!profile?.id_verified) return res.status(403).json({ error: "ID verification required for withdrawal" });
    next();
  } catch (err) { res.status(500).json({ error: 'Verification check failed' }); }
}

router.post('/bio', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { bio } = req.body;
    if (typeof bio!== 'string') return res.status(400).json({ error: 'Bio must be text' });
    const cleanBio = bio.slice(0, 150).trim();
    await db4.query(`INSERT INTO profiles(user_id, bio) VALUES($1,$2) ON CONFLICT (user_id) DO UPDATE SET bio=$2`, [userId][cleanBio]);
    res.json({ success: true, bio: cleanBio });
  } catch (err) { res.status(500).json({ error: 'Failed to save bio' }); }
});

router.delete('/bio', authenticateToken, async (req, res) => {
  try { await db4.query(`UPDATE profiles SET bio='' WHERE user_id=$1`, [req.user.userId]); res.json({ success: true, bio: "" }); }
  catch { res.status(500).json({ error: 'Failed to clear bio' }); }
});

router.post('/avatar-save', async (req, res) => {
  const key = req.headers['x-api-key'];
  if (key!== CDN_API_KEY) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { userId, file_id, botId } = req.body;
    if (!userId ||!file_id) return res.status(400).json({ error: 'Missing fields' });
    await db4.query(`INSERT INTO profiles(user_id, avatar_file_id, avatar_bot_id, avatar_updated_at) VALUES($1,$2,$3,NOW()) ON CONFLICT (user_id) DO UPDATE SET avatar_file_id=$2, avatar_bot_id=$3, avatar_updated_at=NOW()`, [userId, file_id, botId || 0]);
    avatarCache.delete(userId); // clear cache
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: 'Failed to save avatar' }); }
});

router.delete('/avatar', authenticateToken, async (req, res) => {
  try {
    await db4.query(`UPDATE profiles SET avatar_file_id=NULL, avatar_bot_id=NULL, avatar_updated_at=NOW() WHERE user_id=$1`, [req.user.userId]);
    avatarCache.delete(req.user.userId);
    res.json({ success: true, avatarUrl: null });
  } catch (err) { res.status(500).json({ error: 'Failed to delete avatar' }); }
});

router.post('/face-verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user; const { faceHash } = req.body;
    if (!faceHash || faceHash.length!== 64) return res.status(400).json({ error: 'Invalid faceHash' });
    const dup = await db4.query(`SELECT user_id FROM profiles WHERE face_hash=$1`, [faceHash]);
    if (dup.rows.length > 0 && dup.rows[0].user_id!== userId) return res.status(400).json({ error: "This face is already registered to another account" });
    await db4.query(`INSERT INTO profiles(user_id, face_hash, face_verified) VALUES($1,$2,true) ON CONFLICT (user_id) DO UPDATE SET face_hash=$2, face_verified=true`, [userId, faceHash]);
    try {
      const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
      let pendingRef = null;
      for (const db of dbs) { const ref = await db.referral.findFirst({ where: { refereeId: userId, status: 'PENDING' } }).catch(() => null); if (ref) { pendingRef = ref; break; } }
      if (pendingRef) {
        const referrerId = pendingRef.referrerId; const refDb = getDbShard(referrerId);
        const updated = await refDb.client.referral.updateMany({ where: { id: pendingRef.id, status: 'PENDING' }, data: { status: 'QUALIFIED' } });
        if (updated.count > 0) await processWalletTransaction({ userId: referrerId, action: 'REFERRAL_BONUS', isCreator: false });
      }
    } catch (refErr) { console.error('[Referral Payout Error]', refErr.message); }
    res.json({ success: true, faceVerified: true });
  } catch (err) { res.status(500).json({ error: 'Face verification failed' }); }
});

router.post('/id-upload', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user; const { idPhotoBase64 } = req.body;
    if (!idPhotoBase64 ||!idPhotoBase64.startsWith('data:image')) return res.status(400).json({ error: 'Invalid image data' });
    await db4.query(`INSERT INTO profiles(user_id, id_photo_temp, id_status) VALUES($1,$2,'PENDING') ON CONFLICT (user_id) DO UPDATE SET id_photo_temp=$2, id_status='PENDING'`, [userId][idPhotoBase64]);
    res.json({ success: true, message: 'ID submitted for review' });
  } catch (err) { res.status(500).json({ error: 'ID upload failed' }); }
});

router.get('/kyc-status', authenticateToken, async (req, res) => {
  try {
    const profile = await getProfile(req.user.userId);
    const avatarUrl = await getAvatarUrl(profile);
    res.json({ bio: profile?.bio || "", avatarUrl, faceVerified: profile?.face_verified || false, idVerified: profile?.id_verified || false, idStatus: profile?.id_status || 'NONE', canRefer: profile?.face_verified || false, canWithdraw: (profile?.face_verified && profile?.id_verified) || false });
  } catch (err) { res.status(500).json({ error: 'Failed to load KYC' }); }
});

router.get('/:userId', async (req, res) => {
  try {
    const profile = await getProfile(req.params.userId);
    const avatarUrl = await getAvatarUrl(profile);
    res.json({ userId: req.params.userId, bio: profile?.bio || "", avatarUrl, isVerified: profile?.face_verified || false, joinedAt: profile?.created_at || null });
  } catch (err) { res.status(500).json({ error: 'Failed to load profile' }); }
});

module.exports = { router, requireFaceVerified, requireIdVerified };