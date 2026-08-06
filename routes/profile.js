
const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');

// Import all 4 shards
const { profilePool, prismaClients, getDbShard, processWalletTransaction } = require('../utils/shard');

// DB4 POOL = raw pg Pool
const db4 = profilePool;

// ========== HELPERS ==========
async function getProfile(userId) {
  const { rows } = await db4.query(
    `SELECT user_id, bio, face_hash, face_verified, id_hash, id_verified, id_verified_at,
            id_photo_temp, id_status, created_at, updated_at
     FROM profiles WHERE user_id=$1`,
    [userId]
  );
  return rows[0];
}

// ========== AUTH MIDDLEWARE ==========
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
  } catch (err) {
    res.status(500).json({ error: 'Auth error' });
  }
}

// ========== VERIFICATION MIDDLEWARE ==========
async function requireFaceVerified(req, res, next) {
  try {
    const { userId } = req.user;
    const profile = await getProfile(userId);
    if (!profile?.face_verified) {
      return res.status(403).json({ error: "Face verification required. Complete in Profile > Verify Face" });
    }
    next();
  } catch (err) {
    console.error('[requireFaceVerified Error]', err.message);
    res.status(500).json({ error: 'Verification check failed' });
  }
}

async function requireIdVerified(req, res, next) {
  try {
    const { userId } = req.user;
    const profile = await getProfile(userId);
    if (!profile?.id_verified) {
      return res.status(403).json({ error: "ID verification required for withdrawal" });
    }
    next();
  } catch (err) {
    console.error('[requireIdVerified Error]', err.message);
    res.status(500).json({ error: 'Verification check failed' });
  }
}

// ========== ROUTE 1: SAVE/EDIT BIO ==========
router.post('/bio', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { bio } = req.body;
    if (typeof bio !== 'string') return res.status(400).json({ error: 'Bio must be text' });
    const cleanBio = bio.slice(0, 150).trim();

    await db4.query(`
      INSERT INTO profiles(user_id, bio)
      VALUES($1,$2)
      ON CONFLICT (user_id) DO UPDATE SET bio=$2
    `, [userId, cleanBio]);

    res.json({ success: true, bio: cleanBio });
  } catch (err) {
    console.error('[Profile Bio Error]', err.message);
    res.status(500).json({ error: 'Failed to save bio' });
  }
});

// ========== ROUTE 2: DELETE BIO ==========
router.delete('/bio', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    await db4.query(`UPDATE profiles SET bio='' WHERE user_id=$1`, [userId]);
    res.json({ success: true, bio: "" });
  } catch (err) {
    console.error('[Delete Bio Error]', err.message);
    res.status(500).json({ error: 'Failed to clear bio' });
  }
});

// ========== ROUTE 3: FACE VERIFY ==========
router.post('/face-verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { faceHash } = req.body;

    if (!faceHash || faceHash.length !== 64) {
      return res.status(400).json({ error: 'Invalid faceHash' });
    }

    // Check duplicate in DB4
    const dup = await db4.query(`SELECT user_id FROM profiles WHERE face_hash=$1`, [faceHash]);
    if (dup.rows.length > 0 && dup.rows[0].user_id !== userId) {
      return res.status(400).json({ error: "This face is already registered to another account" });
    }

    // Save to db4
    await db4.query(`
      INSERT INTO profiles(user_id, face_hash, face_verified)
      VALUES($1,$2,true)
      ON CONFLICT (user_id) DO UPDATE SET face_hash=$2, face_verified=true
    `, [userId, faceHash]);

    // PAY REFERRAL BONUS IF PENDING
    try {
      const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
      let pendingRef = null;
      for (const db of dbs) {
        const ref = await db.referral.findFirst({ where: { refereeId: userId, status: 'PENDING' } }).catch(() => null);
        if (ref) { pendingRef = ref; break; }
      }
      if (pendingRef) {
        const referrerId = pendingRef.referrerId;
        const refDb = getDbShard(referrerId);
        const updated = await refDb.client.referral.updateMany({
          where: { id: pendingRef.id, status: 'PENDING' },
          data: { status: 'QUALIFIED' }
        });
        if (updated.count > 0) {
          await processWalletTransaction({ userId: referrerId, action: 'REFERRAL_BONUS', isCreator: false });
          console.log(`[Referral Paid] ${referrerId} got 1000 FREE pts for verifying ${userId}`);
        }
      }
    } catch (refErr) {
      console.error('[Referral Payout Error]', refErr.message);
    }

    res.json({ success: true, faceVerified: true, message: 'Face verified. Referral unlocked!' });
  } catch (err) {
    console.error('[Face Verify Error]', err.message);
    res.status(500).json({ error: 'Face verification failed' });
  }
});

// ========== ROUTE 4: ID UPLOAD - ADMIN APPROVAL FLOW ==========
router.post('/id-upload', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { idPhotoBase64 } = req.body;

    if (!idPhotoBase64 || !idPhotoBase64.startsWith('data:image')) {
      return res.status(400).json({ error: 'Invalid image data' });
    }
    if (idPhotoBase64.length > 2 * 1024 * 1024) {
      return res.status(400).json({ error: 'Image too large. Max 1.5MB' });
    }

    // Save to db4 as PENDING
    await db4.query(`
      INSERT INTO profiles(user_id, id_photo_temp, id_status)
      VALUES($1,$2,'PENDING')
      ON CONFLICT (user_id) DO UPDATE SET id_photo_temp=$2, id_status='PENDING'
    `, [userId, idPhotoBase64]);

    res.json({ success: true, message: 'ID submitted for review' });
  } catch (err) {
    console.error('[ID Upload Error]', err.message);
    res.status(500).json({ error: 'ID upload failed' });
  }
});

// ========== ROUTE 5: GET MY KYC STATUS ==========
router.get('/kyc-status', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const profile = await getProfile(userId);

    res.json({
      bio: profile?.bio || "",
      faceVerified: profile?.face_verified || false,
      idVerified: profile?.id_verified || false,
      idStatus: profile?.id_status || 'NONE',
      canRefer: profile?.face_verified || false,
      canWithdraw: (profile?.face_verified && profile?.id_verified) || false
    });
  } catch (err) {
    console.error('[Get KYC Error]', err.message);
    res.status(500).json({ error: 'Failed to load KYC' });
  }
});

// ========== ROUTE 6: GET PUBLIC PROFILE ==========
router.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const profile = await getProfile(userId);
    res.json({
      userId,
      bio: profile?.bio || "",
      isVerified: profile?.face_verified || false,
      joinedAt: profile?.created_at || null
    });
  } catch (err) {
    console.error('[Get Profile Error]', err.message);
    res.status(500).json({ error: 'Failed to load profile' });
  }
});

module.exports = { router, requireFaceVerified, requireIdVerified };

