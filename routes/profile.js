const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
profilePool.on('error', (err) => console.error('[ProfileDB Error]', err.message));

// Import these from server.js for referral payouts
const { prismaClients, getDbShard, processWalletTransaction } = require('../server');

// ========== HELPERS ==========
async function getProfile(userId) {
  const { rows } = await profilePool.query(
    `SELECT user_id, bio, face_verified, id_verified, created_at FROM profiles WHERE user_id=$1`,
    [userId]
  );
  return rows[0];
}

// ========== AUTH MIDDLEWARE ==========
function authenticateToken(req, res, next) {
  const JWT_SECRET = process.env.JWTSECRET || 'critical_fallback_shard_key_2026_prod';
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

    await profilePool.query(`
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

    await profilePool.query(`
      UPDATE profiles SET bio='' WHERE user_id=$1
    `, [userId]);

    res.json({ success: true, bio: "" });
  } catch (err) {
    console.error('[Delete Bio Error]', err.message);
    res.status(500).json({ error: 'Failed to clear bio' });
  }
});

// ========== ROUTE 3: FACE VERIFY - AUTO APPROVE + PAY REFERRAL ==========
router.post('/face-verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { faceHash } = req.body; // 64 char sha256 from face-api.js

    if (!faceHash || faceHash.length !== 64) {
      return res.status(400).json({ error: 'Invalid faceHash' });
    }

    // Check if this face is already used by another account
    const dup = await profilePool.query(
      `SELECT user_id FROM profiles WHERE face_hash=$1`,
      [faceHash]
    );
    if (dup.rows.length > 0 && dup.rows[0].user_id !== userId) {
      return res.status(400).json({ error: "This face is already registered to another account" });
    }

    // Save and auto-verify. Create row if doesn't exist
    await profilePool.query(`
      INSERT INTO profiles(user_id, face_hash, face_verified)
      VALUES($1,$2,true)
      ON CONFLICT (user_id) DO UPDATE SET face_hash=$2, face_verified=true
    `, [userId, faceHash]);

    // ========== PAY REFERRAL BONUS IF PENDING ==========
    try {
      // Find pending referral where this user is the referee
      const dbs = [prismaClients.db1, prismaClients.db2, prismaClients.db3];
      let pendingRef = null;

      for (const db of dbs) {
        const ref = await db.referral.findFirst({
          where: { refereeId: userId, status: 'PENDING' }
        }).catch(() => null);
        
        if (ref) { 
          pendingRef = ref; 
          break; 
        }
      }

      if (pendingRef) {
        const referrerId = pendingRef.referrerId;
        const refDb = getDbShard(referrerId);

        // 1. Mark referral as QUALIFIED
        await refDb.client.referral.update({
          where: { id: pendingRef.id },
          data: { status: 'QUALIFIED' }
        });

        // 2. Pay FREE points to referrer
        await processWalletTransaction({
          userId: referrerId,
          action: 'REFERRAL_BONUS',
          isCreator: false
        });

        console.log(`[Referral Paid] ${referrerId} got referral bonus for verifying ${userId}`);
      }
    } catch (refErr) {
      console.error('[Referral Payout Error]', refErr.message);
      // Don't fail face verify if referral processing encounters an error
    }
    // ========== END REFERRAL LOGIC ==========

    res.json({ success: true, faceVerified: true, message: 'Face verified. Referral unlocked!' });
  } catch (err) {
    console.error('[Face Verify Error]', err.message);
    res.status(500).json({ error: 'Face verification failed' });
  }
});

// ========== ROUTE 4: ID VERIFY - AUTO APPROVE ==========
router.post('/id-verify', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.user;
    const { idHash } = req.body; // sha256 of ID image

    if (!idHash || idHash.length !== 64) {
      return res.status(400).json({ error: 'Invalid idHash' });
    }

    // Check if this ID is already used
    const dup = await profilePool.query(
      `SELECT user_id FROM profiles WHERE id_hash=$1`,
      [idHash]
    );
    if (dup.rows.length > 0 && dup.rows[0].user_id !== userId) {
      return res.status(400).json({ error: "This ID is already registered to another account" });
    }

    // Save and auto-verify
    await profilePool.query(`
      INSERT INTO profiles(user_id, id_hash, id_verified, id_verified_at)
      VALUES($1,$2,true,NOW())
      ON CONFLICT (user_id) DO UPDATE SET id_hash=$2, id_verified=true, id_verified_at=NOW()
    `, [userId, idHash]);

    res.json({ success: true, idVerified: true, message: 'ID verified. Payout unlocked!' });
  } catch (err) {
    console.error('[ID Verify Error]', err.message);
    res.status(500).json({ error: 'ID verification failed' });
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
      canRefer: profile?.face_verified || false, // Must be verified to claim
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
      isVerified: profile?.face_verified || false, // blue tick = face
      joinedAt: profile?.created_at || null
    });
  } catch (err) {
    console.error('[Get Profile Error]', err.message);
    res.status(500).json({ error: 'Failed to load profile' });
  }
});

module.exports = { router, requireFaceVerified, requireIdVerified };
