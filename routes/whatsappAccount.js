const express = require('express');
const router = express.Router();
const { getWaConfig, metaGraphGet, graphPost, getWaPhoneNumberId } = require('../connectors/whatsapp/graph');

// Middleware to ensure admin access
const requireAdmin = (req, res, next) => {
  if (req.user?.role !== 'admin') {
    return res.status(403).json({ success: false, message: 'Admin access required' });
  }
  next();
};

/**
 * GET /whatsapp/account
 * Returns WABA account details, namespace, and all registered phone numbers with quality & limits
 */
router.get('/account', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.query.tenantId || 'default';
    const config = getWaConfig(tenantId);

    // Fetch WABA account info
    const wabaInfo = await metaGraphGet(
      `${config.wabaId}?fields=id,name,currency,timezone_id,message_template_namespace`,
      tenantId
    ).catch(err => {
      console.warn('WABA info fetch warning:', err.message);
      return { id: config.wabaId, name: 'WhatsApp Business Account' };
    });

    // Fetch registered phone numbers
    const phoneData = await metaGraphGet(
      `${config.wabaId}/phone_numbers?fields=id,display_phone_number,verified_name,quality_rating,messaging_limit_tier,status,name_status,code_verification_status,throughput`,
      tenantId
    ).catch(err => {
      console.warn('Phone numbers fetch warning:', err.message);
      return { data: [] };
    });

    return res.json({
      success: true,
      account: {
        id: wabaInfo.id,
        name: wabaInfo.name,
        currency: wabaInfo.currency || 'USD',
        timezoneId: wabaInfo.timezone_id || 'UTC',
        namespace: wabaInfo.message_template_namespace || null,
        phoneNumbers: phoneData.data || [],
      },
    });
  } catch (err) {
    console.error('GET /whatsapp/account error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to fetch WhatsApp account details',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * GET /whatsapp/profile
 * Returns the public WhatsApp business profile for a phone number
 */
router.get('/profile', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.query.tenantId || 'default';
    let phoneNumberId = req.query.phoneNumberId;

    if (!phoneNumberId) {
      phoneNumberId = await getWaPhoneNumberId(tenantId);
    }

    const profileData = await metaGraphGet(
      `${phoneNumberId}/whatsapp_business_profile?fields=about,address,description,email,profile_picture_url,websites,vertical`,
      tenantId
    );

    const profile = (profileData.data && profileData.data[0]) || profileData || {};

    return res.json({
      success: true,
      phoneNumberId,
      profile,
    });
  } catch (err) {
    console.error('GET /whatsapp/profile error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to fetch WhatsApp business profile',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * POST /whatsapp/profile or PUT /whatsapp/profile
 * Updates the WhatsApp business profile (about, address, description, email, websites, vertical)
 */
const updateProfileHandler = async (req, res) => {
  try {
    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    let phoneNumberId = req.body?.phoneNumberId || req.query.phoneNumberId;

    if (!phoneNumberId) {
      phoneNumberId = await getWaPhoneNumberId(tenantId);
    }

    const { about, address, description, email, websites, vertical } = req.body;

    const payload = {
      messaging_product: 'whatsapp',
    };

    if (about !== undefined) payload.about = String(about).trim().slice(0, 139);
    if (address !== undefined) payload.address = String(address).trim().slice(0, 256);
    if (description !== undefined) payload.description = String(description).trim().slice(0, 512);
    if (email !== undefined) payload.email = String(email).trim().slice(0, 128);
    if (vertical !== undefined) payload.vertical = String(vertical).trim();

    if (websites !== undefined) {
      if (Array.isArray(websites)) {
        payload.websites = websites.map(w => String(w).trim()).filter(Boolean).slice(0, 2);
      } else if (typeof websites === 'string' && websites.trim()) {
        payload.websites = [websites.trim()];
      } else {
        payload.websites = [];
      }
    }

    const result = await graphPost(
      `${phoneNumberId}/whatsapp_business_profile`,
      payload,
      tenantId
    );

    return res.json({
      success: true,
      message: 'WhatsApp business profile updated successfully on Meta',
      metaResponse: result,
    });
  } catch (err) {
    console.error('Update /whatsapp/profile error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to update WhatsApp business profile',
      metaError: err.metaError || undefined,
    });
  }
};

router.post('/profile', requireAdmin, updateProfileHandler);
router.put('/profile', requireAdmin, updateProfileHandler);

module.exports = router;
