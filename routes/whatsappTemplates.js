const express = require('express');
const router = express.Router();
const mongoose = require('mongoose');
const multer = require('multer');
const { WaTemplate } = require('../connectors/whatsapp/model');
const { syncTemplatesFromMeta } = require('../connectors/whatsapp/sync');
const { getWaConfig, metaGraphGet, graphPost, graphDelete, uploadHeaderMedia, sendTemplateMessage } = require('../connectors/whatsapp/graph');
const { validateTemplatePayload } = require('../connectors/whatsapp/templateValidator');

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 16 * 1024 * 1024 }, // 16MB max
});

// Middleware to ensure admin access
const requireAdmin = (req, res, next) => {
  if (req.user?.role !== 'admin') {
    return res.status(403).json({ success: false, message: 'Admin access required' });
  }
  next();
};

/**
 * GET /whatsapp/templates/approved
 * Read-only list of APPROVED templates for the Bulk WhatsApp page, open to any signed-in
 * non-HR user (counsellors send campaigns too). Only the fields needed to pick + render a template.
 */
router.get('/approved', async (req, res) => {
  try {
    if (req.user?.role === 'hr') {
      return res.status(403).json({ success: false, message: 'Not allowed' });
    }
    const templates = await WaTemplate.find({ tenantId: 'default', status: 'APPROVED' })
      .select('name language category parameterFormat components')
      .sort({ name: 1 })
      .lean();
    return res.json({ success: true, templates });
  } catch (err) {
    console.error('GET /whatsapp/templates/approved error:', err);
    return res.status(500).json({ success: false, message: err.message || 'Failed to fetch approved templates' });
  }
});

/**
 * GET /whatsapp/templates
 * List templates from MongoDB with search, filters, pagination, and status breakdown counts
 */
router.get('/', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.query.tenantId || 'default';
    const {
      search = '',
      status = '',
      category = '',
      language = '',
      page = 1,
      limit = 50,
      sortBy = 'updatedAt',
      sortOrder = 'asc',
    } = req.query;

    const query = { tenantId };

    if (search && search.trim()) {
      const escaped = search.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      query.name = new RegExp(escaped, 'i');
    }

    if (status && status !== 'all') {
      const statuses = status.split(',').map((s) => s.trim().toUpperCase());
      query.status = { $in: statuses };
    } else {
      query.status = { $ne: 'DELETED' };
    }

    if (category && category !== 'all') {
      query.category = category.toUpperCase();
    }

    if (language && language !== 'all') {
      query.language = language;
    }

    const pageNum = Math.max(1, parseInt(page, 10) || 1);
    const limitNum = Math.min(200, Math.max(1, parseInt(limit, 10) || 50));
    const skip = (pageNum - 1) * limitNum;

    const sortDir = sortOrder === 'asc' ? 1 : -1;
    let sortOption = {};
    if (sortBy === 'name') {
      sortOption = { name: sortDir, _id: sortDir };
    } else if (sortBy === 'category') {
      sortOption = { category: sortDir, _id: sortDir };
    } else if (sortBy === 'status') {
      sortOption = { status: sortDir, _id: sortDir };
    } else if (sortBy === 'createdAt') {
      sortOption = { createdAt: sortDir, _id: sortDir };
    } else {
      // Default: newest / latest first (descending by _id and updatedAt)
      sortOption = { updatedAt: sortDir, _id: sortDir };
    }

    const [templates, total, statsAgg] = await Promise.all([
      WaTemplate.find(query)
        .sort(sortOption)
        .skip(skip)
        .limit(limitNum)
        .lean(),
      WaTemplate.countDocuments(query),
      WaTemplate.aggregate([
        { $match: { tenantId } },
        {
          $group: {
            _id: null,
            total: { $sum: 1 },
            approved: { $sum: { $cond: [{ $eq: ['$status', 'APPROVED'] }, 1, 0] } },
            pending: { $sum: { $cond: [{ $eq: ['$status', 'PENDING'] }, 1, 0] } },
            rejected: { $sum: { $cond: [{ $eq: ['$status', 'REJECTED'] }, 1, 0] } },
            paused: { $sum: { $cond: [{ $eq: ['$status', 'PAUSED'] }, 1, 0] } },
            disabled: { $sum: { $cond: [{ $eq: ['$status', 'DISABLED'] }, 1, 0] } },
          },
        },
      ]),
    ]);

    const stats = statsAgg[0] || {
      total: 0,
      approved: 0,
      pending: 0,
      rejected: 0,
      paused: 0,
      disabled: 0,
    };

    return res.json({
      success: true,
      templates,
      total,
      page: pageNum,
      limit: limitNum,
      totalPages: Math.ceil(total / limitNum) || 1,
      stats,
    });
  } catch (err) {
    console.error('GET /whatsapp/templates error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to fetch WhatsApp templates',
    });
  }
});

/**
 * POST /whatsapp/templates/sync
 * Sync all message templates from Meta Graph API into MongoDB
 */
router.post('/sync', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    const result = await syncTemplatesFromMeta(tenantId);
    return res.json({
      success: true,
      message: `Sync completed: ${result.added} added, ${result.updated} updated, ${result.deleted || 0} removed (${result.totalMeta} active in Meta)`,
      data: result,
    });
  } catch (err) {
    console.error('POST /whatsapp/templates/sync error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to sync templates from Meta',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * POST /whatsapp/templates
 * Create a new message template, validate according to Meta rules, submit to Meta for review,
 * and persist in MongoDB with initial status PENDING.
 */
router.post('/', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    const config = getWaConfig(tenantId);
    const {
      name,
      language = 'en_US',
      category = 'UTILITY',
      parameterFormat = 'POSITIONAL',
      components = [],
    } = req.body || {};

    // 1. Run strict pre-flight validation
    const validation = validateTemplatePayload({
      name,
      language,
      category,
      parameterFormat,
      components,
    });

    if (!validation.isValid) {
      return res.status(400).json({
        success: false,
        message: validation.errors[0],
        errors: validation.errors,
      });
    }

    const cleanName = name.trim().toLowerCase();
    const cleanCategory = category.trim().toUpperCase();

    // Check if template with same name and language already exists locally
    const existing = await WaTemplate.findOne({
      tenantId,
      name: cleanName,
      language,
    });

    if (existing && existing.status !== 'DELETED') {
      return res.status(409).json({
        success: false,
        message: `A template with name "${cleanName}" and language "${language}" already exists (status: ${existing.status}).`,
      });
    }

    // 2. Prepare payload for Meta Graph API
    const metaPayload = {
      name: cleanName,
      language,
      category: cleanCategory,
      parameter_format: parameterFormat,
      components,
    };

    // 3. Submit directly to Meta WABA
    const metaRes = await graphPost(`${config.wabaId}/message_templates`, metaPayload, tenantId);

    const metaId = metaRes.id;
    const initialStatus = (metaRes.status || 'PENDING').toUpperCase();
    const now = new Date();

    // 4. Save in MongoDB
    let savedTemplate;
    if (existing) {
      existing.metaId = metaId;
      existing.category = cleanCategory;
      existing.parameterFormat = parameterFormat;
      existing.components = components;
      existing.status = initialStatus;
      existing.rejectedReason = null;
      existing.source = 'crm';
      existing.createdBy = req.user?.username || 'admin';
      existing.lastSyncedAt = now;
      existing.statusHistory.push({
        status: initialStatus,
        reason: 'Submitted via CRM Template Builder',
        at: now,
      });
      savedTemplate = await existing.save();
    } else {
      savedTemplate = await WaTemplate.create({
        tenantId,
        metaId,
        name: cleanName,
        language,
        category: cleanCategory,
        parameterFormat,
        components,
        status: initialStatus,
        qualityScore: 'UNKNOWN',
        rejectedReason: null,
        source: 'crm',
        createdBy: req.user?.username || 'admin',
        statusHistory: [
          {
            status: initialStatus,
            reason: 'Submitted via CRM Template Builder',
            at: now,
          },
        ],
        lastSyncedAt: now,
      });
    }

    return res.status(201).json({
      success: true,
      message: `Template "${cleanName}" submitted to Meta for review (status: ${initialStatus})`,
      template: savedTemplate,
      metaResponse: metaRes,
    });
  } catch (err) {
    console.error('POST /whatsapp/templates error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to submit template to Meta',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * POST /whatsapp/templates/media/upload
 * Upload a sample media file (image/video/doc) to obtain a Meta resumable upload handle
 */
router.post('/media/upload', requireAdmin, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: 'No file provided' });
    }

    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    const result = await uploadHeaderMedia(
      req.file.buffer,
      req.file.mimetype,
      req.file.originalname,
      tenantId
    );

    return res.json({
      success: true,
      handle: result.handle,
      fileName: result.fileName,
      mimeType: result.mimeType,
      size: result.size,
    });
  } catch (err) {
    console.error('POST /whatsapp/templates/media/upload error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to upload media to Meta',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * PUT /whatsapp/templates/:id
 * Edit and resubmit an existing template to Meta.
 * Allowed when status is APPROVED, REJECTED, or PAUSED.
 */
router.put('/:id', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    const template = await WaTemplate.findById(req.params.id);
    if (!template) {
      return res.status(404).json({ success: false, message: 'Template not found' });
    }

    if (template.status === 'PENDING') {
      return res.status(400).json({
        success: false,
        message: 'Template is currently under review by Meta and cannot be edited until reviewed.',
      });
    }

    const {
      category = template.category,
      components = template.components,
    } = req.body || {};

    // Validate updated components
    const validation = validateTemplatePayload({
      name: template.name,
      language: template.language,
      category,
      components,
    });

    if (!validation.isValid) {
      return res.status(400).json({
        success: false,
        message: validation.errors[0],
        errors: validation.errors,
      });
    }

    // Call Meta to update template (POST /{TEMPLATE_ID})
    let metaRes = null;
    if (template.metaId) {
      metaRes = await graphPost(
        template.metaId,
        {
          category: category.toUpperCase(),
          components,
        },
        tenantId
      );
    }

    const now = new Date();
    template.category = category.toUpperCase();
    template.components = components;
    template.status = 'PENDING'; // Re-enters Meta review
    template.rejectedReason = null;
    template.lastSyncedAt = now;
    template.statusHistory.push({
      status: 'PENDING',
      reason: 'Edited & Resubmitted via CRM',
      at: now,
    });

    await template.save();

    return res.json({
      success: true,
      message: `Template "${template.name}" updated and resubmitted for Meta review.`,
      template,
      metaResponse: metaRes,
    });
  } catch (err) {
    console.error('PUT /whatsapp/templates/:id error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to update template',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * DELETE /whatsapp/templates/:id
 * Delete template from Meta WABA and MongoDB
 */
router.delete('/:id', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.query.tenantId || 'default';
    const config = getWaConfig(tenantId);
    let template = null;
    if (mongoose.Types.ObjectId.isValid(req.params.id)) {
      template = await WaTemplate.findById(req.params.id);
    }
    if (!template) {
      template = await WaTemplate.findOne({
        tenantId,
        $or: [{ metaId: req.params.id }, { name: req.params.id }],
      });
    }

    if (!template) {
      // If not in MongoDB, try deleting from Meta anyway just in case
      try {
        const deleteUrl = `${config.wabaId}/message_templates?name=${encodeURIComponent(req.params.id)}`;
        await graphDelete(deleteUrl, tenantId);
      } catch (e) {
        // ignore
      }
      return res.json({ success: true, message: 'Template removed.' });
    }

    // Call Meta to delete template from Meta WhatsApp Manager
    let metaDeleted = false;
    let metaErrorMsg = null;

    const isAlreadyGone = (errMsg) => {
      const msg = (errMsg || '').toLowerCase();
      return msg.includes('not found') || msg.includes('does not exist') || msg.includes('unknown object');
    };

    // Strategy 1: Delete by name and specific hsm_id
    if (template.name && template.metaId) {
      try {
        const deleteUrl = `${config.wabaId}/message_templates?name=${encodeURIComponent(template.name)}&hsm_id=${encodeURIComponent(template.metaId)}`;
        const metaRes = await graphDelete(deleteUrl, tenantId);
        if (metaRes?.success !== false) metaDeleted = true;
      } catch (err) {
        if (isAlreadyGone(err.message)) {
          metaDeleted = true;
        } else {
          metaErrorMsg = err.message;
          console.warn(`Meta delete by name & hsm_id notice (${err.message}), falling back to delete by name...`);
        }
      }
    }

    // Strategy 2: Delete by template name (official standard for Meta WABA)
    if (!metaDeleted && template.name) {
      try {
        const deleteUrl = `${config.wabaId}/message_templates?name=${encodeURIComponent(template.name)}`;
        const metaRes = await graphDelete(deleteUrl, tenantId);
        if (metaRes?.success !== false) metaDeleted = true;
      } catch (err) {
        if (isAlreadyGone(err.message)) {
          metaDeleted = true;
        } else {
          metaErrorMsg = err.message;
          console.warn(`Meta delete by name notice: ${err.message}`);
        }
      }
    }

    // If Meta refused to delete, stop and return the exact Meta error
    if (!metaDeleted) {
      console.error(`Meta deletion blocked for "${template.name}":`, metaErrorMsg);
      return res.status(400).json({
        success: false,
        message: `Meta refused to delete template "${template.name}": ${metaErrorMsg || 'Permission denied by Meta'}.`,
        metaError: metaErrorMsg,
      });
    }

    // Only remove from local MongoDB when Meta has confirmed deletion (or already gone)
    await WaTemplate.findByIdAndDelete(template._id);

    // Broadcast real-time deletion event
    const io = req.app?.get('io');
    if (io) {
      io.emit('wa_template_deleted', {
        templateId: String(template._id),
        metaId: template.metaId,
        name: template.name,
      });
    }

    return res.json({
      success: true,
      message: `Template "${template.name}" deleted successfully from both Meta WhatsApp Manager and CRM.`,
      metaDeleted: true,
    });
  } catch (err) {
    console.error('DELETE /whatsapp/templates/:id error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to delete template',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * POST /whatsapp/templates/:id/duplicate
 * Duplicate template into a new draft
 */
router.post('/:id/duplicate', requireAdmin, async (req, res) => {
  try {
    const template = await WaTemplate.findById(req.params.id);
    if (!template) {
      return res.status(404).json({ success: false, message: 'Template not found' });
    }

    const copyName = (req.body?.name || `${template.name}_copy`).toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');

    return res.json({
      success: true,
      draft: {
        name: copyName,
        language: template.language,
        category: template.category,
        parameterFormat: template.parameterFormat || 'POSITIONAL',
        components: template.components,
      },
    });
  } catch (err) {
    console.error('POST /whatsapp/templates/:id/duplicate error:', err);
    return res.status(500).json({ success: false, message: err.message });
  }
});

/**
 * POST /whatsapp/templates/:id/test
 * Send a live test message to an admin/test phone number using the template
 */
router.post('/:id/test', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.body?.tenantId || req.query.tenantId || 'default';
    const template = await WaTemplate.findById(req.params.id);
    if (!template) {
      return res.status(404).json({ success: false, message: 'Template not found' });
    }

    const { to, parameters = {} } = req.body || {};
    if (!to || !String(to).trim()) {
      return res.status(400).json({ success: false, message: 'Recipient phone number is required (with country code)' });
    }

    // Prepare components with parameters if provided
    const testComponents = [];

    // Body variables
    const bodyValues = Array.isArray(parameters.body) ? parameters.body : [];
    if (bodyValues.length > 0) {
      testComponents.push({
        type: 'body',
        parameters: bodyValues.map(val => ({
          type: 'text',
          text: String(val || '').trim() || 'Sample',
        })),
      });
    } else {
      // Fallback to example values stored in template
      const bodyComp = template.components?.find(c => c.type === 'BODY');
      const sampleValues = bodyComp?.example?.body_text?.[0] || [];
      if (sampleValues.length > 0) {
        testComponents.push({
          type: 'body',
          parameters: sampleValues.map(val => ({
            type: 'text',
            text: String(val || '').trim() || 'Sample',
          })),
        });
      }
    }

    // Header variable / media
    if (parameters.header) {
      testComponents.push({
        type: 'header',
        parameters: [
          {
            type: 'text',
            text: String(parameters.header).trim(),
          },
        ],
      });
    }

    const result = await sendTemplateMessage({
      to,
      templateName: template.name,
      language: template.language,
      components: testComponents,
      tenantId,
    });

    return res.json({
      success: true,
      message: `Test template sent successfully to ${to}`,
      metaResponse: result,
    });
  } catch (err) {
    console.error('POST /whatsapp/templates/:id/test error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to send test message',
      metaError: err.metaError || undefined,
    });
  }
});

/**
 * GET /whatsapp/templates/:id/analytics
 * Fetches granular daily performance metrics, cost, delivery, reads, and errors from Meta Graph API for a single template
 */
router.get('/:id/analytics', requireAdmin, async (req, res) => {
  try {
    const tenantId = req.query.tenantId || 'default';
    const config = getWaConfig(tenantId);

    // 1. Resolve template by MongoDB ObjectId, metaId, or name
    let template = null;
    if (mongoose.Types.ObjectId.isValid(req.params.id)) {
      template = await WaTemplate.findById(req.params.id);
    }
    if (!template) {
      template = await WaTemplate.findOne({
        tenantId,
        $or: [{ metaId: req.params.id }, { name: req.params.id }],
      });
    }

    // 2. If template is still missing or has no metaId, query Meta's message_templates directly
    if (!template || !template.metaId) {
      try {
        const metaTemplatesRes = await metaGraphGet(
          `${config.wabaId}/message_templates?fields=id,name,language,category,status,components&limit=100`,
          tenantId
        );
        const allMeta = metaTemplatesRes?.data || [];
        const match = allMeta.find(
          t => t.id === req.params.id ||
               (template && t.name === template.name) ||
               t.name === req.params.id
        );

        if (match) {
          if (!template) {
            template = await WaTemplate.findOneAndUpdate(
              { tenantId, name: match.name, language: match.language },
              {
                tenantId,
                metaId: match.id,
                name: match.name,
                language: match.language,
                category: (match.category || 'UTILITY').toUpperCase(),
                status: (match.status || 'APPROVED').toUpperCase(),
                components: match.components || [],
                lastSyncedAt: new Date(),
              },
              { upsert: true, new: true }
            );
          } else {
            template.metaId = match.id;
            if (!template.components || template.components.length === 0) {
              template.components = match.components || [];
            }
            await template.save();
          }
        }
      } catch (syncErr) {
        console.warn('Could not auto-resolve template from Meta:', syncErr.message);
      }
    }

    if (!template) {
      return res.status(404).json({ success: false, message: 'Template not found' });
    }

    // 3. Proven date range calculation (UTC day start, end includes full endDate up to now)
    const dayStartTs = (dateStr) => Math.floor(new Date(`${dateStr}T00:00:00Z`).getTime() / 1000);
    const isDateStr = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s || '');

    const todayStr = new Date().toISOString().split('T')[0];
    const sevenDaysAgoStr = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];

    const sDate = isDateStr(req.query.startDate) ? req.query.startDate : sevenDaysAgoStr;
    const eDate = isDateStr(req.query.endDate) ? req.query.endDate : todayStr;

    const start = dayStartTs(sDate);
    // end is inclusive of the full endDate (adds 86400 seconds), capped at Date.now() / 1000
    const end = Math.min(dayStartTs(eDate) + 86400, Math.floor(Date.now() / 1000));
    const minStart = Math.floor(Date.now() / 1000) - 89 * 86400;
    const aStart = Math.max(start, minStart);

    let analyticsData = {
      amountSpent: 0,
      costPerDelivered: 0,
      sent: 0,
      delivered: 0,
      read: 0,
      replied: 0,
      undelivered: 0,
      dailyPoints: [],
      currency: 'INR',
    };
    let analyticsError = null;

    // 4. Query Meta template_analytics with fallback parameter shapes
    const targetMetaId = template.metaId || (req.params.id !== String(template._id) ? req.params.id : null);

    if (targetMetaId) {
      let metaRes = null;

      // Attempt 1: Standard template_analytics with metric_types
      try {
        metaRes = await metaGraphGet(
          `${config.wabaId}/template_analytics?start=${aStart}&end=${end}&granularity=daily&metric_types=cost,delivered,read,sent&template_ids=[${targetMetaId}]`,
          tenantId
        );
      } catch (err1) {
        // Attempt 2: Without metric_types, uppercase DAILY
        try {
          metaRes = await metaGraphGet(
            `${config.wabaId}/template_analytics?start=${aStart}&end=${end}&granularity=DAILY&template_ids=[${targetMetaId}]`,
            tenantId
          );
        } catch (err2) {
          // Attempt 3: URL encoded JSON array
          try {
            metaRes = await metaGraphGet(
              `${config.wabaId}/template_analytics?start=${aStart}&end=${end}&granularity=DAILY&template_ids=${encodeURIComponent(JSON.stringify([String(targetMetaId)]))}`,
              tenantId
            );
          } catch (err3) {
            console.warn('Meta template_analytics fetch warning:', err3.message || err2.message || err1.message);
            analyticsError = err3.message || err2.message || err1.message;
          }
        }
      }

      if (metaRes) {
        const rawItems = Array.isArray(metaRes.data) ? metaRes.data : [metaRes];
        const rawPoints = [];
        for (const item of rawItems) {
          if (Array.isArray(item.data_points)) {
            rawPoints.push(...item.data_points);
          }
        }
        if (rawPoints.length === 0 && Array.isArray(metaRes.data_points)) {
          rawPoints.push(...metaRes.data_points);
        }

        const pointsMap = new Map();
        let totalSent = 0;
        let totalDelivered = 0;
        let totalRead = 0;
        let totalReplied = 0;
        let totalCost = 0;
        let metaCostPerDelivered = null;

        for (const p of rawPoints) {
          const pSent = Number(p.sent) || 0;
          const pDelivered = Number(p.delivered) || 0;
          const pRead = Number(p.read) || 0;
          const pReplied = Number(p.unique_replies || p.replied || p.replies) || 0;

          let pCost = 0;
          if (Array.isArray(p.cost)) {
            for (const c of p.cost) {
              if (c.type === 'amount_spent' || !c.type) {
                pCost += Number(c.value || c.amount || 0);
              } else if (c.type === 'cost_per_delivered' || c.type === 'cost_per_message_delivered') {
                metaCostPerDelivered = Number(c.value) || metaCostPerDelivered;
              }
            }
          } else if (typeof p.cost === 'number') {
            pCost = p.cost;
          } else if (typeof p.cost === 'string') {
            pCost = Number(p.cost) || 0;
          }

          totalSent += pSent;
          totalDelivered += pDelivered;
          totalRead += pRead;
          totalReplied += pReplied;
          totalCost += pCost;

          const dateObj = new Date(p.start * 1000);
          const dateKey = dateObj.toISOString().split('T')[0];
          const dateStr = dateObj.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });

          const existing = pointsMap.get(dateKey) || {
            date: dateStr,
            dateKey,
            timestamp: p.start * 1000,
            sent: 0,
            delivered: 0,
            read: 0,
            cost: 0,
            replied: 0,
            undelivered: 0,
          };

          existing.sent += pSent;
          existing.delivered += pDelivered;
          existing.read += pRead;
          existing.replied += pReplied;
          existing.cost = Number((existing.cost + pCost).toFixed(2));
          existing.undelivered = Math.max(0, existing.sent - existing.delivered);
          pointsMap.set(dateKey, existing);
        }

        // Build continuous daily sequence for chart so all days in the date range are represented
        const sortedPoints = [];
        const startDay = new Date(`${sDate}T00:00:00Z`);
        const endDay = new Date(`${eDate}T00:00:00Z`);

        for (let d = new Date(startDay); d <= endDay; d.setUTCDate(d.getUTCDate() + 1)) {
          const dateKey = d.toISOString().split('T')[0];
          const dateStr = d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
          if (pointsMap.has(dateKey)) {
            sortedPoints.push(pointsMap.get(dateKey));
          } else {
            sortedPoints.push({
              date: dateStr,
              dateKey,
              timestamp: d.getTime(),
              sent: 0,
              delivered: 0,
              read: 0,
              cost: 0,
              replied: 0,
              undelivered: 0,
            });
          }
        }

        const costPerDeliveredFinal =
          metaCostPerDelivered !== null && metaCostPerDelivered !== undefined
            ? metaCostPerDelivered
            : totalDelivered > 0
            ? Number((totalCost / totalDelivered).toFixed(2))
            : 0;

        analyticsData = {
          amountSpent: Number(totalCost.toFixed(2)),
          costPerDelivered: Number(costPerDeliveredFinal.toFixed(2)),
          sent: totalSent,
          delivered: totalDelivered,
          read: totalRead,
          replied: totalReplied,
          undelivered: Math.max(0, totalSent - totalDelivered),
          dailyPoints: sortedPoints,
          currency: 'INR',
        };
      }
    }

    // 5. If dailyPoints is still empty (no metaId or no response), fill with 0 placeholders
    if (analyticsData.dailyPoints.length === 0) {
      const generated = [];
      const startDay = new Date(`${sDate}T00:00:00Z`);
      const endDay = new Date(`${eDate}T00:00:00Z`);
      for (let d = new Date(startDay); d <= endDay; d.setUTCDate(d.getUTCDate() + 1)) {
        generated.push({
          date: d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }),
          dateKey: d.toISOString().split('T')[0],
          timestamp: d.getTime(),
          sent: 0,
          delivered: 0,
          read: 0,
          cost: 0,
          replied: 0,
          undelivered: 0,
        });
      }
      analyticsData.dailyPoints = generated;
    }

    return res.json({
      success: true,
      template,
      dateRange: {
        startDate: sDate,
        endDate: eDate,
      },
      analytics: analyticsData,
      analyticsError: analyticsError || undefined,
    });
  } catch (err) {
    console.error('GET /whatsapp/templates/:id/analytics error:', err);
    return res.status(err.status || 500).json({
      success: false,
      message: err.message || 'Failed to fetch template analytics',
    });
  }
});

module.exports = router;
