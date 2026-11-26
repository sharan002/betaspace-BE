const crypto = require('crypto');
const express = require('express');
const router = express.Router();
const { WaTemplate } = require('../connectors/whatsapp/model');

const safeEqual = (a, b) => {
  const x = Buffer.from(String(a || ''));
  const y = Buffer.from(String(b || ''));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};

/**
 * Shared-secret check for the (unauthenticated-by-JWT) events endpoint.
 * The endpoint is DISABLED until WA_EVENTS_SECRET is set in .env — there is no default secret.
 */
const verifySecret = (req, res, next) => {
  const expectedSecret = (process.env.WA_EVENTS_SECRET || '').trim();
  if (!expectedSecret) {
    return res.status(503).json({ success: false, message: 'WhatsApp events endpoint disabled: set WA_EVENTS_SECRET in .env' });
  }

  const providedSecret =
    req.headers['x-events-secret'] ||
    req.headers['authorization']?.replace(/^Bearer\s+/i, '') ||
    req.query.secret;

  if (!safeEqual(providedSecret, expectedSecret)) {
    return res.status(401).json({ success: false, message: 'Invalid or missing events secret' });
  }
  next();
};

/**
 * GET /internal/whatsapp/events
 * Webhook verification handshake (if Meta calls this endpoint directly)
 */
router.get('/', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  const expectedToken = (process.env.WA_VERIFY_TOKEN || process.env.WA_EVENTS_SECRET || '').trim();

  if (expectedToken && mode === 'subscribe' && safeEqual(token, expectedToken)) {
    console.log('✅ WhatsApp Webhook verified directly with Meta');
    return res.status(200).send(challenge);
  }
  return res.status(403).json({ success: false, message: 'Verification token mismatch' });
});

/**
 * POST /internal/whatsapp/events
 * Ingest WhatsApp Webhook events (from Meta or forwarded from wpchat-api)
 */
router.post('/', verifySecret, async (req, res) => {
  try {
    const io = req.app.get('io');
    const body = req.body || {};
    const now = new Date();

    // Collect all event objects, supporting both raw Meta entry[] and forwarded flat events
    const eventsToProcess = [];

    if (Array.isArray(body.entry)) {
      // Raw Meta Webhook format
      for (const entry of body.entry) {
        for (const change of entry.changes || []) {
          eventsToProcess.push({
            field: change.field,
            value: change.value,
          });
        }
      }
    } else if (body.field) {
      // Forwarded from wpchat-api
      eventsToProcess.push({
        field: body.field,
        value: body.value || body,
      });
    }

    console.log(`📩 Processing ${eventsToProcess.length} WhatsApp event(s)...`);

    for (const item of eventsToProcess) {
      const field = item.field;
      const val = item.value || {};

      // 1. Template Status Updates (APPROVED, REJECTED, PAUSED, DISABLED, IN_APPEAL)
      if (field === 'message_template_status_update') {
        const metaId = val.message_template_id || val.templateId;
        const name = (val.message_template_name || val.name || '').trim().toLowerCase();
        const language = val.message_template_language || val.language || 'en_US';
        const rawStatus = (val.event || val.status || 'PENDING').toUpperCase();
        const reason = val.reason || val.rejected_reason || null;

        // Map Meta status event
        let normalizedStatus = rawStatus;
        if (rawStatus === 'REJECTED') normalizedStatus = 'REJECTED';
        else if (rawStatus === 'APPROVED') normalizedStatus = 'APPROVED';
        else if (rawStatus === 'PAUSED') normalizedStatus = 'PAUSED';
        else if (rawStatus === 'DISABLED') normalizedStatus = 'DISABLED';
        else if (rawStatus === 'DELETED') normalizedStatus = 'DELETED';

        // If template was deleted in Meta, delete it immediately from CRM
        if (normalizedStatus === 'DELETED' || val.event === 'DELETED') {
          const deleted = await WaTemplate.findOneAndDelete({
            $or: [
              { name, language },
              ...(metaId ? [{ metaId: String(metaId) }] : []),
            ],
          });
          if (deleted) {
            console.log(`🗑️ Template "${deleted.name}" deleted via Meta webhook event`);
            if (io) {
              io.emit('wa_template_deleted', {
                templateId: String(deleted._id),
                metaId: deleted.metaId,
                name: deleted.name,
              });
            }
          }
          continue;
        }

        const template = await WaTemplate.findOne({
          $or: [
            { name, language },
            ...(metaId ? [{ metaId: String(metaId) }] : []),
          ],
        });

        if (template) {
          template.status = normalizedStatus;
          if (reason) template.rejectedReason = reason;
          template.lastSyncedAt = now;
          template.statusHistory.push({
            status: normalizedStatus,
            reason,
            at: now,
          });
          await template.save();

          console.log(`🔔 Template "${template.name}" status updated to ${normalizedStatus} (Reason: ${reason || 'None'})`);

          // Broadcast via Socket.IO
          if (io) {
            io.emit('wa_template_update', {
              templateId: template._id,
              metaId: template.metaId,
              name: template.name,
              language: template.language,
              status: normalizedStatus,
              rejectedReason: reason,
              qualityScore: template.qualityScore,
              updatedAt: now,
            });
          }
        } else {
          console.log(`⚠️ Template update received for unknown template: ${name} (${metaId})`);
        }
      }

      // 2. Template Quality Score Updates
      else if (field === 'message_template_quality_update') {
        const metaId = val.message_template_id || val.templateId;
        const name = (val.message_template_name || val.name || '').trim().toLowerCase();
        const qualityScore = (val.new_quality_score || val.quality_score || 'UNKNOWN').toUpperCase();

        const template = await WaTemplate.findOne({
          $or: [
            { name },
            ...(metaId ? [{ metaId: String(metaId) }] : []),
          ],
        });

        if (template) {
          template.qualityScore = qualityScore;
          template.lastSyncedAt = now;
          await template.save();

          if (io) {
            io.emit('wa_template_quality_update', {
              templateId: template._id,
              name: template.name,
              qualityScore,
              updatedAt: now,
            });
          }
        }
      }

      // 3. Phone Number Quality Updates
      else if (field === 'phone_number_quality_update') {
        if (io) {
          io.emit('wa_phone_quality_update', {
            phoneNumberId: val.display_phone_number || val.phone_number_id,
            currentLimit: val.current_limit,
            qualityScore: val.new_quality_score,
            event: val.event,
          });
        }
      }

      // 4. Account Updates
      else if (field === 'account_update') {
        if (io) {
          io.emit('wa_account_update', val);
        }
      }
    }

    return res.json({ success: true, processed: eventsToProcess.length });
  } catch (err) {
    console.error('Error handling WhatsApp internal event:', err);
    return res.status(500).json({ success: false, message: err.message });
  }
});

module.exports = router;
