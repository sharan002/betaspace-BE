const { getWaConfig, metaGraphGet } = require('./graph');
const { WaTemplate } = require('./model');

/**
 * Fetch all message templates from Meta Graph API and sync into MongoDB.
 */
async function syncTemplatesFromMeta(tenantId = 'default') {
  const config = getWaConfig(tenantId);
  const wabaId = config.wabaId;

  const fields = [
    'id',
    'name',
    'language',
    'category',
    'status',
    'components',
    'quality_score',
    'rejected_reason',
    'parameter_format',
  ].join(',');

  let nextUrl = `${wabaId}/message_templates?fields=${fields}&limit=100`;
  const metaTemplates = [];

  for (let page = 0; nextUrl && page < 20; page++) {
    const res = await metaGraphGet(nextUrl, tenantId);
    if (Array.isArray(res.data)) {
      metaTemplates.push(...res.data);
    }
    nextUrl = res.paging?.next || null;
  }

  let addedCount = 0;
  let updatedCount = 0;
  const now = new Date();

  for (const t of metaTemplates) {
    const metaId = t.id;
    const name = t.name;
    const language = t.language;
    const category = (t.category || 'UTILITY').toUpperCase();
    const status = (t.status || 'PENDING').toUpperCase();
    const qualityScore = t.quality_score?.score || 'UNKNOWN';
    const rejectedReason = t.rejected_reason || null;
    const parameterFormat = (t.parameter_format || 'POSITIONAL').toUpperCase();
    const components = Array.isArray(t.components) ? t.components : [];

    const existing = await WaTemplate.findOne({
      tenantId,
      $or: [
        { name, language },
        { metaId },
      ],
    });

    if (existing) {
      let isChanged = false;

      if (existing.status !== status || existing.rejectedReason !== rejectedReason) {
        existing.statusHistory.push({
          status,
          reason: rejectedReason,
          at: now,
        });
        existing.status = status;
        existing.rejectedReason = rejectedReason;
        isChanged = true;
      }

      if (existing.metaId !== metaId) {
        existing.metaId = metaId;
        isChanged = true;
      }

      if (existing.category !== category) {
        existing.category = category;
        isChanged = true;
      }

      if (existing.qualityScore !== qualityScore) {
        existing.qualityScore = qualityScore;
        isChanged = true;
      }

      if (existing.parameterFormat !== parameterFormat) {
        existing.parameterFormat = parameterFormat;
        isChanged = true;
      }

      existing.components = components;
      existing.lastSyncedAt = now;
      await existing.save();

      if (isChanged) updatedCount += 1;
    } else {
      await WaTemplate.create({
        tenantId,
        metaId,
        name,
        language,
        category,
        parameterFormat,
        components,
        status,
        qualityScore,
        rejectedReason,
        source: 'meta',
        createdBy: 'meta_sync',
        statusHistory: [
          {
            status,
            reason: rejectedReason,
            at: now,
          },
        ],
        lastSyncedAt: now,
      });
      addedCount += 1;
    }
  }

  // 3. Prune / remove templates that were deleted from Meta WhatsApp Manager
  // If a template exists locally in MongoDB for this tenant but is not in Meta's live list,
  // it has been deleted in Meta WhatsApp Manager and must be removed from MongoDB.
  let deletedCount = 0;
  try {
    const liveMetaIds = new Set(metaTemplates.map((t) => String(t.id)).filter(Boolean));
    const liveMetaKeys = new Set(
      metaTemplates.map((t) => `${(t.name || '').trim().toLowerCase()}:::${t.language || 'en_US'}`)
    );

    const localTemplates = await WaTemplate.find({ tenantId }).select('_id metaId name language');
    const idsToDelete = [];

    for (const lt of localTemplates) {
      const inMetaById = lt.metaId && liveMetaIds.has(String(lt.metaId));
      const inMetaByKey = liveMetaKeys.has(
        `${(lt.name || '').trim().toLowerCase()}:::${lt.language || 'en_US'}`
      );

      // If neither its Meta ID nor its name+language is in Meta's active templates, it was deleted on Meta
      if (!inMetaById && !inMetaByKey) {
        idsToDelete.push(lt._id);
      }
    }

    if (idsToDelete.length > 0) {
      const delResult = await WaTemplate.deleteMany({ _id: { $in: idsToDelete } });
      deletedCount = delResult.deletedCount || idsToDelete.length;
      console.log(`🗑️ WhatsApp template sync: Pruned ${deletedCount} deleted template(s) from MongoDB`);
    }
  } catch (pruneErr) {
    console.error('Error pruning deleted WhatsApp templates:', pruneErr.message);
  }

  return {
    success: true,
    totalMeta: metaTemplates.length,
    added: addedCount,
    updated: updatedCount,
    deleted: deletedCount,
    lastSyncedAt: now,
  };
}

module.exports = {
  syncTemplatesFromMeta,
};
