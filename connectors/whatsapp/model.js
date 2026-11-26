const mongoose = require('mongoose');

const WaTemplateComponentButtonSchema = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: ['QUICK_REPLY', 'URL', 'PHONE_NUMBER', 'COPY_CODE', 'FLOW', 'CATALOG'],
      required: true,
    },
    text: { type: String, required: true },
    url: { type: String },
    phone_number: { type: String },
    example: [{ type: String }],
  },
  { _id: false }
);

const WaTemplateComponentSchema = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: ['HEADER', 'BODY', 'FOOTER', 'BUTTONS'],
      required: true,
    },
    format: {
      type: String,
      enum: ['TEXT', 'IMAGE', 'DOCUMENT', 'VIDEO', 'LOCATION'],
    },
    text: { type: String },
    example: { type: mongoose.Schema.Types.Mixed },
    buttons: [WaTemplateComponentButtonSchema],
  },
  { _id: false }
);

const WaTemplateStatusHistorySchema = new mongoose.Schema(
  {
    status: { type: String, required: true },
    reason: { type: String, default: null },
    at: { type: Date, default: Date.now },
  },
  { _id: false }
);

const WaTemplateSchema = new mongoose.Schema(
  {
    tenantId: { type: String, default: 'default', index: true },
    metaId: { type: String, index: true },
    name: { type: String, required: true },
    language: { type: String, required: true, default: 'en_US' },
    category: {
      type: String,
      enum: ['MARKETING', 'UTILITY', 'AUTHENTICATION'],
      required: true,
      index: true,
    },
    parameterFormat: {
      type: String,
      enum: ['POSITIONAL', 'NAMED'],
      default: 'POSITIONAL',
    },
    components: [WaTemplateComponentSchema],
    status: {
      type: String,
      enum: [
        'PENDING',
        'APPROVED',
        'REJECTED',
        'PAUSED',
        'DISABLED',
        'IN_APPEAL',
        'DELETED',
      ],
      default: 'PENDING',
      index: true,
    },
    qualityScore: { type: String, default: 'UNKNOWN' }, // GREEN, YELLOW, RED, UNKNOWN
    rejectedReason: { type: String, default: null },
    source: {
      type: String,
      enum: ['crm', 'meta'],
      default: 'crm',
    },
    createdBy: { type: String, default: 'admin' },
    statusHistory: [WaTemplateStatusHistorySchema],
    lastSyncedAt: { type: Date, default: Date.now },
  },
  {
    timestamps: true,
  }
);

// Prevent duplicate template name + language combinations per tenant
WaTemplateSchema.index({ tenantId: 1, name: 1, language: 1 }, { unique: true });

// Helper to update status and append history
WaTemplateSchema.methods.updateStatus = function (newStatus, reason = null) {
  if (this.status !== newStatus || (reason && this.rejectedReason !== reason)) {
    this.status = newStatus;
    if (reason !== undefined) this.rejectedReason = reason;
    this.statusHistory.push({
      status: newStatus,
      reason,
      at: new Date(),
    });
  }
};

const WaTemplate = mongoose.model('WaTemplate', WaTemplateSchema);

module.exports = {
  WaTemplate,
};
