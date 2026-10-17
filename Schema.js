const mongoose = require("mongoose");

const conversationSchema = new mongoose.Schema({
  userMsg: { type: String, required: true },
  botReply: { type: String, required: true },
  timestamp: { type: Date, default: Date.now }
});

const remarksSchema = new mongoose.Schema({
  remark: String,
  timestamp: { type: Date, default: Date.now },
});

const userSchema = new mongoose.Schema({
  username: { type: String },
  useremail: { type: String, unique: true, required: true },
  userNumber: { type: String, required: true, unique: true }, 
  password: { type: String },
  role: {
    type: String,
    enum: ["admin", "user", "cert", "hr"],
    default: "user"
  },
  // Controls whether this counsellor receives the daily 5 PM AI review meeting.
  // Dashboard/target reporting still includes them unless a report-specific
  // exclusion also applies.
  managerReviewEnabled: { type: Boolean, default: true }
});


const InternSchema = new mongoose.Schema({
  salutation: { type: String, trim: true, default: "" },
  userName: { type: String, trim: true, default: "" },
  userEmail: { type: String, trim: true, lowercase: true, default: "" },
  phone: { type: String, trim: true, default: "" },
  reg_no: { type: String, trim: true, default: "" },
  year_of_study: { type: String, trim: true, default: "" },
  college: { type: String, trim: true, default: "" },
  college_location: { type: String, trim: true, default: "" },
  degree: { type: String, trim: true, default: "" },
  other_degree: { type: String, trim: true, default: "" },
  department: { type: String, trim: true, default: "" },
  internship: { type: String, trim: true, default: "" },
  other_internship: { type: String, trim: true, default: "" },
  workshop: { type: String, trim: true, default: "" },
  other_workshop: { type: String, trim: true, default: "" },
  start_date: { type: Date, default: null },
  end_date: { type: Date, default: null },
  date_scheduled: { type: Date, default: null },
  issue_date: { type: Date, default: null },
  company: { type: String, trim: true, default: "" },
  batch_code: { type: String, trim: true, default: "" }
}, { timestamps: true });



const CourseCertSchema = new mongoose.Schema({
  salutation: { type: String, trim: true, default: "" },
  userName: { type: String, trim: true, default: "" },
  course: { type: String, trim: true, default: "" },
  issue_date: { type: Date, default: null },
  project_title: { type: String, trim: true, default: "" },
  intern_title: { type: String, trim: true, default: "" }
}, { timestamps: true });



const IdCardSchema = new mongoose.Schema({
  dob: { type: Date, default: null },
  address: { type: String, trim: true, default: "" },
  idNo: { type: String, trim: true, default: "" },
  classStart: { type: Date, default: null },
  validTill: { type: Date, default: null },
  photoUrl: { type: String, default: "" },
  photoKey: { type: String, default: "" },
}, { timestamps: true });



const feesSchema = new mongoose.Schema({
  billedAmount: { type: Number, default: 0 },
  billedAt: { type: Date, default: null },
  updatedAt: { type: Date, default: null },
  installments: [
    {
      number: Number,
      dueDate: Date,
      amount: Number,
      status: {
        type: String,
        enum: ["Pending", "Paid"],
        default: "Pending"
      },
         billGenerated: { type: Boolean, default: false },
         billGeneratedAt: { type: Date, default: null },
         billUrl: { type: String, default: null },
         paidAt: { type: Date, default: null }
    }
  ]
});

const chatSchema = new mongoose.Schema({
  userName: { type: String },
  userNumber: { type: String, required: true, unique: true },
  courseofintrest: { type: String },
  course: { type: String, default: null },
  leadfrom: { type: String },
  // Name of the existing student who referred this lead, set only when leadfrom === "Referral"
  // via the /external/referral endpoint. Kept as a dedicated field (rather than parsed out of
  // remarks) so the dashboard tooltip doesn't depend on remark text formatting.
  referredBy: { type: String, default: null },
  // Deterministic referral-group code (first 4 letters of the referrer's name + last 5 digits
  // of their number), shared by the referrer (leader) and everyone they've referred via
  // /external/referral(/bulk). Same referrer always yields the same code, so repeat referrals
  // stay grouped together no matter how much time passes between them.
  referralCode: { type: String, trim: true, default: "" },
  // True on the referrer's own Chat doc once they've referred at least one lead — marks them
  // as the "leader" of their referralCode group in the dashboard's referral-groups popup.
  is_referral_leader: { type: Boolean, default: false },
  // Meta lead_id (a.k.a. leadgen_id), captured by wpchat-api from the Lead Ads webhook.
  // Required to report "Qualified Lead" events back to Meta via the Conversions API for Leads.
  leadgenId: { type: String, default: null },
  // History of Conversions API for Leads events sent to Meta for this lead.
  metaCapiEvents: [
    {
      eventName: { type: String },
      sentAt: { type: Date, default: Date.now },
      success: { type: Boolean, default: false },
      message: { type: String, default: null },
    }
  ],
  assignedto: { type: String },
  batchId: { type: String, default: null },
  conversations: [conversationSchema],
  programType: { type: String },
  city: { type: String },
  profession: { type: String },
  location: { type: String },
  status: { type: String, default: "Cold" },
  pipeline: { type: String, default: "New Lead" },
  remarks: [remarksSchema],
  reminder: { type: Date, default: null },
  batchId: { type: String, default: null },
  Trainer: { type: String, default: null },
  batch_date : { type: String, default: null },
  batch_start_time: { type: String, default: null },   // "HH:mm", e.g. "09:30"
  batch_end_time: { type: String, default: null },     // "HH:mm", e.g. "10:30"
  batch_days: { type: [String], default: [] },         // subset of ["Mon","Tue","Wed","Thu","Fri","Sat"]
  fees: { type: feesSchema, default: () => ({}) },
  followUpCount: { type: Number, default: 0 },
  respondedAfterFollowUp: { type: Boolean, default: false },
  // True once the /email/demobooking webhook fires for this lead (set by wpchat-api).
  // Drives the CRM dashboard's red "Demo Booked" dot on the lead row.
  demoBooked: { type: Boolean, default: false },
  demoBookedAt: { type: Date, default: null },
  lastFollowUpSentAt: { type: Date, default: null },
  noRemarkEscalationCount: { type: Number, default: 0 },
  lastNoRemarkEscalationAt: { type: Date, default: null },
  // Set to true once a lead has been auto-transferred to another counsellor because
  // it still had zero remarks after the 3rd no-remark escalation. Prevents repeat transfers.
  noRemarkReassigned: { type: Boolean, default: false },
  // Timestamp of the last time this lead's `pipeline` value actually changed. Seeded from
  // `datecreated` on creation and refreshed by the save / findOneAndUpdate hooks below.
  // Drives the "stale pipeline" auto-reassignment cron.
  pipelineChangedAt: { type: Date, default: Date.now },
  // Last time the stale-pipeline cron auto-transferred this lead, so it is not re-transferred
  // again until it has been stale for a further 15 days.
  lastStalePipelineReassignAt: { type: Date, default: null },
  // Audit trail of automatic counsellor reassignments (no-remark + stale-pipeline crons).
  autoReassignHistory: [{
    from: { type: String, default: null },
    to: { type: String, default: null },
    reason: { type: String, default: null },
    at: { type: Date, default: Date.now },
  }],
  datecreated: { type: Date, default: Date.now },
  lastInteracted: { type: Date, default: Date.now },
  aiAnalysis: { type: Object, default: null },
  aiConversationSummary: { type: Object, default: null },
  InternDetails : [InternSchema],
  certificates: [
    {
      success: { type: Boolean, default: false },
      fileName: { type: String, default: null },
      message: { type: String, default: null },
      s3: { type: mongoose.Schema.Types.Mixed, default: null }
    }
  ],
  CourseCertDetails: [CourseCertSchema],
  courseCertificates: [
    {
      success: { type: Boolean, default: false },
      type: { type: String, default: null },
      fileName: { type: String, default: null },
      message: { type: String, default: null },
      s3: { type: mongoose.Schema.Types.Mixed, default: null }
    }
  ],
  IdCardDetails: [IdCardSchema],
  idCards: [
    {
      success: { type: Boolean, default: false },
      fileName: { type: String, default: null },
      message: { type: String, default: null },
      s3: { type: mongoose.Schema.Types.Mixed, default: null }
    }
  ],
  is_leader : { type: Boolean, default: false },
  campaignHistory: [{
    campaignName: { type: String, required: true },
    message: { type: String, default: null },
    sentAt: { type: Date, default: Date.now },
    repliedAt: { type: Date, default: null }
  }]
});

// Course is filtered on heavily by the reporting endpoints and the manager
// chatbot's conversion aggregates — index it (exact-match, case as stored).
chatSchema.index({ course: 1 });
chatSchema.index({ pipeline: 1 });

// Staging area for bulk-Excel WhatsApp campaign contacts that are not yet real leads.
// Promoted into a Chat doc (and removed from here) once the contact replies on WhatsApp.
const tempLeadSchema = new mongoose.Schema({
  userName: { type: String, default: "Unknown" },
  userNumber: { type: String, required: true, unique: true },
  campaignName: { type: String, required: true },
  message: { type: String, default: null },
  sentAt: { type: Date, default: Date.now },
  status: { type: String, enum: ["sent", "failed"], default: "sent" },
  source: { type: String, default: "bulk-excel" },
  createdAt: { type: Date, default: Date.now }
});

// --- Static Method for Round Robin Assignment ---
chatSchema.statics.getNextAssignee = async function() {
  const User = mongoose.model("User");

  const db = mongoose.connection.db;
  const excludedConfig = db ? await db.collection('configs').findOne({ key: 'auto_assign_excluded_staffs' }) : null;
  const excludedStaffs = excludedConfig ? excludedConfig.value : [];

  const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 });
  const staffs = allStaffs.filter(s => !excludedStaffs.includes(s.username));
  if (staffs.length === 0) return null;

  const staffUsernames = staffs.map(s => s.username);

  const lastAssignedLead = await this.findOne({ assignedto: { $in: staffUsernames } })
    .sort({ datecreated: -1 });

  if (!lastAssignedLead || !lastAssignedLead.assignedto) return staffs[0].username;

  const lastIndex = staffUsernames.indexOf(lastAssignedLead.assignedto);
  const nextIndex = (lastIndex + 1) % staffs.length;
  return staffs[nextIndex].username;
};

// --- Pre-Save Hook for Auto Assignment ---
chatSchema.pre('save', async function(next) {
  console.log("pre reached")
  this.$locals.wasNew = this.isNew;

  if (this.isNew && (!this.assignedto || this.assignedto.trim() === "")) {
    try {
      const db = mongoose.connection.db;
      if (db) {
        const config = await db.collection('configs').findOne({ key: 'auto_assign_enabled' });
        const autoAssignEnabled = config ? !!config.value : true;

        if (autoAssignEnabled) {
          const assignee = await this.constructor.getNextAssignee();
          if (assignee) {
            console.log(assignee)
            this.assignedto = assignee;
          }
        }
      }
    } catch (err) {
      console.error("Auto-assign hook error:", err);
    }
  }
  next();
});

// Keep `pipelineChangedAt` in sync on document .save() paths (e.g. the remarks route,
// which loads a doc and saves it). New docs are covered by the schema default.
chatSchema.pre('save', function (next) {
  if (!this.isNew && this.isModified('pipeline')) {
    this.pipelineChangedAt = new Date();
  }
  next();
});


chatSchema.pre("findOneAndUpdate", async function (next) {
  // get the document BEFORE update
  const docToUpdate = await this.model.findOne(this.getQuery()).lean();

  // store previous value on the query object
  this._oldAssignedTo = docToUpdate?.assignedto;
  this._oldPipeline = docToUpdate?.pipeline;

  // Stamp `pipelineChangedAt` whenever this update actually changes the pipeline stage,
  // so the stale-pipeline cron measures from the real transition, not the last edit.
  const upd = this.getUpdate() || {};
  const nextPipeline = upd.$set?.pipeline ?? (upd.$set ? undefined : upd.pipeline);
  if (nextPipeline !== undefined && nextPipeline !== docToUpdate?.pipeline) {
    if (upd.$set) upd.$set.pipelineChangedAt = new Date();
    else upd.pipelineChangedAt = new Date();
    this.setUpdate(upd);
  }

  next();
});


chatSchema.post("findOneAndUpdate", async function (doc) {
  if (!doc) return;

  const update = this.getUpdate();

  // 🧠 safely extract assignedto
  const newAssignedTo =
    update?.$set?.assignedto ??
    update?.assignedto ??
    null;

  // 🔐 if assignedto not part of update → DO NOTHING
  if (newAssignedTo === null) return;

  const oldAssignedTo = this._oldAssignedTo;

  // 🔐 notify ONLY if value actually changed
  if (!oldAssignedTo || oldAssignedTo === newAssignedTo) return;

  try {
    await NotifyCounsellor({
      leadId: doc._id,
      assignedTo: newAssignedTo,
      userNumber: doc.userNumber,
      userName: doc.userName,
      message: `Lead assigned to ${newAssignedTo}`
    });
  } catch (err) {
    console.error("NotifyCounsellor failed:", err);
  }
});

// Logs pipeline-stage transitions into PipelineChange for the manager RAG
// chatbot ("what pipeline changes happened today"). Separate from the
// assignedto-notify hook above so a failure here never blocks that notify.
chatSchema.post("findOneAndUpdate", async function (doc) {
  if (!doc) return;

  const update = this.getUpdate();
  const newPipeline = update?.$set?.pipeline ?? update?.pipeline ?? null;
  if (newPipeline === null) return;

  const oldPipeline = this._oldPipeline;
  if (oldPipeline === newPipeline) return;

  try {
    await PipelineChange.create({
      leadId: doc._id,
      userName: doc.userName,
      userNumber: doc.userNumber,
      assignedto: (update?.$set?.assignedto ?? update?.assignedto) || doc.assignedto || null,
      fromStage: oldPipeline || "New Lead",
      toStage: newPipeline,
    });
  } catch (err) {
    console.error("PipelineChange log failed:", err);
  }
});

chatSchema.post("save", async function (doc) {
  // 🔐 notify ONLY when document is newly created
  console.log("post reached")
  if (!doc.$locals.wasNew) return;
  if (doc.$locals.skipNotify) return;
  console.log("new user post reached")
  try {
    await NotifyCounsellor({
      leadId: doc._id,
      assignedTo: doc.assignedto,
      userNumber: doc.userNumber,
      userName: doc.userName,
      message: `New lead assigned to ${doc.assignedto}`
    });
  } catch (err) {
    console.error("NotifyCounsellor failed:", err);
  }
});

async function NotifyCounsellor(body) {
  console.log("Notify payload:", body);
  console.log("AssignedTo:", body.assignedTo);

  const response = await fetch(
    `${process.env.WP_CHAT_SERVICE_URL}/notify/crm`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }
  );

  const data = await response.json();
  console.log("Microservice response:", data);

  return data;
}


const trainerSchema = new mongoose.Schema({
  name: { type: String, required: true },
  mobile: { type: String, required: true, unique: true },
  username: { type: String, unique: true, sparse: true },
  password: { type: String, default: null },
  batchIds: [{ type: String }]
});
const batchSchema = new mongoose.Schema({
  batchId: { type: String, required: true, unique: true },
  trainerId: { type: String, required: true },
  status: { type: String, enum: ['active', 'completed'], default: 'active' },
  completedAt: { type: Date, default: null },
  studentCount: { type: Number, default: 0 },
  reopenedBy: { type: String, default: null },
  reopenedAt: { type: Date, default: null },
});

const trainerTaskSchema = new mongoose.Schema({
  studentId: { type: String, required: true },
  trainerId: { type: String, required: true },
  batchId: { type: String },
  title: { type: String, required: true },
  description: { type: String, default: '' },
  driveLink: { type: String, default: '' },
  studentRemark: { type: String, default: '' },
  trainerRemark: { type: String, default: '' },
  status: { type: String, enum: ['pending', 'completed'], default: 'pending' },
  createdAt: { type: Date, default: Date.now }
});

const trainerTopicSchema = new mongoose.Schema({
  title: { type: String, required: true },
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  date: { type: Date, default: Date.now }
});

const trainerTopicFeedbackSchema = new mongoose.Schema({
  topicId: { type: String, required: true },
  studentId: { type: String, required: true },
  status: { type: String, enum: ['pending', 'approved', 'denied'], default: 'pending' },
  updatedAt: { type: Date, default: Date.now }
});

const announcementSchema = new mongoose.Schema({
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  title: { type: String, required: true },
  message: { type: String, required: true },
  createdAt: { type: Date, default: Date.now }
});

const attendanceSchema = new mongoose.Schema({
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  date: { type: Date, default: Date.now },
  studentId: { type: String, required: true },
  status: { type: String, enum: ['present', 'absent', 'late'], default: 'absent' },
  topicId: { type: String, default: null }
});

const userMonthlyTargetSchema = new mongoose.Schema({
  username: { type: String, required: true },
  year: { type: Number, required: true },
  month: { type: Number, required: true },
  target: { type: Number, default: 0 }
});
userMonthlyTargetSchema.index({ username: 1, year: 1, month: 1 }, { unique: true });

const groupMessageSchema = new mongoose.Schema({
  sender: { type: String, required: true },
  message: { type: String, required: true },
  timestamp: { type: Date, default: Date.now }
});

// A user-defined, named bundle of dashboard leads (Chat docs). Independent of the
// lead documents themselves so renaming / deleting a group never touches a lead,
// and a lead can belong to many groups. Shown as a badge in the dashboard's
// "Program & Source" column; only the creator or an admin may modify it.
const leadGroupSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  color: { type: String, default: "violet" },
  members: [{ type: mongoose.Schema.Types.ObjectId, ref: "Chat" }],
  createdBy: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

const weeklyReportSchema = new mongoose.Schema({
  counsellor: { type: String, required: true },
  weekStart: { type: Date, required: true },
  weekEnd: { type: Date, required: true },
  generatedAt: { type: Date, default: Date.now },
  metrics: {
    newLeads: { type: Number, default: 0 },
    enrolledThisWeek: { type: Number, default: 0 },
    noContactCount: { type: Number, default: 0 },
    notContactedPct: { type: Number, default: 0 },
    avgResponseMins: { type: Number, default: null },
    avgOffHoursResponseMins: { type: Number, default: null },
    offHoursLeadsCount: { type: Number, default: 0 },
    offHoursContactedCount: { type: Number, default: 0 },
    slowResponses: { type: Number, default: 0 },
    slowBuckets: { type: Object, default: {} },
    thisWeekFees: {
      totalBilled: { type: Number, default: 0 },
      totalPaid: { type: Number, default: 0 }
    },
    prevWeekFees: {
      totalBilled: { type: Number, default: 0 },
      totalPaid: { type: Number, default: 0 }
    },
    billedPct: { type: Number, default: null },
    paidPct: { type: Number, default: null },
    upcomingPayments: {
      totalAmount: { type: Number, default: 0 },
      count: { type: Number, default: 0 }
    },
  },
  aiSummary: { type: String, default: '' },
  likelyToConfirm: { type: String, default: '' },
  hotNotEnrolled: [{
    userName: String,
    course: String,
    status: String,
    lastRemark: String
  }],
  respondedAfterFollowUp: [{
    userName: String,
    course: String,
    status: String
  }],
  upcomingInstallments: [{
    userName: String,
    course: String,
    amount: Number,
    dueDate: Date
  }],
});
weeklyReportSchema.index({ counsellor: 1, weekStart: -1 });

const minminiReportSchema = new mongoose.Schema({
  weekStart: { type: Date, required: true },
  weekEnd: { type: Date, required: true },
  generatedAt: { type: Date, default: Date.now },
  activeLeads: { type: Number, default: 0 },
  enrolledAfterWA: { type: Number, default: 0 },
  totalMessages: { type: Number, default: 0 },
  avgMessagesPerLead: { type: Number, default: 0 },
  avgDurationMins: { type: Number, default: 0 },
  peakHour: { type: Number, default: null },
  peakHours: { type: Object, default: {} },
  topCourses: [{ course: String, count: Number }],
  followUpsSent: { type: Number, default: 0 },
  followUpResponded: { type: Number, default: 0 },
  followUpResponsePct: { type: Number, default: 0 },
  aiSummary: { type: String, default: '' },
});
minminiReportSchema.index({ weekStart: -1 });

const monthlyReportSchema = new mongoose.Schema({
  counsellor: { type: String, required: true },
  monthStart: { type: Date, required: true },
  monthEnd: { type: Date, required: true },
  generatedAt: { type: Date, default: Date.now },
  metrics: {
    newLeads: { type: Number, default: 0 },
    enrolledThisMonth: { type: Number, default: 0 },
    noContactCount: { type: Number, default: 0 },
    notContactedPct: { type: Number, default: 0 },
    avgResponseMins: { type: Number, default: null },
    avgOffHoursResponseMins: { type: Number, default: null },
    offHoursLeadsCount: { type: Number, default: 0 },
    offHoursContactedCount: { type: Number, default: 0 },
    slowResponses: { type: Number, default: 0 },
    slowBuckets: { type: Object, default: {} },
    thisMonthFees: {
      totalBilled: { type: Number, default: 0 },
      totalPaid: { type: Number, default: 0 }
    },
    prevMonthFees: {
      totalBilled: { type: Number, default: 0 },
      totalPaid: { type: Number, default: 0 }
    },
    billedPct: { type: Number, default: null },
    paidPct: { type: Number, default: null },
  },
  aiSummary: { type: String, default: '' },
  likelyToConfirm: { type: String, default: '' },
  hotNotEnrolled: [{
    userName: String,
    course: String,
    status: String,
    lastRemark: String
  }],
  respondedAfterFollowUp: [{
    userName: String,
    course: String,
    status: String
  }],
});
monthlyReportSchema.index({ counsellor: 1, monthStart: -1 });

const monthlyMinminiReportSchema = new mongoose.Schema({
  monthStart: { type: Date, required: true },
  monthEnd: { type: Date, required: true },
  generatedAt: { type: Date, default: Date.now },
  activeLeads: { type: Number, default: 0 },
  enrolledAfterWA: { type: Number, default: 0 },
  totalMessages: { type: Number, default: 0 },
  avgMessagesPerLead: { type: Number, default: 0 },
  avgDurationMins: { type: Number, default: 0 },
  peakHour: { type: Number, default: null },
  peakHours: { type: Object, default: {} },
  topCourses: [{ course: String, count: Number }],
  followUpsSent: { type: Number, default: 0 },
  followUpResponded: { type: Number, default: 0 },
  followUpResponsePct: { type: Number, default: 0 },
  aiSummary: { type: String, default: '' },
});
monthlyMinminiReportSchema.index({ monthStart: -1 });

// Daily WhatsApp "manager" check-in conversation with a counsellor (role: "user").
// One doc per counsellor per IST calendar day (date = "YYYY-MM-DD").
const managerCheckInSchema = new mongoose.Schema({
  counsellorUsername: { type: String, required: true },
  counsellorNumber: { type: String, required: true },
  date: { type: String, required: true },
  metrics: { type: Object, default: {} },
  messages: [{
    role: { type: String, enum: ["manager", "counsellor"] },
    text: String,
    timestamp: { type: Date, default: Date.now }
  }],
  questionCount: { type: Number, default: 0 },
  status: { type: String, enum: ["in_progress", "completed", "no_response"], default: "in_progress" },
  verdict: { type: String, enum: ["SATISFACTORY", "NEEDS_IMPROVEMENT", null], default: null },
  verdictReason: { type: String, default: "" },
  adminSummaryReport: { type: String, default: "" },
  escalatedToAdmin: { type: Boolean, default: false },
  lastManagerMessageAt: { type: Date, default: null },
  nudgeSentAt: { type: Date, default: null },
  startedAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null },
});
managerCheckInSchema.index({ counsellorUsername: 1, date: 1 }, { unique: true });

// Audit trail of Chat.pipeline stage transitions (New Lead -> Site Visit -> ...).
// There's no other record of *when* a lead's stage changed, only its current
// value, so the manager RAG chatbot's "pipeline changes today" answers depend
// on this. Populated by the chatSchema findOneAndUpdate hooks below.
const pipelineChangeSchema = new mongoose.Schema({
  leadId: { type: mongoose.Schema.Types.ObjectId, ref: "Chat" },
  userName: { type: String, default: null },
  userNumber: { type: String, default: null },
  assignedto: { type: String, default: null },
  fromStage: { type: String, default: "New Lead" },
  toStage: { type: String, required: true },
  changedAt: { type: Date, default: Date.now },
});
pipelineChangeSchema.index({ changedAt: -1 });

// Follow-up items the business owner (MANAGER_ADMIN_NUMBER) asked the manager
// chatbot to chase with a specific counsellor ("why no enrollment from Abhi").
// Injected into that counsellor's next daily check-in(s) until they give a
// substantive answer (capped at 3 attempts), then reported back to the owner.
// Populated + resolved entirely from ManagerChat.js.
const managerFollowUpSchema = new mongoose.Schema({
  counsellorUsername: { type: String, default: null },
  question: { type: String, required: true },
  rawAdminMessage: { type: String, default: "" },
  status: { type: String, enum: ["open", "asked", "answered", "closed"], default: "open" },
  askCount: { type: Number, default: 0 },
  lastAskedCheckinDate: { type: String, default: null },
  answer: { type: String, default: "" },
  answeredAt: { type: Date, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});
managerFollowUpSchema.index({ counsellorUsername: 1, status: 1 });

// Daily sales-training quiz sent to every counsellor as a WhatsApp voice
// note + text: a topic, a short script, and 5 questions they answer by text.
// Owned entirely by wpchat-api (see its SalesTraining.js) — this app only
// reads it, for the CRM's read-only "Sales Training" view. Field shapes must
// stay in sync with wpchat-api's Schema.js since both point at the same
// underlying "salestrainingsessions" collection.
const salesTrainingQuestionSchema = new mongoose.Schema({
  text: { type: String, required: true },
  answer: { type: String, default: "" },
  score: { type: Number, default: null }, // 0 or 1, set once answered
  feedback: { type: String, default: "" },
}, { _id: false });

const salesTrainingSessionSchema = new mongoose.Schema({
  userNumber: { type: String, required: true },
  username: { type: String, default: "" },
  topic: { type: String, required: true },
  script: { type: String, default: "" },
  questions: [salesTrainingQuestionSchema],
  currentQuestionIndex: { type: Number, default: 0 },
  status: { type: String, enum: ["in_progress", "completed"], default: "in_progress" },
  totalScore: { type: Number, default: null }, // out of 5, set once completed
  startedAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null },
});

const User = mongoose.model("User", userSchema);
const Chat = mongoose.model("Chat", chatSchema);
const Trainer = mongoose.model("Trainer", trainerSchema);
const Batch = mongoose.model("Batch", batchSchema);
const TrainerTask = mongoose.model("TrainerTask", trainerTaskSchema);
const TrainerTopic = mongoose.model("TrainerTopic", trainerTopicSchema);
const TrainerTopicFeedback = mongoose.model("TrainerTopicFeedback", trainerTopicFeedbackSchema);
const Announcement = mongoose.model("Announcement", announcementSchema);
const Attendance = mongoose.model("Attendance", attendanceSchema);
const UserMonthlyTarget = mongoose.model("UserMonthlyTarget", userMonthlyTargetSchema);
const GroupMessage = mongoose.model("GroupMessage", groupMessageSchema);
const LeadGroup = mongoose.model("LeadGroup", leadGroupSchema);
const WeeklyReport = mongoose.model("WeeklyReport", weeklyReportSchema);
const MinminiReport = mongoose.model("MinminiReport", minminiReportSchema);
const MonthlyReport = mongoose.model("MonthlyReport", monthlyReportSchema);
const MonthlyMinminiReport = mongoose.model("MonthlyMinminiReport", monthlyMinminiReportSchema);
const TempLead = mongoose.model("TempLead", tempLeadSchema);
const ManagerCheckIn = mongoose.model("ManagerCheckIn", managerCheckInSchema);
const PipelineChange = mongoose.model("PipelineChange", pipelineChangeSchema);
const ManagerFollowUp = mongoose.model("ManagerFollowUp", managerFollowUpSchema);
const SalesTrainingSession = mongoose.model("SalesTrainingSession", salesTrainingSessionSchema);
// ── Co-working space management ──────────────────────────────────────────────
// A branch/hub with its seat layout, meeting rooms, private offices, virtual-office
// plans and managed-office options. `spaceId` is the client-visible id (e.g. "ram-nagar").
const coworkSeatSchema = new mongoose.Schema({
  id: { type: String, required: true },
  row: { type: String, default: "" },
  number: { type: Number, default: 0 },
  category: {
    type: String,
    enum: ["hotdesk", "co_working", "meeting_room", "private_office", "managed_office", "virtual_office"],
    default: "co_working",
  },
  name: { type: String, default: "" },
  pricePerHour: { type: Number, default: 0 },
  pricePerDay: { type: Number, default: 0 },
  amenities: [{ type: String }],
  meetingRoomId: { type: String, default: null },
  privateOfficeId: { type: String, default: null },
  isUnderMaintenance: { type: Boolean, default: false },
}, { _id: false });

const coworkSpaceSchema = new mongoose.Schema({
  spaceId: { type: String, required: true, unique: true },
  name: { type: String, required: true },
  branchArea: { type: String, default: "" },
  tagline: { type: String, default: "" },
  address: { type: String, default: "" },
  city: { type: String, default: "" },
  pincode: { type: String, default: "" },
  phone: { type: String, default: "" },
  screenLabel: { type: String, default: "" },
  openHours: { open: { type: String, default: "09:30" }, close: { type: String, default: "18:30" } },
  amenities: [{ type: String }],
  seats: [coworkSeatSchema],
  // Rooms/offices/plans are nested, loosely-typed config blobs owned by the layout editor.
  meetingRooms: { type: [mongoose.Schema.Types.Mixed], default: [] },
  privateOffices: { type: [mongoose.Schema.Types.Mixed], default: [] },
  virtualOfficePlans: { type: [mongoose.Schema.Types.Mixed], default: [] },
  managedOffices: { type: [mongoose.Schema.Types.Mixed], default: [] },
  createdBy: { type: String, default: null },
}, { timestamps: true });

const seatBookingSchema = new mongoose.Schema({
  bookingId: { type: String, required: true, unique: true },
  spaceId: { type: String, required: true, index: true },
  spaceName: { type: String, default: "" },
  branchArea: { type: String, default: "" },
  seatIds: [{ type: String }],
  meetingRoomIds: [{ type: String }],
  serviceType: {
    type: String,
    enum: ["seats", "meeting_room", "private_office", "virtual_office", "managed_office"],
    default: "seats",
  },
  date: { type: String, required: true },          // YYYY-MM-DD (primary date)
  fromDate: { type: String, required: true },      // YYYY-MM-DD
  toDate: { type: String, required: true },        // YYYY-MM-DD
  totalDays: { type: Number, default: 1 },
  startTime: { type: String, default: "09:30" },   // HH:mm
  endTime: { type: String, default: "18:30" },     // HH:mm
  durationHours: { type: Number, default: 0 },
  clientName: { type: String, default: "" },
  clientEmail: { type: String, default: "" },
  clientPhone: { type: String, default: "" },
  companyName: { type: String, default: "" },
  notes: { type: String, default: "" },
  totalPrice: { type: Number, default: 0 },
  status: { type: String, enum: ["confirmed", "checked_in", "completed", "cancelled"], default: "confirmed" },
  paymentMethod: { type: String, default: "pay_at_desk" },
  qrCodeData: { type: String, default: "" },
  // Linked CRM lead (Chat) when the client's phone number matches an existing lead.
  leadId: { type: mongoose.Schema.Types.ObjectId, ref: "Chat", default: null },
  createdBy: { type: String, default: null },
}, { timestamps: true });

seatBookingSchema.index({ spaceId: 1, fromDate: 1, toDate: 1 });

const CoworkSpace = mongoose.model("CoworkSpace", coworkSpaceSchema);
const SeatBooking = mongoose.model("SeatBooking", seatBookingSchema);

module.exports = { User, Chat, Trainer, Batch, TrainerTask, TrainerTopic, TrainerTopicFeedback, Announcement, Attendance, UserMonthlyTarget, GroupMessage, LeadGroup, WeeklyReport, MinminiReport, MonthlyReport, MonthlyMinminiReport, TempLead, ManagerCheckIn, PipelineChange, ManagerFollowUp, SalesTrainingSession, CoworkSpace, SeatBooking };

