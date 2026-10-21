// ManagerChat.js
//
// Single home for BOTH "AI manager" WhatsApp features, sharing one RAG store
// (the ManagerChatChunk collection):
//
//   1. Daily "manager" check-in — Mon–Sat 5 PM IST an AI manager opens a
//      WhatsApp chat with every counsellor, asks pointed questions about
//      today's lead activity AND digs into 6 months of history (why old
//      pipelines haven't moved, why last week's leads got no action, why
//      named leads didn't convert), records a verdict, and rolls everything
//      into one 6 PM team report to MANAGER_ADMIN_NUMBER. It also raises any
//      follow-up the owner asked for (see #3) and reports back the answer.
//
//   2. Admin RAG chatbot — MANAGER_ADMIN_NUMBER asks free-form questions on
//      WhatsApp ("why did enrollments drop", "what did Abhi do with lead X",
//      "revenue this month vs last") and gets answers grounded in: live CRM
//      figures, on-demand today/this-month aggregates, and a 6-month corpus
//      of per-lead timelines + reconstructed daily reports + weekly/monthly
//      reports, retrieved by in-process cosine similarity.
//
//   3. Follow-up memory — when the admin asks the chatbot to chase something
//      with a counsellor ("why no enrollment from Abhi"), it's stored in
//      ManagerFollowUp and injected into that counsellor's next check-in(s)
//      until they give a real answer (cap 3), then surfaced in the 6 PM report.
//
// Entry point: registerManagerChatRoutes(app, MANAGER_ADMIN_NUMBER) — mounts
//   POST /manager/chat/incoming     (admin chatbot; wpchat-api forwards here)
//   POST /manager/checkin/reply     (counsellor check-in replies; wpchat-api)
//   POST /manager/rag/reindex       (x-rag-key gated — full RAG rebuild)
//   GET  /manager/rag/status        (x-rag-key gated — chunk counts)
// and schedules all manager crons (5 PM check-in, minute nudge sweep, 6 PM
// consolidated report, 8 PM no-response sweep, 1:30 AM nightly RAG ingest).
//
// wpchat-api is NOT touched — its webhook already routes MANAGER_ADMIN_NUMBER
// messages to /manager/chat/incoming and in-progress check-in replies to
// /manager/checkin/reply.

const crypto = require("crypto");
const mongoose = require("mongoose");
const OpenAI = require("openai");
const cron = require("node-cron");
const {
  Chat,
  User,
  UserMonthlyTarget,
  ManagerCheckIn,
  ManagerFollowUp,
  WeeklyReport,
  MonthlyReport,
  MinminiReport,
  MonthlyMinminiReport,
  PipelineChange,
} = require("./Schema");

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ─── Config (all env vars optional) ────────────────────────────────────
const CHAT_MODEL = process.env.MANAGER_CHAT_MODEL || "gpt-4o-mini";           // check-in conversation
const ADMIN_CHAT_MODEL = process.env.MANAGER_ADMIN_CHAT_MODEL || "gpt-4o-mini"; // admin chatbot
const EMBED_MODEL = "text-embedding-3-small";
const EMBED_DIMENSIONS = Number(process.env.MANAGER_RAG_EMBED_DIMS || 512);
const RAG_MONTHS = Number(process.env.MANAGER_RAG_MONTHS || 6);
const RAG_DAILY_DAYS = Number(process.env.MANAGER_RAG_DAILY_DAYS || 120);
const LEAD_CHAR_CAP = Number(process.env.MANAGER_RAG_LEAD_CHAR_CAP || 4000);
const RAG_KEY = process.env.MANAGER_RAG_KEY || "";

// Branch Manager Director Review Framework config — these four facts (seat
// capacity, named competitors) have no backing CRM field, so they're configured
// constants rather than DB truth. Everything derived from them is labeled as such
// in the chunk text builders below.
const JGP_COURSE_NAME = process.env.MANAGER_JGP_COURSE_NAME || "MERN AI Job Guarantee Program";
const JGP_SEATS_PER_BATCH = Number(process.env.MANAGER_JGP_SEATS_PER_BATCH || 5);
const KNOWN_COMPETITORS = String(process.env.MANAGER_KNOWN_COMPETITORS || "Qtree,Upshot")
  .split(",").map(s => s.trim()).filter(Boolean);

const MANAGER_MIN_QUESTIONS = Number(process.env.MANAGER_MIN_QUESTIONS || 20);
const MANAGER_MAX_QUESTIONS = Number(process.env.MANAGER_MAX_QUESTIONS || 25);
const MANAGER_NUDGE_MINUTES = Number(process.env.MANAGER_NUDGE_MINUTES || 10);
const MANAGER_FINAL_NUDGE_MINUTES = Number(process.env.MANAGER_FINAL_NUDGE_MINUTES || 20);
// Section 6 rating bands: below 4.0/10 (40%) = Escalate. This is deliberately
// lower than the "On Track" cutoff below — NEEDS_IMPROVEMENT (Needs Coaching
// band, 40-59%) should not by itself page the admin every day, only a true
// Escalate-band score or a NO_RESPONSE should.
const MANAGER_ESCALATE_SCORE_PCT = Number(process.env.MANAGER_ESCALATE_SCORE_PCT || 40);
// On Track cutoff — below this the daily verdict is NEEDS_IMPROVEMENT.
const MANAGER_NEEDS_IMPROVEMENT_PCT = Number(process.env.MANAGER_NEEDS_IMPROVEMENT_PCT || 60);

const REVIEW_DISABLED_USERNAMES = new Set(
  String(process.env.MANAGER_REVIEW_DISABLED_USERNAMES || "Shindu")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean)
);

// Counsellors (User docs) whose mobile is in this list are skipped by the daily
// manager check-in — internal/test accounts only. Mirrors EXCLUDED_REPORT_NUMBERS
// in index.js. Compared after normalising with cleanIndianMobileNumber, since
// User.userNumber formatting is free-form (+91.., 10-digit, 0-prefixed, ...).
const EXCLUDED_CHECKIN_NUMBERS = ["918667850844", "917397081629", "917708171533"];

// Set from registerManagerChatRoutes(app, managerAdminNumber); falls back to
// the hard-coded number index.js has always used.
let MANAGER_ADMIN_NUMBER = process.env.MANAGER_ADMIN_NUMBER || "919025489889";

// let MANAGER_ADMIN_NUMBER = process.env.MANAGER_ADMIN_NUMBER || "919360549855";
// ─── RAG store ──────────────────────────────────────────────────────────
const managerChatChunkSchema = new mongoose.Schema({
  chunkId: { type: String, required: true, unique: true },
  periodKey: { type: String, default: "" },
  type: { type: String, default: "general" },
  text: { type: String, required: true },
  embedding: { type: [Number], default: [] },
  hash: { type: String, default: "" },
  // Per-counsellor / recency pre-filtering for retrieval (used by the check-in).
  assignedto: { type: String, default: null },
  leadId: { type: mongoose.Schema.Types.ObjectId, default: null },
  refDate: { type: Date, default: null },
  updatedAt: { type: Date, default: Date.now },
});
managerChatChunkSchema.index({ type: 1 });
managerChatChunkSchema.index({ assignedto: 1 });
const ManagerChatChunk = mongoose.model("ManagerChatChunk", managerChatChunkSchema);

// Short rolling conversation memory per admin number, so follow-up
// questions like "what about last week?" keep context.
const managerChatSessionSchema = new mongoose.Schema({
  number: { type: String, required: true, unique: true },
  messages: [{
    role: { type: String, enum: ["user", "assistant"] },
    text: String,
    timestamp: { type: Date, default: Date.now },
  }],
  updatedAt: { type: Date, default: Date.now },
});
const ManagerChatSession = mongoose.model("ManagerChatSession", managerChatSessionSchema);

// ─── IST date helpers (mirrors index.js's getISTDateString/getISTDayBounds) ─
function getISTDateString(d = new Date()) {
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

// Returns both {start,end} (used by the RAG aggregates) and {dayStart,dayEnd}
// (the names the moved check-in code expects).
function getISTDayBounds(d = new Date()) {
  const dateStr = getISTDateString(d);
  const start = new Date(`${dateStr}T00:00:00.000+05:30`);
  const end = new Date(`${dateStr}T23:59:59.999+05:30`);
  return { start, end, dayStart: start, dayEnd: end };
}

function getISTMonthBounds(year, month /* 1-12 */) {
  const start = new Date(`${year}-${String(month).padStart(2, "0")}-01T00:00:00.000+05:30`);
  const lastDay = new Date(year, month, 0).getDate();
  const end = new Date(`${year}-${String(month).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}T23:59:59.999+05:30`);
  const key = `${year}-${String(month).padStart(2, "0")}`;
  const label = start.toLocaleDateString("en-IN", { month: "long", year: "numeric", timeZone: "Asia/Kolkata" });
  return { start, end, key, label };
}

function getISTWeekBounds(d = new Date()) {
  const [y, m, day] = getISTDateString(d).split("-").map(Number);
  const noon = new Date(Date.UTC(y, m - 1, day, 12));
  const dow = noon.getUTCDay(); // 0=Sun..6=Sat
  const diffToMonday = (dow + 6) % 7;
  const monday = new Date(noon);
  monday.setUTCDate(noon.getUTCDate() - diffToMonday);
  const mondayStr = monday.toISOString().slice(0, 10);
  const sunday = new Date(monday);
  sunday.setUTCDate(monday.getUTCDate() + 6);
  const sundayStr = sunday.toISOString().slice(0, 10);
  return {
    start: new Date(`${mondayStr}T00:00:00.000+05:30`),
    end: new Date(`${sundayStr}T23:59:59.999+05:30`),
  };
}

// Report-style bounds — plain local-TZ setHours(), matching EXACTLY what
// /reports/user-activity, /reports/monthly-activity and the 10 PM daily
// report cron use, so numbers quoted by the chatbot agree with the dashboard.
function reportDayBounds(d = new Date()) {
  const start = new Date(d); start.setHours(0, 0, 0, 0);
  const end = new Date(d); end.setHours(23, 59, 59, 999);
  return { start, end };
}
function reportMonthBounds(year, month /* 1-12 */) {
  const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
  const end = new Date(year, month, 0, 23, 59, 59, 999);
  return { start, end };
}

function shiftMonth(year, month, delta) {
  const total = year * 12 + (month - 1) + delta;
  return { year: Math.floor(total / 12), month: (total % 12) + 1 };
}

function inr(n) {
  return `₹${Math.round(Number(n) || 0).toLocaleString("en-IN")}`;
}

function fmtDate(d) {
  return new Date(d).toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
}

function fmtRange(start, end) {
  const s = fmtDate(start), e = fmtDate(end);
  return s === e ? s : `${s} to ${e}`;
}

// Small pure helper copied from index.js (cleanIndianMobileNumber).
function cleanIndianMobileNumber(input) {
  if (!input) return null;
  let digits = input.toString().replace(/\D/g, "");
  if (digits.startsWith("91") && digits.length === 12) return digits;
  if (digits.length === 10) return "91" + digits;
  if (digits.length === 11 && digits.startsWith("0")) return "91" + digits.slice(1);
  return digits;
}

// Internal/test counsellor accounts (EXCLUDED_CHECKIN_NUMBERS) that manager
// check-ins and reports must skip. Compare on the normalised mobile, since
// User.userNumber formatting is free-form.
const _excludedCheckinNumbers = new Set(
  EXCLUDED_CHECKIN_NUMBERS.map(cleanIndianMobileNumber).filter(Boolean)
);
function isExcludedCounsellor(userDoc) {
  return _excludedCheckinNumbers.has(cleanIndianMobileNumber(userDoc && userDoc.userNumber));
}
// Drop excluded accounts from a list of User docs (each must carry userNumber).
function filterExcludedCounsellors(userDocs) {
  return (userDocs || []).filter(u => !isExcludedCounsellor(u));
}

function participatesInManagerReview(userDoc) {
  if (!userDoc || isExcludedCounsellor(userDoc)) return false;
  if (userDoc.managerReviewEnabled === false) return false;
  return !REVIEW_DISABLED_USERNAMES.has(String(userDoc.username || "").trim().toLowerCase());
}

function filterManagerReviewCounsellors(userDocs) {
  return (userDocs || []).filter(participatesInManagerReview);
}

// Mirrors index.js's TERMINAL_PIPELINES (the auto-reassignment cron's definition of
// "no longer active") so the Director Review aggregates agree with the rest of the CRM.
const MC_TERMINAL_PIPELINES = [/^on hold$/i, /^(enrolled|converted)$/i, /^looking for job$/i, /^not interested$/i, /^not intrested$/i];
function isTerminalPipeline(stage) {
  return MC_TERMINAL_PIPELINES.some(re => re.test(String(stage || "New")));
}

// ─── Live CRM aggregations ──────────────────────────────────────────────
async function getRevenueSummary(start, end) {
  const rows = await Chat.aggregate([
    { $project: {
      assignedto: 1,
      firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] },
      billedAmount: { $ifNull: ["$fees.billedAmount", 0] },
      installments: { $ifNull: ["$fees.installments", []] },
    } },
    { $addFields: {
      billedInRange: {
        $cond: [
          { $and: [{ $ifNull: ["$firstInstDueDate", false] }, { $gte: ["$firstInstDueDate", start] }, { $lte: ["$firstInstDueDate", end] }] },
          "$billedAmount", 0,
        ],
      },
      paidInRange: {
        $sum: {
          $map: {
            input: "$installments", as: "i",
            in: {
              $cond: [
                { $and: [{ $eq: ["$$i.status", "Paid"] }, { $ifNull: ["$$i.dueDate", false] }, { $gte: ["$$i.dueDate", start] }, { $lte: ["$$i.dueDate", end] }] },
                { $ifNull: ["$$i.amount", 0] }, 0,
              ],
            },
          },
        },
      },
    } },
    { $match: { $or: [{ billedInRange: { $gt: 0 } }, { paidInRange: { $gt: 0 } }] } },
    { $group: { _id: "$assignedto", billed: { $sum: "$billedInRange" }, paid: { $sum: "$paidInRange" } } },
  ]);
  const totalBilled = rows.reduce((s, r) => s + (r.billed || 0), 0);
  const totalPaid = rows.reduce((s, r) => s + (r.paid || 0), 0);
  const byCounsellor = rows.filter(r => r._id).map(r => ({ counsellor: r._id, billed: r.billed || 0, paid: r.paid || 0 })).sort((a, b) => b.paid - a.paid);
  return { totalBilled, totalPaid, byCounsellor };
}

async function getNewLeadsSummary(start, end) {
  const rows = await Chat.aggregate([
    { $match: { datecreated: { $gte: start, $lte: end } } },
    { $group: { _id: { course: { $ifNull: ["$course", "Unspecified"] }, assignedto: { $ifNull: ["$assignedto", "Unassigned"] } }, count: { $sum: 1 } } },
  ]);
  const byCourse = {}, byCounsellor = {};
  let total = 0;
  for (const r of rows) {
    total += r.count;
    byCourse[r._id.course] = (byCourse[r._id.course] || 0) + r.count;
    byCounsellor[r._id.assignedto] = (byCounsellor[r._id.assignedto] || 0) + r.count;
  }
  return { total, byCourse, byCounsellor };
}

// Leads created in [start,end] that still aren't Enrolled as of *now*.
async function getNotConvertedLeads(start, end) {
  const leads = await Chat.find({
    datecreated: { $gte: start, $lte: end },
    pipeline: { $not: /^(enrolled|converted)$/i },
  }).select("userName course pipeline assignedto").lean();

  const byStage = {};
  for (const l of leads) {
    const stage = l.pipeline || "New Lead";
    byStage[stage] = (byStage[stage] || 0) + 1;
  }
  const sample = leads.slice(0, 20).map(l => `${l.userName} (${l.course || "N/A"}, stage: ${l.pipeline || "New Lead"}, with ${l.assignedto || "unassigned"})`);
  return { total: leads.length, byStage, sample };
}

async function getPipelineChangesSummary(start, end) {
  const changes = await PipelineChange.find({ changedAt: { $gte: start, $lte: end } }).sort({ changedAt: -1 }).limit(150).lean();
  const byCounsellor = {}, transitions = {};
  for (const c of changes) {
    const who = c.assignedto || "Unassigned";
    byCounsellor[who] = (byCounsellor[who] || 0) + 1;
    const key = `${c.fromStage || "New Lead"} -> ${c.toStage}`;
    transitions[key] = (transitions[key] || 0) + 1;
  }
  const sample = changes.slice(0, 25).map(c =>
    `${c.userName || "Lead"}: ${c.fromStage || "New Lead"} -> ${c.toStage} (by ${c.assignedto || "unassigned"}, ${new Date(c.changedAt).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" })})`
  );
  return { total: changes.length, byCounsellor, transitions, sample };
}

const REMARK_STOPWORDS = new Set(["the", "and", "for", "with", "that", "this", "will", "have", "has", "not", "are", "was", "were", "been", "from", "they", "them", "she", "him", "his", "her", "its", "who", "whom", "call", "called", "said", "told", "asked", "also", "today", "tomorrow", "about", "then", "need", "want", "just", "still", "yet", "but", "you", "your", "our", "out", "into", "over", "after", "before", "again"]);

async function getRemarksSummary(start, end) {
  const rows = await Chat.aggregate([
    { $unwind: "$remarks" },
    { $match: { "remarks.timestamp": { $gte: start, $lte: end } } },
    { $project: { userName: 1, course: 1, assignedto: 1, remark: "$remarks.remark", timestamp: "$remarks.timestamp" } },
    { $sort: { timestamp: -1 } },
    { $limit: 200 },
  ]);

  const wordFreq = {};
  const byCounsellor = {};
  for (const r of rows) {
    byCounsellor[r.assignedto || "Unassigned"] = (byCounsellor[r.assignedto || "Unassigned"] || 0) + 1;
    const words = String(r.remark || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(w => w.length > 3 && !REMARK_STOPWORDS.has(w));
    for (const w of words) wordFreq[w] = (wordFreq[w] || 0) + 1;
  }
  const topWords = Object.entries(wordFreq).sort((a, b) => b[1] - a[1]).slice(0, 15).map(([w, c]) => `${w} (${c})`);
  const sample = rows.slice(0, 50).map(r => `${r.userName} (${r.course || "N/A"}, ${r.assignedto || "unassigned"}): "${r.remark}"`);
  return { total: rows.length, topWords, byCounsellor, sample };
}

async function getCounsellorActivitySummary(start, end) {
  const counsellors = filterExcludedCounsellors(
    await User.find({ role: "user" }).select("username userNumber").lean()
  );
  const results = [];
  for (const c of counsellors) {
    const [remarksAgg, newLeads, enrolled] = await Promise.all([
      Chat.aggregate([
        { $match: { assignedto: c.username } },
        { $unwind: "$remarks" },
        { $match: { "remarks.timestamp": { $gte: start, $lte: end } } },
        { $count: "n" },
      ]),
      Chat.countDocuments({ assignedto: c.username, datecreated: { $gte: start, $lte: end } }),
      Chat.countDocuments({ assignedto: c.username, pipeline: /^(enrolled|converted)$/i, lastInteracted: { $gte: start, $lte: end } }),
    ]);
    results.push({
      username: c.username,
      remarksLogged: remarksAgg[0]?.n || 0,
      newLeadsAssigned: newLeads,
      enrolledInRange: enrolled,
    });
  }
  return results;
}

async function getCoursePerformanceSummary(monthsBack = 3) {
  const now = new Date();
  const startInfo = shiftMonth(now.getFullYear(), now.getMonth() + 1, -(monthsBack - 1));
  const { start } = getISTMonthBounds(startInfo.year, startInfo.month);
  const end = now;

  const rows = await Chat.aggregate([
    { $match: { datecreated: { $gte: start, $lte: end } } },
    { $group: {
      _id: { $ifNull: ["$course", "Unspecified"] },
      totalLeads: { $sum: 1 },
      enrolled: { $sum: { $cond: [{ $regexMatch: { input: { $ifNull: ["$pipeline", ""] }, regex: /^(enrolled|converted)$/i } }, 1, 0] } },
      revenue: { $sum: { $sum: { $map: { input: { $ifNull: ["$fees.installments", []] }, as: "i", in: { $cond: [{ $eq: ["$$i.status", "Paid"] }, { $ifNull: ["$$i.amount", 0] }, 0] } } } } },
    } },
    { $sort: { totalLeads: -1 } },
    { $limit: 10 },
  ]);

  const topCourses = rows.slice(0, 3).map(r => r._id);
  const remarkSamples = {};
  for (const course of topCourses) {
    const leads = await Chat.find({ course, datecreated: { $gte: start, $lte: end } })
      .select("remarks").sort({ lastInteracted: -1 }).limit(15).lean();
    const remarks = [];
    for (const l of leads) {
      const last = (l.remarks || []).slice(-1)[0];
      if (last?.remark) remarks.push(last.remark);
      if (remarks.length >= 8) break;
    }
    remarkSamples[course] = remarks;
  }

  return {
    start, end,
    courses: rows.map(r => ({
      course: r._id,
      totalLeads: r.totalLeads,
      enrolled: r.enrolled,
      conversionPct: r.totalLeads ? Math.round((r.enrolled / r.totalLeads) * 100) : 0,
      revenue: r.revenue,
    })),
    remarkSamples,
  };
}

async function getManagerCheckinDigest(dateStr) {
  return ManagerCheckIn.find({ date: dateStr })
    .select("counsellorUsername status verdict verdictReason adminSummaryReport")
    .lean();
}

// Per-counsellor daily-report rows, computed the SAME way as the 10 PM daily
// report cron in index.js (local-TZ bounds). Reused for today's live figures
// and for the reconstructed historical daily chunks.
async function computeDailyReportRows(start, end) {
  return Chat.aggregate([
    { $match: { assignedto: { $exists: true, $ne: null } } },
    { $addFields: {
      isNewLeadOnDate: {
        $and: [
          { $gte: ["$datecreated", start] }, { $lte: ["$datecreated", end] },
          { $not: { $and: [
            { $eq: [{ $toLower: { $ifNull: ["$pipeline", ""] } }, "enrolled"] },
            { $eq: [{ $toLower: { $ifNull: ["$course", ""] } }, "college internship"] },
          ] } },
        ],
      },
    } },
    { $addFields: {
      dateRemarks: { $filter: { input: { $ifNull: ["$remarks", []] }, as: "r", cond: { $and: [{ $gte: ["$$r.timestamp", start] }, { $lte: ["$$r.timestamp", end] }] } } },
    } },
    { $addFields: {
      paidOnDateInstallments: { $filter: { input: { $ifNull: ["$fees.installments", []] }, as: "i", cond: { $and: [{ $eq: ["$$i.status", "Paid"] }, { $gte: ["$$i.dueDate", start] }, { $lte: ["$$i.dueDate", end] }] } } },
      firstInstDueDate: { $let: { vars: { first: { $arrayElemAt: ["$fees.installments", 0] } }, in: { $ifNull: ["$$first.dueDate", null] } } },
      reminderOnDateFlag: { $cond: [{ $and: [{ $ifNull: ["$reminder", false] }, { $gte: ["$reminder", start] }, { $lte: ["$reminder", end] }] }, 1, 0] },
    } },
    { $addFields: {
      paidOnDateAmount: { $sum: { $map: { input: "$paidOnDateInstallments", as: "i", in: { $ifNull: ["$$i.amount", 0] } } } },
      billedOnDateAmount: { $cond: [{ $and: [{ $ifNull: ["$firstInstDueDate", false] }, { $gte: ["$firstInstDueDate", start] }, { $lte: ["$firstInstDueDate", end] }] }, { $ifNull: ["$fees.billedAmount", 0] }, 0] },
    } },
    { $group: {
      _id: "$assignedto",
      newLeadsToday: { $sum: { $cond: ["$isNewLeadOnDate", 1, 0] } },
      newLeadsMissed: { $sum: { $cond: [{ $and: ["$isNewLeadOnDate", { $eq: [{ $size: "$dateRemarks" }, 0] }] }, 1, 0] } },
      oldRemarksCount: { $sum: { $cond: [{ $not: "$isNewLeadOnDate" }, { $size: "$dateRemarks" }, 0] } },
      newRemarksCount: { $sum: { $cond: ["$isNewLeadOnDate", { $size: "$dateRemarks" }, 0] } },
      feesUpdatedToday: { $sum: "$paidOnDateAmount" },
      billedTodayTotal: { $sum: "$billedOnDateAmount" },
      remindersDone: { $sum: { $cond: [{ $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $gt: [{ $size: "$dateRemarks" }, 0] }] }, 1, 0] } },
      remindersMissed: { $sum: { $cond: [{ $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $eq: [{ $size: "$dateRemarks" }, 0] }] }, 1, 0] } },
    } },
    { $project: { _id: 0, username: "$_id", newLeadsToday: 1, newLeadsMissed: 1, oldRemarksCount: 1, newRemarksCount: 1, feesUpdatedToday: 1, billedTodayTotal: 1, remindersDone: 1, remindersMissed: 1 } },
    { $sort: { username: 1 } },
  ]);
}

// Per-counsellor monthly-report rows, mirroring /reports/monthly-activity.
async function computeMonthlyReportRows(start, end) {
  return Chat.aggregate([
    { $match: { assignedto: { $exists: true, $ne: null } } },
    { $addFields: {
      isNewLeadInMonth: {
        $and: [
          { $gte: ["$datecreated", start] }, { $lte: ["$datecreated", end] },
          { $or: [{ $ne: ["$course", "College Internship"] }, { $not: [{ $in: ["$pipeline", ["Converted", "Enrolled"]] }] }] },
        ],
      },
      monthRemarks: { $filter: { input: { $ifNull: ["$remarks", []] }, as: "r", cond: { $and: [{ $gte: ["$$r.timestamp", start] }, { $lte: ["$$r.timestamp", end] }] } } },
      firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] },
      paidInstallmentsInMonth: { $filter: { input: { $ifNull: ["$fees.installments", []] }, as: "i", cond: { $and: [{ $eq: ["$$i.status", "Paid"] }, { $ifNull: ["$$i.dueDate", false] }, { $gte: ["$$i.dueDate", start] }, { $lte: ["$$i.dueDate", end] }] } } },
    } },
    { $group: {
      _id: "$assignedto",
      newLeads: { $sum: { $cond: ["$isNewLeadInMonth", 1, 0] } },
      totalRemarks: { $sum: { $size: "$monthRemarks" } },
      totalPaid: { $sum: { $sum: { $map: { input: "$paidInstallmentsInMonth", as: "i", in: { $ifNull: ["$$i.amount", 0] } } } } },
      totalBilled: { $sum: { $cond: [{ $and: [{ $ifNull: ["$firstInstDueDate", false] }, { $gte: ["$firstInstDueDate", start] }, { $lte: ["$firstInstDueDate", end] }] }, { $ifNull: ["$fees.billedAmount", 0] }, 0] } },
    } },
    { $project: { _id: 0, username: "$_id", newLeads: 1, totalRemarks: 1, totalPaid: 1, totalBilled: 1 } },
    { $sort: { username: 1 } },
  ]);
}

async function getWeeklyReportDigestFor(weekStart) {
  const rows = await WeeklyReport.find({ weekStart }).lean();
  if (!rows.length) return null;
  const minmini = await MinminiReport.findOne({ weekStart }).lean();
  return { weekStart, weekEnd: rows[0].weekEnd, counsellors: rows, teamStats: minmini };
}

async function getMonthlyReportDigestFor(monthStart) {
  const rows = await MonthlyReport.find({ monthStart }).lean();
  if (!rows.length) return null;
  const minmini = await MonthlyMinminiReport.findOne({ monthStart }).lean();
  return { monthStart, monthEnd: rows[0].monthEnd, counsellors: rows, teamStats: minmini };
}

// ─── Director Review Framework aggregations ────────────────────────────
// Section 1: branch + per-counsellor target, rupee gap, and pace forecast for
// the current month — one grouped query instead of computeTargetAchievement()
// called N times.
async function getBranchTargetAchievement() {
  const now = new Date();
  const year = now.getFullYear(), month = now.getMonth() + 1;
  const { start, end } = reportMonthBounds(year, month);
  const daysInMonth = new Date(year, month, 0).getDate();
  const dayOfMonth = Math.min(daysInMonth, Math.max(1, now.getDate()));
  const daysRemaining = Math.max(0, daysInMonth - dayOfMonth);
  const expectedPacePct = Math.min(100, Math.round((dayOfMonth / daysInMonth) * 100));

  const [targets, counsellors, paidRows] = await Promise.all([
    UserMonthlyTarget.find({ year, month }).lean(),
    User.find({ role: "user" }).select("username userNumber").lean(),
    Chat.aggregate([
      { $match: { assignedto: { $exists: true, $ne: null } } },
      { $unwind: { path: "$fees.installments", preserveNullAndEmptyArrays: false } },
      { $match: { "fees.installments.status": "Paid", "fees.installments.dueDate": { $gte: start, $lte: end } } },
      { $group: { _id: "$assignedto", totalPaid: { $sum: { $ifNull: ["$fees.installments.amount", 0] } } } },
    ]),
  ]);

  const names = filterExcludedCounsellors(counsellors).map(c => c.username);
  const targetByUser = new Map(targets.map(t => [t.username, t.target || 0]));
  const paidByUser = new Map(paidRows.map(r => [r._id, r.totalPaid || 0]));

  const byCounsellor = names.map(username => {
    const target = targetByUser.get(username) || 0;
    const paid = paidByUser.get(username) || 0;
    const gap = Math.max(0, target - paid);
    const achievementPct = target > 0 ? Math.round((paid / target) * 100) : null;
    return {
      username, target, paid, gap, achievementPct, expectedPacePct,
      onPace: achievementPct === null ? null : achievementPct >= expectedPacePct,
      projectedMonthEnd: Math.round(paid / (dayOfMonth / daysInMonth)),
    };
  }).sort((a, b) => b.gap - a.gap);

  const branchTarget = byCounsellor.reduce((s, c) => s + c.target, 0);
  const branchPaid = byCounsellor.reduce((s, c) => s + c.paid, 0);
  const branchGap = Math.max(0, branchTarget - branchPaid);

  return {
    year, month, daysInMonth, dayOfMonth, daysRemaining, expectedPacePct,
    branchTarget, branchPaid, branchGap,
    branchAchievementPct: branchTarget > 0 ? Math.round((branchPaid / branchTarget) * 100) : null,
    dailyRunRateNeeded: daysRemaining > 0 ? Math.round(branchGap / daysRemaining) : branchGap,
    projectedMonthEnd: Math.round(branchPaid / (dayOfMonth / daysInMonth)),
    byCounsellor,
  };
}

// Per-counsellor conversion breakdown for one period (cohort basis) — mirrors
// getConversionByCourse but grouped by assignedto. Feeds the Sec 1/4 volume-vs-
// conversion diagnosis and the Sec 2 this-month-vs-last-month trend.
async function getConversionByCounsellor(start, end) {
  return Chat.aggregate([
    { $match: { assignedto: { $nin: [null, ""] } } },
    { $project: {
      assignedto: 1,
      isEnrolled: { $regexMatch: { input: { $ifNull: ["$pipeline", ""] }, regex: /^(enrolled|converted)$/i } },
      createdInPeriod: { $and: [{ $gte: ["$datecreated", start] }, { $lte: ["$datecreated", end] }] },
    } },
    { $group: {
      _id: "$assignedto",
      newLeads: { $sum: { $cond: ["$createdInPeriod", 1, 0] } },
      conversions: { $sum: { $cond: [{ $and: ["$createdInPeriod", "$isEnrolled"] }, 1, 0] } },
    } },
    { $match: { $or: [{ newLeads: { $gt: 0 } }, { conversions: { $gt: 0 } }] } },
    { $sort: { conversions: -1, newLeads: -1 } },
  ]);
}

// Section 2: new leads grouped by lead source (leadfrom). Kept separate from
// getNewLeadsSummary so that function's existing shape/callers stay untouched.
async function getLeadSourceSummary(start, end) {
  const rows = await Chat.aggregate([
    { $match: { datecreated: { $gte: start, $lte: end } } },
    { $group: { _id: { $ifNull: ["$leadfrom", "Unspecified"] }, count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]);
  const total = rows.reduce((s, r) => s + r.count, 0);
  return { total, bySource: rows.map(r => ({ source: r._id, count: r.count })) };
}

// Section 2/4/8: active (non-terminal) leads bucketed by hours since last touch
// (24-48h / 48-72h / 72h+), named per counsellor. This is the single most
// important output the framework asks for — specific stale leads by name.
async function getStaleLeadBuckets() {
  const leads = await Chat.find({
    assignedto: { $nin: [null, ""] },
    pipeline: { $nin: MC_TERMINAL_PIPELINES },
  }).select("userName course assignedto pipeline datecreated lastInteracted remarks").lean();

  const now = Date.now();
  const buckets = { "24-48h": [], "48-72h": [], "72h+": [] };
  const countsByCounsellor = { "24-48h": {}, "48-72h": {}, "72h+": {} };

  for (const l of leads) {
    const remarkTimes = (l.remarks || []).map(r => r.timestamp && new Date(r.timestamp).getTime()).filter(Boolean);
    const lastTouch = Math.max(
      l.lastInteracted ? new Date(l.lastInteracted).getTime() : 0,
      remarkTimes.length ? Math.max(...remarkTimes) : 0,
      l.datecreated ? new Date(l.datecreated).getTime() : 0,
    );
    const hours = (now - lastTouch) / 3600000;
    let bucket = null;
    if (hours >= 72) bucket = "72h+";
    else if (hours >= 48) bucket = "48-72h";
    else if (hours >= 24) bucket = "24-48h";
    if (!bucket) continue;
    countsByCounsellor[bucket][l.assignedto] = (countsByCounsellor[bucket][l.assignedto] || 0) + 1;
    if (buckets[bucket].length < 15) {
      buckets[bucket].push({ userName: l.userName, course: l.course || "N/A", assignedto: l.assignedto, hoursStale: Math.round(hours) });
    }
  }
  return { buckets, countsByCounsellor };
}

// Section 2/8: demo scheduling funnel. "Scheduled"/"completed" are read off the
// PipelineChange audit log (real data); "no-show" has no CRM field, so it is a
// labeled proxy — currently at "Site Visit" with no activity in 3+ days.
async function getDemoFunnelSummary(start, end) {
  const [scheduledChanges, completedChanges, currentlyScheduled] = await Promise.all([
    PipelineChange.countDocuments({ changedAt: { $gte: start, $lte: end }, toStage: /^(site visit|demo scheduled)$/i }),
    PipelineChange.countDocuments({ changedAt: { $gte: start, $lte: end }, fromStage: /^(site visit|demo scheduled)$/i }),
    Chat.find({ pipeline: /^(site visit|demo scheduled)$/i }).select("userName course assignedto lastInteracted remarks").lean(),
  ]);
  const cutoff = Date.now() - 3 * 864e5;
  const noShowProxy = currentlyScheduled.filter(l => {
    const remarkTimes = (l.remarks || []).map(r => r.timestamp && new Date(r.timestamp).getTime()).filter(Boolean);
    const lastTouch = Math.max(l.lastInteracted ? new Date(l.lastInteracted).getTime() : 0, remarkTimes.length ? Math.max(...remarkTimes) : 0);
    return lastTouch < cutoff;
  }).map(l => ({ userName: l.userName, course: l.course || "N/A", assignedto: l.assignedto || "unassigned" }));

  return { scheduled: scheduledChanges, completed: completedChanges, currentlyAtDemoStage: currentlyScheduled.length, noShowProxy };
}

// Section 2/7: every Hot/Warm not-yet-enrolled lead branch-wide, with its last
// remark/chat so the chatbot can name the specific blocker from real text
// instead of a generic nudge.
async function getHotLeadsBranchWide() {
  const leads = await Chat.find({
    status: /^(hot|warm)$/i,
    pipeline: { $nin: MC_TERMINAL_PIPELINES },
  }).select("userName course assignedto status pipeline remarks conversations").lean();
  return leads.map(l => ({
    userName: l.userName, course: l.course || "N/A", assignedto: l.assignedto || "unassigned",
    status: l.status, pipeline: l.pipeline || "New Lead",
    lastRemark: lastRemarkText(l), lastConvo: lastConvoSnippet(l),
  }));
}

// Section 3: seat fill for a configured course (e.g. the Job Guarantee Program).
// Seat capacity is NOT a CRM field — it's the configured constant passed in;
// this only counts real Enrolled leads against it, grouped by batch_date cohort.
async function getCourseSeatFillStatus(courseName, seatsPerBatch) {
  const rows = await Chat.aggregate([
    { $match: { course: courseName, pipeline: /^(enrolled|converted)$/i } },
    { $group: { _id: { $ifNull: ["$batch_date", "Unscheduled"] }, count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]);
  return {
    courseName, seatsPerBatch,
    batches: rows.map(r => ({ batchDate: r._id, enrolled: r.count, seatsLeft: Math.max(0, seatsPerBatch - r.count) })),
  };
}

// Section 3: upcoming batch cohorts (batch_date parses to today or later) and
// how many leads are enrolled/in-pipeline for each, to flag under-filled start
// dates. batch_date is a free-text field (set from index.js /chat/assign-batch,
// not schema-validated) so dates are parsed/filtered in JS rather than trusting
// Mongo string comparison to sort them correctly.
async function getUpcomingBatchFillSummary() {
  const rows = await Chat.aggregate([
    { $match: { batch_date: { $nin: [null, ""] } } },
    { $group: {
      _id: { course: { $ifNull: ["$course", "Unspecified"] }, batchDate: "$batch_date" },
      total: { $sum: 1 },
      enrolled: { $sum: { $cond: [{ $regexMatch: { input: { $ifNull: ["$pipeline", ""] }, regex: /^(enrolled|converted)$/i } }, 1, 0] } },
    } },
  ]);
  const now = Date.now();
  return rows
    .map(r => ({ course: r._id.course, batchDate: r._id.batchDate, parsedAt: new Date(r._id.batchDate).getTime(), total: r.total, enrolled: r.enrolled }))
    .filter(r => !isNaN(r.parsedAt) && r.parsedAt >= now - 864e5)
    .sort((a, b) => a.parsedAt - b.parsedAt)
    .slice(0, 20);
}

// Section 4/6/8: trailing check-in scorecards. metrics.scorecard is populated by
// applyScoreBasedVerdict on every completed check-in, so this reads the score
// back rather than re-deriving it — who's >=7/10, who's <4/10, and the trend.
async function getRecentScorecardHistory(days = 7) {
  const since = getISTDateString(new Date(Date.now() - days * 864e5));
  const rows = await ManagerCheckIn.find({ date: { $gte: since }, status: "completed" })
    .select("counsellorUsername date metrics.scorecard verdict verdictReason")
    .sort({ date: -1 })
    .lean();
  return rows.map(r => ({
    username: r.counsellorUsername,
    date: r.date,
    compositePct: r.metrics?.scorecard?.compositePct ?? null,
    band: r.metrics?.scorecard?.band || null,
    verdict: r.verdict,
    verdictReason: r.verdictReason,
  }));
}

// Section 5: remark-text search for named competitors. No CRM field backs this —
// the answer is only as good as what counsellors typed into remarks.
async function getCompetitorMentions(start, end, competitors = KNOWN_COMPETITORS) {
  if (!competitors.length) return [];
  const pattern = new RegExp(competitors.map(escapeRegex).join("|"), "i");
  const rows = await Chat.aggregate([
    { $unwind: "$remarks" },
    { $match: { "remarks.timestamp": { $gte: start, $lte: end }, "remarks.remark": pattern } },
    { $project: { userName: 1, course: 1, assignedto: 1, remark: "$remarks.remark", timestamp: "$remarks.timestamp" } },
    { $sort: { timestamp: -1 } },
    { $limit: 30 },
  ]);
  return rows.map(r => ({
    userName: r.userName, course: r.course || "N/A", assignedto: r.assignedto || "unassigned",
    remark: r.remark, timestamp: r.timestamp, competitor: (String(r.remark).match(pattern) || [])[0] || null,
  }));
}

const OBJECTION_KEYWORDS = {
  price: ["expensive", "cost", "afford", "budget", "discount", "emi", "price", "costly", "cheaper", "money"],
  timing: ["later", "busy", "exam", "next month", "after", "postpone", "not now", "think about", "some time"],
  trust: ["fake", "scam", "trust", "genuine", "verify", "doubt", "not sure", "authentic"],
};
// Section 5: deterministic keyword-bucket signal over remark text (not an LLM
// call) — how many remarks in [start,end] mention each objection theme. No CRM
// field tags objections, so this is a rough text signal, not an exact count.
async function classifyObjectionThemes(start, end) {
  const rows = await Chat.aggregate([
    { $unwind: "$remarks" },
    { $match: { "remarks.timestamp": { $gte: start, $lte: end } } },
    { $project: { remark: { $toLower: { $ifNull: ["$remarks.remark", ""] } } } },
    { $limit: 2000 },
  ]);
  const counts = { price: 0, timing: 0, trust: 0 };
  for (const r of rows) {
    const text = String(r.remark || "");
    for (const [theme, words] of Object.entries(OBJECTION_KEYWORDS)) {
      if (words.some(w => text.includes(w))) counts[theme]++;
    }
  }
  return { totalRemarksScanned: rows.length, counts };
}

// Section 6: branch-wide commitment follow-through, reusing the per-counsellor
// loadYesterdayCommitments/computeCommitmentFollowThrough the check-in already uses.
async function getBranchCommitmentFollowThrough() {
  const counsellors = filterExcludedCounsellors(await User.find({ role: "user" }).select("username").lean());
  const today = getISTDateString();
  const { dayStart, dayEnd } = getISTDayBounds();
  const results = [];
  for (const c of counsellors) {
    const commitments = await loadYesterdayCommitments(c.username, today).catch(() => []);
    if (!commitments.length) { results.push({ username: c.username, total: 0, kept: 0, missed: 0, pct: null }); continue; }
    const ft = await computeCommitmentFollowThrough(c.username, commitments, dayStart, dayEnd).catch(() => ({ total: 0, kept: 0, missed: 0, pct: null, details: [] }));
    results.push({ username: c.username, total: ft.total, kept: ft.kept, missed: ft.missed, pct: ft.pct, details: ft.details });
  }
  return results;
}

// Section 8: this-week branch scorecard vs last week and vs monthly target —
// composes the aggregates above rather than re-querying.
async function getBranchWeeklyScorecard() {
  const now = new Date();
  const thisWeek = getISTWeekBounds(now);
  const lastWeekAnchor = new Date(now); lastWeekAnchor.setDate(lastWeekAnchor.getDate() - 7);
  const lastWeek = getISTWeekBounds(lastWeekAnchor);

  const [
    revenueThisWeek, revenueLastWeek,
    leadsThisWeek, leadsLastWeek,
    conversionThisWeek,
    staleBuckets, demoFunnel, targetAchievement, jgpFill, scorecardHistory,
  ] = await Promise.all([
    getRevenueSummary(thisWeek.start, thisWeek.end),
    getRevenueSummary(lastWeek.start, lastWeek.end),
    getNewLeadsSummary(thisWeek.start, thisWeek.end),
    getNewLeadsSummary(lastWeek.start, lastWeek.end),
    getConversionByCounsellor(thisWeek.start, thisWeek.end),
    getStaleLeadBuckets(),
    getDemoFunnelSummary(thisWeek.start, thisWeek.end),
    getBranchTargetAchievement(),
    getCourseSeatFillStatus(JGP_COURSE_NAME, JGP_SEATS_PER_BATCH),
    getRecentScorecardHistory(7),
  ]);

  const conversionTotals = conversionThisWeek.reduce((acc, r) => ({ newLeads: acc.newLeads + r.newLeads, conversions: acc.conversions + r.conversions }), { newLeads: 0, conversions: 0 });
  // countsByCounsellor holds the true per-bucket totals (buckets.* arrays are capped samples).
  const sumBucketCounts = (b) => Object.values(staleBuckets.countsByCounsellor[b]).reduce((s, n) => s + n, 0);
  const untouched48hPlus = sumBucketCounts("48-72h") + sumBucketCounts("72h+");
  const demoShowRatePct = demoFunnel.scheduled > 0 ? Math.round(((demoFunnel.scheduled - demoFunnel.noShowProxy.length) / demoFunnel.scheduled) * 100) : null;
  const latestPerCounsellor = new Map();
  for (const r of scorecardHistory) if (!latestPerCounsellor.has(r.username)) latestPerCounsellor.set(r.username, r);
  const above7 = [...latestPerCounsellor.values()].filter(r => r.compositePct !== null && r.compositePct >= 70).length;
  const below4 = [...latestPerCounsellor.values()].filter(r => r.compositePct !== null && r.compositePct < 40).length;

  return {
    thisWeek, lastWeek,
    revenueThisWeek, revenueLastWeek,
    leadsThisWeek: leadsThisWeek.total, leadsLastWeek: leadsLastWeek.total,
    conversionRatePctThisWeek: conversionTotals.newLeads ? Math.round((conversionTotals.conversions / conversionTotals.newLeads) * 100) : null,
    untouched48hPlus,
    demoShowRatePct, demoFunnel,
    jgpSeatsFilled: jgpFill.batches.reduce((s, b) => s + b.enrolled, 0),
    jgpSeatsPerBatch: JGP_SEATS_PER_BATCH,
    targetAchievement,
    counsellorsAbove7: above7,
    counsellorsBelow4: below4,
    byCounsellor: targetAchievement.byCounsellor,
  };
}

// ─── Chunk text builders ────────────────────────────────────────────────
function buildRevenueText(label, start, end, d) {
  let t = `[REVENUE — ${label} (${fmtRange(start, end)})]\n`;
  t += `Total Billed: ${inr(d.totalBilled)}\nTotal Paid: ${inr(d.totalPaid)}\nPending: ${inr(d.totalBilled - d.totalPaid)}\n`;
  if (d.byCounsellor.length) {
    t += `By counsellor:\n${d.byCounsellor.map(c => `- ${c.counsellor}: billed ${inr(c.billed)}, paid ${inr(c.paid)}`).join("\n")}`;
  } else {
    t += `No billed/paid installments due in this period.`;
  }
  return t;
}

function buildLeadsText(label, start, end, d) {
  let t = `[NEW LEADS — ${label} (${fmtRange(start, end)})]\nTotal new leads: ${d.total}\n`;
  const courses = Object.entries(d.byCourse).sort((a, b) => b[1] - a[1]);
  if (courses.length) t += `By course:\n${courses.map(([c, n]) => `- ${c}: ${n}`).join("\n")}\n`;
  const counsellors = Object.entries(d.byCounsellor).sort((a, b) => b[1] - a[1]);
  if (counsellors.length) t += `By counsellor:\n${counsellors.map(([c, n]) => `- ${c}: ${n}`).join("\n")}`;
  return t;
}

function buildNotConvertedText(label, start, end, d) {
  let t = `[LEADS NOT YET CONVERTED — created ${label} (${fmtRange(start, end)}), still not Converted as of now]\nTotal: ${d.total}\n`;
  const stages = Object.entries(d.byStage).sort((a, b) => b[1] - a[1]);
  if (stages.length) t += `By current stage:\n${stages.map(([s, n]) => `- ${s}: ${n}`).join("\n")}\n`;
  if (d.sample.length) t += `Sample leads:\n${d.sample.map(s => `- ${s}`).join("\n")}`;
  return t;
}

function buildPipelineText(label, start, end, d) {
  let t = `[PIPELINE STAGE CHANGES — ${label} (${fmtRange(start, end)})]\nTotal changes: ${d.total}\n`;
  if (!d.total) {
    t += `(No pipeline-change records for this period — the audit log only tracks changes made after this feature was deployed.)`;
    return t;
  }
  const transitions = Object.entries(d.transitions).sort((a, b) => b[1] - a[1]);
  if (transitions.length) t += `By transition:\n${transitions.map(([k, n]) => `- ${k}: ${n}`).join("\n")}\n`;
  const byCounsellor = Object.entries(d.byCounsellor).sort((a, b) => b[1] - a[1]);
  if (byCounsellor.length) t += `By counsellor:\n${byCounsellor.map(([c, n]) => `- ${c}: ${n}`).join("\n")}\n`;
  if (d.sample.length) t += `Recent changes:\n${d.sample.map(s => `- ${s}`).join("\n")}`;
  return t;
}

function buildRemarksText(label, start, end, d) {
  let t = `[COUNSELLOR REMARKS — ${label} (${fmtRange(start, end)})]\nTotal remarks logged: ${d.total}\n`;
  const byCounsellor = Object.entries(d.byCounsellor).sort((a, b) => b[1] - a[1]);
  if (byCounsellor.length) t += `By counsellor:\n${byCounsellor.map(([c, n]) => `- ${c}: ${n}`).join("\n")}\n`;
  if (d.topWords.length) t += `Most frequent words across remarks (rough signal, use the sample text below for real themes): ${d.topWords.join(", ")}\n`;
  if (d.sample.length) t += `Sample remarks:\n${d.sample.map(s => `- ${s}`).join("\n")}`;
  return t;
}

function buildCounsellorActivityText(label, start, end, rows) {
  let t = `[COUNSELLOR ACTIVITY — ${label} (${fmtRange(start, end)})]\n`;
  t += rows.map(r => `- ${r.username}: ${r.newLeadsAssigned} new leads, ${r.remarksLogged} remarks logged, ${r.enrolledInRange} enrolled`).join("\n");
  return t;
}

function buildCourseText(d) {
  let t = `[COURSE PERFORMANCE — trailing period ${fmtRange(d.start, d.end)}]\n`;
  t += d.courses.map(c => `- ${c.course}: ${c.totalLeads} leads, ${c.enrolled} enrolled (${c.conversionPct}% conversion), ${inr(c.revenue)} collected so far`).join("\n");
  t += `\n\nRecent counsellor remarks by top courses (for judging sentiment/momentum):\n`;
  for (const [course, remarks] of Object.entries(d.remarkSamples)) {
    if (!remarks.length) continue;
    t += `${course}:\n${remarks.map(r => `  - "${r}"`).join("\n")}\n`;
  }
  return t;
}

function buildCheckinText(dateStr, rows) {
  if (!rows.length) return `[MANAGER 5PM CHECK-IN — ${dateStr}]\nNo check-in records for this date (check-ins run Mon-Sat from 5PM IST).`;
  let t = `[MANAGER 5PM CHECK-IN — ${dateStr}]\n`;
  t += rows.map(r => {
    const parts = [`${r.counsellorUsername}: status=${r.status}${r.verdict ? `, verdict=${r.verdict}` : ""}`];
    if (r.verdictReason) parts.push(`reason: ${r.verdictReason}`);
    if (r.adminSummaryReport) parts.push(`summary: ${r.adminSummaryReport}`);
    return `- ${parts.join(" | ")}`;
  }).join("\n");
  return t;
}

function buildWeeklyText(d, labelOverride) {
  if (!d) return `[WEEKLY REPORT]\nNo weekly report has been generated yet (runs Sunday 11:59PM IST).`;
  let t = `[WEEKLY REPORT — ${labelOverride || fmtRange(d.weekStart, d.weekEnd)}]\n`;
  if (d.teamStats) {
    t += `Team: ${d.teamStats.activeLeads} active leads, ${d.teamStats.enrolledAfterWA} enrolled after WhatsApp bot, ${d.teamStats.totalMessages} messages.\n`;
    if (d.teamStats.aiSummary) t += `Team summary: ${d.teamStats.aiSummary}\n`;
  }
  t += d.counsellors.map(c => {
    const m = c.metrics || {};
    const respBit = m.avgResponseMins !== undefined && m.avgResponseMins !== null ? `, avg response time ${Math.round(m.avgResponseMins)}min` : "";
    return `- ${c.counsellor}: ${m.newLeads || 0} new leads, ${m.enrolledThisWeek || 0} enrolled, billed ${inr(m.thisWeekFees?.totalBilled)}, paid ${inr(m.thisWeekFees?.totalPaid)}${respBit}${c.aiSummary ? ` — ${c.aiSummary}` : ""}`;
  }).join("\n");
  return t;
}

function buildMonthlyText(d, labelOverride) {
  if (!d) return `[MONTHLY REPORT]\nNo monthly report has been generated yet (runs 1st of month, 9AM IST).`;
  let t = `[MONTHLY REPORT — ${labelOverride || fmtRange(d.monthStart, d.monthEnd)}]\n`;
  if (d.teamStats) {
    t += `Team: ${d.teamStats.activeLeads} active leads, ${d.teamStats.enrolledAfterWA} enrolled after WhatsApp bot.\n`;
    if (d.teamStats.aiSummary) t += `Team summary: ${d.teamStats.aiSummary}\n`;
  }
  t += d.counsellors.map(c => {
    const m = c.metrics || {};
    const respBit = m.avgResponseMins !== undefined && m.avgResponseMins !== null ? `, avg response time ${Math.round(m.avgResponseMins)}min` : "";
    return `- ${c.counsellor}: ${m.newLeads || 0} new leads, ${m.enrolledThisMonth || 0} enrolled, billed ${inr(m.thisMonthFees?.totalBilled)}, paid ${inr(m.thisMonthFees?.totalPaid)}${respBit}${c.aiSummary ? ` — ${c.aiSummary}` : ""}`;
  }).join("\n");
  return t;
}

// ─── Director Review Framework — chunk text builders ───────────────────
function buildTargetGapText(d) {
  let t = `[TARGET & REVENUE GAP — ${d.year}-${String(d.month).padStart(2, "0")}, day ${d.dayOfMonth}/${d.daysInMonth}, ${d.daysRemaining} days left]\n`;
  t += `Branch target: ${inr(d.branchTarget)} | Collected (paid): ${inr(d.branchPaid)} | Gap: ${inr(d.branchGap)}\n`;
  t += `Branch achievement: ${d.branchAchievementPct === null ? "N/A (no targets set)" : `${d.branchAchievementPct}%`} vs expected pace ${d.expectedPacePct}% of month elapsed\n`;
  t += `Daily run-rate needed for the rest of the month to close the gap: ${inr(d.dailyRunRateNeeded)}/day\n`;
  t += `If nothing changes (linear pace from today), branch lands around: ${inr(d.projectedMonthEnd)}\n`;
  t += `By counsellor (sorted by biggest rupee gap first):\n`;
  t += d.byCounsellor.map(c => `- ${c.username}: target ${inr(c.target)}, collected ${inr(c.paid)}, gap ${inr(c.gap)}${c.achievementPct === null ? " (no target set)" : ` (${c.achievementPct}% achieved, ${c.onPace ? "on pace" : "BEHIND pace"})`}, projected month-end ${inr(c.projectedMonthEnd)}`).join("\n");
  return t;
}

function buildConversionByCounsellorText(label, rows) {
  const total = rows.reduce((s, r) => s + (r.conversions || 0), 0);
  let t = `[CONVERSIONS BY COUNSELLOR — ${label}]\n`;
  t += `A "conversion" = a lead now at "Converted", counted by the month the lead was CREATED.\n`;
  t += `>>> TOTAL conversions ${label}: ${total} <<<\n\n`;
  t += rows.map(r => `- ${r._id}: ${r.conversions} conversions from ${r.newLeads} new leads (${r.newLeads ? Math.round((r.conversions / r.newLeads) * 100) : 0}% conversion rate)`).join("\n");
  return t;
}

function buildVolumeVsConversionText(conversionRows, branchAvgConversionPct) {
  let t = `[VOLUME VS CONVERSION DIAGNOSIS]\nUse this to judge whether a counsellor's shortfall is a LEAD-VOLUME problem (few leads, so conversion% alone looks fine) or a CONVERSION problem (leads exist but aren't closing). Branch average conversion rate this month: ${branchAvgConversionPct === null ? "N/A" : `${branchAvgConversionPct}%`}.\n`;
  const avgLeads = conversionRows.length ? Math.round(conversionRows.reduce((s, r) => s + r.newLeads, 0) / conversionRows.length) : 0;
  t += `Branch average new leads per counsellor this month: ${avgLeads}\n`;
  t += conversionRows.map(r => {
    const convPct = r.newLeads ? Math.round((r.conversions / r.newLeads) * 100) : 0;
    const lowVolume = r.newLeads < avgLeads * 0.8;
    const lowConversion = branchAvgConversionPct !== null && convPct < branchAvgConversionPct * 0.8;
    const diagnosis = lowVolume ? "VOLUME problem (fewer leads than branch average)" : (lowConversion ? "CONVERSION problem (leads exist, not closing)" : "on pace");
    return `- ${r._id}: ${r.newLeads} leads, ${r.conversions} conversions (${convPct}%) — ${diagnosis}`;
  }).join("\n");
  return t;
}

function buildLeadSourceText(label, d) {
  let t = `[LEAD SOURCE — ${label}]\nTotal new leads: ${d.total}\n`;
  t += `NOTE: cost-per-lead / ad spend is NOT tracked anywhere in this CRM — do not estimate or invent a CPL figure; state plainly that it isn't tracked if asked.\n`;
  t += d.bySource.map(s => `- ${s.source}: ${s.count} (${d.total ? Math.round((s.count / d.total) * 100) : 0}%)`).join("\n");
  return t;
}

function buildStaleLeadBucketsText(d) {
  let t = `[STALE / UNTOUCHED LEADS — bucketed by hours since last touch, active leads only]\n`;
  for (const bucket of ["24-48h", "48-72h", "72h+"]) {
    const total = Object.values(d.countsByCounsellor[bucket]).reduce((s, n) => s + n, 0);
    t += `\n${bucket} untouched: ${total} total`;
    const byC = Object.entries(d.countsByCounsellor[bucket]).sort((a, b) => b[1] - a[1]);
    if (byC.length) t += ` (by counsellor: ${byC.map(([c, n]) => `${c} ${n}`).join(", ")})`;
    t += `\n`;
    if (d.buckets[bucket].length) {
      t += `Named leads (sample, up to 15): ${d.buckets[bucket].map(l => `${l.userName} (${l.course}, ${l.assignedto}, ${l.hoursStale}h)`).join("; ")}\n`;
    }
  }
  return t;
}

function buildDemoFunnelText(label, d) {
  let t = `[SITE VISIT FUNNEL — ${label}]\nScheduled (entered "Site Visit" stage): ${d.scheduled}\nCompleted (moved on from "Site Visit"): ${d.completed}\nCurrently sitting at "Site Visit": ${d.currentlyAtDemoStage}\n`;
  t += `NOTE: there is no explicit no-show field in the CRM. "No-show (proxy)" below = leads still at Site Visit with NO activity in 3+ days — treat as an approximate signal, not a confirmed no-show count.\n`;
  t += `No-show (proxy): ${d.noShowProxy.length}`;
  if (d.noShowProxy.length) t += ` — ${d.noShowProxy.map(l => `${l.userName} (${l.course}, ${l.assignedto})`).join("; ")}`;
  return t;
}

function buildHotLeadsBranchText(rows) {
  let t = `[HOT/WARM LEADS NOT YET ENROLLED — branch-wide]\nTotal: ${rows.length}\n`;
  t += rows.slice(0, 25).map(l => {
    const parts = [`${l.userName} (${l.course}, ${l.assignedto}, ${l.status}, stage "${l.pipeline}")`];
    if (l.lastRemark) parts.push(`last remark: "${l.lastRemark}"`);
    if (l.lastConvo) parts.push(`recent chat: ${l.lastConvo}`);
    return `- ${parts.join("; ")}`;
  }).join("\n");
  return t;
}

function buildCourseSeatFillText(d) {
  let t = `[SEAT FILL — ${d.courseName}]\nNOTE: seat capacity (${d.seatsPerBatch}/batch) is a CONFIGURED number (MANAGER_JGP_SEATS_PER_BATCH), not a CRM field — treat it as approximate unless confirmed.\n`;
  if (!d.batches.length) { t += `No Enrolled leads found for this course.`; return t; }
  t += d.batches.map(b => `- Batch "${b.batchDate}": ${b.enrolled}/${d.seatsPerBatch} seats filled (${b.seatsLeft} left)`).join("\n");
  return t;
}

function buildUpcomingBatchFillText(rows) {
  if (!rows.length) return `[UPCOMING BATCHES]\nNo leads have a future batch_date set.`;
  let t = `[UPCOMING BATCHES — by batch_date, soonest first]\n`;
  t += rows.map(r => `- ${r.course} — ${r.batchDate}: ${r.total} leads in pipeline, ${r.enrolled} already Enrolled`).join("\n");
  return t;
}

function buildScorecardHistoryText(days, rows) {
  let t = `[CHECK-IN SCORECARD HISTORY — trailing ${days} days]\n`;
  if (!rows.length) { t += `No completed check-ins in this window.`; return t; }
  const above7 = new Set(), below4 = new Set();
  for (const r of rows) {
    if (r.compositePct === null) continue;
    if (r.compositePct >= 70) above7.add(r.username);
    if (r.compositePct < 40) below4.add(r.username);
  }
  t += `Counsellors scoring 7/10+ at least once: ${[...above7].join(", ") || "none"}\n`;
  t += `Counsellors scoring below 4/10 at least once: ${[...below4].join(", ") || "none"}\n`;
  t += rows.slice(0, 40).map(r => `- ${r.date} ${r.username}: ${r.compositePct === null ? "N/A" : `${(r.compositePct / 10).toFixed(1)}/10`} (${r.band || "N/A"})${r.verdictReason ? ` — ${r.verdictReason}` : ""}`).join("\n");
  return t;
}

function buildCompetitorMentionsText(start, end, rows) {
  let t = `[COMPETITOR MENTIONS — ${fmtRange(start, end)}]\nFrom remark text search for: ${KNOWN_COMPETITORS.join(", ")}. This is NOT a tracked field — only as good as what counsellors typed in remarks; absence of a mention does not mean no competitor was involved.\n`;
  if (!rows.length) { t += `No remarks mentioning a named competitor in this period.`; return t; }
  t += rows.map(r => `- ${fmtDate(r.timestamp)}: ${r.userName} (${r.course}, ${r.assignedto}) mentioned "${r.competitor}" — remark: "${r.remark}"`).join("\n");
  return t;
}

function buildObjectionThemesText(label, d) {
  let t = `[OBJECTION THEMES — ${label}]\nKeyword-based signal over ${d.totalRemarksScanned} remarks (not a tracked/tagged field, just word matching — treat as directional, not exact):\n`;
  t += `- Price-related: ${d.counts.price}\n- Timing-related: ${d.counts.timing}\n- Trust-related: ${d.counts.trust}`;
  return t;
}

function buildBranchCommitmentText(rows) {
  let t = `[COMMITMENT FOLLOW-THROUGH — branch-wide, most recent prior check-in per counsellor]\n`;
  t += rows.map(r => r.total
    ? `- ${r.username}: ${r.kept}/${r.total} commitments kept (${r.pct}%)`
    : `- ${r.username}: no prior commitments on record`
  ).join("\n");
  return t;
}

function buildBranchWeeklyScorecardText(s) {
  const pctOrNA = (v) => v === null || v === undefined ? "N/A" : `${v}%`;
  let t = `[WEEKLY SCORECARD — ${fmtRange(s.thisWeek.start, s.thisWeek.end)} vs ${fmtRange(s.lastWeek.start, s.lastWeek.end)}]\n\n`;
  t += `Branch revenue collected: ${inr(s.revenueThisWeek.totalPaid)} (last week ${inr(s.revenueLastWeek.totalPaid)})\n`;
  t += `New leads (all sources): ${s.leadsThisWeek} (last week ${s.leadsLastWeek})\n`;
  t += `Lead-to-enrollment conversion rate this week: ${pctOrNA(s.conversionRatePctThisWeek)}\n`;
  t += `Leads untouched > 48 hrs right now: ${s.untouched48hPlus}\n`;
  t += `Site-visit show rate this week: ${pctOrNA(s.demoShowRatePct)} (${s.demoFunnel.scheduled} scheduled, ${s.demoFunnel.noShowProxy.length} no-show proxy — see note on demo tracking)\n`;
  t += `${JGP_COURSE_NAME} seats filled: ${s.jgpSeatsFilled} of ${s.jgpSeatsPerBatch} (configured capacity, not a CRM field)\n`;
  t += `Counsellors at/above 7/10 rating (last 7 days): ${s.counsellorsAbove7}\n`;
  t += `Counsellors below 4/10 rating (last 7 days): ${s.counsellorsBelow4}\n\n`;
  t += `Monthly target progress: collected ${inr(s.targetAchievement.branchPaid)} of ${inr(s.targetAchievement.branchTarget)} target (${pctOrNA(s.targetAchievement.branchAchievementPct)}), projected month-end ${inr(s.targetAchievement.projectedMonthEnd)}\n\n`;
  t += `Per counsellor (this month, sorted by biggest gap first):\n`;
  t += s.byCounsellor.map(c => `- ${c.username}: target ${inr(c.target)}, collected ${inr(c.paid)}, gap ${inr(c.gap)}${c.achievementPct === null ? "" : ` (${c.achievementPct}%)`}`).join("\n");
  return t;
}

// One dated timeline per lead — the core of the 6-month RAG corpus.
function buildLeadTimelineText(lead, pcRows) {
  const evts = [];
  for (const r of (lead.remarks || [])) {
    if (r && r.timestamp) evts.push({ t: new Date(r.timestamp), s: `REMARK: ${String(r.remark || "").trim()}` });
  }
  for (const c of (lead.conversations || [])) {
    if (!c || !c.timestamp) continue;
    const u = String(c.userMsg || "").trim(), b = String(c.botReply || "").trim();
    evts.push({ t: new Date(c.timestamp), s: `WHATSAPP — lead: "${u.slice(0, 200)}"${b ? ` | reply: "${b.slice(0, 200)}"` : ""}` });
  }
  for (const ch of (lead.campaignHistory || [])) {
    if (ch && ch.sentAt) evts.push({ t: new Date(ch.sentAt), s: `CAMPAIGN "${ch.campaignName || ""}" sent${ch.message ? `: ${String(ch.message).slice(0, 160)}` : ""}` });
    if (ch && ch.repliedAt) evts.push({ t: new Date(ch.repliedAt), s: `CAMPAIGN "${ch.campaignName || ""}" — lead replied` });
  }
  const fees = lead.fees || {};
  if (fees.billedAt) evts.push({ t: new Date(fees.billedAt), s: `BILLED ${inr(fees.billedAmount)}` });
  for (const i of (fees.installments || [])) {
    if (i && i.dueDate) evts.push({ t: new Date(i.dueDate), s: `INSTALLMENT #${i.number || "?"} ${inr(i.amount)} — ${i.status || "Pending"}${i.paidAt ? ` (paid ${fmtDate(i.paidAt)})` : ""}` });
  }
  for (const p of (pcRows || [])) {
    if (p && p.changedAt) evts.push({ t: new Date(p.changedAt), s: `PIPELINE ${p.fromStage || "New Lead"} → ${p.toStage}` });
  }
  evts.sort((a, b) => a.t - b.t);
  let lines = evts.filter(e => !isNaN(e.t)).map(e => `${fmtDate(e.t)}: ${e.s}`);

  const header =
    `[LEAD — ${lead.userName || "Unknown"}]\n` +
    `Course: ${lead.course || lead.courseofintrest || "N/A"} | Source: ${lead.leadfrom || "N/A"}` +
    `${lead.referredBy ? ` (referred by ${lead.referredBy})` : ""}${lead.leadgenId ? ` | Meta paid lead` : ""}\n` +
    `Counsellor: ${lead.assignedto || "unassigned"} | Current stage: ${lead.pipeline || "New Lead"} | Status: ${lead.status || "Cold"}\n` +
    `Created: ${fmtDate(lead.datecreated)} | Last interacted: ${lead.lastInteracted ? fmtDate(lead.lastInteracted) : "N/A"}` +
    `${lead.reminder ? ` | Next reminder: ${fmtDate(lead.reminder)}` : ""}\n` +
    `Timeline (oldest → newest):\n`;

  if ((header.length + lines.join("\n").length) > LEAD_CHAR_CAP) {
    while (lines.length > 4 && (header.length + lines.join("\n").length + 40) > LEAD_CHAR_CAP) lines.shift();
    lines.unshift("… [earlier history truncated] …");
  }
  return header + (lines.length ? lines.join("\n") : "(no recorded activity beyond creation)");
}

function buildDailyHistText(dateStr, rows) {
  const teamNew = rows.reduce((s, r) => s + (r.newLeadsToday || 0), 0);
  const teamBilled = rows.reduce((s, r) => s + (r.billedTodayTotal || 0), 0);
  const teamPaid = rows.reduce((s, r) => s + (r.feesUpdatedToday || 0), 0);
  let t = `[DAILY REPORT — ${dateStr}]\n`;
  t += `Team: ${teamNew} new leads, billed ${inr(teamBilled)}, paid ${inr(teamPaid)}\n`;
  if (!rows.length) { t += `(no counsellor activity recorded)`; return t; }
  t += rows.map(r =>
    `- ${r.username}: new ${r.newLeadsToday} (uncontacted ${r.newLeadsMissed}), remarks old/new ${r.oldRemarksCount}/${r.newRemarksCount}, billed ${inr(r.billedTodayTotal)}, paid ${inr(r.feesUpdatedToday)}, reminders done/missed ${r.remindersDone}/${r.remindersMissed}`
  ).join("\n");
  return t;
}

// ─── Embedding + chunk store plumbing ───────────────────────────────────
function hashText(text) {
  return crypto.createHash("sha256").update(text).digest("hex");
}

async function embedText(text) {
  const res = await openai.embeddings.create({ model: EMBED_MODEL, dimensions: EMBED_DIMENSIONS, input: String(text).slice(0, 8000) });
  return res.data[0].embedding;
}

// Returns true if it actually wrote (content changed), false if hash-skipped.
async function upsertChunk(chunkId, periodKey, type, text, meta = {}) {
  const hash = hashText(text);
  const existing = await ManagerChatChunk.findOne({ chunkId }).select("hash").lean();
  if (existing && existing.hash === hash) return false;
  const embedding = await embedText(text);
  await ManagerChatChunk.findOneAndUpdate(
    { chunkId },
    {
      chunkId, periodKey, type, text, embedding, hash, updatedAt: new Date(),
      assignedto: meta.assignedto ?? null,
      leadId: meta.leadId ?? null,
      refDate: meta.refDate ?? null,
    },
    { upsert: true }
  );
  return true;
}

async function ensureChunk(chunkId, periodKey, type, buildTextFn, opts = {}) {
  try {
    if (opts.ttlMinutes > 0) {
      const existing = await ManagerChatChunk.findOne({ chunkId }).select("updatedAt").lean();
      if (existing && Date.now() - new Date(existing.updatedAt).getTime() < opts.ttlMinutes * 60000) return false;
    }
    const text = await buildTextFn();
    return await upsertChunk(chunkId, periodKey, type, text, opts.meta || {});
  } catch (err) {
    console.error(`⚠️ Manager chat chunk build failed (${chunkId}):`, err.message);
    return false;
  }
}

// Batched upsert for bulk ingest — one embeddings call per ~96 changed chunks.
async function bulkUpsertChunks(items) {
  if (!items.length) return { wrote: 0, skipped: 0 };
  const ids = items.map(i => i.chunkId);
  const existing = await ManagerChatChunk.find({ chunkId: { $in: ids } }).select("chunkId hash").lean();
  const hashById = new Map(existing.map(e => [e.chunkId, e.hash]));
  const need = [];
  for (const it of items) {
    it.hash = hashText(it.text);
    if (hashById.get(it.chunkId) !== it.hash) need.push(it);
  }
  let wrote = 0;
  for (let i = 0; i < need.length; i += 96) {
    const batch = need.slice(i, i + 96);
    let embeddings;
    try {
      const res = await openai.embeddings.create({
        model: EMBED_MODEL, dimensions: EMBED_DIMENSIONS,
        input: batch.map(b => String(b.text).slice(0, 8000)),
      });
      embeddings = res.data.map(d => d.embedding);
    } catch (err) {
      console.error("⚠️ bulk embed batch failed:", err.message);
      continue;
    }
    const ops = batch.map((b, j) => ({
      updateOne: {
        filter: { chunkId: b.chunkId },
        update: { $set: {
          chunkId: b.chunkId, periodKey: b.periodKey || "", type: b.type, text: b.text,
          embedding: embeddings[j], hash: b.hash, updatedAt: new Date(),
          assignedto: b.assignedto ?? null, leadId: b.leadId ?? null, refDate: b.refDate ?? null,
        } },
        upsert: true,
      },
    }));
    try {
      await ManagerChatChunk.bulkWrite(ops, { ordered: false });
      wrote += ops.length;
    } catch (err) {
      console.error("⚠️ bulk chunk write failed:", err.message);
    }
  }
  return { wrote, skipped: items.length - wrote };
}

function cosineSim(a, b) {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  return dot / (Math.sqrt(na) * Math.sqrt(nb) + 1e-9);
}

// ─── In-memory chunk cache (retrieval hot path) ────────────────────────
let _chunkCache = null;
let _chunkCacheAt = 0;
const CHUNK_CACHE_TTL_MS = 15 * 60 * 1000;

// Only the cosine-retrievable types are cached — the on-demand aggregate
// chunks (revenue/leads/conversion/…) are fetched by id, not searched.
const RETRIEVABLE_TYPES = ["lead", "daily-hist", "weekly-hist", "monthly-hist"];
let _chunkCacheLoading = null;

async function loadChunks() {
  if (_chunkCache && Date.now() - _chunkCacheAt < CHUNK_CACHE_TTL_MS) return _chunkCache;
  if (_chunkCacheLoading) return _chunkCacheLoading;
  _chunkCacheLoading = (async () => {
    const rows = await ManagerChatChunk.find({ type: { $in: RETRIEVABLE_TYPES } })
      .select("chunkId type assignedto refDate text embedding")
      .lean();
    _chunkCache = rows.filter(c => Array.isArray(c.embedding) && c.embedding.length === EMBED_DIMENSIONS);
    _chunkCacheAt = Date.now();
    _chunkCacheLoading = null;
    return _chunkCache;
  })();
  return _chunkCacheLoading;
}

function invalidateChunkCache() {
  _chunkCache = null;
  _chunkCacheAt = 0;
  _chunkCacheLoading = null;
}

// ─── Query-time period detection ────────────────────────────────────────
const MONTH_NAMES = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];

function detectExplicitMonths(message) {
  const lower = message.toLowerCase();
  const now = new Date();
  const found = [];
  for (let i = 0; i < 12; i++) {
    const name = MONTH_NAMES[i];
    const m = lower.match(new RegExp(`\\b${name}\\b(?:\\s+(\\d{4}))?`, "i"));
    if (!m) continue;
    let year = m[1] ? Number(m[1]) : now.getFullYear();
    if (!m[1] && (i + 1) > now.getMonth() + 1) year -= 1;
    found.push({ year, month: i + 1 });
  }
  return found;
}

function mentionsAny(message, words) {
  const lower = message.toLowerCase();
  return words.some(w => lower.includes(w));
}

function escapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Distinct real course names (College Internship + blanks excluded), cached 1h.
let _courseListCache = null, _courseListAt = 0;
async function getKnownCourses() {
  if (_courseListCache && Date.now() - _courseListAt < 3600e3) return _courseListCache;
  const rows = await Chat.distinct("course");
  _courseListCache = rows.filter(c => c && String(c).trim() && !/^college internship$/i.test(c));
  _courseListAt = Date.now();
  return _courseListCache;
}

// Which known courses does this message name? Requires every significant word of
// the course name to appear, so "data analytics" ≠ "data science".
function matchCourses(message, known) {
  const l = ` ${message.toLowerCase()} `;
  const hits = [];
  for (const c of known) {
    const cl = c.toLowerCase();
    if (l.includes(cl)) { hits.push(c); continue; }
    const words = cl.split(/\s+/).filter(w => w.length > 2 && !["with", "and", "the", "for"].includes(w));
    if (words.length >= 1 && words.every(w => l.includes(w))) hits.push(c);
  }
  return [...new Set(hits)];
}

// Fast keyword intent router — decides which aggregates to build and whether
// per-lead history retrieval is even needed, so a plain "how many X this month"
// question doesn't pay for 15 aggregations + a full-corpus cosine search.
async function classifyQuery(message, counsellorNames = []) {
  const l = message.toLowerCase();
  const has = (re) => re.test(l);
  const nameHit = counsellorNames.some(n => n && l.includes(String(n).toLowerCase()));
  const courses = matchCourses(message, await getKnownCourses().catch(() => []));
  const explicitMonths = detectExplicitMonths(message);

  // NB: stem prefixes use a leading \b but NO trailing \b, so "conver" matches
  // "conversion"/"conversions"/"converted", "enrol" matches "enrolled", etc.
  const intent = {
    today: has(/\b(today|so far|right now|currently|as of now|this morning|yesterday)/),
    thisMonth: has(/\b(this month|current month|month to date|\bmtd\b|this mnth)/),
    lastMonth: has(/\b(last month|previous month|prev month|past month)/),
    week: has(/\bweek/),
    explicitMonths,
    revenue: has(/\b(revenue|collect|billed|billing|\bpaid\b|payment|\bfee|income|amount|money|target|achiev|\bdue\b|earn|turnover)/),
    leads: has(/\b(lead|enquir|enquiries|inquir|prospect|walk.?in)/),
    conversion: has(/\b(conver|enrol|admission|closure|\bclos(e|ing|ed)|\bjoin|\bsold\b|\bsale)/),
    pipeline: has(/\b(pipeline|stage|funnel|\bmoved?\b|\bdemo|follow.?up|stuck|stagnant|stall)/),
    remarks: has(/\b(remark|\bnote|comment|feedback)/) || /what did .*(say|tell|mention)/.test(l),
    counsellor: nameHit || has(/\b(counsel|\bteam\b|\bstaff\b|who is|which (person|staff|counsel)|each person|per (person|counsel))/),
    checkin: has(/\bcheck.?in/),
    why: has(/\b(why|reason|explain|how come|what happened|because)/) || /(not|didn.?t|hasn.?t|haven.?t|never) (conver|enrol|close|join|move|respond|pick|call|follow)/.test(l) || /no (enrol|conver|response|action|follow)/.test(l),
    courses,
    nameHit,

    // ─── Branch Manager Director Review Framework (Sections 1-8) ─────────
    targetGap: has(/\b(gap|shortfall|rupee|how much (short|behind|more)|run.?rate|behind target|close the gap)/),
    forecast: has(/\b(forecast|land(ing)?|projection|projected|month.?end|if nothing changes|where (will|do) we (end|land))/),
    volumeVsConversion: has(/\b(volume problem|conversion problem|volume (or|vs\.?|versus) conversion|not enough leads)/),
    staleWindow: has(/\b(24.?hour|48.?hour|72.?hour|untouched|backlog|sitting (idle|untouched))/),
    demo: has(/\b(demo|no.?show|counsel(l)?ing session)/),
    source: has(/\b(source|meta ads?|google ads?|walk.?in|organic|referral|which source|cost per lead|\bcpl\b|ad spend)/),
    competitor: has(/\b(competitor|lost to|went with|qtree|upshot)/) || KNOWN_COMPETITORS.some(c => l.includes(c.toLowerCase())),
    objection: has(/\b(objection|hesitat|price.?(objection|resistance)|too expensive)/),
    responseTime: has(/\b(response time|time to (first )?contact|how fast|how quickly|response speed)/),
    batchFill: has(/\b(batch|seats?|job guarantee|start date|fill(ed)?)/),
    courseMix: has(/\b(course.?wise|per course|by course|course mix|which courses?|each course|course performance|revenue mix|over.?index|under.?sell)/),
    counsellorRanking: has(/\b(rank|ranking|top performer|bottom performer|who.?s (behind|ahead|top|bottom))/),
    scorecardHistory: has(/\b(scored?|rating|\/\s?10|out of 10|below 4|above 7|escalat)/),
    directorReview: has(/\b(director review|branch review|weekly scorecard|branch scorecard|full review)/),
  };
  // A question that names/implies a specific lead person (not a counsellor) or
  // asks "why" needs the per-lead timelines; a pure metric question does not.
  // responseTime is the one new intent that also needs retrieval: avg response
  // time only lives in the weekly-hist/monthly-hist RAG chunks (WeeklyReport/
  // MonthlyReport only cover completed periods, so there's no live aggregate for it).
  intent.needsHistory = intent.why || intent.remarks || intent.responseTime ||
    (intent.pipeline && !intent.revenue && !intent.conversion) ||
    (!intent.revenue && !intent.conversion && !intent.leads && !intent.counsellor && !intent.checkin && !courses.length
      && !intent.targetGap && !intent.forecast && !intent.volumeVsConversion && !intent.staleWindow && !intent.demo
      && !intent.source && !intent.competitor && !intent.objection && !intent.responseTime && !intent.batchFill
      && !intent.counsellorRanking && !intent.scorecardHistory && !intent.directorReview && !intent.courseMix);
  intent.isMetric = intent.revenue || intent.conversion || intent.leads || intent.counsellor || intent.checkin || courses.length > 0
    || intent.targetGap || intent.forecast || intent.volumeVsConversion || intent.staleWindow || intent.demo
    || intent.source || intent.competitor || intent.objection || intent.responseTime || intent.batchFill
    || intent.counsellorRanking || intent.scorecardHistory || intent.directorReview || intent.courseMix;
  return intent;
}

// Conversion / enrolment numbers for one course (or all courses) in a period.
// "Conversion" = pipeline is now "Converted". Reported two ways: cohort basis
// (by the month the lead was created — matches the CRM dashboard) and
// billed-in-period basis (by fees.billedAt — "enrolled during this month").
async function getConversionSummary(course, start, end) {
  // Exact course match (uses the { course: 1 } index); `course` is always a real
  // stored value from getKnownCourses(). null → every non-internship course.
  const match = course ? { course } : { course: { $not: /^college internship$/i } };
  const rows = await Chat.aggregate([
    { $match: match },
    { $project: {
      isEnrolled: { $regexMatch: { input: { $ifNull: ["$pipeline", ""] }, regex: /^(enrolled|converted)$/i } },
      pipeline: { $ifNull: ["$pipeline", "New"] },
      createdInPeriod: { $and: [{ $gte: ["$datecreated", start] }, { $lte: ["$datecreated", end] }] },
      billedInPeriod: { $and: [
        { $ifNull: ["$fees.billedAt", false] },
        { $gte: ["$fees.billedAt", start] }, { $lte: ["$fees.billedAt", end] },
      ] },
    } },
    { $group: {
      _id: null,
      newLeads: { $sum: { $cond: ["$createdInPeriod", 1, 0] } },
      cohortEnrolled: { $sum: { $cond: [{ $and: ["$createdInPeriod", "$isEnrolled"] }, 1, 0] } },
      billedInPeriod: { $sum: { $cond: [{ $and: ["$isEnrolled", "$billedInPeriod"] }, 1, 0] } },
      openStages: { $push: { $cond: ["$isEnrolled", "$$REMOVE", "$pipeline"] } },
    } },
  ]);
  const r = rows[0] || { newLeads: 0, cohortEnrolled: 0, billedInPeriod: 0, openStages: [] };
  const stageCounts = {};
  for (const s of r.openStages) stageCounts[s] = (stageCounts[s] || 0) + 1;
  return {
    newLeads: r.newLeads, cohortEnrolled: r.cohortEnrolled, billedInPeriod: r.billedInPeriod,
    openByStage: Object.entries(stageCounts).sort((a, b) => b[1] - a[1]),
  };
}

// Per-course conversion breakdown for one period (cohort basis).
async function getConversionByCourse(start, end) {
  return Chat.aggregate([
    { $match: { course: { $nin: [null, ""], $not: /^college internship$/i } } },
    { $project: {
      course: 1,
      isEnrolled: { $regexMatch: { input: { $ifNull: ["$pipeline", ""] }, regex: /^(enrolled|converted)$/i } },
      createdInPeriod: { $and: [{ $gte: ["$datecreated", start] }, { $lte: ["$datecreated", end] }] },
    } },
    { $group: {
      _id: "$course",
      newLeads: { $sum: { $cond: ["$createdInPeriod", 1, 0] } },
      conversions: { $sum: { $cond: [{ $and: ["$createdInPeriod", "$isEnrolled"] }, 1, 0] } },
    } },
    { $match: { $or: [{ newLeads: { $gt: 0 } }, { conversions: { $gt: 0 } }] } },
    { $sort: { conversions: -1, newLeads: -1 } },
  ]);
}

function buildConversionByCourseText(label, rows) {
  const total = rows.reduce((s, r) => s + (r.conversions || 0), 0);
  let t = `[CONVERSIONS BY COURSE — ${label}]\n`;
  t += `A "conversion" = a lead now at "Converted", counted by the month the lead was CREATED (matches the CRM dashboard). College Internship is excluded.\n`;
  t += `>>> TOTAL conversions ${label}: ${total} <<<\n\n`;
  t += rows.map(r => `- ${r._id}: ${r.conversions} conversions from ${r.newLeads} new leads (${r.newLeads ? Math.round((r.conversions / r.newLeads) * 100) : 0}%)`).join("\n");
  return t;
}

function buildConversionText(courseLabel, periods) {
  // periods: [{ label, data }], THIS month first, then last month.
  const tm = periods[0].data;
  let t = `[CONVERSIONS — ${courseLabel}]\n`;
  t += `>>> ${courseLabel} conversions THIS MONTH (${periods[0].label}): ${tm.cohortEnrolled} <<<\n`;
  t += `(A "conversion" = a lead now at pipeline stage "Converted", counted by the month the lead was CREATED — this matches the CRM dashboard. Do NOT add periods together; each line is a standalone total for that one period.)\n`;
  for (const p of periods) {
    const d = p.data;
    const pct = d.newLeads ? Math.round((d.cohortEnrolled / d.newLeads) * 100) : 0;
    t += `\n${p.label}: ${d.cohortEnrolled} conversions (from ${d.newLeads} new leads that period = ${pct}% conversion rate). Alt count by fee-billed date: ${d.billedInPeriod}.`;
  }
  const latestOpen = tm.openByStage || [];
  if (latestOpen.length) {
    t += `\n\nOpen ${courseLabel} pipeline right now (not yet enrolled): ${latestOpen.map(([s, n]) => `${s} ${n}`).join(", ")}.`;
  }
  return t;
}

// ─── Ensure the on-demand aggregate chunks THIS question needs are fresh ──
// Intent-gated: only builds the aggregates the question is actually about, so a
// plain "how many X this month" doesn't run 15 aggregations. Returns the list
// of chunkIds it touched.
async function ensureCoreChunks(message, intent) {
  const now = new Date();
  const today = getISTDayBounds(now);
  const todayKey = getISTDateString(now);
  const thisMonth = getISTMonthBounds(now.getFullYear(), now.getMonth() + 1);
  const lastMonthInfo = shiftMonth(now.getFullYear(), now.getMonth() + 1, -1);
  const lastMonth = getISTMonthBounds(lastMonthInfo.year, lastMonthInfo.month);

  const ensured = [];
  const track = (id) => { ensured.push(id); return id; };
  const tasks = [];
  const thisWeek = getISTWeekBounds(now);

  // Nothing specific detected → a small default so generic questions still work.
  const generic = !intent.today && !intent.thisMonth && !intent.lastMonth && !intent.week &&
    !intent.explicitMonths.length && !intent.revenue && !intent.leads && !intent.conversion &&
    !intent.pipeline && !intent.remarks && !intent.counsellor && !intent.checkin && !intent.courses.length &&
    !intent.targetGap && !intent.forecast && !intent.volumeVsConversion && !intent.staleWindow && !intent.demo &&
    !intent.source && !intent.competitor && !intent.objection && !intent.batchFill &&
    !intent.counsellorRanking && !intent.scorecardHistory && !intent.directorReview && !intent.courseMix;

  const wantToday = intent.today || generic;
  const wantThisMonth = intent.thisMonth || intent.explicitMonths.length === 0 && (generic || intent.revenue || intent.leads || intent.conversion || intent.counsellor);
  const wantLastMonth = intent.lastMonth || (intent.conversion && !intent.courses.length) || (intent.revenue && (intent.lastMonth || /\bvs\b|compare|comparison|than/.test(message.toLowerCase())));

  if (wantToday && (intent.revenue || generic)) tasks.push(ensureChunk(track(`revenue-${todayKey}`), todayKey, "revenue", async () => buildRevenueText("Today", today.start, today.end, await getRevenueSummary(today.start, today.end))));
  if (wantToday && (intent.leads || generic)) tasks.push(ensureChunk(track(`leads-${todayKey}`), todayKey, "leads", async () => buildLeadsText("Today", today.start, today.end, await getNewLeadsSummary(today.start, today.end))));
  if (wantToday && intent.pipeline) tasks.push(ensureChunk(track(`pipeline-${todayKey}`), todayKey, "pipeline", async () => buildPipelineText("Today", today.start, today.end, await getPipelineChangesSummary(today.start, today.end))));
  if (wantToday && intent.remarks) tasks.push(ensureChunk(track(`remarks-${todayKey}`), todayKey, "remarks", async () => buildRemarksText("Today", today.start, today.end, await getRemarksSummary(today.start, today.end))));
  if (wantToday && intent.counsellor) tasks.push(ensureChunk(track(`counsellor-activity-${todayKey}`), todayKey, "counsellor", async () => buildCounsellorActivityText("Today", today.start, today.end, await getCounsellorActivitySummary(today.start, today.end))));
  if (intent.checkin || (intent.counsellor && intent.today)) tasks.push(ensureChunk(track(`checkin-${todayKey}`), todayKey, "checkin", async () => buildCheckinText(todayKey, await getManagerCheckinDigest(todayKey))));

  if (wantThisMonth && (intent.revenue || generic)) tasks.push(ensureChunk(track(`revenue-month-${thisMonth.key}`), thisMonth.key, "revenue", async () => buildRevenueText(thisMonth.label, thisMonth.start, thisMonth.end, await getRevenueSummary(thisMonth.start, thisMonth.end)), { ttlMinutes: 10 }));
  if (wantThisMonth && (intent.leads || generic || intent.conversion)) tasks.push(ensureChunk(track(`leads-month-${thisMonth.key}`), thisMonth.key, "leads", async () => buildLeadsText(thisMonth.label, thisMonth.start, thisMonth.end, await getNewLeadsSummary(thisMonth.start, thisMonth.end)), { ttlMinutes: 10 }));
  if (wantLastMonth && intent.revenue) tasks.push(ensureChunk(track(`revenue-month-${lastMonth.key}`), lastMonth.key, "revenue", async () => buildRevenueText(lastMonth.label, lastMonth.start, lastMonth.end, await getRevenueSummary(lastMonth.start, lastMonth.end)), { ttlMinutes: 60 }));
  if (wantLastMonth && intent.leads) tasks.push(ensureChunk(track(`leads-month-${lastMonth.key}`), lastMonth.key, "leads", async () => buildLeadsText(lastMonth.label, lastMonth.start, lastMonth.end, await getNewLeadsSummary(lastMonth.start, lastMonth.end)), { ttlMinutes: 60 }));

  // Conversion / enrolment — the targeted, correct answer for "how many
  // conversions this month".
  if (intent.courses.length) {
    // One dated chunk per named course.
    for (const course of intent.courses) {
      const slug = course.toLowerCase().replace(/[^a-z0-9]+/g, "-");
      tasks.push(ensureChunk(track(`conversion-${slug}-${thisMonth.key}`), thisMonth.key, "conversion", async () => {
        const [tm, lm] = await Promise.all([
          getConversionSummary(course, thisMonth.start, now),
          getConversionSummary(course, lastMonth.start, lastMonth.end),
        ]);
        return buildConversionText(course, [
          { label: `This month (${thisMonth.label}, to ${fmtDate(now)})`, data: tm },
          { label: `Last month (${lastMonth.label})`, data: lm },
        ]);
      }, { ttlMinutes: 10 }));
    }
  } else if (intent.conversion || intent.courseMix) {
    // No course named → per-course breakdown for the relevant period(s).
    const wantLM = intent.lastMonth || /\bvs\b|compare|comparison|than|versus/.test(message.toLowerCase());
    tasks.push(ensureChunk(track(`conversion-by-course-${thisMonth.key}`), thisMonth.key, "conversion",
      async () => buildConversionByCourseText(`this month (${thisMonth.label}, to ${fmtDate(now)})`, await getConversionByCourse(thisMonth.start, now)),
      { ttlMinutes: 10 }));
    if (wantLM) {
      tasks.push(ensureChunk(track(`conversion-by-course-${lastMonth.key}`), lastMonth.key, "conversion",
        async () => buildConversionByCourseText(`last month (${lastMonth.label})`, await getConversionByCourse(lastMonth.start, lastMonth.end)),
        { ttlMinutes: 60 }));
    }
  }

  // Trailing course performance — useful for "how is course X doing" but noise
  // for a pointed "how many conversions this month" question. courseMix (e.g.
  // "which courses are ahead of target", "revenue mix across courses") always
  // wants this — it's the only source of per-course revenue/enrollment data.
  if (intent.courseMix || (intent.courses.length && !intent.conversion) || (!intent.courses.length && intent.conversion && !intent.revenue && !intent.thisMonth && !intent.lastMonth)) {
    tasks.push(ensureChunk(track("course-performance-3mo"), "trailing-3mo", "course", async () => buildCourseText(await getCoursePerformanceSummary(3)), { ttlMinutes: 30 }));
  }

  for (const em of intent.explicitMonths) {
    const mb = getISTMonthBounds(em.year, em.month);
    if (intent.revenue || generic) tasks.push(ensureChunk(track(`revenue-month-${mb.key}`), mb.key, "revenue", async () => buildRevenueText(mb.label, mb.start, mb.end, await getRevenueSummary(mb.start, mb.end)), { ttlMinutes: 60 }));
    if (intent.leads || generic) tasks.push(ensureChunk(track(`leads-month-${mb.key}`), mb.key, "leads", async () => buildLeadsText(mb.label, mb.start, mb.end, await getNewLeadsSummary(mb.start, mb.end)), { ttlMinutes: 60 }));
    if (intent.conversion && !intent.courses.length) tasks.push(ensureChunk(track(`conversion-by-course-${mb.key}`), mb.key, "conversion", async () => buildConversionByCourseText(mb.label, await getConversionByCourse(mb.start, mb.end)), { ttlMinutes: 60 }));
    for (const course of intent.courses) {
      const slug = course.toLowerCase().replace(/[^a-z0-9]+/g, "-");
      tasks.push(ensureChunk(track(`conversion-${slug}-${mb.key}`), mb.key, "conversion", async () => buildConversionText(course, [{ label: mb.label, data: await getConversionSummary(course, mb.start, mb.end) }]), { ttlMinutes: 60 }));
    }
  }

  if (intent.week && intent.revenue) {
    const lastWeekAnchor = new Date(now); lastWeekAnchor.setDate(lastWeekAnchor.getDate() - 7);
    const lastWeek = getISTWeekBounds(lastWeekAnchor);
    tasks.push(ensureChunk(track(`revenue-week-${getISTDateString(thisWeek.start)}`), getISTDateString(thisWeek.start), "revenue", async () => buildRevenueText("This week", thisWeek.start, thisWeek.end, await getRevenueSummary(thisWeek.start, thisWeek.end)), { ttlMinutes: 15 }));
    tasks.push(ensureChunk(track(`revenue-week-${getISTDateString(lastWeek.start)}`), getISTDateString(lastWeek.start), "revenue", async () => buildRevenueText("Last week", lastWeek.start, lastWeek.end, await getRevenueSummary(lastWeek.start, lastWeek.end)), { ttlMinutes: 60 }));
  }

  // ─── Branch Manager Director Review Framework — new gated blocks ───────
  if (intent.targetGap || intent.forecast || intent.counsellorRanking || intent.directorReview) {
    tasks.push(ensureChunk(track(`target-gap-${thisMonth.key}`), thisMonth.key, "target-gap",
      async () => buildTargetGapText(await getBranchTargetAchievement()), { ttlMinutes: 10 }));
  }

  if (intent.volumeVsConversion || intent.directorReview) {
    tasks.push(ensureChunk(track(`volume-vs-conversion-${thisMonth.key}`), thisMonth.key, "conversion-counsellor",
      async () => {
        const rows = await getConversionByCounsellor(thisMonth.start, now);
        const totalLeads = rows.reduce((s, r) => s + r.newLeads, 0);
        const totalConv = rows.reduce((s, r) => s + r.conversions, 0);
        const branchAvgPct = totalLeads ? Math.round((totalConv / totalLeads) * 100) : null;
        return buildVolumeVsConversionText(rows, branchAvgPct);
      }, { ttlMinutes: 10 }));
  }

  if (intent.counsellorRanking && intent.conversion) {
    tasks.push(ensureChunk(track(`conversion-counsellor-${thisMonth.key}`), thisMonth.key, "conversion-counsellor",
      async () => buildConversionByCounsellorText(`this month (${thisMonth.label}, to ${fmtDate(now)})`, await getConversionByCounsellor(thisMonth.start, now)),
      { ttlMinutes: 10 }));
  }

  if (intent.staleWindow || intent.directorReview) {
    tasks.push(ensureChunk(track(`stale-buckets-${todayKey}`), todayKey, "stale-buckets",
      async () => buildStaleLeadBucketsText(await getStaleLeadBuckets()), { ttlMinutes: 5 }));
  }

  if (intent.demo || intent.directorReview) {
    const demoPeriod = intent.week || intent.directorReview ? thisWeek : thisMonth;
    const demoLabel = intent.week || intent.directorReview ? "this week" : thisMonth.label;
    tasks.push(ensureChunk(track(`demo-funnel-${getISTDateString(demoPeriod.start)}`), getISTDateString(demoPeriod.start), "demo-funnel",
      async () => buildDemoFunnelText(demoLabel, await getDemoFunnelSummary(demoPeriod.start, demoPeriod.end || now)), { ttlMinutes: 10 }));
  }

  if ((intent.pipeline || intent.conversion || intent.counsellorRanking) && (mentionsAny(message, ["hot", "warm", "blocking", "block", "close this week", "ready to enrol"]) || intent.directorReview)) {
    tasks.push(ensureChunk(track("hot-leads-branch"), todayKey, "hot-leads", async () => buildHotLeadsBranchText(await getHotLeadsBranchWide()), { ttlMinutes: 10 }));
  }

  if (intent.source) {
    const period = intent.today ? today : thisMonth;
    const label = intent.today ? "Today" : thisMonth.label;
    tasks.push(ensureChunk(track(`lead-source-${getISTDateString(period.start)}`), getISTDateString(period.start), "lead-source",
      async () => buildLeadSourceText(label, await getLeadSourceSummary(period.start, period.end)), { ttlMinutes: 15 }));
  }

  if (intent.batchFill || intent.directorReview) {
    tasks.push(ensureChunk(track("jgp-seat-fill"), "current", "batch-fill",
      async () => buildCourseSeatFillText(await getCourseSeatFillStatus(JGP_COURSE_NAME, JGP_SEATS_PER_BATCH)), { ttlMinutes: 30 }));
    tasks.push(ensureChunk(track("upcoming-batch-fill"), "current", "batch-fill",
      async () => buildUpcomingBatchFillText(await getUpcomingBatchFillSummary()), { ttlMinutes: 30 }));
  }

  if (intent.scorecardHistory || intent.counsellorRanking || intent.directorReview) {
    tasks.push(ensureChunk(track("scorecard-history-7d"), todayKey, "scorecard-history",
      async () => buildScorecardHistoryText(7, await getRecentScorecardHistory(7)), { ttlMinutes: 15 }));
  }

  if (intent.competitor || intent.directorReview) {
    const period = intent.week ? thisWeek : thisMonth;
    tasks.push(ensureChunk(track(`competitor-${getISTDateString(period.start)}`), getISTDateString(period.start), "competitor",
      async () => buildCompetitorMentionsText(period.start, period.end || now, await getCompetitorMentions(period.start, period.end || now)), { ttlMinutes: 15 }));
  }

  if (intent.objection || intent.directorReview) {
    const period = intent.week ? thisWeek : thisMonth;
    const label = intent.week ? "this week" : thisMonth.label;
    tasks.push(ensureChunk(track(`objection-${getISTDateString(period.start)}`), getISTDateString(period.start), "objection",
      async () => buildObjectionThemesText(label, await classifyObjectionThemes(period.start, period.end || now)), { ttlMinutes: 15 }));
  }

  if (intent.directorReview) {
    tasks.push(ensureChunk(track("branch-commitment-followthrough"), todayKey, "commitment",
      async () => buildBranchCommitmentText(await getBranchCommitmentFollowThrough()), { ttlMinutes: 10 }));
  }

  if (!tasks.length) {
    // ultra-generic fallback: this month's leads only
    tasks.push(ensureChunk(track(`leads-month-${thisMonth.key}`), thisMonth.key, "leads", async () => buildLeadsText(thisMonth.label, thisMonth.start, thisMonth.end, await getNewLeadsSummary(thisMonth.start, thisMonth.end)), { ttlMinutes: 10 }));
  }

  await Promise.all(tasks);
  // NB: no cache invalidation — these aggregate chunks are fetched by id
  // (getChunkTextsByIds), never cosine-retrieved, so the retrieval cache
  // (lead / *-hist chunks only) is unaffected.
  return ensured;
}

// Cosine retrieval over the cached corpus, with optional pre-filters.
//   opts.topK       (default 12)
//   opts.assignedto  restrict to lead chunks owned by this counsellor
//   opts.types       restrict to these chunk types
async function retrieveRelevantChunks(query, opts = {}) {
  const topK = opts.topK || 12;
  try {
    const [qEmbedding, all] = await Promise.all([embedText(query), loadChunks()]);
    let pool = all;
    if (opts.assignedto) pool = pool.filter(c => c.assignedto === opts.assignedto);
    if (opts.types) pool = pool.filter(c => opts.types.includes(c.type));
    const scored = pool.map(c => ({ text: c.text, score: cosineSim(qEmbedding, c.embedding) }));
    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, topK).map(c => c.text);
  } catch (err) {
    console.error("⚠️ Manager chat retrieval failed, falling back to recent chunks:", err.message);
    const q = opts.types ? { type: { $in: opts.types } } : {};
    if (opts.assignedto) q.assignedto = opts.assignedto;
    const recent = await ManagerChatChunk.find(q).sort({ updatedAt: -1 }).limit(topK).select("text").lean();
    return recent.map(c => c.text);
  }
}

// Fetch the text of specific chunkIds (the core aggregates). Small, indexed
// query by chunkId — never triggers a full-corpus load.
async function getChunkTextsByIds(ids) {
  if (!ids || !ids.length) return [];
  if (_chunkCache) {
    const idSet = new Set(ids);
    const hit = _chunkCache.filter(c => idSet.has(c.chunkId));
    if (hit.length === ids.length) return hit.map(c => c.text);
  }
  const rows = await ManagerChatChunk.find({ chunkId: { $in: ids } }).select("text").lean();
  return rows.map(r => r.text);
}

// ─── 6-month RAG ingest ────────────────────────────────────────────────
let _lastIngestAt = null;
let _ingestRunning = false;

// One `lead` chunk per lead with any activity in the last RAG_MONTHS months.
async function ingestLeadChunks() {
  const since = new Date(Date.now() - RAG_MONTHS * 30 * 864e5);
  const query = { $or: [
    { datecreated: { $gte: since } },
    { lastInteracted: { $gte: since } },
    { "remarks.timestamp": { $gte: since } },
    { "conversations.timestamp": { $gte: since } },
    { "campaignHistory.sentAt": { $gte: since } },
  ] };

  // PipelineChange is tiny — load it all once, index by leadId.
  const pcByLead = new Map();
  for (const p of await PipelineChange.find().lean()) {
    const k = String(p.leadId || "");
    if (!k) continue;
    if (!pcByLead.has(k)) pcByLead.set(k, []);
    pcByLead.get(k).push(p);
  }

  const cursor = Chat.find(query)
    .select("userName course courseofintrest leadfrom referredBy leadgenId assignedto pipeline status datecreated lastInteracted reminder remarks conversations campaignHistory fees")
    .lean().cursor({ batchSize: 200 });

  let batch = [], total = 0, wrote = 0;
  for (let lead = await cursor.next(); lead != null; lead = await cursor.next()) {
    total++;
    batch.push({
      chunkId: `lead-${lead._id}`, periodKey: "", type: "lead",
      text: buildLeadTimelineText(lead, pcByLead.get(String(lead._id))),
      assignedto: lead.assignedto || null,
      leadId: lead._id,
      refDate: lead.lastInteracted || lead.datecreated || null,
    });
    if (batch.length >= 300) { wrote += (await bulkUpsertChunks(batch)).wrote; batch = []; }
  }
  if (batch.length) wrote += (await bulkUpsertChunks(batch)).wrote;
  return { total, wrote };
}

// One `daily-hist` chunk per calendar day for the last RAG_DAILY_DAYS days,
// reconstructed the same way the 10 PM daily report cron computes it.
async function ingestDailyHistChunks() {
  const items = [];
  for (let back = 1; back <= RAG_DAILY_DAYS; back++) {
    const anchor = new Date();
    anchor.setDate(anchor.getDate() - back);
    const { start, end } = reportDayBounds(anchor);
    const dateStr = getISTDateString(anchor);
    let rows;
    try { rows = await computeDailyReportRows(start, end); }
    catch (err) { console.error(`⚠️ daily-hist ${dateStr} failed:`, err.message); continue; }
    if (!rows.some(r => r.newLeadsToday || r.oldRemarksCount || r.newRemarksCount || r.billedTodayTotal || r.feesUpdatedToday)) continue;
    items.push({ chunkId: `daily-hist-${dateStr}`, periodKey: dateStr, type: "daily-hist", text: buildDailyHistText(dateStr, rows), refDate: end });
  }
  const r = await bulkUpsertChunks(items);
  return { total: items.length, wrote: r.wrote };
}

async function ingestWeeklyMonthlyHistChunks() {
  const items = [];

  const weeks = await WeeklyReport.aggregate([
    { $group: { _id: "$weekStart", weekEnd: { $first: "$weekEnd" } } },
    { $sort: { _id: -1 } }, { $limit: 40 },
  ]);
  for (const w of weeks) {
    const d = await getWeeklyReportDigestFor(w._id).catch(() => null);
    if (!d) continue;
    const key = getISTDateString(new Date(w._id));
    items.push({ chunkId: `weekly-hist-${key}`, periodKey: key, type: "weekly-hist", text: buildWeeklyText(d), refDate: new Date(w._id) });
  }

  const months = await MonthlyReport.aggregate([
    { $group: { _id: "$monthStart", monthEnd: { $first: "$monthEnd" } } },
    { $sort: { _id: -1 } }, { $limit: 18 },
  ]);
  for (const mth of months) {
    const d = await getMonthlyReportDigestFor(mth._id).catch(() => null);
    if (!d) continue;
    const key = getISTDateString(new Date(mth._id)).slice(0, 7);
    items.push({ chunkId: `monthly-hist-${key}`, periodKey: key, type: "monthly-hist", text: buildMonthlyText(d), refDate: new Date(mth._id) });
  }

  const r = await bulkUpsertChunks(items);
  return { total: items.length, wrote: r.wrote };
}

async function runFullRagIngest() {
  if (_ingestRunning) { console.log("ℹ️ Manager RAG ingest already running — skipping"); return null; }
  _ingestRunning = true;
  const summary = {};
  const t0 = Date.now();
  try {
    console.log("🧠 Manager RAG ingest starting…");
    for (const [name, fn] of [
      ["weeklyMonthly", ingestWeeklyMonthlyHistChunks],
      ["daily", ingestDailyHistChunks],
      ["leads", ingestLeadChunks],
    ]) {
      try { summary[name] = await fn(); }
      catch (err) { console.error(`❌ Manager RAG ingest step "${name}" failed:`, err.message); summary[name] = { error: err.message }; }
    }
    _lastIngestAt = new Date();
    invalidateChunkCache();
    warmManagerCaches(); // so the next admin question isn't cold
    console.log(`✅ Manager RAG ingest done in ${Math.round((Date.now() - t0) / 1000)}s:`, JSON.stringify(summary));
  } finally {
    _ingestRunning = false;
  }
  return summary;
}

// Pre-warm the slow-to-build caches so the first admin question isn't cold.
function warmManagerCaches() {
  loadChunks().catch(() => {});
  getLiveFiguresBlock().catch(() => {});
  getKnownCourses().catch(() => {});
}

async function bootIngestIfEmpty() {
  if (mongoose.connection.readyState !== 1) return;
  const n = await ManagerChatChunk.countDocuments({ type: "lead" });
  if (n > 0) { warmManagerCaches(); return; }
  console.log("🧠 Manager RAG: no lead chunks found — running initial ingest…");
  await runFullRagIngest();
}

// ─── Authoritative live CRM figures (matches the dashboard exactly) ────
let _figCache = null, _figCacheAt = 0;

async function getLiveFiguresBlock() {
  if (_figCache && Date.now() - _figCacheAt < 180000) return _figCache;
  const now = new Date();
  const { start: dStart, end: dEnd } = reportDayBounds(now);
  const { start: mStart, end: mEnd } = reportMonthBounds(now.getFullYear(), now.getMonth() + 1);
  const lm = shiftMonth(now.getFullYear(), now.getMonth() + 1, -1);
  const { start: lmStart, end: lmEnd } = reportMonthBounds(lm.year, lm.month);

  const [dayRows, monthRows, lastMonthRows, targets] = await Promise.all([
    computeDailyReportRows(dStart, dEnd).catch(() => []),
    computeMonthlyReportRows(mStart, mEnd).catch(() => []),
    computeMonthlyReportRows(lmStart, lmEnd).catch(() => []),
    UserMonthlyTarget.find({ year: now.getFullYear(), month: now.getMonth() + 1 }).lean().catch(() => []),
  ]);

  const sum = (rows, k) => rows.reduce((s, r) => s + (r[k] || 0), 0);
  const targetByUser = new Map(targets.map(t => [t.username, t.target || 0]));

  let t = `[LIVE CRM FIGURES — authoritative, trust these over anything in the context below]\n`;
  t += `As of ${now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })} IST\n\n`;
  t += `TODAY (${getISTDateString(now)}): billed ${inr(sum(dayRows, "billedTodayTotal"))}, paid ${inr(sum(dayRows, "feesUpdatedToday"))}, new leads ${sum(dayRows, "newLeadsToday")}\n`;
  for (const r of dayRows) {
    if (!(r.newLeadsToday || r.billedTodayTotal || r.feesUpdatedToday || r.oldRemarksCount || r.newRemarksCount)) continue;
    t += `  - ${r.username}: new ${r.newLeadsToday} (uncontacted ${r.newLeadsMissed}), remarks ${r.oldRemarksCount + r.newRemarksCount}, billed ${inr(r.billedTodayTotal)}, paid ${inr(r.feesUpdatedToday)}\n`;
  }
  t += `\nTHIS MONTH: billed ${inr(sum(monthRows, "totalBilled"))}, paid ${inr(sum(monthRows, "totalPaid"))}, new leads ${sum(monthRows, "newLeads")}\n`;
  for (const r of monthRows) {
    const tgt = targetByUser.get(r.username) || 0;
    const pct = tgt > 0 ? Math.round((r.totalPaid / tgt) * 100) : null;
    t += `  - ${r.username}: ${r.newLeads} new leads, billed ${inr(r.totalBilled)}, paid ${inr(r.totalPaid)}${tgt ? `, target ${inr(tgt)} (${pct}% achieved)` : ", no target set"}\n`;
  }
  t += `\nLAST MONTH: billed ${inr(sum(lastMonthRows, "totalBilled"))}, paid ${inr(sum(lastMonthRows, "totalPaid"))}, new leads ${sum(lastMonthRows, "newLeads")}\n`;

  _figCache = t;
  _figCacheAt = Date.now();
  return t;
}

// ─── Admin follow-up memory ────────────────────────────────────────────
// When the owner asks the chatbot to chase something with a counsellor,
// classify + store it so the next check-in(s) raise it (see startManagerCheckIn).
async function captureFollowUpFromAdmin(adminMsg, answer) {
  let counsellors;
  try { counsellors = await User.find({ role: "user" }).select("username userNumber").lean(); }
  catch (_) { return; }
  const names = filterExcludedCounsellors(counsellors).map(c => c.username).filter(Boolean);

  const prompt = `The business owner sent this to the manager assistant over WhatsApp:
"${adminMsg}"

Decide if this is a DIRECTIVE to chase a specific counsellor at their next check-in.
It IS a follow-up ONLY when the owner's message itself singles out one named counsellor and either asks why that counsellor is/isn't doing something ("why is Abhi not getting enrollments", "Bindhu's leads are all stuck — why") OR explicitly tells the manager to ask/check/confront that counsellor.
It is NOT a follow-up when: the message names no specific counsellor; it's a ranking/exploratory question ("which counsellor has the most stale leads"); or it's a plain data question ("what is revenue this month"). Do not infer a counsellor from the answer — only from the owner's message.
Counsellor usernames: ${names.join(", ") || "(none)"}.
Reply ONLY as JSON: {"isActionItem": true|false, "counsellorUsername": "<exact username from the list, or null>", "question": "<the question for the manager to put to that counsellor directly, or empty>"}`;

  let parsed;
  try {
    const c = await openai.chat.completions.create({
      model: ADMIN_CHAT_MODEL,
      messages: [{ role: "system", content: "You output only valid JSON." }, { role: "user", content: prompt }],
      temperature: 0, max_tokens: 250, response_format: { type: "json_object" },
    });
    parsed = JSON.parse(c.choices[0].message.content);
  } catch (err) {
    console.error("⚠️ follow-up classify failed:", err.message);
    return;
  }
  if (!parsed || !parsed.isActionItem || !parsed.question) return;

  const username = names.includes(parsed.counsellorUsername) ? parsed.counsellorUsername : null;
  const dupe = await ManagerFollowUp.findOne({
    counsellorUsername: username,
    question: parsed.question,
    status: { $in: ["open", "asked"] },
  }).lean();
  if (dupe) return;

  await ManagerFollowUp.create({
    counsellorUsername: username,
    question: String(parsed.question).slice(0, 500),
    rawAdminMessage: String(adminMsg).slice(0, 1000),
    status: "open",
  });

  await sendManagerWhatsAppText(
    MANAGER_ADMIN_NUMBER,
    username
      ? `📌 Noted — I'll ask ${username} about this in their next check-in and report back what they say.`
      : `📌 Noted — I'll raise this with the team in the next check-ins and report back.`
  );
}

async function loadOpenFollowUpsFor(username) {
  return ManagerFollowUp.find({
    counsellorUsername: username,
    status: { $in: ["open", "asked"] },
    askCount: { $lt: 3 },
  }).sort({ createdAt: 1 }).limit(5).lean();
}

// Recent counsellor answers to the owner's follow-ups — surfaced to the admin
// chatbot so "what did Abhi say about enrollment" works.
async function getAnsweredFollowUpBlock() {
  const rows = await ManagerFollowUp.find({
    status: { $in: ["answered", "closed"] },
    updatedAt: { $gte: new Date(Date.now() - 30 * 864e5) },
  }).sort({ updatedAt: -1 }).limit(15).lean();
  if (!rows.length) return "";
  return `[FOLLOW-UPS YOU ASKED ABOUT — what the counsellors said in their check-ins]\n` +
    rows.map(r => `- (${r.counsellorUsername || "team"}) Q: ${r.question}\n  A: ${r.status === "closed" ? "No clear answer after 3 attempts — still open." : (r.answer || "answered")}`).join("\n");
}

// After a check-in ends, extract whether each raised follow-up got a real answer.
async function resolveFollowUpsFromCheckin(checkIn) {
  const ids = (checkIn.metrics && checkIn.metrics.openFollowUpIds) || [];
  if (!ids.length) return;
  const docs = await ManagerFollowUp.find({ _id: { $in: ids }, status: "asked" }).lean();
  if (!docs.length) return;

  const transcript = checkIn.messages
    .map(m => `${m.role === "manager" ? "Manager" : checkIn.counsellorUsername}: ${m.text}`)
    .join("\n");

  const prompt = `A sales manager ran a WhatsApp check-in with counsellor "${checkIn.counsellorUsername}". The business owner wanted answers to these specific questions:
${docs.map((d, i) => `${i + 1}. ${d.question}`).join("\n")}

Full transcript:
${transcript}

For EACH numbered question, did the counsellor give a concrete, substantive answer (a real reason or a specific plan — not a vague deflection, "will do", or silence)?
Reply ONLY as JSON: {"results":[{"n":1,"answered":true|false,"answer":"<1-2 sentence summary of what they said, or empty>"}]}`;

  let parsed;
  try {
    const c = await openai.chat.completions.create({
      model: CHAT_MODEL,
      messages: [{ role: "system", content: "You output only valid JSON." }, { role: "user", content: prompt }],
      temperature: 0, max_tokens: 500, response_format: { type: "json_object" },
    });
    parsed = JSON.parse(c.choices[0].message.content);
  } catch (err) {
    console.error("⚠️ follow-up resolve failed:", err.message);
    return;
  }

  const results = (parsed && parsed.results) || [];
  for (let i = 0; i < docs.length; i++) {
    const d = docs[i];
    const r = results.find(x => Number(x.n) === i + 1) || {};
    if (r.answered) {
      await ManagerFollowUp.updateOne({ _id: d._id }, { $set: {
        status: "answered", answer: String(r.answer || "").slice(0, 1000), answeredAt: new Date(), updatedAt: new Date(),
      } });
    } else if ((d.askCount || 0) >= 3) {
      await ManagerFollowUp.updateOne({ _id: d._id }, { $set: {
        status: "closed", answer: String(r.answer || "").slice(0, 1000), updatedAt: new Date(),
      } });
    }
    // else: stays "asked" → re-picked by loadOpenFollowUpsFor next check-in
  }
}

// ─── Admin RAG chatbot ─────────────────────────────────────────────────
// Section 7: one line per counsellor combining Sec 1 (gap + problem type), Sec
// 2/5 (named leads to contact today), Sec 6 (last commitment kept?), and
// tomorrow's number to hit — pure composition of what's already computed above.
function buildCounsellorCascadeLines(targetAchievement, staleBuckets, hotLeads, branchCommitments) {
  const commitByUser = new Map(branchCommitments.map(c => [c.username, c]));
  const hotByUser = {};
  for (const l of hotLeads) (hotByUser[l.assignedto] = hotByUser[l.assignedto] || []).push(l);
  const staleByUser = {};
  for (const bucket of ["72h+", "48-72h", "24-48h"]) {
    for (const l of staleBuckets.buckets[bucket]) (staleByUser[l.assignedto] = staleByUser[l.assignedto] || []).push(l);
  }

  const daysLeft = Math.max(1, targetAchievement.daysRemaining);
  return targetAchievement.byCounsellor.map(c => {
    const namedHot = (hotByUser[c.username] || []).slice(0, 2).map(l => l.userName);
    const namedStale = (staleByUser[c.username] || []).slice(0, 2).map(l => l.userName);
    const namedLeads = [...namedHot, ...namedStale].slice(0, 3);
    const commit = commitByUser.get(c.username);
    const commitLine = commit && commit.total ? `${commit.kept}/${commit.total} kept` : "no prior commitments on record";
    const paceLabel = c.achievementPct === null ? "no target set" : (c.onPace ? "on pace" : "BEHIND pace");
    const dailyNumber = Math.round(c.gap / daysLeft);
    return `- ${c.username}: gap ${inr(c.gap)} (${paceLabel}); contact today: ${namedLeads.length ? namedLeads.join(", ") : "none flagged"}; last commitments: ${commitLine}; hit ${inr(dailyNumber)} by tomorrow's check-in.`;
  }).join("\n");
}

// ─── Director Review — full framework report (deterministic, not LLM-
// paraphrased). Numbers never pass through a GPT rewrite step here: this report
// is full of rupee figures and named leads, and that's exactly where letting a
// model "smooth" the text risks quietly dropping or rounding a number. Any
// single question from the framework (asked as free text) still goes through
// the normal GPT-over-context flow in answerManagerQuery below, same as every
// other question this chatbot already answers.
async function buildDirectorReviewReport() {
  const now = new Date();
  const thisMonth = getISTMonthBounds(now.getFullYear(), now.getMonth() + 1);

  const [
    targetAchievement, staleBuckets, hotLeads, demoFunnel, scorecardHistory,
    competitorMentions, objectionThemes, branchCommitments, weeklyScorecard,
  ] = await Promise.all([
    getBranchTargetAchievement(),
    getStaleLeadBuckets(),
    getHotLeadsBranchWide(),
    getDemoFunnelSummary(thisMonth.start, now),
    getRecentScorecardHistory(7),
    getCompetitorMentions(thisMonth.start, now),
    classifyObjectionThemes(thisMonth.start, now),
    getBranchCommitmentFollowThrough(),
    getBranchWeeklyScorecard(),
  ]);

  const sections = [
    `*SECTION 1 — TARGET & REVENUE*\n${buildTargetGapText(targetAchievement)}`,
    `*SECTION 2 — STALE LEADS & DEMO FUNNEL*\n${buildStaleLeadBucketsText(staleBuckets)}\n\n${buildDemoFunnelText(thisMonth.label, demoFunnel)}\n\n${buildHotLeadsBranchText(hotLeads)}`,
    `*SECTION 4 — COUNSELLOR SCORECARD (last 7 days)*\n${buildScorecardHistoryText(7, scorecardHistory)}`,
    `*SECTION 5 — COMPETITIVE PULSE*\n${buildCompetitorMentionsText(thisMonth.start, now, competitorMentions)}\n\n${buildObjectionThemesText(thisMonth.label, objectionThemes)}`,
    `*SECTION 6 — COMMITMENT FOLLOW-THROUGH*\n${buildBranchCommitmentText(branchCommitments)}`,
    `*SECTION 7 — CASCADE TO TODAY'S COUNSELLOR REVIEW*\n${buildCounsellorCascadeLines(targetAchievement, staleBuckets, hotLeads, branchCommitments)}`,
    `*SECTION 8 — WEEKLY SCORECARD*\n${buildBranchWeeklyScorecardText(weeklyScorecard)}`,
  ];

  const todayLabel = now.toLocaleDateString("en-IN", { weekday: "long", day: "2-digit", month: "long", year: "numeric", timeZone: "Asia/Kolkata" });
  return toWhatsAppMarkdown(
    `*DIRECTOR REVIEW — ${todayLabel}*\n\n` +
    sections.join("\n\n────────────────\n\n") +
    `\n\n_Not tracked in this CRM: ad-spend/cost-per-lead by source, and structured competitor/objection tagging — Section 5 above is a best-effort read of remark text, not an exact count. Seat capacity is a configured number, not a CRM fact._`
  );
}

async function answerManagerQuery(from, message) {
  const counsellorNames = filterExcludedCounsellors(
    await User.find({ role: "user" }).select("username userNumber").lean().catch(() => [])
  ).map(u => u.username);
  const intent = await classifyQuery(message, counsellorNames);

  // "director review" / "branch review" / "weekly scorecard" → the full framework
  // report, composed deterministically (see buildDirectorReviewReport's comment
  // for why it skips the GPT rewrite step). Every other question — including any
  // single sub-question from the framework — still goes through the normal flow.
  if (intent.directorReview) {
    const answer = await buildDirectorReviewReport();
    const session = await ManagerChatSession.findOne({ number: from }).lean();
    const messages = (session?.messages || [])
      .concat({ role: "user", text: message, timestamp: new Date() }, { role: "assistant", text: answer, timestamp: new Date() })
      .slice(-16);
    await ManagerChatSession.findOneAndUpdate(
      { number: from },
      { number: from, messages, updatedAt: new Date() },
      { upsert: true }
    );
    return answer;
  }

  const [ensuredIds, figures, session, followUpBlock] = await Promise.all([
    ensureCoreChunks(message, intent),
    // Authoritative live totals — needed for metric questions, skipped for
    // pure "why did lead X…" history questions (saves 3 aggregations).
    (intent.isMetric || !intent.needsHistory)
      ? getLiveFiguresBlock().catch(err => { console.error("⚠️ live figures failed:", err.message); return ""; })
      : Promise.resolve(""),
    ManagerChatSession.findOne({ number: from }).lean(),
    getAnsweredFollowUpBlock().catch(() => ""),
  ]);

  // Per-lead timeline retrieval only when the question actually needs history.
  const retrieved = intent.needsHistory
    ? await retrieveRelevantChunks(message, {
        topK: 12,
        types: ["lead", "daily-hist", "weekly-hist", "monthly-hist"],
      })
    : [];
  const coreTexts = await getChunkTextsByIds(ensuredIds);

  const seen = new Set();
  const contextParts = [];
  for (const txt of [...coreTexts, ...retrieved]) {
    if (!txt || seen.has(txt)) continue;
    seen.add(txt);
    contextParts.push(txt);
  }
  if (followUpBlock) contextParts.unshift(followUpBlock);
  const context = contextParts.join("\n\n---\n\n") || "(no data available yet)";

  const history = (session?.messages || []).slice(-8).map(m => ({ role: m.role, content: m.text }));
  const todayLabel = new Date().toLocaleDateString("en-IN", { weekday: "long", day: "2-digit", month: "long", year: "numeric", timeZone: "Asia/Kolkata" });

  const systemPrompt = [
    `You are the CRM analytics assistant for the business owner, speaking over WhatsApp.`,
    `Today is ${todayLabel} (IST). All amounts are Indian Rupees (₹).`,
    `You have three kinds of information below: (1) a LIVE CRM FIGURES block with today's/this month's authoritative totals — trust it over everything else for numbers; (2) freshly-pulled aggregate summaries; (3) retrieved history from the last ${RAG_MONTHS} months — individual lead timelines (remarks, WhatsApp chat, campaigns, billing), reconstructed daily reports, and weekly/monthly reports.`,
    `Answer ONLY from this information. When asked WHY something happened (a lead didn't convert, revenue dropped, a counsellor is behind), reason from the specific lead timelines and remarks — cite lead names, dates, and counsellors. If the context genuinely doesn't cover it, say so plainly instead of guessing.`,
    `For "conversions" / "enrollments" / "admissions" numbers, use the [CONVERSIONS …] block if one is present and quote its exact figures — never derive a conversion count from lead counts or from the trailing-3-month [COURSE PERFORMANCE] block. If asked about a period the blocks don't cover, say so.`,
    `This CRM does NOT track ad-spend or cost-per-lead by source, and has no structured field for which competitor a lead went with or why (price/timing/trust) — if asked, say plainly that it isn't tracked instead of estimating a number. Any [COMPETITOR MENTIONS] or [OBJECTION THEMES] block is a best-effort read of remark text, not an exact count — say so if you use it. Any seat-capacity figure (e.g. desk or cabin capacity) is a configured number, not a CRM fact — say so if asked.`,
    `Format for WhatsApp ONLY: single *asterisks* for bold, "-" for bullets, blank lines between sections. NEVER use markdown headings (#), double asterisks (**), or tables. Keep it tight — short lines, no filler.`,
  ].join(" ");

  const completion = await openai.chat.completions.create({
    model: ADMIN_CHAT_MODEL,
    messages: [
      { role: "system", content: systemPrompt },
      ...(figures ? [{ role: "system", content: figures }] : []),
      { role: "system", content: `CONTEXT DATA:\n\n${context}` },
      ...history,
      { role: "user", content: message },
    ],
    max_tokens: 850,
    temperature: 0.4,
  });

  const answer = toWhatsAppMarkdown(completion.choices[0].message.content.trim());

  const messages = (session?.messages || [])
    .concat({ role: "user", text: message, timestamp: new Date() }, { role: "assistant", text: answer, timestamp: new Date() })
    .slice(-16);
  await ManagerChatSession.findOneAndUpdate(
    { number: from },
    { number: from, messages, updatedAt: new Date() },
    { upsert: true }
  );

  // Fire-and-forget: does this warrant chasing a counsellor at their next check-in?
  captureFollowUpFromAdmin(message, answer).catch(err => console.error("⚠️ follow-up capture failed:", err.message));

  return answer;
}

// ─── Daily WhatsApp "Manager" Check-in ────────────────────────────────
// Mon–Sat 5 PM IST an AI "manager" opens a WhatsApp chat with each counsellor,
// asks pointed questions about today's lead activity AND digs into 6 months of
// history, records a verdict, escalates poor verdicts / no-shows / stalls to
// MANAGER_ADMIN_NUMBER, and rolls everything into one 6 PM team report.

function lastRemarkText(lead) {
  const sorted = (lead.remarks || [])
    .filter(r => r.timestamp)
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return sorted[0] ? sorted[0].remark : null;
}

function lastConvoSnippet(lead) {
  const convos = (lead.conversations || []).slice(-2);
  if (!convos.length) return null;
  return convos.map(c => `Q: ${c.userMsg} / A: ${c.botReply}`).join(" | ");
}

function pct(numerator, denominator, emptyValue = 100) {
  const den = Number(denominator) || 0;
  if (den <= 0) return emptyValue;
  return Math.max(0, Math.min(100, Math.round(((Number(numerator) || 0) / den) * 100)));
}

function scoreBand(scorePct) {
  if (scorePct === null || scorePct === undefined) return "No score";
  if (scorePct >= 80) return "Outstanding";
  if (scorePct >= 60) return "On Track";
  if (scorePct >= 40) return "Needs Coaching";
  return "Escalate";
}

function formatScore(scorePct) {
  return scorePct === null || scorePct === undefined ? "N/A" : `${(scorePct / 10).toFixed(1)}/10`;
}

function formatCommitment(c) {
  const lead = c.leadName || c.lead || "lead";
  const action = c.action || c.commitment || "follow up";
  const deadline = c.deadline || c.byWhen || "today";
  return `${lead}: ${action} by ${deadline}`;
}

async function loadYesterdayCommitments(username, today) {
  const previous = await ManagerCheckIn.findOne({
    counsellorUsername: username,
    date: { $lt: today },
    "metrics.commitmentsMade.0": { $exists: true },
  }).sort({ date: -1 }).lean();

  return ((previous?.metrics && previous.metrics.commitmentsMade) || [])
    .slice(0, 8)
    .map(c => ({
      leadName: c.leadName || c.lead || "",
      action: c.action || c.commitment || "",
      deadline: c.deadline || c.byWhen || "",
      sourceDate: previous.date,
    }))
    .filter(c => c.leadName || c.action);
}

async function computeCommitmentFollowThrough(username, commitments, dayStart, dayEnd) {
  const items = Array.isArray(commitments) ? commitments : [];
  if (!items.length) {
    return { total: 0, kept: 0, missed: 0, pct: null, details: [] };
  }

  const details = [];
  let kept = 0;
  for (const commitment of items) {
    const leadName = String(commitment.leadName || commitment.lead || "").trim();
    const query = { assignedto: username };
    if (leadName) query.userName = { $regex: leadName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), $options: "i" };
    const lead = await Chat.findOne(query)
      .select("userName pipeline remarks lastInteracted")
      .sort({ lastInteracted: -1 })
      .lean();

    const touchedToday = !!lead && (
      (lead.remarks || []).some(r => r.timestamp && new Date(r.timestamp) >= dayStart && new Date(r.timestamp) <= dayEnd) ||
      (lead.lastInteracted && new Date(lead.lastInteracted) >= dayStart && new Date(lead.lastInteracted) <= dayEnd)
    );
    const enrolled = !!lead && /^(enrolled|converted)$/i.test(lead.pipeline || "");
    const isKept = touchedToday || enrolled;
    if (isKept) kept++;
    details.push({
      ...commitment,
      kept: isKept,
      evidence: enrolled ? "lead is enrolled" : (touchedToday ? "CRM has activity today" : "no matching CRM activity today"),
      matchedLead: lead?.userName || null,
    });
  }

  return {
    total: items.length,
    kept,
    missed: items.length - kept,
    pct: pct(kept, items.length, null),
    details,
  };
}

function buildManagerScorecard(metrics) {
  const ta = metrics.targetAchievement || {};
  const commitment = metrics.commitmentFollowThrough || {};
  const stale = metrics.staleLeadReduction || {};
  const expectedPacePct = ta.expectedPacePct || null;
  const targetPacePct = ta.achievementPct === null || ta.achievementPct === undefined || !expectedPacePct
    ? null
    : Math.max(0, Math.min(100, Math.round((ta.achievementPct / expectedPacePct) * 100)));

  const components = {
    newLeadContactRate: {
      label: "New lead contact rate",
      weight: 25,
      pct: pct(metrics.newLeadsWorkedOnToday, metrics.newLeadsToday),
      raw: `${metrics.newLeadsWorkedOnToday || 0}/${metrics.newLeadsToday || 0} new leads touched today`,
    },
    followUpSla: {
      label: "Follow-up SLA compliance",
      weight: 25,
      pct: pct(metrics.remindersHandledToday, metrics.remindersDueToday),
      raw: `${metrics.remindersHandledToday || 0}/${metrics.remindersDueToday || 0} due reminders handled`,
    },
    monthlyTargetProgress: {
      label: "Monthly target progress",
      weight: 30,
      pct: targetPacePct,
      raw: ta && ta.target > 0
        ? `Rs.${(ta.totalPaid || 0).toLocaleString("en-IN")} collected / Rs.${(ta.target || 0).toLocaleString("en-IN")} target; expected pace ${expectedPacePct}%`
        : "No monthly target set",
    },
    staleLeadReduction: {
      label: "Stale-lead reduction",
      weight: 10,
      pct: stale.pct,
      raw: `${stale.previous7d ?? 0} stale yesterday -> ${stale.current7d ?? 0} stale today`,
    },
    commitmentFollowThrough: {
      label: "Commitment follow-through",
      weight: 10,
      pct: commitment.pct === null || commitment.pct === undefined ? 100 : commitment.pct,
      raw: commitment.total
        ? `${commitment.kept}/${commitment.total} yesterday commitments kept`
        : "No prior commitments found",
    },
  };

  let weighted = 0;
  let weights = 0;
  for (const component of Object.values(components)) {
    if (component.pct === null || component.pct === undefined) continue;
    weighted += component.pct * component.weight;
    weights += component.weight;
  }

  const compositePct = weights ? Math.round(weighted / weights) : null;
  return {
    compositePct,
    ratingOutOf10: compositePct === null ? null : Number((compositePct / 10).toFixed(1)),
    band: scoreBand(compositePct),
    escalates: compositePct !== null && compositePct < MANAGER_ESCALATE_SCORE_PCT,
    components,
  };
}

function buildScorecardText(scorecard) {
  if (!scorecard || scorecard.compositePct === null || scorecard.compositePct === undefined) {
    return "CRM score: N/A";
  }
  const lines = [
    `CRM score: ${formatScore(scorecard.compositePct)} (${scorecard.band})`,
  ];
  for (const c of Object.values(scorecard.components || {})) {
    lines.push(`- ${c.label}: ${c.pct === null || c.pct === undefined ? "N/A" : `${c.pct}%`} (${c.raw})`);
  }
  return lines.join("\n");
}

// Defaults are deliberately generic — set MANAGER_COURSE_KNOWLEDGE / MANAGER_COMPETITOR_PLAYBOOK /
// MANAGER_MARKET_TRENDS in .env with verified BetaSpace pricing and facts. Never invent figures here.
const DEFAULT_COURSE_KNOWLEDGE = [
  "BetaSpace (https://betaspace.in) offers flexible co-working workspaces: hot desks, dedicated desks, private cabins, team offices, meeting rooms and virtual offices.",
  "Do not quote prices, availability, discounts or lease terms unless they appear in the CRM data or the configured knowledge base. Availability is always confirmed by the BetaSpace team.",
].join("\n");

const DEFAULT_COMPETITOR_PLAYBOOK = [
  "If a lead compares other co-working spaces or traditional offices, ask which one and what they were offered (price, location, lock-in, amenities).",
  "Position flexibility (no long lease lock-in), ready infrastructure and lower setup cost versus a traditional office — only where these are true for the plan being discussed.",
  "Push for a site visit: leads who see the space convert far better than leads who only get a quote.",
].join("\n");

const DEFAULT_MARKET_TRENDS = [
  "Startups, freelancers and remote teams increasingly prefer flexible, short-commitment workspace over a traditional lease.",
  "Use this only as coaching context for price/timing hesitation; do not recite it as a script.",
].join("\n");

function getManagerKnowledgeBase() {
  return {
    courseKnowledge: process.env.MANAGER_COURSE_KNOWLEDGE || DEFAULT_COURSE_KNOWLEDGE,
    competitorPlaybook: process.env.MANAGER_COMPETITOR_PLAYBOOK || DEFAULT_COMPETITOR_PLAYBOOK,
    marketTrends: process.env.MANAGER_MARKET_TRENDS || DEFAULT_MARKET_TRENDS,
  };
}

async function computeCounsellorDailyMetrics(username, dayStart, dayEnd) {
  const activeLeads = await Chat.find({
    assignedto: username,
    pipeline: { $not: { $regex: /^(enrolled|converted)$/i } }
  }).select("userName course status pipeline remarks conversations reminder lastInteracted datecreated leadfrom leadgenId respondedAfterFollowUp").lean();

  const monthStart = new Date(dayStart.getFullYear(), dayStart.getMonth(), 1, 0, 0, 0, 0);
  const totalActiveLeads = await Chat.countDocuments({
    assignedto: username,
    datecreated: { $gte: monthStart, $lte: dayEnd },
    pipeline: { $not: { $regex: /^(enrolled|not interested)$/i } }
  });

  const newLeadsTodayList = activeLeads.filter(l =>
    l.datecreated && new Date(l.datecreated) >= dayStart && new Date(l.datecreated) <= dayEnd
  );
  const newLeadsToday = newLeadsTodayList.length;

  const touchedTodayIds = new Set();
  for (const lead of activeLeads) {
    const touchedToday = (lead.remarks || []).some(r =>
      r.timestamp && new Date(r.timestamp) >= dayStart && new Date(r.timestamp) <= dayEnd
    );
    if (touchedToday) touchedTodayIds.add(String(lead._id));
  }
  const remarksAddedToday = touchedTodayIds.size;

  const newLeadsWorkedOnToday = newLeadsTodayList.filter(l => touchedTodayIds.has(String(l._id))).length;
  const newLeadsUntouchedTodayAll = newLeadsTodayList.filter(l => !touchedTodayIds.has(String(l._id)));
  const newLeadsUntouchedToday = newLeadsUntouchedTodayAll.length;
  const newLeadsUntouched = newLeadsUntouchedTodayAll.slice(0, 5).map(l => ({
    userName: l.userName,
    course: l.course || "N/A"
  }));

  const remindersDueToday = activeLeads.filter(l =>
    l.reminder && new Date(l.reminder) >= dayStart && new Date(l.reminder) <= dayEnd
  );
  const remindersHandledToday = remindersDueToday.filter(l => touchedTodayIds.has(String(l._id))).length;
  const remindersMissedToday = remindersDueToday.length - remindersHandledToday;

  const windowStart = new Date(dayStart);
  windowStart.setDate(windowStart.getDate() - 2);
  const windowStart2 = new Date(dayStart);
  windowStart2.setDate(windowStart2.getDate() - 1);

  for (const lead of activeLeads) {
    const createdAt = lead.datecreated ? new Date(lead.datecreated) : windowStart;
    const effectiveStart = createdAt > windowStart ? createdAt : windowStart;
    const expectedDays = Math.max(1, Math.min(3, Math.floor((dayEnd - effectiveStart) / 86400000) + 1));
    const followUpDateSet = new Set(
      (lead.remarks || [])
        .filter(r => r.timestamp && new Date(r.timestamp) >= windowStart && new Date(r.timestamp) <= dayEnd)
        .map(r => getISTDateString(new Date(r.timestamp)))
    );
    lead._followUp3Day = {
      daysFollowedUp: followUpDateSet.size,
      expectedDays,
      lagging: followUpDateSet.size < expectedDays
    };
    lead._stagnant2Day = !(lead.remarks || []).some(r =>
      r.timestamp && new Date(r.timestamp) >= windowStart2 && new Date(r.timestamp) <= dayEnd
    );
  }

  const laggingAll = activeLeads.filter(l => l._followUp3Day.lagging);
  const likelyToConvert = activeLeads.filter(l => /^(hot|warm)$/i.test(l.status || ""));
  const laggingLikelyToConvert = likelyToConvert
    .filter(l => l._followUp3Day.lagging)
    .sort((a, b) => a._followUp3Day.daysFollowedUp - b._followUp3Day.daysFollowedUp)
    .slice(0, 5)
    .map(l => ({
      userName: l.userName,
      course: l.course || "N/A",
      status: l.status,
      daysFollowedUp: l._followUp3Day.daysFollowedUp,
      expectedDays: l._followUp3Day.expectedDays,
      lastRemark: lastRemarkText(l),
      lastConvo: lastConvoSnippet(l)
    }));

  const hotNotEnrolledAll = likelyToConvert;
  const hotNotEnrolled = hotNotEnrolledAll.slice(0, 5).map(l => ({
    userName: l.userName,
    course: l.course || "N/A",
    status: l.status,
    lastRemark: lastRemarkText(l),
    lastConvo: lastConvoSnippet(l)
  }));

  const pipelineStagnantAll = activeLeads.filter(l =>
    l._stagnant2Day && !/^(not interested|irrelevant)$/i.test(l.pipeline || "")
  );
  const pipelineStagnant = pipelineStagnantAll
    .sort((a, b) => new Date(a.lastInteracted || 0) - new Date(b.lastInteracted || 0))
    .slice(0, 5)
    .map(l => ({
      userName: l.userName,
      course: l.course || "N/A",
      pipeline: l.pipeline || "New Lead",
      status: l.status,
      lastRemark: lastRemarkText(l),
      lastConvo: lastConvoSnippet(l)
    }));

  const demoScheduled = activeLeads
    .filter(l => /^(site visit|demo scheduled)$/i.test(l.pipeline || ""))
    .slice(0, 5)
    .map(l => ({
      userName: l.userName,
      course: l.course || "N/A",
      lastRemark: lastRemarkText(l),
      lastConvo: lastConvoSnippet(l),
      touchedLast2Days: !l._stagnant2Day
    }));

  const metaLeadsToday = activeLeads
    .filter(l =>
      l.datecreated && new Date(l.datecreated) >= dayStart && new Date(l.datecreated) <= dayEnd &&
      (l.leadgenId || /meta/i.test(l.leadfrom || ""))
    )
    .slice(0, 5)
    .map(l => ({ userName: l.userName, course: l.course || "N/A" }));

  const respondedAfterFollowUpToday = activeLeads
    .filter(l =>
      l.respondedAfterFollowUp && l.lastInteracted &&
      new Date(l.lastInteracted) >= dayStart && new Date(l.lastInteracted) <= dayEnd
    )
    .slice(0, 5)
    .map(l => ({ userName: l.userName, course: l.course || "N/A" }));

  const enrolledToday = await Chat.countDocuments({
    assignedto: username,
    pipeline: { $regex: /^(enrolled|converted)$/i },
    lastInteracted: { $gte: dayStart, $lte: dayEnd }
  });

  const lastTouchAt = (lead) => {
    const remarkAt = (lead.remarks || [])
      .map(r => r.timestamp && new Date(r.timestamp))
      .filter(Boolean)
      .sort((a, b) => b - a)[0];
    const interactedAt = lead.lastInteracted ? new Date(lead.lastInteracted) : null;
    const createdAt = lead.datecreated ? new Date(lead.datecreated) : null;
    return [remarkAt, interactedAt, createdAt].filter(Boolean).sort((a, b) => b - a)[0] || null;
  };
  const staleCutoff = new Date(dayEnd.getTime() - 7 * 86400000);
  const previousStaleCutoff = new Date(dayStart.getTime() - 7 * 86400000);
  const current7d = activeLeads.filter(l => {
    const ts = lastTouchAt(l);
    return !ts || ts < staleCutoff;
  }).length;
  const previous7d = activeLeads.filter(l => {
    const createdAt = l.datecreated ? new Date(l.datecreated) : null;
    if (createdAt && createdAt > dayStart) return false;
    const ts = lastTouchAt(l);
    return !ts || ts < previousStaleCutoff;
  }).length;
  const staleReductionPct = previous7d <= 0
    ? (current7d <= 0 ? 100 : 0)
    : Math.max(0, Math.min(100, Math.round(((previous7d - current7d) / previous7d) * 100)));

  return {
    totalActiveLeads,
    newLeadsToday,
    newLeadsWorkedOnToday,
    newLeadsUntouchedToday,
    newLeadsUntouched,
    remarksAddedToday,
    remindersDueToday: remindersDueToday.length,
    remindersHandledToday,
    remindersMissedToday,
    followUpContinuity: {
      laggingCount: laggingAll.length,
      continuousCount: activeLeads.length - laggingAll.length,
      lagging: laggingLikelyToConvert
    },
    hotNotEnrolledCount: hotNotEnrolledAll.length,
    hotNotEnrolled,
    enrolledToday,
    pipelineStagnantCount: pipelineStagnantAll.length,
    pipelineStagnant,
    demoScheduled,
    todayHighlights: {
      metaLeadsToday,
      respondedAfterFollowUpToday
    },
    staleLeadReduction: {
      previous7d,
      current7d,
      reduced: Math.max(0, previous7d - current7d),
      pct: staleReductionPct
    }
  };
}

async function computeTargetAchievement(username) {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
  const end = new Date(year, month, 0, 23, 59, 59, 999);

  const [targetDoc, paidAgg] = await Promise.all([
    UserMonthlyTarget.findOne({ username, year, month }).lean(),
    Chat.aggregate([
      { $match: { assignedto: username } },
      { $unwind: { path: "$fees.installments", preserveNullAndEmptyArrays: false } },
      { $match: { "fees.installments.status": "Paid", "fees.installments.dueDate": { $gte: start, $lte: end } } },
      { $group: { _id: null, totalPaid: { $sum: { $ifNull: ["$fees.installments.amount", 0] } } } }
    ])
  ]);

  const target = targetDoc?.target || 0;
  const totalPaid = paidAgg[0]?.totalPaid || 0;
  const achievementPct = target > 0 ? Math.min(100, Math.round((totalPaid / target) * 100)) : null;
  const daysInMonth = new Date(year, month, 0).getDate();
  const dayOfMonth = Math.min(daysInMonth, Math.max(1, now.getDate()));
  const expectedPacePct = Math.min(100, Math.round((dayOfMonth / daysInMonth) * 100));
  return { year, month, target, totalPaid, achievementPct, expectedPacePct };
}

// Per-counsellor 6-month history block injected into the check-in prompt so the
// manager can ask specific "why hasn't this moved / why no action" questions.
async function buildCheckinRagContext(username) {
  const now = Date.now();
  const d7 = new Date(now - 7 * 864e5);
  const d14 = new Date(now - 14 * 864e5);
  const d30 = new Date(now - 30 * 864e5);
  const d180 = new Date(now - 180 * 864e5);

  const leads = await Chat.find({
    assignedto: username,
    pipeline: { $not: /^(enrolled|not interested|irrelevant|junk)$/i },
  }).select("userName course status pipeline datecreated lastInteracted remarks").lean();

  for (const l of leads) {
    const ts = (l.remarks || [])
      .map(r => r.timestamp && new Date(r.timestamp))
      .filter(Boolean)
      .sort((a, b) => b - a)[0] || null;
    l._lastRemarkAt = ts;
    l._daysStale = Math.floor((now - (ts ? ts.getTime() : new Date(l.datecreated).getTime())) / 864e5);
  }

  const fmtLead = (l) => {
    const r = lastRemarkText(l);
    return `${l.userName} (${l.course || "N/A"}, stage "${l.pipeline || "New Lead"}", ${l.status || "Cold"}) — ${l._daysStale}d since last touch${r ? `; last remark: "${String(r).slice(0, 120)}"` : "; no remarks ever"}`;
  };

  const stale = leads
    .filter(l => new Date(l.datecreated) < d7 && (!l._lastRemarkAt || l._lastRemarkAt < d7))
    .sort((a, b) => b._daysStale - a._daysStale)
    .slice(0, 8);
  const lastWeek = leads
    .filter(l => {
      const c = new Date(l.datecreated);
      return c >= d14 && c < d7 && (!l._lastRemarkAt || l._lastRemarkAt < d7);
    })
    .slice(0, 6);
  const oldHotWarm = leads
    .filter(l => /^(hot|warm)$/i.test(l.status || "") && new Date(l.datecreated) < d30 && new Date(l.datecreated) > d180)
    .sort((a, b) => b._daysStale - a._daysStale)
    .slice(0, 6);

  let ragChunks = [];
  try {
    ragChunks = await retrieveRelevantChunks(
      `leads that stalled, did not convert, no follow up, delay, objection, pending decision for counsellor ${username}`,
      { topK: 4, assignedto: username, types: ["lead"] }
    );
  } catch (_) { /* best effort */ }
  // Keep the injected context bounded — the list sections above are the
  // high-signal part; trim each verbose lead timeline.
  ragChunks = ragChunks.map(c => (c.length > 900 ? c.slice(0, 900) + "\n…" : c));

  const sections = [];
  if (stale.length) sections.push(`OPEN LEADS SITTING UNTOUCHED (7+ days, no recent remark) — ask why each hasn't been worked:\n${stale.map(l => `  - ${fmtLead(l)}`).join("\n")}`);
  if (lastWeek.length) sections.push(`LAST WEEK'S LEADS WITH NO ACTION SINCE — ask what happened to these:\n${lastWeek.map(l => `  - ${fmtLead(l)}`).join("\n")}`);
  if (oldHotWarm.length) sections.push(`OLDER HOT/WARM LEADS STILL NOT CLOSED (1–6 months old) — ask why they never converted and what the current plan is:\n${oldHotWarm.map(l => `  - ${fmtLead(l)}`).join("\n")}`);
  if (ragChunks.length) sections.push(`RECENT HISTORY ON SOME OF THIS COUNSELLOR'S LEADS (use concrete details to ask informed questions):\n${ragChunks.join("\n\n")}`);

  return sections.join("\n\n");
}

function buildManagerSystemPrompt(username, metrics, questionCount, extras = {}) {
  const fc = metrics.followUpContinuity || { laggingCount: 0, continuousCount: 0, lagging: [] };
  const laggingList = (fc.lagging || []).map(l => {
    const parts = [`${l.userName} (${l.course}, ${l.status}) — followed up ${l.daysFollowedUp}/${l.expectedDays} days`];
    if (l.lastRemark) parts.push(`last remark: "${l.lastRemark}"`);
    if (l.lastConvo) parts.push(`recent chat: ${l.lastConvo}`);
    return `  - ${parts.join("; ")}`;
  }).join("\n");

  const hotList = (metrics.hotNotEnrolled || []).map(l => {
    const parts = [`${l.userName} (${l.course}, ${l.status})`];
    if (l.lastRemark) parts.push(`last remark: "${l.lastRemark}"`);
    if (l.lastConvo) parts.push(`recent chat: ${l.lastConvo}`);
    return `  - ${parts.join("; ")}`;
  }).join("\n");

  const newLeadsUntouchedList = (metrics.newLeadsUntouched || []).map(l => `${l.userName} (${l.course})`).join(", ");

  const th = metrics.todayHighlights || { metaLeadsToday: [], respondedAfterFollowUpToday: [] };
  const metaList = (th.metaLeadsToday || []).map(l => `${l.userName} (${l.course})`).join(", ");
  const respondedList = (th.respondedAfterFollowUpToday || []).map(l => `${l.userName} (${l.course})`).join(", ");

  const stagnantList = (metrics.pipelineStagnant || []).map(l => {
    const parts = [`${l.userName} (${l.course}) — stuck at pipeline stage "${l.pipeline}"`];
    if (l.lastRemark) parts.push(`last remark: "${l.lastRemark}"`);
    if (l.lastConvo) parts.push(`recent chat: ${l.lastConvo}`);
    return `  - ${parts.join("; ")}`;
  }).join("\n");

  const demoList = (metrics.demoScheduled || []).map(l => {
    const parts = [`${l.userName} (${l.course})`, l.touchedLast2Days ? "touched in last 2 days" : "NOT touched in last 2 days"];
    if (l.lastRemark) parts.push(`last remark: "${l.lastRemark}"`);
    if (l.lastConvo) parts.push(`recent chat: ${l.lastConvo}`);
    return `  - ${parts.join("; ")}`;
  }).join("\n");

  const ta = metrics.targetAchievement || null;
  const lowAchievement = ta && ta.target > 0 && ta.achievementPct !== null && ta.achievementPct < 50;
  const scorecard = metrics.scorecard || buildManagerScorecard(metrics);
  const scorecardText = buildScorecardText(scorecard);
  const commitmentDetails = metrics.commitmentFollowThrough || { details: [] };
  const commitmentBlock = (commitmentDetails.details || []).length
    ? (commitmentDetails.details || []).map(c => `- ${formatCommitment(c)} => ${c.kept ? "kept" : "missed"} (${c.evidence || "CRM checked"})`).join("\n")
    : "- No yesterday commitments found in the CRM memory.";
  const { courseKnowledge, competitorPlaybook, marketTrends } = getManagerKnowledgeBase();

  const historicalContext = extras.historicalContext || "";
  const openFollowUps = extras.openFollowUps || [];

  return `You are the daily Review Meeting agent for BetaSpace's co-working sales team. Act like a hands-on branch manager running a short 5 PM coaching huddle with sales executive "${username}". Your job is to increase workspace bookings by reviewing CRM facts, catching gaps, and giving specific next actions.
It is currently around 5 PM IST (early evening) — greet accordingly (e.g. "Hi" or "Good evening"). Never say "Good morning" or "Good afternoon".

Today's numbers for ${username}:
- Total active (non-enrolled) leads assigned: ${metrics.totalActiveLeads}
- New leads received today: ${metrics.newLeadsToday} (already worked on: ${metrics.newLeadsWorkedOnToday}, still untouched: ${metrics.newLeadsUntouchedToday})
${newLeadsUntouchedList ? `  Untouched new leads — ask specifically why these haven't been contacted yet: ${newLeadsUntouchedList}` : ""}
- Leads that got a remark logged today: ${metrics.remarksAddedToday}
- Reminders/follow-ups due today: ${metrics.remindersDueToday} (handled: ${metrics.remindersHandledToday}, missed: ${metrics.remindersMissedToday})
- Converted today: ${metrics.enrolledToday}

Last-3-days follow-up continuity:
- Leads NOT followed up continuously over the last 3 days: ${fc.laggingCount}
- Leads with continuous daily follow-up: ${fc.continuousCount}
${laggingList ? `- Likely-to-convert leads (Hot/Warm) with GAPS in daily follow-up — ask exactly what is delaying each, using the remark/chat context:\n${laggingList}` : ""}

Last-2-days pipeline movement:
- Leads with NO remark logged in the last 2 days: ${metrics.pipelineStagnantCount}
${stagnantList ? `  Ask specifically why each hasn't moved forward — check whether the remark shown is old before accepting "I already followed up":\n${stagnantList}` : "  (none — everything active has an update in the last 2 days)"}

Site Visit leads — ask how the site visit went and whether they followed up afterwards:
${demoList || "  (none currently at Site Visit stage)"}

Hot/Warm leads still not converted: ${metrics.hotNotEnrolledCount}
${hotList || "  (none)"}

Today's leads needing special attention:
${metaList ? `- PAID Meta ad leads received today (company pays per lead — must NOT be wasted): ${metaList}` : "- No paid Meta ad leads today."}
${respondedList ? `- Leads that responded to an automated follow-up today (fresh interest — act fast): ${respondedList}` : "- No leads responded to a follow-up today."}

This month's target (${ta ? `${ta.month}/${ta.year}` : "N/A"}):
${ta && ta.target > 0
    ? `- Revenue target: Rs.${ta.target.toLocaleString("en-IN")}, collected so far: Rs.${ta.totalPaid.toLocaleString("en-IN")} (${ta.achievementPct}% achieved)`
    : "- No target set for this month for this counsellor."}
${lowAchievement ? `- Achievement is LOW. After today's activity, ask about target progress and suggest 2-3 specific Hot/Warm leads above to prioritize closing.` : ""}
CRM scorecard for today's review:
${scorecardText}

Yesterday commitment follow-through:
${commitmentBlock}

Course and product knowledge you can use for coaching:
${courseKnowledge}

Competitor positioning playbook:
${competitorPlaybook}

Job-market/timing talking points:
${marketTrends}

${historicalContext ? `
────────────────────────────
LEAD HISTORY & CONTEXT (from the last ${RAG_MONTHS} months of CRM data — use these SPECIFIC facts to ask informed, pointed questions; always refer to leads by name):
${historicalContext}
────────────────────────────` : ""}
${openFollowUps.length ? `
THE BUSINESS OWNER PERSONALLY ASKED YOU TO GET ANSWERS ON THESE — you MUST raise every one during this check-in and keep pressing until you get a concrete, specific answer (not "I'll do it" / "will follow up"):
${openFollowUps.map((f, i) => `  ${i + 1}. ${f.question}`).join("\n")}
` : ""}

Your job:
- Always start by checking yesterday's commitments above if any exist. Do not move to new work until they say whether each named commitment happened.
- When a lead is stalled, diagnose the blocker first: price, timing, trust, competitor, or unreachable. Then suggest one specific tactic from the course knowledge, competitor playbook, or market trends. Never give generic "follow up more" advice.
- After every counsellor answer, do three checks before the next question: (1) does it contradict the CRM data above, (2) is it vague, and (3) is there a specific next action with owner and deadline? If it contradicts CRM data, name the mismatch. If it is vague, ask for the lead name/number/figure. If there is no next action, get one before moving topics.
- Do not state that the counsellor confirmed/reported/said anything unless they actually typed it in this chat.
- Sound like a real human manager on WhatsApp — natural, varied phrasing, reacting to what they actually say. Do NOT read a fixed checklist or repeat yourself. Short, direct questions (under ~50 words), ONE at a time, with genuine follow-ups when an answer is vague, evasive, or doesn't add up.
- Cover today's activity: new leads received and whether the untouched ones have been contacted (ask by name), remarks/follow-ups skipped, why stagnant-pipeline leads haven't moved, how Site Visit demos went, and what's delaying each named likely-to-convert lead with a follow-up gap.
- Dig into the LEAD HISTORY above: ask why specific old leads haven't moved in weeks, why last week's leads got no action, and why named Hot/Warm leads still haven't converted after months — cite the actual remark/stage/age. Don't accept "I'll check" — get the real reason.
- Ask about progress toward this month's target and % achieved; if low, name specific leads to prioritize.
- For any paid Meta ad lead or lead that responded to a follow-up today, point out why it matters and press for their follow-up plan.
- If they claim "no leads" or "nothing to report" while leads ARE assigned, press for the real reason and state it plainly in your summary. Push back firmly (but professionally, never insulting) on excuses like admin/office work — lead follow-up is priority #1.
- If they mention offering a discount, check whether they cleared the specific discount with the manager.
- This is a thorough review: you have asked ${questionCount} questions so far. You MUST ask at least ${MANAGER_MIN_QUESTIONS} and at most ${MANAGER_MAX_QUESTIONS} questions in total. ${questionCount < MANAGER_MIN_QUESTIONS ? `You are still below the minimum of ${MANAGER_MIN_QUESTIONS} — do NOT wrap up and do NOT output a VERDICT; keep asking the next question.` : `You may conclude only once every area below is covered.`}
- Spread the questions across ALL of these areas, one at a time, and go to the next area once the current one is answered concretely: (1) yesterday's commitments, (2) new leads received today and whether each untouched one was contacted, (3) calls/WhatsApps made today and connect rate, (4) remarks and follow-ups skipped or overdue, (5) stagnant-pipeline leads and the blocker for each, (6) Site Visit leads and demo outcomes, (7) Hot/Warm likely-to-convert leads, one by one by name, (8) old leads from the history above, (9) paid Meta ad leads and leads that replied today, (10) objections heard today (price, timing, trust, competitor) and how they handled them, (11) discounts offered, (12) month target, % achieved and the plan to close the gap, (13) tomorrow's plan with named leads and deadlines, (14) any help or support they need from the manager.
- Do not repeat a question already answered. If an area has nothing to ask about, move on, but still reach the minimum by digging deeper into named leads and following up on vague answers.
- When you have asked at least ${MANAGER_MIN_QUESTIONS} questions and covered the areas above (never beyond ${MANAGER_MAX_QUESTIONS}), end your FINAL message with a new line in EXACTLY this internal control format:
VERDICT: SATISFACTORY - <one-line reason>
or
VERDICT: NEEDS_IMPROVEMENT - <one-line reason>
Do not mention the word "VERDICT" at any other time. The actual report score comes from the CRM scorecard, not your subjective wording.`;
}

async function extractCommitmentsFromCheckin(checkIn) {
  const counsellorMessages = (checkIn.messages || []).filter(m => m.role === "counsellor");
  if (!counsellorMessages.length) return [];
  const transcript = (checkIn.messages || [])
    .map(m => `${m.role === "manager" ? "Manager" : checkIn.counsellorUsername}: ${m.text}`)
    .join("\n");

  const prompt = `Extract only specific, checkable commitments made by counsellor "${checkIn.counsellorUsername}" in this WhatsApp review.

Rules:
- Include only commitments the counsellor actually typed or clearly agreed to.
- Each item needs a lead name if available, action, and deadline/by-when if available.
- Do not invent commitments from manager suggestions unless the counsellor accepted them.

Transcript:
${transcript}

Reply only as JSON: {"commitments":[{"leadName":"", "action":"", "deadline":""}]}`;

  try {
    const c = await openai.chat.completions.create({
      model: CHAT_MODEL,
      messages: [{ role: "system", content: "You output only valid JSON." }, { role: "user", content: prompt }],
      temperature: 0,
      max_tokens: 400,
      response_format: { type: "json_object" },
    });
    const parsed = JSON.parse(c.choices[0].message.content);
    return ((parsed && parsed.commitments) || [])
      .map(item => ({
        leadName: String(item.leadName || item.lead || "").slice(0, 120),
        action: String(item.action || item.commitment || "").slice(0, 300),
        deadline: String(item.deadline || item.byWhen || "").slice(0, 120),
      }))
      .filter(item => item.leadName || item.action)
      .slice(0, 10);
  } catch (err) {
    console.error(`⚠️ Commitment extraction failed for ${checkIn.counsellorUsername}:`, err.message);
    return [];
  }
}

async function ensureCommitmentsCaptured(checkIn) {
  const metrics = checkIn.metrics || {};
  if (Array.isArray(metrics.commitmentsMade)) return metrics.commitmentsMade;
  const commitments = await extractCommitmentsFromCheckin(checkIn);
  checkIn.metrics = { ...metrics, commitmentsMade: commitments };
  await checkIn.save();
  return commitments;
}

async function generateAdminSummaryReport(checkIn) {
  const transcript = checkIn.messages
    .map(m => `${m.role === "manager" ? "Manager" : checkIn.counsellorUsername}: ${m.text}`)
    .join("\n");

  const counsellorMessages = (checkIn.messages || []).filter(m => m.role === "counsellor");
  const metrics = checkIn.metrics || {};
  const scorecard = metrics.scorecard || buildManagerScorecard(metrics);
  const scoreText = buildScorecardText(scorecard);

  if (checkIn.status === "no_response" || counsellorMessages.length === 0) {
    return [
      `Counsellor: ${checkIn.counsellorUsername}`,
      `Date: ${checkIn.date}`,
      "Status: NO_RESPONSE",
      "No response received.",
      "",
      scoreText,
      "",
      "Commitments kept vs missed: No response received.",
      "New commitments made today: None.",
      // NO_RESPONSE always implies ESCALATE (Section 6), independent of score.
      "Flags: NO_RESPONSE, ESCALATE",
    ].join("\n");
  }

  const commitmentLines = ((metrics.commitmentFollowThrough && metrics.commitmentFollowThrough.details) || [])
    .map(c => `- ${formatCommitment(c)} => ${c.kept ? "kept" : "missed"} (${c.evidence || "CRM checked"})`)
    .join("\n") || "- None found.";

  const outcomeLabel = checkIn.status === "no_response"
    ? "Did not respond to the check-in"
    : (checkIn.verdict || "Unknown");

  const prompt = `You are preparing an internal report for the admin about a counsellor's daily WhatsApp manager check-in.

Counsellor: ${checkIn.counsellorUsername}
Date: ${checkIn.date}
Outcome: ${outcomeLabel}

CRM scorecard:
${scoreText}

Yesterday commitments from CRM memory:
${commitmentLines}

Full check-in transcript (every question and answer):
${transcript || "(no conversation took place)"}

Write a thorough report (max 450 words, plain text for WhatsApp) covering EVERY topic that came up in the transcript — do not skip, drop, or silently summarize away any question/answer pair, even minor ones:
1. What the counsellor worked on today (based on their answers)
2. Pipeline updates: which leads are stuck and why, which Site Visit leads came up and their outcome/follow-up status
3. This month's target achievement % and what they said about it (and any leads they were pointed toward for closing the gap)
4. If they claimed to have "no leads" or "nothing to report" at any point, state PLAINLY the reason they gave for that — never omit this
5. What could have been done better
6. What they appear to have missed (leads/remarks/follow-ups)
7. An overall productivity rating: Poor / Average / Good / Excellent, with a one-line justification
Be factual and specific, referencing the actual numbers/leads/reasons discussed. If a topic did not come up in the transcript, say so briefly rather than inventing an answer — but do not leave out anything that DID come up.
Hard rule: never state that the counsellor "confirmed", "said", or "reported" anything unless it appears in the counsellor's own messages above. If a topic was not answered, write "No response received" for that topic.`;

  try {
    const completion = await openai.chat.completions.create({
      model: CHAT_MODEL,
      messages: [
        { role: "system", content: "You are an internal reporting assistant for a sales manager. Respond in plain text suitable for WhatsApp. Never infer answers from silence." },
        { role: "user", content: prompt }
      ],
      max_tokens: 900,
      temperature: 0.4
    });
    return completion.choices[0].message.content.trim();
  } catch (err) {
    console.error(`⚠️ Admin summary report generation failed for ${checkIn.counsellorUsername}:`, err.message);
    return null;
  }
}

async function ensureAdminSummaryReport(checkIn) {
  if (checkIn.adminSummaryReport) return checkIn.adminSummaryReport;
  if ((checkIn.messages || []).some(m => m.role === "counsellor")) {
    await ensureCommitmentsCaptured(checkIn);
  }
  const summary = await generateAdminSummaryReport(checkIn);
  if (!summary) return null;
  checkIn.adminSummaryReport = summary;
  await checkIn.save();
  return summary;
}

async function sendAdminSummaryReport(checkIn) {
  const summary = await ensureAdminSummaryReport(checkIn);
  if (!summary) return;
  await sendManagerWhatsAppText(
    MANAGER_ADMIN_NUMBER,
    `📋 Daily Check-in Report — ${checkIn.counsellorUsername} (${checkIn.date})\n\n${summary}`
  );
}

function applyScoreBasedVerdict(checkIn, fallbackReason = "") {
  const metrics = checkIn.metrics || {};
  const scorecard = metrics.scorecard || buildManagerScorecard(metrics);
  checkIn.metrics = { ...metrics, scorecard };
  if (scorecard.compositePct === null || scorecard.compositePct === undefined) {
    checkIn.verdict = "NEEDS_IMPROVEMENT";
    checkIn.verdictReason = fallbackReason || "CRM score unavailable; needs manual review.";
    checkIn.escalatedToAdmin = true;
    return scorecard;
  }
  checkIn.verdict = scorecard.compositePct < MANAGER_NEEDS_IMPROVEMENT_PCT ? "NEEDS_IMPROVEMENT" : "SATISFACTORY";
  checkIn.verdictReason = `CRM score ${formatScore(scorecard.compositePct)} (${scorecard.band}). ${fallbackReason || ""}`.trim();
  checkIn.escalatedToAdmin = scorecard.escalates;
  return scorecard;
}

async function startManagerCheckIn(counsellor) {
  const username = counsellor.username;
  if (!participatesInManagerReview(counsellor)) {
    console.log(`Manager review disabled for ${username} - skipping check-in`);
    return;
  }
  const mobile = cleanIndianMobileNumber(counsellor.userNumber);
  if (!mobile) {
    console.warn(`⚠️ No mobile for counsellor ${username} — skipping manager check-in`);
    return;
  }

  const today = getISTDateString();
  const existing = await ManagerCheckIn.findOne({ counsellorUsername: username, date: today });
  if (existing) {
    console.log(`ℹ️ Manager check-in already exists for ${username} on ${today} — skipping`);
    return;
  }

  const now = new Date();
  const { dayStart, dayEnd } = getISTDayBounds(now);

  const metrics = await computeCounsellorDailyMetrics(username, dayStart, dayEnd);
  metrics.targetAchievement = await computeTargetAchievement(username).catch(err => {
    console.error(`⚠️ Target achievement lookup failed for ${username}:`, err.message);
    return null;
  });
  metrics.yesterdaysCommitments = await loadYesterdayCommitments(username, today).catch(err => {
    console.error(`⚠️ Yesterday commitment lookup failed for ${username}:`, err.message);
    return [];
  });
  metrics.commitmentFollowThrough = await computeCommitmentFollowThrough(
    username,
    metrics.yesterdaysCommitments,
    dayStart,
    dayEnd
  ).catch(err => {
    console.error(`⚠️ Commitment follow-through lookup failed for ${username}:`, err.message);
    return { total: 0, kept: 0, missed: 0, pct: null, details: [] };
  });
  metrics.scorecard = buildManagerScorecard(metrics);

  const [historicalContext, followUps] = await Promise.all([
    buildCheckinRagContext(username).catch(err => {
      console.error(`⚠️ Check-in RAG context failed for ${username}:`, err.message);
      return "";
    }),
    loadOpenFollowUpsFor(username).catch(() => []),
  ]);
  metrics.historicalContext = historicalContext;
  metrics.openFollowUps = followUps.map(f => ({ id: String(f._id), question: f.question }));
  metrics.openFollowUpIds = followUps.map(f => String(f._id));

  const extras = { historicalContext, openFollowUps: metrics.openFollowUps };
  const systemPrompt = buildManagerSystemPrompt(username, metrics, 0, extras);

  const commitments = metrics.yesterdaysCommitments || [];
  let openingMessage = commitments.length
    ? `Hi ${username}, quick 5 PM review. Yesterday you committed to ${commitments.slice(0, 3).map(formatCommitment).join("; ")}. Did these happen?`
    : `Hi ${username}, quick 5 PM review on today's leads. CRM score is ${formatScore(metrics.scorecard?.compositePct)}. Which lead needs the most help converting before EOD?`;
  try {
    const completion = await openai.chat.completions.create({
      model: CHAT_MODEL,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: "Start the check-in now with a brief greeting and your first question." }
      ],
      max_tokens: 150,
      temperature: 0.6
    });
    openingMessage = completion.choices[0].message.content.trim();
  } catch (aiErr) {
    console.error(`⚠️ Manager check-in AI opening failed for ${username}, using fallback:`, aiErr.message);
  }

  await ManagerCheckIn.create({
    counsellorUsername: username,
    counsellorNumber: mobile,
    date: today,
    metrics,
    messages: [{ role: "manager", text: openingMessage }],
    questionCount: 1,
    status: "in_progress",
    startedAt: now,
    lastManagerMessageAt: now
  });

  if (followUps.length) {
    await ManagerFollowUp.updateMany(
      { _id: { $in: followUps.map(f => f._id) } },
      { $set: { status: "asked", lastAskedCheckinDate: today, updatedAt: new Date() }, $inc: { askCount: 1 } }
    );
  }

  await sendManagerWhatsAppText(mobile, openingMessage);
  console.log(`✅ Manager check-in started for ${username} (${mobile})`);
}

// Coerce GPT's markdown into WhatsApp's dialect: **bold** → *bold*, drop
// "# " / "### " heading markers, tidy "* " bullets → "- ".
function toWhatsAppMarkdown(text) {
  return String(text)
    .replace(/\*\*([^*\n]+)\*\*/g, "*$1*")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/^(\s*)\*\s+/gm, "$1- ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// ─── WhatsApp send helpers (mirror index.js's sendWhatsAppText) ────────
async function sendManagerWhatsAppText(to, message) {
  const wpUrl = process.env.WP_CHAT_SERVICE_URL;
  if (!wpUrl || !to) return;
  try {
    await fetch(`${wpUrl}/internal/send-text`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, message }),
    });
  } catch (err) {
    console.error(`❌ Manager chat WhatsApp send failed for ${to}:`, err.message);
  }
}

async function sendChunkedWhatsApp(to, text) {
  const LIMIT = 3800;
  if (text.length <= LIMIT) {
    await sendManagerWhatsAppText(to, text);
    return;
  }
  const lines = text.split("\n");
  const chunks = [];
  let current = "";
  for (const line of lines) {
    const candidate = current ? `${current}\n${line}` : line;
    if (candidate.length > LIMIT) {
      if (current) chunks.push(current);
      current = line;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  for (const c of chunks) await sendManagerWhatsAppText(to, c);
}

// ─── Consolidated 6 PM team report ────────────────────────────────────
async function runConsolidatedCheckinReport() {
  console.log("📋 Running consolidated manager check-in report cron...");
  const today = getISTDateString();
  const [allCounsellors, checkIns] = await Promise.all([
    // managerReviewEnabled must be selected here or filterManagerReviewCounsellors
    // silently falls back to only the hardcoded username list below.
    User.find({ role: "user" }).select("username userNumber managerReviewEnabled").lean(),
    ManagerCheckIn.find({ date: today }),
  ]);
  const counsellors = filterManagerReviewCounsellors(allCounsellors);
  const checkInByUsername = new Map(checkIns.map(c => [c.counsellorUsername, c]));

  let completedCount = 0, needsImprovementCount = 0, inProgressCount = 0, notStartedCount = 0;
  const sections = [];

  for (const counsellor of counsellors) {
    const checkIn = checkInByUsername.get(counsellor.username);
    if (!checkIn) {
      notStartedCount++;
      sections.push(`*${counsellor.username}* — check-in did not start today.`);
      continue;
    }
    let statusLabel;
    if (checkIn.status === "in_progress") { inProgressCount++; statusLabel = "IN PROGRESS as of 6 PM"; }
    else if (checkIn.status === "no_response") { notStartedCount++; statusLabel = "NO RESPONSE"; }
    else if (checkIn.verdict === "NEEDS_IMPROVEMENT") { needsImprovementCount++; statusLabel = "NEEDS IMPROVEMENT"; }
    else { completedCount++; statusLabel = checkIn.verdict || "COMPLETED"; }

    const summary = await ensureAdminSummaryReport(checkIn).catch(err => {
      console.error(`⚠️ Consolidated report: summary failed for ${checkIn.counsellorUsername}:`, err.message);
      return null;
    });
    sections.push(`*${checkIn.counsellorUsername}* — ${statusLabel}\n${summary || "(summary unavailable)"}`);
  }

  const header = `📊 *Daily Manager Check-in — Team Report (${today})*\n\n` +
    `Completed: ${completedCount} | Needs improvement: ${needsImprovementCount} | Still in progress: ${inProgressCount} | Not started/no response: ${notStartedCount}`;

  const CHUNK_LIMIT = 3800;
  const chunks = [];
  let current = header;
  for (const section of sections) {
    const candidate = `${current}\n\n${section}`;
    if (candidate.length > CHUNK_LIMIT) { chunks.push(current); current = section; }
    else { current = candidate; }
  }
  chunks.push(current);
  for (const chunk of chunks) await sendManagerWhatsAppText(MANAGER_ADMIN_NUMBER, chunk);

  // Follow-ups the owner asked about, raised in today's check-ins.
  try {
    const fups = await ManagerFollowUp.find({ lastAskedCheckinDate: today }).lean();
    if (fups.length) {
      let block = `📌 *Follow-ups you asked about (${today})*\n`;
      block += fups.map(f => {
        const who = f.counsellorUsername || "team";
        if (f.status === "answered") return `• (${who}) ${f.question}\n   → ${f.answer || "answered"}`;
        if (f.status === "closed") return `• (${who}) ${f.question}\n   → No clear answer after 3 attempts — needs your attention.`;
        return `• (${who}) ${f.question}\n   → No clear answer yet — will re-ask next check-in.`;
      }).join("\n");
      await sendChunkedWhatsApp(MANAGER_ADMIN_NUMBER, block);
    }
  } catch (err) {
    console.error("⚠️ Follow-up report section failed:", err.message);
  }

  console.log(`✅ Consolidated manager check-in report sent (${chunks.length} message(s))`);
}

// ─── Manual trigger (admin "Trigger Now" button in Review Meeting) ─────
// Same eligibility + per-counsellor call as the 5 PM cron below, just invoked
// on demand instead of on a schedule. Skips anyone who already has today's
// check-in started (the ManagerCheckIn unique index would reject a duplicate
// anyway), so pressing the button twice — or pressing it after 5 PM has
// already run — is always safe.
async function triggerManagerCheckInsNow() {
  const counsellors = filterManagerReviewCounsellors(await User.find({ role: "user" }).lean());
  const today = getISTDateString();
  const existingToday = new Set(
    (await ManagerCheckIn.find({ date: today }, { counsellorUsername: 1 }).lean())
      .map((c) => c.counsellorUsername)
  );

  let started = 0;
  let alreadyStarted = 0;
  const failures = [];

  for (const counsellor of counsellors) {
    if (existingToday.has(counsellor.username)) {
      alreadyStarted++;
      continue;
    }
    try {
      await startManagerCheckIn(counsellor);
      started++;
    } catch (err) {
      failures.push({ username: counsellor.username, message: err.message });
      console.error(`❌ Manual manager check-in trigger failed for ${counsellor.username}:`, err.message);
    }
  }

  return { eligible: counsellors.length, started, alreadyStarted, failed: failures.length, failures };
}

// ─── Cron scheduling ─────────────────────────────────────────────────
let _cronsScheduled = false;
function scheduleManagerCrons() {
  if (_cronsScheduled) return;
  _cronsScheduled = true;
  const TZ = { timezone: "Asia/Kolkata" };

  // Daily manager check-in — Mon–Sat 5 PM IST
  cron.schedule("0 17 * * 1-6", async () => {
    try {
      console.log("🧑‍💼 Running daily manager check-in cron...");
      const counsellors = filterManagerReviewCounsellors(await User.find({ role: "user" }).lean());
      for (const counsellor of counsellors) {
        try { await startManagerCheckIn(counsellor); }
        catch (err) { console.error(`❌ Manager check-in failed to start for ${counsellor.username}:`, err.message); }
      }
    } catch (err) {
      console.error("❌ Daily manager check-in cron error:", err);
    }
  }, TZ);

  // Reply-timeout nudge — every minute during the check-in window
  cron.schedule("* 17-19 * * 1-6", async () => {
    try {
      const today = getISTDateString();
      const finalCutoff = new Date(Date.now() - MANAGER_FINAL_NUDGE_MINUTES * 60 * 1000);
      const noShows = await ManagerCheckIn.find({
        date: today,
        status: "in_progress",
        lastManagerMessageAt: { $ne: null, $lte: finalCutoff },
        nudgeSentAt: { $ne: null },
        messages: { $not: { $elemMatch: { role: "counsellor" } } },
      });
      for (const checkIn of noShows) {
        try {
          checkIn.status = "no_response";
          checkIn.escalatedToAdmin = true;
          checkIn.completedAt = new Date();
          await checkIn.save();
          await sendManagerWhatsAppText(
            checkIn.counsellorNumber,
            `${checkIn.counsellorUsername}, no response received after two reminders, so today's review is closed as NO_RESPONSE.`
          );
          await sendManagerWhatsAppText(
            MANAGER_ADMIN_NUMBER,
            `⚠️ ${checkIn.counsellorUsername} has NO_RESPONSE for today's manager review after ${MANAGER_FINAL_NUDGE_MINUTES} minutes.`
          );
          await sendAdminSummaryReport(checkIn);
          await resolveFollowUpsFromCheckin(checkIn).catch(() => {});
        } catch (err) {
          console.error(`❌ Manager check-in no-show close failed for ${checkIn.counsellorUsername}:`, err.message);
        }
      }

      const cutoff = new Date(Date.now() - MANAGER_NUDGE_MINUTES * 60 * 1000);
      const pending = await ManagerCheckIn.find({
        date: today,
        status: "in_progress",
        lastManagerMessageAt: { $ne: null, $lte: cutoff },
        nudgeSentAt: null,
      });
      for (const checkIn of pending) {
        try {
          checkIn.nudgeSentAt = new Date();
          await checkIn.save();
          await sendManagerWhatsAppText(
            checkIn.counsellorNumber,
            `⏰ ${checkIn.counsellorUsername}, please reply to today's review. If there is still no response by ${MANAGER_FINAL_NUDGE_MINUTES} minutes, it will close as NO_RESPONSE.`
          );
        } catch (err) {
          console.error(`❌ Manager check-in nudge failed for ${checkIn.counsellorUsername}:`, err.message);
        }
      }
    } catch (err) {
      console.error("❌ Manager check-in nudge sweep error:", err);
    }
  }, TZ);

  // Consolidated team report — Mon–Sat 6 PM IST
  cron.schedule("0 18 * * 1-6", async () => {
    try { await runConsolidatedCheckinReport(); }
    catch (err) { console.error("❌ Consolidated manager check-in report cron error:", err); }
  }, TZ);

  // No-response escalation — Mon–Sat 8 PM IST
  cron.schedule("0 20 * * 1-6", async () => {
    try {
      console.log("🧑‍💼 Running manager check-in no-response sweep...");
      const today = getISTDateString();
      const stragglers = await ManagerCheckIn.find({ date: today, status: "in_progress" });
      for (const checkIn of stragglers) {
        try {
          const alreadyEscalated = checkIn.escalatedToAdmin;
          const repliedAtLeastOnce = checkIn.messages.some(m => m.role === "counsellor");
          let scorecard = null;
          if (repliedAtLeastOnce) {
            // They engaged but never reached a VERDICT before the cutoff — this is
            // NOT the same failure mode as a total no-show (spec 7.2), so score it
            // from CRM data like any other completed check-in instead of wiping
            // their transcript with a blanket "no response" report.
            checkIn.status = "completed";
            checkIn.completedAt = new Date();
            scorecard = applyScoreBasedVerdict(checkIn, "Conversation was abandoned before 8 PM cutoff without a clear conclusion.");
          } else {
            checkIn.status = "no_response";
            checkIn.escalatedToAdmin = true;
          }
          await checkIn.save();
          if (!alreadyEscalated) {
            const note = repliedAtLeastOnce
              ? `⚠️ ${checkIn.counsellorUsername} started today's manager check-in but stopped replying. CRM score: ${formatScore(scorecard?.compositePct)}.`
              : `⚠️ ${checkIn.counsellorUsername} has NOT responded to today's manager check-in at all.`;
            await sendManagerWhatsAppText(MANAGER_ADMIN_NUMBER, note);
          }
          await sendAdminSummaryReport(checkIn);
          // Follow-ups raised in this abandoned check-in: close them if they've
          // hit the 3-attempt cap, otherwise leave them to be re-asked.
          await resolveFollowUpsFromCheckin(checkIn).catch(() => {});
        } catch (err) {
          console.error(`❌ No-response escalation failed for ${checkIn.counsellorUsername}:`, err.message);
        }
      }
    } catch (err) {
      console.error("❌ Manager check-in no-response sweep error:", err);
    }
  }, TZ);

  // Nightly full RAG ingest — 1:30 AM IST
  cron.schedule("30 1 * * *", async () => {
    try { await runFullRagIngest(); }
    catch (err) { console.error("❌ Nightly manager RAG ingest error:", err); }
  }, TZ);

  console.log("🗓️  Manager crons scheduled (check-in 5PM, nudge, 6PM report, 8PM sweep, 1:30AM RAG ingest)");
}

// ─── Routes ────────────────────────────────────────────────────────────
function ragKeyOk(req, res) {
  if (!RAG_KEY) { res.status(503).json({ ok: false, message: "MANAGER_RAG_KEY not configured" }); return false; }
  if (req.get("x-rag-key") !== RAG_KEY) { res.status(401).json({ ok: false, message: "invalid x-rag-key" }); return false; }
  return true;
}

function registerManagerChatRoutes(app, managerAdminNumber) {
  if (managerAdminNumber) MANAGER_ADMIN_NUMBER = String(managerAdminNumber);

  // Admin RAG chatbot — wpchat-api forwards MANAGER_ADMIN_NUMBER's messages here.
  app.post("/manager/chat/incoming", async (req, res) => {
    try {
      console.log("from local")
      const { from, message } = req.body || {};
      if (!from || !message || typeof message !== "string") {
        return res.status(400).json({ handled: false, message: "from and message are required" });
      }
      if (String(from) !== String(MANAGER_ADMIN_NUMBER)) {
        return res.json({ handled: false });
      }
      const answer = await answerManagerQuery(from, message.trim());
      await sendChunkedWhatsApp(from, answer);
      res.json({ handled: true, answer });
    } catch (err) {
      console.error("❌ /manager/chat/incoming error:", err);
      try {
        await sendManagerWhatsAppText(req.body?.from, "Sorry, I hit an error pulling that data. Please try again in a bit.");
      } catch (_) { /* best-effort */ }
      res.status(500).json({ handled: false, message: err.message });
    }
  });

  // Counsellor check-in replies — wpchat-api routes in-progress check-in replies here.
  app.post("/manager/checkin/reply", async (req, res) => {
    try {
      const { userNumber, message } = req.body;
      const mobile = cleanIndianMobileNumber(userNumber);
      if (!mobile || !message) {
        return res.status(400).json({ handled: false, message: "userNumber and message are required" });
      }

      const today = getISTDateString();
      const checkIn = await ManagerCheckIn.findOne({ counsellorNumber: mobile, date: today, status: "in_progress" });
      if (!checkIn) return res.json({ handled: false });

      checkIn.messages.push({ role: "counsellor", text: message });

      const systemPrompt = buildManagerSystemPrompt(
        checkIn.counsellorUsername,
        checkIn.metrics,
        checkIn.questionCount,
        {
          historicalContext: (checkIn.metrics && checkIn.metrics.historicalContext) || "",
          openFollowUps: (checkIn.metrics && checkIn.metrics.openFollowUps) || [],
        }
      );
      const conversation = checkIn.messages.map(m => ({
        role: m.role === "counsellor" ? "user" : "assistant",
        content: m.text
      }));

      let replyText = "Noted. Please make sure all pending leads are followed up today.";
      try {
        const completion = await openai.chat.completions.create({
          model: CHAT_MODEL,
          messages: [{ role: "system", content: systemPrompt }, ...conversation],
          max_tokens: 220,
          temperature: 0.6
        });
        replyText = completion.choices[0].message.content.trim();
      } catch (aiErr) {
        console.error(`⚠️ Manager check-in AI reply failed for ${checkIn.counsellorUsername}:`, aiErr.message);
      }

      let verdictMatch = replyText.match(/VERDICT:\s*(SATISFACTORY|NEEDS_IMPROVEMENT)\s*-?\s*(.*)/i);

      // The model tried to wrap up before the minimum number of questions —
      // regenerate once, forcing it to keep asking.
      if (verdictMatch && checkIn.questionCount < MANAGER_MIN_QUESTIONS) {
        try {
          const retry = await openai.chat.completions.create({
            model: CHAT_MODEL,
            messages: [
              { role: "system", content: systemPrompt },
              ...conversation,
              { role: "system", content: `Do not conclude yet — only ${checkIn.questionCount} of the minimum ${MANAGER_MIN_QUESTIONS} questions have been asked. Reply with your next question on an area not yet covered. No VERDICT.` }
            ],
            max_tokens: 220,
            temperature: 0.6
          });
          const retryText = retry.choices[0].message.content.trim();
          if (retryText && !/VERDICT:/i.test(retryText)) {
            replyText = retryText;
            verdictMatch = null;
          }
        } catch (retryErr) {
          console.error(`⚠️ Manager check-in retry failed for ${checkIn.counsellorUsername}:`, retryErr.message);
        }
      }
      let justCompleted = false;

      if (verdictMatch) {
        const verdictReason = verdictMatch[2].trim();
        replyText = replyText.slice(0, verdictMatch.index).trim();
        if (!replyText) replyText = "Thanks — that's all for today's check-in.";

        checkIn.status = "completed";
        checkIn.completedAt = new Date();
        justCompleted = true;
        const scorecard = applyScoreBasedVerdict(checkIn, verdictReason);

        if (checkIn.escalatedToAdmin) {
          await sendManagerWhatsAppText(
            MANAGER_ADMIN_NUMBER,
            `⚠️ ${checkIn.counsellorUsername}'s daily review needs attention: CRM score ${formatScore(scorecard.compositePct)}. ${checkIn.verdictReason}`
          );
        }
      } else if (checkIn.questionCount >= MANAGER_MAX_QUESTIONS) {
        replyText = replyText || "Thanks for your responses — that's all for today's check-in.";
        checkIn.status = "completed";
        checkIn.completedAt = new Date();
        justCompleted = true;
        const scorecard = applyScoreBasedVerdict(checkIn, "Check-in reached the maximum question limit.");
        if (checkIn.escalatedToAdmin) {
          await sendManagerWhatsAppText(
            MANAGER_ADMIN_NUMBER,
            `⚠️ ${checkIn.counsellorUsername}'s daily review needs attention: CRM score ${formatScore(scorecard.compositePct)}. ${checkIn.verdictReason}`
          );
        }
      } else {
        checkIn.questionCount += 1;
        checkIn.lastManagerMessageAt = new Date();
        checkIn.nudgeSentAt = null;
      }

      checkIn.messages.push({ role: "manager", text: replyText });
      await checkIn.save();

      await sendManagerWhatsAppText(mobile, replyText);

      if (justCompleted) {
        ensureAdminSummaryReport(checkIn).catch(err =>
          console.error(`⚠️ Admin summary generation failed for ${checkIn.counsellorUsername}:`, err.message)
        );
        resolveFollowUpsFromCheckin(checkIn).catch(err =>
          console.error(`⚠️ Follow-up resolution failed for ${checkIn.counsellorUsername}:`, err.message)
        );
      }

      res.json({ handled: true });
    } catch (err) {
      console.error("❌ /manager/checkin/reply error:", err);
      res.status(500).json({ handled: false, message: err.message });
    }
  });

  // Full RAG rebuild — runs in the background, responds immediately.
  app.post("/manager/rag/reindex", (req, res) => {
    if (!ragKeyOk(req, res)) return;
    res.json({ ok: true, message: "reindex started" });
    runFullRagIngest().catch(err => console.error("❌ manual RAG reindex failed:", err.message));
  });

  // RAG store status.
  app.get("/manager/rag/status", async (req, res) => {
    if (!ragKeyOk(req, res)) return;
    try {
      const counts = await ManagerChatChunk.aggregate([{ $group: { _id: "$type", n: { $sum: 1 } } }]);
      res.json({
        ok: true,
        lastIngestAt: _lastIngestAt,
        ingestRunning: _ingestRunning,
        byType: counts.reduce((o, c) => (o[c._id] = c.n, o), {}),
        total: counts.reduce((s, c) => s + c.n, 0),
      });
    } catch (err) {
      res.status(500).json({ ok: false, message: err.message });
    }
  });

  scheduleManagerCrons();

  // One-time RAG ingest on boot if the corpus is empty. Waits for the Mongo
  // connection, then delays a little so it doesn't compete with startup work.
  const kickBootIngest = () => setTimeout(() => {
    bootIngestIfEmpty().catch(e => console.error("⚠️ Manager RAG boot ingest failed:", e.message));
  }, 45000);
  if (mongoose.connection.readyState === 1) kickBootIngest();
  else mongoose.connection.once("open", kickBootIngest);
}

module.exports = {
  registerManagerChatRoutes,
  runFullRagIngest,
  answerManagerQuery,
  startManagerCheckIn,
  triggerManagerCheckInsNow,
  // Internals exposed for scripted testing / one-off ops.
  _test: {
    buildCheckinRagContext,
    loadOpenFollowUpsFor,
    captureFollowUpFromAdmin,
    resolveFollowUpsFromCheckin,
    runConsolidatedCheckinReport,
    getLiveFiguresBlock,
    retrieveRelevantChunks,
    ingestLeadChunks,
    ingestDailyHistChunks,
    classifyQuery,
    ensureCoreChunks,
    getConversionByCourse,
    buildConversionByCourseText,
    getConversionSummary,
    // Branch Manager Director Review Framework internals.
    getBranchTargetAchievement,
    getConversionByCounsellor,
    getLeadSourceSummary,
    getStaleLeadBuckets,
    getDemoFunnelSummary,
    getHotLeadsBranchWide,
    getCourseSeatFillStatus,
    getUpcomingBatchFillSummary,
    getRecentScorecardHistory,
    getCompetitorMentions,
    classifyObjectionThemes,
    getBranchCommitmentFollowThrough,
    getBranchWeeklyScorecard,
    buildDirectorReviewReport,
  },
};
