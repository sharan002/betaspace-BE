require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server: SocketIOServer } = require('socket.io');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');
const cron = require('node-cron');
const app = express();
const server = http.createServer(app);
const cors = require("cors");
const { User, Chat, Trainer: TrainerModel, Batch, UserMonthlyTarget, GroupMessage, LeadGroup, WeeklyReport, MinminiReport, MonthlyReport, MonthlyMinminiReport, TempLead, ManagerCheckIn, CoworkSpace, SeatBooking } = require("./Schema");
const jwt = require('jsonwebtoken');
const cookieParser = require("cookie-parser");
const { generateAndSendBill, deleteBill } = require("./billUtils");
const { generateCertificates, deleteFromS3, generateCombinedCertificate, uploadBufferToS3 } = require("./certificateGenerator");
const { generateCourseCertificate } = require("./CourseCertificateGenerator");
const { generateCourseCompletionCertificates, generateCombinedCourseCompletionCertificate } = require("./CourseCompletionCertificateGenerator");
const { generateIdCard } = require("./IdCardGenerator");
const { sendLeadCapiEvent } = require("./metaConversions");
const { CONVERTED_STAGES, CONVERTED_RE, META_CAPI_STAGES } = require("./pipelineStages");
const { isWithinOfficeHours, businessMinutesBetween, offHoursMinutesBetween } = require("./businessHours");
const multer = require('multer');
const idCardPhotoUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype)),
});
app.use(cookieParser());
app.use(bodyParser.json());
const { Configuration, OpenAIApi } = require("openai");
const OpenAI = require("openai");
const path = require('path');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const analyticsClient = new BetaAnalyticsDataClient({
  keyFilename: path.join(__dirname, 'gen-lang-client-0473171780-0991219131a2.json')
});

const dns  = require("dns");

dns.setServers(["1.1.1.1", "8.8.8.8"]);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const openai = new OpenAI({
apiKey : process.env.OPENAI_API_KEY,});

// Classify a lead as Hot / Warm / Cold based on recent counselor remarks
async function analyzeRemarkStatus(chat) {
  try {
    const recentRemarks = (chat.remarks || [])
      .slice(-5)
      .map(r => `[${new Date(r.timestamp).toLocaleDateString("en-IN")}] ${r.remark}`)
      .join("\n");
    if (!recentRemarks.trim()) return null;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: `You are a CRM lead-scoring assistant for BetaSpace, a co-working space provider.
Classify the lead's likelihood of booking a workspace based on sales executive remarks.
Reply with ONLY one word: Hot, Warm, or Cold.
Hot  — strong interest, asking about pricing/availability/move-in date/site visit, ready to decide soon.
Warm — some interest but hesitant, needs follow-up, still evaluating.
Cold — not interested, unresponsive, postponing indefinitely, or negative signals.`
        },
        {
          role: "user",
          content: `Lead: ${chat.userName || "Unknown"}
Workspace Interest: ${chat.courseofintrest || chat.course || "Not specified"}
Recent Counselor Remarks:\n${recentRemarks}

Classify this lead:`
        }
      ],
      max_tokens: 5,
      temperature: 0.2
    });

    const raw = (completion.choices[0]?.message?.content || "").trim().toLowerCase();
    if (raw === "hot") return "Hot";
    if (raw === "warm") return "Warm";
    if (raw === "cold") return "Cold";
    return null;
  } catch (err) {
    console.error("AI remark status error:", err.message);
    return null;
  }
}

const corsOptions = {
  origin: [
    'http://localhost:4200',
    'https://shadowcarsfe.vercel.app',
    'https://sharan002.github.io',
    'http://127.0.0.1:5500',
    'http://localhost:5173',
    'http://localhost:5174',
    'http://localhost:3000',
    'https://appincoimbatore.com',
    'https://cert.appincoimbatore.com',
    'https://placement.appincoimbatore.com',
    'https://localhost'
  ],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
};

// External-facing routes (/external/*) are called from static sites hosted anywhere,
// with no fixed origin, so they get an open CORS origin. Every other route keeps
// the strict origin allowlist above, unchanged.
const corsOptionsDelegate = (req, callback) => {
  const isExternalPortal = req.path.startsWith('/external/');
  callback(null, {
    origin: isExternalPortal ? true : corsOptions.origin,
    methods: corsOptions.methods,
    allowedHeaders: [...corsOptions.allowedHeaders, 'x-portal-username', 'x-portal-password'],
    credentials: isExternalPortal ? false : corsOptions.credentials,
  });
};

// ✅ APPLY FIRST
app.use(cors(corsOptionsDelegate));

// ✅ EXPLICITLY HANDLE PREFLIGHT
app.options('*', cors(corsOptionsDelegate));

// Body parsers after CORS
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Socket.IO setup ────────────────────────────────────────────────────────
const io = new SocketIOServer(server, {
  cors: {
    origin: corsOptions.origin,
    methods: ['GET', 'POST'],
    credentials: true,
  },
});

// Authenticate socket connections using the same JWT
io.use((socket, next) => {
  const token = socket.handshake.auth.token;
  if (!token) return next(new Error('Unauthorized'));
  try {
    const decoded = jwt.verify(token, 'ACCESS_SECRET_123');
    socket.user = decoded;
    next();
  } catch {
    next(new Error('Unauthorized'));
  }
});

io.on('connection', (socket) => {
  const username = socket.user.username;
  socket.join('crm-group');

  socket.on('send_message', async ({ message }) => {
    if (!message || !message.trim()) return;
    try {
      const msg = await GroupMessage.create({ sender: username, message: message.trim() });
      io.to('crm-group').emit('new_message', {
        _id: msg._id,
        sender: msg.sender,
        message: msg.message,
        timestamp: msg.timestamp,
      });
      notifyGroupChatOnWhatsApp(msg.sender, msg.message);
    } catch (err) {
      console.error('Group chat save error:', err);
    }
  });
});
// ────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8001;
mongoose.set('strictQuery', false);
mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
})
  .then(async () => {
    console.log('✅ Connected to MongoDB');
    // One-time backfill: existing leads created before the `pipelineChangedAt` field
    // existed have no value, so seed it from `datecreated` (their last known stage date).
    try {
      const r = await Chat.updateMany(
        { pipelineChangedAt: { $exists: false } },
        [{ $set: { pipelineChangedAt: { $ifNull: ['$datecreated', '$$NOW'] } } }]
      );
      if (r.modifiedCount) console.log(`🔧 Backfilled pipelineChangedAt on ${r.modifiedCount} leads`);
    } catch (e) {
      console.error('pipelineChangedAt backfill failed:', e.message);
    }
  })
  .catch((err) => console.error('❌ MongoDB connection error:', err));
// Get all users
app.get("/users", async (req, res) => {
  try {
    const users = await User.find({})
      .sort({ lastInteracted: -1 });

    const leads = await Chat.find({})
      .sort({ datecreated: -1 });
    console.log("From CRM backend")
    return res.status(200).json({
      success: true,
      users,
      leads
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: "Server error" });
  }
});

// Update user (General update for status, assignee, reminder, etc.)
app.put("/Users", async (req, res) => {
  const updatedChat = req.body;
  if (!updatedChat._id) return res.status(400).json({ error: "_id required" });
  try {
    const chat = await Chat.findByIdAndUpdate(updatedChat._id, updatedChat, { new: true });
    if (!chat) return res.status(404).json({ error: "User not found" });
    res.json(chat);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});



// --------------------------------------------
// 🔐 JWT Functions
// --------------------------------------------
const createAccessToken = (user) => {
  console.log("Access")
  return jwt.sign(
    { id: user._id, role: user.role, username: user.username },
    "ACCESS_SECRET_123",
    { expiresIn: "20y" }
  );
};

const createRefreshToken = (user) => {
  return jwt.sign(
    { id: user._id },
    "REFRESH_SECRET_123",
    { expiresIn: "7d" }
  );
};

// --------------------------------------------
// 🛡 Auth Middleware
// --------------------------------------------
const auth = (req, res, next) => {
  const token = req.headers.authorization?.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "No token provided" });
  }

  try {
    const decoded = jwt.verify(token, "ACCESS_SECRET_123");
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({ message: "Invalid token" });
  }
};


// --------------------------------------------
// 🔄 Refresh Token Route
// --------------------------------------------
app.post("/refresh", (req, res) => {
  const token = req.cookies.refreshToken;

  if (!token)
    return res.status(401).json({ message: "No refresh token" });

  jwt.verify(token, "REFRESH_SECRET_123", (err, data) => {
    if (err)
      return res.status(403).json({ message: "Invalid refresh token" });

    const newAccessToken = jwt.sign(
      { id: data.id },
      "ACCESS_SECRET_123",
      { expiresIn: "15m" }
    );

    res.json({ accessToken: newAccessToken });
  });
});


// app.post("/leads/analyze/:id", async (req, res) => {
//   try {
//     const leadId = req.params.id;
//     const lead = await Chat.findById(leadId);

//     if (!lead) {
//       return res.status(404).json({
//         success: false,
//         message: "Lead not found"
//       });
//     }

//     // Safe guards for undefined arrays
//     const remarksText = (lead.remarks || [])
//       .map(r => `[${r.timestamp}] ${r.remark}`)
//       .join("\n");

//     const conversationsText = (lead.conversations || [])
//       .map(c => `User: ${c.userMsg}\nBot: ${c.botReply}`)
//       .join("\n---\n");

//     const prompt = `
// Analyze the following lead based on their remarks and conversation history.

// Lead Details:
// - Name: ${lead.userName}
// - Course of Interest: ${lead.courseofintrest}
// - Current Status: ${lead.status}
// - Pipeline Stage: ${lead.pipeline}

// Remarks History:
// ${remarksText || "No remarks available."}

// Conversation History:
// ${conversationsText || "No conversation history available."}

// Return ONLY valid JSON with:
// {
//   "summary": "",
//   "intentScore": 0,
//   "buyingSignals": [],
//   "objections": [],
//   "recommendedAction": "",
//   "suggestedMessage": ""
// }
// `;

//     // ✅ Works with your OpenAI setup
//     const completion = await openai.chat.completions.create({
//       model: "gpt-4o-mini",
//       messages: [
//         { role: "system", content: "You are an expert sales analyst assistant." },
//         { role: "user", content: prompt }
//       ],
//       response_format: { type: "json_object" }
//     });

//     const analysisResult = JSON.parse(
//       completion.choices[0].message.content
//     );

//     // Save AI analysis
//     lead.aiAnalysis = analysisResult;
//     await lead.save();

//     res.json({
//       success: true,
//       analysis: analysisResult
//     });

//   } catch (error) {
//     console.error("Error analyzing lead:", error);
//     res.status(500).json({
//       success: false,
//       message: "Failed to analyze lead",
//       error: error.message
//     });
//   }
// });



function cleanIndianMobileNumber(input) {
  if (!input) return null;

  let digits = input.toString().replace(/\D/g, '');

  if (digits.startsWith("91") && digits.length === 12) {
    return digits;
  }

  if (digits.length === 10) {
    return "91" + digits;
  }

  if (digits.length === 11 && digits.startsWith("0")) {
    return "91" + digits.slice(1);
  }

  return digits;
}

async function sendWhatsAppText(to, message) {
  const wpUrl = process.env.WP_CHAT_SERVICE_URL;
  if (!wpUrl || !to) return;
  try {
    await fetch(`${wpUrl}/internal/send-text`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, message })
    });
  } catch (err) {
    console.error(`❌ WhatsApp send-text failed for ${to}:`, err.message);
  }
}

// Admin number that receives escalations for the daily manager check-in feature.
const MANAGER_ADMIN_NUMBER = "919597271533";

// RAG chatbot letting MANAGER_ADMIN_NUMBER ask free-form questions about CRM
// data (revenue, leads, pipeline changes, remarks, reports) over WhatsApp.
// See ManagerChat.js — wpchat-api's webhook forwards this number's messages
// to POST /manager/chat/incoming, registered here.
const { registerManagerChatRoutes } = require("./ManagerChat");
registerManagerChatRoutes(app, MANAGER_ADMIN_NUMBER);

// --- HR Portal (isolated employee-records + documents feature) ---
// All routes live under /hr/* and are admin-only. See Hrportal.js.
const { registerHrPortalRoutes } = require("./Hrportal");
registerHrPortalRoutes(app, auth);

// --- Review Meeting (read-only view over the existing manager check-in data) ---
// All routes live under /review-meetings/* and are admin-only. See ReviewMeeting.js.
const { registerReviewMeetingRoutes } = require("./ReviewMeeting");
registerReviewMeetingRoutes(app, auth);

// --- Sales Training (read-only view + AI assistant over the existing daily
// WhatsApp sales-training quiz data) ---
// All routes live under /training/*. Admins see every counsellor's sessions;
// counsellors see only their own. See SalesTraining.js.
const { registerSalesTrainingRoutes } = require("./SalesTraining");
registerSalesTrainingRoutes(app, auth);

function getISTDateString(d = new Date()) {
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

// Start/end instants of the IST calendar day containing `d`. Plain
// `date.setHours(0,0,0,0)` uses the server process's local timezone, not
// IST — on a server running in UTC that silently shifts "today" by 5.5
// hours and misattributes leads/remarks logged near day boundaries.
function getISTDayBounds(d = new Date()) {
  const dateStr = getISTDateString(d);
  const dayStart = new Date(`${dateStr}T00:00:00.000+05:30`);
  const dayEnd = new Date(`${dateStr}T23:59:59.999+05:30`);
  return { dayStart, dayEnd };
}

// Notifies every counsellor (role: "user") on WhatsApp about a new Team Group Chat message,
// so the group chat is mirrored to WhatsApp within the 24h session window. Sender is skipped.
async function notifyGroupChatOnWhatsApp(sender, message) {
  try {
    const counsellors = await User.find({ role: "user" }).select("username userNumber").lean();
    const text = `${sender} : ${message}`;

    await Promise.allSettled(
      counsellors
        .filter((c) => c.username !== sender)
        .map((c) => {
          const mobile = cleanIndianMobileNumber(c.userNumber);
          if (!mobile) return Promise.resolve();
          return sendWhatsAppText(mobile, text);
        })
    );
  } catch (err) {
    console.error('❌ Group chat WhatsApp notify error:', err.message);
  }
}

const getNextAssignee = async () => {
  const collection = mongoose.connection.db.collection('configs');
  const excludedConfig = await collection.findOne({ key: 'auto_assign_excluded_staffs' });
  const excludedStaffs = excludedConfig ? excludedConfig.value : [];

  const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 });
  const staffs = allStaffs.filter(s => !excludedStaffs.includes(s.username));
  if (staffs.length === 0) return null;

  const staffUsernames = staffs.map(s => s.username);

  const lastAssignedLead = await Chat.findOne({ assignedto: { $in: staffUsernames } })
    .sort({ datecreated: -1 });

  if (!lastAssignedLead || !lastAssignedLead.assignedto) return staffs[0].username;

  const lastIndex = staffUsernames.indexOf(lastAssignedLead.assignedto);
  const nextIndex = (lastIndex + 1) % staffs.length;
  return staffs[nextIndex].username;
};

const getAutoAssignSetting = async () => {
  const collection = mongoose.connection.db.collection('configs');
  const config = await collection.findOne({ key: 'auto_assign_enabled' });
  return config ? config.value : true; // Default to true
};


app.get("/settings", auth, async (req, res) => {
  try {
    const collection = mongoose.connection.db.collection('configs');
    const config = await collection.findOne({ key: 'auto_assign_enabled' });
    const autoAssign = config ? !!config.value : true;
    res.json({ success: true, autoAssign });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

app.post("/settings", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const { autoAssign } = req.body;
    const collection = mongoose.connection.db.collection('configs');
    await collection.updateOne(
      { key: 'auto_assign_enabled' },
      { $set: { value: autoAssign } },
      { upsert: true }
    );
    res.json({ success: true, message: `Auto-Assign turned ${autoAssign ? 'ON' : 'OFF'}` });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

app.get("/settings/auto-assign-staffs", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const collection = mongoose.connection.db.collection('configs');
    const config = await collection.findOne({ key: 'auto_assign_excluded_staffs' });
    const excludedStaffs = config ? config.value : [];
    const staffs = await User.find({ role: 'user' }).sort({ username: 1 }).select('username');
    res.json({ success: true, staffs: staffs.map(s => s.username), excludedStaffs });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

app.post("/settings/auto-assign-staffs", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const { excludedStaffs } = req.body;
    if (!Array.isArray(excludedStaffs)) return res.status(400).json({ message: "excludedStaffs must be an array" });
    const collection = mongoose.connection.db.collection('configs');
    await collection.updateOne(
      { key: 'auto_assign_excluded_staffs' },
      { $set: { value: excludedStaffs } },
      { upsert: true }
    );
    res.json({ success: true, message: "Auto-assign staff preferences saved" });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});


app.post("/leads/distribute", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const unassignedLeads = await Chat.find({
      $or: [{ assignedto: { $exists: false } }, { assignedto: null }, { assignedto: "" }]
    }).sort({ datecreated: 1 });

    if (unassignedLeads.length === 0) return res.json({ success: true, message: "No unassigned leads found." });

    const collection = mongoose.connection.db.collection('configs');
    const excludedConfig = await collection.findOne({ key: 'auto_assign_excluded_staffs' });
    const excludedStaffs = excludedConfig ? excludedConfig.value : [];

    const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 });
    const staffs = allStaffs.filter(s => !excludedStaffs.includes(s.username));
    if (staffs.length === 0) return res.status(400).json({ message: "No staff members available for assignment." });

    const staffUsernames = staffs.map(s => s.username);
    let startIndex = 0;
    const lastAssignedLead = await Chat.findOne({ assignedto: { $in: staffUsernames } }).sort({ datecreated: -1 });
    if (lastAssignedLead) {
      const idx = staffUsernames.indexOf(lastAssignedLead.assignedto);
      startIndex = (idx + 1) % staffs.length;
    }

    for (let i = 0; i < unassignedLeads.length; i++) {
      const staff = staffs[(startIndex + i) % staffs.length];
      await Chat.findByIdAndUpdate(unassignedLeads[i]._id, { assignedto: staff.username });
    }

    res.json({ success: true, message: `Distributed ${unassignedLeads.length} leads across ${staffs.length} staff.` });
  } catch (err) {
    res.status(500).json({ message: "Distribution failed." });
  }
});

app.post("/login", async (req, res) => {
  try {
    const { username, password } = req.body;

    const user = await User.findOne({ username, password });

    if (!user) {
      return res.json({ success: false, message: "Invalid credentials" });
    }

    const accessToken = createAccessToken(user);

    return res.json({
      success: true,
      accessToken,   // send access token to frontend
      user,
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, message: "Server error" });
  }
});


app.post("/dashboard", auth, async (req, res) => {
    
    const { username, includeLeads = true } = req.body;

    // 1️⃣ Validate user
    const user = await User.findOne({ username });

  
  let leads = [];

    // 3️⃣ Fetch leads based on role
    if (user.role === "user") {
      const scope = { assignedto: user.username };

      if (!includeLeads) {
        const [notInterestedCount, remindersCount] =
          await Promise.all([
            Chat.countDocuments({ ...scope, pipeline: { $in: [/^not interested$/i, /^not intrested$/i] } }),
            Chat.countDocuments({ ...scope, reminder: { $ne: null } }),
          ]);

        return res.json({
          success: true,
          user,
          counts: { notInterestedCount, remindersCount },
        });
      }

      leads = await Chat.find(scope).sort({ datecreated: -1 });
      return res.json({ success: true, user, leads });
    } 
    else if (user.role === "admin") {
      const staffs = await User.find(
        { role: { $ne: "admin" } },
        { userNumber: 1, useremail: 1, username: 1, _id: 0 }
      );

      if (!includeLeads) {
        const [notInterestedCount, remindersCount] =
          await Promise.all([
            Chat.countDocuments({ pipeline: { $in: [/^not interested$/i, /^not intrested$/i] } }),
            Chat.countDocuments({ reminder: { $ne: null } }),
          ]);

        return res.json({
          success: true,
          user,
          staffs,
          counts: { notInterestedCount, remindersCount },
        });
      }

      leads = await Chat.find({}).sort({ datecreated: -1 });
      return res.json({ success: true, user, leads, staffs });
    }
    else if (user.role === "hr") {
      const scope = { pipeline: "Looking for job" };

      if (!includeLeads) {
        // Note: these first three counts intentionally ignore `scope` — its only
        // key is `pipeline`, which the count queries below override anyway, and
        // spreading it in previously masked that by silently discarding the
        // override (e.g. `{...scope, pipeline: "Enrolled"}` used to evaluate to
        // just `{pipeline: "Enrolled"}` regardless, since object spread applies
        // left-to-right and later keys win).
        const [notInterestedCount, remindersCount] =
          await Promise.all([
            Chat.countDocuments({ pipeline: { $in: [/^not interested$/i, /^not intrested$/i] } }),
            Chat.countDocuments({ ...scope, reminder: { $ne: null } }),
          ]);

        return res.json({
          success: true,
          user,
          counts: { notInterestedCount, remindersCount },
        });
      }

      leads = await Chat.find(scope).sort({ datecreated: -1 });
      return res.json({ success: true, user, leads });
    }
    else {
      return res.json({
        success: false,
        message: "Invalid role"
      });
    }

});

function escapeRegex(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// A lead is "Qualified" when it had more than 5 conversation exchanges on any
// single one of its first 3 days of chatting — day 1, or day 2/3 when they only
// engaged after a follow-up (follow-ups go out morning/evening for up to 3 days).
// Expressed as an aggregation expression so it can run both as a $match filter
// (via $expr) and as a $sum/$cond inside the stats aggregation.
const QUALIFIED_LEAD_EXPR = {
  $let: {
    vars: { times: { $map: { input: { $ifNull: ["$conversations", []] }, as: "c", in: "$$c.timestamp" } } },
    in: {
      $cond: [
        { $eq: [{ $size: "$$times" }, 0] },
        false,
        {
          $let: {
            vars: { minTime: { $min: "$$times" } },
            in: {
              $gt: [
                {
                  // Max exchange count across day 1 (offset 0), day 2 (offset 1), day 3 (offset 2).
                  $max: {
                    $map: {
                      input: [0, 1, 2],
                      as: "dayOffset",
                      in: {
                        $size: {
                          $filter: {
                            input: "$$times",
                            as: "t",
                            cond: {
                              $eq: [
                                { $dateDiff: { startDate: "$$minTime", endDate: "$$t", unit: "day" } },
                                "$$dayOffset",
                              ],
                            },
                          },
                        },
                      },
                    },
                  },
                },
                5,
              ],
            },
          },
        },
      ],
    },
  },
};

function buildLeadMatch({ view, searchQuery, filters, scope, ignoreStatusFilter = false }) {
  const match = { ...(scope || {}) };
  const and = [];

  const viewKey = (view || "dashboard").toLowerCase();
  const isHrScoped = !!(scope && scope.pipeline === HR_PIPELINE_STAGE);
  if (viewKey === "dashboard") {
    // "Looking for job" hands the lead off to HR, same as Enrolled/Not Interested,
    // and leaves the counsellor's working dashboard — but not when the query is
    // already scoped to the HR pipeline stage itself (the HR user's own queue),
    // otherwise the two filters contradict each other and match nothing.
    if (!isHrScoped) {
      and.push({ pipeline: { $nin: [...CONVERTED_STAGES, "Looking for job", /^not interested$/i, /^not intrested$/i] } });
    }
  } else if (viewKey === "transferred") {
    // Leads the auto-reassignment crons (no-remark / stale-pipeline) moved from
    // one counsellor to another — identified by a non-empty autoReassignHistory.
    // Once a transferred lead's pipeline is set to Not Interested, it belongs in
    // the Not Interested view instead — not here.
    and.push({ "autoReassignHistory.0": { $exists: true } });
    and.push({ pipeline: { $nin: [/^not interested$/i, /^not intrested$/i] } });
  } else if (viewKey === "enrolled" || viewKey === "converted" || viewKey === "fees") {
    and.push({ pipeline: { $in: CONVERTED_STAGES } });
  } else if (viewKey === "internship-enrolled") {
    and.push({ pipeline: { $in: CONVERTED_STAGES } });
    and.push({ course: { $regex: /^college internship$/i } });
  } else if (viewKey === "not-interested") {
    and.push({ pipeline: { $in: [/^not interested$/i, /^not intrested$/i] } });
  } else if (viewKey === "reminders") {
    and.push({ reminder: { $ne: null } });
  }

  const q = (searchQuery || "").trim();
  if (q) {
    const safe = escapeRegex(q);
    if (viewKey === "internship-enrolled") {
      match.$or = [
        { userName: { $regex: safe, $options: "i" } },
        { userNumber: { $regex: safe, $options: "i" } },
        { "InternDetails.batch_code": { $regex: safe, $options: "i" } },
      ];
    } else {
      match.$or = [
        { userName: { $regex: safe, $options: "i" } },
        { userNumber: { $regex: safe, $options: "i" } },
      ];
    }
  }

  const f = filters || {};

  if (Array.isArray(f.courses) && f.courses.length > 0) {
    and.push({ course: { $in: f.courses.map(c => new RegExp(`^${escapeRegex(c)}$`, "i")) } });
  }
  if (Array.isArray(f.programTypes) && f.programTypes.length > 0) {
    and.push({ programType: { $in: f.programTypes.map(p => new RegExp(`^${escapeRegex(p)}$`, "i")) } });
  }
  if (Array.isArray(f.professions) && f.professions.length > 0) {
    and.push({ profession: { $in: f.professions.map(p => new RegExp(`^${escapeRegex(p)}$`, "i")) } });
  }
  if (Array.isArray(f.sources) && f.sources.length > 0) {
    and.push({ leadfrom: { $in: f.sources.map(s => new RegExp(escapeRegex(s), "i")) } });
  }
  if (Array.isArray(f.statuses) && f.statuses.length > 0) {
    and.push({ status: { $in: f.statuses.map(s => new RegExp(`^${escapeRegex(s)}$`, "i")) } });
  }
  if (Array.isArray(f.pipelines) && f.pipelines.length > 0) {
    // "Converted" also matches legacy "Enrolled" leads.
    and.push({ pipeline: { $in: f.pipelines.map(p => CONVERTED_RE.test(p) ? CONVERTED_RE : new RegExp(`^${escapeRegex(p)}$`, "i")) } });
  }
  if (Array.isArray(f.assignedUsers) && f.assignedUsers.length > 0) {
    and.push({ assignedto: { $in: f.assignedUsers } });
  }
  if (Array.isArray(f.campaigns) && f.campaigns.length > 0) {
    and.push({
      campaignHistory: {
        $elemMatch: {
          campaignName: { $in: f.campaigns.map(c => new RegExp(`^${escapeRegex(c)}$`, "i")) },
          repliedAt: { $ne: null }
        }
      }
    });
  }

  if (f.fromDate || f.toDate) {
    const from = f.fromDate ? new Date(f.fromDate) : null;
    const to = f.toDate ? new Date(f.toDate) : null;
    const range = {};
    if (from && !isNaN(from.getTime())) range.$gte = from;
    if (to && !isNaN(to.getTime())) range.$lte = new Date(to.getTime() + 86400000 - 1);
    if (Object.keys(range).length > 0) and.push({ datecreated: range });
  }

  const statusFilter = ignoreStatusFilter ? null : f.statusFilter;
  if (statusFilter === "unassigned") {
    and.push({ $or: [{ assignedto: { $exists: false } }, { assignedto: null }, { assignedto: "" }] });
  } else if (statusFilter === "no-remarks") {
    and.push({ $or: [{ remarks: { $exists: false } }, { remarks: { $size: 0 } }] });
  } else if (statusFilter === "responded") {
    and.push({ respondedAfterFollowUp: true });
  } else if (statusFilter === "hot" || statusFilter === "warm" || statusFilter === "cold") {
    and.push({ status: new RegExp(`^${escapeRegex(statusFilter)}$`, "i") });
  } else if (statusFilter === "qualified") {
    and.push({ $expr: QUALIFIED_LEAD_EXPR });
  }

  // Internship filters (college / batch_code live inside InternDetails)
  if (f.college) and.push({ "InternDetails.college": new RegExp(`^${escapeRegex(f.college)}$`, "i") });
  if (f.batch) and.push({ "InternDetails.batch_code": new RegExp(`^${escapeRegex(f.batch)}$`, "i") });
  if (f.assignedUser) and.push({ assignedto: new RegExp(`^${escapeRegex(f.assignedUser)}$`, "i") });
  if (f.source) and.push({ leadfrom: new RegExp(`^${escapeRegex(f.source)}$`, "i") });

  if (and.length > 0) {
    match.$and = (match.$and || []).concat(and);
  }

  return match;
}

// Job-seeker handoff stage: leads set to this pipeline value move out of the
// counsellor's dashboard (see buildLeadMatch above) and into HR's queue.
const HR_PIPELINE_STAGE = "Looking for job";

// A "user" (counsellor) only ever sees their own assigned leads; an "hr" user
// only sees leads that have been handed off to the HR pipeline stage; every
// other role (admin, cert) is unrestricted. Mirrors the per-lead access check
// in isLeadAccessible below.
function scopeForRole(currentUser) {
  if (currentUser.role === "user") return { assignedto: currentUser.username };
  if (currentUser.role === "hr") return { pipeline: HR_PIPELINE_STAGE };
  return {};
}

function isLeadAccessible(currentUser, lead) {
  if (currentUser.role === "user") return lead.assignedto === currentUser.username;
  if (currentUser.role === "hr") return lead.pipeline === HR_PIPELINE_STAGE;
  return true;
}

app.post("/leads/query", auth, async (req, res) => {
  try {
    const {
      view = "dashboard",
      searchQuery = "",
      filters = {},
      page = 1,
      limit = 50,
      includeStats = true,
      mode = "summary",
    } = req.body || {};

    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const scope = scopeForRole(currentUser);

    const parsedLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const parsedPage = Math.max(1, Number(page) || 1);

    const matchFiltered = buildLeadMatch({ view, searchQuery, filters, scope, ignoreStatusFilter: false });
    const matchBaseForStats = buildLeadMatch({ view, searchQuery, filters, scope, ignoreStatusFilter: true });

    const projection = mode === "full" ? undefined : "-conversations -aiAnalysis -aiConversationSummary -InternDetails -certificates";

    const [leads, totalFiltered, statsAgg] = await Promise.all([
      Chat.find(matchFiltered)
        .sort({ datecreated: -1 })
        .skip((parsedPage - 1) * parsedLimit)
        .limit(parsedLimit)
        .select(projection)
        .lean(),
      Chat.countDocuments(matchFiltered),
      includeStats
        ? Chat.aggregate([
            { $match: matchBaseForStats },
            {
              $group: {
                _id: null,
                total: { $sum: 1 },
                cold: { $sum: { $cond: [{ $eq: [{ $toLower: "$status" }, "cold"] }, 1, 0] } },
                warm: { $sum: { $cond: [{ $eq: [{ $toLower: "$status" }, "warm"] }, 1, 0] } },
                hot: { $sum: { $cond: [{ $eq: [{ $toLower: "$status" }, "hot"] }, 1, 0] } },
                unassigned: {
                  $sum: { $cond: [{ $or: [{ $eq: ["$assignedto", null] }, { $eq: ["$assignedto", ""] }] }, 1, 0] },
                },
                noRemarks: {
                  $sum: { $cond: [{ $eq: [{ $size: { $ifNull: ["$remarks", []] } }, 0] }, 1, 0] },
                },
                responded: { $sum: { $cond: [{ $eq: ["$respondedAfterFollowUp", true] }, 1, 0] } },
                qualified: { $sum: { $cond: [QUALIFIED_LEAD_EXPR, 1, 0] } },
              },
            },
          ])
        : Promise.resolve([]),
    ]);

    const statsRow = includeStats && Array.isArray(statsAgg) && statsAgg[0] ? statsAgg[0] : null;

    return res.json({
      success: true,
      leads,
      total: totalFiltered,
      stats: statsRow
        ? {
            total: statsRow.total || 0,
            cold: statsRow.cold || 0,
            warm: statsRow.warm || 0,
            hot: statsRow.hot || 0,
            unassigned: statsRow.unassigned || 0,
            noRemarks: statsRow.noRemarks || 0,
            responded: statsRow.responded || 0,
            qualified: statsRow.qualified || 0,
          }
        : null,
    });
  } catch (err) {
    console.error("leads/query error:", err);
    return res.status(500).json({ success: false, message: "Failed to query leads" });
  }
});

app.get("/leads/campaigns", auth, async (req, res) => {
  try {
    const campaigns = await Chat.distinct("campaignHistory.campaignName");
    return res.json({ success: true, campaigns: campaigns.filter(Boolean).sort() });
  } catch (err) {
    console.error("leads/campaigns error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch campaigns" });
  }
});

// Status table for prospects staged from bulk-Excel campaigns (not yet leads).
// Once a contact replies they're promoted into Chat and disappear from here.
// Must stay registered before /leads/:id, or Express matches the :id wildcard first.
app.get("/leads/temp-campaigns", async (req, res) => {
  try {
    const tempLeads = await TempLead.find().sort({ sentAt: -1 });
    return res.status(200).json({ success: true, tempLeads });
  } catch (error) {
    console.error("Fetch temp campaigns error:", error);
    return res.status(500).json({ success: false, message: "Failed to fetch temp campaigns" });
  }
});

// ─── Co-working spaces & seat bookings ──────────────────────────────────────
// Space layouts (seats, meeting rooms, private offices, virtual/managed plans) and
// the bookings made against them. Shapes mirror the frontend's cowork/types.
const shapeCoworkSpace = (d) => ({
  id: d.spaceId,
  name: d.name,
  branchArea: d.branchArea,
  tagline: d.tagline,
  address: d.address,
  city: d.city,
  pincode: d.pincode,
  phone: d.phone,
  screenLabel: d.screenLabel,
  openHours: { open: d.openHours?.open || "09:30", close: d.openHours?.close || "18:30" },
  amenities: d.amenities || [],
  seats: (d.seats || []).map((s) => ({
    id: s.id, row: s.row, number: s.number, category: s.category, name: s.name,
    pricePerHour: s.pricePerHour, pricePerDay: s.pricePerDay, amenities: s.amenities || [],
    ...(s.meetingRoomId ? { meetingRoomId: s.meetingRoomId } : {}),
    ...(s.privateOfficeId ? { privateOfficeId: s.privateOfficeId } : {}),
    ...(s.isUnderMaintenance ? { isUnderMaintenance: true } : {}),
  })),
  meetingRooms: d.meetingRooms || [],
  privateOffices: d.privateOffices || [],
  virtualOfficePlans: d.virtualOfficePlans || [],
  managedOffices: d.managedOffices || [],
});

const coworkSpaceFields = (b) => ({
  name: String(b.name || "").trim(),
  branchArea: b.branchArea || "",
  tagline: b.tagline || "",
  address: b.address || "",
  city: b.city || "",
  pincode: b.pincode || "",
  phone: b.phone || "",
  screenLabel: b.screenLabel || "",
  openHours: b.openHours || { open: "09:30", close: "18:30" },
  amenities: Array.isArray(b.amenities) ? b.amenities : [],
  seats: Array.isArray(b.seats) ? b.seats : [],
  meetingRooms: Array.isArray(b.meetingRooms) ? b.meetingRooms : [],
  privateOffices: Array.isArray(b.privateOffices) ? b.privateOffices : [],
  virtualOfficePlans: Array.isArray(b.virtualOfficePlans) ? b.virtualOfficePlans : [],
  managedOffices: Array.isArray(b.managedOffices) ? b.managedOffices : [],
});

const shapeSeatBooking = (d) => ({
  id: d.bookingId,
  spaceId: d.spaceId,
  spaceName: d.spaceName,
  branchArea: d.branchArea,
  seatIds: d.seatIds || [],
  meetingRoomIds: d.meetingRoomIds || [],
  serviceType: d.serviceType,
  date: d.date,
  fromDate: d.fromDate,
  toDate: d.toDate,
  totalDays: d.totalDays,
  startTime: d.startTime,
  endTime: d.endTime,
  durationHours: d.durationHours,
  clientName: d.clientName,
  clientEmail: d.clientEmail,
  clientPhone: d.clientPhone,
  companyName: d.companyName,
  notes: d.notes || undefined,
  totalPrice: d.totalPrice,
  status: d.status,
  bookedAt: (d.createdAt || new Date()).toISOString(),
  paymentMethod: d.paymentMethod,
  qrCodeData: d.qrCodeData,
  leadId: d.leadId ? String(d.leadId) : null,
});

const COWORK_HHMM = /^([01]\d|2[0-3]):[0-5]\d$/;
const COWORK_YMD = /^\d{4}-\d{2}-\d{2}$/;

app.get("/cowork/spaces", auth, async (req, res) => {
  try {
    const spaces = await CoworkSpace.find({}).sort({ createdAt: 1 }).lean();
    return res.json({ success: true, spaces: spaces.map(shapeCoworkSpace) });
  } catch (err) {
    console.error("GET /cowork/spaces error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch spaces" });
  }
});

// Create or replace a space (the layout editor saves the whole space). Admin only.
app.put("/cowork/spaces/:spaceId", auth, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ success: false, message: "Only admins can manage spaces" });
    }
    const fields = coworkSpaceFields(req.body || {});
    if (!fields.name) return res.status(400).json({ success: false, message: "Space name is required" });
    const doc = await CoworkSpace.findOneAndUpdate(
      { spaceId: req.params.spaceId },
      { $set: fields, $setOnInsert: { spaceId: req.params.spaceId, createdBy: req.user?.username || null } },
      { upsert: true, new: true, runValidators: true }
    ).lean();
    return res.json({ success: true, space: shapeCoworkSpace(doc) });
  } catch (err) {
    console.error("PUT /cowork/spaces/:spaceId error:", err);
    return res.status(500).json({ success: false, message: "Failed to save space" });
  }
});

app.delete("/cowork/spaces/:spaceId", auth, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ success: false, message: "Only admins can manage spaces" });
    }
    const active = await SeatBooking.countDocuments({
      spaceId: req.params.spaceId,
      status: { $in: ["confirmed", "checked_in"] },
    });
    if (active > 0) {
      return res.status(409).json({ success: false, message: `Space has ${active} active booking(s); cancel them first` });
    }
    await CoworkSpace.deleteOne({ spaceId: req.params.spaceId });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /cowork/spaces/:spaceId error:", err);
    return res.status(500).json({ success: false, message: "Failed to delete space" });
  }
});

app.get("/cowork/bookings", auth, async (req, res) => {
  try {
    const query = {};
    if (req.query.spaceId) query.spaceId = String(req.query.spaceId);
    const bookings = await SeatBooking.find(query).sort({ createdAt: -1 }).lean();
    return res.json({ success: true, bookings: bookings.map(shapeSeatBooking) });
  } catch (err) {
    console.error("GET /cowork/bookings error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch bookings" });
  }
});

app.post("/cowork/bookings", auth, async (req, res) => {
  try {
    const b = req.body || {};
    const space = await CoworkSpace.findOne({ spaceId: b.spaceId }).lean();
    if (!space) return res.status(404).json({ success: false, message: "Space not found" });

    const fromDate = b.fromDate || b.date;
    const toDate = b.toDate || fromDate;
    if (!COWORK_YMD.test(fromDate || "") || !COWORK_YMD.test(toDate || "") || toDate < fromDate) {
      return res.status(400).json({ success: false, message: "Invalid date range" });
    }
    const startTime = b.startTime || "09:30";
    const endTime = b.endTime || "18:30";
    if (!COWORK_HHMM.test(startTime) || !COWORK_HHMM.test(endTime) || startTime >= endTime) {
      return res.status(400).json({ success: false, message: "Invalid time range" });
    }
    if (!String(b.clientName || "").trim()) {
      return res.status(400).json({ success: false, message: "Client name is required" });
    }

    const seatIds = Array.isArray(b.seatIds) ? [...new Set(b.seatIds.map(String))] : [];
    const known = new Map((space.seats || []).map((s) => [s.id, s]));
    const unknown = seatIds.find((id) => !known.has(id) || known.get(id).isUnderMaintenance);
    if (unknown) {
      return res.status(400).json({ success: false, message: `Seat ${unknown} is not available` });
    }

    // Server-side double-booking guard: same space, overlapping dates AND times, shared seat.
    if (seatIds.length > 0) {
      const clash = await SeatBooking.findOne({
        spaceId: space.spaceId,
        status: { $ne: "cancelled" },
        seatIds: { $in: seatIds },
        fromDate: { $lte: toDate },
        toDate: { $gte: fromDate },
        startTime: { $lt: endTime },
        endTime: { $gt: startTime },
      }).lean();
      if (clash) {
        const taken = clash.seatIds.filter((id) => seatIds.includes(id)).join(", ");
        return res.status(409).json({ success: false, message: `Already booked: ${taken} (${clash.bookingId})` });
      }
    }

    let bookingId = "";
    for (let i = 0; i < 5; i++) {
      bookingId = `CBE-${Math.floor(10000 + Math.random() * 90000)}`;
      if (!(await SeatBooking.exists({ bookingId }))) break;
    }

    // Link to an existing CRM lead when the client's phone matches one (last 10 digits).
    const phone = String(b.clientPhone || "").replace(/\D/g, "");
    const lead = phone.length >= 10
      ? await Chat.findOne({ userNumber: { $regex: `${phone.slice(-10)}$` } }).select("_id").lean()
      : null;

    const doc = await SeatBooking.create({
      bookingId,
      spaceId: space.spaceId,
      spaceName: space.name,
      branchArea: space.branchArea,
      seatIds,
      meetingRoomIds: Array.isArray(b.meetingRoomIds) ? b.meetingRoomIds.map(String) : [],
      serviceType: b.serviceType || "seats",
      date: fromDate,
      fromDate,
      toDate,
      totalDays: Number(b.totalDays) || 1,
      startTime,
      endTime,
      durationHours: Number(b.durationHours) || 0,
      clientName: String(b.clientName).trim(),
      clientEmail: b.clientEmail || "",
      clientPhone: b.clientPhone || "",
      companyName: b.companyName || "",
      notes: b.notes || "",
      totalPrice: Number(b.totalPrice) || 0,
      status: "confirmed",
      paymentMethod: b.paymentMethod || "pay_at_desk",
      qrCodeData: `${bookingId}|${b.clientName}|${b.companyName || ""}|${fromDate}..${toDate}|${space.branchArea}`,
      leadId: lead?._id || null,
      createdBy: req.user?.username || null,
    });
    return res.json({ success: true, booking: shapeSeatBooking(doc.toObject()) });
  } catch (err) {
    console.error("POST /cowork/bookings error:", err);
    return res.status(500).json({ success: false, message: "Failed to create booking" });
  }
});

app.put("/cowork/bookings/:bookingId/status", auth, async (req, res) => {
  try {
    const { status } = req.body || {};
    if (!["checked_in", "completed", "cancelled"].includes(status)) {
      return res.status(400).json({ success: false, message: "Invalid status" });
    }
    const doc = await SeatBooking.findOneAndUpdate(
      { bookingId: req.params.bookingId },
      { $set: { status } },
      { new: true }
    ).lean();
    if (!doc) return res.status(404).json({ success: false, message: "Booking not found" });
    return res.json({ success: true, booking: shapeSeatBooking(doc) });
  } catch (err) {
    console.error("PUT /cowork/bookings/:bookingId/status error:", err);
    return res.status(500).json({ success: false, message: "Failed to update booking" });
  }
});

// ─── Lead Groups ────────────────────────────────────────────────────────────
// User-defined named bundles of leads, shown as a badge in the dashboard's
// "Program & Source" column. Registered before /leads/:id so the ":id" wildcard
// never shadows these (the path prefix differs anyway: /lead-groups vs /leads).
const LEAD_GROUP_MEMBER_FIELDS =
  "userName userNumber course programType leadfrom status pipeline profession assignedto";

const shapeLeadGroup = (g) => ({
  _id: g._id,
  name: g.name,
  color: g.color || "violet",
  memberIds: (g.members || []).map((m) => String(m)),
  memberCount: (g.members || []).length,
  createdBy: g.createdBy || null,
});

// True when the requester may modify the group (its creator, or any admin).
const canManageLeadGroup = (group, reqUser) =>
  reqUser?.role === "admin" || (group.createdBy && group.createdBy === reqUser?.username);

app.get("/lead-groups", auth, async (req, res) => {
  try {
    const groups = await LeadGroup.find({}).sort({ updatedAt: -1 }).lean();
    return res.json({ success: true, groups: groups.map(shapeLeadGroup) });
  } catch (err) {
    console.error("GET /lead-groups error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch groups" });
  }
});

app.post("/lead-groups", auth, async (req, res) => {
  try {
    const { name, color, memberIds } = req.body || {};
    if (!name || !String(name).trim()) {
      return res.status(400).json({ success: false, message: "Group name is required" });
    }
    const ids = Array.isArray(memberIds)
      ? [...new Set(memberIds.map(String))].filter((id) => mongoose.Types.ObjectId.isValid(id))
      : [];
    const group = await LeadGroup.create({
      name: String(name).trim(),
      color: color || "violet",
      members: ids,
      createdBy: req.user?.username || null,
    });
    return res.json({ success: true, group: shapeLeadGroup(group.toObject()) });
  } catch (err) {
    console.error("POST /lead-groups error:", err);
    return res.status(500).json({ success: false, message: "Failed to create group" });
  }
});

// PUT (not PATCH) to stay within the app-wide CORS method allowlist.
app.put("/lead-groups/:id", auth, async (req, res) => {
  try {
    const group = await LeadGroup.findById(req.params.id);
    if (!group) return res.status(404).json({ success: false, message: "Group not found" });
    if (!canManageLeadGroup(group, req.user)) {
      return res.status(403).json({ success: false, message: "Only the group creator or an admin can edit this group" });
    }
    const { name, color } = req.body || {};
    if (typeof name === "string" && name.trim()) group.name = name.trim();
    if (typeof color === "string" && color.trim()) group.color = color.trim();
    group.updatedAt = new Date();
    await group.save();
    return res.json({ success: true, group: shapeLeadGroup(group.toObject()) });
  } catch (err) {
    console.error("PUT /lead-groups/:id error:", err);
    return res.status(500).json({ success: false, message: "Failed to update group" });
  }
});

app.delete("/lead-groups/:id", auth, async (req, res) => {
  try {
    const group = await LeadGroup.findById(req.params.id);
    if (!group) return res.status(404).json({ success: false, message: "Group not found" });
    if (!canManageLeadGroup(group, req.user)) {
      return res.status(403).json({ success: false, message: "Only the group creator or an admin can delete this group" });
    }
    await group.deleteOne();
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /lead-groups/:id error:", err);
    return res.status(500).json({ success: false, message: "Failed to delete group" });
  }
});

app.post("/lead-groups/:id/members", auth, async (req, res) => {
  try {
    const group = await LeadGroup.findById(req.params.id);
    if (!group) return res.status(404).json({ success: false, message: "Group not found" });
    if (!canManageLeadGroup(group, req.user)) {
      return res.status(403).json({ success: false, message: "Only the group creator or an admin can change members" });
    }
    const { add, remove } = req.body || {};
    const toAdd = Array.isArray(add) ? add.map(String).filter((id) => mongoose.Types.ObjectId.isValid(id)) : [];
    const toRemove = Array.isArray(remove) ? remove.map(String) : [];

    let members = (group.members || []).map(String);
    members = members.filter((id) => !toRemove.includes(id));
    for (const id of toAdd) if (!members.includes(id)) members.push(id);

    group.members = members;
    group.updatedAt = new Date();
    await group.save();
    return res.json({ success: true, group: shapeLeadGroup(group.toObject()) });
  } catch (err) {
    console.error("POST /lead-groups/:id/members error:", err);
    return res.status(500).json({ success: false, message: "Failed to update group members" });
  }
});

app.get("/lead-groups/:id/members", auth, async (req, res) => {
  try {
    const group = await LeadGroup.findById(req.params.id).lean();
    if (!group) return res.status(404).json({ success: false, message: "Group not found" });
    const members = await Chat.find({ _id: { $in: group.members || [] } })
      .select(LEAD_GROUP_MEMBER_FIELDS)
      .lean();
    return res.json({ success: true, members });
  } catch (err) {
    console.error("GET /lead-groups/:id/members error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch group members" });
  }
});

// Lead search for the "add to group" box. Mirrors /leads/query's role scoping:
// a "user" only finds their own assigned leads; an admin searches everyone.
app.get("/lead-groups/search/leads", auth, async (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    if (!q) return res.json({ success: true, leads: [] });

    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const scope = scopeForRole(currentUser);
    const safe = escapeRegex(q);
    const leads = await Chat.find({
      ...scope,
      $or: [
        { userName: { $regex: safe, $options: "i" } },
        { userNumber: { $regex: safe, $options: "i" } },
      ],
    })
      .sort({ datecreated: -1 })
      .limit(20)
      .select(LEAD_GROUP_MEMBER_FIELDS)
      .lean();
    return res.json({ success: true, leads });
  } catch (err) {
    console.error("GET /lead-groups/search/leads error:", err);
    return res.status(500).json({ success: false, message: "Failed to search leads" });
  }
});
// ────────────────────────────────────────────────────────────────────────────

app.get("/leads/:id", auth, async (req, res) => {
  try {
    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const lead = await Chat.findById(req.params.id).lean();
    if (!lead) return res.status(404).json({ success: false, message: "Lead not found" });

    if (!isLeadAccessible(currentUser, lead)) {
      return res.status(403).json({ success: false, message: "Forbidden" });
    }

    return res.json({ success: true, lead });
  } catch (err) {
    console.error("GET /leads/:id error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch lead" });
  }
});

app.post("/fees/stats", auth, async (req, res) => {
  try {
    const { searchQuery = "", filters = {} } = req.body || {};

    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const scope = scopeForRole(currentUser);

    const match = buildLeadMatch({
      view: "fees",
      searchQuery,
      filters: { ...filters, statusFilter: null },
      scope,
      ignoreStatusFilter: true,
    });

    const now = new Date();
    const start = new Date(now.getFullYear(), now.getMonth() - 5, 1, 0, 0, 0, 0);
    const end = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);

    const [raw, overallAgg] = await Promise.all([
      Chat.aggregate([
        { $match: match },
        { $match: { datecreated: { $gte: start, $lte: end } } },
        {
          $group: {
            _id: { year: { $year: "$datecreated" }, month: { $month: "$datecreated" } },
            billed: { $sum: { $ifNull: ["$fees.billedAmount", 0] } },
            paid: {
              $sum: {
                $reduce: {
                  input: { $ifNull: ["$fees.installments", []] },
                  initialValue: 0,
                  in: {
                    $cond: [
                      { $eq: ["$$this.status", "Paid"] },
                      { $add: ["$$value", { $ifNull: ["$$this.amount", 0] }] },
                      "$$value",
                    ],
                  },
                },
              },
            },
            installmentCount: {
              $sum: { $size: { $ifNull: ["$fees.installments", []] } },
            },
          },
        },
      ]),
      Chat.aggregate([
        { $match: match },
        {
          $project: {
            billed: { $ifNull: ["$fees.billedAmount", 0] },
            installments: { $ifNull: ["$fees.installments", []] },
          },
        },
        {
          $project: {
            billed: 1,
            installmentCount: { $size: "$installments" },
            paid: {
              $sum: {
                $map: {
                  input: "$installments",
                  as: "i",
                  in: { $cond: [{ $eq: ["$$i.status", "Paid"] }, { $ifNull: ["$$i.amount", 0] }, 0] },
                },
              },
            },
          },
        },
        {
          $group: {
            _id: null,
            billed: { $sum: "$billed" },
            paid: { $sum: "$paid" },
            installmentCount: { $sum: "$installmentCount" },
          },
        },
        {
          $project: {
            _id: 0,
            billed: 1,
            paid: 1,
            pending: { $subtract: ["$billed", "$paid"] },
            installmentCount: 1,
          },
        },
      ]),
    ]);

    const byKey = new Map();
    for (const row of raw) {
      const year = row._id.year;
      const monthIndex = row._id.month - 1;
      const key = `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
      const paid = Number(row.paid) || 0;
      const billed = Number(row.billed) || 0;
      byKey.set(key, {
        year,
        month: monthIndex,
        billed,
        paid,
        pending: billed - paid,
        installmentCount: Number(row.installmentCount) || 0,
      });
    }

    const months = [];
    for (let i = 0; i < 6; i++) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
      months.push(
        byKey.get(key) || {
          year: d.getFullYear(),
          month: d.getMonth(),
          billed: 0,
          paid: 0,
          pending: 0,
          installmentCount: 0,
        }
      );
    }

    const overall = Array.isArray(overallAgg) && overallAgg[0] ? overallAgg[0] : { billed: 0, paid: 0, pending: 0, installmentCount: 0 };
    return res.json({ success: true, months, overall });
  } catch (err) {
    console.error("fees/stats error:", err);
    return res.status(500).json({ success: false, message: "Failed to compute fees stats" });
  }
});



// Add remark — auto-updates Hot/Warm/Cold via AI analysis
app.post("/Users/remarks", async (req, res) => {
  const { _id, remark } = req.body;
  if (!_id || !remark) return res.status(400).json({ error: "_id and remark required" });
  try {
    const chat = await Chat.findById(_id);
    if (!chat) return res.status(404).json({ error: "User not found" });
    const remarkTimestamp = new Date();
    chat.remarks.push({ remark, timestamp: remarkTimestamp });
    // A reminder in the past that's now being followed up on (new remark logged after
    // it was due) is stale — clear it so it stops showing up as an outstanding reminder.
    // A reminder still in the future is left untouched since it hasn't come due yet.
    if (chat.reminder && chat.reminder < remarkTimestamp) {
      chat.reminder = null;
    }
    // Persist the remark before the AI call below — analyzeRemarkStatus is a network
    // round-trip to OpenAI and can take a few seconds. Saving here first closes the
    // window where the no-remark escalation cron (runs every 10 min) could still see
    // an empty `remarks` array and auto-reassign the lead out from under whoever just
    // logged it.
    await chat.save();

    const aiStatus = await analyzeRemarkStatus(chat);
    if (aiStatus) {
      chat.status = aiStatus;
      await chat.save();
    }

    res.json(chat);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});
// Delete remark
app.delete("/Users/remarks", async (req, res) => {
  const { _id, remarkId } = req.body;
  if (!_id || !remarkId) return res.status(400).json({ error: "_id and remarkId required" });
  try {
    const chat = await Chat.findById(_id);
    if (!chat) return res.status(404).json({ error: "User not found" });
    chat.remarks = chat.remarks.filter(r => r._id.toString() !== remarkId);
    await chat.save();
    res.json(chat);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

// One-time AI summary of a lead's WhatsApp bot conversation.
// Called by the CRM when the Remarks tab is opened. Idempotent: if a summary
// already exists it is returned untouched and OpenAI is never called, so a
// conversation is summarised exactly once and never re-summarised.
app.post("/leads/:id/summarize-conversation", auth, async (req, res) => {
  try {
    const leadId = req.params.id;

    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const lead = await Chat.findById(leadId);
    if (!lead) return res.status(404).json({ success: false, message: "Lead not found" });

    if (!isLeadAccessible(currentUser, lead)) {
      return res.status(403).json({ success: false, message: "Forbidden" });
    }

    // Already summarised once -> return as-is, no re-summarisation, no AI call.
    if (lead.aiConversationSummary && lead.aiConversationSummary.text) {
      return res.json({ success: true, lead, alreadySummarized: true });
    }

    const convs = (lead.conversations || []).filter(c => c && (c.userMsg || c.botReply));
    if (convs.length === 0) {
      return res.json({ success: true, lead, skipped: true, message: "No conversation to summarize" });
    }

    const transcript = convs
      .map(c => `Lead: ${c.userMsg || ""}\nBot: ${c.botReply || ""}`)
      .join("\n---\n")
      .slice(0, 12000);

    let summary = "";
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: "You are a CRM assistant for BetaSpace, a co-working space provider. Summarize a lead's WhatsApp bot conversation for a sales executive. Be concise and factual, plain text only."
          },
          {
            role: "user",
            content: `Lead: ${lead.userName || "Unknown"}
Course interest: ${lead.courseofintrest || lead.course || "Not specified"}

WhatsApp conversation transcript:
${transcript}

Write a short summary (1-2 lines, plain text, no markdown) covering:
- The lead's main intent / what they asked about
- Key details they shared (background, timing, budget, location, etc.)
- Interest level and any objections or concerns
- One suggested next step for the counsellor`
          }
        ],
        max_tokens: 350,
        temperature: 0.3
      });
      summary = (completion.choices[0]?.message?.content || "").trim();
    } catch (aiErr) {
      console.error("summarize-conversation AI error:", aiErr.message);
      return res.status(502).json({ success: false, message: "AI summarization failed" });
    }

    if (!summary) {
      return res.status(502).json({ success: false, message: "Empty AI summary" });
    }

    lead.aiConversationSummary = {
      text: summary,
      generatedAt: new Date(),
      basedOnMessages: convs.length,
    };
    await lead.save();

    return res.json({ success: true, lead });
  } catch (err) {
    console.error("POST /leads/:id/summarize-conversation error:", err);
    return res.status(500).json({ success: false, message: "Failed to summarize conversation" });
  }
});

// app.get("/reports/today-user-activity", async (req, res) => {
//   try {
//     // ✅ Start & End of Today
//     const start = new Date();
//     start.setHours(0, 0, 0, 0);

//     const end = new Date();
//     end.setHours(23, 59, 59, 999);

//     const report = await Chat.aggregate([
//       {
//         $match: {
//           assignedto: { $exists: true, $ne: null }
//         }
//       },

//       // ✅ Filter today's remarks safely
//       {
//         $addFields: {
//           todaysRemarks: {
//             $filter: {
//               input: { $ifNull: ["$remarks", []] },
//               as: "remark",
//               cond: {
//                 $and: [
//                   { $gte: ["$$remark.timestamp", start] },
//                   { $lte: ["$$remark.timestamp", end] }
//                 ]
//               }
//             }
//           }
//         }
//       },

//       // ✅ Determine if fees updated today (safe check)
//       {
//         $addFields: {
//           feesUpdatedFlag: {
//             $cond: [
//               {
//                 $and: [
//                   { $ifNull: ["$fees.updatedAt", false] },
//                   { $gte: ["$fees.updatedAt", start] },
//                   { $lte: ["$fees.updatedAt", end] }
//                 ]
//               },
//               1,
//               0
//             ]
//           }
//         }
//       },

//       {
//         $group: {
//           _id: "$assignedto",

//           // ✅ New Leads Today
//           newLeadsToday: {
//             $sum: {
//               $cond: [
//                 {
//                   $and: [
//                     { $gte: ["$datecreated", start] },
//                     { $lte: ["$datecreated", end] }
//                   ]
//                 },
//                 1,
//                 0
//               ]
//             }
//           },

//           // ✅ Total Remarks Count Today
//           remarksCount: {
//             $sum: { $size: "$todaysRemarks" }
//           },

//           // ✅ Only push leads having remarks today
//           remarksDetails: {
//             $push: {
//               $cond: [
//                 { $gt: [{ $size: "$todaysRemarks" }, 0] },
//                 {
//                   userName: "$userName",
//                   userNumber: "$userNumber",
//                   remarks: "$todaysRemarks"
//                 },
//                 "$$REMOVE"
//               ]
//             }
//           },

//           // ✅ Fees updated count
//           feesUpdatedToday: {
//             $sum: "$feesUpdatedFlag"
//           },

//           // ✅ Only push fee details if updated today
//           feeDetails: {
//             $push: {
//               $cond: [
//                 { $eq: ["$feesUpdatedFlag", 1] },
//                 {
//                   userName: "$userName",
//                   userNumber: "$userNumber",
//                   billedAmount: "$fees.billedAmount",
//                   updatedAt: "$fees.updatedAt"
//                 },
//                 "$$REMOVE"
//               ]
//             }
//           }
//         }
//       },

//       {
//         $project: {
//           _id: 0,
//           username: "$_id",
//           newLeadsToday: 1,
//           remarksCount: 1,
//           feesUpdatedToday: 1,
//           remarksDetails: 1,
//           feeDetails: 1
//         }
//       },

//       { $sort: { username: 1 } }
//     ]);

//     res.json({
//       success: true,
//       date: start.toISOString().split("T")[0],
//       report
//     });

//   } catch (err) {
//     console.error("Report Error:", err);
//     res.status(500).json({
//       success: false,
//       message: "Error generating report"
//     });
//   }
// });



app.get("/reports/user-activity", auth, async (req, res) => {
  try {
    const { date } = req.query;
    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    let start, end;
    if (date && /^\d{4}-\d{2}-\d{2}$/.test(date)) {
      start = new Date(date);
      start.setHours(0, 0, 0, 0);
      end = new Date(date);
      end.setHours(23, 59, 59, 999);
    } else {
      start = new Date();
      start.setHours(0, 0, 0, 0);
      end = new Date();
      end.setHours(23, 59, 59, 999);
    }

    const dailyMatchStage = { assignedto: { $exists: true, $ne: null } };
    if (currentUser.role !== 'admin') {
      dailyMatchStage.assignedto = currentUser.username;
    }

    const report = await Chat.aggregate([
      { $match: dailyMatchStage },
      {
        $addFields: {
          isNewLeadOnDate: {
            $and: [
              { $gte: ["$datecreated", start] },
              { $lte: ["$datecreated", end] },
              {
                $not: {
                  $and: [
                    { $eq: [{ $toLower: { $ifNull: ["$pipeline", ""] } }, "enrolled"] },
                    { $eq: [{ $toLower: { $ifNull: ["$course", ""] } }, "college internship"] }
                  ]
                }
              }
            ]
          }
        }
      },
      {
        $addFields: {
          dateRemarks: {
            $filter: {
              input: { $ifNull: ["$remarks", []] },
              as: "remark",
              cond: {
                $and: [
                  { $gte: ["$$remark.timestamp", start] },
                  { $lte: ["$$remark.timestamp", end] }
                ]
              }
            }
          }
        }
      },
      {
        $addFields: {
          paidOnDateInstallments: {
            $filter: {
              input: { $ifNull: ["$fees.installments", []] },
              as: "inst",
              cond: {
                $and: [
                  { $eq: ["$$inst.status", "Paid"] },
                  { $gte: ["$$inst.dueDate", start] },
                  { $lte: ["$$inst.dueDate", end] }
                ]
              }
            }
          },
          firstInstDueDate: {
            $let: {
              vars: { first: { $arrayElemAt: ["$fees.installments", 0] } },
              in: { $ifNull: ["$$first.dueDate", null] }
            }
          },
          reminderOnDateFlag: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$reminder", false] },
                  { $gte: ["$reminder", start] },
                  { $lte: ["$reminder", end] }
                ]
              },
              1, 0
            ]
          }
        }
      },
      {
        $addFields: {
          paidOnDateAmount: {
            $sum: {
              $map: {
                input: "$paidOnDateInstallments",
                as: "inst",
                in: { $ifNull: ["$$inst.amount", 0] }
              }
            }
          },
          hasPaidOnDate: { $gt: [{ $size: "$paidOnDateInstallments" }, 0] },
          billedOnDate: {
            $and: [
              { $ifNull: ["$firstInstDueDate", false] },
              { $gte: ["$firstInstDueDate", start] },
              { $lte: ["$firstInstDueDate", end] }
            ]
          },
          billedOnDateAmount: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$firstInstDueDate", false] },
                  { $gte: ["$firstInstDueDate", start] },
                  { $lte: ["$firstInstDueDate", end] }
                ]
              },
              { $ifNull: ["$fees.billedAmount", 0] },
              0
            ]
          }
        }
      },
      {
        $group: {
          _id: "$assignedto",
          newLeadsToday: { $sum: { $cond: ["$isNewLeadOnDate", 1, 0] } },
          remarksCount: { $sum: { $size: "$dateRemarks" } },
          feesUpdatedToday: { $sum: "$paidOnDateAmount" },
          billedTodayTotal: { $sum: "$billedOnDateAmount" },
          remindersTodayCount: { $sum: "$reminderOnDateFlag" },
          newLeadsDetails: {
            $push: {
              $cond: [
                "$isNewLeadOnDate",
                { userName: "$userName", userNumber: "$userNumber", courseofintrest: "$courseofintrest", datecreated: "$datecreated" },
                "$$REMOVE"
              ]
            }
          },
          newLeadsRemarksToday: {
            $push: {
              $cond: [
                { $and: ["$isNewLeadOnDate", { $gt: [{ $size: "$dateRemarks" }, 0] }] },
                { userName: "$userName", userNumber: "$userNumber", datecreated: "$datecreated", remarks: "$dateRemarks" },
                "$$REMOVE"
              ]
            }
          },
          oldLeadsRemarksToday: {
            $push: {
              $cond: [
                { $and: [{ $not: "$isNewLeadOnDate" }, { $gt: [{ $size: "$dateRemarks" }, 0] }] },
                { userName: "$userName", userNumber: "$userNumber", remarks: "$dateRemarks" },
                "$$REMOVE"
              ]
            }
          },
          newLeadsWithoutRemarks: {
            $push: {
              $cond: [
                { $and: ["$isNewLeadOnDate", { $eq: [{ $size: "$dateRemarks" }, 0] }] },
                { userName: "$userName", userNumber: "$userNumber", datecreated: "$datecreated" },
                "$$REMOVE"
              ]
            }
          },
          remarksDetails: {
            $push: {
              $cond: [
                { $gt: [{ $size: "$dateRemarks" }, 0] },
                { userName: "$userName", userNumber: "$userNumber", remarks: "$dateRemarks" },
                "$$REMOVE"
              ]
            }
          },
          feeDetails: {
            $push: {
              $cond: [
                { $or: ["$hasPaidOnDate", "$billedOnDate"] },
                {
                  userName: "$userName",
                  userNumber: "$userNumber",
                  billedAmount: "$billedOnDateAmount",
                  paidAmount: "$paidOnDateAmount",
                  installments: "$paidOnDateInstallments"
                },
                "$$REMOVE"
              ]
            }
          },
          remindersTodayDetails: {
            $push: {
              $cond: [
                { $eq: ["$reminderOnDateFlag", 1] },
                { userName: "$userName", userNumber: "$userNumber", status: "$status", pipeline: "$pipeline", reminder: "$reminder" },
                "$$REMOVE"
              ]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          username: "$_id",
          newLeadsToday: 1,
          newLeadsDetails: 1,
          newLeadsRemarksToday: 1,
          oldLeadsRemarksToday: 1,
          newLeadsWithoutRemarks: 1,
          remarksCount: 1,
          feesUpdatedToday: 1,
          billedTodayTotal: 1,
          remarksDetails: 1,
          feeDetails: 1,
          remindersTodayCount: 1,
          remindersTodayDetails: 1
        }
      },
      { $sort: { username: 1 } }
    ]);

    res.json({ success: true, date: start.toISOString().split("T")[0], report });
  } catch (err) {
    console.error("Report Error:", err);
    res.status(500).json({ success: false, message: "Error generating report" });
  }
});

// Monthly counselor performance report
// app.get("/reports/monthly-activity", async (req, res) => {
//   try {
//     const year = parseInt(req.query.year) || new Date().getFullYear();
//     const month = parseInt(req.query.month) || (new Date().getMonth() + 1);

//     const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
//     const end = new Date(year, month, 0, 23, 59, 59, 999);

//     const report = await Chat.aggregate([
//       { $match: { assignedto: { $exists: true, $ne: null } } },
//       {
//         $addFields: {
//           isNewLeadInMonth: {
//             $and: [
//               { $gte: ["$datecreated", start] },
//               { $lte: ["$datecreated", end] }
//             ]
//           },
//           monthRemarks: {
//             $filter: {
//               input: { $ifNull: ["$remarks", []] },
//               as: "remark",
//               cond: {
//                 $and: [
//                   { $gte: ["$$remark.timestamp", start] },
//                   { $lte: ["$$remark.timestamp", end] }
//                 ]
//               }
//             }
//           },
//           feesUpdatedInMonth: {
//             $cond: [
//               {
//                 $and: [
//                   { $ifNull: ["$fees.updatedAt", false] },
//                   { $gte: ["$fees.updatedAt", start] },
//                   { $lte: ["$fees.updatedAt", end] }
//                 ]
//               },
//               1, 0
//             ]
//           }
//         }
//       },
//       {
//         $group: {
//           _id: "$assignedto",
//           newLeads: { $sum: { $cond: ["$isNewLeadInMonth", 1, 0] } },
//           totalRemarks: { $sum: { $size: "$monthRemarks" } },
//           feesUpdated: { $sum: "$feesUpdatedInMonth" },
//           totalBilled: {
//             $sum: {
//               $cond: [
//                 {
//                   $and: [
//                     { $ifNull: ["$fees.billedAt", false] },
//                     { $gte: ["$fees.billedAt", start] },
//                     { $lte: ["$fees.billedAt", end] }
//                   ]
//                 },
//                 { $ifNull: ["$fees.billedAmount", 0] },
//                 0
//               ]
//             }
//           }
//         }
//       },
//       {
//         $project: {
//           _id: 0,
//           username: "$_id",
//           newLeads: 1,
//           totalRemarks: 1,
//           feesUpdated: 1,
//           totalBilled: 1
//         }
//       },
//       { $sort: { username: 1 } }
//     ]);

//     res.json({ success: true, year, month, report });
//   } catch (err) {
//     console.error("Monthly Report Error:", err);
//     res.status(500).json({ success: false, message: "Error generating monthly report" });
//   }
// });
// Monthly counselor performance report
app.put("/Users/edit", auth, async (req, res) => {
  // console.log(req.body)
  try {
    const {
      _id,
      course,
      leadfrom,
      pipeline,
      profession,
      programType,
      status,
      userName,
      location,
      userNumber
    } = req.body;

    if (!_id) {
      return res.status(400).json({ message: "User ID missing!" });
    }

    // 1️⃣ Get existing user
    const existingUser = await Chat.findById(_id);

    if (!existingUser) {
      return res.status(404).json({ message: "User not found!" });
    }

    // Non-admins cannot change the program type once it has already been set.
    if (
      programType !== undefined &&
      existingUser.programType &&
      programType !== existingUser.programType &&
      req.user?.role !== 'admin'
    ) {
      return res.status(403).json({ message: "Only admins can change the program type once it has been set." });
    }

    const oldPipeline = existingUser.pipeline;

    // console.log(oldPipeline)

    // 2️⃣ Prepare update object (avoid overwriting with undefined)
    const updateData = {
      course,
      leadfrom,
      pipeline,
      profession,
      programType,
      status,
      userName,
      location,
      userNumber
    };

    // remove undefined fields
    Object.keys(updateData).forEach(
      (key) => updateData[key] === undefined && delete updateData[key]
    );

    // 3️⃣ Update user
    const updatedUser = await Chat.findByIdAndUpdate(
      _id,
      updateData,
      { new: true }
    );

    console.log(pipeline)

    // 4️⃣ Trigger microservice ONLY when pipeline changed to enrolled
    if (
      pipeline &&
      pipeline === "Enrolled" &&
      oldPipeline !== "Enrolled"
    ) {
      try {
        console.log("Enrollment triggered");

        // Generate enrollment certificate
        let certPayload = null;
        try {
          const certResult = await generateCourseCertificate({
            userName: updatedUser.userName,
            course: updatedUser.course,
            programType: updatedUser.programType,
            userId: updatedUser._id,
          });
          certPayload = {
            fileName: certResult.fileName,
            url: certResult.s3Url || null,
            base64: certResult.s3Url ? null : certResult.buffer.toString('base64'),
          };
          console.log("Enrollment certificate generated:", certResult.fileName);

          // Persist it alongside course completion certificates so it's viewable in the Cert tab.
          // Only the "enrollment" entry is replaced — completion/project/internship certs are preserved.
          try {
            const preserved = (Array.isArray(updatedUser.courseCertificates) ? updatedUser.courseCertificates : [])
              .filter(c => c && c.type !== 'enrollment');
            const enrollmentCertEntry = {
              success: true,
              type: 'enrollment',
              fileName: certResult.fileName,
              message: 'Generated successfully',
              s3: certResult.s3Url ? { uploaded: true, url: certResult.s3Url } : null
            };
            const nextCourseCertificates = [...preserved, enrollmentCertEntry];
            await Chat.findByIdAndUpdate(updatedUser._id, { $set: { courseCertificates: nextCourseCertificates } });
            updatedUser.courseCertificates = nextCourseCertificates;
          } catch (persistErr) {
            console.warn("Warning: could not save enrollment certificate to DB:", persistErr && persistErr.message ? persistErr.message : persistErr);
          }
        } catch (certErr) {
          console.error("Enrollment certificate generation failed:", certErr.message);
        }

        await fetch(`${process.env.WP_CHAT_SERVICE_URL}/new/enrollment`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            userId: updatedUser._id,
            userName: updatedUser.userName,
            userNumber: updatedUser.userNumber,
            course: updatedUser.course,
            programType: updatedUser.programType,
            certificate: certPayload,
          }),
        });

        console.log("Enrollment data sent to microservice");
      } catch (microErr) {
        console.error("Microservice call failed:", microErr.message);
      }
    }

    // 5️⃣ Trigger Meta Conversions API for Leads when pipeline changed to Pre-Enrollment or Enrolled
    const metaCapiStages = META_CAPI_STAGES;
    if (pipeline && metaCapiStages[pipeline] && oldPipeline !== pipeline) {
      const capiEventName = metaCapiStages[pipeline];
      try {
        const result = await sendLeadCapiEvent(updatedUser, capiEventName);
        if (!result.skipped) {
          const capiEvent = { eventName: capiEventName, sentAt: new Date(), success: result.success, message: result.message };
          await Chat.findByIdAndUpdate(updatedUser._id, { $push: { metaCapiEvents: capiEvent } });
          updatedUser.metaCapiEvents = [...(updatedUser.metaCapiEvents || []), capiEvent];
        }
        console.log(`Meta CAPI ${capiEventName} event:`, result.message);
      } catch (capiErr) {
        console.error("Meta CAPI trigger failed:", capiErr.message);
      }
    }

    res.json(updatedUser);

  } catch (err) {
    console.error("Update error:", err);
    res.status(500).json({ message: "Server error", error: err.message });
  }
});

app.post("/add", async (req, res) => {
  try {
    const { userName, userNumber, course, location, leadfrom, profession, status, programType, pipeline, assignedto } = req.body;

    if (!userNumber) return res.status(400).json({ message: "Phone number is required." });

    const sanitizedNumber = userNumber.replace(/\D/g, "");
    if (sanitizedNumber.length !== 10) return res.status(400).json({ message: "Phone number must be exactly 10 digits." });

    const fullNumber = sanitizedNumber.startsWith("91") ? sanitizedNumber : "91" + sanitizedNumber;

    // LeadAddNotification(fullNumber)
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) return res.status(400).json({ message: "User already exists." });

    // Assignment Logic: 
    // 1. If assignedto is provided (manual entry), use it.
    // 2. Otherwise, check if Auto-Assign is enabled.
    // 3. If enabled, use Round Robin. If disabled, leave as unassigned.

    userChat = new Chat({
      userNumber: fullNumber,
      userName: userName?.trim() || "Unknown",
      conversations: [],
      course: course || null,
      location: location || null,
      profession: profession || null,
      programType: programType || null,
      status: status || "Cold",
      pipeline: pipeline || "New Lead",
      lastInteracted: new Date(),
      leadfrom: leadfrom || "Walkin",
       assignedto: assignedto || undefined,
      datecreated: new Date()
    });

    await userChat.save();
    await LeadAddNotification(fullNumber)
    return res.status(200).json({ success: true, message: "Lead added.", user: userChat });
  } catch (err) {
    console.error("❌ Error adding user:", err);
    return res.status(500).json({ message: "Internal Server Error." });
  }
});

// Deterministic referral-group code: first 4 letters of the referrer's name (A-Z only,
// upper-cased, padded with X) + last 5 digits of their number. Same referrer -> same code
// every time, so repeat referrals (even months apart) stay grouped under one code.
function buildReferralCode(name, number) {
  const namePart = (name || "").replace(/[^A-Za-z]/g, "").slice(0, 4).toUpperCase().padEnd(4, "X");
  const digits = (number || "").replace(/\D/g, "");
  const numberPart = digits.slice(-5).padStart(5, "0");
  return `REF-${namePart}-${numberPart}`;
}

app.post("/external/referral", async (req, res) => {
  try {
    const { referralName, referralNumber, course, existingUserNumber } = req.body;

    if (!referralName || !referralName.trim() || !referralNumber) {
      return res.status(400).json({ message: "Referral name and number are required." });
    }
    if (!existingUserNumber) {
      return res.status(400).json({ message: "Existing student's mobile number is required." });
    }

    const referredNumber = cleanIndianMobileNumber(referralNumber);
    const referrerNumber = cleanIndianMobileNumber(existingUserNumber);

    if (!referredNumber || referredNumber.length !== 12) {
      return res.status(400).json({ message: "Referral number must be a valid 10-digit mobile number." });
    }
    if (!referrerNumber || referrerNumber.length !== 12) {
      return res.status(400).json({ message: "Existing student's number must be a valid 10-digit mobile number." });
    }

    const referrer = await Chat.findOne({ userNumber: referrerNumber });
    if (!referrer) {
      return res.status(404).json({ message: "We couldn't find an existing student with that mobile number." });
    }

    const existingReferred = await Chat.findOne({ userNumber: referredNumber });
    if (existingReferred) {
      return res.status(400).json({ message: "This number is already registered with us." });
    }

    const referralCode = buildReferralCode(referrer.userName, referrer.userNumber);

    const referredChat = new Chat({
      userNumber: referredNumber,
      userName: referralName.trim(),
      conversations: [],
      course: course || null,
      status: "Cold",
      pipeline: "New Lead",
      leadfrom: "Referral",
      referredBy: referrer.userName || "Unknown",
      referralCode,
      assignedto: referrer.assignedto || undefined,
      lastInteracted: new Date(),
      datecreated: new Date(),
      remarks: [{
        remark: `Referred by ${referrer.userName || "Unknown"} (${referrer.userNumber})`,
        timestamp: new Date()
      }]
    });

    await referredChat.save();
    await Chat.updateOne(
      { _id: referrer._id },
      { $set: { referralCode, is_referral_leader: true } }
    );
    await LeadAddNotification(referredNumber);

    return res.status(200).json({ success: true, message: "Referral submitted successfully.", user: referredChat });
  } catch (err) {
    console.error("❌ Error adding referral:", err);
    return res.status(500).json({ message: "Internal Server Error." });
  }
});

// Bulk referral: an existing student refers a group of students in one request.
// Body: { userName, existingUserNumber, course?, referrals: [{ name, number, course? }] }
app.post("/external/referral/bulk", async (req, res) => {
  try {
    const { userName, existingUserNumber, course, referrals } = req.body;

    if (!existingUserNumber) {
      return res.status(400).json({ message: "Existing student's mobile number is required." });
    }
    if (!Array.isArray(referrals) || referrals.length === 0) {
      return res.status(400).json({ message: "Please provide at least one referral." });
    }

    const referrerNumber = cleanIndianMobileNumber(existingUserNumber);
    if (!referrerNumber || referrerNumber.length !== 12) {
      return res.status(400).json({ message: "Existing student's number must be a valid 10-digit mobile number." });
    }

    const referrer = await Chat.findOne({ userNumber: referrerNumber });
    if (!referrer) {
      return res.status(404).json({ message: "We couldn't find an existing student with that mobile number." });
    }

    const referrerName = referrer.userName || (userName && userName.trim()) || "Unknown";
    const referralCode = buildReferralCode(referrer.userName, referrer.userNumber);

    const created = [];
    const skipped = [];
    const seenInBatch = new Set();

    for (let i = 0; i < referrals.length; i++) {
      const entry = referrals[i] || {};
      const name = (entry.name || "").trim();
      const rawNumber = entry.number;

      if (!name || !rawNumber) {
        skipped.push({ index: i, name: name || null, number: rawNumber || null, reason: "Name and number are required." });
        continue;
      }

      const referredNumber = cleanIndianMobileNumber(rawNumber);
      if (!referredNumber || referredNumber.length !== 12) {
        skipped.push({ index: i, name, number: rawNumber, reason: "Number must be a valid 10-digit mobile number." });
        continue;
      }

      if (referredNumber === referrerNumber) {
        skipped.push({ index: i, name, number: rawNumber, reason: "A student cannot refer themselves." });
        continue;
      }

      if (seenInBatch.has(referredNumber)) {
        skipped.push({ index: i, name, number: rawNumber, reason: "Duplicate number in this request." });
        continue;
      }
      seenInBatch.add(referredNumber);

      const existingReferred = await Chat.findOne({ userNumber: referredNumber });
      if (existingReferred) {
        skipped.push({ index: i, name, number: rawNumber, reason: "This number is already registered with us." });
        continue;
      }

      const referredChat = new Chat({
        userNumber: referredNumber,
        userName: name,
        conversations: [],
        course: entry.course || course || null,
        status: "Cold",
        pipeline: "New Lead",
        leadfrom: "Referral",
        referredBy: referrerName,
        referralCode,
        assignedto: referrer.assignedto || undefined,
        lastInteracted: new Date(),
        datecreated: new Date(),
        remarks: [{
          remark: `Referred by ${referrerName} (${referrer.userNumber})`,
          timestamp: new Date()
        }]
      });

      await referredChat.save();
      try {
        await LeadAddNotification(referredNumber);
      } catch (notifyErr) {
        console.error("❌ Referral notification failed:", notifyErr.message);
      }

      created.push({ index: i, name, number: referredNumber, id: referredChat._id });
    }

    if (created.length > 0) {
      await Chat.updateOne(
        { _id: referrer._id },
        { $set: { referralCode, is_referral_leader: true } }
      );
    }

    return res.status(200).json({
      success: true,
      message: `${created.length} referral(s) submitted successfully${skipped.length ? `, ${skipped.length} skipped` : ""}.`,
      createdCount: created.length,
      skippedCount: skipped.length,
      created,
      skipped
    });
  } catch (err) {
    console.error("❌ Error adding bulk referrals:", err);
    return res.status(500).json({ message: "Internal Server Error." });
  }
});

// ─── External Referral Groups ──────────────────────────────────────────────
// Groups leads created via /external/referral(/bulk) by the deterministic
// referralCode assigned to their referrer. Mirrors the /lead-groups pattern:
// one endpoint lists all groups (for the dashboard chip row), the other returns
// the full member list — leader + referred leads — for the popup.
const REFERRAL_MEMBER_FIELDS =
  "userName userNumber course leadfrom status pipeline assignedto is_referral_leader datecreated";

app.get("/external/referral/groups", auth, async (req, res) => {
  try {
    const leaders = await Chat.find({ is_referral_leader: true, referralCode: { $ne: "" } })
      .select("userName userNumber referralCode")
      .lean();
    if (leaders.length === 0) return res.json({ success: true, groups: [] });

    const codes = leaders.map((l) => l.referralCode);
    const counts = await Chat.aggregate([
      { $match: { referralCode: { $in: codes }, leadfrom: "Referral" } },
      { $group: { _id: "$referralCode", count: { $sum: 1 } } },
    ]);
    const countByCode = new Map(counts.map((c) => [c._id, c.count]));

    const groups = leaders
      .map((l) => ({
        code: l.referralCode,
        leaderId: String(l._id),
        leaderName: l.userName || "Unknown",
        leaderNumber: l.userNumber,
        memberCount: countByCode.get(l.referralCode) || 0,
      }))
      .filter((g) => g.memberCount > 0)
      .sort((a, b) => b.memberCount - a.memberCount);

    return res.json({ success: true, groups });
  } catch (err) {
    console.error("GET /external/referral/groups error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch referral groups" });
  }
});

app.get("/external/referral/groups/:code", auth, async (req, res) => {
  try {
    const code = String(req.params.code || "").trim();
    if (!code) return res.status(400).json({ success: false, message: "Referral code is required." });

    const members = await Chat.find({ referralCode: code })
      .select(REFERRAL_MEMBER_FIELDS)
      .sort({ is_referral_leader: -1, datecreated: 1 })
      .lean();

    return res.json({ success: true, code, members });
  } catch (err) {
    console.error("GET /external/referral/groups/:code error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch referral group" });
  }
});

app.post("/leads/distribute", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const unassignedLeads = await Chat.find({
      $or: [{ assignedto: { $exists: false } }, { assignedto: null }, { assignedto: "" }]
    }).sort({ datecreated: 1 });

    if (unassignedLeads.length === 0) return res.json({ success: true, message: "No unassigned leads found." });

    const collection = mongoose.connection.db.collection('configs');
    const excludedConfig = await collection.findOne({ key: 'auto_assign_excluded_staffs' });
    const excludedStaffs = excludedConfig ? excludedConfig.value : [];

    const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 });
    const staffs = allStaffs.filter(s => !excludedStaffs.includes(s.username));
    if (staffs.length === 0) return res.status(400).json({ message: "No staff members available for assignment." });

    const staffUsernames = staffs.map(s => s.username);
    let startIndex = 0;
    const lastAssignedLead = await Chat.findOne({ assignedto: { $in: staffUsernames } }).sort({ datecreated: -1 });
    if (lastAssignedLead) {
      const idx = staffUsernames.indexOf(lastAssignedLead.assignedto);
      startIndex = (idx + 1) % staffs.length;
    }

    for (let i = 0; i < unassignedLeads.length; i++) {
      const staff = staffs[(startIndex + i) % staffs.length];
      await Chat.findByIdAndUpdate(unassignedLeads[i]._id, { assignedto: staff.username });
    }

    res.json({ success: true, message: `Distributed ${unassignedLeads.length} leads across ${staffs.length} staff.` });
  } catch (err) {
    res.status(500).json({ message: "Distribution failed." });
  }
});


app.post("/leads/auto-assign/:id", auth, async (req, res) => {
  try {
    const lead = await Chat.findById(req.params.id);
    if (!lead) return res.status(404).json({ message: "Lead not found" });

    // Check if toggle is actually ON
    const collection = mongoose.connection.db.collection('configs');
    const config = await collection.findOne({ key: 'auto_assign_enabled' });
    const autoAssignEnabled = config ? !!config.value : true;

    if (!autoAssignEnabled) {
       return res.json({ success: false, message: "Auto-Assign toggle is OFF", lead });
    }

    if (lead.assignedto && lead.assignedto.trim() !== "") {
       return res.json({ success: true, message: "Already assigned", lead });
    }

    const assignee = await Chat.getNextAssignee();
    if (assignee) {
      lead.assignedto = assignee;
      await lead.save();
      return res.json({ success: true, message: "Lead auto-assigned", lead });
    }
    
    res.json({ success: false, message: "No staff available for assignment", lead });
  } catch (err) {
    res.status(500).json({ success: false, message: "Assignment failed" });
  }
});

// Manual (re)trigger of the Meta Conversions API lead event — used when the
// automatic send (on pipeline -> Pre-Enrollment/Enrolled) failed, or to resend after a fix.
app.post("/leads/:id/send-to-meta", auth, async (req, res) => {
  try {
    const lead = await Chat.findById(req.params.id);
    if (!lead) return res.status(404).json({ success: false, message: "Lead not found" });

    const metaCapiStages = META_CAPI_STAGES;
    const capiEventName = metaCapiStages[lead.pipeline];
    if (!capiEventName) {
      return res.status(400).json({ success: false, message: "Lead must be in the Site Visit or Converted stage to send this event.", lead });
    }

    const result = await sendLeadCapiEvent(lead, capiEventName);
    if (result.skipped) {
      return res.status(400).json({ success: false, message: result.message, lead });
    }

    const capiEvent = { eventName: capiEventName, sentAt: new Date(), success: result.success, message: result.message };
    const updatedLead = await Chat.findByIdAndUpdate(
      lead._id,
      { $push: { metaCapiEvents: capiEvent } },
      { new: true }
    );

    res.json({ success: result.success, message: result.message, lead: updatedLead });
  } catch (err) {
    console.error("send-to-meta error:", err.message);
    res.status(500).json({ success: false, message: "Failed to send event to Meta." });
  }
});


app.post("/leads/backend/:id", async (req, res) => {
  try {
    const lead = await Chat.findById(req.params.id);
    if (!lead) return res.status(404).json({ message: "Lead not found" });

    // Check if toggle is actually ON
    const collection = mongoose.connection.db.collection('configs');
    const config = await collection.findOne({ key: 'auto_assign_enabled' });
    const autoAssignEnabled = config ? !!config.value : true;

    if (!autoAssignEnabled) {
       return res.json({ success: false, message: "Auto-Assign toggle is OFF", lead });
    }

    if (lead.assignedto && lead.assignedto.trim() !== "") {
       return res.json({ success: true, message: "Already assigned", lead });
    }

    const assignee = await Chat.getNextAssignee();
    if (assignee) {
      lead.assignedto = assignee;
      await lead.save();
      return res.json({ success: true, message: "Lead auto-assigned", lead });
    }
    
    res.json({ success: false, message: "No staff available for assignment", lead });
  } catch (err) {
    res.status(500).json({ success: false, message: "Assignment failed" });
  }
});

app.delete("/leads/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Fetch lead first so we can cleanup any generated bills (S3 or local)
    const lead = await Chat.findById(id);
    if (!lead) {
      return res.status(404).json({ success: false, message: "Lead not found" });
    }

    // Attempt to delete all associated installment bills (best-effort)
    try {
      const installments = lead.fees?.installments || [];
      for (const inst of installments) {
        if (inst && inst.billUrl) {
          try {
            await deleteBill(inst.billUrl);
            console.log(`Deleted bill for installment #${inst.number} of lead ${id}`);
          } catch (err) {
            console.warn(`Failed to delete bill for installment #${inst.number}:`, err.message || err);
          }
        }
      }
    } catch (err) {
      console.warn('Error cleaning up bills for lead deletion:', err.message || err);
    }

    const deletedLead = await Chat.findByIdAndDelete(id);

    res.status(200).json({ success: true, message: "Lead deleted successfully", data: deletedLead });
  } catch (error) {
    console.error("Delete error:", error);
    res.status(500).json({
      success: false,
      message: "Server error while deleting lead",
    });
  }
});


app.post("/leads/send-message", async (req, res) => {
  try {
    const body = req.body;
    console.log("Incoming body:", body);

    const response = await fetch(
      `${process.env.WP_CHAT_SERVICE_URL}/leadmsg/crm`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      }
    );

    const data = await response.json();

    return res.status(response.status).json({
      success: true,
      data
    });

  } catch (error) {
    console.error("Send message error:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to send message"
    });
  }
});


// Bulk WhatsApp campaign to brand-new (non-lead) contacts uploaded from Excel
// on the frontend. Proxies straight through to wpchat-api, which owns the
// Meta WhatsApp send + the TempLead staging collection.
app.post("/leads/bulk-campaign", async (req, res) => {
  try {
    const response = await fetch(
      `${process.env.WP_CHAT_SERVICE_URL}/leadmsg/bulk-temp`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(req.body)
      }
    );

    const data = await response.json();

    return res.status(response.status).json({
      success: true,
      data
    });

  } catch (error) {
    console.error("Bulk campaign error:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to send bulk campaign"
    });
  }
});

app.put("/leads/:id/fees", async (req, res) => {
  try {
    const { id } = req.params;
    const { billedAmount, installments } = req.body;

    // Fetch existing lead to detect removed installments (so we can delete associated bills)
    const leadBefore = await Chat.findById(id);

    const oldInstallments = leadBefore?.fees?.installments || [];
    const processedInstallments = (installments || []).map(inst => {
      const processed = { ...inst, dueDate: new Date(inst.dueDate) };
      if (inst.status === 'Paid') {
        const existing = oldInstallments.find(o => o.number === inst.number);
        if (existing?.status === 'Paid') {
          // Already paid — preserve the original paidAt (even if null for legacy data)
          processed.paidAt = existing.paidAt ?? null;
        } else {
          // Newly paid today
          processed.paidAt = new Date();
        }
      } else {
        processed.paidAt = null;
      }
      return processed;
    });

    // Determine removed installments by `number` field and attempt to delete their stored bills
    try {
      const oldNums = (leadBefore?.fees?.installments || []).map(i => i.number).filter(Boolean);
      const newNums = processedInstallments.map(i => i.number).filter(Boolean);
      const removedNums = oldNums.filter(n => !newNums.includes(n));

      for (const num of removedNums) {
        const inst = (leadBefore?.fees?.installments || []).find(i => i.number === num);
        if (inst && inst.billUrl) {
          try {
            await deleteBill(inst.billUrl);
            console.log(`Deleted bill for removed installment #${num} of lead ${id}`);
          } catch (err) {
            console.warn(`Failed deleting bill for installment #${num}:`, err.message || err);
          }
        }
      }
    } catch (err) {
      console.warn('Error while attempting to clean up removed installment bills:', err.message || err);
    }

    const updateFields = {
      "fees.billedAmount": billedAmount,
      "fees.installments": processedInstallments,
      "fees.updatedAt": new Date()
    };

    // Only stamp billedAt the first time a billedAmount is established
    const previousBilled = leadBefore?.fees?.billedAmount || 0;
    if (billedAmount > 0 && !(previousBilled > 0)) {
      updateFields["fees.billedAt"] = new Date();
    }

    const updatedLead = await Chat.findByIdAndUpdate(
      id,
      { $set: updateFields },
      { new: true }
    );

    res.json({ success: true, lead: updatedLead });
  } catch (error) {
    console.error("Error updating fees:", error);
    res.status(500).json({
      success: false,
      message: "Failed to update fees",
    });
  }
});

// ============================================
// 📄 Bill Generation & Sending Endpoint
// ============================================
app.post("/leads/:id/generate-bill", async (req, res) => {
  try {
    const { id } = req.params;
    const { installmentIndex, sendEmail } = req.body;

    // Validate input
    if (installmentIndex === undefined || installmentIndex === null) {
      return res.status(400).json({
        success: false,
        message: "installmentIndex is required (0-based index)"
      });
    }

    // Fetch the lead
    const lead = await Chat.findById(id);
    if (!lead) {
      return res.status(404).json({
        success: false,
        message: "Lead not found"
      });
    }

    // Generate and send bill
    const result = await generateAndSendBill(lead, installmentIndex, sendEmail !== false);

    // Update the lead with bill generation status if successful
    if (result.success) {
      await Chat.findByIdAndUpdate(
        id,
        {
          $set: {
            "fees.installments.$[elem].billGenerated": true,
            "fees.installments.$[elem].billGeneratedAt": new Date(),
            "fees.installments.$[elem].billUrl": result?.details?.billUrl || result?.details?.billFile || null
          }
        },
        {
          arrayFilters: [{ "elem.number": installmentIndex + 1 }],
          new: true
        }
      ).catch(err => console.log("Note: Could not update bill status in DB:", err.message));
    }

    const totalBilledAmount = lead.fees?.billedAmount || 0;
    const amountPaidFirstInstallment = lead.fees?.installments?.[0]?.amount || 0;
    const totalPaid = (lead.fees?.installments || []).reduce((sum, i) => sum + (i.status === "Paid" ? (i.amount || 0) : 0), 0);
    const balance = totalBilledAmount - totalPaid;

    // Return response with bill generation status and requested totals
    res.json({
      success: result.success,
      message: result.message,
      billGenerated: result.billGenerated,
      emailSent: result.emailSent,
      details: result.details || null,
      installmentNumber: installmentIndex + 1,
      totalBilledAmount,
      amountPaidFirstInstallment,
      totalPaid,
      balance
    });
  } catch (error) {
    console.error("Error generating bill:", error);
    res.status(500).json({
      success: false,
      message: "Failed to generate bill",
      error: error.message
    });
  }
});


// ============================================
// 📋 Get Bill Status Endpoint (Check if bill was generated)
// ============================================
app.get("/leads/:id/bill-status", async (req, res) => {
  try {
    const { id } = req.params;
    const { installmentIndex } = req.query;

    if (installmentIndex === undefined) {
      return res.status(400).json({
        success: false,
        message: "installmentIndex query parameter is required"
      });
    }

    const lead = await Chat.findById(id);
    if (!lead) {
      return res.status(404).json({
        success: false,
        message: "Lead not found"
      });
    }

    const installment = lead.fees?.installments?.[parseInt(installmentIndex)];
    if (!installment) {
      return res.status(404).json({
        success: false,
        message: `Installment #${parseInt(installmentIndex) + 1} not found`
      });
    }

    const totalBilledAmount = lead.fees?.billedAmount || 0;
    const amountPaidFirstInstallment = lead.fees?.installments?.[0]?.amount || 0;
    const totalPaid = (lead.fees?.installments || []).reduce((sum, i) => sum + (i.status === "Paid" ? (i.amount || 0) : 0), 0);
    const balance = totalBilledAmount - totalPaid;

    res.json({
      success: true,
      installmentNumber: parseInt(installmentIndex) + 1,
      billGenerated: installment.billGenerated || false,
      billGeneratedAt: installment.billGeneratedAt || null,
      status: installment.status,
      amount: installment.amount,
      dueDate: installment.dueDate,
      totalBilledAmount,
      amountPaidFirstInstallment,
      totalPaid,
      balance
    });
  } catch (error) {
    console.error("Error fetching bill status:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch bill status",
      error: error.message
    });
  }
});


async function NotifyCounsellor(body) {
  const response = await fetch(
    `${process.env.WP_CHAT_SERVICE_URL}/leadmsg/crm`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }
  );

  return response.json();
}

app.get("/reports/monthly-activity", auth, async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const month = parseInt(req.query.month) || (new Date().getMonth() + 1);
    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });

    const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
    const end = new Date(year, month, 0, 23, 59, 59, 999);

    const matchStage = { assignedto: { $exists: true, $ne: null } };
    if (currentUser.role !== 'admin') {
      matchStage.assignedto = currentUser.username;
    }

    const report = await Chat.aggregate([
      { $match: matchStage },
      {
        $addFields: {
          isNewLeadInMonth: {
            $and: [
              { $gte: ["$datecreated", start] },
              { $lte: ["$datecreated", end] },
              {
                $or: [
                  { $ne: ["$course", "College Internship"] },
                  { $not: [{ $in: ["$pipeline", CONVERTED_STAGES] }] }
                ]
              }
            ]
          },
          monthRemarks: {
            $filter: {
              input: { $ifNull: ["$remarks", []] },
              as: "remark",
              cond: {
                $and: [
                  { $gte: ["$$remark.timestamp", start] },
                  { $lte: ["$$remark.timestamp", end] }
                ]
              }
            }
          },
          feesUpdatedInMonth: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$fees.updatedAt", false] },
                  { $gte: ["$fees.updatedAt", start] },
                  { $lte: ["$fees.updatedAt", end] }
                ]
              },
              1, 0
            ]
          },
          firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] },
          paidInstallmentsInMonth: {
            $filter: {
              input: { $ifNull: ["$fees.installments", []] },
              as: "inst",
              cond: {
                $and: [
                  { $eq: ["$$inst.status", "Paid"] },
                  { $ifNull: ["$$inst.dueDate", false] },
                  { $gte: ["$$inst.dueDate", start] },
                  { $lte: ["$$inst.dueDate", end] }
                ]
              }
            }
          }
        }
      },
      {
        $group: {
          _id: "$assignedto",
          newLeads: { $sum: { $cond: ["$isNewLeadInMonth", 1, 0] } },
          totalRemarks: { $sum: { $size: "$monthRemarks" } },
          feesUpdated: { $sum: "$feesUpdatedInMonth" },
          totalPaid: {
            $sum: {
              $sum: {
                $map: {
                  input: "$paidInstallmentsInMonth",
                  as: "inst",
                  in: { $ifNull: ["$$inst.amount", 0] }
                }
              }
            }
          },
          totalBilled: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $ifNull: ["$firstInstDueDate", false] },
                    { $gte: ["$firstInstDueDate", start] },
                    { $lte: ["$firstInstDueDate", end] }
                  ]
                },
                { $ifNull: ["$fees.billedAmount", 0] },
                0
              ]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          username: "$_id",
          newLeads: 1,
          totalRemarks: 1,
          feesUpdated: 1,
          totalPaid: 1,
          totalBilled: 1
        }
      },
      { $sort: { username: 1 } }
    ]);

    res.json({ success: true, year, month, report });
  } catch (err) {
    console.error("Monthly Report Error:", err);
    res.status(500).json({ success: false, message: "Error generating monthly report" });
  }
});

// Get monthly targets (admin sees all, non-admin sees own)
app.get("/reports/user-targets", auth, async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const month = parseInt(req.query.month) || (new Date().getMonth() + 1);
    const currentUser = await User.findById(req.user?.id).lean();
    if (!currentUser) return res.status(401).json({ success: false, message: "Unauthorized" });
    const query = { year, month };
    if (currentUser.role !== 'admin') {
      query.username = currentUser.username;
    }
    const targets = await UserMonthlyTarget.find(query).lean();
    res.json({ success: true, targets });
  } catch (err) {
    console.error("GET /reports/user-targets error:", err);
    res.status(500).json({ success: false, message: "Failed to fetch targets" });
  }
});

// Set monthly target for a user (admin only)
app.post("/reports/user-targets", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const { username, year, month, target } = req.body;
    if (!username || !year || !month || target === undefined) {
      return res.status(400).json({ success: false, message: "username, year, month, target are required" });
    }
    const doc = await UserMonthlyTarget.findOneAndUpdate(
      { username, year, month },
      { target: Number(target) },
      { upsert: true, new: true }
    );
    res.json({ success: true, target: doc });
  } catch (err) {
    console.error("POST /reports/user-targets error:", err);
    res.status(500).json({ success: false, message: "Failed to set target" });
  }
});

// Monthly enrolled pipeline grouped by lead source
app.get("/reports/monthly-pipeline-by-source", async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const month = parseInt(req.query.month) || (new Date().getMonth() + 1);
    const assignedto = req.query.assignedto || req.query.counsellor || req.query.username || null;
    const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
    const end = new Date(year, month, 0, 23, 59, 59, 999);

    const matchStage = {
      pipeline: { $in: CONVERTED_STAGES },
      course: { $ne: "College Internship" },
      datecreated: { $gte: start, $lte: end },
      ...(assignedto ? { assignedto } : {})
    };

    const data = await Chat.aggregate([
      { $match: matchStage },
      {
        $group: {
          _id: { $ifNull: ["$leadfrom", "Unknown"] },
          count: { $sum: 1 }
        }
      },
      { $project: { _id: 0, label: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]);

    res.json({ success: true, year, month, data });
  } catch (err) {
    console.error("Pipeline By Source Error:", err);
    res.status(500).json({ success: false, message: "Error generating pipeline by source report" });
  }
});

// Monthly enrolled pipeline grouped by course
app.get("/reports/monthly-pipeline-by-course", async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const month = parseInt(req.query.month) || (new Date().getMonth() + 1);
    const assignedto = req.query.assignedto || req.query.counsellor || req.query.username || null;
    const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
    const end = new Date(year, month, 0, 23, 59, 59, 999);

    const matchStage = {
      pipeline: { $in: CONVERTED_STAGES },
      course: { $ne: "College Internship" },
      datecreated: { $gte: start, $lte: end },
      ...(assignedto ? { assignedto } : {})
    };

    const data = await Chat.aggregate([
      { $match: matchStage },
      {
        $group: {
          _id: { $ifNull: ["$course", "Not Specified"] },
          count: { $sum: 1 }
        }
      },
      { $project: { _id: 0, label: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]);

    res.json({ success: true, year, month, data });
  } catch (err) {
    console.error("Pipeline By Course Error:", err);
    res.status(500).json({ success: false, message: "Error generating pipeline by course report" });
  }
});

// Monthly enrolled pipeline split by whether the lead was ever a Qualified Lead
// or a Responded (post-follow-up) lead before converting
app.get("/reports/monthly-pipeline-qualified-vs-responded", async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const month = parseInt(req.query.month) || (new Date().getMonth() + 1);
    const assignedto = req.query.assignedto || req.query.counsellor || req.query.username || null;
    const start = new Date(year, month - 1, 1, 0, 0, 0, 0);
    const end = new Date(year, month, 0, 23, 59, 59, 999);

    const matchStage = {
      pipeline: { $in: CONVERTED_STAGES },
      course: { $ne: "College Internship" },
      datecreated: { $gte: start, $lte: end },
      ...(assignedto ? { assignedto } : {})
    };

    const [row] = await Chat.aggregate([
      { $match: matchStage },
      {
        $group: {
          _id: null,
          qualified: { $sum: { $cond: [QUALIFIED_LEAD_EXPR, 1, 0] } },
          responded: { $sum: { $cond: [{ $eq: ["$respondedAfterFollowUp", true] }, 1, 0] } }
        }
      }
    ]);

    const data = [
      { label: "Qualified Leads", count: row?.qualified || 0 },
      { label: "Responded Leads", count: row?.responded || 0 }
    ];

    res.json({ success: true, year, month, data });
  } catch (err) {
    console.error("Pipeline Qualified vs Responded Error:", err);
    res.status(500).json({ success: false, message: "Error generating qualified vs responded report" });
  }
});

// ─── Weekly AI Report ──────────────────────────────────────────────────────────
app.get("/reports/weekly-ai", auth, async (req, res) => {
  try {
    const { role, username } = req.user;
    const query = role === 'admin' ? {} : { counsellor: username };
    const reports = await WeeklyReport.find(query)
      .sort({ weekStart: -1 })
      .limit(52)
      .lean();
    res.json({ success: true, reports });
  } catch (err) {
    console.error("GET /reports/weekly-ai error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Monthly AI Report ──────────────────────────────────────────────────────────
app.get("/reports/monthly-ai", auth, async (req, res) => {
  try {
    const { role, username } = req.user;
    const query = role === 'admin' ? {} : { counsellor: username };
    const reports = await MonthlyReport.find(query)
      .sort({ monthStart: -1 })
      .limit(24)
      .lean();
    res.json({ success: true, reports });
  } catch (err) {
    console.error("GET /reports/monthly-ai error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Minmini: shared computation (no DB write) ──────────────────────────────
async function computeMinminiData(weekStart, weekEnd, matchFilter) {
  // Run WA leads aggregation and follow-up counts in parallel
  const [leads, followUpsSent, followUpResponded] = await Promise.all([
    Chat.aggregate([
      {
        $match: {
          ...matchFilter,
          datecreated: { $gte: weekStart, $lte: weekEnd },
          "conversations.0": { $exists: true }
        }
      },
      {
        $project: {
          userName: 1, courseofintrest: 1, course: 1,
          assignedto: 1, status: 1, pipeline: 1, conversations: 1
        }
      }
    ]),
    // Leads created this week that also received a follow-up this week
    Chat.countDocuments({
      ...matchFilter,
      datecreated: { $gte: weekStart, $lte: weekEnd },
      lastFollowUpSentAt: { $gte: weekStart, $lte: weekEnd }
    }),
    // Of those, leads who replied after the follow-up
    Chat.countDocuments({
      ...matchFilter,
      datecreated: { $gte: weekStart, $lte: weekEnd },
      lastFollowUpSentAt: { $gte: weekStart, $lte: weekEnd },
      respondedAfterFollowUp: true
    })
  ]);

  const followUpResponsePct = followUpsSent > 0
    ? Math.round((followUpResponded / followUpsSent) * 100)
    : 0;

  if (leads.length === 0) {
    return {
      activeLeads: 0, enrolledAfterWA: 0, totalMessages: 0,
      avgMessagesPerLead: 0, avgDurationMins: 0,
      peakHour: null, peakHours: {}, topCourses: [],
      followUpsSent, followUpResponded, followUpResponsePct,
      aiSummary: "No WhatsApp conversations found for leads created this week."
    };
  }

  const SKIP_HOUR = 8;
  const hourCounts = new Array(24).fill(0);
  const courseCounts = {};
  let totalMessages = 0, totalDurationMins = 0, leadsWithDuration = 0;
  const leadSamples = [];

  for (const lead of leads) {
    const convs = lead.conversations || [];
    if (!convs.length) continue;
    totalMessages += convs.length;
    for (const c of convs) {
      if (c.timestamp) {
        // Convert to IST (UTC+5:30) before extracting hour
        const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
        const h = new Date(new Date(c.timestamp).getTime() + IST_OFFSET_MS).getUTCHours();
        if (h !== SKIP_HOUR) hourCounts[h]++;
      }
    }
    if (convs.length > 1) {
      const sorted = [...convs].filter(c => c.timestamp).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      if (sorted.length > 1) {
        const dur = (new Date(sorted[sorted.length - 1].timestamp) - new Date(sorted[0].timestamp)) / 60000;
        if (dur >= 0) { totalDurationMins += dur; leadsWithDuration++; }
      }
    }
    const course = lead.courseofintrest || lead.course || "Not Specified";
    courseCounts[course] = (courseCounts[course] || 0) + 1;
    if (leadSamples.length < 12) {
      const sample = convs.slice(0, 3).map(c => c.userMsg).join(" | ");
      leadSamples.push(`• ${lead.userName || "Unknown"} (${course}): "${sample}"`);
    }
  }

  const nonInternshipLeads = leads.filter(l => !/^college internship$/i.test(l.course || ""));
  const activeLeads = nonInternshipLeads.length;
  const enrolledAfterWA = nonInternshipLeads.filter(l => CONVERTED_RE.test(l.pipeline || "")).length;
  const avgMessagesPerLead = Math.round(totalMessages / leads.length);
  const avgDurationMins = leadsWithDuration > 0 ? Math.round(totalDurationMins / leadsWithDuration) : 0;

  const peakHours = {};
  hourCounts.forEach((count, hour) => { if (count > 0 && hour !== SKIP_HOUR) peakHours[hour] = count; });
  let peakHour = null;
  const peakEntries = Object.entries(peakHours);
  if (peakEntries.length > 0) peakHour = Number(peakEntries.sort((a, b) => b[1] - a[1])[0][0]);

  const topCourses = Object.entries(courseCounts)
    .sort((a, b) => b[1] - a[1]).slice(0, 8)
    .map(([course, count]) => ({ course, count }));

  const weekLabel = `${weekStart.toLocaleDateString("en-IN")} – ${weekEnd.toLocaleDateString("en-IN")}`;
  const courseText = topCourses.map(c => `${c.course}: ${c.count} lead${c.count > 1 ? "s" : ""}`).join(", ");
  const peakHourLabel = peakHour !== null ? `${peakHour}:00–${peakHour + 1}:00 (${peakHours[peakHour]} messages)` : "N/A";
  const conversionRate = activeLeads > 0 ? Math.round((enrolledAfterWA / activeLeads) * 100) : 0;

  let aiSummary = "AI analysis unavailable.";
  try {
    const prompt = `You are a WhatsApp engagement analyst for BetaSpace, a co-working space CRM. Analyze the WhatsApp bot conversation data from this week and provide a comprehensive, data-driven summary.

Week: ${weekLabel}
New Leads with WhatsApp Activity: ${activeLeads}
Enrolled After WhatsApp Conversation: ${enrolledAfterWA} (${conversionRate}% conversion rate)
Total Conversation Exchanges: ${totalMessages}
Avg Exchanges per Lead: ${avgMessagesPerLead}
Avg Session Duration: ${avgDurationMins} minutes
Peak Organic Hour (excl. 8 AM follow-up blast): ${peakHourLabel}

Follow-up Performance:
- Follow-up Messages Sent This Week: ${followUpsSent} leads
- Responded After Follow-up: ${followUpResponded} leads (${followUpResponsePct}% response rate)

Course Enquiry Distribution:
${courseText}

Sample WhatsApp Queries from Leads:
${leadSamples.join("\n")}

Write a rich WhatsApp engagement analysis (max 300 words) covering:
1. Engagement health and conversion — is the WA channel converting well? What does the ${conversionRate}% conversion rate tell us?
2. Follow-up effectiveness — is the ${followUpResponsePct}% follow-up response rate good or poor? What does it suggest?
3. Course demand patterns — which courses dominate, which show strong buying intent, any unexpected trends?
4. Peak hour behavior — what does the organic engagement timing reveal about lead persona (freelancers vs. startups vs. corporate teams, etc.)?
5. Conversation quality — based on the sample queries, are leads asking price/feature/placement questions or just casually exploring?
6. Two specific, actionable recommendations to improve either WhatsApp conversion or follow-up response rates

Be data-driven, reference the actual numbers, and avoid generic advice.`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are a WhatsApp engagement analyst for BetaSpace, a co-working space CRM. Be concise, data-driven, and specific." },
        { role: "user", content: prompt }
      ],
      max_tokens: 600
    });
    aiSummary = completion.choices[0].message.content.trim();
  } catch (aiErr) {
    console.error("Minmini AI error:", aiErr.message);
  }

  return { activeLeads, enrolledAfterWA, totalMessages, avgMessagesPerLead, avgDurationMins, peakHour, peakHours, topCourses, followUpsSent, followUpResponded, followUpResponsePct, aiSummary };
}

// Org-wide compute + upsert to DB (called by cron and by admin refresh)
async function computeAndSaveMinminiReport(weekStart, weekEnd) {
  const data = await computeMinminiData(weekStart, weekEnd, {});
  const wsDay = new Date(weekStart); wsDay.setHours(0, 0, 0, 0);
  const wsDayEnd = new Date(weekStart); wsDayEnd.setHours(23, 59, 59, 999);
  await MinminiReport.findOneAndUpdate(
    { weekStart: { $gte: wsDay, $lte: wsDayEnd } },
    { weekStart, weekEnd, generatedAt: new Date(), ...data },
    { upsert: true, new: true }
  );
  return data;
}

// ─── WhatsApp Minmini Summary ───────────────────────────────────────────────
app.get("/reports/whatsapp-minmini", auth, async (req, res) => {
  try {
    const { role, username } = req.user;
    const forceRefresh = req.query.refresh === "true";

    // Week range — default to last complete Mon–Sun
    let weekStart, weekEnd;
    if (req.query.weekStart && req.query.weekEnd) {
      weekStart = new Date(req.query.weekStart);
      weekEnd = new Date(req.query.weekEnd);
    } else {
      const now = new Date();
      weekEnd = new Date(now);
      weekEnd.setDate(weekEnd.getDate() - 1);
      weekEnd.setHours(23, 59, 59, 999);
      weekStart = new Date(weekEnd);
      weekStart.setDate(weekStart.getDate() - 6);
      weekStart.setHours(0, 0, 0, 0);
    }

    // Admin: serve from DB unless force-refresh requested
    if (role === "admin" && !forceRefresh) {
      const wsDay = new Date(weekStart); wsDay.setHours(0, 0, 0, 0);
      const wsDayEnd = new Date(weekStart); wsDayEnd.setHours(23, 59, 59, 999);
      const saved = await MinminiReport.findOne({ weekStart: { $gte: wsDay, $lte: wsDayEnd } }).lean();
      if (saved && saved.followUpsSent != null) {
        return res.json({ success: true, ...saved });
      }
      // Not in DB yet — compute and save
      const data = await computeAndSaveMinminiReport(weekStart, weekEnd);
      return res.json({ success: true, weekStart, weekEnd, ...data });
    }

    // Admin force-refresh: recompute and save
    if (role === "admin" && forceRefresh) {
      const data = await computeAndSaveMinminiReport(weekStart, weekEnd);
      return res.json({ success: true, weekStart, weekEnd, ...data });
    }

    // Counsellor: compute their own scoped data (not saved to DB)
    const data = await computeMinminiData(weekStart, weekEnd, { assignedto: username });
    return res.json({ success: true, weekStart, weekEnd, ...data });
  } catch (err) {
    console.error("GET /reports/whatsapp-minmini error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Monthly Minmini: shared computation (no DB write) ──────────────────────
async function computeMonthlyMinminiData(monthStart, monthEnd, matchFilter) {
  const [leads, followUpsSent, followUpResponded] = await Promise.all([
    Chat.aggregate([
      {
        $match: {
          ...matchFilter,
          datecreated: { $gte: monthStart, $lte: monthEnd },
          "conversations.0": { $exists: true }
        }
      },
      {
        $project: {
          userName: 1, courseofintrest: 1, course: 1,
          assignedto: 1, status: 1, pipeline: 1, conversations: 1
        }
      }
    ]),
    Chat.countDocuments({
      ...matchFilter,
      datecreated: { $gte: monthStart, $lte: monthEnd },
      lastFollowUpSentAt: { $gte: monthStart, $lte: monthEnd }
    }),
    Chat.countDocuments({
      ...matchFilter,
      datecreated: { $gte: monthStart, $lte: monthEnd },
      lastFollowUpSentAt: { $gte: monthStart, $lte: monthEnd },
      respondedAfterFollowUp: true
    })
  ]);

  const followUpResponsePct = followUpsSent > 0
    ? Math.round((followUpResponded / followUpsSent) * 100)
    : 0;

  if (leads.length === 0) {
    return {
      activeLeads: 0, enrolledAfterWA: 0, totalMessages: 0,
      avgMessagesPerLead: 0, avgDurationMins: 0,
      peakHour: null, peakHours: {}, topCourses: [],
      followUpsSent, followUpResponded, followUpResponsePct,
      aiSummary: "No WhatsApp conversations found for leads created this month."
    };
  }

  const SKIP_HOUR = 8;
  const hourCounts = new Array(24).fill(0);
  const courseCounts = {};
  let totalMessages = 0, totalDurationMins = 0, leadsWithDuration = 0;
  const leadSamples = [];

  for (const lead of leads) {
    const convs = lead.conversations || [];
    if (!convs.length) continue;
    totalMessages += convs.length;
    for (const c of convs) {
      if (c.timestamp) {
        const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
        const h = new Date(new Date(c.timestamp).getTime() + IST_OFFSET_MS).getUTCHours();
        if (h !== SKIP_HOUR) hourCounts[h]++;
      }
    }
    if (convs.length > 1) {
      const sorted = [...convs].filter(c => c.timestamp).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      if (sorted.length > 1) {
        const dur = (new Date(sorted[sorted.length - 1].timestamp) - new Date(sorted[0].timestamp)) / 60000;
        if (dur >= 0) { totalDurationMins += dur; leadsWithDuration++; }
      }
    }
    const course = lead.courseofintrest || lead.course || "Not Specified";
    courseCounts[course] = (courseCounts[course] || 0) + 1;
    if (leadSamples.length < 12) {
      const sample = convs.slice(0, 3).map(c => c.userMsg).join(" | ");
      leadSamples.push(`• ${lead.userName || "Unknown"} (${course}): "${sample}"`);
    }
  }

  const activeLeads = leads.length;
  const enrolledAfterWA = leads.filter(l => CONVERTED_RE.test(l.pipeline || "")).length;
  const avgMessagesPerLead = Math.round(totalMessages / activeLeads);
  const avgDurationMins = leadsWithDuration > 0 ? Math.round(totalDurationMins / leadsWithDuration) : 0;

  const peakHours = {};
  hourCounts.forEach((count, hour) => { if (count > 0 && hour !== SKIP_HOUR) peakHours[hour] = count; });
  let peakHour = null;
  const peakEntries = Object.entries(peakHours);
  if (peakEntries.length > 0) peakHour = Number(peakEntries.sort((a, b) => b[1] - a[1])[0][0]);

  const topCourses = Object.entries(courseCounts)
    .sort((a, b) => b[1] - a[1]).slice(0, 8)
    .map(([course, count]) => ({ course, count }));

  const monthLabel = `${monthStart.toLocaleDateString("en-IN")} – ${monthEnd.toLocaleDateString("en-IN")}`;
  const courseText = topCourses.map(c => `${c.course}: ${c.count} lead${c.count > 1 ? "s" : ""}`).join(", ");
  const peakHourLabel = peakHour !== null ? `${peakHour}:00–${peakHour + 1}:00 (${peakHours[peakHour]} messages)` : "N/A";
  const conversionRate = Math.round((enrolledAfterWA / activeLeads) * 100);

  let aiSummary = "AI analysis unavailable.";
  try {
    const prompt = `You are a WhatsApp engagement analyst for BetaSpace, a co-working space CRM. Analyze the WhatsApp bot conversation data from this month and provide a comprehensive, data-driven summary.

Month: ${monthLabel}
New Leads with WhatsApp Activity: ${activeLeads}
Enrolled After WhatsApp Conversation: ${enrolledAfterWA} (${conversionRate}% conversion rate)
Total Conversation Exchanges: ${totalMessages}
Avg Exchanges per Lead: ${avgMessagesPerLead}
Avg Session Duration: ${avgDurationMins} minutes
Peak Organic Hour (excl. 8 AM follow-up blast): ${peakHourLabel}

Follow-up Performance:
- Follow-up Messages Sent This Month: ${followUpsSent} leads
- Responded After Follow-up: ${followUpResponded} leads (${followUpResponsePct}% response rate)

Course Enquiry Distribution:
${courseText}

Sample WhatsApp Queries from Leads:
${leadSamples.join("\n")}

Write a rich WhatsApp engagement analysis (max 300 words) covering:
1. Engagement health and conversion — is the WA channel converting well? What does the ${conversionRate}% conversion rate tell us for the month?
2. Follow-up effectiveness — is the ${followUpResponsePct}% follow-up response rate good or poor? What does it suggest?
3. Course demand patterns — which courses dominate, which show strong buying intent, any unexpected trends?
4. Peak hour behavior — what does the organic engagement timing reveal about lead persona (freelancers vs. startups vs. corporate teams, etc.)?
5. Conversation quality — based on the sample queries, are leads asking price/feature/placement questions or just casually exploring?
6. Two specific, actionable recommendations to improve either WhatsApp conversion or follow-up response rates next month

Be data-driven, reference the actual numbers, and avoid generic advice.`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are a WhatsApp engagement analyst for BetaSpace, a co-working space CRM. Be concise, data-driven, and specific." },
        { role: "user", content: prompt }
      ],
      max_tokens: 600
    });
    aiSummary = completion.choices[0].message.content.trim();
  } catch (aiErr) {
    console.error("Monthly Minmini AI error:", aiErr.message);
  }

  return { activeLeads, enrolledAfterWA, totalMessages, avgMessagesPerLead, avgDurationMins, peakHour, peakHours, topCourses, followUpsSent, followUpResponded, followUpResponsePct, aiSummary };
}

async function computeAndSaveMonthlyMinminiReport(monthStart, monthEnd) {
  const data = await computeMonthlyMinminiData(monthStart, monthEnd, {});
  const msDay = new Date(monthStart); msDay.setHours(0, 0, 0, 0);
  const msDayEnd = new Date(monthStart); msDayEnd.setHours(23, 59, 59, 999);
  await MonthlyMinminiReport.findOneAndUpdate(
    { monthStart: { $gte: msDay, $lte: msDayEnd } },
    { monthStart, monthEnd, generatedAt: new Date(), ...data },
    { upsert: true, new: true }
  );
  return data;
}

// ─── Monthly WhatsApp Minmini Summary ──────────────────────────────────────
app.get("/reports/monthly-whatsapp-minmini", auth, async (req, res) => {
  try {
    const { role, username } = req.user;
    const forceRefresh = req.query.refresh === "true";

    // Month range — default to last complete calendar month
    let monthStart, monthEnd;
    if (req.query.monthStart && req.query.monthEnd) {
      monthStart = new Date(req.query.monthStart);
      monthEnd = new Date(req.query.monthEnd);
    } else {
      const now = new Date();
      const y = now.getFullYear();
      const m = now.getMonth(); // 0-indexed
      monthStart = new Date(y, m - 1, 1, 0, 0, 0, 0);
      monthEnd = new Date(y, m, 0, 23, 59, 59, 999);
    }

    if (role === "admin" && !forceRefresh) {
      const msDay = new Date(monthStart); msDay.setHours(0, 0, 0, 0);
      const msDayEnd = new Date(monthStart); msDayEnd.setHours(23, 59, 59, 999);
      const saved = await MonthlyMinminiReport.findOne({ monthStart: { $gte: msDay, $lte: msDayEnd } }).lean();
      if (saved && saved.followUpsSent != null) {
        return res.json({ success: true, ...saved });
      }
      const data = await computeAndSaveMonthlyMinminiReport(monthStart, monthEnd);
      return res.json({ success: true, monthStart, monthEnd, ...data });
    }

    if (role === "admin" && forceRefresh) {
      const data = await computeAndSaveMonthlyMinminiReport(monthStart, monthEnd);
      return res.json({ success: true, monthStart, monthEnd, ...data });
    }

    const data = await computeMonthlyMinminiData(monthStart, monthEnd, { assignedto: username });
    return res.json({ success: true, monthStart, monthEnd, ...data });
  } catch (err) {
    console.error("GET /reports/monthly-whatsapp-minmini error:", err);
    res.status(500).json({ error: err.message });
  }
});

async function LeadAddNotification(number) {
  try {
    console.log("Add Lead Notified",number);

    const response = await fetch(
      `${process.env.WP_CHAT_SERVICE_URL}/notify/addlead`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fullNumber: number
        })
      }
    );

    const text = await response.text();
    if (!response.ok) {
      console.error("LeadAddNotification failed response:", {
        status: response.status,
        bodyPreview: text.slice(0, 200)
      });
      return {
        success: false,
        status: response.status,
        message: "LeadAddNotification request failed"
      };
    }

    return {
      success: true,
      status: response.status,
      message: text || "Notification sent"
    };
  } catch (error) {
    console.error("LeadAddNotification error:", error.message);
    return {
      success: false,
      message: "LeadAddNotification failed"
    };
  }
}

module.exports = { NotifyCounsellor };




// --- AI Analysis Endpoint & Logic (Add this to your server file) ---

// Required imports:
// const OpenAI = require('openai');

// Initialize OpenAI
// const openai = new OpenAI({
//   apiKey: process.env.OPENAI_API_KEY,
// });


// app.post('/leads/analyze/:id', async (req, res) => {
//     try {
//         const leadId = req.params.id;
//         const lead = await ChatModel.findById(leadId); // Assuming your model is named ChatModel

//         if (!lead) {
//             return res.status(404).json({ success: false, message: "Lead not found" });
//         }

//         // Prepare data for analysis
//         const remarksText = lead.remarks.map(r => `[${r.timestamp}] ${r.remark}`).join("\n");
//         const conversationsText = lead.conversations.map(c => `User: ${c.userMsg}\nBot: ${c.botReply}`).join("\n---\n");
        
//         const prompt = `
//         Analyze the following lead based on their remarks and conversation history.
        
//         Lead Details:
//         - Name: ${lead.userName}
//         - Course of Interest: ${lead.courseofintrest}
//         - Current Status: ${lead.status}
//         - Pipeline Stage: ${lead.pipeline}

//         Remarks History:
//         ${remarksText || "No remarks available."}

//         Conversation History:
//         ${conversationsText || "No conversation history available."}

//         Please provide a JSON response with the following fields and all fields are mandatory if you cannot fill any field say need more details:
//         1. "summary": A brief summary of the lead's engagement and interest.
//         2. "intentScore": A score from 0-100 indicating their likelihood to convert.
//         3. "buyingSignals": A list of positive buying signals observed.
//         4. "objections": A list of potential objections or concerns raised.
//         5. "recommendedAction": Specific next steps to move this lead forward.
//         6. "suggestedMessage": A personalized follow-up message draft.

//         Format the output purely as valid JSON.
//         `;

//         const completion = await openai.chat.completions.create({
//             messages: [{ role: "system", content: "You are an expert sales analyst assistant." }, { role: "user", content: prompt }],
//             model: "gpt-3.5-turbo",
//             response_format: { type: "json_object" },
//         });

//         const analysisResult = JSON.parse(completion.choices[0].message.content);

//         // Update lead with analysis
//         // lead.aiAnalysis = analysisResult;
//         // await lead.save();

//         res.json({ success: true, analysis: analysisResult });

//     } catch (error) {
//         console.error("Error analyzing lead:", error);
//         res.status(500).json({ success: false, message: "Failed to analyze lead", error: error.message });
//     }
// });


app.post("/leads/analyze/:id", async (req, res) => {
  try {
    const leadId = req.params.id;
    const lead = await Chat.findById(leadId);

    if (!lead) {
      return res.status(404).json({ success: false, message: "Lead not found" });
    }

    const remarksText = (lead.remarks || []).map(r => `[${r.timestamp}] ${r.remark}`).join("\n");
    const conversationsText = (lead.conversations || []).map(c => `User: ${c.userMsg}\nBot: ${c.botReply}`).join("\n---\n");

    const prompt = `
Analyze the lead "${lead.userName}" for "${lead.course || lead.courseofintrest}".
Current Status: ${lead.status} / ${lead.pipeline}
History: ${remarksText}
Conversations: ${conversationsText}

Generate exactly TWO distinct strategy solutions (Solution A and Solution B).
Mandatory rules:
1. If details for a field are missing, state "No details provided".
2. Solution A MUST be "Soft Re-Engagement Strategy".
3. Solution B MUST be "Site Visit Push Strategy".

Fields for each solution:
1) probability: Conversion Probability (e.g., 64%)
2) why: Brief explanation of the probability.
3) psychology: Behaviour & Psychology Insight.
4) marketTrend: Market Trend Injection.
5) roi: ROI & Business Value Framing.
6) whatsapp: Suggested WhatsApp Message.
7) nextAction: Recommended Next Action.

Return ONLY valid JSON:
{
  "solutionA": { "title": "Soft Re-Engagement Strategy", "probability": "...", "why": "...", "psychology": "...", "marketTrend": "...", "roi": "...", "whatsapp": "...", "nextAction": "..." },
  "solutionB": { "title": "Site Visit Push Strategy", "probability": "...", "why": "...", "psychology": "...", "marketTrend": "...", "roi": "...", "whatsapp": "...", "nextAction": "..." }
}
`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are a senior co-working space sales strategist." },
        { role: "user", content: prompt }
      ],
      response_format: { type: "json_object" }
    });

    const analysisResult = JSON.parse(completion.choices[0].message.content);
    lead.aiAnalysis = analysisResult;
    await lead.save();

    res.json({ success: true, analysis: analysisResult });
  } catch (error) {
    console.error("AI Analysis Error:", error);
    res.status(500).json({ success: false, message: "Failed to generate strategies" });
  }
});

app.post("/leads/assistant/ask", async (req, res) => {
  try {
    const { lead, question, history = [] } = req.body;


    if (!lead || !question) {
      return res.status(400).json({
        success: false,
        message: "Lead and question are required"
      });
    }

    const remarksText = (lead.remarks || [])
      .map(r => `[${r.timestamp}] ${r.remark}`)
      .join("\n");

    const conversationsText = (lead.conversations || [])
      .map(c => `User: ${c.userMsg}\nBot: ${c.botReply}`)
      .join("\n---\n");

     const chatHistory = history.map(m => ({
  role: m.role,
  content: m.content
}));


const prompt = `
You are Minmini, an intelligent CRM Lead Assistant helping a sales executive manage and convert leads.

The person asking questions is an INTERNAL CRM USER (sales executive), NOT the lead.

--------------------------------------------------
STEP 1 — Understand the Question Type
--------------------------------------------------

First determine the intent of the user's question.

There are 3 types:

1️⃣ GREETING / SMALL TALK
Examples:
hi
hello
good morning
how are you

→ Respond like a friendly chatbot I'm minmini you're sales assistant and here to guide.
→ DO NOT talk about the lead.


2️⃣ GENERAL QUESTION
Examples:
job opportunities for python
what is full stack
course duration
salary for python developer
career scope

→ Answer normally like a chatbot.
→ DO NOT talk about the lead unless explicitly asked.


3️⃣ LEAD RELATED QUESTION
Examples:
summarize this lead
is this lead interested
what should be the next follow up
generate whatsapp message for this lead
what is the status of the lead

→ Use the lead profile and analyze.


--------------------------------------------------
STEP 2 — Response Rules
--------------------------------------------------

If GREETING:
Reply friendly in 1–2 lines.

If GENERAL QUESTION:
Answer normally in 3–4 lines.

If LEAD QUESTION:
Analyze the lead and provide insights.

Possible actions:
• Lead summary
• Conversion potential
• Next step suggestion

If follow-up seems needed ask:

"Would you like me to generate a suggested WhatsApp follow-up message? or How can to convert the lead"

If user says YES:
Generate ONLY 2 line WhatsApp message.

Never restart the conversation if the user replies with short confirmations like yes, ok, sure, thanks.
Always interpret them based on previous assistant message.


--------------------------------------------------
USER QUESTION
--------------------------------------------------
${question}


--------------------------------------------------
LEAD PROFILE (USE ONLY IF QUESTION IS LEAD RELATED)
--------------------------------------------------
Name: ${lead.userName}
Phone: ${lead.userNumber}
Course Interest: ${lead.course || lead.courseofintrest || "N/A"}
Status: ${lead.status}
Pipeline: ${lead.pipeline}
Source: ${lead.leadfrom || "N/A"}
Plan Duration: ${lead.programType || "N/A"}


--------------------------------------------------
REMARKS
--------------------------------------------------
${remarksText || "No remarks available."}


--------------------------------------------------
CONVERSATION HISTORY
--------------------------------------------------
${conversationsText || "No conversation history available."}
`;


    console.log(history)
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are an minmini sales assistant." },
         ...chatHistory,
        { role: "user", content: prompt }
      ],
    });

    const reply = completion.choices[0].message.content;

    res.json({
      success: true,
      reply
    });
  } catch (error) {
    console.error("Lead assistant error:", error);
    res.status(500).json({
      success: false,
      message: "Failed to get assistant response",
      error: error.message
    });
  }
});


// Converts 1st letter to string
// function toTitleCase(str) {
//   if (!str || typeof str !== "string") return str;
//   return str
//     .trim()
//     .toLowerCase()
//     .replace(/\b\w/g, (char) => char.toUpperCase());
// }

// Converts all to uppercase
function toTitleCase(str) {
  if (!str || typeof str !== "string") return str;
  return str.trim().toUpperCase();
}

app.post("/intern/certificate", async (req, res) => {
  console.log(req.body)
  try {
    const {
      salutation,
      userName,
      userEmail,
      phone,
      reg_no,
      year_of_study,
      college,
      college_location,
      degree,
      other_degree,
      department,
      internship,
      other_internship,
      workshop,
      other_workshop,
      start_date,
      end_date,
      issue_date,
      company,
      batch_code,
      career_advisor,
      is_leader
    } = req.body;

    if (!userName || !phone) {
      return res.status(400).json({
        success: false,
        message: "userName and phone are required."
      });
    }

    const sanitizedNumber = String(phone).replace(/\D/g, "");


    const fullNumber = cleanIndianMobileNumber(sanitizedNumber)
    const parsedIsLeader =
      typeof is_leader === "boolean"
        ? is_leader
        : typeof is_leader === "string"
          ? is_leader.trim().toLowerCase() === "true"
          : false;

    const internDetails = {
      salutation: salutation?.trim() || "",
      userName: toTitleCase(userName) || "",
      userEmail: userEmail?.trim().toLowerCase() || "",
      phone: sanitizedNumber,
      reg_no: reg_no?.trim() || "",
      year_of_study: year_of_study?.trim() || "",
      college: toTitleCase(college) || "",
      college_location: toTitleCase(college_location)|| "",
      degree: toTitleCase(degree) || "",
      other_degree: other_degree?.trim() || "",
      department: toTitleCase(department) || "",
      internship: toTitleCase(internship)|| "",
      other_internship: other_internship?.trim() || "",
      workshop: toTitleCase(workshop) || "",
      other_workshop: other_workshop?.trim() || "",
      start_date: start_date || null,
      end_date: end_date || null,
      issue_date: issue_date || null,
      company: company?.trim() || "",
      batch_code: batch_code?.trim() || ""
    };

    let chat = await Chat.findOne({ userNumber: fullNumber });

    if (chat) {
      chat.userName = chat.userName || internDetails.userName;
      chat.is_leader = parsedIsLeader;
      if (!Array.isArray(chat.InternDetails)) {
        chat.InternDetails = [];
      }
      // chat.InternDetails.push(internDetails);
      chat.InternDetails = [internDetails];
      await chat.save();
    } else {
      console.log(career_advisor)
      chat = new Chat({
        userName: internDetails.userName,
        userNumber: fullNumber,
        status: "Cold",
        pipeline: "Enrolled",
        leadfrom: "Website",
        course : "College Internship",
        assignedto : career_advisor?.trim() || "",
        is_leader: parsedIsLeader,
        lastInteracted: new Date(),
        datecreated: new Date(),
        InternDetails: [internDetails]
      });
      chat.$locals.skipNotify = true;
      await chat.save();
    }

    return res.status(201).json({
      success: true,
      message: "Intern certificate details saved successfully.",
      chat
    });
  } catch (error) {
    console.error("Error saving intern certificate details:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to save intern certificate details.",
      error: error.message
    });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// Public certificate verification.
// The frontend passes a single identifier — either the InternDetails
// subdocument _id (this is what's printed on the certificate) or the intern's
// mobile number — and gets back the core certificate fields so it can confirm
// the certificate is genuine. No auth: this backs the public
// cert.appincoimbatore.com verification page.
//
// Accepts the identifier as a path param, a query param
// (?id= / ?value= / ?certId= / ?mobile=) or a JSON body field of the same names.
// ─────────────────────────────────────────────────────────────────────────────
const verifyInternCertificate = async (req, res) => {
  try {
    const raw = (
      req.params.identifier ||
      req.query.id || req.query.value || req.query.certId || req.query.mobile ||
      (req.body && (req.body.id || req.body.identifier || req.body.certId || req.body.mobile)) ||
      ""
    ).toString().trim();

    if (!raw) {
      return res.status(400).json({
        success: false,
        verified: false,
        message: "Certificate id or mobile number is required."
      });
    }

    let chat = null;
    let intern = null;

    // A real 24-hex ObjectId round-trips through the constructor; a 12-char
    // mobile number (e.g. 919xxxxxxxxx) passes isValid() but won't round-trip.
    const looksLikeObjectId =
      mongoose.Types.ObjectId.isValid(raw) &&
      String(new mongoose.Types.ObjectId(raw)) === raw;

    if (looksLikeObjectId) {
      chat = await Chat.findOne({ "InternDetails._id": raw });
      if (chat) {
        intern = chat.InternDetails.id(raw);
      } else {
        // Fallback: caller may have passed the Chat document id instead
        chat = await Chat.findById(raw);
        if (chat && Array.isArray(chat.InternDetails) && chat.InternDetails.length) {
          intern = chat.InternDetails[chat.InternDetails.length - 1];
        }
      }
    }

    if (!intern) {
      const digits = raw.replace(/\D/g, "");
      if (digits.length >= 10) {
        const fullNumber = cleanIndianMobileNumber(digits);
        const last10 = digits.slice(-10);
        chat = await Chat.findOne({
          $or: [
            { userNumber: fullNumber },
            { "InternDetails.phone": digits },
            { "InternDetails.phone": last10 },
            { "InternDetails.phone": { $regex: escapeRegex(last10) + "$" } }
          ]
        });
        if (chat && Array.isArray(chat.InternDetails) && chat.InternDetails.length) {
          intern = chat.InternDetails[chat.InternDetails.length - 1];
        }
      }
    }

    if (!chat || !intern) {
      return res.status(404).json({
        success: false,
        verified: false,
        message: "No certificate found for the provided details."
      });
    }

    return res.json({
      success: true,
      verified: true,
      certificateId: intern._id,
      data: {
        userName: intern.userName || chat.userName || "",
        reg_no: intern.reg_no || "",
        college: intern.college || "",
        internship: intern.internship || "",
        start_date: intern.start_date || null,
        end_date: intern.end_date || null
      }
    });
  } catch (error) {
    console.error("Error verifying certificate:", error);
    return res.status(500).json({
      success: false,
      verified: false,
      message: "Failed to verify certificate.",
      error: error.message
    });
  }
};

// app.get("/intern/certificate/verify/:identifier", verifyInternCertificate);
// app.get("/intern/certificate/verify", verifyInternCertificate);
app.post("/intern/certificate/verify", verifyInternCertificate);

app.post("/intern/certificate/generate", async (req, res) => {
  try {
    const { userNumber } = req.body;

    if (!userNumber) {
      return res.status(400).json({
        success: false,
        message: "userNumber is required."
      });
    }

    const fullNumber = cleanIndianMobileNumber(userNumber);
    if (!fullNumber || fullNumber.length !== 12) {
      return res.status(400).json({
        success: false,
        message: "Please send a valid Indian mobile number."
      });
    }

    const chat = await Chat.findOne({ userNumber: fullNumber }).lean();

    if (!chat) {
      return res.status(404).json({
        success: false,
        message: "No user found for this userNumber."
      });
    }

    const latestInternDetails = Array.isArray(chat.InternDetails) && chat.InternDetails.length
      ? chat.InternDetails[chat.InternDetails.length - 1]
      : null;

    if (!latestInternDetails) {
      return res.status(404).json({
        success: false,
        message: "No intern certificate details found for this user."
      });
    }

    // Delete old S3 certificates for this user before regenerating
    if (Array.isArray(chat.certificates) && chat.certificates.length) {
      await Promise.all(
        chat.certificates
          .filter(c => c && c.fileName)
          .map(c => deleteFromS3(c.fileName))
      );
    }

    const certificates = await generateCertificates({
      ...latestInternDetails,
      userNumber: chat.userNumber,
      userName: latestInternDetails.userName || chat.userName
    });

    const certificatesSaved = (certificates || []).map(cert => ({
      success: !!cert.success,
      fileName: cert.fileName || null,
      message: cert.message || null,
      s3: cert.s3 || null
    }));

    // Persist the generated certificates on the Chat document (best-effort)
    try {
      await Chat.findByIdAndUpdate(chat._id, { $set: { certificates: certificatesSaved } });
    } catch (err) {
      console.warn("Warning: could not save certificates to DB:", err && err.message ? err.message : err);
    }

    // Send acceptance certificate (first certificate) to intern email (best-effort)
    try {
      const acceptanceCert = certificates && certificates[0];
      const internEmail = latestInternDetails.userEmail;
      const company = latestInternDetails.company;
      if (acceptanceCert && acceptanceCert.success && acceptanceCert.buffer && internEmail) {
        const mailer = getCompanyMailer(company);
        if (mailer) {
          await mailer.transporter.sendMail({
            from: mailer.from,
            to: internEmail,
            subject: `Acceptance Certificate – ${company}`,
            html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;"><p>Dear ${latestInternDetails.salutation ? latestInternDetails.salutation + ' ' : ''}${latestInternDetails.userName},</p><p>We are pleased to inform you that you have been accepted as an intern at <strong>${company}</strong>. We were impressed with your interest and enthusiasm, and we're excited to have you onboard as part of our internship program.</p><p>Please find the Internship Acceptance Letter attached with this email for your reference. Kindly review the document carefully and feel free to reach out if you have any questions.</p><br/><p>Best regards,<br/>${company} Team</p></div>`,
            attachments: [
              {
                filename: "Acceptance_Certificate.pdf",
                content: acceptanceCert.buffer,
                contentType: "application/pdf"
              }
            ]
          });
        } else {
          console.warn(`No SMTP config found for company: "${company}"`);
        }
      }
    } catch (mailErr) {
      console.warn("Warning: could not send acceptance certificate email:", mailErr && mailErr.message ? mailErr.message : mailErr);
    }

    return res.status(200).json({
      success: true,
      message: "Certificates generated successfully.",
      userNumber: chat.userNumber,
      internDetails: latestInternDetails,
      certificates: certificatesSaved
    });
  } catch (error) {
    console.error("Error generating intern certificates:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to generate certificates.",
      error: error.message
    });
  }
});

// Combine all intern certificates into one PDF and return an S3 link for printing
app.post("/intern/certificates/print-all", async (req, res) => {
  try {
    const { userNumber } = req.body;

    if (!userNumber) {
      return res.status(400).json({ success: false, message: "userNumber is required." });
    }

    const fullNumber = cleanIndianMobileNumber(userNumber);
    if (!fullNumber || fullNumber.length !== 12) {
      return res.status(400).json({ success: false, message: "Please send a valid Indian mobile number." });
    }

    const chat = await Chat.findOne({ userNumber: fullNumber }).lean();
    if (!chat) {
      return res.status(404).json({ success: false, message: "No user found for this userNumber." });
    }

    const latestInternDetails = Array.isArray(chat.InternDetails) && chat.InternDetails.length
      ? chat.InternDetails[chat.InternDetails.length - 1]
      : null;

    if (!latestInternDetails) {
      return res.status(404).json({ success: false, message: "No intern certificate details found for this user." });
    }

    const result = await generateCombinedCertificate({
      ...latestInternDetails,
      userNumber: chat.userNumber,
      userName: latestInternDetails.userName || chat.userName
    });

    if (!result.url) {
      return res.status(500).json({ success: false, message: result.message || "Failed to upload combined certificate to S3." });
    }

    return res.status(200).json({
      success: true,
      message: "Combined certificate generated successfully.",
      url: result.url,
      fileName: result.fileName
    });
  } catch (error) {
    console.error("Error generating combined certificate:", error);
    return res.status(500).json({ success: false, message: "Failed to generate combined certificate.", error: error.message });
  }
});

// Generate a unique batch_code for an intern entry on a Chat document
app.post('/intern/:id/generate-batch', async (req, res) => {
  try {
    const { id } = req.params;
    const { internIndex } = req.body;

    if (!id) return res.status(400).json({ success: false, message: 'Chat id required in URL' });

    const chat = await Chat.findById(id);
    if (!chat) return res.status(404).json({ success: false, message: 'Chat not found' });

    if (!Array.isArray(chat.InternDetails) || chat.InternDetails.length === 0) {
      return res.status(404).json({ success: false, message: 'No intern details found for this chat' });
    }

    const idx = (internIndex !== undefined && internIndex !== null)
      ? parseInt(internIndex, 10)
      : (chat.InternDetails.length - 1);

    if (isNaN(idx) || idx < 0 || idx >= chat.InternDetails.length) {
      return res.status(400).json({ success: false, message: 'Invalid internIndex' });
    }

    const intern = chat.InternDetails[idx] || {};
    const collegeRaw = (intern.college || '').toString().trim();
    const collegeCode = (collegeRaw.replace(/[^A-Za-z]/g, '').slice(0, 3) || 'OTH').toUpperCase();

    const dateSrc = intern.start_date ? new Date(intern.start_date) : new Date();
    const mm = String(dateSrc.getMonth() + 1).padStart(2, '0');
    const yy = String(dateSrc.getFullYear()).slice(-2);

    // Short time suffix for uniqueness (HHMM)
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const min = String(now.getMinutes()).padStart(2, '0');
    const timeSuffix = `${hh}${min}`;

    const proposed = `BATCH-${collegeCode}-${mm}${yy}-${timeSuffix}`;

    const existing = await Chat.findOne({ 'InternDetails.batch_code': proposed });
    const finalCode = existing
      ? existing.InternDetails.find(i => i.batch_code === proposed).batch_code
      : proposed;

    chat.InternDetails[idx].batch_code = finalCode;
    chat.is_leader = true 
    await chat.save();

    // ✅ Fixed: was using undefined `candidate`, now using `finalCode`
    return res.json({ success: true, batch_code: finalCode, internIndex: idx, chatId: chat._id });
  } catch (err) {
    console.error('Error generating batch code:', err);
    return res.status(500).json({ success: false, message: 'Failed to generate batch code', error: err.message });
  }
});


// Save course completion certificate details (salutation, userName, course,
// issue_date, project_title, intern_title) for an already-enrolled lead.
app.post("/course/certificate", async (req, res) => {
  try {
    const { userNumber, salutation, userName, course, issue_date, project_title, intern_title } = req.body;

    if (!userNumber) {
      return res.status(400).json({
        success: false,
        message: "userNumber is required."
      });
    }

    const fullNumber = cleanIndianMobileNumber(userNumber);
    if (!fullNumber || fullNumber.length !== 12) {
      return res.status(400).json({
        success: false,
        message: "Please send a valid Indian mobile number."
      });
    }

    const chat = await Chat.findOne({ userNumber: fullNumber });
    if (!chat) {
      return res.status(404).json({
        success: false,
        message: "No user found for this userNumber."
      });
    }

    const courseCertDetails = {
      salutation: salutation?.trim() || "",
      userName: toTitleCase(userName) || "",
      course: course?.trim() || chat.course || "",
      issue_date: issue_date || null,
      project_title: project_title?.trim() || "",
      intern_title: intern_title?.trim() || ""
    };

    if (!Array.isArray(chat.CourseCertDetails)) {
      chat.CourseCertDetails = [];
    }
    chat.CourseCertDetails = [courseCertDetails];
    await chat.save();

    return res.status(201).json({
      success: true,
      message: "Course certificate details saved successfully.",
      chat
    });
  } catch (error) {
    console.error("Error saving course certificate details:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to save course certificate details.",
      error: error.message
    });
  }
});

app.post("/course/certificate/generate", async (req, res) => {
  try {
    const { userNumber } = req.body;

    if (!userNumber) {
      return res.status(400).json({
        success: false,
        message: "userNumber is required."
      });
    }

    const fullNumber = cleanIndianMobileNumber(userNumber);
    if (!fullNumber || fullNumber.length !== 12) {
      return res.status(400).json({
        success: false,
        message: "Please send a valid Indian mobile number."
      });
    }

    const chat = await Chat.findOne({ userNumber: fullNumber }).lean();

    if (!chat) {
      return res.status(404).json({
        success: false,
        message: "No user found for this userNumber."
      });
    }

    const latestCourseCertDetails = Array.isArray(chat.CourseCertDetails) && chat.CourseCertDetails.length
      ? chat.CourseCertDetails[chat.CourseCertDetails.length - 1]
      : null;

    if (!latestCourseCertDetails) {
      return res.status(404).json({
        success: false,
        message: "No course certificate details found for this user."
      });
    }

    // Delete old S3 certificates for this user before regenerating (the enrollment
    // certificate is a separate cert generated at enrollment time — leave it alone)
    const existingCourseCertificates = Array.isArray(chat.courseCertificates) ? chat.courseCertificates : [];
    const preservedEnrollmentCert = existingCourseCertificates.filter(c => c && c.type === 'enrollment');
    if (existingCourseCertificates.length) {
      await Promise.all(
        existingCourseCertificates
          .filter(c => c && c.fileName && c.type !== 'enrollment')
          .map(c => deleteFromS3(c.fileName))
      );
    }

    const certificates = await generateCourseCompletionCertificates({
      ...latestCourseCertDetails,
      _id: latestCourseCertDetails._id || chat._id,
      programType: chat.programType,
      userName: latestCourseCertDetails.userName || chat.userName
    });

    const certificatesSaved = (certificates || []).map(cert => ({
      success: !!cert.success,
      type: cert.type || null,
      fileName: cert.fileName || null,
      message: cert.message || null,
      s3: cert.s3 || null
    }));

    const nextCourseCertificates = [...preservedEnrollmentCert, ...certificatesSaved];

    // Persist the generated certificates on the Chat document (best-effort)
    try {
      await Chat.findByIdAndUpdate(chat._id, { $set: { courseCertificates: nextCourseCertificates } });
    } catch (err) {
      console.warn("Warning: could not save course certificates to DB:", err && err.message ? err.message : err);
    }

    return res.status(200).json({
      success: true,
      message: "Course certificates generated successfully.",
      userNumber: chat.userNumber,
      courseCertDetails: latestCourseCertDetails,
      certificates: nextCourseCertificates
    });
  } catch (error) {
    console.error("Error generating course certificates:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to generate course certificates.",
      error: error.message
    });
  }
});

// Combine all course completion certificates into one PDF and return an S3 link for printing
app.post("/course/certificates/print-all", async (req, res) => {
  try {
    const { userNumber } = req.body;

    if (!userNumber) {
      return res.status(400).json({ success: false, message: "userNumber is required." });
    }

    const fullNumber = cleanIndianMobileNumber(userNumber);
    if (!fullNumber || fullNumber.length !== 12) {
      return res.status(400).json({ success: false, message: "Please send a valid Indian mobile number." });
    }

    const chat = await Chat.findOne({ userNumber: fullNumber }).lean();
    if (!chat) {
      return res.status(404).json({ success: false, message: "No user found for this userNumber." });
    }

    const latestCourseCertDetails = Array.isArray(chat.CourseCertDetails) && chat.CourseCertDetails.length
      ? chat.CourseCertDetails[chat.CourseCertDetails.length - 1]
      : null;

    if (!latestCourseCertDetails) {
      return res.status(404).json({ success: false, message: "No course certificate details found for this user." });
    }

    const result = await generateCombinedCourseCompletionCertificate({
      ...latestCourseCertDetails,
      _id: latestCourseCertDetails._id || chat._id,
      programType: chat.programType,
      userName: latestCourseCertDetails.userName || chat.userName
    });

    if (!result.url) {
      return res.status(500).json({ success: false, message: result.message || "Failed to upload combined certificate to S3." });
    }

    return res.status(200).json({
      success: true,
      message: "Combined certificate generated successfully.",
      url: result.url,
      fileName: result.fileName
    });
  } catch (error) {
    console.error("Error generating combined course certificate:", error);
    return res.status(500).json({ success: false, message: "Failed to generate combined course certificate.", error: error.message });
  }
});


// Save ID card details (dob, address, idNo, classStart, validTill) for a lead.
app.post("/idcard/details", auth, async (req, res) => {
  try {
    const { _id, dob, address, idNo, classStart, validTill } = req.body;

    if (!_id) {
      return res.status(400).json({ success: false, message: "_id is required." });
    }

    const chat = await Chat.findById(_id);
    if (!chat) {
      return res.status(404).json({ success: false, message: "No lead found for this _id." });
    }

    const existing = Array.isArray(chat.IdCardDetails) && chat.IdCardDetails.length
      ? chat.IdCardDetails[chat.IdCardDetails.length - 1]
      : null;

    const classStartDate = classStart ? new Date(classStart) : null;
    let validTillDate = validTill ? new Date(validTill) : null;
    if (!validTillDate && classStartDate) {
      validTillDate = new Date(classStartDate);
      validTillDate.setMonth(validTillDate.getMonth() + 6);
    }

    const idCardDetails = {
      dob: dob || null,
      address: address?.trim() || "",
      idNo: idNo?.trim() || chat._id.toString(),
      classStart: classStartDate,
      validTill: validTillDate,
      photoUrl: existing?.photoUrl || "",
      photoKey: existing?.photoKey || "",
    };

    chat.IdCardDetails = [idCardDetails];
    await chat.save();

    return res.status(201).json({
      success: true,
      message: "ID card details saved successfully.",
      chat
    });
  } catch (error) {
    console.error("Error saving ID card details:", error);
    return res.status(500).json({ success: false, message: "Failed to save ID card details.", error: error.message });
  }
});

// Upload/replace the student photo used on the ID card.
app.post("/idcard/photo", auth, idCardPhotoUpload.single('photo'), async (req, res) => {
  try {
    const { leadId } = req.body;

    if (!leadId) {
      return res.status(400).json({ success: false, message: "leadId is required." });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, message: "photo file is required." });
    }

    const chat = await Chat.findById(leadId);
    if (!chat) {
      return res.status(404).json({ success: false, message: "No lead found for this leadId." });
    }

    const ext = (req.file.mimetype.split('/')[1] || 'jpg').replace('jpeg', 'jpg');
    const key = `idcards/photos/${leadId}_${Date.now()}.${ext}`;

    const s3Info = await uploadBufferToS3(req.file.buffer, key, req.file.mimetype);
    if (!s3Info.uploaded) {
      return res.status(500).json({ success: false, message: s3Info.error || "Failed to upload photo." });
    }

    if (!Array.isArray(chat.IdCardDetails) || !chat.IdCardDetails.length) {
      chat.IdCardDetails = [{}];
    }
    const latest = chat.IdCardDetails[chat.IdCardDetails.length - 1];
    const oldPhotoKey = latest.photoKey;
    latest.photoUrl = s3Info.url;
    latest.photoKey = key;
    await chat.save();

    if (oldPhotoKey) {
      await deleteFromS3(oldPhotoKey);
    }

    return res.status(200).json({ success: true, url: s3Info.url });
  } catch (error) {
    console.error("Error uploading ID card photo:", error);
    return res.status(500).json({ success: false, message: "Failed to upload photo.", error: error.message });
  }
});

// Generate the ID card PDF from the saved details and upload it to S3.
app.post("/idcard/generate", auth, async (req, res) => {
  try {
    const { _id } = req.body;

    if (!_id) {
      return res.status(400).json({ success: false, message: "_id is required." });
    }

    const chat = await Chat.findById(_id).lean();
    if (!chat) {
      return res.status(404).json({ success: false, message: "No lead found for this _id." });
    }

    const latestIdCardDetails = Array.isArray(chat.IdCardDetails) && chat.IdCardDetails.length
      ? chat.IdCardDetails[chat.IdCardDetails.length - 1]
      : null;

    if (!latestIdCardDetails) {
      return res.status(404).json({ success: false, message: "No ID card details found for this lead." });
    }

    const existingIdCards = Array.isArray(chat.idCards) ? chat.idCards : [];
    await Promise.all(
      existingIdCards.filter(c => c && c.fileName).map(c => deleteFromS3(c.fileName))
    );

    const idCard = await generateIdCard({
      ...latestIdCardDetails,
      _id: chat._id,
      userName: chat.userName,
      course: chat.course,
      mobile: chat.userNumber,
    });

    const idCardSaved = {
      success: !!idCard.success,
      fileName: idCard.fileName || null,
      message: idCard.message || null,
      s3: idCard.s3 || null,
    };

    await Chat.findByIdAndUpdate(chat._id, { $set: { idCards: [idCardSaved] } });

    return res.status(200).json({
      success: true,
      message: "ID card generated successfully.",
      idCard: idCardSaved,
    });
  } catch (error) {
    console.error("Error generating ID card:", error);
    return res.status(500).json({ success: false, message: "Failed to generate ID card.", error: error.message });
  }
});


// async function migrateChats() {
//   const session = await mongoose.startSession();
//   session.startTransaction();

//   try {
//     // 1. Fetch chats assigned to Abhi
//     const chats = await Chat.find({ assignedto: "Abhi" }).session(session);

//     if (!chats.length) {
//       console.log("No chats found for Abhi");
//       await session.abortTransaction();
//       session.endSession();
//       return;
//     }

//     console.log(`Found ${chats.length} chats assigned to Abhi`);

//     // 2. Backup to temp collection
//     const backupData = chats.map(chat => {
//       const obj = chat.toObject();
//       obj.originalId = obj._id; // optional tracking
//       obj.backupAt = new Date();
//       return obj;
//     });

//     await TempChat.insertMany(backupData, { session });

//     console.log("Backup completed");

//     // 3. Update assignedto → Shindu
//     const updateResult = await Chat.updateMany(
//       { assignedto: "Abhi" },
//       { $set: { assignedto: "Shindu" } },
//       { session }
//     );

//     console.log(`Updated ${updateResult.modifiedCount} chats`);

//     // 4. Commit transaction
//     await session.commitTransaction();
//     session.endSession();

//     console.log("Migration completed successfully");
//   } catch (error) {
//     console.error("Migration failed:", error);

//     await session.abortTransaction();
//     session.endSession();
//   }
// }


// migrateChats()


// ============================================
// 🧑‍🏫 Trainer Endpoints
// ============================================

// GET /trainers — all trainers with their name and existing batchIds
app.get("/trainers", async (req, res) => {
  try {
    const trainers = await TrainerModel.find({}, { username: 1, batchIds: 1, _id: 1 }).lean();
    return res.json({ success: true, trainers });
  } catch (err) {
    console.error("GET /trainers error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch trainers" });
  }
});

// "8 Hours" -> "8hr", "2 Hours" -> "2hr"
function getProgramTypeShortCode(programType) {
  const hours = (String(programType || "").match(/(\d+)/) || [])[1];
  return hours ? `${hours}hr` : "";
}

// Build a batch code like "Moha-PYT-8hr-2307-0930-1030"
// Trainer4 = first 4 letters of trainer name, Course3 = first 3 alpha letters of course name,
// ProgramType = e.g. "8hr"/"2hr", DDMM = batch date, HHMM-HHMM = start/end time (24h, no colon)
function buildBatchCode({ trainer, course, programType, dateSrc, startTime, endTime }) {
  const trainerRaw = (trainer || "").trim().replace(/\s+/g, "").slice(0, 4);
  const trainerCode = trainerRaw ? trainerRaw.charAt(0).toUpperCase() + trainerRaw.slice(1).toLowerCase() : "";
  const courseCode = (course || "").replace(/[^A-Za-z]/g, "").slice(0, 3).toUpperCase();
  const programCode = getProgramTypeShortCode(programType);
  const d = dateSrc ? new Date(dateSrc) : new Date();
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const startCode = (startTime || "").replace(":", "");
  const endCode = (endTime || "").replace(":", "");

  return [trainerCode, courseCode, programCode, `${dd}${mm}`, startCode, endCode].filter(Boolean).join("-");
}

// POST /chat/assign-batch — save Trainer, batchId, batch timing to a chat
// Body: { _id, Trainer, batch_date, batch_start_time, batch_end_time, course?, batchId? }
// If batchId is omitted, a new one is generated as TrainerCode-CourseCode-DDMM-StartTime-EndTime
// (falls back to the legacy TrainerName-DDMM-HHmmss format if start/end time aren't available)
app.post("/chat/assign-batch", async (req, res) => {
  try {
    const { _id, Trainer, batch_date, batchId, batch_start_time, batch_end_time, course, programType } = req.body;

    if (!_id) return res.status(400).json({ success: false, message: "_id is required" });
    if (!Trainer) return res.status(400).json({ success: false, message: "Trainer is required" });

    const chat = await Chat.findById(_id);
    if (!chat) return res.status(404).json({ success: false, message: "Chat not found" });

    let assignedBatchId = batchId;

    if (!assignedBatchId) {
      const startForCode = batch_start_time || chat.batch_start_time || "";
      const endForCode = batch_end_time || chat.batch_end_time || "";

      if (startForCode && endForCode) {
        assignedBatchId = buildBatchCode({
          trainer: Trainer,
          course: course || chat.course || "",
          programType: programType || chat.programType || "",
          dateSrc: batch_date || chat.batch_date,
          startTime: startForCode,
          endTime: endForCode,
        });
      } else {
        // Legacy fallback format, kept for when start/end time aren't provided
        const now = new Date();
        const dd = String(now.getDate()).padStart(2, "0");
        const mm = String(now.getMonth() + 1).padStart(2, "0");
        const hh = String(now.getHours()).padStart(2, "0");
        const ss = String(now.getSeconds()).padStart(2, "0");

        const trainerSlug = Trainer.trim().replace(/\s+/g, "");
        assignedBatchId = `${trainerSlug}-${dd}${mm}-${hh}${ss}`;
      }

      // Push generated batchId into Trainer document if found
      await TrainerModel.findOneAndUpdate(
        { username: new RegExp(`^${Trainer.trim()}$`, "i") },
        { $addToSet: { batchIds: assignedBatchId } }
      );
    }

    chat.batchId = assignedBatchId;
    chat.Trainer = Trainer.trim();
    if (batch_date !== undefined) chat.batch_date = batch_date;
    if (batch_start_time !== undefined) chat.batch_start_time = batch_start_time;
    if (batch_end_time !== undefined) chat.batch_end_time = batch_end_time;
    await chat.save();

    // Send WhatsApp notifications to student and trainer
    try {
      const trainerDoc = await TrainerModel.findOne(
        { username: new RegExp(`^${Trainer.trim()}$`, "i") },
        { mobile: 1 }
      ).lean();

      if (trainerDoc?.mobile) {
        await fetch(`${process.env.WP_CHAT_SERVICE_URL}/new/batch-assigned`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            studentNumber: chat.userNumber,
            studentName: chat.userName,
            trainerMobile: trainerDoc.mobile,
            trainerName: Trainer.trim(),
            batchId: assignedBatchId,
            batchDate: chat.batch_date,
            course: chat.course,
          }),
        });
        console.log("Batch assignment WA notifications dispatched");
      } else {
        console.warn("Trainer mobile not found — WA notifications skipped for trainer");
      }
    } catch (notifErr) {
      // Non-fatal: log and continue
      console.error("Batch WA notification failed:", notifErr.message);
    }

    return res.json({
      success: true,
      batchId: assignedBatchId,
      Trainer: chat.Trainer,
      batch_start_time: chat.batch_start_time,
      batch_end_time: chat.batch_end_time,
      chat,
    });
  } catch (err) {
    console.error("POST /chat/assign-batch error:", err);
    return res.status(500).json({ success: false, message: "Failed to assign batch" });
  }
});

// Update date_scheduled for the latest InternDetails entry
app.put('/leads/:id/date-scheduled', async (req, res) => {
  try {
    const { id } = req.params;
    const { date_scheduled } = req.body;

    const chat = await Chat.findByIdAndUpdate(
      id,
      { $set: { date_scheduled: date_scheduled ? new Date(date_scheduled) : null } },
      { new: true, strict: false }
    );
    if (!chat) return res.status(404).json({ success: false, message: 'Lead not found' });

    // Propagate date_scheduled to all leads in the same batch
    const dateValue = date_scheduled ? new Date(date_scheduled) : null;
    const batchCode = chat.InternDetails?.[0]?.batch_code?.trim();

    if (chat.batchId) {
      await Chat.updateMany(
        { batchId: chat.batchId, _id: { $ne: chat._id } },
        { $set: { date_scheduled: dateValue } },
        { strict: false }
      );
    } else if (batchCode) {
      await Chat.updateMany(
        { "InternDetails.batch_code": batchCode, _id: { $ne: chat._id } },
        { $set: { date_scheduled: dateValue } },
        { strict: false }
      );
    }

    // fetch fresh so date_scheduled is always present in response
    const fresh = await Chat.findById(chat._id).lean();
    return res.json({ success: true, lead: fresh });
  } catch (err) {
    console.error('PUT /leads/:id/date-scheduled error:', err);
    return res.status(500).json({ success: false, message: 'Failed to update date scheduled' });
  }
});

const nodemailer = require("nodemailer");

const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 465,
  secure: true,
  auth: {
    user: "sharan@etherservices.com",
    pass: "clup sprd frdb vlck"
  }
});

const COMPANY_SMTP_CONFIG = {
  etherservices: {
    user: "mohan@etherservices.com",
    pass: "nlwp czuc sriq tbdk",
    from: "mohan@etherservices.com"
  },
  appintechnology:{
    user: "shindu@appincoimbatore.com",
    pass: "mjix bxsp rawz vvqm",
    from: "shindu@appincoimbatore.com"
  }
};

function getCompanyMailer(company) {
  const key = (company || "").trim().toLowerCase().replace(/\s+/g, "");
  const config = COMPANY_SMTP_CONFIG[key];
  if (!config) return null;
  return {
    transporter: nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 465,
      secure: true,
      auth: { user: config.user, pass: config.pass }
    }),
    from: config.from
  };
}


// app.post("/email/demobooking", async (req, res) => {
//   try {
//     const { to, subject, html, pdfUrl } = req.body;

//     await transporter.sendMail({
//       from: "sharan@etherservices.com",
//       to,
//       subject: subject || "Demo Booking Confirmation",
//       html: html || "<h1>Hello</h1>",
//       attachments: pdfUrl
//         ? [
//             {
//               filename: "certificate.pdf",
//               path: pdfUrl
//             }
//           ]
//         : []
//     });

//     return res.json({
//       success: true,
//       message: "Email sent successfully"
//     });

//   } catch (err) {
//     console.error(err);
//     return res.status(500).json({
//       success: false,
//       error: err.message
//     });
//   }
// });


// --- Staff Management (admin only) ---

app.get("/staff", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const staff = await User.find({}).sort({ username: 1 }).select('-__v');
    res.json({ success: true, staff });
  } catch (err) {
    res.status(500).json({ success: false, message: "Failed to fetch staff" });
  }
});

app.post("/staff", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const { username, useremail, userNumber, password, role, managerReviewEnabled } = req.body;
    if (!username || !useremail || !userNumber || !password) {
      return res.status(400).json({ success: false, message: "username, useremail, userNumber, and password are required" });
    }
    const user = new User({
      username,
      useremail,
      userNumber,
      password,
      role: role || 'user',
      ...(managerReviewEnabled !== undefined ? { managerReviewEnabled: !!managerReviewEnabled } : {})
    });
    await user.save();
    res.status(201).json({ success: true, user });
  } catch (err) {
    if (err.code === 11000) {
      const field = Object.keys(err.keyPattern || {})[0] || 'field';
      return res.status(409).json({ success: false, message: `A user with that ${field} already exists` });
    }
    res.status(500).json({ success: false, message: "Failed to create user" });
  }
});

app.put("/staff/:id", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    const { username, useremail, userNumber, password, role, managerReviewEnabled } = req.body;
    const update = {};
    if (username !== undefined) update.username = username;
    if (useremail !== undefined) update.useremail = useremail;
    if (userNumber !== undefined) update.userNumber = userNumber;
    if (password !== undefined && password !== '') update.password = password;
    if (role !== undefined) update.role = role;
    if (managerReviewEnabled !== undefined) update.managerReviewEnabled = !!managerReviewEnabled;
    const user = await User.findByIdAndUpdate(req.params.id, { $set: update }, { new: true, runValidators: true }).select('-__v');
    if (!user) return res.status(404).json({ success: false, message: "User not found" });
    res.json({ success: true, user });
  } catch (err) {
    if (err.code === 11000) {
      const field = Object.keys(err.keyPattern || {})[0] || 'field';
      return res.status(409).json({ success: false, message: `A user with that ${field} already exists` });
    }
    res.status(500).json({ success: false, message: "Failed to update user" });
  }
});

app.delete("/staff/:id", auth, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ message: "Admin access required" });
  try {
    if (req.user._id === req.params.id || req.user.id === req.params.id) {
      return res.status(400).json({ success: false, message: "You cannot delete your own account" });
    }
    const user = await User.findByIdAndDelete(req.params.id);
    if (!user) return res.status(404).json({ success: false, message: "User not found" });
    res.json({ success: true, message: "User deleted successfully" });
  } catch (err) {
    res.status(500).json({ success: false, message: "Failed to delete user" });
  }
});

const jobsRouter = require('./jobs');
app.use(jobsRouter);


// Counsellors (User docs) whose mobile is in this list are left OUT of the
// daily / weekly / monthly performance reports below. Internal/test accounts only.
// Numbers are stored on the User doc as 91XXXXXXXXXX.
const EXCLUDED_REPORT_NUMBERS = ["918667850844", "917397081629", "917708171533"];

// Resolve the excluded counsellor mobile numbers to their usernames. Used by the
// daily report, which aggregates Chat docs by `assignedto` (a username string).
// User.userNumber is a free-form string (may be stored as +91.., 10-digit, 0-prefixed,
// etc.), so normalise both sides with cleanIndianMobileNumber rather than an exact
// `$in` match — otherwise a formatting difference lets an excluded user slip into the report.
async function getExcludedReportUsernames() {
  const excludedNormalised = new Set(
    EXCLUDED_REPORT_NUMBERS.map(cleanIndianMobileNumber).filter(Boolean)
  );
  const users = await User.find({}).select("username userNumber").lean();
  return users
    .filter(u => excludedNormalised.has(cleanIndianMobileNumber(u.userNumber)))
    .map(u => u.username)
    .filter(Boolean);
}

// Daily WhatsApp report sent at 6 PM IST to admin number
cron.schedule("0 22 * * *", async () => {
  try {
    console.log("📊 Running daily WhatsApp report cron...");

    const now = new Date();
    // now.setDate(now.getDate() - 1); For previous day
    const start = new Date(now);
    start.setHours(0, 0, 0, 0);
    const end = new Date(now);
    end.setHours(23, 59, 59, 999);

    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);

    const excludedUsernames = await getExcludedReportUsernames();

    const [report, monthlyAgg, leadsThisMonth] = await Promise.all([
      Chat.aggregate([
      { $match: { assignedto: { $exists: true, $ne: null, $nin: excludedUsernames } } },
      {
        $addFields: {
          isNewLeadOnDate: {
            $and: [
              { $gte: ["$datecreated", start] },
              { $lte: ["$datecreated", end] },
              {
                $not: {
                  $and: [
                    { $eq: [{ $toLower: { $ifNull: ["$pipeline", ""] } }, "enrolled"] },
                    { $eq: [{ $toLower: { $ifNull: ["$course", ""] } }, "college internship"] }
                  ]
                }
              }
            ]
          }
        }
      },
      {
        $addFields: {
          dateRemarks: {
            $filter: {
              input: { $ifNull: ["$remarks", []] },
              as: "remark",
              cond: {
                $and: [
                  { $gte: ["$$remark.timestamp", start] },
                  { $lte: ["$$remark.timestamp", end] }
                ]
              }
            }
          }
        }
      },
      {
        $addFields: {
          paidOnDateInstallments: {
            $filter: {
              input: { $ifNull: ["$fees.installments", []] },
              as: "inst",
              cond: {
                $and: [
                  { $eq: ["$$inst.status", "Paid"] },
                  { $gte: ["$$inst.dueDate", start] },
                  { $lte: ["$$inst.dueDate", end] }
                ]
              }
            }
          },
          firstInstDueDate: {
            $let: {
              vars: { first: { $arrayElemAt: ["$fees.installments", 0] } },
              in: { $ifNull: ["$$first.dueDate", null] }
            }
          },
          reminderOnDateFlag: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$reminder", false] },
                  { $gte: ["$reminder", start] },
                  { $lte: ["$reminder", end] }
                ]
              },
              1, 0
            ]
          }
        }
      },
      {
        $addFields: {
          paidOnDateAmount: {
            $sum: {
              $map: {
                input: "$paidOnDateInstallments",
                as: "inst",
                in: { $ifNull: ["$$inst.amount", 0] }
              }
            }
          },
          billedOnDate: {
            $and: [
              { $ifNull: ["$firstInstDueDate", false] },
              { $gte: ["$firstInstDueDate", start] },
              { $lte: ["$firstInstDueDate", end] }
            ]
          },
          billedOnDateAmount: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$firstInstDueDate", false] },
                  { $gte: ["$firstInstDueDate", start] },
                  { $lte: ["$firstInstDueDate", end] }
                ]
              },
              { $ifNull: ["$fees.billedAmount", 0] },
              0
            ]
          }
        }
      },
      {
        $group: {
          _id: "$assignedto",
          newLeadsToday: { $sum: { $cond: ["$isNewLeadOnDate", 1, 0] } },
          // New leads with no remarks today (not contacted)
          newLeadsMissed: {
            $sum: {
              $cond: [
                { $and: ["$isNewLeadOnDate", { $eq: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          },
          // Remarks on old leads today
          oldRemarksCount: {
            $sum: {
              $cond: [
                { $not: "$isNewLeadOnDate" },
                { $size: "$dateRemarks" },
                0
              ]
            }
          },
          // Remarks on new leads today
          newRemarksCount: {
            $sum: {
              $cond: [
                "$isNewLeadOnDate",
                { $size: "$dateRemarks" },
                0
              ]
            }
          },
          feesUpdatedToday: { $sum: "$paidOnDateAmount" },
          billedTodayTotal: { $sum: "$billedOnDateAmount" },
          // Reminders today where lead was contacted (has remarks today)
          remindersDone: {
            $sum: {
              $cond: [
                { $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $gt: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          },
          // Reminders today where lead was NOT contacted (no remarks today)
          remindersMissed: {
            $sum: {
              $cond: [
                { $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $eq: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          username: "$_id",
          newLeadsToday: 1,
          newLeadsMissed: 1,
          oldRemarksCount: 1,
          newRemarksCount: 1,
          feesUpdatedToday: 1,
          billedTodayTotal: 1,
          remindersDone: 1,
          remindersMissed: 1
        }
      },
      { $sort: { username: 1 } }
      ]),
      Chat.aggregate([
        { $match: { pipeline: { $in: CONVERTED_STAGES }, assignedto: { $nin: excludedUsernames } } },
        {
          $project: {
            firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] },
            billedAmount: { $ifNull: ["$fees.billedAmount", 0] },
            paidInstallments: { $ifNull: ["$fees.installments", []] }
          }
        },
        {
          $addFields: {
            billedThisMonth: {
              $cond: [
                {
                  $and: [
                    { $ifNull: ["$firstInstDueDate", false] },
                    { $gte: ["$firstInstDueDate", monthStart] },
                    { $lte: ["$firstInstDueDate", monthEnd] }
                  ]
                },
                "$billedAmount",
                0
              ]
            },
            paidThisMonth: {
              $sum: {
                $map: {
                  input: "$paidInstallments",
                  as: "inst",
                  in: {
                    $cond: [
                      {
                        $and: [
                          { $eq: ["$$inst.status", "Paid"] },
                          { $ifNull: ["$$inst.dueDate", false] },
                          { $gte: ["$$inst.dueDate", monthStart] },
                          { $lte: ["$$inst.dueDate", monthEnd] }
                        ]
                      },
                      { $ifNull: ["$$inst.amount", 0] },
                      0
                    ]
                  }
                }
              }
            }
          }
        },
        {
          $group: {
            _id: null,
            totalBilledMonth: { $sum: "$billedThisMonth" },
            totalPaidMonth: { $sum: "$paidThisMonth" }
          }
        }
      ]),
      Chat.countDocuments({
        datecreated: { $gte: monthStart, $lte: monthEnd },
        assignedto: { $nin: excludedUsernames },
        $nor: [{ pipeline: { $regex: CONVERTED_RE }, course: { $regex: /^college internship$/i } }]
      })
    ]);

    const dateStr = now.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });

    let totalBilled = 0;
    let totalPaid = 0;
    let msg = `📊 *Daily Report - ${dateStr}*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━\n\n`;

    for (const staff of report) {
      totalBilled += staff.billedTodayTotal || 0;
      totalPaid += staff.feesUpdatedToday || 0;

      const billed = (staff.billedTodayTotal || 0).toLocaleString("en-IN");
      const paid = (staff.feesUpdatedToday || 0).toLocaleString("en-IN");

      msg += `👤 *${staff.username}*\n`;
      msg += `NEW: ${staff.newLeadsToday}  |  Missed: ${staff.newLeadsMissed}\n`;
      msg += `REMARKS:\n  old: ${staff.oldRemarksCount} , new: ${staff.newRemarksCount}\n`;
      msg += `BILLED: ₹${billed}  |  PAID: ₹${paid}\n`;
      msg += `REMINDERS Marked Today:\n  Done: ${staff.remindersDone}  |  Missed: ${staff.remindersMissed}\n\n`;
    }

    const totalNewLeadsToday = report.reduce((sum, s) => sum + (s.newLeadsToday || 0), 0);
    const monthBilled = monthlyAgg[0]?.totalBilledMonth || 0;
    const monthPaid = monthlyAgg[0]?.totalPaidMonth || 0;
    const monthName = now.toLocaleDateString("en-IN", { month: "long" });

    msg += `━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `💰 *FEES BILLED TODAY: ₹${totalBilled.toLocaleString("en-IN")}*\n`;
    msg += `✅ *FEES PAID TODAY: ₹${totalPaid.toLocaleString("en-IN")}*\n\n`;
    msg += `🆕 New Leads Today: *${totalNewLeadsToday}*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `📅 *${monthName.toUpperCase()} SUMMARY*\n`;
    msg += `💵 Total Billed This Month: *₹${monthBilled.toLocaleString("en-IN")}*\n`;
    msg += `✅ Total Paid This Month: *₹${monthPaid.toLocaleString("en-IN")}*\n\n`;
    msg += `📋 Leads This Month: *${leadsThisMonth}*`;

    const wpUrl = process.env.WP_CHAT_SERVICE_URL;
    if (!wpUrl) {
      console.error("❌ WP_CHAT_SERVICE_URL not set — cannot send daily report");
      return;
    }

    await fetch(`${wpUrl}/internal/send-text`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to: "919597271533", message: msg })
    });

    console.log("✅ Daily WhatsApp report sent successfully");
  } catch (err) {
    console.error("❌ Daily report cron error:", err);
  }
}, {
  timezone: "Asia/Kolkata"
});

// ─── Automatic counsellor reassignment for neglected leads ────────────────
// Shared by the two crons below (no-remark and stale-pipeline). Picks the
// eligible counsellor (role "user", not in `auto_assign_excluded_staffs`, and
// not the current owner) carrying the fewest active leads, then transfers the
// lead there via findByIdAndUpdate so the existing NotifyCounsellor hook fires.

// "Looking for job" leads are handed off to HR and are expected to sit there —
// treat that stage as terminal too, same as Enrolled/Not Interested, so the
// stale-pipeline cron never reclaims them back to a counsellor.
const TERMINAL_PIPELINES = [...CONVERTED_STAGES, "Looking for job", "On Hold", /^not interested$/i, /^not intrested$/i];

async function getReassignmentTarget(currentAssignee) {
  const collection = mongoose.connection.db.collection('configs');
  const excludedConfig = await collection.findOne({ key: 'auto_assign_excluded_staffs' });
  const excludedStaffs = excludedConfig ? excludedConfig.value : [];

  const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 }).select('username');
  const eligible = allStaffs
    .map(s => s.username)
    .filter(u => u && !excludedStaffs.includes(u) && u !== currentAssignee);
  if (eligible.length === 0) return null;

  // Balance by current active-lead count so transfers go to the least-loaded counsellor.
  const counts = await Chat.aggregate([
    { $match: { assignedto: { $in: eligible }, pipeline: { $nin: TERMINAL_PIPELINES } } },
    { $group: { _id: "$assignedto", c: { $sum: 1 } } },
  ]);
  const countMap = Object.fromEntries(counts.map(x => [x._id, x.c]));
  eligible.sort((a, b) => (countMap[a] || 0) - (countMap[b] || 0));
  return eligible[0];
}

async function autoReassignLead(lead, reason, extraSet = {}) {
  const target = await getReassignmentTarget(lead.assignedto);
  if (!target) {
    console.warn(`⚠️ Auto-reassign skipped for ${lead.userNumber || lead._id} — no eligible counsellor`);
    return null;
  }
  await Chat.findByIdAndUpdate(lead._id, {
    $set: { assignedto: target, ...extraSet },
    $push: { autoReassignHistory: { from: lead.assignedto || null, to: target, reason, at: new Date() } },
  });
  await sendWhatsAppText(
    MANAGER_ADMIN_NUMBER,
    `🔁 *Lead auto-reassigned*\n\n` +
    `👤 ${lead.userName || "Unknown"}\n` +
    `📞 ${lead.userNumber || "-"}\n` +
    `🧑‍💼 ${lead.assignedto || "Unassigned"} → *${target}*\n` +
    `📋 ${reason}`
  );
  console.log(`🔁 Auto-reassigned ${lead.userNumber || lead._id}: ${lead.assignedto || "Unassigned"} → ${target} (${reason})`);
  return target;
}

cron.schedule("*/10 * * * *", async () => {
  try {
    // Only escalate during office hours (Mon–Sat, 09:30–18:30 IST)
    if (!isWithinOfficeHours(new Date())) return;

    const now = Date.now();
    const THIRTY_MIN = 30 * 60 * 1000;

    const windowStart = new Date(now - 4 * 60 * 60 * 1000);

    const leads = await Chat.find({
      datecreated: { $gte: windowStart },
      // Some leads enroll directly without any remark — never escalate/nag the admin for those.
      // pipeline: { $not: { $regex: CONVERTED_RE } },
      pipeline: {$nin: [/^pre-enrollment$/i, /^advance payment$/i, /^on hold$/i, CONVERTED_RE]},
      $and: [
        { $or: [{ noRemarkEscalationCount: { $exists: false } }, { noRemarkEscalationCount: { $lt: 3 } }] },
        { $or: [{ remarks: { $exists: false } }, { remarks: { $size: 0 } }] },
      ],
    }).select("userName userNumber course courseofintrest assignedto datecreated noRemarkEscalationCount lastNoRemarkEscalationAt");

    for (const lead of leads) {
      const since = lead.lastNoRemarkEscalationAt
        ? new Date(lead.lastNoRemarkEscalationAt).getTime()
        : new Date(lead.datecreated).getTime();
      if (now - since < THIRTY_MIN) continue;

      const attempt = (lead.noRemarkEscalationCount || 0) + 1;
      const minsOld = Math.round((now - new Date(lead.datecreated).getTime()) / 60000);
      const msg =
        `🚨 *No remark logged for a new lead*\n\n` +
        `👤 ${lead.userName || "Unknown"}\n` +
        `📞 ${lead.userNumber || "-"}\n` +
        `🎓 ${lead.course || lead.courseofintrest || "Not specified"}\n` +
        `🧑‍💼 Assigned to: ${lead.assignedto || "Unassigned"}\n` +
        `🕒 Created ${minsOld} min ago — still no remark\n\n` +
        `Escalation ${attempt}/3`;

      await sendWhatsAppText(MANAGER_ADMIN_NUMBER, msg);
      await Chat.updateOne(
        { _id: lead._id },
        { $inc: { noRemarkEscalationCount: 1 }, $set: { lastNoRemarkEscalationAt: new Date() } }
      );
    }

    // ── After the 3rd escalation with still no remark → transfer to another counsellor ──
    const noRemarkReassignCandidates = await Chat.find({
      datecreated: { $gte: new Date(now - 7 * 24 * 60 * 60 * 1000) },
      // Don't transfer a lead that has already enrolled, or been marked Not Interested
      // (direct enrollment/decline without remarks).
      pipeline: { $nin: [CONVERTED_RE, /^on hold$/i, /^not interested$/i, /^not intrested$/i] },
      noRemarkEscalationCount: { $gte: 3 },
      noRemarkReassigned: { $ne: true },
      $or: [{ remarks: { $exists: false } }, { remarks: { $size: 0 } }],
    }).select("userName userNumber assignedto");

    for (const lead of noRemarkReassignCandidates) {
      await autoReassignLead(
        lead,
        "No remark logged even after 3 escalations",
        { noRemarkReassigned: true }
      );
    }
  } catch (err) {
    console.error("❌ No-remark escalation cron error:", err);
  }
}, {
  timezone: "Asia/Kolkata"
});

// ─── Stale-pipeline auto-reassignment — once daily, 11:00 IST (Mon–Sat) ────
// If a lead's pipeline stage has not changed for 30+ days AND it has no active
// (future) reminder, its counsellor is effectively sitting on it — transfer it
// to another counsellor. Terminal stages (Enrolled / Not Interested) are left
// alone. A lead is not re-transferred for this reason until it has been stale
// for a further 30 days.
cron.schedule("0 11 * * 1-6", async () => {
  try {
    const now = new Date();
    const cutoffStale = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

    const staleLeads = await Chat.find({
      pipelineChangedAt: { $lte: cutoffStale },
      assignedto: { $nin: [null, ""] },
      pipeline: { $nin: TERMINAL_PIPELINES },
      $and: [
        { $or: [
          { reminder: null },
          { reminder: { $exists: false } },
          { reminder: { $lte: now } },
        ] },
        { $or: [
          { lastStalePipelineReassignAt: { $exists: false } },
          { lastStalePipelineReassignAt: null },
          { lastStalePipelineReassignAt: { $lte: cutoffStale } },
        ] },
      ],
    }).select("userName userNumber assignedto pipeline pipelineChangedAt");

    if (!staleLeads.length) return;
    console.log(`🕰️ Stale-pipeline cron: ${staleLeads.length} lead(s) to reassign`);

    for (const lead of staleLeads) {
      const daysStale = Math.round((now.getTime() - new Date(lead.pipelineChangedAt).getTime()) / 86400000);
      await autoReassignLead(
        lead,
        `Pipeline "${lead.pipeline || "New Lead"}" unchanged ${daysStale} days with no active reminder`,
        { lastStalePipelineReassignAt: now }
      );
    }
  } catch (err) {
    console.error("❌ Stale-pipeline reassignment cron error:", err);
  }
}, {
  timezone: "Asia/Kolkata"
});

// Fetch last 100 group chat messages (newest first → reversed for display)
app.get('/group-chat/messages', auth, async (req, res) => {
  try {
    const messages = await GroupMessage.find().sort({ timestamp: -1 }).limit(100);
    res.json({ success: true, messages: messages.reverse() });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to fetch messages' });
  }
});


// ─── Daily WhatsApp "Manager" Check-in + Admin RAG chatbot ────────────────
// Both AI-manager features (the 5 PM counsellor check-in and the
// MANAGER_ADMIN_NUMBER RAG chatbot) now live in ManagerChat.js, sharing one
// RAG store. registerManagerChatRoutes(app, MANAGER_ADMIN_NUMBER) — called
// near the top of this file — mounts /manager/chat/incoming,
// /manager/checkin/reply, /manager/rag/reindex, /manager/rag/status and
// schedules every manager cron (5 PM check-in, minute nudge sweep, 6 PM
// consolidated report, 8 PM no-response sweep, 1:30 AM nightly RAG ingest).

// 0 9 * * 1
// Weekly performance report — Monday 9 AM IST, sent to each counsellor's mobile
cron.schedule("30 23 * * 0", async () => {
  try {
    console.log("📊 Running weekly WhatsApp report cron...");

    const now = new Date();

    // Past week: Mon–Sun ending yesterday
    const weekEnd = new Date(now);
    weekEnd.setDate(weekEnd.getDate() - 1);
    weekEnd.setHours(23, 59, 59, 999);

    const weekStart = new Date(weekEnd);
    weekStart.setDate(weekStart.getDate() - 6);
    weekStart.setHours(0, 0, 0, 0);

    // Week before that (for comparison)
    const prevWeekEnd = new Date(weekStart);
    prevWeekEnd.setDate(prevWeekEnd.getDate() - 1);
    prevWeekEnd.setHours(23, 59, 59, 999);

    const prevWeekStart = new Date(prevWeekEnd);
    prevWeekStart.setDate(prevWeekStart.getDate() - 6);
    prevWeekStart.setHours(0, 0, 0, 0);

    // Next week: the week ahead of this report (for "upcoming payments")
    const nextWeekStart = new Date(weekEnd);
    nextWeekStart.setDate(nextWeekStart.getDate() + 1);
    nextWeekStart.setHours(0, 0, 0, 0);

    const nextWeekEnd = new Date(nextWeekStart);
    nextWeekEnd.setDate(nextWeekEnd.getDate() + 6);
    nextWeekEnd.setHours(23, 59, 59, 999);

    const wpUrl = process.env.WP_CHAT_SERVICE_URL;
    if (!wpUrl) {
      console.error("❌ WP_CHAT_SERVICE_URL not set — cannot send weekly report");
      return;
    }

    const counsellors = await User.find({
      role: "user",
      userNumber: { $nin: EXCLUDED_REPORT_NUMBERS }
    }).lean();

    // Billed = sum of billedAmount for leads whose 1st installment dueDate falls in [start, end].
    // Paid   = sum of installment amounts with status="Paid" and dueDate in [start, end].
    const feesAgg = async (username, start, end) => {
      const result = await Chat.aggregate([
        {
          $match: {
            assignedto: username,
            pipeline: { $in: CONVERTED_STAGES }
          }
        },
        {
          $facet: {
            billed: [
              {
                $addFields: {
                  firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] }
                }
              },
              {
                $match: {
                  firstInstDueDate: { $gte: start, $lte: end }
                }
              },
              {
                $group: {
                  _id: null,
                  totalBilled: { $sum: { $ifNull: ["$fees.billedAmount", 0] } }
                }
              }
            ],
            paid: [
              { $unwind: { path: "$fees.installments", preserveNullAndEmptyArrays: false } },
              {
                $match: {
                  "fees.installments.status": "Paid",
                  "fees.installments.dueDate": { $gte: start, $lte: end }
                }
              },
              {
                $group: {
                  _id: null,
                  totalPaid: { $sum: { $ifNull: ["$fees.installments.amount", 0] } }
                }
              }
            ]
          }
        }
      ]);
      return {
        totalBilled: result[0]?.billed?.[0]?.totalBilled || 0,
        totalPaid: result[0]?.paid?.[0]?.totalPaid || 0
      };
    };

    for (const counsellor of counsellors) {
      try {
        const username = counsellor.username;
        const mobile = cleanIndianMobileNumber(counsellor.userNumber);
        if (!mobile) {
          console.warn(`⚠️ No mobile for counsellor ${username} — skipping`);
          continue;
        }

        // New leads this week (exclude college internship enrolled)
        const newLeadsThisWeek = await Chat.find({
          assignedto: username,
          datecreated: { $gte: weekStart, $lte: weekEnd },
          $nor: [
            { pipeline: { $regex: CONVERTED_RE }, course: { $regex: /^college internship$/i } }
          ]
        }).select("userName userNumber datecreated remarks conversations course pipeline").lean();

        // 1. Response time: office-hours (Mon–Sat, 9:30am–6:30pm IST) time between
        //    lead creation and first remark. Leads that arrive outside office hours
        //    also get an off-hours "extra effort" figure — how many off-hours
        //    minutes elapsed before first contact, to credit responsiveness beyond
        //    working hours without skewing the in-office average.
        const responseTimes = [];
        const offHoursResponseTimes = [];
        let noContactCount = 0;
        let offHoursLeadsCount = 0;
        for (const lead of newLeadsThisWeek) {
          const sorted = (lead.remarks || [])
            .filter(r => r.timestamp)
            .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
          const createdAt = new Date(lead.datecreated);
          const arrivedOffHours = !isWithinOfficeHours(createdAt);
          if (arrivedOffHours) offHoursLeadsCount++;
          if (sorted.length > 0) {
            const firstContact = new Date(sorted[0].timestamp);
            if (firstContact >= createdAt) {
              responseTimes.push(businessMinutesBetween(createdAt, firstContact));
              if (arrivedOffHours) {
                offHoursResponseTimes.push(offHoursMinutesBetween(createdAt, firstContact));
              }
            }
          } else {
            noContactCount++;
          }
        }
        const avgResponseMins = responseTimes.length > 0
          ? Math.round(responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length)
          : null;
        const avgOffHoursResponseMins = offHoursResponseTimes.length > 0
          ? Math.round(offHoursResponseTimes.reduce((a, b) => a + b, 0) / offHoursResponseTimes.length)
          : null;
        const offHoursContactedCount = offHoursResponseTimes.length;
        const slowResponses = responseTimes.filter(t => t > 60).length;
        const notContactedPct = newLeadsThisWeek.length > 0
          ? Math.round((noContactCount / newLeadsThisWeek.length) * 100)
          : 0;
        const slowBuckets = {
          "1h-2h":   responseTimes.filter(t => t > 60  && t <= 120).length,
          "2h-3h":   responseTimes.filter(t => t > 120 && t <= 180).length,
          "3h-4h":   responseTimes.filter(t => t > 180 && t <= 240).length,
          "4h-5h":   responseTimes.filter(t => t > 240 && t <= 300).length,
          "above5h": responseTimes.filter(t => t > 300).length,
        };

        // 2. Enrolled this week (excluding college internship)
        const enrolledThisWeek = await Chat.countDocuments({
          assignedto: username,
          pipeline: { $regex: CONVERTED_RE },
          course: { $not: { $regex: /^college internship$/i } },
          datecreated: { $gte: weekStart, $lte: weekEnd }
        });

        // 3. Fees this week vs previous week
        const [thisWeekFees, prevWeekFees] = await Promise.all([
          feesAgg(username, weekStart, weekEnd),
          feesAgg(username, prevWeekStart, prevWeekEnd)
        ]);

        // 4b. Active non-enrolled leads with recent interaction (last 30 days)
        const thirtyDaysAgo = new Date(now);
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

        const activeNotEnrolled = await Chat.find({
          assignedto: username,
          pipeline: { $not: { $regex: CONVERTED_RE } },
          lastInteracted: { $gte: thirtyDaysAgo },
          $nor: [
            { course: { $regex: /^college internship$/i } }
          ]
        }).select("userName userNumber status pipeline course remarks conversations lastInteracted").lean();

        // Hot/Warm leads not yet moved to Enrolled pipeline
        const hotNotEnrolled = activeNotEnrolled.filter(l =>
          /^(hot|warm)$/i.test(l.status || "")
        );

        // Leads who responded after a follow-up during this report week (non-enrolled)
        const respondedAfterFollowUpLeads = await Chat.find({
          assignedto: username,
          respondedAfterFollowUp: true,
          lastInteracted: { $gte: weekStart, $lte: weekEnd },
          pipeline: { $not: { $regex: CONVERTED_RE } }
        }).select("userName course status pipeline").lean();

        // 4c. Upcoming payments — pending installments due next week
        const upcomingPaymentLeads = await Chat.find({
          assignedto: username,
          pipeline: { $in: CONVERTED_STAGES },
          "fees.installments": {
            $elemMatch: { status: "Pending", dueDate: { $gte: nextWeekStart, $lte: nextWeekEnd } }
          }
        }).select("userName course fees.installments").lean();

        const upcomingInstallments = [];
        let upcomingPaymentsTotal = 0;
        for (const lead of upcomingPaymentLeads) {
          for (const inst of (lead.fees?.installments || [])) {
            if (
              inst.status === "Pending" &&
              inst.dueDate &&
              new Date(inst.dueDate) >= nextWeekStart &&
              new Date(inst.dueDate) <= nextWeekEnd
            ) {
              upcomingInstallments.push({
                userName: lead.userName,
                course: lead.course || 'N/A',
                amount: inst.amount || 0,
                dueDate: inst.dueDate
              });
              upcomingPaymentsTotal += inst.amount || 0;
            }
          }
        }
        upcomingInstallments.sort((a, b) => new Date(a.dueDate) - new Date(b.dueDate));

        // 4. Remarks and conversations this week for AI analysis
        const weeklyRemarksText = newLeadsThisWeek
          .slice(0, 20)
          .map(lead => {
            const weekRemarks = (lead.remarks || [])
              .filter(r => r.timestamp && new Date(r.timestamp) >= weekStart && new Date(r.timestamp) <= weekEnd)
              .map(r => r.remark)
              .join("; ");
            return weekRemarks ? `"${lead.userName}" (${lead.course || "N/A"}): ${weekRemarks}` : null;
          })
          .filter(Boolean)
          .join("\n");

        const weeklyConvText = newLeadsThisWeek
          .filter(l => (l.conversations || []).length > 0)
          .slice(0, 5)
          .map(lead => {
            const convos = (lead.conversations || [])
              .slice(-3)
              .map(c => `Q: ${c.userMsg} / A: ${c.botReply}`)
              .join("; ");
            return `"${lead.userName}": ${convos}`;
          })
          .join("\n");

        // Conversion analysis: build text for active non-enrolled leads
        const conversionLeadsText = activeNotEnrolled
          .slice(0, 15)
          .map(lead => {
            const latestRemark = (lead.remarks || [])
              .filter(r => r.timestamp)
              .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
            const recentConvs = (lead.conversations || [])
              .slice(-2)
              .map(c => `Q: ${c.userMsg} / A: ${c.botReply}`)
              .join("; ");
            let entry = `• ${lead.userName} (${lead.course || "N/A"}, Status: ${lead.status || "Cold"}, Pipeline: ${lead.pipeline || "New Lead"})`;
            if (latestRemark) entry += `\n  Remark: "${latestRemark.remark}"`;
            if (recentConvs) entry += `\n  Conv: ${recentConvs}`;
            return entry;
          })
          .join("\n\n");

        // 5. AI summary
        const billedPct = prevWeekFees.totalBilled > 0
          ? Math.round(((thisWeekFees.totalBilled - prevWeekFees.totalBilled) / prevWeekFees.totalBilled) * 100)
          : null;
        const paidPct = prevWeekFees.totalPaid > 0
          ? Math.round(((thisWeekFees.totalPaid - prevWeekFees.totalPaid) / prevWeekFees.totalPaid) * 100)
          : null;
        const billedTrendLabel = billedPct !== null
          ? `${billedPct >= 0 ? "UP" : "DOWN"} ${Math.abs(billedPct)}% vs last week`
          : "no previous data";
        const paidTrendLabel = paidPct !== null
          ? `${paidPct >= 0 ? "UP" : "DOWN"} ${Math.abs(paidPct)}% vs last week`
          : "no previous data";

        let aiSummary = "AI analysis unavailable.";
        try {
          const aiPrompt = `You are a CRM performance coach giving an honest weekly review to a sales executive.

Counsellor: ${username}
Week: ${weekStart.toLocaleDateString("en-IN")} – ${weekEnd.toLocaleDateString("en-IN")}

Key Metrics:
- New Leads Assigned: ${newLeadsThisWeek.length}
- Enrolled This Week (excl. internship): ${enrolledThisWeek}
- Avg First Response Time (office hours, Mon-Sat 9:30am-6:30pm): ${avgResponseMins !== null ? avgResponseMins + " mins" : "N/A"}
- Leads That Arrived After Office Hours: ${offHoursLeadsCount}
- Avg Off-Hours Effort (extra minutes spent responding outside office hours): ${avgOffHoursResponseMins !== null ? avgOffHoursResponseMins + " mins" : "N/A"}
- Leads Never Contacted: ${noContactCount}
- Responses >1 Hour (office hours): ${slowResponses}
- Billed This Week: ₹${thisWeekFees.totalBilled.toLocaleString("en-IN")} (${billedTrendLabel})
- Paid This Week: ₹${thisWeekFees.totalPaid.toLocaleString("en-IN")} (${paidTrendLabel})
- Billed Prev Week: ₹${prevWeekFees.totalBilled.toLocaleString("en-IN")}
- Paid Prev Week: ₹${prevWeekFees.totalPaid.toLocaleString("en-IN")}

Sample Remarks This Week:
${weeklyRemarksText || "No remarks recorded."}

Sample Conversations:
${weeklyConvText || "No conversations."}

IMPORTANT INSTRUCTIONS:
- If billed or paid amounts are DOWN vs last week, explicitly call that out as a concern — do NOT frame a revenue decline as positive or neutral.
- Match your tone to the actual numbers: a strong week deserves praise, a weak week needs honest feedback.
- Do not use generic phrases like "great progress" or "good job" when revenue is declining.

Write a concise WhatsApp-friendly performance summary (max 180 words) with:
1. Honest overall assessment (reflect actual trend — up or down)
2. 2-3 specific strengths observed
3. 2-3 concrete areas to improve (always include revenue trend if fees dropped)
4. One actionable tip for next week
Be direct, specific, and professional.`;

          const completion = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            messages: [
              { role: "system", content: "You are a helpful CRM performance coach. Respond in plain text suitable for WhatsApp." },
              { role: "user", content: aiPrompt }
            ],
            max_tokens: 400
          });
          aiSummary = completion.choices[0].message.content.trim();
        } catch (aiErr) {
          console.error(`⚠️ AI summary failed for ${username}:`, aiErr.message);
        }

        // 5b. AI: identify likely-to-confirm leads from active non-enrolled pool
        let likelyToConfirm = "No active leads to analyse.";
        if (activeNotEnrolled.length > 0) {
          try {
            const convPrompt = `You are a CRM sales analyst. Review these leads (not yet enrolled) assigned to counsellor "${username}". Based on their status, latest remarks, and WhatsApp conversations, identify up to 5 leads most likely to confirm enrollment soon.

Leads:
${conversionLeadsText || "No lead data available."}

List up to 5 leads most likely to confirm. Format each line as:
"[Name] ([Course]) — [one-line reason based on conversation/remarks/status]"

If none show strong signals, respond: "No strong conversion signals found."`;

            const convCompletion = await openai.chat.completions.create({
              model: "gpt-4o-mini",
              messages: [
                { role: "system", content: "You are a CRM sales analyst. Be concise and specific. Plain text suitable for WhatsApp." },
                { role: "user", content: convPrompt }
              ],
              max_tokens: 280
            });
            likelyToConfirm = convCompletion.choices[0].message.content.trim();
          } catch (aiErr) {
            console.error(`⚠️ Conversion AI failed for ${username}:`, aiErr.message);
            likelyToConfirm = "AI analysis unavailable.";
          }
        }

        // 6. Build WhatsApp message
        const weekLabel = `${weekStart.toLocaleDateString("en-IN", { day: "2-digit", month: "short" })} – ${weekEnd.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" })}`;
        const billedDiff = thisWeekFees.totalBilled - prevWeekFees.totalBilled;
        const paidDiff = thisWeekFees.totalPaid - prevWeekFees.totalPaid;
        const billedTrend = billedDiff >= 0 ? `↑ ₹${billedDiff.toLocaleString("en-IN")}` : `↓ ₹${Math.abs(billedDiff).toLocaleString("en-IN")}`;
        const paidTrend = paidDiff >= 0 ? `↑ ₹${paidDiff.toLocaleString("en-IN")}` : `↓ ₹${Math.abs(paidDiff).toLocaleString("en-IN")}`;
        const avgRespText = avgResponseMins !== null
          ? `${avgResponseMins} mins ${avgResponseMins > 60 ? "⚠️ (target: <60 min)" : "✅"}`
          : "N/A";
        const avgOffHoursRespText = avgOffHoursResponseMins !== null
          ? `${avgOffHoursResponseMins} mins`
          : "N/A";

        let msg = `📅 *Weekly Performance Report from Mohan*\n`;
        msg += `👤 *${username}*  |  🗓 ${weekLabel}\n`;
        msg += `━━━━━━━━━━━━━━━━━━━━\n\n`;

        msg += `📥 *LEADS*\n`;
        msg += `New Leads: *${newLeadsThisWeek.length}*\n`;
        msg += `Enrolled (excl. internship): *${enrolledThisWeek}*\n`;
        msg += `Not Contacted: *${noContactCount}* (${notContactedPct}%)\n\n`;

        msg += `⏱ *RESPONSE TIME (Office Hours)*\n`;
        msg += `Avg First Contact: *${avgRespText}*\n`;
        if (offHoursLeadsCount > 0) {
          msg += `After-Hours Leads: *${offHoursLeadsCount}* (contacted: ${offHoursContactedCount}) — Avg Off-Hours Effort: *${avgOffHoursRespText}*\n`;
        }
        msg += `Slow Responses (>1 hr): *${slowResponses}*\n`;
        if (slowResponses > 0) {
          if (slowBuckets["1h-2h"]   > 0) msg += `  • 1hr to 2hr: ${slowBuckets["1h-2h"]} leads\n`;
          if (slowBuckets["2h-3h"]   > 0) msg += `  • 2hr to 3hr: ${slowBuckets["2h-3h"]} leads\n`;
          if (slowBuckets["3h-4h"]   > 0) msg += `  • 3hr to 4hr: ${slowBuckets["3h-4h"]} leads\n`;
          if (slowBuckets["4h-5h"]   > 0) msg += `  • 4hr to 5hr: ${slowBuckets["4h-5h"]} leads\n`;
          if (slowBuckets["above5h"] > 0) msg += `  • Above 5hr: ${slowBuckets["above5h"]} leads\n`;
        }
        msg += `\n`;

        msg += `💰 *FEES THIS WEEK*\n`;
        msg += `Billed: *₹${thisWeekFees.totalBilled.toLocaleString("en-IN")}* (${billedTrend} vs last week)\n`;
        msg += `Paid: *₹${thisWeekFees.totalPaid.toLocaleString("en-IN")}* (${paidTrend} vs last week)\n\n`;

        msg += `📆 *UPCOMING PAYMENTS (Next Week)*\n`;
        msg += `Due: *₹${upcomingPaymentsTotal.toLocaleString("en-IN")}* across ${upcomingInstallments.length} installment${upcomingInstallments.length !== 1 ? "s" : ""}\n\n`;

        msg += `━━━━━━━━━━━━━━━━━━━━\n`;
        msg += `PERFORMANCE SUMMARY*\n\n`;
        msg += aiSummary;

        msg += `\n\n━━━━━━━━━━━━━━━━━━━━\n`;
        msg += `🔥 *LIKELY TO CONFIRM*\n\n`;
        msg += likelyToConfirm;

        if (respondedAfterFollowUpLeads.length > 0) {
          msg += `\n\n💬 *RESPONDED AFTER FOLLOW-UP* (${respondedAfterFollowUpLeads.length})\n`;
          respondedAfterFollowUpLeads.slice(0, 10).forEach(l => {
            msg += `• *${l.userName}* (${l.course || "N/A"}) — ${l.status || "New"}\n`;
          });
        }

        if (hotNotEnrolled.length > 0) {
          msg += `\n\n━━━━━━━━━━━━━━━━━━━━\n`;
          msg += `⚠️ *PIPELINE NOT UPDATED* (${hotNotEnrolled.length} leads)\n`;
          msg += `_Hot/Warm leads not moved to Enrolled:_\n\n`;
          hotNotEnrolled.slice(0, 8).forEach(l => {
            const lastRemark = (l.remarks || [])
              .filter(r => r.timestamp)
              .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
            msg += `• *${l.userName}* (${l.course || "N/A"}) — ${l.status}\n`;
            if (lastRemark) {
              const short = lastRemark.remark.length > 70
                ? lastRemark.remark.slice(0, 70) + "…"
                : lastRemark.remark;
              msg += `  _"${short}"_\n`;
            }
          });
        }

        // Save report to MongoDB for the AI Report view
        try {
          await WeeklyReport.findOneAndUpdate(
            { counsellor: username, weekStart },
            {
              counsellor: username,
              weekStart,
              weekEnd,
              generatedAt: new Date(),
              metrics: {
                newLeads: newLeadsThisWeek.length,
                enrolledThisWeek,
                noContactCount,
                notContactedPct,
                avgResponseMins,
                avgOffHoursResponseMins,
                offHoursLeadsCount,
                offHoursContactedCount,
                slowResponses,
                slowBuckets,
                thisWeekFees,
                prevWeekFees,
                billedPct,
                paidPct,
                upcomingPayments: {
                  totalAmount: upcomingPaymentsTotal,
                  count: upcomingInstallments.length
                },
              },
              aiSummary,
              likelyToConfirm,
              hotNotEnrolled: hotNotEnrolled.slice(0, 8).map(l => {
                const lastRmk = (l.remarks || [])
                  .filter(r => r.timestamp)
                  .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
                return {
                  userName: l.userName,
                  course: l.course || 'N/A',
                  status: l.status,
                  lastRemark: lastRmk ? lastRmk.remark.slice(0, 120) : ''
                };
              }),
              respondedAfterFollowUp: respondedAfterFollowUpLeads.slice(0, 10).map(l => ({
                userName: l.userName,
                course: l.course || 'N/A',
                status: l.status || 'New'
              })),
              upcomingInstallments: upcomingInstallments.slice(0, 10),
            },
            { upsert: true, new: true }
          );
          console.log(`💾 Weekly report saved to DB for ${username}`);
        } catch (dbErr) {
          console.error(`⚠️ Failed to save weekly report to DB for ${username}:`, dbErr.message);
        }

        // await fetch(`${wpUrl}/internal/send-text`, {
        //   method: "POST",
        //   headers: { "Content-Type": "application/json" },
        //   body: JSON.stringify({ to: mobile, message: msg })
        // });

        try {
          await fetch(`${wpUrl}/internal/send-text`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ to: "919597271533", message: msg })
          });
          console.log(`✅ Weekly report sent to ${username} (${mobile})`);
        } catch (waErr) {
          console.error(`⚠️ Weekly report WhatsApp send failed for ${username} (report was still saved to DB):`, waErr.message);
        }
      } catch (counsellorErr) {
        console.error(`❌ Weekly report error for ${counsellor.username}:`, counsellorErr);
      }
    }

    console.log("✅ All weekly WhatsApp reports sent successfully");

    // Auto-generate org-wide Minmini report and save to DB
    try {
      await computeAndSaveMinminiReport(weekStart, weekEnd);
      console.log("💾 Minmini report generated and saved to DB");
    } catch (minminiErr) {
      console.error("❌ Minmini report generation failed:", minminiErr.message);
    }
  } catch (err) {
    console.error("❌ Weekly report cron error:", err);
  }
}, {
  timezone: "Asia/Kolkata"
});


// ─── Monthly Report Cron: 1st of each month at 9 AM IST ───────────────────
// 0 9 1 * *
cron.schedule("0 9 1 * *", async () => {
  try {
    console.log("📊 Running monthly AI report cron...");

    const now = new Date();
    const y = now.getFullYear();
    const m = now.getMonth(); // 0-indexed

    // Previous complete calendar month -- AI
    // const monthStart = new Date(y, m - 1, 1, 0, 0, 0, 0);
    // const monthEnd = new Date(y, m, 0, 23, 59, 59, 999);

    // // Month before that (for comparison)
    // const prevMonthStart = new Date(y, m - 2, 1, 0, 0, 0, 0);
    // const prevMonthEnd = new Date(y, m - 1, 0, 23, 59, 59, 999);

    // Current calendar month (June, if run in June) -- sharan
const monthStart = new Date(y, m, 1, 0, 0, 0, 0);
const monthEnd = new Date(y, m + 1, 0, 23, 59, 59, 999);

// Previous month (for comparison)
const prevMonthStart = new Date(y, m - 1, 1, 0, 0, 0, 0);
const prevMonthEnd = new Date(y, m, 0, 23, 59, 59, 999);

    const counsellors = await User.find({
      role: "user",
      userNumber: { $nin: EXCLUDED_REPORT_NUMBERS }
    }).lean();

    const feesAgg = async (username, start, end) => {
      const result = await Chat.aggregate([
        { $match: { assignedto: username, pipeline: { $in: CONVERTED_STAGES } } },
        {
          $facet: {
            billed: [
              { $addFields: { firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] } } },
              { $match: { firstInstDueDate: { $gte: start, $lte: end } } },
              { $group: { _id: null, totalBilled: { $sum: { $ifNull: ["$fees.billedAmount", 0] } } } }
            ],
            paid: [
              { $unwind: { path: "$fees.installments", preserveNullAndEmptyArrays: false } },
              { $match: { "fees.installments.status": "Paid", "fees.installments.dueDate": { $gte: start, $lte: end } } },
              { $group: { _id: null, totalPaid: { $sum: { $ifNull: ["$fees.installments.amount", 0] } } } }
            ]
          }
        }
      ]);
      return {
        totalBilled: result[0]?.billed?.[0]?.totalBilled || 0,
        totalPaid: result[0]?.paid?.[0]?.totalPaid || 0
      };
    };

    for (const counsellor of counsellors) {
      try {
        const username = counsellor.username;

        const newLeadsThisMonth = await Chat.find({
          assignedto: username,
          datecreated: { $gte: monthStart, $lte: monthEnd },
          $nor: [
            { pipeline: { $regex: CONVERTED_RE }, course: { $regex: /^college internship$/i } }
          ]
        }).select("userName userNumber datecreated remarks conversations course pipeline").lean();

        const responseTimes = [];
        const offHoursResponseTimes = [];
        let noContactCount = 0;
        let offHoursLeadsCount = 0;
        for (const lead of newLeadsThisMonth) {
          const sorted = (lead.remarks || [])
            .filter(r => r.timestamp)
            .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
          const createdAt = new Date(lead.datecreated);
          const arrivedOffHours = !isWithinOfficeHours(createdAt);
          if (arrivedOffHours) offHoursLeadsCount++;
          if (sorted.length > 0) {
            const firstContact = new Date(sorted[0].timestamp);
            if (firstContact >= createdAt) {
              responseTimes.push(businessMinutesBetween(createdAt, firstContact));
              if (arrivedOffHours) {
                offHoursResponseTimes.push(offHoursMinutesBetween(createdAt, firstContact));
              }
            }
          } else {
            noContactCount++;
          }
        }
        const avgResponseMins = responseTimes.length > 0
          ? Math.round(responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length)
          : null;
        const avgOffHoursResponseMins = offHoursResponseTimes.length > 0
          ? Math.round(offHoursResponseTimes.reduce((a, b) => a + b, 0) / offHoursResponseTimes.length)
          : null;
        const offHoursContactedCount = offHoursResponseTimes.length;
        const slowResponses = responseTimes.filter(t => t > 60).length;
        const notContactedPct = newLeadsThisMonth.length > 0
          ? Math.round((noContactCount / newLeadsThisMonth.length) * 100)
          : 0;
        const slowBuckets = {
          "1h-2h":   responseTimes.filter(t => t > 60  && t <= 120).length,
          "2h-3h":   responseTimes.filter(t => t > 120 && t <= 180).length,
          "3h-4h":   responseTimes.filter(t => t > 180 && t <= 240).length,
          "4h-5h":   responseTimes.filter(t => t > 240 && t <= 300).length,
          "above5h": responseTimes.filter(t => t > 300).length,
        };

        const enrolledThisMonth = await Chat.countDocuments({
          assignedto: username,
          pipeline: { $regex: CONVERTED_RE },
          course: { $not: { $regex: /^college internship$/i } },
          datecreated: { $gte: monthStart, $lte: monthEnd }
        });

        const [thisMonthFees, prevMonthFees] = await Promise.all([
          feesAgg(username, monthStart, monthEnd),
          feesAgg(username, prevMonthStart, prevMonthEnd)
        ]);

        const thirtyDaysAgo = new Date(now);
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

        const activeNotEnrolled = await Chat.find({
          assignedto: username,
          pipeline: { $not: { $regex: CONVERTED_RE } },
          lastInteracted: { $gte: thirtyDaysAgo },
          $nor: [{ course: { $regex: /^college internship$/i } }]
        }).select("userName userNumber status pipeline course remarks conversations lastInteracted").lean();

        const hotNotEnrolled = activeNotEnrolled.filter(l => /^(hot|warm)$/i.test(l.status || ""));

        const respondedAfterFollowUpLeads = await Chat.find({
          assignedto: username,
          respondedAfterFollowUp: true,
          lastInteracted: { $gte: monthStart, $lte: monthEnd },
          pipeline: { $not: { $regex: CONVERTED_RE } }
        }).select("userName course status pipeline").lean();

        const monthlyRemarksText = newLeadsThisMonth
          .slice(0, 20)
          .map(lead => {
            const mRemarks = (lead.remarks || [])
              .filter(r => r.timestamp && new Date(r.timestamp) >= monthStart && new Date(r.timestamp) <= monthEnd)
              .map(r => r.remark)
              .join("; ");
            return mRemarks ? `"${lead.userName}" (${lead.course || "N/A"}): ${mRemarks}` : null;
          })
          .filter(Boolean)
          .join("\n");

        const conversionLeadsText = activeNotEnrolled
          .slice(0, 15)
          .map(lead => {
            const latestRemark = (lead.remarks || [])
              .filter(r => r.timestamp)
              .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
            const recentConvs = (lead.conversations || [])
              .slice(-2)
              .map(c => `Q: ${c.userMsg} / A: ${c.botReply}`)
              .join("; ");
            let entry = `• ${lead.userName} (${lead.course || "N/A"}, Status: ${lead.status || "Cold"}, Pipeline: ${lead.pipeline || "New Lead"})`;
            if (latestRemark) entry += `\n  Remark: "${latestRemark.remark}"`;
            if (recentConvs) entry += `\n  Conv: ${recentConvs}`;
            return entry;
          })
          .join("\n\n");

        const billedPct = prevMonthFees.totalBilled > 0
          ? Math.round(((thisMonthFees.totalBilled - prevMonthFees.totalBilled) / prevMonthFees.totalBilled) * 100)
          : null;
        const paidPct = prevMonthFees.totalPaid > 0
          ? Math.round(((thisMonthFees.totalPaid - prevMonthFees.totalPaid) / prevMonthFees.totalPaid) * 100)
          : null;
        const billedTrendLabel = billedPct !== null
          ? `${billedPct >= 0 ? "UP" : "DOWN"} ${Math.abs(billedPct)}% vs last month`
          : "no previous data";
        const paidTrendLabel = paidPct !== null
          ? `${paidPct >= 0 ? "UP" : "DOWN"} ${Math.abs(paidPct)}% vs last month`
          : "no previous data";

        let aiSummary = "AI analysis unavailable.";
        try {
          const aiPrompt = `You are a CRM performance coach giving an honest monthly review to a sales executive.

Counsellor: ${username}
Month: ${monthStart.toLocaleDateString("en-IN")} – ${monthEnd.toLocaleDateString("en-IN")}

Key Metrics:
- New Leads Assigned: ${newLeadsThisMonth.length}
- Enrolled This Month (excl. internship): ${enrolledThisMonth}
- Avg First Response Time (office hours, Mon-Sat 9:30am-6:30pm): ${avgResponseMins !== null ? avgResponseMins + " mins" : "N/A"}
- Leads That Arrived After Office Hours: ${offHoursLeadsCount}
- Avg Off-Hours Effort (extra minutes spent responding outside office hours): ${avgOffHoursResponseMins !== null ? avgOffHoursResponseMins + " mins" : "N/A"}
- Leads Never Contacted: ${noContactCount}
- Responses >1 Hour (office hours): ${slowResponses}
- Billed This Month: ₹${thisMonthFees.totalBilled.toLocaleString("en-IN")} (${billedTrendLabel})
- Paid This Month: ₹${thisMonthFees.totalPaid.toLocaleString("en-IN")} (${paidTrendLabel})
- Billed Prev Month: ₹${prevMonthFees.totalBilled.toLocaleString("en-IN")}
- Paid Prev Month: ₹${prevMonthFees.totalPaid.toLocaleString("en-IN")}

Sample Remarks This Month:
${monthlyRemarksText || "No remarks recorded."}

IMPORTANT INSTRUCTIONS:
- If billed or paid amounts are DOWN vs last month, explicitly call that out as a concern — do NOT frame a revenue decline as positive or neutral.
- Match your tone to the actual numbers: a strong month deserves praise, a weak month needs honest feedback.
- Do not use generic phrases like "great progress" or "good job" when revenue is declining.

Write a concise monthly performance summary (max 200 words) with:
1. Honest overall assessment (reflect actual trend — up or down)
2. 2-3 specific strengths observed this month
3. 2-3 concrete areas to improve (always include revenue trend if fees dropped)
4. One actionable goal for next month
Be direct, specific, and professional.`;

          const completion = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            messages: [
              { role: "system", content: "You are a helpful CRM performance coach. Respond in plain text." },
              { role: "user", content: aiPrompt }
            ],
            max_tokens: 450
          });
          aiSummary = completion.choices[0].message.content.trim();
        } catch (aiErr) {
          console.error(`⚠️ Monthly AI summary failed for ${username}:`, aiErr.message);
        }

        let likelyToConfirm = "No active leads to analyse.";
        if (activeNotEnrolled.length > 0) {
          try {
            const convPrompt = `You are a CRM sales analyst. Review these leads (not yet enrolled) assigned to counsellor "${username}". Based on their status, latest remarks, and conversations, identify up to 5 leads most likely to confirm enrollment soon.

Leads:
${conversionLeadsText || "No lead data available."}

List up to 5 leads most likely to confirm. Format each line as:
"[Name] ([Course]) — [one-line reason based on conversation/remarks/status]"

If none show strong signals, respond: "No strong conversion signals found."`;

            const convCompletion = await openai.chat.completions.create({
              model: "gpt-4o-mini",
              messages: [
                { role: "system", content: "You are a CRM sales analyst. Be concise and specific." },
                { role: "user", content: convPrompt }
              ],
              max_tokens: 280
            });
            likelyToConfirm = convCompletion.choices[0].message.content.trim();
          } catch (aiErr) {
            console.error(`⚠️ Monthly conversion AI failed for ${username}:`, aiErr.message);
            likelyToConfirm = "AI analysis unavailable.";
          }
        }

        try {
          await MonthlyReport.findOneAndUpdate(
            { counsellor: username, monthStart },
            {
              counsellor: username,
              monthStart,
              monthEnd,
              generatedAt: new Date(),
              metrics: {
                newLeads: newLeadsThisMonth.length,
                enrolledThisMonth,
                noContactCount,
                notContactedPct,
                avgResponseMins,
                avgOffHoursResponseMins,
                offHoursLeadsCount,
                offHoursContactedCount,
                slowResponses,
                slowBuckets,
                thisMonthFees,
                prevMonthFees,
                billedPct,
                paidPct,
              },
              aiSummary,
              likelyToConfirm,
              hotNotEnrolled: hotNotEnrolled.slice(0, 8).map(l => {
                const lastRmk = (l.remarks || [])
                  .filter(r => r.timestamp)
                  .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
                return {
                  userName: l.userName,
                  course: l.course || 'N/A',
                  status: l.status,
                  lastRemark: lastRmk ? lastRmk.remark.slice(0, 120) : ''
                };
              }),
              respondedAfterFollowUp: respondedAfterFollowUpLeads.slice(0, 10).map(l => ({
                userName: l.userName,
                course: l.course || 'N/A',
                status: l.status || 'New'
              })),
            },
            { upsert: true, new: true }
          );
          console.log(`💾 Monthly report saved to DB for ${username}`);
        } catch (dbErr) {
          console.error(`⚠️ Failed to save monthly report to DB for ${username}:`, dbErr.message);
        }

        console.log(`✅ Monthly report generated for ${username}`);
      } catch (counsellorErr) {
        console.error(`❌ Monthly report error for ${counsellor.username}:`, counsellorErr);
      }
    }

    console.log("✅ All monthly reports generated successfully");

    try {
      await computeAndSaveMonthlyMinminiReport(monthStart, monthEnd);
      console.log("💾 Monthly Minmini report generated and saved to DB");
    } catch (minminiErr) {
      console.error("❌ Monthly Minmini report generation failed:", minminiErr.message);
    }
  } catch (err) {
    console.error("❌ Monthly report cron error:", err);
  }
}, {
  timezone: "Asia/Kolkata"
});

// ─── Google Analytics 4 – Acquisition Overview ──────────────────────────────
app.get('/analytics/acquisition', async (req, res) => {
  try {
    const propertyId = (process.env.property_id || '').trim();
    if (!propertyId) {
      return res.status(500).json({ success: false, message: 'GA4 property_id not configured in .env' });
    }

    const startDate = req.query.startDate || '28daysAgo';
    const endDate   = req.query.endDate   || 'today';

    const [report] = await analyticsClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'sessionDefaultChannelGrouping' }],
      metrics: [
        { name: 'sessions' },
        { name: 'newUsers' },
        { name: 'totalUsers' },
        { name: 'engagementRate' },
        { name: 'averageSessionDuration' },
        { name: 'bounceRate' },
      ],
      orderBys: [{ metric: { metricName: 'sessions' }, desc: true }],
    });

    const rows = (report.rows || []).map(row => ({
      channel:            row.dimensionValues[0].value,
      sessions:           parseInt(row.metricValues[0].value)  || 0,
      newUsers:           parseInt(row.metricValues[1].value)  || 0,
      totalUsers:         parseInt(row.metricValues[2].value)  || 0,
      engagementRate:     parseFloat((parseFloat(row.metricValues[3].value) * 100).toFixed(1)) || 0,
      avgSessionDuration: Math.round(parseFloat(row.metricValues[4].value)) || 0,
      bounceRate:         parseFloat((parseFloat(row.metricValues[5].value) * 100).toFixed(1)) || 0,
    }));

    const totals = rows.reduce(
      (acc, r) => ({
        sessions:   acc.sessions   + r.sessions,
        newUsers:   acc.newUsers   + r.newUsers,
        totalUsers: acc.totalUsers + r.totalUsers,
      }),
      { sessions: 0, newUsers: 0, totalUsers: 0 }
    );

    return res.json({ success: true, dateRange: { startDate, endDate }, rows, totals });
  } catch (err) {
    console.error('GA4 acquisition error:', err.message);
    return res.status(500).json({ success: false, message: 'Failed to fetch GA4 data', error: err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

// ─── Google Analytics 4 – Session Manual Source ─────────────────────────────
app.get('/analytics/session-source', async (req, res) => {
  try {
    const propertyId = (process.env.property_id || '').trim();
    if (!propertyId) {
      return res.status(500).json({ success: false, message: 'GA4 property_id not configured in .env' });
    }

    const startDate = req.query.startDate || '28daysAgo';
    const endDate   = req.query.endDate   || 'today';

    const [report] = await analyticsClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'sessionManualSource' }],
      metrics: [
        { name: 'sessions' },
        { name: 'newUsers' },
        { name: 'totalUsers' },
        { name: 'engagementRate' },
        { name: 'averageSessionDuration' },
        { name: 'bounceRate' },
      ],
      orderBys: [{ metric: { metricName: 'sessions' }, desc: true }],
    });

    const rows = (report.rows || []).map(row => ({
      source:             row.dimensionValues[0].value,
      sessions:           parseInt(row.metricValues[0].value)  || 0,
      newUsers:           parseInt(row.metricValues[1].value)  || 0,
      totalUsers:         parseInt(row.metricValues[2].value)  || 0,
      engagementRate:     parseFloat((parseFloat(row.metricValues[3].value) * 100).toFixed(1)) || 0,
      avgSessionDuration: Math.round(parseFloat(row.metricValues[4].value)) || 0,
      bounceRate:         parseFloat((parseFloat(row.metricValues[5].value) * 100).toFixed(1)) || 0,
    }));

    const totals = rows.reduce(
      (acc, r) => ({
        sessions:   acc.sessions   + r.sessions,
        newUsers:   acc.newUsers   + r.newUsers,
        totalUsers: acc.totalUsers + r.totalUsers,
      }),
      { sessions: 0, newUsers: 0, totalUsers: 0 }
    );

    return res.json({ success: true, dateRange: { startDate, endDate }, rows, totals });
  } catch (err) {
    console.error('GA4 session source error:', err.message);
    return res.status(500).json({ success: false, message: 'Failed to fetch GA4 data', error: err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

// ─── Google Analytics 4 – Google Organic Search: Landing Page + Query String ─
app.get('/analytics/organic-landing-pages', async (req, res) => {
  try {
    const propertyId = (process.env.property_id || '').trim();
    if (!propertyId) {
      return res.status(500).json({ success: false, message: 'GA4 property_id not configured in .env' });
    }

    const startDate = req.query.startDate || '28daysAgo';
    const endDate   = req.query.endDate   || 'today';

    const [report] = await analyticsClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'landingPagePlusQueryString' }],
      metrics: [
        { name: 'sessions' },
        { name: 'screenPageViews' },
        { name: 'bounceRate' },
        { name: 'eventsPerSession' },
        { name: 'engagedSessions' },
        { name: 'conversions' },
      ],
      dimensionFilter: {
        andGroup: {
          expressions: [
            { filter: { fieldName: 'sessionSource', stringFilter: { matchType: 'EXACT', value: 'google' } } },
            { filter: { fieldName: 'sessionMedium', stringFilter: { matchType: 'EXACT', value: 'organic' } } },
          ],
        },
      },
      orderBys: [{ metric: { metricName: 'sessions' }, desc: true }],
    });

    const rows = (report.rows || []).map(row => ({
      landingPage:    row.dimensionValues[0].value,
      sessions:       parseInt(row.metricValues[0].value) || 0,
      views:          parseInt(row.metricValues[1].value) || 0,
      bounceRate:     parseFloat((parseFloat(row.metricValues[2].value) * 100).toFixed(2)) || 0,
      eventsPerSession: parseFloat(parseFloat(row.metricValues[3].value).toFixed(2)) || 0,
      engagedSessions: parseInt(row.metricValues[4].value) || 0,
      conversions:    parseInt(row.metricValues[5].value) || 0,
    }));

    const totals = rows.reduce(
      (acc, r) => ({
        sessions:        acc.sessions        + r.sessions,
        views:           acc.views           + r.views,
        engagedSessions: acc.engagedSessions + r.engagedSessions,
        conversions:     acc.conversions     + r.conversions,
      }),
      { sessions: 0, views: 0, engagedSessions: 0, conversions: 0 }
    );

    return res.json({ success: true, dateRange: { startDate, endDate }, rows, totals });
  } catch (err) {
    console.error('GA4 organic landing pages error:', err.message);
    return res.status(500).json({ success: false, message: 'Failed to fetch GA4 data', error: err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

// ─── Google Analytics 4 – Engagement Overview ───────────────────────────────
app.get('/analytics/engagement-overview', async (req, res) => {
  try {
    const propertyId = (process.env.property_id || '').trim();
    if (!propertyId) {
      return res.status(500).json({ success: false, message: 'GA4 property_id not configured in .env' });
    }

    const startDate = req.query.startDate || '28daysAgo';
    const endDate   = req.query.endDate   || 'today';

    const [report] = await analyticsClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'date' }],
      metrics: [
        { name: 'activeUsers' },
        { name: 'userEngagementDuration' },
        { name: 'engagedSessions' },
        { name: 'sessions' },
      ],
      orderBys: [{ dimension: { dimensionName: 'date' } }],
    });

    const parsedRows = (report.rows || []).map(row => {
      const activeUsers            = parseInt(row.metricValues[0].value) || 0;
      const userEngagementDuration = parseFloat(row.metricValues[1].value) || 0;
      const engagedSessions        = parseInt(row.metricValues[2].value) || 0;
      const sessions               = parseInt(row.metricValues[3].value) || 0;
      const dim = row.dimensionValues[0].value; // YYYYMMDD

      return {
        date: `${dim.slice(0, 4)}-${dim.slice(4, 6)}-${dim.slice(6, 8)}`,
        activeUsers,
        userEngagementDuration,
        engagedSessions,
        sessions,
      };
    });

    const rows = parsedRows.map(r => ({
      date:                            r.date,
      activeUsers:                     r.activeUsers,
      engagedSessions:                 r.engagedSessions,
      sessions:                        r.sessions,
      avgEngagementTimePerActiveUser:  r.activeUsers ? Math.round(r.userEngagementDuration / r.activeUsers) : 0,
      engagedSessionsPerActiveUser:    r.activeUsers ? parseFloat((r.engagedSessions / r.activeUsers).toFixed(2)) : 0,
      avgEngagementTimePerSession:     r.sessions ? Math.round(r.userEngagementDuration / r.sessions) : 0,
    }));

    const totalActiveUsers        = parsedRows.reduce((sum, r) => sum + r.activeUsers, 0);
    const totalEngagedSessions    = parsedRows.reduce((sum, r) => sum + r.engagedSessions, 0);
    const totalSessions           = parsedRows.reduce((sum, r) => sum + r.sessions, 0);
    const totalEngagementDuration = parsedRows.reduce((sum, r) => sum + r.userEngagementDuration, 0);

    const summary = {
      avgEngagementTimePerActiveUser: totalActiveUsers ? Math.round(totalEngagementDuration / totalActiveUsers) : 0,
      engagedSessionsPerActiveUser:   totalActiveUsers ? parseFloat((totalEngagedSessions / totalActiveUsers).toFixed(2)) : 0,
      avgEngagementTimePerSession:    totalSessions ? Math.round(totalEngagementDuration / totalSessions) : 0,
    };

    return res.json({ success: true, dateRange: { startDate, endDate }, summary, rows });
  } catch (err) {
    console.error('GA4 engagement overview error:', err.message);
    return res.status(500).json({ success: false, message: 'Failed to fetch GA4 data', error: err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
