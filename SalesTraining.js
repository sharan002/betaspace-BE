// SalesTraining.js
// ---------------------------------------------------------------------------
// Read-only "Sales Training" feature for the CRM.
//
// The daily 3:30 PM WhatsApp sales-training quiz (wpchat-api/SalesTraining.js)
// already runs the whole flow — picks a topic, sends a voice note + script,
// asks 5 questions, grades the replies — and persists everything in the
// SalesTrainingSession collection (Schema.js). Nothing here changes that
// flow; this module only adds:
//   - read-only routes so admins can browse every counsellor's training
//     transcript, questions, answers and scores, and counsellors can see
//     their own history
//   - an AI assistant endpoint, familiar with every topic the program has
//     covered so far, so counsellors and admins can both ask general
//     training questions; counsellors additionally get their own recent
//     sessions folded in for personal follow-up questions
//
//   - no new writes to SalesTrainingSession — reuses it as-is
//   - all routes mounted under /training/*, gated by the shared `auth`
//     middleware and scoped by role: admins see every counsellor,
//     counsellors only ever see their own sessions
//
// Wiring (added to index.js):
//     const { registerSalesTrainingRoutes } = require("./SalesTraining");
//     registerSalesTrainingRoutes(app, auth);
// ---------------------------------------------------------------------------

const OpenAI = require("openai");
const { SalesTrainingSession } = require("./Schema");

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

function getISTDateString(d = new Date()) {
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

const DATE_RX = /^\d{4}-\d{2}-\d{2}$/;

// Start/end instants (as real Dates) of the IST calendar day for a
// "YYYY-MM-DD" string, for filtering the startedAt timestamp field.
function istDayBounds(dateStr) {
  const start = new Date(`${dateStr}T00:00:00+05:30`);
  const end = new Date(`${dateStr}T23:59:59.999+05:30`);
  return { start, end };
}

function registerSalesTrainingRoutes(app, auth) {
  if (!app || typeof app.use !== "function") {
    throw new Error("registerSalesTrainingRoutes(app, auth): valid Express app required");
  }
  const authMw = typeof auth === "function" ? auth : (req, res, next) => next();

  const adminOnly = (req, res, next) => {
    if (req.user && req.user.role === "admin") return next();
    return res.status(403).json({ success: false, message: "Admin access required" });
  };

  // Admins see every counsellor's sessions; counsellors only ever see their own.
  const scopeQuery = (req) => (req.user?.role === "admin" ? {} : { username: req.user?.username });

  // Distinct dates with sessions, newest first, with per-day counts and
  // average score so the frontend can render a date list without fetching
  // every transcript. Admin: all counsellors. Counsellor: their own days only.
  app.get("/training/dates", authMw, async (req, res) => {
    try {
      const rows = await SalesTrainingSession.aggregate([
        { $match: scopeQuery(req) },
        {
          $group: {
            _id: { $dateToString: { format: "%Y-%m-%d", date: "$startedAt", timezone: "Asia/Kolkata" } },
            total: { $sum: 1 },
            completed: { $sum: { $cond: [{ $eq: ["$status", "completed"] }, 1, 0] } },
            inProgress: { $sum: { $cond: [{ $eq: ["$status", "in_progress"] }, 1, 0] } },
            avgScore: { $avg: "$totalScore" },
          },
        },
        { $sort: { _id: -1 } },
        { $limit: 90 },
      ]);
      const dates = rows.map((r) => ({
        date: r._id,
        total: r.total,
        completed: r.completed,
        inProgress: r.inProgress,
        avgScore: r.avgScore === null || r.avgScore === undefined ? null : Math.round(r.avgScore * 10) / 10,
      }));
      res.json({ success: true, dates });
    } catch (err) {
      console.error("[SalesTraining] dates error:", err);
      res.status(500).json({ success: false, message: "Failed to load training dates" });
    }
  });

  // Sessions for one day (transcript/questions/answers/scores). Defaults to
  // today (IST). Admin: every counsellor. Counsellor: only their own.
  app.get("/training/sessions", authMw, async (req, res) => {
    try {
      const date = DATE_RX.test(req.query.date || "") ? req.query.date : getISTDateString();
      const { start, end } = istDayBounds(date);
      const sessions = await SalesTrainingSession.find({
        ...scopeQuery(req),
        startedAt: { $gte: start, $lte: end },
      }).sort({ username: 1 }).lean();
      res.json({ success: true, date, sessions });
    } catch (err) {
      console.error("[SalesTraining] sessions error:", err);
      res.status(500).json({ success: false, message: "Failed to load training sessions" });
    }
  });

  // Single session by id (for a direct link/refresh). Counsellors can only
  // fetch their own.
  app.get("/training/sessions/:id", authMw, async (req, res) => {
    try {
      const session = await SalesTrainingSession.findById(req.params.id).lean();
      if (!session) return res.status(404).json({ success: false, message: "Training session not found" });
      if (req.user?.role !== "admin" && session.username !== req.user?.username) {
        return res.status(403).json({ success: false, message: "Forbidden" });
      }
      res.json({ success: true, session });
    } catch (err) {
      console.error("[SalesTraining] session get error:", err);
      res.status(500).json({ success: false, message: "Failed to load training session" });
    }
  });

  // A counsellor's own recent history (for their "My Training" list, and as
  // grounding context for the assistant below). Admins may pass ?username=
  // to look up a specific counsellor's history.
  app.get("/training/history", authMw, async (req, res) => {
    try {
      const username = (req.user?.role === "admin" && req.query.username) ? req.query.username : req.user?.username;
      const sessions = await SalesTrainingSession.find({ username })
        .sort({ startedAt: -1 })
        .limit(30)
        .lean();
      res.json({ success: true, sessions });
    } catch (err) {
      console.error("[SalesTraining] history error:", err);
      res.status(500).json({ success: false, message: "Failed to load training history" });
    }
  });

  // AI assistant: a free-form question about the sales-training program.
  // Grounded in every topic actually covered so far (across all counsellors)
  // so it's familiar with the curriculum generally, not just one person's
  // history. Counsellors additionally get their own recent sessions/quiz
  // feedback folded in so they can ask about their personal results too.
  // Admins get the general, program-wide assistant — no counsellor lookup.
  app.post("/training/assistant/ask", authMw, async (req, res) => {
    try {
      const { question, history = [] } = req.body;
      if (!question) return res.status(400).json({ success: false, message: "Question is required" });

      const isAdmin = req.user?.role === "admin";

      const topicRows = await SalesTrainingSession.aggregate([
        { $group: { _id: "$topic" } },
        { $match: { _id: { $ne: null } } },
        { $sort: { _id: 1 } },
        { $limit: 150 },
      ]);
      const curriculumContext = topicRows.length === 0
        ? "No training topics have been recorded yet."
        : topicRows.map((t, i) => `${i + 1}. ${t._id}`).join("\n");

      let personalContext = "";
      if (!isAdmin) {
        const recentSessions = await SalesTrainingSession.find({ username: req.user?.username })
          .sort({ startedAt: -1 })
          .limit(10)
          .select("topic script questions totalScore startedAt")
          .lean();

        personalContext = recentSessions.length === 0
          ? "\n\nThis counsellor has no recorded training sessions yet."
          : "\n\nThis counsellor's own recent sessions:\n" + recentSessions.map((s, i) => {
              const qa = (s.questions || [])
                .map((q, j) => `  Q${j + 1}: ${q.text}\n  Their answer: ${q.answer || "(no answer)"}${q.feedback ? `\n  Feedback: ${q.feedback}` : ""}`)
                .join("\n");
              return `Session ${i + 1} — Topic: "${s.topic}" (score ${s.totalScore ?? "n/a"}/5)\nScript: ${s.script}\n${qa}`;
            }).join("\n\n");
      }

      const chatHistory = history.map((m) => ({ role: m.role, content: m.content }));

      const systemPrompt = isAdmin
        ? `You are the sales training assistant for Appin Technology's course counsellor team. You are deeply familiar with the daily WhatsApp sales-training program sent to every counsellor — explain any of its topics, sales techniques, or objection-handling approaches clearly and practically, and answer general questions about the training program. Be concise and practical.\n\nTopics covered by the training so far:\n${curriculumContext}`
        : `You are a sales training assistant for course counsellors at Appin Technology. You are deeply familiar with the daily sales-training program below and can explain any of these topics clearly and practically — even ones this counsellor hasn't personally been quizzed on yet. When relevant, also reference their own sessions and quiz feedback. Be concise and practical.\n\nTopics covered by the training so far:\n${curriculumContext}${personalContext}`;

      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: systemPrompt },
          ...chatHistory,
          { role: "user", content: question },
        ],
      });

      const reply = completion.choices[0].message.content;
      res.json({ success: true, reply });
    } catch (err) {
      console.error("[SalesTraining] assistant error:", err);
      res.status(500).json({ success: false, message: "Failed to get assistant response" });
    }
  });

  // Manually kick off today's WhatsApp sales training right now, instead of
  // waiting for the 3:30 PM cron in wpchat-api/SalesTraining.js. Just proxies
  // to wpchat-api, which owns the actual send and already skips counsellors
  // who have a training session started today — so pressing this twice is
  // always safe. Admin-only.
  app.post("/training/trigger", authMw, adminOnly, async (req, res) => {
    try {
      const wpUrl = process.env.WP_CHAT_SERVICE_URL;
      const response = await fetch(`${wpUrl}/training/trigger-now`, { method: "POST" });
      const data = await response.json();
      if (!response.ok || !data.success) {
        return res.status(502).json({ success: false, message: data.message || "Failed to trigger sales training" });
      }
      res.json(data);
    } catch (err) {
      console.error("[SalesTraining] trigger error:", err);
      res.status(500).json({ success: false, message: "Failed to trigger sales training" });
    }
  });
}

module.exports = { registerSalesTrainingRoutes };
