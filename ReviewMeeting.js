// ReviewMeeting.js
// ---------------------------------------------------------------------------
// Read-only "Review Meeting" feature for the admin dashboard.
//
// The daily 5 PM manager check-in (ManagerChat.js) already runs an AI-driven
// WhatsApp conversation with every counsellor and already persists the full
// transcript + an AI-generated admin summary report in the `ManagerCheckIn`
// collection (Schema.js). Nothing here changes that flow — this module only
// adds read-only routes so the admin can browse that data (the "review
// meeting" conversation + summary) inside the CRM instead of only ever
// seeing it as a WhatsApp message.
//
//   - no new Mongoose model, no writes — reuses ManagerCheckIn as-is
//   - its own routes, all mounted under  /review-meetings/*  and gated by
//     the shared `auth` middleware + an admin-role check
//
// Wiring (added to index.js):
//     const { registerReviewMeetingRoutes } = require("./ReviewMeeting");
//     registerReviewMeetingRoutes(app, auth);
// ---------------------------------------------------------------------------

const { ManagerCheckIn } = require("./Schema");
const { triggerManagerCheckInsNow } = require("./ManagerChat");

function getISTDateString(d = new Date()) {
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

const DATE_RX = /^\d{4}-\d{2}-\d{2}$/;

function registerReviewMeetingRoutes(app, auth) {
  if (!app || typeof app.use !== "function") {
    throw new Error("registerReviewMeetingRoutes(app, auth): valid Express app required");
  }
  const authMw = typeof auth === "function" ? auth : (req, res, next) => next();

  const adminOnly = (req, res, next) => {
    if (req.user && req.user.role === "admin") return next();
    return res.status(403).json({ success: false, message: "Admin access required" });
  };

  const guard = [authMw, adminOnly];

  // Distinct dates that have check-in records, newest first, with per-day counts
  // so the frontend can render a date list without fetching every transcript.
  app.get("/review-meetings/dates", guard, async (req, res) => {
    try {
      const rows = await ManagerCheckIn.aggregate([
        {
          $group: {
            _id: "$date",
            total: { $sum: 1 },
            completed: { $sum: { $cond: [{ $eq: ["$status", "completed"] }, 1, 0] } },
            inProgress: { $sum: { $cond: [{ $eq: ["$status", "in_progress"] }, 1, 0] } },
            noResponse: { $sum: { $cond: [{ $eq: ["$status", "no_response"] }, 1, 0] } },
            needsImprovement: { $sum: { $cond: [{ $eq: ["$verdict", "NEEDS_IMPROVEMENT"] }, 1, 0] } },
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
        noResponse: r.noResponse,
        needsImprovement: r.needsImprovement,
      }));
      res.json({ success: true, dates });
    } catch (err) {
      console.error("[ReviewMeeting] dates error:", err);
      res.status(500).json({ success: false, message: "Failed to load review meeting dates" });
    }
  });

  // All counsellor check-ins (full transcript + admin summary) for one day.
  // Defaults to "today" (IST) when no date is given.
  app.get("/review-meetings", guard, async (req, res) => {
    try {
      const date = DATE_RX.test(req.query.date || "") ? req.query.date : getISTDateString();
      const checkIns = await ManagerCheckIn.find({ date }).sort({ counsellorUsername: 1 }).lean();
      res.json({ success: true, date, checkIns });
    } catch (err) {
      console.error("[ReviewMeeting] list error:", err);
      res.status(500).json({ success: false, message: "Failed to load review meeting" });
    }
  });

  // Single check-in by id, in case a direct link/refresh needs just one record.
  app.get("/review-meetings/:id", guard, async (req, res) => {
    try {
      const checkIn = await ManagerCheckIn.findById(req.params.id).lean();
      if (!checkIn) return res.status(404).json({ success: false, message: "Review meeting not found" });
      res.json({ success: true, checkIn });
    } catch (err) {
      console.error("[ReviewMeeting] get error:", err);
      res.status(500).json({ success: false, message: "Failed to load review meeting" });
    }
  });

  // Manually kick off today's check-ins right now, instead of waiting for the
  // 5 PM cron in ManagerChat.js. Safe to press more than once — counsellors
  // who already have a check-in started today are skipped, never restarted.
  app.post("/review-meetings/trigger", guard, async (req, res) => {
    try {
      const result = await triggerManagerCheckInsNow();
      res.json({ success: true, ...result });
    } catch (err) {
      console.error("[ReviewMeeting] trigger error:", err);
      res.status(500).json({ success: false, message: "Failed to trigger review meeting" });
    }
  });
}

module.exports = { registerReviewMeetingRoutes };
