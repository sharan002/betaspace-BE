// Click-to-WhatsApp (CTWA) Ads manager for the CRM.
//
// Lets an admin create / manage / measure Meta Click-to-WhatsApp ads from the CRM and
// tie each ad to a CRM course. When a lead clicks the ad and messages the WhatsApp
// number, wpchat-api looks the ad up (CtwaAd.metaAdId === webhook referral.source_id),
// presets Chat.course and skips the course-selection list.
//
// Fully additive: registers /ctwa/* routes only and owns its own `ctwaads` collection.
//
// .env:
//   META_ADS_ACCESS_TOKEN  (ads_management, ads_read, pages_show_list, business_management;
//                           falls back to WHATSAPP_TOKEN)
//   META_AD_ACCOUNT_ID     (digits, with or without "act_")
//   META_PAGE_ID           (Facebook Page linked to the WhatsApp number)
//   CTWA_WHATSAPP_NUMBER   (optional, e.g. +91XXXXXXXXXX linked to the Page)
//   META_INSTAGRAM_ACTOR_ID (optional)
//   META_CTWA_DATASET_ID   (optional, Conversions API dataset for business messaging; see metaConversions.js)

const mongoose = require("mongoose");
const multer = require("multer");

const GRAPH_VERSION = process.env.META_ADS_API_VERSION || "v24.0";
const GRAPH = `https://graph.facebook.com/${GRAPH_VERSION}`;

const ZERO_DECIMAL_CURRENCIES = new Set(["JPY", "KRW", "VND", "CLP", "ISK", "HUF", "TWD", "UGX", "XAF", "XOF", "PYG"]);
const OBJECTIVES = ["OUTCOME_ENGAGEMENT", "OUTCOME_LEADS", "OUTCOME_SALES", "OUTCOME_TRAFFIC"];
const GOALS_BY_OBJECTIVE = {
  OUTCOME_ENGAGEMENT: ["CONVERSATIONS", "LINK_CLICKS"],
  OUTCOME_LEADS: ["CONVERSATIONS"],
  OUTCOME_SALES: ["CONVERSATIONS", "LINK_CLICKS", "IMPRESSIONS", "REACH"],
  OUTCOME_TRAFFIC: ["CONVERSATIONS", "LINK_CLICKS", "LANDING_PAGE_VIEWS", "IMPRESSIONS", "REACH", "POST_ENGAGEMENT"],
};
const BID_STRATEGIES = ["LOWEST_COST_WITHOUT_CAP", "LOWEST_COST_WITH_BID_CAP", "COST_CAP"];
const CONVERSATION_ACTION = "onsite_conversion.messaging_conversation_started_7d";

// ─── Local model ────────────────────────────────────────────────────────────
const ctwaAdSchema = new mongoose.Schema({
  name: { type: String, required: true },          // campaign display name
  adName: { type: String, default: "" },
  course: { type: String, required: true },        // exact value written to Chat.course
  greeting: { type: String, default: "" },         // optional first bot message ({{name}}, {{course}})
  skipCourseSelection: { type: Boolean, default: true },
  assignedTo: { type: String, default: null },     // counsellor username — every lead from this ad goes to them
  syncedFromMeta: { type: Boolean, default: false }, // ad added/duplicated in Ads Manager, adopted automatically
  linked: { type: Boolean, default: false },       // pre-existing Meta ad linked from the CRM (never created/owned by the CRM)
  objective: { type: String },
  optimizationGoal: { type: String },
  metaCampaignId: { type: String, index: true },
  metaAdSetId: { type: String, index: true },
  metaAdId: { type: String, index: true },
  metaCreativeId: { type: String },
  whatsappNumber: { type: String },
  status: { type: String, default: "PAUSED" },
  archived: { type: Boolean, default: false },
  config: { type: mongoose.Schema.Types.Mixed, default: null }, // request snapshot, for duplicating/editing
  createdBy: { type: String },
}, { timestamps: true, collection: "ctwaads" });
const CtwaAd = mongoose.models.CtwaAd || mongoose.model("CtwaAd", ctwaAdSchema);

// ─── Meta Graph helpers ─────────────────────────────────────────────────────
const cfg = () => {
  const token = (process.env.META_ADS_ACCESS_TOKEN || process.env.WHATSAPP_TOKEN || "").trim();
  const rawAcct = (process.env.META_AD_ACCOUNT_ID || "").trim();
  const accountId = rawAcct.replace(/^act_/, "");
  return {
    token,
    accountId,
    act: accountId ? `act_${accountId}` : "",
    pageId: (process.env.META_PAGE_ID || "").trim(),
    waNumber: (process.env.CTWA_WHATSAPP_NUMBER || "").trim(),
    igId: (process.env.META_INSTAGRAM_ACTOR_ID || "").trim(),
  };
};

const httpError = (status, message, extra) => Object.assign(new Error(message), { status, ...extra });

const metaError = (body, status) => {
  const e = body?.error || {};
  const msg = e.error_user_msg || e.error_user_title
    ? `${e.error_user_title ? e.error_user_title + ": " : ""}${e.error_user_msg || e.message}`
    : (e.message || `Meta API error (${status})`);
  return httpError(502, msg, { metaCode: e.code, metaSubcode: e.error_subcode });
};

async function graph(method, path, { params = {}, body, form, token: tokenOverride } = {}) {
  const token = tokenOverride || cfg().token;
  if (!token) throw httpError(500, "META_ADS_ACCESS_TOKEN is not configured in .env");
  const url = new URL(path.startsWith("http") ? path : `${GRAPH}/${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, typeof v === "object" ? JSON.stringify(v) : String(v));
  }
  const headers = { Authorization: `Bearer ${token}` };
  let payload;
  if (form) payload = form;
  else if (body) { headers["Content-Type"] = "application/json"; payload = JSON.stringify(body); }
  const r = await fetch(url, { method, headers, body: payload });
  const json = await r.json().catch(() => ({}));
  if (!r.ok || json?.error) throw metaError(json, r.status);
  return json;
}

async function accountInfo() {
  const { act } = cfg();
  if (!act) throw httpError(500, "META_AD_ACCOUNT_ID is not configured in .env");
  return graph("GET", act, { params: { fields: "name,currency,account_status,timezone_name,amount_spent,balance" } });
}

const toMinor = (amount, currency) =>
  ZERO_DECIMAL_CURRENCIES.has(String(currency).toUpperCase()) ? Math.round(Number(amount)) : Math.round(Number(amount) * 100);
const fromMinor = (amount, currency) =>
  ZERO_DECIMAL_CURRENCIES.has(String(currency).toUpperCase()) ? Number(amount) : Number(amount) / 100;

// ─── Payload builders ───────────────────────────────────────────────────────
function buildTargeting(t = {}, placements = {}) {
  const geo = { location_types: ["home", "recent"] };
  // Meta rejects a country together with a city/state inside it, so drop any country that a
  // selected region or city already belongs to (the UI starts with "India" pre-selected).
  const coveredCountries = new Set([...(t.regions || []), ...(t.cities || [])].map(x => x.countryCode).filter(Boolean));
  const hasSubLocations = (t.regions?.length || 0) + (t.cities?.length || 0) > 0;
  const countries = (t.countries || []).filter(Boolean)
    .filter(code => !(coveredCountries.has(code) || (hasSubLocations && coveredCountries.size === 0)));
  if (countries.length) geo.countries = countries;
  if (t.regions?.length) geo.regions = t.regions.map(r => ({ key: String(r.key) }));
  if (t.cities?.length) {
    geo.cities = t.cities.map(c => ({
      key: String(c.key),
      ...(c.radius ? { radius: Number(c.radius), distance_unit: c.distanceUnit || "kilometer" } : {}),
    }));
  }
  if (!geo.countries && !geo.regions && !geo.cities) geo.countries = ["IN"];

  const targeting = {
    geo_locations: geo,
    age_min: Math.max(13, Number(t.ageMin) || 18),
    age_max: Math.min(65, Number(t.ageMax) || 65),
    targeting_automation: { advantage_audience: t.advantageAudience ? 1 : 0 },
  };
  if (t.genders?.length === 1) targeting.genders = t.genders.map(Number);
  if (t.locales?.length) targeting.locales = t.locales.map(Number);

  const flexible = {};
  if (t.interests?.length) flexible.interests = t.interests.map(i => ({ id: String(i.id), name: i.name }));
  if (t.behaviors?.length) flexible.behaviors = t.behaviors.map(i => ({ id: String(i.id), name: i.name }));
  // Demographic items come in several kinds (life_events, family_statuses, education_statuses, …);
  // each kind is its own flexible_spec key.
  for (const d of t.demographics || []) {
    const key = /^[a-z_]+$/.test(d.type || "") ? d.type : "life_events";
    (flexible[key] = flexible[key] || []).push({ id: String(d.id), name: d.name });
  }
  if (Object.keys(flexible).length) targeting.flexible_spec = [flexible];
  if (t.customAudiences?.length) targeting.custom_audiences = t.customAudiences.map(a => ({ id: String(a.id || a) }));
  if (t.excludedCustomAudiences?.length) targeting.excluded_custom_audiences = t.excludedCustomAudiences.map(a => ({ id: String(a.id || a) }));

  if (placements && placements.automatic === false) {
    if (placements.publisherPlatforms?.length) targeting.publisher_platforms = placements.publisherPlatforms;
    if (placements.facebookPositions?.length) targeting.facebook_positions = placements.facebookPositions;
    if (placements.instagramPositions?.length) targeting.instagram_positions = placements.instagramPositions;
    if (placements.messengerPositions?.length) targeting.messenger_positions = placements.messengerPositions;
  }
  if (placements?.devicePlatforms?.length) targeting.device_platforms = placements.devicePlatforms;
  return targeting;
}

function buildWelcomeMessage(w = {}, course) {
  const text = (w.text || `Hi! Thanks for your interest in ${course}. How can we help you?`).slice(0, 400);
  const prefill = (w.prefill || `Hi, I want to know more about ${course}`).slice(0, 300);
  const iceBreakers = (w.iceBreakers || []).map(s => String(s).trim()).filter(Boolean).slice(0, 4);
  const textFormat = iceBreakers.length
    ? { customer_action_type: "ice_breakers", message: { text, ice_breakers: iceBreakers.map(title => ({ title: title.slice(0, 80) })) } }
    : { customer_action_type: "autofill_message", message: { text, autofill_message: { content: prefill } } };
  return JSON.stringify({
    type: "VISUAL_EDITOR",
    version: 2,
    landing_screen_type: "welcome_message",
    media_type: "text",
    text_format: textFormat,
    user_edit: false,
    surface: "visual_editor_new",
  });
}

function buildCreativePayload({ name, c, course, pageId, igId }) {
  const cta = { type: "WHATSAPP_MESSAGE", value: { app_destination: "WHATSAPP" } };
  const common = {
    message: c.primaryText || "",
    name: c.headline || "",
    description: c.description || "",
    link: "https://api.whatsapp.com/send",
    call_to_action: cta,
    page_welcome_message: buildWelcomeMessage(c.welcome, course),
  };
  const spec = { page_id: pageId };
  if (igId) spec.instagram_actor_id = igId;
  if (c.videoId) {
    spec.video_data = {
      video_id: c.videoId,
      message: common.message,
      title: common.name,
      link_description: common.description,
      call_to_action: cta,
      page_welcome_message: common.page_welcome_message,
      ...(c.thumbnailHash ? { image_hash: c.thumbnailHash } : c.thumbnailUrl ? { image_url: c.thumbnailUrl } : {}),
    };
    if (!c.thumbnailHash && !c.thumbnailUrl) throw httpError(400, "Upload a thumbnail image for the video ad");
  } else {
    if (!c.imageHash) throw httpError(400, "Upload an image or video for the ad creative");
    spec.link_data = { ...common, image_hash: c.imageHash };
  }
  const creative = { name, object_story_spec: spec };
  // `standard_enhancements` is deprecated by Meta; opt in to individual Advantage+ features only when asked.
  // When off, the spec is omitted entirely so the creative is delivered exactly as uploaded.
  if (c.enhancements) {
    creative.degrees_of_freedom_spec = {
      creative_features_spec: {
        text_optimizations: { enroll_status: "OPT_IN" },
        image_brightness_and_contrast: { enroll_status: "OPT_IN" },
        enhance_cta: { enroll_status: "OPT_IN" },
      },
    };
  }
  return creative;
}

const nonEmpty = (v, label) => {
  if (v === undefined || v === null || String(v).trim() === "") throw httpError(400, `${label} is required`);
  return String(v).trim();
};

// ─── Insights ───────────────────────────────────────────────────────────────
const INSIGHT_FIELDS = "spend,impressions,reach,clicks,inline_link_clicks,ctr,cpm,cpc,frequency,actions,cost_per_action_type";

function parseInsight(row, currency) {
  if (!row) return { spend: 0, impressions: 0, reach: 0, clicks: 0, ctr: 0, cpm: 0, cpc: 0, frequency: 0, conversations: 0, costPerConversation: 0 };
  const conv = Number((row.actions || []).find(a => a.action_type === CONVERSATION_ACTION)?.value || 0);
  const spend = Number(row.spend || 0);
  return {
    spend,
    impressions: Number(row.impressions || 0),
    reach: Number(row.reach || 0),
    clicks: Number(row.inline_link_clicks || row.clicks || 0),
    ctr: Number(row.ctr || 0),
    cpm: Number(row.cpm || 0),
    cpc: Number(row.cpc || 0),
    frequency: Number(row.frequency || 0),
    conversations: conv,
    costPerConversation: conv ? Math.round((spend / conv) * 100) / 100 : 0,
  };
}

function insightRange(q) {
  const isD = s => /^\d{4}-\d{2}-\d{2}$/.test(s || "");
  if (isD(q.since) && isD(q.until)) return { time_range: { since: q.since, until: q.until } };
  const presets = ["today", "yesterday", "last_7d", "last_14d", "last_30d", "this_month", "last_month", "maximum"];
  return { date_preset: presets.includes(q.datePreset) ? q.datePreset : "last_7d" };
}

// Counsellors = CRM users with role "user" (same pool the round-robin auto-assign uses).
async function counsellorList() {
  const User = mongoose.model("User");
  return (await User.find({ role: "user" }).select("username").sort({ username: 1 }).lean()).map(u => u.username);
}

// Adopt ads that were created/duplicated directly in Ads Manager inside a campaign we already
// track: they inherit the course / counsellor / greeting of a sibling so leads still skip course
// selection and show up in the CRM.
async function syncCampaignAds(docs) {
  const byCampaign = {};
  // Linked (pre-existing) ads never auto-adopt siblings: their campaign may serve other courses.
  for (const d of docs) if (d.metaCampaignId && !d.linked && !byCampaign[d.metaCampaignId]) byCampaign[d.metaCampaignId] = d;
  const campaignIds = Object.keys(byCampaign);
  if (!campaignIds.length) return false;
  const known = new Set(docs.map(d => d.metaAdId));
  let added = false;
  for (let i = 0; i < campaignIds.length; i += 40) {
    const chunk = campaignIds.slice(i, i + 40);
    const d = await graph("GET", "", { params: { ids: chunk.join(","), fields: "ads.limit(200){id,name,adset_id}" } });
    for (const cid of chunk) {
      for (const ad of d[cid]?.ads?.data || []) {
        if (known.has(ad.id)) continue;
        const base = byCampaign[cid];
        try {
          await CtwaAd.create({
            name: base.name, adName: ad.name, course: base.course, greeting: base.greeting,
            skipCourseSelection: base.skipCourseSelection, assignedTo: base.assignedTo || null,
            objective: base.objective, optimizationGoal: base.optimizationGoal,
            metaCampaignId: cid, metaAdSetId: ad.adset_id, metaAdId: ad.id,
            whatsappNumber: base.whatsappNumber, status: "PAUSED", syncedFromMeta: true,
          });
          known.add(ad.id); added = true;
        } catch (e) { console.warn("CTWA sync create failed:", e.message); }
      }
    }
  }
  return added;
}

// ─── Routes ─────────────────────────────────────────────────────────────────
function registerCtwaRoutes(app, auth, requireAdmin, { Chat }) {
  const guard = [auth, requireAdmin];
  const mediaUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 100 * 1024 * 1024 } });

  const wrap = fn => async (req, res) => {
    try { await fn(req, res); }
    catch (err) {
      console.error(`CTWA ${req.method} ${req.path} error:`, err.message);
      res.status(err.status || 500).json({ success: false, message: err.message, metaCode: err.metaCode });
    }
  };

  // Setup status, account, pages, supported options
  app.get("/ctwa/config", ...guard, wrap(async (req, res) => {
    const c = cfg();
    const missing = [];
    if (!c.token) missing.push("META_ADS_ACCESS_TOKEN");
    if (!c.accountId) missing.push("META_AD_ACCOUNT_ID");
    if (!c.pageId) missing.push("META_PAGE_ID");
    if (!c.waNumber) missing.push("CTWA_WHATSAPP_NUMBER");
    const out = {
      success: true, configured: missing.length === 0, missing,
      pageId: c.pageId, whatsappNumber: c.waNumber, instagramActorId: c.igId,
      objectives: OBJECTIVES, goalsByObjective: GOALS_BY_OBJECTIVE, bidStrategies: BID_STRATEGIES,
    };
    if (c.token && c.accountId) {
      try {
        const a = await accountInfo();
        out.account = { name: a.name, currency: a.currency, status: a.account_status, timezone: a.timezone_name, balance: a.balance };
      } catch (e) { out.accountError = e.message; }
    }
    res.json(out);
  }));

  // Pages the token can use (to pick a different Page than the default)
  app.get("/ctwa/pages", ...guard, wrap(async (req, res) => {
    // Pages this ad account can promote (what Ads Manager offers in its Page picker).
    const { act } = cfg();
    const d = await graph("GET", `${act}/promote_pages`, { params: { fields: "id,name", limit: 100 } });
    res.json({ success: true, pages: d.data || [] });
  }));

  // Counsellors an ad can be assigned to
  app.get("/ctwa/counsellors", ...guard, wrap(async (req, res) => {
    res.json({ success: true, counsellors: await counsellorList() });
  }));

  // WhatsApp Cloud API numbers on the WABA (click-to-fill suggestions). Needs WABA_ID + WHATSAPP_TOKEN.
  app.get("/ctwa/whatsapp-numbers", ...guard, wrap(async (req, res) => {
    const wabaId = (process.env.WABA_ID || "").trim();
    const token = (process.env.WHATSAPP_TOKEN || "").trim();
    if (!wabaId || !token) return res.json({ success: true, numbers: [] });
    const d = await graph("GET", `${wabaId}/phone_numbers`, { token, params: { fields: "display_phone_number,verified_name", limit: 50 } });
    const norm = n => String(n || "").replace(/\D/g, "");
    const def = norm(cfg().waNumber);
    res.json({
      success: true,
      numbers: (d.data || []).map(p => ({
        number: "+" + norm(p.display_phone_number), name: p.verified_name, isDefault: !!def && norm(p.display_phone_number) === def,
      })),
    });
  }));

  // Targeting search: interests, behaviors, demographics, locales, geo
  app.get("/ctwa/targeting/search", ...guard, wrap(async (req, res) => {
    const q = String(req.query.q || "").trim();
    const type = String(req.query.type || "adinterest");
    const allowed = ["adinterest", "adgeolocation", "adlocale", "adTargetingCategory"];
    if (!allowed.includes(type)) throw httpError(400, "Unsupported targeting type");
    let params = { type, limit: 25 };
    if (type === "adTargetingCategory") {
      params.class = ["behaviors", "demographics", "life_events"].includes(req.query.class) ? req.query.class : "behaviors";
      params.limit = 400; // small fixed catalogues — filtered by name below
    } else {
      if (!q) throw httpError(400, "q is required");
      params.q = q;
    }
    if (type === "adgeolocation") {
      params.location_types = ["country", "region", "city"];
      params.country_code = req.query.country || "IN";
    }
    const d = await graph("GET", "search", { params });
    let data = d.data || [];
    if (type === "adTargetingCategory" && q) data = data.filter(x => String(x.name).toLowerCase().includes(q.toLowerCase()));
    res.json({ success: true, data: data.slice(0, 40) });
  }));

  app.get("/ctwa/audiences", ...guard, wrap(async (req, res) => {
    const { act } = cfg();
    const d = await graph("GET", `${act}/customaudiences`, { params: { fields: "id,name,subtype,approximate_count_lower_bound", limit: 200 } });
    res.json({ success: true, audiences: d.data || [] });
  }));

  // Audience size estimate for a targeting spec (best-effort)
  app.post("/ctwa/estimate", ...guard, wrap(async (req, res) => {
    const { act } = cfg();
    const { targeting, placements, optimizationGoal } = req.body || {};
    const d = await graph("GET", `${act}/delivery_estimate`, {
      params: { targeting_spec: buildTargeting(targeting, placements), optimization_goal: optimizationGoal || "CONVERSATIONS" },
    });
    res.json({ success: true, estimate: d.data?.[0] || null });
  }));

  // Existing ads in the Meta ad account (WhatsApp-destination ones) that are not yet linked to the CRM
  app.get("/ctwa/meta-ads", ...guard, wrap(async (req, res) => {
    const { act } = cfg();
    if (!act) throw httpError(500, "META_AD_ACCOUNT_ID is not configured in .env");
    const tracked = new Set((await CtwaAd.find({ archived: { $ne: true } }).select("metaAdId").lean()).map(d => d.metaAdId));
    const ads = [];
    let d = await graph("GET", `${act}/ads`, { params: {
      fields: "id,name,effective_status,campaign{id,name},adset{id,name,destination_type}",
      effective_status: ["ACTIVE", "PAUSED", "CAMPAIGN_PAUSED", "ADSET_PAUSED", "PENDING_REVIEW", "IN_PROCESS", "WITH_ISSUES"],
      limit: 100,
    } });
    for (let page = 0; page < 10; page++) {
      ads.push(...(d.data || []));
      if (!d.paging?.next) break;
      d = await graph("GET", d.paging.next);
    }
    const rows = ads
      .filter(a => a.adset?.destination_type === "WHATSAPP" && !tracked.has(a.id))
      .map(a => ({
        id: a.id, name: a.name, status: a.effective_status,
        campaignId: a.campaign?.id, campaignName: a.campaign?.name,
        adsetId: a.adset?.id, adsetName: a.adset?.name,
      }));
    res.json({ success: true, ads: rows });
  }));

  // Link existing (already running) Meta ads to a course / counsellor. Nothing is changed on Meta —
  // the webhook just starts recognising these ad ids.
  app.post("/ctwa/link", ...guard, wrap(async (req, res) => {
    const b = req.body || {};
    const c = cfg();
    const ids = [...new Set((Array.isArray(b.metaAdIds) ? b.metaAdIds : []).map(String).filter(Boolean))];
    if (!ids.length) throw httpError(400, "Select at least one ad");
    const course = nonEmpty(b.course, "Course");
    const assignedTo = typeof b.assignedTo === "string" && b.assignedTo.trim() ? b.assignedTo.trim() : null;
    if (assignedTo && !(await counsellorList()).includes(assignedTo)) throw httpError(400, `Counsellor "${assignedTo}" not found`);

    const linked = [];
    for (const adId of ids) {
      const ad = await graph("GET", adId, { params: {
        fields: "id,name,account_id,effective_status,campaign{id,name,objective},adset{id,destination_type,optimization_goal}",
      } });
      if (String(ad.account_id) !== c.accountId) throw httpError(400, `Ad ${adId} does not belong to the configured ad account`);
      const existing = await CtwaAd.findOne({ metaAdId: ad.id });
      const fields = {
        name: ad.campaign?.name || ad.name, adName: ad.name, course,
        greeting: (b.greeting || "").trim(), skipCourseSelection: b.skipCourseSelection !== false, assignedTo,
        objective: ad.campaign?.objective, optimizationGoal: ad.adset?.optimization_goal,
        metaCampaignId: ad.campaign?.id, metaAdSetId: ad.adset?.id, metaAdId: ad.id,
        whatsappNumber: c.waNumber, status: ad.effective_status === "ACTIVE" ? "ACTIVE" : "PAUSED",
        archived: false, linked: true,
      };
      const doc = existing
        ? await CtwaAd.findByIdAndUpdate(existing._id, { $set: fields }, { new: true })
        : await CtwaAd.create({ ...fields, createdBy: req.user?.username });
      linked.push(doc);
    }
    res.json({ success: true, linked });
  }));

  // Upload creative media: image → image_hash, video → video_id
  app.post("/ctwa/upload-media", ...guard, mediaUpload.single("file"), wrap(async (req, res) => {
    const f = req.file;
    if (!f) throw httpError(400, "No file uploaded");
    const { act } = cfg();
    const form = new FormData();
    const blob = new Blob([f.buffer], { type: f.mimetype });
    if (/^image\//.test(f.mimetype)) {
      form.append("filename", blob, f.originalname);
      const d = await graph("POST", `${act}/adimages`, { form });
      const img = Object.values(d.images || {})[0];
      if (!img?.hash) throw httpError(502, "Meta did not return an image hash");
      return res.json({ success: true, type: "image", imageHash: img.hash, url: img.url });
    }
    if (/^video\//.test(f.mimetype)) {
      form.append("source", blob, f.originalname);
      form.append("name", f.originalname);
      const d = await graph("POST", `${act}/advideos`, { form });
      return res.json({ success: true, type: "video", videoId: d.id });
    }
    throw httpError(400, "Only image or video files are supported");
  }));

  // Create Campaign → Ad set → Creative → Ad, all linked to a CRM course
  app.post("/ctwa/campaigns", ...guard, wrap(async (req, res) => {
    const b = req.body || {};
    const c = cfg();
    if (!c.act || !c.pageId) throw httpError(500, "META_AD_ACCOUNT_ID / META_PAGE_ID are not configured in .env");

    const course = nonEmpty(b.course, "Course");
    const name = nonEmpty(b.name, "Campaign name");
    const objective = OBJECTIVES.includes(b.objective) ? b.objective : "OUTCOME_ENGAGEMENT";
    const goal = (GOALS_BY_OBJECTIVE[objective] || []).includes(b.optimizationGoal) ? b.optimizationGoal : "CONVERSATIONS";
    const status = b.status === "ACTIVE" ? "ACTIVE" : "PAUSED";
    const pageId = (b.pageId || c.pageId).trim();
    const waNumber = (b.whatsappNumber || c.waNumber || "").trim();
    if (!waNumber) throw httpError(500, "CTWA_WHATSAPP_NUMBER is not configured in .env (the WhatsApp Cloud API number the ad should open)");
    const igId = (b.instagramActorId || c.igId || "").trim();
    const cr = b.creative || {};
    const assignedTo = typeof b.assignedTo === "string" && b.assignedTo.trim() ? b.assignedTo.trim() : null;
    if (assignedTo && !(await counsellorList()).includes(assignedTo)) throw httpError(400, `Counsellor "${assignedTo}" not found`);

    const budgetAmount = Number(b.budget?.amount);
    if (!(budgetAmount > 0)) throw httpError(400, "Budget must be greater than 0");
    const lifetime = b.budget?.type === "LIFETIME";
    if (lifetime && !b.schedule?.endTime) throw httpError(400, "End date is required for a lifetime budget");
    const bidStrategy = BID_STRATEGIES.includes(b.bidStrategy) ? b.bidStrategy : "LOWEST_COST_WITHOUT_CAP";
    if (bidStrategy !== "LOWEST_COST_WITHOUT_CAP" && !(Number(b.bidAmount) > 0)) throw httpError(400, "Bid amount is required for the chosen bid strategy");

    const { currency } = await accountInfo();
    const created = { campaign: null, adset: null, creative: null, ad: null };

    try {
      const camp = await graph("POST", `${c.act}/campaigns`, { body: {
        name, objective, status: "PAUSED",
        special_ad_categories: Array.isArray(b.specialAdCategories) ? b.specialAdCategories : [],
        buying_type: "AUCTION",
        // Required by Meta when budget is set at ad set level (no campaign budget).
        is_adset_budget_sharing_enabled: false,
      } });
      created.campaign = camp.id;

      const adsetBody = {
        name: `${name} | Ad set`,
        campaign_id: camp.id,
        status: "PAUSED",
        billing_event: "IMPRESSIONS",
        optimization_goal: goal,
        destination_type: "WHATSAPP",
        bid_strategy: bidStrategy,
        promoted_object: { page_id: pageId, ...(waNumber ? { whatsapp_phone_number: waNumber } : {}) },
        targeting: buildTargeting(b.targeting, b.placements),
        ...(lifetime
          ? { lifetime_budget: toMinor(budgetAmount, currency) }
          : { daily_budget: toMinor(budgetAmount, currency) }),
      };
      if (bidStrategy !== "LOWEST_COST_WITHOUT_CAP") adsetBody.bid_amount = toMinor(b.bidAmount, currency);
      if (b.schedule?.startTime) adsetBody.start_time = new Date(b.schedule.startTime).toISOString();
      if (b.schedule?.endTime) adsetBody.end_time = new Date(b.schedule.endTime).toISOString();
      if (lifetime && Array.isArray(b.adSchedule) && b.adSchedule.length) {
        adsetBody.adset_schedule = b.adSchedule.map(s => ({
          start_minute: Number(s.startMinute), end_minute: Number(s.endMinute), days: s.days, timezone_type: "ADVERTISER",
        }));
        adsetBody.pacing_type = ["day_parting"];
      }
      const adset = await graph("POST", `${c.act}/adsets`, { body: adsetBody });
      created.adset = adset.id;

      const adName = (b.adName || `${name} | Ad`).trim();
      const creative = await graph("POST", `${c.act}/adcreatives`, {
        body: buildCreativePayload({ name: `${adName} | Creative`, c: cr, course, pageId, igId }),
      });
      created.creative = creative.id;

      const ad = await graph("POST", `${c.act}/ads`, { body: {
        name: adName, adset_id: adset.id, creative: { creative_id: creative.id }, status: "PAUSED",
      } });
      created.ad = ad.id;

      // Persist the course mapping BEFORE activating, so no lead can arrive unmapped.
      const doc = await CtwaAd.create({
        name, adName, course, greeting: (b.greeting || "").trim(),
        skipCourseSelection: b.skipCourseSelection !== false, assignedTo,
        objective, optimizationGoal: goal,
        metaCampaignId: camp.id, metaAdSetId: adset.id, metaAdId: ad.id, metaCreativeId: creative.id,
        whatsappNumber: waNumber, status: "PAUSED",
        config: { ...b, creative: { ...cr } },
        createdBy: req.user?.username,
      });

      if (status === "ACTIVE") {
        await graph("POST", camp.id, { body: { status: "ACTIVE" } });
        await graph("POST", adset.id, { body: { status: "ACTIVE" } });
        await graph("POST", ad.id, { body: { status: "ACTIVE" } });
        doc.status = "ACTIVE";
        await doc.save();
      }
      res.json({ success: true, ctwaAd: doc, ids: created });
    } catch (err) {
      // Roll back anything half-created so the Meta account isn't littered.
      for (const id of [created.ad, created.creative, created.adset, created.campaign]) {
        if (id) { try { await graph("DELETE", id); } catch (_) { /* best effort */ } }
      }
      throw err;
    }
  }));

  // Add another ad (creative variant) to an existing CTWA ad set — same course
  app.post("/ctwa/campaigns/:id/ads", ...guard, wrap(async (req, res) => {
    const base = await CtwaAd.findById(req.params.id);
    if (!base || base.archived) throw httpError(404, "Campaign not found");
    const c = cfg();
    const b = req.body || {};
    const cr = b.creative || {};
    const pageId = (base.config?.pageId || c.pageId).trim();
    const igId = (base.config?.instagramActorId || c.igId || "").trim();
    const adName = nonEmpty(b.adName, "Ad name");
    const creative = await graph("POST", `${c.act}/adcreatives`, {
      body: buildCreativePayload({ name: `${adName} | Creative`, c: cr, course: base.course, pageId, igId }),
    });
    const ad = await graph("POST", `${c.act}/ads`, { body: {
      name: adName, adset_id: base.metaAdSetId, creative: { creative_id: creative.id }, status: "PAUSED",
    } });
    const doc = await CtwaAd.create({
      name: base.name, adName, course: base.course, greeting: base.greeting, skipCourseSelection: base.skipCourseSelection, assignedTo: base.assignedTo || null,
      objective: base.objective, optimizationGoal: base.optimizationGoal,
      metaCampaignId: base.metaCampaignId, metaAdSetId: base.metaAdSetId, metaAdId: ad.id, metaCreativeId: creative.id,
      whatsappNumber: base.whatsappNumber, status: "PAUSED",
      config: { ...(base.config || {}), creative: { ...cr } }, createdBy: req.user?.username,
    });
    res.json({ success: true, ctwaAd: doc });
  }));

  // List ads with live Meta status + insights + CRM-attributed leads/enrollments
  app.get("/ctwa/campaigns", ...guard, wrap(async (req, res) => {
    let docs = await CtwaAd.find({ archived: { $ne: true } }).sort({ createdAt: -1 }).lean();
    try {
      if (await syncCampaignAds(docs)) docs = await CtwaAd.find({ archived: { $ne: true } }).sort({ createdAt: -1 }).lean();
    } catch (e) { console.warn("CTWA ad sync skipped:", e.message); }
    const adIds = docs.map(d => d.metaAdId).filter(Boolean);
    let live = {};
    let currency = "INR";
    let warning = "";
    if (adIds.length) {
      try {
        const range = insightRange(req.query);
        const insightsParam = range.time_range
          ? `insights.time_range(${JSON.stringify(range.time_range)}){${INSIGHT_FIELDS}}`
          : `insights.date_preset(${range.date_preset}){${INSIGHT_FIELDS}}`;
        for (let i = 0; i < adIds.length; i += 40) {
          const chunk = adIds.slice(i, i + 40);
          const d = await graph("GET", "", { params: {
            ids: chunk.join(","),
            fields: `name,status,effective_status,adset{daily_budget,lifetime_budget,start_time,end_time,effective_status,bid_strategy},campaign{effective_status},${insightsParam}`,
          } });
          Object.assign(live, d);
        }
        currency = (await accountInfo()).currency || "INR";
      } catch (e) { warning = e.message; }
    }
    const leadAgg = await Chat.aggregate([
      { $match: { "ctwa.adId": { $in: adIds } } },
      { $group: { _id: "$ctwa.adId", leads: { $sum: 1 }, enrolled: { $sum: { $cond: [{ $eq: ["$pipeline", "Enrolled"] }, 1, 0] } } } },
    ]);
    const leadMap = Object.fromEntries(leadAgg.map(a => [a._id, a]));

    const rows = docs.map(d => {
      const l = live[d.metaAdId] || {};
      const ins = parseInsight(l.insights?.data?.[0], currency);
      const adset = l.adset || {};
      const leads = leadMap[d.metaAdId]?.leads || 0;
      const enrolled = leadMap[d.metaAdId]?.enrolled || 0;
      return {
        ...d,
        effectiveStatus: l.effective_status || d.status,
        campaignStatus: l.campaign?.effective_status || null,
        adsetStatus: adset.effective_status || null,
        budget: adset.daily_budget
          ? { type: "DAILY", amount: fromMinor(adset.daily_budget, currency) }
          : adset.lifetime_budget ? { type: "LIFETIME", amount: fromMinor(adset.lifetime_budget, currency) } : null,
        startTime: adset.start_time || null,
        endTime: adset.end_time || null,
        insights: ins,
        crm: {
          leads, enrolled,
          costPerLead: leads ? Math.round((ins.spend / leads) * 100) / 100 : 0,
          costPerEnrollment: enrolled ? Math.round((ins.spend / enrolled) * 100) / 100 : 0,
        },
      };
    });
    const totals = rows.reduce((t, r) => {
      t.spend += r.insights.spend; t.impressions += r.insights.impressions; t.reach += r.insights.reach;
      t.clicks += r.insights.clicks; t.conversations += r.insights.conversations;
      t.leads += r.crm.leads; t.enrolled += r.crm.enrolled;
      return t;
    }, { spend: 0, impressions: 0, reach: 0, clicks: 0, conversations: 0, leads: 0, enrolled: 0 });
    res.json({ success: true, currency, warning, totals, rows });
  }));

  // Daily insights + breakdowns for one ad
  app.get("/ctwa/campaigns/:id/insights", ...guard, wrap(async (req, res) => {
    const doc = await CtwaAd.findById(req.params.id).lean();
    if (!doc) throw httpError(404, "Not found");
    const range = insightRange(req.query);
    const breakdown = ["age", "gender", "publisher_platform", "platform_position", "region", "device_platform"].includes(req.query.breakdown)
      ? req.query.breakdown : null;
    const params = { fields: INSIGHT_FIELDS, limit: 200, ...range };
    if (breakdown) params.breakdowns = breakdown; else params.time_increment = 1;
    const d = await graph("GET", `${doc.metaAdId}/insights`, { params });
    const { currency } = await accountInfo();
    const rows = (d.data || []).map(r => ({
      date: r.date_start,
      breakdown: breakdown ? (r[breakdown] || "unknown") : null,
      ...parseInsight(r, currency),
    }));
    res.json({ success: true, currency, rows });
  }));

  // Pause / resume (campaign + ad set + ad together)
  app.patch("/ctwa/campaigns/:id/status", ...guard, wrap(async (req, res) => {
    const status = req.body?.status;
    if (!["ACTIVE", "PAUSED"].includes(status)) throw httpError(400, "status must be ACTIVE or PAUSED");
    const doc = await CtwaAd.findById(req.params.id);
    if (!doc || doc.archived) throw httpError(404, "Not found");
    const siblings = await CtwaAd.find({ metaCampaignId: doc.metaCampaignId, archived: { $ne: true } });
    if (req.body?.scope === "ad") {
      await graph("POST", doc.metaAdId, { body: { status } });
      doc.status = status; await doc.save();
    } else {
      await graph("POST", doc.metaCampaignId, { body: { status } });
      await graph("POST", doc.metaAdSetId, { body: { status } });
      for (const s of siblings) { await graph("POST", s.metaAdId, { body: { status } }); s.status = status; await s.save(); }
    }
    res.json({ success: true, status });
  }));

  // Edit name, budget, schedule, bot settings (course / greeting)
  app.patch("/ctwa/campaigns/:id", ...guard, wrap(async (req, res) => {
    const doc = await CtwaAd.findById(req.params.id);
    if (!doc || doc.archived) throw httpError(404, "Not found");
    const b = req.body || {};
    const { currency } = await accountInfo();

    if (typeof b.course === "string" && b.course.trim()) {
      // course applies to every ad in the campaign — they share one ad set / audience
      await CtwaAd.updateMany(doc.linked ? { _id: doc._id } : { metaCampaignId: doc.metaCampaignId }, { $set: { course: b.course.trim() } });
      doc.course = b.course.trim();
    }
    if (typeof b.assignedTo === "string") {
      const a = b.assignedTo.trim();
      if (a && !(await counsellorList()).includes(a)) throw httpError(400, `Counsellor "${a}" not found`);
      await CtwaAd.updateMany(doc.linked ? { _id: doc._id } : { metaCampaignId: doc.metaCampaignId }, { $set: { assignedTo: a || null } });
      doc.assignedTo = a || null;
    }
    if (typeof b.greeting === "string") doc.greeting = b.greeting.trim();
    if (typeof b.skipCourseSelection === "boolean") doc.skipCourseSelection = b.skipCourseSelection;

    if (typeof b.name === "string" && b.name.trim()) {
      await graph("POST", doc.metaCampaignId, { body: { name: b.name.trim() } });
      await CtwaAd.updateMany({ metaCampaignId: doc.metaCampaignId }, { $set: { name: b.name.trim() } });
      doc.name = b.name.trim();
    }
    const adsetPatch = {};
    if (b.budget?.amount > 0) {
      adsetPatch[b.budget.type === "LIFETIME" ? "lifetime_budget" : "daily_budget"] = toMinor(b.budget.amount, currency);
    }
    if (b.schedule?.startTime) adsetPatch.start_time = new Date(b.schedule.startTime).toISOString();
    if (b.schedule?.endTime) adsetPatch.end_time = new Date(b.schedule.endTime).toISOString();
    if (Object.keys(adsetPatch).length) await graph("POST", doc.metaAdSetId, { body: adsetPatch });

    await doc.save();
    res.json({ success: true, ctwaAd: doc });
  }));

  // Archive (stops delivery, keeps history on Meta). Local mapping kept so old leads still attribute.
  app.delete("/ctwa/campaigns/:id", ...guard, wrap(async (req, res) => {
    const doc = await CtwaAd.findById(req.params.id);
    if (!doc) throw httpError(404, "Not found");
    if (doc.linked) {
      // Linked ads belong to Ads Manager: only unlink from the CRM, never touch the live ad on Meta.
      await CtwaAd.updateOne({ _id: doc._id }, { $set: { archived: true } });
      return res.json({ success: true, unlinked: true });
    }
    try { await graph("POST", doc.metaCampaignId, { body: { status: "ARCHIVED" } }); }
    catch (e) { console.warn("CTWA archive on Meta failed:", e.message); }
    await CtwaAd.updateMany({ metaCampaignId: doc.metaCampaignId }, { $set: { archived: true, status: "ARCHIVED" } });
    res.json({ success: true });
  }));

  // Ad preview (iframe HTML from Meta)
  app.get("/ctwa/campaigns/:id/preview", ...guard, wrap(async (req, res) => {
    const doc = await CtwaAd.findById(req.params.id).lean();
    if (!doc) throw httpError(404, "Not found");
    const fmt = ["MOBILE_FEED_STANDARD", "INSTAGRAM_STANDARD", "INSTAGRAM_STORY", "FACEBOOK_STORY_MOBILE"].includes(req.query.format)
      ? req.query.format : "MOBILE_FEED_STANDARD";
    const d = await graph("GET", `${doc.metaAdId}/previews`, { params: { ad_format: fmt } });
    res.json({ success: true, html: d.data?.[0]?.body || "" });
  }));

  // Leads that came in through a campaign's ads
  app.get("/ctwa/campaigns/:id/leads", ...guard, wrap(async (req, res) => {
    const doc = await CtwaAd.findById(req.params.id).lean();
    if (!doc) throw httpError(404, "Not found");
    const ads = await CtwaAd.find({ metaCampaignId: doc.metaCampaignId }).select("metaAdId").lean();
    const leads = await Chat.find({ "ctwa.adId": { $in: ads.map(a => a.metaAdId) } })
      .select("userName userNumber course status pipeline assignedto datecreated ctwa")
      .sort({ datecreated: -1 }).limit(500).lean();
    res.json({ success: true, leads });
  }));
}

module.exports = { registerCtwaRoutes, CtwaAd, _internals: { buildTargeting, buildCreativePayload } };
