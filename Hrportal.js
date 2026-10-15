// Hrportal.js
// ---------------------------------------------------------------------------
// Self-contained "HR Portal" feature: manage employee records + documents.
//
// Everything here is isolated from the rest of the CRM:
//   - its own Mongoose model  ("HrEmployee" -> collection "hremployees")
//   - its own routes, all mounted under  /hr/*  and gated by the shared
//     `auth` middleware + an admin-role check
//   - its own tiny S3 helper (documents/photos are stored in the same bucket
//     the rest of the app already uses, under the  hrportal/  key prefix)
//
// Wiring (already added to index.js):
//     const { registerHrPortalRoutes } = require("./Hrportal");
//     registerHrPortalRoutes(app, auth);
//
// Required env (already present for the certificate feature):
//     S3_BUCKET, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
// If S3 is not configured the upload endpoints return a clear 500 and the
// rest of the HR portal (text fields) keeps working.
// ---------------------------------------------------------------------------

const mongoose = require("mongoose");
const multer = require("multer");

// --- S3 (optional, lazy) ---------------------------------------------------
let s3Client = null;
let PutObjectCommand = null;
let GetObjectCommand = null;
let DeleteObjectCommand = null;
let s3InitTried = false;

function getS3() {
  if (s3InitTried) return s3Client;
  s3InitTried = true;
  try {
    const aws = require("@aws-sdk/client-s3");
    PutObjectCommand = aws.PutObjectCommand;
    GetObjectCommand = aws.GetObjectCommand;
    DeleteObjectCommand = aws.DeleteObjectCommand;
    s3Client = new aws.S3Client({ region: process.env.S3_REGION || "us-east-1" });
  } catch (err) {
    console.warn("[HrPortal] AWS SDK unavailable:", err.message || err);
    s3Client = null;
  }
  return s3Client;
}

const S3_BUCKET = process.env.S3_BUCKET;

async function s3Upload(buffer, key, contentType) {
  const client = getS3();
  if (!client || !S3_BUCKET) {
    return { uploaded: false, error: "S3 is not configured on the server" };
  }
  await client.send(
    new PutObjectCommand({ Bucket: S3_BUCKET, Key: key, Body: buffer, ContentType: contentType })
  );
  return { uploaded: true, key };
}

async function s3Delete(key) {
  const client = getS3();
  if (!client || !S3_BUCKET || !key) return;
  try {
    await client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: key }));
  } catch (err) {
    console.warn(`[HrPortal] S3 delete failed for "${key}":`, err.message || err);
  }
}

async function s3GetStream(key) {
  const client = getS3();
  if (!client || !S3_BUCKET) throw new Error("S3 is not configured on the server");
  const out = await client.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
  return out; // out.Body is a Node Readable stream in the v3 SDK
}

// --- Schema --------------------------------------------------------------
const hrDocumentSchema = new mongoose.Schema(
  {
    docType: { type: String, trim: true, default: "other" }, // aadhar | pan | educational | bank | photo | other | free text
    label: { type: String, trim: true, default: "" },
    fileName: { type: String, trim: true, default: "" },
    key: { type: String, trim: true, default: "" }, // S3 object key
    contentType: { type: String, trim: true, default: "" },
    size: { type: Number, default: 0 },
    uploadedAt: { type: Date, default: Date.now },
  },
  { _id: true }
);

const hrEducationSchema = new mongoose.Schema(
  {
    qualification: { type: String, trim: true, default: "" },
    institution: { type: String, trim: true, default: "" },
    specialization: { type: String, trim: true, default: "" },
    yearOfPassing: { type: String, trim: true, default: "" },
    percentage: { type: String, trim: true, default: "" },
  },
  { _id: true }
);

const hrPreviousOrgSchema = new mongoose.Schema(
  {
    organization: { type: String, trim: true, default: "" },
    designation: { type: String, trim: true, default: "" },
    fromDate: { type: String, trim: true, default: "" },
    toDate: { type: String, trim: true, default: "" },
    lastCtc: { type: String, trim: true, default: "" },
    reasonForLeaving: { type: String, trim: true, default: "" },
  },
  { _id: true }
);

const hrEmployeeSchema = new mongoose.Schema(
  {
    name: { type: String, trim: true, required: true },
    dateOfBirth: { type: Date, default: null },
    designation: { type: String, trim: true, default: "" },
    dateOfJoining: { type: Date, default: null },
    sex: { type: String, enum: ["male", "female", ""], default: "" },
    maritalStatus: { type: String, trim: true, default: "" },
    bloodGroup: { type: String, trim: true, default: "" },
    age: { type: Number, default: null },

    idCardNumber: { type: String, trim: true, default: "" },
    email: { type: String, trim: true, lowercase: true, default: "" },
    contactNumber: { type: String, trim: true, default: "" },
    emergencyContactNumber: { type: String, trim: true, default: "" },

    aadharNumber: { type: String, trim: true, default: "" },
    panNumber: { type: String, trim: true, default: "" },

    bankDetails: {
      accountHolderName: { type: String, trim: true, default: "" },
      accountNumber: { type: String, trim: true, default: "" },
      bankName: { type: String, trim: true, default: "" },
      branch: { type: String, trim: true, default: "" },
      ifsc: { type: String, trim: true, default: "" },
    },

    previousOrganizations: { type: [hrPreviousOrgSchema], default: [] },
    education: { type: [hrEducationSchema], default: [] },

    photo: {
      key: { type: String, trim: true, default: "" },
      contentType: { type: String, trim: true, default: "" },
      uploadedAt: { type: Date, default: null },
    },

    documents: { type: [hrDocumentSchema], default: [] },

    notes: { type: String, trim: true, default: "" },
    active: { type: Boolean, default: true },

    createdBy: { type: String, trim: true, default: "" },
    updatedBy: { type: String, trim: true, default: "" },
  },
  { timestamps: true, collection: "hremployees" }
);

const HrEmployee =
  mongoose.models.HrEmployee || mongoose.model("HrEmployee", hrEmployeeSchema);

// --- helpers ------------------------------------------------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20 MB / file
});

const EDITABLE_FIELDS = [
  "name",
  "dateOfBirth",
  "designation",
  "dateOfJoining",
  "sex",
  "maritalStatus",
  "bloodGroup",
  "age",
  "idCardNumber",
  "email",
  "contactNumber",
  "emergencyContactNumber",
  "aadharNumber",
  "panNumber",
  "notes",
  "active",
];

function pickBody(body) {
  const out = {};
  for (const f of EDITABLE_FIELDS) {
    if (body[f] !== undefined) out[f] = body[f] === "" ? "" : body[f];
  }
  if (body.sex !== undefined) {
    const s = String(body.sex || "").toLowerCase();
    out.sex = ["male", "female"].includes(s) ? s : "";
  }
  if (body.age !== undefined) {
    const n = Number(body.age);
    out.age = Number.isFinite(n) && n > 0 ? n : null;
  }
  for (const d of ["dateOfBirth", "dateOfJoining"]) {
    if (body[d] !== undefined) {
      out[d] = body[d] ? new Date(body[d]) : null;
    }
  }
  if (body.bankDetails && typeof body.bankDetails === "object") {
    out.bankDetails = {
      accountHolderName: body.bankDetails.accountHolderName || "",
      accountNumber: body.bankDetails.accountNumber || "",
      bankName: body.bankDetails.bankName || "",
      branch: body.bankDetails.branch || "",
      ifsc: body.bankDetails.ifsc || "",
    };
  }
  if (Array.isArray(body.previousOrganizations)) {
    out.previousOrganizations = body.previousOrganizations
      .filter((o) => o && (o.organization || o.designation))
      .map((o) => ({
        organization: o.organization || "",
        designation: o.designation || "",
        fromDate: o.fromDate || "",
        toDate: o.toDate || "",
        lastCtc: o.lastCtc || "",
        reasonForLeaving: o.reasonForLeaving || "",
      }));
  }
  if (Array.isArray(body.education)) {
    out.education = body.education
      .filter((e) => e && (e.qualification || e.institution))
      .map((e) => ({
        qualification: e.qualification || "",
        institution: e.institution || "",
        specialization: e.specialization || "",
        yearOfPassing: e.yearOfPassing || "",
        percentage: e.percentage || "",
      }));
  }
  return out;
}

function listItem(e) {
  return {
    _id: e._id,
    name: e.name,
    designation: e.designation,
    email: e.email,
    contactNumber: e.contactNumber,
    dateOfJoining: e.dateOfJoining,
    idCardNumber: e.idCardNumber,
    active: e.active,
    hasPhoto: !!(e.photo && e.photo.key),
    documentCount: Array.isArray(e.documents) ? e.documents.length : 0,
    updatedAt: e.updatedAt,
  };
}

// --- routes ------------------------------------------------------------
function registerHrPortalRoutes(app, auth) {
  if (!app || typeof app.use !== "function") {
    throw new Error("registerHrPortalRoutes(app, auth): valid Express app required");
  }
  const authMw = typeof auth === "function" ? auth : (req, res, next) => next();

  // Admin/HR-only guard for the whole portal
  const adminOnly = (req, res, next) => {
    if (req.user && (req.user.role === "admin" || req.user.role === "hr")) return next();
    return res.status(403).json({ success: false, message: "Admin access required" });
  };

  const guard = [authMw, adminOnly];

  // List employees
  app.get("/hr/employees", guard, async (req, res) => {
    try {
      const q = (req.query.q || "").trim();
      const filter = {};
      if (q) {
        const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
        filter.$or = [{ name: rx }, { email: rx }, { designation: rx }, { contactNumber: rx }, { idCardNumber: rx }];
      }
      const employees = await HrEmployee.find(filter).sort({ createdAt: -1 }).lean();
      res.json({ success: true, employees: employees.map(listItem) });
    } catch (err) {
      console.error("[HrPortal] list error:", err);
      res.status(500).json({ success: false, message: "Failed to load employees" });
    }
  });

  // Single employee (full record)
  app.get("/hr/employees/:id", guard, async (req, res) => {
    try {
      const emp = await HrEmployee.findById(req.params.id).lean();
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });
      res.json({ success: true, employee: emp });
    } catch (err) {
      console.error("[HrPortal] get error:", err);
      res.status(500).json({ success: false, message: "Failed to load employee" });
    }
  });

  // Create
  app.post("/hr/employees", guard, async (req, res) => {
    try {
      const data = pickBody(req.body);
      if (!data.name || !String(data.name).trim()) {
        return res.status(400).json({ success: false, message: "Name is required" });
      }
      data.createdBy = req.user?.username || "";
      data.updatedBy = req.user?.username || "";
      const emp = await HrEmployee.create(data);
      res.json({ success: true, employee: emp.toObject() });
    } catch (err) {
      console.error("[HrPortal] create error:", err);
      res.status(500).json({ success: false, message: "Failed to create employee" });
    }
  });

  // Update
  app.put("/hr/employees/:id", guard, async (req, res) => {
    try {
      const data = pickBody(req.body);
      data.updatedBy = req.user?.username || "";
      const emp = await HrEmployee.findByIdAndUpdate(req.params.id, { $set: data }, { new: true });
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });
      res.json({ success: true, employee: emp.toObject() });
    } catch (err) {
      console.error("[HrPortal] update error:", err);
      res.status(500).json({ success: false, message: "Failed to update employee" });
    }
  });

  // Delete (also removes S3 objects)
  app.delete("/hr/employees/:id", guard, async (req, res) => {
    try {
      const emp = await HrEmployee.findById(req.params.id);
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });
      const keys = [];
      if (emp.photo && emp.photo.key) keys.push(emp.photo.key);
      (emp.documents || []).forEach((d) => d.key && keys.push(d.key));
      await Promise.all(keys.map((k) => s3Delete(k)));
      await emp.deleteOne();
      res.json({ success: true });
    } catch (err) {
      console.error("[HrPortal] delete error:", err);
      res.status(500).json({ success: false, message: "Failed to delete employee" });
    }
  });

  // Upload / replace photo
  app.post("/hr/employees/:id/photo", guard, upload.single("photo"), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ success: false, message: "photo file is required" });
      const emp = await HrEmployee.findById(req.params.id);
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });

      const ext = (req.file.mimetype.split("/")[1] || "jpg").replace("jpeg", "jpg");
      const key = `hrportal/${emp._id}/photo_${Date.now()}.${ext}`;
      const up = await s3Upload(req.file.buffer, key, req.file.mimetype);
      if (!up.uploaded) return res.status(500).json({ success: false, message: up.error || "Upload failed" });

      const oldKey = emp.photo && emp.photo.key;
      emp.photo = { key, contentType: req.file.mimetype, uploadedAt: new Date() };
      emp.updatedBy = req.user?.username || "";
      await emp.save();
      if (oldKey && oldKey !== key) await s3Delete(oldKey);

      res.json({ success: true, photo: emp.photo });
    } catch (err) {
      console.error("[HrPortal] photo upload error:", err);
      res.status(500).json({ success: false, message: "Failed to upload photo" });
    }
  });

  // Stream photo
  app.get("/hr/employees/:id/photo", guard, async (req, res) => {
    try {
      const emp = await HrEmployee.findById(req.params.id).lean();
      if (!emp || !emp.photo || !emp.photo.key) {
        return res.status(404).json({ success: false, message: "No photo" });
      }
      const obj = await s3GetStream(emp.photo.key);
      if (obj.ContentType) res.setHeader("Content-Type", obj.ContentType);
      res.setHeader("Cache-Control", "private, max-age=300");
      obj.Body.pipe(res);
    } catch (err) {
      console.error("[HrPortal] photo stream error:", err);
      res.status(500).json({ success: false, message: "Failed to load photo" });
    }
  });

  // Upload a document
  app.post("/hr/employees/:id/documents", guard, upload.single("document"), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ success: false, message: "document file is required" });
      const emp = await HrEmployee.findById(req.params.id);
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });

      const safeName = (req.file.originalname || "document").replace(/[^\w.\-() ]+/g, "_").slice(0, 120);
      const key = `hrportal/${emp._id}/docs/${Date.now()}_${safeName}`;
      const up = await s3Upload(req.file.buffer, key, req.file.mimetype);
      if (!up.uploaded) return res.status(500).json({ success: false, message: up.error || "Upload failed" });

      const doc = {
        docType: (req.body.docType || "other").trim(),
        label: (req.body.label || "").trim(),
        fileName: safeName,
        key,
        contentType: req.file.mimetype,
        size: req.file.size,
        uploadedAt: new Date(),
      };
      emp.documents.push(doc);
      emp.updatedBy = req.user?.username || "";
      await emp.save();

      res.json({ success: true, document: emp.documents[emp.documents.length - 1] });
    } catch (err) {
      console.error("[HrPortal] document upload error:", err);
      res.status(500).json({ success: false, message: "Failed to upload document" });
    }
  });

  // Download a document (streamed through the API, auth-gated)
  app.get("/hr/employees/:id/documents/:docId", guard, async (req, res) => {
    try {
      const emp = await HrEmployee.findById(req.params.id).lean();
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });
      const doc = (emp.documents || []).find((d) => String(d._id) === String(req.params.docId));
      if (!doc || !doc.key) return res.status(404).json({ success: false, message: "Document not found" });

      const obj = await s3GetStream(doc.key);
      res.setHeader("Content-Type", doc.contentType || obj.ContentType || "application/octet-stream");
      const disposition = req.query.inline === "1" ? "inline" : "attachment";
      res.setHeader("Content-Disposition", `${disposition}; filename="${doc.fileName || "document"}"`);
      obj.Body.pipe(res);
    } catch (err) {
      console.error("[HrPortal] document download error:", err);
      res.status(500).json({ success: false, message: "Failed to download document" });
    }
  });

  // Delete a document
  app.delete("/hr/employees/:id/documents/:docId", guard, async (req, res) => {
    try {
      const emp = await HrEmployee.findById(req.params.id);
      if (!emp) return res.status(404).json({ success: false, message: "Employee not found" });
      const doc = emp.documents.id(req.params.docId);
      if (!doc) return res.status(404).json({ success: false, message: "Document not found" });
      const key = doc.key;
      doc.deleteOne();
      emp.updatedBy = req.user?.username || "";
      await emp.save();
      if (key) await s3Delete(key);
      res.json({ success: true });
    } catch (err) {
      console.error("[HrPortal] document delete error:", err);
      res.status(500).json({ success: false, message: "Failed to delete document" });
    }
  });

  console.log("[HrPortal] routes mounted under /hr");
}

module.exports = { registerHrPortalRoutes, HrEmployee };
