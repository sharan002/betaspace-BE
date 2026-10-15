const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');

// S3 support (optional, mirrors certificateGenerator.js setup)
let s3Client = null;
let PutObjectCommand = null;
const USE_S3 = !!process.env.S3_BUCKET || process.env.USE_S3 === 'true';
if (USE_S3) {
  try {
    const aws = require('@aws-sdk/client-s3');
    PutObjectCommand = aws.PutObjectCommand;
    s3Client = new aws.S3Client({ region: process.env.S3_REGION || 'us-east-1' });
  } catch (err) {
    s3Client = null;
    PutObjectCommand = null;
  }
}

const COURSE_TEMPLATE_DIR = path.join(__dirname, 'Course_Enrollment');
const PAGE_W    = 595.28;
const PAGE_H    = 841.89;
const MARGIN    = 85;
const CONTENT_W = PAGE_W - MARGIN * 2;
const START_Y   = MARGIN + 110;

// ─── Template Resolution ─────────────────────────────────────────────────────

function resolveCourseTemplatePath(course) {
  if (!course || !fs.existsSync(COURSE_TEMPLATE_DIR)) return null;
  const courseName = String(course).trim();
  const files = fs.readdirSync(COURSE_TEMPLATE_DIR);
  const exact = files.find(f => f === `${courseName}.txt`);
  if (exact) return path.join(COURSE_TEMPLATE_DIR, exact);
  const loose = files.find(f => f.toLowerCase() === `${courseName.toLowerCase()}.txt`);
  if (loose) return path.join(COURSE_TEMPLATE_DIR, loose);
  return null;
}

// Parse the correct section from the template based on programType.
// Sections are delimited by headers like "8 Hours:" or "2 Hours :"
function parseProgramTypeSection(raw, programType) {
  const headerRe = /^(\d+)\s*Hours?\s*:/gim;
  const headers = [];
  let m;
  while ((m = headerRe.exec(raw)) !== null) {
    headers.push({ hours: parseInt(m[1]), headerStart: m.index, contentStart: m.index + m[0].length });
  }

  if (headers.length === 0) return raw;

  const sectMap = {};
  for (let i = 0; i < headers.length; i++) {
    const start = headers[i].contentStart;
    const end = i + 1 < headers.length ? headers[i + 1].headerStart : raw.length;
    sectMap[headers[i].hours] = raw.slice(start, end).trim();
  }

  // Extract hour number from programType (handles "8 Hours", "8hr", "2 Hours", etc.)
  const ptNum = parseInt((String(programType || '')).match(/(\d+)/)?.[1] || '');
  if (ptNum && sectMap[ptNum]) return sectMap[ptNum];

  // Default to first section
  return Object.values(sectMap)[0] || raw;
}

// ─── PDF Helpers (self-contained, no dependency on certificateGenerator.js) ──

function parseBgUrl(raw) {
  const m = raw.match(/^BACKGROUND\s*[-–]\s*(https?:\/\/\S+)/mi);
  return m ? m[1].trim() : null;
}

function stripBgLine(text) {
  return text.replace(/^[ \t]*BACKGROUND\s*[-–]\s*https?:\/\/\S+[ \t]*\r?\n?/mi, '');
}

async function fetchImageBuffer(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? require('https') : require('http');
    const req = mod.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchImageBuffer(res.headers.location).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('error', reject);
  });
}

function drawBackground(doc, bgBuffer) {
  doc.image(bgBuffer, 0, 0, { width: PAGE_W, height: PAGE_H });
}

function buildPdfBuffer(doc) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    doc.end();
  });
}

function parseInlineSegments(str) {
  const segments = [];
  const re = /\*([^*]+)\*/g;
  let last = 0, match;
  while ((match = re.exec(str)) !== null) {
    if (match.index > last) segments.push({ text: str.slice(last, match.index), bold: false });
    segments.push({ text: match[1], bold: true });
    last = match.index + match[0].length;
  }
  if (last < str.length) segments.push({ text: str.slice(last), bold: false });
  return segments;
}

function renderInlineText(doc, text, x, y, width, align, fontSize, lineGap = 0) {
  const segments = parseInlineSegments(text);
  if (segments.length === 1 && !segments[0].bold) {
    doc.font('Helvetica').fontSize(fontSize).text(text, x, y, { width, align, lineGap });
    return;
  }
  for (let s = 0; s < segments.length; s++) {
    const seg = segments[s];
    const isLast = s === segments.length - 1;
    doc.font(seg.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(fontSize);
    if (s === 0) {
      doc.text(seg.text, x, y, { width, align, lineGap, continued: !isLast, lineBreak: isLast });
    } else {
      doc.text(seg.text, { width, align, lineGap, continued: !isLast, lineBreak: isLast });
    }
  }
}

// Font sizes
const FONT_TITLE  = 18;   // "Certificate of Enrollment"
const FONT_HEADER = 12;   // section headers like "Course Highlights:"
const FONT_BODY   = 11.5; // justified body paragraphs
const FONT_BULLET = 11;   // bullet list items

// Renders certificate content for course enrollment templates.
function renderCourseContent(doc, text) {
  let y = START_Y;
  const lines = text.split('\n');

  for (let i = 0; i < lines.length; i++) {
    const rawLine = lines[i];
    const line = rawLine.trim();

    if (!line) {
      y += 8;
      continue;
    }

    const plain      = line.replace(/\*([^*]+)\*/g, '$1');
    const isIndented = /^[ \t]+\S/.test(rawLine);
    const isThanking = /^thank/i.test(plain);
    const hasLower   = /[a-z]/.test(plain);
    const isAllCaps  = !hasLower && /[A-Z]/.test(plain) && plain.replace(/[\s\d:.\-&()]/g, '').length > 2;
    const isCertTitle = /^certificate\s+of\s+enrollment/i.test(plain);
    // Entire line wrapped in *...* e.g. *Course Highlights:* or *Advanced Training...*
    const isFullBold   = !isIndented && /^\*[^*]+\*$/.test(line);
    // Short non-bold line ending with colon e.g. "Course Highlights:"
    const isHeaderColon = !isIndented && !isFullBold && plain.endsWith(':') && plain.length <= 65;

    if (isThanking) {
      renderInlineText(doc, plain, MARGIN, y, CONTENT_W, 'center', FONT_BODY);
      y = doc.y + 24;

    } else if (isCertTitle || isAllCaps) {
      // Large centred underlined title
      doc.font('Helvetica-Bold').fontSize(FONT_TITLE);
      doc.text(plain, MARGIN, y, { width: CONTENT_W, align: 'center', underline: true });
      y = doc.y + 18;

    } else if (isFullBold || isHeaderColon) {
      // Section header — bold, left-aligned, never joined with the next line
      const headerText = line.replace(/^\*|\*$/g, '');
      doc.font('Helvetica-Bold').fontSize(FONT_HEADER);
      doc.text(headerText, MARGIN, y, { width: CONTENT_W, align: 'left' });
      y = doc.y + 10;

    } else if (isIndented) {
      // Bullet item with hanging indent:
      // • is drawn at bulletX; text starts at textX so wrapped lines indent under text, not bullet
      const bulletX = MARGIN + 12;
      const textX   = MARGIN + 24;
      const textW   = CONTENT_W - 24;
      doc.font('Helvetica').fontSize(FONT_BULLET).text('•', bulletX, y, { lineBreak: false });
      renderInlineText(doc, ' ' + line, textX, y, textW, 'left', FONT_BULLET, 2);
      y = doc.y + 4;

    } else {
      // Body paragraph — collect consecutive body lines into one block, then justify
      const paraLines = [line];
      while (i + 1 < lines.length) {
        const nextRaw   = lines[i + 1];
        const next      = nextRaw.trim();
        if (!next) break;
        if (/^[ \t]+\S/.test(nextRaw)) break;                                         // indented → bullet
        const nextPlain = next.replace(/\*([^*]+)\*/g, '$1');
        if (/^thank/i.test(nextPlain)) break;
        if (/^\*[^*]+\*$/.test(next)) break;                                          // full-bold header
        if (nextPlain.endsWith(':') && nextPlain.length <= 65) break;                 // colon header
        if (!/[a-z]/.test(nextPlain) && /[A-Z]/.test(nextPlain) && nextPlain.replace(/[\s\d:.\-&()]/g, '').length > 2) break; // ALL-CAPS line
        if (/^certificate\s+of\s+enrollment/i.test(nextPlain)) break;
        paraLines.push(next);
        i++;
      }
      renderInlineText(doc, paraLines.join(' '), MARGIN, y, CONTENT_W, 'justify', FONT_BODY, 4);
      y = doc.y + 14;
    }
  }
}

// ─── S3 Upload ────────────────────────────────────────────────────────────────

async function uploadToS3(buffer, fileName) {
  if (!USE_S3 || !s3Client || !PutObjectCommand) return null;
  const bucket = process.env.S3_BUCKET;
  if (!bucket) return null;
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: bucket,
      Key: fileName,
      Body: buffer,
      ContentType: 'application/pdf',
    }));
    return `https://${bucket}.s3.${process.env.S3_REGION || 'us-east-1'}.amazonaws.com/${encodeURIComponent(fileName)}`;
  } catch (err) {
    console.warn('S3 upload failed for course certificate:', err.message);
    return null;
  }
}

// ─── Main Export ──────────────────────────────────────────────────────────────

// Generates a course enrollment certificate PDF for the given user and course.
// Returns: { buffer, fileName, s3Url, localPath }
// - s3Url is set if S3 is configured; localPath is set otherwise.
// Throws if no matching template is found for the course.
async function generateCourseCertificate({ userName, course, programType, userId }) {
  const templatePath = resolveCourseTemplatePath(course);
  if (!templatePath) {
    throw new Error(
      `No enrollment certificate template found for course: "${course}". ` +
      `Add a matching file inside Course_Enrollment/`
    );
  }

  const raw = fs.readFileSync(templatePath, 'utf8');
  const sectionContent = parseProgramTypeSection(raw, programType);

  const bgUrl = parseBgUrl(sectionContent);
  let bgBuffer = null;
  if (bgUrl) {
    try {
      bgBuffer = await fetchImageBuffer(bgUrl);
    } catch (e) {
      console.warn('Course certificate background image fetch failed:', e.message);
    }
  }

  const filled = stripBgLine(sectionContent)
    .replace(/\[userName\]/gi, userName || '')
    .replace(/\[course\]/gi, course || '');

  const doc = new PDFDocument({ size: 'A4', margin: 0 });
  if (bgBuffer) drawBackground(doc, bgBuffer);
  renderCourseContent(doc, filled);

  const buffer = await buildPdfBuffer(doc);

  const safeCourse = String(course || 'course').replace(/[^a-zA-Z0-9_\-]/g, '_');
  const fileName = `enrollment_cert_${userId || 'unknown'}_${safeCourse}_${Date.now()}.pdf`;

  let s3Url = null;
  let localPath = null;

  if (USE_S3) {
    s3Url = await uploadToS3(buffer, fileName);
  } else {
    const outDir = path.join(__dirname, 'bills');
    if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
    localPath = path.join(outDir, fileName);
    fs.writeFileSync(localPath, buffer);
  }

  return { buffer, fileName, s3Url, localPath };
}

module.exports = { generateCourseCertificate };
