const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');

// Minimal S3 support (optional)
let s3Client = null;
let PutObjectCommand = null;
let DeleteObjectCommand = null;
const USE_S3 = !!process.env.S3_BUCKET || process.env.USE_S3 === 'true';
if (USE_S3) {
  try {
    const aws = require('@aws-sdk/client-s3');
    const { S3Client } = aws;
    PutObjectCommand = aws.PutObjectCommand || aws.PutObject || aws.PutObjectCommand;
    DeleteObjectCommand = aws.DeleteObjectCommand;
    s3Client = new S3Client({ region: process.env.S3_REGION || 'us-east-1' });
  } catch (err) {
    s3Client = null;
    PutObjectCommand = null;
    DeleteObjectCommand = null;
  }
}

function safeValue(key, intern) {
  if (!intern) return '';
  if (key === '_id') return normalizePlaceholderValue(intern._id || intern.id || '');
  if (intern.hasOwnProperty(key)) return normalizePlaceholderValue(intern[key]);
  const lower = key.toLowerCase();
  if (intern.hasOwnProperty(lower)) return normalizePlaceholderValue(intern[lower]);
  const underscored = key.replace(/\s+/g, '_').toLowerCase();
  if (intern.hasOwnProperty(underscored)) return normalizePlaceholderValue(intern[underscored]);
  return '';
}

function normalizePlaceholderValue(value) {
  if (value === undefined || value === null) return '';
  if (typeof value !== 'string') return value;
  return value.replace(/\s+/g, ' ').trim();
}

function formatDateVal(val) {
  if (!val) return '';
  const d = (val instanceof Date) ? val : (isNaN(Date.parse(val)) ? null : new Date(val));
  if (!d) return String(val);
  return d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
}

function resolveTemplatePath(company) {
  const certDir = path.join(__dirname, 'Certificates');
  if (company && fs.existsSync(certDir)) {
    const companyName = String(company).trim();
    const files = fs.readdirSync(certDir);
    // Exact match first
    const exact = files.find(f => f === `${companyName}.txt`);
    if (exact) return path.join(certDir, exact);
    // Case-insensitive match
    const loose = files.find(
      f => f.toLowerCase() === `${companyName.toLowerCase()}.txt`
    );
    if (loose) return path.join(certDir, loose);
  }
  // Default fallback
  return path.join(__dirname, 'certificate.txt');
}

async function deleteFromS3(key) {
  if (!USE_S3 || !s3Client || !DeleteObjectCommand) return;
  const bucket = process.env.S3_BUCKET;
  if (!bucket || !key) return;
  try {
    await s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  } catch (err) {
    console.warn(`S3 delete failed for key "${key}":`, err.message || err);
  }
}

async function uploadBufferToS3(buffer, key, contentType = 'application/pdf') {
  if (!USE_S3) return { uploaded: false };
  if (!s3Client || !PutObjectCommand) return { uploaded: false, error: 'AWS SDK not available' };
  const bucket = process.env.S3_BUCKET;
  if (!bucket) return { uploaded: false, error: 'S3_BUCKET not configured' };
  try {
    await s3Client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: buffer, ContentType: contentType }));
    const url = `https://${bucket}.s3.${process.env.S3_REGION || 'us-east-1'}.amazonaws.com/${encodeURIComponent(key)}`;
    return { uploaded: true, url };
  } catch (err) {
    return { uploaded: false, error: err.message || String(err) };
  }
}

async function uploadToS3IfNeeded(buffer, key) {
  return uploadBufferToS3(buffer, key, 'application/pdf');
}

function buildPdfBuffer(doc) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    doc.end();
  });
}

function parseBgUrl(raw) {
  const m = raw.match(/^BACKGROUND\s*[-–]\s*(https?:\/\/\S+)/mi);
  return m ? m[1].trim() : null;
}

async function fetchImageBuffer(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? require('https') : require('http');
    const request = mod.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        fetchImageBuffer(res.headers.location).then(resolve).catch(reject);
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    request.on('error', reject);
  });
}

function stripBgLine(text) {
  return text.replace(/^[ \t]*BACKGROUND\s*[-–]\s*https?:\/\/\S+[ \t]*\r?\n?/mi, '');
}

function drawBackground(doc, bgBuffer) {
  doc.image(bgBuffer, 0, 0, { width: 595.28, height: 841.89 });
}

function resolvePronouns(salutation) {
  const sal = (salutation || '').trim().toLowerCase().replace(/[.\s]/g, '');
  const isFemale = ['mrs', 'ms', 'miss'].includes(sal);
  return {
    'he_she':  isFemale ? 'she' : 'he',
    'He_She':  isFemale ? 'She' : 'He',
    'his_her': isFemale ? 'her' : 'his',
    'him_her': isFemale ? 'her' : 'him',
  };
}

function capitalizeLabel(str) {
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
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

function hasInlineBold(str) {
  return /\*[^*]+\*/.test(str);
}

function extractParagraphAlignment(text) {
  if (!text) return { text: '', align: 'left' };
  const centerCount  = (text.match(/\/center/g) || []).length;
  const justifyCount = (text.match(/\/ju/g) || []).length;
  const align = centerCount >= 2 ? 'center' : justifyCount >= 2 ? 'justify' : 'left';
  return {
    text: text.replace(/\/center/g, '').replace(/\/ju/g, '').replace(/\s+/g, ' ').trim(),
    align,
  };
}

function renderInlineText(doc, text, x, y, width, align, fontSize, lineGap = 0, boldBase = false) {
  const segments = parseInlineSegments(text);
  const effectiveAlign = align;

  // If justification requested, use custom renderer that preserves inline bold
  // and distributes extra spaces between words so both left and right edges align.
  if (align === 'justify') {
    return renderJustifiedInline(doc, segments, x, y, width, fontSize, lineGap, boldBase);
  }

  if (segments.length === 1 && !segments[0].bold && !boldBase) {
    doc.font('Helvetica').fontSize(fontSize).text(text, x, y, { width, align: effectiveAlign, lineGap });
    return;
  }

  for (let s = 0; s < segments.length; s++) {
    const seg      = segments[s];
    const isBold   = seg.bold || boldBase;
    const font     = isBold ? 'Helvetica-Bold' : 'Helvetica';
    const isLast   = s === segments.length - 1;

    doc.font(font).fontSize(fontSize);

    if (s === 0) {
      doc.text(seg.text, x, y, {
        width,
        align: effectiveAlign,
        lineGap,
        continued: !isLast,
        lineBreak: isLast,
      });
    } else {
      doc.text(seg.text, {
        width,
        align: effectiveAlign,
        lineGap,
        continued: !isLast,
        lineBreak: isLast,
      });
    }
  }
}

function renderJustifiedInline(doc, segments, x, startY, width, fontSize, lineGap = 0, boldBase = false) {
  // Convert segments into an array of word objects: { text, bold }
  const words = [];
  for (let seg of segments) {
    const isBold = seg.bold || boldBase;
    const parts = seg.text.replace(/\s+/g, ' ').trim().split(' ');
    for (let p of parts) if (p.length) words.push({ text: p, bold: isBold });
  }

  // No words -> nothing to render
  if (words.length === 0) return;

  let y = startY;
  const baseFont = 'Helvetica';
  const baseBold = 'Helvetica-Bold';
  const baseSpaceWidth = (() => {
    doc.font(baseFont).fontSize(fontSize);
    return doc.widthOfString(' ');
  })();

  // Build lines by fitting words
  const lines = [];
  let cur = [];
  let curWidth = 0;
  for (let i = 0; i < words.length; i++) {
    const w = words[i];
    doc.font(w.bold ? baseBold : baseFont).fontSize(fontSize);
    const wW = doc.widthOfString(w.text);
    if (cur.length === 0) {
      cur.push({ w, wW });
      curWidth = wW;
    } else {
      const projected = curWidth + baseSpaceWidth + wW;
      if (projected <= width || cur.length === 0) {
        cur.push({ w, wW });
        curWidth = projected;
      } else {
        lines.push({ items: cur, totalW: curWidth });
        cur = [{ w, wW }];
        curWidth = wW;
      }
    }
  }
  if (cur.length) lines.push({ items: cur, totalW: curWidth });

  // Render each line with distributed spacing, but keep the final line natural.
  for (let li = 0; li < lines.length; li++) {
    const line = lines[li];
    const items = line.items;
    const gaps = Math.max(0, items.length - 1);
    const totalWordsW = items.reduce((s, it) => s + it.wW, 0);
    const isLastLine = li === lines.length - 1;
    const extra = !isLastLine && gaps > 0 ? (width - totalWordsW - baseSpaceWidth * gaps) / gaps : 0;
    const gapWidth = baseSpaceWidth + (isFinite(extra) ? extra : 0);

    let curX = x;
    for (let k = 0; k < items.length; k++) {
      const it = items[k];
      const w = it.w;
      doc.font(w.bold ? baseBold : baseFont).fontSize(fontSize);
      // Render at absolute position to control spacing precisely
      doc.text(w.text, curX, y, { lineBreak: false });
      curX += it.wW;
      if (k < items.length - 1) curX += gapWidth;
    }

    y += fontSize + lineGap;
  }

  // Update pdfkit cursor
  doc.x = x;
  doc.y = y;
  return;
}

function renderContent(doc, text, contentX, contentW, startY) {
  let y = startY;
  const lines = text.split('\n');

  for (let i = 0; i < lines.length; i++) {
    const raw  = lines[i];
    const line = raw.trim();

    if (!line) {
      y += 1;
      continue;
    }

    const plain = line.replace(/\*([^*]+)\*/g, '$1');

    const hasLower   = /[a-z]/.test(plain);
    const isAllCaps  = !hasLower && /[A-Z]/.test(plain) && plain.replace(/[\s\d:.\-&·()]/g, '').length > 2;
    const isBullet   = /^[·•]/.test(plain);
    const isThanking = /^thanking/i.test(plain);

    const metaMatch = plain.match(/^(internship\s*period|domain|issued\s*date|certificate\s*id)\s*[:\-]?\s*(.*)/i);
    const noteMatch = plain.match(/^(note|reference)\s*:\s*(.*)/i);

    if (isBullet) {
      const bulletText = line.replace(/^[·•]\s*/, '');
      renderInlineText(doc, '•  ' + bulletText, contentX + 20, y, contentW - 20, 'left', 11, 3);
      y = doc.y + 6;

    } else if (isThanking) {
      renderInlineText(doc, line, contentX, y, contentW, 'center', 11);
      y = doc.y + 20;

    } else if (metaMatch) {
      const label     = capitalizeLabel(metaMatch[1].replace(/\s+/g, ' ').trim()) + ':';
      const labelRaw  = line.match(/^[^:\-]+[:\-]\s*/);
      const valueRaw  = labelRaw ? line.slice(labelRaw[0].length).trim() : metaMatch[2].trim();
      const value     = valueRaw ? ' ' + valueRaw : '';

      doc.font('Helvetica-Bold').fontSize(11).text(label, contentX, y, {
        continued: !!value,
        lineBreak: !value,
      });

      if (value) {
        const valSegments = parseInlineSegments(value);
        for (let s = 0; s < valSegments.length; s++) {
          const seg    = valSegments[s];
          const isLast = s === valSegments.length - 1;
          doc.font(seg.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(11).text(seg.text, {
            continued: !isLast,
            lineBreak: isLast,
          });
        }
      }
      y = doc.y + 10;

    } else if (noteMatch) {
      const label    = capitalizeLabel(noteMatch[1]) + ':';
      const noteRaw  = line.match(/^[^:]+:\s*/);
      const firstVal = noteRaw ? line.slice(noteRaw[0].length).trim() : noteMatch[2].trim();

      const bodyParts = firstVal ? [firstVal] : [];
      while (i + 1 < lines.length) {
        const next = lines[i + 1].trim();
        if (!next) break;
        if (/^[·•]/.test(next)) break;
        if (/^\/\/$/.test(next)) { i++; break; } 
        if (/^(internship\s*period|domain|issued\s*date|certificate\s*id)\s*[:\-]/i.test(next.replace(/\*([^*]+)\*/g, '$1'))) break;
        if (/^(note|reference)\s*:/i.test(next.replace(/\*([^*]+)\*/g, '$1'))) break;
        if (/^thanking/i.test(next)) break;
        const np = next.replace(/\*([^*]+)\*/g, '$1');
        if (!/[a-z]/.test(np) && /[A-Z]/.test(np) && np.replace(/[\s\d:.\-&·()]/g, '').length > 2) break;
        bodyParts.push(next);
        i++;
      }
      const { text: body, align: bodyAlign } = extractParagraphAlignment(bodyParts.join(' '));

      doc.font('Helvetica-Bold').fontSize(10).text(label + ' ', contentX, y, { continued: true, lineBreak: false });
      const bodySegs = parseInlineSegments(body);
      for (let s = 0; s < bodySegs.length; s++) {
        const seg    = bodySegs[s];
        const isLast = s === bodySegs.length - 1;
        doc.font(seg.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(10).text(seg.text, {
          width:    contentW,
          align:    bodyAlign,
          continued: !isLast,
          lineBreak: isLast,
        });
      }
      y = doc.y + 12;

    } else if (isAllCaps) {
      doc.font('Helvetica-Bold').fontSize(14);
      doc.text(plain, contentX, y, { width: contentW, align: 'center', underline: true });
      y = doc.y + 14;

    } else {
      const paraLines = [line];
      while (i + 1 < lines.length) {
        const next  = lines[i + 1].trim();
        const nextp = next.replace(/\*([^*]+)\*/g, '$1');
        if (!next) break;
        if (/^[·•]/.test(nextp)) break;
        if (/^(internship\s*period|domain|issued\s*date|certificate\s*id)\s*[:\-]/i.test(nextp)) break;
        if (/^(note|reference)\s*:/i.test(nextp)) break;
        if (/^thanking/i.test(nextp)) break;
        if (!/[a-z]/.test(nextp) && /[A-Z]/.test(nextp) && nextp.replace(/[\s\d:.\-&·()]/g, '').length > 2) break;
        paraLines.push(next);
        i++;
      }
      const { text: para, align: paraAlign } = extractParagraphAlignment(paraLines.join(' '));
      renderInlineText(doc, para, contentX, y, contentW, paraAlign, 11, 4);
      y = doc.y + 14;
    }
  }

  return y;
}

async function generateCertificates(internDetails = {}) {
  const filePath = resolveTemplatePath(internDetails.company);
  if (!fs.existsSync(filePath)) throw new Error(`Certificate template not found: ${filePath}`);

  const raw = fs.readFileSync(filePath, 'utf8');

  const re = /Certificate\s*\d+\s*:\s*([\s\S]*?)(?=Certificate\s*\d+\s*:|$)/gi;
  const matches = [...raw.matchAll(re)].map(m => m[1].trim()).filter(Boolean);

  const pronouns = resolvePronouns(safeValue('salutation', internDetails));
  const results  = [];
  const outDir   = path.join(__dirname, 'bills');
  if (!USE_S3 && !fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const bgCache = {};

  for (let i = 0; i < Math.min(5, matches.length); i++) {
    const template = matches[i];

    const templateBgUrl = parseBgUrl(template);
    let bgBuffer = null;
    if (templateBgUrl) {
      if (bgCache[templateBgUrl]) {
        bgBuffer = bgCache[templateBgUrl];
      } else {
        try {
          bgBuffer = await fetchImageBuffer(templateBgUrl);
          bgCache[templateBgUrl] = bgBuffer;
        } catch (e) { console.warn('Background fetch failed:', e.message); }
      }
    }

    const filledRaw = template.replace(/\[([^\]]+)\]/g, (_, rawKey) => {
      const key = rawKey.trim();
      if (pronouns[key] !== undefined) return pronouns[key];
      const v = safeValue(key, internDetails);
      if (/date/i.test(key)) return formatDateVal(v) || '';
      return (v === undefined || v === null) ? '' : String(v);
    });
    const filled = stripBgLine(filledRaw);

    const PAGE_W    = 595.28;
    const PAGE_H    = 841.89;
    const MARGIN    = 85           // 1.25 inch margin (90pt) on all sides
    const CONTENT_W = PAGE_W - MARGIN * 2;
    const START_Y   = MARGIN + 110;

    const doc = new PDFDocument({ size: 'A4', margin: 0 });

    if (bgBuffer) drawBackground(doc, bgBuffer);
    renderContent(doc, filled, MARGIN, CONTENT_W, START_Y);

    const buffer   = await buildPdfBuffer(doc);
    const fileName = `certificate_${internDetails._id || internDetails.id || 'unknown'}_${i + 1}_${Date.now()}.pdf`;

    let storedPath = null;
    let s3Info     = null;
    if (USE_S3) {
      s3Info     = await uploadToS3IfNeeded(buffer, fileName);
      storedPath = s3Info.uploaded ? s3Info.url : null;
    } else {
      const outPath = path.join(outDir, fileName);
      fs.writeFileSync(outPath, buffer);
      storedPath = outPath;
    }

    results.push({
      success: true,
      fileName,
      filePath: storedPath,
      buffer,
      s3: s3Info || null,
      message: s3Info && s3Info.error
        ? `Generated but S3 upload failed: ${s3Info.error}`
        : 'Generated successfully',
    });
  }

  return results;
}

async function generateCombinedCertificate(internDetails = {}) {
  const filePath = resolveTemplatePath(internDetails.company);
  if (!fs.existsSync(filePath)) throw new Error(`Certificate template not found: ${filePath}`);

  const raw = fs.readFileSync(filePath, 'utf8');
  const re = /Certificate\s*\d+\s*:\s*([\s\S]*?)(?=Certificate\s*\d+\s*:|$)/gi;
  const matches = [...raw.matchAll(re)].map(m => m[1].trim()).filter(Boolean);

  const pronouns = resolvePronouns(safeValue('salutation', internDetails));

  const MARGIN    = 85;
  const CONTENT_W = 595.28 - MARGIN * 2;
  const START_Y   = MARGIN + 110;

  const bgCache = {};
  const doc = new PDFDocument({ size: 'A4', margin: 0, autoFirstPage: true });
  let firstPage = true;

  for (let i = 0; i < Math.min(5, matches.length); i++) {
    const template = matches[i];

    const templateBgUrl = parseBgUrl(template);
    let bgBuffer = null;
    if (templateBgUrl) {
      if (bgCache[templateBgUrl]) {
        bgBuffer = bgCache[templateBgUrl];
      } else {
        try {
          bgBuffer = await fetchImageBuffer(templateBgUrl);
          bgCache[templateBgUrl] = bgBuffer;
        } catch (e) { console.warn('Background fetch failed:', e.message); }
      }
    }

    const filledRaw = template.replace(/\[([^\]]+)\]/g, (_, rawKey) => {
      const key = rawKey.trim();
      if (pronouns[key] !== undefined) return pronouns[key];
      const v = safeValue(key, internDetails);
      if (/date/i.test(key)) return formatDateVal(v) || '';
      return (v === undefined || v === null) ? '' : String(v);
    });
    const filled = stripBgLine(filledRaw);

    if (!firstPage) {
      doc.addPage({ size: 'A4', margin: 0 });
    }
    firstPage = false;

    if (bgBuffer) drawBackground(doc, bgBuffer);
    renderContent(doc, filled, MARGIN, CONTENT_W, START_Y);
  }

  const buffer = await buildPdfBuffer(doc);
  const fileName = `combined_certificate_${internDetails._id || internDetails.id || 'unknown'}_${Date.now()}.pdf`;

  let s3Info = null;
  if (USE_S3) {
    s3Info = await uploadToS3IfNeeded(buffer, fileName);
  }

  return {
    success: true,
    fileName,
    buffer,
    s3: s3Info,
    url: s3Info && s3Info.uploaded ? s3Info.url : null,
    message: s3Info && s3Info.error
      ? `Generated but S3 upload failed: ${s3Info.error}`
      : 'Generated successfully',
  };
}

module.exports = {
  generateCertificates,
  deleteFromS3,
  generateCombinedCertificate,
  // Shared rendering/PDF helpers reused by CourseCompletionCertificateGenerator.js
  safeValue,
  formatDateVal,
  resolvePronouns,
  parseBgUrl,
  fetchImageBuffer,
  stripBgLine,
  drawBackground,
  buildPdfBuffer,
  renderContent,
  uploadToS3IfNeeded,
  uploadBufferToS3,
};
