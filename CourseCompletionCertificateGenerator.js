const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');
const {
  safeValue,
  formatDateVal,
  resolvePronouns,
  parseBgUrl,
  fetchImageBuffer,
  stripBgLine,
  drawBackground,
  buildPdfBuffer,
  uploadToS3IfNeeded,
} = require('./certificateGenerator');

const USE_S3 = !!process.env.S3_BUCKET || process.env.USE_S3 === 'true';

const COURSE_COMPLETION_DIR = path.join(__dirname, 'Course_Completion');
const PAGE_W    = 595.28;
const PAGE_H    = 841.89;
const MARGIN    = 85;
const CONTENT_W = PAGE_W - MARGIN * 2;
const START_Y   = MARGIN + 110;

// ─── Private Rendering Engine ───────────────────────────────────────────────
// Own copy of the text-markup renderer (bold/justify/center/newline + layout),
// kept independent from certificateGenerator.js so changes here (e.g. the /n
// forced-line-break token below) can never affect internship certificate
// generation, which uses its own renderContent in certificateGenerator.js.

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

function extractParagraphAlignment(text) {
  if (!text) return { text: '', align: 'left' };
  const centerCount  = (text.match(/\/center/g) || []).length;
  const justifyCount = (text.match(/\/ju/g) || []).length;
  const align = centerCount >= 2 ? 'center' : justifyCount >= 2 ? 'justify' : 'left';
  const cleaned = text.replace(/\/center/g, '').replace(/\/ju/g, '');
  // "/n" is a forced line break within a paragraph. Each segment between
  // "/n" tokens is whitespace-normalized on its own, then rejoined with a
  // real newline so renderParagraphWithBreaks can render each on its own line.
  const normalized = cleaned
    .split(/\/n/)
    .map(segment => segment.replace(/\s+/g, ' ').trim())
    .join('\n');
  return {
    text: normalized,
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

// Renders paragraph text that may contain forced "/n" line breaks, one line
// at a time, and returns the y position after the whole paragraph.
function renderParagraphWithBreaks(doc, text, x, y, width, align, fontSize, lineGap, paragraphGap) {
  const lines = text.split('\n');
  for (let idx = 0; idx < lines.length; idx++) {
    const isLast = idx === lines.length - 1;
    if (!lines[idx]) {
      y += lineGap;
      continue;
    }
    renderInlineText(doc, lines[idx], x, y, width, align, fontSize, lineGap);
    y = doc.y + (isLast ? paragraphGap : lineGap);
  }
  return y;
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
      y = renderParagraphWithBreaks(doc, para, contentX, y, contentW, paraAlign, 11, 4, 14);
    }
  }

  return y;
}

// ─── Template Resolution ─────────────────────────────────────────────────────

function resolveCourseTemplatePath(course) {
  if (!course || !fs.existsSync(COURSE_COMPLETION_DIR)) return null;
  const courseName = String(course).trim();
  const files = fs.readdirSync(COURSE_COMPLETION_DIR);
  const exact = files.find(f => f === `${courseName}.txt`);
  if (exact) return path.join(COURSE_COMPLETION_DIR, exact);
  const loose = files.find(f => f.toLowerCase() === `${courseName.toLowerCase()}.txt`);
  if (loose) return path.join(COURSE_COMPLETION_DIR, loose);
  return null;
}

function resolveHoursFromProgramType(programType) {
  const n = parseInt((String(programType || '')).match(/(\d+)/)?.[1] || '', 10);
  return n || null;
}

// Templates are split into sections using two kinds of markers:
//   "8 Hours:" / "2 Hours:"                   -> scopes the block that follows to a program type
//   "Only For 8 Hours:"                       -> internship certificate (8-hour program only)
//   "PROJECT EXPERIENCE CERTIFICATE:"         -> legacy standalone marker (no scope), used by
//                                                 older single-template courses
// Newer templates nest a Project Experience Certificate inside the same "8 Hours:" / "2 Hours:"
// scope as the Course Completion Certificate (see Course_Completion/Python Fullstack with AI.txt),
// so which certificate a scoped block belongs to is resolved from the block's own title line
// rather than from the header alone.
const SCOPE_HEADER_RE = /^[ \t]*(8\s*Hours|2\s*Hours|ONLY\s+FOR\s+8\s+HOURS|PROJECT\s+EXPERIENCE\s+CERTIFICATE)\s*:/gim;

function detectCertType(content, fallback) {
  const firstLine = (content.split('\n').map(l => l.trim()).find(Boolean) || '').replace(/\*/g, '');
  if (/^COURSE COMPLETION CERTIFICATE/i.test(firstLine)) return 'completion';
  if (/^Certificate of Enrollment/i.test(firstLine)) return 'completion';
  if (/^PROJECT EXPERIENCE CERTIFICATE/i.test(firstLine)) return 'project';
  if (/^INTERNSHIP CERTIFICATE/i.test(firstLine)) return 'internship';
  return fallback;
}

function splitNamedSections(raw) {
  const headers = [];
  let m;
  SCOPE_HEADER_RE.lastIndex = 0;
  while ((m = SCOPE_HEADER_RE.exec(raw)) !== null) {
    headers.push({ raw: m[1], headerStart: m.index, contentStart: m.index + m[0].length });
  }

  const completion = {};  // { 8: template, 2: template }
  const project = {};     // { 8: template, 2: template }
  let projectGlobal = null;
  let internship = null;

  for (let i = 0; i < headers.length; i++) {
    const norm = headers[i].raw.replace(/\s+/g, ' ').trim().toLowerCase();
    const start = headers[i].contentStart;
    const end = i + 1 < headers.length ? headers[i + 1].headerStart : raw.length;
    const content = raw.slice(start, end).trim();
    if (!content) continue;

    if (norm === 'project experience certificate') {
      projectGlobal = content;
    } else if (norm === 'only for 8 hours') {
      if (detectCertType(content, 'internship') === 'internship') internship = content;
    } else {
      const hours = /^8/.test(norm) ? 8 : 2;
      const type = detectCertType(content, 'completion');
      if (type === 'project') project[hours] = content;
      else if (type === 'internship') internship = content;
      else completion[hours] = content;
    }
  }

  return { completion, project, projectGlobal, internship };
}

// Fills [placeholder] tokens. Aliases project_title/project_name/project to the
// same value, and renders the projects_skills array as one bullet line per skill.
function fillPlaceholders(template, details, pronouns) {
  return template.replace(/\[([^\]]+)\]/g, (_, rawKey) => {
    const key = rawKey.trim();
    if (pronouns[key] !== undefined) return pronouns[key];

    if (/^project[_ ]?(title|name)$/i.test(key) || /^project$/i.test(key)) {
      return safeValue('project_title', details) || '';
    }
    if (/^project[_ ]?s?[_ ]?skills$/i.test(key)) {
      const skills = Array.isArray(details.projects_skills) ? details.projects_skills : [];
      return skills.map(s => (s || '').toString().trim()).filter(Boolean).map(s => `• ${s}`).join('\n');
    }

    const v = safeValue(key, details);
    if (/date/i.test(key)) return formatDateVal(v) || '';
    return (v === undefined || v === null) ? '' : String(v);
  });
}

// Picks the ordered list of raw section templates that apply to this user:
// completion certificate (by hours) + project experience (always) + internship (8h only)
function pickSections(course, programType) {
  const templatePath = resolveCourseTemplatePath(course);
  if (!templatePath) {
    throw new Error(
      `No completion certificate template found for course: "${course}". ` +
      `Add a matching file inside Course_Completion/`
    );
  }

  const raw = fs.readFileSync(templatePath, 'utf8');
  const { completion, project, projectGlobal, internship } = splitNamedSections(raw);
  const hours = resolveHoursFromProgramType(programType);

  const completionSection = hours === 2
    ? (completion[2] || completion[8])
    : (completion[8] || completion[2]);

  const projectSection = hours === 2
    ? (project[2] || project[8] || projectGlobal)
    : (project[8] || project[2] || projectGlobal);

  const ordered = [];
  if (completionSection) ordered.push({ type: 'completion', template: completionSection });
  if (projectSection) ordered.push({ type: 'project', template: projectSection });
  if (hours === 8 && internship) ordered.push({ type: 'internship', template: internship });

  if (!ordered.length) {
    throw new Error(`No certificate sections found in completion template for course: "${course}".`);
  }

  return ordered;
}

// ─── Main Exports ──────────────────────────────────────────────────────────────

// Generates course-completion related certificates (completion, project experience,
// and — for 8-hour programs — internship) as separate PDFs.
// Returns an array of { success, type, fileName, filePath, buffer, s3, message }.
async function generateCourseCompletionCertificates(details = {}) {
  const ordered = pickSections(details.course, details.programType);
  const pronouns = resolvePronouns(safeValue('salutation', details));

  const outDir = path.join(__dirname, 'bills');
  if (!USE_S3 && !fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const bgCache = {};
  const results = [];

  for (const section of ordered) {
    const templateBgUrl = parseBgUrl(section.template);
    let bgBuffer = null;
    if (templateBgUrl) {
      if (bgCache[templateBgUrl]) {
        bgBuffer = bgCache[templateBgUrl];
      } else {
        try {
          bgBuffer = await fetchImageBuffer(templateBgUrl);
          bgCache[templateBgUrl] = bgBuffer;
        } catch (e) { console.warn('Course completion certificate background fetch failed:', e.message); }
      }
    }

    const filledRaw = fillPlaceholders(section.template, details, pronouns);
    const filled = stripBgLine(filledRaw);

    const doc = new PDFDocument({ size: 'A4', margin: 0 });
    if (bgBuffer) drawBackground(doc, bgBuffer);
    renderContent(doc, filled, MARGIN, CONTENT_W, START_Y);

    const buffer   = await buildPdfBuffer(doc);
    const fileName = `course_completion_cert_${details._id || details.userId || 'unknown'}_${section.type}_${Date.now()}.pdf`;

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
      type: section.type,
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

// Combines all applicable certificates for this user into a single multi-page PDF.
async function generateCombinedCourseCompletionCertificate(details = {}) {
  const ordered = pickSections(details.course, details.programType);
  const pronouns = resolvePronouns(safeValue('salutation', details));

  const bgCache = {};
  const doc = new PDFDocument({ size: 'A4', margin: 0, autoFirstPage: true });
  let firstPage = true;

  for (const section of ordered) {
    const templateBgUrl = parseBgUrl(section.template);
    let bgBuffer = null;
    if (templateBgUrl) {
      if (bgCache[templateBgUrl]) {
        bgBuffer = bgCache[templateBgUrl];
      } else {
        try {
          bgBuffer = await fetchImageBuffer(templateBgUrl);
          bgCache[templateBgUrl] = bgBuffer;
        } catch (e) { console.warn('Course completion certificate background fetch failed:', e.message); }
      }
    }

    const filledRaw = fillPlaceholders(section.template, details, pronouns);
    const filled = stripBgLine(filledRaw);

    if (!firstPage) doc.addPage({ size: 'A4', margin: 0 });
    firstPage = false;

    if (bgBuffer) drawBackground(doc, bgBuffer);
    renderContent(doc, filled, MARGIN, CONTENT_W, START_Y);
  }

  const buffer = await buildPdfBuffer(doc);
  const fileName = `combined_course_completion_cert_${details._id || details.userId || 'unknown'}_${Date.now()}.pdf`;

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

module.exports = { generateCourseCompletionCertificates, generateCombinedCourseCompletionCertificate };
