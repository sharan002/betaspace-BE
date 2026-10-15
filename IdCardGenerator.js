const PDFDocument = require('pdfkit');
const {
  buildPdfBuffer,
  fetchImageBuffer,
  formatDateVal,
  uploadToS3IfNeeded,
} = require('./certificateGenerator');

const CARD_W = 360;
const MIN_CARD_H = 620;

const LOGO_URL = 'https://appintechnologycbe.s3.us-east-1.amazonaws.com/appin-logo2.png';
const ADDRESS_LINES = '144, Sengupta Street, Ram Nagar,\nCoimbatore – 641009';

const NAVY = '#0f2a6b';
const NAVY_DARK = '#0b1f52';
const BLUE = '#2563eb';
const PINK = '#ec4899';
const SLATE = '#334155';
const LABEL = '#1f2937';
const LIGHT_BORDER = '#e2e8f0';

const HEADER_H = 76;
const BANNER_H = 38;
const FOOTER_H = 38;

function formatDDMMYYYY(val) {
  if (!val) return '';
  const d = (val instanceof Date) ? val : (isNaN(Date.parse(val)) ? null : new Date(val));
  if (!d) return String(val);
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  return `${dd}/${mm}/${d.getFullYear()}`;
}

function formatMobile(val) {
  if (!val) return '';
  const digits = String(val).replace(/\D/g, '');
  if (!digits) return '';
  const local = digits.length > 10 ? digits.slice(-10) : digits;
  if (local.length === 10) return `+91 ${local.slice(0, 5)} ${local.slice(5)}`;
  return `+91 ${local}`;
}

function toTitleCase(str) {
  if (!str) return '';
  return str.replace(/[A-Za-z]+/g, (w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

function gradientBar(doc, x, y, w, h, from, to, vertical = false) {
  const grad = vertical
    ? doc.linearGradient(x, y, x, y + h)
    : doc.linearGradient(x, y, x + w, y);
  grad.stop(0, from).stop(1, to);
  doc.rect(x, y, w, h).fill(grad);
}

function drawPin(doc, cx, cy, r, color, holeColor) {
  doc.save();
  doc.moveTo(cx - r, cy)
    .bezierCurveTo(cx - r, cy - r * 1.1, cx + r, cy - r * 1.1, cx + r, cy)
    .bezierCurveTo(cx + r, cy + r * 0.7, cx, cy + r * 1.9, cx, cy + r * 1.9)
    .bezierCurveTo(cx, cy + r * 1.9, cx - r, cy + r * 0.7, cx - r, cy)
    .closePath()
    .fill(color);
  doc.circle(cx, cy - r * 0.1, r * 0.4).fill(holeColor);
  doc.restore();
}

async function drawHeader(doc) {
  doc.rect(0, 0, CARD_W, HEADER_H).fill('#ffffff');

  const logoW = 190, logoH = 54;
  const logoX = (CARD_W - logoW) / 2;
  const logoY = (HEADER_H - logoH) / 2;
  try {
    const logoBuffer = await fetchImageBuffer(LOGO_URL);
    doc.image(logoBuffer, logoX, logoY, { fit: [logoW, logoH], align: 'center', valign: 'center' });
  } catch (err) {
    console.warn('ID card logo fetch failed:', err.message || err);
    doc.fillColor(NAVY).font('Helvetica-Bold').fontSize(18).text('APPIN TECHNOLOGY', 0, HEADER_H / 2 - 9, { width: CARD_W, align: 'center' });
  }

  gradientBar(doc, 0, HEADER_H, CARD_W, BANNER_H, NAVY_DARK, BLUE);
  doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(15)
    .text('STUDENT ID CARD', 0, HEADER_H + 12, { width: CARD_W, align: 'center' });

  const midY = HEADER_H + BANNER_H / 2;
  doc.strokeColor('#ffffff').lineWidth(1.2);
  doc.moveTo(32, midY).lineTo(66, midY).stroke();
  doc.circle(27, midY, 1.6).fill('#ffffff');
  doc.moveTo(CARD_W - 66, midY).lineTo(CARD_W - 32, midY).stroke();
  doc.circle(CARD_W - 27, midY, 1.6).fill('#ffffff');
}

async function drawPhoto(doc, photoUrl, y) {
  const photoW = 124;
  const photoH = 140;
  const photoX = (CARD_W - photoW) / 2;
  const pad = 6;

  doc.roundedRect(photoX - pad, y - pad, photoW + pad * 2, photoH + pad * 2, 9)
    .fillAndStroke('#eff6ff', '#bfdbfe');

  let drew = false;
  if (photoUrl) {
    try {
      const imgBuffer = await fetchImageBuffer(photoUrl);
      doc.save();
      doc.roundedRect(photoX, y, photoW, photoH, 6).clip();
      doc.image(imgBuffer, photoX, y, { width: photoW, height: photoH, fit: [photoW, photoH], align: 'center', valign: 'center' });
      doc.restore();
      drew = true;
    } catch (err) {
      console.warn('ID card photo fetch failed:', err.message || err);
    }
  }
  if (!drew) {
    const cx = photoX + photoW / 2;
    doc.roundedRect(photoX, y, photoW, photoH, 6).stroke('#bfdbfe');
    doc.circle(cx, y + photoH * 0.4, photoH * 0.18).fill('#93c5fd');
    doc.moveTo(cx - photoW * 0.28, y + photoH * 0.92)
      .quadraticCurveTo(cx, y + photoH * 0.6, cx + photoW * 0.28, y + photoH * 0.92)
      .fill('#93c5fd');
  }
  return y + photoH + pad;
}

function iconIdCard(doc, cx, cy) {
  doc.save();
  doc.lineWidth(1.2).strokeColor('#ffffff');
  doc.roundedRect(cx - 6, cy - 4.2, 12, 8.4, 1.2).stroke();
  doc.moveTo(cx - 6, cy - 0.4).lineTo(cx + 6, cy - 0.4).stroke();
  doc.circle(cx - 3.1, cy - 2, 1.2).fill('#ffffff');
  doc.restore();
}

function iconCake(doc, cx, cy) {
  doc.save();
  doc.fillColor('#ffffff').strokeColor('#ffffff');
  doc.rect(cx - 5.4, cy - 0.6, 10.8, 5.4).fill();
  doc.lineWidth(1.2).moveTo(cx, cy - 0.6).lineTo(cx, cy - 4.8).stroke();
  doc.circle(cx, cy - 6, 1.3).fill();
  doc.restore();
}

function iconPhone(doc, cx, cy) {
  doc.save();
  doc.roundedRect(cx - 3.6, cy - 6.6, 7.2, 13.2, 1.8).fill('#ffffff');
  doc.circle(cx, cy + 4, 0.7).fill(NAVY);
  doc.restore();
}

function iconAddress(doc, cx, cy) {
  drawPin(doc, cx, cy - 4.8, 4.8, '#ffffff', NAVY);
}

function iconCalendar(doc, cx, cy) {
  doc.save();
  doc.roundedRect(cx - 6, cy - 4.8, 12, 10.8, 1.2).fill('#ffffff');
  doc.strokeColor(NAVY).lineWidth(1.2);
  doc.moveTo(cx - 6, cy - 1.6).lineTo(cx + 6, cy - 1.6).stroke();
  doc.fillColor(NAVY);
  doc.rect(cx - 3.4, cy - 7.2, 1.3, 3.6).fill();
  doc.rect(cx + 2, cy - 7.2, 1.3, 3.6).fill();
  doc.restore();
}

function iconCheck(doc, cx, cy) {
  doc.save();
  doc.strokeColor('#ffffff').lineWidth(2).lineJoin('round').lineCap('round');
  doc.moveTo(cx - 4.8, cy).lineTo(cx - 1.2, cy + 3.8).lineTo(cx + 5, cy - 4.8).stroke();
  doc.restore();
}

const ROW_BADGE_R = 11;
const ROW_LABEL_W = 78;
const ROW_VALUE_OFFSET = ROW_BADGE_R * 2 + 12 + ROW_LABEL_W + 10; // labelX + labelW + gap-to-value

function drawRow(doc, iconFn, label, value, x, y, width) {
  const badgeR = ROW_BADGE_R;
  const badgeCX = x + badgeR;
  const labelX = x + badgeR * 2 + 12;
  const labelW = ROW_LABEL_W;
  const pipeX = labelX + labelW;
  const valueX = x + ROW_VALUE_OFFSET;
  const valueW = x + width - valueX;

  const valText = value || '-';
  doc.font('Helvetica').fontSize(12);
  const valHeight = doc.heightOfString(valText, { width: valueW, lineGap: 2 });
  const rowH = Math.max(valHeight, badgeR * 2);
  const rowCenterY = y + rowH / 2;

  doc.circle(badgeCX, rowCenterY, badgeR).fill(NAVY);
  iconFn(doc, badgeCX, rowCenterY);

  doc.fillColor(LABEL).font('Helvetica-Bold').fontSize(12)
    .text(label, labelX, y + (rowH - 12) / 2, { width: labelW });

  doc.fillColor('#94a3b8').font('Helvetica-Bold').fontSize(12)
    .text('|', pipeX, y + (rowH - 12) / 2, { width: 10 });

  // PDFKit's text ink sits slightly above the given y (same effect noted for the
  // footer address), so nudge the value down a touch to visually center it
  // against the badge/label instead of sitting high.
  const VALUE_Y_NUDGE = 2;
  doc.fillColor(SLATE).font('Helvetica-Bold').fontSize(12)
    .text(valText, valueX, y + (rowH - valHeight) / 2 + VALUE_Y_NUDGE, { width: valueW, lineGap: 2 });

  const bottomY = y + rowH + 7;
  doc.moveTo(x, bottomY).lineTo(x + width, bottomY).lineWidth(0.5).stroke(LIGHT_BORDER);
  return bottomY + 12;
}

function computeCardHeight(details) {
  const contentW = CARD_W - 40;
  const valueW = contentW - ROW_VALUE_OFFSET;

  const measureDoc = new PDFDocument({ size: [CARD_W, 100], margin: 0 });

  measureDoc.font('Helvetica-Bold').fontSize(19);
  const nameH = measureDoc.heightOfString((details.userName || '').toUpperCase(), { width: CARD_W, align: 'center' });

  measureDoc.font('Helvetica-Bold').fontSize(13);
  const courseH = details.course ? measureDoc.heightOfString(details.course, { width: CARD_W, align: 'center' }) : 13;

  const rowValues = [
    details.idNo,
    formatDateVal(details.dob),
    formatMobile(details.mobile),
    toTitleCase(details.address),
    formatDDMMYYYY(details.classStart),
    formatDDMMYYYY(details.validTill),
  ];

  measureDoc.font('Helvetica-Bold').fontSize(12);
  let rowsTotal = 0;
  for (const v of rowValues) {
    // Measure a touch narrower than the real render width so borderline word-wrap
    // never estimates fewer lines than drawRow actually produces.
    const h = measureDoc.heightOfString(v || '-', { width: valueW - 4, lineGap: 2 });
    rowsTotal += Math.max(h, ROW_BADGE_R * 2) + 19; // matches drawRow's rowH + 7 (divider gap) + 12 (post gap)
  }

  const photoBlockH = 140 + 12; // photoH + pad*2

  const contentBottom = HEADER_H + BANNER_H + 18  // header + banner + gap to photo
    + photoBlockH + 14                            // photo + gap
    + nameH + 4                                   // name + gap
    + courseH + 12                                // course + gap
    + 3 + 18                                       // divider + gap
    + rowsTotal;

  return Math.max(MIN_CARD_H, Math.ceil(contentBottom) + FOOTER_H + 8);
}

async function generateIdCard(details = {}) {
  const CARD_H = computeCardHeight(details);
  const doc = new PDFDocument({ size: [CARD_W, CARD_H], margin: 0 });

  await drawHeader(doc);

  let y = HEADER_H + BANNER_H + 18;
  y = await drawPhoto(doc, details.photoUrl, y);
  y += 14;

  doc.fillColor(NAVY).font('Helvetica-Bold').fontSize(19)
    .text((details.userName || '').toUpperCase(), 0, y, { width: CARD_W, align: 'center' });
  y = doc.y + 4;

  doc.fillColor(BLUE).font('Helvetica-Bold').fontSize(13)
    .text(details.course || '', 0, y, { width: CARD_W, align: 'center' });
  y = doc.y + 12;

  const dividerW = 110;
  gradientBar(doc, (CARD_W - dividerW) / 2, y, dividerW, 3, PINK, BLUE);
  y += 18;

  const contentX = 20;
  const contentW = CARD_W - 40;

  y = drawRow(doc, iconIdCard, 'ID No', details.idNo, contentX, y, contentW);
  y = drawRow(doc, iconCake, 'DOB', formatDateVal(details.dob), contentX, y, contentW);
  y = drawRow(doc, iconPhone, 'Mobile', formatMobile(details.mobile), contentX, y, contentW);
  y = drawRow(doc, iconAddress, 'Address', toTitleCase(details.address), contentX, y, contentW);
  y = drawRow(doc, iconCalendar, 'Enrolled on', formatDDMMYYYY(details.classStart), contentX, y, contentW);
  y = drawRow(doc, iconCheck, 'Valid Till', formatDDMMYYYY(details.validTill), contentX, y, contentW);

  const footerY = CARD_H - FOOTER_H;
  gradientBar(doc, 0, footerY, CARD_W, FOOTER_H, NAVY_DARK, BLUE);

  const footerAddr = toTitleCase(ADDRESS_LINES);
  const footerPad = 20;
  const footerTextW = CARD_W - footerPad * 2;

  doc.font('Helvetica-Bold').fontSize(9);
  const footerAddrH = doc.heightOfString(footerAddr, { width: footerTextW, align: 'center', lineGap: 2 });

  // PDFKit renders glyph ink ~3.2pt above the y position it's given, so nudge
  // down to make the visible text (not the theoretical text box) sit centered.
  const footerTextY = footerY + (FOOTER_H - footerAddrH) / 2 + 3.2;
  doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(9)
    .text(footerAddr, footerPad, footerTextY, { width: footerTextW, align: 'center', lineGap: 2 });

  doc.lineWidth(1).rect(0.5, 0.5, CARD_W - 1, CARD_H - 1).stroke(LIGHT_BORDER);

  const buffer = await buildPdfBuffer(doc);
  const fileName = `idcard_${details._id || 'unknown'}_${Date.now()}.pdf`;

  const s3Info = await uploadToS3IfNeeded(buffer, fileName);

  return {
    success: true,
    fileName,
    buffer,
    s3: s3Info,
    message: s3Info && s3Info.error
      ? `Generated but S3 upload failed: ${s3Info.error}`
      : 'Generated successfully',
  };
}

module.exports = {
  generateIdCard,
};
