const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');

// Optional AWS S3 support. Enabled when `process.env.USE_S3 === 'true'` or `process.env.S3_BUCKET` is set.
let s3Client = null;
let PutObjectCommand = null;
let DeleteObjectCommand = null;
let S3_ENABLED = false;
if (process.env.USE_S3 === 'true' || process.env.S3_BUCKET) {
  try {
    const aws = require('@aws-sdk/client-s3');
    const { S3Client } = aws;
    PutObjectCommand = aws.PutObjectCommand || aws.PutObject || aws.PutObjectCommand;
    DeleteObjectCommand = aws.DeleteObjectCommand || aws.DeleteObject || aws.DeleteObjectCommand;
    s3Client = new S3Client({ region: process.env.S3_REGION || 'us-east-1' });
    S3_ENABLED = true;
  } catch (e) {
    // If SDK not installed, we'll surface clear errors at runtime when attempting S3 upload.
    s3Client = null;
    PutObjectCommand = null;
    DeleteObjectCommand = null;
    S3_ENABLED = true; // still true to indicate intent, but missing SDK will be handled later
  }
}

// Note: Email sending removed per request. This module now only generates PDFs and
// optionally uploads to S3. The generated file URL is returned so callers can persist it.

/**
 * Generate a PDF bill for a specific installment
 * @param {Object} lead - Lead document from database
 * @param {Number} installmentIndex - Index of the installment (0-based)
 * @returns {Promise<{success: boolean, filePath: string, message: string}>}
 */
// async function generateBillPDF(lead, installmentIndex) {
//   try {
//     if (!lead.fees || !lead.fees.installments) {
//       return { success: false, message: 'No fees/installments found for this lead' };
//     }

//     const installment = lead.fees.installments[installmentIndex];
//     if (!installment) {
//       return { success: false, message: `Installment #${installmentIndex + 1} not found` };
//     }

//     const fileName = `bill_${lead._id}_installment_${installmentIndex + 1}_${Date.now()}.pdf`;
//     const billsDir = path.join(__dirname, 'bills');
//     const filePath = path.join(billsDir, fileName);

//     const useS3 = (process.env.USE_S3 === 'true' || !!process.env.S3_BUCKET);
//     if (!useS3) {
//       if (!fs.existsSync(billsDir)) fs.mkdirSync(billsDir, { recursive: true });
//     }

//     // ─── Design Tokens (Modern & Clean) ──────────────────────────────────
//     const PRIMARY_DARK   = '#0F2B4D';
//     const PRIMARY_BLUE   = '#1E3A8A';
//     const ACCENT_BLUE    = '#3B82F6';
//     const LIGHT_BG       = '#F8FAFC';
//     const HEADER_BG      = '#EFF6FF';
//     const CARD_BG        = '#FFFFFF';
//     const BORDER_LIGHT   = '#E2E8F0';
//     const TEXT_PRIMARY   = '#1E293B';
//     const TEXT_SECONDARY = '#475569';
//     const TEXT_MUTED     = '#64748B';
//     const SUCCESS_GREEN  = '#10B981';
//     const WARNING_ORANGE = '#F59E0B';
//     const WHITE          = '#FFFFFF';

//     const PAGE_W    = 595.28;
//     const PAGE_H    = 841.89;
//     const MARGIN    = 40;
//     const CONTENT_W = PAGE_W - MARGIN * 2;

//     // ─── Column layout: label col + gap + value col ───────────────────────
//     const COL_GAP   = 16;          // visual gap between label and value columns
//     const VAL_W     = 150;
//     const LBL_W     = CONTENT_W - VAL_W - COL_GAP;

//     const doc = new PDFDocument({ size: 'A4', margin: 0, info: { Title: 'Invoice', Author: 'Appin Technology' } });
//     const chunks = [];
//     doc.on('data', chunk => chunks.push(chunk));

//     // ─── Helpers ──────────────────────────────────────────────────────────
//     const fc = (hex) => { doc.fillColor(hex); return doc; };
//     const sc = (hex) => { doc.strokeColor(hex); return doc; };

//     const fillRect = (x, y, w, h, hex, r = 0) => {
//       fc(hex);
//       r > 0 ? doc.roundedRect(x, y, w, h, r).fill() : doc.rect(x, y, w, h).fill();
//     };

//     const strokeRect = (x, y, w, h, hex = BORDER_LIGHT, lw = 0.5) => {
//       sc(hex);
//       doc.rect(x, y, w, h).lineWidth(lw).stroke();
//     };

//     const vLine = (x, y1, y2, hex = BORDER_LIGHT, lw = 0.5) => {
//       sc(hex);
//       doc.moveTo(x, y1).lineTo(x, y2).lineWidth(lw).stroke();
//     };

//     // Draw a subtle left accent bar for section headers
//     const accentBar = (x, y, h, hex = ACCENT_BLUE) => {
//       fillRect(x, y, 4, h, hex, 2);
//     };

//     // ─── Fetch Logo ───────────────────────────────────────────────────────
//     let logoBuffer = null;
//     try {
//       const https = require('https');
//       logoBuffer = await new Promise((resolve, reject) => {
//         https.get('https://appincoimbatore.com/wp-content/uploads/2023/05/appin-technology-coimbatore.png', res => {
//           const bufs = [];
//           res.on('data', d => bufs.push(d));
//           res.on('end', () => resolve(Buffer.concat(bufs)));
//           res.on('error', reject);
//         }).on('error', reject);
//       });
//     } catch (_) { logoBuffer = null; }

//     // ══════════════════════════════════════════════════════════════════════
//     //  SECTION 1 — COMPANY HEADER
//     // ══════════════════════════════════════════════════════════════════════
//     let curY = MARGIN;

//     // Top gradient-like decorative stripe (two-layer)
//     fillRect(0, 0, PAGE_W, 6, PRIMARY_BLUE);
//     fillRect(0, 4, PAGE_W, 4, ACCENT_BLUE);
//     curY = MARGIN + 4;

//     const HEADER_H  = 114;
//     const RECEIPT_W = 186;
//     const COMPANY_W = CONTENT_W - RECEIPT_W - 8; // 8px gap between boxes

//     // Company info box (left) — no border, clean white card
//     fillRect(MARGIN, curY, COMPANY_W, HEADER_H, CARD_BG, 6);

//     let logoEndY = curY + 10;
//     if (logoBuffer) {
//       try {
//         doc.image(logoBuffer, MARGIN + 14, curY + 12, { fit: [145, 50] });
//         logoEndY = curY + 66;
//       } catch (_) {}
//     }

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(11).text('Appin Technology, Coimbatore', MARGIN + 14, logoEndY);
//     fc(TEXT_SECONDARY);
//     doc.font('Helvetica').fontSize(8)
//        .text('[division of Ether Services]',                             MARGIN + 14, logoEndY + 14)
//        .text('144, Sengupta Street, Ram Nagar, Coimbatore - 641009',    MARGIN + 14, logoEndY + 25)
//        .text('M: 7708040308',                                            MARGIN + 14, logoEndY + 36)
//        .text('GSTIN: 33AAEFE1664N1ZC',                                  MARGIN + 14, logoEndY + 47);

//     // Receipt box (right) — deeper styling with gradient-like header strip
//     const RECEIPT_X = MARGIN + COMPANY_W + 8;
//     fillRect(RECEIPT_X, curY, RECEIPT_W, HEADER_H, HEADER_BG, 6);
//     strokeRect(RECEIPT_X, curY, RECEIPT_W, HEADER_H, BORDER_LIGHT, 1);
//     // Top accent strip inside receipt box
//     fillRect(RECEIPT_X, curY, RECEIPT_W, 5, ACCENT_BLUE, 6);
//     fillRect(RECEIPT_X, curY + 3, RECEIPT_W, 5, ACCENT_BLUE); // flatten bottom of strip

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(9).text('Receipt No:',   RECEIPT_X + 14, curY + 16);
//     fc(TEXT_SECONDARY);
//     doc.font('Helvetica').fontSize(9)
//        .text(`${lead._id || '—'}`, RECEIPT_X + 14, curY + 28, { width: RECEIPT_W - 28 });

//     // Thin separator inside receipt box
//     sc(BORDER_LIGHT);
//     doc.moveTo(RECEIPT_X + 14, curY + 46).lineTo(RECEIPT_X + RECEIPT_W - 14, curY + 46).lineWidth(0.5).stroke();

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(9).text('Date:',         RECEIPT_X + 14, curY + 52);
//     fc(TEXT_SECONDARY);
//     doc.font('Helvetica').fontSize(9).text(
//       new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }),
//       RECEIPT_X + 14, curY + 64
//     );

//     sc(BORDER_LIGHT);
//     doc.moveTo(RECEIPT_X + 14, curY + 80).lineTo(RECEIPT_X + RECEIPT_W - 14, curY + 80).lineWidth(0.5).stroke();

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(9).text('Invoice No:',   RECEIPT_X + 14, curY + 86);
//     fc(TEXT_SECONDARY);
//     doc.font('Helvetica').fontSize(9).text(`INV-${installmentIndex + 1}`, RECEIPT_X + 14, curY + 98);

//     curY += HEADER_H + 14;

//     // ══════════════════════════════════════════════════════════════════════
//     //  SECTION 2 — STUDENT DETAILS
//     // ══════════════════════════════════════════════════════════════════════
//     const SEC_H = 30;
//     const ROW_H = 26;

//     // Section header with left accent bar
//     fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
//     fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(11)
//        .text('Student Details', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
//     curY += SEC_H;

//     const studentRows = [
//       ['Name',             lead.userName || 'N/A'],
//       ['Course',           lead.course || lead.courseofintrest || 'N/A'],
//       ['Mobile',           lead.userNumber || 'N/A'],
//       ['Career Counselor', lead.assignedto || lead.counselor || lead.counselorName || 'N/A'],
//     ];

//     studentRows.forEach(([label, value], idx) => {
//       const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
//       fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
//       strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);

//       // Vertical divider — positioned with the COL_GAP incorporated
//       const dividerX = MARGIN + LBL_W + COL_GAP / 2;
//       vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);

//       fc(TEXT_MUTED);
//       doc.font('Helvetica-Bold').fontSize(8.5).text(label, MARGIN + 14, curY + 8, { width: LBL_W - 20 });

//       fc(TEXT_PRIMARY);
//       doc.font('Helvetica').fontSize(9)
//          .text(value, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14 });
//       curY += ROW_H;
//     });

//     curY += 14;

//     // ══════════════════════════════════════════════════════════════════════
//     //  SECTION 3 — PAYMENT SUMMARY
//     // ══════════════════════════════════════════════════════════════════════
//     const totalBilledAmount         = lead.fees?.billedAmount || 0;
//     const amountPaidThisInstallment = installment.amount || 0;
//     const totalPaid                 = (lead.fees?.installments || []).reduce(
//       (sum, i) => sum + (i.status === 'Paid' ? (i.amount || 0) : 0), 0
//     );
//     const balance = totalBilledAmount - totalPaid;

//     fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
//     fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(11)
//        .text('Payment Summary', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
//     curY += SEC_H;

//     const summaryRows = [
//       ['Total Course Fees',                             `Rs. ${totalBilledAmount.toFixed(2)}`],
//       ['Amount Paid',                                   `Rs. ${amountPaidThisInstallment.toFixed(2)}`],
//       ['Total Amount Paid Till Now (inclusive of GST)', `Rs. ${totalPaid.toFixed(2)}`],
//     ];

//     summaryRows.forEach(([label, value], idx) => {
//       const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
//       fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
//       strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);

//       const dividerX = MARGIN + LBL_W + COL_GAP / 2;
//       vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);

//       fc(TEXT_MUTED);
//       doc.font('Helvetica-Bold').fontSize(8.5).text(label, MARGIN + 14, curY + 8, { width: LBL_W - 20 });

//       fc(PRIMARY_BLUE);
//       doc.font('Helvetica-Bold').fontSize(9)
//          .text(value, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14 });
//       curY += ROW_H;
//     });

//     // Spacer row
//     fillRect(MARGIN, curY, CONTENT_W, ROW_H, CARD_BG);
//     strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
//     curY += ROW_H;

//     // Balance row — highlighted pill-style card
//     const BAL_H = 38;
//     fillRect(MARGIN, curY, CONTENT_W, BAL_H, '#FEF3C7', 6);
//     strokeRect(MARGIN, curY, CONTENT_W, BAL_H, WARNING_ORANGE, 1.2);

//     const balDividerX = MARGIN + LBL_W + COL_GAP / 2;
//     vLine(balDividerX, curY + 6, curY + BAL_H - 6, WARNING_ORANGE, 0.8);

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(12)
//        .text('Balance Amount', MARGIN + 14, curY + 13, { width: LBL_W - 20 });
//     fc(WARNING_ORANGE);
//     doc.font('Helvetica-Bold').fontSize(12)
//        .text(`Rs. ${balance.toFixed(2)}`, MARGIN + LBL_W + COL_GAP + 10, curY + 13, { width: VAL_W - 14 });
//     curY += BAL_H + 14;

//     // ══════════════════════════════════════════════════════════════════════
//     //  SECTION 4 — UPCOMING PAYMENTS
//     // ══════════════════════════════════════════════════════════════════════
//     const upcomingInstallments = (lead.fees?.installments || []).slice(installmentIndex + 1);

//     fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
//     fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(11)
//        .text('Upcoming Payments', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
//     curY += SEC_H;

//     const ordinal = (n) => {
//       const s = ['th', 'st', 'nd', 'rd'];
//       const v = n % 100;
//       return n + (s[(v - 20) % 10] || s[v] || s[0]);
//     };

//     if (upcomingInstallments.length === 0) {
//       fillRect(MARGIN, curY, CONTENT_W, ROW_H, LIGHT_BG, 4);
//       strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
//       fc(TEXT_MUTED);
//       doc.font('Helvetica').fontSize(9).text('No upcoming payments', MARGIN + 14, curY + 8);
//       curY += ROW_H;
//     } else {
//       upcomingInstallments.forEach((inst, idx) => {
//         const humanIndex = installmentIndex + 2 + idx;
//         const due = inst.dueDate
//           ? new Date(inst.dueDate).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
//           : '—';

//         const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
//         fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
//         strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);

//         const dividerX = MARGIN + LBL_W + COL_GAP / 2;
//         vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);

//         fc(TEXT_MUTED);
//         doc.font('Helvetica-Bold').fontSize(8.5)
//            .text(`${ordinal(humanIndex)} Payment  ·  Due ${due}`, MARGIN + 14, curY + 8, { width: LBL_W - 20 });
//         fc(PRIMARY_BLUE);
//         doc.font('Helvetica-Bold').fontSize(9)
//            .text(`Rs. ${(inst.amount || 0).toFixed(2)}`, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14 });
//         curY += ROW_H;
//       });
//     }

//     curY += 16;

//     // ══════════════════════════════════════════════════════════════════════
//     //  SECTION 5 — NOTES & SIGNATURE
//     // ══════════════════════════════════════════════════════════════════════
//     const notes = [
//       'This is Computer-generated Receipt',
//       'No signature required.',
//       'Fees once paid are non-refundable.',
//     ];
//     const NOTES_H = notes.length * 20 + 20;

//     // Notes header
//     fillRect(MARGIN, curY, CONTENT_W, 28, HEADER_BG, 6);
//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(10).text('Note', MARGIN + 14, curY + 9);
//     curY += 28;

//     fillRect(MARGIN, curY, CONTENT_W, NOTES_H, CARD_BG, 6);
//     strokeRect(MARGIN, curY, CONTENT_W, NOTES_H, BORDER_LIGHT, 1);

//     // Left accent strip on notes card
//     fillRect(MARGIN, curY, 4, NOTES_H, ACCENT_BLUE, 2);
//     // Flatten right edge of left strip
//     fillRect(MARGIN + 2, curY, 2, NOTES_H, ACCENT_BLUE);

//     notes.forEach((note, i) => {
//       fc(TEXT_SECONDARY);
//       doc.font('Helvetica').fontSize(8.5)
//          .text(`• ${note}`, MARGIN + 18, curY + 10 + i * 20, { width: CONTENT_W - 170 });
//     });

//     // Signature block (right side)
//     const SIG_X = PAGE_W - MARGIN - 165;
//     fillRect(SIG_X, curY + 8, 155, NOTES_H - 16, '#F8FAFC', 4);
//     strokeRect(SIG_X, curY + 8, 155, NOTES_H - 16, BORDER_LIGHT, 0.8);

//     fc(TEXT_PRIMARY);
//     doc.font('Helvetica-Bold').fontSize(9)
//        .text('For Accounts',                   SIG_X, curY + 16, { width: 155, align: 'center' });
//     fc(TEXT_MUTED);
//     doc.font('Helvetica').fontSize(8)
//        .text('APPIN Technology, Coimbatore',   SIG_X, curY + 30, { width: 155, align: 'center' });

//     // Signature line
//     sc(BORDER_LIGHT);
//     const sigLineY = curY + NOTES_H - 22;
//     doc.moveTo(SIG_X + 20, sigLineY).lineTo(SIG_X + 135, sigLineY).lineWidth(0.6).stroke();
//     fc(TEXT_MUTED);
//     doc.font('Helvetica').fontSize(7)
//        .text('Authorised Signatory', SIG_X, sigLineY + 4, { width: 155, align: 'center' });

//     curY += NOTES_H + 16;

//     // ══════════════════════════════════════════════════════════════════════
//     //  FOOTER
//     // ══════════════════════════════════════════════════════════════════════
//     const FOOTER_Y = PAGE_H - 48;

//     // Footer gradient-like (two layers)
//     fillRect(0, FOOTER_Y, PAGE_W, 48, PRIMARY_DARK);
//     fillRect(0, FOOTER_Y, PAGE_W, 4, ACCENT_BLUE);

//     fc(WHITE);
//     doc.font('Helvetica-Bold').fontSize(8)
//        .text('Appin Technology Coimbatore  |  appincoimbatore.com  |  M: 7708040308',
//              MARGIN, FOOTER_Y + 12, { width: CONTENT_W, align: 'center' });
//     fc('#94A3B8');
//     doc.font('Helvetica').fontSize(7)
//        .text(`Generated on ${new Date().toLocaleString('en-IN')}   •   This is a computer-generated invoice and does not require a physical signature.`,
//              MARGIN, FOOTER_Y + 30, { width: CONTENT_W, align: 'center' });

//     doc.end();

//     // ─── Buffer → S3 or local ─────────────────────────────────────────────
//     return await new Promise(async (resolve) => {
//       doc.on('end', async () => {
//         try {
//           const pdfBuffer = Buffer.concat(chunks);

//           if (useS3) {
//             if (!PutObjectCommand || !s3Client) {
//               return resolve({ success: false, message: 'AWS SDK for S3 not installed or misconfigured' });
//             }
//             const bucket = process.env.S3_BUCKET;
//             if (!bucket) return resolve({ success: false, message: 'S3_BUCKET not configured' });
//             try {
//               await s3Client.send(new PutObjectCommand({ Bucket: bucket, Key: fileName, Body: pdfBuffer, ContentType: 'application/pdf' }));
//               const fileUrl = `https://${bucket}.s3.${process.env.S3_REGION}.amazonaws.com/${fileName}`;
//               return resolve({ success: true, filePath: fileUrl, fileName });
//             } catch (err) {
//               return resolve({ success: false, message: `Error uploading to S3: ${err.message}` });
//             }
//           }

//           const outStream = fs.createWriteStream(filePath);
//           outStream.write(pdfBuffer);
//           outStream.end();
//           outStream.on('finish', () =>
//             resolve({ success: true, filePath, fileName, buffer: pdfBuffer, message: `Bill generated successfully for Installment #${installmentIndex + 1}` })
//           );
//           outStream.on('error', err =>
//             resolve({ success: false, message: `Error writing PDF file: ${err.message}` })
//           );
//         } catch (error) {
//           return resolve({ success: false, message: `Error in PDF generation: ${error.message || error}` });
//         }
//       });

//       doc.on('error', err =>
//         resolve({ success: false, message: `Error generating PDF: ${err.message}` })
//       );
//     });
//   } catch (error) {
//     return { success: false, message: `Error in PDF generation: ${error.message}` };
//   }
// }


async function generateBillPDF(lead, installmentIndex) {
  try {
    if (!lead.fees || !lead.fees.installments) {
      return { success: false, message: 'No fees/installments found for this lead' };
    }

    const installment = lead.fees.installments[installmentIndex];
    if (!installment) {
      return { success: false, message: `Installment #${installmentIndex + 1} not found` };
    }

    const fileName = `bill_${lead._id}_installment_${installmentIndex + 1}_${Date.now()}.pdf`;
    const billsDir = path.join(__dirname, 'bills');
    const filePath = path.join(billsDir, fileName);

    const useS3 = (process.env.USE_S3 === 'true' || !!process.env.S3_BUCKET);
    if (!useS3) {
      if (!fs.existsSync(billsDir)) fs.mkdirSync(billsDir, { recursive: true });
    }

    // ─── Design Tokens ────────────────────────────────────────────────────
    const PRIMARY_DARK   = '#0F2B4D';
    const PRIMARY_BLUE   = '#1E3A8A';
    const ACCENT_BLUE    = '#3B82F6';
    const LIGHT_BG       = '#F8FAFC';
    const HEADER_BG      = '#EFF6FF';
    const CARD_BG        = '#FFFFFF';
    const BORDER_LIGHT   = '#E2E8F0';
    const TEXT_PRIMARY   = '#1E293B';
    const TEXT_SECONDARY = '#475569';
    const TEXT_MUTED     = '#64748B';
    const WARNING_ORANGE = '#F59E0B';
    const WHITE          = '#FFFFFF';

    const PAGE_W    = 595.28;
    const PAGE_H    = 841.89;
    const MARGIN    = 40;
    const CONTENT_W = PAGE_W - MARGIN * 2;

    // Column layout
    const COL_GAP = 16;
    const VAL_W   = 150;
    const LBL_W   = CONTENT_W - VAL_W - COL_GAP;

    // Page area constants
    const FOOTER_H    = 48;
    const PAGE_TOP_H  = 8;
    const CONTENT_TOP = PAGE_TOP_H;
    const CONTENT_BOT = PAGE_H - FOOTER_H - 10;

    // bufferPages lets us stamp footer on ALL pages after content is drawn
    const doc = new PDFDocument({
      size: 'A4',
      margin: 0,
      bufferPages: true,
      autoFirstPage: false,
      info: { Title: 'Invoice', Author: 'Appin Technology' }
    });
    const chunks = [];
    doc.on('data', chunk => chunks.push(chunk));

    // ─── Helpers ──────────────────────────────────────────────────────────
    const fc = (hex) => { doc.fillColor(hex); return doc; };
    const sc = (hex) => { doc.strokeColor(hex); return doc; };

    const fillRect = (x, y, w, h, hex, r = 0) => {
      fc(hex);
      r > 0 ? doc.roundedRect(x, y, w, h, r).fill() : doc.rect(x, y, w, h).fill();
    };

    const strokeRect = (x, y, w, h, hex = BORDER_LIGHT, lw = 0.5) => {
      sc(hex);
      doc.rect(x, y, w, h).lineWidth(lw).stroke();
    };

    const vLine = (x, y1, y2, hex = BORDER_LIGHT, lw = 0.5) => {
      sc(hex);
      doc.moveTo(x, y1).lineTo(x, y2).lineWidth(lw).stroke();
    };

    const drawPageTop = () => {
      fillRect(0, 0, PAGE_W, 6, PRIMARY_BLUE);
      fillRect(0, 4, PAGE_W, 4, ACCENT_BLUE);
    };

    let curY = CONTENT_TOP;

    // Before drawing any block of height h, check if it fits — if not, add a new page
    const ensureSpace = (h) => {
      if (curY + h > CONTENT_BOT) {
        doc.addPage();
        drawPageTop();
        curY = MARGIN + 4;
      }
    };

    // ─── Fetch Logo ───────────────────────────────────────────────────────
    let logoBuffer = null;
    try {
      const https = require('https');
      logoBuffer = await new Promise((resolve, reject) => {
        https.get('https://appincoimbatore.com/wp-content/uploads/2023/05/appin-technology-coimbatore.png', res => {
          const bufs = [];
          res.on('data', d => bufs.push(d));
          res.on('end', () => resolve(Buffer.concat(bufs)));
          res.on('error', reject);
        }).on('error', reject);
      });
    } catch (_) { logoBuffer = null; }

    // ══════════════════════════════════════════════════════════════════════
    //  Start page 1
    // ══════════════════════════════════════════════════════════════════════
    doc.addPage();
    drawPageTop();
    curY = MARGIN + 4;

    // ══════════════════════════════════════════════════════════════════════
    //  SECTION 1 — COMPANY HEADER
    // ══════════════════════════════════════════════════════════════════════
    const HEADER_H  = 114;
    const RECEIPT_W = 186;
    const COMPANY_W = CONTENT_W - RECEIPT_W - 8;

    fillRect(MARGIN, curY, COMPANY_W, HEADER_H, CARD_BG, 6);

    let logoEndY = curY + 10;
    if (logoBuffer) {
      try {
        doc.image(logoBuffer, MARGIN + 14, curY + 12, { fit: [145, 50] });
        logoEndY = curY + 66;
      } catch (_) {}
    }

    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(11).text('Appin Technology, Coimbatore', MARGIN + 14, logoEndY);
    fc(TEXT_SECONDARY);
    doc.font('Helvetica').fontSize(8)
       .text('[division of Ether Services]',                             MARGIN + 14, logoEndY + 14)
       .text('144, Sengupta Street, Ram Nagar, Coimbatore - 641009',    MARGIN + 14, logoEndY + 25)
       .text('Mobile: 7708040308',                                            MARGIN + 14, logoEndY + 36)
       .text('GSTIN: 33AAEFE1664N1ZC',                                  MARGIN + 14, logoEndY + 47);

    const RECEIPT_X = MARGIN + COMPANY_W + 8;
    fillRect(RECEIPT_X, curY, RECEIPT_W, HEADER_H, HEADER_BG, 6);
    strokeRect(RECEIPT_X, curY, RECEIPT_W, HEADER_H, BORDER_LIGHT, 1);
    fillRect(RECEIPT_X, curY, RECEIPT_W, 5, ACCENT_BLUE, 6);
    fillRect(RECEIPT_X, curY + 3, RECEIPT_W, 5, ACCENT_BLUE);

    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(9).text('Receipt No:', RECEIPT_X + 14, curY + 16);
    fc(TEXT_SECONDARY);
    doc.font('Helvetica').fontSize(9)
       .text(`${lead._id || '—'}`, RECEIPT_X + 14, curY + 28, { width: RECEIPT_W - 28 });

    sc(BORDER_LIGHT);
    doc.moveTo(RECEIPT_X + 14, curY + 46).lineTo(RECEIPT_X + RECEIPT_W - 14, curY + 46).lineWidth(0.5).stroke();

    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(9).text('Date:', RECEIPT_X + 14, curY + 52);
    fc(TEXT_SECONDARY);
    doc.font('Helvetica').fontSize(9).text(
      new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }),
      RECEIPT_X + 14, curY + 64
    );

    sc(BORDER_LIGHT);
    doc.moveTo(RECEIPT_X + 14, curY + 80).lineTo(RECEIPT_X + RECEIPT_W - 14, curY + 80).lineWidth(0.5).stroke();

    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(9).text('Payment No:', RECEIPT_X + 14, curY + 86);
    fc(TEXT_SECONDARY);
    doc.font('Helvetica').fontSize(9).text(`PAY-${installmentIndex + 1}`, RECEIPT_X + 14, curY + 98);

    curY += HEADER_H + 14;

    // ══════════════════════════════════════════════════════════════════════
    //  SECTION 2 — STUDENT DETAILS
    // ══════════════════════════════════════════════════════════════════════
    const SEC_H = 30;
    const ROW_H = 26;

    const studentRows = [
      ['Name',             lead.userName || 'N/A'],
      ['Course',           lead.course || lead.courseofintrest || 'N/A'],
      ['Mobile',           lead.userNumber || 'N/A'],
      ['Career Counselor', lead.assignedto || lead.counselor || lead.counselorName || 'N/A'],
    ];

    ensureSpace(SEC_H + ROW_H * studentRows.length);

    fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
    fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(11)
       .text('Student Details', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
    curY += SEC_H;

    studentRows.forEach(([label, value], idx) => {
      const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
      fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
      strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
      const dividerX = MARGIN + LBL_W + COL_GAP / 2;
      vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);
      fc(TEXT_MUTED);
      doc.font('Helvetica-Bold').fontSize(8.5).text(label, MARGIN + 14, curY + 8, { width: LBL_W - 20, lineBreak: false });
      fc(TEXT_PRIMARY);
      doc.font('Helvetica').fontSize(9)
         .text(value, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14, lineBreak: false });
      curY += ROW_H;
    });

    curY += 14;

    // ══════════════════════════════════════════════════════════════════════
    //  SECTION 3 — PAYMENT SUMMARY
    // ══════════════════════════════════════════════════════════════════════
    const totalBilledAmount         = lead.fees?.billedAmount || 0;
    const amountPaidThisInstallment = installment.amount || 0;
    const totalPaid                 = (lead.fees?.installments || []).reduce(
      (sum, i) => sum + (i.status === 'Paid' ? (i.amount || 0) : 0), 0
    );
    const balance = totalBilledAmount - totalPaid;

    const summaryRows = [
      ['Total Course Fees',                             `Rs. ${totalBilledAmount.toFixed(2)}`],
      ['Amount Paid',                                   `Rs. ${amountPaidThisInstallment.toFixed(2)}`],
      ['Total Amount Paid Till Now (inclusive of GST)', `Rs. ${totalPaid.toFixed(2)}`],
    ];

    const BAL_H = 32;
    ensureSpace(SEC_H + ROW_H * summaryRows.length + ROW_H + BAL_H + 14);

    fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
    fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(11)
       .text('Payment Summary', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
    curY += SEC_H;

    summaryRows.forEach(([label, value], idx) => {
      const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
      fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
      strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
      const dividerX = MARGIN + LBL_W + COL_GAP / 2;
      vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);
      fc(TEXT_MUTED);
      doc.font('Helvetica-Bold').fontSize(8.5).text(label, MARGIN + 14, curY + 8, { width: LBL_W - 20, lineBreak: false });
      fc(PRIMARY_BLUE);
      doc.font('Helvetica-Bold').fontSize(9)
         .text(value, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14, lineBreak: false });
      curY += ROW_H;
    });

    // Spacer row
    fillRect(MARGIN, curY, CONTENT_W, ROW_H, CARD_BG);
    strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
    curY += ROW_H;

    // Balance row
    fillRect(MARGIN, curY, CONTENT_W, BAL_H, '#FEF3C7', 6);
    strokeRect(MARGIN, curY, CONTENT_W, BAL_H, WARNING_ORANGE, 1.2);
    const balDividerX = MARGIN + LBL_W + COL_GAP / 2;
    vLine(balDividerX, curY + 6, curY + BAL_H - 6, WARNING_ORANGE, 0.8);
    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(10)
       .text('Balance Amount', MARGIN + 14, curY + 13, { width: LBL_W - 20 });
    fc(WARNING_ORANGE);
    doc.font('Helvetica-Bold').fontSize(10)
       .text(`Rs. ${balance.toFixed(2)}`, MARGIN + LBL_W + COL_GAP + 10, curY + 13, { width: VAL_W - 14 });
    curY += BAL_H + 14;

    // ══════════════════════════════════════════════════════════════════════
    //  SECTION 4 — UPCOMING PAYMENTS
    // ══════════════════════════════════════════════════════════════════════
    const upcomingInstallments = (lead.fees?.installments || []).slice(installmentIndex + 1);

    const ordinal = (n) => {
      const s = ['th', 'st', 'nd', 'rd'];
      const v = n % 100;
      return n + (s[(v - 20) % 10] || s[v] || s[0]);
    };

    ensureSpace(SEC_H + ROW_H);
    fillRect(MARGIN, curY, CONTENT_W, SEC_H, HEADER_BG, 6);
    fillRect(MARGIN, curY + SEC_H - 2, CONTENT_W, 2, ACCENT_BLUE);
    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(11)
       .text('Upcoming Payments', MARGIN, curY + 9, { width: CONTENT_W, align: 'center' });
    curY += SEC_H;

    if (upcomingInstallments.length === 0) {
      ensureSpace(ROW_H);
      fillRect(MARGIN, curY, CONTENT_W, ROW_H, LIGHT_BG, 4);
      strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);
      fc(TEXT_MUTED);
      doc.font('Helvetica').fontSize(9).text('No upcoming payments', MARGIN + 14, curY + 8);
      curY += ROW_H;
    } else {
      upcomingInstallments.forEach((inst, idx) => {
        // Each row checked individually — this is what prevents overflow and blank pages
        ensureSpace(ROW_H);

        const humanIndex = installmentIndex + 2 + idx;
        const due = inst.dueDate
          ? new Date(inst.dueDate).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
          : '—';

        const rowBg = idx % 2 === 0 ? CARD_BG : LIGHT_BG;
        fillRect(MARGIN, curY, CONTENT_W, ROW_H, rowBg);
        strokeRect(MARGIN, curY, CONTENT_W, ROW_H, BORDER_LIGHT);

        const dividerX = MARGIN + LBL_W + COL_GAP / 2;
        vLine(dividerX, curY + 4, curY + ROW_H - 4, BORDER_LIGHT, 0.8);

        fc(TEXT_MUTED);
        doc.font('Helvetica-Bold').fontSize(8.5)
           .text(`${ordinal(humanIndex)} Payment`, MARGIN + 14, curY + 4, { width: LBL_W - 20, lineBreak: false });
        fc(TEXT_SECONDARY);
        doc.font('Helvetica').fontSize(7.5)
           .text(`Due: ${due}`, MARGIN + 14, curY + 15, { width: LBL_W - 20, lineBreak: false });
        fc(PRIMARY_BLUE);
        doc.font('Helvetica-Bold').fontSize(9)
           .text(`Rs. ${(inst.amount || 0).toFixed(2)}`, MARGIN + LBL_W + COL_GAP + 10, curY + 8, { width: VAL_W - 14, lineBreak: false });
        curY += ROW_H;
      });
    }

    curY += 16;

    // ══════════════════════════════════════════════════════════════════════
    //  SECTION 5 — NOTES & SIGNATURE
    // ══════════════════════════════════════════════════════════════════════
    const notes = [
      'This is Computer-generated Receipt',
      'No signature required.',
      'Fees once paid are non-refundable.',
    ];
    const NOTES_H    = notes.length * 20 + 20;
    const NOTE_HDR_H = 28;

    ensureSpace(NOTE_HDR_H + NOTES_H);

    fillRect(MARGIN, curY, CONTENT_W, NOTE_HDR_H, HEADER_BG, 6);
    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(10).text('Note', MARGIN + 14, curY + 9);
    curY += NOTE_HDR_H;

    fillRect(MARGIN, curY, CONTENT_W, NOTES_H, CARD_BG, 6);
    strokeRect(MARGIN, curY, CONTENT_W, NOTES_H, BORDER_LIGHT, 1);

    fillRect(MARGIN, curY, 4, NOTES_H, ACCENT_BLUE, 2);
    fillRect(MARGIN + 2, curY, 2, NOTES_H, ACCENT_BLUE);

    notes.forEach((note, i) => {
      fc(TEXT_SECONDARY);
      doc.font('Helvetica').fontSize(8.5)
         .text(`• ${note}`, MARGIN + 18, curY + 10 + i * 20, { width: CONTENT_W - 170 });
    });

    const SIG_X     = PAGE_W - MARGIN - 165;
    const SIG_BOX_H = NOTES_H - 16;
    const SIG_MID_Y = curY + 8 + SIG_BOX_H / 2;
    fillRect(SIG_X, curY + 8, 155, SIG_BOX_H, '#F8FAFC', 4);
    strokeRect(SIG_X, curY + 8, 155, SIG_BOX_H, BORDER_LIGHT, 0.8);

    fc(TEXT_PRIMARY);
    doc.font('Helvetica-Bold').fontSize(9)
       .text('For Accounts', SIG_X, SIG_MID_Y - 10, { width: 155, align: 'center' });
    fc(TEXT_MUTED);
    doc.font('Helvetica').fontSize(8)
       .text('Appin Technology, Coimbatore', SIG_X, SIG_MID_Y + 4, { width: 155, align: 'center' });

    // ══════════════════════════════════════════════════════════════════════
    //  FOOTER — stamped on every page via bufferPages
    // ══════════════════════════════════════════════════════════════════════
    const FOOTER_Y = PAGE_H - FOOTER_H;

    const range = doc.bufferedPageRange();
    for (let i = 0; i < range.count; i++) {
      doc.switchToPage(range.start + i);
      fillRect(0, FOOTER_Y, PAGE_W, FOOTER_H, PRIMARY_DARK);
      fillRect(0, FOOTER_Y, PAGE_W, 4, ACCENT_BLUE);
      fc(WHITE);
      doc.font('Helvetica-Bold').fontSize(8)
         .text('Appin Technology Coimbatore  |  appincoimbatore.com  |  Mobile: 7708040308',
               MARGIN, FOOTER_Y + 12, { width: CONTENT_W, align: 'center' });
      fc('#94A3B8');
      doc.font('Helvetica').fontSize(7)
         .text(`Generated on ${new Date().toLocaleString('en-IN')}   •   This is a computer-generated invoice and does not require a physical signature.`,
               MARGIN, FOOTER_Y + 30, { width: CONTENT_W, align: 'center' });
    }

    doc.end();

    // ─── Buffer → S3 or local ─────────────────────────────────────────────
    return await new Promise(async (resolve) => {
      doc.on('end', async () => {
        try {
          const pdfBuffer = Buffer.concat(chunks);

          if (useS3) {
            if (!PutObjectCommand || !s3Client) {
              return resolve({ success: false, message: 'AWS SDK for S3 not installed or misconfigured' });
            }

            const bucket = process.env.S3_BUCKET;
            if (!bucket) return resolve({ success: false, message: 'S3_BUCKET not configured' });

            const key = fileName;

            try {
              await s3Client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: pdfBuffer, ContentType: 'application/pdf' }));
              const fileUrl = `https://${bucket}.s3.${process.env.S3_REGION}.amazonaws.com/${key}`;

              return resolve({
                success: true,
                filePath: fileUrl,
                fileName,
              });
            } catch (err) {
              return resolve({ success: false, message: `Error uploading to S3: ${err.message}` });
            }
          }

          // Local file fallback: write the buffer to disk
          const outStream = fs.createWriteStream(filePath);
          outStream.write(pdfBuffer);
          outStream.end();
          outStream.on('finish', () => {
            return resolve({ success: true, filePath, fileName, buffer: pdfBuffer, message: `Bill generated successfully for Installment #${installmentIndex + 1}` });
          });
          outStream.on('error', (err) => {
            return resolve({ success: false, message: `Error writing PDF file: ${err.message}` });
          });
        } catch (error) {
          return resolve({ success: false, message: `Error in PDF generation: ${error.message || error}` });
        }
      });

      doc.on('error', (err) => {
        return resolve({ success: false, message: `Error generating PDF: ${err.message}` });
      });
    });
  } catch (error) {
    return {
      success: false,
      message: `Error in PDF generation: ${error.message}`
    };
  }
}

// Email sending removed.

/**
 * Generate and send bill
 * @param {Object} lead - Lead document
 * @param {Number} installmentIndex - Index of installment (0-based)
 * @param {Boolean} sendEmail - Whether to send email
 * @returns {Promise<{success: boolean, message: string, billGenerated: boolean, emailSent: boolean}>}
 */
async function generateAndSendBill(lead, installmentIndex, sendEmail = true) {
  try {
    // Step 1: Validate installment exists
    if (!lead.fees || !lead.fees.installments || !lead.fees.installments[installmentIndex]) {
      return {
        success: false,
        message: `Installment #${installmentIndex + 1} not found`,
        billGenerated: false,
        emailSent: false
      };
    }

    // Step 2: Generate PDF
    const pdfResult = await generateBillPDF(lead, installmentIndex);
    
    if (!pdfResult.success) {
      return {
        success: false,
        message: pdfResult.message,
        billGenerated: false,
        emailSent: false
      };
    }

    // Return result including the generated file URL (local path or s3:// URL)
    return {
      success: true,
      message: `Bill generated for Installment #${installmentIndex + 1}`,
      billGenerated: true,
      emailSent: false,
      details: {
        billFile: pdfResult.fileName,
        billUrl: pdfResult.filePath,
        buffer: pdfResult.buffer
      }
    };
  } catch (error) {
    return {
      success: false,
      message: `Error in generateAndSendBill: ${error.message}`,
      billGenerated: false,
      emailSent: false
    };
  }
}

module.exports = {
  generateBillPDF,
  generateAndSendBill
};

/**
 * Delete a generated bill file from S3 or local filesystem
 * @param {String} fileRef - S3 URL, filename, or local path
 * @returns {Promise<{success:boolean,message:string}>}
 */
async function deleteBill(fileRef) {
  try {
    if (!fileRef) return { success: false, message: 'No file reference provided' };

    const useS3 = (process.env.USE_S3 === 'true' || !!process.env.S3_BUCKET);

    // If looks like an S3 URL, attempt S3 deletion
    if (useS3 && typeof fileRef === 'string' && fileRef.includes('.s3.')) {
      if (!DeleteObjectCommand || !s3Client) return { success: false, message: 'S3 SDK not available' };
      const bucket = process.env.S3_BUCKET;
      if (!bucket) return { success: false, message: 'S3_BUCKET not configured' };

      // Extract key from URL (strip query string if any)
      const urlWithoutQuery = fileRef.split('?')[0];
      const parts = urlWithoutQuery.split('/');
      const key = parts.slice(-1)[0];

      try {
        await s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
        return { success: true, message: `Deleted S3 object ${key}` };
      } catch (err) {
        return { success: false, message: `S3 deletion error: ${err.message}` };
      }
    }

    // Otherwise attempt local deletion — support filename or path
    const billsDir = path.join(__dirname, 'bills');
    let localPath = fileRef;
    if (!path.isAbsolute(localPath)) {
      localPath = path.join(billsDir, localPath);
    }

    if (fs.existsSync(localPath)) {
      try {
        fs.unlinkSync(localPath);
        return { success: true, message: `Deleted local file ${localPath}` };
      } catch (err) {
        return { success: false, message: `Local deletion error: ${err.message}` };
      }
    }

    return { success: false, message: 'File not found' };
  } catch (err) {
    return { success: false, message: `deleteBill error: ${err.message || err}` };
  }
}

// Re-export including deleteBill
module.exports = Object.assign({}, module.exports, { deleteBill });
