// One-off: renames legacy education-era pipeline values on existing leads to the
// BetaSpace co-working stages. Dry-run by default — pass --apply to write.
//   node migratePipelineStages.js           (preview counts)
//   node migratePipelineStages.js --apply   (perform the rename)
// Uses the raw collection so Chat's pipeline-change hooks (PipelineChange audit
// log, pipelineChangedAt) are not fired for what is a pure rename.
require('dotenv').config();
const mongoose = require('mongoose');
const { Chat } = require('./Schema');

const MAPPING = [
  ['New', 'New Lead'],
  ['RNR', 'Not Reachable'],
  ['Asked Time', 'Contacted'],
  ['Demo Scheduled', 'Site Visit'],
  ['Pre-Enrollment', 'Proposal Shared'],
  ['Enrolled', 'Converted'],
  ['Not Intrested', 'Not Interested'],
  ['Irrelevant', 'Not Interested'],
  // "Looking for job" (HR handoff) and "Contacted" are intentionally left as-is.
];

(async () => {
  const apply = process.argv.includes('--apply');
  await mongoose.connect(process.env.MONGO_URI || process.env.MONGODB_URI);
  for (const [from, to] of MAPPING) {
    const filter = { pipeline: from };
    const count = await Chat.collection.countDocuments(filter);
    if (!count) continue;
    if (apply) {
      const r = await Chat.collection.updateMany(filter, { $set: { pipeline: to } });
      console.log(`${from} -> ${to}: ${r.modifiedCount} updated`);
    } else {
      console.log(`${from} -> ${to}: ${count} would be updated`);
    }
  }
  await mongoose.disconnect();
})().catch(e => { console.error(e); process.exit(1); });
