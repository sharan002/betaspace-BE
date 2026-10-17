// BetaSpace co-working sales pipeline. `Chat.pipeline` is a free-form String, so
// these constants are the single source of truth for stage names that the
// backend branches on. Legacy education-era values ("Enrolled", "Not Intrested")
// are still matched so leads created before the switch keep working.
const PIPELINE_STAGES = [
  "New Lead",
  "Contacted",
  "Site Visit",
  "Proposal Shared",
  "Negotiation",
  "Advance Payment",
  "Converted",
  "Not Interested",
  "On Hold",
  "Not Reachable",
];

const DEFAULT_PIPELINE = "New Lead";

// A won deal. "Enrolled" is the legacy name for the same stage.
const CONVERTED_STAGES = ["Converted", "Enrolled"];
const CONVERTED_RE = /^(converted|enrolled)$/i;

const NOT_INTERESTED_RES = [/^not interested$/i, /^not intrested$/i];

// Stage → Meta Conversions API event.
const META_CAPI_STAGES = {
  "Site Visit": "Qualified Lead",
  "Pre-Enrollment": "Qualified Lead", // legacy
  "Converted": "Converted Lead",
  "Enrolled": "Converted Lead", // legacy
};

module.exports = {
  PIPELINE_STAGES,
  DEFAULT_PIPELINE,
  CONVERTED_STAGES,
  CONVERTED_RE,
  NOT_INTERESTED_RES,
  META_CAPI_STAGES,
};
