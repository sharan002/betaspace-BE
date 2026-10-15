const axios = require("axios");

// Meta Conversions API for Leads — reports CRM lead-stage changes (e.g. "Qualified Lead")
// back to Meta so Lead Ads campaigns can optimize for lead quality, not just volume.
// Docs: https://developers.facebook.com/documentation/ads-commerce/conversions-api/conversion-leads-integration
// Payload shape below matches the request verified working directly against the Graph API.

const API_VERSION = process.env.META_CAPI_API_VERSION || "v21.0";
const DATASET_ID = process.env.META_CAPI_DATASET_ID;
const ACCESS_TOKEN = process.env.META_CAPI_ACCESS_TOKEN;
const TEST_EVENT_CODE = process.env.META_CAPI_TEST_EVENT_CODE || null;
const LEAD_EVENT_SOURCE = process.env.META_CAPI_LEAD_EVENT_SOURCE || "CRM";
const EVENT_SOURCE = process.env.META_CAPI_EVENT_SOURCE || "minminicrm";

/**
 * Sends a lead-stage event to Meta's Conversions API for Leads.
 * Skips silently (does not throw) if CAPI isn't configured or the lead has no
 * Meta lead_id — only genuine Meta Lead Ads leads can be optimized this way.
 */
async function sendLeadCapiEvent(chat, eventName) {
  console.log("sendingtometa",eventName)
  if (!DATASET_ID || !ACCESS_TOKEN) {
    return { success: false, skipped: true, message: "Meta CAPI is not configured (missing dataset id / access token)." };
  }
  if (!chat?.leadgenId) {
    return { success: false, skipped: true, message: "Lead has no Meta lead_id — not a Meta Lead Ads lead." };
  }

  const payload = {
    data: [
      {
        action_source: "system_generated",
        custom_data: {
          event_source: "appincrm",
          lead_event_source: "CRM",
        },
        event_name: "Converted Lead",
        event_time: Math.floor(Date.now() / 1000),
        user_data: {
          lead_id: String(chat.leadgenId),
        },
      },
    ],
  };
  if (TEST_EVENT_CODE) payload.test_event_code = TEST_EVENT_CODE;

  const url = `https://graph.facebook.com/${API_VERSION}/${DATASET_ID}/events?access_token=${ACCESS_TOKEN}`;

  try {
    console.log(payload)
    const response = await axios.post(url, payload);
    return { success: true, message: `Sent to Meta (${response.data?.events_received ?? 1} event received).` };
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message;
    console.error("❌ Meta CAPI send failed:", message);
    return { success: false, message };
  }
}

module.exports = { sendLeadCapiEvent };
