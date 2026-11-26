const META_GRAPH_VERSION = process.env.META_GRAPH_VERSION || 'v24.0';

/**
 * Returns WhatsApp credentials and config for the given tenant.
 * Reads WABA_ID / WHATSAPP_TOKEN (+ optional META_APP_ID, WA_PHONE_NUMBER_ID, WA_EVENTS_SECRET) from .env.
 */
function getWaConfig(tenantId = 'default') {
  const wabaId = (process.env.WABA_ID || '').trim();
  const token = (process.env.WHATSAPP_TOKEN || '').trim();
  const appId = (process.env.META_APP_ID || '').trim();
  const phoneNumberId = (process.env.WA_PHONE_NUMBER_ID || '').trim();
  const eventsSecret = (process.env.WA_EVENTS_SECRET || '').trim();

  if (!wabaId || !token) {
    const err = new Error('WABA_ID or WHATSAPP_TOKEN not configured in .env');
    err.status = 500;
    throw err;
  }

  return {
    tenantId,
    wabaId,
    token,
    appId,
    phoneNumberId,
    eventsSecret,
    apiVersion: META_GRAPH_VERSION,
    baseUrl: `https://graph.facebook.com/${META_GRAPH_VERSION}`,
  };
}

/**
 * Format Meta API error response into a clean Error object.
 */
function formatMetaError(status, body) {
  const errObj = body?.error || {};
  const message =
    errObj.error_user_msg ||
    errObj.error_user_title ||
    errObj.message ||
    `Meta API Error (${status})`;

  const err = new Error(message);
  err.status = status >= 400 && status < 500 ? status : 502;
  err.metaError = errObj;
  err.fbtrace_id = errObj.fbtrace_id;
  err.code = errObj.code;
  err.subcode = errObj.error_subcode;
  return err;
}

/**
 * Meta Graph API GET request
 */
async function metaGraphGet(path, tenantId = 'default') {
  const config = getWaConfig(tenantId);
  const url = path.startsWith('http') ? path : `${config.baseUrl}/${path}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${config.token}`,
      Accept: 'application/json',
    },
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw formatMetaError(response.status, body);
  }
  return body;
}

/**
 * Meta Graph API POST request
 */
async function graphPost(path, payload = {}, tenantId = 'default') {
  const config = getWaConfig(tenantId);
  const url = path.startsWith('http') ? path : `${config.baseUrl}/${path}`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.token}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw formatMetaError(response.status, body);
  }
  return body;
}

/**
 * Meta Graph API DELETE request
 */
async function graphDelete(path, tenantId = 'default') {
  const config = getWaConfig(tenantId);
  const url = path.startsWith('http') ? path : `${config.baseUrl}/${path}`;

  const response = await fetch(url, {
    method: 'DELETE',
    headers: {
      Authorization: `Bearer ${config.token}`,
      Accept: 'application/json',
    },
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw formatMetaError(response.status, body);
  }
  return body;
}

/**
 * Meta Resumable Upload for message template media headers.
 * Returns the permanent header_handle to be used in components.HEADER.example.header_handle.
 */
async function uploadHeaderMedia(fileBuffer, mimeType, fileName = 'sample', tenantId = 'default') {
  const config = getWaConfig(tenantId);
  const appId = config.appId;

  if (!appId) {
    const err = new Error('META_APP_ID is not configured in .env. Required for Meta Resumable Uploads.');
    err.status = 400;
    throw err;
  }

  // 1. Initiate upload session
  const initUrl = `${config.baseUrl}/${appId}/uploads?file_length=${fileBuffer.length}&file_type=${encodeURIComponent(mimeType)}&file_name=${encodeURIComponent(fileName)}`;
  const initRes = await fetch(initUrl, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.token}`,
      Accept: 'application/json',
    },
  });

  const initBody = await initRes.json().catch(() => ({}));
  if (!initRes.ok) {
    throw formatMetaError(initRes.status, initBody);
  }

  const uploadSessionId = initBody.id;
  if (!uploadSessionId) {
    throw new Error('Meta did not return an upload session id');
  }

  // 2. Upload file data to session
  const uploadUrl = `${config.baseUrl}/${uploadSessionId}`;
  const uploadRes = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      Authorization: `OAuth ${config.token}`,
      file_offset: '0',
      'Content-Type': 'application/octet-stream',
    },
    body: fileBuffer,
  });

  const uploadBody = await uploadRes.json().catch(() => ({}));
  if (!uploadRes.ok) {
    throw formatMetaError(uploadRes.status, uploadBody);
  }

  const handle = uploadBody.h;
  if (!handle) {
    throw new Error('Meta upload succeeded but did not return a handle (h)');
  }

  return {
    handle,
    mimeType,
    fileName,
    size: fileBuffer.length,
  };
}

/**
 * Discovers or retrieves the primary WhatsApp Phone Number ID for sending messages.
 */
async function getWaPhoneNumberId(tenantId = 'default') {
  const config = getWaConfig(tenantId);
  if (config.phoneNumberId) return config.phoneNumberId;

  // Auto-discover from Meta WABA phone numbers
  const res = await metaGraphGet(`${config.wabaId}/phone_numbers?fields=id,display_phone_number,status`, tenantId);
  const numbers = res.data || [];
  if (numbers.length === 0 || !numbers[0].id) {
    throw new Error('No WhatsApp phone numbers found attached to this WABA account. Please configure WA_PHONE_NUMBER_ID in .env.');
  }

  return numbers[0].id;
}

/**
 * Send a WhatsApp template message (used for test messages and bulk messages)
 */
async function sendTemplateMessage({
  to,
  templateName,
  language = 'en_US',
  components = [],
  tenantId = 'default',
}) {
  const config = getWaConfig(tenantId);
  const phoneNumberId = await getWaPhoneNumberId(tenantId);

  // Clean phone number (strip spaces, symbols, ensure country code)
  const cleanNumber = String(to || '').replace(/\D/g, '');
  if (!cleanNumber || cleanNumber.length < 10) {
    throw new Error(`Invalid recipient phone number: "${to}"`);
  }

  const payload = {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: cleanNumber,
    type: 'template',
    template: {
      name: templateName,
      language: {
        code: language,
      },
      ...(Array.isArray(components) && components.length > 0 ? { components } : {}),
    },
  };

  const url = `${config.baseUrl}/${phoneNumberId}/messages`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.token}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw formatMetaError(res.status, body);
  }

  return body;
}

module.exports = {
  getWaConfig,
  metaGraphGet,
  graphPost,
  graphDelete,
  uploadHeaderMedia,
  getWaPhoneNumberId,
  sendTemplateMessage,
};
