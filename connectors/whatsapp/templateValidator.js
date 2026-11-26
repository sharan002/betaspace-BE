/**
 * Pre-submission validation for WhatsApp Message Templates according to Meta's strict rules.
 * Running this before calling Meta saves API rate limits and gives immediate, actionable error messages.
 */

function validateTemplatePayload(payload) {
  const errors = [];

  const {
    name,
    language,
    category,
    components = [],
  } = payload || {};

  // 1. Name validation
  if (!name || typeof name !== 'string' || !name.trim()) {
    errors.push('Template name is required.');
  } else {
    const trimmedName = name.trim();
    if (trimmedName.length > 512) {
      errors.push('Template name cannot exceed 512 characters.');
    }
    // Meta requires lowercase letters, numbers, and underscores only
    if (!/^[a-z0-9_]+$/.test(trimmedName)) {
      errors.push('Template name can only contain lowercase alphanumeric characters and underscores (e.g., student_admission_update). Spaces and uppercase letters are not allowed.');
    }
  }

  // 2. Language validation
  if (!language || typeof language !== 'string' || !language.trim()) {
    errors.push('Language code is required (e.g., en_US).');
  }

  // 3. Category validation
  const validCategories = ['MARKETING', 'UTILITY', 'AUTHENTICATION'];
  if (!category || !validCategories.includes(String(category).toUpperCase())) {
    errors.push('Category must be one of: UTILITY, MARKETING, or AUTHENTICATION.');
  }

  // 4. Components validation
  if (!Array.isArray(components) || components.length === 0) {
    errors.push('Template must include at least a BODY component.');
  }

  const bodyComp = components.find(c => c && c.type === 'BODY');
  const headerComp = components.find(c => c && c.type === 'HEADER');
  const footerComp = components.find(c => c && c.type === 'FOOTER');
  const buttonsComp = components.find(c => c && c.type === 'BUTTONS');

  // Body is strictly mandatory
  if (!bodyComp || !bodyComp.text || !bodyComp.text.trim()) {
    errors.push('Body component with non-empty text is mandatory.');
  } else {
    const bodyText = bodyComp.text.trim();

    if (bodyText.length > 1024) {
      errors.push(`Body text exceeds Meta limit of 1024 characters (currently ${bodyText.length} characters).`);
    }

    // Meta rule: Body cannot start or end with a parameter
    if (/^\{\{\d+\}\}/.test(bodyText)) {
      errors.push('Meta rule violation: Body text cannot start with a variable (e.g., {{1}}). Add some introductory text before the variable.');
    }
    if (/\{\{\d+\}\}$/.test(bodyText)) {
      errors.push('Meta rule violation: Body text cannot end with a variable (e.g., {{1}}). Add a closing punctuation or sign-off after the variable.');
    }

    // Check sequential variables {{1}}, {{2}}, ...
    const varMatches = [...bodyText.matchAll(/\{\{(\d+)\}\}/g)].map(m => parseInt(m[1], 10));
    if (varMatches.length > 0) {
      const uniqueVars = Array.from(new Set(varMatches)).sort((a, b) => a - b);
      for (let i = 0; i < uniqueVars.length; i++) {
        if (uniqueVars[i] !== i + 1) {
          errors.push(`Variables must be sequential starting from {{1}} without skipping. Found {{${uniqueVars[i]}}}, expected {{${i + 1}}}.`);
          break;
        }
      }

      // Check if example values are provided for every body variable
      const sampleValues = bodyComp.example?.body_text?.[0] || [];
      if (!Array.isArray(sampleValues) || sampleValues.length < uniqueVars.length) {
        errors.push(`Please provide sample values for all ${uniqueVars.length} variable(s) in the body (e.g., Rahul, React JS). Meta requires samples for review.`);
      } else {
        sampleValues.forEach((val, idx) => {
          if (!val || !String(val).trim()) {
            errors.push(`Sample value for variable {{${idx + 1}}} cannot be empty.`);
          }
        });
      }

      // Check variable-to-text density ratio (Meta rejects templates where variables dominate the message)
      const textWithoutVars = bodyText.replace(/\{\{\d+\}\}/g, ' ').trim();
      const words = textWithoutVars.split(/\s+/).filter(Boolean);
      const varCount = uniqueVars.length;
      if (varCount >= 2 && words.length < varCount * 2) {
        errors.push(
          `This template has too many variables for its length (${varCount} variables for only ${words.length} static words). Reduce the number of variables or add more explanatory message text to prevent Meta rejection.`
        );
      }
    }
  }

  // Header validation (optional)
  if (headerComp) {
    if (!['TEXT', 'IMAGE', 'DOCUMENT', 'VIDEO', 'LOCATION'].includes(headerComp.format)) {
      errors.push('Header format must be one of: TEXT, IMAGE, DOCUMENT, VIDEO, or LOCATION.');
    }

    if (headerComp.format === 'TEXT') {
      const headerText = (headerComp.text || '').trim();
      if (!headerText) {
        errors.push('Header text cannot be empty when header format is TEXT.');
      } else if (headerText.length > 60) {
        errors.push(`Header text exceeds Meta limit of 60 characters (currently ${headerText.length} characters).`);
      }

      // Check variable in header (Meta allows max 1 variable in header: {{1}})
      const headerVars = [...headerText.matchAll(/\{\{(\d+)\}\}/g)].map(m => parseInt(m[1], 10));
      if (headerVars.length > 1) {
        errors.push('Header text can contain at most one variable {{1}}.');
      } else if (headerVars.length === 1) {
        if (headerVars[0] !== 1) {
          errors.push('Header variable must be {{1}}.');
        }
        const sampleHeader = headerComp.example?.header_text?.[0];
        if (!sampleHeader || !String(sampleHeader).trim()) {
          errors.push('Please provide a sample value for the header variable {{1}}.');
        }
      }
    } else if (['IMAGE', 'DOCUMENT', 'VIDEO'].includes(headerComp.format)) {
      // For media headers, Meta requires a header_handle in example
      const handle = headerComp.example?.header_handle?.[0];
      if (!handle) {
        errors.push(`Media headers (${headerComp.format}) require an uploaded sample media file handle for Meta review.`);
      }
    }
  }

  // Footer validation (optional)
  if (footerComp) {
    const footerText = (footerComp.text || '').trim();
    if (footerText.length > 60) {
      errors.push(`Footer text exceeds Meta limit of 60 characters (currently ${footerText.length} characters).`);
    }
    if (/\{\{\d+\}\}/.test(footerText)) {
      errors.push('Variables are not permitted in template footer text.');
    }
  }

  // Buttons validation (optional)
  if (buttonsComp) {
    const buttons = buttonsComp.buttons || [];
    if (!Array.isArray(buttons) || buttons.length === 0) {
      errors.push('BUTTONS component defined without any buttons.');
    } else if (buttons.length > 10) {
      errors.push('Template can have a maximum of 10 buttons total.');
    } else {
      let quickReplyCount = 0;
      buttons.forEach((btn, idx) => {
        if (!btn.text || !btn.text.trim()) {
          errors.push(`Button #${idx + 1} text is required.`);
        } else if (btn.text.trim().length > 25) {
          errors.push(`Button #${idx + 1} text exceeds 25 characters.`);
        }

        if (btn.type === 'QUICK_REPLY') {
          quickReplyCount++;
        } else if (btn.type === 'URL') {
          if (!btn.url || !btn.url.trim()) {
            errors.push(`Button #${idx + 1} (URL) requires a valid URL.`);
          } else if (!/^https?:\/\//i.test(btn.url.trim())) {
            errors.push(`Button #${idx + 1} URL must start with http:// or https://.`);
          }
        } else if (btn.type === 'PHONE_NUMBER') {
          if (!btn.phone_number || !btn.phone_number.trim()) {
            errors.push(`Button #${idx + 1} (Phone) requires a phone number with country code.`);
          }
        }
      });

      if (quickReplyCount > 10) {
        errors.push('Maximum 10 quick reply buttons allowed.');
      }
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
}

module.exports = {
  validateTemplatePayload,
};
