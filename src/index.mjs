// ============================================================
// HubSpot → Google Ads Offline Conversion Tracking
// Node.js version — runs via GitHub Actions
// ============================================================
// Required environment variables (set as GitHub Secrets):
//   HUBSPOT_API_KEY             — HubSpot private app token
//   GOOGLE_SERVICE_ACCOUNT_JSON — Full JSON key for service account
//   SPREADSHEET_ID              — Google Sheet ID
//   ALERT_EMAIL                 — Email to notify on failure
// ============================================================

import fetch from "node-fetch";
import { google } from "googleapis";
import crypto from "crypto";
import nodemailer from "nodemailer";

// ============================================================
// CONFIG
// ============================================================
const CONFIG = {
  // How far back to fetch contacts from HubSpot (by lastmodifieddate).
  // Cast a wide net — stage dates are not server-side filterable.
  LOOKBACK_DAYS: 90,

  // How far back to include contacts in the Google Ads upload sheet (by stage date).
  // Only contacts who entered their stage within this window get uploaded.
  // A 7-day buffer gives cover if the workflow misses a day or two.
  UPLOAD_LOOKBACK_DAYS: 7,

  // Set to true for a one-off full historical pull. Bypasses the upload window filter.
  BACKFILL_MODE: false,

  HUBSPOT_API_BASE: "https://api.hubapi.com",

  CONTACT_PROPERTIES: [
    "email",
    "phone",
    "hs_google_click_id",
    "lifecyclestage",
    "createdate",
    "hs_lifecyclestage_opportunity_date",
    "hs_lifecyclestage_customer_date",
    "total_revenue"
  ],

  SHEETS: {
    RAW: "RAW_HUBSPOT",
    UPLOAD: "GOOGLE_ADS_UPLOAD",
    LOG: "EXECUTION_LOG"
  },

  CONVERSION_MAP: {
    opportunity: "Hubspot Contacts - Opportunity",
    customer: "Hubspot Contacts - Confirmed"
  },

  CURRENCY: "USD",
  SPREADSHEET_ID: process.env.SPREADSHEET_ID
};


// ============================================================
// MAIN
// ============================================================
async function main() {
  const startTime = new Date();
  const errors = [];
  let rawRowCount = 0;
  let uploadRowCount = 0;

  console.log(`dailyRun started at: ${startTime.toISOString()}`);

  const sheetsClient = await getSheetsClient();
  let contacts = [];

  // Step 1: Fetch from HubSpot, write full set to RAW sheet
  try {
    contacts = await fetchContacts();
    rawRowCount = contacts.length;
    console.log(`Step 1 complete. Contacts fetched: ${rawRowCount}`);
    await writeRawData(sheetsClient, contacts);
  } catch (e) {
    errors.push(`Step 1 (HubSpot fetch) failed: ${e.message}`);
    console.error(errors[errors.length - 1]);
  }

  // Step 2: Filter to new stage transitions, write upload sheet
  if (errors.length === 0) {
    try {
      uploadRowCount = await formatForGoogleAds(sheetsClient, contacts);
      console.log(`Step 2 complete. Upload rows: ${uploadRowCount}`);
    } catch (e) {
      errors.push(`Step 2 (Google Ads transform) failed: ${e.message}`);
      console.error(errors[errors.length - 1]);
    }
  } else {
    console.log("Skipping Step 2 — Step 1 failed.");
  }

  const status = errors.length > 0 ? "FAILED" : "OK";
  await writeExecutionLog(sheetsClient, startTime, status, rawRowCount, uploadRowCount, errors);

  if (errors.length > 0) {
    await sendErrorAlert(errors, startTime);
    process.exit(1);
  } else {
    console.log("dailyRun completed successfully.");
  }
}


// ============================================================
// STEP 1: FETCH CONTACTS FROM HUBSPOT
// ============================================================
async function fetchContacts() {
  const token = process.env.HUBSPOT_API_KEY;
  if (!token) throw new Error("HUBSPOT_API_KEY not set.");

  const cutoffMs = CONFIG.BACKFILL_MODE
    ? 0
    : Date.now() - CONFIG.LOOKBACK_DAYS * 24 * 60 * 60 * 1000;

  if (CONFIG.BACKFILL_MODE) {
    console.log("BACKFILL_MODE enabled — fetching all opportunity and customer contacts.");
  } else {
    console.log(`Fetch cutoff (lastmodifieddate): ${new Date(cutoffMs).toISOString()}`);
  }

  const opportunities = await searchContacts(token, "opportunity", cutoffMs);
  const confirmed = await searchContacts(token, "customer", cutoffMs);

  console.log(`Opportunities: ${opportunities.length} | Confirmed: ${confirmed.length}`);
  return [...opportunities, ...confirmed];
}


async function searchContacts(token, stage, cutoffMs) {
  const allContacts = [];
  let after = undefined;

  const filters = [
    { propertyName: "lifecyclestage", operator: "EQ", value: stage }
  ];

  if (cutoffMs > 0) {
    filters.push({
      propertyName: "lastmodifieddate",
      operator: "GTE",
      value: cutoffMs
    });
  }

  while (true) {
    const payload = {
      filterGroups: [{ filters }],
      properties: CONFIG.CONTACT_PROPERTIES,
      limit: 100,
      ...(after && { after })
    };

    const response = await hubspotPost(
      `${CONFIG.HUBSPOT_API_BASE}/crm/v3/objects/contacts/search`,
      token,
      payload
    );

    allContacts.push(...response.results);

    if (response.paging?.next?.after) {
      after = response.paging.next.after;
      await sleep(300); // Stay well within HubSpot's 4 req/sec search limit
    } else {
      break;
    }
  }

  return allContacts;
}


// ============================================================
// STEP 2: FILTER + TRANSFORM → GOOGLE ADS UPLOAD SHEET
// Operates on in-memory contacts — no sheet read required.
// Only contacts who entered their stage within UPLOAD_LOOKBACK_DAYS
// are written to the upload sheet. Older contacts stay in RAW only.
// ============================================================
async function formatForGoogleAds(sheetsClient, contacts) {
  const uploadCutoffMs = CONFIG.BACKFILL_MODE
    ? 0
    : Date.now() - CONFIG.UPLOAD_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;

  if (!CONFIG.BACKFILL_MODE) {
    console.log(`Upload cutoff (stage date): ${new Date(uploadCutoffMs).toISOString()}`);
  }

  const outputRows = [];
  let skipped = 0;

  for (const contact of contacts) {
    const p = contact.properties;
    const stage = (p.lifecyclestage || "").toLowerCase().trim();
    const conversionName = CONFIG.CONVERSION_MAP[stage];

    if (!conversionName) { skipped++; continue; }

    const email = (p.email || "").toLowerCase().trim();
    const gclid = (p.hs_google_click_id || "").trim();

    if (!email && !gclid) { skipped++; continue; }

    // Use the stage-specific transition date as conversion time
    const stageDate = getStageDateForContact(contact);
    if (!stageDate) { skipped++; continue; }
    if (stageDate.getTime() < uploadCutoffMs) { skipped++; continue; }

    const hashedEmail = email ? hashEmail(email) : "";
    const hashedPhone = p.phone ? hashPhone(p.phone) : "";
    const conversionTime = formatDateForGoogleAds(stageDate);

    let conversionValue = "";
    if (stage === "customer") {
      const revenue = parseFloat(p.total_revenue);
      if (!isNaN(revenue) && revenue > 0) {
        conversionValue = revenue;
      }
    }

    outputRows.push([
      gclid,
      hashedEmail,
      hashedPhone,
      conversionName,
      conversionTime,
      CONFIG.CURRENCY,
      conversionValue
    ]);
  }

  console.log(`Upload rows: ${outputRows.length} | Skipped: ${skipped}`);

  const headers = [
    "Google Click ID",
    "Email",
    "Phone Number",
    "Conversion Name",
    "Conversion Time",
    "Conversion Currency",
    "Conversion Value"
  ];

  await clearAndWriteSheet(sheetsClient, CONFIG.SHEETS.UPLOAD, [headers, ...outputRows]);
  console.log(`Wrote ${outputRows.length} rows to ${CONFIG.SHEETS.UPLOAD}`);

  return outputRows.length;
}


// ============================================================
// WRITE RAW DATA TO SHEET
// Full set of fetched contacts — used for auditing.
// Stage date is stored in place of createdate.
// ============================================================
async function writeRawData(sheetsClient, contacts) {
  const headers = [
    "Contact ID",
    "Email",
    "Phone",
    "GCLID",
    "Lifecycle Stage",
    "Contact Create Date",
    "Stage Date (Raw)",
    "Stage Date (Formatted)",
    "Total Revenue"
  ];

  const rows = contacts.map(contact => {
    const p = contact.properties;
    const stageDate = getStageDateForContact(contact);

    return [
      contact.id,
      p.email || "",
      p.phone || "",
      p.hs_google_click_id || "",
      p.lifecyclestage || "",
      p.createdate || "",
      stageDate ? stageDate.toISOString() : "",
      stageDate ? formatDateForGoogleAds(stageDate) : "",
      p.total_revenue || ""
    ];
  });

  await clearAndWriteSheet(sheetsClient, CONFIG.SHEETS.RAW, [headers, ...rows]);
  console.log(`Wrote ${rows.length} rows to ${CONFIG.SHEETS.RAW}`);
}


// ============================================================
// EXECUTION LOG
// ============================================================
async function writeExecutionLog(sheetsClient, startTime, status, rawRows, uploadRows, errors) {
  const timestamp = formatDateForGoogleAds(startTime);
  const errorSummary = errors.length > 0 ? errors.join(" | ") : "";

  const existingData = await sheetsClient.spreadsheets.values.get({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${CONFIG.SHEETS.LOG}!A1:E1`
  }).catch(() => null);

  if (!existingData?.data.values?.length) {
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CONFIG.SPREADSHEET_ID,
      range: `${CONFIG.SHEETS.LOG}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [["Timestamp (UTC)", "Status", "Raw Rows", "Upload Rows", "Errors"]]
      }
    });
  }

  await sheetsClient.spreadsheets.values.append({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${CONFIG.SHEETS.LOG}!A:E`,
    valueInputOption: "RAW",
    requestBody: {
      values: [[timestamp, status, rawRows, uploadRows, errorSummary]]
    }
  });

  console.log("Execution log updated.");
}


// ============================================================
// GOOGLE SHEETS CLIENT
// ============================================================
async function getSheetsClient() {
  const serviceAccountJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!serviceAccountJson) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON not set.");

  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(serviceAccountJson),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  return google.sheets({ version: "v4", auth });
}


async function clearAndWriteSheet(sheetsClient, sheetName, rows) {
  await sheetsClient.spreadsheets.values.clear({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${sheetName}!A:Z`
  });

  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: rows }
  });
}


// ============================================================
// ERROR ALERT
// ============================================================
async function sendErrorAlert(errors, startTime) {
  const alertEmail = process.env.ALERT_EMAIL;
  if (!alertEmail) {
    console.log("ALERT_EMAIL not set — skipping email alert.");
    return;
  }

  const gmailUser = process.env.GMAIL_USER;
  const gmailPass = process.env.GMAIL_APP_PASSWORD;

  if (!gmailUser || !gmailPass) {
    console.log("GMAIL_USER / GMAIL_APP_PASSWORD not set — error logged to Actions only.");
    return;
  }

  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: { user: gmailUser, pass: gmailPass }
  });

  const subject = `[ACTION REQUIRED] Offline Conversion Script Failed — ${startTime.toISOString().split("T")[0]}`;
  const body = [
    "The offline conversion tracking script failed:\n",
    ...errors.map(e => `• ${e}`),
    `\nSpreadsheet: https://docs.google.com/spreadsheets/d/${CONFIG.SPREADSHEET_ID}`,
    `Time: ${startTime.toISOString()}`,
    "Workflow: https://github.com/hnewellwise/hs-googleads-otc/actions"
  ].join("\n");

  await transporter.sendMail({ from: gmailUser, to: alertEmail, subject, text: body });
  console.log(`Error alert sent to ${alertEmail}`);
}


// ============================================================
// HELPERS
// ============================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getStageDateForContact(contact) {
  const p = contact.properties;
  const stage = (p.lifecyclestage || "").toLowerCase();
  const raw = stage === "customer"
    ? p.hs_lifecyclestage_customer_date
    : p.hs_lifecyclestage_opportunity_date;
  return raw ? new Date(raw) : null;
}

async function hubspotPost(url, token, payload, retries = 4) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    if (response.status === 429) {
      const retryAfterMs = parseInt(response.headers.get("Retry-After") || "0", 10) * 1000;
      const backoffMs = retryAfterMs || Math.min(1000 * 2 ** (attempt - 1), 16000);
      console.warn(`Rate limited by HubSpot. Attempt ${attempt}/${retries}. Waiting ${backoffMs}ms...`);
      if (attempt === retries) throw new Error("HubSpot rate limit exceeded after max retries.");
      await sleep(backoffMs);
      continue;
    }

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`HubSpot API error ${response.status}: ${text}`);
    }

    return response.json();
  }
}

function hashEmail(email) {
  return crypto
    .createHash("sha256")
    .update(email.toLowerCase().trim())
    .digest("hex");
}

function hashPhone(phone) {
  // Normalise to E.164 format before hashing: strip all non-digit characters
  // except a leading +, then hash. Google requires E.164 (e.g. +254712345678).
  const normalised = phone.trim().replace(/(?!^\+)[^\d]/g, "");
  return crypto
    .createHash("sha256")
    .update(normalised)
    .digest("hex");
}

function formatDateForGoogleAds(date) {
  // Format: yyyy-MM-dd HH:mm:ss+00:00
  return date.toISOString()
    .replace("T", " ")
    .replace(/\.\d{3}Z$/, "+00:00");
}


// ============================================================
// RUN
// ============================================================
main().catch(e => {
  console.error("Unhandled error:", e.message);
  process.exit(1);
});
