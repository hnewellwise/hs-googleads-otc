// ============================================================
// HubSpot → Google Ads Offline Conversion Tracking
// Node.js version — runs via GitHub Actions
// ============================================================
// Required environment variables (set as GitHub Secrets):
//   HUBSPOT_API_KEY           — HubSpot private app token
//   GOOGLE_SERVICE_ACCOUNT_JSON — Full JSON key for service account
//   SPREADSHEET_ID            — Google Sheet ID
//   ALERT_EMAIL               — Email to notify on failure
// ============================================================

import fetch from "node-fetch";
import { google } from "googleapis";
import crypto from "crypto";
import nodemailer from "nodemailer";

// ============================================================
// CONFIG
// ============================================================
const CONFIG = {
  LOOKBACK_DAYS: 90,
  BACKFILL_MODE: false,

  HUBSPOT_API_BASE: "https://api.hubapi.com",

  CONTACT_PROPERTIES: [
    "email",
    "hs_google_click_id",
    "lifecyclestage",
    "createdate",
    "total_revenue"
  ],

  INCLUDED_STAGES: ["opportunity", "customer"],

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

  // Step 1: Fetch from HubSpot
  try {
    const contacts = await fetchContacts();
    rawRowCount = contacts.length;
    console.log(`Step 1 complete. Contacts fetched: ${rawRowCount}`);
    await writeRawData(sheetsClient, contacts);
  } catch (e) {
    errors.push(`Step 1 (HubSpot fetch) failed: ${e.message}`);
    console.error(errors[errors.length - 1]);
  }

  // Step 2: Transform + write Google Ads upload sheet
  if (errors.length === 0) {
    try {
      uploadRowCount = await formatForGoogleAds(sheetsClient);
      console.log(`Step 2 complete. Upload rows: ${uploadRowCount}`);
    } catch (e) {
      errors.push(`Step 2 (Google Ads transform) failed: ${e.message}`);
      console.error(errors[errors.length - 1]);
    }
  } else {
    console.log("Skipping Step 2 — Step 1 failed.");
  }

  // Write execution log
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
    console.log(`Cutoff date: ${new Date(cutoffMs).toISOString()}`);
  }

  const opportunities = await searchContacts(token, "opportunity", cutoffMs);
  console.log(`Opportunities found: ${opportunities.length}`);

  const confirmed = await searchContacts(token, "customer", cutoffMs);
  console.log(`Confirmed found: ${confirmed.length}`);

  return [...opportunities, ...confirmed];
}


async function searchContacts(token, stage, cutoffMs) {
  const allContacts = [];
  let after = undefined;
  let hasMore = true;

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

  while (hasMore) {
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
    } else {
      hasMore = false;
    }
  }

  return allContacts;
}


// ============================================================
// STEP 2: TRANSFORM + WRITE GOOGLE ADS UPLOAD SHEET
// ============================================================
async function formatForGoogleAds(sheetsClient) {
  // Read raw data from sheet
  const rawData = await sheetsClient.spreadsheets.values.get({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${CONFIG.SHEETS.RAW}!A:G`
  });

  const rows = rawData.data.values || [];
  if (rows.length <= 1) {
    console.log("RAW_HUBSPOT is empty. Nothing to transform.");
    return 0;
  }

  // Skip header row
  const dataRows = rows.slice(1);
  const outputRows = [];
  let skipped = 0;

  // Column indices matching RAW_HUBSPOT headers
  const COL = {
    CONTACT_ID: 0,
    EMAIL: 1,
    GCLID: 2,
    STAGE: 3,
    CREATE_RAW: 4,
    CREATE_FMT: 5,
    REVENUE: 6
  };

  for (const row of dataRows) {
    const stage = (row[COL.STAGE] || "").toLowerCase().trim();
    const conversionName = CONFIG.CONVERSION_MAP[stage];

    if (!conversionName) { skipped++; continue; }

    const email = (row[COL.EMAIL] || "").toLowerCase().trim();
    const gclid = (row[COL.GCLID] || "").trim();
    const conversionTime = row[COL.CREATE_FMT] || "";

    if (!email && !gclid) { skipped++; continue; }

    const hashedEmail = email ? hashEmail(email) : "";

    // Currency is always written as a valid ISO 4217 code — Google rejects empty strings
    let conversionValue = "";
    let conversionCurrency = CONFIG.CURRENCY;
    if (stage === "customer") {
      const revenue = parseFloat(row[COL.REVENUE]);
      if (!isNaN(revenue) && revenue > 0) {
        conversionValue = revenue;
      }
    }

    outputRows.push([
      gclid,
      hashedEmail,
      conversionName,
      conversionTime,
      conversionCurrency,
      conversionValue
    ]);
  }

  console.log(`Output rows: ${outputRows.length} | Skipped: ${skipped}`);

  // Write to GOOGLE_ADS_UPLOAD sheet
  const headers = [
    "Google Click ID",
    "Email",
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
// ============================================================
async function writeRawData(sheetsClient, contacts) {
  const headers = [
    "Contact ID",
    "Email",
    "GCLID",
    "Lifecycle Stage",
    "Contact Create Date (Raw)",
    "Contact Create Date (Formatted)",
    "Total Revenue"
  ];

  const rows = contacts.map(contact => {
    const p = contact.properties;
    const createDate = p.createdate ? new Date(p.createdate) : null;
    const createFormatted = createDate
      ? formatDateForGoogleAds(createDate)
      : "";

    return [
      contact.id,
      p.email || "",
      p.hs_google_click_id || "",
      p.lifecyclestage || "",
      p.createdate || "",
      createFormatted,
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

  // Check if log sheet exists and has headers
  const existingData = await sheetsClient.spreadsheets.values.get({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${CONFIG.SHEETS.LOG}!A1:E1`
  }).catch(() => null);

  if (!existingData || !existingData.data.values?.length) {
    // Write headers first
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CONFIG.SPREADSHEET_ID,
      range: `${CONFIG.SHEETS.LOG}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [["Timestamp (UTC)", "Status", "Raw Rows", "Upload Rows", "Errors"]]
      }
    });
  }

  // Append log row
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

  const credentials = JSON.parse(serviceAccountJson);

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  return google.sheets({ version: "v4", auth });
}


async function clearAndWriteSheet(sheetsClient, sheetName, rows) {
  // Clear existing content
  await sheetsClient.spreadsheets.values.clear({
    spreadsheetId: CONFIG.SPREADSHEET_ID,
    range: `${sheetName}!A:Z`
  });

  // Write new content
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

  // GitHub Actions will surface the error in the workflow log
  // Email via nodemailer using Gmail SMTP if credentials provided
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
    "Workflow: https://github.com"
  ].join("\n");

  await transporter.sendMail({
    from: gmailUser,
    to: alertEmail,
    subject,
    text: body
  });

  console.log(`Error alert sent to ${alertEmail}`);
}


// ============================================================
// HELPERS
// ============================================================
async function hubspotPost(url, token, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HubSpot API error ${response.status}: ${text}`);
  }

  return response.json();
}

function hashEmail(email) {
  return crypto
    .createHash("sha256")
    .update(email.toLowerCase().trim())
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
