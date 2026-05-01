// ============================================================
// STEP 2: Transform RAW_HUBSPOT → GOOGLE_ADS_UPLOAD tab
// ============================================================
// Reads from RAW_HUBSPOT, compares against UPLOADED_CONTACTS
// tracking sheet, and only writes new contact+stage combinations
// to GOOGLE_ADS_UPLOAD. Appends new entries to tracking sheet.
//
// Conversion actions:
//   opportunity → Hubspot Contacts - Opportunity (no value)
//   customer    → Hubspot Contacts - Confirmed (revenue value in USD)
// ============================================================

var TRANSFORM_CONFIG = {
  RAW_SHEET_NAME: "RAW_HUBSPOT",
  OUTPUT_SHEET_NAME: "GOOGLE_ADS_UPLOAD",
  TRACKING_SHEET_NAME: "UPLOADED_CONTACTS",

  // Set to true for a one-off full historical upload. Bypasses tracking filter.
  BACKFILL_MODE: false,

  CONVERSION_MAP: {
    "opportunity": "Hubspot Contacts - Opportunity",
    "customer":    "Hubspot Contacts - Confirmed"
  },

  // Must be a valid ISO 4217 three-letter currency code
  CURRENCY: "USD"
};

// Column indices in RAW_HUBSPOT (0-based, matching Step 1 output)
var RAW_COLS = {
  CONTACT_ID:    0,
  EMAIL:         1,
  PHONE:         2,
  GCLID:         3,
  STAGE:         4,
  CREATE_DATE:   5,
  LAST_MODIFIED: 6,
  REVENUE:       7
};


// ============================================================
// MAIN FUNCTION
// ============================================================
function formatForGoogleAds() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var rawSheet = ss.getSheetByName(TRANSFORM_CONFIG.RAW_SHEET_NAME);

  if (!rawSheet) {
    throw new Error("RAW_HUBSPOT sheet not found. Run populateRawHubspot() first.");
  }

  var data = rawSheet.getDataRange().getValues();

  if (data.length <= 1) {
    Logger.log("RAW_HUBSPOT is empty. Nothing to transform.");
    return;
  }

  var alreadyUploaded = loadTrackingSheet(ss);
  Logger.log("Tracking sheet loaded. Previously uploaded: " + alreadyUploaded.size);

  var runTimestamp = Utilities.formatDate(new Date(), "UTC", "yyyy-MM-dd HH:mm:ss") + "+00:00";
  var rows = data.slice(1);
  Logger.log("Rows to process: " + rows.length);

  var outputRows = [];
  var newTrackingRows = [];
  var skipped = 0;

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var stage = (row[RAW_COLS.STAGE] || "").toLowerCase().trim();
    var conversionName = TRANSFORM_CONFIG.CONVERSION_MAP[stage];

    if (!conversionName) { skipped++; continue; }

    var email = (row[RAW_COLS.EMAIL] || "").toLowerCase().trim();
    var gclid = (row[RAW_COLS.GCLID] || "").trim();

    if (!email && !gclid) { skipped++; continue; }

    // Skip if this contact+stage has already been uploaded
    var contactId = String(row[RAW_COLS.CONTACT_ID] || "");
    var trackingKey = contactId + "|" + stage;

    if (!TRANSFORM_CONFIG.BACKFILL_MODE && alreadyUploaded.has(trackingKey)) {
      skipped++;
      continue;
    }

    // Use lastmodifieddate as conversion time — best available proxy
    // since hs_lifecyclestage_*_date is not reliably populated in HubSpot
    var lastModifiedRaw = row[RAW_COLS.LAST_MODIFIED] || "";
    var conversionTime = "";
    if (lastModifiedRaw) {
      var lastModifiedDate = new Date(lastModifiedRaw);
      conversionTime = Utilities.formatDate(lastModifiedDate, "UTC", "yyyy-MM-dd HH:mm:ss") + "+00:00";
    } else {
      conversionTime = runTimestamp;
    }

    var hashedEmail = email ? hashEmail(email) : "";
    var hashedPhone = row[RAW_COLS.PHONE] ? hashPhone(String(row[RAW_COLS.PHONE])) : "";

    // Currency is always written as a valid ISO 4217 code — Google rejects empty strings
    var conversionValue = "";
    if (stage === "customer") {
      var revenue = parseFloat(row[RAW_COLS.REVENUE]);
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
      TRANSFORM_CONFIG.CURRENCY,
      conversionValue
    ]);

    newTrackingRows.push([contactId, stage, conversionTime, runTimestamp]);
  }

  Logger.log("Upload rows: " + outputRows.length + " | Skipped: " + skipped);
  writeOutputSheet(ss, outputRows);

  if (newTrackingRows.length > 0) {
    appendToTrackingSheet(ss, newTrackingRows);
    Logger.log("Tracking sheet updated with " + newTrackingRows.length + " new entries.");
  }

  Logger.log("GOOGLE_ADS_UPLOAD tab updated successfully.");
}


// ============================================================
// TRACKING SHEET
// Append-only log of every contact+stage combination uploaded.
// Key: contactId|stage — prevents re-uploads on subsequent runs.
// ============================================================
function loadTrackingSheet(ss) {
  var uploaded = new Set ? new Set() : { _data: {}, has: function(k) { return !!this._data[k]; }, add: function(k) { this._data[k] = true; }, get size() { return Object.keys(this._data).length; } };
  var sheet = ss.getSheetByName(TRANSFORM_CONFIG.TRACKING_SHEET_NAME);

  if (!sheet || sheet.getLastRow() <= 1) return uploaded;

  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues();
  for (var i = 0; i < rows.length; i++) {
    if (rows[i][0] && rows[i][1]) {
      uploaded.add(rows[i][0] + "|" + rows[i][1]);
    }
  }

  return uploaded;
}

function appendToTrackingSheet(ss, newRows) {
  var sheet = ss.getSheetByName(TRANSFORM_CONFIG.TRACKING_SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(TRANSFORM_CONFIG.TRACKING_SHEET_NAME);
    sheet.getRange(1, 1, 1, 4).setValues([["Contact ID", "Stage", "Conversion Time", "First Uploaded (UTC)"]]);
    sheet.getRange(1, 1, 1, 4).setFontWeight("bold");
  }

  sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
}


// ============================================================
// WRITE OUTPUT SHEET
// ============================================================
function writeOutputSheet(ss, outputRows) {
  var sheet = ss.getSheetByName(TRANSFORM_CONFIG.OUTPUT_SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(TRANSFORM_CONFIG.OUTPUT_SHEET_NAME);
    Logger.log("Created new sheet: " + TRANSFORM_CONFIG.OUTPUT_SHEET_NAME);
  }

  sheet.clearContents();

  var headers = [
    "Google Click ID",
    "Email",
    "Phone Number",
    "Conversion Name",
    "Conversion Time",
    "Conversion Currency",
    "Conversion Value"
  ];

  var allRows = [headers].concat(outputRows);
  sheet.getRange(1, 1, allRows.length, headers.length).setValues(allRows);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");

  Logger.log("Wrote " + outputRows.length + " data rows to " + TRANSFORM_CONFIG.OUTPUT_SHEET_NAME);
}


// ============================================================
// SHA-256 HASHING
// Google requires: normalised, SHA-256 hashed, hex encoded
// ============================================================
function hashEmail(email) {
  var normalised = email.toLowerCase().trim();
  var bytes = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    normalised,
    Utilities.Charset.UTF_8
  );
  return bytes.map(function(b) {
    var hex = (b & 0xff).toString(16);
    return hex.length === 1 ? "0" + hex : hex;
  }).join("");
}

function hashPhone(phone) {
  // Normalise to E.164 format before hashing: strip all non-digit characters
  // except a leading +, then hash. Google requires E.164 (e.g. +254712345678).
  var normalised = phone.trim().replace(/(?!^\+)[^\d]/g, "");
  var bytes = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    normalised,
    Utilities.Charset.UTF_8
  );
  return bytes.map(function(b) {
    var hex = (b & 0xff).toString(16);
    return hex.length === 1 ? "0" + hex : hex;
  }).join("");
}
