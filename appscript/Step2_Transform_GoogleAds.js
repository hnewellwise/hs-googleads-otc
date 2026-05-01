// ============================================================
// STEP 2: Transform RAW_HUBSPOT → GOOGLE_ADS_UPLOAD tab
// ============================================================
// Reads from RAW_HUBSPOT, filters to contacts who entered their
// stage within the last UPLOAD_LOOKBACK_DAYS, hashes email via
// SHA-256, and writes to GOOGLE_ADS_UPLOAD tab.
//
// Conversion actions:
//   opportunity → Hubspot Contacts - Opportunity (no value)
//   customer    → Hubspot Contacts - Confirmed (revenue value in USD)
// ============================================================

var TRANSFORM_CONFIG = {
  RAW_SHEET_NAME: "RAW_HUBSPOT",
  OUTPUT_SHEET_NAME: "GOOGLE_ADS_UPLOAD",

  // Only contacts who entered their stage within this window are uploaded.
  // Set to 0 (or match BACKFILL_MODE) to bypass for a full historical upload.
  UPLOAD_LOOKBACK_DAYS: 7,

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
  CONTACT_ID:   0,
  EMAIL:        1,
  PHONE:        2,
  GCLID:        3,
  STAGE:        4,
  CREATE_DATE:  5,
  STAGE_RAW:    6,
  STAGE_FMT:    7,
  REVENUE:      8
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

  var uploadCutoffMs = TRANSFORM_CONFIG.BACKFILL_MODE
    ? 0
    : Date.now() - (TRANSFORM_CONFIG.UPLOAD_LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

  if (!TRANSFORM_CONFIG.BACKFILL_MODE) {
    Logger.log("Upload cutoff (stage date): " + new Date(uploadCutoffMs).toISOString());
  }

  var rows = data.slice(1);
  Logger.log("Rows to process: " + rows.length);

  var outputRows = [];
  var skipped = 0;

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var stage = (row[RAW_COLS.STAGE] || "").toLowerCase().trim();
    var conversionName = TRANSFORM_CONFIG.CONVERSION_MAP[stage];

    if (!conversionName) { skipped++; continue; }

    var email = (row[RAW_COLS.EMAIL] || "").toLowerCase().trim();
    var gclid = (row[RAW_COLS.GCLID] || "").trim();

    if (!email && !gclid) { skipped++; continue; }

    // Filter by stage date — skip if no date or outside upload window
    var stageDateRaw = row[RAW_COLS.STAGE_RAW] || "";
    if (!stageDateRaw) { skipped++; continue; }

    var stageDateMs = new Date(stageDateRaw).getTime();
    if (isNaN(stageDateMs)) { skipped++; continue; }
    if (stageDateMs < uploadCutoffMs) { skipped++; continue; }

    var conversionTime = row[RAW_COLS.STAGE_FMT] || "";
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
  }

  Logger.log("Upload rows: " + outputRows.length + " | Skipped: " + skipped);
  writeOutputSheet(ss, outputRows);
  Logger.log("GOOGLE_ADS_UPLOAD tab updated successfully.");
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
