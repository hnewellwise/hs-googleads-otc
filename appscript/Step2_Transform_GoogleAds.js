// ============================================================
// STEP 2: Transform RAW_HUBSPOT → GOOGLE_ADS_UPLOAD tab
// ============================================================
// Reads from RAW_HUBSPOT, hashes email via SHA-256,
// maps lifecycle stages to Google Ads conversion names,
// and writes formatted data to GOOGLE_ADS_UPLOAD tab.
//
// Conversion actions:
//   opportunity → Hubspot Contacts - Opportunity (no value)
//   customer    → Hubspot Contacts - Confirmed (revenue value in USD)
// ============================================================

var TRANSFORM_CONFIG = {
  RAW_SHEET_NAME: "RAW_HUBSPOT",
  OUTPUT_SHEET_NAME: "GOOGLE_ADS_UPLOAD",

  CONVERSION_MAP: {
    "opportunity": "Hubspot Contacts - Opportunity",
    "customer":    "Hubspot Contacts - Confirmed"
  },

  CURRENCY: "USD"
};

// Column indices in RAW_HUBSPOT (0-based, matching Step 1 output)
var RAW_COLS = {
  CONTACT_ID:  0,
  EMAIL:       1,
  GCLID:       2,
  STAGE:       3,
  CREATE_RAW:  4,
  CREATE_FMT:  5,
  REVENUE:     6
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

  var rows = data.slice(1);
  Logger.log("Rows to process: " + rows.length);

  var outputRows = [];
  var skipped = 0;

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var stage = (row[RAW_COLS.STAGE] || "").toLowerCase().trim();
    var conversionName = TRANSFORM_CONFIG.CONVERSION_MAP[stage];

    if (!conversionName) {
      skipped++;
      continue;
    }

    var email = (row[RAW_COLS.EMAIL] || "").toLowerCase().trim();
    var gclid = (row[RAW_COLS.GCLID] || "").trim();
    var conversionTime = row[RAW_COLS.CREATE_FMT] || "";

    // Skip if no email or GCLID — nothing to match on
    if (!email && !gclid) {
      skipped++;
      continue;
    }

    var hashedEmail = email ? hashEmail(email) : "";

    // Revenue only applies to confirmed (customer) rows
    var conversionValue = "";
    var conversionCurrency = "";
    if (stage === "customer") {
      var revenue = parseFloat(row[RAW_COLS.REVENUE]);
      if (!isNaN(revenue) && revenue > 0) {
        conversionValue = revenue;
        conversionCurrency = TRANSFORM_CONFIG.CURRENCY;
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

  Logger.log("Output rows: " + outputRows.length + " | Skipped: " + skipped);
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
// SHA-256 EMAIL HASHING
// Google requires: lowercase, trimmed, SHA-256 hashed, hex encoded
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
