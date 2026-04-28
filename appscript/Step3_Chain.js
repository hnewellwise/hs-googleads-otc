// ============================================================
// STEP 3: Chain Function + Error Alerting + Execution Logging
// ============================================================
// Run dailyRun() manually from the Apps Script console,
// or point a time-based trigger at dailyRun() via the
// Apps Script triggers UI (Triggers > Add Trigger).
//
// Required Script Properties (Apps Script > Project Settings):
//   ALERT_EMAIL — email address to notify on failure
// ============================================================

var LOG_SHEET_NAME = "EXECUTION_LOG";


// ============================================================
// MAIN CHAIN FUNCTION
// ============================================================
function dailyRun() {
  var errors = [];
  var startTime = new Date();
  var rawRowCount = 0;
  var uploadRowCount = 0;

  Logger.log("dailyRun started at: " + startTime.toISOString());

  // Step 1: Fetch from HubSpot
  try {
    populateRawHubspot();
    rawRowCount = getRawRowCount();
    Logger.log("Step 1 complete. Raw rows: " + rawRowCount);
  } catch (e) {
    errors.push("Step 1 (HubSpot fetch) failed: " + e.message);
    Logger.log(errors[errors.length - 1]);
  }

  // Step 2: Only runs if Step 1 succeeded
  if (errors.length === 0) {
    try {
      formatForGoogleAds();
      uploadRowCount = getUploadRowCount();
      Logger.log("Step 2 complete. Upload rows: " + uploadRowCount);
    } catch (e) {
      errors.push("Step 2 (Google Ads transform) failed: " + e.message);
      Logger.log(errors[errors.length - 1]);
    }
  } else {
    Logger.log("Skipping Step 2 — Step 1 failed.");
  }

  // Write to execution log regardless of outcome
  var status = errors.length > 0 ? "FAILED" : "OK";
  writeExecutionLog(startTime, status, rawRowCount, uploadRowCount, errors);

  if (errors.length > 0) {
    sendErrorAlert(errors, startTime);
  } else {
    Logger.log("dailyRun completed successfully.");
  }
}


// ============================================================
// EXECUTION LOG
// Appends one row per run to EXECUTION_LOG tab
// Columns: Timestamp | Status | Raw Rows | Upload Rows | Errors
// ============================================================
function writeExecutionLog(startTime, status, rawRows, uploadRows, errors) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(LOG_SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(LOG_SHEET_NAME);
    var headers = ["Timestamp (UTC)", "Status", "Raw Rows", "Upload Rows", "Errors"];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
    Logger.log("Created EXECUTION_LOG sheet.");
  }

  var safeDate = new Date(startTime.getTime());
  var timestamp = Utilities.formatDate(safeDate, "UTC", "yyyy-MM-dd HH:mm:ss") + "+00:00";
  var errorSummary = errors.length > 0 ? errors.join(" | ") : "";

  sheet.appendRow([timestamp, status, rawRows, uploadRows, errorSummary]);
  Logger.log("Execution log updated.");
}


// ============================================================
// ROW COUNT HELPERS
// ============================================================
function getRawRowCount() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName("RAW_HUBSPOT");
  if (!sheet) return 0;
  var count = sheet.getLastRow() - 1;
  return count > 0 ? count : 0;
}

function getUploadRowCount() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName("GOOGLE_ADS_UPLOAD");
  if (!sheet) return 0;
  var count = sheet.getLastRow() - 1;
  return count > 0 ? count : 0;
}


// ============================================================
// ERROR ALERT EMAIL
// ============================================================
function sendErrorAlert(errors, startTime) {
  var alertEmail = PropertiesService.getScriptProperties().getProperty("ALERT_EMAIL");
  if (!alertEmail) {
    Logger.log("ALERT_EMAIL not set in Script Properties — skipping email alert.");
    return;
  }

  var safeDate = new Date(startTime.getTime());
  var subject = "[ACTION REQUIRED] Offline Conversion Script Failed — "
                + Utilities.formatDate(safeDate, "UTC", "yyyy-MM-dd");

  var body = "The offline conversion tracking script failed:\n\n";
  for (var i = 0; i < errors.length; i++) {
    body += "• " + errors[i] + "\n";
  }
  body += "\nSpreadsheet: " + SpreadsheetApp.getActiveSpreadsheet().getUrl() + "\n";
  body += "Time: " + startTime.toISOString() + "\n";
  body += "Execution logs: https://script.google.com\n";

  MailApp.sendEmail({ to: alertEmail, subject: subject, body: body });
  Logger.log("Error alert sent to " + alertEmail);
}
