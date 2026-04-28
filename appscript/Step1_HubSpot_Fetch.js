// ============================================================
// STEP 1: HubSpot Fetch → RAW_HUBSPOT tab
// ============================================================
// Pulls opportunity and customer contacts from HubSpot
// modified in the last 90 days, writes to RAW_HUBSPOT tab.
//
// Required Script Properties (Apps Script > Project Settings):
//   HUBSPOT_API_KEY — HubSpot private app token
//
// To run a one-off full historical pull:
//   Set BACKFILL_MODE to true, run dailyRun(), then set back to false.
// ============================================================

var CONFIG = {
  RAW_SHEET_NAME: "RAW_HUBSPOT",
  LOOKBACK_DAYS: 90,
  BACKFILL_MODE: false,

  HUBSPOT_API_BASE: "https://api.hubapi.com",

  CONTACT_PROPERTIES: [
    "email",
    "hs_google_click_id",
    "lifecyclestage",
    "createdate",
    "total_revenue"
  ]
};


// ============================================================
// MAIN FUNCTION
// ============================================================
function populateRawHubspot() {
  var token = getApiToken();

  var cutoffMs = CONFIG.BACKFILL_MODE
    ? 0
    : Date.now() - (CONFIG.LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

  if (CONFIG.BACKFILL_MODE) {
    Logger.log("BACKFILL_MODE enabled — fetching all opportunity and customer contacts.");
  } else {
    Logger.log("Cutoff date: " + new Date(cutoffMs).toISOString());
  }

  var opportunities = searchContacts(token, "opportunity", cutoffMs);
  Logger.log("Opportunities found: " + opportunities.length);

  var confirmed = searchContacts(token, "customer", cutoffMs);
  Logger.log("Confirmed found: " + confirmed.length);

  var allContacts = opportunities.concat(confirmed);
  Logger.log("Total contacts to write: " + allContacts.length);

  writeRawData(allContacts);
  Logger.log("RAW_HUBSPOT tab updated successfully.");
}


// ============================================================
// SEARCH CONTACTS
// Filters server-side by lifecyclestage + lastmodifieddate.
// ============================================================
function searchContacts(token, stage, cutoffMs) {
  var allContacts = [];
  var after = 0;
  var hasMore = true;

  var filters = [{
    propertyName: "lifecyclestage",
    operator: "EQ",
    value: stage
  }];

  if (cutoffMs > 0) {
    filters.push({
      propertyName: "lastmodifieddate",
      operator: "GTE",
      value: cutoffMs
    });
  }

  while (hasMore) {
    var payload = {
      filterGroups: [{ filters: filters }],
      properties: CONFIG.CONTACT_PROPERTIES,
      limit: 100,
      after: after
    };

    var response = hubspotPost(
      CONFIG.HUBSPOT_API_BASE + "/crm/v3/objects/contacts/search",
      token,
      payload
    );

    if (!response || !response.results) {
      Logger.log("Unexpected response for stage '" + stage + "': " + JSON.stringify(response));
      break;
    }

    allContacts = allContacts.concat(response.results);

    if (response.paging && response.paging.next && response.paging.next.after) {
      after = response.paging.next.after;
    } else {
      hasMore = false;
    }
  }

  return allContacts;
}


// ============================================================
// WRITE TO RAW_HUBSPOT TAB
// ============================================================
function writeRawData(contacts) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.RAW_SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(CONFIG.RAW_SHEET_NAME);
    Logger.log("Created new sheet: " + CONFIG.RAW_SHEET_NAME);
  }

  sheet.clearContents();

  var headers = [
    "Contact ID",                      // 0
    "Email",                           // 1
    "GCLID",                           // 2
    "Lifecycle Stage",                 // 3
    "Contact Create Date (Raw)",       // 4
    "Contact Create Date (Formatted)", // 5
    "Total Revenue"                    // 6
  ];

  var rows = [headers];

  for (var i = 0; i < contacts.length; i++) {
    var p = contacts[i].properties;
    var createDate = p.createdate ? new Date(p.createdate) : null;
    var createFormatted = createDate
      ? Utilities.formatDate(createDate, "UTC", "yyyy-MM-dd HH:mm:ss") + "+00:00"
      : "";

    rows.push([
      contacts[i].id,
      p.email || "",
      p.hs_google_click_id || "",
      p.lifecyclestage || "",
      p.createdate || "",
      createFormatted,
      p.total_revenue || ""
    ]);
  }

  sheet.getRange(1, 1, rows.length, headers.length).setValues(rows);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");

  Logger.log("Wrote " + (rows.length - 1) + " rows to " + CONFIG.RAW_SHEET_NAME);
}


// ============================================================
// HTTP POST HELPER
// ============================================================
function hubspotPost(url, token, payload) {
  var options = {
    method: "post",
    headers: {
      "Authorization": "Bearer " + token,
      "Content-Type": "application/json"
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  var response = UrlFetchApp.fetch(url, options);
  var code = response.getResponseCode();

  if (code !== 200) {
    Logger.log("HubSpot API error " + code + ": " + response.getContentText());
    throw new Error("HubSpot API returned status " + code);
  }

  return JSON.parse(response.getContentText());
}


// ============================================================
// GET API TOKEN FROM SCRIPT PROPERTIES
// ============================================================
function getApiToken() {
  var token = PropertiesService.getScriptProperties().getProperty("HUBSPOT_API_KEY");
  if (!token) {
    throw new Error("HUBSPOT_API_KEY not found in Script Properties. Add it under Project Settings > Script Properties.");
  }
  return token;
}
