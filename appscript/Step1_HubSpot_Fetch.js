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

  // How far back to fetch contacts from HubSpot (by lastmodifieddate).
  // Cast a wide net — stage dates are not server-side filterable.
  LOOKBACK_DAYS: 90,

  // Set to true for a one-off full historical pull.
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
    Logger.log("Fetch cutoff (lastmodifieddate): " + new Date(cutoffMs).toISOString());
  }

  var opportunities = searchContacts(token, "opportunity", cutoffMs);
  Logger.log("Opportunities: " + opportunities.length);

  var confirmed = searchContacts(token, "customer", cutoffMs);
  Logger.log("Confirmed: " + confirmed.length);

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

  while (true) {
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
      break;
    }
  }

  return allContacts;
}


// ============================================================
// WRITE TO RAW_HUBSPOT TAB
// Full set of fetched contacts — used for auditing.
// Stage date is stored in place of createdate.
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
    "Contact ID",             // 0
    "Email",                  // 1
    "Phone",                  // 2
    "GCLID",                  // 3
    "Lifecycle Stage",        // 4
    "Contact Create Date",    // 5
    "Stage Date (Raw)",       // 6
    "Stage Date (Formatted)", // 7
    "Total Revenue"           // 8
  ];

  var rows = [headers];

  for (var i = 0; i < contacts.length; i++) {
    var p = contacts[i].properties;
    var stageDate = getStageDateForContact(contacts[i]);
    var stageDateFormatted = stageDate
      ? Utilities.formatDate(stageDate, "UTC", "yyyy-MM-dd HH:mm:ss") + "+00:00"
      : "";

    rows.push([
      contacts[i].id,
      p.email || "",
      p.phone || "",
      p.hs_google_click_id || "",
      p.lifecyclestage || "",
      p.createdate || "",
      stageDate ? stageDate.toISOString() : "",
      stageDateFormatted,
      p.total_revenue || ""
    ]);
  }

  sheet.getRange(1, 1, rows.length, headers.length).setValues(rows);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");

  Logger.log("Wrote " + (rows.length - 1) + " rows to " + CONFIG.RAW_SHEET_NAME);
}


// ============================================================
// HELPERS
// ============================================================
function getStageDateForContact(contact) {
  var p = contact.properties;
  var stage = (p.lifecyclestage || "").toLowerCase();
  var raw = stage === "customer"
    ? p.hs_lifecyclestage_customer_date
    : p.hs_lifecyclestage_opportunity_date;
  return raw ? new Date(raw) : null;
}

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

function getApiToken() {
  var token = PropertiesService.getScriptProperties().getProperty("HUBSPOT_API_KEY");
  if (!token) {
    throw new Error("HUBSPOT_API_KEY not found in Script Properties. Add it under Project Settings > Script Properties.");
  }
  return token;
}
