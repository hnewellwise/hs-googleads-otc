# HubSpot → Google Ads Offline Conversion Tracking

Pulls contact data from HubSpot and formats it for Google Ads offline conversion import via Data Manager (Google Sheets connection).

Originally built as a Google Apps Script running on a time-based trigger. Migrated to GitHub Actions for more reliable scheduling, better error visibility, and easier version control. The Apps Script files remain in `/appscript` as a working backup.

## What it does

- Pulls HubSpot contacts at `Opportunity` and `Customer` (Confirmed) lifecycle stages modified in the last 90 days
- Filters server-side via the HubSpot search API — only relevant contacts are downloaded
- Hashes email addresses using SHA-256 in line with Google's enhanced conversions requirements
- Formats data into the correct Google Ads Data Manager template
- Passes `total_revenue` as the conversion value for Confirmed contacts
- Logs each execution to a Google Sheet tab and sends an email alert on failure

## Sheet structure

| Tab | Purpose |
|-----|---------|
| `RAW_HUBSPOT` | Raw contact data pulled from HubSpot API |
| `GOOGLE_ADS_UPLOAD` | Formatted data connected to Google Ads Data Manager |
| `EXECUTION_LOG` | Timestamped log of each script run |

## Conversion actions

| HubSpot Lifecycle Stage | Google Ads Conversion Name |
|-------------------------|---------------------------|
| `opportunity` | Hubspot Contacts - Opportunity |
| `customer` | Hubspot Contacts - Confirmed |

---

## Primary: GitHub Actions

The main version runs as a Node.js script via GitHub Actions (`src/index.mjs`), triggered daily at 1:00 AM UTC. It can also be triggered manually from the Actions tab.

### Setup

#### 1. HubSpot private app

1. In HubSpot: Settings > Integrations > Private Apps > Create private app
2. Grant scope: `crm.objects.contacts.read`
3. Copy the token

#### 2. Google service account

1. Create a service account in Google Cloud Console with access to the Google Sheet
2. Download the JSON key file
3. Share the Google Sheet with the service account email

#### 3. GitHub Secrets

In the repo: Settings → Secrets and variables → Actions → New repository secret

| Secret | Value |
|--------|-------|
| `HUBSPOT_API_KEY` | HubSpot private app token |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full contents of the service account JSON key file |
| `SPREADSHEET_ID` | Google Sheet ID (from the sheet URL) |
| `ALERT_EMAIL` | Email address to notify on failure |
| `GMAIL_USER` | Gmail address used to send alerts |
| `GMAIL_APP_PASSWORD` | Gmail app password (not your login password) |

No credentials or personal data should ever be hardcoded in the script files.

#### 4. Initial backfill (optional)

In `src/index.mjs`, set `BACKFILL_MODE: true` and trigger the workflow manually. Set it back to `false` afterwards.

#### 5. Google Ads Data Manager

1. In Google Ads: Tools → Data Manager → + New data source → Google Sheets
2. Connect to the `GOOGLE_ADS_UPLOAD` tab
3. Map columns to the correct conversion fields
4. Set refresh schedule to daily

---

## Backup: Google Apps Script

The original Apps Script implementation is in `/appscript`. It is fully functional and can be used as a fallback if needed.

### Files

| File | Purpose |
|------|---------|
| `Step1_HubSpot_Fetch.js` | Pulls contacts from HubSpot, writes to `RAW_HUBSPOT` tab |
| `Step2_Transform_GoogleAds.js` | Transforms raw data, writes to `GOOGLE_ADS_UPLOAD` tab |
| `Step3_Chain.js` | Chains steps 1 and 2, handles error alerting and execution logging |

### Setup

1. Open your Google Sheet → Extensions → Apps Script
2. Create three script files and paste the contents of each from `/appscript`
3. In Apps Script → Project Settings → Script Properties, add:

| Key | Value |
|-----|-------|
| `HUBSPOT_API_KEY` | Your HubSpot private app token |
| `ALERT_EMAIL` | Email address to notify on script failure |

4. Set a time-based trigger pointing at `dailyRun` (Apps Script editor → Triggers → Add Trigger)

---

## Configuration

All configurable values are at the top of `src/index.mjs` in the `CONFIG` block.

| Setting | Default | Description |
|---------|---------|-------------|
| `LOOKBACK_DAYS` | `90` | Lookback window for lastmodifieddate filter |
| `BACKFILL_MODE` | `false` | Set to `true` for a one-off full historical pull |
| `CURRENCY` | `USD` | Conversion currency for Confirmed rows |

## Notes

- Contacts without both email and GCLID are skipped
- Confirmed rows with no `total_revenue` value send no conversion value
- Google deduplicates on `GCLID + Conversion Name + Conversion Time` — re-uploads won't double-count where a GCLID is present
- Enhanced conversions (hashed email) covers contacts where GCLID is missing, provided the email matches Google's records
- `hs_lifecyclestage_opportunity_date` and `hs_lifecyclestage_customer_date` are not filterable via the HubSpot search API — `lastmodifieddate` is used as a reliable proxy
