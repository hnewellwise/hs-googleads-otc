# HubSpot → Google Ads Offline Conversion Tracking

Google Apps Script that pulls contact data from HubSpot and formats it for Google Ads offline conversion import via Data Manager (Google Sheets connection).

## What it does

- Pulls HubSpot contacts at `Opportunity` and `Customer` (Confirmed) lifecycle stages modified in the last 90 days
- Filters server-side via the HubSpot search API — only relevant contacts are downloaded
- Hashes email addresses using SHA-256 in line with Google's enhanced conversions requirements
- Formats data into the correct Google Ads Data Manager template
- Passes `total_revenue` as the conversion value for Confirmed contacts
- Logs each execution to a sheet tab and sends an email alert on failure

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

## Setup

### 1. HubSpot private app

1. In HubSpot: Settings > Integrations > Private Apps > Create private app
2. Grant scope: `crm.objects.contacts.read`
3. Copy the token

### 2. Apps Script

1. Open your Google Sheet > Extensions > Apps Script
2. Create three script files and paste the contents of each:
   - `Step1_HubSpot_Fetch.js`
   - `Step2_Transform_GoogleAds.js`
   - `Step3_Chain.js`

### 3. Script Properties

In Apps Script > Project Settings > Script Properties, add the following:

| Key | Value |
|-----|-------|
| `HUBSPOT_API_KEY` | Your HubSpot private app token |
| `ALERT_EMAIL` | Email address to notify on script failure |

No credentials or personal data should ever be hardcoded in the script files.

### 4. Initial backfill (optional)

In `Step1_HubSpot_Fetch.js`, set `BACKFILL_MODE: true` and run `dailyRun()` once to populate historical data. Then set it back to `false`.

### 5. Daily trigger

Apps Script editor > Triggers (clock icon, left sidebar) > Add Trigger:
- Function: `dailyRun`
- Event source: Time-driven
- Type: Day timer
- Time: 1am–2am (or your preferred time)

### 6. Google Ads Data Manager

1. In Google Ads: Tools > Data Manager > + New data source > Google Sheets
2. Connect to the `GOOGLE_ADS_UPLOAD` tab
3. Map columns to the correct conversion fields
4. Set refresh schedule to daily

## Configuration

All configurable values are at the top of each file in the `CONFIG` / `TRANSFORM_CONFIG` blocks.

| Setting | File | Default | Description |
|---------|------|---------|-------------|
| `LOOKBACK_DAYS` | Step 1 | `90` | Lookback window for lastmodifieddate filter |
| `BACKFILL_MODE` | Step 1 | `false` | Set to `true` for a one-off full historical pull |
| `CURRENCY` | Step 2 | `USD` | Conversion currency for Confirmed rows |

## Notes

- Contacts without both email and GCLID are skipped
- Confirmed rows with no `total_revenue` value send no conversion value
- Google deduplicates on `GCLID + Conversion Name + Conversion Time` — re-uploads won't double-count where a GCLID is present
- Enhanced conversions (hashed email) covers contacts where GCLID is missing, provided the email matches Google's records
- `hs_lifecyclestage_opportunity_date` and `hs_lifecyclestage_customer_date` are not filterable via the HubSpot search API — `lastmodifieddate` is used as a reliable proxy
