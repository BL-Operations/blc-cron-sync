/**
 * Single Buyer Coverage Zips Sync Script
 *
 * This script performs a full sync of coverage data:
 * 1. Extracts data from Tableau (full view download)
 * 2. Groups data by Category and upserts raw data into Postgres
 * 3. Cleans up stale data from previous runs
 * 4. Syncs Campaign Groups to Salesforce
 * 5. Refreshes materialized views and caches
 *
 * IMPORTANT: This is a single self-contained .mjs script for GitHub Actions.
 * It cannot import from other local files.
 */

import postgres from 'postgres';

// =============================================================================
// Config / Env Vars
// =============================================================================

const REQUIRED_ENV_VARS = [
  'DATABASE_URL',
  'TABLEAU_SERVER_URL',
  'TABLEAU_PAT_NAME',
  'TABLEAU_PAT_SECRET',
  'TABLEAU_SITE_ID',
  'TABLEAU_VIEW_COVERAGE_ZIPS_ID',
  'SALESFORCE_CLIENT_ID',
  'SALESFORCE_CLIENT_SECRET',
  'SALESFORCE_USERNAME',
  'SALESFORCE_PASSWORD',
  'SALESFORCE_SECURITY_TOKEN',
  'SALESFORCE_LOGIN_URL',
  'BLC_APP_URL',
];

function checkEnvVars() {
  const missing = REQUIRED_ENV_VARS.filter((v) => !process.env[v]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
  log('All required environment variables are present.');
}

// =============================================================================
// DB Connection
// =============================================================================

const sql = postgres(process.env.DATABASE_URL, {
  ssl: { rejectUnauthorized: false },
});

// =============================================================================
// Logging
// =============================================================================

function log(msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
}

// =============================================================================
// CSV Parsing
// =============================================================================

function parseCSV(text) {
  const lines = text.split('\n');
  if (lines.length === 0) return [];

  const headers = parseCSVLine(lines[0]);
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] ?? '';
    }
    rows.push(row);
  }

  return rows;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current.trim());
  return result;
}

// =============================================================================
// Sync State
// =============================================================================

let syncStateId = null;
let syncRunId = null;

function setSyncStateId(id) {
  syncStateId = id;
}

function setSyncRunId(id) {
  syncRunId = id;
}

async function updateSyncState({ phase, state, status, errorMessage, completedAt, phaseStats } = {}) {
  if (!syncStateId) return;

  const updates = {
    last_updated_at: new Date(),
  };

  if (phase !== undefined) updates.current_phase = phase;
  if (state !== undefined) updates.current_state = state;
  if (status !== undefined) updates.status = status;
  if (errorMessage !== undefined) updates.error_message = errorMessage;
  if (completedAt !== undefined) updates.completed_at = completedAt;
  if (phaseStats !== undefined) updates.phase_stats = phaseStats;

  try {
    await sql`
      UPDATE single_coverage_zips_sync_state
      SET
        last_updated_at = ${updates.last_updated_at},
        current_phase = COALESCE(${updates.current_phase ?? null}, current_phase),
        current_state = COALESCE(${updates.current_state ?? null}, current_state),
        status = COALESCE(${updates.status ?? null}, status),
        error_message = COALESCE(${updates.error_message ?? null}, error_message),
        completed_at = COALESCE(${updates.completed_at ?? null}, completed_at)
      WHERE id = ${syncStateId}
    `;
  } catch (err) {
    console.error('Failed to update sync state:', err);
  }
}

// =============================================================================
// Tableau Auth
// =============================================================================

let tableauToken = null;
let tableauSiteId = null;

async function authenticateTableau() {
  const serverUrl = process.env.TABLEAU_SERVER_URL;
  const patName = process.env.TABLEAU_PAT_NAME;
  const patSecret = process.env.TABLEAU_PAT_SECRET;
  const siteContentUrl = process.env.TABLEAU_SITE_ID;

  log(`Authenticating with Tableau at ${serverUrl}...`);

  const response = await fetch(`${serverUrl}/api/3.17/auth/signin`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      credentials: {
        personalAccessTokenName: patName,
        personalAccessTokenSecret: patSecret,
        site: { contentUrl: siteContentUrl },
      },
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Tableau auth failed: ${response.status} ${text}`);
  }

  const data = await response.json();
  tableauToken = data.credentials.token;
  tableauSiteId = data.credentials.site.id;
  log(`Tableau authenticated. Site ID: ${tableauSiteId}`);
}

async function downloadViewData(filter) {
  const serverUrl = process.env.TABLEAU_SERVER_URL;
  const viewId = process.env.TABLEAU_VIEW_COVERAGE_ZIPS_ID;

  const url = `${serverUrl}/api/3.17/sites/${tableauSiteId}/views/${viewId}/data?maxAge=0${filter ? `&${filter}` : ''}`;

  log(`Downloading view data from Tableau: ${url}`);

  const response = await fetch(url, {
    headers: {
      'X-Tableau-Auth': tableauToken,
      Accept: 'text/csv',
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Tableau view download failed: ${response.status} ${text}`);
  }

  return await response.text();
}

// =============================================================================
// Salesforce Auth
// =============================================================================

let sfAccessToken = null;
let sfInstanceUrl = null;

async function authenticateSalesforce() {
  const loginUrl = process.env.SALESFORCE_LOGIN_URL;
  const clientId = process.env.SALESFORCE_CLIENT_ID;
  const clientSecret = process.env.SALESFORCE_CLIENT_SECRET;
  const username = process.env.SALESFORCE_USERNAME;
  const password = process.env.SALESFORCE_PASSWORD;
  const securityToken = process.env.SALESFORCE_SECURITY_TOKEN;

  log('Authenticating with Salesforce...');

  const params = new URLSearchParams({
    grant_type: 'password',
    client_id: clientId,
    client_secret: clientSecret,
    username,
    password: `${password}${securityToken}`,
  });

  const response = await fetch(`${loginUrl}/services/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Salesforce auth failed: ${response.status} ${text}`);
  }

  const data = await response.json();
  sfAccessToken = data.access_token;
  sfInstanceUrl = data.instance_url;
  log(`Salesforce authenticated. Instance: ${sfInstanceUrl}`);
}

async function salesforceRequest(method, path, body) {
  if (!sfAccessToken) {
    await authenticateSalesforce();
  }

  const url = `${sfInstanceUrl}${path}`;
  const options = {
    method,
    headers: {
      Authorization: `Bearer ${sfAccessToken}`,
      'Content-Type': 'application/json',
    },
  };

  if (body !== undefined) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);

  if (response.status === 401) {
    log('Salesforce token expired, re-authenticating...');
    sfAccessToken = null;
    await authenticateSalesforce();
    return salesforceRequest(method, path, body);
  }

  return response;
}

// =============================================================================
// Phase: Process Category
// =============================================================================

async function processCategory(category, rows) {
  log(`Processing category "${category}" with ${rows.length} rows...`);

  if (rows.length === 0) {
    log(`No rows for category "${category}", skipping.`);
    return;
  }

  const upsertRows = rows.map((row) => ({
    category,
    lead_buyer: row['Lead Buyer'] ?? '',
    lead_buy_campaign: row['Lead Buy Campaign'] ?? '',
    zip: row['Zip'] ?? '',
    state: row['State'] ?? '',
    city: row['City'] ?? '',
    campaign_status: row['Campaign Status'] ?? '',
    is_branded: (row['Branded'] ?? '').toLowerCase() === 'yes',
    sync_run_id: syncRunId,
  }));

  // Batch upsert
  const BATCH_SIZE = 500;
  let upserted = 0;

  for (let i = 0; i < upsertRows.length; i += BATCH_SIZE) {
    const batch = upsertRows.slice(i, i + BATCH_SIZE);

    await sql`
      INSERT INTO single_coverage_zips_raw
        (category, lead_buyer, lead_buy_campaign, zip, state, city, campaign_status, is_branded, sync_run_id, last_seen_at)
      SELECT
        r.category,
        r.lead_buyer,
        r.lead_buy_campaign,
        r.zip,
        r.state,
        r.city,
        r.campaign_status,
        r.is_branded,
        r.sync_run_id,
        NOW()
      FROM ${sql.json(batch)} AS r(category, lead_buyer, lead_buy_campaign, zip, state, city, campaign_status, is_branded, sync_run_id)
      ON CONFLICT (category, lead_buyer, lead_buy_campaign, zip)
      DO UPDATE SET
        state = EXCLUDED.state,
        city = EXCLUDED.city,
        campaign_status = EXCLUDED.campaign_status,
        is_branded = EXCLUDED.is_branded,
        sync_run_id = EXCLUDED.sync_run_id,
        last_seen_at = NOW()
    `;

    upserted += batch.length;
    log(`  Upserted ${upserted}/${upsertRows.length} rows for "${category}"`);
  }

  log(`Category "${category}" processing complete.`);
}

// =============================================================================
// Phase: Cleanup Stale Data
// =============================================================================

async function cleanupStaleData() {
  log('Cleaning up stale data from previous runs...');

  const result = await sql`
    DELETE FROM single_coverage_zips_raw
    WHERE sync_run_id != ${syncRunId}
    RETURNING id
  `;

  log(`Deleted ${result.length} stale rows.`);
}

// =============================================================================
// Phase: Salesforce Sync
// =============================================================================

async function syncSalesforce() {
  log('Starting Salesforce Campaign Group Sync...');

  // Load active category mappings from DB
  const mappingRows = await sql`
    SELECT tableau_category, salesforce_category
    FROM category_mappings
    WHERE is_active = true AND salesforce_category IS NOT NULL
  `;
  const categoryMap = new Map();
  for (const row of mappingRows) {
    categoryMap.set(row.tableau_category, row.salesforce_category);
  }
  log(`Loaded ${categoryMap.size} active category mappings.`);

  await authenticateSalesforce();

  // Get distinct combos of lead_buyer + category + is_branded with active zip counts
  const combos = await sql`
    SELECT
      lead_buyer,
      category,
      is_branded,
      COUNT(DISTINCT zip) FILTER (WHERE campaign_status = 'Active') AS active_zip_count
    FROM single_coverage_zips_raw
    GROUP BY lead_buyer, category, is_branded
    ORDER BY lead_buyer, category, is_branded
  `;

  log(`Found ${combos.length} lead_buyer/category/is_branded combos to sync.`);

  let created = 0;
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const combo of combos) {
    const { lead_buyer, category, is_branded, active_zip_count } = combo;

    // Resolve mapped Salesforce category
    const sfCategory = categoryMap.get(category) || null;
    if (!sfCategory) {
      log(`Skipping "${lead_buyer}" / "${category}": No Salesforce category mapping found.`);
      skipped++;
      continue;
    }

    try {
      const derivedProduct = is_branded ? 'Branded' : 'Non-Branded';
      const campaignGroupName = `${derivedProduct} | ${sfCategory}`;

      // Check tracking table (uses raw `category` as the key — Tableau category)
      const existing = await sql`
        SELECT id, sf_campaign_group_id, last_synced_at
        FROM campaign_groups_sync_tracking
        WHERE lead_buyer = ${lead_buyer}
          AND category = ${category}
          AND is_branded = ${is_branded}
        LIMIT 1
      `;

      let sfCampaignGroupId = existing.length > 0 ? existing[0].sf_campaign_group_id : null;

      if (sfCampaignGroupId) {
        // Update existing Campaign Group in Salesforce
        const patchRes = await salesforceRequest(
          'PATCH',
          `/services/data/v57.0/sobjects/Campaign_Group__c/${sfCampaignGroupId}`,
          {
            Name: campaignGroupName,
            Category_New__c: sfCategory,
            Active_Zip_Count__c: Number(active_zip_count),
            Is_Branded__c: is_branded,
          }
        );

        if (!patchRes.ok && patchRes.status !== 204) {
          const text = await patchRes.text();
          throw new Error(`SF PATCH failed: ${patchRes.status} ${text}`);
        }

        // Update tracking table (uses raw `category`)
        await sql`
          UPDATE campaign_groups_sync_tracking
          SET
            last_synced_at = NOW(),
            active_zip_count = ${Number(active_zip_count)},
            campaign_group_name = ${campaignGroupName}
          WHERE lead_buyer = ${lead_buyer}
            AND category = ${category}
            AND is_branded = ${is_branded}
        `;

        updated++;
      } else {
        // Create new Campaign Group in Salesforce
        const postRes = await salesforceRequest(
          'POST',
          '/services/data/v57.0/sobjects/Campaign_Group__c',
          {
            Name: campaignGroupName,
            Category_New__c: sfCategory,
            Active_Zip_Count__c: Number(active_zip_count),
            Is_Branded__c: is_branded,
            Lead_Buyer__c: lead_buyer,
          }
        );

        if (!postRes.ok) {
          const text = await postRes.text();
          throw new Error(`SF POST failed: ${postRes.status} ${text}`);
        }

        const postData = await postRes.json();
        sfCampaignGroupId = postData.id;

        // Insert into tracking table (uses raw `category`)
        await sql`
          INSERT INTO campaign_groups_sync_tracking
            (lead_buyer, category, is_branded, sf_campaign_group_id, campaign_group_name, active_zip_count, last_synced_at)
          VALUES
            (${lead_buyer}, ${category}, ${is_branded}, ${sfCampaignGroupId}, ${campaignGroupName}, ${Number(active_zip_count)}, NOW())
          ON CONFLICT (lead_buyer, category, is_branded)
          DO UPDATE SET
            sf_campaign_group_id = EXCLUDED.sf_campaign_group_id,
            campaign_group_name = EXCLUDED.campaign_group_name,
            active_zip_count = EXCLUDED.active_zip_count,
            last_synced_at = NOW()
        `;

        created++;
      }
    } catch (err) {
      errors++;
      const message = err instanceof Error ? err.message : String(err);
      console.error(`Error syncing "${lead_buyer}" / "${category}" / branded=${is_branded}: ${message}`);
    }
  }

  log(`Salesforce sync complete. Created: ${created}, Updated: ${updated}, Skipped: ${skipped}, Errors: ${errors}`);
}

// =============================================================================
// Phase: Upload Coverage Zips CSV
// =============================================================================

async function uploadCoverageZipsCsv() {
  log('Uploading coverage zips CSV to app...');

  const appUrl = process.env.BLC_APP_URL;

  try {
    const response = await fetch(`${appUrl}/api/internal/coverage-zips/upload-csv`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ syncRunId }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`CSV upload failed: ${response.status} ${text}`);
    }

    log('Coverage zips CSV uploaded successfully.');
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`CSV upload error (non-fatal): ${message}`);
  }
}

// =============================================================================
// Phase: Post-Processing
// =============================================================================

async function postProcessing() {
  log('Starting post-processing...');

  // 1. Location enrichment — fill in missing state/city from known zips
  log('Enriching missing location data...');
  await sql`
    UPDATE single_coverage_zips_raw t
    SET
      state = s.state,
      city = s.city
    FROM (
      SELECT DISTINCT ON (zip) zip, state, city
      FROM single_coverage_zips_raw
      WHERE state != '' AND city != ''
      ORDER BY zip, last_seen_at DESC
    ) s
    WHERE t.zip = s.zip
      AND (t.state = '' OR t.city = '')
  `;
  log('Location enrichment complete.');

  // 2. Recalculate density counts per-category to avoid deadlocks
  log('Recalculating density counts...');
  const densityCategories = await sql`SELECT DISTINCT category FROM single_coverage_zips_raw`;
  for (const { category } of densityCategories) {
    await sql`
      WITH stats AS (
        SELECT
          zip,
          COUNT(DISTINCT lead_buyer) FILTER (WHERE campaign_status = 'Active') AS buyer_count,
          COUNT(DISTINCT lead_buy_campaign) AS campaign_count
        FROM single_coverage_zips_raw
        WHERE category = ${category}
        GROUP BY zip
      )
      UPDATE single_coverage_zips_raw t
      SET
        num_lead_buyers = s.buyer_count,
        num_lead_buy_campaigns = s.campaign_count
      FROM stats s
      WHERE t.category = ${category}
        AND t.zip = s.zip
    `;
  }
  log(`Density counts recalculated for ${densityCategories.length} categories.`);

  // 3. Refresh materialized views
  log('Refreshing materialized views...');
  await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY coverage_zips_summary`;
  log('Materialized views refreshed.');

  // 4. Clear caches
  log('Clearing app caches...');
  const appUrl = process.env.BLC_APP_URL;
  try {
    const response = await fetch(`${appUrl}/api/internal/coverage-zips/clear-cache`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ syncRunId }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Cache clear failed: ${response.status} ${text}`);
    }

    log('App caches cleared successfully.');
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`Cache clear error (non-fatal): ${message}`);
  }

  // 5. Upload CSV
  await uploadCoverageZipsCsv();

  log('Post-processing complete.');
}

// =============================================================================
// Main Function
// =============================================================================

async function main() {
  checkEnvVars();
  const start = Date.now();

  try {
    // Generate run ID without crypto.randomUUID()
    const runId = `sync_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    setSyncRunId(runId);

    const syncRes = await sql`
      INSERT INTO single_coverage_zips_sync_state
        (sync_run_id, view_id, status, current_phase, current_state, started_at, last_updated_at, phase_stats)
      VALUES
        (
          ${runId},
          ${process.env.TABLEAU_VIEW_COVERAGE_ZIPS_ID ?? 'unknown'},
          'extracting',
          1,
          'Starting',
          NOW(),
          NOW(),
          '{}'::jsonb
        )
      RETURNING id
    `;

    setSyncStateId(syncRes[0].id);
    log(`Started Sync Run: ${runId} (state row id=${syncRes[0].id})`);

    // --- Phase 1: Extraction ---
    await updateSyncState({ phase: 1, state: 'Authenticating with Tableau' });
    await authenticateTableau();

    // 1. Download full view data
    await updateSyncState({ state: 'Downloading full Tableau view data...' });
    log('Downloading full view data from Tableau...');
    const csvText = await downloadViewData('');
    const allRows = parseCSV(csvText);
    log(`Downloaded and parsed ${allRows.length} total rows.`);

    // 2. Group by Category
    const rowsByCategory = new Map();
    for (const row of allRows) {
      const cat = row['Category'];
      if (!cat) continue;

      if (!rowsByCategory.has(cat)) {
        rowsByCategory.set(cat, []);
      }
      rowsByCategory.get(cat).push(row);
    }

    const categories = [...rowsByCategory.keys()];
    log(`Found ${categories.length} categories: ${categories.join(', ')}`);

    // 3. Process each category
    await updateSyncState({ state: `Extracting ${categories.length} categories` });
    for (const cat of categories) {
      log(`Processing category: ${cat}`);
      const categoryRows = rowsByCategory.get(cat) || [];
      await processCategory(cat, categoryRows);
    }

    // --- Phase 2: Cleanup ---
    await updateSyncState({ phase: 2, state: 'Cleaning up stale data' });
    await cleanupStaleData();

    // --- Phase 3: Salesforce Sync ---
    await updateSyncState({ phase: 3, state: 'Syncing Campaign Groups to Salesforce' });
    await syncSalesforce();

    // --- Phase 4: Post-Processing ---
    await updateSyncState({ phase: 4, state: 'Post-processing' });
    await postProcessing();

    const duration = ((Date.now() - start) / 1000).toFixed(1);
    await updateSyncState({ status: 'completed', state: 'Done', completedAt: new Date() });
    log(`Sync completed successfully in ${duration}s.`);
    process.exit(0);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Fatal Error:', err);

    if (syncStateId) {
      await updateSyncState({
        status: 'failed',
        state: 'Failed',
        errorMessage: message,
        completedAt: new Date(),
      });
    }

    process.exit(1);
  } finally {
    await sql.end();
  }
}

main();
