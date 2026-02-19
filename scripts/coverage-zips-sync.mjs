/**
 * Single Buyer Coverage Zips Sync Script
 *
 * This script performs a full sync of coverage data:
 * 1. Extracts data from Tableau for all categories
 * 2. Upserts raw data into Postgres
 * 3. Cleans up stale data from previous runs
 * 4. Syncs Campaign Groups to Salesforce
 * 5. Refreshes materialized views and caches
 */

import postgres from 'postgres';

// =============================================================================
// 1. Configuration & Env Vars
// =============================================================================

const MAX_RETRIES = 3;
const BATCH_SIZE = 2000;
const SALESFORCE_API_VERSION = 'v59.0';
const TABLEAU_API_VERSION = '3.22';

const REQUIRED_ENV_VARS = [
    'SUPABASE_DATABASE_URL',
  'TABLEAU_SERVER_URL',
  'TABLEAU_SITE_ID',
  'TABLEAU_PAT_NAME',
  'TABLEAU_PAT_SECRET',
  'TABLEAU_VIEW_COVERAGE_ZIPS_ID',
  'SALESFORCE_OAUTH_CLIENT_ID',
  'SALESFORCE_OAUTH_CLIENT_SECRET',
];

const missingVars = REQUIRED_ENV_VARS.filter((v) => !process.env[v]);
if (missingVars.length > 0) {
  console.error(`Missing required environment variables: ${missingVars.join(', ')}`);
  process.exit(1);
}

// =============================================================================
// 2. DB Connection
// =============================================================================

const sql = postgres(process.env.SUPABASE_DATABASE_URL, {
  ssl: { rejectUnauthorized: false },
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10,
});

// =============================================================================
// Shared State
// =============================================================================

/** Integer primary key from single_coverage_zips_sync_state */
let syncStateId = null;
/** UUID string used as the logical run identifier */
let syncRunId = null;

// =============================================================================
// Logging & State Helpers
// =============================================================================

function log(message, data = null) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
  if (data) console.log(JSON.stringify(data, null, 2));
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Updates specific fields on the sync state row.
 * Writes explicit SET clauses to avoid dynamic sql() helper issues.
 *
 * @param {{ phase?: number, state?: string, recordsProcessed?: number, errorMessage?: string, completedAt?: Date }} fields
 */
async function updateSyncState(fields) {
  if (!syncStateId) return;

  try {
    if (fields.phase !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET current_phase = ${fields.phase}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
    if (fields.state !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET current_state = ${fields.state}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
    if (fields.recordsProcessed !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET records_processed = records_processed + ${fields.recordsProcessed}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
    if (fields.errorMessage !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET error_message = ${fields.errorMessage}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
    if (fields.completedAt !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET completed_at = ${fields.completedAt}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
    if (fields.status !== undefined) {
      await sql`
        UPDATE single_coverage_zips_sync_state
        SET status = ${fields.status}, last_updated_at = NOW()
        WHERE id = ${syncStateId}
      `;
    }
  } catch (err) {
    console.error('Failed to update sync state:', err);
  }
}

// =============================================================================
// 3. Tableau Auth Helpers
// =============================================================================

let tableauAuthToken = null;
let tableauSiteId = null; // actual site ID (UUID) returned from auth

async function authenticateTableau() {
  log('Authenticating with Tableau...');
  const url = `${process.env.TABLEAU_SERVER_URL}/api/${TABLEAU_API_VERSION}/auth/signin`;

  const body = {
    credentials: {
      personalAccessTokenName: process.env.TABLEAU_PAT_NAME,
      personalAccessTokenSecret: process.env.TABLEAU_PAT_SECRET,
      site: {
        contentUrl: process.env.TABLEAU_SITE_ID,
      },
    },
  };

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`Tableau Auth Failed: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  tableauAuthToken = data.credentials.token;
  tableauSiteId = data.credentials.site.id;
  log('Tableau authentication successful.');
}

async function downloadViewData(filter) {
  const viewId = process.env.TABLEAU_VIEW_COVERAGE_ZIPS_ID;
  const url = `${process.env.TABLEAU_SERVER_URL}/api/${TABLEAU_API_VERSION}/sites/${tableauSiteId}/views/${viewId}/data?${filter}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: { 'X-Tableau-Auth': tableauAuthToken },
  });

  if (!response.ok) {
    throw new Error(`Failed to download Tableau data (filter=${filter}): ${response.statusText}`);
  }

  return response.text();
}

async function getCategories() {
  log('Discovering categories...');

  const rows = await sql`
    SELECT DISTINCT category FROM single_coverage_zips_raw WHERE category IS NOT NULL
  `;

  let categories = rows.map((r) => r.category);

  if (categories.length === 0) {
    log('No categories found in DB. Fetching sample from Tableau to discover categories...');
    const csvText = await downloadViewData('vf_State=CA');
    const sampleRows = parseCSV(csvText);
    const discovered = [...new Set(sampleRows.map((r) => r['Category']).filter(Boolean))];
    if (discovered.length > 0) {
      categories = discovered;
      log(`Discovered ${categories.length} categories from Tableau sample.`);
    } else {
      log('Could not discover categories from Tableau. Falling back to default list.');
      categories = ['Home Improvement', 'Solar', 'Insurance', 'Home Services'];
    }
  }

  log(`Found ${categories.length} categories to process.`, categories);
  return categories;
}

// =============================================================================
// 4. CSV Parsing
// =============================================================================

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current);
  return result.map((s) => s.trim().replace(/^"|"$/g, '').replace(/""/g, '"'));
}

function parseCSV(text) {
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const results = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    if (values.length !== headers.length) continue;

    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx];
    });
    results.push(row);
  }
  return results;
}

// =============================================================================
// 5. Salesforce Token Helpers
// =============================================================================

let cachedAccessToken = null;
let cachedInstanceUrl = null;
let cachedTokenExpiresAt = null;

function normalizeSfUrl(url) {
  return url?.replace(/\/$/, '') ?? '';
}

async function getSalesforceTokens() {
  if (
    cachedAccessToken &&
    cachedInstanceUrl &&
    cachedTokenExpiresAt &&
    cachedTokenExpiresAt > Date.now() + 5 * 60 * 1000
  ) {
    return { accessToken: cachedAccessToken, instanceUrl: cachedInstanceUrl };
  }

  const rows = await sql`
    SELECT * FROM salesforce_oauth_tokens ORDER BY created_at DESC LIMIT 1
  `;

  if (rows.length === 0) {
    throw new Error('No Salesforce tokens found in DB.');
  }

  const tokenData = rows[0];
  const expiresAt = new Date(tokenData.expires_at).getTime();

  if (Date.now() > expiresAt - 5 * 60 * 1000) {
    log('Salesforce token expired or near expiry, refreshing...');
    return refreshSalesforceToken(tokenData);
  }

  cachedAccessToken = tokenData.access_token;
  cachedInstanceUrl = normalizeSfUrl(tokenData.instance_url);
  cachedTokenExpiresAt = expiresAt;

  return { accessToken: cachedAccessToken, instanceUrl: cachedInstanceUrl };
}

async function refreshSalesforceToken(oldTokenData) {
  const params = new URLSearchParams();
  params.append('grant_type', 'refresh_token');
  params.append('client_id', process.env.SALESFORCE_OAUTH_CLIENT_ID);
  params.append('client_secret', process.env.SALESFORCE_OAUTH_CLIENT_SECRET);
  params.append('refresh_token', oldTokenData.refresh_token);

  const tokenUrl =
    process.env.SALESFORCE_OAUTH_TOKEN_URL ||
    'https://login.salesforce.com/services/oauth2/token';

  const response = await fetch(normalizeSfUrl(tokenUrl) + (tokenUrl.includes('/token') ? '' : '/services/oauth2/token'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params,
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`SF Token Refresh Failed: ${txt}`);
  }

  const data = await response.json();
  const newAccessToken = data.access_token;
  const newInstanceUrl = normalizeSfUrl(data.instance_url);
  const newRefreshToken = data.refresh_token || oldTokenData.refresh_token;
  const newExpiresAt = new Date(Date.now() + 2 * 60 * 60 * 1000);

  // Single-row pattern: delete all then insert
  await sql`DELETE FROM salesforce_oauth_tokens`;
  await sql`
    INSERT INTO salesforce_oauth_tokens
      (access_token, refresh_token, instance_url, expires_at, token_type)
    VALUES
      (${newAccessToken}, ${newRefreshToken}, ${newInstanceUrl}, ${newExpiresAt}, 'Bearer')
  `;

  cachedAccessToken = newAccessToken;
  cachedInstanceUrl = newInstanceUrl;
  cachedTokenExpiresAt = newExpiresAt.getTime();

  log('Salesforce token refreshed and stored.');
  return { accessToken: cachedAccessToken, instanceUrl: cachedInstanceUrl };
}

// =============================================================================
// 6. Salesforce Request Helper
// =============================================================================

/**
 * Makes an authenticated request to Salesforce.
 *
 * @param {string} endpoint - Full path starting with /services/data/...
 * @param {{ method?: string, body?: unknown, headers?: Record<string, string> }} options
 * @param {number} retryCount
 * @returns {Promise<Response>} Raw fetch Response (caller decides how to read body)
 */
async function salesforceRequest(endpoint, options = {}, retryCount = 0) {
  const { accessToken, instanceUrl } = await getSalesforceTokens();
  const url = `${instanceUrl}${endpoint}`;

  const fetchOptions = {
    method: options.method ?? 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  };

  if (options.body !== undefined) {
    fetchOptions.body =
      typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
  }

  const res = await fetch(url, fetchOptions);

  if (res.status === 401 && retryCount < 1) {
    log('SF 401 Unauthorized, clearing token cache and retrying...');
    cachedAccessToken = null;
    cachedInstanceUrl = null;
    cachedTokenExpiresAt = null;
    return salesforceRequest(endpoint, options, retryCount + 1);
  }

  return res;
}

// =============================================================================
// 7. Phase Functions
// =============================================================================

// --- Phase 1: Tableau Extraction ---

async function processCategory(category) {
  try {
    const csvText = await downloadViewData(`vf_Category=${encodeURIComponent(category)}`);
    const rows = parseCSV(csvText);
    log(`Parsed ${rows.length} rows for category: ${category}`);

    const validRows = [];

    for (const row of rows) {
      const brandedVal = row['Branded Campaign'];
      if (brandedVal === 'All') continue; // skip aggregate rows

      const isBranded = brandedVal === 'Yes';
      const leadBuyer = row['Lead Buyer'];
      const zip = row['Zip'];

      if (!leadBuyer || !zip) continue;

      validRows.push({
        lead_buyer: leadBuyer,
        lead_buy_campaign: row['Lead Buy Campaign'] ?? null,
        category: row['Category'] ?? category,
        zip,
        city: row['City'] ?? null,
        state: row['State'] ?? null,
        county: row['County'] ?? null,
        dma: row['DMA'] ?? null,
        country: row['Country'] ?? 'US',
        is_branded: isBranded,
        buyer_status: row['Buyer Status'] ?? null,
        campaign_status: row['Campaign Status'] ?? null,
        sync_run_id: syncRunId,
        last_updated_at: new Date(),
      });
    }

    for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
      const batch = validRows.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO single_coverage_zips_raw ${sql(batch)}
        ON CONFLICT (lead_buyer, lead_buy_campaign, category, zip)
        DO UPDATE SET
          sync_run_id    = EXCLUDED.sync_run_id,
          last_updated_at     = NOW(),
          is_branded     = EXCLUDED.is_branded,
          city           = EXCLUDED.city,
          state          = EXCLUDED.state,
          county         = EXCLUDED.county,
          dma            = EXCLUDED.dma,
          buyer_status   = EXCLUDED.buyer_status,
          campaign_status = EXCLUDED.campaign_status
      `;
    }

    log(`Upserted ${validRows.length} rows for category: ${category}`);
    await updateSyncState({ recordsProcessed: validRows.length });
  } catch (err) {
    log(`Error processing category "${category}": ${err instanceof Error ? err.message : String(err)}`);
    // Non-fatal — continue with remaining categories
  }
}

// --- Phase 2: Stale Data Cleanup ---

async function cleanupStaleData() {
  log('Cleaning up stale data...');
  const result = await sql`
    DELETE FROM single_coverage_zips_raw
    WHERE sync_run_id != ${syncRunId} OR sync_run_id IS NULL
  `;
  log(`Deleted ${result.count} stale rows.`);
}

// --- Phase 3: Salesforce Sync ---

async function uploadCoverageZipsCsv(sfGroupId, category, product, zipsData) {
  const { accessToken, instanceUrl } = await getSalesforceTokens();

  const sanitizedCategory = category.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
  const sanitizedProduct = product.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();

  const csvHeader = 'zip,city,state,county,dma,campaign_status,lead_buy_campaign';
  const csvRows = zipsData.map((z) =>
    [
      z.zip ?? '',
      z.city ?? '',
      z.state ?? '',
      z.county ?? '',
      z.dma ?? '',
      z.campaign_status ?? '',
      z.lead_buy_campaign ?? '',
    ]
      .map((v) => `"${String(v).replace(/"/g, '""')}"`)
      .join(',')
  );
  const csvContent = [csvHeader, ...csvRows].join('\n');

  const boundary = `----FlootBoundary${Math.random().toString(36).substr(2, 16)}`;

  const metadata = {
    Title: `Coverage Zips - ${category}`,
    PathOnClient: `coverage_zips_${sanitizedCategory}_${sanitizedProduct}.csv`,
    FirstPublishLocationId: sfGroupId,
  };

  let body = '';
  body += `--${boundary}\r\n`;
  body += 'Content-Disposition: form-data; name="entity_content";\r\n';
  body += 'Content-Type: application/json\r\n\r\n';
  body += JSON.stringify(metadata) + '\r\n';
  body += `--${boundary}\r\n`;
  body += `Content-Disposition: form-data; name="VersionData"; filename="${metadata.PathOnClient}"\r\n`;
  body += 'Content-Type: text/csv\r\n\r\n';
  body += csvContent + '\r\n';
  body += `--${boundary}--`;

  const url = `${instanceUrl}/services/data/${SALESFORCE_API_VERSION}/sobjects/ContentVersion`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
    },
    body,
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`Failed to upload ContentVersion for "${category}": ${txt}`);
  }

  return response.json();
}

async function syncSalesforce() {
  log('Starting Salesforce Campaign Group Sync...');

  // After cleanup, all rows in the table belong to the current run.
  // Count active zips only (campaign_status = 'Active').
  const combos = await sql`
    SELECT
      lead_buyer,
      category,
      is_branded,
      COUNT(*) FILTER (WHERE campaign_status = 'Active') AS active_zip_count
    FROM single_coverage_zips_raw
    GROUP BY lead_buyer, category, is_branded
  `;

  log(`Found ${combos.length} unique buyer/category/branded combinations to sync.`);

  for (const combo of combos) {
    try {
      const { lead_buyer, category, is_branded, active_zip_count } = combo;

      const derivedProduct = is_branded
        ? 'Marketplace - Branded'
        : 'Marketplace - Standard - HS';

      // Strip trailing " (123)" buyer ID suffix
      const cleanBuyerName = lead_buyer.replace(/\s*\([^)]*\)\s*$/, '').trim();

      // Resolve Salesforce Account ID
      const accountLink = await sql`
        SELECT sa.salesforce_id
        FROM unified_accounts ua
        JOIN salesforce_accounts sa ON ua.linked_account_id = sa.id
        WHERE ua.tableau_account_name = ${cleanBuyerName}
        LIMIT 1
      `;

      if (accountLink.length === 0) {
        log(`Skipping "${cleanBuyerName}": No linked Salesforce Account found.`);
        continue;
      }
      const sfAccountId = accountLink[0].salesforce_id;

      const campaignGroupName = `${derivedProduct} | ${category}`;

      // Check local tracking table first
      const tracking = await sql`
        SELECT salesforce_campaign_group_id
        FROM campaign_groups_sync_tracking
        WHERE salesforce_account_id = ${sfAccountId}
          AND product = ${derivedProduct}
          AND salesforce_category = ${category}
        LIMIT 1
      `;

      let sfCampaignGroupId = null;

      if (tracking.length > 0) {
        sfCampaignGroupId = tracking[0].salesforce_campaign_group_id;
        log(`Found existing Campaign Group (tracking): ${sfCampaignGroupId}`);
      } else {
        // Query Salesforce
        const soql = `SELECT Id FROM Campaign_Group__c WHERE Name = '${campaignGroupName.replace(/'/g, "\\'")}' AND Account__c = '${sfAccountId}' LIMIT 1`;
        const queryRes = await salesforceRequest(
          `/services/data/${SALESFORCE_API_VERSION}/query?q=${encodeURIComponent(soql)}`
        );

        if (!queryRes.ok) {
          const txt = await queryRes.text();
          throw new Error(`SF SOQL query failed: ${txt}`);
        }

        const queryData = await queryRes.json();

        if (queryData.totalSize > 0) {
          sfCampaignGroupId = queryData.records[0].Id;
          log(`Found existing Campaign Group (SF query): ${sfCampaignGroupId}`);
        } else {
          log(`Creating new Campaign Group: "${campaignGroupName}" for Account ${sfAccountId}`);
          const createRes = await salesforceRequest(
            `/services/data/${SALESFORCE_API_VERSION}/sobjects/Campaign_Group__c`,
            {
              method: 'POST',
              body: {
                Name: campaignGroupName,
                Account__c: sfAccountId,
                Product_c__c: derivedProduct,
                Category_New__c: category,
                Buyerlink_Vertical__c: 'Home Services',
                Status__c: 'Active',
              },
            }
          );

          if (!createRes.ok) {
            const txt = await createRes.text();
            throw new Error(`SF Campaign Group creation failed: ${txt}`);
          }

          const createData = await createRes.json();
          sfCampaignGroupId = createData.id;
          log(`Created Campaign Group: ${sfCampaignGroupId}`);
        }

        // Upsert into local tracking table
        await sql`
          INSERT INTO campaign_groups_sync_tracking
            (salesforce_account_id, product, salesforce_category, salesforce_campaign_group_id, status)
          VALUES
            (${sfAccountId}, ${derivedProduct}, ${category}, ${sfCampaignGroupId}, 'active')
          ON CONFLICT (salesforce_account_id, product, salesforce_category)
          DO UPDATE SET
            salesforce_campaign_group_id = EXCLUDED.salesforce_campaign_group_id,
            last_updated_at = NOW()
        `;
      }

      // PATCH active zip count
      const patchRes = await salesforceRequest(
        `/services/data/${SALESFORCE_API_VERSION}/sobjects/Campaign_Group__c/${sfCampaignGroupId}`,
        {
          method: 'PATCH',
          body: { Active_Zip_Code_Count__c: parseInt(active_zip_count, 10) },
        }
      );

      if (patchRes.status !== 204 && !patchRes.ok) {
        const txt = await patchRes.text();
        throw new Error(`SF PATCH zip count failed: ${txt}`);
      }

      // Fetch all zip data for this combo and upload CSV
      const zipsData = await sql`
        SELECT zip, city, state, county, dma, campaign_status, lead_buy_campaign
        FROM single_coverage_zips_raw
        WHERE lead_buyer = ${lead_buyer}
          AND category = ${category}
          AND is_branded = ${is_branded}
      `;

      await uploadCoverageZipsCsv(sfCampaignGroupId, category, derivedProduct, zipsData);

      log(
        `Synced "${cleanBuyerName}" — "${campaignGroupName}": active_zip_count=${active_zip_count}, csv_rows=${zipsData.length}`
      );
    } catch (err) {
      log(
        `Error syncing combo ${JSON.stringify(combo)}: ${err instanceof Error ? err.message : String(err)}`
      );
      // Non-fatal — continue with remaining combos
    }
  }
}

// --- Phase 4: Post-Processing ---

async function postProcessing() {
  log('Starting post-processing...');

  // 1. Location enrichment for rows missing state or county
  log('Enriching locations from zip_code_lookup...');
  await sql`
    UPDATE single_coverage_zips_raw r
    SET
      state  = z.state,
      county = z.county
    FROM zip_code_lookup z
    WHERE r.zip = z.zip
      AND (r.state IS NULL OR r.county IS NULL)
  `;

  // 2. Recalculate density counts across ALL rows in the table
  //    num_lead_buyers   = distinct buyers with campaign_status = 'Active' per category+zip
  //    num_lead_buy_campaigns = distinct campaigns (all statuses) per category+zip
  log('Recalculating density counts...');
  await sql`
    WITH stats AS (
      SELECT
        category,
        zip,
        COUNT(DISTINCT lead_buyer) FILTER (WHERE campaign_status = 'Active') AS buyer_count,
        COUNT(DISTINCT lead_buy_campaign)                                      AS campaign_count
      FROM single_coverage_zips_raw
      GROUP BY category, zip
    )
    UPDATE single_coverage_zips_raw t
    SET
      num_lead_buyers        = s.buyer_count,
      num_lead_buy_campaigns = s.campaign_count
    FROM stats s
    WHERE t.category = s.category
      AND t.zip      = s.zip
  `;

  // 3. Clear application cache
  log('Clearing application cache...');
  await sql`TRUNCATE TABLE cache_entries`;

  // 4. Refresh materialized view
  log('Refreshing materialized view...');
  try {
    await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY single_buyer_coverage_summary`;
  } catch (err) {
    log('Concurrent refresh failed (view likely not yet populated), trying standard refresh...');
    await sql`REFRESH MATERIALIZED VIEW single_buyer_coverage_summary`;
  }

  log('Post-processing complete.');
}

// =============================================================================
// 8. Main Function
// =============================================================================

async function main() {
  const start = Date.now();

  try {
    // Generate run ID without crypto.randomUUID()
    syncRunId = `sync_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const syncRes = await sql`
      INSERT INTO single_coverage_zips_sync_state
        (sync_run_id, view_id, status, current_phase, current_state, started_at, last_updated_at, phase_stats)
      VALUES
        (
          ${syncRunId},
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

    syncStateId = syncRes[0].id;
    log(`Started Sync Run: ${syncRunId} (state row id=${syncStateId})`);

    // --- Phase 1: Extraction ---
    await updateSyncState({ phase: 1, state: 'Authenticating with Tableau' });
    await authenticateTableau();

    await updateSyncState({ state: 'Discovering categories' });
    const categories = await getCategories();

    await updateSyncState({ state: `Extracting ${categories.length} categories` });
    for (const cat of categories) {
      log(`Processing category: ${cat}`);
      await processCategory(cat);
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
