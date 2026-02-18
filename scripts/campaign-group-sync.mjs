import postgres from 'postgres';

// --- Configuration ---
const CONFIG = {
  BATCH_SIZE: 2000,
  SALESFORCE_API_VERSION: 'v59.0',
};

// --- Utils ---

function getEnv(key) {
  const val = process.env[key];
  if (!val) {
    console.error(`Missing environment variable: ${key}`);
    process.exit(1);
  }
  return val;
}

const dbUrl = getEnv('DATABASE_URL');
const sfClientId = getEnv('SALESFORCE_OAUTH_CLIENT_ID');
const sfClientSecret = getEnv('SALESFORCE_OAUTH_CLIENT_SECRET');
// Token URL might be missing if not set in secrets, provide default
const sfTokenUrlRaw = process.env['SALESFORCE_OAUTH_TOKEN_URL'] || 'https://login.salesforce.com/services/oauth2/token';

const sql = postgres(dbUrl, {
  ssl: { rejectUnauthorized: false }, // Should be true in strict prod but often false for hosted DBs
  idle_timeout: 20,
  max: 5
});

// Helper to normalize SF URLs
function normalizeSfUrl(url) {
  let normalized = url.trim();
  if (!normalized.startsWith('http')) {
    normalized = `https://${normalized}`;
  }
  if (!normalized.includes('token')) {
    normalized = normalized.replace(/\/$/, '') + '/services/oauth2/token';
  }
  return normalized;
}

const SF_TOKEN_URL = normalizeSfUrl(sfTokenUrlRaw);

// --- Salesforce Helpers ---

let cachedAccessToken = null;
let cachedInstanceUrl = null;

async function getSalesforceTokens() {
  if (cachedAccessToken && cachedInstanceUrl) {
    return { accessToken: cachedAccessToken, instanceUrl: cachedInstanceUrl };
  }

  // Get tokens from DB
  const tokens = await sql`
    SELECT access_token, refresh_token, instance_url, expires_at 
    FROM salesforce_oauth_tokens 
    ORDER BY created_at DESC LIMIT 1
  `;

  if (tokens.length === 0) {
    throw new Error("No Salesforce tokens found in database.");
  }

  const tokenRecord = tokens[0];
  const now = new Date();
  const expiresAt = new Date(tokenRecord.expires_at);
  const bufferMs = 5 * 60 * 1000; // 5 min buffer

  // If token is valid, use it
  if (expiresAt.getTime() > now.getTime() + bufferMs) {
    console.log("Using existing Salesforce access token.");
    cachedAccessToken = tokenRecord.access_token;
    cachedInstanceUrl = tokenRecord.instance_url;
    return { accessToken: cachedAccessToken, instanceUrl: cachedInstanceUrl };
  }

  // Refresh token
  console.log("Salesforce token expired, refreshing...");
  return refreshSalesforceToken(tokenRecord.refresh_token, tokenRecord.instance_url);
}

async function refreshSalesforceToken(refreshToken, currentInstanceUrl) {
  const params = new URLSearchParams();
  params.append("grant_type", "refresh_token");
  params.append("refresh_token", refreshToken);
  params.append("client_id", sfClientId);
  params.append("client_secret", sfClientSecret);

  const response = await fetch(SF_TOKEN_URL, {
    method: "POST",
    body: params,
  });

  if (!response.ok) {
    const errorBody = await response.text();
    console.error("Salesforce token refresh failed:", errorBody);
    throw new Error(`Failed to refresh Salesforce token: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  const accessToken = data.access_token;
  const instanceUrl = data.instance_url || currentInstanceUrl;
  
  // Update DB
  // Salesforce usually returns expires_in, but if not we assume 2 hours
  const expiresInSeconds = 2 * 60 * 60;
  const newExpiresAt = new Date(Date.now() + expiresInSeconds * 1000);

  // We delete old and insert new to be safe with single row logic often used
  await sql`DELETE FROM salesforce_oauth_tokens`;
  await sql`
    INSERT INTO salesforce_oauth_tokens (access_token, refresh_token, instance_url, token_type, expires_at)
    VALUES (${accessToken}, ${data.refresh_token || refreshToken}, ${instanceUrl}, ${data.token_type || 'Bearer'}, ${newExpiresAt})
  `;

  cachedAccessToken = accessToken;
  cachedInstanceUrl = instanceUrl;

  console.log("Salesforce token refreshed successfully.");
  return { accessToken, instanceUrl };
}

async function salesforceRequest(endpoint, options = {}, retryCount = 0) {
  const { accessToken, instanceUrl } = await getSalesforceTokens();
  
  const url = endpoint.startsWith('http') ? endpoint : `${instanceUrl}${endpoint}`;
  
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
    ...options.headers
  };

  const response = await fetch(url, {
    ...options,
    headers
  });

  if (response.status === 401 && retryCount < 1) {
    console.log("Got 401 from Salesforce, forcing token refresh and retrying...");
    cachedAccessToken = null; // Force refresh
    return salesforceRequest(endpoint, options, retryCount + 1);
  }

  // PATCH returns 204 No Content on success
  if (response.status === 204) {
    return response;
  }

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Salesforce Request Failed (${response.status}): ${errorText}`);
  }

  return response;
}

async function salesforceQuery(soql) {
  const response = await salesforceRequest(`/services/data/${CONFIG.SALESFORCE_API_VERSION}/query?q=${encodeURIComponent(soql)}`);
  return response.json();
}

async function salesforceCreate(objectName, data) {
  const response = await salesforceRequest(`/services/data/${CONFIG.SALESFORCE_API_VERSION}/sobjects/${objectName}`, {
    method: "POST",
    body: JSON.stringify(data)
  });
  return response.json();
}


// --- Main Processing Logic ---

async function main() {
  console.log("Starting Campaign Group Sync Script...");
  
  const stats = {
    processed: 0,
    created: 0,
    existing: 0,
    skipped: 0,
    errors: 0,
    details: [],
  };

  try {
    // 1. Fetch Source Data (Tableau + Mappings)
    console.log("Fetching distinct combinations from database...");
    
    // This query mirrors the logic in helpers/campaignGroupSyncHelper.tsx and campaignGroupSyncDeltaCheck.tsx
    // It joins tableau data with unified accounts (to get SF Account ID) and category mappings (to get SF Category)
    const sourceRows = await sql`
      SELECT DISTINCT
        tcd.account AS tableau_account,
        tcd.campaign AS tableau_campaign,
        tcd.category AS tableau_category,
        tcd.product_type,
        sa.salesforce_id,
        cm.salesforce_category AS mapped_category
      FROM tableau_campaign_data AS tcd
      INNER JOIN unified_accounts AS ua ON ua.tableau_account_name = tcd.account
      INNER JOIN salesforce_accounts AS sa ON sa.id = ua.linked_account_id
      LEFT JOIN category_mappings AS cm ON cm.tableau_category = tcd.category
      WHERE tcd.category IS NOT NULL
        AND tcd.category != '-'
        AND ua.linked_account_id IS NOT NULL
        AND ua.is_active = true
        AND cm.is_active = true
    `;

    console.log(`Found ${sourceRows.length} source combinations.`);

    // 2. Fetch Tracking Data
    const trackingRows = await sql`
      SELECT salesforce_account_id, product, salesforce_category 
      FROM campaign_groups_sync_tracking
    `;

    const trackedSet = new Set();
    trackingRows.forEach(row => {
      trackedSet.add(`${row.salesforce_account_id}|${row.product}|${row.salesforce_category}`);
    });

    console.log(`Found ${trackingRows.length} already tracked combinations.`);

    // 3. Process Differences
    for (const row of sourceRows) {
      stats.processed++;
      const { 
        tableau_account, 
        tableau_campaign, 
        product_type, 
        salesforce_id: sfAccountId, 
        mapped_category: sfCategory 
      } = row;

      // Logic: Determine Product
      let product = product_type;
      if (!product) {
        product = "Marketplace - Standard - HS";
        if (
          tableau_campaign && (
          tableau_campaign.includes("Branded") ||
          tableau_campaign.includes("Microsite")
          )
        ) {
          product = "Marketplace - Branded";
        }
      }

      if (!sfCategory || !sfAccountId) {
        stats.skipped++;
        continue;
      }

      const key = `${sfAccountId}|${product}|${sfCategory}`;
      
      // If already tracked, skip
      if (trackedSet.has(key)) {
        continue;
      }

      console.log(`Processing untracked group: Account=${sfAccountId}, Product=${product}, Category=${sfCategory}`);

      // Construct Name
      const campaignGroupName = `${product} | ${sfCategory}`;
      let sfId = null;

      try {
        // 4. Check Salesforce
        const soql = `SELECT Id FROM Campaign_Group__c WHERE Account__c = '${sfAccountId}' AND Name = '${campaignGroupName}' LIMIT 1`;
        const sfResult = await salesforceQuery(soql);

        if (sfResult.records && sfResult.records.length > 0) {
          // Exists in SF
          sfId = sfResult.records[0].Id;
          stats.existing++;
          console.log(`-> Found existing in SF: ${sfId}`);
        } else {
          // 5. Create in Salesforce
          console.log(`-> Creating new Campaign Group: ${campaignGroupName}`);
          
          const createResult = await salesforceCreate("Campaign_Group__c", {
            Name: campaignGroupName,
            Account__c: sfAccountId,
            Product_c__c: product,
            Category_New__c: sfCategory,
            Buyerlink_Vertical__c: "Home Services",
            Status__c: "Active",
          });

          if (createResult.success) {
            sfId = createResult.id;
            stats.created++;
            console.log(`-> Created successfully: ${sfId}`);
          } else {
            throw new Error(`Failed to create Campaign Group: ${JSON.stringify(createResult)}`);
          }
        }

        // 6. Update Tracking Table
        if (sfId) {
          await sql`
            INSERT INTO campaign_groups_sync_tracking (salesforce_account_id, product, salesforce_category, salesforce_campaign_group_id, updated_at)
            VALUES (${sfAccountId}, ${product}, ${sfCategory}, ${sfId}, NOW())
            ON CONFLICT (salesforce_account_id, product, salesforce_category) 
            DO UPDATE SET updated_at = NOW(), salesforce_campaign_group_id = ${sfId}
          `;
                // Add to local set so we don't process duplicates if source data has multiples mapping to same result
          trackedSet.add(key); 
        }

      } catch (err) {
        console.error(`Error processing group ${product}|${sfCategory} for account ${tableau_account}:`, err);
        stats.errors++;
        stats.details.push(`Error: ${product}|${sfCategory} - ${err.message || String(err)}`);
      }
    }

    console.log("Campaign Group Sync Completed.");
    console.log(JSON.stringify(stats, null, 2));

    if (stats.errors > 0) {
       console.warn("Finished with errors.");
       // We usually don't want to crash the whole job if some items failed, unless it was catastrophic.
       // But if you want to flag the job as failed in GitHub, you can exit 1 here.
       // For now, we exit 0 because partial success is better than retrying the whole thing.
    }

  } catch (error) {
    console.error("Fatal error in main loop:", error);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

main();
