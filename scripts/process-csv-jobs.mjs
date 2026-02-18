import postgres from 'postgres';
import fs from 'fs';

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
const supabaseApiUrl = getEnv('SUPABASE_API_URL').replace(/\/$/, "");
const supabaseKey = getEnv('SUPABASE_SERVICE_ROLE_KEY');

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

// --- Supabase Helpers ---

async function downloadChunk(path) {
  // Remove leading slash
  const cleanPath = path.startsWith("/") ? path.slice(1) : path;
  const url = `${supabaseApiUrl}/storage/v1/object/csv-uploads/${cleanPath}`;
  
  const response = await fetch(url, {
    headers: { 'Authorization': `Bearer ${supabaseKey}` }
  });

  if (!response.ok) {
    throw new Error(`Failed to download chunk ${cleanPath}: ${response.statusText}`);
  }
  
  return response.json();
}

async function deleteChunk(path) {
  const cleanPath = path.startsWith("/") ? path.slice(1) : path;
  const url = `${supabaseApiUrl}/storage/v1/object/csv-uploads/${cleanPath}`;
  
  await fetch(url, {
    method: 'DELETE',
    headers: { 'Authorization': `Bearer ${supabaseKey}` }
  });
}

// --- Main Processing Logic ---

async function main() {
  const forcedJobId = process.argv[2];
  
  try {
    // 1. Find Job
    let job;
    if (forcedJobId) {
      console.log(`Looking for specific job: ${forcedJobId}`);
      [job] = await sql`SELECT * FROM csv_upload_jobs WHERE id = ${forcedJobId}`;
    } else {
      console.log("Looking for oldest pending job...");
      [job] = await sql`
        SELECT * FROM csv_upload_jobs 
        WHERE status IN ('pending', 'processing_upsert', 'processing_finalize')
        ORDER BY created_at ASC
        LIMIT 1
      `;
    }

    if (!job) {
      console.log("No jobs to process.");
      process.exit(0);
    }

    if (['completed', 'failed'].includes(job.status)) {
      console.log(`Job ${job.id} is already ${job.status}. Exiting.`);
      process.exit(0);
    }

    console.log(`Starting processing for job ${job.id} (Status: ${job.status})`);
    
    // 2. Phase 1: UPSERT
    if (['pending', 'processing_upsert'].includes(job.status)) {
      console.log("--- Phase 1: Upsert ---");
      
      if (job.status === 'pending') {
        await sql`UPDATE csv_upload_jobs SET status = 'processing_upsert', updated_at = NOW() WHERE id = ${job.id}`;
      }

      // Download all chunks
      const numChunks = job.chunk_count || 1;
      const allRows = [];
      console.log(`Downloading ${numChunks} chunks...`);
      
      for (let i = 0; i < numChunks; i++) {
        const chunkPath = `${job.storage_path}_chunk_${i}.json`;
        try {
          const rows = await downloadChunk(chunkPath);
          allRows.push(...rows);
          console.log(`Chunk ${i} loaded: ${rows.length} rows`);
        } catch (e) {
          throw new Error(`Failed to download chunk ${i}: ${e.message}`);
        }
      }

      console.log(`Total rows to process: ${allRows.length}`);

      // Resume from rows_processed
      let currentIndex = job.rows_processed || 0;
      
      if (currentIndex < allRows.length) {
        console.log(`Resuming from row ${currentIndex}...`);
        
        while (currentIndex < allRows.length) {
          const batchRaw = allRows.slice(currentIndex, currentIndex + CONFIG.BATCH_SIZE);
          
          // Deduplicate batch
          const dedupedMap = new Map();
          for (const r of batchRaw) {
            const key = `${r.lead_buyer}\0${r.lead_buy_campaign}\0${r.category}\0${r.zip}`;
            dedupedMap.set(key, r);
          }
          const uniqueBatch = Array.from(dedupedMap.values());

          // Prepare for insert
          const now = new Date();
          const batchTimestamp = new Date(job.batch_timestamp);
          
          const records = uniqueBatch.map(row => ({
            lead_buyer: row.lead_buyer,
            lead_buy_campaign: row.lead_buy_campaign,
            category: row.category,
            zip: row.zip,
            max_bid: row.max_bid ?? null,
            vertical: row.vertical ?? null,
            campaign_status: row.campaign_status || 'Active',
            country: 'US',
            job_id: job.id,
            updated_at: batchTimestamp,
            created_at: now
          }));

          if (records.length > 0) {
            await sql`
              INSERT INTO single_coverage_zips_raw ${sql(records)}
              ON CONFLICT (lead_buyer, lead_buy_campaign, category, zip)
              DO UPDATE SET
                max_bid = EXCLUDED.max_bid,
                vertical = EXCLUDED.vertical,
                campaign_status = EXCLUDED.campaign_status,
                country = 'US',
                job_id = ${job.id},
                updated_at = ${batchTimestamp}
            `;
          }

          currentIndex += batchRaw.length;
          
          // Update progress
          await sql`
            UPDATE csv_upload_jobs 
            SET rows_processed = ${currentIndex}, updated_at = NOW() 
            WHERE id = ${job.id}
          `;
          
          if (currentIndex % 10000 === 0) {
            console.log(`Processed ${currentIndex}/${allRows.length} rows...`);
          }
        }
      }

      // Mark upsert complete
      await sql`
        UPDATE csv_upload_jobs 
        SET status = 'processing_finalize', updated_at = NOW() 
        WHERE id = ${job.id}
      `;
      job.status = 'processing_finalize';
      console.log("Upsert phase complete.");
    }

    // 3. Phase 2: FINALIZE
    if (job.status === 'processing_finalize') {
      console.log("--- Phase 2: Finalize ---");
      
      const stats = {
        pausedRows: 0,
        groupsCreated: 0,
        groupsExisting: 0,
        groupsUpdated: 0,
        csvAttached: 0,
        errors: 0,
        details: [],
      };

      // 3a. Pause Stale Rows
      // Only necessary if skipPauseStep isn't true (we can assume false for full run)
      // We implement it fully here.
      console.log("Pausing stale rows...");
      
      const affectedBuyers = job.affected_lead_buyers || [];
      if (affectedBuyers.length > 0) {
        const pauseResult = await sql`
          WITH uploaded_combos AS (
            SELECT DISTINCT 
              lead_buyer,
              category,
              CASE 
                WHEN lead_buy_campaign ILIKE '%branded%' OR lead_buy_campaign ILIKE '%microsite%' 
                THEN 'Marketplace - Branded' 
                ELSE 'Marketplace - Standard - HS' 
              END as derived_product
            FROM single_coverage_zips_raw
            WHERE lead_buyer = ANY(${affectedBuyers})
            AND job_id = ${job.id}
          ),
          updates AS (
            UPDATE single_coverage_zips_raw r
            SET campaign_status = 'Paused', updated_at = NOW()
            FROM uploaded_combos uc
            WHERE r.lead_buyer = uc.lead_buyer
              AND r.category = uc.category
              AND (CASE 
                WHEN r.lead_buy_campaign ILIKE '%branded%' OR r.lead_buy_campaign ILIKE '%microsite%' 
                THEN 'Marketplace - Branded' 
                ELSE 'Marketplace - Standard - HS' 
              END) = uc.derived_product
              AND (r.job_id IS NULL OR r.job_id != ${job.id})
              AND r.campaign_status = 'Active'
            RETURNING 1
          )
          SELECT count(*) as count FROM updates
        `;
        stats.pausedRows = parseInt(pauseResult[0]?.count || 0);
        console.log(`Paused ${stats.pausedRows} rows.`);
      }

      // 3b. Campaign Group Sync
      console.log("Starting Campaign Group Sync...");
      
      const activeCombos = await sql`
        SELECT DISTINCT 
          lead_buyer,
          category,
          CASE 
            WHEN lead_buy_campaign ILIKE '%branded%' OR lead_buy_campaign ILIKE '%microsite%' 
            THEN 'Marketplace - Branded' 
            ELSE 'Marketplace - Standard - HS' 
          END as derived_product
        FROM single_coverage_zips_raw
        WHERE lead_buyer = ANY(${affectedBuyers})
        AND job_id = ${job.id}
        ORDER BY lead_buyer, category, derived_product
      `;

      console.log(`Found ${activeCombos.length} unique campaign groups to process.`);

      for (const combo of activeCombos) {
        const { lead_buyer, category, derived_product } = combo;
        const cleanedName = lead_buyer.replace(/\s*\([^)]+\)\s*$/, "").trim();

        try {
          // Resolve SF Account
          const [account] = await sql`
            SELECT sa.salesforce_id
            FROM unified_accounts ua
            JOIN salesforce_accounts sa ON sa.id = ua.linked_account_id
            WHERE ua.tableau_account_name = ${cleanedName}
            LIMIT 1
          `;

          if (!account || !account.salesforce_id) {
            stats.details.push(`Skipped ${lead_buyer}: No linked Salesforce Account found.`);
            continue;
          }

          const sfAccountId = account.salesforce_id;
          const campaignGroupName = `${derived_product} | ${category}`;
          let sfGroupId = null;

          // Check local tracking
          const [tracked] = await sql`
            SELECT salesforce_campaign_group_id 
            FROM campaign_groups_sync_tracking 
            WHERE salesforce_account_id = ${sfAccountId} 
              AND product = ${derived_product} 
              AND salesforce_category = ${category}
          `;

          if (tracked) {
            sfGroupId = tracked.salesforce_campaign_group_id;
            stats.groupsExisting++;
          } else {
            // Check Salesforce
            const query = `SELECT Id FROM Campaign_Group__c WHERE Account__c = '${sfAccountId}' AND Name = '${campaignGroupName}' LIMIT 1`;
            const sfRes = await salesforceRequest(`/services/data/${CONFIG.SALESFORCE_API_VERSION}/query?q=${encodeURIComponent(query)}`)
              .then(r => r.json());
            
            if (sfRes.records && sfRes.records.length > 0) {
              sfGroupId = sfRes.records[0].Id;
              stats.groupsExisting++;
            } else {
              // Create in SF
              console.log(`Creating Campaign Group: ${campaignGroupName}`);
              const createRes = await salesforceRequest(`/services/data/${CONFIG.SALESFORCE_API_VERSION}/sobjects/Campaign_Group__c`, {
                method: 'POST',
                body: JSON.stringify({
                  Name: campaignGroupName,
                  Account__c: sfAccountId,
                  Product_c__c: derived_product,
                  Category_New__c: category,
                  Buyerlink_Vertical__c: "Home Services",
                  Status__c: "Active"
                })
              }).then(r => r.json());

              if (createRes.success) {
                sfGroupId = createRes.id;
                stats.groupsCreated++;
              } else {
                throw new Error(`Failed to create group: ${JSON.stringify(createRes.errors)}`);
              }
            }

            // Update tracking
            if (sfGroupId) {
              await sql`
                INSERT INTO campaign_groups_sync_tracking 
                  (salesforce_account_id, product, salesforce_category, salesforce_campaign_group_id, updated_at)
                VALUES (${sfAccountId}, ${derived_product}, ${category}, ${sfGroupId}, NOW())
                ON CONFLICT (salesforce_account_id, product, salesforce_category) 
                DO UPDATE SET salesforce_campaign_group_id = ${sfGroupId}, updated_at = NOW()
              `;
            }
          }

          if (!sfGroupId) continue;

          // Count active zips
          const [countRes] = await sql`
            SELECT COUNT(*) as count
            FROM single_coverage_zips_raw
            WHERE lead_buyer = ${lead_buyer}
              AND category = ${category}
              AND (CASE 
                WHEN lead_buy_campaign ILIKE '%branded%' OR lead_buy_campaign ILIKE '%microsite%' 
                THEN 'Marketplace - Branded' 
                ELSE 'Marketplace - Standard - HS' 
              END) = ${derived_product}
              AND campaign_status = 'Active'
          `;
          
          // Update SF count
          await salesforceRequest(`/services/data/${CONFIG.SALESFORCE_API_VERSION}/sobjects/Campaign_Group__c/${sfGroupId}`, {
            method: 'PATCH',
            body: JSON.stringify({
              Active_Zip_Code_Count__c: parseInt(countRes.count)
            })
          });
          stats.groupsUpdated++;

          // Generate and Attach CSV
          const rows = await sql`
            SELECT zip, city, state, county, dma, max_bid, campaign_status, lead_buy_campaign
            FROM single_coverage_zips_raw
            WHERE lead_buyer = ${lead_buyer}
              AND category = ${category}
              AND (CASE 
                WHEN lead_buy_campaign ILIKE '%branded%' OR lead_buy_campaign ILIKE '%microsite%' 
                THEN 'Marketplace - Branded' 
                ELSE 'Marketplace - Standard - HS' 
              END) = ${derived_product}
          `;

          if (rows.length > 0) {
            const header = "zip,city,state,county,dma,max_bid,campaign_status,lead_buy_campaign";
            const csvContent = [
              header,
              ...rows.map(r => 
                [
                  `"${r.zip || ""}"`,
                  `"${r.city || ""}"`,
                  `"${r.state || ""}"`,
                  `"${r.county || ""}"`,
                  `"${r.dma || ""}"`,
                  r.max_bid !== null ? r.max_bid : "",
                  `"${r.campaign_status || ""}"`,
                  `"${r.lead_buy_campaign || ""}"`,
                ].join(",")
              )
            ].join("\n");

            const fileName = `coverage_zips_${category.replace(/[^a-zA-Z0-9]/g,"_")}_${derived_product.replace(/[^a-zA-Z0-9]/g,"_")}.csv`;
            const boundary = '----GitHubActionBoundary' + Math.random().toString(36).substring(2);
            const { accessToken, instanceUrl } = await getSalesforceTokens();

            // Multipart body construction manually
            let body = '';
            body += `--${boundary}\r\n`;
            body += `Content-Disposition: form-data; name="entity_content"\r\n`;
            body += `Content-Type: application/json\r\n\r\n`;
            body += JSON.stringify({
              "Title": `Coverage Zips - ${category}`,
              "PathOnClient": fileName,
              "FirstPublishLocationId": sfGroupId
            }) + '\r\n';
            
            body += `--${boundary}\r\n`;
            body += `Content-Disposition: form-data; name="VersionData"; filename="${fileName}"\r\n`;
            body += `Content-Type: application/octet-stream\r\n\r\n`;
            body += csvContent + '\r\n';
            body += `--${boundary}--`;

            const uploadRes = await fetch(`${instanceUrl}/services/data/${CONFIG.SALESFORCE_API_VERSION}/sobjects/ContentVersion`, {
              method: 'POST',
              headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': `multipart/form-data; boundary=${boundary}`
              },
              body: body
            });

            if (!uploadRes.ok) {
              const errText = await uploadRes.text();
              throw new Error(`File upload failed: ${errText}`);
            }
            stats.csvAttached++;
          }

        } catch (err) {
          console.error(`Error processing group ${lead_buyer} | ${derived_product} | ${category}:`, err);
          stats.errors++;
          stats.details.push(`Group Error (${lead_buyer}): ${err.message}`);
        }
      }

      console.log("Salesforce sync complete.");

      // 3c. Location Enrichment
      console.log("Enriching locations...");
      await sql`
        UPDATE single_coverage_zips_raw r
        SET state = z.state, county = z.county, country = 'US'
        FROM zip_code_lookup z
        WHERE r.zip = z.zip AND r.state IS NULL
      `;

      // 3d. Recalculation
      console.log("Recalculating stats...");
      const categories = job.affected_categories || [];
      if (categories.length > 0) {
        await sql`
          UPDATE single_coverage_zips_raw r
          SET num_lead_buyers = sub.cnt
          FROM (
            SELECT category, zip, COUNT(DISTINCT lead_buyer) as cnt
            FROM single_coverage_zips_raw
            WHERE category = ANY(${categories})
            GROUP BY category, zip
          ) sub
          WHERE r.category = sub.category AND r.zip = sub.zip
            AND r.category = ANY(${categories})
        `;

        await sql`
          UPDATE single_coverage_zips_raw r
          SET num_lead_buy_campaigns = sub.cnt
          FROM (
            SELECT category, zip, COUNT(DISTINCT lead_buy_campaign) as cnt
            FROM single_coverage_zips_raw
            WHERE category = ANY(${categories})
            GROUP BY category, zip
          ) sub
          WHERE r.category = sub.category AND r.zip = sub.zip
            AND r.category = ANY(${categories})
        `;
      }

      // 3e. Clear Cache & Refresh View
      console.log("Clearing cache and refreshing views...");
      await sql`DELETE FROM cache_entries`;
      
      try {
        await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY single_buyer_coverage_summary`;
      } catch (e) {
        console.warn("Concurrent refresh failed, trying standard...", e.message);
        await sql`REFRESH MATERIALIZED VIEW single_buyer_coverage_summary`;
      }

      // 3f. Cleanup Storage
      console.log("Cleaning up storage chunks...");
      const numChunks = job.chunk_count || 1;
      for (let i = 0; i < numChunks; i++) {
        const chunkPath = `${job.storage_path}_chunk_${i}.json`;
        try {
          await deleteChunk(chunkPath);
        } catch (e) {
          console.warn(`Failed to delete chunk ${i}: ${e.message}`);
        }
      }

      // 3g. Complete
      await sql`
        UPDATE csv_upload_jobs
        SET status = 'completed', sync_stats = ${JSON.stringify(stats)}, updated_at = NOW()
        WHERE id = ${job.id}
      `;
      
      console.log("Job completed successfully!");
    }

    } catch (error) {
    console.error("FATAL ERROR:", error);
    
    // Attempt to mark job as failed if we can identify which job we were processing
    try {
      const jobId = process.argv[2];
      if (jobId) {
        await sql`
          UPDATE csv_upload_jobs 
          SET status = 'failed', error_message = ${error.message || 'Unknown error'}, updated_at = NOW()
          WHERE id = ${jobId} AND status NOT IN ('completed', 'failed')
        `;
        console.log(`Marked job ${jobId} as failed.`);
      } else {
        // Try to find the job we were working on (the oldest processing one)
        const [activeJob] = await sql`
          SELECT id FROM csv_upload_jobs 
          WHERE status IN ('processing_upsert', 'processing_finalize')
          ORDER BY updated_at DESC LIMIT 1
        `;
        if (activeJob) {
          await sql`
            UPDATE csv_upload_jobs 
            SET status = 'failed', error_message = ${error.message || 'Unknown error'}, updated_at = NOW()
            WHERE id = ${activeJob.id}
          `;
          console.log(`Marked job ${activeJob.id} as failed.`);
        }
      }
    } catch (markError) {
      console.error("Failed to mark job as failed:", markError);
    }
    
    process.exit(1);
  } finally {
    await sql.end();
  }
}

main();
