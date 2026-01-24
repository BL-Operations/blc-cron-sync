import postgres from "postgres";

// =============================================================================
// CONFIGURATION
// =============================================================================

const config = {
  tableau: {
    serverUrl: process.env.TABLEAU_SERVER_URL!,
    siteId: process.env.TABLEAU_SITE_ID!,
    patName: process.env.TABLEAU_PAT_NAME!,
    patSecret: process.env.TABLEAU_PAT_SECRET!,
    viewId: process.env.TABLEAU_VIEW_SBC_ID!,
    apiVersion: "3.20",
  },
  db: {
    connectionString: process.env.SUPABASE_DATABASE_URL!,
  },
  batchSize: 1000,
};

// Validate config
for (const [key, value] of Object.entries(config.tableau)) {
  if (!value) throw new Error(`Missing env var for tableau.${key}`);
}
if (!config.db.connectionString) throw new Error("Missing SUPABASE_DATABASE_URL");

// =============================================================================
// TABLEAU API
// =============================================================================

type TableauAuth = { token: string; siteId: string };

async function authenticateTableau(): Promise<TableauAuth> {
  const url = `${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/auth/signin`;
  
  console.log(`🔐 Authenticating to Tableau...`);
  
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify({
      credentials: {
        personalAccessTokenName: config.tableau.patName,
        personalAccessTokenSecret: config.tableau.patSecret,
        site: { contentUrl: config.tableau.siteId },
      },
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Tableau auth failed: ${response.status} - ${error}`);
  }

  const data = await response.json();
  console.log(`✅ Authenticated successfully`);
  
  return {
    token: data.credentials.token,
    siteId: data.credentials.site.id,
  };
}

async function signOutTableau(token: string): Promise<void> {
  try {
    await fetch(`${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/auth/signout`, {
      method: "POST",
      headers: { "X-Tableau-Auth": token },
    });
    console.log(`🚪 Signed out from Tableau`);
  } catch (e) {
    console.warn("Sign out warning:", e);
  }
}

async function queryViewData(auth: TableauAuth, viewId: string): Promise<string> {
  const url = `${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/sites/${auth.siteId}/views/${viewId}/data`;

  console.log(`⬇️ Downloading all data from view...`);
  
  const response = await fetch(url, {
    method: "GET",
    headers: { "X-Tableau-Auth": auth.token },
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`View query failed: ${response.status} - ${error}`);
  }

  const csv = await response.text();
  console.log(`✅ Downloaded ${csv.length.toLocaleString()} characters`);
  
  // Strip leading blank lines
  const lines = csv.split(/\r?\n/);
  const firstNonEmpty = lines.findIndex(line => line.trim() !== "");
  
  if (firstNonEmpty === -1) return "";
  
  return lines.slice(firstNonEmpty).join("\n");
}

// =============================================================================
// CSV PARSING
// =============================================================================

function parseCSVLine(text: string): string[] {
  const result: string[] = [];
  let cur = "";
  let inQuote = false;
  
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (inQuote) {
      if (char === '"') {
        if (i + 1 < text.length && text[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuote = false;
        }
      } else {
        cur += char;
      }
    } else {
      if (char === '"') {
        inQuote = true;
      } else if (char === ',') {
        result.push(cur);
        cur = "";
      } else {
        cur += char;
      }
    }
  }
  result.push(cur);
  return result;
}

function parseNumeric(value: string | undefined): number | null {
  if (!value) return null;
  const cleaned = value.replace(/[,$%]/g, "").trim();
  if (cleaned === "" || cleaned.toLowerCase() === "null") return null;
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

// =============================================================================
// DATA TRANSFORMATION
// =============================================================================

const MEASURE_MAP: Record<string, string> = {
  "uec": "u_ec",
  "rec": "r_ec",
  "mec": "m_ec",
  "rleads": "r_leads",
  "uleads": "u_leads",
  "legs": "legs",
  "clicks lmp": "clicks_lmp",
  "cmp bid": "cmp_bid",
};

function mapMeasureName(name: string): string | null {
  const normalized = name.trim().toLowerCase();
  
  if (MEASURE_MAP[normalized]) return MEASURE_MAP[normalized];
  if (normalized.includes("lead") && normalized.includes("rev")) return "lead_rev_scrubbed";
  if (normalized.includes("click") && normalized.includes("rev")) return "click_rev";
  if (normalized.includes("cmp") && normalized.includes("bid") && normalized.includes("mec")) return "cmp_bid_per_mec";
  if (normalized.includes("conv") && normalized.includes("%")) return "conv_percent";
  if (normalized.includes("price")) return "price";
  
  return null;
}

type Record = {
  buyer_type: string | null;
  category: string;
  subcategory: string | null;
  channel: string | null;
  zip: string;
  u_ec: number | null;
  r_ec: number | null;
  m_ec: number | null;
  r_leads: number | null;
  u_leads: number | null;
  legs: number | null;
  lead_rev_scrubbed: number | null;
  click_rev: number | null;
  cmp_bid: number | null;
  cmp_bid_per_mec: number | null;
  conv_percent: number | null;
  price: number | null;
  clicks_lmp: number | null;
};

function pivotCSVData(csv: string): Record[] {
  const lines = csv.split(/\r?\n/);
  if (lines.length < 2) return [];
  
  const headers = parseCSVLine(lines[0]).map(h => h.trim());
  const lowerHeaders = headers.map(h => h.toLowerCase());
  
  console.log(`📊 CSV has ${(lines.length - 1).toLocaleString()} rows, ${headers.length} columns`);
  console.log(`   Headers: ${headers.slice(0, 8).join(", ")}${headers.length > 8 ? "..." : ""}`);
  
  // Find column indices
  const idx = {
    buyerType: lowerHeaders.findIndex(h => h.includes("buyer") && h.includes("type")),
    category: lowerHeaders.findIndex(h => h.includes("category") && !h.includes("sub")),
    subcategory: lowerHeaders.findIndex(h => h.includes("subcategory")),
    channel: lowerHeaders.findIndex(h => h === "channel"),
    zip: lowerHeaders.findIndex(h => h === "zip"),
    measureNames: lowerHeaders.findIndex(h => h.includes("measure") && h.includes("name")),
    measureValues: lowerHeaders.findIndex(h => h.includes("measure") && h.includes("value")),
  };
  
  console.log(`   Column indices: zip=${idx.zip}, category=${idx.category}, measureNames=${idx.measureNames}, measureValues=${idx.measureValues}`);
  
  // Check for required columns
  if (idx.zip === -1) {
    console.log(`   ❌ Missing 'zip' column. Available: ${headers.join(", ")}`);
    return [];
  }
  
  if (idx.measureNames === -1 || idx.measureValues === -1) {
    console.log(`   ❌ Missing Measure Names/Values columns`);
    return [];
  }
  
  if (idx.category === -1) {
    console.log(`   ⚠️ Missing 'category' column - will use 'Unknown'`);
  }
  
  // Group by composite key
  const grouped = new Map<string, { base: Partial<Record>; measures: Map<string, number> }>();
  
  let processedRows = 0;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const row = parseCSVLine(line);
    const zip = row[idx.zip]?.trim();
    
    if (!zip) continue;
    
    const category = idx.category !== -1 ? row[idx.category]?.trim() || "Unknown" : "Unknown";
    const buyerType = idx.buyerType !== -1 ? row[idx.buyerType]?.trim() || null : null;
    const subcategory = idx.subcategory !== -1 ? row[idx.subcategory]?.trim() || null : null;
    const channel = idx.channel !== -1 ? row[idx.channel]?.trim() || null : null;
    
    const key = `${buyerType}|${category}|${subcategory}|${channel}|${zip}`;
    
    if (!grouped.has(key)) {
      grouped.set(key, {
        base: { buyer_type: buyerType, category, subcategory, channel, zip },
        measures: new Map(),
      });
    }
    
    const measureName = row[idx.measureNames]?.trim();
    const measureValue = row[idx.measureValues]?.trim();
    
    if (measureName && measureValue) {
      const col = mapMeasureName(measureName);
      if (col) {
        const num = parseNumeric(measureValue);
        if (num !== null) {
          grouped.get(key)!.measures.set(col, num);
        }
      }
    }
    
    processedRows++;
    if (processedRows % 100000 === 0) {
      console.log(`   Processed ${processedRows.toLocaleString()} rows...`);
    }
  }
  
  console.log(`   Processed ${processedRows.toLocaleString()} total rows`);
  console.log(`   Grouped into ${grouped.size.toLocaleString()} unique records`);
  
  // Convert to flat records
  return Array.from(grouped.values()).map(({ base, measures }) => ({
    buyer_type: base.buyer_type ?? null,
    category: base.category!,
    subcategory: base.subcategory ?? null,
    channel: base.channel ?? null,
    zip: base.zip!,
    u_ec: measures.get("u_ec") ?? null,
    r_ec: measures.get("r_ec") ?? null,
    m_ec: measures.get("m_ec") ?? null,
    r_leads: measures.get("r_leads") ?? null,
    u_leads: measures.get("u_leads") ?? null,
    legs: measures.get("legs") ?? null,
    lead_rev_scrubbed: measures.get("lead_rev_scrubbed") ?? null,
    click_rev: measures.get("click_rev") ?? null,
    cmp_bid: measures.get("cmp_bid") ?? null,
    cmp_bid_per_mec: measures.get("cmp_bid_per_mec") ?? null,
    conv_percent: measures.get("conv_percent") ?? null,
    price: measures.get("price") ?? null,
    clicks_lmp: measures.get("clicks_lmp") ?? null,
  }));
}

// =============================================================================
// DATABASE
// =============================================================================

async function upsertRecords(sql: postgres.Sql, records: Record[]): Promise<number> {
  if (records.length === 0) return 0;
  
  console.log(`💾 Upserting ${records.length.toLocaleString()} records in batches of ${config.batchSize}...`);
  
  let upserted = 0;
  
  for (let i = 0; i < records.length; i += config.batchSize) {
    const batch = records.slice(i, i + config.batchSize);
    
    await sql`
      INSERT INTO single_coverage_zips_raw ${sql(batch, 
        'buyer_type', 'category', 'subcategory', 'channel', 'zip',
        'u_ec', 'r_ec', 'm_ec', 'r_leads', 'u_leads', 'legs',
        'lead_rev_scrubbed', 'click_rev', 'cmp_bid', 'cmp_bid_per_mec',
        'conv_percent', 'price', 'clicks_lmp'
      )}
      ON CONFLICT (buyer_type, category, subcategory, channel, zip)
      DO UPDATE SET
        u_ec = EXCLUDED.u_ec,
        r_ec = EXCLUDED.r_ec,
        m_ec = EXCLUDED.m_ec,
        r_leads = EXCLUDED.r_leads,
        u_leads = EXCLUDED.u_leads,
        legs = EXCLUDED.legs,
        lead_rev_scrubbed = EXCLUDED.lead_rev_scrubbed,
        click_rev = EXCLUDED.click_rev,
        cmp_bid = EXCLUDED.cmp_bid,
        cmp_bid_per_mec = EXCLUDED.cmp_bid_per_mec,
        conv_percent = EXCLUDED.conv_percent,
        price = EXCLUDED.price,
        clicks_lmp = EXCLUDED.clicks_lmp,
        updated_at = NOW()
    `;
    
    upserted += batch.length;
    
    if (upserted % 10000 === 0) {
      console.log(`   Upserted ${upserted.toLocaleString()} records...`);
    }
  }
  
  return upserted;
}

// =============================================================================
// MAIN
// =============================================================================

async function main() {
  console.log("🚀 Starting Single Coverage Zips extraction");
  console.log(`   View ID: ${config.tableau.viewId}`);
  
  const sql = postgres(config.db.connectionString);
  let auth: TableauAuth | null = null;
  
  try {
    // 1. Authenticate
    auth = await authenticateTableau();
    
    // 2. Download ALL data (no category filter)
    const csv = await queryViewData(auth, config.tableau.viewId);
    
    if (!csv || csv.length < 100) {
      console.log("⚠️ Empty or minimal response from Tableau. Exiting.");
      return;
    }
    
    // 3. Pivot the data
    console.log(`\n🔄 Pivoting data...`);
    const records = pivotCSVData(csv);
    
    if (records.length === 0) {
      console.log("⚠️ No records after pivot. Exiting.");
      return;
    }
    
    // 4. Upsert to database
    console.log(`\n💾 Writing to database...`);
    const count = await upsertRecords(sql, records);
    
    console.log(`\n${"=".repeat(50)}`);
    console.log(`🎉 Extraction complete!`);
    console.log(`   Total records upserted: ${count.toLocaleString()}`);
    console.log(`${"=".repeat(50)}`);
    
  } finally {
    // Cleanup
    if (auth) await signOutTableau(auth.token);
    await sql.end();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
