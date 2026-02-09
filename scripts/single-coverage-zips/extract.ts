import postgres from "postgres";

// =======================================================================
// CONFIGURATION
// =======================================================================
const BASE_URL = process.env.BASE_URL!;
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
if (!BASE_URL) throw new Error("Missing BASE_URL");

// =======================================================================
// TABLEAU API
// =======================================================================
type TableauAuth = { token: string; siteId: string };

async function authenticateTableau(): Promise<TableauAuth> {
  const url = `${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/auth/signin`;
  console.log("🔐 Authenticating to Tableau...");

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
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
  console.log("✅ Authenticated successfully");

  return {
    token: data.credentials.token,
    siteId: data.credentials.site.id,
  };
}

async function signOutTableau(token: string): Promise<void> {
  try {
    await fetch(
      `${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/auth/signout`,
      {
        method: "POST",
        headers: { "X-Tableau-Auth": token },
      }
    );
    console.log("🚪 Signed out from Tableau");
  } catch (e) {
    console.warn("Sign out warning:", e);
  }
}

async function queryViewData(auth: TableauAuth, viewId: string): Promise<string> {
  const url = `${config.tableau.serverUrl}/api/${config.tableau.apiVersion}/sites/${auth.siteId}/views/${viewId}/data`;
  console.log("⬇ Downloading all data from view...");

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

  const lines = csv.split(/\r?\n/);
  const firstNonEmpty = lines.findIndex((line) => line.trim() !== "");
  if (firstNonEmpty === -1) return "";
  return lines.slice(firstNonEmpty).join("\n");
}

// =======================================================================
// CSV PARSING
// =======================================================================
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
      } else if (char === ",") {
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
  return Number.isNaN(num) ? null : num;
}

// =======================================================================
// STATE ABBREVIATIONS
// =======================================================================
const STATE_NAME_TO_ABBREV: Record<string, string> = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR", california: "CA",
  colorado: "CO", connecticut: "CT", delaware: "DE", florida: "FL", georgia: "GA",
  hawaii: "HI", idaho: "ID", illinois: "IL", indiana: "IN", iowa: "IA",
  kansas: "KS", kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD",
  massachusetts: "MA", michigan: "MI", minnesota: "MN", mississippi: "MS", missouri: "MO",
  montana: "MT", nebraska: "NE", nevada: "NV", "new hampshire": "NH", "new jersey": "NJ",
  "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND",
  ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA", "rhode island": "RI",
  "south carolina": "SC", "south dakota": "SD", tennessee: "TN", texas: "TX", utah: "UT",
  vermont: "VT", virginia: "VA", washington: "WA", "west virginia": "WV",
  wisconsin: "WI", wyoming: "WY", "district of columbia": "DC",
};

function normalizeState(state: string | null): string | null {
  if (!state) return null;
  const trimmed = state.trim();
  if (trimmed.length === 2) return trimmed.toUpperCase();
  const abbrev = STATE_NAME_TO_ABBREV[trimmed.toLowerCase()];
  return abbrev || trimmed.substring(0, 2).toUpperCase();
}

// =======================================================================
// DATA TRANSFORMATION (Flat view: Campaign Coverage List for Jonathan)
// =======================================================================
type CoverageZipRecord = {
  lead_buyer: string;
  lead_buy_campaign: string | null;
  category: string;
  zip: string;
  city: string | null;
  country: string | null;
  county: string | null;
  dma: string | null;
  state: string | null;
  vertical: string | null;
  max_bid: number | null;
  num_lead_buyers: number | null;
  num_lead_buy_campaigns: number | null;
};

function parseCSVData(csv: string): CoverageZipRecord[] {
  const lines = csv.split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]).map((h) => h.trim());
  const lowerHeaders = headers.map((h) => h.toLowerCase());

  console.log(
    `📊 CSV has ${(lines.length - 1).toLocaleString()} rows, ${headers.length} columns`
  );
  console.log(`   Headers: ${headers.join(", ")}`);

  // Map columns to the flat "Campaign Coverage List for Jonathan" view
  const idx = {
    buyerAccount: lowerHeaders.findIndex(
      (h) => h.includes("buyer") && h.includes("account")
    ),
    category: lowerHeaders.findIndex(
      (h) => h === "category" || (h.includes("category") && !h.includes("sub"))
    ),
    campaign: lowerHeaders.findIndex((h) => h === "campaign"),
    county: lowerHeaders.findIndex((h) => h === "county"),
    state: lowerHeaders.findIndex((h) => h === "state"),
    zip: lowerHeaders.findIndex((h) => h === "zip"),
    city: lowerHeaders.findIndex((h) => h === "city"),
    country: lowerHeaders.findIndex((h) => h === "country"),
    dma: lowerHeaders.findIndex((h) => h === "dma"),
    vertical: lowerHeaders.findIndex((h) => h === "vertical"),
    numBuyers: lowerHeaders.findIndex(
      (h) => h.includes("#") && h.includes("buyer")
    ),
    campaignBid: lowerHeaders.findIndex(
      (h) => h.includes("campaign") && h.includes("bid")
    ),
  };

  console.log(`   Column indices:`, JSON.stringify(idx));

  // Validate required columns
  if (idx.zip === -1) {
    console.log(
      `   ❌ Missing 'Zip' column. Available: ${headers.join(", ")}`
    );
    return [];
  }
  if (idx.buyerAccount === -1) {
    console.log(
      `   ❌ Missing 'Buyer Account' column. Available: ${headers.join(", ")}`
    );
    return [];
  }
  if (idx.category === -1) {
    console.log(
      `   ❌ Missing 'Category' column. Available: ${headers.join(", ")}`
    );
    return [];
  }

  // Group by composite key: (lead_buyer, lead_buy_campaign, category, zip)
  const grouped = new Map<
    string,
    { base: Partial<CoverageZipRecord>; maxBid: number | null }
  >();

  let processedRows = 0;
  let skippedRows = 0;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const row = parseCSVLine(line);

    const zip = row[idx.zip]?.trim();
    if (!zip) {
      skippedRows++;
      continue;
    }

    const leadBuyer = row[idx.buyerAccount]?.trim() || "Unknown";
    const category = row[idx.category]?.trim() || "Unknown";
    const leadBuyCampaign =
      idx.campaign !== -1 ? row[idx.campaign]?.trim() || null : null;
    const county = idx.county !== -1 ? row[idx.county]?.trim() || null : null;
    const state = idx.state !== -1 ? normalizeState(row[idx.state]) : null;
    const city = idx.city !== -1 ? row[idx.city]?.trim() || null : null;
    const country =
      idx.country !== -1 ? row[idx.country]?.trim() || null : null;
    const dma = idx.dma !== -1 ? row[idx.dma]?.trim() || null : null;
    const vertical =
      idx.vertical !== -1 ? row[idx.vertical]?.trim() || null : null;
    const numBuyers =
      idx.numBuyers !== -1 ? parseNumeric(row[idx.numBuyers]) : null;
    const campaignBid =
      idx.campaignBid !== -1 ? parseNumeric(row[idx.campaignBid]) : null;

    const key = `${leadBuyer}|${leadBuyCampaign || ""}|${category}|${zip}`;

    if (!grouped.has(key)) {
      grouped.set(key, {
        base: {
          lead_buyer: leadBuyer,
          lead_buy_campaign: leadBuyCampaign,
          category,
          zip,
          city,
          country,
          county,
          dma,
          state,
          vertical,
          num_lead_buyers:
            numBuyers !== null ? Math.round(numBuyers) : null,
          num_lead_buy_campaigns: null,
        },
        maxBid: campaignBid,
      });
    } else {
      // If duplicate key, keep the highest bid
      const existing = grouped.get(key)!;
      if (
        campaignBid !== null &&
        (existing.maxBid === null || campaignBid > existing.maxBid)
      ) {
        existing.maxBid = campaignBid;
      }
      // Keep the highest num_lead_buyers
      if (numBuyers !== null) {
        const existingNum = existing.base.num_lead_buyers;
        if (existingNum === null || Math.round(numBuyers) > existingNum) {
          existing.base.num_lead_buyers = Math.round(numBuyers);
        }
      }
    }

    processedRows++;
    if (processedRows % 100000 === 0) {
      console.log(`   Processed ${processedRows.toLocaleString()} rows...`);
    }
  }

  console.log(
    `   Processed ${processedRows.toLocaleString()} total rows, skipped ${skippedRows}`
  );
  console.log(
    `   Grouped into ${grouped.size.toLocaleString()} unique records`
  );

  // Convert to flat records
  return Array.from(grouped.values()).map(({ base, maxBid }) => ({
    lead_buyer: base.lead_buyer!,
    lead_buy_campaign: base.lead_buy_campaign ?? null,
    category: base.category!,
    zip: base.zip!,
    city: base.city ?? null,
    country: base.country ?? null,
    county: base.county ?? null,
    dma: base.dma ?? null,
    state: base.state ?? null,
    vertical: base.vertical ?? null,
    max_bid: maxBid,
    num_lead_buyers: base.num_lead_buyers ?? null,
    num_lead_buy_campaigns: base.num_lead_buy_campaigns ?? null,
  }));
}

// =======================================================================
// DATABASE
// =======================================================================
async function upsertRecords(
  sql: postgres.Sql,
  records: CoverageZipRecord[]
): Promise<number> {
  if (records.length === 0) return 0;

  console.log(
    `💾 Upserting ${records.length.toLocaleString()} records in batches of ${config.batchSize}...`
  );

  let upserted = 0;
  for (let i = 0; i < records.length; i += config.batchSize) {
    const batch = records.slice(i, i + config.batchSize);

    await sql`
      INSERT INTO single_coverage_zips_raw ${sql(
        batch,
        "lead_buyer",
        "lead_buy_campaign",
        "category",
        "zip",
        "city",
        "country",
        "county",
        "dma",
        "state",
        "vertical",
        "max_bid",
        "num_lead_buyers",
        "num_lead_buy_campaigns"
      )}
      ON CONFLICT (lead_buyer, lead_buy_campaign, category, zip)
      DO UPDATE SET
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        county = EXCLUDED.county,
        dma = EXCLUDED.dma,
        state = EXCLUDED.state,
        vertical = EXCLUDED.vertical,
        max_bid = EXCLUDED.max_bid,
        num_lead_buyers = EXCLUDED.num_lead_buyers,
        num_lead_buy_campaigns = EXCLUDED.num_lead_buy_campaigns,
        updated_at = NOW()
    `;

    upserted += batch.length;
    if (upserted % 10000 === 0) {
      console.log(`   Upserted ${upserted.toLocaleString()} records...`);
    }
  }

  return upserted;
}

// =======================================================================
// MAIN
// =======================================================================
async function main() {
  console.log("🚀 Starting Single Coverage Zips extraction");
  console.log(`   View ID: ${config.tableau.viewId}`);

  const sql = postgres(config.db.connectionString);
  let auth: TableauAuth | null = null;

  try {
    auth = await authenticateTableau();

    const csv = await queryViewData(auth, config.tableau.viewId);
    if (!csv || csv.length < 100) {
      console.log("⚠ Empty or minimal response from Tableau. Exiting.");
      return;
    }

    console.log("\n🔄 Parsing flat CSV data...");
    const records = parseCSVData(csv);
    if (records.length === 0) {
      console.log("⚠ No records after parsing. Exiting.");
      return;
    }

    // Log a summary of what we found
    const categories = new Set(records.map((r) => r.category));
    const buyers = new Set(records.map((r) => r.lead_buyer));
    console.log(`   Categories found (${categories.size}): ${[...categories].join(", ")}`);
    console.log(`   Unique buyers: ${buyers.size}`);
    console.log(
      `   Records with num_lead_buyers: ${records.filter((r) => r.num_lead_buyers !== null).length}`
    );
    console.log(
      `   Records with max_bid: ${records.filter((r) => r.max_bid !== null).length}`
    );
    console.log(
      `   Records with campaign: ${records.filter((r) => r.lead_buy_campaign !== null).length}`
    );

    // Truncate old data from the previous (wrong) view before inserting
    console.log("\n🗑 Truncating old data from single_coverage_zips_raw...");
    await sql`TRUNCATE TABLE single_coverage_zips_raw RESTART IDENTITY`;
    console.log("   Truncated.");

    console.log("\n💾 Writing to database...");
    const count = await upsertRecords(sql, records);

    console.log(`\n${"=".repeat(50)}`);
    console.log("🎉 Extraction complete!");
    console.log(`   Total records upserted: ${count.toLocaleString()}`);
    console.log(`${"=".repeat(50)}`);

    // Refresh the materialized view so the report picks up new data
    console.log("\n📊 Refreshing materialized view...");
    await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY single_buyer_coverage_summary`;
    console.log("   Materialized view refreshed.");

    // Trigger ZIP code enrichment via Floot endpoint
    console.log("\n🌍 Triggering ZIP code enrichment...");
    const enrichResponse = await fetch(
      `${BASE_URL}/_api/single-coverage-zips/enrich`,
      {
        method: "POST",
        headers: {
          "X-API-Key": process.env.EXTRACTION_API_KEY!,
          "Content-Type": "application/json",
        },
      }
    );

    if (!enrichResponse.ok) {
      const errorText = await enrichResponse.text();
      console.error("ZIP enrichment failed:", errorText);
    } else {
      const enrichResult = await enrichResponse.json();
      console.log("✅ ZIP enrichment complete:", enrichResult);
    }
  } finally {
    if (auth) await signOutTableau(auth.token);
    await sql.end();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
