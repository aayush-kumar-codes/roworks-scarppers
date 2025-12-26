import dotenv from "dotenv"
dotenv.config()
import fs from "fs/promises";

/* ================================
   CONFIG (FREE PLAN SAFE)
================================ */

const FIRECRAWL_API_KEY = "fc-820afc0472c0428caa6b03c4f9f95105";
if (!FIRECRAWL_API_KEY) {
    throw new Error("FIRECRAWL_API_KEY is not set");
} 

const CATEGORY_URLS = [
  "https://www.radwell.eu/uk/automation-and-control-systems.html",
  "https://www.radwell.eu/uk/circuit-breakers-fuses-protection.html",
  "https://www.radwell.eu/uk/electrical-and-power-systems.html",
  "https://www.radwell.eu/uk/heating-and-cooling-solutions.html",
  "https://www.radwell.eu/uk/plcs-hmis.html",
  "https://www.radwell.eu/uk/power-transmission-components.html",
  "https://www.radwell.eu/uk/switches.html",
  "https://www.radwell.eu/uk/automation-and-control-systems/specialized-control-systems.html",
  "https://www.radwell.eu/automation-and-control-systems/robotic-systems-and-accessories.html",
  "https://www.radwell.eu/uk/data-communications.html",
  "https://www.radwell.eu/uk/electronic-parts-and-devices.html",
  "https://www.radwell.eu/uk/pneumatic-and-hydraulic-solutions.html",
  "https://www.radwell.eu/uk/sensors.html",
];

// HARD LIMITS (FREE PLAN)
const MAX_PAGES_PER_URL = 5;
const RATE_LIMIT_DELAY_MS = 6500; // ~9 req/min (safe)
const OUTPUT_FILE = "./radwell-products-merged.json";

/* ================================
   HELPERS
================================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchMarkdown(url) {
  const response = await fetch("https://api.firecrawl.dev/v2/scrape", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${FIRECRAWL_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      url,
      formats: ["markdown"],
      onlyMainContent: false,
      maxAge: 172800000,
    }),
  });

  if (response.status === 429) {
    throw new Error("Rate limit hit (429)");
  }

  const data = await response.json();

  if (!data.success) {
    throw new Error("Firecrawl scrape failed");
  }

  return data.data.markdown;
}

/* ================================
   MARKDOWN NORMALIZER
================================ */

function normalizeRadwellMarkdown(markdown, categoryUrl, page) {
  const products = [];

  // Each product starts with: - [![IMAGE](...)
  const blocks = markdown.split(/\n- \[\!\[/).slice(1);

  for (const raw of blocks) {
    const block = "- [![" + raw;

    const imageMatch = block.match(
      /\((https:\/\/cdn\.radwell\.eu\/[^\s)]+)\)/
    );
    const productUrlMatch = block.match(
      /\]\((https?:\/\/[^\s)]+)\s+"/
    );
    const codeMatch = block.match(/\*\*\(([^)]+)\)\*\*/);
    const manufacturerMatch = block.match(/\*\*\*\*([^*]+)\*\*\*\*/);
    const descriptionMatch = block.match(/\*\*(?!\()(.*?)\*\*/);
    const priceExVatMatch = block.match(/From:\s*£([\d,\.]+)/i);
    const priceIncVatMatch = block.match(/Price with VAT:\s*£([\d,\.]+)/i);
    const stockMatch = block.match(/In Stock!/i);

    if (!productUrlMatch) continue;

    products.push({
      code: codeMatch ? `(${codeMatch[1].trim()})` : null,
      manufacturer: manufacturerMatch
        ? manufacturerMatch[1].trim()
        : null,
      description: descriptionMatch
        ? descriptionMatch[1].trim()
        : null,
      price_ex_vat: priceExVatMatch
        ? `£${priceExVatMatch[1]}`
        : null,
      price_inc_vat: priceIncVatMatch
        ? `£${priceIncVatMatch[1]}`
        : null,
      stock: stockMatch ? "In Stock" : "Unknown",
      product_url: productUrlMatch[1],
      image_url: imageMatch ? imageMatch[1] : null,
      category_url: categoryUrl,
      page,
    });
  }

  return products;
}

/* ================================
   MAIN SCRAPER (SEQUENTIAL)
================================ */

async function run() {
  console.log("🚀 Firecrawl Free-Plan Scraper Started\n");

  let allProducts = [];

  for (const categoryUrl of CATEGORY_URLS) {
    console.log(`📂 CATEGORY: ${categoryUrl}`);

    for (let page = 1; page <= MAX_PAGES_PER_URL; page++) {
      const pageUrl = `${categoryUrl}?p=${page}`;
      console.log(`  🌐 Page ${page}/${MAX_PAGES_PER_URL}`);

      try {
        const markdown = await fetchMarkdown(pageUrl);
        const products = normalizeRadwellMarkdown(
          markdown,
          categoryUrl,
          page
        );

        if (products.length === 0) {
          console.log(
            `  🛑 No products on page ${page}. Stop pagination for this category.`
          );
          break;
        }

        console.log(`  ✅ Products scraped: ${products.length}`);
        allProducts.push(...products);
      } catch (err) {
        console.error(
          `  ❌ Error on page ${page}: ${err.message}`
        );
        break;
      }

      console.log(
        `  ⏳ Waiting ${RATE_LIMIT_DELAY_MS / 1000}s (rate-limit safe)`
      );
      await sleep(RATE_LIMIT_DELAY_MS);
    }

    console.log(""); // spacing between categories
  }

  console.log("💾 Writing merged JSON file...");
  await fs.writeFile(
    OUTPUT_FILE,
    JSON.stringify(allProducts, null, 2),
    "utf-8"
  );

  console.log("🎉 Scraping completed successfully!");
  console.log(`📦 Total products: ${allProducts.length}`);
  console.log(`📄 Output file: ${OUTPUT_FILE}`);
}

/* ================================
   RUN
================================ */

run().catch((err) => {
  console.error("❌ Fatal error:", err);
});
