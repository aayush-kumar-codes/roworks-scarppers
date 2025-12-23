import puppeteer from "puppeteer";
import * as cheerio from "cheerio";
import fs from "fs-extra";
import path from "path";

const OUTPUT_PATH = path.join("data", "product-catalog.json");

const log = {
  info: (msg) => console.log(`\x1b[36m[INFO]\x1b[0m ${msg}`),
  success: (msg) => console.log(`\x1b[32m[SUCCESS]\x1b[0m ${msg}`),
  warn: (msg) => console.warn(`\x1b[33m[WARN]\x1b[0m ${msg}`),
  error: (msg) => console.error(`\x1b[31m[ERROR]\x1b[0m ${msg}`),
};

export async function scrapeCatalog() {
  log.info("Launching Puppeteer browser...");

  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();

  await page.setViewport({ width: 1440, height: 900 });

  log.info("Navigating to Siemens Product Catalog...");
  await page.goto(
    "https://www.sw.siemens.com/en-US/product-catalog/",
    { waitUntil: "networkidle2", timeout: 0 }
  );

  log.info("Waiting for product cards to load...");
  await page.waitForSelector(".ecommerce-product-card", { timeout: 0 });

  log.success("Product cards loaded");

  const html = await page.content();
  log.info("HTML content fetched, loading into Cheerio...");

  const $ = cheerio.load(html);
  const catalog = [];

  log.info("Scanning product sections...");

  $('div[id]').each((index, section) => {
    const categoryName = $(section)
      .find(".product-catalog-technology-header")
      .first()
      .text()
      .trim();

    if (!categoryName) return;

    log.success(`Found section: ${categoryName}`);

    const products = $(section).find(".ecommerce-product-card");

    log.info(`Products found in "${categoryName}": ${products.length}`);

    const productList = [];

    products.each((i, card) => {
      const name = $(card)
        .find(".ecommerce-product-body-name")
        .text()
        .trim();

      if (!name) {
        log.warn(`Unnamed product found in ${categoryName}`);
        return;
      }

      log.info(`Scraping product: ${name}`);

      const description = $(card)
        .find(".ecommerce-product-body-short-description")
        .text()
        .replace(/\s+/g, " ")
        .trim();

      const price = $(card)
        .find(".ecommerce-product-body-price-main-div")
        .text()
        .replace(/\s+/g, " ")
        .trim();

      const priceUnit = $(card)
        .find(".ecommerce-product-body-price-bottom-text")
        .text()
        .trim();

      const subscriptionOptions = [];
      $(card)
        .find("disw-product-term-selector option")
        .each((_, opt) => {
          subscriptionOptions.push($(opt).text().trim());
        });

      const quantity =
        $(card).find("disw-drop-down").attr("value") || "1";

      const addToCartUrl = $(card)
        .find("a.btn-commerce")
        .attr("href");

      productList.push({
        productName: name,
        description,
        price,
        priceUnit,
        subscriptionOptions,
        defaultQuantity: quantity,
        addToCartUrl,
      });

      log.success(`Scraped: ${name}`);
    });

    catalog.push({
      category: categoryName,
      products: productList,
    });
  });

  log.info("Ensuring data directory exists...");
  await fs.ensureDir("data");

  log.info("Writing data to JSON file...");
  await fs.writeJSON(OUTPUT_PATH, catalog, { spaces: 2 });

  log.success(`Data successfully saved → ${OUTPUT_PATH}`);
  log.success(`Total categories scraped: ${catalog.length}`);

  await browser.close();
  log.success("Browser closed");

  return catalog;
}

scrapeCatalog();