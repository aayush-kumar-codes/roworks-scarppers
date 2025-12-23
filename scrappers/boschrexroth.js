import puppeteer from "puppeteer";
import fs from "fs-extra";
import path from "path";
import https from "https";
import http from "http";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DOWNLOAD_DIR = path.join(__dirname, "downloadedPdf");

const TARGET_URL =
  "https://www.boschrexroth.com/en/gb/search.html?q=ALL&dnavs=DC_mediatype%3Adc_media_type_data_sheet&origin=datasheet_search&s=download";

(async () => {
  await fs.ensureDir(DOWNLOAD_DIR);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
    ],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  console.log("Opening search page...");
  await page.goto(TARGET_URL, {
    waitUntil: "networkidle2",
    timeout: 0,
  });

  await page.waitForSelector("body", { timeout: 60000 });
  await new Promise((r) => setTimeout(r, 3000));

  await autoScroll(page);

  const mediaPageUrls = await page.evaluate(() => {
    const results = [];
    const anchors = document.querySelectorAll('a[href*="media-details"]');

    anchors.forEach(function (a) {
      const href = a.getAttribute("href");
      if (!href) return;

      const text = (a.innerText || "").trim();
      const absoluteUrl = href.startsWith("http")
        ? href
        : "https://www.boschrexroth.com" + href;

      results.push({
        title: text || "Unknown",
        url: absoluteUrl,
      });
    });

    return results;
  });

  const uniqueMediaPages = Array.from(
    new Map(mediaPageUrls.map((m) => [m.url, m])).values()
  );

  console.log(`Found ${uniqueMediaPages.length} PDFs to download\n`);

  let downloadedCount = 0;

  for (let i = 0; i < uniqueMediaPages.length; i++) {
    const mediaPage = uniqueMediaPages[i];
    console.log(
      `[${i + 1}/${uniqueMediaPages.length}] Processing: ${mediaPage.title}`
    );

    const detailPage = await browser.newPage();

    let capturedDownloadUrl = null;

    // Listen for all network responses
    await detailPage.on("response", async (response) => {
      const url = response.url();
      const contentType = response.headers()["content-type"] || "";

      // Capture PDF downloads
      if (
        contentType.includes("application/pdf") ||
        url.includes("download") ||
        url.endsWith(".pdf")
      ) {
        console.log(`   📡 Captured download URL: ${url}`);
        capturedDownloadUrl = url;
      }
    });

    try {
      await detailPage.goto(mediaPage.url, {
        waitUntil: "networkidle2",
        timeout: 30000,
      });

      await new Promise((r) => setTimeout(r, 1500));

      // Click the Download button
      await detailPage.click('button[aria-label="Download"]');

      // Wait for the download to be initiated
      await new Promise((r) => setTimeout(r, 4000));

      if (capturedDownloadUrl) {
        console.log(`   📥 Downloading from: ${capturedDownloadUrl}`);

        const fileName = mediaPage.title.replace(/[\/\\:*?"<>|]/g, "_") + ".pdf";
        const filePath = path.join(DOWNLOAD_DIR, fileName);

        await downloadFile(capturedDownloadUrl, filePath);
        downloadedCount++;
        console.log(`   ✅ Saved: ${fileName}\n`);
      } else {
        console.log(`   ⚠️  No download URL captured\n`);
      }
    } catch (error) {
      console.log(`   ❌ Error: ${error.message}\n`);
    } finally {
      await detailPage.close();
    }

    if (downloadedCount >= 10) {
      console.log("Reached limit (10). Stopping...");
      break;
    }
  }

  console.log(
    `\n✅ Downloaded ${downloadedCount} PDFs to: ${DOWNLOAD_DIR}`
  );

  await browser.close();
})();

function downloadFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith("https") ? https : http;
    const file = fs.createWriteStream(filePath);

    protocol
      .get(url, { headers: { "User-Agent": "Mozilla/5.0" } }, (response) => {
        if (response.statusCode === 301 || response.statusCode === 302) {
          fs.unlinkSync(filePath);
          return downloadFile(response.headers.location, filePath)
            .then(resolve)
            .catch(reject);
        }

        if (response.statusCode !== 200) {
          reject(new Error(`HTTP ${response.statusCode}: ${url}`));
          return;
        }

        response.pipe(file);

        file.on("finish", () => {
          file.close();
          resolve();
        });

        file.on("error", (err) => {
          try {
            fs.unlinkSync(filePath);
          } catch (e) {}
          reject(err);
        });
      })
      .on("error", (err) => {
        try {
          fs.unlinkSync(filePath);
        } catch (e) {}
        reject(err);
      });
  });
}

async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise(function (resolve) {
      let totalHeight = 0;
      const distance = 600;
      const timer = setInterval(function () {
        const scrollHeight = document.body.scrollHeight;
        window.scrollBy(0, distance);
        totalHeight += distance;

        if (totalHeight >= scrollHeight - window.innerHeight) {
          clearInterval(timer);
          resolve();
        }
      }, 300);
    });
  });
}
