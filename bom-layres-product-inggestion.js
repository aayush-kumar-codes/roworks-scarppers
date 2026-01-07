import dotenv from "dotenv"
import fs from "fs"
import path from "path"
import crypto from "crypto"
import { createWriteStream } from "fs"
import { pipeline } from "stream/promises"
import {
  TextractClient,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand
} from "@aws-sdk/client-textract"
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand
} from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { MongoClient, ObjectId } from "mongodb"
import OpenAI from "openai"
import pdfParse from "pdf-parse"

dotenv.config()

// ---------------- CONFIG ----------------
const BUCKET_NAME = "roworks-pdf-extract"
const S3_PREFIX = "uploads/"
const AWS_REGION = process.env.AWS_REGION
const MONGO_URI = process.env.MONGO_URI
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o"
const IMAGE_S3_BUCKET = process.env.IMAGE_S3_BUCKET || "roworks-robot-catalog-bucket"

// Validate required environment variables
if (!OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is required")
  process.exit(1)
}
if (!AWS_REGION) {
  console.error("❌ AWS_REGION is required")
  process.exit(1)
}
if (!MONGO_URI) {
  console.error("❌ MONGO_URI is required")
  process.exit(1)
}

const BATCH_SIZE = 3
const POLL_INTERVAL = 3000

// ---------------- LOGGER ----------------
const log = {
  info: msg => console.log(`ℹ️  ${msg}`),
  success: msg => console.log(`✅ ${msg}`),
  warn: msg => console.log(`⚠️  ${msg}`),
  error: msg => console.error(`❌ ${msg}`)
}

// ---------------- CLIENTS ----------------
const textract = new TextractClient({ 
  region: AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
  }
})
const s3 = new S3Client({ 
  region: AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
  }
})
const mongo = new MongoClient(MONGO_URI)
const openai = new OpenAI({
  apiKey: OPENAI_API_KEY
})

// ---------------- LOAD MANIFEST ----------------
let manifest
try {
  const manifestPath = path.join(process.cwd(), "manifest.json")
  
  // Check if file exists
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Manifest file not found at: ${manifestPath}`)
  }
  
  const manifestContent = fs.readFileSync(manifestPath, "utf-8")
  manifest = JSON.parse(manifestContent)
  
  // Validate manifest structure
  if (!manifest || !Array.isArray(manifest.vendors)) {
    throw new Error("Manifest file is missing required 'vendors' array")
  }
  
  log.success(`Manifest loaded: ${manifest.vendors.length} vendor(s)`)
} catch (err) {
  log.error(`Failed to load manifest: ${err.message}`)
  process.exit(1)
}

// ---------------- HELPERS ----------------
const sleep = ms => new Promise(r => setTimeout(r, ms))

function extractText(blocks) {
  return blocks
    .filter(b => b.BlockType === "LINE")
    .map(b => b.Text)
    .join("\n")
}

async function getS3PresignedUrl(s3Client, bucket, key) {
  // Generate a presigned URL valid for 7 days (604800 seconds)
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key
  })
  return await getSignedUrl(s3Client, command, { expiresIn: 604800 })
}

// ---------------- TEXT EXTRACTION PER PAGE ----------------
function extractTextByPage(blocks) {
  const pages = {}
  
  for (const block of blocks) {
    if (block.BlockType === "LINE" && block.Page !== undefined) {
      const pageNum = block.Page
      if (!pages[pageNum]) {
        pages[pageNum] = []
      }
      pages[pageNum].push(block.Text)
    }
  }
  
  return Object.keys(pages).map(pageNum => ({
    page: parseInt(pageNum),
    text: pages[pageNum].join("\n")
  }))
}

// ---------------- OPENAI PRODUCT MATCHING ----------------
async function matchProductsWithOpenAI(extractedText, manifest, retryCount = 0) {
  const MAX_RETRIES = 3
  
  try {
    if (!extractedText || extractedText.trim().length === 0) {
      log.warn("Empty text provided to OpenAI matching")
      return []
    }

    log.info("Sending full extracted text and manifest to OpenAI for product matching...")
    
    const prompt = `You are an expert at identifying industrial automation products from PDF documents.

Analyze the following extracted text from a PDF document and match it against the provided product manifest.

Your task:
1. Identify exact product matches from the manifest that appear in the PDF text
2. Extract the product name, brand (vendor), product type, and sub-type
3. Return a JSON array of matched products

Manifest structure:
${JSON.stringify(manifest, null, 2)}

Extracted PDF text:
${extractedText}

Return a JSON object with a "products" array containing matched products. Each product should have this structure:
{
  "products": [
    {
      "name": "exact product name from PDF",
      "brand": "vendor name from manifest",
      "product_type": "product group from manifest",
      "sub_type": "specific item from manifest or null",
      "bom_layer": "bom_layer from manifest",
      "vendor_name": "vendor_name from manifest",
      "page": page number where found (if known, else null),
      "price": numeric price value if found in PDF (e.g., 123.45), or null if not found
    }
  ]
}

Only return products that have a clear match. Return {"products": []} if no matches are found.
Return ONLY valid JSON, no additional text or markdown.`

    const completion = await openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages: [
        {
          role: "system",
          content: "You are a precise product matching assistant. Always return valid JSON with a 'products' array."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 4000
    })

    const content = completion.choices[0]?.message?.content
    if (!content) {
      log.warn("OpenAI returned empty response")
      return []
    }

    // Try to parse the response
    let parsed
    try {
      parsed = JSON.parse(content)
      // Handle case where OpenAI returns { products: [...] } or just the array
      if (Array.isArray(parsed)) {
        return parsed
      } else if (parsed.products && Array.isArray(parsed.products)) {
        return parsed.products
      } else if (parsed.matches && Array.isArray(parsed.matches)) {
        return parsed.matches
      } else {
        // Try to find any array in the response
        const arrayKeys = Object.keys(parsed).filter(k => Array.isArray(parsed[k]))
        if (arrayKeys.length > 0) {
          return parsed[arrayKeys[0]]
        }
        log.warn("OpenAI response doesn't contain expected array structure")
        return []
      }
    } catch (parseError) {
      log.error(`Failed to parse OpenAI response: ${parseError.message}`)
      log.error(`Response content: ${content.substring(0, 500)}`)
      return []
    }
  } catch (error) {
    // Handle rate limiting
    if (error.status === 429) {
      const retryAfter = error.headers?.['retry-after'] || 60
      log.warn(`OpenAI rate limit hit, waiting ${retryAfter} seconds...`)
      await sleep(retryAfter * 1000)
      if (retryCount < MAX_RETRIES) {
        return await matchProductsWithOpenAI(extractedText, manifest, retryCount + 1)
      }
      log.error("Max retries reached for rate limit")
      return []
    } 
    // Handle server errors
    else if (error.status === 500 || error.status === 503) {
      log.warn(`OpenAI server error (${error.status}), retrying in 10 seconds...`)
      await sleep(10000)
      if (retryCount < MAX_RETRIES) {
        return await matchProductsWithOpenAI(extractedText, manifest, retryCount + 1)
      }
      log.error("Max retries reached for server error")
      return []
    }
    // Handle other errors
    else {
      log.error(`OpenAI API error: ${error.message}`)
      if (error.status) {
        log.error(`Status code: ${error.status}`)
      }
      return []
    }
  }
}

// ---------------- IMAGE EXTRACTION ----------------
async function downloadPDFFromS3(fileKey, localPath) {
  try {
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: fileKey
    })
    
    const response = await s3.send(command)
    const writeStream = createWriteStream(localPath)
    
    await pipeline(response.Body, writeStream)
    log.info(`Downloaded PDF to ${localPath}`)
    return true
  } catch (error) {
    log.error(`Failed to download PDF: ${error.message}`)
    return false
  }
}

async function extractImagesFromPDF(fileKey, pageCount) {
  const tempDir = path.join(process.cwd(), "temp")
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true })
  }
  
  const fileName = fileKey.split("/").pop()
  const localPdfPath = path.join(tempDir, fileName)
  const images = []
  
  try {
    // Download PDF from S3
    const downloaded = await downloadPDFFromS3(fileKey, localPdfPath)
    if (!downloaded) {
      log.warn("Failed to download PDF for image extraction")
      return images
    }
    
    // Read PDF buffer
    const pdfBuffer = fs.readFileSync(localPdfPath)
    const pdfData = await pdfParse(pdfBuffer)
    
    // Note: pdf-parse doesn't directly extract images, but we can get page info
    // For actual image extraction, we'd need pdf-lib or another library
    // For now, we'll return page metadata and handle images separately if needed
    
    log.info(`PDF parsed: ${pdfData.numpages} pages (expected ${pageCount})`)
    
    // Clean up temp file
    if (fs.existsSync(localPdfPath)) {
      fs.unlinkSync(localPdfPath)
    }
    
    return images
  } catch (error) {
    log.error(`Image extraction error: ${error.message}`)
    // Clean up temp file if it exists
    if (fs.existsSync(localPdfPath)) {
      try {
        fs.unlinkSync(localPdfPath)
      } catch (unlinkError) {
        log.warn(`Failed to clean up temp file: ${unlinkError.message}`)
      }
    }
    return images
  }
}

async function uploadImageToS3(imageBuffer, productId, pageNumber, fileName) {
  try {
    const hash = crypto.createHash("sha256").update(imageBuffer).digest("hex")
    const extension = path.extname(fileName) || ".png"
    const s3Key = `catalog/images/${productId}/image/${pageNumber}/${hash}/${fileName}${extension}`
    
    const command = new PutObjectCommand({
      Bucket: IMAGE_S3_BUCKET,
      Key: s3Key,
      Body: imageBuffer,
      ContentType: `image/${extension.slice(1)}`
    })
    
    await s3.send(command)
    
    return {
      bucket: IMAGE_S3_BUCKET,
      key: s3Key,
      sha256: hash,
      size_bytes: imageBuffer.length,
      source_url: null
    }
  } catch (error) {
    log.error(`Failed to upload image to S3: ${error.message}`)
    return null
  }
}

// ---------------- PRICE EXTRACTION ----------------
function extractPrice(text, productName) {
  if (!text || typeof text !== 'string') {
    return null
  }

  // Try to find price patterns near the product name
  const namePattern = productName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const pricePatterns = [
    // Currency symbols with numbers: $123.45, €123,45, £123.45, ¥123
    /[$€£¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)/g,
    // Numbers with currency words: 123.45 USD, 123 EUR
    /(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:USD|EUR|GBP|JPY|CAD|AUD)/gi,
    // Price: $123.45 or Price:123.45
    /(?:price|cost|pricing)[\s:]*[$€£¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)/gi,
    // Just numbers that look like prices (with decimal points)
    /\b(\d{1,3}(?:[.,]\d{3})*\.\d{2})\b/g
  ]

  // Look for price near the product name (within 200 characters)
  const nameIndex = text.toLowerCase().indexOf(productName.toLowerCase())
  if (nameIndex !== -1) {
    const contextStart = Math.max(0, nameIndex - 100)
    const contextEnd = Math.min(text.length, nameIndex + productName.length + 100)
    const context = text.substring(contextStart, contextEnd)

    for (const pattern of pricePatterns) {
      const matches = context.match(pattern)
      if (matches && matches.length > 0) {
        // Extract the numeric value from the first match
        const priceMatch = matches[0].match(/(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)/)
        if (priceMatch) {
          // Normalize price: handle both comma and dot as decimal separators
          let priceStr = priceMatch[1]
          // If comma is used as decimal (European format), replace with dot
          if (priceStr.includes(',') && !priceStr.includes('.')) {
            priceStr = priceStr.replace(/,/g, '.')
          } else {
            // Remove thousands separators (commas), keep decimal dot
            priceStr = priceStr.replace(/,/g, '')
          }
          const price = parseFloat(priceStr)
          if (!isNaN(price) && price > 0 && price < 1000000) {
            return price
          }
        }
      }
    }
  }

  // If not found near product name, search the entire text
  for (const pattern of pricePatterns) {
    const matches = text.match(pattern)
    if (matches && matches.length > 0) {
      const priceMatch = matches[0].match(/(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)/)
      if (priceMatch) {
        // Normalize price: handle both comma and dot as decimal separators
        let priceStr = priceMatch[1]
        // If comma is used as decimal (European format), replace with dot
        if (priceStr.includes(',') && !priceStr.includes('.')) {
          priceStr = priceStr.replace(/,/g, '.')
        } else {
          // Remove thousands separators (commas), keep decimal dot
          priceStr = priceStr.replace(/,/g, '')
        }
        const price = parseFloat(priceStr)
        if (!isNaN(price) && price > 0 && price < 1000000) {
          return price
        }
      }
    }
  }

  return null
}

// ---------------- DATA NORMALIZATION ----------------
function normalizeProductData(product) {
  const brandNorm = (product.brand || "").toLowerCase().trim()
  const nameNorm = (product.name || "").toLowerCase().trim()
  
  // Tokenize name (split by spaces, hyphens, underscores, etc.)
  const nameTokens = nameNorm
    .split(/[\s\-_]+/)
    .filter(token => token.length > 0)
  
  // Generate aliases (variations of the name)
  const aliases = []
  if (nameNorm) {
    aliases.push(nameNorm)
    // Add version without spaces
    aliases.push(nameNorm.replace(/\s+/g, ""))
    // Add version with hyphens
    aliases.push(nameNorm.replace(/\s+/g, "-"))
    // Add version with underscores
    aliases.push(nameNorm.replace(/\s+/g, "_"))
    // Add product type combinations
    if (product.product_type) {
      aliases.push(`${nameNorm} ${product.product_type.toLowerCase()}`)
    }
  }
  
  // Remove duplicates
  const uniqueAliases = [...new Set(aliases)]
  
  return {
    brand_norm: brandNorm,
    name_norm: nameNorm,
    name_tokens: nameTokens,
    aliases: uniqueAliases
  }
}

// ---------------- MANIFEST INGESTION LOGIC ----------------
function ingestUsingManifest(text) {
  log.info("Starting manifest-based ingestion")

  const lowerText = text.toLowerCase()
  const ingestedVendors = []

  for (const vendor of manifest.vendors) {
    const vendorName = vendor.vendor_name
    const vendorResult = {
      vendor_name: vendorName,
      product_groups: []
    }

    log.info(`Checking vendor: ${vendorName}`)

    for (const group of vendor.product_groups) {
      const matchedItems = []

      for (const item of group.items) {
        // Basic keyword detection (first meaningful word)
        const keyword = item
          .replace(/[()]/g, "")
          .split(" ")[0]
          .toLowerCase()

        if (lowerText.includes(keyword)) {
          log.success(
            `Matched → Vendor: ${vendorName}, Group: ${group.product_group}, Item: ${item}`
          )
          matchedItems.push(item)
        }
      }

      if (matchedItems.length > 0) {
        vendorResult.product_groups.push({
          product_group: group.product_group,
          bom_layer: group.bom_layer,
          items: matchedItems
        })
      }
    }

    if (vendorResult.product_groups.length > 0) {
      ingestedVendors.push(vendorResult)
      log.success(`Vendor ingested: ${vendorName}`)
    } else {
      log.warn(`No matches found for vendor: ${vendorName}`)
    }
  }

  return ingestedVendors
}

// ---------------- LIST PDF FILES ----------------
async function listPDFFiles() {
  const command = new ListObjectsV2Command({
    Bucket: BUCKET_NAME,
    Prefix: S3_PREFIX
  })

  const response = await s3.send(command)

  return (response.Contents || [])
    .map(obj => obj.Key)
    .filter(key => key.toLowerCase().endsWith(".pdf"))
}

// ---------------- TEXTRACT ASYNC ----------------
async function runTextract(fileKey) {
  log.info(`Starting Textract job for ${fileKey}`)

  const start = await textract.send(
    new StartDocumentAnalysisCommand({
      DocumentLocation: {
        S3Object: { Bucket: BUCKET_NAME, Name: fileKey }
      },
      FeatureTypes: ["TABLES", "FORMS"]
    })
  )

  const jobId = start.JobId
  let status = "IN_PROGRESS"
  let result

  while (status === "IN_PROGRESS") {
    await sleep(POLL_INTERVAL)

    result = await textract.send(
      new GetDocumentAnalysisCommand({ JobId: jobId })
    )

    status = result.JobStatus
    log.info(`Textract status: ${status}`)
  }

  if (status !== "SUCCEEDED") {
    throw new Error("Textract failed")
  }

  log.success(`Textract completed for ${fileKey}`)
  return result
}

// ---------------- PROCESS SINGLE PDF ----------------
async function processPDF(fileKey, pdfCollection, productCollection) {
  const fileName = fileKey.split("/").pop()

  const exists = await pdfCollection.findOne({ fileName })
  if (exists) {
    log.warn(`Skipped (already processed): ${fileName}`)
    return
  }

  log.info(`Processing PDF: ${fileName}`)

  try {
    // Run Textract to extract text and get page metadata
    const result = await runTextract(fileKey)
    const pageCount = result.DocumentMetadata.Pages || 1
    
    log.info(`PDF has ${pageCount} page(s)`)

    // Extract text by page
    const pagesData = extractTextByPage(result.Blocks)
    const combinedText = pagesData.map(p => p.text).join("\n\n")
    
    log.info(`Extracted text length: ${combinedText.length} characters`)

    // Match products using OpenAI
    log.info("Calling OpenAI for product matching...")
    const matchedProducts = await matchProductsWithOpenAI(combinedText, manifest)
    
    log.info(`OpenAI matched ${matchedProducts.length} product(s)`)
    if (matchedProducts.length > 0) {
      log.info(`Sample matched products: ${JSON.stringify(matchedProducts.slice(0, 2), null, 2)}`)
    }

    // Extract images from PDF (if any)
    const images = await extractImagesFromPDF(fileKey, pageCount)
    
    // Get S3 presigned URL for the PDF
    const s3Link = await getS3PresignedUrl(s3, BUCKET_NAME, fileKey)

    // Store PDF metadata document
    const pdfDoc = {
      fileName,
      s3Key: fileKey,
      s3Link,
      bucket: BUCKET_NAME,
      pages: pageCount,
      extractedText: combinedText,
      pagesData: pagesData,
      matchedProductsCount: matchedProducts.length,
      createdAt: new Date()
    }
    
    log.info(`Inserting PDF document into MongoDB...`)
    const pdfInsertResult = await pdfCollection.insertOne(pdfDoc)
    const pdfId = pdfInsertResult.insertedId

    if (!pdfInsertResult.acknowledged) {
      throw new Error("PDF document insertion was not acknowledged by MongoDB")
    }

    log.success(`Saved PDF metadata: ${fileName} (ID: ${pdfId})`)

    // Process each matched product and store as separate documents
    let savedCount = 0
    let failedCount = 0
    
    for (const product of matchedProducts) {
      try {
        // Validate product data
        if (!product.name || !product.brand) {
          log.warn(`Skipping product with missing name or brand: ${JSON.stringify(product)}`)
          failedCount++
          continue
        }

        // Generate normalized fields
        const normFields = normalizeProductData(product)
        
        // Determine which page this product was found on
        const productPage = product.page || (pagesData.length > 0 ? pagesData[0].page : 1)
        const pageText = pagesData.find(p => p.page === productPage)?.text || combinedText

        // Extract price from text or product data
        let price = null
        if (product.price !== undefined && product.price !== null) {
          // If OpenAI returned a price, use it
          price = typeof product.price === 'number' ? product.price : parseFloat(product.price)
          if (isNaN(price)) {
            price = null
          }
        } else {
          // Try to extract price from the page text
          price = extractPrice(pageText, product.name)
        }

        // Create product document
        const productDoc = {
          name: product.name,
          brand: product.brand,
          product_type: product.product_type || null,
          sub_type: product.sub_type || null,
          price: price,
          s3Key: fileKey,
          s3Link: s3Link,
          source_refs: [{
            source: "pdf_extract",
            collection: "pdfExtracts",
            source_id: pdfId,
            page: productPage,
            fileName: fileName
          }],
          raw: {
            extractedText: pageText,
            page: productPage,
            fileName: fileName,
            s3Key: fileKey
          },
          assets: [],
          _norm: normFields,
          created_at: new Date(),
          updated_at: new Date()
        }

        // Insert product document into products collection
        log.info(`Inserting product: ${product.name} (${product.brand}) into products collection...`)
        const productInsertResult = await productCollection.insertOne(productDoc)
        
        if (!productInsertResult.acknowledged) {
          throw new Error("Product document insertion was not acknowledged by MongoDB")
        }
        
        savedCount++
        log.success(`Saved product: ${product.name} (${product.brand}) (ID: ${productInsertResult.insertedId})`)
      } catch (productError) {
        failedCount++
        log.error(`Failed to save product ${product.name || "unknown"}: ${productError.message}`)
        if (productError.stack) {
          log.error(`Stack trace: ${productError.stack}`)
        }
        // Continue with next product
      }
    }

    if (savedCount > 0) {
      log.success(`Successfully saved ${savedCount} product(s) from ${fileName}`)
    }
    if (failedCount > 0) {
      log.warn(`Failed to save ${failedCount} product(s) from ${fileName}`)
    }

    if (matchedProducts.length === 0) {
      log.warn(`No products matched for ${fileName} - PDF metadata still saved`)
    }
  } catch (error) {
    log.error(`Error processing PDF ${fileName}: ${error.message}`)
    if (error.stack) {
      log.error(`Stack trace: ${error.stack}`)
    }
    // Re-throw to be caught by batch processor
    throw error
  }
}

// ---------------- BATCH PROCESSOR ----------------
async function processInBatches(files, pdfCollection, productCollection) {
  let totalProcessed = 0
  let totalFailed = 0

  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE)

    log.info(
      `Processing batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(files.length / BATCH_SIZE)} (${batch.length} file(s))`
    )

    const results = await Promise.allSettled(
      batch.map(fileKey =>
        processPDF(fileKey, pdfCollection, productCollection).catch(err => {
          log.error(`${fileKey} → ${err.message}`)
          if (err.stack) {
            log.error(`Stack: ${err.stack}`)
          }
          throw err
        })
      )
    )

    // Count successes and failures
    for (const result of results) {
      if (result.status === "fulfilled") {
        totalProcessed++
      } else {
        totalFailed++
        log.error(`Batch item failed: ${result.reason?.message || "Unknown error"}`)
      }
    }
  }

  log.info(`Batch processing complete: ${totalProcessed} succeeded, ${totalFailed} failed`)
}

// ---------------- MAIN ----------------
async function ingestAllPDFs() {
  try {
    log.info("Connecting to MongoDB...")
    await mongo.connect()
    await mongo.db("admin").command({ ping: 1 })
    log.success("MongoDB connected")
  } catch (err) {
    log.error(`MongoDB connection failed: ${err.message}`)
    process.exit(1)
  }

  const db = mongo.db("inggestData")
  const pdfCollection = db.collection("pdfExtracts")
  const productCollection = db.collection("products")

  const pdfFiles = await listPDFFiles()
  log.info(`Found ${pdfFiles.length} PDF(s) in S3`)

  if (pdfFiles.length === 0) {
    log.warn("No PDF files found in S3. Exiting.")
    await mongo.close()
    return
  }

  await processInBatches(pdfFiles, pdfCollection, productCollection)
  
  // Log summary
  const pdfCount = await pdfCollection.countDocuments()
  const productCount = await productCollection.countDocuments()
  log.info(`Database summary: ${pdfCount} PDF document(s), ${productCount} product document(s)`)

  await mongo.close()
  log.success("All PDFs processed successfully")
}

// ---------------- RUN ----------------
ingestAllPDFs().catch(err => {
  log.error(`Fatal error: ${err.message}`)
  process.exit(1)
})
