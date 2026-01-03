import dotenv from "dotenv"
import fs from "fs"
import path from "path"
import {
  TextractClient,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand
} from "@aws-sdk/client-textract"
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand
} from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { MongoClient } from "mongodb"

dotenv.config()

// ---------------- CONFIG ----------------
const BUCKET_NAME = "roworks-pdf-extract"
const S3_PREFIX = "uploads/"
const AWS_REGION = process.env.AWS_REGION
const MONGO_URI = process.env.MONGO_URI

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
async function processPDF(fileKey, collection) {
  const fileName = fileKey.split("/").pop()

  const exists = await collection.findOne({ fileName })
  if (exists) {
    log.warn(`Skipped (already processed): ${fileName}`)
    return
  }

  log.info(`Processing PDF: ${fileName}`)

  const result = await runTextract(fileKey)
  const text = extractText(result.Blocks)

  log.info(`Extracted text length: ${text.length} characters`)

  const normalizedBOM = ingestUsingManifest(text)

  const s3Link = await getS3PresignedUrl(s3, BUCKET_NAME, fileKey)

  await collection.insertOne({
    fileName,
    s3Key: fileKey,
    s3Link,
    bucket: BUCKET_NAME,
    pages: result.DocumentMetadata.Pages,
    extractedText: text,
    normalizedBOM,
    createdAt: new Date()
  })

  log.success(`Saved PDF + BOM data: ${fileName}`)
}

// ---------------- BATCH PROCESSOR ----------------
async function processInBatches(files, collection) {
  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE)

    log.info(
      `Processing batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} file(s))`
    )

    await Promise.allSettled(
      batch.map(fileKey =>
        processPDF(fileKey, collection).catch(err =>
          log.error(`${fileKey} → ${err.message}`)
        )
      )
    )
  }
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
  const collection = db.collection("pdfExtracts")

  const pdfFiles = await listPDFFiles()
  log.info(`Found ${pdfFiles.length} PDF(s) in S3`)

  await processInBatches(pdfFiles, collection)

  await mongo.close()
  log.success("All PDFs processed successfully")
}

// ---------------- RUN ----------------
ingestAllPDFs().catch(err => {
  log.error(`Fatal error: ${err.message}`)
  process.exit(1)
})
