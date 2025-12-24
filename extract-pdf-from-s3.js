import dotenv from "dotenv"
import {
  TextractClient,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand
} from "@aws-sdk/client-textract"
import {
  S3Client,
  ListObjectsV2Command
} from "@aws-sdk/client-s3"
import { MongoClient } from "mongodb"

dotenv.config()

// ---------------- CONFIG ----------------
const BUCKET_NAME = "roworks-pdf-extract"
const S3_PREFIX = "uploads/"
const AWS_REGION = process.env.AWS_REGION
const MONGO_URI = process.env.MONGO_URI

const BATCH_SIZE = 3          // 🔥 parallel Textract jobs
const POLL_INTERVAL = 3000    // ms

// ---------------- CLIENTS ----------------
const textract = new TextractClient({ region: AWS_REGION })
const s3 = new S3Client({ region: AWS_REGION })
const mongo = new MongoClient(MONGO_URI)

// ---------------- HELPERS ----------------
const sleep = ms => new Promise(r => setTimeout(r, ms))

const log = {
  info: msg => console.log(`ℹ️  ${msg}`),
  success: msg => console.log(`✅ ${msg}`),
  warn: msg => console.log(`⚠️  ${msg}`),
  error: msg => console.error(`❌ ${msg}`)
}

function extractText(blocks) {
  return blocks
    .filter(b => b.BlockType === "LINE")
    .map(b => b.Text)
    .join("\n")
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
  }

  if (status !== "SUCCEEDED") {
    throw new Error("Textract failed")
  }

  return result
}

// ---------------- PROCESS SINGLE PDF ----------------
async function processPDF(fileKey, collection) {
  const fileName = fileKey.split("/").pop()

  const exists = await collection.findOne({ fileName })
  if (exists) {
    log.warn(`Skipped (already done): ${fileName}`)
    return
  }

  log.info(`Processing: ${fileName}`)

  const result = await runTextract(fileKey)
  const text = extractText(result.Blocks)

  await collection.insertOne({
    fileName,
    s3Key: fileKey,
    bucket: BUCKET_NAME,
    pages: result.DocumentMetadata.Pages,
    extractedText: text,
    createdAt: new Date()
  })

  log.success(`Saved: ${fileName}`)
}

// ---------------- BATCH PROCESSOR ----------------
async function processInBatches(files, collection) {
  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE)

    log.info(
      `Batch ${Math.floor(i / BATCH_SIZE) + 1} → ${batch.length} file(s)`
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
  // MongoDB connection must succeed before any PDF processing
  try {
    log.info("Connecting to MongoDB...")
    await mongo.connect()
    
    // Verify connection by pinging the database
    await mongo.db("admin").command({ ping: 1 })
    log.success("MongoDB connected")
  } catch (err) {
    log.error(`Failed to connect to MongoDB: ${err.message}`)
    log.error("Exiting: Cannot process PDFs without MongoDB connection")
    process.exit(1)
  }

  // Only proceed if MongoDB connection is successful
  const db = mongo.db("inggestData")
  const collection = db.collection("pdfExtracts")

  const pdfFiles = await listPDFFiles()
  log.info(`Found ${pdfFiles.length} PDF(s) in S3`)

  await processInBatches(pdfFiles, collection)

  await mongo.close()
  log.success("All PDFs processed")
}

// ---------------- RUN ----------------
ingestAllPDFs().catch(err => {
  log.error(`Fatal error: ${err.message}`)
  process.exit(1)
})
