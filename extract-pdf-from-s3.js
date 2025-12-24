import dotenv from "dotenv"
import {
  TextractClient,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand
} from "@aws-sdk/client-textract"
import { writeFile } from "fs/promises"

dotenv.config()

const BUCKET_NAME = "roworks-pdf-extract"
const FILE_KEY =
  "uploads/ABB-Robotic-product-range-brochure-4pages-2019-9AKK1074920493-RevD.pdf"

const textract = new TextractClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
  }
})

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function countTables(blocks) {
  return blocks.filter(b => b.BlockType === "TABLE").length
}

async function extractPDFAsync() {
  try {
    console.log("🚀 Starting async Textract job...")

    const startCommand = new StartDocumentAnalysisCommand({
      DocumentLocation: {
        S3Object: {
          Bucket: BUCKET_NAME,
          Name: FILE_KEY
        }
      },
      FeatureTypes: ["TABLES", "FORMS"]
    })

    const startResponse = await textract.send(startCommand)
    const jobId = startResponse.JobId

    console.log("🆔 Job ID:", jobId)

    let status = "IN_PROGRESS"
    let result

    while (status === "IN_PROGRESS") {
      await sleep(3000)

      const getCommand = new GetDocumentAnalysisCommand({
        JobId: jobId
      })

      result = await textract.send(getCommand)
      status = result.JobStatus

      console.log("⏳ Job status:", status)
    }

    if (status !== "SUCCEEDED") {
      throw new Error("Textract job failed")
    }

    const extractedText = result.Blocks
      .filter(b => b.BlockType === "LINE")
      .map(b => b.Text)
      .join("\n")

    const tableCount = countTables(result.Blocks)

    // Create JSON data structure
    const jsonData = {
      metadata: {
        totalPages: result.DocumentMetadata.Pages,
        tablesFound: tableCount,
        blocksReturned: result.Blocks.length,
        bucket: BUCKET_NAME,
        fileKey: FILE_KEY,
        jobId: jobId,
        extractedAt: new Date().toISOString()
      },
      extractedText: extractedText
    }

    // Save to JSON file
    const timestamp = Date.now()
    const filename = `extracted-data-${timestamp}.json`

    await writeFile(filename, JSON.stringify(jsonData, null, 2), "utf-8")

    console.log(`\n✅ Data saved to: ${filename}`)
    console.log(`\n================ METADATA ================\n`)
    console.log(`Total pages    : ${jsonData.metadata.totalPages}`)
    console.log(`Tables found   : ${jsonData.metadata.tablesFound}`)
    console.log(`Blocks returned: ${jsonData.metadata.blocksReturned}`)
  } catch (err) {
    console.error("❌ Async Textract failed:", err.message)
  }
}

extractPDFAsync()
