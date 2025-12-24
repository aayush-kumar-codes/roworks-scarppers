import dotenv from "dotenv"
import { S3Client, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3"

import fs from "fs"
import path from "path"

dotenv.config()

const s3 = new S3Client({ region: "us-east-1", 
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY
      }
    
})

// Check if file already exists in S3
async function fileExistsInS3(s3Key) {
  try {
    await s3.send(
      new HeadObjectCommand({
        Bucket: "roworks-pdf-extract",
        Key: s3Key
      })
    )
    return true
  } catch (error) {
    // If error code is NotFound, file doesn't exist
    if (error.name === "NotFound" || error.$metadata?.httpStatusCode === 404) {
      return false
    }
    // For other errors, throw to handle them
    throw error
  }
}

async function uploadToS3(filePath, s3Key) {
  try {
    // Check if file exists locally
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`)
    }

    const file = fs.readFileSync(filePath)
    const fileName = path.basename(filePath)
    const fileSizeMB = (file.length / (1024 * 1024)).toFixed(2)

    console.log(`  📤 Uploading: ${fileName} (${fileSizeMB} MB)...`)

    await s3.send(
      new PutObjectCommand({
        Bucket: "roworks-pdf-extract",
        Key: s3Key || `uploads/${fileName}`,
        Body: file,
        ContentType: "application/pdf"
      })
    )

    console.log(`  ✅ Successfully uploaded: ${fileName} → S3 key: ${s3Key || `uploads/${fileName}`}`)
  } catch (error) {
    console.error(`  ❌ Error uploading ${path.basename(filePath)}:`, error.message)
    throw error
  }
}

// Function to get all PDF files from the pdfs folder
function getAllPdfFiles(folderPath) {
  try {
    const files = fs.readdirSync(folderPath)
    return files
      .filter(file => file.toLowerCase().endsWith('.pdf'))
      .map(file => path.join(folderPath, file))
  } catch (error) {
    console.error(`Error reading folder ${folderPath}:`, error.message)
    return []
  }
}

// Process a single file (check existence and upload if needed)
async function processFile(pdfPath, index, total) {
  const fileName = path.basename(pdfPath)
  const s3Key = `uploads/${fileName}`
  
  console.log(`[${index + 1}/${total}] Processing: ${fileName}`)
  
  try {
    // Check if file already exists in S3
    const exists = await fileExistsInS3(s3Key)
    
    if (exists) {
      console.log(`  ⏭️  Skipped: ${fileName} already exists in S3`)
      return { status: 'skipped', fileName }
    } else {
      await uploadToS3(pdfPath, s3Key)
      return { status: 'success', fileName }
    }
  } catch (error) {
    console.error(`  ❌ Failed to upload ${fileName}:`, error.message)
    return { status: 'failed', fileName, error: error.message }
  }
}

// Process files in batches
async function processBatch(files, batchNumber, batchSize, totalFiles) {
  const startIndex = batchNumber * batchSize
  const endIndex = Math.min(startIndex + batchSize, files.length)
  const batch = files.slice(startIndex, endIndex)
  
  console.log(`\n📦 Batch ${batchNumber + 1} (Processing ${batch.length} file(s))...\n`)
  
  // Process all files in the batch concurrently
  const results = await Promise.allSettled(
    batch.map((filePath, batchIndex) => 
      processFile(filePath, startIndex + batchIndex, totalFiles)
    )
  )
  
  // Count results
  const batchStats = {
    success: 0,
    skipped: 0,
    failed: 0
  }
  
  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      const fileResult = result.value
      if (fileResult.status === 'success') batchStats.success++
      else if (fileResult.status === 'skipped') batchStats.skipped++
      else if (fileResult.status === 'failed') batchStats.failed++
    } else {
      batchStats.failed++
      console.error(`  ❌ Error processing file:`, result.reason)
    }
  })
  
  console.log(`\n✅ Batch ${batchNumber + 1} completed: ${batchStats.success} uploaded, ${batchStats.skipped} skipped, ${batchStats.failed} failed\n`)
  
  return batchStats
}

// Upload all PDFs from the pdfs folder in batches
async function uploadAllPdfs() {
  const pdfsFolder = "./pdfs"
  const pdfFiles = getAllPdfFiles(pdfsFolder)
  const BATCH_SIZE = 5 // Number of files to process concurrently per batch

  if (pdfFiles.length === 0) {
    console.log("❌ No PDF files found in the pdfs folder")
    return
  }

  console.log("\n📁 Found PDF files:")
  pdfFiles.forEach((filePath, index) => {
    const fileName = path.basename(filePath)
    console.log(`   ${index + 1}. ${fileName}`)
  })

  const totalBatches = Math.ceil(pdfFiles.length / BATCH_SIZE)
  console.log(`\n🚀 Starting batch upload: ${pdfFiles.length} file(s) in ${totalBatches} batch(es) (${BATCH_SIZE} files per batch)\n`)

  let successCount = 0
  let failCount = 0
  let skippedCount = 0

  // Process files in batches
  for (let batchNumber = 0; batchNumber < totalBatches; batchNumber++) {
    const batchStats = await processBatch(pdfFiles, batchNumber, BATCH_SIZE, pdfFiles.length)
    successCount += batchStats.success
    skippedCount += batchStats.skipped
    failCount += batchStats.failed
  }

  console.log("=".repeat(50))
  console.log(`📊 Upload Summary:`)
  console.log(`   ✅ Successful: ${successCount}`)
  console.log(`   ⏭️  Skipped: ${skippedCount}`)
  console.log(`   ❌ Failed: ${failCount}`)
  console.log(`   📦 Total: ${pdfFiles.length}`)
  console.log("=".repeat(50))
}

// Upload all PDFs from the pdfs folder
uploadAllPdfs()
