import dotenv from "dotenv"
import fs from "fs"
import path from "path"
import { XMLParser } from "fast-xml-parser"
import { MongoClient, ObjectId } from "mongodb"
import OpenAI from "openai"

dotenv.config()

// ---------------- CONFIG ----------------
const MONGO_URI = process.env.MONGO_URI
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o"
const URDF_FOLDER = process.env.URDF_FOLDER || "./URDF"

// Validate required environment variables
if (!OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is required")
  process.exit(1)
}
if (!MONGO_URI) {
  console.error("❌ MONGO_URI is required")
  process.exit(1)
}

const BATCH_SIZE = 3

// ---------------- LOGGER ----------------
const log = {
  info: msg => console.log(`ℹ️  ${msg}`),
  success: msg => console.log(`✅ ${msg}`),
  warn: msg => console.log(`⚠️  ${msg}`),
  error: msg => console.error(`❌ ${msg}`)
}

// ---------------- CLIENTS ----------------
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

// ---------------- URDF PARSING ----------------
function parseURDFFile(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, "utf-8")
    
    // Check if it's a valid URDF file (should have <robot> root element)
    if (!fileContent.includes("<robot") && !fileContent.includes("<robot>")) {
      throw new Error("File does not appear to be a valid URDF file (missing <robot> root element)")
    }
    
    // Configure XML parser
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      textNodeName: "#text",
      parseAttributeValue: true,
      parseNodeValue: true,
      trimValues: true,
      ignoreNameSpace: false,
      parseTrueNumberOnly: false
    })
    
    const parsed = parser.parse(fileContent)
    
    // Extract robot data
    const robot = parsed.robot || {}
    const robotName = robot["@_name"] || path.basename(filePath, path.extname(filePath))
    
    // Extract links
    const links = []
    if (robot.link) {
      const linkArray = Array.isArray(robot.link) ? robot.link : [robot.link]
      links.push(...linkArray.map(link => ({
        name: link["@_name"] || null,
        visual: link.visual || null,
        collision: link.collision || null,
        inertial: link.inertial || null,
        material: link.material || null
      })))
    }
    
    // Extract joints
    const joints = []
    if (robot.joint) {
      const jointArray = Array.isArray(robot.joint) ? robot.joint : [robot.joint]
      joints.push(...jointArray.map(joint => ({
        name: joint["@_name"] || null,
        type: joint["@_type"] || null,
        parent: joint.parent?.["@_link"] || null,
        child: joint.child?.["@_link"] || null,
        origin: joint.origin || null,
        axis: joint.axis || null,
        limit: joint.limit || null
      })))
    }
    
    // Extract materials
    const materials = []
    if (robot.material) {
      const materialArray = Array.isArray(robot.material) ? robot.material : [robot.material]
      materials.push(...materialArray.map(mat => ({
        name: mat["@_name"] || null,
        color: mat.color || null,
        texture: mat.texture || null
      })))
    }
    
    // Extract sensors (if any)
    const sensors = []
    if (robot.sensor) {
      const sensorArray = Array.isArray(robot.sensor) ? robot.sensor : [robot.sensor]
      sensors.push(...sensorArray.map(sensor => ({
        name: sensor["@_name"] || null,
        type: sensor["@_type"] || null,
        parent: sensor.parent?.["@_link"] || null
      })))
    }
    
    // Extract actuators (if any)
    const actuators = []
    if (robot.actuator) {
      const actuatorArray = Array.isArray(robot.actuator) ? robot.actuator : [robot.actuator]
      actuators.push(...actuatorArray.map(actuator => ({
        name: actuator["@_name"] || null,
        type: actuator["@_type"] || null,
        joint: actuator.joint?.["@_name"] || null
      })))
    }
    
    return {
      robotName,
      fileName: path.basename(filePath),
      filePath: path.resolve(filePath),
      links,
      joints,
      materials,
      sensors,
      actuators,
      raw: parsed
    }
  } catch (error) {
    log.error(`Failed to parse URDF file ${filePath}: ${error.message}`)
    throw error
  }
}

// ---------------- FORMAT URDF FOR LLM ----------------
function formatURDFForLLM(urdfData) {
  const parts = []
  
  parts.push(`Robot Name: ${urdfData.robotName}`)
  parts.push(`File: ${urdfData.fileName}`)
  parts.push("")
  
  // Links section
  if (urdfData.links && urdfData.links.length > 0) {
    parts.push(`Links (${urdfData.links.length}):`)
    urdfData.links.forEach((link, idx) => {
      parts.push(`  ${idx + 1}. ${link.name || "Unnamed"}`)
      if (link.material) {
        const matName = typeof link.material === "object" ? link.material["@_name"] : link.material
        if (matName) parts.push(`     Material: ${matName}`)
      }
    })
    parts.push("")
  }
  
  // Joints section
  if (urdfData.joints && urdfData.joints.length > 0) {
    parts.push(`Joints (${urdfData.joints.length}):`)
    urdfData.joints.forEach((joint, idx) => {
      parts.push(`  ${idx + 1}. ${joint.name || "Unnamed"} (Type: ${joint.type || "unknown"})`)
      if (joint.parent) parts.push(`     Parent: ${joint.parent}`)
      if (joint.child) parts.push(`     Child: ${joint.child}`)
      if (joint.limit) {
        const limit = joint.limit
        if (limit["@_lower"]) parts.push(`     Lower Limit: ${limit["@_lower"]}`)
        if (limit["@_upper"]) parts.push(`     Upper Limit: ${limit["@_upper"]}`)
        if (limit["@_effort"]) parts.push(`     Max Effort: ${limit["@_effort"]}`)
        if (limit["@_velocity"]) parts.push(`     Max Velocity: ${limit["@_velocity"]}`)
      }
    })
    parts.push("")
  }
  
  // Materials section
  if (urdfData.materials && urdfData.materials.length > 0) {
    parts.push(`Materials (${urdfData.materials.length}):`)
    urdfData.materials.forEach((mat, idx) => {
      parts.push(`  ${idx + 1}. ${mat.name || "Unnamed"}`)
      if (mat.color) {
        const rgba = mat.color["@_rgba"] || mat.color
        if (rgba) parts.push(`     Color: ${rgba}`)
      }
    })
    parts.push("")
  }
  
  // Sensors section
  if (urdfData.sensors && urdfData.sensors.length > 0) {
    parts.push(`Sensors (${urdfData.sensors.length}):`)
    urdfData.sensors.forEach((sensor, idx) => {
      parts.push(`  ${idx + 1}. ${sensor.name || "Unnamed"} (Type: ${sensor.type || "unknown"})`)
      if (sensor.parent) parts.push(`     Attached to: ${sensor.parent}`)
    })
    parts.push("")
  }
  
  // Actuators section
  if (urdfData.actuators && urdfData.actuators.length > 0) {
    parts.push(`Actuators (${urdfData.actuators.length}):`)
    urdfData.actuators.forEach((actuator, idx) => {
      parts.push(`  ${idx + 1}. ${actuator.name || "Unnamed"} (Type: ${actuator.type || "unknown"})`)
      if (actuator.joint) parts.push(`     Controls Joint: ${actuator.joint}`)
    })
    parts.push("")
  }
  
  return parts.join("\n")
}

// ---------------- OPENAI PRODUCT MATCHING ----------------
async function matchURDFProductsWithOpenAI(urdfText, manifest, retryCount = 0) {
  const MAX_RETRIES = 3
  
  try {
    if (!urdfText || urdfText.trim().length === 0) {
      log.warn("Empty URDF text provided to OpenAI matching")
      return []
    }

    log.info("Sending URDF data and manifest to OpenAI for product matching...")
    
    const prompt = `You are an expert at identifying industrial automation products from URDF (Unified Robot Description Format) files.

Analyze the following URDF robot description and match it against the provided product manifest.

URDF files describe robot structures including:
- Complete robots (robot arms, manipulators)
- Robot components (links, joints, actuators, sensors)
- Materials and parts used in robot construction

Your task:
1. Identify products from the manifest that match components described in the URDF
2. Extract product names, brands (vendors), product types, and sub-types
3. Identify the complete robot as a product if it matches manifest entries
4. Identify individual components (sensors, actuators, joints) as products if they match
5. Return a JSON array of matched products

Manifest structure:
${JSON.stringify(manifest, null, 2)}

URDF Robot Description:
${urdfText}

Return a JSON object with a "products" array containing matched products. Each product should have this structure:
{
  "products": [
    {
      "name": "exact product name from URDF or manifest",
      "brand": "vendor name from manifest",
      "product_type": "product group from manifest",
      "sub_type": "specific item from manifest or null",
      "bom_layer": "bom_layer from manifest",
      "vendor_name": "vendor_name from manifest",
      "component_type": "robot|link|joint|sensor|actuator|material",
      "price": null (URDF files typically don't contain pricing)
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
        return await matchURDFProductsWithOpenAI(urdfText, manifest, retryCount + 1)
      }
      log.error("Max retries reached for rate limit")
      return []
    } 
    // Handle server errors
    else if (error.status === 500 || error.status === 503) {
      log.warn(`OpenAI server error (${error.status}), retrying in 10 seconds...`)
      await sleep(10000)
      if (retryCount < MAX_RETRIES) {
        return await matchURDFProductsWithOpenAI(urdfText, manifest, retryCount + 1)
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

// ---------------- DUPLICATE CHECKING ----------------
async function checkDuplicate(productCollection, product) {
  try {
    const normFields = normalizeProductData(product)
    
    // Query by normalized name and brand
    const existing = await productCollection.findOne({
      "_norm.name_norm": normFields.name_norm,
      "_norm.brand_norm": normFields.brand_norm
    })
    
    return existing
  } catch (error) {
    log.error(`Error checking duplicate: ${error.message}`)
    return null
  }
}

// ---------------- UPSERT PRODUCT ----------------
async function upsertProduct(productCollection, productDoc) {
  try {
    // Check for duplicate
    const existing = await checkDuplicate(productCollection, productDoc)
    
    if (existing) {
      // Update existing product (preserve existing data, only update fields)
      const updateDoc = {
        $set: {
          updated_at: new Date(),
          // Only update fields that are provided and not null
          ...(productDoc.product_type && { product_type: productDoc.product_type }),
          ...(productDoc.sub_type && { sub_type: productDoc.sub_type }),
          ...(productDoc.price !== null && productDoc.price !== undefined && { price: productDoc.price })
        },
        // Add to source_refs array if not already present
        $addToSet: {
          source_refs: { $each: productDoc.source_refs || [] }
        }
      }
      
      const result = await productCollection.updateOne(
        { _id: existing._id },
        updateDoc
      )
      
      if (result.modifiedCount > 0) {
        log.success(`Updated existing product: ${productDoc.name} (${productDoc.brand})`)
        return { updated: true, id: existing._id }
      } else {
        log.info(`Product already exists (no changes): ${productDoc.name} (${productDoc.brand})`)
        return { updated: false, id: existing._id }
      }
    } else {
      // Insert new product
      const result = await productCollection.insertOne(productDoc)
      
      if (!result.acknowledged) {
        throw new Error("Product document insertion was not acknowledged by MongoDB")
      }
      
      log.success(`Inserted new product: ${productDoc.name} (${productDoc.brand})`)
      return { inserted: true, id: result.insertedId }
    }
  } catch (error) {
    log.error(`Error upserting product: ${error.message}`)
    throw error
  }
}

// ---------------- LIST URDF FILES ----------------
function listURDFFiles() {
  const urdfDir = path.resolve(URDF_FOLDER)
  
  if (!fs.existsSync(urdfDir)) {
    log.warn(`URDF folder not found: ${urdfDir}`)
    return []
  }
  
  const files = fs.readdirSync(urdfDir)
  const urdfFiles = files
    .filter(file => {
      const ext = path.extname(file).toLowerCase()
      return ext === ".urdf" || ext === ".xml"
    })
    .map(file => path.join(urdfDir, file))
  
  return urdfFiles
}

// ---------------- PROCESS SINGLE URDF FILE ----------------
async function processURDF(filePath, urdfCollection, productCollection) {
  const fileName = path.basename(filePath)

  // Check if already processed
  const exists = await urdfCollection.findOne({ fileName })
  if (exists) {
    log.warn(`Skipped (already processed): ${fileName}`)
    return
  }

  log.info(`Processing URDF: ${fileName}`)

  try {
    // Parse URDF file
    const urdfData = parseURDFFile(filePath)
    log.info(`Parsed URDF: ${urdfData.robotName} (${urdfData.links.length} links, ${urdfData.joints.length} joints)`)

    // Format for LLM
    const urdfText = formatURDFForLLM(urdfData)
    log.info(`Formatted URDF text length: ${urdfText.length} characters`)

    // Match products using OpenAI
    log.info("Calling OpenAI for product matching...")
    const matchedProducts = await matchURDFProductsWithOpenAI(urdfText, manifest)
    
    log.info(`OpenAI matched ${matchedProducts.length} product(s)`)
    if (matchedProducts.length > 0) {
      log.info(`Sample matched products: ${JSON.stringify(matchedProducts.slice(0, 2), null, 2)}`)
    }

    // Store URDF metadata document
    const urdfDoc = {
      fileName,
      filePath: urdfData.filePath,
      robotName: urdfData.robotName,
      linksCount: urdfData.links.length,
      jointsCount: urdfData.joints.length,
      materialsCount: urdfData.materials.length,
      sensorsCount: urdfData.sensors.length,
      actuatorsCount: urdfData.actuators.length,
      urdfData: urdfData.raw,
      formattedText: urdfText,
      matchedProductsCount: matchedProducts.length,
      createdAt: new Date()
    }
    
    log.info(`Inserting URDF document into MongoDB...`)
    const urdfInsertResult = await urdfCollection.insertOne(urdfDoc)
    const urdfId = urdfInsertResult.insertedId

    if (!urdfInsertResult.acknowledged) {
      throw new Error("URDF document insertion was not acknowledged by MongoDB")
    }

    log.success(`Saved URDF metadata: ${fileName} (ID: ${urdfId})`)

    // Process each matched product and store as separate documents
    let savedCount = 0
    let updatedCount = 0
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

        // Create product document
        const productDoc = {
          name: product.name,
          brand: product.brand,
          product_type: product.product_type || null,
          sub_type: product.sub_type || null,
          price: product.price || null,
          s3Key: null, // URDF files are local
          s3Link: null,
          source_refs: [{
            source: "urdf_extract",
            collection: "urdfExtracts",
            source_id: urdfId,
            fileName: fileName,
            filePath: urdfData.filePath,
            componentType: product.component_type || null
          }],
          raw: {
            urdfData: urdfData.raw,
            fileName: fileName,
            filePath: urdfData.filePath,
            robotName: urdfData.robotName,
            componentType: product.component_type || null
          },
          assets: [],
          _norm: normFields,
          created_at: new Date(),
          updated_at: new Date()
        }

        // Upsert product document into products collection
        log.info(`Upserting product: ${product.name} (${product.brand}) into products collection...`)
        const result = await upsertProduct(productCollection, productDoc)
        
        if (result.inserted) {
          savedCount++
        } else if (result.updated) {
          updatedCount++
        }
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
      log.success(`Successfully inserted ${savedCount} new product(s) from ${fileName}`)
    }
    if (updatedCount > 0) {
      log.success(`Successfully updated ${updatedCount} existing product(s) from ${fileName}`)
    }
    if (failedCount > 0) {
      log.warn(`Failed to save ${failedCount} product(s) from ${fileName}`)
    }

    if (matchedProducts.length === 0) {
      log.warn(`No products matched for ${fileName} - URDF metadata still saved`)
    }
  } catch (error) {
    log.error(`Error processing URDF ${fileName}: ${error.message}`)
    if (error.stack) {
      log.error(`Stack trace: ${error.stack}`)
    }
    // Re-throw to be caught by batch processor
    throw error
  }
}

// ---------------- BATCH PROCESSOR ----------------
async function processInBatches(files, urdfCollection, productCollection) {
  let totalProcessed = 0
  let totalFailed = 0

  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE)

    log.info(
      `Processing batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(files.length / BATCH_SIZE)} (${batch.length} file(s))`
    )

    const results = await Promise.allSettled(
      batch.map(filePath =>
        processURDF(filePath, urdfCollection, productCollection).catch(err => {
          log.error(`${filePath} → ${err.message}`)
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
async function ingestAllURDFs() {
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
  const urdfCollection = db.collection("urdfExtracts")
  const productCollection = db.collection("products")

  const urdfFiles = listURDFFiles()
  log.info(`Found ${urdfFiles.length} URDF file(s) in ${URDF_FOLDER}`)

  if (urdfFiles.length === 0) {
    log.warn("No URDF files found. Exiting.")
    await mongo.close()
    return
  }

  await processInBatches(urdfFiles, urdfCollection, productCollection)
  
  // Log summary
  const urdfCount = await urdfCollection.countDocuments()
  const productCount = await productCollection.countDocuments()
  log.info(`Database summary: ${urdfCount} URDF document(s), ${productCount} product document(s)`)

  await mongo.close()
  log.success("All URDF files processed successfully")
}

// ---------------- RUN ----------------
ingestAllURDFs().catch(err => {
  log.error(`Fatal error: ${err.message}`)
  process.exit(1)
})

