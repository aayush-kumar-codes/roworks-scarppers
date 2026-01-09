import dotenv from "dotenv"
import { MongoClient } from "mongodb"

dotenv.config()

// ---------------- CONFIG ----------------
const MONGO_URI = process.env.MONGO_URI
const DB_NAME = "inggestData"
const COLLECTION_NAME = "products"

// Validate required environment variables
if (!MONGO_URI) {
  console.error("❌ MONGO_URI is required")
  process.exit(1)
}

// ---------------- LOGGER ----------------
const log = {
  info: msg => console.log(`ℹ️  ${msg}`),
  success: msg => console.log(`✅ ${msg}`),
  warn: msg => console.log(`⚠️  ${msg}`),
  error: msg => console.error(`❌ ${msg}`)
}

// ---------------- MIGRATION FUNCTION ----------------
async function migrateSIField() {
  const mongo = new MongoClient(MONGO_URI)
  
  try {
    log.info("Connecting to MongoDB...")
    await mongo.connect()
    await mongo.db("admin").command({ ping: 1 })
    log.success("MongoDB connected")

    const db = mongo.db(DB_NAME)
    const productCollection = db.collection(COLLECTION_NAME)

    log.info(`Checking for products missing 'si' field in collection '${COLLECTION_NAME}'...`)
    
    // Count products that need migration
    const countToMigrate = await productCollection.countDocuments({
      $or: [
        { si: { $exists: false } },
        { si: null }
      ]
    })
    
    log.info(`Found ${countToMigrate} product(s) that need migration`)
    
    if (countToMigrate === 0) {
      log.success("All products already have 'si' field. No migration needed.")
      return
    }
    
    // Perform migration
    const result = await productCollection.updateMany(
      {
        $or: [
          { si: { $exists: false } },
          { si: null }
        ]
      },
      {
        $set: { si: "registered" }
      }
    )
    
    if (result.modifiedCount > 0) {
      log.success(`Migration complete: Updated ${result.modifiedCount} product(s) with si: "registered"`)
      
      // Verify migration
      const remainingCount = await productCollection.countDocuments({
        $or: [
          { si: { $exists: false } },
          { si: null }
        ]
      })
      
      if (remainingCount === 0) {
        log.success("Verification: All products now have 'si' field")
      } else {
        log.warn(`Warning: ${remainingCount} product(s) still missing 'si' field after migration`)
      }
    } else {
      log.warn("No products were updated")
    }
    
    // Show summary
    const totalProducts = await productCollection.countDocuments()
    const productsWithSI = await productCollection.countDocuments({ si: { $exists: true, $ne: null } })
    log.info(`Database summary: ${totalProducts} total product(s), ${productsWithSI} with 'si' field`)
    
  } catch (error) {
    log.error(`Migration error: ${error.message}`)
    if (error.stack) {
      log.error(`Stack trace: ${error.stack}`)
    }
    throw error
  } finally {
    await mongo.close()
    log.info("MongoDB connection closed")
  }
}

// ---------------- RUN ----------------
migrateSIField()
  .then(() => {
    log.success("Migration script completed successfully")
    process.exit(0)
  })
  .catch(err => {
    log.error(`Fatal error: ${err.message}`)
    process.exit(1)
  })
