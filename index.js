import express from "express";
import multer from "multer";
import fs from "fs";

import { connectDB, getDB } from "./db.js";

// Configure pdfjs-dist worker BEFORE importing pdf-tables-parser
import * as pdfjsLib from "pdfjs-dist";
// Resolve worker path using import.meta.resolve for proper ES module resolution
const workerModulePath = import.meta.resolve("pdfjs-dist/build/pdf.worker.min.mjs");
pdfjsLib.GlobalWorkerOptions.workerSrc = workerModulePath;

// Now import pdf-tables-parser after configuration
import { PdfDocument } from "pdf-tables-parser";

const app = express();
const upload = multer({ dest: "uploads/" });

app.post("/upload-pdf", upload.single("pdf"), async (req, res) => {
    try {
      const pdfPath = req.file.path;
      // const db = getDB();
      // const collection = db.collection("robots");
  
      // Read PDF file as buffer and convert to Uint8Array
      const pdfBuffer = fs.readFileSync(pdfPath);
      const pdfData = new Uint8Array(pdfBuffer);
      
      // Load PDF and extract tables
      const pdfDoc = new PdfDocument({
        hasTitles: false,
        threshold: 1.5
      });
      
      await pdfDoc.load(pdfData);
  
      const records = [];
      
      // Helper function to check if a string is a robot model
      const isRobotModel = (str) => {
        if (!str || typeof str !== 'string') return false;
        const trimmed = str.trim();
        return /^IRB\s+\d+/i.test(trimmed) || 
               /^IRB\s+\d+[A-Z]/i.test(trimmed) ||
               /^YuMi/i.test(trimmed) ||
               /^FlexPicker/i.test(trimmed);
      };
      
      // Helper function to check if we should skip a row
      const shouldSkipRow = (firstCell) => {
        if (!firstCell) return true;
        const trimmed = firstCell.trim().toLowerCase();
        return trimmed.includes("articulated") || 
               trimmed.includes("robotics product") ||
               trimmed.includes("creating the flexible") ||
               trimmed === "—" ||
               trimmed.startsWith("www.") ||
               trimmed.includes("facebook.com") ||
               trimmed.includes("twitter.com") ||
               trimmed.includes("youtube.com") ||
               trimmed.includes("linkedin.com") ||
               trimmed.includes("abb robotics is") ||
               trimmed.includes("additional information") ||
               trimmed.includes("we reserve") ||
               trimmed.includes("© copyright") ||
               trimmed.includes("specifications subject") ||
               trimmed.includes("parallel robots") ||
               trimmed.includes("scara robots") ||
               trimmed.includes("paint robots") ||
               trimmed.includes("compliant with iso");
      };
      
      // Collect all rows first
      const allRows = [];
      for (const page of pdfDoc.pages) {
        for (const table of page.tables) {
          for (const row of table.data) {
            if (!row || !row[0]) continue;
            const firstCell = String(row[0] || '').trim();
            if (shouldSkipRow(firstCell)) continue;
            allRows.push(row);
          }
        }
      }
      
      // Group rows by model - when we find a model row, collect subsequent rows until next model
      let currentRecord = null;
      
      for (let i = 0; i < allRows.length; i++) {
        const row = allRows[i];
        
        // Check both column 0 and column 1 for model (column 0 might be image)
        let modelCell = null;
        let modelColIdx = -1;
        
        for (let j = 0; j < Math.min(2, row.length); j++) {
          const cell = String(row[j] || '').trim();
          if (isRobotModel(cell)) {
            modelCell = cell;
            modelColIdx = j;
            break;
          }
        }
        
        // Check if this is a new model row
        if (modelCell) {
          // Save previous record if exists
          if (currentRecord && currentRecord.model) {
            records.push(currentRecord);
          }
          
          // Start new record
          currentRecord = {
            model: modelCell,
            payloadKg: null,
            reachM: null,
            repeatabilityMm: null,
            mounting: null,
            protection: null,
            axes: null,
            controller: null,
            remark: null,
            createdAt: new Date()
          };
          
          // Try to extract data from this row's columns (start after model column)
          for (let colIdx = modelColIdx + 1; colIdx < row.length; colIdx++) {
            const cell = String(row[colIdx] || '').trim();
            if (!cell) continue;
            
            const numValue = parseFloat(cell);
            
            // Payload: whole number, typically 1-800
            if (!isNaN(numValue) && !cell.includes('.') && numValue >= 1 && numValue <= 800 && !currentRecord.payloadKg) {
              currentRecord.payloadKg = numValue;
            }
            // Reach: decimal, typically 0.3-5.5
            else if (!isNaN(numValue) && cell.includes('.') && numValue >= 0.3 && numValue <= 5.5 && !currentRecord.reachM) {
              currentRecord.reachM = numValue;
            }
            // Repeatability: decimal, typically 0.01-0.30
            else if (!isNaN(numValue) && cell.includes('.') && numValue >= 0.01 && numValue <= 0.30 && !currentRecord.repeatabilityMm) {
              currentRecord.repeatabilityMm = numValue;
            }
            // Axes: whole number, typically 3-14
            else if (!isNaN(numValue) && !cell.includes('.') && numValue >= 3 && numValue <= 14 && !currentRecord.axes) {
              currentRecord.axes = numValue;
            }
            // Mounting: contains keywords
            else if (cell.toLowerCase().includes('floor') || cell.toLowerCase().includes('wall') || 
                     cell.toLowerCase().includes('inverted') || cell.toLowerCase().includes('shelf') ||
                     cell.toLowerCase().includes('table') || cell.toLowerCase().includes('tilted')) {
              if (!currentRecord.mounting) {
                currentRecord.mounting = cell;
              }
            }
            // Protection: contains IP, Std, Foundry, etc.
            else if (cell.toLowerCase().includes('ip') || cell.toLowerCase().includes('std:') || 
                     cell.toLowerCase().includes('foundry') || cell.toLowerCase().includes('clean room')) {
              if (!currentRecord.protection) {
                currentRecord.protection = cell;
              }
            }
            // Controller: contains IRC5, OmniCore, etc.
            else if (cell.toLowerCase().includes('irc5') || cell.toLowerCase().includes('omnicore')) {
              if (!currentRecord.controller) {
                currentRecord.controller = cell;
              }
            }
          }
        }
        // This row belongs to the current model - it's continuation data
        else if (currentRecord) {
          // Check if this row contains data for the current model
          for (let colIdx = 0; colIdx < row.length; colIdx++) {
            const cell = String(row[colIdx] || '').trim();
            if (!cell) continue;
            
            const numValue = parseFloat(cell);
            
            // Payload values (whole numbers)
            if (!isNaN(numValue) && !cell.includes('.') && numValue >= 1 && numValue <= 800) {
              // If we already have a payload, this might be an alternative (store in remark or append)
              if (!currentRecord.payloadKg) {
                currentRecord.payloadKg = numValue;
              } else if (currentRecord.payloadKg !== numValue) {
                // Multiple payload options - store as string
                currentRecord.payloadKg = `${currentRecord.payloadKg}/${numValue}`;
              }
            }
            // Reach values (decimals)
            else if (!isNaN(numValue) && cell.includes('.') && numValue >= 0.3 && numValue <= 5.5) {
              if (!currentRecord.reachM) {
                currentRecord.reachM = numValue;
              } else if (currentRecord.reachM !== numValue) {
                currentRecord.reachM = `${currentRecord.reachM}/${numValue}`;
              }
            }
            // Repeatability values
            else if (!isNaN(numValue) && cell.includes('.') && numValue >= 0.01 && numValue <= 0.30) {
              if (!currentRecord.repeatabilityMm) {
                currentRecord.repeatabilityMm = numValue;
              } else if (typeof currentRecord.repeatabilityMm === 'number' && currentRecord.repeatabilityMm !== numValue) {
                currentRecord.repeatabilityMm = `${currentRecord.repeatabilityMm} - ${numValue}`;
              }
            }
            // Mounting continuation
            else if (cell.toLowerCase().includes('floor') || cell.toLowerCase().includes('wall') || 
                     cell.toLowerCase().includes('inverted') || cell.toLowerCase().includes('shelf') ||
                     cell.toLowerCase().includes('table') || cell.toLowerCase().includes('tilted')) {
              if (currentRecord.mounting) {
                currentRecord.mounting += ', ' + cell;
              } else {
                currentRecord.mounting = cell;
              }
            }
            // Protection continuation
            else if (cell.toLowerCase().includes('ip') || cell.toLowerCase().includes('std:') || 
                     cell.toLowerCase().includes('foundry') || cell.toLowerCase().includes('clean room') ||
                     cell.toLowerCase().includes('option:')) {
              if (currentRecord.protection) {
                currentRecord.protection += ', ' + cell;
              } else {
                currentRecord.protection = cell;
              }
            }
            // Controller
            else if (cell.toLowerCase().includes('irc5') || cell.toLowerCase().includes('omnicore')) {
              if (!currentRecord.controller) {
                currentRecord.controller = cell;
              }
            }
          }
        }
      }
      
      // Don't forget the last record
      if (currentRecord && currentRecord.model) {
        records.push(currentRecord);
      }
  
      // if (records.length > 0) {
      //   await collection.insertMany(records);
      // }
  
      // Write extracted data to JSON file
      console.log(`Extracted ${records.length} records`);
      
      if (records.length > 0) {
        const jsonFilePath = `extracted-data-${Date.now()}.json`;
        fs.writeFileSync(jsonFilePath, JSON.stringify(records, null, 2));
      }
  
      fs.unlinkSync(pdfPath);
  
      res.json({
        success: true,
        inserted: records.length
      });
  
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

connectDB().then(() => {
    app.listen(3099, () => {
        console.log("Server is running on port 3099");
    });
}).catch((err) => {
    console.error(err);
    process.exit(1);
});