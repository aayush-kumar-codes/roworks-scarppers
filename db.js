import { MongoClient } from "mongodb";

const url = "mongodb://35.169.142.73:27017";
const client = new MongoClient(url);

let db;

export async function connectDB() {
  await client.connect();
  db = client.db("robotsDB");
  console.log("MongoDB connected");
}


export function getDB() {
  return db;
}
