// check.js - inspect the exact document for this email

const sourceDb = db.getSiblingDB("xpress_health");

print("=== xpress_health.users doc for sahad2421@gmail.com ===");
printjson(sourceDb.users.findOne({ email: "sahad2421@gmail.com" }));