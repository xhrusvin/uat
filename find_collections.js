// find_collections.js

const sourceDb = db.getSiblingDB("xpress_health");
const pioDb = db.getSiblingDB("xpress_health_pio");

print("=== Collections in xpress_health ===");
printjson(sourceDb.getCollectionNames());

print("\n=== Collections in xpress_health_pio ===");
printjson(pioDb.getCollectionNames());

// Check if the email exists in xpress_health.users
print("\n=== findOne in xpress_health.users ===");
printjson(sourceDb.users.findOne({ email: "sahad2421@gmail.com" }));

// Check xpress_health_pio.users
print("\n=== findOne in xpress_health_pio.users ===");
printjson(pioDb.users.findOne({ email: "sahad2421@gmail.com" }));