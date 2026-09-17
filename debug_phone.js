// debug_phone.js - test exactly 3 emails to trace the issue

const sourceDb = db.getSiblingDB("xpress_health");
const targetDb = db.getSiblingDB("xpress_health_pio");

// Get 3 care_learning_users docs
const cursor = targetDb.care_learning_users.find({}).limit(3);

cursor.forEach((clUser) => {
  print("\n----------------------------------");
  print(`care_learning_users doc:`);
  printjson(clUser);

  const email = clUser.email;
  print(`\nEmail value     : [${email}]`);
  print(`Email type      : ${typeof email}`);
  print(`Email length    : ${email ? email.length : 'null'}`);

  // Try exact match
  const exact = sourceDb.users.findOne({ email: email });
  print(`\nExact match     : ${exact ? "FOUND" : "NOT FOUND"}`);

  // Try case-insensitive
  const icase = sourceDb.users.findOne({ email: { $regex: new RegExp(`^${email}$`, "i") } });
  print(`Case-insensitive: ${icase ? "FOUND" : "NOT FOUND"}`);

  // Try trimmed (hidden spaces)
  const trimmed = email ? email.trim() : null;
  const trim = sourceDb.users.findOne({ email: trimmed });
  print(`Trimmed match   : ${trim ? "FOUND" : "NOT FOUND"}`);

  if (exact) {
    print(`\nPhone in source : [${exact.phone}]`);
  }
  if (trim && !exact) {
    print(`\nPhone (trimmed) : [${trim.phone}]`);
  }
});