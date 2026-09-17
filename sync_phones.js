// sync_phones.js

const sourceDb = db.getSiblingDB("xpress_health");
const targetDb = db.getSiblingDB("xpress_health_pio");

let updated = 0;
let notFound = 0;
let noPhone = 0;
let errors = 0;

const cursor = targetDb.care_learning_users.find(
  { email: { $exists: true, $ne: null } },
  { projection: { _id: 1, email: 1 } }
);

cursor.forEach((clUser) => {
  const sourceUser = sourceDb.users.findOne(
    { email: clUser.email },
    { projection: { phone: 1 } }  // ✅ fixed: only inclusion fields
  );

  if (!sourceUser) {
    print(`[NOT FOUND] ${clUser.email}`);
    notFound++;
    return;
  }

  if (!sourceUser.phone) {
    print(`[NO PHONE]  ${clUser.email}`);
    noPhone++;
    return;
  }

  try {
    targetDb.care_learning_users.updateOne(
      { _id: clUser._id },
      { $set: { phone: sourceUser.phone } }
    );
    print(`[OK] ${clUser.email} → ${sourceUser.phone}`);
    updated++;
  } catch (e) {
    print(`[ERROR] ${clUser.email} → ${e.message}`);
    errors++;
  }
});

print("\n========== SUMMARY ==========");
print(`✅ Updated   : ${updated}`);
print(`❌ Not found : ${notFound}`);
print(`⚠️  No phone  : ${noPhone}`);
print(`🔴 Errors    : ${errors}`);