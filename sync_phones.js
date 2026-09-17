// sync_phones.js

const sourceDb = db.getSiblingDB("xpress_health");
const targetDb = db.getSiblingDB("xpress_health_pio");

let updated = 0;
let notFound = 0;
let noPhone = 0;
let errors = 0;

// ✅ No projection — fetch the full document to avoid field mapping issues
const cursor = targetDb.care_learning_users.find(
  { email: { $exists: true, $ne: null } }
);

cursor.forEach((clUser) => {
  const email = clUser.email;

  if (!email) {
    print(`[SKIP] Document has no email: ${clUser._id}`);
    return;
  }

  const sourceUser = sourceDb.users.findOne(
    { email: email },
    { projection: { phone: 1 } }
  );

  if (!sourceUser) {
    print(`[NOT FOUND] ${email}`);
    notFound++;
    return;
  }

  if (!sourceUser.phone) {
    print(`[NO PHONE]  ${email}`);
    noPhone++;
    return;
  }

  try {
    targetDb.care_learning_users.updateOne(
      { _id: clUser._id },
      { $set: { phone: sourceUser.phone } }
    );
    print(`[OK] ${email} → ${sourceUser.phone}`);
    updated++;
  } catch (e) {
    print(`[ERROR] ${email} → ${e.message}`);
    errors++;
  }
});

print("\n========== SUMMARY ==========");
print(`✅ Updated   : ${updated}`);
print(`❌ Not found : ${notFound}`);
print(`⚠️  No phone  : ${noPhone}`);
print(`🔴 Errors    : ${errors}`);