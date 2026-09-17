// sync_phones.js

const sourceDb = db.getSiblingDB("xpress_health");
const targetDb = db.getSiblingDB("xpress_health_pio");

let updated = 0;
let notFound = 0;
let noPhone = 0;
let errors = 0;

const cursor = targetDb.care_learning_users.find({});

cursor.forEach((clUser) => {
  const email = clUser.email;

  if (!email) {
    print(`[SKIP] No email on _id: ${clUser._id}`);
    return;
  }

  // ✅ No projection — fetch full document
  const sourceUser = sourceDb.users.findOne({ email: email });

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