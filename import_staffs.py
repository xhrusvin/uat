# import_staffs.py
import json
import sys
import os
from datetime import datetime
from bson import ObjectId

# ── Bootstrap Flask app context ───────────────────────────────────────
from app import app
from database import db

def import_staffs(filepath, dry_run=False):
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        sys.exit(1)

    with open(filepath, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    records = raw if isinstance(raw, list) else raw.get('records', [raw])
    print(f"📂 File: {filepath}")
    print(f"📋 Records found: {len(records)}")
    if dry_run:
        print("🔍 DRY RUN — no changes will be saved\n")

    col = db.live_staffs
    inserted = updated = skipped = 0
    errors = []

    for idx, rec in enumerate(records, start=1):
        email = (rec.get('email') or '').strip().lower()
        name  = rec.get('section_1_personal_details', {}).get('full_name', '?')

        if not email:
            print(f"  [{idx:>4}] ⚠️  Skipped — no email")
            skipped += 1
            continue

        try:
            if dry_run:
                exists = col.count_documents({"email": email}) > 0
                action = "UPDATE" if exists else "INSERT"
                print(f"  [{idx:>4}] {action:<6} {email}  ({name})")
            else:
                result = col.update_one(
                    {"email": email},
                    {
                        "$set": rec,
                        "$setOnInsert": {"created_at": datetime.utcnow()}
                    },
                    upsert=True
                )
                if result.upserted_id:
                    inserted += 1
                    print(f"  [{idx:>4}] ✅ Inserted  {email}  ({name})")
                else:
                    updated += 1
                    print(f"  [{idx:>4}] 🔄 Updated   {email}  ({name})")
        except Exception as e:
            errors.append(f"Row {idx} ({email}): {e}")
            print(f"  [{idx:>4}] ❌ Error     {email} — {e}")

    print()
    if not dry_run:
        print(f"✅ Inserted : {inserted}")
        print(f"🔄 Updated  : {updated}")
        print(f"⚠️  Skipped  : {skipped}")
        print(f"❌ Errors   : {len(errors)}")
    print("Done.")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Import live_staffs from JSON')
    parser.add_argument('file', nargs='?',
                        help='Path to JSON file (default: static/documents/live_staffs.json)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing to DB')
    args = parser.parse_args()

    filepath = args.file or os.path.join('static', 'documents', 'live_staffs.json')

    with app.app_context():
        import_staffs(filepath, dry_run=args.dry_run)