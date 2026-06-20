# import_staffs.py
import json
import sys
import os
from datetime import datetime

from app import app
from database import db


def load_records(filepath):
    """Try multiple JSON formats until one works."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read().strip()

    # 1. Standard JSON (object or array)
    try:
        raw = json.loads(content)
        return raw if isinstance(raw, list) else raw.get('records', [raw])
    except json.JSONDecodeError:
        pass

    # 2. JSONL — one JSON object per line
    try:
        records = [json.loads(line) for line in content.splitlines() if line.strip()]
        if records:
            print("ℹ️  Detected JSONL format (one record per line)")
            return records
    except json.JSONDecodeError:
        pass

    # 3. Concatenated JSON objects  {...}{...}{...}
    try:
        records = []
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(content):
            while idx < len(content) and content[idx] in ' \t\r\n,':
                idx += 1
            if idx >= len(content):
                break
            obj, end = decoder.raw_decode(content, idx)
            records.append(obj)
            idx = end
        if records:
            print(f"ℹ️  Detected concatenated JSON objects ({len(records)} found)")
            return records
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not parse JSON. Error near char {e.pos}: {e.msg}")

    raise ValueError("Unrecognised file format — not JSON, JSONL, or concatenated JSON.")


def import_staffs(filepath, dry_run=False):
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        sys.exit(1)

    try:
        records = load_records(filepath)
    except ValueError as e:
        print(f"❌ {e}")
        # Show first 200 chars so you can see what the file actually looks like
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            preview = f.read(200)
        print(f"\n📄 File preview:\n{preview}\n")
        sys.exit(1)

    print(f"📂 File   : {filepath}")
    print(f"📋 Records: {len(records)}")
    if dry_run:
        print("🔍 DRY RUN — no changes will be saved\n")

    col = db.live_staffs
    inserted = updated = skipped = 0
    errors = []

    for idx, rec in enumerate(records, start=1):
        email = (rec.get('email') or '').strip().lower()
        name  = rec.get('section_1_personal_details', {}).get('full_name', '?')

        if not email:
            print(f"  [{idx:>4}] ⚠️  Skipped — no email  ({name})")
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