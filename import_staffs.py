# import_staffs.py  — place at project root
import json
import sys
import os
from datetime import datetime

from app import app
from database import db


def _parse_json_content(content):
    """
    Handle all JSON variants:
      1. Standard JSON array     [ {...}, ... ]
      2. Standard JSON object    { "records": [ ... ] }
      3. Bare fragment           "records": [ ... ]   ← missing outer braces
      4. JSONL                   {...}\n{...}\n
      5. Concatenated objects    {...}{...}
    """
    content = content.strip()

    # 1 & 2
    try:
        raw = json.loads(content)
        return raw if isinstance(raw, list) else raw.get('records', [raw])
    except json.JSONDecodeError:
        pass

    # 3 — bare fragment
    try:
        raw = json.loads('{' + content + '}')
        if 'records' in raw:
            print("ℹ️  Detected bare JSON fragment — wrapped automatically")
            return raw['records']
    except json.JSONDecodeError:
        pass

    # 4 — JSONL
    try:
        lines   = [l for l in content.splitlines() if l.strip()]
        records = [json.loads(l) for l in lines]
        if records:
            print("ℹ️  Detected JSONL format")
            return records
    except json.JSONDecodeError:
        pass

    # 5 — concatenated objects
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
        raise ValueError(f"Could not parse JSON near char {e.pos}: {e.msg}")

    raise ValueError("Unrecognised file format.")


def import_staffs(filepath, dry_run=False):
    if not os.path.exists(filepath):
        print(f"❌  File not found: {filepath}")
        sys.exit(1)

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        records = _parse_json_content(content)
    except ValueError as e:
        print(f"❌  {e}")
        print(f"\n📄  File preview (first 300 chars):\n{content[:300]}\n")
        sys.exit(1)

    print(f"📂  File    : {filepath}")
    print(f"📋  Records : {len(records)}")
    if dry_run:
        print("🔍  DRY RUN — no changes will be saved\n")
    else:
        print()

    col = db.live_staffs
    inserted = updated = skipped = 0
    errors = []

    for idx, rec in enumerate(records, start=1):
        email = (rec.get('email') or '').strip().lower()
        name  = (rec.get('section_1_personal_details') or {}).get('full_name', '?')

        if not email:
            print(f"  [{idx:>5}] ⚠️   Skipped  — no email  ({name})")
            skipped += 1
            continue

        try:
            if dry_run:
                exists = col.count_documents({"email": email}) > 0
                tag    = "UPDATE" if exists else "INSERT"
                print(f"  [{idx:>5}] {tag:<6}  {email}  ({name})")
            else:
                # strip mongodb_id from import — let MongoDB manage _id
                rec.pop('mongodb_id', None)
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
                    print(f"  [{idx:>5}] ✅  Inserted  {email}  ({name})")
                else:
                    updated += 1
                    print(f"  [{idx:>5}] 🔄  Updated   {email}  ({name})")
        except Exception as e:
            errors.append(f"Row {idx} ({email}): {e}")
            print(f"  [{idx:>5}] ❌  Error     {email} — {e}")

    print()
    if not dry_run:
        print(f"  ✅  Inserted : {inserted}")
        print(f"  🔄  Updated  : {updated}")
        print(f"  ⚠️   Skipped  : {skipped}")
        print(f"  ❌  Errors   : {len(errors)}")
        if errors:
            print("\nError details:")
            for err in errors:
                print(f"   {err}")
    print("\nDone.")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Import live_staffs from JSON')
    parser.add_argument(
        'file', nargs='?',
        default=os.path.join('static', 'documents', 'live_staff.json'),
        help='Path to JSON file (default: static/documents/live_staff.json)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview what would be inserted/updated without touching the DB'
    )
    args = parser.parse_args()

    with app.app_context():
        import_staffs(args.file, dry_run=args.dry_run)
