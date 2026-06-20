def load_records(filepath):
    """Handle standard JSON, JSONL, concatenated objects, and bare 'records' fragments."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read().strip()

    # 1. Standard JSON (full object or array)
    try:
        raw = json.loads(content)
        return raw if isinstance(raw, list) else raw.get('records', [raw])
    except json.JSONDecodeError:
        pass

    # 2. Bare fragment — missing outer braces e.g.  "records": [ ... ]
    #    Wrap it and try again
    try:
        raw = json.loads('{' + content + '}')
        if 'records' in raw:
            print("ℹ️  Detected bare JSON fragment — wrapped automatically")
            return raw['records']
    except json.JSONDecodeError:
        pass

    # 3. JSONL — one JSON object per line
    try:
        records = [json.loads(line) for line in content.splitlines() if line.strip()]
        if records:
            print("ℹ️  Detected JSONL format")
            return records
    except json.JSONDecodeError:
        pass

    # 4. Concatenated JSON objects  {...}{...}{...}
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

    raise ValueError("Unrecognised file format.")