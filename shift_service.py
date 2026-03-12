import json, os

THIS_DIR = os.path.dirname(__file__)
SHIFTS_FILE = os.path.join(THIS_DIR, 'sample_shifts.json')

def find_shifts(job_title: str, location: str):
    job_title = (job_title or '').lower()
    location = (location or '').lower()
    if not os.path.exists(SHIFTS_FILE):
        return []
    with open(SHIFTS_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    matches = []
    for s in data.get('shifts', []):
        if (not job_title or job_title in s.get('job_title','').lower()) and (not location or location in s.get('location','').lower()):
            matches.append(s)
    return matches
