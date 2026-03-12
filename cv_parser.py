# CV parser: tries to read PDF using PyMuPDF (fitz). If not available, falls back to plain text.
import os, re
try:
    import fitz  # PyMuPDF
    _HAS_FITZ = True
except Exception:
    _HAS_FITZ = False

def _read_pdf_text(path):
    text = ''
    if _HAS_FITZ:
        doc = fitz.open(path)
        for page in doc:
            text += page.get_text()
    else:
        # fallback: if file is text-like, try reading
        with open(path, 'rb') as f:
            raw = f.read()
        try:
            text = raw.decode('utf-8', errors='ignore')
        except Exception:
            text = ''
    return text

def extract_cv_details(path):
    if not os.path.exists(path):
        raise FileNotFoundError('CV file not found: ' + path)
    txt = _read_pdf_text(path)
    result = {}
    # regex heuristics
    name = re.search(r'Name[:\s-]{1,10}([A-Z][a-zA-Z\s]{2,50})', txt)
    email = re.search(r'[\w\.-]+@[\w\.-]+', txt)
    phone = re.search(r'\+?\d[\d\s\-]{7,}\d', txt)
    job_title = re.search(r'(?i)(Nurse|Doctor|Engineer|Developer|Caregiver|Teacher)', txt)
    if name:
        result['name'] = name.group(1).strip()
    if email:
        result['email'] = email.group(0).strip()
    if phone:
        result['phone'] = phone.group(0).strip()
    if job_title:
        result['job_title'] = job_title.group(0).strip()
    return result
