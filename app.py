import streamlit as st
import pandas as pd
import os
import time
import json
import re
import hashlib
import tempfile
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from datetime import timedelta

# ==== OpenAI client (supports new and old SDKs) ====
try:
    from openai import OpenAI  # new SDK
    _OPENAI_NEW = True
except Exception:
    import openai               # old SDK
    _OPENAI_NEW = False

import gspread
from gspread.exceptions import APIError as GSpreadAPIError
# Modern Google auth
from google.oauth2.service_account import Credentials

# URL helpers
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

# ==== Optional robust PDF stack ====
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except Exception:
    PYMUPDF_AVAILABLE = False

try:
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# ===============================
# App config + Theme (Deep Purple, professional)
# ===============================
st.set_page_config(page_title="Food Policy Analyzer", layout="wide")

st.markdown("""
<style>
:root {
  --accent: #4c1d95;   /* deep purple */
  --accent-2: #5b21b6; /* slightly lighter deep purple */
  --accent-3: #7c3aed; /* vibrant accent for hovers */
  --bg-soft: #f4f2ff;
  --text-main: #1f2937; /* dark slate for readability */
}
html, body, [class*="css"]  { color: var(--text-main); }
.block-container { padding-top: 1rem; }
h1, h2, h3, h4 { color: var(--accent); letter-spacing: 0.2px; }
.stButton>button, .stDownloadButton>button {
  background: var(--accent); color: white; border-radius: 10px; border: none;
  padding: 0.5rem 1rem; font-weight: 600; box-shadow: 0 2px 6px rgba(76,29,149,0.15);
}
.stButton>button:hover, .stDownloadButton>button:hover { background: var(--accent-2); }
.stButton>button:focus, .stDownloadButton>button:focus { outline: 2px solid var(--accent-3); }
[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #ffffff 0%, #f7f5ff 100%); border-right: 1px solid #eee;
}
hr { border-color: #eae6ff; }
.stRadio > label { font-weight: 700; color: #111827; font-size: 16px; margin-bottom: 8px; }
.stRadio div[role="radiogroup"] > div {
  padding: 6px 10px; border-radius: 6px; margin-bottom: 4px; transition: background-color 0.2s ease, border-color 0.2s ease; border: 1px solid #eee;
}
.stRadio div[role="radiogroup"] > div:hover { background-color: #f4f2ff; border-color: #e0d7ff; }
.dataframe tbody tr:hover { background: #faf7ff !important; }
.sidebar-title {
    font-size: 26px; font-weight: 800; color: #4c1d95; padding-bottom: 12px; border-bottom: 3px solid #5b21b6; letter-spacing: 0.3px;
}
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown('<div class="sidebar-title">Navigation</div>', unsafe_allow_html=True)

choice = st.sidebar.radio(
    "**Choose analysis type**",
    ["Analyze Excel", "Analyze PDF"],
    horizontal=False,
    label_visibility="visible"
)

st.sidebar.markdown(
    '<a href="http://localhost:8502" target="_blank">Open Dashboard</a>',
    unsafe_allow_html=True
)

# =========================
# Config & Secrets
# =========================
api_key = st.secrets.get("OPENAI_API_KEY", None)

# show masked api key tail to confirm it's loaded
if api_key:
    tail = api_key[-6:] if len(api_key) >= 6 else api_key
    st.sidebar.caption(f"OpenAI key loaded: **…{tail}**")
else:
    st.sidebar.error("No OPENAI_API_KEY found in secrets.")

# Default data sheet (your live source)
DEFAULT_SHEET_URL = st.secrets.get("DATA_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1DqqpBxkWUGM8zi6C7_SUlg0LzzlMstq7jDfw8BCHZNk/edit#gid=0"
)

# Optional separate registry workbook (recommended). If not provided, app tries to use the data sheet.
PROCESSED_SHEET_KEY = st.secrets.get("PROCESSED_SHEET_KEY", None)
PROCESSED_TAB_TITLE = st.secrets.get("PROCESSED_TAB_TITLE", "Processed")
REGISTRY_BACKEND_PREF = st.secrets.get("REGISTRY_BACKEND", "auto").lower()  # auto|sheet|local

# =========================
# URL canonicalization + detection
# =========================
def looks_like_url(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    try:
        u = urlparse(s)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False

def canonical_url(base_url_or_blank: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    abs_url = urljoin(base_url_or_blank or "", href)
    u = urlparse(abs_url)
    q = [(k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
         if not (k.startswith("utm_") or k in {"fbclid", "gclid", "igshid"})]
    cleaned = u._replace(query=urlencode(q, doseq=True), fragment="")
    return urlunparse(cleaned)

# =========================
# Google Sheets Auth (modern)
# =========================
@st.cache_resource
def get_gs_client_and_email():
    SCOPE = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = None
    if "GSHEET_JSON" in st.secrets:
        try:
            creds_dict = json.loads(st.secrets["GSHEET_JSON"])
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
        except Exception as e:
            st.error(f"Invalid GSHEET_JSON in secrets: {e}")
            raise
    else:
        try:
            creds = Credentials.from_service_account_file("food_analyzer.json", scopes=SCOPE)
        except Exception:
            st.error("Google Sheets credentials not found. Add GSHEET_JSON to secrets or include food_analyzer.json.")
            raise
    client = gspread.authorize(creds)
    sa_email = ""
    try:
        sa_email = creds.service_account_email
    except Exception:
        pass
    return client, sa_email

try:
    client_gsheets, SERVICE_ACCOUNT_EMAIL = get_gs_client_and_email()
except Exception:
    client_gsheets, SERVICE_ACCOUNT_EMAIL = None, ""

if SERVICE_ACCOUNT_EMAIL:
    st.sidebar.info(f"Service Account:\n\n`{SERVICE_ACCOUNT_EMAIL}`\n\nShare your Google Sheet(s) with this email (Editor).")

# =========================
# DB CONNECTION (from secrets)
# =========================
DB_CONFIG = {
    "host": st.secrets.get("DB_HOST", "localhost"),
    "user": st.secrets.get("DB_USER", "root"),
    "password": st.secrets.get("DB_PASSWORD", ""),
    "database": st.secrets.get("DB_NAME", "food_analysis")
}

def insert_to_db(data, summaries):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for row, summary in zip(data, summaries):
            cursor.execute("""
                INSERT IGNORE INTO analysis_results 
                (date, actor, tactic, description, stakeholders, policy_area, focus, impact, source, tag, summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["Date"], row["Industry Actor(s)"], row["Industry Tactic"], row["Description"],
                row["Stakeholders"], row["Policy Area"], row["Geographical Focus"],
                row["Outcome/Impact"], row["Source"], row["Tag"], summary
            ))
        conn.commit()
        cursor.close()
        conn.close()
    except Error as e:
        st.error(f"Database error: {e}")

# =========================
# OpenAI helpers (new & old SDK) + token usage tracking
# =========================
if _OPENAI_NEW:
    _client_new = OpenAI(api_key=api_key) if api_key else None
else:
    _client_new = None
    if api_key:
        openai.api_key = api_key

# token usage accumulators
if "tok_prompt" not in st.session_state:
    st.session_state.update(tok_prompt=0, tok_completion=0, tok_total=0)

def chat_create(model, messages, temperature=0, max_tokens=1400):
    """Return (response_object, content_text, usage_dict) across SDKs; accumulates usage."""
    if _client_new:
        resp = _client_new.chat.completions.create(
            model=model, messages=messages, temperature=temperature, max_tokens=max_tokens,
        )
        content = resp.choices[0].message.content
        usage = getattr(resp, "usage", None)
        if usage:
            pt = usage.prompt_tokens
            ct = usage.completion_tokens
            tt = usage.total_tokens
            st.session_state.tok_prompt += int(pt or 0)
            st.session_state.tok_completion += int(ct or 0)
            st.session_state.tok_total += int(tt or 0)
        return resp, content, dict(prompt_tokens=getattr(usage, "prompt_tokens", 0),
                                   completion_tokens=getattr(usage, "completion_tokens", 0),
                                   total_tokens=getattr(usage, "total_tokens", 0))
    else:
        resp = openai.ChatCompletion.create(
            model=model, messages=messages, temperature=temperature, max_tokens=max_tokens,
        )
        content = resp["choices"][0]["message"]["content"]
        usage = resp.get("usage", {})
        st.session_state.tok_prompt += int(usage.get("prompt_tokens", 0))
        st.session_state.tok_completion += int(usage.get("completion_tokens", 0))
        st.session_state.tok_total += int(usage.get("total_tokens", 0))
        return resp, content, usage

# =========================
# Inference helpers + row enrichment
# =========================
TZ_HINTS = [
    "tanzania", "dar es salaam", "dodoma",
    "tbs", "tmda", "ministry of health", "moh",
    "who tanzania", "nec", "ppm", "nbs", "po-ralg", "tra"
]

TACTIC_MAP = {
    "lobby": "Lobbying",
    "advoc": "Advocacy",
    "sponsor": "Sponsorship",
    "csr": "CSR",
    "misleading": "Misleading advertising",
    "marketing": "Marketing",
    "funding": "Funding research",
    "third-party": "Third-party advocacy",
    "petition": "Petitioning",
    "legal": "Legal threat",
    "lawsuit": "Legal threat",
    "meeting": "Closed-door meeting",
    "press conference": "Press conference",
    "press release": "Press release",
    "campaign": "Public campaign",
}

POLICY_MAP = {
    "ssb": "SSB tax",
    "sugar-sweetened": "SSB tax",
    "front-of-pack": "Front-of-Pack Labelling",
    "fopl": "Front-of-Pack Labelling",
    "nutrition label": "Nutrition labelling",
    "label": "Nutrition labelling",
    "standard": "Food standards",
    "fortification": "Food standards",
    "alcohol": "Alcohol marketing",
    "marketing": "Advertising restrictions",
    "school": "School food policy",
    "tax": "Taxation",
    "advert": "Advertising restrictions",
}

def hard_fill(x: str) -> bool:
    return bool((x or "").strip()) and (x not in ["—", "N/A", "Unspecified", "🔍 Needs review"])

def infer_tactic(text: str) -> str:
    t = text.lower()
    for k, v in TACTIC_MAP.items():
        if k in t:
            return v
    return ""

def infer_policy_area(text: str) -> str:
    t = text.lower()
    for k, v in POLICY_MAP.items():
        if k in t:
            return v
    return ""

def enrich_row(result: dict) -> dict:
    # Normalize whitespace
    for k, v in list(result.items()):
        if isinstance(v, str):
            result[k] = v.replace("\n", " ").strip()

    # 🚫 If Source is a proper URL, keep it (and normalize)
    if looks_like_url(result.get("Source", "")):
        result["Source"] = canonical_url("", result["Source"])

    desc = result.get("Description", "") or ""
    stks = result.get("Stakeholders", "") or ""
    actor = (result.get("Industry Actor(s)", "") or "").strip()
    tactic = (result.get("Industry Tactic", "") or "").strip()
    policy = (result.get("Policy Area", "") or "").strip()
    geo   = (result.get("Geographical Focus", "") or "").strip()
    src   = (result.get("Source", "") or "").strip()

    # --- Actor inference ---
    if not hard_fill(actor):
        if stks:
            actor = stks.split(",")[0].strip()
        else:
            m = re.search(r"\b([A-Z][A-Za-z&\-\.\s]{2,40})\b (advocate|calls|urges|announc|launch|meet)", desc)
            if m:
                actor = m.group(1).strip()
    if hard_fill(actor):
        result["Industry Actor(s)"] = actor

    # --- Tactic inference ---
    if not hard_fill(tactic):
        t_inf = infer_tactic(desc)
        tactic = t_inf or ("Advocacy" if "advoc" in desc.lower() else "")
    if hard_fill(tactic):
        result["Industry Tactic"] = tactic

    # --- Policy Area inference ---
    if not hard_fill(policy):
        p_inf = infer_policy_area(desc)
        if p_inf:
            policy = p_inf
    if hard_fill(policy):
        result["Policy Area"] = policy

    # --- Geography inference ---
    if not hard_fill(geo):
        d = desc.lower()
        s = stks.lower()
        if any(h in d for h in TZ_HINTS) or any(h in s for h in TZ_HINTS):
            geo = "Tanzania"
    if hard_fill(geo):
        result["Geographical Focus"] = geo

    # --- Source inference (only if not a real URL already) ---
    def build_reference_from_text(text: str) -> str:
        txt = text or ""
        m_page = re.search(r"(p\.?\s*\d+|page\s+\d+|pp\.?\s*\d+(?:-\d+)?)", txt, re.I)
        page_str = m_page.group(0) if m_page else ""
        m_date = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s*\d{4}", txt, re.I)
        if not m_date:
            m_date = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}", txt, re.I)
        date_str = m_date.group(0) if m_date else ""
        pub_patterns = [
            r"\b[A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z&\-']+){0,4}\s+(?:Magazine|Newspaper|Journal|Gazette|Bulletin|Times|Daily|News|Report)\b",
            r"\b[A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z&\-']+){1,6}\s+Report\b",
            r"\b[A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z&\-']+){1,6}\s+Press Release\b",
            r"\bMinistry of [A-Z][A-Za-z ]+\b",
            r"\bWorld Health Organization\b|\bWHO\b",
            r"\bTanzania Bureau of Standards\b|\bTBS\b"
        ]
        pub = ""
        for pat in pub_patterns:
            m = re.search(pat, txt)
            if m:
                pub = m.group(0).strip()
                break
        parts = [p for p in [pub, date_str, page_str] if p]
        if parts:
            return ", ".join(parts)
        m_title = re.search(r"[“\"']([^“\"']{5,120})[”\"']", txt)
        if m_title:
            return m_title.group(1).strip()
        return ""

    if not looks_like_url(src):
        if not hard_fill(src):
            m_url = re.search(r"https?://\S+", desc)
            if m_url:
                src = m_url.group(0)
        if not hard_fill(src):
            ref = build_reference_from_text(desc)
            if ref:
                src = ref
        if not hard_fill(src):
            date_hint = (result.get("Date", "") or "").strip() or "undated"
            actor_hint = (result.get("Industry Actor(s)", "") or "").strip() or "Unknown actor"
            src = f"{actor_hint} statement ({date_hint})"
        result["Source"] = src

    # Outcome/Impact
    if not hard_fill(result.get("Outcome/Impact", "")):
        lower = desc.lower()
        if any(k in lower for k in ["aim", "target", "seek", "to "]):
            m = re.search(r"\bto\s+[a-z].{0,140}", lower)
            if m:
                result["Outcome/Impact"] = "Intended: " + m.group(0).strip().capitalize()
            else:
                result["Outcome/Impact"] = "Intended policy/public health effect"
        else:
            result["Outcome/Impact"] = "Intended policy/public health effect"

    # Date guard
    if not hard_fill(result.get("Date", "")):
        m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}", desc, re.I)
        result["Date"] = m.group(0) if m else "Needs review (date)"

    # Stakeholders
    if not hard_fill(result.get("Stakeholders", "")) and result.get("Industry Actor(s)"):
        result["Stakeholders"] = result["Industry Actor(s)"]

    for field in [
        "Date","Industry Actor(s)","Industry Tactic","Description","Stakeholders",
        "Policy Area","Geographical Focus","Outcome/Impact","Source"
    ]:
        if not hard_fill(result.get(field, "")):
            result[field] = "🔍 Needs review"

    return result

# ======================================================
# ====== Registry backends (Sheet + Local JSON)
# ======================================================
def _extract_key_and_gid(url_or_key: str):
    url = str(url_or_key).strip()
    key, gid = url, None
    if "docs.google.com" in url and "/spreadsheets/d/" in url:
        try: key = url.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception: pass
        if "gid=" in url:
            try: gid = int(url.split("gid=")[1].split("&")[0])
            except Exception: gid = None
    return key, gid

def _sheet_key_from_url(url_or_key: str):
    key, _ = _extract_key_and_gid(url_or_key)
    return key

class LocalRegistry:
    """Fallback when the Sheet registry is not accessible (e.g., 403)."""
    def __init__(self, sheet_key: str):
        safe_key = re.sub(r'[^A-Za-z0-9_\-]', '_', sheet_key)[:60]
        self.path = os.path.join(tempfile.gettempdir(), f"processed_registry_{safe_key}.json")
        self._ensure()

    def _ensure(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"fps": []}, f)

    def load(self) -> set:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return set(data.get("fps", []))
        except Exception:
            return set()

    def append(self, rows_fp_url: list):
        if not rows_fp_url:
            return
        try:
            cur = self.load()
            for fp, _url in rows_fp_url:
                cur.add(fp)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"fps": sorted(list(cur))}, f)
        except Exception:
            pass

class SheetRegistry:
    """Registry stored in a Google Sheet tab 'Processed' (or custom)."""
    def __init__(self, gs_client, target_sheet_key: str, tab_title: str):
        self.client = gs_client
        self.key = target_sheet_key
        self.tab = tab_title

    def _open_ws(self):
        sh = self.client.open_by_key(self.key)
        ws = next((w for w in sh.worksheets() if w.title == self.tab), None)
        if ws is None:
            ws = sh.add_worksheet(title=self.tab, rows=1000, cols=3)
            ws.append_row(["fingerprint", "source_url", "first_seen_utc"])
        return ws

    def load(self) -> set:
        try:
            ws = self._open_ws()
            vals = ws.col_values(1)[1:]  # skip header
            return set(v.strip() for v in vals if v.strip())
        except GSpreadAPIError as e:
            st.warning(f"Processed registry not available: {e}")
            raise
        except Exception as e:
            st.warning(f"Processed registry error: {e}")
            raise

    def append(self, rows_fp_url: list):
        if not rows_fp_url:
            return
        try:
            ws = self._open_ws()
            now = pd.Timestamp.utcnow().isoformat(timespec="seconds")
            payload = [[fp, url, now] for fp, url in rows_fp_url]
            CHUNK = 500
            for i in range(0, len(payload), CHUNK):
                ws.append_rows(payload[i:i+CHUNK], value_input_option="USER_ENTERED")
        except Exception as e:
            st.warning(f"Could not write processed registry: {e}")
            raise

def build_registry(preferred_backend: str, data_sheet_url: str):
    sheet_key = _sheet_key_from_url(data_sheet_url)
    reg_sheet_key = PROCESSED_SHEET_KEY or sheet_key

    # Try Sheet registry if preferred or auto
    if preferred_backend in ("auto", "sheet"):
        if client_gsheets is None:
            st.info("Google client not available; using local registry.")
        else:
            try:
                reg = SheetRegistry(client_gsheets, reg_sheet_key, PROCESSED_TAB_TITLE)
                _ = reg.load()  # may raise 403
                st.success("Processed registry: Google Sheet ✅")
                return reg
            except Exception as e:
                if isinstance(e, GSpreadAPIError) and "403" in str(e):
                    st.error("Processed registry permission issue (403). "
                             "Share the registry sheet with the Service Account email shown in the sidebar.")
                else:
                    st.warning("Processed registry (sheet) not available, falling back to local.")
    # Fallback to local JSON
    st.info("Processed registry: Local JSON (temporary) 🗂️")
    return LocalRegistry(sheet_key)

# =========================
# Google Sheets: read & write helpers
# =========================
def load_google_sheet_df(url_or_key: str):
    if not client_gsheets:
        st.error("Google Sheets client not configured. Add GSHEET_JSON to secrets or the credential file.")
        return None
    key, gid = _extract_key_and_gid(url_or_key)
    try:
        sh = client_gsheets.open_by_key(key)
    except GSpreadAPIError as e:
        if "403" in str(e):
            st.error("Permission denied (403). Share the sheet with the Service Account email in the sidebar.")
        else:
            st.error(f"Could not open sheet: {e}")
        return None
    except Exception as e:
        st.error(f"Could not open sheet: {e}")
        return None

    ws = None
    if gid is not None:
        try:
            for w in sh.worksheets():
                if w._properties.get("sheetId") == gid:
                    ws = w; break
        except Exception:
            ws = None
    if ws is None:
        ws = sh.get_worksheet(0)

    try:
        records = ws.get_all_records()
        if records:
            df_sheet = pd.DataFrame(records)
        else:
            values = ws.get_all_values()
            if not values:
                return pd.DataFrame()
            header, rows = values[0], values[1:]
            df_sheet = pd.DataFrame(rows, columns=header)
        df_sheet.columns = [str(c).strip() for c in df_sheet.columns]
        return df_sheet
    except Exception as e:
        st.error(f"Failed to read worksheet: {e}")
        return None

def upload_to_google_sheet(df):
    """Append results to a (fixed) results sheet. Ensure SA has Editor access."""
    if not client_gsheets:
        st.warning("Google Sheets client not available; skipping upload.")
        return
    try:
        sheet = client_gsheets.open_by_key("1N_reX0o4c4_iNJE8PX4Mh6oVVugnpgl6hzDIS2FVsow")
        worksheet = sheet.get_worksheet(0)
        rows = df.astype(str).fillna("").values.tolist()
        CHUNK = 500
        for i in range(0, len(rows), CHUNK):
            worksheet.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")
        st.success("✅ Data uploaded to Google Sheets!")
    except GSpreadAPIError as e:
        if "403" in str(e):
            st.error("Upload failed (403). Share the RESULTS sheet with the Service Account email in the sidebar.")
        else:
            st.error(f"❌ Failed to upload to Google Sheets: {e}")
    except Exception as e:
        st.error(f"❌ Failed to upload to Google Sheets: {e}")

# ======================================================
# 📄 ANALYZE PDF (with extraction PREVIEW before analyze)
# ======================================================
if choice == "Analyze PDF":
    st.title("📄 Analyze PDF for Food Policy & Industry Interference")
    st.markdown("""
    <div style="background:#f4f2ff; padding:12px 16px; border:1px solid #e0d7ff; border-radius:12px;">
    Upload one or more PDFs. The app will extract text, let you preview it by pages & chunks,
    and then produce structured rows in batches.
    </div>
    """, unsafe_allow_html=True)

    uploaded_pdfs = st.file_uploader("📂 Upload PDF file(s)", type=["pdf"], accept_multiple_files=True)

    pdf_model_choice = st.selectbox("🤖 Choose OpenAI Model (PDF)", ["gpt-4o-mini", "gpt-4o"], index=0)
    pdf_batch_size = st.slider("📦 Chunk batch size (items/call)", min_value=3, max_value=25, value=8, step=1)
    pdf_max_chars_per_chunk = st.slider("✂️ Max characters per chunk", min_value=800, max_value=6000, value=2400, step=200)
    pdf_rpm_limit = st.number_input("🧭 RPM limit (requests/min, PDF)", min_value=1, value=3, step=1)

    max_pages = st.number_input("🔒 Max pages per PDF (0 = no limit)", min_value=0, value=0, step=1)
    low_mem_mode = st.checkbox("🧠 Low memory mode (streaming, safer for big PDFs)", value=True)
    if OCR_AVAILABLE:
        force_ocr = st.checkbox("Force OCR for PDFs (slower; requires Tesseract)", value=False)
    else:
        force_ocr = False

    if not api_key:
        st.warning("Add `OPENAI_API_KEY` to your `.streamlit/secrets.toml` to analyze PDFs.")
        st.stop()

    # ---- PDF text extraction helpers ----
    def extract_text_pages(file_like, use_ocr=False, ocr_lang="eng"):
        raw = file_like.read()
        file_like.seek(0)
        pages = []

        # 1) pdfplumber
        try:
            import pdfplumber
            with pdfplumber.open(file_like) as pdf:
                for idx, p in enumerate(pdf.pages):
                    if max_pages and idx >= max_pages:
                        break
                    txt = p.extract_text() or ""
                    if txt.strip():
                        pages.append(txt.strip())
        except Exception:
            pass

        # 2) PyMuPDF
        if not any(pages) and PYMUPDF_AVAILABLE:
            try:
                doc = fitz.open(stream=raw, filetype="pdf")
                for i, page in enumerate(doc):
                    if max_pages and i >= max_pages:
                        break
                    txt = page.get_text("text") or ""
                    if txt.strip():
                        pages.append(txt.strip())
                doc.close()
            except Exception:
                pass

        # 3) OCR (optional)
        if use_ocr and OCR_AVAILABLE and PYMUPDF_AVAILABLE:
            try:
                doc = fitz.open(stream=raw, filetype="pdf")
                new_pages = []
                for i, page in enumerate(doc):
                    if max_pages and i >= max_pages:
                        break
                    if i < len(pages) and pages[i]:
                        new_pages.append(pages[i]); continue
                    pix = page.get_pixmap(dpi=200)
                    from PIL import Image
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    text = pytesseract.image_to_string(img, lang=ocr_lang).strip()
                    new_pages.append(text)
                doc.close()
                pages = new_pages
            except Exception:
                pass

        file_like.seek(0)
        return pages

    def stream_chunker(paragraphs, max_chars):
        buf, total = [], 0
        for p in paragraphs:
            if not p:
                continue
            add = (2 if buf else 0) + len(p)
            if total + add <= max_chars:
                buf.append(p); total += add
            else:
                yield "\n\n".join(buf)
                buf, total = [p], len(p)
        if buf:
            yield "\n\n".join(buf)

    # ---- Extract + build chunks (with preview storage) ----
    chunks = []
    total_pages_all = 0
    st.session_state["pdf_preview_files"] = []  # reset per run

    if uploaded_pdfs:
        with st.spinner("📥 Extracting text from PDF(s)…"):
            for up in uploaded_pdfs:
                pages = extract_text_pages(up, use_ocr=force_ocr)
                st.session_state["pdf_preview_files"].append({"name": up.name, "pages": pages})

                if not any(pages):
                    st.warning(f"⚠️ Could not extract text from: **{up.name}**.")
                    continue

                for page_text in pages:
                    total_pages_all += 1
                    paras = [p.strip() for p in re.split(r"\n{2,}", page_text or "") if p.strip()]
                    for ch in stream_chunker(paras, pdf_max_chars_per_chunk):
                        chunks.append(ch)

    if uploaded_pdfs and not chunks:
        st.error("No extractable text found in the uploaded PDFs.")
        st.stop()

    if uploaded_pdfs:
        st.info(f"🧩 Created **{len(chunks)}** chunks from ~{total_pages_all} page(s).")

        st.subheader("🔍 Preview extracted text")
        st.session_state["pdf_preview_chunks"] = chunks

        tab_chunks, tab_pages, tab_stats = st.tabs(["Chunks", "Pages", "Stats / Download"])

        with tab_chunks:
            st.caption("These are the chunked blocks that will be sent to the model.")
            max_cap_chunks = min(len(chunks), 50)
            max_show_chunks = 1 if max_cap_chunks <= 1 else st.slider(
                "How many chunks to preview",
                min_value=1, max_value=max_cap_chunks, value=min(10, max_cap_chunks), step=1, key="preview_num_chunks",
            )
            for i, ch in enumerate(chunks[:max_show_chunks], start=1):
                with st.expander(f"Chunk {i} • {len(ch)} chars"):
                    st.code(ch[:5000])

        with tab_pages:
            st.caption("Original page-level text per uploaded PDF (pre-chunking).")
            files = st.session_state.get("pdf_preview_files", [])
            if not files:
                st.info("No page previews available.")
            else:
                for f in files:
                    st.markdown(f"**{f['name']}**")
                    total_pages = len(f["pages"])
                    if total_pages == 0:
                        st.write("_No pages with extractable text._")
                        continue
                    max_cap_pages = min(total_pages, 30)
                    safe_name = re.sub(r'[^A-Za-z0-9_\\-]+', "_", f['name'])
                    max_show_pages = 1 if max_cap_pages <= 1 else st.slider(
                        f"Show pages from **{f['name']}**", min_value=1, max_value=max_cap_pages,
                        value=min(total_pages, 5), step=1, key=f"preview_pages_{safe_name}",
                    )
                    for pi, pg in enumerate(f["pages"][:max_show_pages], start=1):
                        with st.expander(f"Page {pi} • {len(pg)} chars"):
                            st.code(pg[:5000])

        with tab_stats:
            st.caption("Quick stats and downloads of the extracted text that will be analyzed.")
            lengths = [len(c) for c in chunks]
            st.write(f"**Total pages (approx):** {total_pages_all}")
            st.write(f"**Total chunks:** {len(chunks)}")
            if lengths:
                st.write(f"**Avg chunk length:** {int(sum(lengths)/len(lengths))} chars")
                st.write(f"**Max chunk length:** {max(lengths)} chars")
                st.write(f"**Min chunk length:** {min(lengths)} chars")
            concat_text = "\\n\\n---CHUNK_BREAK---\\n\\n".join(chunks)
            st.download_button("⬇️ Download extracted text (.txt)", data=concat_text.encode("utf-8"),
                               file_name="extracted_text.txt", mime="text/plain", key="preview_download_txt")

    pdf_prompt_preset = st.selectbox("🧠 PDF prompt preset", ["Original (yours)", "Optimized (shorter)"], index=0)
    PDF_PROMPTS = {
        "Original (yours)": """

        You extract structured records about food policy, NCDs, and industry interference in Tanzania.

        INPUT: JSON array of items with fields: id, date, text, url.
        OUTPUT: JSON object: {"rows": [ ... ]} where each row has EXACTLY:
        1. "Date"                 – use provided date; else extract; else a put today's date; format YYYY-MM-DD or "Month YYYY"
        2. "Industry Actor(s)"    – name the primary actor; if not explicit, select the most central stakeholder mentioned
        3. "Industry Tactic"      – e.g., advocacy, lobbying, CSR, sponsorship, funding research, third-party advocacy, misleading ads; infer if needed
        4. "Description"          – one concise sentence, no line breaks
        5. "Stakeholders"         – comma-separated list of organizations/people involved; if only the actor is present, repeat actor
        6. "Policy Area"          – e.g., SSB tax, front-of-pack labelling, nutrition labelling, food standards, alcohol marketing; infer if needed
        7. "Geographical Focus"   – country/region/city; if Tanzanian entities/regulators appear, use "Tanzania"
        8. "Outcome/Impact"       – result/intended effect; infer if needed
        9. "Source"               – If a URL is provided, copy it. Otherwise, return a clear reference string from the text such as:
                                   "<Publication/Outlet>, <date>, p.<page>" (e.g., "The Citizen, 2024-08-12, p.5");
                                   "<Report/Document title>, <section/page>";
                                   "<Organization> press release <date>";
                                   "<Journal> (<year>), <volume/issue>, <pages>".
                                   Never leave this blank and never write "No URL". Always return the best available reference text.
        10. "Tag"                 – one of:
            - "Food Policy"
            - "Food Policy & Industrial Interference"

        Rules:
        - Return only output about food policy and industry interference in Tanzania.
        - If the input text is NOT about food policy or industry interference, return Tag = "Not related to food/NCD policy" and fill other fields with "—".
        - Use only information present or reasonably inferred from each chunk. Do not invent organizations/quotes/URLs.
        - NEVER output "—", "N/A", or "Unspecified". Always provide the best-supported inference.
        - Keep fields short and clean (no line breaks).
        - Return ONLY valid JSON.
        """
    }
    PDF_SINGLE_CALL_SYSTEM = PDF_PROMPTS[pdf_prompt_preset]

    def pdf_extract_json(text: str):
        try:
            return json.loads(text)
        except Exception:
            m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text or "")
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    return None
        return None

    def pdf_safe_call(messages, max_tokens=1400, temperature=0.0):
        backoff = 8
        for attempt in range(6):
            try:
                _, content, _ = chat_create(pdf_model_choice, messages, temperature=temperature, max_tokens=max_tokens)
                return content
            except Exception as e:
                msg = str(e).lower()
                if "rate limit" in msg or "429" in msg:
                    wait = backoff * (2 ** attempt)
                    st.warning(f"⏳ PDF rate limit: waiting {wait}s (attempt {attempt+1}/6)…")
                    time.sleep(wait); continue
                raise
        raise RuntimeError("Rate limit retries exhausted (PDF).")

    if uploaded_pdfs and st.button("🚀 Analyze PDF"):
        if not api_key:
            st.warning("Add `OPENAI_API_KEY` to secrets to run analysis.")
            st.stop()
        try:
            results = []
            total = len(chunks)
            progress = st.progress(0.0, text="Analyzing PDF chunks…")
            batch_size = pdf_batch_size
            calls_made = 0
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_chunks = chunks[start:end]
                items = [{"id": i, "date": "", "text": ch, "url": ""} for i, ch in enumerate(batch_chunks)]
                payload = json.dumps(items, ensure_ascii=False)
                messages = [{"role": "system", "content": PDF_SINGLE_CALL_SYSTEM},
                            {"role": "user", "content": payload}]
                try:
                    txt = pdf_safe_call(messages, max_tokens=1400, temperature=0.0)
                    calls_made += 1
                    parsed = pdf_extract_json(txt)
                    if not parsed or "rows" not in parsed or not isinstance(parsed["rows"], list):
                        st.warning(f"⚠️ Could not parse AI JSON for batch {start+1}-{end}. Skipping.")
                        progress.progress(end/total, text=f"Analyzed chunks {start+1}–{end} / {total}")
                        continue
                    for r in parsed["rows"]:
                        row = {
                            "Date": r.get("Date", ""),
                            "Industry Actor(s)": r.get("Industry Actor(s)", ""),
                            "Industry Tactic": r.get("Industry Tactic", ""),
                            "Description": r.get("Description", ""),
                            "Stakeholders": r.get("Stakeholders", ""),
                            "Policy Area": r.get("Policy Area", ""),
                            "Geographical Focus": r.get("Geographical Focus", ""),
                            "Outcome/Impact": r.get("Outcome/Impact", ""),
                            "Source": r.get("Source", ""),
                            "Tag": r.get("Tag", "Not related to food/NCD policy"),
                        }
                        row = enrich_row(row)
                        results.append(row)
                except Exception as e:
                    st.error(f"Batch {start+1}-{end} failed: {e}")
                progress.progress(end/total, text=f"Analyzed chunks {start+1}–{end} / {total}")

            st.info(f"🔌 OpenAI calls made: **{calls_made}**")
            if not results:
                st.warning("No structured rows extracted from the PDFs.")
                st.stop()
            df_pdf = pd.DataFrame(results)

            # 🔗 Display-only link column
            df_pdf["SourceURL"] = df_pdf["Source"].where(df_pdf["Source"].apply(looks_like_url), "")

            st.success("✅ PDF analysis complete!")
            st.dataframe(
                df_pdf,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "SourceURL": st.column_config.LinkColumn("Source", display_text="Open"),
                },
                height=420
            )

            db_rows = df_pdf[df_pdf["Tag"].isin(["Food Policy", "Food Policy & Industrial Interference"])].to_dict(orient="records")
            summaries = [f"• {r['Description']} ([source]({r['Source']}))" for r in db_rows]
            if db_rows:
                insert_to_db(db_rows, summaries)
            try:
                upload_to_google_sheet(df_pdf)
            except Exception as e:
                st.warning(f"Google Sheets upload skipped: {e}")
            st.download_button("📥 Download CSV (PDF results)",
                               df_pdf.to_csv(index=False).encode("utf-8"),
                               "pdf_analysis_results.csv", key="final_download_pdf_csv")
            st.subheader("📌 Executive Summary (PDF)")
            st.markdown(f"**Records analyzed:** {len(df_pdf)}")
            for _, r in df_pdf.iterrows():
                src = r.get("Source", "")
                st.markdown(f"- {r.get('Description','—')} ([source]({src}))")

            # show token usage + cost
            PRICES = {
                "gpt-4o-mini": {"input": 0.00015, "output": 0.00060},  # $/1K tokens (update with your plan if needed)
                "gpt-4o":      {"input": 0.00500, "output": 0.01500},
            }
            price = PRICES.get(pdf_model_choice, {"input": 0.0, "output": 0.0})
            pt, ct = st.session_state.tok_prompt, st.session_state.tok_completion
            cost = (pt/1000.0)*price["input"] + (ct/1000.0)*price["output"]
            st.info(f"🔢 Tokens — prompt: {pt:,} | completion: {ct:,} | total: {pt+ct:,}")
            st.success(f"💳 Estimated spend this run: **${cost:,.4f}** on **{pdf_model_choice}**")

        except MemoryError:
            st.error("Out of memory. Lower 'Max pages', lower chunk size, or analyze fewer PDFs.")

# =================================
# 📊 ANALYZE EXCEL (live Google Sheet + upload) with NO manual toggling
# =================================
if choice == "Analyze Excel":
    st.title("Food Policy & Industry Interference Analyzer")
    st.markdown("""
    <div style="background:#f4f2ff; padding:12px 16px; border:1px solid #e0d7ff; border-radius:12px;">
    Use the live Google Sheet or upload a file. No manual checkboxes — choose a selection mode
    (Unseen only, Latest N rows, Date range, Keyword/Column filters). A <b>Processed</b> registry
    prevents re-analyzing the same rows. If you see a 403, share the sheet with the Service Account email in the sidebar.
    </div>
    """, unsafe_allow_html=True)

    # ---------- Load Google Sheet ----------
    if "live_sheet_df" not in st.session_state:
        with st.spinner("Loading Google Sheet…"):
            st.session_state["live_sheet_df"] = load_google_sheet_df(DEFAULT_SHEET_URL)
    if st.button("🔄 Reload Google Sheet"):
        with st.spinner("Refreshing…"):
            st.session_state["live_sheet_df"] = load_google_sheet_df(DEFAULT_SHEET_URL)

    df_live = st.session_state.get("live_sheet_df")

    st.subheader("📄 Live Google Sheet (preview)")
    if isinstance(df_live, pd.DataFrame) and not df_live.empty:
        st.write(f"**Rows:** {len(df_live)}  •  **Columns:** {len(df_live.columns)}")
        sample_rows = min(200, len(df_live))
        st.dataframe(df_live.head(sample_rows), use_container_width=True, height=320)
    else:
        st.warning("Could not load the Google Sheet or it's empty.")

    st.markdown("---")
    # ---------- Upload Excel/CSV (optional) ----------
    st.subheader("📂 OR Upload Excel/CSV")
    uploaded_file = st.file_uploader("Upload Excel or CSV File", type=[".csv", ".xls", ".xlsx"])

    df_upload = None
    if uploaded_file:
        ext = os.path.splitext(uploaded_file.name)[1].lower()
        try:
            if ext == ".csv":
                try:
                    df_upload = pd.read_csv(uploaded_file, encoding="utf-8", on_bad_lines="skip")
                except UnicodeDecodeError:
                    df_upload = pd.read_csv(uploaded_file, encoding="latin1", on_bad_lines="skip")
            elif ext == ".xls":
                df_upload = pd.read_excel(uploaded_file, engine="xlrd")
            else:
                df_upload = pd.read_excel(uploaded_file, engine="openpyxl")
        except Exception as e:
            st.error(f"❌ Could not read file: {e}")
            st.stop()

    # ---------- Choose source ----------
    st.subheader("🎯 Choose data source for analysis")
    source_options = []
    if isinstance(df_live, pd.DataFrame) and not df_live.empty: source_options.append("Google Sheet")
    if isinstance(df_upload, pd.DataFrame): source_options.append("Uploaded file")
    if not source_options: source_options = ["(no data loaded yet)"]
    chosen_source = st.radio("Data source", options=source_options, index=0)

    df = None
    if chosen_source == "Google Sheet" and isinstance(df_live, pd.DataFrame) and not df_live.empty:
        df = df_live.copy()
    elif chosen_source == "Uploaded file" and isinstance(df_upload, pd.DataFrame):
        df = df_upload.copy()
    else:
        st.info("Load the Google Sheet (or upload a file) to proceed.")
        st.stop()

    # ---------- Map columns ----------
    st.subheader("📊 Map Columns")
    st.dataframe(df.head(10), use_container_width=True)
    # choose safe defaults
    col_date = st.selectbox("🗓️ Date Column", df.columns, index=0)
    col_text = st.selectbox("📝 Content Column", df.columns, index=min(1, len(df.columns)-1))
    col_url  = st.selectbox("🔗 Source URL Column", df.columns, index=min(2, len(df.columns)-1))

    # ---------- Selection Mode ----------
    st.markdown("---")
    st.subheader("🎛️ Selection Mode (no manual checking)")

    selection_mode = st.radio(
        "Pick how you want to choose rows:",
        [
            "Unseen only (recommended)",
            "Latest N rows",
            "Date range",
            "Keyword filter (in Content column)",
            "Column contains…"
        ],
        index=0
    )

    candidate_df = df.copy()

    # ---------- Freshness / Bypass ----------
    force_fresh = st.toggle("♻️ Force fresh analysis (ignore cache & registry)", value=False)

    if selection_mode == "Latest N rows":
        maxn = len(candidate_df)
        n = st.number_input("How many latest rows?", min_value=1, max_value=maxn, value=min(500, maxn), step=1)
        candidate_df = candidate_df.tail(int(n)).copy()

    elif selection_mode == "Date range":
        parsed_dates = pd.to_datetime(candidate_df[col_date], errors="coerce")
        min_d, max_d = parsed_dates.min(), parsed_dates.max()
        if pd.isna(min_d) or pd.isna(max_d):
            st.warning("Could not parse dates in the selected Date column. Using all rows.")
        else:
            default_start = max_d - timedelta(days=30)
            start_date, end_date = st.date_input(
                "Pick date range", value=(default_start.date(), max_d.date())
            )
            mask = (parsed_dates.dt.date >= start_date) & (parsed_dates.dt.date <= end_date)
            candidate_df = candidate_df[mask].copy()

    elif selection_mode == "Keyword filter (in Content column)":
        kw_input = st.text_input("Keywords (comma separated, case-insensitive)", value="")
        if kw_input.strip():
            kws = [k.strip() for k in kw_input.split(",") if k.strip()]
            pattern = "|".join(re.escape(k) for k in kws)
            candidate_df = candidate_df[candidate_df[col_text].astype(str).str.contains(pattern, case=False, na=False)].copy()

    elif selection_mode == "Column contains…":
        col_choice = st.selectbox("Choose column", options=list(candidate_df.columns), index=0, key="col_contains_choice")
        substr = st.text_input("Substring (case-insensitive)", value="")
        if substr.strip():
            candidate_df = candidate_df[candidate_df[col_choice].astype(str).str.contains(re.escape(substr), case=False, na=False)].copy()

    # ---------- Registry selection & dedup ----------
    registry = build_registry(REGISTRY_BACKEND_PREF, DEFAULT_SHEET_URL)

    st.markdown("---")
    st.subheader("🧹 Dedup before analysis")
    skip_processed = st.checkbox("Skip rows already processed (recommended)", value=True)
    if force_fresh:
        skip_processed = False  # override when forcing fresh

    # Compute fingerprints
    def _normalize(s): return re.sub(r"\s+", " ", str(s or "")).strip().lower()
    def row_fingerprint(date_val, text_val, url_val):
        raw = "|".join([_normalize(date_val), _normalize(text_val), _normalize(url_val)])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    candidate_df["__fp__"] = candidate_df.apply(lambda r: row_fingerprint(r.get(col_date, ""), r.get(col_text, ""), r.get(col_url, "")), axis=1)
    candidate_df["__url__"] = candidate_df[col_url].astype(str) if col_url in candidate_df.columns else ""

    skipped = 0
    if skip_processed:
        try:
            processed_set = registry.load()
        except Exception:
            processed_set = set()
        before = len(candidate_df)
        candidate_df = candidate_df[~candidate_df["__fp__"].isin(processed_set)].copy()
        skipped = before - len(candidate_df)

    st.info(f"Will analyze **{len(candidate_df)}** row(s). Skipped **{skipped}** already processed row(s).")
    if len(candidate_df) == 0:
        st.stop()

    # ---------- Cap rows this run ----------
    st.markdown("---")
    st.subheader("🪫 Cap rows this run")
    max_rows = max(1, len(candidate_df))
    cap = st.number_input("Maximum rows to process now", min_value=1, max_value=max_rows, value=min(1000, max_rows), step=1)
    process_newest_first = st.toggle("Process newest first", value=True)
    if process_newest_first:
        candidate_df = candidate_df.tail(int(cap)).copy()
    else:
        candidate_df = candidate_df.head(int(cap)).copy()

    # ---------- Prompts & helpers ----------
    st.markdown("---")
    st.subheader("🧠 Prompt preset")
    prompt_preset = st.selectbox(
        "Choose prompt wording",
        ["Original (yours)", "Optimized (shorter)"],
        index=0
    )
    PROMPTS = {
    "Original (yours)": {
        "FILTER_POLICY_PROMPT": """
Is this text related to any of the following?
- Tobacco products, cigarette manufacturing, import/export, or marketing;
- Tobacco control, regulation, taxation, advertising bans, packaging/labelling, smoke-free laws, illicit trade, or enforcement actions;
- Public health or NCDs (e.g., cancer, CVD, respiratory diseases) caused or influenced by tobacco use;
- Policy, regulation, guidelines, enforcement, or updates from tobacco-related regulators (TBS, MOH, TFDA, TRA, WHO FCTC)?
Respond with Yes or No only.
""",
        "FILTER_INTERF_PROMPT": """
Does the text show tobacco industry influence on policy or public perception?
Examples include lobbying, CSR, sponsorship, marketing, public relations, meetings with regulators,
voluntary codes, front groups, litigation threats, funding research, or spreading misinformation.
Respond with Yes or No only.
""",
        "SYSTEM_PROMPT_EXTRACT": """
You are an expert analyst extracting structured records about tobacco products, tobacco control policies, and tobacco industry interference in Tanzania.

Inputs per row:
- Date (may be empty or noisy)
- Content text (event/news narrative)
- Source URL (may be empty)

OUTPUT: exactly 9 fields separated by `|` in this order:
1. Date — Use the provided date if plausible; otherwise extract from text if available; otherwise write a best-guess like 'July 2025' (never leave blank).
2. Industry Actor(s) — Name the main tobacco actor (e.g., Tanzania Cigarette Company, British American Tobacco, local distributors, lobby groups). If not explicit, infer the most central stakeholder.
3. Industry Tactic — Choose a specific tactic (e.g., lobbying, CSR, marketing, sponsorship, litigation, misleading health claims, third-party advocacy, funding research). Infer from context if not explicit.
4. Description — One concise sentence (no line breaks) summarizing what happened.
5. Stakeholders — List all named parties (comma-separated). If only the actor is present, repeat the actor here.
6. Policy Area — e.g., tobacco tax, advertising bans, packaging/labelling, smoke-free laws, illicit trade, or enforcement of tobacco control legislation. Infer the most relevant one if implicit.
7. Geographical Focus — Country/region/city involved. If not explicit but Tanzanian entities appear, use "Tanzania".
8. Outcome/Impact — The result or intended effect (e.g., delayed regulation, strengthened enforcement, policy adoption, public backlash, influence on perception).
9. Source — If a URL is provided, copy it. Otherwise, return a clear reference string from the text such as:
   "<Publication/Outlet>, <date>, p.<page>";
   "<Report/Document title>, <section/page>";
   "<Organization> press release <date>";
   "<Journal> (<year>), <volume/issue>, <pages>".
   Never leave this blank and never write "No URL". Always return the best available reference text.

You MUST fill every field (never output "—", "N/A", or "Unspecified").
If the text is unrelated to tobacco policy or tobacco industry interference, SKIP the row entirely (return nothing).
"""
    },
    "Optimized (shorter)": {
        "FILTER_POLICY_PROMPT": "Is the text about tobacco products, tobacco control, taxation, packaging/labelling, smoke-free laws, or enforcement (e.g., TBS/MOH/TRA)? Reply Yes or No.",
        "FILTER_INTERF_PROMPT": "Does it show tobacco industry influence (lobbying, CSR, marketing, front groups, litigation, research funding, or misinformation)? Reply Yes or No.",
        "SYSTEM_PROMPT_EXTRACT": "Extract 9 fields separated by `|`: Date | Industry Actor(s) | Industry Tactic | Description | Stakeholders | Policy Area | Geographical Focus | Outcome/Impact | Source. Focus only on tobacco policy or tobacco industry interference; infer when reasonable; skip unrelated rows."
    }
}

    FILTER_POLICY_PROMPT   = PROMPTS[prompt_preset]["FILTER_POLICY_PROMPT"]
    FILTER_INTERF_PROMPT   = PROMPTS[prompt_preset]["FILTER_INTERF_PROMPT"]
    SYSTEM_PROMPT_EXTRACT  = PROMPTS[prompt_preset]["SYSTEM_PROMPT_EXTRACT"]

    model_choice = st.selectbox("🤖 Choose OpenAI Model", ["gpt-4o-mini", "gpt-4o"], index=0)

    # estimates are illustrative only; real usage is shown after run
    estimated_cost = len(candidate_df) * 0.002
    estimated_time = len(candidate_df) * 6
    st.info(f"⏳ Rough estimate — time: {estimated_time//60} min | cost: ~${estimated_cost:.2f} (actual shown after run)")

    # ---------- Caching ----------
    cache_key = hashlib.md5((
        candidate_df.to_csv(index=False).encode("utf-8")
        + model_choice.encode()
        + str(SYSTEM_PROMPT_EXTRACT).encode()
        + str(FILTER_POLICY_PROMPT).encode()
        + str(FILTER_INTERF_PROMPT).encode()
    )).hexdigest()

    if "analysis_cache" not in st.session_state:
        st.session_state["analysis_cache"] = {}
    session_cache = st.session_state["analysis_cache"]

    temp_dir = tempfile.gettempdir()
    cache_path = os.path.join(temp_dir, f"cache_{cache_key}.json")

    st.caption(f"Cache key: `{cache_key[:12]}…`  |  Path: `{cache_path}`")
    if st.button("🧹 Clear disk cache for this selection"):
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
                st.success("Disk cache cleared for this selection.")
            if cache_key in session_cache:
                del session_cache[cache_key]
                st.success("Session cache cleared for this selection.")
        except Exception as e:
            st.warning(f"Could not clear cache: {e}")

    if (not force_fresh) and (cache_key in session_cache):
        df_out = session_cache[cache_key]
        df_out["SourceURL"] = df_out["Source"].where(df_out["Source"].apply(looks_like_url), "")
        st.success("✅ Using session cache.")
        st.dataframe(
            df_out,
            use_container_width=True,
            hide_index=True,
            column_config={"SourceURL": st.column_config.LinkColumn("Source", display_text="Open")},
        )
        st.download_button("📥 Download CSV", df_out.to_csv(index=False).encode("utf-8"),
                           "analysis_results.csv", key="final_download_excel_csv")
        st.subheader("📌 Executive Summary")
        for _, row in df_out.iterrows():
            st.markdown(f"• {row['Description']} ([source]({row['Source']}))")
        st.stop()
    elif (not force_fresh) and os.path.exists(cache_path):
        try:
            df_out = pd.read_json(cache_path)
            df_out["SourceURL"] = df_out["Source"].where(df_out["Source"].apply(looks_like_url), "")
            session_cache[cache_key] = df_out
            st.success("✅ Loaded previous cached analysis.")
            st.dataframe(
                df_out,
                use_container_width=True,
                hide_index=True,
                column_config={"SourceURL": st.column_config.LinkColumn("Source", display_text="Open")},
            )
            st.download_button("📥 Download CSV", df_out.to_csv(index=False).encode("utf-8"),
                               "analysis_results.csv", key="final_download_excel_csv")
            st.subheader("📌 Executive Summary")
            for _, row in df_out.iterrows():
                st.markdown(f"• {row['Description']} ([source]({row['Source']}))")
            st.stop()
        except Exception as e:
            st.warning(f"Cache exists but could not be read: {e}")

    # ---------- OpenAI call helper ----------
    def safe_openai_call(payload_func, retries=3):
        for attempt in range(retries):
            try:
                return payload_func()
            except Exception as e:
                msg = str(e).lower()
                if "rate limit" in msg or "429" in msg:
                    wait_time = 20 * (attempt + 1)
                    st.warning(f"⏳ Rate limited. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                raise
        raise Exception("❌ Exceeded retry limit for OpenAI calls.")

    # strict yes/no helper
    def _yn(raw: str) -> str:
        s = (raw or "").strip().lower()
        s_norm = re.sub(r'[^a-z]', '', s)
        if s_norm == "yes":
            return "yes"
        if s_norm == "no":
            return "no"
        # fallback exact word boundary
        if re.search(r'\byes\b', s):
            return "yes"
        if re.search(r'\bno\b', s):
            return "no"
        return "unknown"

    # ---------- Start analysis ----------
    if not api_key:
        st.warning("Add `OPENAI_API_KEY` to `.streamlit/secrets.toml` to run analysis.")
        st.stop()

    if st.button("🚀 Start Batch Analysis"):
        results, summaries = [], []
        batch_size = 10
        total_rows = len(candidate_df)
        progress = st.progress(0, text="Analyzing data...")
        attempted_fp_url = []
        calls_made = 0
        skipped_policy_no = 0
        skipped_empty_text = 0

        # reset token counters for this run
        st.session_state.tok_prompt = 0
        st.session_state.tok_completion = 0
        st.session_state.tok_total = 0

        for batch_start in range(0, total_rows, batch_size):
            batch = candidate_df.iloc[batch_start:batch_start+batch_size]
            progress.progress(min(batch_start+batch_size, total_rows)/total_rows,
                              text=f"Batch {batch_start+1} - {min(batch_start+batch_size, total_rows)}")

            for i, row in enumerate(batch.iterrows(), start=1):
                idx, row = row
                try:
                    date = str(row.get(col_date, ""))
                    text_content = str(row.get(col_text, ""))
                    url_val = str(row.get(col_url, ""))

                    if not text_content.strip():
                        skipped_empty_text += 1
                        continue

                    attempted_fp_url.append((row["__fp__"], row["__url__"]))  # mark attempted

                    # Clean/validate provided URL
                    url_val_clean = canonical_url("", url_val) if looks_like_url(url_val) else ""

                    # 1) Policy relevance
                    def _rel():
                        return chat_create(
                            model_choice,
                            [{"role": "system", "content": "You are a binary classifier."},
                             {"role": "user", "content": FILTER_POLICY_PROMPT + f"\nText: {text_content}"}],
                            temperature=0, max_tokens=20
                        )
                    _, rel_content, _ = safe_openai_call(_rel)
                    calls_made += 1
                    rel = _yn(rel_content)
                    if rel != "yes":
                        skipped_policy_no += 1
                        continue

                    # 2) Interference?
                    def _interf():
                        return chat_create(
                            model_choice,
                            [{"role": "system", "content": "You are a binary classifier."},
                             {"role": "user", "content": FILTER_INTERF_PROMPT + f"\nText: {text_content}"}],
                            temperature=0, max_tokens=20
                        )
                    _, interf_content, _ = safe_openai_call(_interf)
                    calls_made += 1
                    interf = _yn(interf_content)
                    tag = "Food Policy & Industrial Interference" if interf == "yes" else "Food Policy"

                    # 3) Extract fields
                    def _extract():
                        return chat_create(
                            model_choice,
                            [{"role": "user", "content": SYSTEM_PROMPT_EXTRACT + f"\n\nText: {text_content}\nDate: {date}\nSource: {url_val_clean or url_val}"}],
                            temperature=0, max_tokens=512
                        )
                    _, extract_content, _ = safe_openai_call(_extract)
                    calls_made += 1

                    parts = (extract_content or "").strip().split("|")
                    if len(parts) == 9:
                        def clean_result_field(value):
                            val = (value or "").strip()
                            return val if val and val not in ["—", "N/A", "Unspecified"] else ""
                        row_out = {
                            "Date": clean_result_field(parts[0]),
                            "Industry Actor(s)": clean_result_field(parts[1]),
                            "Industry Tactic": clean_result_field(parts[2]),
                            "Description": clean_result_field(parts[3]),
                            "Stakeholders": clean_result_field(parts[4]),
                            "Policy Area": clean_result_field(parts[5]),
                            "Geographical Focus": clean_result_field(parts[6]),
                            "Outcome/Impact": clean_result_field(parts[7]),
                            "Source": clean_result_field(parts[8]),
                            "Tag": tag
                        }
                        row_out = enrich_row(row_out)

                        # ✅ Always prefer the sheet’s real link when present; don't overwrite true URLs
                        if looks_like_url(url_val_clean):
                            row_out["Source"] = url_val_clean

                        summary = f"• {row_out['Description']} ([source]({row_out['Source']}))"
                        results.append(row_out)
                        summaries.append(summary)
                except Exception as e:
                    st.warning(f"⚠️ Row {idx+1}: {str(e)}")
                    time.sleep(0.3)

        # Mark processed rows (anything attempted) unless forcing fresh
        if skip_processed and attempted_fp_url:
            try:
                registry.append(attempted_fp_url)
                st.success(f"📌 Marked {len(attempted_fp_url)} row(s) as processed.")
            except Exception:
                st.warning("Could not update processed registry. (Using local cache or fix sheet permissions.)")

        st.info(f"🔌 OpenAI calls made: **{calls_made}**")
        st.info(f"⛔ Skipped — empty text: {skipped_empty_text} | policy=No: {skipped_policy_no}")

        if results:
            df_out = pd.DataFrame(results)

            # 🔗 Display-only link column
            df_out["SourceURL"] = df_out["Source"].where(df_out["Source"].apply(looks_like_url), "")

            # Save to session + temp cache
            session_cache[cache_key] = df_out
            try:
                df_out.to_json(cache_path, orient="records")
            except Exception as e:
                st.warning(f"Could not write temp cache: {e}")

            # DB insert (optional)
            try:
                insert_to_db(results, summaries)
            except Exception as e:
                st.warning(f"DB insert skipped: {e}")

            # Google Sheets append (optional)
            try:
                upload_to_google_sheet(df_out)
            except Exception as e:
                st.warning(f"Google Sheets upload skipped: {e}")

            st.success("✅ Analysis complete!")
            st.dataframe(
                df_out,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "SourceURL": st.column_config.LinkColumn("Source", display_text="Open"),
                },
            )
            st.download_button("📥 Download CSV", df_out.to_csv(index=False).encode("utf-8"),
                               "analysis_results.csv", key="final_download_excel_csv")
            st.subheader("📌 Executive Summary")
            for item in summaries:
                st.markdown(item)
        else:
            st.warning("No relevant data extracted. Here's a quick keyword overview from your selected text:")
            try:
                topics = candidate_df[col_text].astype(str).str.lower().str.extractall(
                    r'(food|beverage|nutrition|alcohol|standard|training|tbs|policy|health|product|regulation)'
                )[0]
                if topics.empty:
                    st.markdown("- No common policy-related keywords were found in the content column.")
                else:
                    topic_summary = topics.value_counts().head(10)
                    for topic, count in topic_summary.items():
                        st.markdown(f"- **{topic.capitalize()}** mentioned **{count}** times")
            except Exception:
                st.markdown("- (Could not compute keyword overview.)")

        # show token usage + cost
        PRICES = {
            "gpt-4o-mini": {"input": 0.00015, "output": 0.00060},  # $/1K tokens (update with your plan if needed)
            "gpt-4o":      {"input": 0.00500, "output": 0.01500},
        }
        price = PRICES.get(model_choice, {"input": 0.0, "output": 0.0})
        pt, ct = st.session_state.tok_prompt, st.session_state.tok_completion
        cost = (pt/1000.0)*price["input"] + (ct/1000.0)*price["output"]
        st.info(f"🔢 Tokens — prompt: {pt:,} | completion: {ct:,} | total: {pt+ct:,}")
        st.success(f"💳 Estimated spend this run: **${cost:,.4f}** on **{model_choice}**")
