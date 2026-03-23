"""
Pure-function helpers for extracting lease terms from Entrata API responses.

This module is intentionally isolated from the existing reconciliation flow.
All I/O behavior is dependency-injected via callables so callers can plug in
their own API helper, PDF parser, and field extraction logic.
"""

from __future__ import annotations

import base64
from datetime import datetime
from datetime import timedelta
import hashlib
import importlib
import json
import logging
import os
import re
import shutil
import string
import time
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence

import pandas as pd
import requests

from .canonical_fields import CanonicalField
from .lease_term_rules import (
    format_ar_code_display,
    get_primary_ar_code_for_term,
    get_term_to_ar_code_rules,
)
from .lease_term_extraction_rules import get_term_extraction_rule


def _load_dotenv_if_available() -> None:
    try:
        dotenv_module = importlib.import_module("dotenv")
        load_fn = getattr(dotenv_module, "load_dotenv", None)
        if callable(load_fn):
            load_fn()
    except Exception:
        pass


def _load_pymupdf_if_available():
    try:
        return importlib.import_module("fitz")
    except Exception:
        return None


def _load_pdfplumber_if_available():
    try:
        return importlib.import_module("pdfplumber")
    except Exception:
        return None


_load_dotenv_if_available()
fitz = _load_pymupdf_if_available()
pdfplumber = _load_pdfplumber_if_available()
logger = logging.getLogger(__name__)


JsonDict = Dict[str, Any]
Pair = tuple[str, str]
ApiFetcher = Callable[[Mapping[str, Any]], Mapping[str, Any]]
FieldExtractor = Callable[[Mapping[str, Any]], Mapping[str, Any]]


API_KEY = os.environ.get("ENTRATA_API_KEY")
ORG = os.environ.get("ENTRATA_ORG", "peakmade")
BASE_URL = f"https://apis.entrata.com/ext/orgs/{ORG}/v1/leases"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY,
}

OUT_DIR = os.environ.get("OUT_DIR", r"C:\Users\svanorder\Downloads\EntrataLeases")
SAVE_LOCAL_ENTRATA_PDFS = os.environ.get("SAVE_LOCAL_ENTRATA_PDFS", "false").lower() == "true"
if SAVE_LOCAL_ENTRATA_PDFS:
    os.makedirs(OUT_DIR, exist_ok=True)
PROPERTY_ID = os.environ.get("ENTRATA_DEFAULT_PROPERTY_ID")
LEASE_ID = os.environ.get("ENTRATA_DEFAULT_LEASE_ID")
PICKLIST_CACHE = {
    "leaseFileTypes": {}
}


def post_entrata(payload: dict, url: str = None) -> dict:
    """Send POST request to Entrata API."""
    target_url = url or BASE_URL
    logger.info("=" * 80)
    logger.info("POST REQUEST TO ENTRATA API")
    logger.info(f"URL: {target_url}")
    logger.info(f"Payload: {json.dumps(payload, indent=2)}")

    resp = requests.post(target_url, headers=HEADERS, json=payload, timeout=60)
    resp.raise_for_status()

    response_json = resp.json()
    logger.info(f"Response Status: {resp.status_code}")
    logger.info(f"Response: {json.dumps(response_json, indent=2)}")
    logger.info("=" * 80)

    return response_json


def fetch_lease_picklist() -> bool:
    """Fetch only lease file types needed by document selection logic."""
    global PICKLIST_CACHE

    try:
        logger.info("Fetching lease picklist file types...")

        payload = {
            "auth": {"type": "apikey"},
            "requestId": "picklist",
            "method": {"name": "getLeasePickList"}
        }

        response = post_entrata(payload)
        result = response.get("response", {}).get("result", {})

        file_types = result.get("leaseFileTypes", {}).get("leaseFileType", [])
        if isinstance(file_types, dict):
            file_types = [file_types]

        PICKLIST_CACHE["leaseFileTypes"] = {
            str(item["@attributes"]["id"]): {
                "name": item["@attributes"].get("name", ""),
                "systemCode": item["@attributes"].get("systemCode", "")
            }
            for item in file_types
            if isinstance(item, dict) and "@attributes" in item
        }

        logger.info(f"Cached {len(PICKLIST_CACHE['leaseFileTypes'])} lease file types")
        return True

    except Exception as e:
        logger.error(f"Failed to fetch lease file types: {e}")
        return False


def get_file_type_name(file_type_id) -> str:
    """Get human-readable file type name from ID."""
    if file_type_id is None:
        return ""

    file_type_id_str = str(file_type_id)

    if not PICKLIST_CACHE.get("leaseFileTypes"):
        fetch_lease_picklist()

    return PICKLIST_CACHE.get("leaseFileTypes", {}).get(
        file_type_id_str, {}
    ).get("name", f"File Type ID {file_type_id}")


def ensure_file_type_cache_loaded() -> None:
    """Call once at startup in non-Flask usage."""
    if not PICKLIST_CACHE.get("leaseFileTypes"):
        fetch_lease_picklist()


def parse_mmddyyyy(s: str) -> datetime:
    """Parse date string in MM/DD/YYYY format."""
    return datetime.strptime(s, "%m/%d/%Y")


def get_doc_code(doc: dict) -> str:
    """Extract trailing document code from Type/type string (e.g., LP, OTHER)."""
    type_text = (doc.get("Type") or doc.get("type") or "").strip()
    if not type_text:
        return ""

    match = re.search(r"\((?:[^()]*)-\s*([A-Za-z0-9_]+)\)\s*$", type_text)
    if match:
        return match.group(1).upper()

    fallback_match = re.search(r"\(([A-Za-z0-9_]+)\)\s*$", type_text)
    if fallback_match:
        return fallback_match.group(1).upper()

    return ""


def is_signed(doc: dict) -> bool:
    """Best-effort signed detection using status first, then legacy title/type fallback."""
    status_keys = [
        "Status", "status", "DocumentStatus", "documentStatus", "statusName", "StatusName"
    ]
    status_val = None
    for key in status_keys:
        value = doc.get(key)
        if value is not None and str(value).strip():
            status_val = str(value)
            break

    if status_val is not None:
        status_upper = status_val.upper()
        return ("SIGNED" in status_upper) or ("EXECUTED" in status_upper)

    type_text = (doc.get("Type") or doc.get("type") or "")
    title_text = (doc.get("Title") or doc.get("title") or "")
    return ("SIGNED" in type_text) or ("Signed" in title_text)


def parse_doc_datetime(value) -> datetime:
    """Best-effort parser for Entrata document date/time fields."""
    if value is None:
        return datetime.min

    raw = str(value).strip()
    if not raw:
        return datetime.min

    try:
        return parse_mmddyyyy(raw)
    except Exception:
        pass

    cleaned = re.sub(r"\s+[A-Z]{2,5}$", "", raw)
    formats = [
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue

    return datetime.min


def parse_doc_name_timestamp(doc: dict) -> datetime:
    """Extract sortable timestamp from Entrata document name when available."""
    name = (doc.get("name") or doc.get("Name") or "").strip()
    if not name:
        return datetime.min

    ymd_match = re.search(r"(20\d{12})", name)
    if ymd_match:
        try:
            return datetime.strptime(ymd_match.group(1), "%Y%m%d%H%M%S")
        except ValueError:
            pass

    epoch_match = re.search(r"(?:_|\b)(1\d{9})(?:\D|$)", name)
    if epoch_match:
        try:
            return datetime.fromtimestamp(int(epoch_match.group(1)))
        except (OverflowError, OSError, ValueError):
            pass

    return datetime.min


def get_doc_id(doc: dict) -> str:
    """Get document ID from known fields."""
    doc_id_raw = doc.get("Id") or doc.get("id")
    if "@attributes" in doc and isinstance(doc["@attributes"], dict) and "Id" in doc["@attributes"]:
        doc_id_raw = doc["@attributes"]["Id"]
    return str(doc_id_raw) if doc_id_raw is not None else ""


def get_doc_title(doc: dict) -> str:
    """Get document title from known fields."""
    return (doc.get("Title") or doc.get("title") or "").strip()


def get_doc_activity_timestamp(doc: dict) -> datetime:
    """Get sortable timestamp for comparing doc recency."""
    added_on = parse_doc_datetime(doc.get("AddedOn") or doc.get("addedOn"))
    modified_on = parse_doc_datetime(doc.get("ModifiedOn") or doc.get("modifiedOn"))
    name_ts = parse_doc_name_timestamp(doc)
    return max(added_on, modified_on, name_ts)


def get_doc_numeric_id(doc: dict) -> int:
    """Get numeric doc ID for recency fallback when timestamps are missing."""
    doc_id = get_doc_id(doc)
    try:
        return int(str(doc_id))
    except (TypeError, ValueError):
        return 0


def get_doc_recency_key(doc: dict) -> tuple:
    """Build a robust recency key using timestamps and doc ID fallback."""
    activity_ts = get_doc_activity_timestamp(doc)
    name_ts = parse_doc_name_timestamp(doc)
    doc_id_num = get_doc_numeric_id(doc)
    return (activity_ts, name_ts, doc_id_num)


def get_doc_declared_lease_start(doc: Mapping[str, Any]) -> datetime:
    """Get declared lease interval start date from doc metadata."""
    return parse_doc_datetime((doc or {}).get("leaseIntervalStartDate"))


def get_doc_declared_file_size(doc: Mapping[str, Any]) -> int:
    """Best-effort parse of file size metadata for tie-breaking."""
    candidates = [
        (doc or {}).get("FileSize"),
        (doc or {}).get("fileSize"),
        (doc or {}).get("Size"),
        (doc or {}).get("size"),
    ]
    for value in candidates:
        if value is None:
            continue
        try:
            parsed = int(float(value))
            if parsed >= 0:
                return parsed
        except Exception:
            continue
    return 0


def _coerce_period_datetime(value: Any) -> datetime | None:
    """Coerce period boundary input to datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def is_signed_addendum(doc: dict) -> bool:
    """Return True when doc appears to be an addendum eligible for inclusion."""
    title_text = get_doc_title(doc).lower()
    type_text = (doc.get("Type") or doc.get("type") or "").lower()
    file_type_id = doc.get("FileType") or doc.get("fileType") or doc.get("fileTypeId")
    file_type_name = get_file_type_name(file_type_id).lower() if file_type_id is not None else ""

    searchable = " | ".join([title_text, type_text, file_type_name])
    has_addendum_signal = ("addendum" in searchable) or ("addenda" in searchable)
    has_esign_addenda_signal = ("e-sign: addenda" in searchable) or ("esign: addenda" in searchable)

    if has_esign_addenda_signal:
        return True

    if has_addendum_signal and is_signed(doc):
        return True

    return False


def is_floorplan_rate_addendum(doc: dict) -> bool:
    """Return True when doc appears to be a Floor Plan Rate Addendum."""
    title_text = get_doc_title(doc).lower()
    type_text = (doc.get("Type") or doc.get("type") or "").lower()
    file_type_id = doc.get("FileType") or doc.get("fileType") or doc.get("fileTypeId")
    file_type_name = get_file_type_name(file_type_id).lower() if file_type_id is not None else ""

    searchable = " | ".join([title_text, type_text, file_type_name])
    return bool(re.search(r"floor\s*plan\s*rate\s*addendum|floorplan\s*rate\s*addendum", searchable, re.IGNORECASE))


def get_addendum_name_key(doc: dict) -> str:
    """Build a normalized addendum name key for deduping same-named addenda only."""
    title_text = get_doc_title(doc).lower()
    type_text = (doc.get("Type") or doc.get("type") or "").lower()
    file_type_id = doc.get("FileType") or doc.get("fileType") or doc.get("fileTypeId")
    file_type_name = get_file_type_name(file_type_id).lower() if file_type_id is not None else ""

    base_name = title_text if title_text else " ".join([type_text, file_type_name])

    combined = base_name
    combined = re.sub(r"e[-\s]?sign", " ", combined)
    combined = re.sub(r"addenda?|document|signed|executed", " ", combined)
    combined = re.sub(r"[^a-z0-9\s]+", " ", combined)
    combined = re.sub(r"\s+", " ", combined).strip()

    if not combined:
        doc_id = get_doc_id(doc) or "unknown"
        return f"unknown_{doc_id}"

    return combined


def safe_filename(name: str) -> str:
    """Clean filename by removing invalid characters."""
    return re.sub(r"[\\/:*?\"<>|]+", "_", name).strip()


def _json_safe_for_logging(value: Any) -> Any:
    """Convert non-JSON-safe values (like bytes) into lightweight log-safe forms."""
    if isinstance(value, (bytes, bytearray)):
        return f"<bytes:{len(value)}>"
    if isinstance(value, dict):
        return {k: _json_safe_for_logging(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_for_logging(v) for v in value]
    return value


def normalize_money(money_str: str) -> str | None:
    """Normalize money string to clean decimal format.

    Args:
        money_str: Money string like '$1,110.00' or '1,110.00'

    Returns:
        Clean decimal string like '1110.00' or None if parsing fails
    """
    if not money_str:
        return None

    cleaned = re.sub(r'[$,\s]', '', money_str.strip())

    try:
        float(cleaned)
        return cleaned
    except ValueError:
        logger.warning(f"Could not normalize money: {money_str}")
        return None


def parse_pdf_to_text_pack(pdf_source: str | bytes | bytearray) -> dict:
    """Parse PDF and extract text using PyMuPDF (respects ToUnicode for Type0/Type3 fonts).

    This uses PyMuPDF which handles embedded fonts with proper ToUnicode mappings,
    and improves extraction from embedded-font PDFs.

    Args:
        pdf_source: Path to a PDF file or PDF bytes

    Returns:
        dict with:
            - total_pages: int
            - pages: list of dicts with page_number, char_count, text, preview, words
    """
    result = {
        "total_pages": 0,
        "pages": []
    }

    if fitz is None:
        message = "PyMuPDF (fitz) is not installed in the active environment"
        logger.error(message)
        result["error"] = message
        return result

    try:
        if isinstance(pdf_source, (bytes, bytearray)):
            logger.info("Extracting text from PDF using PyMuPDF from in-memory bytes")
            doc = fitz.open(stream=bytes(pdf_source), filetype="pdf")
        else:
            logger.info(f"Extracting text from PDF using PyMuPDF: {pdf_source}")
            doc = fitz.open(str(pdf_source))
        result["total_pages"] = len(doc)

        def _score_text_candidate(candidate_text: str) -> float:
            text_value = str(candidate_text or "")
            if not text_value.strip():
                return -1.0

            length = len(text_value)
            alpha_count = sum(1 for ch in text_value if ch.isalpha())
            digit_count = sum(1 for ch in text_value if ch.isdigit())
            alnum_count = alpha_count + digit_count
            symbol_count = sum(1 for ch in text_value if not ch.isalnum() and not ch.isspace())

            alnum_ratio = alnum_count / max(1, length)
            symbol_ratio = symbol_count / max(1, length)

            keyword_hits = len(re.findall(
                r"rent|lease|term|amount|installment|monthly|start|end|date|application|administration",
                text_value,
                re.IGNORECASE,
            ))

            score = (
                alnum_ratio * 100.0
                - symbol_ratio * 40.0
                + min(length, 12000) / 400.0
                + keyword_hits * 2.0
            )
            return score

        def _extract_page_text_best_effort(page: Any) -> tuple[str, str]:
            candidates: list[tuple[str, str]] = []

            try:
                candidates.append(("text", page.get_text("text") or ""))
            except Exception:
                pass

            try:
                blocks = page.get_text("blocks") or []
                block_parts = []
                for block in blocks:
                    if len(block) >= 5 and isinstance(block[4], str):
                        block_parts.append(block[4])
                candidates.append(("blocks", "\n".join(block_parts)))
            except Exception:
                pass

            try:
                words = page.get_text("words") or []
                words_sorted = sorted(words, key=lambda item: (item[5], item[6], item[7], item[1], item[0]))
                word_parts = [str(item[4]) for item in words_sorted if len(item) >= 5 and str(item[4]).strip()]
                candidates.append(("words", " ".join(word_parts)))
            except Exception:
                pass

            try:
                text_dict = page.get_text("dict") or {}
                dict_parts: list[str] = []
                for block in text_dict.get("blocks", []):
                    for line in block.get("lines", []):
                        line_parts = []
                        for span in line.get("spans", []):
                            span_text = str(span.get("text") or "")
                            if span_text:
                                line_parts.append(span_text)
                        if line_parts:
                            dict_parts.append("".join(line_parts))
                candidates.append(("dict", "\n".join(dict_parts)))
            except Exception:
                pass

            if not candidates:
                return "", "none"

            best_mode, best_text = "none", ""
            best_score = -1.0
            for mode, candidate_text in candidates:
                score = _score_text_candidate(candidate_text)
                if score > best_score:
                    best_score = score
                    best_mode = mode
                    best_text = candidate_text

            return best_text or "", best_mode

        logger.info(f"Extracting all pages (total pages: {len(doc)})...")
        for page_index in range(len(doc)):
            page = doc[page_index]
            page_num = page_index + 1

            text, extraction_mode = _extract_page_text_best_effort(page)

            form_field_lines: list[str] = []
            try:
                widgets = page.widgets() or []
                for widget in widgets:
                    field_name = str(getattr(widget, "field_name", "") or "").strip()
                    field_value = str(getattr(widget, "field_value", "") or "").strip()
                    if not field_value:
                        continue
                    if field_name:
                        form_field_lines.append(f"{field_name}: {field_value}")
                    else:
                        form_field_lines.append(field_value)
            except Exception:
                form_field_lines = []

            if form_field_lines:
                text = (text + "\n\n[FORM_FIELDS]\n" + "\n".join(form_field_lines)).strip()

            char_count = len(text)
            preview = text[:300] if text else ""

            words_serialized: list[dict[str, Any]] = []
            try:
                words_raw = page.get_text("words") or []
                for item in words_raw:
                    if len(item) < 5:
                        continue
                    token_text = str(item[4] or "").strip()
                    if not token_text:
                        continue
                    words_serialized.append({
                        "x0": float(item[0]),
                        "y0": float(item[1]),
                        "x1": float(item[2]),
                        "y1": float(item[3]),
                        "text": token_text,
                        "block_no": int(item[5]) if len(item) > 5 else -1,
                        "line_no": int(item[6]) if len(item) > 6 else -1,
                        "word_no": int(item[7]) if len(item) > 7 else -1,
                    })
            except Exception:
                words_serialized = []

            result["pages"].append({
                "page_number": page_num,
                "char_count": char_count,
                "text": text,
                "preview": preview,
                "extraction_mode": extraction_mode,
                "words": words_serialized,
            })

            logger.info(f"Page {page_num}: Extracted {char_count} characters (mode={extraction_mode})")

        doc.close()
    except Exception as e:
        logger.error(f"Error parsing PDF source: {str(e)}")
        result["error"] = str(e)

    return result


def identify_relevant_pages(text_pack: dict) -> dict:
    """Scan pages and identify which likely contain lease dates and rent info.

    Args:
        text_pack: Output from parse_pdf_to_text_pack

    Returns:
        dict with:
            - lease_dates_pages: list of page numbers
            - rent_pages: list of page numbers
    """
    lease_date_keywords = [
        r'lease\s+term',
        r'term\s+of\s+lease',
        r'lease\s+start',
        r'lease\s+end',
        r'commencement\s+date',
        r'expiration\s+date',
        r'move[- ]in',
        r'move[- ]out',
        r'term\s+begins',
        r'term\s+ends'
    ]

    base_rent_rule = get_term_extraction_rule("BASE_RENT")
    rent_keywords = list(base_rent_rule.get("page_hint_patterns") or [])

    amenity_rule = get_term_extraction_rule("AMENITY_PREMIUM")
    premium_keywords = list(amenity_rule.get("focus_patterns") or []) + list(amenity_rule.get("include_patterns") or [])

    parking_rule = get_term_extraction_rule("PARKING")
    parking_keywords = list(parking_rule.get("page_hint_patterns") or [])

    lease_dates_pages = []
    rent_pages = []
    premium_pages = []
    parking_pages = []

    for page_info in text_pack.get("pages", []):
        page_num = page_info["page_number"]
        text = page_info.get("text", page_info.get("preview", "")).lower()

        for pattern in lease_date_keywords:
            if re.search(pattern, text, re.IGNORECASE):
                if page_num not in lease_dates_pages:
                    lease_dates_pages.append(page_num)
                break

        for pattern in rent_keywords:
            if re.search(pattern, text, re.IGNORECASE):
                if page_num not in rent_pages:
                    rent_pages.append(page_num)
                break

        for pattern in premium_keywords:
            if re.search(pattern, text, re.IGNORECASE):
                if page_num not in premium_pages:
                    premium_pages.append(page_num)
                break

        for pattern in parking_keywords:
            if re.search(pattern, text, re.IGNORECASE):
                if page_num not in parking_pages:
                    parking_pages.append(page_num)
                break

    return {
        "lease_dates_pages": lease_dates_pages,
        "rent_pages": rent_pages,
        "premium_pages": premium_pages,
        "parking_pages": parking_pages
    }


def extract_parking_fee(text_pack: dict, page_hints: dict = None) -> dict | None:
    """Extract parking monthly amount using coordinate anchors first, then text fallback."""
    parking_rule = get_term_extraction_rule("PARKING")
    if page_hints is None:
        page_hints = identify_relevant_pages(text_pack)

    pages = [page for page in text_pack.get("pages", []) if page.get("page_number") is not None]
    pages_by_number = {int(page["page_number"]): page for page in pages}

    parking_pages = list(page_hints.get("parking_pages") or [])
    if not parking_pages:
        fallback_page_pattern = str(parking_rule.get("page_fallback_pattern") or "").strip()
        if not fallback_page_pattern:
            return None
        parking_pages = [
            page_num
            for page_num, page_info in pages_by_number.items()
            if re.search(fallback_page_pattern, str(page_info.get("text") or ""), re.IGNORECASE)
        ]

    if not parking_pages:
        return None

    def _normalized_token(text_value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(text_value or "").lower())

    def _money_value(token_text: str) -> float | None:
        normalized = str(token_text or "").strip().replace("$", "").replace(",", "")
        if not re.fullmatch(r"\d{1,6}\.\d{2}", normalized):
            return None
        parsed = _as_float(normalized)
        if parsed is None or parsed <= 0:
            return None
        return parsed

    def _money_tokens(words: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        tokens: list[dict[str, Any]] = []
        for word in words:
            value = _money_value(str(word.get("text") or ""))
            if value is None:
                continue
            x0 = float(word.get("x0") or 0.0)
            y0 = float(word.get("y0") or 0.0)
            x1 = float(word.get("x1") or x0)
            y1 = float(word.get("y1") or y0)
            tokens.append({
                "amount": float(value),
                "text": str(word.get("text") or ""),
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "cx": (x0 + x1) / 2.0,
                "cy": (y0 + y1) / 2.0,
            })
        return tokens

    def _anchor_boxes(words: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        anchor_phrase_tokens = [
            re.sub(r"[^a-z0-9]+", "", str(token or "").lower())
            for token in (parking_rule.get("anchor_phrase_tokens") or [])
        ]
        anchor_phrase_tokens = [token for token in anchor_phrase_tokens if token]
        anchor_keyword_tokens = {
            re.sub(r"[^a-z0-9]+", "", str(token or "").lower())
            for token in (parking_rule.get("anchor_keywords") or [])
        }
        anchor_keyword_tokens = {token for token in anchor_keyword_tokens if token}

        seq = []
        for word in words:
            token_text = str(word.get("text") or "").strip()
            if not token_text:
                continue
            seq.append({
                "token": _normalized_token(token_text),
                "x0": float(word.get("x0") or 0.0),
                "y0": float(word.get("y0") or 0.0),
                "x1": float(word.get("x1") or 0.0),
                "y1": float(word.get("y1") or 0.0),
            })

        anchors: list[dict[str, Any]] = []
        window_size = len(anchor_phrase_tokens)
        if window_size >= 2:
            for idx in range(len(seq) - window_size + 1):
                window = seq[idx:idx + window_size]
                if [item["token"] for item in window] != anchor_phrase_tokens:
                    continue
                anchors.append({
                    "label": "cost_for_parking",
                    "x0": min(item["x0"] for item in window),
                    "y0": min(item["y0"] for item in window),
                    "x1": max(item["x1"] for item in window),
                    "y1": max(item["y1"] for item in window),
                })

        for item in seq:
            if item["token"] in anchor_keyword_tokens:
                anchors.append({
                    "label": item["token"],
                    "x0": item["x0"],
                    "y0": item["y0"],
                    "x1": item["x1"],
                    "y1": item["y1"],
                })

        return anchors

    def _row_excerpt(words: Sequence[Mapping[str, Any]], target_cy: float) -> str:
        row_words = [
            word for word in words
            if abs(float((float(word.get("y0") or 0.0) + float(word.get("y1") or 0.0)) / 2.0) - target_cy) <= 3.0
            and str(word.get("text") or "").strip()
        ]
        row_words = sorted(row_words, key=lambda item: float(item.get("x0") or 0.0))
        return re.sub(r"\s+", " ", " ".join([str(item.get("text") or "").strip() for item in row_words])).strip()

    coordinate_candidates: list[dict[str, Any]] = []

    for page_number in parking_pages:
        page_info = pages_by_number.get(int(page_number))
        if not page_info:
            continue
        words = list(page_info.get("words") or [])
        if not words:
            continue

        anchors = _anchor_boxes(words)
        tokens = _money_tokens(words)
        if not anchors or not tokens:
            continue

        for anchor in anchors:
            anchor_cy = (float(anchor["y0"]) + float(anchor["y1"])) / 2.0
            y_tolerance = max(3.0, (float(anchor["y1"]) - float(anchor["y0"])) * 0.75)
            for token in tokens:
                dx = float(token["x0"]) - float(anchor["x1"])
                dy = abs(float(token["cy"]) - anchor_cy)
                to_right = dx >= -1.5
                same_row = dy <= y_tolerance
                below = float(token["cy"]) > anchor_cy

                rule = "other"
                score_boost = 0.0
                if to_right and same_row:
                    rule = "right_same_row"
                    score_boost = 3000.0
                elif to_right:
                    rule = "right_diff_row"
                    score_boost = 1800.0
                elif below:
                    rule = "below"
                    score_boost = 900.0

                if anchor.get("label") == "cost_for_parking":
                    score_boost += 180.0

                row_text = _row_excerpt(words, float(token["cy"]))
                row_lower = row_text.lower()

                exclusion_signals = [str(token).lower() for token in (parking_rule.get("exclusion_signals") or [])]
                if any(signal in row_lower for signal in exclusion_signals):
                    continue

                positive_context_signals = [str(token).lower() for token in (parking_rule.get("positive_context_signals") or [])]
                if anchor.get("label") != "cost_for_parking" and not any(signal in row_lower for signal in positive_context_signals):
                    continue

                monthly_parking_signals = [str(token).lower() for token in (parking_rule.get("monthly_signals") or [])]
                monthly_bonus_signals = [str(token).lower() for token in (parking_rule.get("monthly_bonus_signals") or [])]
                one_time_signals = [str(token).lower() for token in (parking_rule.get("one_time_signals") or [])]

                if any(signal in row_lower for signal in monthly_parking_signals):
                    score_boost += 350.0
                if any(signal in row_lower for signal in monthly_bonus_signals):
                    score_boost += 220.0
                if any(signal in row_lower for signal in one_time_signals):
                    score_boost -= 260.0
                if anchor.get("label") == "cost_for_parking" and not any(signal in row_lower for signal in monthly_parking_signals):
                    score_boost -= 140.0

                score = score_boost - ((abs(dx) * 1.1) + (dy * 14.0))
                coordinate_candidates.append({
                    "value": f"${float(token['amount']):.2f}",
                    "normalized": f"{float(token['amount']):.2f}",
                    "page_number": int(page_number),
                    "evidence": row_text,
                    "score": score,
                    "rule": rule,
                    "anchor": anchor.get("label"),
                })

    if coordinate_candidates:
        coordinate_candidates_sorted = sorted(
            coordinate_candidates,
            key=lambda item: (item["score"], float(item["normalized"])),
            reverse=True,
        )
        best = coordinate_candidates_sorted[0]
        logger.info(
            "[LEASE TERMS] PARKING extracted method=fitz-anchor rule=%s anchor=%s page=%s value=%s",
            best.get("rule"),
            best.get("anchor"),
            best.get("page_number"),
            best.get("value"),
        )
        return {
            "value": best["value"],
            "normalized": best["normalized"],
            "page_number": best["page_number"],
            "evidence": best["evidence"],
            "candidates": coordinate_candidates_sorted[:20],
        }

    regex_candidates: list[dict[str, Any]] = []
    monthly_regex_patterns = [
        str(pattern).strip()
        for pattern in (parking_rule.get("regex_fallback_monthly_patterns") or [])
        if str(pattern).strip()
    ]
    monthly_regexes = [
        re.compile(pattern, re.IGNORECASE | re.DOTALL)
        for pattern in monthly_regex_patterns
    ]
    regex_exclusion_signals = [str(token).lower() for token in (parking_rule.get("regex_exclusion_signals") or [])]
    regex_score_bonus_signals = [str(token).lower() for token in (parking_rule.get("regex_score_bonus_signals") or [])]

    for page_number in parking_pages:
        page_info = pages_by_number.get(int(page_number))
        page_text = str((page_info or {}).get("text") or "")
        if not page_text:
            continue
        for regex in monthly_regexes:
            for match in regex.finditer(page_text):
                normalized = normalize_money(match.group(1))
                if not normalized:
                    continue
                snippet = re.sub(r"\s+", " ", page_text[max(0, match.start() - 100):min(len(page_text), match.end() + 100)]).strip()
                snippet_lower = snippet.lower()
                if any(signal in snippet_lower for signal in regex_exclusion_signals):
                    continue
                score = 500.0
                if any(signal in snippet_lower for signal in regex_score_bonus_signals):
                    score += 120.0
                regex_candidates.append({
                    "value": f"${float(normalized):.2f}",
                    "normalized": normalized,
                    "page_number": int(page_number),
                    "evidence": snippet,
                    "score": score,
                })

    if regex_candidates:
        regex_sorted = sorted(regex_candidates, key=lambda item: (item["score"], float(item["normalized"])), reverse=True)
        best = regex_sorted[0]
        logger.info(
            "[LEASE TERMS] PARKING extracted method=regex-monthly page=%s value=%s",
            best.get("page_number"),
            best.get("value"),
        )
        return {
            "value": best["value"],
            "normalized": best["normalized"],
            "page_number": best["page_number"],
            "evidence": best["evidence"],
            "candidates": regex_sorted[:20],
        }

    logger.warning("[LEASE TERMS] PARKING extraction failed: no anchor or monthly regex match")
    return None


def download_lease_document(
    property_id=None,
    lease_id=None,
    docs: Sequence[Mapping[str, Any]] | None = None,
    audit_period_start: Any = None,
    audit_period_end: Any = None,
    storage_service: Any = None,
):
    """
    Download the most recent signed lease document.

    Args:
        property_id: Property ID (defaults to PROPERTY_ID constant)
        lease_id: Lease ID (defaults to LEASE_ID constant)

    Returns:
        Tuple of (pdf_bytes, filename, doc_info)
    """
    prop_id = property_id if property_id is not None else PROPERTY_ID
    l_id = lease_id if lease_id is not None else LEASE_ID

    if prop_id is None or l_id is None:
        raise ValueError("property_id and lease_id are required (either args or env defaults).")

    logger.info("\n" + "#" * 80)
    logger.info("DOWNLOAD_LEASE_DOCUMENT CALLED")
    logger.info(f"Input - property_id: {property_id}, lease_id: {lease_id}")
    logger.info(f"Using - prop_id: {prop_id}, l_id: {l_id}")
    logger.info("#" * 80)

    docs_list: list[dict[str, Any]] = []
    if docs is not None:
        docs_list = [dict(doc) for doc in docs if isinstance(doc, Mapping)]
    else:
        list_payload = {
            "auth": {"type": "apikey"},
            "requestId": "doc-list",
            "method": {
                "name": "getLeaseDocumentsList",
                "params": {
                    "propertyId": prop_id,
                    "leaseId": l_id,
                    "showDeletedFile": "0",
                },
            },
        }

        list_json = post_entrata(list_payload)

        result = list_json.get("response", {}).get("result", {})
        lease_documents = result.get("LeaseDocuments") or result.get("leaseDocuments", {})
        docs_raw = lease_documents.get("LeaseDocument") or lease_documents.get("leaseDocument", [])

        if isinstance(docs_raw, dict):
            docs_list = [doc for doc in docs_raw.values() if isinstance(doc, dict)]
        else:
            docs_list = [doc for doc in docs_raw if isinstance(doc, dict)] if isinstance(docs_raw, list) else []

    logger.info(f"Found {len(docs_list)} total documents")

    if not docs_list:
        logger.error("No documents returned from getLeaseDocumentsList")
        raise ValueError("No documents returned from getLeaseDocumentsList.")

    selected_primary_doc, _, selected_reason = select_lease_packet_and_addenda(
        docs_list,
        audit_period_start=audit_period_start,
        audit_period_end=audit_period_end,
    )
    signed_latest = dict(selected_primary_doc)
    doc_id = get_doc_id(signed_latest)
    title = get_doc_title(signed_latest) or f"signed_lease_{doc_id}"
    selected_code = get_doc_code(signed_latest)
    selected_recency_key = get_doc_recency_key(signed_latest)

    logger.info(
        f"Selected document - ID: {doc_id}, Title: {title}, Code: {selected_code or 'N/A'}, Why: {selected_reason}"
    )
    logger.info(f"Document details: {json.dumps(signed_latest, indent=2, default=str)}")

    newer_signed_addenda = []
    for candidate in docs_list:
        candidate_id = get_doc_id(candidate)
        if not candidate_id or candidate_id == doc_id:
            continue
        if not is_signed_addendum(candidate):
            continue

        candidate_recency_key = get_doc_recency_key(candidate)
        if candidate_recency_key > selected_recency_key:
            newer_signed_addenda.append({
                "doc": candidate,
                "doc_id": candidate_id,
                "title": get_doc_title(candidate) or f"addendum_{candidate_id}",
                "activity_ts": get_doc_activity_timestamp(candidate),
                "recency_key": candidate_recency_key
            })

    latest_addendum_by_name = {}
    for item in newer_signed_addenda:
        addendum_name_key = get_addendum_name_key(item["doc"])
        existing = latest_addendum_by_name.get(addendum_name_key)
        if existing is None or item["recency_key"] > existing["recency_key"]:
            latest_addendum_by_name[addendum_name_key] = item

    newer_signed_addenda = sorted(latest_addendum_by_name.values(), key=lambda item: item["recency_key"])
    floorplan_removed_count = len([item for item in newer_signed_addenda if is_floorplan_rate_addendum(item["doc"])])
    newer_signed_addenda = [item for item in newer_signed_addenda if not is_floorplan_rate_addendum(item["doc"])]
    if floorplan_removed_count > 0:
        logger.info(
            f"Skipping {floorplan_removed_count} separate Floor Plan Rate Addendum doc(s); using packet copy only."
        )

    logger.info(f"Found {len(newer_signed_addenda)} newer signed addenda after selected packet")
    if newer_signed_addenda:
        logger.info(
            "Included addenda: " + ", ".join(
                [f"{item['title']} ({item['doc_id']})" for item in newer_signed_addenda]
            )
        )

    def fetch_document_pdf_bytes(target_doc_id: str) -> bytes:
        get_payload = {
            "auth": {"type": "apikey"},
            "requestId": f"doc-get-{target_doc_id}",
            "method": {
                "name": "getLeaseDocuments",
                "version": "r1",
                "params": {
                    "propertyId": prop_id,
                    "leaseId": l_id,
                    "documentIds": target_doc_id,
                    "showDeletedFile": 0,
                },
            },
        }

        get_json = post_entrata(get_payload)
        response = get_json.get("response", {})
        result = response.get("result", {})
        lease_docs = result.get("LeaseDocuments") or result.get("leaseDocuments", {})
        lease_doc_obj = lease_docs.get("LeaseDocument") or lease_docs.get("leaseDocument", {})

        filedata = None

        if isinstance(lease_doc_obj, dict):
            if target_doc_id in lease_doc_obj and isinstance(lease_doc_obj[target_doc_id], dict):
                filedata = (
                    lease_doc_obj[target_doc_id].get("FileData")
                    or lease_doc_obj[target_doc_id].get("fileData")
                    or lease_doc_obj[target_doc_id].get("filedata")
                )
            else:
                for response_doc_id, response_doc in lease_doc_obj.items():
                    if str(response_doc_id) == str(target_doc_id) and isinstance(response_doc, dict):
                        filedata = (
                            response_doc.get("FileData")
                            or response_doc.get("fileData")
                            or response_doc.get("filedata")
                        )
                        if filedata:
                            break

                if not filedata:
                    filedata = lease_doc_obj.get("FileData") or lease_doc_obj.get("fileData") or lease_doc_obj.get("filedata")

        elif isinstance(lease_doc_obj, list):
            for response_doc in lease_doc_obj:
                if not isinstance(response_doc, dict):
                    continue
                response_doc_id = get_doc_id(response_doc)
                if str(response_doc_id) == str(target_doc_id):
                    filedata = response_doc.get("FileData") or response_doc.get("fileData") or response_doc.get("filedata")
                    if filedata:
                        break
            if not filedata and lease_doc_obj and isinstance(lease_doc_obj[0], dict):
                filedata = lease_doc_obj[0].get("FileData") or lease_doc_obj[0].get("fileData") or lease_doc_obj[0].get("filedata")

        if not filedata:
            raise ValueError(f"FileData was empty for doc_id {target_doc_id}. Check getLeaseDocuments response structure.")

        return base64.b64decode(filedata.strip())

    def save_pdf_bytes_locally(pdf_bytes: bytes, preferred_filename: str) -> tuple[str | None, str]:
        filename_local = safe_filename(preferred_filename)
        if not SAVE_LOCAL_ENTRATA_PDFS:
            return None, filename_local

        out_path_local = os.path.join(OUT_DIR, filename_local)
        try:
            with open(out_path_local, "wb") as f:
                f.write(pdf_bytes)
        except PermissionError:
            unique_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_local = safe_filename(f"{os.path.splitext(preferred_filename)[0]}_{unique_suffix}.pdf")
            out_path_local = os.path.join(OUT_DIR, filename_local)
            with open(out_path_local, "wb") as f:
                f.write(pdf_bytes)
            logger.warning(f"Primary output file was locked, wrote to alternate path: {out_path_local}")
        return out_path_local, filename_local

    def upload_pdf_bytes_to_sharepoint(pdf_bytes: bytes, filename_local: str) -> str | None:
        if storage_service is None:
            return None

        if not bool(getattr(storage_service, "use_sharepoint", False)):
            return None

        upload_fn = getattr(storage_service, "_upload_binary_file_to_sharepoint", None)
        if not callable(upload_fn):
            logger.warning("SharePoint storage enabled but upload helper is unavailable on storage_service")
            return None

        property_folder = safe_filename(str(prop_id))
        sharepoint_relative_path = f"Entrata leases/{property_folder}/{filename_local}"

        try:
            upload_success = bool(upload_fn(pdf_bytes, sharepoint_relative_path))
            if upload_success:
                logger.info(f"Uploaded lease PDF to SharePoint: {sharepoint_relative_path}")
                return sharepoint_relative_path

            logger.warning(f"Failed to upload lease PDF to SharePoint: {sharepoint_relative_path}")
            return None
        except Exception as upload_exc:
            logger.warning(f"SharePoint upload failed for {sharepoint_relative_path}: {upload_exc}")
            return None

    primary_pdf_bytes = fetch_document_pdf_bytes(doc_id)
    primary_path, primary_filename = save_pdf_bytes_locally(primary_pdf_bytes, f"{title}_{doc_id}.pdf")
    primary_sharepoint_path = upload_pdf_bytes_to_sharepoint(primary_pdf_bytes, primary_filename)
    primary_page_count: int | None = None
    if fitz is not None:
        try:
            with fitz.open(stream=primary_pdf_bytes, filetype="pdf") as primary_doc:
                primary_page_count = len(primary_doc)
        except Exception as page_count_error:
            logger.warning(f"Could not determine primary packet page count for doc_id={doc_id}: {page_count_error}")
    logger.info(
        f"Prepared primary packet PDF: filename={primary_filename} local_path={primary_path or 'disabled'}"
    )

    addenda_saved = []
    addenda_pdf_bytes: list[dict[str, Any]] = []
    for addendum in newer_signed_addenda:
        add_doc_id = addendum["doc_id"]
        add_title = addendum["title"]
        add_pdf_bytes = fetch_document_pdf_bytes(add_doc_id)
        add_path, add_filename = save_pdf_bytes_locally(add_pdf_bytes, f"{add_title}_{add_doc_id}.pdf")
        add_sharepoint_path = upload_pdf_bytes_to_sharepoint(add_pdf_bytes, add_filename)
        addenda_pdf_bytes.append({
            "doc_id": add_doc_id,
            "pdf_bytes": add_pdf_bytes,
            "filename": add_filename,
        })
        addenda_saved.append({
            "doc_id": add_doc_id,
            "title": add_title,
            "activity_timestamp": addendum["activity_ts"].isoformat() if addendum["activity_ts"] != datetime.min else None,
            "saved_path": add_path,
            "filename": add_filename,
            "file_size": len(add_pdf_bytes),
            "sharepoint_path": add_sharepoint_path,
        })
        logger.info(
            f"Prepared addendum PDF: filename={add_filename} local_path={add_path or 'disabled'}"
        )

    out_pdf_bytes = primary_pdf_bytes
    out_path = primary_path
    filename = primary_filename
    combined_sharepoint_path = None

    if addenda_saved:
        if fitz is None:
            logger.warning("PyMuPDF unavailable; skipping merge of packet + addenda and returning primary PDF")
        else:
            merged_filename = safe_filename(f"{title}_{doc_id}_with_addenda.pdf")
            merged_doc = fitz.open()
            try:
                with fitz.open(stream=primary_pdf_bytes, filetype="pdf") as packet_doc:
                    merged_doc.insert_pdf(packet_doc)
                for addendum_item in addenda_pdf_bytes:
                    with fitz.open(stream=addendum_item["pdf_bytes"], filetype="pdf") as addendum_doc:
                        merged_doc.insert_pdf(addendum_doc)
                out_pdf_bytes = merged_doc.tobytes()
            finally:
                merged_doc.close()

            out_path, merged_filename = save_pdf_bytes_locally(out_pdf_bytes, merged_filename)
            filename = merged_filename
            combined_sharepoint_path = upload_pdf_bytes_to_sharepoint(out_pdf_bytes, merged_filename)
            logger.info(
                f"Prepared merged packet + addenda PDF: filename={filename} local_path={out_path or 'disabled'}"
            )

    doc_info = {
        "doc_id": doc_id,
        "title": title,
        "start_date": signed_latest.get("leaseIntervalStartDate") or signed_latest.get("AddedOn"),
        "file_size": len(out_pdf_bytes),
        "included_addenda_count": len(addenda_saved),
        "included_addenda": addenda_saved,
        "source_packet_path": primary_path,
        "combined_path": out_path if addenda_saved and fitz is not None else None,
        "source_packet_sharepoint_path": primary_sharepoint_path,
        "combined_sharepoint_path": combined_sharepoint_path,
        "primary_page_count": primary_page_count,
    }

    logger.info(f"Document info: {json.dumps(_json_safe_for_logging(doc_info), indent=2, default=str)}")
    logger.info("#" * 80 + "\n")
    return out_pdf_bytes, filename, doc_info


def normalize_id(value: Any) -> str | None:
    """Normalize ID values for consistent key matching across mixed types."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"nan", "none"}:
            return None
        try:
            as_float = float(text)
            if as_float.is_integer():
                return str(int(as_float))
        except Exception:
            pass
        return text

    if isinstance(value, (int, float)):
        as_float = float(value)
        if as_float.is_integer():
            return str(int(as_float))
        return str(as_float)

    text = str(value).strip()
    return text or None


def _find_first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def collect_property_lease_pairs(
    dataframes: Iterable[pd.DataFrame],
    property_id_candidates: Sequence[str] | None = None,
    lease_id_candidates: Sequence[str] | None = None,
) -> list[Pair]:
    """
    Collect unique (property_id, lease_id) pairs from one or more dataframes.

    Defaults favor canonical columns and common raw aliases.
    """
    property_candidates = property_id_candidates or [
        CanonicalField.PROPERTY_ID.value,
        "property_id",
        "PropertyId",
        "Property ID",
    ]
    lease_candidates = lease_id_candidates or [
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.LEASE_ID.value,
        "lease_interval_id",
        "lease_id",
        "LeaseId",
        "Lease ID",
    ]

    pairs: list[Pair] = []
    seen: set[Pair] = set()

    for df in dataframes:
        if df is None or df.empty:
            continue

        property_column = _find_first_existing_column(df, property_candidates)
        lease_column = _find_first_existing_column(df, lease_candidates)
        if not property_column or not lease_column:
            continue

        subset = df[[property_column, lease_column]].copy()
        for _, row in subset.iterrows():
            property_id = normalize_id(row.get(property_column))
            lease_id = normalize_id(row.get(lease_column))
            if not property_id or not lease_id:
                continue

            pair = (property_id, lease_id)
            if pair in seen:
                continue

            seen.add(pair)
            pairs.append(pair)

    return pairs


def build_entrata_params(
    property_id: str,
    lease_id: str,
    base_params: Mapping[str, Any] | None = None,
    property_param_name: str = "property_id",
    lease_param_name: str = "lease_id",
) -> JsonDict:
    """Build Entrata request parameters using input property/lease identifiers."""
    params: JsonDict = dict(base_params or {})
    params[property_param_name] = property_id
    params[lease_param_name] = lease_id
    return params


def get_value_by_paths(payload: Mapping[str, Any], paths: Sequence[str]) -> Any:
    """
    Return first non-null value from dot-path candidates in nested dictionaries.

    Example path: "response.lease.rent".
    """
    for path in paths:
        current: Any = payload
        path_found = True

        for segment in path.split("."):
            if not isinstance(current, Mapping) or segment not in current:
                path_found = False
                break
            current = current[segment]

        if path_found and current is not None:
            return current

    return None


def extract_fields_from_response(
    response_payload: Mapping[str, Any],
    field_paths: Mapping[str, Sequence[str]] | None = None,
) -> JsonDict:
    """
    Extract lease-term fields from API payload using configurable path mappings.

    `field_paths` format:
    {
      "LEASE_START_DATE": ["response.lease.start_date", "lease.startDate"],
      "LEASE_END_DATE": ["response.lease.end_date"],
      "RENT_AMOUNT": ["response.lease.rent", "lease.monthly_rent"]
    }
    """
    if not field_paths:
        return {}

    extracted: JsonDict = {}
    for output_field, candidate_paths in field_paths.items():
        extracted[output_field] = get_value_by_paths(response_payload, candidate_paths)
    return extracted


def extract_lease_terms_for_pairs(
    pairs: Sequence[Pair],
    api_fetcher: ApiFetcher,
    field_extractor: FieldExtractor,
    base_params: Mapping[str, Any] | None = None,
    property_param_name: str = "property_id",
    lease_param_name: str = "lease_id",
) -> pd.DataFrame:
    """
    Execute lease-term extraction for (property_id, lease_id) pairs.

    The function is side-effect free aside from invoking injected callables.
    It returns a tabular result with status/error columns for safe downstream use.
    """
    rows: list[JsonDict] = []

    for property_id, lease_id in pairs:
        row: JsonDict = {
            CanonicalField.PROPERTY_ID.value: property_id,
            CanonicalField.LEASE_INTERVAL_ID.value: lease_id,
            "extraction_status": "success",
            "error_message": "",
        }

        try:
            params = build_entrata_params(
                property_id=property_id,
                lease_id=lease_id,
                base_params=base_params,
                property_param_name=property_param_name,
                lease_param_name=lease_param_name,
            )
            response_payload = api_fetcher(params)
            extracted_fields = dict(field_extractor(response_payload) or {})
            row.update(extracted_fields)
        except Exception as exc:
            row["extraction_status"] = "error"
            row["error_message"] = str(exc)

        rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                CanonicalField.PROPERTY_ID.value,
                CanonicalField.LEASE_INTERVAL_ID.value,
                "extraction_status",
                "error_message",
            ]
        )

    return pd.DataFrame(rows)


def fetch_lease_documents_list(property_id: Any, lease_id: Any) -> list[dict[str, Any]]:
    """Fetch lease document metadata list from Entrata."""
    payload = {
        "auth": {"type": "apikey"},
        "requestId": "doc-list",
        "method": {
            "name": "getLeaseDocumentsList",
            "params": {
                "propertyId": property_id,
                "leaseId": lease_id,
                "showDeletedFile": "0",
            },
        },
    }
    list_json = post_entrata(payload)
    result = list_json.get("response", {}).get("result", {})
    lease_documents = result.get("LeaseDocuments") or result.get("leaseDocuments", {})
    docs_raw = lease_documents.get("LeaseDocument") or lease_documents.get("leaseDocument", [])
    if isinstance(docs_raw, dict):
        return [doc for doc in docs_raw.values() if isinstance(doc, dict)]
    if isinstance(docs_raw, list):
        return [doc for doc in docs_raw if isinstance(doc, dict)]
    return []


def select_lease_packet_and_addenda(
    docs: Sequence[Mapping[str, Any]],
    audit_period_start: Any = None,
    audit_period_end: Any = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    """Select primary signed packet and eligible newer addenda from document metadata."""
    docs = [dict(doc) for doc in docs if isinstance(doc, Mapping)]
    if not docs:
        raise ValueError("No lease documents available for selection")

    preferred_codes_1 = {"LP", "OEP", "PACKET"}
    preferred_codes_2 = {"LEASE", "LD", "OEL"}

    def sort_key(d):
        lease_start = get_doc_declared_lease_start(d)
        added_on = parse_doc_datetime(d.get("AddedOn") or d.get("addedOn"))
        modified_on = parse_doc_datetime(d.get("ModifiedOn") or d.get("modifiedOn"))
        name_ts = parse_doc_name_timestamp(d)
        file_size = get_doc_declared_file_size(d)
        doc_id_value = get_doc_id(d)
        try:
            doc_id_num = int(str(doc_id_value))
        except (TypeError, ValueError):
            doc_id_num = 0
        return (lease_start, added_on, modified_on, name_ts, file_size, doc_id_num)

    code_bucket_1 = [d for d in docs if get_doc_code(d) in preferred_codes_1]
    code_bucket_2 = [d for d in docs if get_doc_code(d) in preferred_codes_2]
    signed_lease_packet = [
        d for d in docs
        if is_signed(d) and (
            ("lease" in (d.get("Title") or d.get("title") or "").lower()) or
            ("packet" in (d.get("Title") or d.get("title") or "").lower())
        )
    ]

    selected_pool = None
    selected_reason = None
    if code_bucket_1:
        selected_pool = code_bucket_1
        selected_reason = "priority code bucket 1 (LP/OEP/PACKET)"
    elif code_bucket_2:
        selected_pool = code_bucket_2
        selected_reason = "priority code bucket 2 (LEASE/LD/OEL)"
    elif signed_lease_packet:
        selected_pool = signed_lease_packet
        selected_reason = "fallback signed doc with lease/packet in title"

    if not selected_pool:
        raise ValueError("No eligible lease document found after code-priority and signed fallback checks")

    period_start = _coerce_period_datetime(audit_period_start)
    period_end = _coerce_period_datetime(audit_period_end)
    if period_start and period_end and period_end < period_start:
        period_start, period_end = period_end, period_start

    if period_start or period_end:
        period_matched_pool = []
        for candidate in selected_pool:
            lease_start = get_doc_declared_lease_start(candidate)
            if lease_start == datetime.min:
                continue
            if period_start and lease_start < period_start:
                continue
            if period_end and lease_start > period_end:
                continue
            period_matched_pool.append(candidate)

        if period_matched_pool:
            selected_pool = period_matched_pool
            if period_start and period_end:
                selected_reason = (
                    f"{selected_reason}; audit-period lease start match "
                    f"({period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')})"
                )
            elif period_start:
                selected_reason = (
                    f"{selected_reason}; audit-period lease start match from {period_start.strftime('%Y-%m-%d')}"
                )
            else:
                selected_reason = (
                    f"{selected_reason}; audit-period lease start match through {period_end.strftime('%Y-%m-%d')}"
                )

    primary = sorted(selected_pool, key=sort_key, reverse=True)[0]
    primary_doc_id = get_doc_id(primary)
    selected_recency_key = get_doc_recency_key(primary)

    newer_signed_addenda = []
    for candidate in docs:
        candidate_id = get_doc_id(candidate)
        if not candidate_id or candidate_id == primary_doc_id:
            continue
        if not is_signed_addendum(candidate):
            continue
        candidate_recency_key = get_doc_recency_key(candidate)
        if candidate_recency_key > selected_recency_key:
            newer_signed_addenda.append({
                "doc": dict(candidate),
                "doc_id": candidate_id,
                "title": get_doc_title(candidate) or f"addendum_{candidate_id}",
                "activity_ts": get_doc_activity_timestamp(candidate),
                "recency_key": candidate_recency_key,
            })

    latest_addendum_by_name = {}
    for item in newer_signed_addenda:
        addendum_name_key = get_addendum_name_key(item["doc"])
        existing = latest_addendum_by_name.get(addendum_name_key)
        if existing is None or item["recency_key"] > existing["recency_key"]:
            latest_addendum_by_name[addendum_name_key] = item

    filtered_addenda = sorted(latest_addendum_by_name.values(), key=lambda item: item["recency_key"])
    filtered_addenda = [item for item in filtered_addenda if not is_floorplan_rate_addendum(item["doc"])]
    return dict(primary), filtered_addenda, selected_reason or "selected"


def build_selected_docs_fingerprint(primary_doc: Mapping[str, Any], addenda: Sequence[Mapping[str, Any]]) -> str:
    """Build deterministic fingerprint hash for selected packet/addenda metadata."""
    selected_docs = [primary_doc] + [item.get("doc", item) for item in addenda]

    serialized = []
    for doc in selected_docs:
        if not isinstance(doc, Mapping):
            continue
        serialized.append({
            "doc_id": get_doc_id(dict(doc)),
            "title": get_doc_title(dict(doc)),
            "status": str(doc.get("Status") or doc.get("status") or ""),
            "recency": str(get_doc_recency_key(dict(doc))),
        })

    serialized = sorted(serialized, key=lambda item: (item.get("doc_id") or "", item.get("title") or ""))
    digest_input = json.dumps(serialized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def build_doc_list_fingerprint(docs: Sequence[Mapping[str, Any]] | None) -> str:
    """Build deterministic fingerprint hash for the full lease document list metadata."""
    serialized = []
    for doc in docs or []:
        if not isinstance(doc, Mapping):
            continue
        doc_dict = dict(doc)
        serialized.append({
            "doc_id": get_doc_id(doc_dict),
            "title": get_doc_title(doc_dict),
            "status": str(doc_dict.get("Status") or doc_dict.get("status") or ""),
            "type": str(doc_dict.get("Type") or doc_dict.get("type") or ""),
            "file_type": str(doc_dict.get("FileType") or doc_dict.get("fileType") or doc_dict.get("fileTypeId") or ""),
            "added_on": str(doc_dict.get("AddedOn") or doc_dict.get("addedOn") or ""),
            "modified_on": str(doc_dict.get("ModifiedOn") or doc_dict.get("modifiedOn") or ""),
            "recency": str(get_doc_recency_key(doc_dict)),
        })

    serialized = sorted(serialized, key=lambda item: (item.get("doc_id") or "", item.get("title") or ""))
    digest_input = json.dumps(serialized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def _extract_basic_terms_from_text_pack(text_pack: Mapping[str, Any], doc_info: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fallback term extractor from raw PDF text when custom extractor isn't supplied."""
    pages = list(text_pack.get("pages", []))
    if not pages:
        return [], []

    all_text = "\n".join([str(page.get("text") or "") for page in pages])
    all_text_lower = all_text.lower()

    primary_pages = pages
    primary_page_count = doc_info.get("primary_page_count")
    if isinstance(primary_page_count, (int, float)):
        primary_page_count_int = int(primary_page_count)
        if 0 < primary_page_count_int <= len(pages):
            primary_pages = pages[:primary_page_count_int]

    source_packet_bytes = doc_info.get("source_packet_bytes")
    if (
        primary_pages is pages
        and isinstance(source_packet_bytes, (bytes, bytearray))
        and source_packet_bytes
    ):
        try:
            primary_pack = parse_pdf_to_text_pack(source_packet_bytes)
            candidate_primary_pages = list(primary_pack.get("pages", []))
            if candidate_primary_pages:
                primary_pages = candidate_primary_pages
        except Exception as e:
            logger.warning(f"[LEASE TERMS] Could not parse source packet bytes for primary-only extraction: {e}")

    primary_text = "\n".join([str(page.get("text") or "") for page in primary_pages])
    primary_text_lower = primary_text.lower()
    primary_page_count = len(primary_pages)
    addenda_pages = pages[primary_page_count:] if len(pages) > primary_page_count else []
    addenda_text = "\n".join([str(page.get("text") or "") for page in addenda_pages])

    terms: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []

    def _normalize_date_value(raw_date: str) -> str | None:
        cleaned = str(raw_date or "").strip()
        if not cleaned:
            return None
        cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bday\s+of\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        parsed = pd.to_datetime(cleaned, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.strftime("%Y-%m-%d")

    date_patterns = [
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b",
        r"\b\d{1,2}(?:st|nd|rd|th)?\s+day\s+of\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*,?\s*\d{4}\b",
    ]
    date_candidates: list[dict[str, Any]] = []
    for pattern in date_patterns:
        for match in re.finditer(pattern, primary_text, re.IGNORECASE):
            raw_date = match.group(0)
            normalized = _normalize_date_value(raw_date)
            if not normalized:
                continue
            context_start = max(0, match.start() - 80)
            context_end = min(len(primary_text), match.end() + 80)
            context = primary_text[context_start:context_end]
            context_lower = context.lower()
            start_score = 0
            end_score = 0
            if any(token in context_lower for token in ["begin", "start", "commencement", "move-in", "move in", "from"]):
                start_score += 2
            if any(token in context_lower for token in ["end", "expiration", "expire", "terminate", "move-out", "move out", "to"]):
                end_score += 2
            date_candidates.append({
                "raw": raw_date,
                "normalized": normalized,
                "index": match.start(),
                "context": re.sub(r"\s+", " ", context).strip(),
                "start_score": start_score,
                "end_score": end_score,
            })

    lease_start = None
    lease_end = None

    def _lease_window_months(start_value: str | None, end_value: str | None) -> float | None:
        if not start_value or not end_value:
            return None
        start_dt = pd.to_datetime(start_value, errors="coerce")
        end_dt = pd.to_datetime(end_value, errors="coerce")
        if pd.isna(start_dt) or pd.isna(end_dt) or end_dt <= start_dt:
            return None
        return (end_dt - start_dt).days / 30.4375

    def _is_valid_lease_window(start_value: str | None, end_value: str | None) -> bool:
        months = _lease_window_months(start_value, end_value)
        return months is not None and 6.0 <= months <= 18.0

    if date_candidates:
        start_choice = sorted(date_candidates, key=lambda d: (d["start_score"], -d["index"]), reverse=True)[0]
        end_choice = sorted(date_candidates, key=lambda d: (d["end_score"], -d["index"]), reverse=True)[0]
        if start_choice["normalized"] != end_choice["normalized"]:
            start_dt = pd.to_datetime(start_choice["normalized"], errors="coerce")
            end_dt = pd.to_datetime(end_choice["normalized"], errors="coerce")
            if not pd.isna(start_dt) and not pd.isna(end_dt) and start_dt < end_dt:
                lease_start = start_choice["normalized"]
                lease_end = end_choice["normalized"]

    if not lease_start or not lease_end:
        ordered_unique_dates = []
        seen_dates = set()
        for item in sorted(date_candidates, key=lambda d: d["index"]):
            norm_val = item["normalized"]
            if norm_val in seen_dates:
                continue
            seen_dates.add(norm_val)
            ordered_unique_dates.append(norm_val)
        if len(ordered_unique_dates) >= 2:
            lease_start = lease_start or ordered_unique_dates[0]
            lease_end = lease_end or ordered_unique_dates[1]

    if not _is_valid_lease_window(lease_start, lease_end):
        unique_dates = []
        seen = set()
        for item in sorted(date_candidates, key=lambda d: d["index"]):
            value = item["normalized"]
            if value in seen:
                continue
            seen.add(value)
            unique_dates.append(value)

        best_pair: tuple[str, str] | None = None
        best_distance = float("inf")
        for i in range(len(unique_dates)):
            for j in range(i + 1, len(unique_dates)):
                start_val = unique_dates[i]
                end_val = unique_dates[j]
                months = _lease_window_months(start_val, end_val)
                if months is None or months < 6.0 or months > 18.0:
                    continue
                distance = abs(months - 12.0)
                if distance < best_distance:
                    best_distance = distance
                    best_pair = (start_val, end_val)

        if best_pair is not None:
            lease_start, lease_end = best_pair
        else:
            lease_start = None
            lease_end = None

    base_rent_amount: float | None = None
    base_rent_evidence: str | None = None
    base_rent_candidates: list[dict[str, Any]] = []

    base_rent_rule = get_term_extraction_rule("BASE_RENT")
    base_rent_heading_patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (base_rent_rule.get("heading_patterns") or [])
        if str(pattern or "").strip()
    ]
    base_rent_fallback_heading_patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (base_rent_rule.get("fallback_heading_patterns") or [])
        if str(pattern or "").strip()
    ]

    base_rent_monthly_signal_pattern = str(base_rent_rule.get("monthly_signal_pattern") or "").strip()
    base_rent_total_signal_pattern = str(base_rent_rule.get("total_signal_pattern") or "").strip()
    base_rent_monthly_signal_regex = (
        re.compile(base_rent_monthly_signal_pattern, re.IGNORECASE)
        if base_rent_monthly_signal_pattern
        else None
    )
    base_rent_total_signal_regex = (
        re.compile(base_rent_total_signal_pattern, re.IGNORECASE)
        if base_rent_total_signal_pattern
        else None
    )

    base_rent_installment_pattern_text = str(base_rent_rule.get("regex_fallback_installment_pattern") or "").strip()
    base_rent_installment_pattern = (
        re.compile(base_rent_installment_pattern_text, re.IGNORECASE)
        if base_rent_installment_pattern_text
        else None
    )
    base_rent_monthly_patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (base_rent_rule.get("regex_fallback_monthly_patterns") or [])
        if str(pattern or "").strip()
    ]
    base_rent_excluded_context_tokens = [
        str(token).lower()
        for token in (base_rent_rule.get("excluded_context_tokens") or [])
        if str(token or "").strip()
    ]
    base_rent_total_context_tokens = [
        str(token).lower()
        for token in (base_rent_rule.get("total_context_tokens") or [])
        if str(token or "").strip()
    ]
    base_rent_installment_context_tokens = [
        str(token).lower()
        for token in (base_rent_rule.get("installment_context_tokens") or [])
        if str(token or "").strip()
    ]
    base_rent_monthly_context_tokens = [
        str(token).lower()
        for token in (base_rent_rule.get("monthly_context_tokens") or [])
        if str(token or "").strip()
    ]
    base_rent_total_rent_guard_tokens = [
        str(token).lower()
        for token in (base_rent_rule.get("total_rent_guard_tokens") or [])
        if str(token or "").strip()
    ]

    application_fee_rule = get_term_extraction_rule("APPLICATION_FEE")
    admin_fee_rule = get_term_extraction_rule("ADMIN_FEE")

    application_include_patterns = list(application_fee_rule.get("include_patterns") or [])
    admin_include_patterns = list(admin_fee_rule.get("include_patterns") or [])
    admin_exclude_patterns = list(admin_fee_rule.get("exclude_patterns") or [])

    clause_anchor_pattern_text = str(application_fee_rule.get("clause_anchor_pattern") or "").strip()
    clause_app_amount_pattern_text = str(application_fee_rule.get("clause_amount_pattern") or "").strip()
    clause_admin_amount_pattern_text = str(admin_fee_rule.get("clause_amount_pattern") or "").strip()

    clause_excluded_tokens = [
        str(token).lower()
        for token in (application_fee_rule.get("clause_excluded_tokens") or [])
        if str(token or "").strip()
    ]
    admin_prioritization_excluded_tokens = [
        str(token).lower()
        for token in (admin_fee_rule.get("prioritization_excluded_tokens") or [])
        if str(token or "").strip()
    ]
    admin_explicit_clause_markers = [
        str(marker).lower()
        for marker in (admin_fee_rule.get("explicit_clause_markers") or [])
        if str(marker or "").strip()
    ]
    admin_strict_pattern_texts = [
        str(pattern).strip()
        for pattern in (admin_fee_rule.get("strict_patterns") or [])
        if str(pattern or "").strip()
    ]
    admin_application_leak_required_tokens = [
        str(token).lower()
        for token in (admin_fee_rule.get("application_leak_required_tokens") or [])
        if str(token or "").strip()
    ]
    admin_application_leak_amount_pattern_text = str(admin_fee_rule.get("application_leak_amount_pattern") or "").strip()
    admin_application_leak_amount_regex = (
        re.compile(admin_application_leak_amount_pattern_text, re.IGNORECASE | re.DOTALL)
        if admin_application_leak_amount_pattern_text
        else None
    )

    def _normalized_word_token(word_value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(word_value or "").lower())

    def _money_value_from_token(token_text: str) -> float | None:
        normalized = str(token_text or "").strip()
        if not normalized:
            return None
        normalized = normalized.replace("$", "").replace(",", "")
        if not re.fullmatch(r"\d{1,6}\.\d{2}", normalized):
            return None
        return _as_float(normalized)

    def _extract_money_tokens_from_words(words: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        tokens: list[dict[str, Any]] = []
        for word in words:
            amount_value = _money_value_from_token(str(word.get("text") or ""))
            if amount_value is None or amount_value <= 0:
                continue
            x0 = float(word.get("x0") or 0.0)
            y0 = float(word.get("y0") or 0.0)
            x1 = float(word.get("x1") or x0)
            y1 = float(word.get("y1") or y0)
            tokens.append({
                "amount": amount_value,
                "text": str(word.get("text") or ""),
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "cx": (x0 + x1) / 2.0,
                "cy": (y0 + y1) / 2.0,
            })
        return tokens

    def _build_anchor_boxes_from_words(words: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        anchors: list[dict[str, Any]] = []
        sequence = []
        for word in words:
            token_text = str(word.get("text") or "").strip()
            if not token_text:
                continue
            sequence.append({
                "token": _normalized_word_token(token_text),
                "x0": float(word.get("x0") or 0.0),
                "y0": float(word.get("y0") or 0.0),
                "x1": float(word.get("x1") or 0.0),
                "y1": float(word.get("y1") or 0.0),
            })

        for idx in range(len(sequence) - 2):
            first, second, third = sequence[idx], sequence[idx + 1], sequence[idx + 2]
            if first["token"] == "rent" and second["token"] == "and" and third["token"] == "charges":
                if abs(first["y0"] - second["y0"]) <= 4.0 and abs(second["y0"] - third["y0"]) <= 4.0:
                    anchors.append({
                        "label": "rent_and_charges",
                        "x0": min(first["x0"], second["x0"], third["x0"]),
                        "y0": min(first["y0"], second["y0"], third["y0"]),
                        "x1": max(first["x1"], second["x1"], third["x1"]),
                        "y1": max(first["y1"], second["y1"], third["y1"]),
                    })

        for token in sequence:
            if token["token"] != "rent":
                continue
            anchors.append({
                "label": "rent",
                "x0": token["x0"],
                "y0": token["y0"],
                "x1": token["x1"],
                "y1": token["y1"],
            })

        return anchors

    def _extract_line_excerpt(words: Sequence[Mapping[str, Any]], target_cy: float) -> str:
        same_row = [
            word for word in words
            if abs(float((float(word.get("y0") or 0.0) + float(word.get("y1") or 0.0)) / 2.0) - target_cy) <= 3.0
            and str(word.get("text") or "").strip()
        ]
        same_row = sorted(same_row, key=lambda item: float(item.get("x0") or 0.0))
        return re.sub(r"\s+", " ", " ".join([str(item.get("text") or "").strip() for item in same_row])).strip()

    def _fitz_anchor_monthly_rent() -> tuple[float | None, str | None, str | None, list[dict[str, Any]]]:
        heading_pages = [
            page for page in primary_pages
            if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_heading_patterns)
        ]
        if not heading_pages:
            heading_pages = [
                page for page in primary_pages
                if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_fallback_heading_patterns)
            ]

        debug_tokens: list[dict[str, Any]] = []
        best_candidate: dict[str, Any] | None = None

        for page in heading_pages:
            page_num = int(page.get("page_number") or 0)
            words = list(page.get("words") or [])
            if not words:
                continue

            anchors = _build_anchor_boxes_from_words(words)
            money_tokens = _extract_money_tokens_from_words(words)

            debug_tokens.extend([
                {
                    "page": page_num,
                    "text": token.get("text"),
                    "amount": token.get("amount"),
                    "x0": round(float(token.get("x0") or 0.0), 2),
                    "y0": round(float(token.get("y0") or 0.0), 2),
                    "x1": round(float(token.get("x1") or 0.0), 2),
                    "y1": round(float(token.get("y1") or 0.0), 2),
                }
                for token in money_tokens
            ])

            if not anchors or not money_tokens:
                continue

            for anchor in anchors:
                anchor_cy = (float(anchor["y0"]) + float(anchor["y1"])) / 2.0
                anchor_height = max(1.0, float(anchor["y1"]) - float(anchor["y0"]))
                y_tolerance = max(3.0, anchor_height * 0.75)

                for token in money_tokens:
                    dx = float(token["x0"]) - float(anchor["x1"])
                    dy = abs(float(token["cy"]) - anchor_cy)
                    same_row = dy <= y_tolerance
                    to_right = dx >= -1.5
                    below = float(token["cy"]) > anchor_cy

                    proximity_score = -((abs(dx) * 1.2) + (dy * 14.0))
                    rule = "other"
                    score_boost = 0.0

                    if to_right and same_row:
                        rule = "right_same_row"
                        score_boost = 3000.0
                    elif to_right:
                        rule = "right_diff_row"
                        score_boost = 2000.0
                    elif below:
                        rule = "below"
                        score_boost = 1000.0

                    if anchor.get("label") == "rent_and_charges":
                        score_boost += 120.0

                    line_excerpt = _extract_line_excerpt(words, float(token["cy"]))
                    line_excerpt_lower = line_excerpt.lower()
                    has_monthly_signal = bool(base_rent_monthly_signal_regex and base_rent_monthly_signal_regex.search(line_excerpt_lower))
                    has_total_signal = bool(base_rent_total_signal_regex and base_rent_total_signal_regex.search(line_excerpt_lower))

                    if has_monthly_signal:
                        score_boost += 950.0
                    if has_total_signal and not has_monthly_signal:
                        score_boost -= 1400.0
                    elif (
                        any(token in line_excerpt_lower for token in base_rent_total_context_tokens)
                        and not any(token in line_excerpt_lower for token in base_rent_installment_context_tokens)
                    ):
                        score_boost -= 350.0

                    score = score_boost + proximity_score
                    candidate = {
                        "amount": float(token["amount"]),
                        "page": page_num,
                        "anchor_label": anchor.get("label"),
                        "rule": rule,
                        "score": score,
                        "x0": float(token["x0"]),
                        "y0": float(token["y0"]),
                        "x1": float(token["x1"]),
                        "y1": float(token["y1"]),
                        "evidence": line_excerpt,
                    }

                    if best_candidate is None or float(candidate["score"]) > float(best_candidate["score"]):
                        best_candidate = candidate

        if best_candidate is None:
            return None, None, None, debug_tokens

        evidence = best_candidate.get("evidence") or (
            f"Rent anchor {best_candidate.get('anchor_label')} selected {best_candidate.get('amount'):.2f}"
        )
        method_detail = (
            f"fitz-anchor:{best_candidate.get('rule')} page={best_candidate.get('page')} "
            f"anchor={best_candidate.get('anchor_label')} "
            f"coords=({best_candidate.get('x0'):.1f},{best_candidate.get('y0'):.1f},"
            f"{best_candidate.get('x1'):.1f},{best_candidate.get('y1'):.1f})"
        )
        return float(best_candidate["amount"]), str(evidence), method_detail, debug_tokens

    def _multiple_inference_monthly_rent() -> tuple[float | None, str | None, str | None]:
        rent_pages = [
            page for page in primary_pages
            if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_heading_patterns)
        ]
        if not rent_pages:
            rent_pages = [
                page for page in primary_pages
                if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_fallback_heading_patterns)
            ]

        amounts: list[float] = []
        for page in rent_pages:
            words = list(page.get("words") or [])
            for token in _extract_money_tokens_from_words(words):
                amounts.append(round(float(token["amount"]), 2))

            page_text = str(page.get("text") or "")
            for raw in re.findall(r"(?<!\d)(\d{1,6}\.\d{2})(?!\d)", page_text.replace(",", "")):
                value = _as_float(raw)
                if value is not None and value > 0:
                    amounts.append(round(float(value), 2))

        if not amounts:
            return None, None, None

        counts: dict[float, int] = {}
        for amount in amounts:
            counts[amount] = counts.get(amount, 0) + 1

        unique_amounts = sorted(counts.keys())
        repeated_amounts = sorted([amount for amount, count in counts.items() if count >= 2])
        if not repeated_amounts:
            return None, None, None

        for monthly in repeated_amounts:
            for total in unique_amounts:
                if total <= monthly:
                    continue
                ratio = total / monthly
                n = int(round(ratio))
                if 6 <= n <= 24 and abs(total - (monthly * n)) <= 0.02:
                    evidence = f"Repeated amount {monthly:.2f} with total {total:.2f} = {n} x monthly"
                    method_detail = f"multiple-inference:n={n} repeated={monthly:.2f} total={total:.2f}"
                    return float(monthly), evidence, method_detail

        return None, None, None

    def _pdfplumber_anchor_monthly_rent() -> tuple[float | None, str | None, str | None, list[dict[str, Any]]]:
        if pdfplumber is None:
            return None, None, None, []

        pdf_path = str(doc_info.get("source_packet_path") or "").strip()
        if not pdf_path or not os.path.exists(pdf_path):
            return None, None, None, []

        target_pages = [
            int(page.get("page_number") or 0)
            for page in primary_pages
            if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_heading_patterns)
        ]
        if not target_pages:
            target_pages = [
                int(page.get("page_number") or 0)
                for page in primary_pages
                if any(pattern.search(str(page.get("text") or "")) for pattern in base_rent_fallback_heading_patterns)
            ]

        debug_tokens: list[dict[str, Any]] = []
        best_candidate: dict[str, Any] | None = None

        try:
            with pdfplumber.open(pdf_path) as pdf_doc:
                page_count = len(pdf_doc.pages)
                for page_num in target_pages:
                    if page_num < 1 or page_num > page_count:
                        continue
                    page = pdf_doc.pages[page_num - 1]
                    words_raw = page.extract_words() or []
                    words = [
                        {
                            "x0": float(word.get("x0") or 0.0),
                            "y0": float(word.get("top") or 0.0),
                            "x1": float(word.get("x1") or 0.0),
                            "y1": float(word.get("bottom") or 0.0),
                            "text": str(word.get("text") or ""),
                        }
                        for word in words_raw
                        if str(word.get("text") or "").strip()
                    ]

                    anchors = _build_anchor_boxes_from_words(words)
                    money_tokens = _extract_money_tokens_from_words(words)

                    debug_tokens.extend([
                        {
                            "page": page_num,
                            "text": token.get("text"),
                            "amount": token.get("amount"),
                            "x0": round(float(token.get("x0") or 0.0), 2),
                            "y0": round(float(token.get("y0") or 0.0), 2),
                            "x1": round(float(token.get("x1") or 0.0), 2),
                            "y1": round(float(token.get("y1") or 0.0), 2),
                        }
                        for token in money_tokens
                    ])

                    if not anchors or not money_tokens:
                        continue

                    for anchor in anchors:
                        anchor_cy = (float(anchor["y0"]) + float(anchor["y1"])) / 2.0
                        anchor_height = max(1.0, float(anchor["y1"]) - float(anchor["y0"]))
                        y_tolerance = max(3.0, anchor_height * 0.75)

                        for token in money_tokens:
                            dx = float(token["x0"]) - float(anchor["x1"])
                            dy = abs(float(token["cy"]) - anchor_cy)
                            same_row = dy <= y_tolerance
                            to_right = dx >= -1.5
                            below = float(token["cy"]) > anchor_cy

                            rule = "other"
                            score_boost = 0.0
                            if to_right and same_row:
                                rule = "right_same_row"
                                score_boost = 3000.0
                            elif to_right:
                                rule = "right_diff_row"
                                score_boost = 2000.0
                            elif below:
                                rule = "below"
                                score_boost = 1000.0

                            if anchor.get("label") == "rent_and_charges":
                                score_boost += 120.0

                            line_excerpt = _extract_line_excerpt(words, float(token["cy"]))
                            line_excerpt_lower = line_excerpt.lower()
                            has_monthly_signal = bool(base_rent_monthly_signal_regex and base_rent_monthly_signal_regex.search(line_excerpt_lower))
                            has_total_signal = bool(base_rent_total_signal_regex and base_rent_total_signal_regex.search(line_excerpt_lower))
                            if has_monthly_signal:
                                score_boost += 950.0
                            if has_total_signal and not has_monthly_signal:
                                score_boost -= 1400.0
                            elif (
                                any(token in line_excerpt_lower for token in base_rent_total_context_tokens)
                                and not any(token in line_excerpt_lower for token in base_rent_installment_context_tokens)
                            ):
                                score_boost -= 350.0

                            score = score_boost - ((abs(dx) * 1.2) + (dy * 14.0))
                            evidence = line_excerpt

                            candidate = {
                                "amount": float(token["amount"]),
                                "page": page_num,
                                "anchor_label": anchor.get("label"),
                                "rule": rule,
                                "score": score,
                                "x0": float(token["x0"]),
                                "y0": float(token["y0"]),
                                "x1": float(token["x1"]),
                                "y1": float(token["y1"]),
                                "evidence": evidence,
                            }
                            if best_candidate is None or float(candidate["score"]) > float(best_candidate["score"]):
                                best_candidate = candidate
        except Exception as e:
            logger.warning(f"[LEASE TERMS] pdfplumber fallback failed: {e}")
            return None, None, None, debug_tokens

        if best_candidate is None:
            return None, None, None, debug_tokens

        evidence = best_candidate.get("evidence") or (
            f"Rent anchor {best_candidate.get('anchor_label')} selected {best_candidate.get('amount'):.2f}"
        )
        method_detail = (
            f"pdfplumber-anchor:{best_candidate.get('rule')} page={best_candidate.get('page')} "
            f"anchor={best_candidate.get('anchor_label')} "
            f"coords=({best_candidate.get('x0'):.1f},{best_candidate.get('y0'):.1f},"
            f"{best_candidate.get('x1'):.1f},{best_candidate.get('y1'):.1f})"
        )
        return float(best_candidate["amount"]), str(evidence), method_detail, debug_tokens

    lease_window_months = _lease_window_months(lease_start, lease_end)

    fitz_rent_amount, fitz_rent_evidence, fitz_method_detail, fitz_debug_tokens = _fitz_anchor_monthly_rent()
    inferred_rent_amount, inferred_rent_evidence, inferred_method_detail = _multiple_inference_monthly_rent()

    if fitz_rent_amount is not None:
        use_inferred_instead = False
        fitz_evidence_lower = str(fitz_rent_evidence or "").lower()
        if inferred_rent_amount is not None and inferred_rent_amount > 0:
            ratio = float(fitz_rent_amount) / float(inferred_rent_amount)
            ratio_n = int(round(ratio))
            ratio_close = 6 <= ratio_n <= 24 and abs(float(fitz_rent_amount) - (float(inferred_rent_amount) * ratio_n)) <= 0.05

            has_total_without_installment = (
                any(token in fitz_evidence_lower for token in base_rent_total_context_tokens)
                and not any(token in fitz_evidence_lower for token in base_rent_installment_context_tokens)
                and not any(token in fitz_evidence_lower for token in base_rent_monthly_context_tokens)
            )

            lease_window_matches = False
            if lease_window_months is not None:
                lease_month_n = int(round(float(lease_window_months)))
                if 6 <= lease_month_n <= 24:
                    lease_window_matches = abs(float(fitz_rent_amount) - (float(inferred_rent_amount) * lease_month_n)) <= 0.05

            if (has_total_without_installment and float(inferred_rent_amount) < float(fitz_rent_amount)) or ratio_close or lease_window_matches:
                use_inferred_instead = True

        if use_inferred_instead and inferred_rent_amount is not None:
            base_rent_amount = inferred_rent_amount
            base_rent_evidence = inferred_rent_evidence
            logger.info(
                "[LEASE TERMS] BASE_RENT extracted method=%s value=%.2f (override fitz-total-like candidate %.2f)",
                inferred_method_detail,
                float(base_rent_amount),
                float(fitz_rent_amount),
            )
        else:
            base_rent_amount = fitz_rent_amount
            base_rent_evidence = fitz_rent_evidence
            logger.info(
                "[LEASE TERMS] BASE_RENT extracted method=%s value=%.2f",
                fitz_method_detail,
                float(base_rent_amount),
            )
    else:
        if inferred_rent_amount is not None:
            base_rent_amount = inferred_rent_amount
            base_rent_evidence = inferred_rent_evidence
            logger.info(
                "[LEASE TERMS] BASE_RENT extracted method=%s value=%.2f",
                inferred_method_detail,
                float(base_rent_amount),
            )
        else:
            plumber_rent_amount, plumber_rent_evidence, plumber_method_detail, plumber_debug_tokens = _pdfplumber_anchor_monthly_rent()
            if plumber_rent_amount is not None:
                base_rent_amount = plumber_rent_amount
                base_rent_evidence = plumber_rent_evidence
                logger.info(
                    "[LEASE TERMS] BASE_RENT extracted method=%s value=%.2f",
                    plumber_method_detail,
                    float(base_rent_amount),
                )
            else:
                logger.warning(
                    "[LEASE TERMS] BASE_RENT extraction failed for doc_id=%s fitz_tokens=%s pdfplumber_tokens=%s rule_failure=%s",
                    str(doc_info.get("doc_id") or ""),
                    json.dumps(fitz_debug_tokens[:120], default=str),
                    json.dumps(plumber_debug_tokens[:120], default=str),
                    "anchor_and_multiple_inference_failed",
                )

    def _normalize_amount_fragment(amount_fragment: str) -> str | None:
        fragment = str(amount_fragment or "")
        if not fragment.strip():
            return None

        fragment = re.sub(r"\s+", "", fragment)
        fragment = re.sub(r"[^0-9.,]", "", fragment)
        if not fragment:
            return None

        if "," in fragment and "." in fragment:
            fragment = fragment.replace(",", "")
        elif "," in fragment and "." not in fragment:
            comma_parts = fragment.split(",")
            if len(comma_parts[-1]) == 2:
                fragment = "".join(comma_parts[:-1]) + "." + comma_parts[-1]
            else:
                fragment = "".join(comma_parts)

        if fragment.count(".") > 1:
            first = fragment.find(".")
            fragment = fragment[: first + 1] + fragment[first + 1 :].replace(".", "")

        return fragment if re.search(r"\d", fragment) else None

    def _append_rent_candidate(amount_str: str, context: str, score: int) -> None:
        normalized_fragment = _normalize_amount_fragment(amount_str)
        if not normalized_fragment:
            return
        normalized = normalize_money(f"${normalized_fragment}")
        if not normalized:
            return
        amount_value = _as_float(normalized)
        if amount_value is None or amount_value <= 0:
            return
        context_lower = context.lower()
        if (
            any(token in context_lower for token in base_rent_total_rent_guard_tokens)
            and not any(token in context_lower for token in base_rent_installment_context_tokens)
        ):
            return
        if any(token in context_lower for token in base_rent_excluded_context_tokens):
            return
        base_rent_candidates.append({
            "amount": amount_value,
            "context": re.sub(r"\s+", " ", context).strip(),
            "score": score,
        })

    if base_rent_amount is None:
        for page in primary_pages:
            page_text = str(page.get("text") or "")
            if base_rent_installment_pattern:
                for match in base_rent_installment_pattern.finditer(page_text):
                    context = page_text[max(0, match.start() - 100):min(len(page_text), match.end() + 100)]
                    _append_rent_candidate(match.group(1), context, score=10)

            for pattern in base_rent_monthly_patterns:
                for match in pattern.finditer(page_text):
                    context = page_text[max(0, match.start() - 100):min(len(page_text), match.end() + 100)]
                    _append_rent_candidate(match.group(1), context, score=8)

        if base_rent_candidates:
            best_rent = sorted(base_rent_candidates, key=lambda c: (c["score"], c["amount"]), reverse=True)[0]
            base_rent_amount = best_rent["amount"]
            base_rent_evidence = best_rent["context"]
            logger.info(
                "[LEASE TERMS] BASE_RENT extracted method=%s value=%.2f",
                "regex-fallback",
                float(base_rent_amount),
            )

    def _extract_fee_candidates(
        text_value: str,
        include_patterns: Sequence[str],
        exclude_patterns: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        exclude_patterns = list(exclude_patterns or [])
        candidates: list[dict[str, Any]] = []
        seen_keys: set[tuple[float, str]] = set()
        for include_pattern in include_patterns:
            forward_regex = re.compile(
                rf"{include_pattern}.{{0,40}}?\$\s*([0-9][0-9,]*(?:\.\d{{2}})?)",
                re.IGNORECASE | re.DOTALL,
            )
            for match in forward_regex.finditer(text_value):
                evidence = text_value[max(0, match.start() - 40):min(len(text_value), match.end() + 40)]
                evidence_lower = evidence.lower()
                if any(re.search(pattern, evidence_lower, re.IGNORECASE) for pattern in exclude_patterns):
                    continue
                normalized = normalize_money(f"${match.group(1)}")
                if not normalized:
                    continue
                amount = _as_float(normalized)
                if amount is None or amount <= 0:
                    continue
                key = (amount, re.sub(r"\s+", " ", evidence).strip().lower())
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                candidates.append({
                    "amount": amount,
                    "evidence": re.sub(r"\s+", " ", evidence).strip(),
                })

        return candidates

    def _extract_application_admin_from_clause_windows(text_value: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Multi-capture extraction for clauses that mention application/admin fees together.

        Example target:
        "Non-Refundable Application Fee shall be $50 and the Administration Fee shall be $20"
        """
        application_candidates: list[dict[str, Any]] = []
        admin_candidates: list[dict[str, Any]] = []

        if not clause_anchor_pattern_text or not clause_app_amount_pattern_text or not clause_admin_amount_pattern_text:
            return application_candidates, admin_candidates

        clause_anchor = re.compile(clause_anchor_pattern_text, re.IGNORECASE | re.DOTALL)
        app_amount_regex = re.compile(clause_app_amount_pattern_text, re.IGNORECASE | re.DOTALL)
        admin_amount_regex = re.compile(clause_admin_amount_pattern_text, re.IGNORECASE | re.DOTALL)

        for clause_match in clause_anchor.finditer(text_value):
            window_text = text_value[max(0, clause_match.start() - 40):min(len(text_value), clause_match.end() + 40)]
            window_text_normalized = re.sub(r"\s+", " ", window_text).strip()
            window_lower = window_text_normalized.lower()

            if any(token in window_lower for token in clause_excluded_tokens):
                continue

            app_match = app_amount_regex.search(window_text)
            if app_match:
                app_normalized = normalize_money(f"${app_match.group(1)}")
                app_amount = _as_float(app_normalized) if app_normalized else None
                if app_amount is not None and app_amount > 0:
                    application_candidates.append({
                        "amount": app_amount,
                        "evidence": window_text_normalized,
                    })

            admin_match = admin_amount_regex.search(window_text)
            if admin_match:
                if any(token in window_lower for token in admin_prioritization_excluded_tokens):
                    continue
                admin_normalized = normalize_money(f"${admin_match.group(1)}")
                admin_amount = _as_float(admin_normalized) if admin_normalized else None
                if admin_amount is not None and admin_amount > 0:
                    admin_candidates.append({
                        "amount": admin_amount,
                        "evidence": window_text_normalized,
                    })

        return application_candidates, admin_candidates

    def _dedupe_fee_candidates(candidates: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[float, str]] = set()
        for candidate in candidates:
            amount = _as_float(candidate.get("amount"))
            evidence = re.sub(r"\s+", " ", str(candidate.get("evidence") or "")).strip()
            if amount is None or amount <= 0 or not evidence:
                continue
            key = (amount, evidence.lower())
            if key in seen:
                continue
            seen.add(key)
            deduped.append({"amount": amount, "evidence": evidence})
        return deduped

    def _is_application_amount_leak_in_admin_context(evidence_text: str, admin_amount: float | None) -> bool:
        """Detect mixed-line false positives where admin match captures application fee amount."""
        if admin_amount is None or admin_amount <= 0:
            return False

        evidence_lower = str(evidence_text or "").lower()
        if admin_application_leak_required_tokens and not all(
            token in evidence_lower for token in admin_application_leak_required_tokens
        ):
            return False

        if admin_application_leak_amount_regex is None:
            return False

        app_amount_match = admin_application_leak_amount_regex.search(evidence_lower)
        if not app_amount_match:
            return False

        app_normalized = normalize_money(f"${app_amount_match.group(1)}")
        app_amount = _as_float(app_normalized) if app_normalized else None
        if app_amount is None:
            return False

        return abs(float(app_amount) - float(admin_amount)) <= 0.005

    def _prioritize_admin_fee_candidates(candidates: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped = _dedupe_fee_candidates(candidates)
        if not deduped:
            return []

        explicit = [
            item
            for item in deduped
            if any(marker in str(item.get("evidence") or "").lower() for marker in admin_explicit_clause_markers)
            and not any(
                token in str(item.get("evidence") or "").lower()
                for token in admin_prioritization_excluded_tokens
            )
        ]
        if explicit:
            return explicit

        filtered = [
            item
            for item in deduped
            if not any(
                token in str(item.get("evidence") or "").lower()
                for token in admin_prioritization_excluded_tokens
            )
        ]
        return filtered or deduped

    def _extract_strict_admin_fee_candidates(text_value: str) -> list[dict[str, Any]]:
        strict_patterns = [
            re.compile(pattern, re.IGNORECASE | re.DOTALL)
            for pattern in admin_strict_pattern_texts
        ]
        strict_candidates: list[dict[str, Any]] = []
        for pattern in strict_patterns:
            for match in pattern.finditer(text_value):
                evidence = text_value[max(0, match.start() - 80):min(len(text_value), match.end() + 80)]
                evidence_norm = re.sub(r"\s+", " ", evidence).strip()
                evidence_lower = evidence_norm.lower()
                if any(token in evidence_lower for token in admin_prioritization_excluded_tokens):
                    continue
                normalized = normalize_money(f"${match.group(1)}")
                amount = _as_float(normalized) if normalized else None
                if amount is None or amount <= 0:
                    continue
                if _is_application_amount_leak_in_admin_context(evidence_norm, amount):
                    continue
                strict_candidates.append({"amount": amount, "evidence": evidence_norm})
        return _dedupe_fee_candidates(strict_candidates)

    application_fee_candidates = _extract_fee_candidates(
        all_text,
        include_patterns=application_include_patterns,
    )
    admin_fee_candidates = _extract_fee_candidates(
        all_text,
        include_patterns=admin_include_patterns,
        exclude_patterns=admin_exclude_patterns,
    )
    strict_admin_fee_candidates = _extract_strict_admin_fee_candidates(all_text)
    clause_app_candidates, clause_admin_candidates = _extract_application_admin_from_clause_windows(all_text)
    if clause_app_candidates:
        application_fee_candidates = _dedupe_fee_candidates(clause_app_candidates)
    else:
        application_fee_candidates = _dedupe_fee_candidates(application_fee_candidates)

    if strict_admin_fee_candidates:
        admin_fee_candidates = _prioritize_admin_fee_candidates(strict_admin_fee_candidates)
    elif clause_admin_candidates:
        admin_fee_candidates = _prioritize_admin_fee_candidates(clause_admin_candidates)
    else:
        admin_fee_candidates = _prioritize_admin_fee_candidates(admin_fee_candidates)
    def _extract_focus_windows(
        text_value: str,
        focus_patterns: Sequence[str],
        radius_before: int = 500,
        radius_after: int = 900,
    ) -> str:
        if not text_value.strip():
            return ""

        windows: list[str] = []
        for pattern in focus_patterns:
            for match in re.finditer(pattern, text_value, re.IGNORECASE):
                start = max(0, match.start() - radius_before)
                end = min(len(text_value), match.end() + radius_after)
                snippet = text_value[start:end]
                snippet = re.sub(r"\s+", " ", snippet).strip()
                if snippet:
                    windows.append(snippet)

        if not windows:
            return ""
        return "\n".join(windows)

    amenity_rule = get_term_extraction_rule("AMENITY_PREMIUM")
    amenity_patterns = list(amenity_rule.get("include_patterns") or [])
    amenity_exclude_patterns = list(amenity_rule.get("exclude_patterns") or [])
    exclusive_bedspace_focus_patterns = list(amenity_rule.get("focus_patterns") or [])
    amenity_source_order = [
        str(item).strip().lower()
        for item in (amenity_rule.get("source_order") or ["focus", "addenda", "all_text"])
        if str(item).strip()
    ]

    amenity_candidates: list[dict[str, Any]] = []

    bedspace_focus_text = _extract_focus_windows(
        addenda_text if addenda_text.strip() else all_text,
        exclusive_bedspace_focus_patterns,
    )

    for extraction_source in amenity_source_order:
        source_text = ""
        if extraction_source == "focus":
            source_text = bedspace_focus_text
        elif extraction_source == "addenda":
            source_text = addenda_text
        elif extraction_source == "all_text":
            source_text = all_text
        else:
            continue

        if not source_text or not source_text.strip():
            continue

        amenity_candidates = _extract_fee_candidates(
            source_text,
            include_patterns=amenity_patterns,
            exclude_patterns=amenity_exclude_patterns,
        )
        if amenity_candidates:
            break

    amenity_premium = amenity_candidates[0]["amount"] if amenity_candidates else None
    amenity_evidence = amenity_candidates[0]["evidence"] if amenity_candidates else None

    def _add_term_row(
        term_type: str,
        mapped_ar_code: str,
        amount: float | None,
        evidence: str | None,
        frequency: str = "monthly",
        confidence: float = 0.7,
        key_suffix: str | None = None,
    ) -> None:
        if amount is None:
            return
        term_key = f"{term_type}:{mapped_ar_code}:{lease_start or ''}:{lease_end or ''}{f':{key_suffix}' if key_suffix else ''}"
        terms.append({
            "term_key": term_key,
            "term_type": term_type,
            "mapped_ar_code": mapped_ar_code,
            "term_label": term_type.replace("_", " ").title(),
            "amount": amount,
            "frequency": frequency,
            "start_date": lease_start,
            "end_date": lease_end,
            "term_source_doc_id": str(doc_info.get("doc_id") or ""),
            "term_source_doc_name": str(doc_info.get("title") or ""),
            "mapping_confidence": confidence,
        })
        evidence_rows.append({
            "term_key": term_key,
            "doc_id": str(doc_info.get("doc_id") or ""),
            "doc_name": str(doc_info.get("title") or ""),
            "page_number": None,
            "excerpt_text": str(evidence or "")[:500],
            "confidence": confidence,
        })

    _add_term_row(
        "BASE_RENT",
        get_primary_ar_code_for_term("BASE_RENT", fallback="RENT"),
        base_rent_amount,
        base_rent_evidence,
        frequency="monthly",
        confidence=0.85,
    )

    def _extract_amount_by_anchors(
        source_pages: Sequence[Mapping[str, Any]],
        anchor_patterns: Sequence[str],
        preferred_tokens: Sequence[str] | None = None,
        excluded_tokens: Sequence[str] | None = None,
        fallback_anchor_token: str | None = None,
        hard_excluded_tokens: Sequence[str] | None = None,
    ) -> dict[str, Any] | None:
        preferred_tokens = [str(token).lower() for token in (preferred_tokens or [])]
        excluded_tokens = [str(token).lower() for token in (excluded_tokens or [])]
        hard_excluded_tokens = [str(token).lower() for token in (hard_excluded_tokens or [])]

        def _iter_currency_matches(text_value: str):
            for match in re.finditer(r"\$\s*[_:\-\.]*\s*([0-9][0-9,]*(?:\.\d{2})?)", text_value):
                yield match
            for match in re.finditer(r"\b([0-9][0-9,]*\.\d{2})\b", text_value):
                yield match

        best: dict[str, Any] | None = None
        for page in source_pages:
            page_text = str(page.get("text") or "")
            page_number = page.get("page_number")

            for anchor_pattern in anchor_patterns:
                for anchor_match in re.finditer(anchor_pattern, page_text, re.IGNORECASE):
                    window_start = max(0, anchor_match.start() - 120)
                    window_end = min(len(page_text), anchor_match.end() + 220)
                    window_text = page_text[window_start:window_end]
                    window_lower = window_text.lower()

                    if excluded_tokens and any(token in window_lower for token in excluded_tokens):
                        continue

                    for amount_match in _iter_currency_matches(window_text):
                        normalized = normalize_money(f"${amount_match.group(1)}")
                        if not normalized:
                            continue
                        amount = _as_float(normalized)
                        if amount is None or amount <= 0:
                            continue

                        amount_context_start = max(0, amount_match.start() - 80)
                        amount_context_end = min(len(window_text), amount_match.end() + 80)
                        amount_context = window_text[amount_context_start:amount_context_end]
                        amount_context_lower = amount_context.lower()

                        if excluded_tokens and any(token in amount_context_lower for token in excluded_tokens):
                            continue
                        if any(token in amount_context_lower for token in hard_excluded_tokens):
                            continue

                        score = 1
                        if preferred_tokens:
                            score += sum(1 for token in preferred_tokens if token in amount_context_lower)
                        if "monthly" in amount_context_lower:
                            score += 2
                        if fallback_anchor_token and fallback_anchor_token in window_lower:
                            score += 1

                        candidate = {
                            "amount": amount,
                            "evidence": re.sub(r"\s+", " ", amount_context).strip(),
                            "page_number": page_number,
                            "score": score,
                        }

                        if best is None or (candidate["score"], candidate["amount"]) > (best["score"], best["amount"]):
                            best = candidate

        if best is not None:
            return best

        if fallback_anchor_token:
            fallback_pages = []
            for page in source_pages:
                page_text = str(page.get("text") or "")
                if fallback_anchor_token.lower() in page_text.lower():
                    fallback_pages.append(page)

            for page in fallback_pages:
                page_text = str(page.get("text") or "")
                page_number = page.get("page_number")
                for amount_match in _iter_currency_matches(page_text):
                    normalized = normalize_money(f"${amount_match.group(1)}")
                    if not normalized:
                        continue
                    amount = _as_float(normalized)
                    if amount is None or amount <= 0:
                        continue
                    context_start = max(0, amount_match.start() - 120)
                    context_end = min(len(page_text), amount_match.end() + 120)
                    amount_context = page_text[context_start:context_end]
                    amount_context_lower = amount_context.lower()
                    if excluded_tokens and any(token in amount_context_lower for token in excluded_tokens):
                        continue
                    if any(token in amount_context_lower for token in hard_excluded_tokens):
                        continue
                    if fallback_anchor_token and fallback_anchor_token.lower() not in amount_context_lower:
                        continue
                    score = 1 + sum(1 for token in preferred_tokens if token in amount_context_lower)
                    if "monthly" in amount_context_lower:
                        score += 2
                    candidate = {
                        "amount": amount,
                        "evidence": re.sub(r"\s+", " ", amount_context).strip(),
                        "page_number": page_number,
                        "score": score,
                    }
                    if best is None or (candidate["score"], candidate["amount"]) > (best["score"], best["amount"]):
                        best = candidate

        return best

    parking_candidate = extract_parking_fee(text_pack, identify_relevant_pages(text_pack))
    if parking_candidate:
        term_key_suffix = "PARKING"
        _add_term_row(
            "PARKING",
            get_primary_ar_code_for_term("PARKING", fallback="PARK"),
            _as_float(parking_candidate.get("normalized") or parking_candidate.get("value")),
            parking_candidate.get("evidence"),
            frequency="monthly",
            confidence=0.75,
            key_suffix=term_key_suffix,
        )
        if evidence_rows:
            evidence_rows[-1]["page_number"] = parking_candidate.get("page_number")

    pet_rent_rule = get_term_extraction_rule("PET_RENT")
    pet_candidate = _extract_amount_by_anchors(
        source_pages=pages,
        anchor_patterns=list(pet_rent_rule.get("anchor_patterns") or []),
        preferred_tokens=list(pet_rent_rule.get("preferred_tokens") or []),
        excluded_tokens=list(pet_rent_rule.get("excluded_tokens") or []),
        hard_excluded_tokens=list(pet_rent_rule.get("hard_excluded_tokens") or []),
    )
    if pet_candidate:
        term_key_suffix = "PET"
        _add_term_row(
            "PET_RENT",
            get_primary_ar_code_for_term("PET_RENT", fallback="PETR"),
            pet_candidate.get("amount"),
            pet_candidate.get("evidence"),
            frequency="monthly",
            confidence=0.7,
            key_suffix=term_key_suffix,
        )
        if evidence_rows:
            evidence_rows[-1]["page_number"] = pet_candidate.get("page_number")

    for idx, candidate in enumerate(application_fee_candidates):
        _add_term_row(
            "APPLICATION_FEE",
            get_primary_ar_code_for_term("APPLICATION_FEE", fallback="APPF"),
            candidate.get("amount"),
            candidate.get("evidence"),
            frequency="one_time",
            confidence=0.8,
            key_suffix=f"{idx + 1}",
        )

    for idx, candidate in enumerate(admin_fee_candidates):
        _add_term_row(
            "ADMIN_FEE",
            get_primary_ar_code_for_term("ADMIN_FEE", fallback="ADMF"),
            candidate.get("amount"),
            candidate.get("evidence"),
            frequency="one_time",
            confidence=0.8,
            key_suffix=f"{idx + 1}",
        )

    _add_term_row(
        "AMENITY_PREMIUM",
        get_primary_ar_code_for_term("AMENITY_PREMIUM", fallback="AMEN"),
        amenity_premium,
        amenity_evidence,
        frequency="monthly",
        confidence=0.75,
    )

    logger.info(
        "[LEASE TERMS] Extracted %s term rows for doc_id=%s title=%s",
        len(terms),
        str(doc_info.get("doc_id") or ""),
        str(doc_info.get("title") or ""),
    )
    for term in terms:
        logger.info(
            "[LEASE TERMS] term_type=%s ar_code=%s amount=%s frequency=%s start=%s end=%s confidence=%s",
            term.get("term_type"),
            term.get("mapped_ar_code"),
            term.get("amount"),
            term.get("frequency"),
            term.get("start_date"),
            term.get("end_date"),
            term.get("mapping_confidence"),
        )

    for evidence in evidence_rows:
        logger.info(
            "[LEASE TERMS] evidence term_key=%s page=%s excerpt=%s",
            evidence.get("term_key"),
            evidence.get("page_number"),
            str(evidence.get("excerpt_text") or "")[:220],
        )

    return terms, evidence_rows


def refresh_lease_terms_for_lease_interval(
    storage_service: Any,
    property_id: int,
    lease_interval_id: int,
    lease_id: int | None = None,
    run_id: str | None = None,
    audit_period_start: Any = None,
    audit_period_end: Any = None,
    force_refresh: bool = False,
    min_recheck_hours: int = 24,
    term_extractor: Callable[[Mapping[str, Any], Mapping[str, Any]], tuple[list[dict[str, Any]], list[dict[str, Any]]]] | None = None,
) -> dict[str, Any]:
    """
    Fingerprint + incremental refresh pipeline for lease terms.

    Fail-open behavior: if refresh fails and cached terms exist, returns cached terms with stale status.
    """
    lease_key = f"{int(property_id)}:{int(lease_interval_id)}"
    lease_identifier = lease_id if lease_id is not None else lease_interval_id
    now = datetime.utcnow()

    cached_term_set = {}
    cached_terms_df = pd.DataFrame()
    try:
        cached_term_set = storage_service.load_lease_term_set_for_lease_key(lease_key) or {}
    except Exception:
        cached_term_set = {}

    try:
        cached_terms_df = storage_service.load_lease_terms_for_lease_key_from_sharepoint_list(lease_key)
    except Exception:
        cached_terms_df = pd.DataFrame()

    cached_run_id_last_seen = str(cached_term_set.get("run_id_last_seen") or "").strip()
    current_run_id = str(run_id or "").strip()
    is_new_run_context = bool(current_run_id and current_run_id != cached_run_id_last_seen)

    if not force_refresh and cached_term_set:
        last_checked_raw = cached_term_set.get("last_checked_at")
        existing_doc_list_fingerprint = str(cached_term_set.get("doc_list_fingerprint") or "").strip()
        last_checked = pd.to_datetime(last_checked_raw, errors="coerce", utc=True)
        if not pd.isna(last_checked):
            now_utc = pd.Timestamp.now(tz="UTC")
            age = now_utc - last_checked
            if (
                age <= timedelta(hours=max(1, int(min_recheck_hours)))
                and existing_doc_list_fingerprint
                and not is_new_run_context
            ):
                return {
                    "lease_key": lease_key,
                    "status": "cached_recent",
                    "refreshed": False,
                    "terms_df": cached_terms_df,
                    "term_set": cached_term_set,
                }

    try:
        docs = fetch_lease_documents_list(property_id=property_id, lease_id=lease_identifier)
        doc_list_fingerprint = build_doc_list_fingerprint(docs)
        existing_doc_list_fingerprint = str(cached_term_set.get("doc_list_fingerprint") or "")
        term_set_version = int(cached_term_set.get("term_set_version") or 0)

        if (
            existing_doc_list_fingerprint
            and existing_doc_list_fingerprint == doc_list_fingerprint
            and not cached_terms_df.empty
        ):
            storage_service.upsert_lease_term_set_to_sharepoint_list({
                "lease_key": lease_key,
                "property_id": property_id,
                "lease_interval_id": lease_interval_id,
                "lease_id": lease_id,
                "term_set_version": max(1, term_set_version),
                "fingerprint_hash": cached_term_set.get("fingerprint_hash") or "",
                "doc_list_fingerprint": doc_list_fingerprint,
                "selected_doc_ids": cached_term_set.get("selected_doc_ids") or "",
                "last_checked_at": now.isoformat(),
                "last_refreshed_at": cached_term_set.get("last_refreshed_at") or now.isoformat(),
                "status": "active",
                "run_id_last_seen": current_run_id,
            })

            return {
                "lease_key": lease_key,
                "status": "doc_list_unchanged",
                "refreshed": False,
                "terms_df": cached_terms_df,
                "term_set": cached_term_set,
            }

        primary_doc, addenda_docs, selection_reason = select_lease_packet_and_addenda(
            docs,
            audit_period_start=audit_period_start,
            audit_period_end=audit_period_end,
        )
        fingerprint_hash = build_selected_docs_fingerprint(primary_doc, addenda_docs)

        existing_fingerprint = str(cached_term_set.get("fingerprint_hash") or "")

        if existing_fingerprint and existing_fingerprint == fingerprint_hash and not cached_terms_df.empty:
            storage_service.upsert_lease_term_set_to_sharepoint_list({
                "lease_key": lease_key,
                "property_id": property_id,
                "lease_interval_id": lease_interval_id,
                "lease_id": lease_id,
                "term_set_version": max(1, term_set_version),
                "fingerprint_hash": fingerprint_hash,
                "doc_list_fingerprint": doc_list_fingerprint,
                "selected_doc_ids": ",".join(
                    [get_doc_id(primary_doc)] + [str(item.get("doc_id") or "") for item in addenda_docs]
                ),
                "last_checked_at": now.isoformat(),
                "last_refreshed_at": cached_term_set.get("last_refreshed_at") or now.isoformat(),
                "status": "active",
                "run_id_last_seen": current_run_id,
            })

            return {
                "lease_key": lease_key,
                "status": "fingerprint_unchanged",
                "refreshed": False,
                "terms_df": cached_terms_df,
                "term_set": cached_term_set,
            }

        pdf_bytes, _, doc_info = download_lease_document(
            property_id=property_id,
            lease_id=lease_identifier,
            docs=docs,
            audit_period_start=audit_period_start,
            audit_period_end=audit_period_end,
            storage_service=storage_service,
        )
        text_pack = parse_pdf_to_text_pack(pdf_bytes)
        extractor = term_extractor or _extract_basic_terms_from_text_pack
        extracted_terms, evidence_rows = extractor(text_pack, doc_info)

        new_version = term_set_version + 1 if term_set_version > 0 else 1

        normalized_term_rows = []
        for idx, term in enumerate(extracted_terms):
            source_term_key = str(term.get("term_key") or "").strip()
            if source_term_key:
                if source_term_key.startswith(f"{lease_key}:"):
                    term_key = source_term_key
                else:
                    term_key = f"{lease_key}:{source_term_key}"
            else:
                term_key = f"{lease_key}:{idx}:{term.get('term_type') or 'TERM'}"
            normalized_term_rows.append({
                "term_key": term_key,
                "lease_key": lease_key,
                "property_id": property_id,
                "lease_interval_id": lease_interval_id,
                "lease_id": lease_id,
                "term_set_version": new_version,
                "is_active": True,
                "term_type": term.get("term_type") or "OTHER",
                "mapped_ar_code": term.get("mapped_ar_code") or "",
                "amount": term.get("amount"),
                "frequency": term.get("frequency"),
                "start_date": term.get("start_date"),
                "end_date": term.get("end_date"),
                "due_day": term.get("due_day"),
                "conditions_key": term.get("conditions_key"),
                "term_source_doc_id": term.get("term_source_doc_id") or doc_info.get("doc_id"),
                "term_source_doc_name": term.get("term_source_doc_name") or doc_info.get("title"),
                "mapping_version": term.get("mapping_version") or "v1",
                "mapping_confidence": term.get("mapping_confidence") or 0,
                "updated_at": now.isoformat(),
            })

        normalized_evidence_rows = []
        for idx, evidence in enumerate(evidence_rows):
            term_key = str(evidence.get("term_key") or (normalized_term_rows[0]["term_key"] if normalized_term_rows else ""))
            evidence_key = f"{term_key}:{evidence.get('doc_id') or doc_info.get('doc_id') or ''}:{evidence.get('page_number') or idx}:{idx}"
            normalized_evidence_rows.append({
                "evidence_key": evidence_key,
                "term_key": term_key,
                "lease_key": lease_key,
                "property_id": property_id,
                "lease_interval_id": lease_interval_id,
                "lease_id": lease_id,
                "doc_id": evidence.get("doc_id") or doc_info.get("doc_id"),
                "doc_name": evidence.get("doc_name") or doc_info.get("title"),
                "page_number": evidence.get("page_number"),
                "excerpt_text": evidence.get("excerpt_text"),
                "confidence": evidence.get("confidence") or 0,
                "captured_at": now.isoformat(),
            })

        storage_service.replace_lease_terms_to_sharepoint_list(lease_key, normalized_term_rows)
        storage_service.replace_lease_term_evidence_to_sharepoint_list(lease_key, normalized_evidence_rows)
        storage_service.upsert_lease_term_set_to_sharepoint_list({
            "lease_key": lease_key,
            "property_id": property_id,
            "lease_interval_id": lease_interval_id,
            "lease_id": lease_id,
            "term_set_version": new_version,
            "fingerprint_hash": fingerprint_hash,
            "doc_list_fingerprint": doc_list_fingerprint,
            "selected_doc_ids": ",".join([get_doc_id(primary_doc)] + [str(item.get("doc_id") or "") for item in addenda_docs]),
            "last_checked_at": now.isoformat(),
            "last_refreshed_at": now.isoformat(),
            "status": "active",
            "run_id_last_seen": current_run_id,
            "refresh_error": "",
        })

        terms_df = pd.DataFrame(normalized_term_rows)
        return {
            "lease_key": lease_key,
            "status": "refreshed",
            "refreshed": True,
            "selection_reason": selection_reason,
            "terms_df": terms_df,
        }
    except Exception as refresh_error:
        logger.warning(f"[LEASE TERMS] Refresh failed for {lease_key}: {refresh_error}")
        try:
            storage_service.upsert_lease_term_set_to_sharepoint_list({
                "lease_key": lease_key,
                "property_id": property_id,
                "lease_interval_id": lease_interval_id,
                "lease_id": lease_id,
                "term_set_version": int(cached_term_set.get("term_set_version") or 1),
                "fingerprint_hash": cached_term_set.get("fingerprint_hash") or "",
                "doc_list_fingerprint": cached_term_set.get("doc_list_fingerprint") or "",
                "selected_doc_ids": cached_term_set.get("selected_doc_ids") or "",
                "last_checked_at": now.isoformat(),
                "last_refreshed_at": cached_term_set.get("last_refreshed_at") or "",
                "status": "stale" if not cached_terms_df.empty else "error",
                "refresh_error": str(refresh_error),
                "run_id_last_seen": current_run_id or cached_run_id_last_seen,
            })
        except Exception:
            pass

        return {
            "lease_key": lease_key,
            "status": "stale_cached" if not cached_terms_df.empty else "error",
            "refreshed": False,
            "error": str(refresh_error),
            "terms_df": cached_terms_df,
        }


def _coerce_to_date_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan"}:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return text
    return parsed.strftime("%Y-%m-%d")


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    normalized = normalize_money(str(value)) if isinstance(value, str) else str(value)
    if normalized is None:
        return None
    try:
        return float(normalized)
    except Exception:
        return None


def _normalize_ar_code_token(value: Any) -> str | None:
    if value is None:
        return None
    token = str(value).strip().upper()
    if not token or token in {"NAN", "NONE"}:
        return None
    return token


def _normalize_label(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _ensure_records(records: Any) -> list[dict[str, Any]]:
    if records is None:
        return []
    if isinstance(records, pd.DataFrame):
        return records.to_dict(orient="records")
    if isinstance(records, list):
        return [item for item in records if isinstance(item, dict)]
    return []


def build_term_ar_code_registry(custom_rules: Sequence[Mapping[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Build normalized mapping registry; pass custom rules to extend/override defaults."""
    base_rules = get_term_to_ar_code_rules(
        custom_rules=[dict(rule) for rule in custom_rules] if custom_rules else None
    )

    normalized_rules: list[dict[str, Any]] = []
    for rule in base_rules:
        term_type = str(rule.get("term_type") or "").strip().upper()
        if not term_type:
            continue
        label_patterns = [p for p in (rule.get("label_patterns") or []) if p]
        accepted_codes = [
            token for token in (_normalize_ar_code_token(code) for code in (rule.get("accepted_ar_codes") or [])) if token
        ]
        normalized_rules.append({
            "term_type": term_type,
            "label_patterns": label_patterns,
            "accepted_ar_codes": accepted_codes,
            "expected_frequency": rule.get("expected_frequency"),
        })

    return normalized_rules


def _infer_term_type(term_record: Mapping[str, Any], registry: Sequence[Mapping[str, Any]]) -> str | None:
    explicit = term_record.get("term_type")
    if explicit:
        return str(explicit).strip().upper()

    label = _normalize_label(
        term_record.get("term_label")
        or term_record.get("term_name")
        or term_record.get("description")
        or term_record.get("label")
    )
    if not label:
        return None

    for rule in registry:
        for pattern in rule.get("label_patterns", []):
            if re.search(pattern, label, re.IGNORECASE):
                return str(rule.get("term_type") or "").upper() or None
    return None


def _resolve_term_codes(term_record: Mapping[str, Any], registry_by_type: Mapping[str, Mapping[str, Any]]) -> list[str]:
    explicit_codes = term_record.get("accepted_ar_codes") or term_record.get("ar_codes")
    if explicit_codes:
        if isinstance(explicit_codes, str):
            explicit_codes = [explicit_codes]
        return [
            code for code in (_normalize_ar_code_token(value) for value in explicit_codes) if code
        ]

    explicit_single = _normalize_ar_code_token(term_record.get("ar_code") or term_record.get("ar_code_id"))
    if explicit_single:
        return [explicit_single]

    term_type = _infer_term_type(term_record, list(registry_by_type.values()))
    if not term_type:
        return []
    return list(registry_by_type.get(term_type, {}).get("accepted_ar_codes", []))


def build_lease_expectation_overlay(
    all_ar_codes: Sequence[Mapping[str, Any]],
    lease_term_records: Any,
    custom_rules: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Build AR-oriented lease expectation payload with scalable term mapping.

    Returns:
      {
        "ar_groups": [...],
        "lease_only_expectations": [...],
        "mapping_diagnostics": {...}
      }
    """
    registry = build_term_ar_code_registry(custom_rules)
    registry_by_type = {str(rule.get("term_type")): rule for rule in registry}
    term_records = _ensure_records(lease_term_records)

    augmented_groups: list[dict[str, Any]] = [dict(item) for item in all_ar_codes]
    by_ar_code: dict[str, dict[str, Any]] = {}
    for group in augmented_groups:
        ar_code = _normalize_ar_code_token(group.get("ar_code_id"))
        if not ar_code:
            continue
        by_ar_code[ar_code] = group
        ar_display = format_ar_code_display(ar_code)
        group.setdefault("lease_expectation", {
            "has_term": False,
            "status": "no_lease_term",
            "message": f"AR has '{ar_display}' charges but no mapped lease term found.",
            "terms": [],
        })

    lease_only_expectations: list[dict[str, Any]] = []
    mapped_count = 0
    unmapped_count = 0

    for term in term_records:
        term_type = _infer_term_type(term, registry)
        candidate_codes = _resolve_term_codes(term, registry_by_type)

        amount = _as_float(
            term.get("amount")
            or term.get("rent_amount")
            or term.get("base_rent")
            or term.get("monthly_amount")
        )
        frequency = term.get("frequency") or term.get("expected_frequency")
        if not frequency and term_type in registry_by_type:
            frequency = registry_by_type[term_type].get("expected_frequency")

        start_date = _coerce_to_date_string(term.get("start_date") or term.get("lease_start_date") or term.get("effective_start"))
        end_date = _coerce_to_date_string(term.get("end_date") or term.get("lease_end_date") or term.get("effective_end"))
        evidence = term.get("evidence") or term.get("evidence_text") or term.get("source_snippet")
        term_label = (
            term.get("term_label")
            or term.get("term_name")
            or term.get("description")
            or term_type
            or "Lease term"
        )

        term_payload = {
            "term_type": term_type,
            "term_label": term_label,
            "amount": amount,
            "frequency": frequency,
            "start_date": start_date,
            "end_date": end_date,
            "evidence": evidence,
            "candidate_ar_codes": candidate_codes,
            "candidate_ar_code_labels": [format_ar_code_display(code) for code in candidate_codes],
        }

        attached = False
        for code in candidate_codes:
            target = by_ar_code.get(code)
            if not target:
                continue
            expectation = target.setdefault("lease_expectation", {
                "has_term": False,
                "status": "no_lease_term",
                "message": "",
                "terms": [],
            })
            expectation["has_term"] = True
            expectation["status"] = "mapped"
            expectation["terms"].append(term_payload)
            expectation["message"] = ""
            attached = True

        if attached:
            mapped_count += 1
            continue

        unmapped_count += 1
        if candidate_codes:
            candidate_labels = [format_ar_code_display(code) for code in candidate_codes]
            amount_text = f"${amount:,.2f}" if amount is not None else "(no amount)"
            frequency_token = str(frequency or "").strip().lower()
            if frequency_token in {"one_time", "onetime", "one-time"}:
                detail_text = "one time"
            elif frequency_token in {"monthly", "per_month", "per month"}:
                detail_text = "monthly"
            else:
                detail_text = frequency_token.replace("_", " ") if frequency_token else "unspecified"
            lease_only_expectations.append({
                **term_payload,
                "message": (
                    f"{', '.join(candidate_labels)} {amount_text} / {detail_text}"
                ).strip(),
            })
        else:
            lease_only_expectations.append({
                **term_payload,
                "message": f"Lease term '{term_label}' could not be mapped to an AR code.",
            })

    for group in augmented_groups:
        expectation = group.get("lease_expectation") or {}
        if expectation.get("has_term"):
            terms = expectation.get("terms", [])
            lines = []
            for term in terms:
                amount = term.get("amount")
                amount_text = f"${amount:,.2f}" if isinstance(amount, (int, float)) else "(no amount)"
                freq_text = term.get("frequency") or ""
                range_text = ""
                if term.get("start_date") or term.get("end_date"):
                    range_text = f"{term.get('start_date') or '?'} → {term.get('end_date') or '?'}"
                lines.append(" ".join(part for part in [amount_text, freq_text, range_text] if part).strip())
            expectation["summary_text"] = "; ".join(lines)
            first_evidence = next((t.get("evidence") for t in terms if t.get("evidence")), None)
            expectation["evidence"] = first_evidence
            expectation["message"] = ""

    diagnostics = {
        "total_terms": len(term_records),
        "mapped_terms": mapped_count,
        "unmapped_terms": unmapped_count,
        "rules_loaded": len(registry),
    }

    return {
        "ar_groups": augmented_groups,
        "lease_only_expectations": lease_only_expectations,
        "mapping_diagnostics": diagnostics,
    }
