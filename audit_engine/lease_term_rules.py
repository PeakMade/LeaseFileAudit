from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path
from typing import Any


DEFAULT_TERM_TO_AR_CODE_RULES: list[dict[str, Any]] = [
    {
        "term_type": "BASE_RENT",
        "label_patterns": [r"base\s*rent", r"monthly\s*rent", r"rent"],
        "accepted_ar_codes": ["154771"],
        "expected_frequency": "monthly",
    },
    {
        "term_type": "PET_RENT",
        "label_patterns": [r"pet\s*rent", r"pet\s*fee"],
        "accepted_ar_codes": ["155034"],
        "expected_frequency": "monthly",
    },
    {
        "term_type": "PARKING",
        "label_patterns": [r"parking", r"garage", r"carport", r"reserved\s*parking"],
        "accepted_ar_codes": ["155052", "155385"],
        "expected_frequency": "monthly",
    },
    {
        "term_type": "UTILITY",
        "label_patterns": [r"utility", r"water", r"sewer", r"electric", r"trash"],
        "accepted_ar_codes": ["155026", "155030", "155023"],
        "expected_frequency": "monthly",
    },
    {
        "term_type": "APPLICATION_FEE",
        "label_patterns": [r"application\s*fee"],
        "accepted_ar_codes": ["154788"],
        "expected_frequency": "one_time",
    },
    {
        "term_type": "ADMIN_FEE",
        "label_patterns": [r"admin\s*fee", r"administrative\s*fee"],
        "accepted_ar_codes": ["155012"],
        "expected_frequency": "one_time",
    },
    {
        "term_type": "AMENITY_PREMIUM",
        "label_patterns": [r"amenity\s*premium", r"premium\s*feature", r"premium\s*amount"],
        "accepted_ar_codes": ["155007"],
        "expected_frequency": "monthly",
    },
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _lease_term_rules_config_path() -> Path:
    configured = os.getenv("LEASE_TERM_RULES_CONFIG_PATH")
    if configured:
        return Path(configured)
    return _repo_root() / "lease_term_mapping_config.json"


@lru_cache(maxsize=1)
def _load_lease_term_rules_from_config() -> list[dict[str, Any]]:
    config_path = _lease_term_rules_config_path()
    if not config_path.exists():
        return []

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return []

    if not isinstance(payload, dict):
        return []

    rules = payload.get("term_to_ar_code_rules")
    if not isinstance(rules, list):
        return []

    normalized_rules: list[dict[str, Any]] = []
    for item in rules:
        if not isinstance(item, dict):
            continue
        if item.get("disabled"):
            continue
        term_type = str(item.get("term_type") or "").strip().upper()
        if not term_type:
            continue
        normalized_rules.append({
            "term_type": term_type,
            "label_patterns": list(item.get("label_patterns") or []),
            "accepted_ar_codes": [str(code).strip() for code in (item.get("accepted_ar_codes") or []) if str(code).strip()],
            "expected_frequency": item.get("expected_frequency"),
        })

    return normalized_rules


def get_term_to_ar_code_rules(custom_rules: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Return lease-term mapping rules from JSON config with fallback to in-code defaults."""
    base_rules = _load_lease_term_rules_from_config() or list(DEFAULT_TERM_TO_AR_CODE_RULES)
    if custom_rules:
        base_rules = list(base_rules) + [dict(rule) for rule in custom_rules]
    return base_rules


def get_primary_ar_code_for_term(term_type: str, fallback: str = "") -> str:
    """Resolve first configured AR code for term type, with fallback when missing."""
    normalized_type = str(term_type or "").strip().upper()
    if not normalized_type:
        return fallback

    for rule in get_term_to_ar_code_rules():
        if str(rule.get("term_type") or "").strip().upper() != normalized_type:
            continue
        accepted = [str(code).strip() for code in (rule.get("accepted_ar_codes") or []) if str(code).strip()]
        if accepted:
            return accepted[0]
        break

    return fallback


@lru_cache(maxsize=1)
def _load_ar_code_name_map() -> dict[str, str]:
    map_path = _repo_root() / "ar_code_name_usage_map.json"
    if not map_path.exists():
        return {}

    try:
        payload = json.loads(map_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}

    mapping = payload.get("mapping") if isinstance(payload, dict) else None
    if not isinstance(mapping, dict):
        return {}

    result: dict[str, str] = {}
    for code, item in mapping.items():
        code_key = str(code).strip()
        if not code_key:
            continue
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            result[code_key] = name
    return result


def get_ar_code_display_name(ar_code: Any) -> str | None:
    """Return human-readable AR code name for a code token, if available."""
    if ar_code is None:
        return None
    token = str(ar_code).strip()
    if not token:
        return None
    return _load_ar_code_name_map().get(token)


def format_ar_code_display(ar_code: Any) -> str:
    """Format AR code as '<name> (<code>)' when name is known."""
    token = str(ar_code or "").strip()
    if not token:
        return ""
    name = get_ar_code_display_name(token)
    return f"{name} ({token})" if name else token
