from __future__ import annotations

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
