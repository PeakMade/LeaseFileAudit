from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
	return Path(__file__).resolve().parent.parent


def _lease_term_extraction_config_path() -> Path:
	configured = os.getenv("LEASE_TERM_EXTRACTION_CONFIG_PATH")
	if configured:
		return Path(configured)
	return _repo_root() / "lease_term_extraction_config.json"


def _normalize_pattern_list(value: Any) -> list[str]:
	if not isinstance(value, list):
		return []
	normalized: list[str] = []
	for item in value:
		token = str(item or "").strip()
		if token:
			normalized.append(token)
	return normalized


def _normalize_source_order(value: Any) -> list[str]:
	if not isinstance(value, list):
		return []
	allowed = {"focus", "addenda", "all_text"}
	ordered: list[str] = []
	for item in value:
		token = str(item or "").strip().lower()
		if token in allowed and token not in ordered:
			ordered.append(token)
	return ordered


def _normalize_optional_string(value: Any) -> str:
	token = str(value or "").strip()
	return token


@lru_cache(maxsize=1)
def _load_term_extraction_config_payload() -> dict[str, Any]:
	config_path = _lease_term_extraction_config_path()
	if not config_path.exists():
		return {}

	try:
		payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
	except Exception:
		return {}

	if not isinstance(payload, dict):
		return {}

	return payload


@lru_cache(maxsize=1)
def _load_term_extraction_rules_from_config() -> dict[str, dict[str, Any]]:
	payload = _load_term_extraction_config_payload()
	if not payload:
		return {}

	raw_rules = payload.get("term_extraction_rules")
	if not isinstance(raw_rules, dict):
		return {}

	normalized_rules: dict[str, dict[str, Any]] = {}
	for term_type, term_rule in raw_rules.items():
		term_key = str(term_type or "").strip().upper()
		if not term_key or not isinstance(term_rule, dict):
			continue
		if term_rule.get("disabled"):
			continue

		normalized_rule: dict[str, Any] = {
			"include_patterns": _normalize_pattern_list(term_rule.get("include_patterns")),
			"exclude_patterns": _normalize_pattern_list(term_rule.get("exclude_patterns")),
			"focus_patterns": _normalize_pattern_list(term_rule.get("focus_patterns")),
			"source_order": _normalize_source_order(term_rule.get("source_order")),
		}

		# BASE_RENT and future term-specific extraction knobs.
		for list_key in [
			"heading_patterns",
			"fallback_heading_patterns",
			"regex_fallback_monthly_patterns",
			"excluded_context_tokens",
			"strict_patterns",
			"explicit_clause_markers",
			"prioritization_excluded_tokens",
			"clause_excluded_tokens",
			"page_hint_patterns",
			"anchor_phrase_tokens",
			"anchor_keywords",
			"anchor_patterns",
			"preferred_tokens",
			"hard_excluded_tokens",
			"exclusion_signals",
			"positive_context_signals",
			"monthly_signals",
			"monthly_bonus_signals",
			"one_time_signals",
			"regex_exclusion_signals",
			"regex_score_bonus_signals",
			"total_context_tokens",
			"installment_context_tokens",
			"monthly_context_tokens",
			"total_rent_guard_tokens",
			"application_leak_required_tokens",
		]:
			if list_key in term_rule:
				normalized_rule[list_key] = _normalize_pattern_list(term_rule.get(list_key))

		for string_key in [
			"monthly_signal_pattern",
			"total_signal_pattern",
			"regex_fallback_installment_pattern",
			"clause_anchor_pattern",
			"clause_amount_pattern",
			"page_fallback_pattern",
			"application_leak_amount_pattern",
		]:
			if string_key in term_rule:
				normalized_rule[string_key] = _normalize_optional_string(term_rule.get(string_key))

		normalized_rules[term_key] = normalized_rule

	return normalized_rules


def get_term_extraction_rule(term_type: str, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
	"""Return extraction rule for a term type from JSON config, with optional caller fallback."""
	term_key = str(term_type or "").strip().upper()
	base_rule = dict((fallback or {}).items())

	configured_rule = _load_term_extraction_rules_from_config().get(term_key) or {}
	if not configured_rule:
		return base_rule

	merged = dict(base_rule)
	for key in [
		"include_patterns",
		"exclude_patterns",
		"focus_patterns",
		"source_order",
		"heading_patterns",
		"fallback_heading_patterns",
		"regex_fallback_monthly_patterns",
		"excluded_context_tokens",
		"strict_patterns",
		"explicit_clause_markers",
		"prioritization_excluded_tokens",
		"clause_excluded_tokens",
		"page_hint_patterns",
		"anchor_phrase_tokens",
		"anchor_keywords",
		"anchor_patterns",
		"preferred_tokens",
		"hard_excluded_tokens",
		"exclusion_signals",
		"positive_context_signals",
		"monthly_signals",
		"monthly_bonus_signals",
		"one_time_signals",
		"regex_exclusion_signals",
		"regex_score_bonus_signals",
		"total_context_tokens",
		"installment_context_tokens",
		"monthly_context_tokens",
		"total_rent_guard_tokens",
		"application_leak_required_tokens",
	]:
		configured_value = configured_rule.get(key)
		if isinstance(configured_value, list) and configured_value:
			merged[key] = list(configured_value)

	for key in [
		"monthly_signal_pattern",
		"total_signal_pattern",
		"regex_fallback_installment_pattern",
		"clause_anchor_pattern",
		"clause_amount_pattern",
		"page_fallback_pattern",
		"application_leak_amount_pattern",
	]:
		configured_value = str(configured_rule.get(key) or "").strip()
		if configured_value:
			merged[key] = configured_value

	return merged


def get_term_extraction_test_status(default: str = "") -> str:
	"""Return optional test status from extraction config payload."""
	payload = _load_term_extraction_config_payload()
	token = str(payload.get("test_status") or "").strip()
	return token or default
