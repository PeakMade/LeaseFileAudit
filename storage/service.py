"""
Storage service for audit run persistence.
Supports both local filesystem and SharePoint Document Library.
"""
import json
import hashlib
import io
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import math
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from time import perf_counter, sleep
from urllib.parse import unquote
from activity_logging.sharepoint import _get_app_only_token
from audit_engine.canonical_fields import CanonicalField

logger = logging.getLogger(__name__)


class StorageService:
    """
    Manage audit run persistence with SharePoint/local fallback.
    
    Uses SharePoint Document Library in production, local filesystem in development.
    
    Structure:
    instance/runs/<run_id>/  OR  SharePoint://<library>/<run_id>/
        inputs_normalized/
            expected_detail.csv
            actual_detail.csv
        outputs/
            bucket_results.csv
            findings.csv
            variance_detail.csv (optional)
        run_meta.json
    """
    _GLOBAL_SITE_ID_CACHE: Dict[str, str] = {}
    _GLOBAL_DRIVE_ID_CACHE: Dict[str, str] = {}
    _GLOBAL_LIST_ID_CACHE: Dict[str, str] = {}
    _GLOBAL_SNAPSHOT_COLUMNS_CACHE: Dict[str, Dict[str, Any]] = {}
    _GLOBAL_SNAPSHOT_COLUMNS_CACHE_LOCK = threading.Lock()
    
    # In-memory cache for audit results (bucket_results and findings)
    _IN_MEMORY_RESULTS_CACHE: Dict[str, Dict[str, pd.DataFrame]] = {}
    _IN_MEMORY_CACHE_LOCK = threading.Lock()
    
    def __init__(self, base_dir: Path, use_sharepoint: bool = False, sharepoint_site_url: str = None,
                 library_name: str = None, access_token: str = None, audit_results_list_name: str = None):
        self.base_dir = Path(base_dir)
        self.use_sharepoint = bool(use_sharepoint and sharepoint_site_url and library_name)
        self.sharepoint_site_url = sharepoint_site_url.rstrip('/') if sharepoint_site_url else None
        self.library_name = library_name
        self.access_token = access_token
        requested_results_list_name = (
            audit_results_list_name
            or os.getenv('SHAREPOINT_AUDIT_RESULTS_LIST_NAME', 'AuditRuns2')
        ).strip()
        if requested_results_list_name.lower() != 'auditruns2':
            logger.warning(
                f"[STORAGE] Ignoring legacy audit results list '{requested_results_list_name}'; "
                "forcing AuditRuns2"
            )
        self.audit_results_list_name = 'AuditRuns2'
        self._site_id = None
        self._drive_id = None
        self._list_ids = {}

        site_cache_key = self.sharepoint_site_url or ""
        drive_cache_key = f"{site_cache_key}|{self.library_name}" if site_cache_key and self.library_name else ""
        if site_cache_key and site_cache_key in self._GLOBAL_SITE_ID_CACHE:
            self._site_id = self._GLOBAL_SITE_ID_CACHE[site_cache_key]
        if drive_cache_key and drive_cache_key in self._GLOBAL_DRIVE_ID_CACHE:
            self._drive_id = self._GLOBAL_DRIVE_ID_CACHE[drive_cache_key]
        
        if self.use_sharepoint:
            logger.debug(f"[STORAGE] Using SharePoint Document Library: {library_name}")
        else:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[STORAGE] Using local filesystem: {self.base_dir}")

    def _snapshot_columns_cache_ttl_seconds(self) -> int:
        """TTL for snapshot column-name cache (worker-memory), default 10 minutes."""
        try:
            return max(30, int(os.getenv('SNAPSHOT_COLUMNS_CACHE_TTL_SECONDS', '600')))
        except Exception:
            return 600

    def _can_use_sharepoint_lists(self) -> bool:
        if self.access_token and self.sharepoint_site_url:
            return True

        if not self.sharepoint_site_url:
            logger.error("[STORAGE] SharePoint site URL not configured; cannot use lists")
            return False

        if not self.access_token:
            logger.info("[STORAGE] No access token; attempting app-only token for SharePoint lists")
            self.access_token = _get_app_only_token()
            if not self.access_token:
                logger.error("[STORAGE] Failed to acquire app-only token for SharePoint lists")
            return bool(self.access_token)

        return False
    
    def _get_site_and_drive_id(self) -> tuple:
        """Get SharePoint site ID and drive ID for document library."""
        if self._site_id and self._drive_id:
            return self._site_id, self._drive_id
        
        try:
            site_id = self._get_site_id()
            if not site_id:
                return None, None
            headers = {'Authorization': f'Bearer {self.access_token}'}
            
            # Get drive ID for document library
            drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
            response = requests.get(drives_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to get drives: {response.status_code}")
                return None, None
            
            # Find the drive matching our library name
            for drive in response.json()['value']:
                if drive['name'] == self.library_name:
                    self._drive_id = drive['id']
                    site_cache_key = self.sharepoint_site_url or ""
                    drive_cache_key = f"{site_cache_key}|{self.library_name}" if site_cache_key and self.library_name else ""
                    if drive_cache_key:
                        self._GLOBAL_DRIVE_ID_CACHE[drive_cache_key] = self._drive_id
                    logger.debug(f"[STORAGE] Found drive ID for library '{self.library_name}'")
                    return site_id, self._drive_id
            
            logger.error(f"[STORAGE] Document library '{self.library_name}' not found")
            return None, None
            
        except Exception as e:
            logger.error(f"[STORAGE] Error getting site/drive ID: {e}", exc_info=True)
            return None, None

    def _get_site_id(self) -> Optional[str]:
        """Get SharePoint site ID without resolving document library."""
        if self._site_id:
            return self._site_id

        site_cache_key = self.sharepoint_site_url or ""
        if site_cache_key and site_cache_key in self._GLOBAL_SITE_ID_CACHE:
            self._site_id = self._GLOBAL_SITE_ID_CACHE[site_cache_key]
            return self._site_id

        try:
            parts = self.sharepoint_site_url.replace('https://', '').split('/')
            hostname = parts[0]
            site_path = '/'.join(parts[1:])
            site_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(site_url, headers=headers, timeout=10)

            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to get site ID: {response.status_code} - {response.text}")
                return None

            self._site_id = response.json()['id']
            if site_cache_key:
                self._GLOBAL_SITE_ID_CACHE[site_cache_key] = self._site_id
            return self._site_id
        except Exception as e:
            logger.error(f"[STORAGE] Error getting site ID: {e}", exc_info=True)
            return None

    def _get_sharepoint_list_id(self, list_name: str) -> Optional[str]:
        logger.info(f"[STORAGE] Attempting to resolve list ID for: {list_name}")
        if list_name in self._list_ids:
            logger.info(f"[STORAGE] Found {list_name} in cache: {self._list_ids[list_name]}")
            return self._list_ids[list_name]

        site_id = self._get_site_id()
        if not site_id:
            logger.error("[STORAGE] Cannot resolve list ID - site ID not found")
            return None

        global_list_key = f"{site_id}|{list_name}"
        if global_list_key in self._GLOBAL_LIST_ID_CACHE:
            list_id = self._GLOBAL_LIST_ID_CACHE[global_list_key]
            self._list_ids[list_name] = list_id
            logger.info(f"[STORAGE] Found {list_name} in global cache: {list_id}")
            return list_id

        list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        params = {'$filter': f"displayName eq '{list_name}'"}
        logger.info(f"[STORAGE] Querying Graph API for list: {list_name}")
        response = requests.get(list_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"[STORAGE] Failed to find list '{list_name}': {response.status_code} - {response.text}")
            return None

        lists_data = response.json()
        if not lists_data.get('value'):
            logger.error(f"[STORAGE] List '{list_name}' not found in SharePoint (empty response)")
            return None

        list_id = lists_data['value'][0]['id']
        self._list_ids[list_name] = list_id
        self._GLOBAL_LIST_ID_CACHE[global_list_key] = list_id
        logger.info(f"[STORAGE] ✅ Resolved SharePoint list '{list_name}' id: {list_id}")
        return list_id

    def _extract_list_id_from_config_value(self, configured_value: str) -> Optional[str]:
        """Extract list ID GUID from env value (GUID, {GUID}, or SharePoint sharing URL)."""
        if not configured_value:
            return None

        decoded = unquote(str(configured_value).strip())
        patterns = [
            r"List=\{?([0-9a-fA-F\-]{36})\}?",
            r"\{([0-9a-fA-F\-]{36})\}",
            r"\b([0-9a-fA-F\-]{36})\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, decoded)
            if match:
                return match.group(1)

        return None

    def _get_configured_sharepoint_list_id(self, env_keys: List[str]) -> Optional[str]:
        """Resolve list id from one of the configured env vars."""
        for env_key in env_keys:
            configured_value = os.getenv(env_key)
            list_id = self._extract_list_id_from_config_value(configured_value) if configured_value else None
            if list_id:
                logger.info(f"[STORAGE] Using configured list id from {env_key}: {list_id}")
                return list_id
        return None

    def _normalize_for_json(self, value: Any) -> Any:
        """Normalize pandas/numpy values into JSON-serializable primitives."""
        if value is None:
            return None

        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d')

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value

        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        if isinstance(value, (int, str, bool, dict, list)):
            return value

        return str(value)

    def _normalize_audit_month_value(self, value: Any) -> str:
        """Normalize audit month to YYYY-MM-DD string for list filters and keys."""
        normalized = self._normalize_for_json(value)
        if normalized is None:
            return ''
        if isinstance(normalized, str):
            return normalized[:10]
        return str(normalized)[:10]

    # def _build_result_composite_key(self, run_id: str, result_type: str, row: Dict[str, Any], row_index: int) -> str:
    #     """Build deterministic composite key for result rows persisted to SharePoint list."""
    #     property_id = self._normalize_for_json(row.get('PROPERTY_ID', row.get('property_id')))
    #     lease_interval_id = self._normalize_for_json(row.get('LEASE_INTERVAL_ID', row.get('lease_interval_id')))
    #     ar_code_id = self._normalize_for_json(row.get('AR_CODE_ID', row.get('ar_code_id')))
    #     audit_month = self._normalize_audit_month_value(row.get('AUDIT_MONTH', row.get('audit_month')))

    #     if property_id is not None and lease_interval_id is not None and ar_code_id is not None and audit_month:
    #         return f"{run_id}:{result_type}:{property_id}:{lease_interval_id}:{ar_code_id}:{audit_month}"

    #     return f"{run_id}:{result_type}:row:{row_index}"

    def _get_audit_results_list_id(self) -> Optional[str]:
        """Resolve audit results list id via env override first, then AuditRuns2 name fallback."""
        configured = self._get_configured_sharepoint_list_id([
            'SHAREPOINT_AUDIT_RESULTS_LIST_ID',
            'SHAREPOINT_AUDIT_RESULTS_LIST_URL',
        ])
        if configured:
            return configured

        for configured_name in ['AuditRuns2', 'AuditRuns 2']:
            list_id = self._get_sharepoint_list_id(configured_name)
            if list_id:
                return list_id

        logger.warning(
            "[STORAGE] AuditRuns2 list not found on SharePoint"
        )
        return None

    def _get_run_display_snapshots_list_id(self) -> Optional[str]:
        """Resolve run display snapshots list id with preferred name first and legacy fallback."""
        preferred_names = ["RunDisplaySnapshots", "Run Display Snapshots"]
        for name in preferred_names:
            list_id = self._get_sharepoint_list_id(name)
            if list_id:
                if name != "RunDisplaySnapshots":
                    logger.warning(
                        f"[STORAGE] Using legacy list name '{name}'. Consider renaming to 'RunDisplaySnapshots'."
                    )
                return list_id
        return None

    def _get_lease_term_set_list_id(self) -> Optional[str]:
        """Resolve lease term set list id via env override first, then by display name."""
        configured = self._get_configured_sharepoint_list_id([
            'LEASE_TERM_SET_LIST_ID',
            'LEASE_TERM_SET_LIST_URL',
        ])
        if configured:
            return configured

        for name in ["LeaseTermSet", "Lease Term Set"]:
            list_id = self._get_sharepoint_list_id(name)
            if list_id:
                return list_id
        return None

    def _get_lease_terms_list_id(self) -> Optional[str]:
        """Resolve lease terms list id via env override first, then by display name."""
        configured = self._get_configured_sharepoint_list_id([
            'LEASE_TERMS_LIST_ID',
            'LEASE_TERMS_LIST_URL',
        ])
        if configured:
            return configured

        for name in ["LeaseTerms", "Lease Terms"]:
            list_id = self._get_sharepoint_list_id(name)
            if list_id:
                return list_id
        return None

    def _get_lease_term_evidence_list_id(self) -> Optional[str]:
        """Resolve lease term evidence list id via env override first, then by display name."""
        configured = self._get_configured_sharepoint_list_id([
            'LEASE_TERM_EVIDENCE_LIST_ID',
            'LEASE_TERM_EVIDENCE_LIST_URL',
        ])
        if configured:
            return configured

        for name in ["LeaseTermEvidence", "Lease Term Evidence"]:
            list_id = self._get_sharepoint_list_id(name)
            if list_id:
                return list_id
        return None

    def _normalize_status_value(self, value: Any) -> str:
        """Normalize status values for consistent matched/exception comparisons."""
        if value is None:
            return ""
        text = str(value).strip().lower()
        if text in {"matched", "match", "status_matched"}:
            return "matched"
        return text

    def _safe_int(self, value: Any) -> Optional[int]:
        """Convert numeric-like value to int, returning None if unavailable."""
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        try:
            return int(float(value))
        except Exception:
            return None
    
    def _safe_field_value(self, value: Any) -> Any:
        """
        Safely handle SharePoint field values that might be 'NA' strings.
        Converts 'NA', 'N/A', '#N/A' strings to None to avoid pandas ambiguity errors.
        """
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip().upper()
            if normalized in ('NA', 'N/A', '#N/A', ''):
                return None
        return value

    def _normalize_snapshot_key_value(self, value: Any, cast_type=str):
        """Normalize key values for snapshot resolved-month matching."""
        if value is None:
            return ""
        try:
            if cast_type is int:
                return int(float(value))
            if cast_type is str:
                if isinstance(value, (int, float)):
                    numeric = float(value)
                    if numeric.is_integer():
                        return str(int(numeric))
                    return str(numeric)
                text = str(value).strip()
                try:
                    numeric = float(text)
                    if numeric.is_integer():
                        return str(int(numeric))
                except Exception:
                    pass
                return text
        except Exception:
            return ""
        return value

    def _normalize_snapshot_audit_month(self, value: Any):
        """Normalize audit month to YYYY-MM for snapshot filtering and cross-run matching."""
        if value is None:
            return ''

        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m')

        try:
            parsed = pd.to_datetime(value, errors='coerce')
            if not pd.isna(parsed):
                return parsed.strftime('%Y-%m')
        except Exception:
            pass

        text = str(value).strip()
        if len(text) >= 7:
            return text[:7]
        return text

    def _build_snapshot_resolved_key(self, property_id, lease_id, ar_code_id, audit_month):
        """Create normalized tuple key for resolved-month lookup during snapshot generation."""
        return (
            self._normalize_snapshot_key_value(property_id, int),
            self._normalize_snapshot_key_value(lease_id, int),
            self._normalize_snapshot_key_value(ar_code_id, str),
            self._normalize_snapshot_audit_month(audit_month)
        )

    def _filter_bucket_results_for_unresolved_snapshot(self, run_id: str, bucket_results: pd.DataFrame) -> pd.DataFrame:
        """Filter out resolved exception months before snapshot metric calculation."""
        if bucket_results is None or len(bucket_results) == 0:
            return bucket_results

        status_column = 'status' if 'status' in bucket_results.columns else 'STATUS'
        property_column = 'PROPERTY_ID' if 'PROPERTY_ID' in bucket_results.columns else 'property_id'
        lease_column = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in bucket_results.columns else 'lease_interval_id'
        ar_code_column = 'AR_CODE_ID' if 'AR_CODE_ID' in bucket_results.columns else 'ar_code_id'
        audit_month_column = 'AUDIT_MONTH' if 'AUDIT_MONTH' in bucket_results.columns else 'audit_month'

        required_columns = [status_column, property_column, lease_column, ar_code_column, audit_month_column]
        if any(col not in bucket_results.columns for col in required_columns):
            logger.warning("[STORAGE] Snapshot unresolved filtering skipped: required columns missing")
            return bucket_results

        status_series = bucket_results[status_column].map(self._normalize_status_value)
        exception_rows = bucket_results[status_series != 'matched'].copy()
        if len(exception_rows) == 0:
            return bucket_results

        resolved_keys = set()
        unique_properties = pd.to_numeric(exception_rows[property_column], errors='coerce').dropna().unique()

        for property_id in unique_properties:
            try:
                bulk_exception_data = self.load_property_exception_months_bulk(run_id, int(float(property_id)))
                for (lease_id, ar_code_id), month_records in bulk_exception_data.items():
                    for month_record in month_records:
                        if str(month_record.get('status', '')).strip().lower() == 'resolved':
                            resolved_key = self._build_snapshot_resolved_key(
                                property_id,
                                lease_id,
                                ar_code_id,
                                month_record.get('audit_month')
                            )
                            resolved_keys.add(resolved_key)
            except Exception as e:
                logger.warning(
                    f"[STORAGE] Snapshot unresolved filtering property lookup failed for {property_id}: {e}"
                )

        if len(resolved_keys) == 0:
            return bucket_results

        def _is_unresolved_bucket(row):
            if self._normalize_status_value(row[status_column]) == 'matched':
                return True

            key = self._build_snapshot_resolved_key(
                row[property_column],
                row[lease_column],
                row[ar_code_column],
                row[audit_month_column]
            )
            return key not in resolved_keys

        filtered = bucket_results[bucket_results.apply(_is_unresolved_bucket, axis=1)].copy()
        logger.info(
            f"[STORAGE] Snapshot unresolved filtering for {run_id}: "
            f"original_rows={len(bucket_results)}, filtered_rows={len(filtered)}, resolved_keys={len(resolved_keys)}"
        )
        return filtered

    def _calculate_static_metrics(self, dataframe: pd.DataFrame) -> Dict[str, Any]:
        """Calculate static display metrics from bucket rows without resolution overlay."""
        if dataframe is None or len(dataframe) == 0:
            return {
                'exception_count': 0,
                'undercharge': 0.0,
                'overcharge': 0.0,
                'total_buckets': 0,
                'matched_buckets': 0,
                'match_rate': 0.0,
            }

        status_column = 'status' if 'status' in dataframe.columns else 'STATUS'
        expected_column = 'expected_total' if 'expected_total' in dataframe.columns else 'EXPECTED_TOTAL'
        actual_column = 'actual_total' if 'actual_total' in dataframe.columns else 'ACTUAL_TOTAL'

        status_series = dataframe[status_column].map(self._normalize_status_value) if status_column in dataframe.columns else pd.Series([], dtype=str)
        # Exclude MATCHED and SCHEDULED_ONLY — same logic as calculate_kpis in metrics.py
        non_exception_statuses = {'matched', 'scheduled_only'}
        non_exception_mask = status_series.isin(non_exception_statuses)
        matched_mask = status_series == 'matched'

        total_buckets = int(len(dataframe))
        matched_buckets = int(matched_mask.sum())
        exception_rows = dataframe[~non_exception_mask].copy() if status_column in dataframe.columns else dataframe.copy()

        if expected_column in exception_rows.columns and actual_column in exception_rows.columns and len(exception_rows) > 0:
            expected_values = pd.to_numeric(exception_rows[expected_column], errors='coerce').fillna(0)
            actual_values = pd.to_numeric(exception_rows[actual_column], errors='coerce').fillna(0)
            undercharge = float((expected_values - actual_values).clip(lower=0).sum())
            overcharge = float((actual_values - expected_values).clip(lower=0).sum())
        else:
            undercharge = 0.0
            overcharge = 0.0

        match_rate = float((matched_buckets / total_buckets) * 100) if total_buckets > 0 else 0.0

        return {
            'exception_count': int(len(exception_rows)),
            'undercharge': undercharge,
            'overcharge': overcharge,
            'total_buckets': total_buckets,
            'matched_buckets': matched_buckets,
            'match_rate': match_rate,
        }

    def _get_snapshot_column_names(self, site_id: str, list_id: str) -> Optional[set]:
        """Resolve RunDisplaySnapshots column internal names with per-worker in-memory cache."""
        cache_key = f"{site_id}|{list_id}"
        now_ts = datetime.utcnow().timestamp()
        ttl_seconds = self._snapshot_columns_cache_ttl_seconds()

        with self._GLOBAL_SNAPSHOT_COLUMNS_CACHE_LOCK:
            cached_entry = self._GLOBAL_SNAPSHOT_COLUMNS_CACHE.get(cache_key)
            if cached_entry and float(cached_entry.get('expires_at', 0)) > now_ts:
                return set(cached_entry.get('column_names', []))

        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
            }
            columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
            params = {
                '$select': 'name',
                '$top': 200
            }
            response = requests.get(columns_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Could not read RunDisplaySnapshots columns: "
                    f"{response.status_code} - {response.text}"
                )
                return None

            column_names = {column.get('name') for column in response.json().get('value', []) if column.get('name')}

            with self._GLOBAL_SNAPSHOT_COLUMNS_CACHE_LOCK:
                self._GLOBAL_SNAPSHOT_COLUMNS_CACHE[cache_key] = {
                    'column_names': sorted(column_names),
                    'expires_at': now_ts + ttl_seconds,
                }

            return column_names
        except Exception as e:
            logger.warning(f"[STORAGE] Failed loading RunDisplaySnapshots columns: {e}")
            return None

    def _resolve_snapshot_exception_count_field_name(self, site_id: str, list_id: str) -> str:
        """Resolve internal SharePoint field name for snapshot exception count."""
        default_name = 'ExceptionCountStatic'
        legacy_name = 'ExceptionCountStatistic'

        try:
            column_names = self._get_snapshot_column_names(site_id, list_id)
            if not column_names:
                return default_name

            if default_name in column_names:
                return default_name
            if legacy_name in column_names:
                logger.info(f"[STORAGE] Using legacy RunDisplaySnapshots exception field: {legacy_name}")
                return legacy_name

            logger.warning(
                f"[STORAGE] Neither {default_name} nor {legacy_name} exists on RunDisplaySnapshots; "
                f"defaulting to {default_name}"
            )
            return default_name
        except Exception as e:
            logger.warning(
                f"[STORAGE] Failed resolving snapshot exception count field; defaulting to {default_name}: {e}"
            )
            return default_name

    def _ensure_list_column_exists(self, site_id: str, list_id: str, column_name: str, column_type: str = 'text') -> bool:
        """Create a column on a SharePoint list via Graph API if it does not already exist.
        Returns True if the column already existed or was successfully created."""
        try:
            column_names = self._get_snapshot_column_names(site_id, list_id)
            if column_names and column_name in column_names:
                return True  # already exists

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
            }
            columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"

            if column_type == 'text':
                body = {'name': column_name, 'text': {}}
            elif column_type == 'number':
                body = {'name': column_name, 'number': {}}
            elif column_type == 'dateTime':
                body = {'name': column_name, 'dateTime': {'format': 'dateOnly'}}
            else:
                body = {'name': column_name, 'text': {}}

            response = requests.post(columns_url, headers=headers, json=body, timeout=30)
            if response.status_code in (200, 201):
                logger.info(f"[STORAGE] Created SharePoint column '{column_name}' on list {list_id}")
                # Bust the column name cache so subsequent writes see the new column
                cache_key = f"{site_id}|{list_id}"
                with self._GLOBAL_SNAPSHOT_COLUMNS_CACHE_LOCK:
                    self._GLOBAL_SNAPSHOT_COLUMNS_CACHE.pop(cache_key, None)
                return True
            else:
                logger.warning(
                    f"[STORAGE] Could not create column '{column_name}': "
                    f"{response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.warning(f"[STORAGE] Failed ensuring column '{column_name}' exists: {e}")
            return False

    def _resolve_snapshot_optional_field_names(self, site_id: str, list_id: str) -> Dict[str, Optional[str]]:
        """Resolve optional snapshot fields used by snapshot-only portfolio rendering."""
        field_candidates = {
            'property_name': ['PropertyNameStatic', 'PropertyName'],
            'total_variance': ['TotalVarianceStatic'],
            'total_lease_intervals': ['TotalLeaseIntervalStatic'],
            'run_scope_type': ['RunScopeType'],
            'audited_through': ['AuditedThrough'],
            'status': ['Status'],
            'ar_code_name': ['ArCodeName'],
            'expected_total': ['ExpectedTotal'],
            'actual_total': ['ActualTotal'],
            'variance': ['Variance'],
            'resident_name': ['ResidentName'],
            'lease_id': ['LeaseId'],
        }
        resolved = {
            'property_name': None,
            'total_variance': None,
            'total_lease_intervals': None,
            'run_scope_type': None,
            'audited_through': None,
            'status': None,
            'ar_code_name': None,
            'expected_total': None,
            'actual_total': None,
            'variance': None,
            'resident_name': None,
            'lease_id': None,
        }

        try:
            column_names = self._get_snapshot_column_names(site_id, list_id)
            if not column_names:
                return resolved

            for logical_name, candidates in field_candidates.items():
                for candidate_name in candidates:
                    if candidate_name in column_names:
                        resolved[logical_name] = candidate_name
                        break

            return resolved
        except Exception as e:
            logger.warning(f"[STORAGE] Failed resolving optional snapshot field names: {e}")
            return resolved

    def _build_run_display_snapshot_rows(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        exception_count_field_name: str = 'ExceptionCountStatic',
        optional_field_names: Optional[Dict[str, Optional[str]]] = None,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
        property_name_map: Optional[Dict[int, str]] = None,
        full_bucket_results: Optional[pd.DataFrame] = None,
        run_scope_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Build static snapshot rows for portfolio/property/lease display scopes."""
        rows: List[Dict[str, Any]] = []
        if bucket_results is None or len(bucket_results) == 0:
            return rows

        optional_field_names = optional_field_names or {}
        property_name_field = optional_field_names.get('property_name')
        total_variance_field = optional_field_names.get('total_variance')
        total_lease_intervals_field = optional_field_names.get('total_lease_intervals')

        property_column = 'PROPERTY_ID' if 'PROPERTY_ID' in bucket_results.columns else 'property_id'
        lease_column = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in bucket_results.columns else 'lease_interval_id'

        # Precompute per-property max AUDIT_MONTH from the FULL (unfiltered) bucket results
        # so that resolved months are not excluded from the "audited through" date.
        _source_for_months = full_bucket_results if (full_bucket_results is not None and not full_bucket_results.empty) else bucket_results
        _audit_month_col = 'AUDIT_MONTH'
        _property_max_audit_month: Dict[int, str] = {}
        _portfolio_max_audit_month: Optional[str] = None
        if _audit_month_col in _source_for_months.columns and not _source_for_months.empty:
            try:
                _months_series = pd.to_datetime(_source_for_months[_audit_month_col], errors='coerce')
                _overall_max = _months_series.max()
                if pd.notna(_overall_max):
                    _portfolio_max_audit_month = _overall_max.strftime('%Y-%m-%d')
                _prop_col = 'PROPERTY_ID' if 'PROPERTY_ID' in _source_for_months.columns else 'property_id'
                if _prop_col in _source_for_months.columns:
                    _grouped = _source_for_months.copy()
                    _grouped['_month_dt'] = _months_series
                    for _pid, _grp in _grouped.groupby(_prop_col, dropna=False):
                        _pid_int = self._safe_int(_pid)
                        if _pid_int is None:
                            continue
                        _max = _grp['_month_dt'].max()
                        if pd.notna(_max):
                            _property_max_audit_month[_pid_int] = _max.strftime('%Y-%m-%d')
            except Exception:
                pass

        resolved_property_name_map: Dict[int, str] = {}

        if property_name_map:
            for raw_property_id, raw_property_name in property_name_map.items():
                property_id_int = self._safe_int(raw_property_id)
                property_name_value = str(raw_property_name).strip() if raw_property_name is not None else ''
                if property_id_int is None or not property_name_value or property_name_value.lower() == 'nan':
                    continue
                if property_id_int not in resolved_property_name_map:
                    resolved_property_name_map[property_id_int] = property_name_value

        def _populate_property_names(detail_df: Optional[pd.DataFrame]) -> None:
            if detail_df is None or len(detail_df) == 0:
                return

            detail_property_column = 'PROPERTY_ID' if 'PROPERTY_ID' in detail_df.columns else 'property_id'
            detail_property_name_column = 'PROPERTY_NAME' if 'PROPERTY_NAME' in detail_df.columns else 'property_name'
            if detail_property_column not in detail_df.columns or detail_property_name_column not in detail_df.columns:
                return

            for _, detail_row in detail_df[[detail_property_column, detail_property_name_column]].dropna().iterrows():
                property_id_int = self._safe_int(detail_row.get(detail_property_column))
                property_name = str(detail_row.get(detail_property_name_column)).strip()
                if property_id_int is None or not property_name or property_name.lower() == 'nan':
                    continue
                if property_id_int not in resolved_property_name_map:
                    resolved_property_name_map[property_id_int] = property_name

        # Name priority: actual detail first, expected detail second.
        _populate_property_names(actual_detail)
        _populate_property_names(expected_detail)

        def _make_row(scope_type: str, subset: pd.DataFrame, property_id: Any = None, lease_interval_id: Any = None, ar_code_id: Any = None, audit_month: Any = None) -> Dict[str, Any]:
            metrics = self._calculate_static_metrics(subset)
            property_id_int = self._safe_int(property_id)
            lease_interval_id_int = self._safe_int(lease_interval_id)
            ar_code_id_int = self._safe_int(ar_code_id)
            audit_month_str = str(audit_month)[:10] if audit_month is not None else None
            property_name = resolved_property_name_map.get(property_id_int, f"Property {property_id_int}") if property_id_int is not None else None

            # Extract resident name for lease-level snapshots
            resident_name = None
            lease_id_value = None
            if scope_type == 'lease' and lease_interval_id_int is not None:
                # DEBUG: Log what columns we have available
                if full_bucket_results is not None and not full_bucket_results.empty:
                    print(f"[SNAPSHOT_DEBUG] full_bucket_results columns: {list(full_bucket_results.columns)[:20]}")
                    print(f"[SNAPSHOT_DEBUG] Looking for lease_interval_id={lease_interval_id_int}")
                
                # Try to get resident name from expected_detail or actual_detail
                for source_df in [expected_detail, actual_detail]:
                    if source_df is not None and not source_df.empty:
                        lid_col = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in source_df.columns else 'lease_interval_id'
                        cust_col = 'CUSTOMER_NAME' if 'CUSTOMER_NAME' in source_df.columns else 'customer_name'
                        lid_col_alt = 'LEASE_ID' if 'LEASE_ID' in source_df.columns else 'lease_id'
                        
                        if lid_col in source_df.columns and cust_col in source_df.columns:
                            lease_rows = source_df[pd.to_numeric(source_df[lid_col], errors='coerce') == lease_interval_id_int]
                            if not lease_rows.empty:
                                names = lease_rows[cust_col].dropna().astype(str).str.strip()
                                names = names[names != '']
                                if not names.empty:
                                    resident_name = names.iloc[0]
                                    break
                        
                        # Also try to get LEASE_ID
                        if lid_col_alt in source_df.columns and lid_col in source_df.columns:
                            lease_rows = source_df[pd.to_numeric(source_df[lid_col], errors='coerce') == lease_interval_id_int]
                            if not lease_rows.empty:
                                lease_ids = pd.to_numeric(lease_rows[lid_col_alt], errors='coerce').dropna()
                                if not lease_ids.empty:
                                    lease_id_value = int(lease_ids.iloc[0])
                                    if resident_name:
                                        break
                
                # Fallback: try full_bucket_results for resident_name and lease_id
                # This is critical when exceptions-only mode is enabled - matched buckets
                # won't be in expected/actual detail, but are still in full_bucket_results
                if (not resident_name or not lease_id_value) and full_bucket_results is not None and not full_bucket_results.empty:
                    lid_col = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in full_bucket_results.columns else 'lease_interval_id'
                    cust_col = 'CUSTOMER_NAME' if 'CUSTOMER_NAME' in full_bucket_results.columns else 'customer_name'
                    lease_id_col = 'LEASE_ID' if 'LEASE_ID' in full_bucket_results.columns else 'lease_id'
                    
                    if lid_col in full_bucket_results.columns:
                        lease_rows = full_bucket_results[pd.to_numeric(full_bucket_results[lid_col], errors='coerce') == lease_interval_id_int]
                        if not lease_rows.empty:
                            # Get resident name
                            if not resident_name and cust_col in full_bucket_results.columns:
                                names = lease_rows[cust_col].dropna().astype(str).str.strip()
                                names = names[names != '']
                                if not names.empty:
                                    resident_name = names.iloc[0]
                            
                            # Get lease ID (parent)
                            if not lease_id_value and lease_id_col in full_bucket_results.columns:
                                lease_ids = pd.to_numeric(lease_rows[lease_id_col], errors='coerce').dropna()
                                if not lease_ids.empty:
                                    lease_id_value = int(lease_ids.iloc[0])

            # DEBUG: Log what we extracted for this lease snapshot
            if scope_type == 'lease' and lease_interval_id_int is not None:
                print(f"[SNAPSHOT_DEBUG] Lease {lease_interval_id_int}: resident_name='{resident_name}', lease_id={lease_id_value}")

            snapshot_key = f"{run_id}:{scope_type}"
            title = f"{scope_type}:{run_id}"
            if property_id_int is not None:
                snapshot_key += f":{property_id_int}"
                title += f":{property_id_int}"
                if property_name:
                    title += f":{property_name}"
            if lease_interval_id_int is not None:
                snapshot_key += f":{lease_interval_id_int}"
                title += f":{lease_interval_id_int}"
            if ar_code_id_int is not None:
                snapshot_key += f":{ar_code_id_int}"
                title += f":{ar_code_id_int}"
            if audit_month_str is not None:
                snapshot_key += f":{audit_month_str}"
                title += f":{audit_month_str}"

            # Calculate total lease intervals from FULL bucket_results (not filtered subset)
            # so properties with all matched buckets still show their lease count correctly
            if scope_type == 'property' and full_bucket_results is not None and not full_bucket_results.empty and property_id_int is not None:
                # Count unique leases for this property from ALL buckets (matched + exceptions)
                if property_column in full_bucket_results.columns and lease_column in full_bucket_results.columns:
                    property_full_buckets = full_bucket_results[full_bucket_results[property_column] == property_id]
                    total_lease_intervals = int(property_full_buckets[lease_column].nunique())
                else:
                    total_lease_intervals = int(subset[lease_column].nunique()) if lease_column in subset.columns else 0
            else:
                # For non-property scopes or when full_bucket_results not available, use subset
                total_lease_intervals = int(subset[lease_column].nunique()) if lease_column in subset.columns else 0

            # Use the precomputed max AUDIT_MONTH from the full (unfiltered) bucket results
            # so resolved months are not excluded from the "audited through" date.
            if scope_type == 'portfolio':
                audited_through_value = _portfolio_max_audit_month
            elif scope_type == 'property' and property_id_int is not None:
                audited_through_value = _property_max_audit_month.get(property_id_int)
            else:
                audited_through_value = None

            run_scope_type_field = optional_field_names.get('run_scope_type')
            audited_through_field = optional_field_names.get('audited_through')

            row_payload = {
                'Title': title,
                'SnapshotKey': snapshot_key,
                'RunId': run_id,
                'ScopeType': scope_type,
                'PropertyId': property_id_int,
                'LeaseIntervalId': lease_interval_id_int,
                exception_count_field_name: metrics['exception_count'],
                'UnderchargeStatic': metrics['undercharge'],
                'OverchargeStatic': metrics['overcharge'],
                'MatchRateStatic': metrics['match_rate'],
                'TotalBucketsStatic': metrics['total_buckets'],
                'MatchedBucketsStatic': metrics['matched_buckets'],
                'CreatedAt': datetime.utcnow().isoformat(),
            }
            # Only include ArCodeId and AuditMonth when they're not None (ar_code/month scopes only)
            if ar_code_id_int is not None:
                row_payload['ArCodeId'] = ar_code_id_int
            if audit_month_str is not None:
                row_payload['AuditMonth'] = audit_month_str
            
            # Include ResidentName and LeaseId for lease-level snapshots (only if fields exist)
            if scope_type == 'lease':
                resident_name_field = optional_field_names.get('resident_name')
                lease_id_field = optional_field_names.get('lease_id')
                # DEBUG: Log field resolution and values
                print(f"[SNAPSHOT_DEBUG] Lease {lease_interval_id_int}: optional_field_names={optional_field_names}")
                print(f"[SNAPSHOT_DEBUG] resident_name_field='{resident_name_field}', lease_id_field='{lease_id_field}'")
                if resident_name_field and resident_name:
                    row_payload[resident_name_field] = resident_name
                    print(f"[SNAPSHOT_DEBUG] Added {resident_name_field}='{resident_name}' to snapshot")
                if lease_id_field and lease_id_value:
                    row_payload[lease_id_field] = lease_id_value
                    print(f"[SNAPSHOT_DEBUG] Added {lease_id_field}={lease_id_value} to snapshot")
            
            if run_scope_type_field:
                row_payload[run_scope_type_field] = run_scope_type or ''
            if audited_through_field:
                row_payload[audited_through_field] = audited_through_value or ''

            if property_name_field and property_name:
                row_payload[property_name_field] = property_name
            if total_variance_field:
                row_payload[total_variance_field] = metrics['undercharge'] + metrics['overcharge']
            if total_lease_intervals_field:
                if scope_type == 'property':
                    row_payload[total_lease_intervals_field] = total_lease_intervals
                elif scope_type == 'lease':
                    row_payload[total_lease_intervals_field] = 1
                else:
                    row_payload[total_lease_intervals_field] = 0

            # For exception scope, add bucket detail fields (AR code, month, expected, actual, variance, status)
            if scope_type == 'exception' and len(subset) > 0:
                # Exception scope should have exactly 1 row (one lease × AR code × month)
                first_row = subset.iloc[0]
                
                # Add status
                status_field = optional_field_names.get('status')
                if status_field:
                    if 'STATUS' in subset.columns:
                        row_payload[status_field] = str(first_row.get('STATUS', ''))
                    elif 'status' in subset.columns:
                        row_payload[status_field] = str(first_row.get('status', ''))
                    
                # Add AR code name
                ar_code_name_field = optional_field_names.get('ar_code_name')
                if ar_code_name_field:
                    if 'AR_CODE_NAME' in subset.columns:
                        row_payload[ar_code_name_field] = str(first_row.get('AR_CODE_NAME', ''))
                    elif 'ar_code_name' in subset.columns:
                        row_payload[ar_code_name_field] = str(first_row.get('ar_code_name', ''))
                    
                # Add expected total
                expected_total_field = optional_field_names.get('expected_total')
                if expected_total_field:
                    if 'EXPECTED_TOTAL' in subset.columns:
                        expected_sum = float(subset['EXPECTED_TOTAL'].sum())
                        row_payload[expected_total_field] = expected_sum
                    elif 'expected_total' in subset.columns:
                        expected_sum = float(subset['expected_total'].sum())
                        row_payload[expected_total_field] = expected_sum
                    
                # Add actual total
                actual_total_field = optional_field_names.get('actual_total')
                if actual_total_field:
                    if 'ACTUAL_TOTAL' in subset.columns:
                        actual_sum = float(subset['ACTUAL_TOTAL'].sum())
                        row_payload[actual_total_field] = actual_sum
                    elif 'actual_total' in subset.columns:
                        actual_sum = float(subset['actual_total'].sum())
                        row_payload[actual_total_field] = actual_sum
                    
                # Add variance
                variance_field = optional_field_names.get('variance')
                if variance_field:
                    if 'VARIANCE' in subset.columns:
                        variance_sum = float(subset['VARIANCE'].sum())
                        row_payload[variance_field] = variance_sum
                    elif 'variance' in subset.columns:
                        variance_sum = float(subset['variance'].sum())
                        row_payload[variance_field] = variance_sum
                
                # Add resident name and lease ID for exception rows (from first_row or fallback)
                resident_name = None
                lease_id_value = None
                
                # Extract from bucket row if available
                if 'CUSTOMER_NAME' in subset.columns:
                    names = subset['CUSTOMER_NAME'].dropna().astype(str).str.strip()
                    names = names[names != '']
                    if not names.empty:
                        resident_name = names.iloc[0]
                
                if 'LEASE_ID' in subset.columns:
                    lease_ids = pd.to_numeric(subset['LEASE_ID'], errors='coerce').dropna()
                    if not lease_ids.empty:
                        lease_id_value = int(lease_ids.iloc[0])
                
                # Add to payload if available
                resident_name_field = optional_field_names.get('resident_name')
                lease_id_field = optional_field_names.get('lease_id')
                if resident_name_field and resident_name:
                    row_payload[resident_name_field] = resident_name
                if lease_id_field and lease_id_value:
                    row_payload[lease_id_field] = lease_id_value

            return row_payload

        # Portfolio-level snapshot
        rows.append(_make_row('portfolio', bucket_results))

        # Property-level snapshots
        if property_column in bucket_results.columns:
            for property_id, property_df in bucket_results.groupby(property_column, dropna=False):
                rows.append(_make_row('property', property_df, property_id=property_id))

                # Exception-level snapshots (one row per lease × AR code × month)
                # This replaces the old lease-level summary approach
                if lease_column in property_df.columns:
                    for lease_interval_id, lease_df in property_df.groupby(lease_column, dropna=False):
                        # Write exception-level rows directly (skip lease summary)
                        ar_code_column = 'AR_CODE_ID' if 'AR_CODE_ID' in lease_df.columns else 'ar_code_id'
                        if ar_code_column in lease_df.columns:
                            for ar_code_id, ar_code_df in lease_df.groupby(ar_code_column, dropna=False):
                                # Exception-level rows (one per month)
                                month_column = 'AUDIT_MONTH' if 'AUDIT_MONTH' in ar_code_df.columns else 'audit_month'
                                if month_column in ar_code_df.columns:
                                    for audit_month, month_df in ar_code_df.groupby(month_column, dropna=False):
                                        rows.append(
                                            _make_row(
                                                'exception',  # Changed from 'month' to 'exception'
                                                month_df,
                                                property_id=property_id,
                                                lease_interval_id=lease_interval_id,
                                                ar_code_id=ar_code_id,
                                                audit_month=audit_month,
                                            )
                                        )

        return rows

        return rows

    def _write_run_display_snapshots_to_sharepoint_list(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
        property_name_map: Optional[Dict[int, str]] = None,
        stage_timers: Optional[Dict[str, float]] = None,
        run_scope_type: Optional[str] = None,
    ) -> bool:
        """Persist static portfolio/property/lease display snapshots to RunDisplaySnapshots list."""
        if not self._can_use_sharepoint_lists():
            print(f"[STORAGE] ❌ RunDisplaySnapshots write skipped — _can_use_sharepoint_lists()=False (token={bool(self.access_token)} site={bool(self.sharepoint_site_url)})")
            logger.debug("[STORAGE] SharePoint lists unavailable; skipping RunDisplaySnapshots write")
            return False

        try:
            site_id = self._get_site_id()
            if not site_id:
                print(f"[STORAGE] ❌ RunDisplaySnapshots write failed — could not resolve site_id")
                return False

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                print(f"[STORAGE] ❌ RunDisplaySnapshots list not found on SharePoint — list does not exist or name mismatch")
                logger.warning("[STORAGE] RunDisplaySnapshots list not found; skipping snapshot persistence")
                return False

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            snapshot_filter_started = perf_counter()
            filtered_bucket_results = self._filter_bucket_results_for_unresolved_snapshot(run_id, bucket_results)
            
            # Apply AR code whitelist filter before creating snapshots to match property/lease detail views
            try:
                from audit_engine.mappings import ALLOWED_AR_CODES_SET, ALLOWED_AR_CODES_TEXT_SET
                if ALLOWED_AR_CODES_SET and 'AR_CODE_ID' in filtered_bucket_results.columns:
                    initial_rows = len(filtered_bucket_results)
                    _ar_col = filtered_bucket_results['AR_CODE_ID']
                    _numeric = pd.to_numeric(_ar_col, errors='coerce')
                    _whitelist_mask = (
                        _numeric.isin(ALLOWED_AR_CODES_SET).fillna(False) |
                        _ar_col.astype(str).str.strip().isin(ALLOWED_AR_CODES_TEXT_SET).fillna(False)
                    )
                    filtered_bucket_results = filtered_bucket_results[_whitelist_mask].copy()
                    print(f"[STORAGE] Applied AR code whitelist filter: {initial_rows} → {len(filtered_bucket_results)} rows (codes={list(ALLOWED_AR_CODES_SET)})")
                    logger.info(f"[STORAGE] AR code whitelist filter for snapshots: {initial_rows} → {len(filtered_bucket_results)} rows")
                    
                    # Debug: check if we have any data left
                    if len(filtered_bucket_results) == 0:
                        logger.warning(f"[STORAGE] ⚠️  AR code whitelist removed ALL rows! No snapshots will be created for run {run_id}")
                        print(f"[STORAGE] ⚠️  WARNING: AR code whitelist removed all data for run {run_id}")
            except ImportError:
                logger.warning("[STORAGE] Could not import ALLOWED_AR_CODES_SET; snapshot will include all AR codes")
            except Exception as e:
                logger.warning(f"[STORAGE] AR code whitelist filter failed for snapshots: {e}")
            
            if stage_timers is not None:
                stage_timers['snapshot_filter_seconds'] = float(perf_counter() - snapshot_filter_started)

            exception_count_field_name = self._resolve_snapshot_exception_count_field_name(site_id, list_id)

            # Ensure AuditedThrough and RunScopeType columns exist BEFORE resolving optional
            # field names — _ensure_list_column_exists busts the column cache on creation,
            # so the subsequent _resolve_snapshot_optional_field_names call will see them.
            self._ensure_list_column_exists(site_id, list_id, 'AuditedThrough', column_type='text')
            self._ensure_list_column_exists(site_id, list_id, 'RunScopeType', column_type='text')
            self._ensure_list_column_exists(site_id, list_id, 'ResidentName', column_type='text')
            self._ensure_list_column_exists(site_id, list_id, 'LeaseId', column_type='number')
            # ArCodeId and AuditMonth columns must be manually created in SharePoint
            # self._ensure_list_column_exists(site_id, list_id, 'ArCodeId', column_type='number')
            # self._ensure_list_column_exists(site_id, list_id, 'AuditMonth', column_type='text')

            optional_field_names = self._resolve_snapshot_optional_field_names(site_id, list_id)

            snapshot_rows = self._build_run_display_snapshot_rows(
                run_id,
                filtered_bucket_results,
                exception_count_field_name=exception_count_field_name,
                optional_field_names=optional_field_names,
                actual_detail=actual_detail,
                expected_detail=expected_detail,
                property_name_map=property_name_map,
                full_bucket_results=bucket_results,
                run_scope_type=run_scope_type,
            )
            
            # Log snapshot details
            property_snapshots = [row for row in snapshot_rows if row.get('ScopeType') == 'property']
            property_ids_in_snapshots = [row.get('PropertyId') for row in property_snapshots]
            print(f"[STORAGE] RunDisplaySnapshots: built {len(snapshot_rows)} rows ({len(property_snapshots)} property snapshots, props={property_ids_in_snapshots})")
            logger.info(
                f"[STORAGE] Building {len(snapshot_rows)} snapshot rows for run {run_id}: "
                f"{len(property_snapshots)} property snapshots with IDs {property_ids_in_snapshots}"
            )
            
            payload_rows = [{'fields': row} for row in snapshot_rows]

            snapshot_write_started = perf_counter()
            created = self._post_list_rows_in_batches(
                site_id=site_id,
                list_id=list_id,
                row_payloads=payload_rows,
                context_label=f"RunDisplaySnapshots run={run_id}",
            )
            if stage_timers is not None:
                stage_timers['snapshot_write_seconds'] = float(perf_counter() - snapshot_write_started)

            print(f"[STORAGE] RunDisplaySnapshots posted: created={created} of {len(payload_rows)} rows for {run_id}")
            logger.info(f"[STORAGE] ✅ Wrote RunDisplaySnapshots rows for {run_id}: rows={created}")
            return True
        except Exception as e:
            print(f"[STORAGE] ❌ Exception in _write_run_display_snapshots_to_sharepoint_list for {run_id}: {e}")
            logger.error(f"[STORAGE] Error writing RunDisplaySnapshots list rows: {e}", exc_info=True)
            return False

    def _post_list_rows_in_batches(
        self,
        site_id: str,
        list_id: str,
        row_payloads: List[Dict[str, Any]],
        context_label: str,
        batch_size: int = 20,
    ) -> int:
        """Create SharePoint list items using Graph $batch with single-post fallback."""
        if not row_payloads:
            return 0

        # Microsoft Graph $batch supports max 20 sub-requests per batch.
        # Allow lowering batch size via env vars to reduce 503 throttling pressure.
        # Context-specific overrides:
        # - SHAREPOINT_BATCH_SIZE_AUDITRUNS
        # - SHAREPOINT_BATCH_SIZE_SNAPSHOTS
        # Global fallback:
        # - SHAREPOINT_BATCH_SIZE
        env_key = None
        env_value = None
        batch_size_source = 'argument_or_default'
        try:
            env_key = None
            context_lower = str(context_label or '').lower()
            if 'auditruns' in context_lower:
                env_key = 'SHAREPOINT_BATCH_SIZE_AUDITRUNS'
            elif 'rundisplaysnapshots' in context_lower:
                env_key = 'SHAREPOINT_BATCH_SIZE_SNAPSHOTS'

            env_value = os.getenv(env_key) if env_key else None
            if not env_value:
                env_value = os.getenv('SHAREPOINT_BATCH_SIZE')
                if env_value:
                    env_key = 'SHAREPOINT_BATCH_SIZE'

            if env_value:
                batch_size = int(env_value)
                batch_size_source = f"env:{env_key}"
        except Exception:
            logger.warning(
                f"[STORAGE][BATCH CONFIG] Invalid batch size env value for context={context_label}; "
                "falling back to context defaults"
            )

        # Default batch size
        default_size = 20
        batch_size = max(1, min(20, int(batch_size or default_size)))
        if batch_size_source == 'argument_or_default':
            batch_size_source = f"default:{default_size}"

        logger.info(
            f"[STORAGE][BATCH CONFIG] context={context_label} rows={len(row_payloads)} "
            f"effective_batch_size={batch_size} source={batch_size_source}"
        )

        batch_concurrency = 8  # Increased from 4 to 8 for faster writes (2x throughput)
        batch_concurrency_source = 'default:8'
        try:
            concurrency_env_key = None
            if 'auditruns' in context_lower:
                concurrency_env_key = 'SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS'
            elif 'rundisplaysnapshots' in context_lower:
                concurrency_env_key = 'SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS'

            concurrency_env_value = os.getenv(concurrency_env_key) if concurrency_env_key else None
            if not concurrency_env_value:
                concurrency_env_value = os.getenv('SHAREPOINT_BATCH_CONCURRENCY')
                if concurrency_env_value:
                    concurrency_env_key = 'SHAREPOINT_BATCH_CONCURRENCY'

            if concurrency_env_value:
                batch_concurrency = int(concurrency_env_value)
                batch_concurrency_source = f"env:{concurrency_env_key}"
        except Exception:
            logger.warning(
                f"[STORAGE][BATCH CONFIG] Invalid batch concurrency env value for context={context_label}; "
                "falling back to default concurrency=2"
            )

        batch_concurrency = max(1, min(10, int(batch_concurrency)))  # Raised cap from 4 to 10
        logger.info(
            f"[STORAGE][BATCH CONFIG] context={context_label} "
            f"effective_batch_concurrency={batch_concurrency} source={batch_concurrency_source}"
        )

        batch_http_requests = 0
        single_http_requests = 0
        batch_retry_count = 0
        single_retry_count = 0
        throttled_batch_count = 0
        batch_durations_seconds: List[float] = []

        token_acquire_started = perf_counter()
        token_acquired = False
        if not self.access_token:
            self.access_token = _get_app_only_token()
            token_acquired = bool(self.access_token)
        token_acquisition_seconds = float(perf_counter() - token_acquire_started)

        batch_url = "https://graph.microsoft.com/v1.0/$batch"
        batch_headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
        }
        items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        item_headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly',
        }

        http_session = requests.Session()
        session_pool_size = max(10, batch_concurrency * batch_size)
        session_adapter = HTTPAdapter(pool_connections=session_pool_size, pool_maxsize=session_pool_size)
        http_session.mount('https://', session_adapter)
        http_session.mount('http://', session_adapter)

        def _post_single(payload: Dict[str, Any], row_idx: int, max_retries: int = 3) -> bool:
            """Post single item with retry logic for throttling errors."""
            nonlocal single_http_requests, single_retry_count
            for attempt in range(max_retries):
                try:
                    single_http_requests += 1
                    create_response = http_session.post(items_url, headers=item_headers, json=payload, timeout=60)
                    if create_response.status_code in [200, 201]:
                        return True
                    
                    # Retry on throttling errors
                    if create_response.status_code in [429, 503, 504] and attempt < max_retries - 1:
                        single_retry_count += 1
                        wait_time = (2 ** attempt) * 0.5  # Exponential backoff: 0.5s, 1s, 2s
                        logger.warning(
                            f"[STORAGE] Throttled creating {context_label} row {row_idx} "
                            f"({create_response.status_code}), retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                        )
                        sleep(wait_time)
                        continue
                    
                    logger.warning(
                        f"[STORAGE] Failed creating {context_label} row {row_idx}: "
                        f"{create_response.status_code} - {create_response.text}"
                    )
                    return False
                except Exception as e:
                    if attempt < max_retries - 1:
                        single_retry_count += 1
                        wait_time = (2 ** attempt) * 0.5
                        logger.warning(
                            f"[STORAGE] Exception creating {context_label} row {row_idx}, "
                            f"retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        sleep(wait_time)
                        continue
                    logger.warning(f"[STORAGE] Failed creating {context_label} row {row_idx}: {e}")
                    return False
            return False

        def _process_batch(batch_index: int, start: int, chunk: List[Dict[str, Any]]) -> Dict[str, Any]:
            created_local = 0
            batch_http_requests_local = 0
            single_http_requests_local = 0
            batch_retry_count_local = 0
            single_retry_count_local = 0
            throttled_batch_count_local = 0

            batch_started = perf_counter()
            payload_build_started = perf_counter()
            batch_requests = []
            batch_http_wait_seconds = 0.0
            batch_response_parse_seconds = 0.0

            def _post_single_local(payload: Dict[str, Any], row_idx: int, max_retries: int = 3) -> bool:
                nonlocal single_http_requests_local, single_retry_count_local
                for attempt in range(max_retries):
                    try:
                        single_http_requests_local += 1
                        create_response = http_session.post(items_url, headers=item_headers, json=payload, timeout=60)
                        if create_response.status_code in [200, 201]:
                            return True

                        if create_response.status_code in [429, 503, 504] and attempt < max_retries - 1:
                            single_retry_count_local += 1
                            wait_time = (2 ** attempt) * 0.5
                            logger.warning(
                                f"[STORAGE] Throttled creating {context_label} row {row_idx} "
                                f"({create_response.status_code}), retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                            )
                            sleep(wait_time)
                            continue

                        logger.warning(
                            f"[STORAGE] Failed creating {context_label} row {row_idx}: "
                            f"{create_response.status_code} - {create_response.text}"
                        )
                        return False
                    except Exception as e:
                        if attempt < max_retries - 1:
                            single_retry_count_local += 1
                            wait_time = (2 ** attempt) * 0.5
                            logger.warning(
                                f"[STORAGE] Exception creating {context_label} row {row_idx}, "
                                f"retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries}): {e}"
                            )
                            sleep(wait_time)
                            continue
                        logger.warning(f"[STORAGE] Failed creating {context_label} row {row_idx}: {e}")
                        return False
                return False

            for offset, payload in enumerate(chunk):
                row_idx = start + offset
                batch_requests.append({
                    'id': str(row_idx),
                    'method': 'POST',
                    'url': f"/sites/{site_id}/lists/{list_id}/items",
                    'headers': {'Content-Type': 'application/json'},
                    'body': payload,
                })

            payload_build_seconds = float(perf_counter() - payload_build_started)

            batch_success = False
            batch_response = None
            total_batches_local = (len(row_payloads) + batch_size - 1) // batch_size
            for attempt in range(3):
                try:
                    batch_http_requests_local += 1
                    http_wait_started = perf_counter()
                    batch_response = http_session.post(
                        batch_url,
                        headers=batch_headers,
                        json={'requests': batch_requests},
                        timeout=120,
                    )
                    batch_http_wait_seconds += float(perf_counter() - http_wait_started)

                    if batch_response.status_code in [429, 503, 504] and attempt < 2:
                        throttled_batch_count_local += 1
                        batch_retry_count_local += 1
                        wait_time = (2 ** attempt) * 1.0
                        logger.warning(
                            f"[STORAGE] Batch {batch_index}/{total_batches_local} throttled for {context_label} "
                            f"({batch_response.status_code}), retrying in {wait_time}s... (attempt {attempt + 1}/3)"
                        )
                        sleep(wait_time)
                        continue

                    if batch_response.status_code == 200:
                        batch_success = True
                        break

                    logger.warning(
                        f"[STORAGE] Batch {batch_index}/{total_batches_local} failed for {context_label} "
                        f"(HTTP {batch_response.status_code}); falling back to single posts"
                    )
                    break
                except Exception as e:
                    if attempt < 2:
                        batch_retry_count_local += 1
                        wait_time = (2 ** attempt) * 1.0
                        logger.warning(
                            f"[STORAGE] Batch {batch_index}/{total_batches_local} request exception for {context_label}, "
                            f"retrying in {wait_time}s... (attempt {attempt + 1}/3): {e}"
                        )
                        sleep(wait_time)
                        continue

                    logger.warning(
                        f"[STORAGE] Batch {batch_index}/{total_batches_local} request failed for {context_label}; "
                        f"falling back to single posts: {e}"
                    )
                    break

            if batch_success and batch_response is not None:
                response_parse_started = perf_counter()
                response_items = batch_response.json().get('responses', [])
                batch_response_parse_seconds += float(perf_counter() - response_parse_started)
                response_map = {item.get('id'): item for item in response_items}

                throttled_items: List[tuple[int, Dict[str, Any], int]] = []  # (row_idx, payload, retry_after)
                other_failed_items: List[tuple[int, Dict[str, Any], int, Any]] = []  # (row_idx, payload, status, body)

                for offset, payload in enumerate(chunk):
                    row_idx = start + offset
                    response_item = response_map.get(str(row_idx))
                    if not response_item:
                        logger.warning(
                            f"[STORAGE] Missing batch response for {context_label} row {row_idx}; "
                            "falling back to single post"
                        )
                        other_failed_items.append((row_idx, payload, None, None))
                        continue

                    status_code = response_item.get('status')
                    response_body = response_item.get('body')
                    
                    if status_code in [200, 201]:
                        created_local += 1
                        continue

                    if status_code == 429:
                        retry_after = 5
                        item_headers_resp = response_item.get('headers') or {}
                        ra_str = item_headers_resp.get('Retry-After') or item_headers_resp.get('retry-after')
                        if ra_str:
                            try:
                                retry_after = int(ra_str)
                            except (ValueError, TypeError):
                                pass
                        throttled_items.append((row_idx, payload, retry_after))
                    else:
                        logger.warning(
                            f"[STORAGE] Batch item failed for {context_label} row {row_idx}: "
                            f"{status_code} - {response_item.get('body')}; retrying individually"
                        )
                        other_failed_items.append((row_idx, payload, status_code, response_item.get('body')))

                # Re-submit throttled items as a new batch after honouring Retry-After
                if throttled_items:
                    max_wait = max(ra for _, _, ra in throttled_items)
                    logger.warning(
                        f"[STORAGE] {len(throttled_items)} batch item(s) throttled for {context_label}; "
                        f"waiting {max_wait}s then re-batching"
                    )
                    sleep(max_wait)
                    throttled_batch_count_local += 1
                    batch_retry_count_local += 1
                    retry_requests = [
                        {
                            'id': str(row_idx),
                            'method': 'POST',
                            'url': f"/sites/{site_id}/lists/{list_id}/items",
                            'headers': {'Content-Type': 'application/json'},
                            'body': payload,
                        }
                        for row_idx, payload, _ in throttled_items
                    ]
                    try:
                        batch_http_requests_local += 1
                        http_wait_started = perf_counter()
                        retry_response = http_session.post(
                            batch_url,
                            headers=batch_headers,
                            json={'requests': retry_requests},
                            timeout=120,
                        )
                        batch_http_wait_seconds += float(perf_counter() - http_wait_started)
                        if retry_response.status_code == 200:
                            retry_map = {item.get('id'): item for item in retry_response.json().get('responses', [])}
                            for row_idx, payload, _ in throttled_items:
                                ri = retry_map.get(str(row_idx))
                                if ri and ri.get('status') in [200, 201]:
                                    created_local += 1
                                else:
                                    rs = ri.get('status') if ri else None
                                    logger.warning(
                                        f"[STORAGE] Batch retry failed for {context_label} row {row_idx}: "
                                        f"{rs} - {ri.get('body') if ri else 'no response'}; retrying individually"
                                    )
                                    if _post_single_local(payload, row_idx):
                                        created_local += 1
                        else:
                            for row_idx, payload, _ in throttled_items:
                                if _post_single_local(payload, row_idx):
                                    created_local += 1
                    except Exception as e:
                        logger.warning(f"[STORAGE] Batch retry request failed for {context_label}: {e}")
                        for row_idx, payload, _ in throttled_items:
                            if _post_single_local(payload, row_idx):
                                created_local += 1

                # Fall back to single posts for non-throttle failures
                for row_idx, payload, _status, _body in other_failed_items:
                    if _post_single_local(payload, row_idx):
                        created_local += 1
            else:
                for offset, payload in enumerate(chunk):
                    if _post_single_local(payload, start + offset):
                        created_local += 1

            batch_duration_seconds = float(perf_counter() - batch_started)
            return {
                'batch_index': batch_index,
                'created': created_local,
                'batch_http_requests': batch_http_requests_local,
                'single_http_requests': single_http_requests_local,
                'batch_retry_count': batch_retry_count_local,
                'single_retry_count': single_retry_count_local,
                'throttled_batch_count': throttled_batch_count_local,
                'batch_duration_seconds': batch_duration_seconds,
                'payload_build_seconds': payload_build_seconds,
                'http_wait_seconds': batch_http_wait_seconds,
                'response_parse_seconds': batch_response_parse_seconds,
                'rows_in_batch': len(chunk),
            }

        created = 0
        total_batches = (len(row_payloads) + batch_size - 1) // batch_size
        chunks: List[tuple[int, int, List[Dict[str, Any]]]] = []
        batch_index = 0
        for start in range(0, len(row_payloads), batch_size):
            batch_index += 1
            chunks.append((batch_index, start, row_payloads[start:start + batch_size]))

        batch_profile_logged = False
        if batch_concurrency == 1:
            for chunk_batch_index, chunk_start, chunk in chunks:
                result = _process_batch(chunk_batch_index, chunk_start, chunk)
                created += int(result['created'])
                batch_http_requests += int(result['batch_http_requests'])
                single_http_requests += int(result['single_http_requests'])
                batch_retry_count += int(result['batch_retry_count'])
                single_retry_count += int(result['single_retry_count'])
                throttled_batch_count += int(result['throttled_batch_count'])
                batch_durations_seconds.append(float(result['batch_duration_seconds']))

                if not batch_profile_logged and int(result['batch_index']) == 1:
                    logger.info(
                        f"[STORAGE][BATCH PROFILE] context={context_label} batch=1/{total_batches} "
                        f"rows_in_batch={int(result['rows_in_batch'])} "
                        f"payload_build_seconds={float(result['payload_build_seconds']):.3f} "
                        f"token_acquisition_seconds={token_acquisition_seconds:.3f} "
                        f"token_acquired={token_acquired} "
                        f"http_wait_seconds={float(result['http_wait_seconds']):.3f} "
                        f"response_parse_seconds={float(result['response_parse_seconds']):.3f}"
                    )
                    batch_profile_logged = True

                if chunk_batch_index < total_batches:
                    sleep(0.5)
        else:
            with ThreadPoolExecutor(max_workers=batch_concurrency) as executor:
                future_to_batch_index = {
                    executor.submit(_process_batch, chunk_batch_index, chunk_start, chunk): chunk_batch_index
                    for chunk_batch_index, chunk_start, chunk in chunks
                }

                for future in as_completed(future_to_batch_index):
                    result = future.result()
                    created += int(result['created'])
                    batch_http_requests += int(result['batch_http_requests'])
                    single_http_requests += int(result['single_http_requests'])
                    batch_retry_count += int(result['batch_retry_count'])
                    single_retry_count += int(result['single_retry_count'])
                    throttled_batch_count += int(result['throttled_batch_count'])
                    batch_durations_seconds.append(float(result['batch_duration_seconds']))

                    if not batch_profile_logged and int(result['batch_index']) == 1:
                        logger.info(
                            f"[STORAGE][BATCH PROFILE] context={context_label} batch=1/{total_batches} "
                            f"rows_in_batch={int(result['rows_in_batch'])} "
                            f"payload_build_seconds={float(result['payload_build_seconds']):.3f} "
                            f"token_acquisition_seconds={token_acquisition_seconds:.3f} "
                            f"token_acquired={token_acquired} "
                            f"http_wait_seconds={float(result['http_wait_seconds']):.3f} "
                            f"response_parse_seconds={float(result['response_parse_seconds']):.3f}"
                        )
                        batch_profile_logged = True

        retry_count = batch_retry_count + single_retry_count
        total_http_requests = batch_http_requests + single_http_requests
        average_batch_duration_seconds = (
            sum(batch_durations_seconds) / len(batch_durations_seconds)
            if batch_durations_seconds else 0.0
        )

        logger.info(
            f"[STORAGE][BATCH SUMMARY] context={context_label} "
            f"total_rows={len(row_payloads)} total_batches={total_batches} "
            f"batch_concurrency={batch_concurrency} "
            f"session_pool_size={session_pool_size} "
            f"total_http_requests={total_http_requests} "
            f"average_batch_duration_seconds={average_batch_duration_seconds:.3f} "
            f"retry_count={retry_count} throttled_batch_count={throttled_batch_count} "
            f"final_rows_written={created} batch_http_requests={batch_http_requests} "
            f"single_http_requests={single_http_requests} batch_retry_count={batch_retry_count} "
            f"single_retry_count={single_retry_count}"
        )

        http_session.close()
        return created

    def _write_results_to_sharepoint_list_async(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
    ) -> None:
        """Background wrapper for detailed AuditRuns2 list persistence."""
        try:
            # Refresh token — background thread may outlive original request token.
            try:
                new_token = _get_app_only_token()
                if new_token:
                    self.access_token = new_token
            except Exception as _token_err:
                logger.warning(f"[STORAGE] Token refresh failed in async results write: {_token_err}")
            logger.info(f"[STORAGE] Background AuditRuns2 write started for {run_id}")
            print(f"[AUDITRUNS2_ASYNC] Starting background write for {run_id}...")
            print(f"[AUDITRUNS2_ASYNC]    bucket_results: {len(bucket_results)} rows")
            print(f"[AUDITRUNS2_ASYNC]    findings: {len(findings)} rows")
            
            write_result = self._write_results_to_sharepoint_list(
                run_id,
                bucket_results,
                findings,
                actual_detail=actual_detail,
                expected_detail=expected_detail,
            )
            
            if write_result:
                logger.info(f"[STORAGE] Background AuditRuns2 write finished for {run_id}")
                print(f"[AUDITRUNS2_ASYNC] [OK] Background write completed for {run_id}")
            else:
                logger.warning(f"[STORAGE] Background AuditRuns2 write returned False for {run_id}")
                print(f"[AUDITRUNS2_ASYNC] [WARN] Background write returned False for {run_id}")
        except Exception as e:
            logger.error(f"[STORAGE] Background AuditRuns2 write failed for {run_id}: {e}", exc_info=True)
            print(f"[AUDITRUNS2_ASYNC] [ERROR] Background write failed for {run_id}: {e}")
    
    def _write_metrics_to_sharepoint_list_async(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        metadata: Dict[str, Any],
    ) -> None:
        """Background wrapper for metrics list persistence."""
        try:
            # Refresh token — background thread may outlive original request token
            try:
                new_token = _get_app_only_token()
                if new_token:
                    self.access_token = new_token
            except Exception as _token_err:
                logger.warning(f"[STORAGE] Token refresh failed in async metrics write: {_token_err}")
            logger.info(f"[STORAGE] 🚀 Background metrics write started for {run_id}")
            self._write_metrics_to_sharepoint_list(run_id, bucket_results, findings, metadata)
            logger.info(f"[STORAGE] ✅ Background metrics write finished for {run_id}")
        except Exception as e:
            logger.error(f"[STORAGE] Background metrics write failed for {run_id}: {e}", exc_info=True)

    def _validate_run_display_snapshots_async(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
    ) -> None:
        """Background wrapper for snapshot validation."""
        try:
            logger.info(f"[STORAGE] 🚀 Background snapshot validation started for {run_id}")
            validation = self.validate_run_display_snapshots(run_id, bucket_results)
            if validation.get('ok'):
                logger.info(
                    f"[STORAGE] ✅ Snapshot validation passed for {run_id}: "
                    f"portfolio={validation['actual']['portfolio']}, "
                    f"property={validation['actual']['property']}, "
                    f"lease={validation['actual']['lease']}"
                )
            else:
                logger.warning(
                    f"[STORAGE] Snapshot validation warnings for {run_id}: {validation.get('errors', [])}"
                )
        except Exception as e:
            logger.error(f"[STORAGE] Background snapshot validation failed for {run_id}: {e}", exc_info=True)

    def _write_run_display_snapshots_async(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
        property_name_map: Optional[Dict[int, str]] = None,
        snapshot_validation_async: bool = True,
        run_scope_type: Optional[str] = None,
    ) -> None:
        """Background wrapper for RunDisplaySnapshots persistence and optional validation."""
        try:
            # Refresh token — this runs in a background thread that may outlive the
            # original request token lifetime. Always acquire a fresh token before
            # making any SharePoint list API calls.
            try:
                new_token = _get_app_only_token()
                if new_token:
                    self.access_token = new_token
                    logger.debug(f"[STORAGE] 🔄 Token refreshed for async snapshot write {run_id}")
                else:
                    logger.warning(f"[STORAGE] ⚠️ Token refresh returned None for async snapshot write {run_id}")
            except Exception as _token_err:
                logger.warning(f"[STORAGE] Token refresh failed in async snapshot write: {_token_err}")

            print(f"[STORAGE] 🚀 Background RunDisplaySnapshots write STARTED for {run_id} | can_use_lists={self._can_use_sharepoint_lists()} | has_token={bool(self.access_token)}")
            logger.info(f"[STORAGE] 🚀 Background RunDisplaySnapshots write started for {run_id}")
            snapshot_stage_timers: Dict[str, float] = {
                'snapshot_filter_seconds': 0.0,
                'snapshot_write_seconds': 0.0,
                'snapshot_validate_seconds': 0.0,
            }
            snapshot_write_ok = self._write_run_display_snapshots_to_sharepoint_list(
                run_id,
                bucket_results,
                actual_detail=actual_detail,
                expected_detail=expected_detail,
                property_name_map=property_name_map,
                stage_timers=snapshot_stage_timers,
                run_scope_type=run_scope_type,
            )
            if snapshot_write_ok:
                print(f"[STORAGE] ✅ Background RunDisplaySnapshots write SUCCESS for {run_id}")
                logger.info(
                    f"[STORAGE] ✅ Background RunDisplaySnapshots write finished for {run_id}: "
                    f"filter={snapshot_stage_timers.get('snapshot_filter_seconds', 0.0):.2f}s "
                    f"write={snapshot_stage_timers.get('snapshot_write_seconds', 0.0):.2f}s"
                )
                if snapshot_validation_async and self._can_use_sharepoint_lists():
                    self._validate_run_display_snapshots_async(run_id, bucket_results)
                else:
                    validate_started = perf_counter()
                    validation = self.validate_run_display_snapshots(run_id, bucket_results)
                    snapshot_stage_timers['snapshot_validate_seconds'] = float(perf_counter() - validate_started)
                    if validation.get('ok'):
                        logger.info(
                            f"[STORAGE] ✅ Background snapshot validation passed for {run_id}: "
                            f"portfolio={validation['actual']['portfolio']}, "
                            f"property={validation['actual']['property']}, "
                            f"lease={validation['actual']['lease']}"
                        )
                    else:
                        logger.warning(
                            f"[STORAGE] Background snapshot validation warnings for {run_id}: {validation.get('errors', [])}"
                        )
            else:
                print(f"[STORAGE] ⚠️ Background RunDisplaySnapshots write SKIPPED/FAILED for {run_id}")
                logger.warning(f"[STORAGE] Background RunDisplaySnapshots write skipped/failed for {run_id}")
        except Exception as e:
            print(f"[STORAGE] ❌ Background RunDisplaySnapshots write EXCEPTION for {run_id}: {e}")
            logger.error(f"[STORAGE] Background RunDisplaySnapshots write failed for {run_id}: {e}", exc_info=True)

    def load_run_display_snapshot_from_sharepoint_list(
        self,
        run_id: str,
        scope_type: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Load a static snapshot row for a given run/scope from RunDisplaySnapshots list."""
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint lists unavailable; cannot load RunDisplaySnapshots")
            return None

        try:
            site_id = self._get_site_id()
            if not site_id:
                return None

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                logger.warning("[STORAGE] RunDisplaySnapshots list not found; cannot load snapshots")
                return None

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            filters = [
                f"fields/RunId eq '{run_id}'",
                f"fields/ScopeType eq '{scope_type}'",
            ]
            if property_id is not None:
                filters.append(f"fields/PropertyId eq {int(property_id)}")
            if lease_interval_id is not None:
                filters.append(f"fields/LeaseIntervalId eq {int(lease_interval_id)}")

            params = {
                '$expand': 'fields',
                '$filter': ' and '.join(filters),
                '$top': 1
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading RunDisplaySnapshots for run={run_id}, scope={scope_type}: "
                    f"{response.status_code} - {response.text}"
                )
                return None

            items = response.json().get('value', [])
            if not items:
                return None

            fields = items[0].get('fields', {})
            exception_count = self._safe_field_value(fields.get('ExceptionCountStatic'))
            if exception_count is None:
                exception_count = self._safe_field_value(fields.get('ExceptionCountStatistic'))

            snapshot = {
                'snapshot_key': self._safe_field_value(fields.get('SnapshotKey')),
                'run_id': self._safe_field_value(fields.get('RunId')) or run_id,
                'scope_type': self._safe_field_value(fields.get('ScopeType')) or scope_type,
                'property_id': self._safe_field_value(fields.get('PropertyId')),
                'lease_interval_id': self._safe_field_value(fields.get('LeaseIntervalId')),
                'property_name': self._safe_field_value(fields.get('PropertyNameStatic')) or self._safe_field_value(fields.get('PropertyName')),
                'exception_count': int(float(exception_count or 0)),
                'undercharge': float(self._safe_field_value(fields.get('UnderchargeStatic')) or 0),
                'overcharge': float(self._safe_field_value(fields.get('OverchargeStatic')) or 0),
                'total_variance': float(self._safe_field_value(fields.get('TotalVarianceStatic')) or 0),
                'total_lease_intervals': int(float(self._safe_field_value(fields.get('TotalLeaseIntervalStatic')) or 0)),
                'match_rate': float(self._safe_field_value(fields.get('MatchRateStatic')) or 0),
                'total_buckets': int(float(self._safe_field_value(fields.get('TotalBucketsStatic')) or 0)),
                'matched_buckets': int(float(self._safe_field_value(fields.get('MatchedBucketsStatic')) or 0)),
            }

            if not snapshot['total_variance']:
                snapshot['total_variance'] = snapshot['undercharge'] + snapshot['overcharge']

            logger.info(
                f"[STORAGE] ✅ Loaded RunDisplaySnapshot: run={run_id}, scope={scope_type}, "
                f"property_id={property_id}, lease_interval_id={lease_interval_id}, "
                f"snapshot_key={snapshot.get('snapshot_key')}"
            )
            return snapshot
        except Exception as e:
            logger.error(f"[STORAGE] Error loading RunDisplaySnapshots row: {e}", exc_info=True)
            return None

    def load_run_display_snapshots_for_property(
        self,
        run_id: str,
        property_id: int,
        scope_type: str = 'lease',
    ) -> Dict[int, Dict[str, Any]]:
        """Load all snapshot rows for a property keyed by lease interval id."""
        if not self._can_use_sharepoint_lists():
            return {}

        try:
            site_id = self._get_site_id()
            if not site_id:
                return {}

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                return {}

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$filter': (
                    f"fields/RunId eq '{run_id}' and "
                    f"fields/ScopeType eq '{scope_type}' and "
                    f"fields/PropertyId eq {int(property_id)}"
                ),
                '$top': 5000
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading RunDisplaySnapshots list rows for property {property_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return {}

            snapshot_map: Dict[int, Dict[str, Any]] = {}
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                lease_interval_id = self._safe_int(self._safe_field_value(fields.get('LeaseIntervalId')))
                if lease_interval_id is None:
                    continue

                exception_count = self._safe_field_value(fields.get('ExceptionCountStatic'))
                if exception_count is None:
                    exception_count = self._safe_field_value(fields.get('ExceptionCountStatistic'))

                undercharge = float(self._safe_field_value(fields.get('UnderchargeStatic')) or 0)
                overcharge = float(self._safe_field_value(fields.get('OverchargeStatic')) or 0)

                snapshot_map[lease_interval_id] = {
                    'snapshot_key': self._safe_field_value(fields.get('SnapshotKey')),
                    'run_id': self._safe_field_value(fields.get('RunId')) or run_id,
                    'scope_type': self._safe_field_value(fields.get('ScopeType')) or scope_type,
                    'property_id': self._safe_int(self._safe_field_value(fields.get('PropertyId'))),
                    'lease_interval_id': lease_interval_id,
                    'property_name': self._safe_field_value(fields.get('PropertyNameStatic')) or self._safe_field_value(fields.get('PropertyName')),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(self._safe_field_value(fields.get('TotalVarianceStatic')) or (undercharge + overcharge)),
                    'total_lease_intervals': int(float(self._safe_field_value(fields.get('TotalLeaseIntervalStatic')) or 1)),
                    'match_rate': float(self._safe_field_value(fields.get('MatchRateStatic')) or 0),
                    'total_buckets': int(float(self._safe_field_value(fields.get('TotalBucketsStatic')) or 0)),
                    'matched_buckets': int(float(self._safe_field_value(fields.get('MatchedBucketsStatic')) or 0)),
                    'resident_name': self._safe_field_value(fields.get('ResidentName')),  # NEW: Load resident name from snapshot
                    'lease_id': self._safe_int(self._safe_field_value(fields.get('LeaseId'))),  # NEW: Load lease ID from snapshot
                }

            logger.info(
                f"[STORAGE] ✅ Loaded {len(snapshot_map)} RunDisplaySnapshots rows for run={run_id}, "
                f"property_id={property_id}, scope={scope_type}"
            )
            return snapshot_map
        except Exception as e:
            logger.error(
                f"[STORAGE] Error loading RunDisplaySnapshots rows for property {property_id}: {e}",
                exc_info=True
            )
            return {}

    def load_exception_snapshots_as_bucket_results(
        self,
        run_id: str,
        property_id: int = None,
        lease_interval_id: int = None,
    ) -> pd.DataFrame:
        """Load exception-level snapshots from RunDisplaySnapshots and convert to bucket_results DataFrame format."""
        if not self._can_use_sharepoint_lists():
            return pd.DataFrame()  # Force sync

        try:
            site_id = self._get_site_id()
            if not site_id:
                return pd.DataFrame()

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                return pd.DataFrame()

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            
            # Build filter for exception-level snapshots
            filter_parts = [f"fields/RunId eq '{run_id}'", "fields/ScopeType eq 'exception'"]
            if property_id is not None:
                filter_parts.append(f"fields/PropertyId eq {int(property_id)}")
            if lease_interval_id is not None:
                filter_parts.append(f"fields/LeaseIntervalId eq {int(lease_interval_id)}")
            
            params = {
                '$expand': 'fields',
                '$filter': ' and '.join(filter_parts),
                '$top': 5000
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading exception snapshots: {response.status_code} - {response.text}"
                )
                return pd.DataFrame()

            # Convert exception rows to DataFrame matching bucket_results format
            rows = []
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                
                row = {
                    CanonicalField.PROPERTY_ID.value: self._safe_int(fields.get('PropertyId')),
                    CanonicalField.PROPERTY_NAME.value: self._safe_field_value(fields.get('PropertyNameStatic')),
                    CanonicalField.LEASE_INTERVAL_ID.value: self._safe_int(fields.get('LeaseIntervalId')),
                    CanonicalField.AR_CODE_ID.value: self._safe_field_value(fields.get('ArCodeId')),
                    'AR_CODE_NAME': self._safe_field_value(fields.get('ArCodeName')),
                    CanonicalField.AUDIT_MONTH.value: self._safe_field_value(fields.get('AuditMonth')),
                    CanonicalField.STATUS.value: self._safe_field_value(fields.get('Status')),
                    CanonicalField.EXPECTED_TOTAL.value: float(self._safe_field_value(fields.get('ExpectedTotal')) or 0),
                    CanonicalField.ACTUAL_TOTAL.value: float(self._safe_field_value(fields.get('ActualTotal')) or 0),
                    CanonicalField.VARIANCE.value: float(self._safe_field_value(fields.get('Variance')) or 0),
                    'CUSTOMER_NAME': self._safe_field_value(fields.get('ResidentName')),
                    'LEASE_ID': self._safe_int(fields.get('LeaseId')),
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            logger.info(
                f"[STORAGE] ✅ Loaded {len(df)} exception snapshots as bucket_results for run={run_id}, "
                f"property_id={property_id}, lease_interval_id={lease_interval_id}"
            )
            
            # Mark as snapshot-sourced for debugging
            if not df.empty:
                df.attrs['_read_source'] = 'RunDisplaySnapshots_exception_scope'
                df.attrs['_read_reason'] = 'exception_level_snapshots'
            
            return df
        except Exception as e:
            logger.error(
                f"[STORAGE] Error loading exception snapshots as bucket_results: {e}",
                exc_info=True
            )
            return pd.DataFrame()

    def load_run_display_snapshots_for_run(
        self,
        run_id: str,
        scope_type: str = 'property',
    ) -> List[Dict[str, Any]]:
        """Load all snapshot rows for a run and scope type."""
        if not self._can_use_sharepoint_lists():
            return []

        try:
            site_id = self._get_site_id()
            if not site_id:
                return []

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                return []

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            params = {
                '$expand': 'fields',
                '$filter': (
                    f"fields/RunId eq '{run_id}' and "
                    f"fields/ScopeType eq '{scope_type}'"
                ),
                '$top': 5000
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading RunDisplaySnapshots rows for run={run_id}, scope={scope_type}: "
                    f"{response.status_code} - {response.text}"
                )
                return []

            # Build lease-count overlay for property rows.
            lease_counts_by_property: Dict[int, int] = {}
            if scope_type == 'property':
                lease_params = {
                    '$expand': 'fields',
                    '$filter': (
                        f"fields/RunId eq '{run_id}' and "
                        "fields/ScopeType eq 'lease'"
                    ),
                    '$top': 5000
                }
                lease_response = requests.get(items_url, headers=headers, params=lease_params, timeout=60)
                if lease_response.status_code == 200:
                    lease_rows = lease_response.json().get('value', [])
                    for row in lease_rows:
                        fields = row.get('fields', {})
                        property_id_int = self._safe_int(fields.get('PropertyId'))
                        if property_id_int is None:
                            continue
                        lease_counts_by_property[property_id_int] = lease_counts_by_property.get(property_id_int, 0) + 1
                else:
                    logger.warning(
                        f"[STORAGE] Failed loading lease snapshots for run={run_id}: "
                        f"{lease_response.status_code} - {lease_response.text}"
                    )

            snapshot_rows: List[Dict[str, Any]] = []
            for item in response.json().get('value', []):
                fields = item.get('fields', {})

                exception_count = self._safe_field_value(fields.get('ExceptionCountStatic'))
                if exception_count is None:
                    exception_count = self._safe_field_value(fields.get('ExceptionCountStatistic'))

                property_id_int = self._safe_int(self._safe_field_value(fields.get('PropertyId')))
                lease_interval_id_int = self._safe_int(self._safe_field_value(fields.get('LeaseIntervalId')))
                undercharge = float(self._safe_field_value(fields.get('UnderchargeStatic')) or 0)
                overcharge = float(self._safe_field_value(fields.get('OverchargeStatic')) or 0)

                snapshot_rows.append({
                    'snapshot_key': self._safe_field_value(fields.get('SnapshotKey')),
                    'run_id': self._safe_field_value(fields.get('RunId')) or run_id,
                    'scope_type': self._safe_field_value(fields.get('ScopeType')) or scope_type,
                    'property_id': property_id_int,
                    'lease_interval_id': lease_interval_id_int,
                    'property_name': self._safe_field_value(fields.get('PropertyNameStatic')) or self._safe_field_value(fields.get('PropertyName')),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(self._safe_field_value(fields.get('TotalVarianceStatic')) or (undercharge + overcharge)),
                    'match_rate': float(self._safe_field_value(fields.get('MatchRateStatic')) or 0),
                    'total_buckets': int(float(self._safe_field_value(fields.get('TotalBucketsStatic')) or 0)),
                    'matched_buckets': int(float(self._safe_field_value(fields.get('MatchedBucketsStatic')) or 0)),
                    'total_lease_intervals': int(float(self._safe_field_value(fields.get('TotalLeaseIntervalStatic')) or lease_counts_by_property.get(property_id_int, 0) or 0))
                })

            snapshot_rows.sort(key=lambda row: (row.get('property_id') is None, row.get('property_id', 0)))

            logger.info(
                f"[STORAGE] ✅ Loaded {len(snapshot_rows)} RunDisplaySnapshots rows for run={run_id}, "
                f"scope={scope_type}"
            )
            return snapshot_rows
        except Exception as e:
            logger.error(
                f"[STORAGE] Error loading RunDisplaySnapshots rows for run={run_id}, scope={scope_type}: {e}",
                exc_info=True
            )
            return []

    def load_latest_property_snapshots_across_runs(self) -> List[Dict[str, Any]]:
        """Load the most recent snapshot for each property across ALL runs (for aggregated portfolio view)."""
        if not self._can_use_sharepoint_lists():
            return []

        try:
            site_id = self._get_site_id()
            if not site_id:
                return []

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                return []

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            # Load ALL property-scoped snapshots (no RunId filter)
            params = {
                '$expand': 'fields',
                '$filter': "fields/ScopeType eq 'property'",
                '$top': 5000,
                '$orderby': 'fields/RunId desc'  # Latest runs first
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading all property snapshots: "
                    f"{response.status_code} - {response.text}"
                )
                return []

            # Group by property_id and keep the latest entry per property.
            # NOTE: $orderby on RunId is unreliable because $filter is on ScopeType (non-indexed column).
            # SharePoint may return items in creation order (oldest first) despite the desc orderby.
            # We compare RunId strings explicitly — run_YYYYMMDD_HHMMSS sorts chronologically as strings.
            latest_by_property: Dict[int, Dict[str, Any]] = {}
            
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                
                property_id_int = self._safe_int(fields.get('PropertyId'))
                if property_id_int is None:
                    continue
                
                # Keep the entry with the highest RunId (newest run) regardless of return order
                current_run_id = fields.get('RunId', '')
                existing = latest_by_property.get(property_id_int)
                if existing is not None and current_run_id <= existing.get('run_id', ''):
                    continue  # existing entry is same age or newer — skip this one
                
                exception_count = fields.get('ExceptionCountStatic')
                if exception_count is None:
                    exception_count = fields.get('ExceptionCountStatistic')
                
                undercharge = float(fields.get('UnderchargeStatic') or 0)
                overcharge = float(fields.get('OverchargeStatic') or 0)
                
                latest_by_property[property_id_int] = {
                    'snapshot_key': fields.get('SnapshotKey'),
                    'run_id': fields.get('RunId'),
                    'scope_type': 'property',
                    'run_scope_type': fields.get('RunScopeType') or None,
                    'property_id': property_id_int,
                    'property_name': fields.get('PropertyNameStatic') or fields.get('PropertyName'),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(fields.get('TotalVarianceStatic') or (undercharge + overcharge)),
                    'match_rate': float(fields.get('MatchRateStatic') or 0),
                    'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                    'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
                    'total_lease_intervals': int(float(fields.get('TotalLeaseIntervalStatic') or 0)),
                    'audited_through': fields.get('AuditedThrough') or None,
                }
            
            snapshot_rows = list(latest_by_property.values())
            snapshot_rows.sort(key=lambda row: row.get('exception_count', 0), reverse=True)
            
            logger.info(
                f"[STORAGE] ✅ Loaded latest snapshots for {len(snapshot_rows)} properties across all runs"
            )
            return snapshot_rows
        except Exception as e:
            logger.error(
                f"[STORAGE] Error loading latest property snapshots across runs: {e}",
                exc_info=True
            )
            return []

    def validate_run_display_snapshots(self, run_id: str, bucket_results: pd.DataFrame) -> Dict[str, Any]:
        """Validate that required run display snapshots exist and counts align with bucket scope."""
        validation = {
            'ok': False,
            'run_id': run_id,
            'expected': {
                'portfolio': 0,
                'property': 0,
                'lease': 0,
            },
            'actual': {
                'portfolio': 0,
                'property': 0,
                'lease': 0,
            },
            'errors': []
        }

        try:
            if bucket_results is None or len(bucket_results) == 0:
                validation['errors'].append('No bucket_results available for snapshot validation')
                return validation

            filtered_bucket_results = self._filter_bucket_results_for_unresolved_snapshot(run_id, bucket_results)
            if filtered_bucket_results is None or len(filtered_bucket_results) == 0:
                validation['errors'].append('No filtered bucket_results available for snapshot validation')
                return validation

            property_column = 'PROPERTY_ID' if 'PROPERTY_ID' in filtered_bucket_results.columns else 'property_id'
            lease_column = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in filtered_bucket_results.columns else 'lease_interval_id'

            if property_column not in filtered_bucket_results.columns or lease_column not in filtered_bucket_results.columns:
                validation['errors'].append('Required property/lease columns missing for snapshot validation')
                return validation

            expected_property_count = int(pd.to_numeric(filtered_bucket_results[property_column], errors='coerce').dropna().nunique())
            expected_lease_count = int(
                filtered_bucket_results[[property_column, lease_column]]
                .dropna()
                .drop_duplicates()
                .shape[0]
            )

            validation['expected'] = {
                'portfolio': 1,
                'property': expected_property_count,
                'lease': expected_lease_count,
            }

            portfolio_snapshot = self.load_run_display_snapshot_from_sharepoint_list(run_id=run_id, scope_type='portfolio')
            property_snapshots = self.load_run_display_snapshots_for_run(run_id=run_id, scope_type='property')
            lease_snapshots = self.load_run_display_snapshots_for_run(run_id=run_id, scope_type='lease')

            validation['actual'] = {
                'portfolio': 1 if portfolio_snapshot else 0,
                'property': len(property_snapshots),
                'lease': len(lease_snapshots),
            }

            if validation['actual']['portfolio'] != validation['expected']['portfolio']:
                validation['errors'].append('Portfolio snapshot missing')
            if validation['actual']['property'] != validation['expected']['property']:
                validation['errors'].append(
                    f"Property snapshot count mismatch (expected={validation['expected']['property']}, "
                    f"actual={validation['actual']['property']})"
                )
            if validation['actual']['lease'] != validation['expected']['lease']:
                validation['errors'].append(
                    f"Lease snapshot count mismatch (expected={validation['expected']['lease']}, "
                    f"actual={validation['actual']['lease']})"
                )

            validation['ok'] = len(validation['errors']) == 0
            return validation
        except Exception as e:
            validation['errors'].append(f"Snapshot validation error: {e}")
            return validation

        try:
            site_id = self._get_site_id()
            if not site_id:
                return {}

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                return {}

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$filter': (
                    f"fields/RunId eq '{run_id}' and "
                    f"fields/ScopeType eq '{scope_type}' and "
                    f"fields/PropertyId eq {int(property_id)}"
                ),
                '$top': 5000
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading RunDisplaySnapshots list rows for property {property_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return {}

            snapshot_map: Dict[int, Dict[str, Any]] = {}
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                lease_interval_id = self._safe_int(fields.get('LeaseIntervalId'))
                if lease_interval_id is None:
                    continue

                exception_count = fields.get('ExceptionCountStatic')
                if exception_count is None:
                    exception_count = fields.get('ExceptionCountStatistic')

                snapshot_map[lease_interval_id] = {
                    'snapshot_key': fields.get('SnapshotKey'),
                    'run_id': fields.get('RunId', run_id),
                    'scope_type': fields.get('ScopeType', scope_type),
                    'property_id': self._safe_int(fields.get('PropertyId')),
                    'lease_interval_id': lease_interval_id,
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': float(fields.get('UnderchargeStatic') or 0),
                    'overcharge': float(fields.get('OverchargeStatic') or 0),
                    'match_rate': float(fields.get('MatchRateStatic') or 0),
                    'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                    'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
                }

            logger.info(
                f"[STORAGE] ✅ Loaded {len(snapshot_map)} RunDisplaySnapshots rows for run={run_id}, "
                f"property_id={property_id}, scope={scope_type}"
            )
            return snapshot_map
        except Exception as e:
            logger.error(
                f"[STORAGE] Error loading RunDisplaySnapshots rows for property {property_id}: {e}",
                exc_info=True
            )
            return {}

    def _write_results_to_sharepoint_list(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
        target_list_name: Optional[str] = None,
    ) -> bool:
        """Persist bucket results and findings to SharePoint list 'AuditRuns2'."""
        logger.info(f"[STORAGE] _write_results_to_sharepoint_list called: run_id={run_id}, target_list_name={target_list_name}, self.audit_results_list_name={self.audit_results_list_name}")

        # Detailed result writes can run long enough that a request-scoped token is
        # missing or stale by the time SharePoint list calls start. Always prefer a
        # fresh app-only token for this path so writes are consistent with snapshot
        # and metrics persistence.
        try:
            fresh_token = _get_app_only_token()
            if fresh_token:
                self.access_token = fresh_token
        except Exception as token_error:
            logger.warning(f"[STORAGE] Failed to refresh token before result write for {run_id}: {token_error}")
        
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint lists unavailable; skipping AuditRuns2 write")
            return False

        # Hard-lock results persistence to AuditRuns2 only.
        if target_list_name and str(target_list_name).strip().lower() != 'auditruns2':
            logger.warning(
                f"[STORAGE] Ignoring target_list_name='{target_list_name}' and forcing AuditRuns2"
            )
        target_list_name = 'AuditRuns2'
        logger.info(f"[STORAGE] Audit results target for run {run_id}: {target_list_name}")

        try:
            site_id = self._get_site_id()
            if not site_id:
                return False

            list_id = self._get_audit_results_list_id() or self._get_sharepoint_list_id(target_list_name)

            if not list_id:
                logger.warning(
                    f"[STORAGE] Audit results list '{target_list_name}' not found; "
                    "skipping list-backed result persistence"
                )
                return False
            
            print(f"[AUDITRUNS2_DEBUG] Writing to list_id: {list_id} (target: {target_list_name})")

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            property_name_field = None
            resident_name_field = None
            target_list_name = 'AuditRuns2'
            audit_field_name_map: Dict[str, str] = {}
            uses_generic_field_names: bool = False
            schema_loaded_ok = False
            try:
                columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
                columns_params = {'$select': 'name,displayName', '$top': 200}
                columns_response = requests.get(columns_url, headers=headers, params=columns_params, timeout=60)
                if columns_response.status_code == 200:
                    column_defs = columns_response.json().get('value', [])
                    column_name_by_display: Dict[str, str] = {}
                    column_names = set()
                    for column in column_defs:
                        internal_name = column.get('name')
                        display_name = column.get('displayName')
                        if internal_name:
                            column_names.add(internal_name)
                        if internal_name and display_name:
                            column_name_by_display[display_name] = internal_name

                    logical_fields = [
                        'Title', 'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId',
                        'ArCodeId', 'AuditMonth', 'Status', 'Severity', 'FindingTitle',
                        'Variance', 'ExpectedTotal', 'ActualTotal', 'ImpactAmount',
                        'MatchRule', 'FindingId', 'Category', 'Description',
                        'ExpectedValue', 'ActualValue', 'CreatedAt',
                        'PropertyName', 'ResidentName',
                    ]
                    for logical_name in logical_fields:
                        if logical_name in column_names:
                            audit_field_name_map[logical_name] = logical_name
                        elif logical_name in column_name_by_display:
                            audit_field_name_map[logical_name] = column_name_by_display[logical_name]

                    if 'PropertyName' in audit_field_name_map:
                        property_name_field = audit_field_name_map['PropertyName']

                    if 'ResidentName' in audit_field_name_map:
                        resident_name_field = audit_field_name_map['ResidentName']

                    # Detect generic field_* column names (text-typed columns created by SharePoint).
                    # In that case all numeric values must be coerced to strings on write.
                    uses_generic_field_names = any(
                        v.startswith('field_') for v in audit_field_name_map.values()
                    )
                    if uses_generic_field_names:
                        logger.info(
                            f"[STORAGE] {target_list_name} uses generic field_* column names; "
                            "numeric values will be coerced to strings"
                        )

                    # Fail fast when target list schema does not match required result fields.
                    required_columns = {
                        'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId',
                        'ArCodeId', 'AuditMonth', 'Status', 'Variance',
                        'ExpectedTotal', 'ActualTotal',
                    }
                    missing_required = sorted(name for name in required_columns if name not in audit_field_name_map)
                    if missing_required:
                        logger.warning(
                            f"[STORAGE] Skipping writes to '{target_list_name}' for run {run_id}: "
                            f"missing required SharePoint columns: {', '.join(missing_required)}"
                        )
                        print(f"[AUDITRUNS2_DEBUG] SCHEMA VALIDATION FAILED - Missing columns: {missing_required}")
                        return False
                    schema_loaded_ok = True
                    print(f"[AUDITRUNS2_DEBUG] Schema loaded OK - Found {len(audit_field_name_map)} field mappings")
                    print(f"[AUDITRUNS2_DEBUG] Field mappings: {audit_field_name_map}")
                else:
                    logger.warning(
                        f"[STORAGE] Could not read {target_list_name} columns: "
                        f"{columns_response.status_code} - {columns_response.text}"
                    )
            except Exception as schema_exc:
                logger.warning(f"[STORAGE] Failed loading {target_list_name} optional column names: {schema_exc}")

            # Never create list items when schema detection failed. Continuing here can
            # produce blank rows with an empty fields payload.
            if not schema_loaded_ok:
                logger.warning(
                    f"[STORAGE] Skipping writes to '{target_list_name}' for run {run_id}: "
                    "schema detection failed"
                )
                print(f"[AUDITRUNS2_DEBUG] ABORTING WRITE - schema_loaded_ok={schema_loaded_ok}")
                return False
            
            print(f"[AUDITRUNS2_DEBUG] Schema validation passed, proceeding with writes")

            def _normalize_person_name(value: Any) -> str:
                if value is None:
                    return ''
                text = str(value).strip()
                if not text or text.lower() == 'nan':
                    return ''
                return text

            def _row_is_guarantor_like(row: pd.Series) -> bool:
                customer_value = _normalize_person_name(
                    row.get('CUSTOMER_NAME', row.get('customer_name'))
                )
                guarantor_value = _normalize_person_name(
                    row.get('GUARANTOR_NAME', row.get('guarantor_name'))
                )
                if not customer_value or not guarantor_value:
                    return False
                return customer_value.casefold() == guarantor_value.casefold()

            def _build_property_name_lookup(*frames: Optional[pd.DataFrame]) -> Dict[int, str]:
                lookup: Dict[int, str] = {}
                for frame in frames:
                    if frame is None or frame.empty:
                        continue

                    property_col = None
                    for candidate in ['PROPERTY_ID', 'property_id']:
                        if candidate in frame.columns:
                            property_col = candidate
                            break
                    if not property_col:
                        continue

                    property_name_col = None
                    for candidate in ['PROPERTY_NAME', 'property_name', 'PropertyName']:
                        if candidate in frame.columns:
                            property_name_col = candidate
                            break
                    if not property_name_col:
                        continue

                    for _, record in frame[[property_col, property_name_col]].dropna().iterrows():
                        property_id_int = self._safe_int(record.get(property_col))
                        property_name_value = str(record.get(property_name_col)).strip()
                        if property_id_int is None or not property_name_value or property_name_value.lower() == 'nan':
                            continue
                        if property_id_int not in lookup:
                            lookup[property_id_int] = property_name_value

                return lookup

            def _build_resident_name_lookup(*frames: Optional[pd.DataFrame]) -> Dict[int, str]:
                lookup: Dict[int, str] = {}
                for frame in frames:
                    if frame is None or frame.empty:
                        continue

                    lease_col = None
                    for candidate in ['LEASE_INTERVAL_ID', 'lease_interval_id']:
                        if candidate in frame.columns:
                            lease_col = candidate
                            break
                    if not lease_col:
                        continue

                    customer_col = None
                    for candidate in ['CUSTOMER_NAME', 'customer_name']:
                        if candidate in frame.columns:
                            customer_col = candidate
                            break
                    if not customer_col:
                        continue

                    non_guarantor_candidates: Dict[int, List[str]] = {}
                    fallback_candidates: Dict[int, List[str]] = {}

                    for _, record in frame.iterrows():
                        lease_id_int = self._safe_int(record.get(lease_col))
                        if lease_id_int is None:
                            continue

                        customer_value = _normalize_person_name(record.get(customer_col))
                        if not customer_value:
                            continue

                        fallback_candidates.setdefault(lease_id_int, []).append(customer_value)
                        if not _row_is_guarantor_like(record):
                            non_guarantor_candidates.setdefault(lease_id_int, []).append(customer_value)

                    for lease_id_int, fallback_values in fallback_candidates.items():
                        if lease_id_int in lookup:
                            continue

                        preferred = non_guarantor_candidates.get(lease_id_int) or fallback_values
                        if not preferred:
                            continue

                        counts: Dict[str, int] = {}
                        first_seen: Dict[str, int] = {}
                        for idx_name, name_value in enumerate(preferred):
                            key = name_value.casefold()
                            counts[key] = counts.get(key, 0) + 1
                            if key not in first_seen:
                                first_seen[key] = idx_name

                        winner_key = max(counts.keys(), key=lambda key: (counts[key], -first_seen[key]))
                        for name_value in preferred:
                            if name_value.casefold() == winner_key:
                                lookup[lease_id_int] = name_value
                                break

                return lookup

            property_name_lookup = _build_property_name_lookup(actual_detail, expected_detail)
            resident_name_lookup = _build_resident_name_lookup(expected_detail, actual_detail)

            def _write_dataframe_rows(df: pd.DataFrame, result_type: str) -> int:
                rows_written = 0
                if df is None or len(df) == 0:
                    return rows_written

                row_payloads: List[Dict[str, Any]] = []

                for idx, (_, row) in enumerate(df.iterrows()):
                    row_dict = {col: self._normalize_for_json(value) for col, value in row.to_dict().items()}

                    property_id_val = row_dict.get('PROPERTY_ID', row_dict.get('property_id'))
                    lease_interval_id_val = row_dict.get('LEASE_INTERVAL_ID', row_dict.get('lease_interval_id'))
                    ar_code_id_val = row_dict.get('AR_CODE_ID', row_dict.get('ar_code_id'))
                    audit_month_val = self._normalize_audit_month_value(row_dict.get('AUDIT_MONTH', row_dict.get('audit_month')))

                    status_val = row_dict.get('status', row_dict.get('STATUS', ''))
                    severity_val = row_dict.get('severity', row_dict.get('SEVERITY', ''))
                    variance_val = row_dict.get('variance', row_dict.get('VARIANCE', 0))
                    expected_total_val = row_dict.get('expected_total', row_dict.get('EXPECTED_TOTAL', 0))
                    actual_total_val = row_dict.get('actual_total', row_dict.get('ACTUAL_TOTAL', 0))
                    finding_title_val = row_dict.get('title', row_dict.get('TITLE', ''))
                    impact_amount_val = row_dict.get('impact_amount', row_dict.get('IMPACT_AMOUNT', 0))

                    property_id_int = self._safe_int(property_id_val)
                    lease_interval_id_int = self._safe_int(lease_interval_id_val)

                    canonical_fields_payload = {
                        'Title': f"{result_type}:{idx}",
                        'RunId': run_id,
                        'ResultType': result_type,
                        'PropertyId': property_id_int,
                        'LeaseIntervalId': lease_interval_id_int,
                        'ArCodeId': str(ar_code_id_val) if ar_code_id_val is not None else '',
                        'AuditMonth': audit_month_val,
                        'Status': str(status_val),
                        'Severity': str(severity_val),
                        'FindingTitle': str(finding_title_val),
                        'Variance': float(variance_val or 0),
                        'ExpectedTotal': float(expected_total_val or 0),
                        'ActualTotal': float(actual_total_val or 0),
                        'ImpactAmount': float(impact_amount_val or 0),
                        'MatchRule': str(row_dict.get('match_rule', row_dict.get('MATCH_RULE', ''))),
                        'FindingId': str(row_dict.get('finding_id', row_dict.get('FINDING_ID', ''))),
                        'Category': str(row_dict.get('category', row_dict.get('CATEGORY', ''))),
                        'Description': str(row_dict.get('description', row_dict.get('DESCRIPTION', ''))),
                        'ExpectedValue': str(row_dict.get('expected_value', row_dict.get('EXPECTED_VALUE', ''))),
                        'ActualValue': str(row_dict.get('actual_value', row_dict.get('ACTUAL_VALUE', ''))),
                        'CreatedAt': datetime.utcnow().isoformat(),
                    }

                    # When list uses generic field_* column names (text type), coerce numeric values to strings.
                    if uses_generic_field_names:
                        canonical_fields_payload = {
                            k: (str(v) if isinstance(v, (int, float)) and v is not None else v)
                            for k, v in canonical_fields_payload.items()
                        }

                    fields_payload: Dict[str, Any] = {}
                    for logical_name, value in canonical_fields_payload.items():
                        internal_name = audit_field_name_map.get(logical_name)
                        if internal_name:
                            fields_payload[internal_name] = value

                    if property_name_field and property_id_int is not None:
                        property_name_value = property_name_lookup.get(property_id_int)
                        if property_name_value:
                            fields_payload[property_name_field] = property_name_value

                    if resident_name_field and lease_interval_id_int is not None:
                        resident_name_value = resident_name_lookup.get(lease_interval_id_int)
                        if resident_name_value:
                            fields_payload[resident_name_field] = resident_name_value

                    # Debug: log first payload to see what we're sending
                    if idx == 0:
                        print(f"[AUDITRUNS2_DEBUG] Sample fields_payload for {result_type}: {fields_payload}")
                    
                    row_payloads.append({'fields': fields_payload})

                print(f"[AUDITRUNS2_DEBUG] About to call _post_list_rows_in_batches with {len(row_payloads)} payloads")
                rows_written += self._post_list_rows_in_batches(
                    site_id=site_id,
                    list_id=list_id,
                    row_payloads=row_payloads,
                    context_label=f"{target_list_name} {result_type} run={run_id}",
                )
                print(f"[AUDITRUNS2_DEBUG] _post_list_rows_in_batches returned: {rows_written} rows written")

                return rows_written

            rows_written_by_type = {
                'bucket_result': 0,
                'finding': 0,
            }

            # Optional: Filter out matched buckets to reduce list size
            filtered_bucket_results = bucket_results
            write_exceptions_only = os.getenv('SHAREPOINT_WRITE_EXCEPTIONS_ONLY', 'false').lower() == 'true'
            if write_exceptions_only:
                original_count = len(bucket_results)
                # Only write exceptions (Status != "Matched")
                # Handle missing Status column or NaN values gracefully
                if 'Status' in bucket_results.columns:
                    filtered_bucket_results = bucket_results[
                        (bucket_results['Status'].isna()) | 
                        (bucket_results['Status'].str.upper() != 'MATCHED')
                    ].copy()
                    exceptions_count = len(filtered_bucket_results)
                    matched_skipped = original_count - exceptions_count
                    logger.info(
                        f"[STORAGE] Exception-only mode: writing {exceptions_count} exceptions, "
                        f"skipping {matched_skipped} matched rows ({matched_skipped/original_count*100:.1f}% reduction)"
                    )
                else:
                    logger.warning("[STORAGE] Exception-only mode enabled but Status column not found; writing all rows")

            write_targets = [
                ('bucket_result', filtered_bucket_results),
                ('finding', findings),
            ]

            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_result_type = {
                    executor.submit(_write_dataframe_rows, dataframe, result_type): result_type
                    for result_type, dataframe in write_targets
                }
                for future, result_type in future_to_result_type.items():
                    rows_written_by_type[result_type] = future.result()

            bucket_rows_written = rows_written_by_type['bucket_result']
            finding_rows_written = rows_written_by_type['finding']
            logger.info(
                f"[STORAGE] Wrote {target_list_name} rows for {run_id}: "
                f"bucket_result={bucket_rows_written}, finding={finding_rows_written}"
            )
            print(
                f"[AUDITRUNS2_WRITE] [OK] Wrote {bucket_rows_written} bucket_result rows "
                f"and {finding_rows_written} finding rows to {target_list_name} for {run_id}"
            )
            return True
        except Exception as e:
            logger.error(f"[STORAGE] Error writing audit results list rows: {e}", exc_info=True)
            return False

    def _load_results_from_sharepoint_list(
        self,
        run_id: str,
        result_type: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Load result rows for a run/type from in-memory cache or CSV fallback."""
        
        # Check in-memory cache first
        with self._IN_MEMORY_CACHE_LOCK:
            if run_id in self._IN_MEMORY_RESULTS_CACHE:
                cache_data = self._IN_MEMORY_RESULTS_CACHE[run_id]
                
                if result_type == 'bucket_result' and 'bucket_results' in cache_data:
                    logger.info(f"[STORAGE] ✅ Found bucket_results in memory cache for run {run_id}")
                    df = cache_data['bucket_results'].copy()
                    
                    # Apply filters if specified
                    if property_id is not None:
                        from audit_engine.canonical_fields import CanonicalField
                        prop_col = CanonicalField.PROPERTY_ID.value
                        if prop_col in df.columns:
                            df = df[df[prop_col] == property_id].copy()
                    
                    if lease_interval_id is not None:
                        from audit_engine.canonical_fields import CanonicalField
                        lease_col = CanonicalField.LEASE_INTERVAL_ID.value
                        if lease_col in df.columns:
                            df = df[df[lease_col] == lease_interval_id].copy()
                    
                    return df
                
                elif result_type == 'finding' and 'findings' in cache_data:
                    logger.info(f"[STORAGE] ✅ Found findings in memory cache for run {run_id}")
                    df = cache_data['findings'].copy()
                    
                    # Apply filters if specified
                    if property_id is not None:
                        from audit_engine.canonical_fields import CanonicalField
                        prop_col = CanonicalField.PROPERTY_ID.value
                        if prop_col in df.columns:
                            df = df[df[prop_col] == property_id].copy()
                    
                    if lease_interval_id is not None:
                        from audit_engine.canonical_fields import CanonicalField
                        lease_col = CanonicalField.LEASE_INTERVAL_ID.value
                        if lease_col in df.columns:
                            df = df[df[lease_col] == lease_interval_id].copy()
                    
                    return df
        
        # Not in memory cache - try SharePoint AuditRuns2 list
        logger.info(
            f"[STORAGE] Results not in memory cache for run={run_id}, type={result_type}. "
            "Attempting to load from SharePoint AuditRuns2 list..."
        )
        
        logger.info(f"[AUDITRUNS2_DEBUG] Starting _load_results_from_sharepoint_list for run={run_id}, type={result_type}")
        
        if not self._can_use_sharepoint_lists():
            logger.warning(f"[AUDITRUNS2_DEBUG] Cannot use SharePoint lists - _can_use_sharepoint_lists() returned False")
            return None

        try:
            site_id = self._get_site_id()
            if not site_id:
                logger.warning(f"[AUDITRUNS2_DEBUG] Site ID not found - _get_site_id() returned None")
                return None
            logger.info(f"[AUDITRUNS2_DEBUG] Site ID resolved: {site_id}")

            list_id = self._get_audit_results_list_id()
            if not list_id:
                logger.warning(f"[AUDITRUNS2_DEBUG] List ID not found - _get_audit_results_list_id() returned None")
                return None
            logger.info(f"[AUDITRUNS2_DEBUG] AuditRuns2 list ID resolved: {list_id}")

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            # Resolve logical field names to the list's real internal names.
            # This supports both canonical columns (RunId/PropertyId/...) and
            # SharePoint generic schemas (field_1/field_2/...).
            field_name_map: Dict[str, str] = {}
            uses_generic_field_names = False
            try:
                columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
                columns_params = {'$select': 'name,displayName', '$top': 200}
                logger.info(f"[AUDITRUNS2_DEBUG] Querying columns schema from: {columns_url}")
                columns_response = requests.get(columns_url, headers=headers, params=columns_params, timeout=60)
                logger.info(f"[AUDITRUNS2_DEBUG] Columns query status: {columns_response.status_code}")
                
                if columns_response.status_code == 200:
                    column_defs = columns_response.json().get('value', [])
                    logger.info(f"[AUDITRUNS2_DEBUG] Found {len(column_defs)} columns in AuditRuns2 list")
                    
                    column_name_by_display: Dict[str, str] = {}
                    column_names = set()
                    for column in column_defs:
                        internal_name = column.get('name')
                        display_name = column.get('displayName')
                        if internal_name:
                            column_names.add(internal_name)
                        if internal_name and display_name:
                            column_name_by_display[display_name] = internal_name

                    logical_fields = [
                        'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId',
                        'ArCodeId', 'AuditMonth', 'Status', 'Severity', 'FindingTitle',
                        'Variance', 'ExpectedTotal', 'ActualTotal', 'ImpactAmount',
                        'MatchRule', 'FindingId', 'Category', 'Description',
                        'ExpectedValue', 'ActualValue', 'PropertyName', 'ResidentName',
                        'RowJson',
                    ]
                    for logical_name in logical_fields:
                        if logical_name in column_names:
                            field_name_map[logical_name] = logical_name
                        elif logical_name in column_name_by_display:
                            field_name_map[logical_name] = column_name_by_display[logical_name]

                    logger.info(f"[AUDITRUNS2_DEBUG] Field name map created with {len(field_name_map)} mappings")
                    logger.info(f"[AUDITRUNS2_DEBUG] Key fields - RunId: {field_name_map.get('RunId')}, ResultType: {field_name_map.get('ResultType')}")
                    
                    uses_generic_field_names = any(
                        v.startswith('field_') for v in field_name_map.values()
                    )
                else:
                    logger.warning(
                        f"[STORAGE] Could not read audit result columns for list id {list_id}: "
                        f"{columns_response.status_code} - {columns_response.text}"
                    )
                    return None
            except Exception as schema_exc:
                logger.warning(f"[STORAGE] Failed loading audit result column names: {schema_exc}")
                return None

            run_id_field = field_name_map.get('RunId')
            result_type_field = field_name_map.get('ResultType')
            if not run_id_field or not result_type_field:
                logger.warning(
                    f"[AUDITRUNS2_DEBUG] Missing required fields! RunId field: {run_id_field}, ResultType field: {result_type_field}"
                )
                logger.warning(
                    "[STORAGE] Cannot query audit results list: missing RunId/ResultType column mapping"
                )
                return None

            filters = [
                f"fields/{run_id_field} eq '{run_id}'",
                f"fields/{result_type_field} eq '{result_type}'",
            ]
            if property_id is not None:
                property_id_field = field_name_map.get('PropertyId')
                if property_id_field:
                    if uses_generic_field_names:
                        filters.append(f"fields/{property_id_field} eq '{int(property_id)}'")
                    else:
                        filters.append(f"fields/{property_id_field} eq {int(property_id)}")
            if lease_interval_id is not None:
                lease_interval_id_field = field_name_map.get('LeaseIntervalId')
                if lease_interval_id_field:
                    if uses_generic_field_names:
                        filters.append(f"fields/{lease_interval_id_field} eq '{int(lease_interval_id)}'")
                    else:
                        filters.append(f"fields/{lease_interval_id_field} eq {int(lease_interval_id)}")

            params = {
                '$expand': 'fields',
                '$filter': ' and '.join(filters),
                '$top': 5000
            }

            logger.info(f"[AUDITRUNS2_DEBUG] Executing query with filters: {params['$filter']}")
            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            logger.info(f"[AUDITRUNS2_DEBUG] Query response status: {response.status_code}")
            if response.status_code != 200:
                logger.warning(
                    f"[AUDITRUNS2_DEBUG] Query failed with error: {response.text[:500]}"
                )
                logger.warning(
                    f"[STORAGE] Failed loading audit results for run={run_id}, type={result_type}: "
                    f"{response.status_code} - {response.text}"
                )
                return None

            items = response.json().get('value', [])
            logger.info(f"[AUDITRUNS2_DEBUG] Query returned {len(items)} items")
            
            if not items:
                logger.info(f"[AUDITRUNS2_DEBUG] No items found - returning None (will fallback to CSV)")
                return None

            rows: List[Dict[str, Any]] = []
            for item in items:
                fields = item.get('fields', {})

                def _field(logical_name: str, default: Any = None) -> Any:
                    internal_name = field_name_map.get(logical_name, logical_name)
                    return fields.get(internal_name, default)

                if result_type == 'bucket_result':
                    row_payload = {
                        'PROPERTY_ID': _field('PropertyId'),
                        'LEASE_INTERVAL_ID': _field('LeaseIntervalId'),
                        'property_name': _field('PropertyName'),
                        'resident_name': _field('ResidentName'),
                        'AR_CODE_ID': _field('ArCodeId'),
                        'AUDIT_MONTH': _field('AuditMonth'),
                        'expected_total': _field('ExpectedTotal'),
                        'actual_total': _field('ActualTotal'),
                        'variance': _field('Variance'),
                        'status': _field('Status'),
                        'match_rule': _field('MatchRule')
                    }

                    # Legacy compatibility: recover full row from RowJson if explicit fields are missing.
                    row_json_value = _field('RowJson')
                    if row_payload.get('status') in [None, ''] and row_json_value:
                        try:
                            legacy = json.loads(row_json_value)
                            for key, value in legacy.items():
                                row_payload[key] = value
                        except Exception:
                            pass

                    rows.append(row_payload)
                elif result_type == 'finding':
                    row_payload = {
                        'finding_id': _field('FindingId'),
                        'run_id': _field('RunId', run_id),
                        'property_id': _field('PropertyId'),
                        'lease_interval_id': _field('LeaseIntervalId'),
                        'property_name': _field('PropertyName'),
                        'resident_name': _field('ResidentName'),
                        'ar_code_id': _field('ArCodeId'),
                        'audit_month': _field('AuditMonth'),
                        'category': _field('Category'),
                        'severity': _field('Severity'),
                        'title': _field('FindingTitle'),
                        'description': _field('Description'),
                        'expected_value': _field('ExpectedValue'),
                        'actual_value': _field('ActualValue'),
                        'variance': _field('Variance'),
                        'impact_amount': _field('ImpactAmount')
                    }

                    # Legacy compatibility: recover fields from RowJson if present.
                    row_json_value = _field('RowJson')
                    if row_payload.get('title') in [None, ''] and row_json_value:
                        try:
                            legacy = json.loads(row_json_value)
                            for key, value in legacy.items():
                                row_payload[key] = value
                        except Exception:
                            pass

                    rows.append(row_payload)
                else:
                    rows.append({
                        'RunId': _field('RunId', run_id),
                        'ResultType': _field('ResultType', result_type)
                    })

            logger.info(
                f"[STORAGE] ✅ Loaded audit results from list for run={run_id}, "
                f"type={result_type}, rows={len(rows)}"
            )
            return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"[STORAGE] Error loading audit results list rows: {e}", exc_info=True)
            return None

    def load_bucket_results(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load bucket results from RunDisplaySnapshots exception-level with CSV fallback."""
        scope = f"run={run_id}, property_id={property_id}, lease_interval_id={lease_interval_id}"
        
        # Load from RunDisplaySnapshots exception-level snapshots (primary source)
        snapshot_results = self.load_exception_snapshots_as_bucket_results(run_id, property_id, lease_interval_id)
        if snapshot_results is not None and len(snapshot_results) > 0:
            snapshot_results = self._normalize_loaded_dataframe(snapshot_results)
            snapshot_results.attrs['read_source'] = 'exception_snapshots'
            snapshot_results.attrs['read_reason'] = 'primary_source'
            snapshot_results.attrs['read_scope'] = scope
            logger.info(
                f"[READ SOURCE][bucket_results] source=exception_snapshots reason=primary_source scope=({scope}) rows={len(snapshot_results)}"
            )
            return snapshot_results
        
        # Fallback to CSV
        csv_results = self._load_dataframe(run_id, "outputs/bucket_results.csv")
        if csv_results is not None and len(csv_results) > 0:
            csv_results = self._normalize_loaded_dataframe(csv_results)
            csv_results.attrs['read_source'] = 'csv'
            csv_results.attrs['read_reason'] = 'snapshots_unavailable'
            csv_results.attrs['read_scope'] = scope
            logger.info(
                f"[READ SOURCE][bucket_results] source=csv reason=snapshots_unavailable scope=({scope}) rows={len(csv_results)}"
            )
            return csv_results
        
        # No data found
        empty_results = pd.DataFrame()
        empty_results.attrs['read_source'] = 'none'
        empty_results.attrs['read_reason'] = 'not_found'
        empty_results.attrs['read_scope'] = scope
        logger.warning(
            f"[READ SOURCE][bucket_results] source=none reason=not_found scope=({scope})"
        )
        return empty_results

    def _load_bucket_results_from_snapshots(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Reconstruct bucket_results DataFrame from RunDisplaySnapshots month-level rows."""
        try:
            # Query for month-level snapshots (most detailed level with bucket data)
            filter_parts = [f"fields/RunId eq '{run_id}'", "fields/ScopeType eq 'month'"]
            if property_id is not None:
                filter_parts.append(f"fields/PropertyId eq {property_id}")
            if lease_interval_id is not None:
                filter_parts.append(f"fields/LeaseIntervalId eq {lease_interval_id}")
            
            filter_str = " and ".join(filter_parts)
            
            # Get snapshots
            snapshots = self._query_snapshots_by_filter(run_id, filter_str)
            if not snapshots:
                return None
            
            # Reconstruct bucket_results DataFrame from snapshots
            rows = []
            for snap in snapshots:
                row = {
                    'PROPERTY_ID': snap.get('PropertyId'),
                    'LEASE_INTERVAL_ID': snap.get('LeaseIntervalId'),
                    'AR_CODE_ID': snap.get('ArCodeId'),
                    'AUDIT_MONTH': snap.get('AuditMonth'),
                    'STATUS': snap.get('Status', ''),
                    'AR_CODE_NAME': snap.get('ArCodeName', ''),
                    'EXPECTED_TOTAL': snap.get('ExpectedTotal', 0.0),
                    'ACTUAL_TOTAL': snap.get('ActualTotal', 0.0),
                    'VARIANCE': snap.get('Variance', 0.0),
                    'CUSTOMER_NAME': snap.get('ResidentName', ''),  # Use canonical field name
                    'LEASE_ID': snap.get('LeaseId'),
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            return df if len(df) > 0 else None
            
        except Exception as e:
            logger.error(f"[STORAGE] Failed to load bucket_results from snapshots: {e}")
            return None
    
    def _load_findings_from_snapshots(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Reconstruct findings DataFrame from RunDisplaySnapshots month-level rows with variance != 0."""
        try:
            # Query for month-level snapshots with variance (findings)
            filter_parts = [f"fields/RunId eq '{run_id}'", "fields/ScopeType eq 'month'"]
            if property_id is not None:
                filter_parts.append(f"fields/PropertyId eq {property_id}")
            if lease_interval_id is not None:
                filter_parts.append(f"fields/LeaseIntervalId eq {lease_interval_id}")
            
            filter_str = " and ".join(filter_parts)
            
            # Get snapshots
            snapshots = self._query_snapshots_by_filter(run_id, filter_str)
            if not snapshots:
                return None
            
            # Filter to only rows with variance != 0 (findings)
            findings_snapshots = [s for s in snapshots if s.get('Variance', 0.0) != 0.0]
            if not findings_snapshots:
                return None
            
            # Reconstruct findings DataFrame from snapshots
            rows = []
            for snap in findings_snapshots:
                variance = snap.get('Variance', 0.0)
                row = {
                    'property_id': snap.get('PropertyId'),
                    'lease_interval_id': snap.get('LeaseIntervalId'),
                    'ar_code_id': snap.get('ArCodeId'),
                    'audit_month': snap.get('AuditMonth'),
                    'status': snap.get('Status', ''),
                    'ar_code_name': snap.get('ArCodeName', ''),
                    'variance': variance,
                    'severity': 'overcharge' if variance > 0 else 'undercharge',
                    'expected': snap.get('ExpectedTotal', 0.0),
                    'actual': snap.get('ActualTotal', 0.0),
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            return df if len(df) > 0 else None
            
        except Exception as e:
            logger.error(f"[STORAGE] Failed to load findings from snapshots: {e}")
            return None
    
    def _query_snapshots_by_filter(
        self,
        run_id: str,
        filter_str: str,
        select_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Query RunDisplaySnapshots with OData filter."""
        try:
            list_id = self._get_sharepoint_list_id('RunDisplaySnapshots')
            if not list_id:
                return []
            
            site_id = self._get_site_id()
            if not site_id:
                return []
            
            token = self.access_token
            if not token:
                return []
            
            # Build query URL
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$filter': filter_str,
                '$expand': 'fields',
                '$top': 5000,
            }
            if select_fields:
                params['$select'] = ','.join(['id'] + select_fields)
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            r = requests.get(url, headers=headers, params=params, timeout=60)
            
            if r.status_code != 200:
                logger.warning(f"[STORAGE] Failed querying RunDisplaySnapshots: {r.status_code} - {r.text}")
                return []
            
            items = r.json().get('value', [])
            logger.info(f"[STORAGE][QUERY DEBUG] filter='{filter_str}' returned {len(items)} items")
            if len(items) == 0:
                logger.warning(f"[STORAGE][QUERY DEBUG] Zero results for filter: {filter_str}")
                # Debug: try querying without ScopeType filter to see what exists
                if 'ScopeType' in filter_str:
                    try:
                        debug_filter = filter_str.split(' and fields/ScopeType')[0]  # Remove ScopeType filter
                        debug_params = {'$filter': debug_filter, '$expand': 'fields', '$top': 10}
                        debug_r = requests.get(url, headers=headers, params=debug_params, timeout=60)
                        if debug_r.status_code == 200:
                            debug_items = debug_r.json().get('value', [])
                            if debug_items:
                                scope_types = [item.get('fields', {}).get('ScopeType') for item in debug_items[:5]]
                                logger.warning(f"[STORAGE][QUERY DEBUG] Found {len(debug_items)} items without ScopeType filter. Sample ScopeType values: {scope_types}")
                            else:
                                logger.warning(f"[STORAGE][QUERY DEBUG] Zero items even without ScopeType filter - run may not exist")
                    except Exception as e:
                        logger.warning(f"[STORAGE][QUERY DEBUG] Debug query failed: {e}")
            return [item.get('fields', {}) for item in items]
            
        except Exception as e:
            logger.error(f"[STORAGE] Error querying snapshots: {e}")
            return []

    def load_findings(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load findings from RunDisplaySnapshots exception-level with CSV fallback."""
        scope = f"run={run_id}, property_id={property_id}, lease_interval_id={lease_interval_id}"
        
        # Load from RunDisplaySnapshots exception-level snapshots (findings = rows with variance)
        snapshot_results = self.load_exception_snapshots_as_bucket_results(run_id, property_id, lease_interval_id)
        if snapshot_results is not None and len(snapshot_results) > 0:
            # Filter for findings (variance != 0)
            from audit_engine.canonical_fields import CanonicalField
            variance_col = CanonicalField.VARIANCE.value
            if variance_col in snapshot_results.columns:
                snapshot_findings = snapshot_results[snapshot_results[variance_col] != 0].copy()
            else:
                snapshot_findings = snapshot_results.copy()
            
            if len(snapshot_findings) > 0:
                snapshot_findings = self._normalize_loaded_dataframe(snapshot_findings)
                snapshot_findings.attrs['read_source'] = 'exception_snapshots'
                snapshot_findings.attrs['read_reason'] = 'primary_source'
                snapshot_findings.attrs['read_scope'] = scope
                logger.info(
                    f"[READ SOURCE][findings] source=exception_snapshots reason=primary_source scope=({scope}) rows={len(snapshot_findings)}"
                )
                return snapshot_findings
        
        # Fallback to CSV
        csv_findings = self._load_dataframe(run_id, "outputs/findings.csv")
        if csv_findings is not None and len(csv_findings) > 0:
            csv_findings = self._normalize_loaded_dataframe(csv_findings)
            csv_findings.attrs['read_source'] = 'csv'
            csv_findings.attrs['read_reason'] = 'snapshots_unavailable'
            csv_findings.attrs['read_scope'] = scope
            logger.info(
                f"[READ SOURCE][findings] source=csv reason=snapshots_unavailable scope=({scope}) rows={len(csv_findings)}"
            )
            return csv_findings
        
        # No data found
        empty_results = pd.DataFrame()
        empty_results.attrs['read_source'] = 'none'
        empty_results.attrs['read_reason'] = 'not_found'
        empty_results.attrs['read_scope'] = scope
        logger.warning(
            f"[READ SOURCE][findings] source=none reason=not_found scope=({scope})"
        )
        return empty_results

    def load_variance_detail(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
        ar_code_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load variance_detail from in-memory cache, optionally filtered by property/lease/AR code."""
        from audit_engine.canonical_fields import CanonicalField
        
        scope = f"run={run_id}, property_id={property_id}, lease_interval_id={lease_interval_id}, ar_code_id={ar_code_id}"
        
        # Load from in-memory cache only
        variance_detail = None
        with self._IN_MEMORY_CACHE_LOCK:
            if run_id in self._IN_MEMORY_RESULTS_CACHE:
                variance_detail = self._IN_MEMORY_RESULTS_CACHE[run_id].get('variance_detail', pd.DataFrame())
                if not variance_detail.empty:
                    variance_detail = variance_detail.copy()
        
        if variance_detail is None or variance_detail.empty:
            empty_results = pd.DataFrame()
            empty_results.attrs['read_source'] = 'none'
            empty_results.attrs['read_reason'] = 'not_found_in_memory'
            empty_results.attrs['read_scope'] = scope
            logger.warning(
                f"[READ SOURCE][variance_detail] source=none reason=not_found_in_memory scope=({scope})"
            )
            return empty_results
        
        # Normalize first
        variance_detail = self._normalize_loaded_dataframe(variance_detail)
        
        # Apply filters if specified
        filtered = variance_detail
        if property_id is not None:
            prop_col = next((c for c in [CanonicalField.PROPERTY_ID.value, 'property_id', 'PROPERTY_ID'] if c in filtered.columns), None)
            if prop_col:
                filtered = filtered[filtered[prop_col] == property_id].copy()
        
        if lease_interval_id is not None:
            lease_col = next((c for c in [CanonicalField.LEASE_INTERVAL_ID.value, 'lease_interval_id', 'LEASE_INTERVAL_ID'] if c in filtered.columns), None)
            if lease_col:
                filtered = filtered[filtered[lease_col] == lease_interval_id].copy()
        
        if ar_code_id is not None:
            ar_col = next((c for c in [CanonicalField.AR_CODE_ID.value, 'ar_code_id', 'AR_CODE_ID'] if c in filtered.columns), None)
            if ar_col:
                filtered = filtered[filtered[ar_col] == ar_code_id].copy()
        
        filtered.attrs['read_source'] = 'memory'
        filtered.attrs['read_reason'] = 'in_memory_cache'
        filtered.attrs['read_scope'] = scope
        logger.info(
            f"[READ SOURCE][variance_detail] source=memory reason=in_memory_cache scope=({scope}) rows={len(filtered)}"
        )
        return filtered

    def load_expected_detail(self, run_id: str) -> pd.DataFrame:
        """Load expected_detail for a run from in-memory cache or CSV."""
        print(f"[STORAGE][DEBUG] load_expected_detail called for run_id={run_id}")
        
        # Try in-memory cache first
        with self._IN_MEMORY_CACHE_LOCK:
            print(f"[STORAGE][DEBUG] Checking in-memory cache, run_id in cache: {run_id in self._IN_MEMORY_RESULTS_CACHE}")
            if run_id in self._IN_MEMORY_RESULTS_CACHE:
                cache_data = self._IN_MEMORY_RESULTS_CACHE[run_id]
                print(f"[STORAGE][DEBUG] Cache data keys: {list(cache_data.keys())}")
                expected_detail = cache_data.get('expected_detail', pd.DataFrame())
                print(f"[STORAGE][DEBUG] expected_detail from cache shape: {expected_detail.shape}")
                if not expected_detail.empty:
                    print(f"[STORAGE][DEBUG] ✓ Returning expected_detail from cache: {expected_detail.shape}")
                    return self._normalize_loaded_dataframe(expected_detail.copy())
                print(f"[STORAGE][DEBUG] ⚠ expected_detail in cache is empty")
        
        # Fall back to CSV file
        print(f"[STORAGE][DEBUG] Falling back to CSV file for expected_detail")
        csv_df = self._load_dataframe(run_id, "inputs_normalized/expected_detail.csv")
        if csv_df is not None and not csv_df.empty:
            print(f"[STORAGE][DEBUG] ✓ Loaded expected_detail from CSV: {csv_df.shape}")
            return self._normalize_loaded_dataframe(csv_df)
        
        logger.warning(f"[STORAGE] Expected detail not found for run {run_id}")
        print(f"[STORAGE][DEBUG] ❌ Returning empty DataFrame for expected_detail")
        return pd.DataFrame()

    def load_actual_detail(self, run_id: str) -> pd.DataFrame:
        """Load actual_detail for a run from in-memory cache or CSV."""
        print(f"[STORAGE][DEBUG] load_actual_detail called for run_id={run_id}")
        
        # Try in-memory cache first
        with self._IN_MEMORY_CACHE_LOCK:
            print(f"[STORAGE][DEBUG] Checking in-memory cache, run_id in cache: {run_id in self._IN_MEMORY_RESULTS_CACHE}")
            if run_id in self._IN_MEMORY_RESULTS_CACHE:
                cache_data = self._IN_MEMORY_RESULTS_CACHE[run_id]
                print(f"[STORAGE][DEBUG] Cache data keys: {list(cache_data.keys())}")
                actual_detail = cache_data.get('actual_detail', pd.DataFrame())
                print(f"[STORAGE][DEBUG] actual_detail from cache shape: {actual_detail.shape}")
                if not actual_detail.empty:
                    print(f"[STORAGE][DEBUG] ✓ Returning actual_detail from cache: {actual_detail.shape}")
                    return self._normalize_loaded_dataframe(actual_detail.copy())
                print(f"[STORAGE][DEBUG] ⚠ actual_detail in cache is empty")
        
        # Fall back to CSV file
        print(f"[STORAGE][DEBUG] Falling back to CSV file for actual_detail")
        csv_df = self._load_dataframe(run_id, "inputs_normalized/actual_detail.csv")
        if csv_df is not None and not csv_df.empty:
            print(f"[STORAGE][DEBUG] ✓ Loaded actual_detail from CSV: {csv_df.shape}")
            return self._normalize_loaded_dataframe(csv_df)
        
        logger.warning(f"[STORAGE] Actual detail not found for run {run_id}")
        print(f"[STORAGE][DEBUG] ❌ Returning empty DataFrame for actual_detail")
        return pd.DataFrame()

    def _normalize_ar_code_value(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ''
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric.is_integer():
                return str(int(numeric))
            return str(numeric)
        text = str(value).strip()
        try:
            numeric = float(text)
            if numeric.is_integer():
                return str(int(numeric))
        except Exception:
            pass
        return text

    def _normalize_loaded_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Normalize loaded data to keep key comparisons stable across data sources."""
        if dataframe is None or dataframe.empty:
            return dataframe

        date_columns = ['AUDIT_MONTH', 'PERIOD_START', 'PERIOD_END', 'POST_DATE', 'audit_month']
        for column_name in date_columns:
            if column_name in dataframe.columns:
                series = pd.to_datetime(dataframe[column_name], errors='coerce')
                try:
                    series = series.dt.tz_localize(None)
                except Exception:
                    pass
                dataframe[column_name] = series

        ar_code_columns = ['AR_CODE_ID', 'ar_code_id']
        for column_name in ar_code_columns:
            if column_name in dataframe.columns:
                dataframe[column_name] = dataframe[column_name].apply(self._normalize_ar_code_value)

        return dataframe

    def upsert_lease_term_set_to_sharepoint_list(self, payload: Dict[str, Any]) -> bool:
        """Upsert LeaseTermSet row by LeaseKey."""
        if not self._can_use_sharepoint_lists():
            return False

        lease_key = str(payload.get('lease_key') or '').strip()
        if not lease_key:
            logger.error("[STORAGE] LeaseTermSet upsert missing lease_key")
            return False

        try:
            site_id = self._get_site_id()
            list_id = self._get_lease_term_set_list_id()
            if not site_id or not list_id:
                return False

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            filter_query = f"fields/LeaseKey eq '{lease_key}'"
            params = {'$expand': 'fields', '$filter': filter_query, '$top': 1}
            response = requests.get(items_url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                logger.error(f"[STORAGE] LeaseTermSet query failed: {response.status_code} - {response.text}")
                return False

            fields_payload = {
                'Title': lease_key,
                'LeaseKey': lease_key,
                'PropertyId': self._safe_int(payload.get('property_id')),
                'LeaseIntervalId': self._safe_int(payload.get('lease_interval_id')),
                'LeaseId': str(payload.get('lease_id') or ''),
                'TermSetVersion': int(payload.get('term_set_version') or 1),
                'FingerprintHash': str(payload.get('fingerprint_hash') or ''),
                'DocListFingerprint': str(payload.get('doc_list_fingerprint') or ''),
                'SelectedDocIds': str(payload.get('selected_doc_ids') or ''),
                'LastCheckedAt': str(payload.get('last_checked_at') or datetime.utcnow().isoformat()),
                'LastRefreshedAt': str(payload.get('last_refreshed_at') or datetime.utcnow().isoformat()),
                'Status': str(payload.get('status') or 'active'),
                'RefreshError': str(payload.get('refresh_error') or ''),
                'RunIdLastSeen': str(payload.get('run_id_last_seen') or ''),
            }

            existing_items = response.json().get('value', [])
            if existing_items:
                item_id = existing_items[0]['id']
                update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
                update_response = requests.patch(update_url, headers=headers, json=fields_payload, timeout=30)
                return update_response.status_code in [200, 204]

            create_response = requests.post(items_url, headers=headers, json={'fields': fields_payload}, timeout=30)
            return create_response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"[STORAGE] Error upserting LeaseTermSet: {e}", exc_info=True)
            return False

    def replace_lease_terms_to_sharepoint_list(self, lease_key: str, rows: List[Dict[str, Any]]) -> bool:
        """Replace LeaseTerms rows for LeaseKey (delete existing + insert current)."""
        if not self._can_use_sharepoint_lists():
            return False

        lease_key = str(lease_key or '').strip()
        if not lease_key:
            logger.error("[STORAGE] replace_lease_terms_to_sharepoint_list missing lease_key")
            return False

        try:
            site_id = self._get_site_id()
            list_id = self._get_lease_terms_list_id()
            if not site_id or not list_id:
                return False

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            existing_params = {'$select': 'id', '$expand': 'fields', '$filter': f"fields/LeaseKey eq '{lease_key}'", '$top': 5000}
            existing_response = requests.get(items_url, headers=headers, params=existing_params, timeout=30)
            if existing_response.status_code == 200:
                for item in existing_response.json().get('value', []):
                    requests.delete(f"{items_url}/{item['id']}", headers=headers, timeout=30)

            for idx, row in enumerate(rows or []):
                term_key = str(row.get('term_key') or f"{lease_key}:row:{idx}")
                start_date_raw = row.get('start_date')
                end_date_raw = row.get('end_date')
                start_date_value = str(start_date_raw).strip() if start_date_raw is not None else ''
                end_date_value = str(end_date_raw).strip() if end_date_raw is not None else ''
                fields_payload = {
                    'Title': term_key,
                    'TermKey': term_key,
                    'LeaseKey': lease_key,
                    'PropertyId': self._safe_int(row.get('property_id')),
                    'LeaseIntervalId': self._safe_int(row.get('lease_interval_id')),
                    'LeaseId': str(row.get('lease_id') or ''),
                    'TermSetVersion': int(row.get('term_set_version') or 1),
                    'IsActive': bool(row.get('is_active', True)),
                    'TermType': str(row.get('term_type') or 'OTHER'),
                    'MappedArCode': str(row.get('mapped_ar_code') or ''),
                    'Amount': float(row.get('amount') or 0),
                    'Frequency': str(row.get('frequency') or ''),
                    'StartDate': start_date_value or None,
                    'EndDate': end_date_value or None,
                    'DueDay': self._safe_int(row.get('due_day')),
                    'ConditionsKey': str(row.get('conditions_key') or ''),
                    'TermSourceDocId': str(row.get('term_source_doc_id') or ''),
                    'TermSourceDocName': str(row.get('term_source_doc_name') or ''),
                    'MappingVersion': str(row.get('mapping_version') or ''),
                    'MappingConfidence': float(row.get('mapping_confidence') or 0),
                    'UpdatedAt': str(row.get('updated_at') or datetime.utcnow().isoformat()),
                }
                create_response = requests.post(items_url, headers=headers, json={'fields': fields_payload}, timeout=30)
                if create_response.status_code not in [200, 201]:
                    logger.warning(f"[STORAGE] Failed creating LeaseTerms row {term_key}: {create_response.status_code} - {create_response.text}")

            return True
        except Exception as e:
            logger.error(f"[STORAGE] Error replacing LeaseTerms rows: {e}", exc_info=True)
            return False

    def replace_lease_term_evidence_to_sharepoint_list(self, lease_key: str, rows: List[Dict[str, Any]]) -> bool:
        """Replace LeaseTermEvidence rows for LeaseKey."""
        if not self._can_use_sharepoint_lists():
            return False

        lease_key = str(lease_key or '').strip()
        if not lease_key:
            return False

        try:
            site_id = self._get_site_id()
            list_id = self._get_lease_term_evidence_list_id()
            if not site_id or not list_id:
                return False

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            existing_params = {'$select': 'id', '$expand': 'fields', '$filter': f"fields/LeaseKey eq '{lease_key}'", '$top': 5000}
            existing_response = requests.get(items_url, headers=headers, params=existing_params, timeout=30)
            if existing_response.status_code == 200:
                for item in existing_response.json().get('value', []):
                    requests.delete(f"{items_url}/{item['id']}", headers=headers, timeout=30)

            for idx, row in enumerate(rows or []):
                evidence_key = str(row.get('evidence_key') or f"{lease_key}:evidence:{idx}")
                fields_payload = {
                    'Title': evidence_key,
                    'EvidenceKey': evidence_key,
                    'TermKey': str(row.get('term_key') or ''),
                    'LeaseKey': lease_key,
                    'PropertyId': self._safe_int(row.get('property_id')),
                    'LeaseIntervalId': self._safe_int(row.get('lease_interval_id')),
                    'LeaseId': str(row.get('lease_id') or ''),
                    'DocId': str(row.get('doc_id') or ''),
                    'DocName': str(row.get('doc_name') or ''),
                    'PageNumber': self._safe_int(row.get('page_number')),
                    'ExcerptText': str(row.get('excerpt_text') or ''),
                    'Confidence': float(row.get('confidence') or 0),
                    'CapturedAt': str(row.get('captured_at') or datetime.utcnow().isoformat()),
                }
                create_response = requests.post(items_url, headers=headers, json={'fields': fields_payload}, timeout=30)
                if create_response.status_code not in [200, 201]:
                    logger.warning(
                        f"[STORAGE] Failed creating LeaseTermEvidence row {evidence_key}: "
                        f"{create_response.status_code} - {create_response.text}"
                    )

            return True
        except Exception as e:
            logger.error(f"[STORAGE] Error replacing LeaseTermEvidence rows: {e}", exc_info=True)
            return False

    def load_lease_terms_for_lease_key_from_sharepoint_list(self, lease_key: str) -> pd.DataFrame:
        """Load active LeaseTerms rows by LeaseKey."""
        if not self._can_use_sharepoint_lists():
            return pd.DataFrame()

        lease_key = str(lease_key or '').strip()
        if not lease_key:
            return pd.DataFrame()

        try:
            site_id = self._get_site_id()
            list_id = self._get_lease_terms_list_id()
            if not site_id or not list_id:
                return pd.DataFrame()

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$filter': f"fields/LeaseKey eq '{lease_key}'",
                '$top': 5000,
            }
            response = requests.get(items_url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                logger.warning(f"[STORAGE] Failed loading LeaseTerms for {lease_key}: {response.status_code} - {response.text}")
                return pd.DataFrame()

            rows = []
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                is_active_raw = fields.get('IsActive')
                is_active = True
                if is_active_raw is not None:
                    if isinstance(is_active_raw, bool):
                        is_active = is_active_raw
                    elif isinstance(is_active_raw, (int, float)):
                        is_active = int(is_active_raw) == 1
                    else:
                        is_active = str(is_active_raw).strip().lower() in {'1', 'true', 'yes'}

                if not is_active:
                    continue

                rows.append({
                    'term_key': fields.get('TermKey'),
                    'lease_key': fields.get('LeaseKey'),
                    'property_id': fields.get('PropertyId'),
                    'lease_interval_id': fields.get('LeaseIntervalId'),
                    'lease_id': fields.get('LeaseId'),
                    'term_set_version': fields.get('TermSetVersion'),
                    'is_active': fields.get('IsActive'),
                    'term_type': fields.get('TermType'),
                    'mapped_ar_code': fields.get('MappedArCode'),
                    'amount': fields.get('Amount'),
                    'frequency': fields.get('Frequency'),
                    'start_date': fields.get('StartDate'),
                    'end_date': fields.get('EndDate'),
                    'term_source_doc_id': fields.get('TermSourceDocId'),
                    'term_source_doc_name': fields.get('TermSourceDocName'),
                    'mapping_confidence': fields.get('MappingConfidence'),
                })
            return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"[STORAGE] Error loading LeaseTerms rows: {e}", exc_info=True)
            return pd.DataFrame()

    def load_lease_term_set_for_lease_key(self, lease_key: str) -> Dict[str, Any]:
        """Load LeaseTermSet row by LeaseKey."""
        if not self._can_use_sharepoint_lists():
            return {}

        lease_key = str(lease_key or '').strip()
        if not lease_key:
            return {}

        try:
            site_id = self._get_site_id()
            list_id = self._get_lease_term_set_list_id()
            if not site_id or not list_id:
                return {}

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$filter': f"fields/LeaseKey eq '{lease_key}'",
                '$top': 1,
            }
            response = requests.get(items_url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                return {}

            items = response.json().get('value', [])
            if not items:
                return {}

            fields = items[0].get('fields', {})
            return {
                'lease_key': fields.get('LeaseKey'),
                'property_id': fields.get('PropertyId'),
                'lease_interval_id': fields.get('LeaseIntervalId'),
                'lease_id': fields.get('LeaseId'),
                'term_set_version': fields.get('TermSetVersion'),
                'fingerprint_hash': fields.get('FingerprintHash'),
                'doc_list_fingerprint': fields.get('DocListFingerprint'),
                'selected_doc_ids': fields.get('SelectedDocIds'),
                'last_checked_at': fields.get('LastCheckedAt'),
                'last_refreshed_at': fields.get('LastRefreshedAt'),
                'status': fields.get('Status'),
                'refresh_error': fields.get('RefreshError'),
                'run_id_last_seen': fields.get('RunIdLastSeen'),
            }
        except Exception as e:
            logger.error(f"[STORAGE] Error loading LeaseTermSet row: {e}", exc_info=True)
            return {}

    def load_exception_months_from_sharepoint_list(self, run_id: str, property_id: int, 
                                                   lease_interval_id: int, ar_code_id: str) -> List[Dict[str, Any]]:
        """
        Load individual month exception states from SharePoint List 'ExceptionMonths'.
        Each row represents one month of one AR code exception.
        
        CROSS-RUN MATCHING WITH RESOLUTION PERSISTENCE: Queries for resolutions from ANY 
        previous audit run. If the same exception month was marked as "Resolved" in a 
        previous run, that resolution status is automatically applied to the current run,
        even if the exception still appears in the new audit data. This prevents resolved
        exceptions from being counted in current undercharge/overcharge metrics.
        
        Deduplication priority:
        1. RESOLVED records from any run (preserves historical resolutions)
        2. CURRENT run records (for new/unresolved exceptions)
        3. HISTORICAL run records (for reference)
        
        Returns list of month records with their resolution status.
        """
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, returning empty exception months")
            return []

        try:
            logger.info(f"[STORAGE] 📊 Loading exception months for AR Code {ar_code_id} (checking ALL runs)")
            site_id = self._get_site_id()
            if not site_id:
                return []

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.warning("[STORAGE] ExceptionMonths list not found - may need to create it")
                return []

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Filter WITHOUT run_id to find resolutions from ANY audit run
            # This enables cross-run historical resolution matching
            filter_query = (
                f"fields/PropertyId eq {int(property_id)} and "
                f"fields/LeaseIntervalId eq {int(lease_interval_id)} and "
                f"fields/ArCodeId eq '{ar_code_id}'"
            )
            logger.info(f"[STORAGE] 🔍 Query params: property_id={property_id}, lease_interval_id={lease_interval_id}, ar_code_id={ar_code_id} (cross-run)")
            logger.info(f"[STORAGE] 🔍 ExceptionMonths filter: {filter_query}")
            params = {'$expand': 'fields', '$filter': filter_query}
            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] ❌ Failed to query exception months: {response.status_code} - {response.text}")
                return []

            items_data = response.json()
            items = items_data.get('value', [])
            logger.info(f"[STORAGE] 📦 SharePoint returned {len(items)} items for AR Code {ar_code_id}")
            
            # First pass: collect all records and group by month
            all_records = []
            for item in items:
                fields = item.get('fields', {})
                audit_month = self._normalize_snapshot_audit_month(fields.get('AuditMonth', ''))
                record_run_id = fields.get('RunId', '')
                
                record = {
                    'item_id': item.get('id'),  # SharePoint internal ID for updates
                    'composite_key': fields.get('CompositeKey', ''),
                    'run_id': record_run_id,
                    'property_id': fields.get('PropertyId', None),
                    'lease_interval_id': fields.get('LeaseIntervalId', None),
                    'ar_code_id': fields.get('ArCodeId', ''),
                    'audit_month': audit_month,
                    'exception_type': fields.get('ExceptionType', ''),
                    'status': fields.get('Status', 'Open'),
                    'fix_label': fields.get('FixLabel', ''),
                    'action_type': fields.get('ActionType', ''),
                    'variance': fields.get('Variance', 0),
                    'expected_total': fields.get('ExpectedTotal', 0),
                    'actual_total': fields.get('ActualTotal', 0),
                    'resolved_at': fields.get('ResolvedAt', ''),
                    'resolved_by': fields.get('ResolvedBy', ''),
                    'resolved_by_name': fields.get('ResolvedByName', ''),
                    'notes': fields.get('Notes', ''),
                    'updated_at': fields.get('UpdatedAt', ''),
                    'updated_by': fields.get('UpdatedBy', ''),
                    'is_historical': record_run_id != run_id,  # Flag if from a previous run
                    'is_current_run': record_run_id == run_id  # Flag if from current run
                }
                all_records.append(record)
            
            # Second pass: deduplicate - prioritize RESOLVED status over run priority
            results = []
            seen_months = set()
            
            # FIRST: Process any RESOLVED records from ANY run (auto-apply historical resolutions)
            for record in all_records:
                if record['status'] == 'Resolved' and record['audit_month'] not in seen_months:
                    results.append(record)
                    seen_months.add(record['audit_month'])
                    if record['is_historical']:
                        logger.debug(f"[STORAGE] ✨ Auto-applied HISTORICAL resolution for {record['audit_month']}: {record['fix_label']}")
                    else:
                        logger.debug(f"[STORAGE] ✅ Using CURRENT run resolution for {record['audit_month']}")
            
            # SECOND: Add current run records for months not yet resolved
            for record in all_records:
                if record['is_current_run'] and record['audit_month'] not in seen_months:
                    results.append(record)
                    seen_months.add(record['audit_month'])
                    logger.debug(f"[STORAGE] 📝 Using CURRENT run unresolved record for {record['audit_month']}")
            
            # THIRD: Add any other historical records for months not yet seen
            for record in all_records:
                if not record['is_current_run'] and record['audit_month'] not in seen_months:
                    results.append(record)
                    seen_months.add(record['audit_month'])
                    logger.debug(f"[STORAGE] 📜 Using HISTORICAL run record for {record['audit_month']}")
                elif not record['is_current_run'] and record['audit_month'] in seen_months:
                    logger.debug(f"[STORAGE] ⏭️ Skipping duplicate historical record for {record['audit_month']}")
            
            logger.info(f"[STORAGE] Loaded {len(results)} unique exception month(s) for AR Code {ar_code_id}")
            if results:
                historical_count = sum(1 for r in results if r.get('is_historical'))
                if historical_count > 0:
                    logger.info(f"[STORAGE] ✨ {historical_count} historical resolution(s) auto-applied from previous runs")
            return results

        except Exception as e:
            logger.error(f"[STORAGE] Error loading exception months: {e}", exc_info=True)
            return []

    def load_property_exception_months_bulk(self, run_id: str, property_id: int) -> Dict[tuple, List[Dict[str, Any]]]:
        """
        BULK FETCH: Load all exception months for an entire property in ONE API call.
        Solves N+1 problem by fetching all lease/AR code combinations at once.
        
        RESOLUTION PERSISTENCE: When the same exception month exists in multiple runs,
        prioritizes "Resolved" status from any previous run. This auto-applies historical
        resolutions to current audit data, preventing resolved exceptions from being
        counted in current undercharge/overcharge metrics.
        
        Deduplication priority:
        1. RESOLVED records from any run (preserves historical resolutions)
        2. CURRENT run records (for new/unresolved exceptions)
        3. HISTORICAL run records (for reference)
        
        Args:
            run_id: Current audit run ID
            property_id: Property to fetch data for
            
        Returns:
            Dictionary keyed by (lease_interval_id, ar_code_id) containing month records
            Example: {(123456, '55052'): [{month1}, {month2}], ...}
        """
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, returning empty bulk results")
            return {}

        try:
            logger.info(f"[CACHE] 🚀 BULK FETCH: Loading ALL exception months for property {property_id}")
            site_id = self._get_site_id()
            if not site_id:
                return {}

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.warning("[STORAGE] ExceptionMonths list not found")
                return {}

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Single filter for entire property (no lease or AR code filtering)
            filter_query = f"fields/PropertyId eq {int(property_id)}"
            params = {'$expand': 'fields', '$filter': filter_query, '$top': 5000}  # Fetch up to 5000 records
            
            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] ❌ Bulk fetch failed: {response.status_code} - {response.text}")
                return {}

            items_data = response.json()
            items = items_data.get('value', [])
            logger.info(f"[CACHE] ✅ Bulk fetched {len(items)} exception month records for property {property_id}")
            
            # Group records by (lease_id, ar_code_id)
            grouped_results = {}
            
            for item in items:
                fields = item.get('fields', {})
                lease_id = fields.get('LeaseIntervalId')
                ar_code_id = fields.get('ArCodeId', '')
                audit_month = self._normalize_snapshot_audit_month(fields.get('AuditMonth', ''))
                record_run_id = fields.get('RunId', '')
                
                if not lease_id or not ar_code_id:
                    continue
                
                record = {
                    'item_id': item.get('id'),
                    'composite_key': fields.get('CompositeKey', ''),
                    'run_id': record_run_id,
                    'property_id': fields.get('PropertyId', None),
                    'lease_interval_id': lease_id,
                    'ar_code_id': ar_code_id,
                    'audit_month': audit_month,
                    'exception_type': fields.get('ExceptionType', ''),
                    'status': fields.get('Status', 'Open'),
                    'fix_label': fields.get('FixLabel', ''),
                    'action_type': fields.get('ActionType', ''),
                    'variance': fields.get('Variance', 0),
                    'expected_total': fields.get('ExpectedTotal', 0),
                    'actual_total': fields.get('ActualTotal', 0),
                    'resolved_at': fields.get('ResolvedAt', ''),
                    'resolved_by': fields.get('ResolvedBy', ''),
                    'resolved_by_name': fields.get('ResolvedByName', ''),
                    'notes': fields.get('Notes', ''),
                    'updated_at': fields.get('UpdatedAt', ''),
                    'updated_by': fields.get('UpdatedBy', ''),
                    'is_historical': record_run_id != run_id,
                    'is_current_run': record_run_id == run_id
                }
                
                # Group by (lease_id, ar_code_id)
                key = (lease_id, ar_code_id)
                if key not in grouped_results:
                    grouped_results[key] = []
                grouped_results[key].append(record)
            
            # Deduplicate months within each group (prioritize resolved historical records)
            for key, records in grouped_results.items():
                seen_months = set()
                deduped = []
                
                # FIRST: Keep any RESOLVED records from ANY run (auto-apply historical resolutions)
                for record in records:
                    if record['status'] == 'Resolved' and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                        logger.debug(f"[STORAGE] ✨ Auto-applied historical resolution for {record['audit_month']}: {record['fix_label']}")
                
                # SECOND: For remaining months, prefer current run records
                for record in records:
                    if record['is_current_run'] and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                
                # THIRD: Fill in any other historical records not yet seen
                for record in records:
                    if not record['is_current_run'] and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                
                grouped_results[key] = deduped
            
            logger.info(f"[CACHE] 📦 Grouped into {len(grouped_results)} lease/AR code combinations")
            return grouped_results

        except Exception as e:
            logger.error(f"[STORAGE] Error in bulk fetch: {e}", exc_info=True)
            return {}

    def load_lease_exception_months_bulk(self, run_id: str, property_id: int, lease_interval_id: int) -> Dict[str, List[Dict[str, Any]]]:
        """
        BULK FETCH: Load all exception months for a single lease in ONE API call,
        grouped by ar_code_id. Replaces the N+1 per-AR-code loop in the lease view.

        Returns:
            Dictionary keyed by ar_code_id (str) -> list of month records
        """
        if not self._can_use_sharepoint_lists():
            return {}

        try:
            logger.info(f"[STORAGE] 🚀 BULK FETCH: Loading ALL exception months for lease {lease_interval_id}")
            site_id = self._get_site_id()
            if not site_id:
                return {}

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.warning("[STORAGE] ExceptionMonths list not found")
                return {}

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }

            filter_query = (
                f"fields/PropertyId eq {int(property_id)} and "
                f"fields/LeaseIntervalId eq {int(lease_interval_id)}"
            )
            params = {'$expand': 'fields', '$filter': filter_query, '$top': 5000}

            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] ❌ Lease bulk fetch failed: {response.status_code} - {response.text}")
                return {}

            items = response.json().get('value', [])
            logger.info(f"[STORAGE] ✅ Bulk fetched {len(items)} exception month records for lease {lease_interval_id}")

            # Group by ar_code_id
            grouped: Dict[str, list] = {}
            for item in items:
                fields = item.get('fields', {})
                ar_code_id = str(fields.get('ArCodeId', '')).strip()
                audit_month = self._normalize_snapshot_audit_month(fields.get('AuditMonth', ''))
                record_run_id = fields.get('RunId', '')

                record = {
                    'item_id': item.get('id'),
                    'composite_key': fields.get('CompositeKey', ''),
                    'run_id': record_run_id,
                    'property_id': fields.get('PropertyId', None),
                    'lease_interval_id': fields.get('LeaseIntervalId', None),
                    'ar_code_id': ar_code_id,
                    'audit_month': audit_month,
                    'exception_type': fields.get('ExceptionType', ''),
                    'status': fields.get('Status', 'Open'),
                    'fix_label': fields.get('FixLabel', ''),
                    'action_type': fields.get('ActionType', ''),
                    'variance': fields.get('Variance', 0),
                    'expected_total': fields.get('ExpectedTotal', 0),
                    'actual_total': fields.get('ActualTotal', 0),
                    'resolved_at': fields.get('ResolvedAt', ''),
                    'resolved_by': fields.get('ResolvedBy', ''),
                    'resolved_by_name': fields.get('ResolvedByName', ''),
                    'notes': fields.get('Notes', ''),
                    'updated_at': fields.get('UpdatedAt', ''),
                    'updated_by': fields.get('UpdatedBy', ''),
                    'is_historical': record_run_id != run_id,
                    'is_current_run': record_run_id == run_id,
                }

                if ar_code_id not in grouped:
                    grouped[ar_code_id] = []
                grouped[ar_code_id].append(record)

            # Deduplicate months within each AR code group (same priority logic as per-code method)
            for ar_code_id, records in grouped.items():
                seen_months: set = set()
                deduped = []
                for record in records:
                    if record['status'] == 'Resolved' and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                for record in records:
                    if record['is_current_run'] and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                for record in records:
                    if not record['is_current_run'] and record['audit_month'] not in seen_months:
                        deduped.append(record)
                        seen_months.add(record['audit_month'])
                grouped[ar_code_id] = deduped

            logger.info(f"[STORAGE] 📦 Lease bulk grouped into {len(grouped)} AR code(s)")
            return grouped

        except Exception as e:
            logger.error(f"[STORAGE] Error in lease bulk fetch: {e}", exc_info=True)
            return {}

    def load_property_audit_status_summary(self) -> Dict[int, Dict[str, int]]:
        """
        Query the ExceptionMonths SharePoint list for all rows and return per-property
        counts of resolved and open exception months.

        Used to derive property-level audit status on the portfolio dashboard:
          - No records (and exception_count > 0) → Not Started
          - resolved_months > 0, open_months > 0 → In Progress
          - resolved_months > 0, open_months == 0 → Complete (all actioned)

        Returns:
            Dict keyed by property_id (int):
                { 'resolved_months': int, 'open_months': int }
        """
        if not self._can_use_sharepoint_lists():
            return {}

        try:
            site_id = self._get_site_id()
            if not site_id:
                return {}

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.warning("[STORAGE] ExceptionMonths list not found for audit status summary")
                return {}

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields($select=PropertyId,Status,Variance)',
                '$top': 5000,
            }

            summary: Dict[int, Dict[str, int]] = {}
            next_url = items_url

            while next_url:
                if next_url == items_url:
                    response = requests.get(next_url, headers=headers, params=params, timeout=30)
                else:
                    response = requests.get(next_url, headers=headers, timeout=30)

                if response.status_code != 200:
                    logger.error(
                        f"[STORAGE] ❌ load_property_audit_status_summary failed: "
                        f"{response.status_code} - {response.text}"
                    )
                    break

                data = response.json()
                for item in data.get('value', []):
                    fields = item.get('fields', {})
                    prop_id = self._safe_int(fields.get('PropertyId'))
                    if prop_id is None:
                        continue
                    status = str(fields.get('Status', 'Open')).strip()
                    variance = float(fields.get('Variance') or 0)
                    if prop_id not in summary:
                        summary[prop_id] = {'resolved_months': 0, 'open_months': 0, 'resolved_undercharge': 0.0, 'resolved_overcharge': 0.0, '_resolved_bucket_keys': set()}
                    if status == 'Resolved':
                        summary[prop_id]['resolved_months'] += 1
                        bucket_key = (fields.get('LeaseIntervalId'), fields.get('ArCodeId'))
                        summary[prop_id]['_resolved_bucket_keys'].add(bucket_key)
                        if variance < 0:
                            summary[prop_id]['resolved_undercharge'] += abs(variance)
                        elif variance > 0:
                            summary[prop_id]['resolved_overcharge'] += variance
                    else:
                        summary[prop_id]['open_months'] += 1

                next_url = data.get('@odata.nextLink')

            # Convert internal bucket key sets to counts before returning
            for prop_id in summary:
                summary[prop_id]['resolved_buckets'] = len(summary[prop_id].pop('_resolved_bucket_keys', set()))

            logger.info(f"[STORAGE] ✅ Loaded audit status summary for {len(summary)} properties")
            return summary

        except Exception as e:
            logger.error(f"[STORAGE] Error loading property audit status summary: {e}", exc_info=True)
            return {}

    def load_all_resolved_totals(self) -> Dict[str, float]:
        """
        Query the ExceptionMonths SharePoint list for ALL Status='Resolved' rows
        and return aggregate variance totals.

        This is the source-of-truth calculation for historical recovery/impact metrics
        on the portfolio dashboard — it is run-agnostic and does not require matching
        resolutions against a specific audit run's bucket data.

        Returns:
            {
                'recovered':  total absolute value of resolved undercharge variances (billed < scheduled),
                'prevented':  total of resolved overcharge variances (billed > scheduled),
                'count':      total number of resolved exception months
            }
        """
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, returning zero resolved totals")
            return {'recovered': 0.0, 'prevented': 0.0, 'count': 0}

        try:
            logger.info("[STORAGE] 📊 load_all_resolved_totals: querying ExceptionMonths for Status=Resolved")
            site_id = self._get_site_id()
            if not site_id:
                return {'recovered': 0.0, 'prevented': 0.0, 'count': 0}

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.warning("[STORAGE] ExceptionMonths list not found")
                return {'recovered': 0.0, 'prevented': 0.0, 'count': 0}

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }

            recovered = 0.0
            prevented = 0.0
            count = 0
            next_url = items_url
            params = {
                '$expand': 'fields($select=Variance,Status)',
                '$filter': "fields/Status eq 'Resolved'",
                '$top': 5000
            }

            while next_url:
                if next_url == items_url:
                    response = requests.get(next_url, headers=headers, params=params, timeout=30)
                else:
                    response = requests.get(next_url, headers=headers, timeout=30)

                if response.status_code != 200:
                    logger.error(f"[STORAGE] ❌ load_all_resolved_totals failed: {response.status_code} - {response.text}")
                    break

                data = response.json()
                for item in data.get('value', []):
                    variance = item.get('fields', {}).get('Variance', 0) or 0
                    try:
                        variance = float(variance)
                    except (TypeError, ValueError):
                        variance = 0.0
                    if variance < 0:
                        recovered += abs(variance)   # undercharge: we were billing too little
                    elif variance > 0:
                        prevented += variance         # overcharge: we caught overbilling
                    count += 1

                next_url = data.get('@odata.nextLink')

            logger.info(
                f"[STORAGE] load_all_resolved_totals: "
                f"recovered=${recovered:,.2f}, prevented=${prevented:,.2f}, count={count}"
            )
            return {'recovered': recovered, 'prevented': prevented, 'count': count}

        except Exception as e:
            logger.error(f"[STORAGE] Error in load_all_resolved_totals: {e}", exc_info=True)
            return {'recovered': 0.0, 'prevented': 0.0, 'count': 0}

    def upsert_exception_month_to_sharepoint_list(self, month_data: Dict[str, Any]) -> bool:
        """
        Upsert a single month's exception state into SharePoint List 'ExceptionMonths'.
        Creates new record if doesn't exist, updates if it does.
        
        Args:
            month_data: Dictionary containing month exception details including:
                - run_id, property_id, lease_interval_id, ar_code_id
                - audit_month (e.g., "2024-01")
                - exception_type, status, fix_label, action_type
                - variance, expected_total, actual_total
                - resolved_at, resolved_by (email), resolved_by_name (display name)
        """
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, skipping exception month upsert")
            return False

        try:
            site_id = self._get_site_id()
            if not site_id:
                return False

            list_id = self._get_sharepoint_list_id("ExceptionMonths")
            if not list_id:
                logger.error("[STORAGE] ExceptionMonths list not found - cannot save month data")
                return False

            normalized_audit_month = self._normalize_snapshot_audit_month(month_data.get('audit_month'))
            audit_month_field_value = self._normalize_audit_month_value(month_data.get('audit_month'))

            # Build composite key for this specific month
            composite_key = (
                f"{month_data.get('run_id')}:{month_data.get('property_id')}:"
                f"{month_data.get('lease_interval_id')}:{month_data.get('ar_code_id')}:"
                f"{normalized_audit_month}"
            )
            logger.info(f"[STORAGE] Upserting ExceptionMonth: {composite_key}")

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }

            # Check if record already exists
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            filter_query = f"fields/CompositeKey eq '{composite_key}'"
            params = {'$expand': 'fields', '$filter': filter_query}
            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to query exception month: {response.status_code} - {response.text}")
                return False

            # Prepare fields payload
            fields_payload = {
                'CompositeKey': composite_key,
                'RunId': month_data.get('run_id'),
                'PropertyId': int(month_data.get('property_id', 0)),
                'LeaseIntervalId': int(month_data.get('lease_interval_id', 0)),
                'ArCodeId': month_data.get('ar_code_id'),
                'AuditMonth': audit_month_field_value,
                'ExceptionType': month_data.get('exception_type', ''),
                'Status': month_data.get('status', 'Open'),
                'FixLabel': month_data.get('fix_label', ''),
                'ActionType': month_data.get('action_type', ''),
                'Variance': float(month_data.get('variance', 0)),
                'ExpectedTotal': float(month_data.get('expected_total', 0)),
                'ActualTotal': float(month_data.get('actual_total', 0)),
                'ResolvedAt': month_data.get('resolved_at', ''),
                'ResolvedBy': month_data.get('resolved_by', ''),
                'ResolvedByName': month_data.get('resolved_by_name', ''),
                'Notes': month_data.get('notes', ''),
                'UpdatedAt': month_data.get('updated_at', ''),
                'UpdatedBy': month_data.get('updated_by', '')
            }

            def _retry_without_unrecognized_field(response: requests.Response, payload: Dict[str, Any], op: str) -> tuple[requests.Response, Dict[str, Any]]:
                """Retry one time when Graph rejects an unknown field (schema drift)."""
                if response.status_code != 400:
                    return response, payload

                try:
                    error_message = response.json().get('error', {}).get('message', '')
                except Exception:
                    error_message = response.text or ''

                match = re.search(r"Field '([^']+)' is not recognized", error_message)
                if not match:
                    return response, payload

                unknown_field = match.group(1)
                if unknown_field not in payload:
                    return response, payload

                retry_payload = dict(payload)
                retry_payload.pop(unknown_field, None)
                logger.warning(
                    f"[STORAGE] SharePoint rejected field '{unknown_field}' during ExceptionMonths {op}; retrying without it"
                )

                if op == 'update':
                    retry_response = requests.patch(update_url, headers=headers, json=retry_payload, timeout=30)
                else:
                    retry_response = requests.post(items_url, headers=headers, json={'fields': retry_payload}, timeout=30)

                return retry_response, retry_payload
            
            logger.info(f"[STORAGE] 💾 Saving fields: RunId={fields_payload['RunId']}, PropertyId={fields_payload['PropertyId']}, LeaseIntervalId={fields_payload['LeaseIntervalId']}, ArCodeId={fields_payload['ArCodeId']}, Status={fields_payload['Status']}, ResolvedBy={fields_payload['ResolvedBy']}, ResolvedByName={fields_payload['ResolvedByName']}")

            items_data = response.json()
            items = items_data.get('value', [])
            
            if items:
                # Update existing record
                item_id = items[0]['id']
                update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
                update_response = requests.patch(update_url, headers=headers, json=fields_payload, timeout=30)
                update_response, fields_payload = _retry_without_unrecognized_field(update_response, fields_payload, 'update')
                
                if update_response.status_code in [200, 204]:
                    logger.info(f"[STORAGE] ✅ Exception month updated: {month_data.get('audit_month')}")
                    return True
                else:
                    logger.error(f"[STORAGE] Failed to update exception month: {update_response.status_code} - {update_response.text}")
                    return False
            else:
                # Create new record
                create_payload = {'fields': fields_payload}
                create_response = requests.post(items_url, headers=headers, json=create_payload, timeout=30)
                create_response, fields_payload = _retry_without_unrecognized_field(create_response, fields_payload, 'create')
                
                if create_response.status_code in [200, 201]:
                    logger.info(f"[STORAGE] ✅ Exception month created: {month_data.get('audit_month')}")
                    return True
                else:
                    logger.error(f"[STORAGE] Failed to create exception month: {create_response.status_code} - {create_response.text}")
                    return False

        except Exception as e:
            logger.error(f"[STORAGE] Error upserting exception month: {e}", exc_info=True)
            return False

    def calculate_ar_code_status(self, run_id: str, property_id: int, 
                                 lease_interval_id: int, ar_code_id: str, 
                                 exception_count: int = 0) -> Dict[str, Any]:
        """
        Calculate overall AR code status based on individual month statuses.
        
        Args:
            exception_count: Total number of exception months from audit data (not just saved ones)
        
        Returns:
            {
                'status': 'Open' | 'Resolved' | 'Passed',
                'total_months': 4,
                'resolved_months': 2,
                'open_months': 2,
                'status_label': 'Open (2 of 4 resolved)'
            }
        """
        logger.info(f"[STATUS_CALC] Calculating status for AR Code {ar_code_id} (Run: {run_id}, Property: {property_id}, Lease: {lease_interval_id}, Total Exceptions: {exception_count})")
        
        months = self.load_exception_months_from_sharepoint_list(
            run_id, property_id, lease_interval_id, ar_code_id
        )
        
        logger.info(f"[STATUS_CALC] Loaded {len(months) if months else 0} months for AR Code {ar_code_id}")
        if months:
            for month in months:
                logger.info(f"[STATUS_CALC]   Month: {month.get('audit_month')}, Status: {month.get('status')}, Fix: {month.get('fix_label')}")
        
        # Use the actual exception count from audit data as total, not just saved SharePoint records
        total_months = exception_count if exception_count > 0 else len(months)
        resolved_months = sum(1 for m in months if m.get('status') == 'Resolved')
        open_months = total_months - resolved_months
        
        logger.info(f"[STATUS_CALC] Total: {total_months}, Resolved: {resolved_months}, Open: {open_months}")
        
        if open_months > 0:
            status = 'Open'
            status_label = 'Open'
        else:
            status = 'Resolved'
            status_label = 'Resolved'
        
        result = {
            'status': status,
            'total_months': total_months,
            'resolved_months': resolved_months,
            'open_months': open_months,
            'status_label': status_label
        }
        
        logger.info(f"[STATUS_CALC] Final status for AR Code {ar_code_id}: {result}")
        return result
    
    def _upload_file_to_sharepoint(self, file_content: str, file_path: str) -> bool:
        """Upload file to SharePoint document library."""
        try:
            site_id, drive_id = self._get_site_and_drive_id()
            if not site_id or not drive_id:
                logger.error(f"[STORAGE] ❌ Failed to upload {file_path} - Cannot get site/drive ID")
                return False
            
            # Upload file
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{file_path}:/content"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'text/plain'
            }
            logger.debug(f"[STORAGE] 📤 Uploading: {file_path} ({len(file_content)} chars)")

            max_attempts = max(1, int(os.getenv('SHAREPOINT_UPLOAD_MAX_ATTEMPTS', '4')))
            base_backoff = max(0.1, float(os.getenv('SHAREPOINT_UPLOAD_BACKOFF_SECONDS', '1.0')))
            transient_statuses = {408, 429, 500, 502, 503, 504}
            token_refresh_attempted = False

            for attempt in range(1, max_attempts + 1):
                try:
                    response = requests.put(url, headers=headers, data=file_content.encode('utf-8'), timeout=30)
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as request_exc:
                    if attempt < max_attempts:
                        delay = base_backoff * (2 ** (attempt - 1))
                        logger.warning(
                            f"[STORAGE] ⚠️ Upload transient exception for {file_path} "
                            f"(attempt {attempt}/{max_attempts}): {request_exc}. Retrying in {delay:.1f}s"
                        )
                        sleep(delay)
                        continue
                    logger.error(f"[STORAGE] ❌ Exception uploading {file_path}: {request_exc}", exc_info=True)
                    return False

                if response.status_code == 401 and not token_refresh_attempted:
                    logger.warning(f"[STORAGE] 🔄 Token expired uploading {file_path}; refreshing and retrying...")
                    token_refresh_attempted = True
                    new_token = _get_app_only_token()
                    if new_token:
                        self.access_token = new_token
                        headers['Authorization'] = f'Bearer {self.access_token}'
                        continue
                    logger.error(f"[STORAGE] ❌ Token refresh failed for {file_path}")
                    return False

                if response.status_code in [200, 201]:
                    logger.debug(f"[STORAGE] ✅ Uploaded: {file_path}")
                    return True

                if response.status_code in transient_statuses and attempt < max_attempts:
                    delay = base_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        f"[STORAGE] ⚠️ Transient upload failure for {file_path}: "
                        f"HTTP {response.status_code} (attempt {attempt}/{max_attempts}). "
                        f"Retrying in {delay:.1f}s. Response: {response.text[:200]}"
                    )
                    sleep(delay)
                    continue

                logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: HTTP {response.status_code} - {response.text[:200]}")
                return False

            logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: retries exhausted")
            return False
                
        except Exception as e:
            logger.error(f"[STORAGE] ❌ Exception uploading {file_path}: {e}", exc_info=True)
            return False
    
    def _upload_binary_file_to_sharepoint(self, file_content: bytes, file_path: str) -> bool:
        """Upload binary file (like Excel) to SharePoint document library."""
        try:
            site_id, drive_id = self._get_site_and_drive_id()
            if not site_id or not drive_id:
                logger.error(f"[STORAGE] ❌ Failed to upload {file_path} - Cannot get site/drive ID")
                return False
            
            # Upload binary file
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{file_path}:/content"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            logger.info(f"[STORAGE] 📤 Uploading binary file: {file_path} ({len(file_content)} bytes)")

            max_attempts = max(1, int(os.getenv('SHAREPOINT_UPLOAD_MAX_ATTEMPTS', '4')))
            base_backoff = max(0.1, float(os.getenv('SHAREPOINT_UPLOAD_BACKOFF_SECONDS', '1.0')))
            transient_statuses = {408, 429, 500, 502, 503, 504}
            token_refresh_attempted = False

            for attempt in range(1, max_attempts + 1):
                try:
                    response = requests.put(url, headers=headers, data=file_content, timeout=30)
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as request_exc:
                    if attempt < max_attempts:
                        delay = base_backoff * (2 ** (attempt - 1))
                        logger.warning(
                            f"[STORAGE] ⚠️ Binary upload transient exception for {file_path} "
                            f"(attempt {attempt}/{max_attempts}): {request_exc}. Retrying in {delay:.1f}s"
                        )
                        sleep(delay)
                        continue
                    logger.error(f"[STORAGE] ❌ Exception uploading {file_path}: {request_exc}", exc_info=True)
                    return False

                if response.status_code == 401 and not token_refresh_attempted:
                    logger.warning(f"[STORAGE] 🔄 Token expired uploading binary {file_path}; refreshing and retrying...")
                    token_refresh_attempted = True
                    new_token = _get_app_only_token()
                    if new_token:
                        self.access_token = new_token
                        headers['Authorization'] = f'Bearer {self.access_token}'
                        continue
                    logger.error(f"[STORAGE] ❌ Token refresh failed for {file_path}")
                    return False

                if response.status_code in [200, 201]:
                    logger.info(f"[STORAGE] ✅ Successfully uploaded: {file_path}")
                    return True

                if response.status_code in transient_statuses and attempt < max_attempts:
                    delay = base_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        f"[STORAGE] ⚠️ Transient binary upload failure for {file_path}: "
                        f"HTTP {response.status_code} (attempt {attempt}/{max_attempts}). "
                        f"Retrying in {delay:.1f}s. Response: {response.text[:200]}"
                    )
                    sleep(delay)
                    continue

                logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: HTTP {response.status_code} - {response.text[:200]}")
                return False

            logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: retries exhausted")
            return False
                
        except Exception as e:
            logger.error(f"[STORAGE] ❌ Exception uploading {file_path}: {e}", exc_info=True)
            return False
    
    def _download_file_from_sharepoint(self, file_path: str, is_binary: bool = False) -> Optional[str | bytes]:
        """Download file from SharePoint document library."""
        try:
            site_id, drive_id = self._get_site_and_drive_id()
            if not site_id or not drive_id:
                return None
            
            # Download file
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{file_path}:/content"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.content if is_binary else response.text
            else:
                if response.status_code == 404:
                    logger.debug(f"[STORAGE] SharePoint file not found (404): {file_path}")
                else:
                    logger.warning(f"[STORAGE] File not found or error downloading {file_path}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"[STORAGE] Error downloading {file_path}: {e}", exc_info=True)
            return None
    
    def create_run_dir(self, run_id: str) -> Path:
        """Create directory structure for a new run."""
        if not self.use_sharepoint:
            run_dir = self.base_dir / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "inputs_normalized").mkdir(exist_ok=True)
            (run_dir / "outputs").mkdir(exist_ok=True)
            return run_dir
        return Path(run_id)  # For SharePoint, just return the run_id as path
    
    def _save_dataframe(self, df: pd.DataFrame, run_id: str, file_path: str):
        """Save DataFrame to either SharePoint or local filesystem."""
        if self.use_sharepoint:
            # Save to SharePoint
            csv_content = df.to_csv(index=False)
            sp_path = f"{run_id}/{file_path}"
            success = self._upload_file_to_sharepoint(csv_content, sp_path)
            if not success:
                raise RuntimeError(f"[STORAGE] SharePoint upload failed for {sp_path}")
        else:
            # Save to local filesystem
            local_path = self.base_dir / run_id / file_path
            df.to_csv(local_path, index=False)
    
    def _load_dataframe(self, run_id: str, file_path: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from either SharePoint or local filesystem."""
        try:
            if self.use_sharepoint:
                # Load from SharePoint
                sp_path = f"{run_id}/{file_path}"
                content = self._download_file_from_sharepoint(sp_path)
                if content:
                    return pd.read_csv(io.StringIO(content))
                return None
            else:
                # Load from local filesystem
                local_path = self.base_dir / run_id / file_path
                if local_path.exists():
                    return pd.read_csv(local_path)
                return None
        except pd.errors.EmptyDataError:
            # CSV file exists but has no data (0 lease intervals audited)
            logger.info(f"[STORAGE] CSV file {file_path} for run {run_id} is empty (no lease intervals to audit)")
            return None
    
    def _save_json(self, data: Dict[str, Any], run_id: str, file_path: str):
        """Save JSON to either SharePoint or local filesystem."""
        if self.use_sharepoint:
            # Save to SharePoint
            json_content = json.dumps(data, indent=2, default=str)
            sp_path = f"{run_id}/{file_path}"
            success = self._upload_file_to_sharepoint(json_content, sp_path)
            if not success:
                raise RuntimeError(f"[STORAGE] SharePoint upload failed for {sp_path}")
        else:
            # Save to local filesystem
            local_path = self.base_dir / run_id / file_path
            with open(local_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
    
    def _load_json(self, run_id: str, file_path: str) -> Optional[Dict[str, Any]]:
        """Load JSON from either SharePoint or local filesystem."""
        if self.use_sharepoint:
            # Load from SharePoint
            sp_path = f"{run_id}/{file_path}"
            content = self._download_file_from_sharepoint(sp_path)
            if content:
                return json.loads(content)
            return None
        else:
            # Load from local filesystem
            local_path = self.base_dir / run_id / file_path
            if local_path.exists():
                with open(local_path, "r") as f:
                    return json.load(f)
            return None
    
    def _write_metrics_to_sharepoint_list(self, run_id: str, bucket_results: pd.DataFrame, 
                                          findings: pd.DataFrame, metadata: dict) -> bool:
        """Write summary metrics to SharePoint List 'Audit Run Metrics'."""
        if not self._can_use_sharepoint_lists():
            logger.debug(f"[STORAGE] Skipping SharePoint list write - not configured")
            return False
        
        try:
            from audit_engine.canonical_fields import CanonicalField
            
            logger.info(f"[STORAGE] 📊 Writing metrics to SharePoint list for run {run_id}")
            
            # Calculate metrics from bucket_results
            total_buckets = len(bucket_results)
            matched = len(bucket_results[bucket_results[CanonicalField.STATUS.value] == 'Matched'])
            exceptions = bucket_results[bucket_results[CanonicalField.STATUS.value] != 'Matched']
            
            # Count by status (normalize labels)
            def _normalize_status(value: str) -> str:
                if not value:
                    return ''
                normalized = str(value).strip().lower()
                if normalized in {'scheduled not billed', 'scheduled_not_billed'}:
                    return 'scheduled_not_billed'
                if normalized in {'billed not scheduled', 'billed without schedule', 'billed_not_scheduled'}:
                    return 'billed_not_scheduled'
                if normalized in {'amount mismatch', 'amount_mismatch'}:
                    return 'amount_mismatch'
                if normalized == 'matched':
                    return 'matched'
                return normalized

            normalized_status = bucket_results[CanonicalField.STATUS.value].map(_normalize_status)
            scheduled_not_billed = int((normalized_status == 'scheduled_not_billed').sum())
            billed_not_scheduled = int((normalized_status == 'billed_not_scheduled').sum())
            amount_mismatch = int((normalized_status == 'amount_mismatch').sum())
            
            # Calculate totals
            total_scheduled = bucket_results[CanonicalField.EXPECTED_TOTAL.value].sum()
            total_actual = bucket_results[CanonicalField.ACTUAL_TOTAL.value].sum()
            
            # Count findings by severity
            high_severity = len(findings[findings[CanonicalField.SEVERITY.value] == 'high']) if len(findings) > 0 else 0
            medium_severity = len(findings[findings[CanonicalField.SEVERITY.value] == 'medium']) if len(findings) > 0 else 0
            
            # Calculate property-level breakdown
            property_summary = {}
            for prop_id in bucket_results[CanonicalField.PROPERTY_ID.value].unique():
                prop_buckets = bucket_results[bucket_results[CanonicalField.PROPERTY_ID.value] == prop_id]
                prop_exceptions = prop_buckets[prop_buckets[CanonicalField.STATUS.value] != 'Matched']
                property_summary[str(int(prop_id))] = {
                    'total_buckets': len(prop_buckets),
                    'exceptions': len(prop_exceptions),
                    'variance': float(prop_buckets[CanonicalField.VARIANCE.value].abs().sum())
                }
            
            # Prepare list item data
            list_item = {
                "fields": {
                    "Title": run_id,
                    "RunDateTime": metadata.get('timestamp', ''),
                    "UploadedBy": metadata.get('uploaded_by', ''),
                    "FileName": metadata.get('filename', ''),
                    "TotalScheduled": float(total_scheduled),
                    "TotalActual": float(total_actual),
                    "Matched": matched,
                    "ScheduledNotBilled": scheduled_not_billed,
                    "BilledNotScheduled": billed_not_scheduled,
                    "AmountMismatch": amount_mismatch,
                    "TotalVariances": len(exceptions),
                    "HighSeverity": high_severity,
                    "MediumSeverity": medium_severity,
                    "Properties": json.dumps(property_summary)
                }
            }
            
            # Get site ID
            site_id = self._get_site_id()
            if not site_id:
                logger.error(f"[STORAGE] Cannot write to list - site ID not found")
                return False
            
            # Get list ID for "Audit Run Metrics"
            list_name = "Audit Run Metrics"
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Get list by display name
            params = {'$filter': f"displayName eq '{list_name}'"}
            response = requests.get(list_url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to find list '{list_name}': {response.status_code} - {response.text}")
                return False
            
            lists_data = response.json()
            if not lists_data.get('value'):
                logger.error(f"[STORAGE] List '{list_name}' not found")
                return False
            
            list_id = lists_data['value'][0]['id']
            
            # Create list item
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            response = requests.post(items_url, headers=headers, json=list_item, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"[STORAGE] ✅ Metrics written to SharePoint list successfully")
                return True
            else:
                logger.error(f"[STORAGE] Failed to create list item: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"[STORAGE] Error writing metrics to SharePoint list: {e}", exc_info=True)
            return False
    
    def load_all_metrics_from_sharepoint_list(self) -> List[Dict[str, Any]]:
        """Load all metrics from SharePoint List 'Audit Run Metrics'."""
        if not self._can_use_sharepoint_lists():
            logger.debug(f"[STORAGE] SharePoint list not configured, returning empty list")
            return []
        
        try:
            logger.info(f"[STORAGE] 📊 Loading metrics from SharePoint list")
            
            # Get site ID
            site_id = self._get_site_id()
            if not site_id:
                logger.error(f"[STORAGE] Cannot read list - site ID not found")
                return []
            
            # Get list ID for "Audit Run Metrics"
            list_name = "Audit Run Metrics"
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Get list by display name
            params = {'$filter': f"displayName eq '{list_name}'"}
            response = requests.get(list_url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to find list '{list_name}': {response.status_code} - {response.text}")
                return []
            
            lists_data = response.json()
            if not lists_data.get('value'):
                logger.error(f"[STORAGE] List '{list_name}' not found")
                return []
            
            list_id = lists_data['value'][0]['id']
            
            # Query all list items
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$top': 1000  # Get up to 1000 items
            }
            response = requests.get(items_url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to query list items: {response.status_code} - {response.text}")
                return []
            
            items_data = response.json()
            items = items_data.get('value', [])
            
            # Extract fields from items
            metrics_list = []
            for item in items:
                fields = item.get('fields', {})
                # Parse Properties JSON if present
                properties = {}
                if 'Properties' in fields and fields['Properties']:
                    try:
                        properties = json.loads(fields['Properties'])
                    except:
                        pass
                
                metrics_list.append({
                    'run_id': fields.get('Title', ''),
                    'timestamp': fields.get('RunDateTime', ''),
                    'uploaded_by': fields.get('UploadedBy', ''),
                    'filename': fields.get('FileName', ''),
                    'total_scheduled': fields.get('TotalScheduled', 0),
                    'total_actual': fields.get('TotalActual', 0),
                    'matched': fields.get('Matched', 0),
                    'scheduled_not_billed': fields.get('ScheduledNotBilled', 0),
                    'billed_not_scheduled': fields.get('BilledNotScheduled', 0),
                    'amount_mismatch': fields.get('AmountMismatch', 0),
                    'total_variances': fields.get('TotalVariances', 0),
                    'high_severity': fields.get('HighSeverity', 0),
                    'medium_severity': fields.get('MediumSeverity', 0),
                    'properties': properties
                })
            
            # Sort by timestamp descending (most recent first)
            metrics_list.sort(key=lambda x: x['timestamp'], reverse=True)
            
            logger.info(f"[STORAGE] ✅ Loaded {len(metrics_list)} metrics from SharePoint list")
            return metrics_list
            
        except Exception as e:
            logger.error(f"[STORAGE] Error loading metrics from SharePoint list: {e}", exc_info=True)
            return []
    
    def save_uploaded_file(self, run_id: str, file_path: Path, original_filename: str):
        """Save the original uploaded Excel file."""
        if self.use_sharepoint:
            # Read file and upload to SharePoint
            logger.info(f"[STORAGE] 📁 Saving uploaded file: {original_filename}")
            try:
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                sp_path = f"{run_id}/{original_filename}"
                success = self._upload_binary_file_to_sharepoint(file_content, sp_path)
                if success:
                    logger.info(f"[STORAGE] ✅ Original file saved to SharePoint: {original_filename}")
                else:
                    logger.error(f"[STORAGE] ❌ Failed to save original file: {original_filename}")
            except Exception as e:
                logger.error(f"[STORAGE] ❌ Exception reading/uploading file {original_filename}: {e}", exc_info=True)
        else:
            logger.debug(f"[STORAGE] 💾 Original file already saved locally: {original_filename}")
        # For local storage, file is already saved by views.py to the run directory
    
    def save_run(
        self,
        run_id: str,
        expected_detail: pd.DataFrame,
        actual_detail: pd.DataFrame,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        metadata: Dict[str, Any],
        variance_detail: Optional[pd.DataFrame] = None,
        original_file_path: Optional[Path] = None,
        property_name_map: Optional[Dict[int, str]] = None,
        write_display_snapshots: bool = True,
    ):
        """Save complete audit run to storage."""
        print(f"\n{'='*80}")
        print(f"[STORAGE] ===== PHASE 8: SAVING TO SHAREPOINT =====")
        print(f"{'='*80}")
        logger.info(f"[STORAGE] 💾 Starting save for run: {run_id}")
        print(f"[STORAGE] Target: {'SharePoint' if self.use_sharepoint else 'Local Filesystem'}")
        print(f"[STORAGE] Data volumes:")
        print(f"  - Expected detail: {expected_detail.shape}")
        print(f"  - Actual detail: {actual_detail.shape}")
        print(f"  - Bucket results: {bucket_results.shape}")
        print(f"  - Findings: {findings.shape}")
        if variance_detail is not None:
            print(f"  - Variance detail: {variance_detail.shape}")
        
        # OVERWRITE DISABLED - Keep all runs (no automatic deletion)
        # # Delete the most recent run before saving this one
        # print(f"\n[STORAGE] Checking for previous runs to overwrite...")
        # recent_runs = self.list_runs(limit=1)
        # if recent_runs:
        #     most_recent_run_id = recent_runs[0]['run_id']
        #     print(f"[STORAGE] Found previous run: {most_recent_run_id}")
        #     print(f"[STORAGE] Deleting previous run to overwrite...")
        #     self.delete_run(most_recent_run_id)
        # else:
        #     print(f"[STORAGE] No previous runs found - this will be the first run")
        print(f"\n[STORAGE] Overwrite disabled - keeping all runs")
        
        self.create_run_dir(run_id)

        write_details_async = os.getenv('ASYNC_AUDIT_RESULTS_WRITE', 'false').lower() == 'true'
        write_metrics_async = os.getenv('ASYNC_METRICS_WRITE', 'true').lower() == 'true'
        write_snapshots_async = os.getenv('ASYNC_RUN_DISPLAY_SNAPSHOTS', 'true').lower() == 'true'
        snapshot_validation_async = os.getenv('ASYNC_SNAPSHOT_VALIDATION', 'true').lower() == 'true'
        
        print(f"[SAVE_RUN DEBUG] write_details_async={write_details_async} (env ASYNC_AUDIT_RESULTS_WRITE={os.getenv('ASYNC_AUDIT_RESULTS_WRITE', 'NOT_SET')})")
        print(f"[SAVE_RUN DEBUG] write_metrics_async={write_metrics_async}")
        print(f"[SAVE_RUN DEBUG] write_snapshots_async={write_snapshots_async}")
        
        files_saved = []
        files_failed = []
        stage_timers: Dict[str, float] = {
            'metrics_write_seconds': 0.0,
            'snapshot_filter_seconds': 0.0,
            'snapshot_write_seconds': 0.0,
            'snapshot_validate_seconds': 0.0,
        }
        
        # Save original uploaded file if provided
        if original_file_path and original_file_path.exists():
            print(f"[STORAGE] Step 1/7: Saving original uploaded file...")
            self.save_uploaded_file(run_id, original_file_path, original_file_path.name)
            files_saved.append(original_file_path.name)
            print(f"[STORAGE] ✓ Saved: {original_file_path.name}")
        
        # Check if CSV writes are disabled
        disable_csv_writes = os.getenv('DISABLE_CSV_WRITES', 'false').lower() == 'true'
        
        # Store bucket_results and findings in memory for fast access
        print(f"\n[STORAGE][DEBUG] BEFORE CACHE SAVE:")
        print(f"[STORAGE][DEBUG]   expected_detail type: {type(expected_detail)}, is None: {expected_detail is None}")
        if expected_detail is not None:
            print(f"[STORAGE][DEBUG]   expected_detail shape: {expected_detail.shape}")
            print(f"[STORAGE][DEBUG]   expected_detail columns: {list(expected_detail.columns)[:10]}")
        print(f"[STORAGE][DEBUG]   actual_detail type: {type(actual_detail)}, is None: {actual_detail is None}")
        if actual_detail is not None:
            print(f"[STORAGE][DEBUG]   actual_detail shape: {actual_detail.shape}")
            print(f"[STORAGE][DEBUG]   actual_detail columns: {list(actual_detail.columns)[:10]}")
        
        with self._IN_MEMORY_CACHE_LOCK:
            self._IN_MEMORY_RESULTS_CACHE[run_id] = {
                'bucket_results': bucket_results.copy(),
                'findings': findings.copy(),
                'expected_detail': expected_detail.copy() if expected_detail is not None else pd.DataFrame(),
                'actual_detail': actual_detail.copy() if actual_detail is not None else pd.DataFrame(),
                'variance_detail': variance_detail.copy() if variance_detail is not None and len(variance_detail) > 0 else pd.DataFrame(),
                'timestamp': datetime.now().isoformat()
            }
        print(f"[STORAGE] ✓ Cached complete run data in memory for {run_id}")
        print(f"[STORAGE][DEBUG] IN-MEMORY CACHE NOW CONTAINS:")
        print(f"[STORAGE][DEBUG]   expected_detail: {len(self._IN_MEMORY_RESULTS_CACHE[run_id]['expected_detail'])} rows")
        print(f"[STORAGE][DEBUG]   actual_detail: {len(self._IN_MEMORY_RESULTS_CACHE[run_id]['actual_detail'])} rows")
        logger.info(f"[STORAGE] Cached {len(bucket_results)} bucket results, {len(findings)} findings, and detail data in memory")
        
        if disable_csv_writes:
            print(f"\n[STORAGE] Step 2/7: CSV writes DISABLED (DISABLE_CSV_WRITES=true)")
            print(f"[STORAGE] Step 3/7: CSV writes DISABLED (DISABLE_CSV_WRITES=true)")
            logger.info(f"[STORAGE] CSV writes disabled via DISABLE_CSV_WRITES environment variable")
        else:
            print(f"\n[STORAGE] Step 2/7: Saving CSV input files...")
            logger.info(f"[STORAGE] 📊 CSV input file writes...")
            try:
                self._save_dataframe(expected_detail, run_id, "inputs_normalized/expected_detail.csv")
                print(f"[STORAGE] ✓ Saved: expected_detail.csv ({len(expected_detail)} rows)")
                files_saved.append("expected_detail.csv")
                
                self._save_dataframe(actual_detail, run_id, "inputs_normalized/actual_detail.csv")
                print(f"[STORAGE] ✓ Saved: actual_detail.csv ({len(actual_detail)} rows)")
                files_saved.append("actual_detail.csv")
            except Exception as e:
                print(f"[STORAGE] ⚠️  CSV input file writes failed: {e}")
                logger.warning(f"[STORAGE] Failed to write CSV input files: {e}")
            
            print(f"\n[STORAGE] Step 3/7: Saving CSV output files...")
            logger.info(f"[STORAGE] 📈 CSV output file writes...")
            try:
                self._save_dataframe(bucket_results, run_id, "outputs/bucket_results.csv")
                print(f"[STORAGE] ✓ Saved: bucket_results.csv ({len(bucket_results)} rows)")
                files_saved.append("bucket_results.csv")
                
                self._save_dataframe(findings, run_id, "outputs/findings.csv")
                print(f"[STORAGE] ✓ Saved: findings.csv ({len(findings)} rows)")
                files_saved.append("findings.csv")
                
                # Store bucket_results and findings in memory for fast access
                with self._IN_MEMORY_CACHE_LOCK:
                    self._IN_MEMORY_RESULTS_CACHE[run_id] = {
                        'bucket_results': bucket_results.copy(),
                        'findings': findings.copy(),
                        'timestamp': datetime.now().isoformat()
                    }
                print(f"[STORAGE] ✓ Cached results in memory for run {run_id}")
                logger.info(f"[STORAGE] Cached {len(bucket_results)} bucket results and {len(findings)} findings in memory")
                
                # Save variance detail if provided
                if variance_detail is not None and len(variance_detail) > 0:
                    self._save_dataframe(variance_detail, run_id, "outputs/variance_detail.csv")
                    print(f"[STORAGE] ✓ Saved: variance_detail.csv ({len(variance_detail)} rows)")
                    files_saved.append("variance_detail.csv")
            except Exception as e:
                print(f"[STORAGE] ⚠️  CSV output file writes failed: {e}")
                logger.warning(f"[STORAGE] Failed to write CSV output files: {e}")
        
        # Save metadata
        print(f"\n[STORAGE] Step 4/7: Saving metadata...")
        logger.info(f"[STORAGE] 📋 Saving metadata...")
        # Include property_name_map in metadata for baseline overlay merging
        if property_name_map:
            metadata = dict(metadata)  # Make a copy to avoid mutating caller's dict
            metadata['property_name_map'] = {str(k): v for k, v in property_name_map.items()}
        self._save_json(metadata, run_id, "run_meta.json")
        print(f"[STORAGE] ✓ Saved: run_meta.json")
        files_saved.append("run_meta.json")
        
        # COMMENTED OUT: Write metrics to SharePoint list (don't fail save if this fails)
        # Testing what breaks when AuditRunMetrics writes are disabled
        print(f"\n[STORAGE] Step 5/7: Writing metrics to SharePoint List (Audit Run Metrics)...")
        print(f"[STORAGE] ⚠️  SKIPPED - Metrics write is commented out for testing")
        stage_timers['metrics_write_seconds'] = 0.0
        # try:
        #     can_write_sharepoint_lists = self._can_use_sharepoint_lists()
        #     if write_metrics_async and can_write_sharepoint_lists:
        #         metrics_started = perf_counter()
        #         print(f"[STORAGE] 🚀 Dispatching async metrics write...")
        #         metrics_thread = threading.Thread(
        #             target=self._write_metrics_to_sharepoint_list_async,
        #             args=(run_id, bucket_results, findings, dict(metadata)),
        #             daemon=True,
        #             name=f"metrics-write-{run_id}",
        #         )
        #         metrics_thread.start()
        #         stage_timers['metrics_write_seconds'] = float(perf_counter() - metrics_started)
        #         print(f"[STORAGE] ✓ Metrics write dispatched (async mode)")
        #     else:
        #         metrics_started = perf_counter()
        #         print(f"[STORAGE] Writing metrics synchronously...")
        #         self._write_metrics_to_sharepoint_list(run_id, bucket_results, findings, metadata)
        #         stage_timers['metrics_write_seconds'] = float(perf_counter() - metrics_started)
        #         print(f"[STORAGE] ✓ Metrics written in {stage_timers['metrics_write_seconds']:.2f}s")
        # except Exception as e:
        #     print(f"[STORAGE] ⚠️  Metrics write failed: {e}")
        #     logger.warning(f"[STORAGE] Failed to write metrics to SharePoint list: {e}")

        # Write static display snapshots (portfolio/property/lease) for fast UI loads.
        print(f"\n[STORAGE] Step 6/7: Writing display snapshots (portfolio/property/lease views)...")
        try:
            can_write_sharepoint_lists = self._can_use_sharepoint_lists()
            if write_snapshots_async and can_write_sharepoint_lists:
                snapshot_dispatch_started = perf_counter()
                print(f"[STORAGE] 🚀 Dispatching async display snapshot write...")
                _run_scope_type = str((metadata.get('run_scope') or {}).get('type') or '')
                snapshot_thread = threading.Thread(
                    target=self._write_run_display_snapshots_async,
                    args=(run_id, bucket_results),
                    kwargs={
                        'actual_detail': actual_detail,
                        'expected_detail': expected_detail,
                        'property_name_map': property_name_map,
                        'snapshot_validation_async': snapshot_validation_async,
                        'run_scope_type': _run_scope_type,
                    },
                    daemon=True,
                    name=f"snapshot-write-{run_id}",
                )
                snapshot_thread.start()
                stage_timers['snapshot_write_seconds'] = float(perf_counter() - snapshot_dispatch_started)
                print(f"[STORAGE] ✓ Display snapshot write dispatched (async mode)")
            else:
                snapshot_write_ok = self._write_run_display_snapshots_to_sharepoint_list(
                    run_id,
                    bucket_results,
                    actual_detail=actual_detail,
                    expected_detail=expected_detail,
                    property_name_map=property_name_map,
                    stage_timers=stage_timers,
                    run_scope_type=str((metadata.get('run_scope') or {}).get('type') or ''),
                )
                if snapshot_write_ok:
                    print(f"[STORAGE] ✓ Display snapshots written successfully")
                    if snapshot_validation_async and self._can_use_sharepoint_lists():
                        validate_started = perf_counter()
                        print(f"[STORAGE] 🚀 Dispatching async snapshot validation...")
                        validate_thread = threading.Thread(
                            target=self._validate_run_display_snapshots_async,
                            args=(run_id, bucket_results),
                            daemon=True,
                            name=f"snapshot-validate-{run_id}",
                        )
                        validate_thread.start()
                        stage_timers['snapshot_validate_seconds'] = float(perf_counter() - validate_started)
                        print(f"[STORAGE] ✓ Snapshot validation dispatched (async mode)")
                    else:
                        validate_started = perf_counter()
                        print(f"[STORAGE] Validating snapshots synchronously...")
                        validation = self.validate_run_display_snapshots(run_id, bucket_results)
                        stage_timers['snapshot_validate_seconds'] = float(perf_counter() - validate_started)
                        if validation.get('ok'):
                            print(
                                f"[STORAGE] ✓ Snapshot validation passed: "
                                f"portfolio={validation['actual']['portfolio']}, "
                                f"property={validation['actual']['property']}, "
                                f"lease={validation['actual']['lease']}"
                            )
                            logger.info(
                                f"[STORAGE] ✅ Snapshot validation passed for {run_id}: "
                                f"portfolio={validation['actual']['portfolio']}, "
                                f"property={validation['actual']['property']}, "
                                f"lease={validation['actual']['lease']}"
                            )
                        else:
                            print(f"[STORAGE] ⚠️  Snapshot validation warnings: {validation.get('errors', [])}")
                            logger.warning(
                                f"[STORAGE] Snapshot validation warnings for {run_id}: {validation.get('errors', [])}"
                            )
        except Exception as e:
            print(f"[STORAGE] ⚠️  Display snapshots write failed: {e}")
            logger.warning(f"[STORAGE] Failed to write run display snapshots to SharePoint list: {e}")

        # COMMENTED OUT: Write detailed results to SharePoint list (AuditRuns2)
        # Testing whether app works without AuditRuns2 writes (using RunDisplaySnapshots only)
        print(f"\n[STORAGE] Step 7/7: Writing detailed results to SharePoint List (AuditRuns2)...")
        print(f"[STORAGE] ⚠️  SKIPPED - AuditRuns2 write is commented out for testing")
        # print(f"[STEP 7 DEBUG] write_details_async={write_details_async}, can_write_sharepoint_lists={can_write_sharepoint_lists}")
        # print(f"[STEP 7 DEBUG] access_token={'SET' if self.access_token else 'MISSING'}, sharepoint_site_url={self.sharepoint_site_url}")
        # 
        # if write_details_async and can_write_sharepoint_lists:
        #     print(f"[STORAGE] 🚀 Dispatching async detailed results write...")
        #     details_thread = threading.Thread(
        #         target=self._write_results_to_sharepoint_list_async,
        #         args=(run_id, bucket_results, findings),
        #         kwargs={'actual_detail': actual_detail, 'expected_detail': expected_detail},
        #         daemon=True,
        #         name=f"details-write-{run_id}",
        #     )
        #     details_thread.start()
        #     print(f"[STORAGE] ✓ Detailed results write dispatched (async mode)")
        # elif can_write_sharepoint_lists:
        #     print(f"[STORAGE] Writing detailed results synchronously...")
        #     write_ok = self._write_results_to_sharepoint_list(
        #         run_id,
        #         bucket_results,
        #         findings,
        #         actual_detail=actual_detail,
        #         expected_detail=expected_detail,
        #     )
        #     if write_ok:
        #         print(f"[STORAGE] ✓ Detailed results written successfully")
        #     else:
        #         print(f"[STORAGE] ⚠️  Detailed results write failed")
        # else:
        #     print(f"[STORAGE] ⚠️  SharePoint lists not available, skipping detailed results write")
        #     print(f"[STEP 7 DEBUG] Why? write_details_async={write_details_async}, can_write_sharepoint_lists={can_write_sharepoint_lists}")

        logger.info(
            f"[STORAGE TIMER] run_id={run_id} "
            f"metrics_write_seconds={stage_timers.get('metrics_write_seconds', 0.0):.2f} "
            f"snapshot_filter_seconds={stage_timers.get('snapshot_filter_seconds', 0.0):.2f} "
            f"snapshot_write_seconds={stage_timers.get('snapshot_write_seconds', 0.0):.2f} "
            f"snapshot_validate_seconds={stage_timers.get('snapshot_validate_seconds', 0.0):.2f} "
            f"metrics_mode={'async' if write_metrics_async else 'sync'} "
            f"snapshots_mode={'async' if write_snapshots_async else 'sync'} "
            f"snapshot_validation_mode={'async' if snapshot_validation_async else 'sync'}"
        )
        
        print(f"\n{'='*80}")
        logger.info(f"[STORAGE] ✅ Successfully saved run {run_id} - {len(files_saved)} files")
        print(f"[STORAGE] ===== SAVE COMPLETE =====")
        print(f"[STORAGE] ✅ Successfully saved {len(files_saved)} files for run {run_id}")
        print(f"[STORAGE] Performance:")
        print(f"  - Metrics write: {stage_timers.get('metrics_write_seconds', 0.0):.2f}s")
        print(f"  - Snapshot filter: {stage_timers.get('snapshot_filter_seconds', 0.0):.2f}s")
        print(f"  - Snapshot write: {stage_timers.get('snapshot_write_seconds', 0.0):.2f}s")
        print(f"  - Snapshot validate: {stage_timers.get('snapshot_validate_seconds', 0.0):.2f}s")
        print(f"{'='*80}\n")
        if self.use_sharepoint:
            logger.info(f"[STORAGE] 📍 Location: SharePoint/{self.library_name}/{run_id}")
        else:
            logger.info(f"[STORAGE] 📍 Location: {self.base_dir}/{run_id}")
    
    def load_run(self, run_id: str) -> Dict[str, Any]:
        """Load complete audit run from memory, SharePoint lists (AuditRuns2), or CSVs."""
        logger.info(f"[STORAGE] Loading run {run_id}")
        
        # Try in-memory cache first (fastest for recently executed runs)
        expected_detail = pd.DataFrame()
        actual_detail = pd.DataFrame()
        bucket_results = pd.DataFrame()
        findings = pd.DataFrame()
        variance_detail = pd.DataFrame()
        loaded_from_memory = False
        
        with self._IN_MEMORY_CACHE_LOCK:
            if run_id in self._IN_MEMORY_RESULTS_CACHE:
                cache_data = self._IN_MEMORY_RESULTS_CACHE[run_id]
                expected_detail = cache_data.get('expected_detail', pd.DataFrame()).copy()
                actual_detail = cache_data.get('actual_detail', pd.DataFrame()).copy()
                bucket_results = cache_data.get('bucket_results', pd.DataFrame()).copy()
                findings = cache_data.get('findings', pd.DataFrame()).copy()
                variance_detail = cache_data.get('variance_detail', pd.DataFrame()).copy()
                loaded_from_memory = True
                logger.info(f"[STORAGE] ✅ Loaded run {run_id} from memory cache: {len(bucket_results)} buckets, {len(findings)} findings")
        
        # If not in memory, load from SharePoint lists (AuditRuns2) or CSVs
        if not loaded_from_memory or bucket_results.empty or findings.empty:
            logger.info(f"[STORAGE] Loading run {run_id} from SharePoint lists (AuditRuns2) or CSVs")
            
            # Load bucket_results (tries AuditRuns2 list first, then snapshots, then CSV)
            bucket_results = self.load_bucket_results(run_id)
            if bucket_results.empty:
                logger.warning(f"[STORAGE] No bucket_results found for run {run_id}")
            
            # Load findings (tries AuditRuns2 list first, then snapshots, then CSV)
            findings = self.load_findings(run_id)
            if findings.empty:
                logger.warning(f"[STORAGE] No findings found for run {run_id}")
            
            # Load variance_detail from CSV
            variance_detail = self.load_variance_detail(run_id)
            
            # Load expected_detail and actual_detail from CSV
            expected_detail = self._load_dataframe(run_id, "inputs_normalized/expected_detail.csv")
            if expected_detail is None:
                expected_detail = pd.DataFrame()
            
            actual_detail = self._load_dataframe(run_id, "inputs_normalized/actual_detail.csv")
            if actual_detail is None:
                actual_detail = pd.DataFrame()
            
            logger.info(
                f"[STORAGE] ✅ Loaded run {run_id} from persistent storage: "
                f"{len(bucket_results)} buckets, {len(findings)} findings, "
                f"{len(expected_detail)} expected rows, {len(actual_detail)} actual rows"
            )
        
        # Bucket results and findings are mandatory
        if bucket_results.empty or findings.empty:
            raise ValueError(f"Run {run_id} incomplete or not found (missing bucket_results or findings)")

        # Normalize dataframes
        for df in [expected_detail, actual_detail, bucket_results, findings]:
            if not df.empty:
                self._normalize_loaded_dataframe(df)
        
        # Also convert dates in variance_detail if loaded
        if variance_detail is not None and not variance_detail.empty:
            date_columns = ['AUDIT_MONTH', 'PERIOD_START', 'PERIOD_END', 'POST_DATE', 'audit_month']
            for col in date_columns:
                if col in variance_detail.columns:
                    series = pd.to_datetime(variance_detail[col], errors='coerce')
                    try:
                        series = series.dt.tz_localize(None)
                    except Exception:
                        pass
                    variance_detail[col] = series
        
        # Load metadata and extract property_name_map if present
        metadata = self.load_metadata(run_id)
        property_name_map = {}
        if metadata.get('property_name_map'):
            # Convert string keys back to integers
            property_name_map = {int(k): v for k, v in metadata['property_name_map'].items()}
        
        # Populate missing property names from detail DataFrames (backfill for incomplete metadata)
        def _extract_property_names_from_detail(detail_df: pd.DataFrame, target_map: dict) -> None:
            if detail_df is None or len(detail_df) == 0:
                return
            
            property_col = 'PROPERTY_ID' if 'PROPERTY_ID' in detail_df.columns else 'property_id'
            name_col = 'PROPERTY_NAME' if 'PROPERTY_NAME' in detail_df.columns else 'property_name'
            
            if property_col not in detail_df.columns or name_col not in detail_df.columns:
                return
            
            for _, row in detail_df[[property_col, name_col]].dropna().iterrows():
                property_id_int = self._safe_int(row.get(property_col))
                property_name = str(row.get(name_col)).strip()
                if property_id_int is None or not property_name or property_name.lower() == 'nan':
                    continue
                if property_id_int not in target_map:
                    target_map[property_id_int] = property_name
                    logger.debug(
                        f"[STORAGE] Backfilled property name from data: {property_id_int} -> {property_name}"
                    )
        
        # Extract from actual_detail first (most authoritative), then expected_detail
        _extract_property_names_from_detail(actual_detail, property_name_map)
        _extract_property_names_from_detail(expected_detail, property_name_map)
        
        return {
            "expected_detail": expected_detail,
            "actual_detail": actual_detail,
            "bucket_results": bucket_results,
            "findings": findings,
            "variance_detail": variance_detail,
            "metadata": metadata,
            "property_name_map": property_name_map if property_name_map else None
        }
    
    def load_metadata(self, run_id: str) -> Dict[str, Any]:
        """Load run metadata."""
        metadata = self._load_json(run_id, "run_meta.json")
        if metadata is None:
            raise ValueError(f"Metadata not found for run {run_id}")
        return metadata
    
    def list_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent audit runs from RunDisplaySnapshots."""
        runs = []
        logger.info(f"[STORAGE] 🔍 list_runs() called with limit={limit}")
        
        # Query RunDisplaySnapshots for portfolio-level snapshots (1 per run)
        try:
            if not self._can_use_sharepoint_lists():
                logger.warning("[STORAGE] Cannot list runs - SharePoint lists not accessible")
                return runs
                
            site_id = self._get_site_id()
            if not site_id:
                logger.warning("[STORAGE] Cannot list runs - site_id not available")
                return runs
                
            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
                logger.warning("[STORAGE] Cannot list runs - RunDisplaySnapshots list not found")
                return runs
            
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            params = {
                '$expand': 'fields',
                '$filter': "fields/ScopeType eq 'portfolio'",
                '$orderby': 'fields/CreatedAt desc',
                '$top': max(1, int(limit))
            }
            
            logger.info(f"[LIST_RUNS_DEBUG] Querying {items_url} with filter: {params['$filter']}")
            response = requests.get(items_url, headers=headers, params=params, timeout=30)
            logger.info(f"[LIST_RUNS_DEBUG] Response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(
                    f"[LIST_RUNS_DEBUG] Failed query response: {response.text[:500]}"
                )
                logger.error(
                    f"[STORAGE] Failed to list runs from RunDisplaySnapshots: "
                    f"{response.status_code} - {response.text}"
                )
                return runs
            
            items = response.json().get('value', [])
            logger.info(f"[LIST_RUNS_DEBUG] Query returned {len(items)} portfolio snapshots")
            
            for item in response.json().get('value', []):
                fields = item.get('fields', {})
                run_id = fields.get('RunId')
                if not run_id:
                    continue
                    
                created_at = fields.get('CreatedAt', 'Unknown')
                run_scope_type = fields.get('RunScopeType') or fields.get('run_scope_type') or 'Property'
                
                run_row = {
                    "run_id": run_id,
                    "timestamp": created_at,
                    "audit_period": {},
                    "run_type": run_scope_type
                }
                runs.append(run_row)
            
            logger.info(f"[STORAGE] ✅ list_runs() returning {len(runs)} runs from RunDisplaySnapshots")
            
        except Exception as e:
            logger.error(f"[STORAGE] Error listing runs from RunDisplaySnapshots: {e}", exc_info=True)

        return runs
    
    def delete_run(self, run_id: str) -> bool:
        """Delete a run from all storage locations (SharePoint lists, document library, and local files)."""
        if not run_id:
            logger.warning("[STORAGE] delete_run called with empty run_id")
            return False
            
        logger.info(f"[STORAGE] 🗑️ Deleting run: {run_id}")
        deleted_from = []
        
        try:
            # Delete from SharePoint lists if available
            if self._can_use_sharepoint_lists():
                site_id = self._get_site_id()
                if site_id:
                    headers = {
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/json',
                        'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
                    }
                    
                    # Delete from AuditRuns2 list
                    try:
                        list_id = self._get_audit_results_list_id() or self._get_sharepoint_list_id('AuditRuns2')
                        if list_id:
                            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
                            params = {'$select': 'id', '$expand': 'fields', '$filter': f"fields/RunId eq '{run_id}'", '$top': 5000}
                            response = requests.get(items_url, headers=headers, params=params, timeout=60)
                            if response.status_code == 200:
                                items = response.json().get('value', [])
                                for item in items:
                                    delete_response = requests.delete(f"{items_url}/{item['id']}", headers=headers, timeout=30)
                                    if delete_response.status_code not in [200, 204]:
                                        logger.warning(f"[STORAGE] Failed to delete AuditRuns2 item {item['id']}: {delete_response.status_code}")
                                if items:
                                    deleted_from.append(f"AuditRuns2 ({len(items)} items)")
                                    logger.info(f"[STORAGE] Deleted {len(items)} items from AuditRuns2")
                    except Exception as e:
                        logger.warning(f"[STORAGE] Error deleting from AuditRuns2: {e}")
                    
                    # Delete from Audit Run Metrics list
                    try:
                        list_id = self._get_sharepoint_list_id('Audit Run Metrics')
                        if list_id:
                            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
                            params = {'$select': 'id', '$expand': 'fields', '$filter': f"fields/Title eq '{run_id}'", '$top': 5000}
                            response = requests.get(items_url, headers=headers, params=params, timeout=60)
                            if response.status_code == 200:
                                items = response.json().get('value', [])
                                for item in items:
                                    delete_response = requests.delete(f"{items_url}/{item['id']}", headers=headers, timeout=30)
                                    if delete_response.status_code not in [200, 204]:
                                        logger.warning(f"[STORAGE] Failed to delete Metrics item {item['id']}: {delete_response.status_code}")
                                if items:
                                    deleted_from.append(f"Audit Run Metrics ({len(items)} items)")
                                    logger.info(f"[STORAGE] Deleted {len(items)} items from Audit Run Metrics")
                    except Exception as e:
                        logger.warning(f"[STORAGE] Error deleting from Audit Run Metrics: {e}")
                    
                    # Delete from RunDisplaySnapshots list
                    try:
                        list_id = self._get_run_display_snapshots_list_id()
                        if list_id:
                            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
                            params = {'$select': 'id', '$expand': 'fields', '$filter': f"fields/RunId eq '{run_id}'", '$top': 5000}
                            response = requests.get(items_url, headers=headers, params=params, timeout=60)
                            if response.status_code == 200:
                                items = response.json().get('value', [])
                                for item in items:
                                    delete_response = requests.delete(f"{items_url}/{item['id']}", headers=headers, timeout=30)
                                    if delete_response.status_code not in [200, 204]:
                                        logger.warning(f"[STORAGE] Failed to delete RunDisplaySnapshots item {item['id']}: {delete_response.status_code}")
                                if items:
                                    deleted_from.append(f"RunDisplaySnapshots ({len(items)} items)")
                                    logger.info(f"[STORAGE] Deleted {len(items)} items from RunDisplaySnapshots")
                    except Exception as e:
                        logger.warning(f"[STORAGE] Error deleting from RunDisplaySnapshots: {e}")
            
            # Delete from SharePoint document library if using SharePoint storage
            if self.use_sharepoint:
                try:
                    _, drive_id = self._get_site_and_drive_id()
                    if drive_id:
                        folder_path = f"/{run_id}"
                        folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{folder_path}"
                        headers = {'Authorization': f'Bearer {self.access_token}'}
                        delete_response = requests.delete(folder_url, headers=headers, timeout=60)
                        if delete_response.status_code in [200, 204]:
                            deleted_from.append(f"Document Library folder")
                            logger.info(f"[STORAGE] Deleted folder from document library: {run_id}")
                        elif delete_response.status_code != 404:
                            logger.warning(f"[STORAGE] Failed to delete document library folder: {delete_response.status_code}")
                except Exception as e:
                    logger.warning(f"[STORAGE] Error deleting from document library: {e}")
            
            # Delete from local filesystem
            else:
                local_path = self.base_dir / run_id
                if local_path.exists():
                    import shutil
                    shutil.rmtree(local_path)
                    deleted_from.append(f"Local filesystem")
                    logger.info(f"[STORAGE] Deleted local folder: {local_path}")
            
            if deleted_from:
                logger.info(f"[STORAGE] ✅ Successfully deleted run {run_id} from: {', '.join(deleted_from)}")
                return True
            else:
                logger.info(f"[STORAGE] No data found to delete for run {run_id}")
                return False
                
        except Exception as e:
            logger.error(f"[STORAGE] Error deleting run {run_id}: {e}", exc_info=True)
            return False
    
    def get_run_exists(self, run_id: str) -> bool:
        """Check if run exists."""
        if self.use_sharepoint:
            metadata = self._load_json(run_id, "run_meta.json")
            return metadata is not None
        else:
            return (self.base_dir / run_id).exists()
    
    @staticmethod
    def calculate_file_hash(file_path: Path) -> str:
        """Calculate SHA256 hash of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    @staticmethod
    def generate_run_id() -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"run_{timestamp}"
    
    def create_metadata(
        self,
        run_id: str,
        file_path: Path,
        config_version: str = "v1"
    ) -> Dict[str, Any]:
        """Create run metadata."""
        return {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "config_version": config_version,
            "file_name": file_path.name,
            "file_hash": self.calculate_file_hash(file_path),
            "file_size": file_path.stat().st_size
        }
