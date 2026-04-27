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
    
    def __init__(self, base_dir: Path, use_sharepoint: bool = False, sharepoint_site_url: str = None, 
                 library_name: str = None, access_token: str = None):
        self.base_dir = Path(base_dir)
        self.use_sharepoint = use_sharepoint and sharepoint_site_url and library_name
        self.sharepoint_site_url = sharepoint_site_url.rstrip('/') if sharepoint_site_url else None
        self.library_name = library_name
        self.access_token = access_token
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
        if list_name in self._list_ids:
            return self._list_ids[list_name]

        site_id = self._get_site_id()
        if not site_id:
            logger.error("[STORAGE] Cannot resolve list ID - site ID not found")
            return None

        global_list_key = f"{site_id}|{list_name}"
        if global_list_key in self._GLOBAL_LIST_ID_CACHE:
            list_id = self._GLOBAL_LIST_ID_CACHE[global_list_key]
            self._list_ids[list_name] = list_id
            return list_id

        list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        params = {'$filter': f"displayName eq '{list_name}'"}
        response = requests.get(list_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"[STORAGE] Failed to find list '{list_name}': {response.status_code} - {response.text}")
            return None

        lists_data = response.json()
        if not lists_data.get('value'):
            logger.error(f"[STORAGE] List '{list_name}' not found")
            return None

        list_id = lists_data['value'][0]['id']
        self._list_ids[list_name] = list_id
        self._GLOBAL_LIST_ID_CACHE[global_list_key] = list_id
        logger.debug(f"[STORAGE] Resolved SharePoint list '{list_name}' id: {list_id}")
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
        """Resolve audit results list id with preferred name first and legacy fallback."""
        preferred_names = ["AuditRuns", "Audit Run Results"]
        for name in preferred_names:
            list_id = self._get_sharepoint_list_id(name)
            if list_id:
                if name != "AuditRuns":
                    logger.warning(f"[STORAGE] Using legacy list name '{name}'. Consider renaming to 'AuditRuns'.")
                return list_id
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
        matched_mask = status_series == 'matched'

        total_buckets = int(len(dataframe))
        matched_buckets = int(matched_mask.sum())
        exception_rows = dataframe[~matched_mask].copy() if status_column in dataframe.columns else dataframe.copy()

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

    def _resolve_snapshot_optional_field_names(self, site_id: str, list_id: str) -> Dict[str, Optional[str]]:
        """Resolve optional snapshot fields used by snapshot-only portfolio rendering."""
        field_candidates = {
            'property_name': ['PropertyNameStatic', 'PropertyName'],
            'total_variance': ['TotalVarianceStatic'],
            'total_lease_intervals': ['TotalLeaseIntervalStatic'],
        }
        resolved = {
            'property_name': None,
            'total_variance': None,
            'total_lease_intervals': None,
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

        def _make_row(scope_type: str, subset: pd.DataFrame, property_id: Any = None, lease_interval_id: Any = None) -> Dict[str, Any]:
            metrics = self._calculate_static_metrics(subset)
            property_id_int = self._safe_int(property_id)
            lease_interval_id_int = self._safe_int(lease_interval_id)
            property_name = resolved_property_name_map.get(property_id_int, f"Property {property_id_int}") if property_id_int is not None else None

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

            total_lease_intervals = int(subset[lease_column].nunique()) if lease_column in subset.columns else 0

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

            return row_payload

        # Portfolio-level snapshot
        rows.append(_make_row('portfolio', bucket_results))

        # Property-level snapshots
        if property_column in bucket_results.columns:
            for property_id, property_df in bucket_results.groupby(property_column, dropna=False):
                rows.append(_make_row('property', property_df, property_id=property_id))

                # Lease-level snapshots nested by property
                if lease_column in property_df.columns:
                    for lease_interval_id, lease_df in property_df.groupby(lease_column, dropna=False):
                        rows.append(
                            _make_row(
                                'lease',
                                lease_df,
                                property_id=property_id,
                                lease_interval_id=lease_interval_id,
                            )
                        )

        return rows

    def _write_run_display_snapshots_to_sharepoint_list(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        actual_detail: Optional[pd.DataFrame] = None,
        expected_detail: Optional[pd.DataFrame] = None,
        property_name_map: Optional[Dict[int, str]] = None,
        stage_timers: Optional[Dict[str, float]] = None,
    ) -> bool:
        """Persist static portfolio/property/lease display snapshots to RunDisplaySnapshots list."""
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint lists unavailable; skipping RunDisplaySnapshots write")
            return False

        try:
            site_id = self._get_site_id()
            if not site_id:
                return False

            list_id = self._get_run_display_snapshots_list_id()
            if not list_id:
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
            if stage_timers is not None:
                stage_timers['snapshot_filter_seconds'] = float(perf_counter() - snapshot_filter_started)

            exception_count_field_name = self._resolve_snapshot_exception_count_field_name(site_id, list_id)
            optional_field_names = self._resolve_snapshot_optional_field_names(site_id, list_id)
            snapshot_rows = self._build_run_display_snapshot_rows(
                run_id,
                filtered_bucket_results,
                exception_count_field_name=exception_count_field_name,
                optional_field_names=optional_field_names,
                actual_detail=actual_detail,
                expected_detail=expected_detail,
                property_name_map=property_name_map,
            )
            
            # Log snapshot details
            property_snapshots = [row for row in snapshot_rows if row.get('ScopeType') == 'property']
            property_ids_in_snapshots = [row.get('PropertyId') for row in property_snapshots]
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

            logger.info(f"[STORAGE] ✅ Wrote RunDisplaySnapshots rows for {run_id}: rows={created}")
            return True
        except Exception as e:
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

        batch_concurrency = 2
        batch_concurrency_source = 'default:2'
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

        batch_concurrency = max(1, min(4, int(batch_concurrency)))
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

                for offset, payload in enumerate(chunk):
                    row_idx = start + offset
                    response_item = response_map.get(str(row_idx))
                    if not response_item:
                        logger.warning(
                            f"[STORAGE] Missing batch response for {context_label} row {row_idx}; "
                            "falling back to single post"
                        )
                        if _post_single_local(payload, row_idx):
                            created_local += 1
                        continue

                    status_code = response_item.get('status')
                    if status_code in [200, 201]:
                        created_local += 1
                        continue

                    logger.warning(
                        f"[STORAGE] Batch item failed for {context_label} row {row_idx}: "
                        f"{status_code} - {response_item.get('body')}; retrying individually"
                    )
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
        """Background wrapper for detailed AuditRuns list persistence."""
        try:
            logger.info(f"[STORAGE] 🚀 Background AuditRuns write started for {run_id}")
            self._write_results_to_sharepoint_list(
                run_id,
                bucket_results,
                findings,
                actual_detail=actual_detail,
                expected_detail=expected_detail,
            )
            logger.info(f"[STORAGE] ✅ Background AuditRuns write finished for {run_id}")
        except Exception as e:
            logger.error(f"[STORAGE] Background AuditRuns write failed for {run_id}: {e}", exc_info=True)
    
    def _write_metrics_to_sharepoint_list_async(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        metadata: Dict[str, Any],
    ) -> None:
        """Background wrapper for metrics list persistence."""
        try:
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
    ) -> None:
        """Background wrapper for RunDisplaySnapshots persistence and optional validation."""
        try:
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
            )
            if snapshot_write_ok:
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
                logger.warning(f"[STORAGE] Background RunDisplaySnapshots write skipped/failed for {run_id}")
        except Exception as e:
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
            exception_count = fields.get('ExceptionCountStatic')
            if exception_count is None:
                exception_count = fields.get('ExceptionCountStatistic')

            snapshot = {
                'snapshot_key': fields.get('SnapshotKey'),
                'run_id': fields.get('RunId', run_id),
                'scope_type': fields.get('ScopeType', scope_type),
                'property_id': fields.get('PropertyId'),
                'lease_interval_id': fields.get('LeaseIntervalId'),
                'property_name': fields.get('PropertyNameStatic') or fields.get('PropertyName'),
                'exception_count': int(float(exception_count or 0)),
                'undercharge': float(fields.get('UnderchargeStatic') or 0),
                'overcharge': float(fields.get('OverchargeStatic') or 0),
                'total_variance': float(fields.get('TotalVarianceStatic') or 0),
                'total_lease_intervals': int(float(fields.get('TotalLeaseIntervalStatic') or 0)),
                'match_rate': float(fields.get('MatchRateStatic') or 0),
                'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
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
                lease_interval_id = self._safe_int(fields.get('LeaseIntervalId'))
                if lease_interval_id is None:
                    continue

                exception_count = fields.get('ExceptionCountStatic')
                if exception_count is None:
                    exception_count = fields.get('ExceptionCountStatistic')

                undercharge = float(fields.get('UnderchargeStatic') or 0)
                overcharge = float(fields.get('OverchargeStatic') or 0)

                snapshot_map[lease_interval_id] = {
                    'snapshot_key': fields.get('SnapshotKey'),
                    'run_id': fields.get('RunId', run_id),
                    'scope_type': fields.get('ScopeType', scope_type),
                    'property_id': self._safe_int(fields.get('PropertyId')),
                    'lease_interval_id': lease_interval_id,
                    'property_name': fields.get('PropertyNameStatic') or fields.get('PropertyName'),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(fields.get('TotalVarianceStatic') or (undercharge + overcharge)),
                    'total_lease_intervals': int(float(fields.get('TotalLeaseIntervalStatic') or 1)),
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

                exception_count = fields.get('ExceptionCountStatic')
                if exception_count is None:
                    exception_count = fields.get('ExceptionCountStatistic')

                property_id_int = self._safe_int(fields.get('PropertyId'))
                lease_interval_id_int = self._safe_int(fields.get('LeaseIntervalId'))
                undercharge = float(fields.get('UnderchargeStatic') or 0)
                overcharge = float(fields.get('OverchargeStatic') or 0)

                snapshot_rows.append({
                    'snapshot_key': fields.get('SnapshotKey'),
                    'run_id': fields.get('RunId', run_id),
                    'scope_type': fields.get('ScopeType', scope_type),
                    'property_id': property_id_int,
                    'lease_interval_id': lease_interval_id_int,
                    'property_name': fields.get('PropertyNameStatic') or fields.get('PropertyName'),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(fields.get('TotalVarianceStatic') or (undercharge + overcharge)),
                    'match_rate': float(fields.get('MatchRateStatic') or 0),
                    'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                    'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
                    'total_lease_intervals': int(float(fields.get('TotalLeaseIntervalStatic') or lease_counts_by_property.get(property_id_int, 0) or 0))
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
                    'property_id': property_id_int,
                    'property_name': fields.get('PropertyNameStatic') or fields.get('PropertyName'),
                    'exception_count': int(float(exception_count or 0)),
                    'undercharge': undercharge,
                    'overcharge': overcharge,
                    'total_variance': float(fields.get('TotalVarianceStatic') or (undercharge + overcharge)),
                    'match_rate': float(fields.get('MatchRateStatic') or 0),
                    'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                    'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
                    'total_lease_intervals': int(float(fields.get('TotalLeaseIntervalStatic') or 0))
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
    ) -> bool:
        """Persist bucket results and findings to SharePoint list 'AuditRuns'."""
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint lists unavailable; skipping AuditRuns write")
            return False

        try:
            site_id = self._get_site_id()
            if not site_id:
                return False

            list_id = self._get_audit_results_list_id()
            if not list_id:
                logger.warning("[STORAGE] AuditRuns/Audit Run Results list not found; skipping list-backed result persistence")
                return False

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

            property_name_field = None
            resident_name_field = None
            try:
                columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
                columns_params = {'$select': 'name', '$top': 200}
                columns_response = requests.get(columns_url, headers=headers, params=columns_params, timeout=60)
                if columns_response.status_code == 200:
                    column_names = {
                        column.get('name')
                        for column in columns_response.json().get('value', [])
                        if column.get('name')
                    }
                    if 'PropertyName' in column_names:
                        property_name_field = 'PropertyName'

                    if 'ResidentName' in column_names:
                        resident_name_field = 'ResidentName'
                else:
                    logger.warning(
                        f"[STORAGE] Could not read AuditRuns columns: "
                        f"{columns_response.status_code} - {columns_response.text}"
                    )
            except Exception as schema_exc:
                logger.warning(f"[STORAGE] Failed loading AuditRuns optional column names: {schema_exc}")

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

                    fields_payload = {
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

                    if property_name_field and property_id_int is not None:
                        property_name_value = property_name_lookup.get(property_id_int)
                        if property_name_value:
                            fields_payload[property_name_field] = property_name_value

                    if resident_name_field and lease_interval_id_int is not None:
                        resident_name_value = resident_name_lookup.get(lease_interval_id_int)
                        if resident_name_value:
                            fields_payload[resident_name_field] = resident_name_value

                    row_payloads.append({'fields': fields_payload})

                rows_written += self._post_list_rows_in_batches(
                    site_id=site_id,
                    list_id=list_id,
                    row_payloads=row_payloads,
                    context_label=f"AuditRuns {result_type} run={run_id}",
                )

                return rows_written

            rows_written_by_type = {
                'bucket_result': 0,
                'finding': 0,
            }

            write_targets = [
                ('bucket_result', bucket_results),
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
                f"[STORAGE] ✅ Wrote AuditRuns rows for {run_id}: "
                f"bucket_result={bucket_rows_written}, finding={finding_rows_written}"
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
        """Load result rows for a run/type from SharePoint list 'AuditRuns'."""
        if not self._can_use_sharepoint_lists():
            return None

        try:
            site_id = self._get_site_id()
            if not site_id:
                return None

            list_id = self._get_audit_results_list_id()
            if not list_id:
                return None

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
            }
            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            filters = [
                f"fields/RunId eq '{run_id}'",
                f"fields/ResultType eq '{result_type}'",
            ]
            if property_id is not None:
                filters.append(f"fields/PropertyId eq {int(property_id)}")
            if lease_interval_id is not None:
                filters.append(f"fields/LeaseIntervalId eq {int(lease_interval_id)}")

            params = {
                '$expand': 'fields',
                '$filter': ' and '.join(filters),
                '$top': 5000
            }

            response = requests.get(items_url, headers=headers, params=params, timeout=60)
            if response.status_code != 200:
                logger.warning(
                    f"[STORAGE] Failed loading audit results for run={run_id}, type={result_type}: "
                    f"{response.status_code} - {response.text}"
                )
                return None

            items = response.json().get('value', [])
            if not items:
                return None

            rows: List[Dict[str, Any]] = []
            for item in items:
                fields = item.get('fields', {})

                if result_type == 'bucket_result':
                    row_payload = {
                        'PROPERTY_ID': fields.get('PropertyId'),
                        'LEASE_INTERVAL_ID': fields.get('LeaseIntervalId'),
                        'property_name': fields.get('PropertyName'),
                        'resident_name': fields.get('ResidentName'),
                        'AR_CODE_ID': fields.get('ArCodeId'),
                        'AUDIT_MONTH': fields.get('AuditMonth'),
                        'expected_total': fields.get('ExpectedTotal'),
                        'actual_total': fields.get('ActualTotal'),
                        'variance': fields.get('Variance'),
                        'status': fields.get('Status'),
                        'match_rule': fields.get('MatchRule')
                    }

                    # Legacy compatibility: recover full row from RowJson if explicit fields are missing.
                    if row_payload.get('status') in [None, ''] and fields.get('RowJson'):
                        try:
                            legacy = json.loads(fields.get('RowJson'))
                            for key, value in legacy.items():
                                row_payload[key] = value
                        except Exception:
                            pass

                    rows.append(row_payload)
                elif result_type == 'finding':
                    row_payload = {
                        'finding_id': fields.get('FindingId'),
                        'run_id': fields.get('RunId', run_id),
                        'property_id': fields.get('PropertyId'),
                        'lease_interval_id': fields.get('LeaseIntervalId'),
                        'property_name': fields.get('PropertyName'),
                        'resident_name': fields.get('ResidentName'),
                        'ar_code_id': fields.get('ArCodeId'),
                        'audit_month': fields.get('AuditMonth'),
                        'category': fields.get('Category'),
                        'severity': fields.get('Severity'),
                        'title': fields.get('FindingTitle'),
                        'description': fields.get('Description'),
                        'expected_value': fields.get('ExpectedValue'),
                        'actual_value': fields.get('ActualValue'),
                        'variance': fields.get('Variance'),
                        'impact_amount': fields.get('ImpactAmount')
                    }

                    # Legacy compatibility: recover fields from RowJson if present.
                    if row_payload.get('title') in [None, ''] and fields.get('RowJson'):
                        try:
                            legacy = json.loads(fields.get('RowJson'))
                            for key, value in legacy.items():
                                row_payload[key] = value
                        except Exception:
                            pass

                    rows.append(row_payload)
                else:
                    rows.append({
                        'RunId': fields.get('RunId', run_id),
                        'ResultType': fields.get('ResultType', result_type)
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
        """Load bucket results from SharePoint list (preferred) with CSV fallback."""
        scope = f"run={run_id}, property_id={property_id}, lease_interval_id={lease_interval_id}"
        bucket_results = self._load_results_from_sharepoint_list(
            run_id,
            'bucket_result',
            property_id=property_id,
            lease_interval_id=lease_interval_id,
        )
        if bucket_results is not None:
            list_results = self._normalize_loaded_dataframe(bucket_results)

            csv_results = self._load_dataframe(run_id, "outputs/bucket_results.csv")
            if csv_results is not None:
                if property_id is not None and 'PROPERTY_ID' in csv_results.columns:
                    csv_results = csv_results[csv_results['PROPERTY_ID'] == float(property_id)]
                if lease_interval_id is not None and 'LEASE_INTERVAL_ID' in csv_results.columns:
                    csv_results = csv_results[csv_results['LEASE_INTERVAL_ID'] == float(lease_interval_id)]

                csv_results = self._normalize_loaded_dataframe(csv_results.copy())
                if len(list_results) < len(csv_results):
                    logger.warning(
                        f"[CSV FALLBACK][bucket_results] Partial list-backed bucket_results for run={run_id} "
                        f"(list_rows={len(list_results)}, csv_rows={len(csv_results)}); "
                        "falling back to CSV for complete view"
                    )
                    csv_results.attrs['read_source'] = 'csv'
                    csv_results.attrs['read_reason'] = 'partial_list'
                    csv_results.attrs['read_scope'] = scope
                    csv_results.attrs['list_rows'] = len(list_results)
                    csv_results.attrs['csv_rows'] = len(csv_results)
                    logger.info(
                        f"[READ SOURCE][bucket_results] source=csv reason=partial_list scope=({scope}) "
                        f"list_rows={len(list_results)} csv_rows={len(csv_results)}"
                    )
                    return csv_results

            list_results.attrs['read_source'] = 'sharepoint_list'
            list_results.attrs['read_reason'] = 'preferred'
            list_results.attrs['read_scope'] = scope
            list_results.attrs['list_rows'] = len(list_results)
            logger.info(
                f"[READ SOURCE][bucket_results] source=sharepoint_list reason=preferred scope=({scope}) "
                f"rows={len(list_results)}"
            )

            return list_results

        bucket_results = self._load_dataframe(run_id, "outputs/bucket_results.csv")
        if bucket_results is None:
            empty_results = pd.DataFrame()
            empty_results.attrs['read_source'] = 'none'
            empty_results.attrs['read_reason'] = 'no_list_and_no_csv'
            empty_results.attrs['read_scope'] = scope
            logger.warning(
                f"[READ SOURCE][bucket_results] source=none reason=no_list_and_no_csv scope=({scope})"
            )
            return empty_results

        if property_id is not None and 'PROPERTY_ID' in bucket_results.columns:
            bucket_results = bucket_results[bucket_results['PROPERTY_ID'] == float(property_id)]
        if lease_interval_id is not None and 'LEASE_INTERVAL_ID' in bucket_results.columns:
            bucket_results = bucket_results[bucket_results['LEASE_INTERVAL_ID'] == float(lease_interval_id)]
        bucket_results.attrs['read_source'] = 'csv'
        bucket_results.attrs['read_reason'] = 'list_unavailable_or_error'
        bucket_results.attrs['read_scope'] = scope
        bucket_results.attrs['csv_rows'] = len(bucket_results)
        logger.info(
            f"[CSV FALLBACK][bucket_results] list_unavailable_or_error; using CSV scope=({scope}) rows={len(bucket_results)}"
        )
        logger.info(
            f"[READ SOURCE][bucket_results] source=csv reason=list_unavailable_or_error scope=({scope}) rows={len(bucket_results)}"
        )
        return self._normalize_loaded_dataframe(bucket_results.copy())

    def load_findings(
        self,
        run_id: str,
        property_id: Optional[int] = None,
        lease_interval_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load findings from SharePoint list (preferred) with CSV fallback."""
        scope = f"run={run_id}, property_id={property_id}, lease_interval_id={lease_interval_id}"
        findings = self._load_results_from_sharepoint_list(
            run_id,
            'finding',
            property_id=property_id,
            lease_interval_id=lease_interval_id,
        )
        if findings is not None:
            list_findings = self._normalize_loaded_dataframe(findings)

            csv_findings = self._load_dataframe(run_id, "outputs/findings.csv")
            if csv_findings is not None:
                if property_id is not None:
                    if 'property_id' in csv_findings.columns:
                        csv_findings = csv_findings[csv_findings['property_id'] == float(property_id)]
                    elif 'PROPERTY_ID' in csv_findings.columns:
                        csv_findings = csv_findings[csv_findings['PROPERTY_ID'] == float(property_id)]

                if lease_interval_id is not None:
                    if 'lease_interval_id' in csv_findings.columns:
                        csv_findings = csv_findings[csv_findings['lease_interval_id'] == float(lease_interval_id)]
                    elif 'LEASE_INTERVAL_ID' in csv_findings.columns:
                        csv_findings = csv_findings[csv_findings['LEASE_INTERVAL_ID'] == float(lease_interval_id)]

                csv_findings = self._normalize_loaded_dataframe(csv_findings.copy())
                if len(list_findings) < len(csv_findings):
                    logger.warning(
                        f"[CSV FALLBACK][findings] Partial list-backed findings for run={run_id} "
                        f"(list_rows={len(list_findings)}, csv_rows={len(csv_findings)}); "
                        "falling back to CSV for complete view"
                    )
                    csv_findings.attrs['read_source'] = 'csv'
                    csv_findings.attrs['read_reason'] = 'partial_list'
                    csv_findings.attrs['read_scope'] = scope
                    csv_findings.attrs['list_rows'] = len(list_findings)
                    csv_findings.attrs['csv_rows'] = len(csv_findings)
                    logger.info(
                        f"[READ SOURCE][findings] source=csv reason=partial_list scope=({scope}) "
                        f"list_rows={len(list_findings)} csv_rows={len(csv_findings)}"
                    )
                    return csv_findings

            list_findings.attrs['read_source'] = 'sharepoint_list'
            list_findings.attrs['read_reason'] = 'preferred'
            list_findings.attrs['read_scope'] = scope
            list_findings.attrs['list_rows'] = len(list_findings)
            logger.info(
                f"[READ SOURCE][findings] source=sharepoint_list reason=preferred scope=({scope}) rows={len(list_findings)}"
            )

            return list_findings

        findings = self._load_dataframe(run_id, "outputs/findings.csv")
        if findings is None:
            empty_results = pd.DataFrame()
            empty_results.attrs['read_source'] = 'none'
            empty_results.attrs['read_reason'] = 'no_list_and_no_csv'
            empty_results.attrs['read_scope'] = scope
            logger.warning(
                f"[READ SOURCE][findings] source=none reason=no_list_and_no_csv scope=({scope})"
            )
            return empty_results

        if property_id is not None:
            if 'property_id' in findings.columns:
                findings = findings[findings['property_id'] == float(property_id)]
            elif 'PROPERTY_ID' in findings.columns:
                findings = findings[findings['PROPERTY_ID'] == float(property_id)]

        if lease_interval_id is not None:
            if 'lease_interval_id' in findings.columns:
                findings = findings[findings['lease_interval_id'] == float(lease_interval_id)]
            elif 'LEASE_INTERVAL_ID' in findings.columns:
                findings = findings[findings['LEASE_INTERVAL_ID'] == float(lease_interval_id)]
        findings.attrs['read_source'] = 'csv'
        findings.attrs['read_reason'] = 'list_unavailable_or_error'
        findings.attrs['read_scope'] = scope
        findings.attrs['csv_rows'] = len(findings)
        logger.info(
            f"[CSV FALLBACK][findings] list_unavailable_or_error; using CSV scope=({scope}) rows={len(findings)}"
        )
        logger.info(
            f"[READ SOURCE][findings] source=csv reason=list_unavailable_or_error scope=({scope}) rows={len(findings)}"
        )
        return self._normalize_loaded_dataframe(findings.copy())

    def load_expected_detail(self, run_id: str) -> pd.DataFrame:
        """Load expected_detail for a run from persisted inputs."""
        expected_detail = self._load_dataframe(run_id, "inputs_normalized/expected_detail.csv")
        if expected_detail is None:
            return pd.DataFrame()
        return self._normalize_loaded_dataframe(expected_detail)

    def load_actual_detail(self, run_id: str) -> pd.DataFrame:
        """Load actual_detail for a run from persisted inputs."""
        actual_detail = self._load_dataframe(run_id, "inputs_normalized/actual_detail.csv")
        if actual_detail is None:
            return pd.DataFrame()
        return self._normalize_loaded_dataframe(actual_detail)

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
                'AuditMonth': normalized_audit_month,
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
                'UpdatedAt': month_data.get('updated_at', ''),
                'UpdatedBy': month_data.get('updated_by', '')
            }
            
            logger.info(f"[STORAGE] 💾 Saving fields: RunId={fields_payload['RunId']}, PropertyId={fields_payload['PropertyId']}, LeaseIntervalId={fields_payload['LeaseIntervalId']}, ArCodeId={fields_payload['ArCodeId']}, Status={fields_payload['Status']}, ResolvedBy={fields_payload['ResolvedBy']}, ResolvedByName={fields_payload['ResolvedByName']}")

            items_data = response.json()
            items = items_data.get('value', [])
            
            if items:
                # Update existing record
                item_id = items[0]['id']
                update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
                update_response = requests.patch(update_url, headers=headers, json=fields_payload, timeout=30)
                
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
            response = requests.put(url, headers=headers, data=file_content.encode('utf-8'), timeout=30)
            
            if response.status_code in [200, 201]:
                logger.debug(f"[STORAGE] ✅ Uploaded: {file_path}")
                return True
            else:
                logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: HTTP {response.status_code} - {response.text[:200]}")
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
            response = requests.put(url, headers=headers, data=file_content, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"[STORAGE] ✅ Successfully uploaded: {file_path}")
                return True
            else:
                logger.error(f"[STORAGE] ❌ Failed to upload {file_path}: HTTP {response.status_code} - {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"[STORAGE] ❌ Exception uploading {file_path}: {e}", exc_info=True)
            return False
    
    def _download_file_from_sharepoint(self, file_path: str) -> Optional[str]:
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
                return response.text
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
            self._upload_file_to_sharepoint(csv_content, sp_path)
        else:
            # Save to local filesystem
            local_path = self.base_dir / run_id / file_path
            df.to_csv(local_path, index=False)
    
    def _load_dataframe(self, run_id: str, file_path: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from either SharePoint or local filesystem."""
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
    
    def _save_json(self, data: Dict[str, Any], run_id: str, file_path: str):
        """Save JSON to either SharePoint or local filesystem."""
        if self.use_sharepoint:
            # Save to SharePoint
            json_content = json.dumps(data, indent=2, default=str)
            sp_path = f"{run_id}/{file_path}"
            self._upload_file_to_sharepoint(json_content, sp_path)
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
        self.create_run_dir(run_id)

        write_details_async = os.getenv('ASYNC_AUDIT_RESULTS_WRITE', 'false').lower() == 'true'
        write_metrics_async = os.getenv('ASYNC_METRICS_WRITE', 'true').lower() == 'true'
        write_snapshots_async = os.getenv('ASYNC_RUN_DISPLAY_SNAPSHOTS', 'true').lower() == 'true'
        snapshot_validation_async = os.getenv('ASYNC_SNAPSHOT_VALIDATION', 'true').lower() == 'true'
        
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
        
        # Save inputs
        print(f"\n[STORAGE] Step 2/7: Saving input files (normalized data)...")
        logger.info(f"[STORAGE] 📊 Saving input files...")
        self._save_dataframe(expected_detail, run_id, "inputs_normalized/expected_detail.csv")
        print(f"[STORAGE] ✓ Saved: expected_detail.csv ({len(expected_detail)} rows)")
        files_saved.append("expected_detail.csv")
        
        self._save_dataframe(actual_detail, run_id, "inputs_normalized/actual_detail.csv")
        print(f"[STORAGE] ✓ Saved: actual_detail.csv ({len(actual_detail)} rows)")
        files_saved.append("actual_detail.csv")
        
        # Save outputs
        print(f"\n[STORAGE] Step 3/7: Saving output files (results)...")
        logger.info(f"[STORAGE] 📈 Saving output files...")
        self._save_dataframe(bucket_results, run_id, "outputs/bucket_results.csv")
        print(f"[STORAGE] ✓ Saved: bucket_results.csv ({len(bucket_results)} rows)")
        files_saved.append("bucket_results.csv")
        
        self._save_dataframe(findings, run_id, "outputs/findings.csv")
        print(f"[STORAGE] ✓ Saved: findings.csv ({len(findings)} rows)")
        files_saved.append("findings.csv")
        
        # Save variance detail if provided
        if variance_detail is not None and len(variance_detail) > 0:
            self._save_dataframe(variance_detail, run_id, "outputs/variance_detail.csv")
            print(f"[STORAGE] ✓ Saved: variance_detail.csv ({len(variance_detail)} rows)")
            files_saved.append("variance_detail.csv")
        
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
        
        # Write metrics to SharePoint list (don't fail save if this fails)
        print(f"\n[STORAGE] Step 5/7: Writing metrics to SharePoint List (AuditRuns)...")
        try:
            can_write_sharepoint_lists = self._can_use_sharepoint_lists()
            if write_metrics_async and can_write_sharepoint_lists:
                metrics_started = perf_counter()
                print(f"[STORAGE] 🚀 Dispatching async metrics write...")
                metrics_thread = threading.Thread(
                    target=self._write_metrics_to_sharepoint_list_async,
                    args=(run_id, bucket_results, findings, dict(metadata)),
                    daemon=True,
                    name=f"metrics-write-{run_id}",
                )
                metrics_thread.start()
                stage_timers['metrics_write_seconds'] = float(perf_counter() - metrics_started)
                print(f"[STORAGE] ✓ Metrics write dispatched (async mode)")
            else:
                metrics_started = perf_counter()
                print(f"[STORAGE] Writing metrics synchronously...")
                self._write_metrics_to_sharepoint_list(run_id, bucket_results, findings, metadata)
                stage_timers['metrics_write_seconds'] = float(perf_counter() - metrics_started)
                print(f"[STORAGE] ✓ Metrics written in {stage_timers['metrics_write_seconds']:.2f}s")
        except Exception as e:
            print(f"[STORAGE] ⚠️  Metrics write failed: {e}")
            logger.warning(f"[STORAGE] Failed to write metrics to SharePoint list: {e}")

        # Write static display snapshots (portfolio/property/lease) for fast UI loads.
        print(f"\n[STORAGE] Step 6/7: Writing display snapshots (portfolio/property/lease views)...")
        try:
            if not write_display_snapshots:
                print("[STORAGE] ↩️  Display snapshot write skipped for run scope")
            else:
                can_write_sharepoint_lists = self._can_use_sharepoint_lists()
                if write_snapshots_async and can_write_sharepoint_lists:
                    snapshot_dispatch_started = perf_counter()
                    print(f"[STORAGE] 🚀 Dispatching async display snapshot write...")
                    snapshot_thread = threading.Thread(
                        target=self._write_run_display_snapshots_async,
                        args=(run_id, bucket_results),
                        kwargs={
                            'actual_detail': actual_detail,
                            'expected_detail': expected_detail,
                            'property_name_map': property_name_map,
                            'snapshot_validation_async': snapshot_validation_async,
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

        # Write detailed results to SharePoint list (list-backed results DB).
        # Keep CSVs as fallback for compatibility. Run asynchronously by default to reduce upload latency.
        print(f"\n[STORAGE] Step 7/7: Writing detailed results to SharePoint List (AuditRuns Detail)...")
        try:
            can_write_sharepoint_lists = self._can_use_sharepoint_lists()
            if write_details_async and can_write_sharepoint_lists:
                print(
                    f"[STORAGE] 🚀 Dispatching async detail write: "
                    f"bucket_rows={len(bucket_results)}, finding_rows={len(findings)}"
                )
                writer_thread = threading.Thread(
                    target=self._write_results_to_sharepoint_list_async,
                    args=(run_id, bucket_results, findings, actual_detail, expected_detail),
                    daemon=True,
                    name=f"auditruns-write-{run_id}",
                )
                writer_thread.start()
                logger.info(
                    f"[STORAGE] 🚀 Dispatched background AuditRuns write for {run_id}: "
                    f"bucket_rows={len(bucket_results)}, finding_rows={len(findings)}"
                )
                print(f"[STORAGE] ✓ Detail write dispatched (async mode)")
            else:
                print(f"[STORAGE] Writing details synchronously...")
                self._write_results_to_sharepoint_list(
                    run_id,
                    bucket_results,
                    findings,
                    actual_detail=actual_detail,
                    expected_detail=expected_detail,
                )
                print(f"[STORAGE] ✓ Details written successfully")
        except Exception as e:
            print(f"[STORAGE] ⚠️  Detail write failed: {e}")
            logger.warning(f"[STORAGE] Failed to write detailed results to SharePoint list: {e}")

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
        """Load complete audit run from storage."""
        # Load core detail data from CSV/document storage
        expected_detail = self._load_dataframe(run_id, "inputs_normalized/expected_detail.csv")
        actual_detail = self._load_dataframe(run_id, "inputs_normalized/actual_detail.csv")

        # Load results from SharePoint list first when available (results DB),
        # then fall back to CSV for compatibility/backfill scenarios.
        bucket_results = self._load_results_from_sharepoint_list(run_id, 'bucket_result')
        if bucket_results is None:
            bucket_results = self._load_dataframe(run_id, "outputs/bucket_results.csv")

        findings = self._load_results_from_sharepoint_list(run_id, 'finding')
        if findings is None:
            findings = self._load_dataframe(run_id, "outputs/findings.csv")

        variance_detail = self._load_dataframe(run_id, "outputs/variance_detail.csv")
        
        if expected_detail is None or actual_detail is None or bucket_results is None or findings is None:
            raise ValueError(f"Run {run_id} not found or incomplete")

        for df in [expected_detail, actual_detail, bucket_results, findings]:
            self._normalize_loaded_dataframe(df)
        
        # Also convert dates in variance_detail if loaded
        if variance_detail is not None:
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
        """List recent audit runs."""
        runs = []
        
        if self.use_sharepoint:
            # List folders from SharePoint
            try:
                site_id, drive_id = self._get_site_and_drive_id()
                if not site_id or not drive_id:
                    logger.warning("[STORAGE] Cannot list runs - SharePoint not accessible")
                    return runs
                
                # List children of root folder
                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
                headers = {'Authorization': f'Bearer {self.access_token}'}
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code != 200:
                    logger.error(f"[STORAGE] Failed to list runs: {response.status_code}")
                    return runs
                
                # Get folders that start with "run_"
                folders = [item for item in response.json().get('value', []) 
                          if item.get('folder') and item['name'].startswith('run_')]
                
                # Sort by name (which includes timestamp) in reverse
                folders.sort(key=lambda x: x['name'], reverse=True)
                
                # Load metadata for each run
                for folder in folders[:limit]:
                    run_id = folder['name']
                    try:
                        meta = self._load_json(run_id, "run_meta.json")
                        if meta:
                            meta["run_id"] = run_id
                            runs.append(meta)
                        else:
                            runs.append({
                                "run_id": run_id,
                                "timestamp": folder.get("createdDateTime", "Unknown"),
                                "audit_period": {},
                                "run_type": "Manual"
                            })
                    except Exception as e:
                        logger.warning(f"[STORAGE] Failed to load metadata for {run_id}; using fallback metadata: {e}")
                        runs.append({
                            "run_id": run_id,
                            "timestamp": folder.get("createdDateTime", "Unknown"),
                            "audit_period": {},
                            "run_type": "Manual"
                        })
                
            except Exception as e:
                logger.error(f"[STORAGE] Error listing SharePoint runs: {e}", exc_info=True)
        else:
            # List from local filesystem
            if not self.base_dir.exists():
                return runs
            
            for run_dir in sorted(self.base_dir.iterdir(), reverse=True):
                if run_dir.is_dir():
                    meta_path = run_dir / "run_meta.json"
                    if meta_path.exists():
                        with open(meta_path, "r") as f:
                            meta = json.load(f)
                            meta["run_id"] = run_dir.name
                            runs.append(meta)
                    
                    if len(runs) >= limit:
                        break
        
        return runs
    
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
