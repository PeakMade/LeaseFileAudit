"""
Storage service for audit run persistence.
Supports both local filesystem and SharePoint Document Library.
"""
import json
import hashlib
import io
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import math
import pandas as pd
import requests
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
        
        if self.use_sharepoint:
            logger.info(f"[STORAGE] Using SharePoint Document Library: {library_name}")
        else:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[STORAGE] Using local filesystem: {self.base_dir}")

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
                    logger.info(f"[STORAGE] Found drive ID for library '{self.library_name}'")
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
        logger.info(f"[STORAGE] Resolved SharePoint list '{list_name}' id: {list_id}")
        return list_id

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

    def _build_result_composite_key(self, run_id: str, result_type: str, row: Dict[str, Any], row_index: int) -> str:
        """Build deterministic composite key for result rows persisted to SharePoint list."""
        property_id = self._normalize_for_json(row.get('PROPERTY_ID', row.get('property_id')))
        lease_interval_id = self._normalize_for_json(row.get('LEASE_INTERVAL_ID', row.get('lease_interval_id')))
        ar_code_id = self._normalize_for_json(row.get('AR_CODE_ID', row.get('ar_code_id')))
        audit_month = self._normalize_audit_month_value(row.get('AUDIT_MONTH', row.get('audit_month')))

        if property_id is not None and lease_interval_id is not None and ar_code_id is not None and audit_month:
            return f"{run_id}:{result_type}:{property_id}:{lease_interval_id}:{ar_code_id}:{audit_month}"

        return f"{run_id}:{result_type}:row:{row_index}"

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

    def _resolve_snapshot_exception_count_field_name(self, site_id: str, list_id: str) -> str:
        """Resolve internal SharePoint field name for snapshot exception count."""
        default_name = 'ExceptionCountStatic'
        legacy_name = 'ExceptionCountStatistic'

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
                    f"[STORAGE] Could not read RunDisplaySnapshots columns; defaulting to {default_name}: "
                    f"{response.status_code} - {response.text}"
                )
                return default_name

            column_names = {column.get('name') for column in response.json().get('value', []) if column.get('name')}
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

    def _build_run_display_snapshot_rows(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        exception_count_field_name: str = 'ExceptionCountStatic'
    ) -> List[Dict[str, Any]]:
        """Build static snapshot rows for portfolio/property/lease display scopes."""
        rows: List[Dict[str, Any]] = []
        if bucket_results is None or len(bucket_results) == 0:
            return rows

        property_column = 'PROPERTY_ID' if 'PROPERTY_ID' in bucket_results.columns else 'property_id'
        lease_column = 'LEASE_INTERVAL_ID' if 'LEASE_INTERVAL_ID' in bucket_results.columns else 'lease_interval_id'

        def _make_row(scope_type: str, subset: pd.DataFrame, property_id: Any = None, lease_interval_id: Any = None) -> Dict[str, Any]:
            metrics = self._calculate_static_metrics(subset)
            property_id_int = self._safe_int(property_id)
            lease_interval_id_int = self._safe_int(lease_interval_id)

            snapshot_key = f"{run_id}:{scope_type}"
            title = f"{scope_type}:{run_id}"
            if property_id_int is not None:
                snapshot_key += f":{property_id_int}"
                title += f":{property_id_int}"
            if lease_interval_id_int is not None:
                snapshot_key += f":{lease_interval_id_int}"
                title += f":{lease_interval_id_int}"

            return {
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

    def _write_run_display_snapshots_to_sharepoint_list(self, run_id: str, bucket_results: pd.DataFrame) -> bool:
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

            # Remove existing snapshots for run to prevent duplicates.
            existing_params = {
                '$select': 'id',
                '$expand': 'fields',
                '$filter': f"fields/RunId eq '{run_id}'",
                '$top': 5000
            }
            existing_response = requests.get(items_url, headers=headers, params=existing_params, timeout=60)
            if existing_response.status_code == 200:
                for item in existing_response.json().get('value', []):
                    delete_url = f"{items_url}/{item['id']}"
                    delete_response = requests.delete(delete_url, headers=headers, timeout=30)
                    if delete_response.status_code not in [200, 202, 204]:
                        logger.warning(
                            f"[STORAGE] Failed deleting existing snapshot item {item['id']}: "
                            f"{delete_response.status_code} - {delete_response.text}"
                        )
            else:
                logger.warning(
                    f"[STORAGE] Could not query existing snapshots for {run_id}: "
                    f"{existing_response.status_code} - {existing_response.text}"
                )

            exception_count_field_name = self._resolve_snapshot_exception_count_field_name(site_id, list_id)
            snapshot_rows = self._build_run_display_snapshot_rows(
                run_id,
                bucket_results,
                exception_count_field_name=exception_count_field_name,
            )
            created = 0
            for row in snapshot_rows:
                create_response = requests.post(items_url, headers=headers, json={'fields': row}, timeout=60)
                if create_response.status_code in [200, 201]:
                    created += 1
                else:
                    logger.warning(
                        f"[STORAGE] Failed creating snapshot row {row.get('SnapshotKey')}: "
                        f"{create_response.status_code} - {create_response.text}"
                    )

            logger.info(f"[STORAGE] ✅ Wrote RunDisplaySnapshots rows for {run_id}: rows={created}")
            return True
        except Exception as e:
            logger.error(f"[STORAGE] Error writing RunDisplaySnapshots list rows: {e}", exc_info=True)
            return False

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
                'exception_count': int(float(exception_count or 0)),
                'undercharge': float(fields.get('UnderchargeStatic') or 0),
                'overcharge': float(fields.get('OverchargeStatic') or 0),
                'match_rate': float(fields.get('MatchRateStatic') or 0),
                'total_buckets': int(float(fields.get('TotalBucketsStatic') or 0)),
                'matched_buckets': int(float(fields.get('MatchedBucketsStatic') or 0)),
            }

            logger.info(
                f"[STORAGE] ✅ Loaded RunDisplaySnapshot: run={run_id}, scope={scope_type}, "
                f"property_id={property_id}, lease_interval_id={lease_interval_id}, "
                f"snapshot_key={snapshot.get('snapshot_key')}"
            )
            return snapshot
        except Exception as e:
            logger.error(f"[STORAGE] Error loading RunDisplaySnapshots row: {e}", exc_info=True)
            return None

    def _write_results_to_sharepoint_list(
        self,
        run_id: str,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame
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

            # Remove existing rows for this run to prevent duplicates during re-save.
            existing_params = {
                '$select': 'id',
                '$expand': 'fields',
                '$filter': f"fields/RunId eq '{run_id}'",
                '$top': 5000
            }
            existing_response = requests.get(items_url, headers=headers, params=existing_params, timeout=60)
            if existing_response.status_code == 200:
                existing_items = existing_response.json().get('value', [])
                for item in existing_items:
                    delete_url = f"{items_url}/{item['id']}"
                    delete_response = requests.delete(delete_url, headers=headers, timeout=30)
                    if delete_response.status_code not in [200, 202, 204]:
                        logger.warning(
                            f"[STORAGE] Failed deleting existing result item {item['id']}: "
                            f"{delete_response.status_code} - {delete_response.text}"
                        )
            else:
                logger.warning(
                    f"[STORAGE] Could not query existing audit result rows for {run_id}: "
                    f"{existing_response.status_code} - {existing_response.text}"
                )

            def _write_dataframe_rows(df: pd.DataFrame, result_type: str) -> int:
                rows_written = 0
                if df is None or len(df) == 0:
                    return rows_written

                for idx, (_, row) in enumerate(df.iterrows()):
                    row_dict = {col: self._normalize_for_json(value) for col, value in row.to_dict().items()}
                    composite_key = self._build_result_composite_key(run_id, result_type, row_dict, idx)

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

                    fields_payload = {
                        'Title': f"{result_type}:{idx}",
                        'CompositeKey': composite_key,
                        'RunId': run_id,
                        'ResultType': result_type,
                        'PropertyId': int(float(property_id_val)) if property_id_val is not None and property_id_val != '' else None,
                        'LeaseIntervalId': int(float(lease_interval_id_val)) if lease_interval_id_val is not None and lease_interval_id_val != '' else None,
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

                    create_payload = {'fields': fields_payload}
                    create_response = requests.post(items_url, headers=headers, json=create_payload, timeout=60)
                    if create_response.status_code not in [200, 201]:
                        logger.warning(
                            f"[STORAGE] Failed writing {result_type} row {idx} for run {run_id}: "
                            f"{create_response.status_code} - {create_response.text}"
                        )
                        continue

                    rows_written += 1

                return rows_written

            bucket_rows_written = _write_dataframe_rows(bucket_results, 'bucket_result')
            finding_rows_written = _write_dataframe_rows(findings, 'finding')
            logger.info(
                f"[STORAGE] ✅ Wrote AuditRuns rows for {run_id}: "
                f"bucket_result={bucket_rows_written}, finding={finding_rows_written}"
            )
            return True
        except Exception as e:
            logger.error(f"[STORAGE] Error writing audit results list rows: {e}", exc_info=True)
            return False

    def _load_results_from_sharepoint_list(self, run_id: str, result_type: str) -> Optional[pd.DataFrame]:
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
            params = {
                '$expand': 'fields',
                '$filter': f"fields/RunId eq '{run_id}' and fields/ResultType eq '{result_type}'",
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

    def load_exception_states_from_sharepoint_list(self, run_id: str, property_id: int, lease_interval_id: int) -> List[Dict[str, Any]]:
        """Load exception workflow states from SharePoint List 'ExceptionStates'."""
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, returning empty exception states")
            return []

        try:
            logger.info("[STORAGE] 📊 Loading exception states from SharePoint list")
            site_id = self._get_site_id()
            if not site_id:
                return []

            list_id = self._get_sharepoint_list_id("ExceptionStates")
            if not list_id:
                return []

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            filter_query = (
                f"fields/RunId eq '{run_id}' and "
                f"fields/PropertyId eq {int(property_id)} and "
                f"fields/LeaseIntervalId eq {int(lease_interval_id)}"
            )
            logger.info(f"[STORAGE] ExceptionStates filter: {filter_query}")
            params = {'$expand': 'fields', '$filter': filter_query}
            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to query exception states: {response.status_code} - {response.text}")
                return []

            items_data = response.json()
            items = items_data.get('value', [])
            results = []
            for item in items:
                fields = item.get('fields', {})
                results.append({
                    'composite_key': fields.get('CompositeKey', ''),
                    'run_id': fields.get('RunId', ''),
                    'property_id': fields.get('PropertyId', None),
                    'lease_interval_id': fields.get('LeaseIntervalId', None),
                    'ar_code_id': fields.get('ArCodeId', None),
                    'exception_type': fields.get('ExceptionType', ''),
                    'status': fields.get('Status', ''),
                    'fix_label': fields.get('FixLabel', ''),
                    'original_fix_label': fields.get('OriginalFixLabel', ''),
                    'updated_fix_label': fields.get('UpdatedFixLabel', ''),
                    'action_type': fields.get('ActionType', ''),
                    'resolved_at': fields.get('ResolvedAt', ''),
                    'resolved_months': fields.get('ResolvedMonths', ''),
                    'updated_at': fields.get('UpdatedAt', ''),
                    'updated_by': fields.get('UpdatedBy', '')
                })
            logger.info(f"[STORAGE] Loaded {len(results)} exception state(s)")
            return results

        except Exception as e:
            logger.error(f"[STORAGE] Error loading exception states from SharePoint list: {e}", exc_info=True)
            return []

    def upsert_exception_state_to_sharepoint_list(self, state: Dict[str, Any]) -> bool:
        """Upsert exception workflow state into SharePoint List 'ExceptionStates'."""
        if not self._can_use_sharepoint_lists():
            logger.debug("[STORAGE] SharePoint list not configured, skipping exception state upsert")
            return False

        try:
            site_id = self._get_site_id()
            if not site_id:
                return False

            list_id = self._get_sharepoint_list_id("ExceptionStates")
            if not list_id:
                return False

            composite_key = state.get('composite_key')
            if not composite_key:
                composite_key = (
                    f"{state.get('run_id')}:{state.get('property_id')}:{state.get('lease_interval_id')}:"
                    f"{state.get('ar_code_id')}:{state.get('exception_type')}"
                )
            logger.info(f"[STORAGE] Upserting ExceptionState: {composite_key}")

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }

            items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            filter_query = f"fields/CompositeKey eq '{composite_key}'"
            params = {'$expand': 'fields', '$filter': filter_query}
            response = requests.get(items_url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to query exception state: {response.status_code} - {response.text}")
                return False

            fields_payload = {
                'CompositeKey': composite_key,
                'RunId': state.get('run_id'),
                'PropertyId': state.get('property_id'),
                'LeaseIntervalId': state.get('lease_interval_id'),
                'ArCodeId': state.get('ar_code_id'),
                'ExceptionType': state.get('exception_type'),
                'Status': state.get('status'),
                'FixLabel': state.get('fix_label'),
                'OriginalFixLabel': state.get('original_fix_label'),
                'UpdatedFixLabel': state.get('updated_fix_label'),
                'ActionType': state.get('action_type'),
                'ResolvedAt': state.get('resolved_at'),
                'ResolvedMonths': state.get('resolved_months'),
                'UpdatedAt': state.get('updated_at'),
                'UpdatedBy': state.get('updated_by')
            }

            items_data = response.json()
            items = items_data.get('value', [])
            if items:
                item_id = items[0]['id']
                update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
                update_response = requests.patch(update_url, headers=headers, json=fields_payload, timeout=30)
                if update_response.status_code in [200, 204]:
                    logger.info("[STORAGE] ✅ Exception state updated")
                    return True
                logger.error(f"[STORAGE] Failed to update exception state: {update_response.status_code} - {update_response.text}")
                return False

            create_payload = {'fields': fields_payload}
            create_response = requests.post(items_url, headers=headers, json=create_payload, timeout=30)
            if create_response.status_code in [200, 201]:
                logger.info("[STORAGE] ✅ Exception state created")
                return True

            logger.error(f"[STORAGE] Failed to create exception state: {create_response.status_code} - {create_response.text}")
            return False

        except Exception as e:
            logger.error(f"[STORAGE] Error upserting exception state: {e}", exc_info=True)
            return False

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
                audit_month = fields.get('AuditMonth', '')
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
                audit_month = fields.get('AuditMonth', '')
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

            # Build composite key for this specific month
            composite_key = (
                f"{month_data.get('run_id')}:{month_data.get('property_id')}:"
                f"{month_data.get('lease_interval_id')}:{month_data.get('ar_code_id')}:"
                f"{month_data.get('audit_month')}"
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
                'AuditMonth': month_data.get('audit_month'),
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
        original_file_path: Optional[Path] = None
    ):
        """Save complete audit run to storage."""
        logger.info(f"[STORAGE] 💾 Starting save for run: {run_id}")
        self.create_run_dir(run_id)
        
        files_saved = []
        files_failed = []
        
        # Save original uploaded file if provided
        if original_file_path and original_file_path.exists():
            self.save_uploaded_file(run_id, original_file_path, original_file_path.name)
            files_saved.append(original_file_path.name)
        
        # Save inputs
        logger.info(f"[STORAGE] 📊 Saving input files...")
        self._save_dataframe(expected_detail, run_id, "inputs_normalized/expected_detail.csv")
        files_saved.append("expected_detail.csv")
        
        self._save_dataframe(actual_detail, run_id, "inputs_normalized/actual_detail.csv")
        files_saved.append("actual_detail.csv")
        
        # Save outputs
        logger.info(f"[STORAGE] 📈 Saving output files...")
        self._save_dataframe(bucket_results, run_id, "outputs/bucket_results.csv")
        files_saved.append("bucket_results.csv")
        
        self._save_dataframe(findings, run_id, "outputs/findings.csv")
        files_saved.append("findings.csv")
        
        # Save variance detail if provided
        if variance_detail is not None and len(variance_detail) > 0:
            self._save_dataframe(variance_detail, run_id, "outputs/variance_detail.csv")
            files_saved.append("variance_detail.csv")
        
        # Save metadata
        logger.info(f"[STORAGE] 📋 Saving metadata...")
        self._save_json(metadata, run_id, "run_meta.json")
        files_saved.append("run_meta.json")
        
        # Write metrics to SharePoint list (don't fail save if this fails)
        try:
            self._write_metrics_to_sharepoint_list(run_id, bucket_results, findings, metadata)
        except Exception as e:
            logger.warning(f"[STORAGE] Failed to write metrics to SharePoint list: {e}")

        # Write detailed results to SharePoint list (list-backed results DB).
        # Keep CSVs as fallback for compatibility.
        try:
            self._write_results_to_sharepoint_list(run_id, bucket_results, findings)
        except Exception as e:
            logger.warning(f"[STORAGE] Failed to write detailed results to SharePoint list: {e}")

        # Write static display snapshots (portfolio/property/lease) for fast UI loads.
        try:
            self._write_run_display_snapshots_to_sharepoint_list(run_id, bucket_results)
        except Exception as e:
            logger.warning(f"[STORAGE] Failed to write run display snapshots to SharePoint list: {e}")
        
        logger.info(f"[STORAGE] ✅ Successfully saved run {run_id} - {len(files_saved)} files")
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

        def _normalize_ar_code_value(value: Any) -> str:
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
        
        # Convert date columns to datetime
        date_columns = ['AUDIT_MONTH', 'PERIOD_START', 'PERIOD_END', 'POST_DATE', 'audit_month']
        for df in [expected_detail, actual_detail, bucket_results, findings]:
            for col in date_columns:
                if col in df.columns:
                    series = pd.to_datetime(df[col], errors='coerce')
                    try:
                        series = series.dt.tz_localize(None)
                    except Exception:
                        pass
                    df[col] = series

        # Normalize AR code column types across DataFrames to avoid string/number mismatches
        # when matching bucket rows to expected/actual detail in lease views.
        ar_code_columns = ['AR_CODE_ID', 'ar_code_id']
        for df in [expected_detail, actual_detail, bucket_results, findings]:
            for col in ar_code_columns:
                if col in df.columns:
                    df[col] = df[col].apply(_normalize_ar_code_value)
        
        # Also convert dates in variance_detail if loaded
        if variance_detail is not None:
            for col in date_columns:
                if col in variance_detail.columns:
                    series = pd.to_datetime(variance_detail[col], errors='coerce')
                    try:
                        series = series.dt.tz_localize(None)
                    except Exception:
                        pass
                    variance_detail[col] = series
        
        return {
            "expected_detail": expected_detail,
            "actual_detail": actual_detail,
            "bucket_results": bucket_results,
            "findings": findings,
            "variance_detail": variance_detail,
            "metadata": self.load_metadata(run_id)
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
                    except Exception as e:
                        logger.warning(f"[STORAGE] Failed to load metadata for {run_id}: {e}")
                
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
