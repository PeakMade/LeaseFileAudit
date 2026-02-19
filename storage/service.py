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
        
        logger.info(f"[STORAGE] ✅ Successfully saved run {run_id} - {len(files_saved)} files")
        if self.use_sharepoint:
            logger.info(f"[STORAGE] 📍 Location: SharePoint/{self.library_name}/{run_id}")
        else:
            logger.info(f"[STORAGE] 📍 Location: {self.base_dir}/{run_id}")
    
    def load_run(self, run_id: str) -> Dict[str, Any]:
        """Load complete audit run from storage."""
        # Load CSVs
        expected_detail = self._load_dataframe(run_id, "inputs_normalized/expected_detail.csv")
        actual_detail = self._load_dataframe(run_id, "inputs_normalized/actual_detail.csv")
        bucket_results = self._load_dataframe(run_id, "outputs/bucket_results.csv")
        findings = self._load_dataframe(run_id, "outputs/findings.csv")
        variance_detail = self._load_dataframe(run_id, "outputs/variance_detail.csv")
        
        if expected_detail is None or actual_detail is None or bucket_results is None or findings is None:
            raise ValueError(f"Run {run_id} not found or incomplete")
        
        # Convert date columns to datetime
        date_columns = ['AUDIT_MONTH', 'PERIOD_START', 'PERIOD_END', 'POST_DATE', 'audit_month']
        for df in [expected_detail, actual_detail, bucket_results, findings]:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Also convert dates in variance_detail if loaded
        if variance_detail is not None:
            for col in date_columns:
                if col in variance_detail.columns:
                    variance_detail[col] = pd.to_datetime(variance_detail[col], errors='coerce')
        
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
