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
        
        if self.use_sharepoint:
            logger.info(f"[STORAGE] Using SharePoint Document Library: {library_name}")
        else:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[STORAGE] Using local filesystem: {self.base_dir}")
    
    def _get_site_and_drive_id(self) -> tuple:
        """Get SharePoint site ID and drive ID for document library."""
        if self._site_id and self._drive_id:
            return self._site_id, self._drive_id
        
        try:
            # Parse site URL: https://tenant.sharepoint.com/sites/sitename
            parts = self.sharepoint_site_url.replace('https://', '').split('/')
            hostname = parts[0]
            site_path = '/'.join(parts[1:])  # sites/BaseCampApps
            
            # Get site ID
            site_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(site_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to get site ID: {response.status_code} - {response.text}")
                return None, None
            
            self._site_id = response.json()['id']
            
            # Get drive ID for document library
            drives_url = f"https://graph.microsoft.com/v1.0/sites/{self._site_id}/drives"
            response = requests.get(drives_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"[STORAGE] Failed to get drives: {response.status_code}")
                return None, None
            
            # Find the drive matching our library name
            for drive in response.json()['value']:
                if drive['name'] == self.library_name:
                    self._drive_id = drive['id']
                    logger.info(f"[STORAGE] Found drive ID for library '{self.library_name}'")
                    return self._site_id, self._drive_id
            
            logger.error(f"[STORAGE] Document library '{self.library_name}' not found")
            return None, None
            
        except Exception as e:
            logger.error(f"[STORAGE] Error getting site/drive ID: {e}", exc_info=True)
            return None, None
    
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
        if not self.use_sharepoint or not self.access_token:
            logger.debug(f"[STORAGE] Skipping SharePoint list write - not configured")
            return False
        
        try:
            from audit_engine.canonical_fields import CanonicalField
            
            logger.info(f"[STORAGE] 📊 Writing metrics to SharePoint list for run {run_id}")
            
            # Calculate metrics from bucket_results
            total_buckets = len(bucket_results)
            matched = len(bucket_results[bucket_results[CanonicalField.STATUS.value] == 'Matched'])
            exceptions = bucket_results[bucket_results[CanonicalField.STATUS.value] != 'Matched']
            
            # Count by status
            scheduled_not_billed = len(bucket_results[bucket_results[CanonicalField.STATUS.value] == 'Scheduled Not Billed'])
            billed_not_scheduled = len(bucket_results[bucket_results[CanonicalField.STATUS.value] == 'Billed Not Scheduled'])
            amount_mismatch = len(bucket_results[bucket_results[CanonicalField.STATUS.value] == 'Amount Mismatch'])
            
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
            site_id, _ = self._get_site_and_drive_id()
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
        if not self.use_sharepoint or not self.access_token:
            logger.debug(f"[STORAGE] SharePoint list not configured, returning empty list")
            return []
        
        try:
            logger.info(f"[STORAGE] 📊 Loading metrics from SharePoint list")
            
            # Get site ID
            site_id, _ = self._get_site_and_drive_id()
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
                '$top': 1000,  # Get up to 1000 items
                '$orderby': 'fields/RunDateTime desc'  # Most recent first
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
