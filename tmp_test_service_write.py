"""
Test the actual _write_results_to_sharepoint_list path for AuditRuns2
using a minimal test DataFrame to see if fields are written.
"""
import os, sys, pandas as pd
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '.')

from config import config
from storage.service import StorageService
from activity_logging.sharepoint import _get_app_only_token

access_token = _get_app_only_token()

svc = StorageService(
    base_dir=config.storage.base_dir,
    use_sharepoint=True,
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=access_token,
    audit_results_list_name='AuditRuns2',  # Only AuditRuns2
)

# Build minimal test dataframes matching what the app produces
bucket_results = pd.DataFrame([{
    'PROPERTY_ID': 9999999,
    'LEASE_INTERVAL_ID': 12345,
    'AR_CODE_ID': 154771,
    'AUDIT_MONTH': '2026-01',
    'status': 'OK',
    'severity': 'low',
    'variance': 0.0,
    'expected_total': 100.0,
    'actual_total': 100.0,
    'impact_amount': 0.0,
    'match_rule': 'exact',
    'title': 'Test row',
}])

findings = pd.DataFrame([{
    'PROPERTY_ID': 9999999,
    'LEASE_INTERVAL_ID': 12345,
    'AR_CODE_ID': 154771,
    'AUDIT_MONTH': '2026-01',
    'status': 'FINDING',
    'severity': 'medium',
    'variance': 50.0,
    'expected_total': 100.0,
    'actual_total': 50.0,
    'impact_amount': 50.0,
    'match_rule': 'none',
    'title': 'Test finding',
    'finding_id': 'f-test-001',
    'category': 'rent',
    'description': 'Debug test finding',
    'expected_value': '100',
    'actual_value': '50',
}])

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

print('Writing test rows to AuditRuns2...')
result = svc._write_results_to_sharepoint_list(
    run_id='run_DEBUG_TEST_999',
    bucket_results=bucket_results,
    findings=findings,
    target_list_name='AuditRuns2',
)
print(f'Result: {result}')
