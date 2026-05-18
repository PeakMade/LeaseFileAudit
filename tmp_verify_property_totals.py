from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from config import config
from activity_logging.sharepoint import _get_app_only_token
from storage.service import StorageService
from audit_engine.canonical_fields import CanonicalField

RUN_ID = os.getenv('VERIFY_RUN_ID', 'run_20260513_130603')
PROPERTY_ID = int(float(os.getenv('VERIFY_PROPERTY_ID', '1150907')))

svc = StorageService(
    base_dir=Path('instance/runs'),
    use_sharepoint=config.storage.is_sharepoint_configured(),
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=_get_app_only_token(),
    audit_results_list_name=config.auth.audit_results_list_name,
)

snapshot = svc.load_run_display_snapshot_from_sharepoint_list(
    run_id=RUN_ID,
    scope_type='property',
    property_id=PROPERTY_ID,
)
print('SNAPSHOT', snapshot)

b = svc.load_bucket_results(RUN_ID, property_id=PROPERTY_ID)
if b.empty:
    print('No bucket rows found')
    raise SystemExit(0)

status_col = CanonicalField.STATUS.value
variance_col = CanonicalField.VARIANCE.value
lease_col = CanonicalField.LEASE_INTERVAL_ID.value

non_exception = {config.reconciliation.status_matched, 'SCHEDULED_ONLY'}
exceptions = b[~b[status_col].isin(non_exception)].copy()
exceptions[variance_col] = pd.to_numeric(exceptions[variance_col], errors='coerce').fillna(0.0)

under = abs(float(exceptions.loc[exceptions[variance_col] < 0, variance_col].sum()))
over = float(exceptions.loc[exceptions[variance_col] > 0, variance_col].sum())

print('CALC_FROM_BUCKETS')
print({'run_id': RUN_ID, 'property_id': PROPERTY_ID, 'exception_rows': int(len(exceptions)), 'undercharge': under, 'overcharge': over})

print('TOP_EXCEPTIONS')
view_cols = [lease_col, status_col, variance_col]
for col in view_cols:
    if col not in exceptions.columns:
        print(f'Missing column: {col}')
        raise SystemExit(0)

show = exceptions[view_cols].copy()
show = show.sort_values(by=variance_col, key=lambda s: s.abs(), ascending=False)
print(show.head(15).to_string(index=False))
