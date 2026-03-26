# LeaseFileAudit Project Takeover Guide

## 1) What this app does

LeaseFileAudit compares **scheduled charges** (what should be billed) to **AR transactions** (what was billed), then surfaces mismatches by:
- Portfolio
- Property
- Lease
- AR code and month

Primary mismatch classes:
- `SCHEDULED_NOT_BILLED`
- `BILLED_NOT_SCHEDULED`
- `AMOUNT_MISMATCH`
- `MATCHED`

Core reconciliation logic lives in:
- `audit_engine/reconcile.py`
- `audit_engine/expand.py`
- `audit_engine/mappings.py`

---

## 2) End-to-end audit flow

Main orchestration:
- `web/views.py` → `execute_audit_run(...)`

Flow steps:
1. Load source data (Excel tabs or API preloaded sources)
2. Map raw columns to canonical schema
3. Normalize and validate both datasets
4. Expand scheduled charges into monthly expected buckets
5. Reconcile expected vs actual at bucket grain (`PROPERTY_ID`, `LEASE_INTERVAL_ID`, `AR_CODE_ID`, `AUDIT_MONTH`)
6. Generate findings and variance detail
7. Save run artifacts + list records

Entry points:
- Excel upload: `web/views.py` → `upload()`
- API property upload: `web/views.py` → `upload_api_property()`

---

## 3) Inputs

## 3.1 Excel upload path

User uploads one `.xlsx` file. The app expects two logical source datasets:
- AR transactions (`ar_transactions`)
- Scheduled charges (`scheduled_charges`)

Configured in:
- `config.py` (`AuditConfig.ar_source`, `AuditConfig.scheduled_source`)

## 3.2 API upload path

For one property (or one lease), API sources are fetched first, then passed into the **same** audit pipeline through `preloaded_sources`.

---

## 4) Storage model

Storage service:
- `storage/service.py` (`StorageService`)

Each run (`run_<timestamp>`) stores:
- `inputs_normalized/expected_detail.csv`
- `inputs_normalized/actual_detail.csv`
- `outputs/bucket_results.csv`
- `outputs/findings.csv`
- `outputs/variance_detail.csv` (when present)
- `run_meta.json`
- original upload file (if provided)

If SharePoint storage is enabled:
- Document library (`SHAREPOINT_LIBRARY_NAME`, default `LeaseFileAudit Runs`) stores run files.
- Lists store queryable app data:
  - `AuditRuns` (bucket_result/finding rows)
  - `RunDisplaySnapshots` (portfolio/property/lease snapshots)
  - `ExceptionMonths` (month-level resolution state)
  - `ExceptionStates` (workflow state)
  - `Audit Run Metrics`
  - lease-term lists (`LeaseTermSet`, `LeaseTerms`, `LeaseTermEvidence`)

Important resilience behavior:
- `load_bucket_results()` and `load_findings()` prefer SharePoint list rows but **fall back to CSV** if list rows are partial.

---

## 5) What users see on the Property page

Route:
- `web/views.py` → `property_view(property_id, run_id)`

Primary dataset for rows:
- `bucket_results` filtered to the property

Supporting datasets:
- `RunDisplaySnapshots` (property + lease scope) for static summary values
- `ExceptionMonths` bulk data for resolved/unresolved month filtering
- `expected_detail` and `actual_detail` for lease/customer labels and IDs

Output behavior:
- One lease summary row per lease interval
- Status per lease: `Passed`, `Resolved`, or `Open`
- KPIs (undercharge/overcharge/counts), using snapshot values when available

---

## 6) What users see on the Lease page

Route:
- `web/views.py` → `lease_view(property_id, lease_interval_id, run_id)`

Primary dataset:
- Lease-scoped `bucket_results`

Supporting datasets:
- `expected_detail` and `actual_detail` for transaction-level detail
- `ExceptionMonths` for month-level resolution state
- `RunDisplaySnapshots` lease scope for header total overrides
- Lease term extraction/overlay (`LeaseTerms` + refresh path)

Output behavior:
- Exception groups by AR code and status
- Monthly line details with expected vs actual transactions
- Matched records section
- Unified AR-code view combining matched + exception months
- Header totals from unresolved months (with snapshot override when available)

---

## 7) Performance and timeout design

Current behavior is optimized to reduce upload response time:
- Async detailed list writes by default (`ASYNC_AUDIT_RESULTS_WRITE=true`)
- Async snapshot writes by default (`ASYNC_RUN_DISPLAY_SNAPSHOTS=true`)
- Async snapshot validation by default (`ASYNC_SNAPSHOT_VALIDATION=true`)

Key safeguard:
- If async list writes are incomplete at read time, UI falls back to CSV-backed results for completeness.

---

## 8) Key environment variables

Storage/Auth:
- `USE_SHAREPOINT_STORAGE`
- `SHAREPOINT_LIBRARY_NAME`
- `SHAREPOINT_SITE_URL`
- `SHAREPOINT_CLIENT_ID`
- `SHAREPOINT_TENANT_ID`
- `REQUIRE_AUTH`

Performance:
- `ASYNC_AUDIT_RESULTS_WRITE`
- `ASYNC_RUN_DISPLAY_SNAPSHOTS`
- `ASYNC_SNAPSHOT_VALIDATION`
- `SHAREPOINT_BATCH_SIZE_AUDITRUNS`
- `SHAREPOINT_BATCH_SIZE_SNAPSHOTS`

Lease terms:
- `LEASE_TERM_REFRESH_TTL_HOURS`
- `LEASE_TERM_FORCE_REFRESH`
- `LEASE_TERM_SET_LIST_ID` / `_URL`
- `LEASE_TERMS_LIST_ID` / `_URL`
- `LEASE_TERM_EVIDENCE_LIST_ID` / `_URL`

---

## 9) Operational debugging checklist

If Property page shows too few leases:
1. Compare row counts in logs for list-backed vs CSV fallback
2. Verify `bucket_results` includes required columns (`PROPERTY_ID`, `LEASE_INTERVAL_ID`, `status`)
3. Check `ExceptionMonths` filtering is not excluding expected rows
4. Confirm run_id alignment across upload redirect and page load

If Lease page totals look off:
1. Verify unresolved-month logic in lease monthly details
2. Check whether snapshot overrides are active
3. Confirm exception month statuses are loading for that lease/AR code

If uploads are timing out:
1. Confirm async flags are enabled
2. Check SharePoint throttling (`429/503/504`) in logs
3. Reduce batch size env vars if throttling is frequent

---

## 10) New owner quick-start

1. Read in order:
   - `MASTER_DOCUMENTATION.md`
   - `SHAREPOINT_CONNECTIONS_HANDOFF.md`
   - `PROJECT_TAKEOVER_GUIDE.md`
2. Run one known file through Excel upload and verify:
   - run folder artifacts
   - `AuditRuns` rows
   - `RunDisplaySnapshots` rows
   - Property and Lease pages render full data
3. Validate one month-resolution update and confirm counts/status update correctly.

---

## 11) File map (most important)

- Audit orchestration and routes: `web/views.py`
- Storage and SharePoint integration: `storage/service.py`
- Mapping raw → canonical: `audit_engine/mappings.py`
- Schedule expansion: `audit_engine/expand.py`
- Reconciliation rules: `audit_engine/reconcile.py`
- Lease term mapping defaults: `audit_engine/lease_term_rules.py`
- App configuration: `config.py`
