# SharePoint Connections Handoff

This document summarizes every SharePoint connection currently used by the app so a new owner can understand what exists, why it exists, and where it is used.

## 1) SharePoint Site + Authentication

- Site URL source: `SHAREPOINT_SITE_URL`
- Main auth paths:
  - App-only token (`_get_app_only_token`) for service/storage/list operations
  - Delegated token path for activity logging in hosted auth scenarios

Primary code entry points:
- `StorageService` SharePoint setup and site/list/drive resolution: `storage/service.py`
- Activity logging client: `activity_logging/sharepoint.py`
- App config/env wiring: `config.py`

## 2) SharePoint Document Libraries

### A) Main run storage library

- Library display name source: `SHAREPOINT_LIBRARY_NAME`
- Default value: `LeaseFileAudit Runs`
- Purpose:
  - Stores run folders (`run_YYYYMMDD_HHMMSS`)
  - Stores normalized inputs, outputs, metadata, and uploaded source files
  - Powers run discovery (`list_runs`) when SharePoint mode is enabled

Used in:
- Library config and mode switch: [config.py](config.py#L65-L73)
- Drive resolution and file upload/download: [storage/service.py](storage/service.py#L97-L126), [storage/service.py](storage/service.py#L2815-L2898)
- Save/load run artifacts and run listing: [storage/service.py](storage/service.py#L3196-L3565)

### B) Entrata lease PDF folder path (inside configured library)

- Relative folder pattern: `Entrata leases/<property_id>/...`
- Purpose:
  - Stores downloaded selected lease packet/addenda/merged PDFs for traceability
  - Keeps lease extraction source artifacts in SharePoint rather than local disk

Used in:
- Relative path construction and upload call: [audit_engine/entrata_lease_terms.py](audit_engine/entrata_lease_terms.py#L1172-L1197)
- Invocation from packet/addenda/merged flows: [audit_engine/entrata_lease_terms.py](audit_engine/entrata_lease_terms.py#L1201-L1263)

## 3) SharePoint Lists (Data + Workflow)

## 3.1 `AuditRuns` (legacy fallback: `Audit Run Results`)

- Purpose:
  - List-backed persistence of detailed reconciliation rows
  - Stores both `bucket_result` and `finding` row types per run
  - Used as read path for run detail loading (with CSV fallback)

Used in:
- Preferred/legacy resolver: [storage/service.py](storage/service.py#L277-L287)
- Write detailed rows: [storage/service.py](storage/service.py#L1598-L1760)
- Read detailed rows: [storage/service.py](storage/service.py#L1708-L1760)
- Save pipeline invocation: [storage/service.py](storage/service.py#L3362-L3380)

## 3.2 `RunDisplaySnapshots` (legacy fallback: `Run Display Snapshots`)

- Purpose:
  - Fast UI snapshots for portfolio/property/lease views
  - Stores static summary rows so UI does not always recompute from full detail data

Used in:
- Preferred/legacy resolver: [storage/service.py](storage/service.py#L288-L299)
- Snapshot write/read logic: [storage/service.py](storage/service.py#L749-L1245)
- View-level reads: [web/views.py](web/views.py#L210)

## 3.3 `Audit Run Metrics`

- Purpose:
  - Aggregated run-level metrics for fast portfolio/dashboard loading
  - Stores totals and summary counts by run

Used in:
- Metrics write: [storage/service.py](storage/service.py#L2964-L3078)
- Metrics read: [storage/service.py](storage/service.py#L3084-L3145)
- Portfolio fast path: [web/views.py](web/views.py#L790)

## 3.4 `ExceptionMonths`

- Purpose:
  - Per-month exception resolution persistence (core resolution state)
  - Enables cross-run historical resolution matching and auto-apply behavior

Used in:
- Load by lease/ar-code and bulk by property: [storage/service.py](storage/service.py#L2390-L2655)
- Upsert month resolution: [storage/service.py](storage/service.py#L2656-L2720)
- UI/API calls in views: [web/views.py](web/views.py#L613), [web/views.py](web/views.py#L2652-L2703), [web/views.py](web/views.py#L3757)

## 3.5 `ExceptionStates`

- Purpose:
  - Exception workflow state tracking at exception-level (distinct from month-level records)

Used in:
- Load/upsert methods: [storage/service.py](storage/service.py#L1937-L2078)
- API usage in views: [web/views.py](web/views.py#L2623-L2637)

## 3.6 `LeaseTermSet` (legacy fallback: `Lease Term Set`)

- Purpose:
  - One row per lease key for lease-term refresh/version/fingerprint control
  - Tracks doc list fingerprint, selected docs, status, and refresh metadata

Used in:
- Resolver + env override (`LEASE_TERM_SET_LIST_ID` / `LEASE_TERM_SET_LIST_URL`): [storage/service.py](storage/service.py#L301-L315)
- Upsert/load methods: [storage/service.py](storage/service.py#L2080-L2139), [storage/service.py](storage/service.py#L2334-L2384)
- Lease-term refresh pipeline: [audit_engine/entrata_lease_terms.py](audit_engine/entrata_lease_terms.py#L2982-L3209)

## 3.7 `LeaseTerms` (legacy fallback: `Lease Terms`)

- Purpose:
  - Active normalized extracted lease term rows by lease key

Used in:
- Resolver + env override (`LEASE_TERMS_LIST_ID` / `LEASE_TERMS_LIST_URL`): [storage/service.py](storage/service.py#L316-L330)
- Replace/load methods: [storage/service.py](storage/service.py#L2140-L2330)
- UI load for lease expectation overlay: [web/views.py](web/views.py#L3989)

## 3.8 `LeaseTermEvidence` (legacy fallback: `Lease Term Evidence`)

- Purpose:
  - Evidence snippets/pages associated with extracted lease terms

Used in:
- Resolver + env override (`LEASE_TERM_EVIDENCE_LIST_ID` / `LEASE_TERM_EVIDENCE_LIST_URL`): [storage/service.py](storage/service.py#L331-L345)
- Replace method: [storage/service.py](storage/service.py#L2207-L2263)

## 3.9 Property picklist list (default: `Properties_0`)

- Purpose:
  - Authoritative property id/name source for API upload property picklist
- Env override: `LEASE_API_PROPERTIES_SHAREPOINT_LIST`
- Default list name: `Properties_0`

Used in:
- Fetch + resolve site/list IDs and read rows: [audit_engine/api_ingest.py](audit_engine/api_ingest.py#L132-L236)
- Upload form/view usage via cached picklist loader: [web/views.py](web/views.py#L150-L153), [web/views.py](web/views.py#L2168-L2173)

## 3.10 Activity log list (config-driven; default: `Innovation Use Log`)

- Purpose:
  - User/session activity log (start session, end session, audit success/failure, etc.)
- Env var: `SHAREPOINT_LIST_NAME`
- Default from config: `Innovation Use Log`
- Note: `SharePointLogger` class default constructor list name is `AuditLog`, but app wiring typically passes configured list name.

Used in:
- Config defaults: [config.py](config.py#L91-L104)
- Logger implementation: [activity_logging/sharepoint.py](activity_logging/sharepoint.py#L68-L188)
- App and view call sites: [app.py](app.py#L159-L194), [web/views.py](web/views.py#L1854-L1863), [web/views.py](web/views.py#L2055-L2063), [web/views.py](web/views.py#L2114-L2121), [web/views.py](web/views.py#L2302-L2308), [web/views.py](web/views.py#L2455-L2461)

## 4) Quick Ownership Checklist

- Confirm site and token env vars are set:
  - `SHAREPOINT_SITE_URL`
  - `SHAREPOINT_CLIENT_ID`
  - `SHAREPOINT_TENANT_ID`
- Confirm library exists with correct display name:
  - `SHAREPOINT_LIBRARY_NAME` (default `LeaseFileAudit Runs`)
- Confirm lists exist (or legacy fallback names are present):
  - `AuditRuns` (or `Audit Run Results`)
  - `RunDisplaySnapshots` (or `Run Display Snapshots`)
  - `Audit Run Metrics`
  - `ExceptionMonths`
  - `ExceptionStates`
  - `LeaseTermSet` / `LeaseTerms` / `LeaseTermEvidence` (or spaced legacy names)
  - Property picklist list (default `Properties_0`)
  - Activity log list (`SHAREPOINT_LIST_NAME`, default `Innovation Use Log`)

## 5) Important Notes for Handoff

- The app intentionally uses both Document Library storage (files) and SharePoint Lists (queryable app data).
- Several list resolvers include legacy-name fallback to avoid hard breaks during migrations.
- Lease-term list IDs can be pinned via env vars (`*_LIST_ID` / `*_LIST_URL`) to avoid display-name drift.
- If a list disappears or is renamed, failures are often soft (warnings + fallback paths), so check logs when data appears stale.