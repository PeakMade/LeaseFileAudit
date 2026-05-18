# SharePoint Write Targets

This document lists every SharePoint destination this app writes to, including list/library names, triggers, and key fields.

## Summary

The app writes to:

- SharePoint list for activity/audit logging (`SHAREPOINT_LIST_NAME`, default `Innovation Use Log`)
- SharePoint list `Audit Run Metrics`
- SharePoint list `AuditRuns` (legacy fallback: `Audit Run Results`)
- SharePoint list `RunDisplaySnapshots` (legacy fallback: `Run Display Snapshots`)
- SharePoint list `ExceptionMonths`
- SharePoint list `LeaseTermSet` (legacy fallback: `Lease Term Set`)
- SharePoint list `LeaseTerms` (legacy fallback: `Lease Terms`)
- SharePoint list `LeaseTermEvidence` (legacy fallback: `Lease Term Evidence`)
- SharePoint document library for run artifacts (`SHAREPOINT_LIBRARY_NAME`, default `LeaseFileAudit Runs`)

## 1) Activity Log List

- Config:
  - Site URL: `SHAREPOINT_SITE_URL`
  - List name: `SHAREPOINT_LIST_NAME` (default `Innovation Use Log`)
  - Toggle: `ENABLE_SHAREPOINT_LOGGING`
- Write trigger:
  - Session start/end events
  - Successful/failed audits from upload routes
- Code path:
  - `activity_logging/sharepoint.py` -> `log_user_activity()` -> `SharePointLogger.log_activity()`
  - Called from `app.py` and `web/views.py`
- Graph write:
  - `POST /sites/{site_id}/lists/{list_id}/items`
- Fields written:
  - `Title`
  - `UserName`
  - `UserEmail`
  - `ActivityType`
  - `Application`
  - `UserRole`
  - `Env`
  - `LoginTimestamp`
  - `SessionID` (when available)

Note: `details` passed to `log_user_activity()` are currently not persisted to columns by `SharePointLogger.log_activity()`.

## 2) Audit Run Metrics List

- List name: `Audit Run Metrics`
- Write trigger:
  - During `StorageService.save_run(...)` step 5 (sync or async based on `ASYNC_METRICS_WRITE`)
- Code path:
  - `storage/service.py` -> `_write_metrics_to_sharepoint_list()`
- Graph write:
  - `POST /sites/{site_id}/lists/{list_id}/items`
- Fields written (high level):
  - `Title` (run id)
  - `RunDateTime`
  - `UploadedBy`
  - `TotalScheduled`
  - `TotalActual`
  - `Matched`
  - `ScheduledNotBilled`
  - `BilledNotScheduled`
  - `AmountMismatch`
  - `TotalVariances`
  - `HighSeverity`
  - `MediumSeverity`
  - `Properties` (JSON string)

## 3) Audit Results Detail List

- List name:
  - Preferred: `AuditRuns`
  - Legacy fallback: `Audit Run Results`
- Write trigger:
  - During `StorageService.save_run(...)` step 7 (sync or async based on `ASYNC_AUDIT_RESULTS_WRITE`)
- Code path:
  - `storage/service.py` -> `_write_results_to_sharepoint_list()`
- Graph write:
  - Batched `POST` (`/v1.0/$batch`) to `.../lists/{list_id}/items` with single-item fallback
- Row types:
  - `bucket_result`
  - `finding`
- Fields written:
  - `Title`
  - `RunId`
  - `ResultType`
  - `PropertyId`
  - `LeaseIntervalId`
  - `ArCodeId`
  - `AuditMonth`
  - `Status`
  - `Severity`
  - `FindingTitle`
  - `Variance`
  - `ExpectedTotal`
  - `ActualTotal`
  - `ImpactAmount`
  - `MatchRule`
  - `FindingId`
  - `Category`
  - `Description`
  - `ExpectedValue`
  - `ActualValue`
  - `CreatedAt`
  - Optional columns when present on list schema: `PropertyName`, `ResidentName`

## 4) Run Display Snapshots List

- List name:
  - Preferred: `RunDisplaySnapshots`
  - Legacy fallback: `Run Display Snapshots`
- Write trigger:
  - During `StorageService.save_run(...)` step 6 (sync or async based on `ASYNC_RUN_DISPLAY_SNAPSHOTS`)
- Code path:
  - `storage/service.py` -> `_write_run_display_snapshots_to_sharepoint_list()`
- Graph write:
  - Batched `POST` (`/v1.0/$batch`) to `.../lists/{list_id}/items`
- Scope rows written:
  - `portfolio`
  - `property`
  - `lease`
- Core fields written:
  - `Title`
  - `SnapshotKey`
  - `RunId`
  - `ScopeType`
  - `PropertyId`
  - `LeaseIntervalId`
  - `ExceptionCountStatic` (or legacy `ExceptionCountStatistic`, detected at runtime)
  - `UnderchargeStatic`
  - `OverchargeStatic`
  - `MatchRateStatic`
  - `TotalBucketsStatic`
  - `MatchedBucketsStatic`
  - `CreatedAt`
- Optional fields written when columns exist:
  - `PropertyNameStatic` / `PropertyName`
  - `TotalVarianceStatic`
  - `TotalLeaseIntervalStatic`
  - `RunScopeType`
  - `AuditedThrough`

## 5) Exception Month Status List

- List name: `ExceptionMonths`
- Write trigger:
  - Manual month resolution/status updates from UI/API flow
- Code path:
  - `web/views.py` calls `storage.upsert_exception_month_to_sharepoint_list(payload)`
  - Method in `storage/service.py`
- Graph write:
  - Query existing by composite key
  - `PATCH /items/{id}/fields` if found
  - `POST /items` if not found
- Fields written:
  - `CompositeKey`
  - `RunId`
  - `PropertyId`
  - `LeaseIntervalId`
  - `ArCodeId`
  - `AuditMonth`
  - `ExceptionType`
  - `Status`
  - `FixLabel`
  - `ActionType`
  - `Variance`
  - `ExpectedTotal`
  - `ActualTotal`
  - `ResolvedAt`
  - `ResolvedBy`
  - `ResolvedByName`
  - `Notes`
  - `UpdatedAt`
  - `UpdatedBy`

## 6) Lease Term Extraction Lists

These are used by lease-term workflows when those paths are executed.

### 6a) LeaseTermSet

- List name:
  - Preferred: `LeaseTermSet`
  - Legacy fallback: `Lease Term Set`
  - Optional direct ID override: `LEASE_TERM_SET_LIST_ID` / `LEASE_TERM_SET_LIST_URL`
- Write mode:
  - Upsert by `LeaseKey` (`PATCH` existing, `POST` create)
- Fields written:
  - `Title`, `LeaseKey`, `PropertyId`, `LeaseIntervalId`, `LeaseId`
  - `TermSetVersion`, `FingerprintHash`, `DocListFingerprint`, `SelectedDocIds`
  - `LastCheckedAt`, `LastRefreshedAt`, `Status`, `RefreshError`, `RunIdLastSeen`

### 6b) LeaseTerms

- List name:
  - Preferred: `LeaseTerms`
  - Legacy fallback: `Lease Terms`
  - Optional direct ID override: `LEASE_TERMS_LIST_ID` / `LEASE_TERMS_LIST_URL`
- Write mode:
  - Replace pattern by `LeaseKey` (delete existing rows, then insert current rows)
- Fields written:
  - `Title`, `TermKey`, `LeaseKey`, `PropertyId`, `LeaseIntervalId`, `LeaseId`
  - `TermSetVersion`, `IsActive`, `TermType`, `MappedArCode`, `Amount`, `Frequency`
  - `StartDate`, `EndDate`, `DueDay`, `ConditionsKey`
  - `TermSourceDocId`, `TermSourceDocName`
  - `MappingVersion`, `MappingConfidence`, `UpdatedAt`

### 6c) LeaseTermEvidence

- List name:
  - Preferred: `LeaseTermEvidence`
  - Legacy fallback: `Lease Term Evidence`
  - Optional direct ID override: `LEASE_TERM_EVIDENCE_LIST_ID` / `LEASE_TERM_EVIDENCE_LIST_URL`
- Write mode:
  - Replace pattern by `LeaseKey` (delete existing rows, then insert current rows)
- Fields written:
  - `Title`, `EvidenceKey`, `TermKey`, `LeaseKey`
  - `PropertyId`, `LeaseIntervalId`, `LeaseId`
  - `DocId`, `DocName`, `PageNumber`, `ExcerptText`
  - `Confidence`, `CapturedAt`

## 7) SharePoint Document Library (Not a List)

- Library config:
  - `SHAREPOINT_LIBRARY_NAME` (default `LeaseFileAudit Runs`)
- Write trigger:
  - Every `save_run(...)` when SharePoint storage is enabled
- Code path:
  - `storage/service.py` file persistence methods (`_save_dataframe`, `_save_json`, original file upload)
- Content written:
  - `inputs_normalized/expected_detail.csv`
  - `inputs_normalized/actual_detail.csv`
  - `outputs/bucket_results.csv`
  - `outputs/findings.csv`
  - `outputs/variance_detail.csv` (if present)
  - `run_meta.json`
  - Original uploaded file (when available)

## Operational Notes

- Most list writes use Microsoft Graph with app-only token fallback when needed.
- `AuditRuns` and `RunDisplaySnapshots` use Graph batch writes with retry and single-row fallback.
- Asynchronous write toggles:
  - `ASYNC_METRICS_WRITE` (default true)
  - `ASYNC_RUN_DISPLAY_SNAPSHOTS` (default true)
  - `ASYNC_AUDIT_RESULTS_WRITE` (default false)
- Cleanup utility `clean_sharepoint_lists.py` can bulk-delete from major list targets.
