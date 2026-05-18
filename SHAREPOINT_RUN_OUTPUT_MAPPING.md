# SharePoint List Data Flow & Run Output Mapping

## Overview

Each audit run writes data to multiple SharePoint lists. This document maps which audit run produces which data for each list.

---

## 1. **AuditRuns** (Primary Results List)
**SharePoint List ID:** `5b6a2632-1f6b-47de-b7ab-653dc20561c1`  
**List Name Fallback:** "Audit Run Results"

### Purpose
Stores detailed row-level audit results and findings from lease reconciliation. Each row represents either a lease bucket or a specific finding.

### Data Source
- **From:** `_write_results_to_sharepoint_list()` in `storage/service.py`
- **Triggered By:** Every audit run completion

### What Gets Written

#### Row Data (Findings)
For each finding identified during the audit:
- `RunId` - Unique audit run identifier
- `ResultType` - Type of result: "finding" or "metric"
- `PropertyId` - Property being audited
- `LeaseIntervalId` - Lease account being audited
- `ARCodeId` - AR code (charge type) being audited
- `AuditMonth` - Month being audited (normalized to full date)
- `Status` - Finding type: "matched", "scheduled_not_billed", "billed_not_scheduled", "amount_mismatch"
- `Severity` - "high" or "medium"
- `Amount` - Variance amount
- `ScheduledTotal` - Expected total from system
- `ActualTotal` - Actual billed amount
- `Variance` - Difference
- `Description` - Human-readable finding description
- `Evidence` - JSON metadata (IDs of scheduled charges, AR transactions)
- `PropertyName` - Property display name (if available)
- `ResidentName` - Resident/lease name (if available)

### Write Frequency
- **Synchronously** during run finalization, OR
- **Asynchronously** (background thread) if `write_results_async=True`

### Example Run Data
When you run an audit for property 17701937, lease 155007:
- 1 row per bucket/finding created
- Each row includes bucket-level status, amounts, and evidence
- All rows tagged with same `RunId`

---

## 2. **RunDisplaySnapshots** (Portfolio Snapshot)
**SharePoint List ID:** `613d8abf-958d-4f1f-a142-4d2ed44c37a4`

### Purpose
Stores portfolio, property, and lease-level snapshot views for quick dashboard loading and audit status tracking.

### Data Source
- **From:** `_write_run_display_snapshots_to_sharepoint_list()` in `storage/service.py`
- **Triggered By:** Every audit run completion

### What Gets Written

#### Snapshot Rows (3 Levels)
For each run, creates snapshots at portfolio, property, and lease levels:

**Portfolio Snapshot (1 row per run):**
- `ScopeType` = "portfolio"
- `RunId` - Audit run ID
- `AuditedThrough` - Latest audit month
- `RunScopeType` - Scope of run (e.g., "by_property")
- `ExceptionCount` - Total exceptions found
- `ResolvedCount` - Resolved exceptions
- `UnresolvedCount` - Unresolved exceptions

**Property Snapshots (1 row per property in run):**
- `ScopeType` = "property"
- `PropertyId` - Property ID
- `PropertyName` - Property display name
- `ExceptionCount` - Exceptions for this property
- `UnresolvedCount` - Unresolved for this property
- `ParentRunId` - Reference to portfolio snapshot
- `AuditedThrough` - Latest month for this property

**Lease Snapshots (1 row per lease audited):**
- `ScopeType` = "lease"
- `LeaseIntervalId` - Lease account ID
- `PropertyId` - Parent property
- `ResidentName` - Resident name
- `ExceptionCount` - Exceptions for this lease
- `UnresolvedCount` - Unresolved for this lease
- `ParentPropertyId` - Reference to property snapshot

### Write Frequency
- **Synchronously** during run finalization, OR
- **Asynchronously** (background thread) if `write_snapshots_async=True`

### Filtering Logic
- Only captures **unresolved** snapshots (exceptions that haven't been marked as resolved)
- Skips matching/resolved buckets

### Example Run Data
When you audit portfolio for Jan 2025:
- 1 portfolio snapshot row
- N property snapshot rows (one per property with exceptions)
- M lease snapshot rows (one per lease with exceptions)
- All linked together with `ParentRunId` / `ParentPropertyId` relationships

---

## 3. **Audit Run Metrics** (Summary Statistics)
**SharePoint List ID:** `0b9b4d90-32c6-4f37-b8df-459c4012d3b5`

### Purpose
Stores high-level run metrics for reporting, trend analysis, and run summary views.

### Data Source
- **From:** `_write_metrics_to_sharepoint_list()` in `storage/service.py`
- **Triggered By:** Every audit run completion

### What Gets Written

#### Metrics Row (1 row per run)
For each completed audit run:
- `Title` - Run ID
- `RunDateTime` - Timestamp of run
- `UploadedBy` - User who initiated the run
- `FileName` - Source file name (if uploaded)
- `TotalScheduled` - Sum of all scheduled amounts
- `TotalActual` - Sum of all actual amounts
- `Matched` - Count of matched buckets
- `ScheduledNotBilled` - Count of scheduled charges not billed
- `BilledNotScheduled` - Count of actual charges without schedules
- `AmountMismatch` - Count of amount mismatches
- `TotalVariances` - Total exception count
- `HighSeverity` - Count of high-severity findings
- `MediumSeverity` - Count of medium-severity findings
- `Properties` - JSON object with per-property breakdown:
  - `total_buckets` - Buckets per property
  - `exceptions` - Exception count per property
  - `variance` - Total variance per property

### Write Frequency
- **Synchronously** during run finalization, OR
- **Asynchronously** (background thread) if `write_metrics_async=True`

### Example Run Data
Single row summarizing an entire audit run:
```json
{
  "RunId": "run_20260512_120000",
  "TotalScheduled": 1250000.00,
  "TotalActual": 1243567.89,
  "Matched": 847,
  "TotalVariances": 23,
  "Properties": {
    "17701937": {"total_buckets": 50, "exceptions": 2, "variance": 425.67},
    "18123383": {"total_buckets": 45, "exceptions": 0, "variance": 0.00}
  }
}
```

---

## 4. **LeaseTermSet, LeaseTerms, LeaseTermEvidence**
**List IDs:**
- `LeaseTermSet`: `959169fb-fd74-4d9f-a3af-5502e999c849`
- `LeaseTerms`: `8c374324-7413-42ae-b6c0-05d1bd248416`
- `LeaseTermEvidence`: `1145d114-2891-4c74-96bd-97b7b4860968`

### Purpose
Store parsed lease term data extracted from resident documents (lease addenda, riders, etc.) for multi-run reconciliation and lease intelligence.

### Data Source
- **From:** `audit_engine/entrata_lease_terms.py` and `audit_engine/canonical_fields.py`
- **Triggered By:** Individual resident/lease processing during audit run
- **Persistence:** Via `storage/service.py` lease term loading/caching

### What Gets Written

#### LeaseTermSet (Sets of lease attributes)
- `LeaseKey` - Unique lease identifier
- `SetName` - Name of lease attribute set (e.g., "rent_schedule")
- `IsActive` - Boolean flag indicating current active term
- `EffectiveDate` - When term became active
- `ExpirationDate` - When term expires

#### LeaseTerms (Individual lease terms)
- `LeaseKey` - Parent lease
- `TermType` - Type (e.g., "rent", "concession", "utility")
- `Amount` - Term value
- `Frequency` - How often it applies (monthly, annual, etc.)
- `ARCode` - Associated AR code
- `SetReference` - Link to parent LeaseTermSet

#### LeaseTermEvidence (Supporting documents)
- `LeaseKey` - Associated lease
- `DocumentType` - Type of lease document (lease, addendum, rider)
- `DocumentPage` - Page number in document
- `ExtractedText` - OCR/extracted text from document
- `ConfidenceScore` - Confidence in extraction (0-1)

### Write Frequency
- **On-demand** during resident document fetch and parsing
- **Cached** - Not rewritten if `DocListFingerprint` hasn't changed (optimization)

### Example Run Data
When auditing a resident with lease 155007:
- 1 LeaseTermSet row per active lease attribute set
- N LeaseTerms rows (one per identified term: rent, concessions, utilities, etc.)
- M LeaseTermEvidence rows (one per supporting document snippet)

---

## 5. **ExceptionMonths**
**SharePoint List ID:** `3638e107-5231-4bac-9a6c-cef9c501db05`

### Purpose
Track which months have unresolved exceptions for each lease, enabling month-level filtering and drill-down.

### Data Source
- **From:** Exception resolution tracking in `web/views.py` and `storage/service.py`
- **Triggered By:** When exceptions are marked as resolved/unresolved

### What Gets Written

#### Exception Month Rows
For each lease and month combination with exceptions:
- `LeaseIntervalId` - Lease account ID
- `AuditMonth` - Month with exception
- `ExceptionCount` - Number of exceptions in this month
- `IsResolved` - Boolean flag
- `ResolvedBy` - User who resolved (if applicable)
- `ResolvedDate` - Date of resolution (if applicable)
- `Notes` - User notes about exception

### Write Frequency
- **On demand** when exception status changes via UI or API

### Example Run Data
If lease 155007 has 2 unresolved exceptions in Jan 2025:
- 1 row: `LeaseIntervalId=155007, AuditMonth=2025-01-01, ExceptionCount=2, IsResolved=false`

---

## Data Flow Diagram

```
Audit Run Execution
    ↓
   ├─→ [AuditRuns] ← Detailed findings, per-bucket results
   ├─→ [RunDisplaySnapshots] ← Portfolio/property/lease snapshots
   ├─→ [Audit Run Metrics] ← Summary metrics and statistics
   ├─→ [LeaseTermSet, LeaseTerms, LeaseTermEvidence] ← Lease term intelligence
   └─→ [ExceptionMonths] ← Month-level exception tracking (on exception update)
```

---

## Synchronous vs. Asynchronous Writes

By default, lists are written **asynchronously** (background threads) to avoid blocking the UI:

- **AuditRuns results:** Background thread via `_write_results_to_sharepoint_list_async()`
- **RunDisplaySnapshots:** Background thread via background task in `save_audit_run()`
- **Audit Run Metrics:** Background thread via `_write_metrics_to_sharepoint_list_async()`

This can be controlled via environment variables:
- `WRITE_RESULTS_ASYNC` (default: true)
- `WRITE_SNAPSHOTS_ASYNC` (default: true)
- `WRITE_METRICS_ASYNC` (default: true)

---

## Cleanup & Data Retention

The cleanup script (`clean_sharepoint_lists.py`) **preserves AuditRuns** but clears:
- RunDisplaySnapshots (stale property/lease views)
- LeaseTermSet, LeaseTerms, LeaseTermEvidence (re-parsed on next run)
- ExceptionMonths (exceptions reset)
- Audit Run Metrics (summary statistics reset)

---

## Quick Reference Table

| List Name | Primary Use | Rows Per Run | Async | Cleared by Cleanup |
|-----------|-------------|----------|-------|-----|
| **AuditRuns** | Findings & results | Many (1 per finding) | Yes | ❌ No |
| **RunDisplaySnapshots** | Portfolio/property views | Few (1 per scope level) | Yes | ✅ Yes |
| **Audit Run Metrics** | Summary stats | 1 | Yes | ✅ Yes |
| **LeaseTermSet** | Lease attributes | Variable | On-demand | ✅ Yes |
| **LeaseTerms** | Lease terms | Variable | On-demand | ✅ Yes |
| **LeaseTermEvidence** | Lease docs | Variable | On-demand | ✅ Yes |
| **ExceptionMonths** | Exception tracking | Variable | On-demand | ✅ Yes |

