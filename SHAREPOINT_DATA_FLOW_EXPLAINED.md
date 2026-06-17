# SharePoint Data Flow & List Architecture
## LeaseFileAudit Application

**Last Updated:** June 11, 2026

---

## Executive Summary

The LeaseFileAudit app writes to **5 SharePoint lists** every time an audit runs. These lists work together to provide both fast portfolio dashboards and detailed drill-down capability. Understanding the data flow helps explain why portfolio totals can reach tens of millions of dollars and why some operations are slower than others.

**Key Points:**
- **AuditRuns2** stores millions of detail rows (the bottleneck)
- **RunDisplaySnapshots** stores pre-calculated summaries (the speed layer)
- **Audit Run Metrics** stores high-level statistics (the dashboard)
- **ExceptionMonths** tracks resolution status (the workflow tracker)
- **Lease Terms** stores document intelligence (optional feature)

---

## The Five SharePoint Lists

### 1. AuditRuns2 - The Detail Archive 📊

**SharePoint List ID:** `d8166180-5dcb-41a9-84c0-0ab104b77c27`  
**List URL:** https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/AuditRuns2/AllItems.aspx

#### Purpose
Stores every single comparison (bucket) from your audit - the complete detail layer that powers drill-down views.

#### What Gets Written
For each **lease × AR code × audit month** combination:

**Row Structure:**
- `RunId` - Unique audit run identifier
- `ResultType` - "bucket_result" or "finding"
- `PropertyId` - Property being audited
- `LeaseIntervalId` - Lease account being audited
- `ArCodeId` - AR code (charge type) being audited
- `AuditMonth` - Month being audited (normalized to YYYY-MM-DD)
- `Status` - "matched", "scheduled_not_billed", "billed_not_scheduled", "amount_mismatch"
- `Severity` - "high" or "medium"
- `Variance` - Difference between expected and actual
- `ExpectedTotal` - Expected amount from scheduled charges
- `ActualTotal` - Actual billed amount
- `MatchRule` - Rule that identified the status
- `FindingId` - Unique finding identifier
- `Category` - Exception category
- `Description` - Human-readable finding description
- `PropertyName` - Property display name (enriched)
- `ResidentName` - Resident/lease name (enriched)

#### Data Volume Example

**Single Property Audit:**
- Property: University Gateway
- Leases: 1,250 active leases
- Audit Period: 12 months
- AR Codes per lease: ~3 (rent, utilities, parking)
- **Rows written:** 1,250 × 12 × 3 = **45,000 rows**

**Full Portfolio Audit:**
- Properties: 94 properties
- Average leases per property: 1,250
- Audit Period: 12 months
- AR Codes per lease: ~3
- **Rows written:** 94 × 1,250 × 12 × 3 = **~4.2 MILLION rows**

#### Write Behavior
- **Write Mode:** Asynchronous (background thread)
- **Environment Variable:** `ASYNC_AUDIT_RESULTS_WRITE=true`
- **Triggered By:** Every audit run completion
- **Batch Size:** 20 rows per batch (configurable via `SHAREPOINT_BATCH_SIZE_AUDITRUNS`)
- **Performance:** This is the slowest write - can take 10-30 minutes for large audits

#### Where It's Read From
- **Property View** (`/property/<property_id>`) - Loads all buckets for one property
- **Lease View** (`/lease/<property_id>/<lease_interval_id>`) - Loads all buckets for one resident
- **AR Status Calculation** - Calculates status badges (Passed/Failed/Partial)

#### Retention
✅ **Preserved by cleanup script** - Historical audit detail is retained

---

### 2. RunDisplaySnapshots - The Dashboard Summary 📸

**SharePoint List ID:** `613d8abf-958d-4f1f-a142-4d2ed44c37a4`  
**List Name:** `RunDisplaySnapshots`

#### Purpose
Stores pre-calculated totals at portfolio, property, and lease levels so the dashboard loads instantly without re-aggregating millions of detail rows.

#### What Gets Written
Creates a **3-level hierarchy** of snapshots:

**Portfolio Snapshot (1 row per run):**
- `ScopeType` = "portfolio"
- `RunId` - Audit run identifier
- `ExceptionCountStatic` - Total exceptions across all properties
- `UnderchargeStatic` - Total undercharge amount
- `OverchargeStatic` - Total overcharge amount
- `TotalVarianceStatic` - Net variance (overcharge - undercharge)
- `MatchRateStatic` - Percentage of buckets that matched
- `TotalBucketsStatic` - Total comparisons performed
- `MatchedBucketsStatic` - Number of matched comparisons
- `AuditedThrough` - Latest audit month in the run

**Property Snapshots (~94 rows for portfolio audit):**
- `ScopeType` = "property"
- `PropertyId` - Property identifier
- `PropertyNameStatic` - Property display name
- `ExceptionCountStatic` - Exceptions for this property
- `UnderchargeStatic` - Undercharge for this property
- `OverchargeStatic` - Overcharge for this property
- `TotalLeaseIntervalStatic` - Number of leases audited
- Plus all portfolio fields aggregated at property level

**Lease Snapshots (~117,500 rows for portfolio audit):**
- `ScopeType` = "lease"
- `LeaseIntervalId` - Lease account identifier
- `PropertyId` - Parent property
- `ExceptionCountStatic` - Exceptions for this lease
- `UnderchargeStatic` - Undercharge for this lease
- `OverchargeStatic` - Overcharge for this lease
- Plus all portfolio fields aggregated at lease level

#### Example Data

```
Portfolio Level:
├─ Run: run_20260422_125827
│  └─ Total: $50.9M undercharge, $5.7M overcharge, 117,742 exceptions, 87.3% match rate
│
Property Level:
├─ University Gateway (639820)
│  └─ $11.5M undercharge, $194K overcharge, 15,651 exceptions
├─ University Center (639810)
│  └─ $23.5M undercharge, $226K overcharge, 13,127 exceptions
└─ Cobalt Row (1126176)
   └─ $15,983 undercharge, $50,847 overcharge, 6,094 exceptions
│
Lease Level:
├─ Lease 155007 @ University Gateway
│  └─ $850 undercharge, $0 overcharge, 2 exceptions
├─ Lease 155008 @ University Gateway
│  └─ $425 undercharge, $120 overcharge, 3 exceptions
└─ ...
```

#### Write Behavior
- **Write Mode:** Asynchronous by default
- **Environment Variable:** `ASYNC_RUN_DISPLAY_SNAPSHOTS=true`
- **Triggered By:** Every audit run completion
- **Performance:** Moderate - typically 30-60 seconds for portfolio audit
- **Filtering:** Only captures **unresolved** exceptions (resolved months are excluded)

#### Where It's Read From
- **Portfolio Page** (`/portfolio`) - Main data source for portfolio aggregation
- **Property List** - Builds the property table with exception counts
- **Dashboard KPIs** - Current undercharge/overcharge totals

#### Retention
❌ **Cleared by cleanup script** - Regenerated on each audit run

---

### 3. Audit Run Metrics - The Summary Card 📈

**SharePoint List ID:** `0b9b4d90-32c6-4f37-b8df-459c4012d3b5`  
**List Name:** `Audit Run Metrics`

#### Purpose
Stores high-level summary statistics for trend analysis, dashboard displays, and run comparison.

#### What Gets Written
**One row per audit run** with aggregated metrics:

**Fields:**
- `Title` - Run ID
- `RunDateTime` - Timestamp of run execution
- `UploadedBy` - User who initiated the audit
- `FileName` - Source file name (if uploaded)
- `TotalScheduled` - Sum of all expected amounts
- `TotalActual` - Sum of all actual billed amounts
- `Matched` - Count of matched buckets
- `ScheduledNotBilled` - Count of scheduled charges not billed
- `BilledNotScheduled` - Count of billed charges without schedule
- `AmountMismatch` - Count of amount discrepancies
- `TotalVariances` - Total exception count
- `HighSeverity` - Count of high-severity findings
- `MediumSeverity` - Count of medium-severity findings
- `Properties` - JSON object with per-property breakdown

#### Example Data

```json
{
  "RunId": "run_20260422_125827",
  "RunDateTime": "2026-04-22T12:58:27Z",
  "UploadedBy": "Tyler Gaskins",
  "TotalScheduled": 125000000.00,
  "TotalActual": 119283331.97,
  "Matched": 4082258,
  "TotalVariances": 117742,
  "HighSeverity": 45123,
  "MediumSeverity": 72619,
  "Properties": {
    "639820": {
      "total_buckets": 45000,
      "exceptions": 15651,
      "variance": 11482325.97
    },
    "639810": {
      "total_buckets": 45000,
      "exceptions": 13127,
      "variance": 23528520.87
    }
  }
}
```

#### Write Behavior
- **Write Mode:** Asynchronous by default
- **Environment Variable:** `ASYNC_METRICS_WRITE=true`
- **Triggered By:** Every audit run completion
- **Performance:** Fast - single row write

#### Where It's Read From
- **Run List** (`/`) - Shows recent audit runs
- **Dashboard Metrics** - Trend analysis and statistics
- **Run Comparison Views** - Compare metrics across runs

#### Retention
❌ **Cleared by cleanup script** - Historical metrics removed

---

### 4. ExceptionMonths - The Resolution Tracker ✅

**SharePoint List ID:** `3638e107-5231-4bac-9a6c-cef9c501db05`  
**List Name:** `ExceptionMonths`

#### Purpose
Tracks which months have been marked as "resolved" for each lease, enabling month-level filtering and resolution workflows.

#### What Gets Written
**Variable rows** - only written when users interact with resolution UI:

**Fields:**
- `PropertyId` - Property identifier
- `LeaseIntervalId` - Lease account identifier
- `ArCodeId` - AR code identifier
- `AuditMonth` - Month with exception (YYYY-MM-DD)
- `ExceptionCount` - Number of exceptions in this month
- `Status` - "Unresolved" or "Resolved"
- `ResolvedBy` - User who marked as resolved
- `ResolvedDate` - Timestamp of resolution
- `Notes` - User notes about resolution
- `UnderchargeAmount` - Undercharge for this month (tracked separately)
- `OverchargeAmount` - Overcharge for this month (tracked separately)

#### Example Data

```
LeaseIntervalId=155007, ArCodeId=154771, AuditMonth=2025-01-01
├─ Status: Resolved
├─ ResolvedBy: Tyler Gaskins
├─ ResolvedDate: 2026-06-10T14:30:00Z
├─ Notes: "Resident paid back charges in full on 6/5"
└─ UnderchargeAmount: $850.00

LeaseIntervalId=155007, ArCodeId=154771, AuditMonth=2025-02-01
├─ Status: Unresolved
├─ ExceptionCount: 1
└─ UnderchargeAmount: $425.00
```

#### Write Behavior
- **Write Mode:** On-demand (synchronous)
- **Triggered By:** User clicks "Mark as Resolved" in UI
- **Performance:** Fast - single or small batch writes
- **Updates:** Can be toggled between Resolved/Unresolved

#### Where It's Read From
- **Portfolio Page** - Subtracts resolved amounts from current totals
- **Property View** - Filters out resolved months
- **Lease View** - Shows resolution status per month
- **Exception Reports** - Filters to unresolved only

#### Retention
❌ **Cleared by cleanup script** - Resolution state reset

---

### 5. LeaseTermSet / LeaseTerms / LeaseTermEvidence - The Lease Intelligence 📄

**SharePoint List IDs:**
- `LeaseTermSet`: `959169fb-fd74-4d9f-a3af-5502e999c849`
- `LeaseTerms`: `8c374324-7413-42ae-b6c0-05d1bd248416`
- `LeaseTermEvidence`: `1145d114-2891-4c74-96bd-97b7b4860968`

#### Purpose
Store parsed lease term data extracted from resident documents (lease agreements, addendums, riders) for reconciliation validation and lease intelligence.

#### What Gets Written

**LeaseTermSet (Sets of lease attributes):**
- `LeaseKey` - Unique lease identifier (text)
- `SetName` - Name of attribute set (e.g., "rent_schedule")
- `IsActive` - Boolean flag for current term
- `EffectiveDate` - When term became active
- `ExpirationDate` - When term expires
- `DocListFingerprint` - Hash of document list (change detection)
- `LastChecked` - Last document check timestamp

**LeaseTerms (Individual lease terms):**
- `LeaseKey` - Parent lease (text)
- `TermType` - Type: "rent", "concession", "utility", etc.
- `Amount` - Term value/amount
- `Frequency` - Application frequency (monthly, annual, etc.)
- `ARCode` - Associated AR code for matching
- `SetReference` - Link to parent LeaseTermSet

**LeaseTermEvidence (Supporting documents):**
- `LeaseKey` - Associated lease (text)
- `DocumentType` - Type: lease, addendum, rider, notice
- `DocumentPage` - Page number in document
- `ExtractedText` - OCR/extracted text snippet
- `ConfidenceScore` - Extraction confidence (0-1)

#### Example Data

When auditing lease 155007 with lease documents:

```
LeaseTermSet:
├─ LeaseKey: "155007"
├─ SetName: "base_rent"
├─ EffectiveDate: 2025-01-01
└─ ExpirationDate: 2025-12-31

LeaseTerms:
├─ LeaseKey: "155007", TermType: "rent", Amount: 1850.00, ARCode: 154771
├─ LeaseKey: "155007", TermType: "parking", Amount: 50.00, ARCode: 154788
└─ LeaseKey: "155007", TermType: "concession", Amount: -200.00, ARCode: 154801

LeaseTermEvidence:
├─ LeaseKey: "155007", DocumentType: "lease", Page: 3, Text: "Monthly rent: $1,850.00"
└─ LeaseKey: "155007", DocumentType: "addendum", Page: 1, Text: "Parking: $50/month"
```

#### Write Behavior
- **Write Mode:** On-demand during document processing
- **Triggered By:** Individual resident/lease processing with documents
- **Performance:** Variable - depends on document parsing
- **Caching:** Uses `DocListFingerprint` to avoid re-parsing unchanged documents

#### Where It's Read From
- **Lease View** - Shows extracted lease terms
- **Term Validation** - Compares scheduled charges to lease terms
- **Lease Intelligence Features** - Document-backed validation

#### Retention
❌ **Cleared by cleanup script** - Re-parsed on next audit run

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Audit Run Execution                      │
│                  (Property or Portfolio)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Completion triggers writes to:
                       │
       ┌───────────────┼───────────────┬────────────────┬─────────────────┐
       │               │               │                │                 │
       ▼               ▼               ▼                ▼                 ▼
┌────────────┐  ┌─────────────┐  ┌──────────┐  ┌─────────────┐  ┌──────────────┐
│ AuditRuns2 │  │RunDisplay   │  │Audit Run │  │Exception    │  │Lease Terms   │
│            │  │Snapshots    │  │Metrics   │  │Months       │  │(3 lists)     │
│            │  │             │  │          │  │             │  │              │
│ MILLIONS   │  │ HUNDREDS    │  │  ONE     │  │ ON-DEMAND   │  │ ON-DEMAND    │
│ of rows    │  │ of rows     │  │  row     │  │ (resolution)│  │ (doc parse)  │
│            │  │             │  │          │  │             │  │              │
│ Async ⚙️   │  │ Async ⚙️    │  │ Async ⚙️ │  │ Sync ✓      │  │ Sync ✓       │
│ 10-30 min  │  │ 30-60 sec   │  │ <1 sec   │  │ <1 sec      │  │ variable     │
└────────────┘  └─────────────┘  └──────────┘  └─────────────┘  └──────────────┘
       │               │               │                │                 │
       │               │               │                │                 │
       ▼               ▼               ▼                ▼                 ▼
  Used for:      Used for:       Used for:        Used for:         Used for:
  • Property     • Portfolio     • Run list       • Resolution      • Term
    drill-down     page          • Dashboard        tracking          validation
  • Lease        • Property      • Metrics        • Filtering       • Document
    drill-down     list                             resolved          intelligence
  • Detail       • Fast KPIs                        months
    views
```

---

## Understanding Portfolio Totals

### Why Are the Numbers So High?

When you see **$50.9M undercharge** and **$5.7M overcharge** on your portfolio page, here's what that represents:

#### The Math Behind Large Numbers

**Scenario:** Full portfolio audit for 12 months of history

1. **Properties:** 94 properties
2. **Leases per property:** ~1,250 active leases (average)
3. **Total leases:** 94 × 1,250 = **117,500 leases**
4. **Audit months:** 12 months of history
5. **AR codes per lease:** ~3 (rent, utilities, parking, etc.)
6. **Total comparisons:** 94 × 1,250 × 12 × 3 = **~4.2 million buckets**

**Each $5-50 variance adds up:**
- If just **2%** of buckets have exceptions (84,000 exceptions)
- Average variance: **$680 per exception**
- **Total variance:** 84,000 × $680 = **$57 million**

#### Cross-Run Aggregation Issue

If you see unexpectedly high numbers, check if you're viewing:

❌ **Default Portfolio Landing (no run selected):**
- Loads latest snapshot **per property** across **all runs**
- University Gateway snapshot from Run A (April 21)
- University Center snapshot from Run B (April 22)
- Cobalt Row snapshot from Run C (April 22)
- **Result:** Sum of different audits = inflated totals

✅ **Selected Run from Dropdown:**
- Loads all property snapshots **from one specific run**
- All properties audited together in Run A
- **Result:** Accurate totals for that one audit

#### How to Verify Accurate Numbers

1. **Use the run dropdown** at top of portfolio page
2. **Select a specific run** (e.g., "04/22/2026 - Manual")
3. **Portfolio totals update** to that run's scope
4. **All properties align** to same audit period

---

## Write Performance & Optimization

### Current Configuration

```env
# From your .env file
SHAREPOINT_AUDIT_RESULTS_LIST_NAME=AuditRuns2
USE_SHAREPOINT_STORAGE=true

# Async write settings (defaults if not specified)
ASYNC_AUDIT_RESULTS_WRITE=false  # ⚠️ Currently synchronous!
ASYNC_RUN_DISPLAY_SNAPSHOTS=true  # ✓ Background write
ASYNC_METRICS_WRITE=true          # ✓ Background write
```

### Performance Bottleneck

**AuditRuns2 writes are currently SYNCHRONOUS**, meaning:
- Audit waits for all millions of rows to upload before completing
- Users see "Processing..." for 10-30 minutes
- Risk of timeout failures on large audits

### Recommended Optimization

Add to your `.env`:

```env
# Enable async AuditRuns2 writes
ASYNC_AUDIT_RESULTS_WRITE=true

# Optionally tune batch sizes if experiencing throttling
SHAREPOINT_BATCH_SIZE_AUDITRUNS=20
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=2
```

**Result:**
- Audit completes immediately
- Detail upload happens in background
- RunDisplaySnapshots writes first (portfolio works immediately)
- AuditRuns2 finishes later (drill-down available after upload completes)

---

## Cleanup & Data Retention

The cleanup script (`clean_sharepoint_lists.py`) manages data retention:

### Lists Cleared by Cleanup ❌
- **RunDisplaySnapshots** - Stale snapshots removed
- **Audit Run Metrics** - Summary statistics reset
- **LeaseTermSet / LeaseTerms / LeaseTermEvidence** - Re-parsed on next run
- **ExceptionMonths** - Resolution state reset

### Lists Preserved ✅
- **AuditRuns2** - Historical audit detail retained

### Cleanup Behavior

```python
# Lists to clear (from clean_sharepoint_lists.py)
LISTS_TO_CLEAR = [
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
]

# Lists to preserve
LISTS_TO_PRESERVE = [
    "AuditRuns2",  # Keep historical detail
    "Innovation Use Log",  # Activity logging
]
```

---

## Quick Reference Table

| **List Name** | **Primary Use** | **Rows Per Run** | **Write Mode** | **Read By** | **Cleaned** |
|---------------|-----------------|------------------|----------------|-------------|-------------|
| **AuditRuns2** | Detailed findings & buckets | MILLIONS (4.2M for portfolio) | Async | Property/Lease views | ❌ No |
| **RunDisplaySnapshots** | Portfolio/property summaries | HUNDREDS (~200) | Async | Portfolio page | ✅ Yes |
| **Audit Run Metrics** | High-level statistics | ONE (1) | Async | Run list, dashboard | ✅ Yes |
| **ExceptionMonths** | Resolution tracking | Variable (on-demand) | Sync | Filtering, resolution UI | ✅ Yes |
| **LeaseTermSet** | Lease attributes | Variable (on-demand) | Sync | Term validation | ✅ Yes |
| **LeaseTerms** | Individual terms | Variable (on-demand) | Sync | Term validation | ✅ Yes |
| **LeaseTermEvidence** | Document snippets | Variable (on-demand) | Sync | Document intelligence | ✅ Yes |

---

## Troubleshooting Common Issues

### Issue: Portfolio dropdown is empty or doesn't populate

**Cause:** RunDisplaySnapshots async writes still processing

**Solution:**
1. Wait 1-2 minutes for async writes to complete
2. Refresh the portfolio page
3. Check if run appears in dropdown

### Issue: Portfolio shows unexpectedly high totals ($50M+)

**Cause:** Cross-run aggregation (viewing latest snapshot per property from different runs)

**Solution:**
1. Select a specific run from the dropdown
2. Verify all properties are from the same audit
3. Consider running fewer audits (one portfolio audit vs. many single-property audits)

### Issue: Audit takes 10-30 minutes to complete

**Cause:** AuditRuns2 writes are synchronous (blocking)

**Solution:**
1. Add `ASYNC_AUDIT_RESULTS_WRITE=true` to `.env`
2. Restart application
3. Next audit completes immediately (background upload)

### Issue: Property drill-down shows "No data"

**Cause:** AuditRuns2 async write hasn't completed yet

**Solution:**
1. Wait for background write to finish (check logs)
2. In Azure deployment, ensure AuditRuns2 writes are enabled
3. CSV fallback works locally but not in Azure

---

## For Azure Deployment

### Critical Requirements

✅ **Must keep AuditRuns2 enabled**
- Azure Web Apps have ephemeral file systems
- CSV files get wiped on restart
- AuditRuns2 is the persistent storage layer

✅ **Must use async writes**
- Add `ASYNC_AUDIT_RESULTS_WRITE=true`
- Prevents HTTP timeout on large audits

✅ **Must keep all 5 lists configured**
- Each list serves a specific purpose
- Removing any list breaks functionality

### Azure Configuration

```env
# Required for Azure deployment
USE_SHAREPOINT_STORAGE=true
SHAREPOINT_AUDIT_RESULTS_LIST_NAME=AuditRuns2

# Performance optimization
ASYNC_AUDIT_RESULTS_WRITE=true
ASYNC_RUN_DISPLAY_SNAPSHOTS=true
ASYNC_METRICS_WRITE=true

# Throttling mitigation
SHAREPOINT_BATCH_SIZE_AUDITRUNS=15
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=1
```

---

## Summary

Your LeaseFileAudit app uses a **5-list architecture** to balance performance and functionality:

1. **AuditRuns2** = Detail layer (slow, large, persistent)
2. **RunDisplaySnapshots** = Speed layer (fast, small, regenerated)
3. **Audit Run Metrics** = Summary layer (dashboard, trends)
4. **ExceptionMonths** = Workflow layer (resolution tracking)
5. **Lease Terms** = Intelligence layer (document validation)

**The portfolio page is fast** because it reads from RunDisplaySnapshots, not AuditRuns2.

**The drill-down is detailed** because it reads from AuditRuns2, not summaries.

**Together, they provide** both immediate portfolio insights and comprehensive detail investigation.

---

**Document Version:** 2.0  
**Created By:** GitHub Copilot (Claude Sonnet 4.5)  
**Source:** LeaseFileAudit codebase analysis and user consultation
