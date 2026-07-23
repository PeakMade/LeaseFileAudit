# LeaseFileAudit Application - Complete Overview

**Last Updated:** 2026-07-23  
**Application Version:** 2.1 (Cross-Interval Matching + AR Pagination)

---

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Business Purpose & Problem Statement](#business-purpose--problem-statement)
3. [Entrata API Integration](#entrata-api-integration)
4. [Application Architecture](#application-architecture)
5. [Data Flow & Audit Process](#data-flow--audit-process)
6. [Storage & Persistence](#storage--persistence)
7. [User Workflow](#user-workflow)
8. [Original vs Current Implementation](#original-vs-current-implementation)

---

## Executive Summary

**LeaseFileAudit** is an automated billing reconciliation system that compares **scheduled rent charges** against **actual posted transactions** in Entrata (property management system). It identifies billing discrepancies across the student housing portfolio and tracks exception resolution over time.

### Key Metrics Tracked
- **Match Rate**: % of scheduled charges that were billed correctly
- **Undercharge**: Revenue lost due to unbilled scheduled charges
- **Overcharge**: Excess charges billed without corresponding schedules
- **Exception Count**: Number of discrepancies requiring manual review

### Primary Users
- **Property Managers**: Monitor billing accuracy per property
- **Finance Team**: Track portfolio-level revenue reconciliation
- **Operations Team**: Resolve billing exceptions month-over-month

---

## Business Purpose & Problem Statement

### The Problem
Property managers schedule recurring rent and fee charges for thousands of residents across multiple properties. However:

1. **Manual Billing Errors**: Charges may be missed, duplicated, or billed at incorrect amounts
2. **Schedule Changes**: Lease modifications may not propagate to billing systems correctly
3. **Revenue Leakage**: Unbilled scheduled charges result in lost revenue
4. **Overcharges**: Charges posted without schedules may lead to resident disputes
5. **Scale**: Manual verification is impossible across 20+ properties with 10,000+ leases

### The Solution
LeaseFileAudit **automatically reconciles** scheduled charges against actual transactions using:
- **Entrata API integration** to fetch live lease and transaction data
- **Four-tier matching algorithm** to identify exact matches, amount mismatches, missing charges, and unit transfer cross-interval matches
- **SharePoint persistence** to track audit history and exception resolution
- **Portfolio dashboard** to monitor billing health across all properties

### Business Impact
- ✅ **Automated audits** run in minutes (previously took days/weeks manually)
- ✅ **Immediate visibility** into billing discrepancies by property/lease/month
- ✅ **Historical tracking** of exceptions and resolution workflow
- ✅ **Revenue protection** by identifying unbilled charges before month-end close

---

## Entrata API Integration

### Overview
The application fetches data from **Entrata** (property management platform) via REST API calls. Entrata stores all lease details, scheduled charges, and financial transactions.

### API Endpoints Used

#### 1. **Lease Details API** (`getLeaseDetails`)
**Purpose**: Fetch scheduled charges, lease terms, and customer information for leases

**Method**: `POST` request with method name `getLeaseDetails`  
**Version**: `r2`  
**Endpoint**: 
- **Production**: `https://apis.entrata.com/ext/orgs/peakmade/v1/leases`
- **Sandbox**: `https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/leases`

**Request Payload Example**:
```json
{
  "auth": {"type": "apikey"},
  "requestId": "1704556800000",
  "method": {
    "name": "getLeaseDetails",
    "version": "r2",
    "params": {
      "propertyIds": [771903],
      "leaseId": 18296704,
      "includeCharges": 1,
      "includeCustomers": 1,
      "includeInactive": 1
    }
  }
}
```

**Response Data**:
- **Lease Information**: Lease ID, Property ID, Unit Number, Lease Dates
- **Customer Details**: Resident names, emails, phone numbers
- **Scheduled Charges**: 
  - AR Code (charge type: Rent, Parking, Pet Fee, etc.)
  - Amount
  - Start/End Dates
  - Frequency (Monthly, One-time, etc.)
  - Description

**Authentication**: `X-Api-Key` header with API key

---

#### 2. **AR Transactions API** (`getLeaseArTransactions`)
**Purpose**: Fetch actual posted transactions (billed charges, payments, adjustments)

**Method**: `POST` request with method name `getLeaseArTransactions`  
**Version**: `r1`  
**Endpoint**: 
- **Production**: `https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions`
- **Sandbox**: `https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/artransactions`

**Request Payload Example**:
```json
{
  "auth": {"type": "apikey"},
  "requestId": "1704556800000",
  "method": {
    "name": "getLeaseArTransactions",
    "version": "r1",
    "params": {
      "propertyIds": [771903],
      "leaseId": 18296704,
      "fromDate": "01/01/2024",
      "toDate": "12/31/2024"
    }
  }
}
```

**Response Data**:
- **Transaction ID**: Unique identifier for each posted charge
- **AR Code**: Charge type (must match scheduled charges)
- **Amount**: Actual billed amount
- **Post Date**: Date transaction was posted to resident ledger
- **Audit Month**: Month the charge applies to (may differ from post date)
- **Transaction Type**: Charge, Payment, Credit, Adjustment

**Authentication**: `X-Api-Key` header with API key

---

### API Configuration

**Environment Variables Required**:
```bash
# Production Credentials
LEASE_API_KEY=<your-api-key>
LEASE_API_BASE_URL=https://apis.entrata.com/ext/orgs/peakmade/v1
LEASE_API_DETAILS_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/leases
LEASE_API_AR_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions

# Sandbox Credentials (for testing)
LEASE_API_SANDBOX_KEY=<sandbox-api-key>
LEASE_API_SANDBOX_BASE_URL=https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1

# API Methods
LEASE_API_DETAILS_METHOD=getLeaseDetails
LEASE_API_AR_METHOD=getLeaseArTransactions
```

**Environment Switching**:
- Configured via `entrata_environment.json` in project root
- Toggle between `"prod"` and `"sandbox"` environments
- Sandbox used for development/testing without affecting production data

---

### API Request Flow

```
1. User selects property/lease in web UI
   ↓
2. Application calls fetch_property_api_sources() or fetch_single_lease_api_sources()
   ↓
3. Function constructs API request payload with property/lease filters
   ↓
4. POST request sent to Entrata API with API key authentication
   ↓
5. Entrata validates API key and returns JSON response
   ↓
6. Application parses response and extracts lease/transaction data
   ↓
7. Data transformed into canonical format (pandas DataFrames)
   ↓
8. Reconciliation engine processes expected vs actual
   ↓
9. Results saved to SharePoint and displayed in UI
```

---

### API Data Transformation Pipeline

#### Step 1: Raw API Response → Source DataFrames
**Scheduled Charges** (from `getLeaseDetails`):
```python
{
  "PROPERTY_ID": 771903,
  "LEASE_ID": 18296704,
  "AR_CODE_ID": 154771,  # "Rent"
  "AMOUNT": 1234.56,
  "START_DATE": "08/01/2024",
  "END_DATE": "04/30/2025",
  "FREQUENCY": "Monthly"
}
```

**AR Transactions** (from `getLeaseArTransactions`):
```python
{
  "PROPERTY_ID": 771903,
  "LEASE_ID": 18296704,
  "AR_CODE_ID": 154771,
  "AMOUNT": 1234.56,
  "POST_DATE": "08/05/2024",
  "AUDIT_MONTH": "08/01/2024",
  "TRANSACTION_ID": "AR-12345"
}
```

#### Step 2: Source Mapping → Canonical Fields
**Purpose**: Standardize field names across different data sources

**Canonical Fields** (defined in `audit_engine/canonical_fields.py`):
- `PROPERTY_ID`: Property identifier
- `LEASE_INTERVAL_ID`: Lease identifier (formerly LEASE_ID)
- `AR_CODE_ID`: Charge type code
- `AMOUNT`: Dollar amount
- `AUDIT_MONTH`: Month the charge applies to
- `POST_DATE`: Date transaction was posted (actual detail only)
- `TRANSACTION_ID`: Unique transaction identifier (actual detail only)
- `PERIOD_START_DATE`: Scheduled charge start date (expected detail only)
- `PERIOD_END_DATE`: Scheduled charge end date (expected detail only)

#### Step 3: Normalization → Validated DataFrames
**Purpose**: Clean, validate, and enrich data

**Normalization Steps**:
- ✅ Convert dates to pandas datetime objects
- ✅ Parse monetary values (handle "$1,234.56" format)
- ✅ Remove deleted/invalid charges
- ✅ Apply AR code whitelist filter (only audit specified charge types)
- ✅ Exclude API-posted charges (system-generated, not scheduled)
- ✅ Add property names from SharePoint picklist

#### Step 4: Expansion → Monthly Buckets
**Purpose**: Expand recurring scheduled charges into individual monthly records

**Example**:
```
Input (1 scheduled charge):
  AR_CODE_ID: 154771 (Rent)
  AMOUNT: $1,234.56
  START_DATE: 08/01/2024
  END_DATE: 04/30/2025
  FREQUENCY: Monthly

Output (9 monthly records):
  Month 1: 08/2024 → $1,234.56
  Month 2: 09/2024 → $1,234.56
  Month 3: 10/2024 → $1,234.56
  ...
  Month 9: 04/2025 → $1,234.56
```

**Expected Detail DataFrame** (after expansion):
- **~9,000 rows** for a property with 668 leases
- Each row represents one expected charge for one month

**Actual Detail DataFrame** (no expansion needed):
- **~9,600 rows** of actual posted transactions
- Each row represents one billed charge

---

## Application Architecture

### Technology Stack

**Backend**:
- **Python 3.11+**: Core language
- **Flask 3.1.0**: Web framework
- **Pandas 2.2.3**: Data processing and reconciliation engine
- **Requests**: HTTP client for Entrata API calls

**Frontend**:
- **Jinja2 Templates**: Server-side HTML rendering
- **Bootstrap 5**: Responsive UI framework
- **JavaScript/jQuery**: Client-side interactivity

**Storage**:
- **SharePoint Document Library**: Audit run files (Parquet/JSON)
- **SharePoint Lists**: 
  - `RunDisplaySnapshots`: Aggregated summary snapshots (portfolio/property/lease/month scopes)
  - `ExceptionMonths`: Manual exception resolution tracking
  - `LeaseTerms`: Lease document metadata (PDFs)
  - `Innovation Use Log`: User activity logging

**Authentication**:
- **Azure AD (Entra ID)**: Single sign-on
- **Microsoft Graph API**: SharePoint data access
- **App Service Authentication**: Managed identity (production)

**Deployment**:
- **Azure App Service**: Linux-based web hosting
- **Egnyte Drive**: Local development file storage
- **GitHub Actions**: CI/CD pipeline

---

### Core Components

#### 1. **Web Layer** (`web/views.py`)
**Responsibilities**:
- Route handlers for all HTTP endpoints
- User authentication and session management
- Template rendering with Jinja2
- Flask caching for performance (4-hour timeout)

**Key Routes**:
- `/` - Home page with audit run picker
- `/portfolio` - Portfolio dashboard (all properties aggregated)
- `/property/<property_id>/run_<run_id>` - Property detail view
- `/lease/<lease_id>/run_<run_id>` - Lease detail view with transaction arrays
- `/api/bulk-audit` - Bulk audit job submission
- `/api/exception-months` - Exception resolution updates

#### 2. **Reconciliation Engine** (`audit_engine/`)
**Responsibilities**:
- Fetch data from Entrata API
- Transform raw API responses into canonical format
- Expand scheduled charges to monthly buckets
- Match expected vs actual transactions (3-tier algorithm)
- Calculate KPIs (match rate, undercharge, overcharge)
- Generate findings with severity levels

**Key Files**:
- `api_ingest.py`: Entrata API client and data fetching
- `mappings.py`: Source-to-canonical field transformations
- `normalize.py`: Data validation and cleaning
- `expand.py`: Scheduled charge expansion to monthly buckets
- `reconcile.py`: Four-tier matching algorithm (includes cross-interval matching for unit transfers)
- `metrics.py`: KPI calculation (match rate, financial impact)
- `canonical_fields.py`: Standard field name enums

**Four-Tier Matching Algorithm**:
```
TIER 1: Exact Match (MATCHED)
  - Property ID matches
  - Lease ID matches
  - AR Code matches
  - Audit Month matches
  - Amount matches (within $0.01 tolerance)
  → Status: MATCHED, Variance: $0.00

TIER 2: Amount Mismatch (AMOUNT_MISMATCH)
  - All identifiers match (property, lease, AR code, month)
  - Amount differs by more than $0.01
  → Status: AMOUNT_MISMATCH, Variance: actual - expected

TIER 3: Unbilled/Unexpected
  - Expected charge has no matching actual → SCHEDULED_NOT_BILLED
  - Actual transaction has no matching expected → BILLED_NOT_SCHEDULED
  → High severity findings requiring manual review

TIER 4: Cross-Interval Match (CROSS_INTERVAL)
  - Handles unit transfers and renewals where Entrata creates a new
    lease interval — AR transactions remain on the old interval while
    scheduled charges move to the new one
  - Matches by LEASE_ID (not LEASE_INTERVAL_ID) + AR_CODE_ID +
    AUDIT_MONTH + amount within tolerance
  - Prevents false mirror-image discrepancies (same amount appearing
    as both undercharge and overcharge)
  → Status: MATCHED (cross-interval), Variance: $0.00
```

#### 3. **Storage Layer** (`storage/service.py`)
**Responsibilities**:
- Save audit results to SharePoint Document Library (Parquet files)
- Write summary snapshots to `RunDisplaySnapshots` list (5,966 rows per run)
- Load cached data from in-memory cache or Parquet files
- Manage exception tracking in `ExceptionMonths` list
- Validate snapshot consistency after save

**Storage Modes**:
- **SharePoint Mode** (`use_sharepoint=True`): Production
  - Parquet files uploaded to SharePoint Document Library
  - RunDisplaySnapshots written via Graph API batch operations (299 batches of 20 rows)
  - In-memory cache populated for fast access (4-hour Flask cache)
  
- **Local Mode** (`use_sharepoint=False`): Development
  - Parquet files saved to Egnyte Drive filesystem
  - No SharePoint writes
  - In-memory cache still used

**Parquet Persistence**:
- Enabled via `PERSIST_DETAIL_DATAFRAMES=true`
- Files saved after audit completes:
  - `expected_detail.parquet`: All scheduled charges expanded to months (~9,000 rows)
  - `actual_detail.parquet`: All posted AR transactions (~9,600 rows)
  - `variance_detail.parquet`: Discrepancies requiring review
- Uses pyarrow engine with snappy compression
- Loaded on app restart or when cache expires

---

## Data Flow & Audit Process

### Full Audit Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                   USER INITIATES AUDIT                       │
│  (Selects property 771903 "CLEMSON EDGE" from bulk audit)   │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│              PHASE 1: API DATA FETCH                         │
│  • POST getLeaseDetails(propertyIds=[771903])                │
│    → Returns 668 leases with scheduled charges               │
│  • POST getLeaseArTransactions(propertyIds=[771903])         │
│    → Returns ~9,600 posted transactions                      │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│           PHASE 2: SOURCE MAPPING                            │
│  • Transform API JSON → pandas DataFrames                    │
│  • Map source fields → canonical fields                      │
│  • ar_canonical: 9,600 rows x 15 columns                     │
│  • scheduled_canonical: 2,100 rows x 12 columns              │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│        PHASE 3: NORMALIZATION & VALIDATION                   │
│  • Remove deleted/invalid charges                            │
│  • Apply AR code whitelist (only audit [154771] "Rent")      │
│  • Filter out API-posted charges (system-generated)          │
│  • Add property names from SharePoint picklist               │
│  • actual_detail: 9,692 rows (AR transactions)               │
│  • scheduled_normalized: 2,123 rows (scheduled charges)      │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│    PHASE 4: EXPANSION (SCHEDULED → MONTHLY BUCKETS)          │
│  • Expand recurring charges to individual months             │
│  • Example: 1 charge ($1,234.56/month, 08/2024-04/2025)     │
│    → 9 monthly records ($1,234.56 each)                      │
│  • expected_detail: 8,824 rows (expanded monthly charges)    │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│      PHASE 5: RECONCILIATION (EXPECTED vs ACTUAL)            │
│  • Group by (property, lease, AR code, month)                │
│  • Four-tier matching algorithm:                             │
│    1. Exact match → MATCHED                                  │
│    2. Amount differs → AMOUNT_MISMATCH                       │
│    3. Unbilled → SCHEDULED_NOT_BILLED                        │
│    4. Unexpected → BILLED_NOT_SCHEDULED                      │
│    + Cross-interval: unit transfers matched by LEASE_ID      │
│  • bucket_results: 8,971 rows (reconciled records)           │
│  • findings: 187 rows (discrepancies)                        │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│           PHASE 6: METRICS CALCULATION                       │
│  • Match rate: 97.9% (8,784 matched / 8,971 total)           │
│  • Undercharge: $34,567.89 (unbilled scheduled charges)      │
│  • Overcharge: $1,234.56 (unexpected transactions)           │
│  • Exception count: 187 records requiring review             │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│          PHASE 7: AGGREGATION & SNAPSHOTS                    │
│  • Aggregate to multiple scopes:                             │
│    - Portfolio: 1 row (all properties combined)              │
│    - Property: 1 row per property (property 771903)          │
│    - Lease: 668 rows (one per lease)                         │
│    - Month: 5,297 rows (lease x AR code x month)             │
│  • Total snapshots: 5,966 rows                               │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│    PHASE 8: SAVE TO SHAREPOINT (299 BATCHES OF 20 ROWS)     │
│  • Write Parquet files to Document Library:                  │
│    - expected_detail.parquet (8,824 rows)                    │
│    - actual_detail.parquet (9,692 rows)                      │
│    - variance_detail.parquet (187 rows)                      │
│  • Populate in-memory cache (4-hour Flask cache)             │
│  • POST 5,966 rows to RunDisplaySnapshots in 299 batches     │
│  • Save metadata JSON (run_id, timestamp, KPIs)              │
│  • Batch processing: ~350 seconds for 5,966 rows             │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│               PHASE 9: USER VIEWS RESULTS                    │
│  • Navigate to property 771903 detail page                   │
│  • Load RunDisplaySnapshots for property scope               │
│  • Display KPIs: Match Rate, Undercharge, Overcharge         │
│  • Show lease summary table (668 leases)                     │
│  • Click lease 18296704 to view transaction details          │
│  • Load expected_detail and actual_detail from Parquet/cache │
│  • Build transaction arrays for display:                     │
│    - expected_transactions: [Period, Amount]                 │
│    - actual_transactions: [Post Date, Amount, Transaction ID]│
│  • Render lease detail page with transaction tables          │
└─────────────────────────────────────────────────────────────┘
```

---

## Storage & Persistence

### Current Architecture (Post-CSV Migration)

**Primary Storage**: SharePoint + In-Memory Cache + Parquet Files

#### 1. **In-Memory Cache** (Temporary Calculation Workspace)
**Purpose**: Fast access during audit execution and immediate post-save access

**Lifecycle**:
- Populated during `execute_audit_run()` as batches complete
- Cached via Flask `@cache.memoize(timeout=14400)` (4 hours)
- Survives until cache expires or app restarts

**Contents**:
- `expected_detail`: DataFrame with expanded scheduled charges
- `actual_detail`: DataFrame with AR transactions
- `bucket_results`: DataFrame with reconciled records
- `findings`: DataFrame with discrepancies
- `variance_detail`: DataFrame with amount mismatches

**Access Pattern**:
```python
# web/views.py
@cache.memoize(timeout=14400)
def cached_load_expected_detail(run_id: str):
    storage = get_storage_service()
    return storage.load_expected_detail(run_id)  # Tries Parquet → in-memory → empty
```

---

#### 2. **Parquet Files** (Durable Detail Storage)
**Purpose**: Persist transaction-level detail across app restarts

**Storage Location**:
- **SharePoint**: `Documents/run_<run_id>/inputs_normalized/`
- **Local Dev**: `Z:\Shared\Technology\AI Projects\LeaseFileAudit\run_<run_id>/inputs_normalized/`

**Files**:
- `expected_detail.parquet`: All scheduled charges expanded to months
- `actual_detail.parquet`: All posted AR transactions
- `variance_detail.parquet`: Discrepancies requiring review

**Configuration**:
```bash
PERSIST_DETAIL_DATAFRAMES=true  # Enable Parquet persistence
```

**Save Flow** (`storage/service.py` line 5095-5105):
```python
persist_enabled = os.getenv('PERSIST_DETAIL_DATAFRAMES', 'true').lower() == 'true'
if persist_enabled:
    self._save_detail_dataframe_parquet(expected_detail, run_id, 'expected_detail.parquet')
    self._save_detail_dataframe_parquet(actual_detail, run_id, 'actual_detail.parquet')
    if variance_detail is not None and len(variance_detail) > 0:
        self._save_detail_dataframe_parquet(variance_detail, run_id, 'variance_detail.parquet')
```

**Load Flow** (`storage/service.py` line 3417-3480):
```python
def load_expected_detail(run_id):
    # Try Parquet first
    if PERSIST_DETAIL_DATAFRAMES:
        df = _load_detail_dataframe_parquet(run_id, 'expected_detail.parquet')
        if df is not None and not df.empty:
            return df
    
    # Fall back to in-memory cache
    if run_id in _IN_MEMORY_RESULTS_CACHE:
        return _IN_MEMORY_RESULTS_CACHE[run_id]['expected_detail']
    
    # Return empty DataFrame if not found
    return pd.DataFrame()
```

**Access Pattern**:
- User views lease detail page → `cached_load_expected_detail(run_id)` called
- Cache miss → `storage.load_expected_detail(run_id)` loads from Parquet
- DataFrame returned → transaction arrays built for display

---

#### 3. **RunDisplaySnapshots** (Summary Totals)
**Purpose**: Aggregated summary data for portfolio/property/lease views

**SharePoint List Schema**:
| Column | Type | Description |
|--------|------|-------------|
| `RunId` | Text | Audit run identifier (e.g., `run_20260706_141025`) |
| `ScopeType` | Choice | `portfolio`, `property`, `lease`, `month` |
| `PropertyId` | Text | Property identifier (NULL for portfolio scope) |
| `LeaseIntervalId` | Text | Lease identifier (NULL for property/portfolio) |
| `ArCodeId` | Text | AR code (NULL for aggregated scopes) |
| `AuditMonth` | Date | Month (NULL for aggregated scopes) |
| `ExpectedTotal` | Number | Sum of scheduled charges ($) |
| `ActualTotal` | Number | Sum of posted transactions ($) |
| `Variance` | Number | Difference (actual - expected) ($) |
| `ExceptionCount` | Number | Count of discrepancies |
| `MatchRate` | Number | % of charges that matched |

**Row Counts per Audit Run**:
- **Portfolio scope**: 1 row (all properties combined)
- **Property scope**: 1 row per property (example: 1 row for property 771903)
- **Lease scope**: N rows (example: 668 rows for 668 leases in property 771903)
- **Month scope**: N x M rows (example: 5,297 rows for 668 leases x multiple months/AR codes)
- **Total**: ~5,966 rows per audit run

**Data NOT Stored in RunDisplaySnapshots**:
- ❌ Individual transaction records
- ❌ Transaction arrays (expected_transactions[], actual_transactions[])
- ❌ Post dates
- ❌ Transaction IDs
- ❌ Scheduled charge periods

**Use Cases**:
- ✅ Portfolio dashboard: Load `ScopeType=portfolio` for all runs
- ✅ Property list view: Load `ScopeType=property` for latest run
- ✅ Lease summary table: Load `ScopeType=lease` for specific property/run
- ❌ Lease transaction detail: Must load from Parquet files

---

#### 4. **ExceptionMonths** (Manual Resolution Tracking)
**Purpose**: Track user resolution of billing exceptions

**SharePoint List Schema**:
| Column | Type | Description |
|--------|------|-------------|
| `PropertyId` | Text | Property identifier |
| `LeaseIntervalId` | Text | Lease identifier |
| `ArCodeId` | Text | AR code |
| `AuditMonth` | Date | Month of exception |
| `ExpectedTotal` | Number | Scheduled amount ($) |
| `ActualTotal` | Number | Billed amount ($) |
| `Variance` | Number | Difference ($) |
| `Status` | Choice | `Pending`, `Resolved`, `Acknowledged` |
| `FixLabel` | Text | User-selected resolution reason |
| `Notes` | Text | Free-form notes |
| `ResolvedBy` | Text | User email |
| `ResolvedAt` | Date | Resolution timestamp |

**Write Trigger**: User clicks "Resolve Exception" button in lease detail view
**Read Trigger**: Property/lease view loads exception status for display

**NOT Auto-Populated**: ExceptionMonths is purely manual resolution tracking. It does NOT contain transaction arrays or auto-generated data.

---

### Data Availability Timeline

**During Audit Execution** (Batches Running):
- ❌ Parquet files: Not yet written
- ❌ RunDisplaySnapshots: Not yet written
- ✅ In-memory cache: Populated as execution proceeds
- **Result**: User cannot view transaction details until save completes

**After save_run() Completes** (PHASE 8):
- ✅ Parquet files: Written to SharePoint/filesystem
- ✅ RunDisplaySnapshots: 5,966 rows posted to SharePoint (299 batches)
- ✅ In-memory cache: Fully populated
- **Result**: User can view all transaction details immediately

**After App Restart** (In-Memory Cache Lost):
- ✅ Parquet files: Still available on disk/SharePoint
- ✅ RunDisplaySnapshots: Still in SharePoint list
- ❌ In-memory cache: Empty (must reload from Parquet)
- **Result**: First page load slower (Parquet read), then cached for 4 hours

---

## User Workflow

### 1. **Bulk Audit** (Multiple Properties)

```
User → Bulk Audit Page
  ↓
Select properties from picklist (e.g., CLEMSON EDGE, REDPOINT ATHENS)
  ↓
Click "Start Audit"
  ↓
Background job created (job_id assigned)
  ↓
Each property audited sequentially:
  - Fetch API data
  - Execute reconciliation
  - Save to SharePoint
  - Update job status
  ↓
Job complete (all properties finished)
  ↓
User navigates to property detail page
```

### 2. **View Audit Results**

```
User → Portfolio Dashboard
  ↓
See aggregated KPIs across all properties:
  - Total Exception Count: 187
  - Total Undercharge: $34,567.89
  - Total Overcharge: $1,234.56
  - Average Match Rate: 97.9%
  ↓
Click property (e.g., CLEMSON EDGE)
  ↓
Property Detail View:
  - Property-level KPIs
  - Lease summary table (668 leases)
  - Exception list (5 unresolved exceptions)
  ↓
Click lease (e.g., Lease 18296704)
  ↓
Lease Detail View:
  - Lease metadata (resident name, unit, dates)
  - Monthly summary table (by month + AR code)
  - Expected Transactions table:
    • Period | Amount | Status
    • 08/01/2024 - 08/31/2024 | $1,234.56 | Scheduled
    • 09/01/2024 - 09/30/2024 | $1,234.56 | Scheduled
  - Actual Transactions table:
    • Post Date | Amount | Transaction ID | Entrata Link
    • 08/05/2024 | $1,234.56 | AR-12345 | [View in Entrata]
    • 09/03/2024 | $1,200.00 | AR-12346 | [View in Entrata]
  - Exception resolution buttons
```

### 3. **Resolve Exceptions**

```
User viewing lease with exception
  ↓
Click "Resolve Exception" button
  ↓
Modal opens with resolution options:
  - Fix Label: "Lease modification approved"
  - Notes: "Resident moved to smaller unit"
  - Status: "Resolved"
  ↓
Click "Save"
  ↓
POST /api/exception-months
  ↓
ExceptionMonths list updated in SharePoint
  ↓
Property view refreshes, exception count decrements
```

---

## Original vs Current Implementation

### Original Architecture (CSV-Based)

**Data Storage**:
- ❌ CSV writes: `expected_detail.csv`, `actual_detail.csv`, `bucket_results.csv`
- ❌ AuditRuns2 SharePoint List: Detailed transaction records
- ✅ In-memory cache: Populated during execution

**Save Flow**:
```
execute_audit_run() completes
  ↓
save_run() writes CSVs incrementally during batches
  ↓
CSVs available immediately (before full audit completes)
  ↓
User navigates to lease detail page
  ↓
Views load from CSVs
  ↓
Transaction details display immediately
```

**Problems**:
- ⚠️ CSV writes to SharePoint Document Library slow (~30-60 seconds per file)
- ⚠️ AuditRuns2 list writes slow (thousands of rows per audit)
- ⚠️ Incremental writes caused partial data visibility during execution
- ⚠️ CSV parsing on every page load (no caching)

---

### Current Architecture (Parquet-Based)

**Data Storage**:
- ✅ Parquet files: `expected_detail.parquet`, `actual_detail.parquet`, `variance_detail.parquet`
- ✅ In-memory cache: Populated during execution, persists for 4 hours
- ✅ RunDisplaySnapshots: Aggregated summary snapshots (5,966 rows per run)
- ❌ CSV writes: DISABLED via `DISABLE_CSV_WRITES=true`
- ❌ AuditRuns2 writes: DISABLED (deprecated list)

**Save Flow**:
```
execute_audit_run() completes
  ↓
save_run() executes AFTER all batches finish (no incremental writes)
  ↓
Parquet files written to SharePoint/filesystem
  ↓
In-memory cache populated
  ↓
RunDisplaySnapshots batch-posted (299 batches of 20 rows)
  ↓
User navigates to lease detail page
  ↓
cached_load_expected_detail() loads from Parquet or cache
  ↓
Transaction details display
```

**Benefits**:
- ✅ Parquet writes faster than CSV (~10x compression)
- ✅ Flask cache serves data instantly (4-hour timeout)
- ✅ No CSV parsing on every page load
- ✅ RunDisplaySnapshots provides fast aggregated views
- ✅ Audit history preserved (Parquet files never deleted)

**Tradeoff**:
- ⚠️ Data NOT available until save_run() completes (10-15 minute wait after audit starts)
- ⚠️ User cannot view transaction details during batch processing

**Migration Status**:
- ✅ CSV writes disabled
- ✅ Parquet persistence enabled
- ✅ In-memory cache working
- ✅ Flask cache decorators fixed
- ⚠️ User expectation: immediate data availability (like CSV system)

---

## Next Steps for Immediate Data Availability

### Problem
User expects to view transaction details **immediately** (like old CSV system), but current system requires waiting for **save_run()** to complete after all 299 batches.

### Solution Options

#### Option 1: Incremental Parquet Saves (Recommended)
**Concept**: Save Parquet files every N batches (e.g., every 10 batches)

**Implementation**:
- After batch 10, 20, 30, etc. → call `_save_detail_dataframe_parquet()`
- Update in-memory cache with latest data
- User can view partial results while audit continues

**Tradeoff**:
- ✅ Data available immediately (like CSV system)
- ⚠️ More SharePoint writes (10x more file operations)
- ⚠️ Potential for partial/inconsistent data if audit fails mid-execution

#### Option 2: Real-Time Streaming to SharePoint List
**Concept**: Write transaction records directly to new SharePoint list during batches

**Implementation**:
- Create `AuditTransactionDetail` list with schema matching expected_detail/actual_detail
- POST rows in batches during reconciliation
- Lease view queries list instead of Parquet

**Tradeoff**:
- ✅ Data available immediately
- ✅ No file I/O bottleneck
- ⚠️ SharePoint list size grows rapidly (thousands of rows per audit)
- ⚠️ Violates "no new SharePoint lists" constraint

#### Option 3: Accept Delay, Improve UX
**Concept**: Keep current architecture, add progress indicators and user guidance

**Implementation**:
- Show progress bar during batch processing
- Display message: "Audit in progress (batch 55/299). Results will be available when complete."
- Send email/notification when audit finishes

**Tradeoff**:
- ✅ No code changes to save flow
- ✅ No additional SharePoint writes
- ⚠️ User must wait (10-15 minutes for large properties)

---

## Conclusion

LeaseFileAudit automates billing reconciliation for student housing properties by:
1. **Fetching live data** from Entrata API (scheduled charges + posted transactions)
2. **Reconciling expected vs actual** using four-tier matching algorithm (including cross-interval matching for unit transfers)
3. **Persisting results** to SharePoint (Parquet files + aggregated snapshots)
4. **Tracking exceptions** via manual resolution workflow
5. **Displaying insights** via portfolio/property/lease drill-down views

The current Parquet-based architecture provides **fast, cached access** to audit data with **durable persistence** across app restarts. The tradeoff is that transaction details are not available until **save_run()** completes after all batches finish.

---

**For Questions or Support**:
- See [ENTRATA_API_GUIDE.md](ENTRATA_API_GUIDE.md) for API integration details
- See [MASTER_DOCUMENTATION.md](MASTER_DOCUMENTATION.md) for technical deep-dive
- See [SHAREPOINT_CONNECTIONS_HANDOFF.md](SHAREPOINT_CONNECTIONS_HANDOFF.md) for SharePoint architecture
