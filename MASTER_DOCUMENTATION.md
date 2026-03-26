# Lease File Audit - Master Documentation

> **📝 IMPORTANT**: This documentation must be updated whenever code changes are made.  
> See [CONTRIBUTING.md](CONTRIBUTING.md) for the documentation update process.

## Table of Contents
1. [Application Overview](#application-overview)
2. [Architecture & Technology Stack](#architecture--technology-stack)
3. [Project Structure](#project-structure)
4. [Data Flow & Audit Process](#data-flow--audit-process)
5. [Core Components](#core-components)
6. [Reconciliation Engine](#reconciliation-engine)
7. [SharePoint Integration](#sharepoint-integration)
8. [Exception Tracking & Resolution](#exception-tracking--resolution)
9. [Configuration & Environment](#configuration--environment)
10. [Deployment](#deployment)
11. [Development Workflow](#development-workflow)
12. [Common Scenarios & Troubleshooting](#common-scenarios--troubleshooting)

---

## Application Overview

### Purpose
The Lease File Audit application automatically reconciles scheduled charges against actual posted transactions in the property management system (Entrata). It identifies billing discrepancies, tracks exceptions, and calculates financial impact across the portfolio.

### Business Problem
Property managers need to ensure that all scheduled rent and fee charges are billed correctly. Manual verification is time-consuming and error-prone. This application automates the audit process to:
- Identify unbilled scheduled charges (revenue leakage)
- Detect charges billed without schedules (potential overcharges)
- Find amount mismatches between scheduled and billed amounts
- Track resolution of billing exceptions over time
- Calculate portfolio-level financial metrics

### Key Features
- **Automated Reconciliation**: Matches thousands of transactions using a three-tier matching algorithm
- **Exception Management**: Track and resolve billing exceptions by month and AR code
- **SharePoint Integration**: Store audit runs, track exceptions, and log user activity in SharePoint
- **Entrata Lease-Term Sidecar**: Extract lease expectations from Entrata documents and overlay on AR-code review without changing core match statuses
- **Portfolio Dashboard**: Aggregated view showing latest audit data for each property across all runs with real-time KPIs
- **Immutable Audit History**: Each property upload creates a new run preserving complete audit trail
- **Drill-Down Views**: Property → Lease → Exception detail hierarchy
- **Azure AD Authentication**: Secure, role-based access using Microsoft accounts

---

## Architecture & Technology Stack

### Backend Framework
- **Flask 3.1.0**: Python web framework
- **Python 3.11+**: Core language
- **Pandas 2.2.3**: Data manipulation and reconciliation engine

### Frontend
- **Jinja2 Templates**: Server-side HTML rendering
- **Bootstrap 5**: Responsive UI framework
- **JavaScript/jQuery**: Client-side interactivity for exception management

### Data Storage
- **SharePoint Document Library**: Audit run files (CSV/JSON)
- **SharePoint Lists**: Exception tracking, metrics, activity logs
- **Local Filesystem**: Fallback storage for development

### Authentication & Security
- **Azure AD (Entra ID)**: Single sign-on authentication
- **Microsoft Graph API**: SharePoint data access
- **App Service Authentication**: Managed identity for production

### Deployment
- **Azure App Service**: Linux-based web hosting
- **GitHub Actions**: CI/CD pipeline
- **Docker**: Containerized deployment (optional)

---

## Project Structure

```
LeaseFileAudit/
├── app.py                          # Flask application factory
├── run.py                          # Development server entry point
├── config.py                       # Centralized configuration
├── requirements.txt                # Python dependencies
├── .env                           # Local environment variables (not in git)
│
├── web/                           # Web application layer
│   ├── __init__.py
│   ├── auth.py                    # Azure AD authentication logic
│   └── views.py                   # Route handlers and business logic
│
├── audit_engine/                  # Core reconciliation engine
│   ├── __init__.py
│   ├── canonical_fields.py        # Field name standardization (enums)
│   ├── mappings.py                # Source data transformations
│   ├── normalize.py               # Data validation and cleaning
│   ├── expand.py                  # Scheduled charge expansion to months
│   ├── reconcile.py               # Matching algorithm (3-tier)
│   ├── rules.py                   # Business rule validation
│   ├── findings.py                # Exception detection and categorization
│   ├── metrics.py                 # KPI calculations
│   ├── entrata_lease_terms.py     # Entrata lease doc selection, term extraction, AR overlay, incremental refresh
│   └── schemas.py                 # Data validation schemas
│
├── storage/                       # Data persistence layer
│   ├── __init__.py
│   └── service.py                 # StorageService (SharePoint + local files)
│
├── activity_logging/              # User activity tracking
│   ├── __init__.py
│   └── sharepoint.py              # SharePoint list logging
│
├── templates/                     # Jinja2 HTML templates
│   ├── base.html
│   ├── upload.html
│   ├── portfolio.html
│   ├── property.html
│   ├── lease.html
│   └── ...
│
├── static/                        # CSS, JavaScript, images
│   └── styles.css
│
└── instance/                      # Runtime data (not in git)
    └── runs/                      # Local audit run storage
        └── run_YYYYMMDD_HHMMSS/
```

### Key Documentation Files
- **CANONICAL_FIELDS_README.md**: Explanation of field standardization
- **DATA_MODEL.md**: Database schema and data structures
- **RECONCILIATION_FRAMEWORK_IMPLEMENTATION.md**: Matching algorithm details
- **SHAREPOINT_STORAGE_DEPLOYMENT.md**: SharePoint setup guide
- **V1_REQUIRED_FIELDS.md**: Data requirements for audit runs

---

## Data Flow & Audit Process

### High-Level Flow

```
1. User provides input data (Excel upload OR API fetch)
   - Excel path: `/upload` with AR Transactions + Scheduled Charges workbook (Lease File Audit - Input folder - Scheduled vs AR Transactions Input v2)
   - API property path: `/upload-api-property` fetches Entrata property data
   - API lease path: `/upload-api-lease` fetches Entrata single-lease data
   ↓
2. Data Loading & Mapping (mappings.py + api_ingest.py)
   - Load raw Excel sheets OR fetch API datasets and stage equivalent raw DataFrames
   - Map source columns to canonical field names
   - Apply source-specific transformations
   ↓
3. Normalization (normalize.py)
   - Validate data types
   - Filter active records only
   - Drop invalid rows
   ↓
4. Expansion (expand.py)
   - Convert scheduled charges to monthly buckets
   - Handle one-time vs recurring charges
   ↓
5. Reconciliation (reconcile.py)
   - Tier 1: Exact match (property, lease, AR code, month)
   - Tier 2: Amount-only match (same bucket keys)
   - Tier 3: Date mismatch (wrong month posted)
   - Identify variances (under/over/unbilled)
   ↓
6. Business Rules (rules.py)
   - Apply severity levels
   - Flag high-priority issues
   ↓
7. Storage (storage/service.py)
   - Save bucket results to SharePoint/local
   - Create run metadata
   - Update metrics list
   ↓
8. Presentation (web/views.py)
   - Filter resolved exceptions from SharePoint
   - Calculate KPIs (current + historical)
   - Render portfolio/property/lease views
```

### Detailed Step Breakdown

#### Step 1: Input Acquisition (Excel or API)
- **Excel upload path** (`/upload`):
   - User selects workbook with AR Transactions + Scheduled Charges
   - `io.load_excel_sources()` auto-detects sheets by keywords:
      - AR Transactions: "AR", "transactions", "posted"
      - Scheduled Charges: "scheduled", "charges"
   - Loads raw DataFrames from workbook sheets
- **API property path** (`/upload-api-property`):
   - User selects property and optional date window
   - `audit_engine/api_ingest.py` fetches Entrata API sources (`getLeaseDetails`, `getLeaseArTransactions`)
   - Builds AR and Scheduled raw DataFrames aligned to the same pipeline contract
- **API lease path** (`/upload-api-lease`):
   - User submits a single lease (and optional property/date scope)
   - `audit_engine/api_ingest.py` fetches lease-scoped Entrata data
   - Builds lease-scoped raw DataFrames for the same downstream audit pipeline

#### Step 2: Source Mapping
- **Purpose**: Convert proprietary column names to standard canonical fields
- **File**: `audit_engine/mappings.py`
- **Process**:
  ```python
  AR_TRANSACTIONS_MAPPING:
    - PROPERTY_ID → PROPERTY_ID (canonical)
    - TRANSACTION_AMOUNT → ACTUAL_AMOUNT
    - POST_DATE (YYYYMMDD) → POST_DATE (datetime)
    - Calculate AUDIT_MONTH (first day of month)
  
  SCHEDULED_CHARGES_MAPPING:
    - CHARGE_AMOUNT → EXPECTED_AMOUNT
    - CHARGE_START_DATE → PERIOD_START
    - CHARGE_END_DATE → PERIOD_END
  ```
- **API note**:
   - API ingestion (`audit_engine/api_ingest.py`) stages Entrata payloads into AR/scheduled DataFrames compatible with the same canonical mapping/normalization path used by Excel uploads.
- **Row Filters Applied**:
  - AR: Only `IS_POSTED=1`, active leases, exclude API-posted codes
  - Scheduled: Only cached to lease, not deleted, not unselected quotes

#### Step 3: Normalization
- **File**: `audit_engine/normalize.py`
- **Validations**:
  - Required fields present
  - Date fields are valid datetime
  - Numeric fields are valid numbers
  - No NaT/NaN in critical fields (drops rows with issues)

#### Step 4: Expansion
- **File**: `audit_engine/expand.py`
- **Purpose**: Create monthly buckets from scheduled charges
- **Logic**:
  ```python
  For each scheduled charge:
    If PERIOD_END is null:
      → One-time charge (single month)
    Else:
      → Recurring charge (expand from PERIOD_START to PERIOD_END)
      → Create one row per month
  ```
- **Output**: Every scheduled charge becomes one or more monthly buckets

#### Step 5: Reconciliation (THE CORE ALGORITHM)
- **File**: `audit_engine/reconcile.py`
- **See detailed section below** → [Reconciliation Engine](#reconciliation-engine)

#### Step 6: Storage
- **File**: `storage/service.py`
- **SharePoint Mode** (production):
  - Upload CSV files to `LeaseFileAudit Runs` document library
  - Create folders: `run_YYYYMMDD_HHMMSS/`
  - Store: `bucket_results.csv`, `expected_detail.csv`, `actual_detail.csv`, `metadata.json`
  - Insert row into `LeaseFileAudit Metrics` list for fast KPI loading
  
- **Local Mode** (development):
  - Write files to `instance/runs/run_YYYYMMDD_HHMMSS/`

#### Step 7: Exception Tracking
- **SharePoint Lists Used**:
  1. **Exception Months List**: Monthly exception status (Open/Resolved/In Progress)
  2. **Exception States List** (legacy, being phased out)
  3. **Metrics List**: Aggregate metrics per run

- **User Actions**:
  - View exceptions by property → lease → AR code → month
  - Mark individual months as "Resolved" with fix actions
  - System auto-calculates AR code status from month-level statuses

#### Step 8: Portfolio Analytics
- **File**: `web/views.py` → `calculate_cumulative_metrics()`
- **Fast Path** (SharePoint enabled):
  - Load all runs from Metrics List (very fast)
  - Load most recent bucket results
  - Filter out resolved exceptions from Exception Months list
  - Calculate current under/overcharge from unresolved exceptions
  - Calculate historical under/overcharge from resolved exceptions
  
- **Slow Path** (local mode):
  - Load all run CSV files
  - Deduplicate exceptions across runs
  - Calculate totals

### Audit Field Mapping & Normalization Requirements

This section summarizes the audit field contract used between mapping and normalization.

#### AR Transactions: Mapping Output Contract

| Canonical Field | Source Column | Expected Data Type | Normalization Required? |
|---|---|---|---|
| `PROPERTY_ID` | `PROPERTY_ID` | Integer / numeric ID | Yes |
| `PROPERTY_NAME` | `PROPERTY_NAME` | String | Yes |
| `LEASE_INTERVAL_ID` | `LEASE_INTERVAL_ID` | Integer / numeric ID | Yes |
| `AR_CODE_ID` | `AR_CODE_ID` | String or numeric code | Yes |
| `AR_CODE_NAME` | `AR_CODE_NAME` | String | Yes |
| `actual_amount` | `TRANSACTION_AMOUNT` | Float | Yes |
| `POST_DATE` | `POST_DATE` | Datetime (parsed from `YYYYMMDD`) | Yes |
| `AUDIT_MONTH` | Derived from `POST_DATE` | Datetime (month start) | Yes |
| `AR_TRANSACTION_ID` | `ID` | String / ID | Yes |
| `IS_REVERSAL` | `IS_REVERSAL` | Numeric flag (0/1) | Yes |
| `CUSTOMER_NAME` | `CUSTOMER_NAME` | String | Yes |
| `GUARANTOR_NAME` | `GUARANTOR_NAME` | String | Yes |
| `LEASE_ID` | `LEASE_ID` | String / ID | Optional passthrough |
| `CUSTOMER_ID` | `CUSTOMER_ID` | String / ID | Optional passthrough |
| `SCHEDULED_CHARGE_ID_LINK` | `SCHEDULED_CHARGE_ID` | String / ID | Optional passthrough |

#### Scheduled Charges: Mapping Output Contract

| Canonical Field | Source Column | Expected Data Type | Normalization Required? |
|---|---|---|---|
| `SCHEDULED_CHARGES_ID` | `ID` | String / ID | Yes |
| `PROPERTY_ID` | `PROPERTY_ID` | Integer / numeric ID | Yes |
| `LEASE_ID` | `LEASE_ID` | String / ID | Yes |
| `LEASE_INTERVAL_ID` | `LEASE_INTERVAL_ID` | Integer / numeric ID | Yes |
| `AR_CODE_ID` | `AR_CODE_ID` | String or numeric code | Yes |
| `AR_CODE_NAME` | `AR_CODE_NAME` | String | Yes |
| `expected_amount` | `CHARGE_AMOUNT` | Float | Yes |
| `PERIOD_START` | Derived from `CHARGE_START_DATE` | Datetime | Yes |
| `PERIOD_END` | Derived from `CHARGE_END_DATE` | Datetime (`NaT` allowed for one-time) | Yes |
| `CUSTOMER_NAME` | `CUSTOMER_NAME` | String | Yes |
| `CUSTOMER_ID` | `CUSTOMER_ID` | String / ID | Yes |
| `GUARANTOR_NAME` | `GUARANTOR_NAME` | String | Yes |
| `SCHEDULED_CHARGE_ID` | `SCHEDULED_CHARGE_ID` | String / ID | Optional passthrough |
| `IS_UNSELECTED_QUOTE` | `IS_UNSELECTED_QUOTE` | Numeric flag (0/1) | Filter/support field |
| `IS_CACHED_TO_LEASE` | `IS_CACHED_TO_LEASE` | Numeric flag (0/1) | Filter/support field |
| `POSTED_THROUGH_DATE` | `POSTED_THROUGH_DATE` | Date-like string | Filter/support field |
| `LAST_POSTED_ON` | `LAST_POSTED_ON` | Date-like string | Filter/support field |
| `AR_CASCADE_ID` | `AR_CASCADE_ID` | Integer / numeric ID | Filter/support field |
| `AR_TRIGGER_ID` | `AR_TRIGGER_ID` | Integer / numeric ID | Filter/support field |
| `SCHEDULED_CHARGE_TYPE_ID` | `SCHEDULED_CHARGE_TYPE_ID` | Integer / numeric ID | Filter/support field |

#### Required Columns During Normalization

Normalization enforces these required canonical columns and raises if missing.

**AR normalization required columns** (`normalize_ar_transactions`):
- `PROPERTY_ID`
- `PROPERTY_NAME`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `AUDIT_MONTH`
- `actual_amount`
- `AR_TRANSACTION_ID`
- `IS_REVERSAL`
- `POST_DATE`
- `AR_CODE_NAME`
- `CUSTOMER_NAME`
- `GUARANTOR_NAME`

**Scheduled normalization required columns** (`normalize_scheduled_charges`):
- `SCHEDULED_CHARGES_ID`
- `PROPERTY_ID`
- `LEASE_ID`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `expected_amount`
- `PERIOD_START`
- `PERIOD_END`
- `AR_CODE_NAME`
- `CUSTOMER_NAME`
- `CUSTOMER_ID`
- `GUARANTOR_NAME`

**Normalization drop rules**:
- AR: rows with invalid `AUDIT_MONTH` (`NaT`) are dropped.
- Scheduled: rows with invalid `PERIOD_START` (`NaT`) are dropped.
- Scheduled `PERIOD_END` may be `NaT` for one-time charges.

---

## Core Components

### 1. Canonical Fields (`audit_engine/canonical_fields.py`)

**Problem**: Different data sources use different column names for same concepts.

**Solution**: Enum-based field standardization
```python
class CanonicalField(Enum):
    PROPERTY_ID = "PROPERTY_ID"
    LEASE_INTERVAL_ID = "LEASE_INTERVAL_ID"
    AR_CODE_ID = "AR_CODE_ID"
    AUDIT_MONTH = "AUDIT_MONTH"
    ACTUAL_AMOUNT = "ACTUAL_AMOUNT"      # From AR Transactions
    EXPECTED_AMOUNT = "EXPECTED_AMOUNT"  # From Scheduled Charges
    # ... etc
```

**Benefits**:
- Type-safe field references (IDE autocomplete)
- Refactoring-safe (rename in one place)
- Self-documenting code

### 2. Storage Service (`storage/service.py`)

**Purpose**: Abstract data persistence (SharePoint vs local filesystem)

**Key Methods**:
```python
StorageService:
  - save_run()              # Save audit results
  - load_run()              # Load audit results
  - list_runs()             # List all runs
  
  # SharePoint List operations:
  - load_exception_months_from_sharepoint_list()
  - upsert_exception_month_to_sharepoint_list()
  - load_all_metrics_from_sharepoint_list()
  
  # Auto-detection:
  - is_sharepoint_configured() → use_sharepoint flag
```

**Configuration**:
- Controlled by `USE_SHAREPOINT_STORAGE` environment variable
- Auto-switches between SharePoint and local file mode

### 3. Authentication (`web/auth.py`)

**Development Mode** (`REQUIRE_AUTH=false`):
- Mock user with local credentials
- Useful for testing without Azure AD

**Production Mode** (`REQUIRE_AUTH=true`):
- Azure AD authentication via App Service
- User info from `/.auth/me` endpoint
- Access token retrieval for SharePoint API calls

**Decorators**:
```python
@require_auth   # Must be logged in
@optional_auth  # Login optional, provides user context
```

### 4. Route Handlers (`web/views.py`)

**Key Routes**:
- `GET /` - Upload form and recent runs
- `POST /upload` - Process Excel file, run audit
- `POST /upload-api-property` - Process API-based property audit (Entrata direct fetch)
- `GET /portfolio` or `GET /portfolio/<run_id>` - Portfolio dashboard (aggregates latest data per property across all runs)
- `GET /property/<property_id>/<run_id>` - Property exceptions grouped by lease
- `GET /lease/<run_id>/<property_id>/<lease_id>` - Detailed lease exceptions

**Portfolio View Behavior** (updated 2026-03-10):
- **Architecture**: Each property upload stores ONLY that property's data (independent audit trail)
- **Aggregation**: Portfolio shows the most recent audit data for each property across ALL runs
- **How it works**:
  1. Queries all property-scoped snapshots from RunDisplaySnapshots list
  2. Groups by `PropertyId` and selects the latest `RunId` for each property
  3. Aggregates KPIs: total undercharge, overcharge, exceptions, match rate
  4. Each property row links to its specific run_id for drill-down
- **Benefits**:
  - Each property has clean, independent audit history
  - No duplicate data stored across runs
  - Simpler architecture (no baseline merging)
  - Portfolio always shows current state of all properties
- **Use case**: Upload property A (creates run_1 with only A's data), upload property B (creates run_2 with only B's data) → Portfolio shows both A (from run_1) + B (from run_2)

**API Endpoints** (for AJAX):
- `POST /api/exception-months` - Update month status
- `GET /api/exception-months/<...>` - Get month statuses
- `GET /api/exception-months/ar-status/<...>` - Get calculated AR code status

### 5. Entrata Lease-Term Sidecar (`audit_engine/entrata_lease_terms.py`)

**Purpose**: Extract lease expectations from Entrata lease packets/addenda and display them in lease drawer UX as a non-disruptive comparison layer.

**Design Guardrail**:
- Does **not** modify reconciliation status calculation (`MATCHED`, `SCHEDULED_NOT_BILLED`, etc.)
- Runs as a sidecar enrichment for lease-level display

**Major Responsibilities**:
- Entrata API helper (`post_entrata`) and lease picklist caching (`fetch_lease_picklist`)
- Signed packet + addenda selection (`select_lease_packet_and_addenda` / `download_lease_document`)
- PDF parsing helpers (`parse_pdf_to_text_pack`, `identify_relevant_pages`) with PyMuPDF text extraction
- Parking-specific extraction helper (`extract_parking_fee`) to prioritize addendum parking cost language
- Primary packet + addenda split extraction model (base rent/dates from packet; fees from addenda/context)
- Scalable term→AR mapping registry (`build_term_ar_code_registry`)
- AR drawer overlay builder (`build_lease_expectation_overlay`)
- Incremental refresh pipeline (`refresh_lease_terms_for_lease_interval`)

**Term Mapping Rules Source**:
- Default term→AR-code mappings are centralized in `audit_engine/lease_term_rules.py` (`DEFAULT_TERM_TO_AR_CODE_RULES`)
- `build_term_ar_code_registry(...)` in `audit_engine/entrata_lease_terms.py` loads these defaults and supports optional overrides
- Current defaults include numeric Entrata AR-code IDs for:
   - `BASE_RENT` (`154771`)
   - `PET_RENT` (`155034`)
   - `PARKING` (`155052`, `155385`)
   - `UTILITY` (`155026`, `155030`, `155023`)
   - `APPLICATION_FEE` (`154788`)
   - `ADMIN_FEE` (`155012`)
   - `AMENITY_PREMIUM` (`155007`)

**Current Extraction Notes (v2)**:
- Avoids hard dependency on numbered clause anchors; uses keyword/context scoring
- Base rent prioritizes monthly/installment language over total-rent-only values
- Date parsing normalizes mixed formats (numeric + textual/ordinal)
- Parking extraction uses section-aware scoring to avoid unrelated `$` values and improve addendum capture
- Extraction emits `[LEASE TERMS]` logs for term rows and evidence snippets (including page number)

**Incremental Refresh Model**:
1. Build lease key (`PROPERTY_ID:LEASE_INTERVAL_ID`)
2. Check `LeaseTermSet` for last check timestamp and fingerprint
3. Skip full parse if within recheck TTL
4. Re-fetch full doc metadata and compute deterministic `DocListFingerprint`
5. If `DocListFingerprint` is unchanged and cached `LeaseTerms` exist, reuse cached terms (no download)
6. Otherwise compute selected-doc fingerprint and re-parse only when selected parse inputs changed (or forced)
7. New audit run context (`run_id` changed) bypasses TTL short-circuit to ensure doc-list check occurs on first lease open in that run
8. Fail open: if refresh errors and cached terms exist, return stale cached terms

**Lease View Integration (`web/views.py`)**:
- `lease_view()` calls `refresh_lease_terms_for_lease_interval(...)`
- Loads active term rows from SharePoint (`load_lease_terms_for_lease_key_from_sharepoint_list`)
- Builds AR-code overlay and lease-only expectation list
- Passes `lease_only_expectations` + `lease_mapping_diagnostics` to `templates/lease.html`

**UI Behavior (`templates/lease.html`)**:
- Adds lease-only alert block when lease terms exist without matching SC/AR rows
- Drawer shows `Lease Expectations` section per AR code with summary + evidence when mapped

---

## Reconciliation Engine

### Overview
The reconciliation engine matches scheduled charges against posted AR transactions to identify variances. It uses a **three-tier matching algorithm** with hash-based grouping for O(n) performance.

### Bucket Key Concept
Each transaction is grouped into a "bucket" defined by:
1. **PROPERTY_ID** - Which property
2. **LEASE_INTERVAL_ID** - Which lease
3. **AR_CODE_ID** - Which charge type (rent, pet fee, etc.)
4. **AUDIT_MONTH** - Which month (normalized to first day)

### Matching Algorithm (file: `reconcile.py`)

#### Tier 1: PRIMARY Match (Exact)
```python
# Group by bucket key
expected_grouped = expected.groupby(BUCKET_KEY).sum()
actual_grouped = actual.groupby(BUCKET_KEY).sum()

# Pandas merge (hash join under the hood)
matched = expected_grouped.merge(actual_grouped, on=BUCKET_KEY, how='inner')

# STATUS assignment:
if expected_total == actual_total:
    STATUS = "MATCHED"
elif expected_total > actual_total:
    STATUS = "SCHEDULED_NOT_BILLED"  (undercharge)
elif actual_total > expected_total:
    STATUS = "BILLED_NOT_SCHEDULED"  (overcharge)
else:
    STATUS = "AMOUNT_MISMATCH"
```

**Performance**: O(n) via hash grouping (pandas groupby uses hash tables)

#### Tier 2: SECONDARY Match (Amount-only, different buckets)
- Matches scheduled charges to AR transactions with different bucket keys but same amount
- Useful for charges posted to wrong AR code or wrong lease
- Still flags as variance but helps explain discrepancies

#### Tier 3: TERTIARY Match (Date mismatch)
- Matches charges posted in wrong month
- Same property, lease, AR code, but different month
- Helps identify timing issues vs true missing charges

### Variance Calculation
```python
VARIANCE = ACTUAL_TOTAL - EXPECTED_TOTAL

# Interpretation:
VARIANCE < 0  → Undercharged (expected more than billed)
VARIANCE > 0  → Overcharged (billed more than expected)
VARIANCE = 0  → Perfectly matched
```

### Why Hash-Based Grouping?

**Naive Approach** (O(n²)):
```python
for scheduled in scheduled_charges:
    for ar in ar_transactions:
        if matches(scheduled, ar):
            # Found match
```
Performance: 10,000 scheduled × 50,000 AR = 500M comparisons 😱

**Hash-Based Grouping** (O(n)):
```python
# Pandas internally creates hash tables:
scheduled_hash = {
    (property, lease, ar_code, month): [charges],
    ...
}
actual_hash = {
    (property, lease, ar_code, month): [transactions],
    ...
}
# Merge = hash lookup per key
```
Performance: 10,000 + 50,000 = 60,000 operations ✅

### Exception Status Types

| Status | Meaning | Financial Impact |
|--------|---------|------------------|
| `MATCHED` | Expected = Actual | None |
| `SCHEDULED_NOT_BILLED` | Charge scheduled but not posted | Undercharge (revenue loss) |
| `BILLED_NOT_SCHEDULED` | Transaction posted without schedule | Potential overcharge |
| `AMOUNT_MISMATCH` | Posted amount ≠ scheduled amount | Under or overcharge |

---

## SharePoint Integration

### Lists Used

#### 1. Exception Months List
**Purpose**: Track resolution status for each exception month

**Columns**:
- `RunID` (text) - e.g., "run_20260127_135019"
- `PropertyID` (number)
- `LeaseIntervalID` (number)
- `ARCodeID` (text)
- `AuditMonth` (date) - e.g., "2025-01-01"
- `Status` (choice) - Open, In Progress, Resolved
- `FixLabel` (text) - Description of fix action
- `ActionType` (choice) - bill_next_cycle, adjust_schedule, etc.
- `Variance` (number)
- `ExpectedTotal` (number)
- `ActualTotal` (number)
- `ResolvedAt` (datetime)
- `ResolvedBy` (text)

**Usage**:
```python
# Load all months for an AR code:
months = storage.load_exception_months_from_sharepoint_list(
    run_id, property_id, lease_id, ar_code_id
)

# Update month status:
storage.upsert_exception_month_to_sharepoint_list({
    'run_id': 'run_20260127_135019',
    'property_id': 100069944,
    'lease_interval_id': 100149619,
    'ar_code_id': '155001',
    'audit_month': '2025-01-01',
    'status': 'Resolved',
    'variance': -500.00,
    # ... other fields
})
```

#### 2. LeaseFileAudit Metrics List
**Purpose**: Fast loading of portfolio metrics without reading CSV files

**Columns**:
- `RunID` (text)
- `Timestamp` (datetime)
- `UploadedBy` (text)
- `TotalBuckets` (number)
- `Matched` (number)
- `TotalVariances` (number)
- `HighSeverity` (number)
- `MediumSeverity` (number)
- `LowSeverity` (number)

**Performance Benefit**:
- Loading portfolio: <100ms (read list)
- vs old way: 5-10 seconds (load all CSVs)

#### 3. Innovation Use Log
**Purpose**: Track user activity for reporting

**Columns**:
- `UserName`, `UserEmail`
- `LoginTimestamp`
- `Application` - "LeaseFileAudit"
- `ActivityType` - "Start Session", "Successful Audit", "Failed Audit"
- `Env` - "Production", "Local"

#### 4. AuditRuns
**Purpose**: Persist detailed reconciliation outputs in SharePoint List so app reads list-backed results (not CSV-only) for bucket results and findings.

**Required Columns (with SharePoint type)**:
- `Title` — **Single line of text** (built-in; e.g., `bucket_result:0`, `finding:24`)
- `CompositeKey` — **Single line of text**
- `RunId` — **Single line of text**
- `ResultType` — **Choice** (values: `bucket_result`, `finding`) *(Single line of text also works)*
- `PropertyId` — **Number**
- `LeaseIntervalId` — **Number**
- `ArCodeId` — **Single line of text**
- `AuditMonth` — **Date and Time** *(Single line of text also works)*
- `Status` — **Single line of text**
- `Severity` — **Single line of text**
- `FindingTitle` — **Single line of text**
- `Variance` — **Number**
- `ExpectedTotal` — **Number**
- `ActualTotal` — **Number**
- `ImpactAmount` — **Number**
- `MatchRule` — **Single line of text**
- `FindingId` — **Single line of text**
- `Category` — **Single line of text**
- `Description` — **Multiple lines of text**
- `ExpectedValue` — **Single line of text**
- `ActualValue` — **Single line of text**
- `CreatedAt` — **Date and Time**

**No JSON blob requirement**:
- `RowJson` is no longer required for reads/writes.
- `Evidence` is also optional and not required for current UI behavior.
- App now writes/loads explicit typed columns for both result types.

**Read/write behavior**:
- On save, app writes `bucket_results` + `findings` rows to `AuditRuns`.
- **Write mode**: Background writes are enabled by default to reduce upload timeout risk.
   - `ASYNC_AUDIT_RESULTS_WRITE=true` (default) writes AuditRuns detail rows asynchronously.
   - `ASYNC_RUN_DISPLAY_SNAPSHOTS=true` (default) writes RunDisplaySnapshots asynchronously.
   - Set either env var to `false` to force synchronous blocking writes for troubleshooting.
- On load, app reads `AuditRuns` first and falls back to CSV files if list rows are unavailable.
- If list-backed results are partially written, loaders compare list row count vs CSV row count and use CSV for complete rendering.
- Existing CSV run artifacts remain as compatibility fallback.
- Write path uses Microsoft Graph `$batch` API with automatic retry logic and configurable batch sizes.

**Batch write resilience (implemented 2026-03-10)**:
- **Default batch sizes**: 10 items for AuditRuns, 20 items for snapshots
- **Retry logic**: Automatic exponential backoff for 429/503/504 throttling errors
  - Individual items: up to 3 retries with 0.5s → 1s → 2s backoff
  - Batch requests: up to 3 retries with 1s → 2s backoff
  - Failed batches automatically fall back to individual item posts with retry
- **Delays between batches**: 0.5s pause reduces API pressure and throttling
- **Environment variables** (optional):
  - `SHAREPOINT_BATCH_SIZE_AUDITRUNS` - Override AuditRuns batch size (default: 10)
  - `SHAREPOINT_BATCH_SIZE_SNAPSHOTS` - Override snapshots batch size (default: 20)
  - `SHAREPOINT_BATCH_SIZE` - Global fallback batch size
- **Logging**: Look for `[STORAGE] Batch X/Y throttled` to monitor retry activity

**Indexing required for reliable filtered reads**:
- Index `RunId` (required)
- Index `ResultType` (required)
- Recommended additional indexes: `CompositeKey`, `PropertyId`, `LeaseIntervalId`, `ArCodeId`, `AuditMonth`

**ResultType column mapping**:
- `bucket_result` rows map to: `PROPERTY_ID`, `LEASE_INTERVAL_ID`, `AR_CODE_ID`, `AUDIT_MONTH`, `expected_total`, `actual_total`, `variance`, `status`, `match_rule`.
- `finding` rows map to: `finding_id`, `run_id`, `property_id`, `lease_interval_id`, `ar_code_id`, `audit_month`, `category`, `severity`, `title`, `description`, `expected_value`, `actual_value`, `variance`, `impact_amount`.

#### 5. RunDisplaySnapshots
**Purpose**: Persist static, precomputed display totals/counts for portfolio/property/lease scopes so UI can load without recalculating from detail rows.

**Required Columns (with SharePoint type)**:
- `Title` — **Single line of text**
- `SnapshotKey` — **Single line of text**
- `RunId` — **Single line of text**
- `ScopeType` — **Choice** (values: `portfolio`, `property`, `lease`)
- `PropertyId` — **Number** *(required for property/lease rows)*
- `LeaseIntervalId` — **Number** *(required for lease rows)*
- `ExceptionCountStatic` — **Number**
- `UnderchargeStatic` — **Number**
- `OverchargeStatic` — **Number**
- `MatchRateStatic` — **Number**
- `TotalBucketsStatic` — **Number**
- `MatchedBucketsStatic` — **Number**
- `CreatedAt` — **Date and Time**

**Indexing required**:
- Index `SnapshotKey` (required)
- Index `RunId` (required)
- Index `ScopeType` (required)
- Recommended additional indexes: `PropertyId`, `LeaseIntervalId`

**Internal name compatibility**:
- The app supports either `ExceptionCountStatic` or legacy internal name `ExceptionCountStatistic` for exception count snapshots.

**Behavior**:
- Rows are written once per upload from `bucket_results`.
- Values are static snapshots and do not recalculate on resolution status changes.
- Snapshot totals/counts are calculated from unresolved-only exceptions at write time by applying current resolved-month state from `ExceptionMonths` before aggregation.
- Resolution status visibility remains driven by `ExceptionMonths`.
- Portfolio/Property/Lease headers prefer `RunDisplaySnapshots`; if a row is missing, routes fall back to in-memory recalculation.
- Debug logs use tags: `[SNAPSHOT][PORTFOLIO]`, `[SNAPSHOT][PROPERTY]`, `[SNAPSHOT][LEASE]` to show snapshot usage vs fallback.

**AuditRuns read-path cutover**:
- Portfolio and Property routes now load bucket/finding result sets from `AuditRuns` (with CSV fallback), instead of relying on full `load_run()` for core result queries.
- Lease route now loads lease bucket data from `AuditRuns` and only uses persisted `expected_detail`/`actual_detail` inputs for transaction/date enrichment in the drawer.

#### 6. LeaseTermSet
**Purpose**: One row per lease key with refresh/fingerprint control metadata.

**Required Columns**:
- `Title` (text)
- `LeaseKey` (text, indexed)
- `PropertyId` (number)
- `LeaseIntervalId` (number)
- `LeaseId` (number)
- `TermSetVersion` (number)
- `FingerprintHash` (text)
- `DocListFingerprint` (text)
- `SelectedDocIds` (text)
- `LastCheckedAt` (datetime)
- `LastRefreshedAt` (datetime)
- `Status` (text; e.g., `active`, `stale`, `error`)
- `RefreshError` (multiline text or text)
- `RunIdLastSeen` (text)

**Usage**:
- Upserted by `upsert_lease_term_set_to_sharepoint_list()`
- Read by `load_lease_term_set_for_lease_key()`

#### 7. LeaseTerms
**Purpose**: Normalized active lease term rows used to build AR overlays.

**Required Columns**:
- `Title`, `TermKey`, `LeaseKey` (text; `LeaseKey` indexed)
- `PropertyId`, `LeaseIntervalId`, `LeaseId`, `TermSetVersion` (number)
- `IsActive` (boolean)
- `TermType`, `MappedArCode`, `Frequency`, `ConditionsKey`, `MappingVersion` (text)
- `Amount`, `MappingConfidence` (number)
- `StartDate`, `EndDate`, `UpdatedAt` (datetime/text per list setup)
- `TermSourceDocId`, `TermSourceDocName` (text)

**Usage**:
- Replaced atomically per lease key via `replace_lease_terms_to_sharepoint_list()`
- Loaded for lease view via `load_lease_terms_for_lease_key_from_sharepoint_list()`

#### 8. LeaseTermEvidence
**Purpose**: Evidence snippets/page references for extracted lease terms.

**Required Columns**:
- `Title`, `EvidenceKey`, `TermKey`, `LeaseKey`, `DocId`, `DocName` (text)
- `PropertyId`, `LeaseIntervalId`, `LeaseId`, `PageNumber` (number)
- `ExcerptText` (multiline text)
- `Confidence` (number)
- `CapturedAt` (datetime)

**Usage**:
- Replaced per lease key via `replace_lease_term_evidence_to_sharepoint_list()`

### Document Library Structure

```
LeaseFileAudit Runs/
├── run_20260127_135019/
│   ├── bucket_results.csv        # Main reconciliation results
│   ├── expected_detail.csv       # Expanded scheduled charges
│   ├── actual_detail.csv         # Posted AR transactions
│   ├── variance_detail.csv       # Detailed row-level variances
│   ├── run_meta.json             # Metadata (timestamp, user, filters)
│   └── input_normalized.xlsx     # Original upload (optional)
├── run_20260128_094838/
│   └── ...
```

---

## Exception Tracking & Resolution

### Workflow

1. **Detection** (automated during audit)
   - Reconciliation engine identifies variances
   - Saves to bucket_results with STATUS field

2. **Review** (user action)
   - User navigates: Portfolio → Property → Lease
   - Exceptions grouped by AR code
   - Drill down to individual months

3. **Resolution** (user marks resolved)
   - Click "Resolve" button on exception month
   - Select fix action (e.g., "Bill in next cycle")
   - Status changes: Open → Resolved
   - Updates SharePoint Exception Months list

4. **Portfolio Impact** (automated)
   - Resolved exceptions filtered from current KPIs
   - Moved to historical undercharge/overcharge totals
   - AR code auto-calculates status from month statuses

### Resolution Tracking Logic

**Current Exceptions**:
```python
# Filter OUT resolved exceptions
def is_unresolved(row):
    key = (property_id, lease_id, ar_code_id, audit_month)
    return key not in resolved_keys

current_exceptions = all_exceptions[all_exceptions.apply(is_unresolved)]
```

**Historical Metrics**:
```python
# Sum variance from resolved exceptions
historical_undercharge = sum(
    abs(exc['variance']) 
    for exc in resolved_exceptions 
    if exc['variance'] < 0
)

historical_overcharge = sum(
    exc['variance'] 
    for exc in resolved_exceptions 
    if exc['variance'] > 0
)
```

### AR Code Status Calculation
When months are marked resolved, the AR code status auto-updates:

```python
Status Logic:
  All months Resolved → AR Code: "Resolved"
  Some months Resolved → AR Code: "In Progress"
  No months Resolved → AR Code: "Open"
```

---

## Configuration & Environment

### Environment Variables (.env file)

**Critical Variables**:
```bash
# SharePoint Storage
USE_SHAREPOINT_STORAGE=true
SHAREPOINT_LIBRARY_NAME=LeaseFileAudit Runs

# Entrata Lease-Term Extraction
ENTRATA_API_KEY=<secret>
ENTRATA_ORG=peakmade
ENTRATA_DEFAULT_PROPERTY_ID=<optional>
ENTRATA_DEFAULT_LEASE_ID=<optional>
OUT_DIR=C:\Users\<user>\Downloads\EntrataLeases
SAVE_LOCAL_ENTRATA_PDFS=false
LEASE_TERM_REFRESH_TTL_HOURS=24
LEASE_TERM_FORCE_REFRESH=false

# SharePoint Lease-Term Lists (GUID or full list URL)
LEASE_TERM_SET_LIST_ID=<optional-guid>
LEASE_TERM_SET_LIST_URL=<optional-list-url>
LEASE_TERMS_LIST_ID=<optional-guid>
LEASE_TERMS_LIST_URL=<optional-list-url>
LEASE_TERM_EVIDENCE_LIST_ID=<optional-guid>
LEASE_TERM_EVIDENCE_LIST_URL=<optional-list-url>

# Azure AD Authentication
SHAREPOINT_CLIENT_ID=03cbb033-c84b-4f5e-a348-ddf5cca87fff
SHAREPOINT_TENANT_ID=ea0cd29c-45e6-4ad1-94ff-2e9f36fb84b5
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=<secret>

# SharePoint Connection
SHAREPOINT_SITE_URL=https://peakcampus.sharepoint.com/sites/BaseCampApps

# SharePoint Performance Tuning
ASYNC_AUDIT_RESULTS_WRITE=true   # Background AuditRuns detail writes (default)
ASYNC_RUN_DISPLAY_SNAPSHOTS=true # Background RunDisplaySnapshots writes (default)
ASYNC_SNAPSHOT_VALIDATION=true   # Background snapshot validation (default)
SHAREPOINT_BATCH_SIZE_AUDITRUNS=10  # Override batch size for AuditRuns (default: 10)
SHAREPOINT_BATCH_SIZE_SNAPSHOTS=20  # Override batch size for snapshots (default: 20)

# Authentication Mode
REQUIRE_AUTH=false  # Development
REQUIRE_AUTH=true   # Production

# Activity Logging
ENABLE_SHAREPOINT_LOGGING=true
SHAREPOINT_LIST_NAME=Innovation Use Log
```

**Development Overrides**:
```bash
LOCAL_DEV_USER_NAME=Sarah VanOrder
LOCAL_DEV_USER_EMAIL=svanorder@peakmade.com
```

### Audit Exclusion Config Files

The audit mapping layer supports JSON-driven exclusions so updates can be made without editing Python source.

- `api_posted_ar_codes.json`
   - Purpose: AR code IDs excluded from AR and Scheduled source audits.
   - Env override: `API_POSTED_AR_CODES_PATH`
   - Supported JSON:
      - `[155023, 154776, ...]`
      - `{"api_posted_ar_codes": [155023, 154776, ...]}`

- `resident_profile_exclusions.json`
   - Purpose: Resident profile names and lease IDs excluded from AR and Scheduled source audits.
   - Env override: `RESIDENT_PROFILE_EXCLUSIONS_PATH`
   - Match behavior: case-insensitive, trimmed, and whitespace-normalized.
   - Scope behavior: when a name is matched, exclusions are expanded to rows sharing `CUSTOMER_ID`, `LEASE_INTERVAL_ID`, or `LEASE_ID` so linked payment rows with blank/non-resident names are also removed.
   - Debug verification: logs `[RESIDENT EXCLUSIONS] Expanded exclusion by identifier columns; ...` with per-column added counts and sample keys.
   - Lease ID behavior: rows with `LEASE_ID` matching configured exclusions are removed from AR and Scheduled source datasets before reconciliation.
   - UI management: `/settings` supports append-only additions for names and lease IDs; current exclusion lists are shown read-only to prevent accidental removals.
   - Supported JSON:
      - `["Resident Name A", "Resident Name B"]`
      - `{"excluded_resident_profile_names": ["Resident Name A", "Resident Name B"]}`
      - `{"excluded_resident_profile_names": ["Resident Name A"], "excluded_lease_ids": [14897278]}`

Default behavior for both configs is safe fallback to empty exclusions when files are missing or invalid.

### Configuration Classes (config.py)

```python
@dataclass
class StorageConfig:
    use_sharepoint_storage: bool
    sharepoint_library_name: str

@dataclass
class AuthConfig:
    require_auth: bool
    enable_sharepoint_logging: bool
    sharepoint_site_url: str

@dataclass
class ReconciliationConfig:
    amount_tolerance: float = 0.0
    status_matched: str = "MATCHED"
    status_scheduled_not_billed: str = "SCHEDULED_NOT_BILLED"
    # ... etc
```

---

## Deployment

### Azure App Service Deployment

1. **GitHub Integration**
   - Repository: https://github.com/PeakMade/LeaseFileAudit
   - Branch: `main`
   - Auto-deploy on push enabled

2. **App Service Configuration**
   - Name: `leasefileaudit`
   - Region: East US
   - Runtime: Python 3.11
   - OS: Linux
   - Pricing: B1 Basic (or higher)

3. **Environment Variables** (set in Azure Portal)
   ```
   USE_SHAREPOINT_STORAGE=true
   SHAREPOINT_LIBRARY_NAME=LeaseFileAudit Runs
   SHAREPOINT_SITE_URL=https://peakcampus.sharepoint.com/sites/BaseCampApps
   REQUIRE_AUTH=true
   ENABLE_SHAREPOINT_LOGGING=true
   ... (full list in .env file, copy to Azure)
   ```

4. **Authentication Setup**
   - Enable App Service Authentication
   - Provider: Microsoft (Azure AD)
   - Tenant: PeakMade tenant
   - Client ID: (from app registration)
   - Redirect URL: `https://leasefileaudit.azurewebsites.net/.auth/login/aad/callback`

5. **Startup Command** (optional)
   ```bash
   gunicorn --bind=0.0.0.0:8000 --timeout 600 app:app
   ```

### Local Development Setup

1. **Clone Repository**
   ```bash
   git clone https://github.com/PeakMade/LeaseFileAudit.git
   cd LeaseFileAudit
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\Activate.ps1  # Windows
   source .venv/bin/activate   # Mac/Linux
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure .env File**
   - Copy sample or get from team
   - Set `REQUIRE_AUTH=false` for local
   - Set `USE_SHAREPOINT_STORAGE=true` (if want SharePoint access)
   - Add Azure credentials

5. **Run Application**
   ```bash
   python run.py
   ```
   - Open: http://localhost:8080

---

## Development Workflow

### ⚠️ DOCUMENTATION-FIRST RULE

**CRITICAL**: Before committing ANY code change, you MUST update this master documentation if your change affects:
- Data flow or reconciliation logic
- Features, functionality, or configuration
- SharePoint schema, environment variables, or deployment
- Project structure or API endpoints

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for detailed guidelines.

### Making Code Changes

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/description
   ```

2. **Make Changes**
   - Edit files
   - Test locally with `python run.py`

3. **Update Documentation** (BEFORE committing!)
   ```bash
   # Open this file and update relevant sections
   code MASTER_DOCUMENTATION.md
   
   # Update change log at bottom
   # Add troubleshooting tips if fixing bugs
   # Update configuration examples if adding env vars
   ```

4. **Commit Code + Documentation Together**
   ```bash
   git add .
   git add MASTER_DOCUMENTATION.md  # Include docs!
   git commit -m "feat: Description of changes

   - Implemented feature X
   - Updated MASTER_DOCUMENTATION.md:
     - Section Y (added example)
     - Section Z (updated config)
   "
   git push origin feature/description
   ```

5. **Merge to Main** (triggers auto-deploy)
   ```bash
   git checkout main
   git merge feature/description
   git push origin main
   ```

6. **Monitor Deployment**
   - Azure Portal → App Service → Deployment Center
   - Check logs for errors

### Setup Documentation Reminder Hook

To get automatic reminders to update documentation:

```powershell
# Run once after cloning repository
.\setup-hooks.ps1
```

This installs a pre-commit hook that will prompt you to update MASTER_DOCUMENTATION.md when committing code changes.

### Testing Workflow

1. **Upload Test File**
   - Use sample Excel with AR Transactions + Scheduled Charges
   - Verify sheets auto-detected

2. **Check Console Logs**
   ```
   [MAPPING DEBUG] Processing source: ar_transactions
   [AR FILTER DEBUG] Total AR transactions: 5000
   [RECONCILIATION STATS] Primary matches: 4500
   [METRICS] Found 145 resolved exception months
   ```

3. **Verify Results**
   - Portfolio shows correct KPIs
   - Exceptions display properly
   - SharePoint lists updated

### Debugging Tips

**Issue: $0 values everywhere**
- Check: `USE_SHAREPOINT_STORAGE` in environment
- Check: SharePoint credentials valid
- Check: Error in try/catch fallback (line ~207 in views.py)

**Issue: Too many exceptions showing**
- Check: SharePoint Exception Months list accessible
- Check: Resolved exceptions being filtered
- Watch logs: `[METRICS] Found X resolved exception months`

**Issue: Matching not working**
- Check: Date formats (should be datetime, not string)
- Check: AUDIT_MONTH normalized to first of month
- Check: AR_CODE_ID data types match (int vs string)

---

## Common Scenarios & Troubleshooting

### Scenario 1: Adding a New Data Field

**Example**: Add "Unit Number" to lease view

1. **Add to Canonical Fields**
   ```python
   # audit_engine/canonical_fields.py
   class CanonicalField(Enum):
       # ... existing fields
       UNIT_NUMBER = "UNIT_NUMBER"
   ```

2. **Add to Source Mapping**
   ```python
   # audit_engine/mappings.py
   class ARSourceColumns:
       UNIT_NUMBER = "UNIT_NUMBER"  # Raw column name
   
   AR_TRANSACTIONS_MAPPING = SourceMapping(
       column_transforms=[
           # ... existing
           ColumnTransform(ARSourceColumns.UNIT_NUMBER, CanonicalField.UNIT_NUMBER),
       ]
   )
   ```

3. **Use in Views**
   ```python
   # web/views.py
   unit_number = row[CanonicalField.UNIT_NUMBER.value]
   ```

4. **Display in Template**
   ```html
   <!-- templates/lease.html -->
   <td>{{ exception.unit_number }}</td>
   ```

### Scenario 2: Changing Matching Logic

**Example**: Adjust tolerance for amount matching

1. **Update Config**
   ```python
   # config.py
   @dataclass
   class ReconciliationConfig:
       amount_tolerance: float = 0.01  # Allow $0.01 variance
   ```

2. **Modify Reconciliation**
   ```python
   # audit_engine/reconcile.py
   def reconcile_buckets(...):
       # ... existing code
       
       # UPDATE STATUS LOGIC:
       if abs(variance) <= config.amount_tolerance:
           status = config.status_matched  # Now tolerates $0.01
       elif variance < -config.amount_tolerance:
           status = config.status_scheduled_not_billed
       # ... etc
   ```

3. **Test Impact**
   - Run audit with sample data
   - Verify fewer AMOUNT_MISMATCH exceptions

### Scenario 3: Adding New Exception Resolution Action

**Example**: Add "Write Off" action type

1. **Update SharePoint List**
   - Go to Exception Months list settings
   - Edit "ActionType" column
   - Add choice: "write_off"

2. **Update Frontend**
   ```html
   <!-- templates/lease.html -->
   <select name="action_type">
       <!-- ... existing options -->
       <option value="write_off">Write Off</option>
   </select>
   ```

3. **Add Business Logic**
   ```python
   # web/views.py
   @bp.route('/api/exception-months', methods=['POST'])
   def upsert_exception_month():
       payload = request.get_json()
       
       if payload['action_type'] == 'write_off':
           # Special handling for write-offs
           payload['requires_approval'] = True
   ```

### Scenario 4: Performance Optimization

**Problem**: Portfolio page slow with 100+ runs

**Solution 1**: Use SharePoint Metrics List (already implemented)
```python
# Fast path (current implementation)
all_metrics = storage.load_all_metrics_from_sharepoint_list()
# Returns pre-calculated metrics, no CSV parsing
```

**Solution 2**: Add pagination
```python
# web/views.py
runs_per_page = 20
page = request.args.get('page', 1, type=int)
paginated_runs = storage.list_runs(limit=runs_per_page, offset=(page-1)*runs_per_page)
```

**Solution 3**: Cache results
```python
from functools import lru_cache

@lru_cache(maxsize=128)
def calculate_cumulative_metrics(run_id):
    # Cached for same run_id
```

**Current implementation notes (2026-02):**
- Flask-Caching is used in `web/views.py` for:
   - `cached_load_run(run_id)`
   - `cached_load_property_exception_months(run_id, property_id)`
   - `get_available_runs()`
   - `calculate_cumulative_metrics()`
- `get_available_runs()` is intentionally limited to the most recent 50 runs to avoid expensive per-run metadata fetches.
- Cache clear/invalidation is hardened to avoid request failure if Flask debug reloader creates cache instance mapping mismatch (`cache.clear()` falls back to extension-backend clear and logs warning).
- Preferred local startup path is `python run.py` to keep factory/import behavior consistent.
- SharePoint run-list loading now handles missing `run_meta.json` gracefully by using folder fallback metadata (run ID + createdDateTime) and logging missing files at debug level for 404 responses.
- `StorageService` now uses process-level caches for SharePoint `site_id`, library `drive_id`, and list IDs across instances to reduce repeated Graph discovery calls.
- Repetitive SharePoint discovery messages (library/list ID resolution) are logged at debug level to reduce noise in normal logs.
- Property page run selector is lazy-loaded via API (`/api/runs`); full run list is fetched only when the selector is opened, not on initial property page render.

**Undercharge includes resolved exceptions (Troubleshooting):**
- Symptom: Resolved month counts look correct, but Current Undercharge is still too high.
- Root cause: Type mismatch in resolved-key tuple comparison (e.g., AR code as string from SharePoint vs numeric in bucket rows).
- Fix: Normalize comparison keys before set membership checks:
   - Property ID -> `int`
   - Lease Interval ID -> `int`
   - AR Code ID -> `str`
   - Audit Month -> `YYYY-MM-DD`
- The normalization helpers live in `web/views.py` and are used in portfolio, property, and KPI filtering paths.

**Lease summary banner nets under/over amounts (Troubleshooting):**
- Symptom: On the lease page header, `Total Undercharge` is reduced by overcharges (for example, undercharge 1760 and overcharge 50 appears as a net-style result).
- Expected behavior: Header totals above AR Code Details should remain independent:
   - `Total Undercharge` = sum of unresolved month-level `max(0, expected_total - actual_total)`
   - `Total Overcharge` = sum of unresolved month-level `max(0, actual_total - expected_total)`
- Scope note: This fix is applied in `lease_view()` summary totals only; drawer rendering/behavior is intentionally unchanged.
- Resolved-month exclusion behavior remains unchanged (`month_status == 'Resolved'` is still excluded from current totals).

**Property view shows fewer leases than expected (Troubleshooting):**
- Symptom: After uploading a property, only 2-5 leases appear in the property view, but the undercharge/overcharge totals reflect all 40+ leases.
- Root cause: SharePoint batch write failures (504 Gateway Timeout errors) causing only partial bucket_results to persist. When background write completes, only the successfully written rows are available for display.
- Diagnosis:
  1. Check terminal logs for `[STORAGE] Batch X/Y throttled` or `504` errors
  2. Compare `bucket_rows` count in dispatch log vs rows loaded from SharePoint
  3. Example: `Dispatched background AuditRuns write: bucket_rows=1603` but `Loaded audit results: rows=80`
- Solution (updated 2026-03-23):
   - Keep async writes enabled for faster responses and lower timeout risk.
   - `load_bucket_results()` and `load_findings()` now automatically fall back to CSV when list row counts are lower than CSV row counts.
   - Property view lease status now falls back to unresolved bucket counts when lease snapshots are not yet available.
   - Read-path source logging now emits explicit tags so fallback usage is visible in terminal/log stream:
     - `[READ SOURCE][bucket_results] source=sharepoint_list|csv|none ...`
     - `[READ SOURCE][findings] source=sharepoint_list|csv|none ...`
     - `[CSV FALLBACK][bucket_results|findings] ...` when CSV is selected due to partial list data or list read unavailability.
   - Optional fallback mode: set `ASYNC_AUDIT_RESULTS_WRITE=false` and/or `ASYNC_RUN_DISPLAY_SNAPSHOTS=false` if you need fully blocking writes.
- Prevention:
  - Ensure AuditRuns list has proper indexes on RunId, ResultType, PropertyId
  - Lower batch size further if throttling persists: set `SHAREPOINT_BATCH_SIZE_AUDITRUNS=5`
  - Monitor batch write completion: `[STORAGE] ✅ Background AuditRuns write finished`
  - If using async writes, wait for completion log before viewing property

**Properties disappear from portfolio after sequential uploads (Troubleshooting):**
- Symptom: Upload property A → appears in portfolio. Upload property B → property A disappears from portfolio.
- Root cause: SharePoint eventual consistency. When property B is uploaded, it queries for the latest run to use as baseline, but SharePoint's folder list hasn't updated yet to show property A's run.
- Diagnosis:
  1. Check logs for `[PROPERTY SCOPE] Using baseline from list_runs: run_xxx`
  2. If all properties are missing, check `[SNAPSHOT][PORTFOLIO] Aggregated X properties from latest runs`
- Solution (implemented 2026-03-10):
  - Session-based baseline tracking: Each property upload stores `run_id` in Flask session
  - Next property upload checks session first: `[PROPERTY SCOPE] Using last saved run from session as baseline`
  - Falls back to SharePoint list_runs() only if session is empty
  - Portfolio dashboard now aggregates latest snapshot for each property across ALL runs (not limited to single run)
- Technical details:
  - `session['last_saved_run_id']` set after each save_run()
  - Property overlay uses session baseline immediately (no SharePoint dependency)
  - Portfolio view calls `load_latest_property_snapshots_across_runs()` which queries all property snapshots and groups by property_id to get most recent data
  - Each property row links to its specific run_id for drill-down

**Property names missing or showing as "Property XXXXX" (Troubleshooting):**
- Symptom: Portfolio or property view shows generic property names instead of actual names from picklist.
- Root cause: Older runs saved without property_name_map in metadata, or baseline run missing property names for all properties.
- Diagnosis:
  1. Check `[OVERLAY] Baseline property_name_map:` log - should show all baseline properties
  2. Check `[OVERLAY] Merged property_name_map:` - should include baseline + newly uploaded property
  3. Check `[STORAGE] Backfilled property name from data:` - indicates names extracted from DataFrames
- Solution (implemented 2026-03-10):
  - Property names saved to metadata JSON: `property_name_map: {property_id: property_name}`
  - Property names merged during overlay: baseline map + scoped map (scoped overrides on conflict)
  - Property names backfilled from actual_detail and expected_detail DataFrames when loading runs
  - Property name priority in snapshots: picklist → API response → DataFrame backfill → "Property {id}"
- Prevention:
  - Ensure Properties_0 SharePoint list has PROPERTY_NAME or PropertyName column populated
  - API uploads automatically fetch property names from picklist before creating runs

### Scenario 5: Audit Period Filtering Not Working

**Problem**: User selects "January 2025" but sees all months

**Check**:
1. Filter applied during upload?
   ```python
   # web/views.py → upload()
   audit_year = request.form.get('audit_year')  # ✅
   audit_month = request.form.get('audit_month')  # ✅
   results = execute_audit_run(..., audit_year, audit_month)  # ✅
   ```

2. Filter applied to both datasets?
   ```python
   # web/views.py → execute_audit_run()
   expected_detail = filter_by_audit_period(expected_detail, audit_year, audit_month)
   actual_detail = filter_by_audit_period(actual_detail, audit_year, audit_month)
   ```

3. AUDIT_MONTH column exists?
   ```python
   # Should see in logs:
   [FILTER] Filtered to month 1: X rows remaining
   ```

---

## Quick Reference

### Important File → Purpose Mapping

| File | Purpose |
|------|---------|
| `web/views.py` | All route handlers, KPI calculations, exception filtering |
| `audit_engine/reconcile.py` | Core matching algorithm |
| `audit_engine/mappings.py` | Source data transformations, ONLY place with raw column names |
| `audit_engine/canonical_fields.py` | Field name standardization |
| `storage/service.py` | SharePoint + local file operations |
| `audit_engine/lease_term_rules.py` | Default lease term → AR code/frequency mapping rules |
| `config.py` | All configuration, read from environment variables |
| `templates/lease.html` | Exception detail view with resolution UI |

### Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `calculate_cumulative_metrics()` | `web/views.py` | Portfolio KPIs (current + historical) |
| `reconcile_buckets()` | `audit_engine/reconcile.py` | Tier 1 matching algorithm |
| `apply_source_mapping()` | `audit_engine/mappings.py` | Convert raw → canonical |
| `load_exception_months_from_sharepoint_list()` | `storage/service.py` | Get resolution status |
| `execute_audit_run()` | `web/views.py` | Full audit pipeline |
| `refresh_lease_terms_for_lease_interval()` | `audit_engine/entrata_lease_terms.py` | Lease-term incremental refresh + fail-open cached fallback |
| `build_lease_expectation_overlay()` | `audit_engine/entrata_lease_terms.py` | Map lease terms onto AR-code drawer rows |

### Data Flow Diagram

```
Excel Upload
    ↓
[Raw DataFrames]
    ↓
apply_source_mapping()  ← mappings.py
    ↓
[Canonical DataFrames]
    ↓
normalize_*()  ← normalize.py
    ↓
[Validated Data]
    ↓
expand_scheduled_to_months()  ← expand.py
    ↓
[Monthly Buckets (Expected)]
    ↓
reconcile_buckets()  ← reconcile.py
    ↓               ↖
[Bucket Results]    [Monthly Buckets (Actual)]
    ↓
save_run()  ← storage/service.py
    ↓
[SharePoint Library + Lists]
    ↓
portfolio() / property_view() / lease_view()  ← web/views.py
    ↓
[HTML Pages via Jinja2]
```

---

## Additional Resources

### Related Documentation
- **Entrata API**: https://www.entrata.com/api
- **Microsoft Graph API (SharePoint)**: https://learn.microsoft.com/en-us/graph/api/resources/sharepoint
- **Pandas Documentation**: https://pandas.pydata.org/docs/
- **Flask Documentation**: https://flask.palletsprojects.com/

### Internal Contacts
- **Product Owner**: Sarah VanOrder (svanorder@peakmade.com)
- **Development Team**: PeakMade IT
- **Support**: BaseCamp Apps site in SharePoint

### Change Log
- **2026-03-24**: Added explicit SharePoint batch-configuration diagnostics in `storage/service.py`: startup log prints configured `SHAREPOINT_BATCH_SIZE_AUDITRUNS`/`SHAREPOINT_BATCH_SIZE_SNAPSHOTS`/`SHAREPOINT_BATCH_SIZE`, and each batched list write now logs `[STORAGE][BATCH CONFIG]` with context, row count, effective batch size, and source (`env` vs default)
- **2026-03-23**: Added portfolio route read-source summary logging in `web/views.py` so each portfolio request emits `[READ SUMMARY][PORTFOLIO_VIEW] ... snapshot_source=latest_property_snapshots_across_runs|run_scoped_snapshots ...` for quick source-path visibility alongside property/lease summaries
- **2026-03-23**: Added route-level read-source summary logging in `web/views.py` so each property/lease page request emits one compact line indicating data source usage for display loads: `[READ SUMMARY][PROPERTY_VIEW] ... bucket_source=... findings_source=...` and `[READ SUMMARY][LEASE_VIEW] ... bucket_source=...`; also preserved DataFrame source metadata through bucket normalization helper for reliable summaries
- **2026-03-23**: Added explicit read-path diagnostics in `storage/service.py` for `load_bucket_results()` and `load_findings()` to make CSV fallback visibility obvious in terminal/log stream: `[READ SOURCE][bucket_results|findings] source=sharepoint_list|csv|none` plus `[CSV FALLBACK][...]` reason logging for partial-list and list-unavailable fallback cases
- **2026-03-23**: Reduced upload timeout risk by moving RunDisplaySnapshots writes to async by default in `storage/service.py` via `ASYNC_RUN_DISPLAY_SNAPSHOTS` (default `true`); added background wrapper `_write_run_display_snapshots_async()` with post-write validation support; retained UI correctness during async lag by adding CSV completeness fallback when list rows are partial in `load_bucket_results()` and `load_findings()` and by updating `property_view` lease status fallback to use unresolved bucket counts when lease snapshots are not yet available
- **2026-03-20**: Switched Entrata lease-term document handling to memory-first extraction in `audit_engine/entrata_lease_terms.py`: selected resident packet/addenda are parsed from in-memory PDF bytes (no required local file path dependency), merged packet+addenda is built in-memory via PyMuPDF, and documents are uploaded to SharePoint Document Library path `Entrata leases/<property_id>/...`; added optional debug/local persistence flag `SAVE_LOCAL_ENTRATA_PDFS` (default `false`) while keeping local path parsing fallback compatibility for explicit path inputs
- **2026-03-16**: Added status badge column to property view lease table showing per-lease status (Passed/Open/Resolved) with color-coded Bootstrap badges; status calculation logic in `web/views.py` compares static exception count from snapshots with unresolved exception count from filtered lease groups to determine resolution progress; status label shows "Open (X/Y resolved)" format for partially resolved leases
- **2026-03-16**: Fixed SharePoint column name reference from `TotalLeaseIntervalsStatic` to `TotalLeaseIntervalStatic` throughout `storage/service.py` to match actual SharePoint list column naming; corrected field candidate mapping and all snapshot read operations to properly display lease interval counts in portfolio view
- **2026-03-16**: Removed "Total Lease Intervals" column from portfolio page table in `templates/portfolio.html` to simplify dashboard layout; portfolio now displays Property Name, Property ID, Exceptions, Undercharge, Overcharge, Total Variance, and Details columns only
- **2026-03-16**: Added responsive CSS breakpoints in `templates/base.html` for action buttons container with three breakpoints (@1200px moves buttons closer, @992px drops buttons below header as static element, @768px stacks buttons vertically) to prevent button overlap with lease header content on smaller screens
- **2026-03-16**: Improved lease detail header layout in `templates/lease.html` by moving lease ID and interval ID from inline parentheses to separate line below customer name with increased font size (0.85rem) and muted styling for better mobile readability
- **2026-03-16**: Created comprehensive documentation files: `ENTRATA_API_GUIDE.md` (beginner's guide to Entrata API integration with request/response examples and troubleshooting), `SINGLE_LEASE_AUDIT_FLOW.md` (detailed technical flow from API call through SharePoint save), and `PROPERTY_LEASE_CONFIG_README.md` (property-specific lease term configuration guide)
- **2026-03-10**: Made SharePoint AuditRuns writes synchronous by default to prevent partial data display: changed `ASYNC_AUDIT_RESULTS_WRITE` default from `true` to `false` so property uploads now wait for complete SharePoint list write before returning; ensures all bucket results and findings are available immediately when viewing property audit; trade-off is uploads take ~2 minutes instead of 20 seconds but eliminates issue where only partial rows (e.g., 60 of 2,731) were visible due to incomplete background writes; added `ASYNC_AUDIT_RESULTS_WRITE` configuration option for users who prefer faster uploads and can tolerate temporary incomplete data
- **2026-03-10**: Enhanced SharePoint batch write resilience with exponential backoff retry logic: reduced default AuditRuns batch size from 20 to 10 items to reduce 504 Gateway Timeout errors; added automatic retry for 429/503/504 throttling with exponential backoff (items: 0.5s→1s→2s, batches: 1s→2s, max 3 attempts); added 0.5s delays between batches to reduce API pressure; failed batches automatically fall back to individual item posts with retry; configurable via `SHAREPOINT_BATCH_SIZE_AUDITRUNS` and `SHAREPOINT_BATCH_SIZE_SNAPSHOTS` environment variables
- **2026-03-10**: Simplified property upload architecture by removing baseline merge/overlay logic: each property upload now stores ONLY that property's data in an independent immutable run (no merging with previous runs); portfolio dashboard aggregates latest snapshot per property across ALL runs using new `load_latest_property_snapshots_across_runs()` function which queries RunDisplaySnapshots with property-scope filter, groups by PropertyId, and selects max RunId per property; eliminates duplicate data storage, session dependencies, and SharePoint eventual consistency issues; each property row in portfolio links to its specific run_id for drill-down
- **2026-03-10**: Integrated SharePoint Properties_0 picklist as authoritative property name source: added property name lookup during API uploads and snapshot building with priority chain (SharePoint picklist → API response → DataFrame backfill → "Property {id}" fallback); property_name_map now saved to metadata JSON and backfilled from actual_detail/expected_detail DataFrames when loading older runs; added `_extract_property_names_from_detail()` helper to populate missing property names from reconciliation data; fixes issue where properties showed as "Property XXXXX" instead of actual names
- **2026-03-09**: Tightened `AMENITY_PREMIUM` extraction configuration in `lease_term_extraction_config.json` to only evaluate Exclusive Bedspace Addendum focus context (`source_order: ["focus"]`) and ignore floorplan multi-option/potential-fee listing language (for example `Floorplan Rate Addendum`, `premium features below`, `potential rent range`) so only signed addendum-specific premium amounts are extracted
- **2026-03-09**: Centralized term-specific lease extraction heuristics into JSON configuration: added `lease_term_extraction_config.json` as the single source for `BASE_RENT`, `APPLICATION_FEE`, `ADMIN_FEE`, `AMENITY_PREMIUM`, `PARKING`, and `PET_RENT` extraction patterns/tokens/filters; added loader/access helpers in `audit_engine/lease_term_extraction_rules.py`; updated `audit_engine/entrata_lease_terms.py` to consume config-driven page hints, anchor tokens, regex patterns, exclusion signals, monthly/one-time context signals, and admin application-leak guard patterns instead of hardcoded term regex lists
- **2026-03-06**: Fixed API parking recurrence classification and property-name rendering across views: `audit_engine/api_ingest.py` now treats non-date explicit end markers (for example, `End During Move-Out`) as recurring lease-end fallbacks when charges are not one-time, preventing monthly charges from collapsing into one-time behavior; `web/views.py` now builds a normalized run-scoped property name lookup (metadata + snapshots + actual/expected detail) and uses it in portfolio/property/lease routes so property names reliably render instead of `Property <id>` fallbacks; `templates/property.html` and `templates/lease.html` page titles now display resolved property names when available
- **2026-03-05**: Externalized lease-term mapping configuration and wired AR-code human-readable labels: added editable rules file `lease_term_mapping_config.json` (loaded by `audit_engine/lease_term_rules.py` via `get_term_to_ar_code_rules()` with env override `LEASE_TERM_RULES_CONFIG_PATH` and safe fallback defaults); added AR code name reference ingestion from `ar_code_name_usage_map.json` for display formatting; updated `audit_engine/entrata_lease_terms.py` to use config-driven primary AR code defaults for extracted terms and to emit human-readable AR labels in lease expectation overlay messages; updated `audit_engine/mappings.py` to apply AR name enrichment from the AR code map and log unknown AR codes not present in the allowlist
- **2026-03-05**: Reduced false exception noise and improved write/read-path performance: `audit_engine/reconcile.py` now carries bucket-level reversal/deleted activity flags and classifies zero-net reversed/deleted buckets as `matched` (suppressing false `scheduled_not_billed` exceptions for reversal pairs); `storage/service.py` now supports context-aware Graph `$batch` sizing via `SHAREPOINT_BATCH_SIZE_AUDITRUNS`, `SHAREPOINT_BATCH_SIZE_SNAPSHOTS`, and fallback `SHAREPOINT_BATCH_SIZE` (bounded to 1–20); `web/views.py` now reduces property-scoped overlay baseline discovery from `storage.list_runs(limit=50)` to `limit=2` in both upload flows
- **2026-03-05**: Improved AR↔scheduled primary linkage and safe prefilter controls: `audit_engine/api_ingest.py` now persists raw Entrata scheduled charge IDs (`SCHEDULED_CHARGE_ID`) alongside synthetic scheduled row IDs; `audit_engine/mappings.py`, `audit_engine/normalize.py`, `audit_engine/expand.py`, and `audit_engine/reconcile.py` now carry and prioritize raw scheduled charge IDs for primary matching (`SCHEDULED_CHARGE_ID_LINK` → `SCHEDULED_CHARGE_ID`, with fallback to synthetic keys); `storage/service.py` now includes async metrics/snapshot-validation execution and storage stage timers; `web/views.py` adds deterministic raw-date normalization + optional guarded early audit-window prefiltering (`EARLY_AUDIT_WINDOW_PREFILTER`) with parity logging
- **2026-03-05**: Updated API ingestion and view resolution paths to improve tenant identity accuracy and scheduled-charge filtering: `audit_engine/api_ingest.py` now prefers tenant over guarantor while persisting guarantor separately, excludes `leaseIntervalStatus=Cancelled` and `postedThrough="Deleted - Never Posted"` scheduled rows, supports accounting-style negative amounts (e.g., `($70.00)`), and treats non-monthly no-end-date charges as one-time; `audit_engine/mappings.py` now applies robust API-posted AR code exclusion across mixed numeric/string dtypes; `web/views.py` now prefers lease-details/expected tenant names over AR names in both lease and property views for consistent resident display
- **2026-03-05**: Added separate property-level API audit upload workflow in `web/views.py` + `templates/upload.html` using new API adapter `audit_engine/api_ingest.py`; flow fetches `getLeaseDetails`/`getLeaseArTransactions` from Entrata endpoints, stages equivalent AR/scheduled raw datasets, and executes the existing reconciliation pipeline without altering the Excel upload path
- **2026-03-05**: Hardened lease-term fee extraction in `audit_engine/entrata_lease_terms.py` to prevent false positives by excluding parking violation/fine/warning context from monthly parking detection and by tightening admin-fee candidate selection to ignore utility billing-cap language (e.g., `for billing`, `not to exceed`)
- **2026-03-03**: Fixed lease-view overlay skip caused by mixed offset-naive/offset-aware datetime subtraction in `audit_engine/entrata_lease_terms.py` by normalizing TTL age comparison timestamps to UTC-aware values before subtraction
- **2026-03-03**: Updated lease-term refresh in `audit_engine/entrata_lease_terms.py` and `web/views.py` to pass `run_id` into refresh and bypass TTL short-circuit for new audit runs, so first lease open in a new run always re-checks resident doc list while still reusing cached terms when `DocListFingerprint` is unchanged
- **2026-03-03**: Fixed persisted lease-term display on unchanged document lists by relaxing SharePoint `LeaseTerms` query filtering in `storage/service.py` (load by `LeaseKey`, then normalize/filter `IsActive` in Python), avoiding Graph boolean-filter mismatches that could return empty term sets on subsequent lease views
- **2026-03-03**: Improved base-rent extraction in `audit_engine/entrata_lease_terms.py` for Bibby-style lease formats by increasing monthly/installment context scoring near rent anchors, adding stronger penalties for total-term rent context, and applying safe monthly-inference override when anchor-selected values appear to be term totals
- **2026-03-03**: Fixed `LeaseTerms` unique-constraint collisions by scoping extracted `term_key` values with lease key (`PROPERTY_ID:LEASE_INTERVAL_ID`) in `audit_engine/entrata_lease_terms.py` before SharePoint writes, preventing cross-lease duplicates for keys like `BASE_RENT:RENT:::`
- **2026-03-03**: Updated lease-term refresh gating in `audit_engine/entrata_lease_terms.py` to use `DocListFingerprint` as the primary no-download reuse check (after doc-list fetch), so unchanged resident document lists return cached `LeaseTerms` from SharePoint without re-downloading packets/addenda; selected-doc `FingerprintHash` remains as secondary parse-input guard
- **2026-03-03**: Added full-document-list fingerprint persistence for `LeaseTermSet` by writing/reading `DocListFingerprint` in `storage/service.py` and computing/storing it during lease-term refresh in `audit_engine/entrata_lease_terms.py`, so lease term checks now retain full doc-list change state alongside selected-doc fingerprints
- **2026-03-03**: Optimized SharePoint write path in `storage/service.py` by batching `RunDisplaySnapshots` inserts via Graph `$batch`, removing redundant delete-before-insert loops for run-unique writes, and dispatching heavy `AuditRuns` detail row persistence (`bucket_result`/`finding`) to background execution after core snapshot persistence so upload redirects faster while detailed rows continue syncing
- **2026-03-03**: Fixed Flask-Caching startup-path instability by using a shared cache extension instance in `extensions.py` and importing it from both `app.py` and `web/views.py`, preventing `AttributeError: 'Cache' object has no attribute 'app'` when launching with different entrypoints
- **2026-03-03**: Optimized portfolio first-load path in `web/views.py` by removing full run-list fetch as a prerequisite for `GET /portfolio/<run_id>`, adding lightweight `get_latest_run()` lookup when `run_id` is omitted, and restricting run-list cache invalidation to new-run creation
- **2026-03-03**: Added lazy run selector loading to `templates/portfolio.html` via asynchronous `/api/runs` fetch after first paint so portfolio data can render immediately without blocking on run-history retrieval
- **2026-03-03**: Added upload timing budget instrumentation in `web/views.py` with stage-level metrics (`file_save`, `execute`, `save_run`, etc.) and end-to-end post-redirect timing (`[AUDIT TIMER][E2E]`) to include first destination page render after upload
- **2026-03-02**: Fixed optional lease date serialization in `storage/service.py` for `LeaseTerms` writes by sending `StartDate`/`EndDate` as null when missing (instead of empty strings), preventing Graph `400 badArgument` for DateTime fields on rows like `PARKING:PARK:::PARKING`
- **2026-03-02**: Hardened parking fee extraction in `audit_engine/entrata_lease_terms.py` to exclude NSF/returned-check rows from coordinate candidates and prioritize explicit monthly parking phrasing (e.g., "monthly per vehicle") so monthly parking amounts are selected over penalties/one-time fees
- **2026-03-02**: Updated lease packet selection to prefer docs whose `leaseIntervalStartDate` falls inside the active audit period window, with deterministic tie-breakers by recency and declared file size; wired lease view to pass derived period bounds into lease-term refresh
- **2026-03-02**: Added layout/coordinate-first base-rent extraction in `audit_engine/entrata_lease_terms.py` using PyMuPDF `words` anchors (`RENT AND CHARGES` / `RENT`), proximity scoring, repeated-amount inference fallback, and pdfplumber coordinate fallback with method-level diagnostic logging
- **2026-03-02**: Fixed SharePoint lease-term list writes in `storage/service.py` by sending `LeaseId` as text for `LeaseTermSet`, `LeaseTerms`, and `LeaseTermEvidence` payloads to match list column typing and prevent Graph `500 generalException` insert failures
- **2026-02-27**: Enhanced non-OCR PDF extraction in `audit_engine/entrata_lease_terms.py` with per-page multi-mode PyMuPDF selection (`text`/`blocks`/`words`/`dict`) and extraction-mode telemetry for problematic lease packets
- **2026-02-27**: Added clause-window fee multi-capture so a single clause can emit both `APPLICATION_FEE` and `ADMIN_FEE` with independently parsed dynamic amounts
- **2026-02-27**: Externalized default lease-term AR mapping rules to `audit_engine/lease_term_rules.py` and wired `audit_engine/entrata_lease_terms.py` to import shared defaults (`DEFAULT_TERM_TO_AR_CODE_RULES`)
- **2026-02-27**: Updated lease-term extraction to v2 behavior in `audit_engine/entrata_lease_terms.py` (primary/addenda-aware parsing, monthly/installment base-rent prioritization, expanded date normalization, application/admin/amenity term extraction, parking section-scored extraction)
- **2026-02-27**: Added richer lease-term extraction logs (`[LEASE TERMS]`) including mapped term rows and page-linked evidence snippets for troubleshooting
- **2026-02-26**: Added Entrata lease-term sidecar module (`audit_engine/entrata_lease_terms.py`) with document selection, optional PDF parsing, term mapping registry, AR overlay generation, and incremental fingerprint refresh pipeline
- **2026-02-26**: Added SharePoint normalized lease-term persistence in `storage/service.py` (`LeaseTermSet`, `LeaseTerms`, `LeaseTermEvidence`) including env-driven list ID/URL resolution and lease-key read/write methods
- **2026-02-26**: Updated lease view/UI integration in `web/views.py` and `templates/lease.html` to render lease expectations by AR code and lease-only expectation alerts without changing reconciliation status logic
- **2026-02-25**: Made portfolio route snapshot-only in `web/views.py` by loading KPIs/property rows from `RunDisplaySnapshots` without recomputing from run detail payloads
- **2026-02-25**: Added snapshot write-time validation and run-scoped snapshot loaders in `storage/service.py` to verify expected snapshot counts and improve load reliability
- **2026-02-25**: Expanded snapshot payload support with `PropertyNameStatic`, `TotalVarianceStatic`, and `TotalLeaseIntervalsStatic` fallback handling so UI display fields persist in snapshots
- **2026-02-25**: Added audit runtime timing logs (`[AUDIT TIMER]`) in upload flow and reduced tertiary reconciliation log noise to bucket-level summaries with unusual-case diagnostics
- **2026-02-25**: Fixed primary-match linkage by mapping AR `SCHEDULED_CHARGE_ID` to canonical `SCHEDULED_CHARGE_ID_LINK` and normalizing match IDs in `audit_engine/reconcile.py`
- **2026-02-24**: Refactored `execute_audit_run()` to perform true property-scoped reconciliation (`PROPERTY_ID` subsets) and aggregate per-property outputs into portfolio-level totals
- **2026-02-24**: Added run-scoped shared caching in `web/views.py` for `bucket_results`, `findings`, `actual_detail`, `expected_detail`, `metadata`, and run display snapshots to reduce repeat loads on back navigation
- **2026-02-24**: Added targeted run/property/lease cache invalidation hooks after uploads and exception status updates so shared cached data remains consistent across users
- **2026-02-24**: Restored full `_match_tertiary_date_mismatch()` execution path and tuple return in `audit_engine/reconcile.py` to fix upload failure (`cannot unpack non-iterable NoneType object`)
- **2026-02-24**: Expanded API-posted AR code exclusion list in `audit_engine/mappings.py` to include additional timed/external codes (`155030`, `155037`)
- **2026-02-23**: Added default Current Academic Year filtering (Aug through current month) when upload Year is left as "Current Academic Year"; optional month selection now applies after academic-year scoping
- **2026-02-23**: Temporarily disabled active lease interval filtering in AR and scheduled source row filters to include inactive lease-interval rows during reconciliation
- **2026-02-23**: Hardened scheduled flag filtering for mixed data types (`1`, `1.0`, `'1'`) and blank `DELETED_ON` handling to prevent unintended row exclusion
- **2026-02-23**: Fixed lease drawer monthly-details table alignment when expected/actual row counts differ so actual values no longer render under expected columns
- **2026-02-23**: Hardened SharePoint AuditRuns row payload ID conversion using safe int parsing to avoid per-row cast failures interrupting list persistence
- **2026-02-20**: Updated exception-month save/status API to use scoped AR status logic so status remains Open until all scoped exceptions for an AR code are resolved
- **2026-02-20**: Refined lease AR-code status logic to remain Open until all scoped exceptions for that lease interval AR code are resolved; status no longer flips to Resolved from cross-run historical state alone
- **2026-02-20**: Aligned lease drawer count/status presentation with scoped run logic (exclude previously resolved historical months from current audit count while preserving current-run resolution behavior)
- **2026-02-20**: Added app-level session lifecycle with SessionID generation, idle-timeout rollover, and Start/End Session correlation in SharePoint activity logs
- **2026-02-20**: Updated SharePoint activity logging to populate `SessionID` on all activity events (Start Session, Successful Audit, Failed Audit, End Session)
- **2026-02-20**: Fixed cross-run exception resolution month matching normalization so historical resolutions are applied before snapshot counts
- **2026-02-20**: Restored property dashboard resident-name population and aligned lease variance/count display logic with unresolved vs static snapshot behavior
- **2026-02-11**: Implemented documentation-first rule with pre-commit hooks and CONTRIBUTING.md
- **2026-02-11**: Fixed duplicate loop in resolved exception filtering, added comprehensive logging
- **2026-02-11**: Added comprehensive master documentation
- **2026-01-27**: Implemented month-level exception tracking
- **2026-01-21**: Added SharePoint storage integration
- **2025-12**: Initial build with three-tier reconciliation

---

## Glossary

- **AR Code**: Accounting receivable code (e.g., 155001 = Base Rent)
- **Bucket**: Grouping of transactions by property, lease, AR code, and month
- **Canonical Field**: Standardized field name used internally (vs raw source names)
- **Exception**: Variance between expected and actual (billing discrepancy)
- **Lease Interval**: A period within a lease (e.g., renewal period)
- **Reconciliation**: Process of matching expected vs actual charges
- **Scheduled Charge**: Expected billing setup in property management system
- **Variance**: Difference between actual and expected amounts (negative = undercharge)
- **SharePoint List**: Database table in SharePoint (like Excel table in cloud)
- **SharePoint Library**: File storage in SharePoint (like folder in cloud)

---

**Last Updated**: March 23, 2026  
**Version**: 1.0  
**Maintained By**: PeakMade Development Team
