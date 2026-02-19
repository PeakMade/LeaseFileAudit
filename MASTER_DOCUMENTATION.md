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
- **Portfolio Dashboard**: Real-time KPIs showing current and historical undercharge/overcharge
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
1. User uploads Excel file (AR Transactions + Scheduled Charges)
   ↓
2. Data Loading & Mapping (mappings.py)
   - Load raw Excel sheets
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

#### Step 1: Upload & Sheet Detection
- User selects Excel file via `/upload` route
- `io.load_excel_sources()` auto-detects sheets by keywords:
  - AR Transactions: "AR", "transactions", "posted"
  - Scheduled Charges: "scheduled", "charges"
- Loads raw DataFrames

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
- `GET /portfolio/<run_id>` - Portfolio KPI dashboard
- `GET /property/<property_id>/<run_id>` - Property exceptions grouped by lease
- `GET /lease/<run_id>/<property_id>/<lease_id>` - Detailed lease exceptions

**API Endpoints** (for AJAX):
- `POST /api/exception-months` - Update month status
- `GET /api/exception-months/<...>` - Get month statuses
- `GET /api/exception-months/ar-status/<...>` - Get calculated AR code status

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
- On load, app reads `AuditRuns` first and falls back to CSV files if list rows are unavailable.
- Existing CSV run artifacts remain as compatibility fallback.

**Indexing required for reliable filtered reads**:
- Index `RunId` (required)
- Index `ResultType` (required)
- Recommended additional indexes: `CompositeKey`, `PropertyId`, `LeaseIntervalId`, `ArCodeId`, `AuditMonth`

**ResultType column mapping**:
- `bucket_result` rows map to: `PROPERTY_ID`, `LEASE_INTERVAL_ID`, `AR_CODE_ID`, `AUDIT_MONTH`, `expected_total`, `actual_total`, `variance`, `status`, `match_rule`.
- `finding` rows map to: `finding_id`, `run_id`, `property_id`, `lease_interval_id`, `ar_code_id`, `audit_month`, `category`, `severity`, `title`, `description`, `expected_value`, `actual_value`, `variance`, `impact_amount`.

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

# Azure AD Authentication
SHAREPOINT_CLIENT_ID=03cbb033-c84b-4f5e-a348-ddf5cca87fff
SHAREPOINT_TENANT_ID=ea0cd29c-45e6-4ad1-94ff-2e9f36fb84b5
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=<secret>

# SharePoint Connection
SHAREPOINT_SITE_URL=https://peakcampus.sharepoint.com/sites/BaseCampApps

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

**Last Updated**: February 11, 2026  
**Version**: 1.0  
**Maintained By**: PeakMade Development Team
