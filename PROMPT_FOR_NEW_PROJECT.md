# Prompt for New LeaseFileAudit Project

Copy and paste this into your new chat:

---

**📋 PROMPT STATUS: FINALIZED - Ready for Implementation**

This prompt contains the **complete, final architecture** for rebuilding the LeaseFileAudit Flask application from scratch. All design decisions have been made:

- ✅ **Storage Architecture**: Single `AuditTransactionDetail` SharePoint list with `DetailType` column (EXPECTED/ACTUAL)
- ✅ **Write Strategy**: Per-property incremental writes with `AuditRunStatus` tracking
- ✅ **Three Audit Types**: Property, Bulk, Single Lease - all fully specified
- ✅ **SharePoint Lists**: 4 lists with complete schemas and indexes defined
- ✅ **Query Patterns**: Portfolio → Property → Lease drill-down fully documented
- ✅ **Exception Resolution**: 17 resolution categories with historical matching
- ✅ **Implementation Order**: 30-day build plan from Flask shell to production

**This is NOT a "help me design" document - this is a "here's exactly what to build" blueprint.**

---

I'm building a **Lease Billing Audit System** for student housing properties. The system compares **scheduled charges** (what we expected to bill) against **actual transactions** (what we actually billed) to identify billing discrepancies.

---

## What This Application Does (Full Lease File Audit System)

### Business Purpose
This is an **automated billing reconciliation and audit system** for a student housing portfolio of 94 properties managing ~120,000 leases. Property managers schedule recurring charges (rent, parking, pet fees, utilities) for residents, but billing errors occur: charges get missed, duplicated, or billed at incorrect amounts. Manual verification is impossible at this scale.

### The Audit Process

**Step 1: Data Collection**
- Connects to **Entrata** (property management system) via REST API
- Fetches two datasets for selected properties:
  1. **Scheduled Charges**: What we EXPECTED to bill residents based on lease agreements
     - Example: "$1,234.56/month rent from Aug 2024 - Apr 2025"
  2. **AR Transactions**: What we ACTUALLY billed residents (posted charges on their ledger)
     - Example: "$1,234.56 posted on 08/05/2024 for August rent"

**Step 2: Data Transformation**
- **Expansion**: Converts recurring scheduled charges into individual monthly records
  - Input: 1 charge record (Aug 2024 - Apr 2025, Monthly, $1,234.56)
  - Output: 9 monthly records (Aug $1,234.56, Sep $1,234.56, ..., Apr $1,234.56)
- **Normalization**: Cleans data, filters to specific charge types (e.g., Rent only), removes system-generated charges
- **Result**: Two comparable datasets with ~18,000 rows per property audit

**Step 3: Three-Tier Reconciliation**
For each **property + lease + charge type + month** combination, the system compares expected vs actual:

1. **Exact Match** ✅ → Expected charge was billed correctly
   - Property 771903, Lease 18296704, Rent, August 2024: Expected $1,234.56, Billed $1,234.56 → **MATCHED**

2. **Amount Mismatch** ⚠️ → Charge was billed, but at wrong amount
   - Property 771903, Lease 18296704, Rent, September 2024: Expected $1,234.56, Billed $1,100.00 → **AMOUNT_MISMATCH** (Undercharge $134.56)

3. **Unbilled Charge** 🚨 → Scheduled charge was never posted to ledger
   - Property 771903, Lease 18296704, Rent, October 2024: Expected $1,234.56, Billed $0.00 → **SCHEDULED_NOT_BILLED** (Revenue leakage $1,234.56)

4. **Unexpected Charge** 🚨 → Transaction posted without corresponding schedule
   - Property 771903, Lease 18296704, Rent, November 2024: Expected $0.00, Billed $1,234.56 → **BILLED_NOT_SCHEDULED** (Potential overcharge/dispute risk)

**Step 4: Metrics Calculation**
For each scope (portfolio, property, lease), calculates:
- **Match Rate %**: Percentage of charges that posted correctly (target: 98%+)
- **Undercharge $**: Total revenue lost from unbilled/under-billed charges
- **Overcharge $**: Total excess charges that may require refunds
- **Exception Count**: Number of discrepancies requiring manual review

**Step 5: User Interface**
Provides drill-down navigation:
- **Portfolio Dashboard**: See all 94 properties, aggregate KPIs ($50M+ in audit volume)
- **Property View**: See all leases for one property (e.g., 668 leases at property 771903)
- **Lease Detail View**: See transaction-by-transaction breakdown for one resident
  - Expected Transactions table (what we scheduled)
  - Actual Transactions table (what we billed)
  - "Open in Entrata" button to view in source system

**Step 6: Exception Resolution Workflow**
When discrepancies are found, users can mark exceptions as resolved with detailed categorization:
- **Ranked Suggestions**: System presents 4-6 resolution options ranked by likelihood
- **Action Classification**: Each resolution tagged as 'external' (requires Entrata action), 'internal' (documentation only), or 'one_time' (special charge)
- **Batch Resolution**: Users can select multiple exception months and apply same resolution
- **Historical Matching**: System auto-displays previous resolutions for recurring exceptions
- **Audit Trail**: Tracks who resolved, when, what category, and optional notes in SharePoint ExceptionMonths list

### What The System Audits

**Charge Types Audited** (configurable via AR code whitelist):
- **Rent** (AR Code 154771) - Base monthly rent charges
- **Parking** (AR Code 154772) - Parking space fees
- **Pet Fee** (AR Code 154773) - Monthly pet rent
- Other recurring charges as needed

**Audit Scope**:
- **Single Lease**: Audit one resident's billing for entire lease term
- **Single Property**: Audit all leases at one property (e.g., 668 leases)
- **Portfolio**: Audit entire portfolio (94 properties, ~120,000 leases)

**Audit Period**:
- Typically covers one lease year (e.g., August 2024 - July 2025)
- Can run historical audits for closed periods
- Run frequency: Weekly or on-demand

### Key Business Outcomes

**Before This System:**
- Manual spot-checking of billing (covered <1% of transactions)
- Billing errors discovered months later (after revenue period closed)
- No systematic way to identify recurring billing issues
- Property managers spent hours investigating resident disputes

**After This System:**
- **100% transaction coverage** - every charge audited automatically
- **Immediate visibility** - discrepancies identified within 2 minutes
- **Revenue protection** - unbilled charges caught before month-end close
- **Dispute prevention** - overcharges identified before residents complain
- **Trend analysis** - identify properties/charge types with recurring issues

**Real-World Impact:**
- Typical property audit (668 leases): Finds $30K-$50K in unbilled charges per month
- Portfolio audit (94 properties): Identifies $2M+ in revenue leakage
- Match rate improvement: 87% → 98% after implementing corrective processes
- Property manager time saved: 40 hours/month previously spent on manual verification

### Why We're Rebuilding

The current system works for **business logic** (APIs, reconciliation, calculations), but has **architectural problems**:
- Users wait 20-30 minutes to see transaction details (poor UX)
- Multiple storage layers (CSV, Parquet, SharePoint lists) cause complexity
- SharePoint list (AuditRuns2) accumulated 4.2M rows causing query slowdown
- Background writes fail silently, leaving incomplete audit data

**New System Goals:**
- Transaction details visible in **2 minutes** (not 30 minutes)
- Single source of truth (one SharePoint list, not three storage layers)
- Incremental writes (show results as audit progresses)
- Simple, debuggable architecture

---

## Core Business Logic (Already Working)

✅ **Data Fetching:** Entrata API integration fetches lease details and AR transactions  
✅ **Reconciliation:** Three-tier matching algorithm (exact match, amount mismatch, unbilled/unexpected)  
✅ **Metrics:** Calculates match rate, undercharge, overcharge, exception count  
✅ **UI:** Flask web app with portfolio/property/lease drill-down views  

**What's Proven (Reuse This Logic):**
- Entrata API calls (`getLeaseDetails`, `getLeaseArTransactions`)
- Field mapping (raw API → canonical fields)
- Normalization filters (AR code whitelist, API-posted exclusion, property exclusion)
- Expansion algorithm (scheduled → monthly buckets)
- Three-tier matching algorithm (groupby + merge logic)
- KPI calculations (match rate = matched/total, undercharge = sum unbilled, etc.)

**What Needs Redesign (THIS IS THE FOCUS):**
- ❌ Storage persistence (was Parquet files, now SharePoint lists)
- ❌ Data availability timeline (was 15 min wait, need 2 min)
- ❌ Query strategy (was file loading, now list filtering)
- ❌ Cache management (needs to work with list-based storage)

---

## Original Architecture Problems (Why We're Starting Over)

### The Old Architecture Had THREE Storage Layers (All Problematic):

**1. AuditRuns2 SharePoint List** (THE BIGGEST BOTTLENECK)
- Stored EVERY bucket comparison (~18,000 rows per audit)
- **Write performance disaster:**
  - Single property audit (668 leases): ~45,000 rows written in background thread
  - Full portfolio audit (94 properties): ~4.2 MILLION rows written asynchronously
  - **Write time: 10-30 minutes** for large audits using 20-row batches
- Used for drill-down queries (property view, lease view)
- Eventually hit **5,000 item query degradation** threshold causing slow page loads
- Accumulated millions of rows over time (30M item limit risk)

**2. CSV Files in SharePoint Document Library**
- Saved complete audit detail as CSV files per property per run
- Used as "intermediate" storage before AuditRuns2 writes completed
- File management complexity (download, parse, cache, cleanup)
- No structured queries - had to load entire CSV to filter one lease

**3. Parquet Files in SharePoint Document Library** (Attempted Fix)
- Replaced CSV with compressed Parquet format to reduce file size
- Three files per audit run:
  - `expected_detail.parquet` (~9K rows)
  - `actual_detail.parquet` (~9.6K rows)
  - `variance_detail.parquet` (~187 rows)
- **Still had delay problem:** Files written AFTER entire audit completed
- For property with 668 leases: audit runs 10-15 minutes, then starts writing files
- User has to wait additional 10-15 minutes before transaction detail is available
- **Total wait time:** 20-30 minutes from audit start to viewing lease detail

### Specific Problems with Old Architecture:

**Problem 1: No Incremental Availability**
- All three storage layers written in batch at the end (all-or-nothing approach)
- Cannot view ANY lease details until entire audit finishes AND storage writes complete
- Poor user experience: "Audit running... please wait 30 minutes"
- Users complained: "Why can't I see results for completed leases while audit continues?"

**Problem 2: AuditRuns2 Write Bottleneck**
- Asynchronous background thread writes to AuditRuns2 after audit completes
- 20 rows per batch × thousands of rows = hundreds of HTTP requests
- Graph API rate limiting occasionally caused failures
- No visibility into write progress (silent background operation)

**Problem 3: Cache Invalidation on Restart**
- In-memory cache lost when app restarts
- On restart, app must reload Parquet files from SharePoint Document Library
- File download + deserialization adds 5-10 seconds per query
- If background AuditRuns2 write still in progress, data inconsistency

**Problem 4: Complex Multi-Layer Storage**
- Three different storage mechanisms for same data (AuditRuns2, CSV, Parquet)
- Unclear "source of truth" - which layer to query for lease detail?
- Cleanup/retention policies complicated (delete from all three places)
- Debugging: "Is the data in cache, in Parquet, or in AuditRuns2?"

**Problem 5: Query Performance Degradation**
- AuditRuns2 accumulated millions of rows (multiple audits per week)
- SharePoint $filter queries slowed down after 5,000 items per list
- Property view queries took 5-10 seconds loading all buckets for one property
- No proper indexing strategy for RunID + LeaseID filters

### What We Want in New Architecture:

- ✅ **Single source of truth** - ONE SharePoint list (not three storage layers)
- ✅ **Write rows incrementally** as reconciliation completes (not after audit finishes)
- ✅ **2-minute availability** - users can view partial results while audit continues
- ✅ **Persistent storage** - data survives restarts without file I/O
- ✅ **Simple queries** - filter by RunID + LeaseID without loading entire dataset
- ✅ **Proper indexing** - prevent 5K item degradation with indexed columns
- ✅ **No background threads** - writes complete before user navigation allowed  

## The Problem I Need to Solve

**Transaction detail data (~18,000 rows per audit) needs to be:**
- ✅ **Available immediately** - visible within 2 minutes of audit start, NOT 15 minutes later
- ✅ **Fast to query** - lease detail page loads in <2 seconds
- ✅ **Durable** - survives app restarts
- ✅ **Preserved** - can view old audits months later without deletion

**Current architecture issues:**
- ❌ Data saved AFTER all batches complete (10-15 minute wait)
- ❌ User can't view lease details until entire audit finishes
- ❌ Old architecture had complex file-based storage causing delays

## Data Structure

**Expected Detail (per audit):**
- ~9,000 rows of scheduled charges expanded to monthly buckets
- Columns: PropertyID, LeaseID, ARCode, AuditMonth, Amount, PeriodStart, PeriodEnd

**Actual Detail (per audit):**
- ~9,600 rows of posted AR transactions
- Columns: PropertyID, LeaseID, ARCode, AuditMonth, Amount, PostDate, TransactionID

**Aggregated Snapshots (per audit):**
- ~5,966 summary rows at portfolio/property/lease/month scopes
- Columns: RunID, ScopeType, PropertyID, LeaseID, ARCode, AuditMonth, ExpectedTotal, ActualTotal, Variance

## Current Tech Stack

- **Backend:** Python 3.11, Flask 3.1.0, pandas 2.2.3
- **Storage:** SharePoint (Document Library + Lists) via Microsoft Graph API
- **Auth:** Azure AD SSO, app-only tokens
- **Caching:** Flask-Caching with 4-hour timeout

## Entrata API Integration

### API Endpoints

**API 1: getLeaseDetails** (scheduled charges)
- **Method:** `POST`
- **Version:** `r2`
- **Endpoint:** `https://apis.entrata.com/ext/orgs/peakmade/v1/leases`
- **Authentication:** `X-Api-Key` header
- **Request Parameters:**
  - `propertyIds`: List of property IDs to audit (array of integers)
  - `leaseId`: Specific lease ID (optional, for single-lease audits)
  - `includeCharges`: 1 (required to get scheduled charges)
  - `includeCustomers`: 1 (required to get resident names)
  - `includeInactive`: 1 (include inactive leases)
- **Response Data:**
  - Lease metadata (Lease ID, Property ID, Unit Number, Lease Dates)
  - Customer details (Resident names, emails, phone numbers, Customer ID for deep linking)
  - Scheduled charges (AR Code, Amount, Start/End Dates, Frequency)
- **Request Example:**
```json
{
  "auth": {"type": "apikey"},
  "requestId": "1704556800000",
  "method": {
    "name": "getLeaseDetails",
    "version": "r2",
    "params": {
      "propertyIds": [771903],
      "includeCharges": 1,
      "includeCustomers": 1,
      "includeInactive": 1
    }
  }
}
```

**API 2: getLeaseArTransactions** (actual transactions)
- **Method:** `POST`
- **Version:** `r1`
- **Endpoint:** `https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions`
- **Authentication:** `X-Api-Key` header
- **Request Parameters:**
  - `propertyIds`: List of property IDs to audit
  - `leaseId`: Specific lease ID (optional)
  - `fromDate`: Start date for transaction filter (MM/DD/YYYY format)
  - `toDate`: End date for transaction filter (MM/DD/YYYY format)
- **Response Data:**
  - Transaction ID (unique identifier like "AR-12345")
  - AR Code ID (charge type)
  - Amount (actual billed amount)
  - Post Date (date transaction hit ledger)
  - Audit Month (month the charge applies to)
  - Transaction Type (Charge, Payment, Credit, Adjustment)
- **Request Example:**
```json
{
  "auth": {"type": "apikey"},
  "requestId": "1704556800000",
  "method": {
    "name": "getLeaseArTransactions",
    "version": "r1",
    "params": {
      "propertyIds": [771903],
      "fromDate": "08/01/2024",
      "toDate": "07/31/2025"
    }
  }
}
```

**API 3: getPropertyPicklist** (property list for multi-select)
- **Method:** `POST`
- **Endpoint:** `https://apis.entrata.com/ext/orgs/peakmade/v1/properties` (or similar)
- **Authentication:** `X-Api-Key` header
- **Purpose:** Load all properties for bulk audit property selection
- **Response:** List of properties with ID, Name, and active status

**Environment Configuration:**
- Production: `https://apis.entrata.com/ext/orgs/peakmade/v1/`
- Sandbox: `https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/`
- Toggle via `entrata_environment.json`: `{"environment": "prod"}` or `{"environment": "sandbox"}`

**Data Filtering Applied:**
- **AR Code Whitelist:** Only audit specific AR codes (default: `[154771]` for "Rent")
  - Configured via `excluded_ar_codes.json`: `{"allowed_ar_codes": [154771]}`
  - Filters both scheduled charges and actual transactions
  - AR Code Names mapped via `ar_code_name_usage_map.json`:
    - `154771` → `"Rent"` (Base charge)
    - `154772` → `"Parking"` (Parking fee)
    - `154773` → `"Pet Fee"` (Pet rent)
- **API-Posted Charges:** Remove system-generated charges that aren't scheduled
  - These are automatically posted by Entrata (e.g., late fees, proration adjustments)
  - Should not appear in audit because they have no corresponding schedule
  - Filter logic: Check if charge has `isApiPosted: true` flag or similar
- **Excluded Properties:** Skip test/closed properties entirely
  - Configured via `excluded_properties.json`: `{"excluded_property_ids": ["123456"]}`

**Entrata Deep Links:**
- Lease detail view must provide "Open in Entrata" button for each transaction
- Constructs URL to open transaction in Entrata web portal
- Opens in external system browser (not embedded in app)
- URL format: 
  - Production: `https://peakmade.entrata.com/users/{customer_id}`
  - Sandbox: `https://peakmade-test-17291.entrata.com/users/{customer_id}`

**API Error Handling:**
- Rate limiting: Entrata may throttle requests (retry with exponential backoff)
- Authentication errors: Invalid API key returns 401 (check environment config)
- Timeout: Set reasonable timeout (30-60 seconds for large property queries)
- Pagination: If property has >1000 leases, may need pagination (check response structure)

---

## Microsoft Graph API Integration (SharePoint)

### Authentication

**MSAL (Microsoft Authentication Library) App-Only Flow:**
```python
from msal import ConfidentialClientApplication

# Initialize MSAL app
app = ConfidentialClientApplication(
    client_id=AZURE_CLIENT_ID,
    client_credential=AZURE_CLIENT_SECRET,
    authority=f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
)

# Acquire token
result = app.acquire_token_for_client(
    scopes=["https://graph.microsoft.com/.default"]
)
access_token = result["access_token"]
```

**Required Azure AD App Registration:**
- **API Permissions:** `Sites.ReadWrite.All` (Application permission, requires admin consent)
- **Token Lifetime:** 1 hour (cache and reuse token, refresh before expiration)
- **Token Caching:** Store in memory, refresh 5 minutes before expiry

### SharePoint List Operations

**Write Operations (Create List Items):**

**Single Item Write:**
```http
POST https://graph.microsoft.com/v1.0/sites/{site-id}/lists/{list-id}/items
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "fields": {
    "Title": "Item title",
    "RunId": "run_20260706_141025",
    "PropertyId": 771903,
    "LeaseIntervalId": 18296704,
    "Amount": 1234.56
  }
}
```

**Batch Write (20 items per batch):**
```http
POST https://graph.microsoft.com/v1.0/$batch
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "requests": [
    {
      "id": "1",
      "method": "POST",
      "url": "/sites/{site-id}/lists/{list-id}/items",
      "body": {
        "fields": { ... }
      },
      "headers": {
        "Content-Type": "application/json"
      }
    },
    ... (up to 20 requests)
  ]
}
```

**Batch Constraints:**
- Maximum 20 requests per batch
- Must use sequential IDs ("1", "2", "3", ... "20")
- Total payload size limit: ~4MB per batch
- Example: Writing 1,000 rows = 50 batches (1,000 ÷ 20)

**Read Operations (Query List Items):**

**Basic Query:**
```http
GET https://graph.microsoft.com/v1.0/sites/{site-id}/lists/{list-id}/items?$filter=RunId eq 'run_20260706_141025'&$expand=fields
Authorization: Bearer {access_token}
```

**Complex Filter (multiple conditions):**
```http
GET https://graph.microsoft.com/v1.0/sites/{site-id}/lists/{list-id}/items?$filter=RunId eq 'run_20260706_141025' and LeaseIntervalId eq 18296704&$expand=fields
Authorization: Bearer {access_token}
```

**Query Performance:**
- Without indexing: Queries degrade after ~5,000 items in list
- With indexed columns: Can handle millions of items efficiently
- **MUST INDEX:** RunId, PropertyId, LeaseIntervalId columns

**List Management:**

**Get List by Name:**
```http
GET https://graph.microsoft.com/v1.0/sites/{site-id}/lists?$filter=displayName eq 'RunDisplaySnapshots'
Authorization: Bearer {access_token}
```

**Get Site ID:**
```http
GET https://graph.microsoft.com/v1.0/sites/{tenant}.sharepoint.com:/sites/{site-name}
Authorization: Bearer {access_token}
```

### Error Handling

**Common Graph API Errors:**
- `401 Unauthorized`: Token expired or invalid (refresh token)
- `403 Forbidden`: Insufficient permissions (check Sites.ReadWrite.All granted)
- `404 Not Found`: List or site doesn't exist (verify list names)
- `429 Too Many Requests`: Rate limit hit (implement exponential backoff)
- `503 Service Unavailable`: SharePoint throttling (retry after delay)

**Retry Logic:**
```python
def write_with_retry(batch_data, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=batch_data)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

---

## KPI Calculations (Detailed Formulas)

### 1. Match Rate %

**Formula:**
```
Match Rate = (Matched Buckets / Total Expected Buckets) × 100
```

**Calculation Logic:**
```python
# Count buckets by status
matched_count = len(buckets[buckets['Status'] == 'MATCHED'])
total_expected = len(buckets[buckets['ExpectedTotal'] > 0])

match_rate = (matched_count / total_expected) * 100 if total_expected > 0 else 0.0
```

**Example:**
- Property 771903 has 668 leases
- Audit period: 9 months (Aug 2024 - Apr 2025)
- Total expected buckets: 668 × 9 = 6,012 charges
- Matched: 5,880 charges
- Match Rate: (5,880 ÷ 6,012) × 100 = **97.8%**

**Scopes:**
- **Portfolio Level**: Aggregate across all properties in audit
- **Property Level**: Aggregate across all leases in property
- **Lease Level**: Aggregate across all AR codes for lease
- **AR Code Level**: Match rate for specific charge type (e.g., Rent only)

### 2. Undercharge $ (Revenue Leakage)

**Formula:**
```
Undercharge = Sum of (Expected - Actual) where Expected > Actual
```

**Calculation Logic:**
```python
# Amount mismatches where we billed less than expected
amount_mismatch_undercharge = buckets[
    (buckets['Status'] == 'AMOUNT_MISMATCH') & 
    (buckets['Variance'] < 0)  # Negative variance = undercharge
]['Variance'].abs().sum()

# Unbilled charges (scheduled but never posted)
unbilled_charges = buckets[
    buckets['Status'] == 'SCHEDULED_NOT_BILLED'
]['ExpectedTotal'].sum()

total_undercharge = amount_mismatch_undercharge + unbilled_charges
```

**Example:**
- Lease 18296704, September Rent: Expected $1,234.56, Billed $1,100.00
  - Amount Mismatch Undercharge: $134.56
- Lease 18296704, October Rent: Expected $1,234.56, Billed $0.00
  - Unbilled Charge: $1,234.56
- **Total Undercharge for Lease**: $1,369.12

### 3. Overcharge $ (Potential Refund Risk)

**Formula:**
```
Overcharge = Sum of (Actual - Expected) where Actual > Expected
```

**Calculation Logic:**
```python
# Amount mismatches where we billed more than expected
amount_mismatch_overcharge = buckets[
    (buckets['Status'] == 'AMOUNT_MISMATCH') & 
    (buckets['Variance'] > 0)  # Positive variance = overcharge
]['Variance'].sum()

# Unexpected charges (posted without schedule)
unexpected_charges = buckets[
    buckets['Status'] == 'BILLED_NOT_SCHEDULED'
]['ActualTotal'].sum()

total_overcharge = amount_mismatch_overcharge + unexpected_charges
```

**Example:**
- Lease 18296705, August Rent: Expected $1,200.00, Billed $1,300.00
  - Amount Mismatch Overcharge: $100.00
- Lease 18296705, November Rent: Expected $0.00, Billed $1,200.00
  - Unexpected Charge: $1,200.00
- **Total Overcharge for Lease**: $1,300.00

### 4. Exception Count (Discrepancies Requiring Review)

**Formula:**
```
Exception Count = Count of high-severity findings
```

**Calculation Logic:**
```python
# High-severity statuses
high_severity_statuses = [
    'SCHEDULED_NOT_BILLED',  # Unbilled charges (revenue loss)
    'BILLED_NOT_SCHEDULED',  # Unexpected charges (dispute risk)
    'AMOUNT_MISMATCH'         # Wrong amount (billing error)
]

exception_count = len(buckets[buckets['Status'].isin(high_severity_statuses)])
```

**Example:**
- Property 771903, 668 leases, 6,012 total buckets
  - Matched: 5,880 (no exceptions)
  - Amount Mismatch: 45 exceptions
  - Unbilled: 72 exceptions
  - Unexpected: 15 exceptions
- **Total Exception Count**: 45 + 72 + 15 = **132 exceptions**

### 5. Total Variance $ (Net Position)

**Formula:**
```
Total Variance = Overcharge - Undercharge
```

**Interpretation:**
- **Positive Variance**: We overbilled (owe residents money)
- **Negative Variance**: We underbilled (lost revenue)
- **Zero Variance**: Net neutral (but may still have offsetting errors)

**Example:**
- Property 771903:
  - Total Undercharge: $45,678.90 (lost revenue)
  - Total Overcharge: $12,345.67 (excess charges)
  - **Net Variance**: $12,345.67 - $45,678.90 = **-$33,333.23** (net undercharge)

### Aggregation Hierarchy

**Data flows from lease → property → portfolio:**

1. **Lease Level** (base calculation):
   - Calculate KPIs for each lease (e.g., Lease 18296704)
   - One row per lease in RunDisplaySnapshots

2. **Property Level** (aggregation):
   - Sum all lease KPIs for property (e.g., Property 771903)
   - Match Rate: Weighted average based on bucket counts
   - One row per property in RunDisplaySnapshots

3. **Portfolio Level** (top aggregation):
   - Sum all property KPIs across portfolio (94 properties)
   - Match Rate: Weighted average across all properties
   - One row for portfolio in RunDisplaySnapshots

**Aggregation Example:**
```python
# Property-level aggregation
property_summary = lease_summaries.groupby('PropertyId').agg({
    'MatchedBuckets': 'sum',
    'TotalBuckets': 'sum',
    'Undercharge': 'sum',
    'Overcharge': 'sum',
    'ExceptionCount': 'sum'
})
property_summary['MatchRate'] = (
    property_summary['MatchedBuckets'] / property_summary['TotalBuckets'] * 100
)
```

---

## Entrata API Integration

## Success Criteria

When I run an audit for property 771903 (668 leases):

1. **Audit starts** → System fetches data from Entrata API
2. **Within 2 minutes** → I can navigate to lease 18296704 and see:
   - ✅ Resident name (not blank)
   - ✅ AR code shows "Rent" (not "-")
   - ✅ Expected transactions table: 9 rows with Period and Amount
   - ✅ Actual transactions table: 9 rows with PostDate, Amount, TransactionID
   - ✅ "Open in Entrata" button works
3. **Run second audit** → First audit data still accessible (history preserved)
4. **Restart app** → Can still load both audits (durability)

---

## User Workflow & Navigation (What the UI Should Do)

### 1. Home Page `/`
- Display list of recent audit runs with metadata:
  - Run ID (format: `run_20260706_141025` - timestamp-based)
  - Run date/time
  - Audit type (Property, Bulk, Single Lease)
  - Number of properties audited
  - Total match rate %
  - Status (Complete, In Progress, Failed)
- "Start New Audit" button → redirects to `/run-audit`

### 2. Run Lease Audit Page `/run-audit`
Single page with three audit type options:

**Option 1: Run Property Audit**
- **Property Selector**: Dropdown to select one property
- **Date Range Options**:
  - Academic Year selector (e.g., 2024-2025 → 08/01/2024 to 07/31/2025)
  - OR Custom date range (From/To date pickers)
- **AR Code Filter**: Checkbox list (default: [154771] "Rent" only)
- **Submit**: Creates RunId, audits all leases for selected property
- **Result**: Navigates to property detail view when complete

**Option 2: Run Bulk Audit**
- **Property Multi-Select**: Checkboxes for properties (load from Entrata API)
  - "Select All" checkbox option
  - Individual property checkboxes
- **Date Range Options**: Same as Property Audit (Academic Year or Custom)
- **AR Code Filter**: Same as Property Audit
- **Submit**: Creates RunId, audits each property sequentially
- **Progress View**: Real-time status updates via AuditRunStatus polling
  - Shows per-property progress (Queued → Fetching → Reconciling → Writing Detail → Viewable)
  - Property becomes clickable when status = Viewable (before entire audit completes)
- **Result**: Navigates to portfolio dashboard showing all properties

**Option 3: Run Single Lease Audit**
- **Property Selector**: Dropdown to select property
- **Lease ID Input**: Text field for lease interval ID
- **Date Range Options**: Same as above
- **AR Code Filter**: Same as above
- **Submit**: Creates RunId, audits only specified lease
- **Result**: Navigates directly to lease detail view when complete

### 3. Portfolio Dashboard `/portfolio/<run_id>`
- **Header KPIs** (aggregated across all properties in audit):
  - Total Match Rate: `97.9%`
  - Total Undercharge: `$34,567.89`
  - Total Overcharge: `$1,234.56`
  - Exception Count: `187`
- **Property Table** (one row per property):
  - Columns: Property Name, Match Rate, Undercharge, Overcharge, Exception Count, Lease Count
  - Click property name → navigates to `/property/<property_id>/<run_id>`

### 4. Property Detail `/property/<property_id>/<run_id>`
- **Header**: Property name, property ID
- **Property-Level KPIs**:
  - Match Rate %
  - Undercharge $
  - Overcharge $
  - Exception Count
- **Lease Summary Table** (one row per lease):
  - Columns:
    - Lease Interval ID (clickable link)
    - Resident Name (from Entrata customer data)
    - Unit Number (if available)
    - Match Rate %
    - Undercharge $
    - Overcharge $
    - Exception Count
    - AR Status Badge (Passed ✓ / Failed ✗ / Partial ⚠)
  - Click Lease Interval ID → navigates to `/lease/<lease_id>/<run_id>`
  - **Filtering**: Filter by AR Status (All / Passed / Failed / Partial)
  - **Sorting**: Sort by any column (default: highest exception count first)

### 5. Lease Detail View `/lease/<lease_id>/<run_id>` (MOST IMPORTANT VIEW)
- **Header**:
  - Resident Name (multiple residents: "John Smith & Jane Doe")
  - Lease ID, Lease Interval ID
  - "Open in Entrata" button (opens Entrata web portal in external browser)
    - URL format: `https://peakmade.entrata.com/users/{customer_id}`
- **Summary Banner**:
  - Total Undercharge: `$1,234.56` (red text)
  - Total Overcharge: `$500.00` (blue text)

- **AR Code Details Table** (one row per AR code audited for this lease):
  - Columns:
    - AR Code ID (`154771`)
    - AR Code Name (`Rent`)
    - Matched Count (e.g., `9` matched months)
    - Discrepancy Count (e.g., `2` exceptions)
    - Undercharge $
    - Overcharge $
    - AR Status Badge (Passed ✓ / Failed ✗)
  - Click AR Code → expands to show transaction detail tables below

- **Expected Transactions Table** (scheduled charges):
  - Visible after clicking AR code row
  - Columns:
    - Period (`08/2024 - 08/2024` or `08/2024 - 04/2025`)
    - Amount (`$1,234.56`)
    - Status (`MATCHED`, `SCHEDULED_NOT_BILLED`, `AMOUNT_MISMATCH`)
  - Highlight rows with discrepancies (red background for unbilled, yellow for mismatch)

- **Actual Transactions Table** (billed charges):
  - Visible after clicking AR code row
  - Columns:
    - Post Date (`08/05/2024`)
    - Audit Month (`08/2024`)
    - Amount (`$1,234.56`)
    - Transaction ID (`AR-12345`)
    - Status (`MATCHED`, `BILLED_NOT_SCHEDULED`, `AMOUNT_MISMATCH`)
  - Click Transaction ID → opens Entrata transaction detail in external browser

- **Exception Resolution** (if discrepancies exist):
  - "Mark as Resolved" button → opens modal dialog with ranked suggestions
  - Modal shows context-specific resolution options based on exception type:
    - **SCHEDULED_NOT_BILLED**: 6 options (e.g., "Post missing charge", "Correct schedule dates", "Concession approved")
    - **BILLED_NOT_SCHEDULED**: 5 options (e.g., "Add to schedule", "Reverse charge", "Move-in proration")
    - **AMOUNT_MISMATCH**: 6 options (e.g., "Correct schedule amount", "Adjust transaction", "Proration")
  - Each option tagged with action type: 'external' (requires Entrata action), 'internal' (documentation only), 'one_time' (special charge)
  - Batch resolution: Select multiple months via checkboxes, apply same fix to all
  - Notes textarea for optional explanation
  - Submit → saves to `ExceptionMonths` SharePoint list with full audit trail
  - After resolution: Shows green badge "✓ [Fix Label]" with "Resolved by [User] on [Date]" subtext
  - **Historical matching**: If same exception was resolved in previous audit, shows previous resolution with timestamp
  - See "Exception Resolution Workflow (Complete System)" section below for full details on all 17 resolution categories

### 6. Run ID Format
- **Pattern**: `run_YYYYMMDD_HHMMSS` (e.g., `run_20260706_141025`)
- **Generation**: Timestamp when audit starts (UTC)
- **Uniqueness**: Guarantees no collision (one audit per second max)

---

## Exception Resolution Workflow (Complete System)

### Overview

When billing discrepancies are identified, users need a structured way to categorize and track resolutions. The system provides **context-specific resolution suggestions** based on the exception type, ranked by likelihood and actionability.

### Resolution Categories by Exception Type

#### 1. SCHEDULED_NOT_BILLED (Charge scheduled but never posted)
Revenue leakage scenario - expected charge missing from resident ledger.

**Resolution Options (Ranked):**

1. **Post missing charge in Entrata** (external) - *PRIMARY FIX*
   - **When to use**: Charge was legitimately scheduled but billing failed
   - **Action required**: Post the AR transaction manually in Entrata
   - **Example**: August rent was scheduled for $1,234.56 but never posted to ledger

2. **Correct schedule dates in Entrata** (external)
   - **When to use**: Schedule has wrong start/end dates causing missed billing cycle
   - **Action required**: Update recurring charge schedule dates
   - **Example**: Schedule shows 08/15/2024 start but lease began 08/01/2024

3. **Lease renewal not yet processed** (external)
   - **When to use**: Resident renewed but new schedule not created
   - **Action required**: Process renewal in Entrata, create new charge schedule
   - **Example**: Lease renewed at new rate but old schedule expired

4. **Resident moved out – charge not applicable** (internal)
   - **When to use**: Resident vacated before schedule end date
   - **Action required**: None - document reason
   - **Example**: Lease ended early due to transfer/buyout

5. **Month-to-month – no active schedule** (external)
   - **When to use**: Lease converted to MTM without recurring schedule
   - **Action required**: Create MTM schedule or manual billing process
   - **Example**: Fixed-term lease expired, no MTM schedule created

6. **Concession / waiver approved** (internal)
   - **When to use**: Management approved charge waiver
   - **Action required**: None - document approval
   - **Example**: One free month rent as move-in special

#### 2. BILLED_NOT_SCHEDULED (Charge posted without recurring schedule)
Dispute risk scenario - resident billed for charge without documented schedule.

**Resolution Options (Ranked):**

1. **Add recurring charge to schedule** (external) - *PRIMARY FIX*
   - **When to use**: Charge is legitimate but schedule was never created
   - **Action required**: Create recurring charge schedule in Entrata
   - **Example**: Pet rent being billed manually instead of via schedule

2. **Reverse incorrect charge** (external)
   - **When to use**: Charge posted in error
   - **Action required**: Reverse AR transaction, refund if paid
   - **Example**: Accidentally posted charge to wrong unit

3. **Move-in proration** (internal)
   - **When to use**: Prorated charge for partial first month
   - **Action required**: None - expected behavior
   - **Example**: Moved in 08/15, prorated half-month rent posted

4. **Backdated / corrected posting** (internal)
   - **When to use**: Correction or backdated charge posted outside normal cycle
   - **Action required**: None - document reason
   - **Example**: Missed August charge posted in September as correction

5. **Mark as one-time charge** (one_time)
   - **When to use**: Legitimate non-recurring charge
   - **Action required**: None - classify as special charge
   - **Example**: Move-in fee, damage charge, special assessment

#### 3. AMOUNT_MISMATCH (Scheduled and billed amounts differ)
Billing error scenario - charge posted at wrong amount.

**Resolution Options (Ranked):**

1. **Correct scheduled amount in Entrata** (external) - *PRIMARY FIX*
   - **When to use**: Recurring schedule has wrong amount
   - **Action required**: Update charge amount in schedule
   - **Example**: Rent schedule shows $1,200 but lease says $1,234.56

2. **Lease renewal amount not updated in schedule** (external)
   - **When to use**: Resident renewed at new rate but schedule not updated
   - **Action required**: Update schedule to match new lease rate
   - **Example**: Renewed from $1,200/mo to $1,300/mo but schedule still shows old rate

3. **Adjust posted transaction** (external)
   - **When to use**: Posted amount is incorrect
   - **Action required**: Reverse and repost with correct amount
   - **Example**: Accidentally posted $1,100 instead of $1,234.56

4. **Proration (move-in or move-out)** (internal)
   - **When to use**: Amount difference due to partial month proration
   - **Action required**: None - expected calculation
   - **Example**: Move-out on 04/15 = half month rent billed

5. **Rent concession applied** (internal)
   - **When to use**: Concession reduced billed amount
   - **Action required**: Verify concession documented in Entrata
   - **Example**: $100/month concession for 3 months reduces rent from $1,200 to $1,100

6. **Document variance reason** (internal)
   - **When to use**: Difference is expected but doesn't fit other categories
   - **Action required**: Add notes explaining variance
   - **Example**: Rounding adjustment, one-time rate modification

---

### Action Type Classification

Each resolution is tagged with an action type indicating where work needs to happen:

#### External Actions
**Definition**: Requires changes in Entrata (property management system)

**Examples:**
- Post missing charges
- Update charge schedules
- Reverse incorrect transactions
- Create recurring schedules
- Adjust posted amounts

**Workflow Impact:**
- Marked as "external" in ExceptionMonths list
- Property managers must take action in Entrata
- Follow-up verification needed after Entrata changes

#### Internal Actions
**Definition**: Documentation/explanation only - variance is expected or acceptable

**Examples:**
- Prorations (move-in, move-out)
- Approved concessions/waivers
- Resident early move-out
- Backdated corrections
- Expected variances

**Workflow Impact:**
- Marked as "internal" in ExceptionMonths list
- No Entrata action required
- Notes field explains why variance is acceptable

#### One-Time Actions
**Definition**: Special classification for non-recurring charges

**Examples:**
- Move-in fees
- Pet deposits
- Damage charges
- Special assessments
- One-off parking charges

**Workflow Impact:**
- Marked as "one_time" in ExceptionMonths list
- Excluded from recurring charge audit going forward
- Tracked separately for non-recurring charge analysis

---

### Resolution Workflow (User Experience)

#### Step 1: Lease Detail View
User navigates to lease with exceptions (e.g., Lease 18296704, Rent charge has 2 mismatches)

#### Step 2: Expand AR Code Section
User clicks "Rent" AR code row → accordion expands showing:
- Expected Transactions table (scheduled charges)
- Actual Transactions table (posted charges)
- Rows highlighted red/yellow for discrepancies

#### Step 3: Resolution Options Presented
System displays ranked suggestions based on exception type:
- **Suggested Fixes Panel**: 4-6 options with descriptions and "Apply as Plan" buttons
- **Dropdown Selector**: Compact dropdown for batch resolution of multiple months
- Each option shows: Title, description, action type indicator

#### Step 4: Select Resolution (Two Methods)

**Method A: Individual Month Resolution**
- Click "Apply as Plan" button on specific suggestion
- Single month marked with that resolution
- Status changes to "In Progress" or "Resolved"

**Method B: Batch Resolution**
- Select multiple exception months via checkboxes
- Choose resolution from dropdown
- Click "Apply" → all selected months get same resolution
- Useful when same issue affects multiple months

#### Step 5: Add Notes (Optional)
- Modal opens with notes textarea
- User adds context: "Spoke with property manager - approved waiver for August-September"
- Notes saved to ExceptionMonths SharePoint list

#### Step 6: Submit Resolution
- System writes to ExceptionMonths list:
  ```json
  {
    "RunId": "run_20260706_141025",
    "PropertyId": 771903,
    "LeaseIntervalId": 18296704,
    "ArCodeId": "154771",
    "AuditMonth": "2024-08",
    "ExceptionType": "SCHEDULED_NOT_BILLED",
    "Status": "Resolved",
    "FixLabel": "Post missing charge in Entrata",
    "ActionType": "external",
    "Variance": -1234.56,
    "ResolvedBy": "john.doe@company.com",
    "ResolvedByName": "John Doe",
    "ResolvedAt": "2026-07-07T14:30:00Z",
    "Notes": "Spoke with property manager - will post tomorrow"
  }
  ```

#### Step 7: Visual Confirmation
- Green checkmark badge appears on resolved month
- Shows: "✓ Post missing charge in Entrata"
- Subtext: "Resolved by John Doe on 07/07/2026 2:30 PM"
- If notes exist: small note icon with hover preview

---

### Historical Resolution Matching (Auto-Apply Previous Fixes)

**Problem**: Same exceptions recur across audit runs (e.g., parking charge always unbilled in August due to schedule gap)

**Solution**: System automatically checks for previous resolutions when new audit finds exceptions

**Logic Flow:**

1. **New Audit Identifies Exception**
   - Property 771903, Lease 18296704, Rent (154771), August 2024: SCHEDULED_NOT_BILLED

2. **Query Historical Resolutions**
   ```sql
   SELECT * FROM ExceptionMonths 
   WHERE PropertyId = 771903 
     AND LeaseIntervalId = 18296704 
     AND ArCodeId = '154771'
     AND AuditMonth = '2024-08'
     AND Status = 'Resolved'
   ORDER BY ResolvedAt DESC
   LIMIT 1
   ```

3. **Display Previous Resolution (If Found)**
   - Shows in Expected or Actual column with checkmark badge
   - Format: "✓ [Fix Label]"
   - Subtext: "Previously resolved by Jane Smith on 06/15/2026"
   - User can see historical context without re-investigating

4. **No Auto-Apply (User Must Confirm)**
   - System SHOWS previous resolution but doesn't auto-apply to new run
   - User must explicitly mark current audit's exception as resolved
   - Reason: Exception may have different cause in new audit run

**Benefits:**
- Faster resolution workflow (user sees what was done before)
- Consistency (same exceptions resolved same way)
- Audit trail (track recurring issues across time)

---

### SharePoint ExceptionMonths List Schema

**Purpose**: Persistent storage of all exception resolutions across all audit runs

**Columns:**

| Column Name | Type | Description | Example |
|------------|------|-------------|---------|
| `CompositeKey` | Text | Unique identifier per exception | `771903_18296704_154771_2024-08_run_20260706_141025` |
| `RunId` | Text (Indexed) | Audit run identifier | `run_20260706_141025` |
| `PropertyId` | Number (Indexed) | Property identifier | `771903` |
| `LeaseIntervalId` | Number (Indexed) | Lease identifier | `18296704` |
| `ArCodeId` | Text | Charge type code | `154771` |
| `ArCodeName` | Text | Charge type name | `Rent` |
| `AuditMonth` | Text (Indexed) | Month of exception | `2024-08` |
| `ExceptionType` | Text | Type of discrepancy | `SCHEDULED_NOT_BILLED` |
| `Status` | Choice | Resolution status | `Open`, `Resolved` |
| `FixLabel` | Text | Selected resolution | `Post missing charge in Entrata` |
| `ActionType` | Choice | Action classification | `external`, `internal`, `one_time` |
| `Variance` | Number | Dollar amount variance | `-1234.56` |
| `ExpectedTotal` | Number | Scheduled amount | `1234.56` |
| `ActualTotal` | Number | Billed amount | `0.00` |
| `Notes` | Text | User explanation | `Spoke with PM - will post tomorrow` |
| `ResolvedBy` | Text | User email | `john.doe@company.com` |
| `ResolvedByName` | Text | User display name | `John Doe` |
| `ResolvedAt` | DateTime | Resolution timestamp | `2026-07-07T14:30:00Z` |

**Key Indexes:**
- `RunId` - Filter by audit run
- `PropertyId` - Property-level queries
- `LeaseIntervalId` - Lease-level queries
- `AuditMonth` - Month-based filtering
- Composite index: `PropertyId + LeaseIntervalId + ArCodeId + AuditMonth` for historical lookups

---

### API Endpoint for Resolution Submission

**Route**: `POST /api/exception-months`

**Purpose**: Upsert exception resolution (single month or batch)

**Request Payload:**
```json
{
  "run_id": "run_20260706_141025",
  "property_id": 771903,
  "lease_interval_id": 18296704,
  "ar_code_id": "154771",
  "ar_code_name": "Rent",
  "audit_month": "2024-08",
  "exception_type": "SCHEDULED_NOT_BILLED",
  "status": "Resolved",
  "fix_label": "Post missing charge in Entrata",
  "action_type": "external",
  "variance": -1234.56,
  "expected_total": 1234.56,
  "actual_total": 0.00,
  "notes": "Optional user notes",
  "resolved_by": "john.doe@company.com",
  "resolved_by_name": "John Doe"
}
```

**Response:**
```json
{
  "status": "success",
  "item_id": 12345,
  "composite_key": "771903_18296704_154771_2024-08_run_20260706_141025"
}
```

**Batch Payload (Multiple Months):**
```json
{
  "fix_label": "Proration (move-in or move-out)",
  "action_type": "internal",
  "notes": "Expected prorations for mid-month move-in",
  "exceptions": [
    {
      "run_id": "run_20260706_141025",
      "property_id": 771903,
      "lease_interval_id": 18296704,
      "ar_code_id": "154771",
      "audit_month": "2024-08",
      "exception_type": "AMOUNT_MISMATCH",
      "variance": -234.56
    },
    {
      "run_id": "run_20260706_141025",
      "property_id": 771903,
      "lease_interval_id": 18296704,
      "ar_code_id": "154771",
      "audit_month": "2024-09",
      "exception_type": "AMOUNT_MISMATCH",
      "variance": -234.56
    }
  ]
}
```

---

### Frontend Implementation (JavaScript)

**Resolution Dropdown Builder:**
```javascript
function buildResolutionDropdown(exceptionType) {
  const options = {
    'SCHEDULED_NOT_BILLED': [
      { label: 'Post missing charge in Entrata', action: 'external' },
      { label: 'Correct schedule dates in Entrata', action: 'external' },
      { label: 'Lease renewal not yet processed', action: 'external' },
      { label: 'Resident moved out – charge not applicable', action: 'internal' },
      { label: 'Month-to-month – no active schedule', action: 'external' },
      { label: 'Concession / waiver approved', action: 'internal' }
    ],
    'BILLED_NOT_SCHEDULED': [
      { label: 'Add recurring charge to schedule', action: 'external' },
      { label: 'Reverse incorrect charge', action: 'external' },
      { label: 'Move-in proration', action: 'internal' },
      { label: 'Backdated / corrected posting', action: 'internal' },
      { label: 'Mark as one-time charge', action: 'one_time' }
    ],
    'AMOUNT_MISMATCH': [
      { label: 'Correct scheduled amount in Entrata', action: 'external' },
      { label: 'Lease renewal amount not updated in schedule', action: 'external' },
      { label: 'Adjust posted transaction', action: 'external' },
      { label: 'Proration (move-in or move-out)', action: 'internal' },
      { label: 'Rent concession applied', action: 'internal' },
      { label: 'Document variance reason', action: 'internal' }
    ]
  };
  
  return options[exceptionType] || [];
}
```

**Batch Resolution Handler:**
```javascript
async function applyBatchResolution(fixLabel, actionType) {
  // Get all checked exception checkboxes
  const selectedMonths = document.querySelectorAll('.exception-checkbox:checked');
  
  const exceptions = Array.from(selectedMonths).map(checkbox => {
    const row = checkbox.closest('.exception-row');
    return {
      run_id: row.dataset.runId,
      property_id: parseInt(row.dataset.propertyId),
      lease_interval_id: parseInt(row.dataset.leaseId),
      ar_code_id: row.dataset.arCodeId,
      audit_month: row.dataset.auditMonth,
      exception_type: row.dataset.exceptionType,
      variance: parseFloat(row.dataset.variance)
    };
  });
  
  const response = await fetch('/api/exception-months/batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      fix_label: fixLabel,
      action_type: actionType,
      notes: document.getElementById('resolutionNotes').value,
      exceptions: exceptions
    })
  });
  
  if (response.ok) {
    // Show success toast
    showToast(`${exceptions.length} exceptions resolved`, 'success');
    // Refresh page to show resolved badges
    location.reload();
  }
}
```

---

### Resolution Analytics (Future Enhancement)

**Tracking Resolution Patterns:**
- Which resolution categories are most common? (e.g., 60% are "Post missing charge")
- Which properties have highest exception rates?
- Which AR codes cause most issues? (e.g., Utilities always have amount mismatches)
- Resolution velocity: How long until external actions completed?

**Dashboard Metrics:**
- Total Exceptions This Month: 1,247
- Resolved: 892 (71%)
- Pending External Action: 287 (23%)
- Pending Investigation: 68 (6%)
- Most Common Fix: "Post missing charge" (487 instances)

**Trend Analysis:**
- August historically has 3x more exceptions (lease renewals)
- Property 771903 has 15% exception rate vs 5% portfolio average
- Parking charges: 40% unbilled rate (schedule gap identified)

---

### Implementation Checklist

**Backend:**
- [ ] Create ExceptionMonths SharePoint list with proper schema
- [ ] Add indexed columns (RunId, PropertyId, LeaseIntervalId, AuditMonth)
- [ ] Implement `/api/exception-months` POST endpoint (single resolution)
- [ ] Implement `/api/exception-months/batch` POST endpoint (batch resolution)
- [ ] Add historical resolution query logic (lookup previous fixes)
- [ ] Write unit tests for resolution data structure

**Frontend:**
- [ ] Build resolution dropdown component with ranked suggestions
- [ ] Implement checkbox selection for batch resolution
- [ ] Add notes modal for optional user explanations
- [ ] Display resolved badges on transaction rows
- [ ] Show historical resolution indicators (checkmark + subtext)
- [ ] Add success/error toast notifications
- [ ] Test batch resolution flow (select 5 months, apply same fix)

**Testing:**
- [ ] Test all 17 resolution categories (6 + 5 + 6)
- [ ] Verify action type classification (external/internal/one_time)
- [ ] Test batch resolution (select multiple months, apply once)
- [ ] Test historical matching (run audit, resolve, run again, verify previous resolution shows)
- [ ] Test notes persistence (add notes, reload page, notes still visible)
- [ ] Test resolved badge display (green checkmark, user name, timestamp)

---

## My Storage Strategy

**Using SharePoint Lists exclusively** (no file-based storage like Parquet/CSV):

**Why Starting Fresh:**
- Old project used Parquet files which caused 10-15 minute delays before data was available
- Want to use SharePoint lists for immediate availability (write incrementally during audit)
- Simpler architecture: all data in lists, no file I/O complexity
- Better query performance with proper list indexing

**FINAL DECISION: Single Transaction Detail List with Read-Through Cache**

**AuditTransactionDetail List:**
- Single list with `DetailType` column (EXPECTED/ACTUAL)
- Write rows incrementally during audit (batch every 20 rows via Graph API)
- Query filtered by RunID + LeaseID + DetailType on lease view
- ✅ Immediate availability, simple architecture
- ✅ Proper indexing prevents 5K query degradation
- ✅ 90-day retention policy prevents hitting 30M item limit
- ✅ SharePoint is source of truth - survives app restarts

**Caching Strategy:**
- Flask-Caching with 4-hour TTL (in-memory simple cache)
- Read-through pattern: Check cache → if miss, query SharePoint → populate cache
- Write-through pattern: Write to SharePoint → invalidate cache for that RunID
- Cache improves performance, SharePoint ensures durability

**Why this approach:**
- **Simpler than separate lists**: One query source, easier joins between expected/actual
- **More durable than cache-only**: Data survives app restarts
- **Better than background writes**: Synchronous writes before marking property viewable (no silent failures)
- **Scalable**: Proper indexing + retention policy handles high volume

## Architecture Summary - All Decisions Finalized

**✅ Storage Architecture (Decided):**
- **Single `AuditTransactionDetail` SharePoint list** with `DetailType` column (EXPECTED/ACTUAL)
- **Per-property incremental writes**: Each property becomes viewable when its data completes (not after entire audit)
- **AuditRunStatus tracking**: Status progression controls viewability (Queued → Fetching → Reconciling → Writing Detail → Viewable)
- **Synchronous writes before marking viewable**: No silent background threads
- **Read-through cache with 4-hour TTL**: Flask-Caching for performance, SharePoint for durability
- **Graph API batching**: 20 rows per batch, proper retry logic for 429/503 errors
- **Proper indexing on all filter columns**: Prevents 5K query degradation (RunId, PropertyId, LeaseIntervalId, ArCodeId, AuditMonth, DetailType)
- **90-day retention policy**: Prevents hitting 30M item limit

**✅ Three SharePoint Lists (All Schemas Defined):**

1. **AuditRunStatus** (14 columns):
   - RunId, AuditType, PropertyId, PropertyName, LeaseIntervalId
   - Status (Queued/Fetching/Reconciling/Writing Detail/Viewable/Complete/Failed)
   - IsViewable, StartedAt, CompletedAt, ExpectedRowsWritten, ActualRowsWritten, SnapshotRowsWritten
   - ErrorMessage, LastUpdated

2. **AuditTransactionDetail** (19 columns):
   - RunId, DetailType (EXPECTED/ACTUAL), PropertyId, PropertyName, LeaseIntervalId, ResidentName, UnitNumber
   - ArCodeId, ArCodeName, AuditMonth, Amount, PostDate
   - TransactionId (null for EXPECTED), ScheduledChargeId (null for ACTUAL)
   - PeriodStartDate, PeriodEndDate, RowKey, Created, Modified

3. **RunDisplaySnapshots** (18 columns):
   - RunId, ScopeType (Portfolio/Property/Lease/Month), PropertyId, PropertyName, LeaseIntervalId, ResidentName
   - ArCodeId, ArCodeName, AuditMonth
   - ExpectedTotal, ActualTotal, Variance, Undercharge, Overcharge, ExceptionCount, MatchRate
   - Created, Modified

4. **ExceptionMonths** (17 columns):
   - CompositeKey, PropertyId, PropertyName, LeaseIntervalId, ResidentName, UnitNumber
   - ArCodeId, ArCodeName, AuditMonth, ExceptionType, Status, FixLabel, ActionType
   - Variance, Notes, ResolvedBy, ResolvedAt

**✅ Three Audit Types (All Fully Specified):**
- **Run Property Audit**: Single property selector, all leases
- **Run Bulk Audit**: Multi-property checkboxes with real-time progress updates
- **Run Single Lease Audit**: Property + Lease ID inputs, fast targeted audit

**✅ Exception Resolution Workflow (Complete):**
- 17 resolution categories across 3 exception types (SCHEDULED_NOT_BILLED: 6 options, BILLED_NOT_SCHEDULED: 5 options, AMOUNT_MISMATCH: 6 options)
- Action type classification (external/internal/one_time)
- Historical resolution matching (suggest same fix for repeated exceptions)
- 7-step user workflow (Audit → View exceptions → Open modal → Select category → Add notes → Save → Background update)

**✅ Implementation Build Order (30-Day Plan Defined):**
- Phase 1-2: Flask shell + base template
- Phase 3-4: Run audit page + Entrata API client
- Phase 5-6: Data normalization + reconciliation engine
- Phase 7-13: SharePoint helpers + incremental writes + status tracking
- Phase 14-16: UI views (portfolio/property/lease) + exception modal + polish

**✅ Performance Targets:**
- Transaction detail visible in 2 minutes (not 30 minutes)
- Lease detail page loads in <2 seconds
- Bulk audit: Property 1 viewable while Property 2 still processing
- Query performance: Fast with millions of rows (proper indexing)

## Flask Application Design & Architecture

### Application Structure

**Flask App Layout:**
```
LeaseFileAudit/
├── app.py                          # Application entry point, Flask app factory
├── run_app.ps1                     # Launcher script (clears port, starts app, opens browser)
├── config.py                       # Configuration classes (Dev, Prod, ReconciliationSettings)
├── requirements.txt                # Python dependencies
├── web/
│   ├── __init__.py                # Blueprint registration
│   ├── views.py                   # All route handlers (home, portfolio, property, lease)
│   └── templates/
│       ├── base.html              # Base template with navigation
│       ├── home.html              # Audit run picker
│       ├── portfolio.html         # Portfolio dashboard
│       ├── property.html          # Property detail with lease table
│       ├── lease.html             # Lease detail with transaction tables
│       └── bulk_audit.html        # Bulk audit submission form
├── audit_engine/
│   ├── api_ingest.py              # Entrata API client
│   ├── mappings.py                # Field mapping functions
│   ├── normalize.py               # Data cleaning and validation
│   ├── expand.py                  # Scheduled charge expansion
│   ├── reconcile.py               # Three-tier matching algorithm
│   └── metrics.py                 # KPI calculations
├── storage/
│   ├── service.py                 # SharePoint Graph API client
│   └── cache.py                   # Flask-Caching configuration
└── static/
    ├── css/
    │   └── styles.css             # Custom styles
    └── js/
        └── app.js                 # Client-side interactions
```

### Flask App Configuration

**app.py** (Application Factory Pattern):
```python
from flask import Flask
from flask_caching import Cache
from waitress import serve
import config

cache = Cache()

def create_app(config_name='development'):
    app = Flask(__name__)
    
    # Load configuration
    if config_name == 'production':
        app.config.from_object(config.ProductionConfig)
    else:
        app.config.from_object(config.DevelopmentConfig)
    
    # Initialize extensions
    cache.init_app(app)
    
    # Register blueprints
    from web.views import main_bp
    app.register_blueprint(main_bp)
    
    return app

if __name__ == '__main__':
    app = create_app('production')
    
    # Use Waitress WSGI server (production-ready)
    print("Starting LeaseFileAudit on http://localhost:8000")
    serve(app, host='0.0.0.0', port=8000, threads=4)
```

**config.py** (Configuration Classes):
```python
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
    
    # Entrata API
    LEASE_API_KEY = os.getenv('LEASE_API_KEY')
    LEASE_API_BASE_URL = os.getenv('LEASE_API_BASE_URL')
    LEASE_API_DETAILS_URL = os.getenv('LEASE_API_DETAILS_URL')
    LEASE_API_AR_URL = os.getenv('LEASE_API_AR_URL')
    
    # SharePoint
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    SHAREPOINT_RUN_DISPLAY_SNAPSHOTS_LIST = os.getenv('SHAREPOINT_RUN_DISPLAY_SNAPSHOTS_LIST')
    SHAREPOINT_EXCEPTION_MONTHS_LIST = os.getenv('SHAREPOINT_EXCEPTION_MONTHS_LIST')
    
    # Azure AD
    AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
    AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
    
    # Caching
    CACHE_TYPE = 'simple'  # In-memory cache
    CACHE_DEFAULT_TIMEOUT = 14400  # 4 hours

class DevelopmentConfig(Config):
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    DEBUG = False
    TESTING = False

class ReconciliationSettings:
    """Business logic configuration for reconciliation"""
    ALLOWED_AR_CODES = [154771]  # Default: Rent only
    EXCLUDED_PROPERTIES = []
    MATCH_TOLERANCE = 0.01  # $0.01 tolerance for float comparison
    HIGH_SEVERITY_STATUSES = [
        'SCHEDULED_NOT_BILLED',
        'BILLED_NOT_SCHEDULED',
        'AMOUNT_MISMATCH'
    ]
```

### Route Handlers

**web/views.py** (Flask Blueprint):
```python
from flask import Blueprint, render_template, request, jsonify, session
from audit_engine.reconcile import execute_audit_run
from storage.service import get_portfolio_summary, get_property_summary, get_lease_detail
from storage.cache import cache

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def home():
    """Home page - shows list of recent audit runs"""
    recent_runs = get_recent_audit_runs()  # Query SharePoint for unique RunIDs
    return render_template('home.html', runs=recent_runs)

@main_bp.route('/bulk-audit', methods=['GET', 'POST'])
def bulk_audit():
    """Bulk audit submission form"""
    if request.method == 'GET':
        properties = get_property_picklist()  # From Entrata API
        return render_template('bulk_audit.html', properties=properties)
    
    # POST: Start audit job
    property_ids = request.form.getlist('property_ids')
    ar_codes = request.form.getlist('ar_codes', [154771])
    from_date = request.form.get('from_date')
    to_date = request.form.get('to_date')
    
    # Execute audit (synchronous for now)
    run_id = execute_audit_run(
        property_ids=property_ids,
        ar_codes=ar_codes,
        from_date=from_date,
        to_date=to_date
    )
    
    return jsonify({'status': 'complete', 'run_id': run_id})

@main_bp.route('/portfolio/<run_id>')
@cache.cached(timeout=14400, key_prefix=lambda: f"portfolio_{request.view_args['run_id']}")
def portfolio_view(run_id):
    """Portfolio dashboard - shows all properties for this audit"""
    data = get_portfolio_summary(run_id)
    return render_template('portfolio.html', 
                         run_id=run_id,
                         properties=data['properties'],
                         kpis=data['kpis'])

@main_bp.route('/property/<int:property_id>/run_<run_id>')
@cache.cached(timeout=14400)
def property_view(property_id, run_id):
    """Property detail - shows all leases for this property"""
    data = get_property_summary(property_id, run_id)
    return render_template('property.html',
                         run_id=run_id,
                         property_id=property_id,
                         property_name=data['property_name'],
                         leases=data['leases'],
                         kpis=data['kpis'])

@main_bp.route('/lease/<int:lease_id>/run_<run_id>')
@cache.cached(timeout=14400)
def lease_view(lease_id, run_id):
    """Lease detail - shows transaction-by-transaction breakdown"""
    data = get_lease_detail(lease_id, run_id)
    
    return render_template('lease.html',
                         run_id=run_id,
                         lease_id=lease_id,
                         resident_name=data['resident_name'],
                         customer_id=data['customer_id'],
                         expected_transactions=data['expected'],
                         actual_transactions=data['actual'],
                         ar_code_summaries=data['ar_codes'],
                         undercharge=data['undercharge'],
                         overcharge=data['overcharge'])

@main_bp.route('/api/exception-resolve', methods=['POST'])
def resolve_exception():
    """Mark an exception as resolved"""
    data = request.json
    
    # Write to ExceptionMonths SharePoint list
    write_exception_resolution(
        property_id=data['property_id'],
        lease_id=data['lease_id'],
        ar_code=data['ar_code'],
        audit_month=data['audit_month'],
        resolution_category=data['category'],
        notes=data['notes'],
        resolved_by=session.get('user_email')
    )
    
    return jsonify({'status': 'success'})
```

### Template Design (Jinja2)

**templates/base.html** (Master Layout):
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}LeaseFileAudit{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="{{ url_for('static', filename='css/styles.css') }}">
</head>
<body>
    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">LeaseFileAudit</a>
            <span class="navbar-text">Billing Reconciliation System</span>
        </div>
    </nav>
    
    <div class="container mt-4">
        {% block content %}{% endblock %}
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="{{ url_for('static', filename='js/app.js') }}"></script>
    {% block scripts %}{% endblock %}
</body>
</html>
```

**templates/lease.html** (Lease Detail View):
```html
{% extends "base.html" %}

{% block title %}Lease {{ lease_id }} - LeaseFileAudit{% endblock %}

{% block content %}
<div class="row">
    <div class="col-md-12">
        <h2>{{ resident_name }}</h2>
        <p class="text-muted">Lease ID: {{ lease_id }}</p>
        <a href="https://peakmade.entrata.com/users/{{ customer_id }}" 
           target="_blank" 
           class="btn btn-primary">
            Open in Entrata
        </a>
    </div>
</div>

<div class="row mt-4">
    <div class="col-md-6">
        <div class="card bg-danger text-white">
            <div class="card-body">
                <h5>Total Undercharge</h5>
                <h2>${{ "%.2f"|format(undercharge) }}</h2>
            </div>
        </div>
    </div>
    <div class="col-md-6">
        <div class="card bg-info text-white">
            <div class="card-body">
                <h5>Total Overcharge</h5>
                <h2>${{ "%.2f"|format(overcharge) }}</h2>
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-md-12">
        <h3>AR Code Details</h3>
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>AR Code</th>
                    <th>Name</th>
                    <th>Matched</th>
                    <th>Discrepancies</th>
                    <th>Undercharge</th>
                    <th>Overcharge</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for ar in ar_code_summaries %}
                <tr>
                    <td>{{ ar.ar_code_id }}</td>
                    <td>{{ ar.ar_code_name }}</td>
                    <td>{{ ar.matched_count }}</td>
                    <td>{{ ar.discrepancy_count }}</td>
                    <td class="text-danger">${{ "%.2f"|format(ar.undercharge) }}</td>
                    <td class="text-info">${{ "%.2f"|format(ar.overcharge) }}</td>
                    <td>
                        {% if ar.status == 'PASSED' %}
                            <span class="badge bg-success">✓ Passed</span>
                        {% else %}
                            <span class="badge bg-danger">✗ Failed</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>

<div class="row mt-4">
    <div class="col-md-6">
        <h4>Expected Transactions</h4>
        <table class="table table-sm">
            <thead>
                <tr>
                    <th>Period</th>
                    <th>Amount</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for txn in expected_transactions %}
                <tr class="{% if txn.status != 'MATCHED' %}table-warning{% endif %}">
                    <td>{{ txn.period }}</td>
                    <td>${{ "%.2f"|format(txn.amount) }}</td>
                    <td>
                        <span class="badge 
                            {% if txn.status == 'MATCHED' %}bg-success
                            {% elif txn.status == 'SCHEDULED_NOT_BILLED' %}bg-danger
                            {% else %}bg-warning{% endif %}">
                            {{ txn.status }}
                        </span>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    
    <div class="col-md-6">
        <h4>Actual Transactions</h4>
        <table class="table table-sm">
            <thead>
                <tr>
                    <th>Post Date</th>
                    <th>Amount</th>
                    <th>Transaction ID</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for txn in actual_transactions %}
                <tr class="{% if txn.status != 'MATCHED' %}table-warning{% endif %}">
                    <td>{{ txn.post_date }}</td>
                    <td>${{ "%.2f"|format(txn.amount) }}</td>
                    <td>{{ txn.transaction_id }}</td>
                    <td>
                        <span class="badge 
                            {% if txn.status == 'MATCHED' %}bg-success
                            {% elif txn.status == 'BILLED_NOT_SCHEDULED' %}bg-danger
                            {% else %}bg-warning{% endif %}">
                            {{ txn.status }}
                        </span>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
{% endblock %}
```

### Frontend Interactivity

**static/js/app.js** (Client-Side JavaScript):
```javascript
// Exception resolution modal
function openResolveModal(propertyId, leaseId, arCode, auditMonth) {
    const modal = new bootstrap.Modal(document.getElementById('resolveModal'));
    
    // Populate hidden form fields
    document.getElementById('propertyId').value = propertyId;
    document.getElementById('leaseId').value = leaseId;
    document.getElementById('arCode').value = arCode;
    document.getElementById('auditMonth').value = auditMonth;
    
    modal.show();
}

// Submit exception resolution
document.getElementById('resolveForm')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = {
        property_id: document.getElementById('propertyId').value,
        lease_id: document.getElementById('leaseId').value,
        ar_code: document.getElementById('arCode').value,
        audit_month: document.getElementById('auditMonth').value,
        category: document.getElementById('category').value,
        notes: document.getElementById('notes').value
    };
    
    const response = await fetch('/api/exception-resolve', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(formData)
    });
    
    if (response.ok) {
        location.reload();  // Refresh page to show resolved status
    }
});

// Bulk audit property multi-select
document.getElementById('selectAll')?.addEventListener('click', () => {
    document.querySelectorAll('.property-checkbox').forEach(cb => {
        cb.checked = true;
    });
});
```

### WSGI Server Configuration

**run_app.ps1** (PowerShell Launcher):
```powershell
# Kill any existing process on port 8000
$port = 8000
$process = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | 
           Select-Object -ExpandProperty OwningProcess -Unique
if ($process) {
    Stop-Process -Id $process -Force
    Start-Sleep -Seconds 2
}

# Start Flask app with Waitress
Write-Host "Starting LeaseFileAudit on port $port..."
$env:FLASK_ENV = "production"
python app.py

# Wait for health check
$maxRetries = 30
for ($i = 0; $i -lt $maxRetries; $i++) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$port" -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            Write-Host "App is ready!"
            Start-Process "http://localhost:$port"
            break
        }
    } catch {
        Start-Sleep -Seconds 1
    }
}
```

**Waitress Benefits Over Flask Dev Server:**
- ✅ Multi-threaded (handles concurrent requests)
- ✅ Production-ready (no debug mode issues)
- ✅ Windows-compatible (unlike gunicorn)
- ✅ Stable (no auto-reload crashes)

### Session Management & Authentication

**For Azure AD SSO:**
```python
from flask import session, redirect, url_for
from functools import wraps
import msal

def get_msal_app():
    return msal.ConfidentialClientApplication(
        client_id=app.config['AZURE_CLIENT_ID'],
        client_credential=app.config['AZURE_CLIENT_SECRET'],
        authority=f"https://login.microsoftonline.com/{app.config['AZURE_TENANT_ID']}"
    )

@main_bp.route('/login')
def login():
    auth_app = get_msal_app()
    auth_url = auth_app.get_authorization_request_url(
        scopes=["User.Read"],
        redirect_uri=url_for('main.auth_callback', _external=True)
    )
    return redirect(auth_url)

@main_bp.route('/auth/callback')
def auth_callback():
    auth_app = get_msal_app()
    result = auth_app.acquire_token_by_authorization_code(
        code=request.args.get('code'),
        scopes=["User.Read"],
        redirect_uri=url_for('main.auth_callback', _external=True)
    )
    
    session['user_email'] = result['id_token_claims']['preferred_username']
    return redirect('/')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_email' not in session:
            return redirect(url_for('main.login'))
        return f(*args, **kwargs)
    return decorated_function
```

### Error Handling

**Custom Error Pages:**
```python
@main_bp.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@main_bp.errorhandler(500)
def internal_error(error):
    app.logger.error(f"Internal error: {error}")
    return render_template('500.html'), 500

# Application-level error logging
import logging
from logging.handlers import RotatingFileHandler

if not app.debug:
    handler = RotatingFileHandler('logs/leasefileaudit.log', maxBytes=10000000, backupCount=3)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
```

### Key Design Principles

1. **Blueprint Pattern**: All routes in single `main_bp` blueprint (can split later if needed)
2. **Template Inheritance**: `base.html` provides consistent navigation and layout
3. **RESTful Routes**: `/resource/<id>/context_<context_id>` pattern for clarity
4. **Caching Decorators**: `@cache.cached()` on expensive queries (4-hour TTL)
5. **Waitress WSGI**: Production-ready server (multi-threaded, stable)
6. **Bootstrap 5**: Responsive UI with minimal custom CSS
7. **AJAX for Modals**: Exception resolution without page reload
8. **Read-Through Cache**: Check cache → query SharePoint → populate cache
9. **Session Storage**: User email for audit history tracking
10. **Error Logging**: Rotating file handler for production debugging

---

## Standard Base Template (MANDATORY - Use This Exact Structure)

**CRITICAL:** The new project MUST use this exact base.html template structure. This is the proven PeakMade design pattern used across all applications.

**⚠️ IMPORTANT DISTINCTION:**
- ✅ **UI/Templates**: USE the standard Base.html template (proven, reusable across apps)
- ❌ **Data Storage**: DO NOT copy from old LeaseFileAudit or any other app
  - OLD LeaseFileAudit had problematic storage (AuditRuns2, CSV, Parquet - all bad)
  - NEW app needs FRESH SharePoint-based architecture designed from scratch
  - Storage rules are PROJECT-SPECIFIC, not reusable like UI templates

### Base Template Structure

```html
<!-- templates/base.html -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Lease File Audit{% endblock %}</title>
    
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Font Awesome -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <!-- Google Fonts - Montserrat -->
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap" rel="stylesheet">
    
    <style>
        /* Standard PeakMade Styling */
        body {
            margin: 0;
            padding: 0;
            font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #f8f9fa;
        }
        
        /* Main content styling */
        .main-content {
            padding: 0;
            margin: 0;
            min-height: calc(100vh - 80px);
            background-color: #f8f9fa;
        }
        
        .content-wrapper {
            width: 100%;
            max-width: none;
            padding: 15px 40px;
            margin: 0;
        }
        
        /* Card styling - PeakMade teal gradient headers */
        .card {
            border: none;
            border-radius: 12px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        
        .card-header {
            background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%);
            /* PeakMade signature teal gradient */
            color: white;
            border-radius: 12px 12px 0 0 !important;
            border: none;
            padding: 12px 20px;
            font-size: 1.2rem;
            font-weight: 700;
        }
        
        .card-body {
            padding: 25px;
        }
        
        /* Button styling */
        .btn-primary {
            background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%);
            border: none;
            border-radius: 8px;
            padding: 10px 25px;
            font-weight: 500;
            transition: all 0.3s ease;
            color: white;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0, 168, 200, 0.4);
            background: linear-gradient(135deg, #0088a8 0%, #00a8c8 100%);
        }
        
        .btn-success {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            border: none;
            border-radius: 8px;
            padding: 10px 25px;
            font-weight: 500;
            color: white;
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #c20068 0%, #a00056 100%);
            /* PeakMade magenta gradient */
            border: none;
            border-radius: 8px;
            padding: 10px 25px;
            font-weight: 500;
            color: white;
        }
        
        .btn-secondary {
            background: linear-gradient(135deg, #231f20 0%, #3d3a3b 100%);
            border: none;
            border-radius: 8px;
            padding: 10px 25px;
            font-weight: 500;
            color: white;
        }
        
        /* Animation effects */
        .fade-in {
            animation: fadeInUp 0.6s ease-out forwards;
        }
        
        @keyframes fadeInUp {
            0% {
                opacity: 0;
                transform: translateY(20px);
            }
            100% {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        /* User info dropdown */
        .user-badge {
            background: white;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 6px 12px;
            display: flex;
            align-items: center;
            gap: 8px;
            cursor: pointer;
            transition: all 0.2s ease;
            color: #231f20;
        }
        
        .user-badge:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
            border-color: #00a8c8;
        }
        
        /* Alert styling */
        .alert {
            border-radius: 8px;
            border: none;
            margin-bottom: 20px;
        }
        
        .alert-success {
            background: linear-gradient(135deg, #d4f7f2 0%, #b8f3e9 100%);
            color: #006b5a;
            padding: 7px 18px;
            font-size: 1rem;
            border-left: 4px solid #00a8c8;
        }
        
        .alert-danger {
            background: linear-gradient(135deg, #ffe6f2 0%, #ffd4e9 100%);
            color: #8a0048;
            border-left: 4px solid #c20068;
        }
        
        .alert-info {
            background: linear-gradient(135deg, #e6f7fb 0%, #d4f1f7 100%);
            color: #006b7a;
            border-left: 4px solid #00a8c8;
        }
    </style>
    
    {% block extra_css %}{% endblock %}
</head>
<body>
    <!-- PeakMade Standard Header -->
    <header style="background: linear-gradient(90deg, #ffb6b6 0%, #1f51ff 50%, #90eeff 100%);
                   box-shadow: 0 2px 10px rgba(0,0,0,0.05);
                   height: 80px;
                   padding: 0;
                   margin-bottom: 0;
                   position: sticky;
                   top: 0;
                   z-index: 1000;">
        <div style="display: flex; align-items: center; justify-content: space-between; max-width: 100%; margin: 0 auto; padding: 0 40px;">
            
            <!-- Left Logo (Onyx) -->
            <div style="flex: 1; display: flex; justify-content: flex-start; align-items: center; height: 80px;">
                <img src="{{ url_for('static', filename='onyx_logo_transparent.png') }}" 
                     alt="Onyx Logo" 
                     style="height: 48px; width: auto;">
            </div>
            
            <!-- Center Title -->
            <div style="flex: 1; display: flex; justify-content: center; align-items: center; height: 80px;">
                <h1 style="font-size: 28px; 
                           font-family: 'Montserrat', Arial, sans-serif; 
                           font-weight: 700; 
                           color: #fff; 
                           margin: 0; 
                           letter-spacing: 2px; 
                           text-shadow: 0 2px 6px rgba(0,0,0,0.12); 
                           line-height: 80px;">
                    {% block app_title %}Lease File Audit{% endblock %}
                </h1>
            </div>
            
            <!-- Right Logo (Redpoint) -->
            <div style="flex: 1; display: flex; justify-content: flex-end; align-items: center; height: 80px;">
                <img src="{{ url_for('static', filename='Redpoint_logo.png') }}" 
                     alt="Redpoint Logo" 
                     style="height: 32px; width: auto;">
            </div>
        </div>
    </header>
    
    <!-- Action Buttons (positioned below header, top-right) -->
    <div style="position: fixed; top: 88px; right: 20px; z-index: 999; display: flex; align-items: center; gap: 8px;">
        {% block action_buttons %}{% endblock %}
        
        {% if user %}
        <div class="user-badge">
            <i class="fas fa-user"></i>
            <span>{{ user.name }}</span>
            <i class="fas fa-chevron-down"></i>
        </div>
        {% endif %}
    </div>

    <!-- Main Content Area -->
    <main class="main-content">
        <div class="content-wrapper">
            <!-- Flash Messages -->
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    <div style="display: flex; justify-content: center; padding: 15px 20px 0;">
                        <div style="max-width: 800px; width: 100%;">
                            {% for category, message in messages %}
                                <div class="alert alert-{{ 'danger' if category == 'error' else category }} alert-dismissible fade show" role="alert">
                                    {{ message }}
                                    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                                </div>
                            {% endfor %}
                        </div>
                    </div>
                {% endif %}
            {% endwith %}
            
            <!-- Page Content -->
            {% block content %}{% endblock %}
        </div>
    </main>

    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    
    {% block extra_js %}{% endblock %}
</body>
</html>
```

### Key Template Features

**1. Signature Header (MUST PRESERVE):**
- Gradient: `linear-gradient(90deg, #ffb6b6 0%, #1f51ff 50%, #90eeff 100%)`
  - Pink (#ffb6b6) → Blue (#1f51ff) → Cyan (#90eeff)
- Height: 80px, sticky positioning (stays at top when scrolling)
- Three-part layout:
  - **Left**: Onyx logo (48px height)
  - **Center**: App title (Montserrat font, 28px, white, letter-spacing 2px)
  - **Right**: Redpoint logo (32px height)

**2. Card Headers (PeakMade Teal Gradient):**
- Background: `linear-gradient(135deg, #00a8c8 0%, #0088a8 100%)`
- Border-radius: 12px (top corners only)
- White text, bold font

**3. Button Styling:**
- **Primary**: Teal gradient `#00a8c8 → #0088a8`
- **Danger**: Magenta gradient `#c20068 → #a00056`
- **Success**: Green gradient `#10b981 → #059669`
- Hover effect: `translateY(-2px)` with enhanced shadow
- Border-radius: 8px

**4. Content Block Structure:**
```html
{% block app_title %}{% endblock %}    <!-- Header title -->
{% block action_buttons %}{% endblock %} <!-- Top-right buttons -->
{% block content %}{% endblock %}        <!-- Main page content -->
{% block extra_css %}{% endblock %}      <!-- Additional CSS -->
{% block extra_js %}{% endblock %}       <!-- Additional JavaScript -->
```

**5. Logo Assets Required:**
- `static/onyx_logo_transparent.png` (left logo, 48px height)
- `static/Redpoint_logo.png` (right logo, 32px height)
- `static/Peakmade_logo.png` (optional, for login screens)

### Template Usage Examples

**Homepage Template:**
```html
{% extends "base.html" %}

{% block title %}Home - Lease File Audit{% endblock %}

{% block app_title %}Lease File Audit{% endblock %}

{% block action_buttons %}
    <a href="{{ url_for('main.settings') }}" class="btn btn-sm btn-secondary">
        <i class="fas fa-cog"></i> Settings
    </a>
{% endblock %}

{% block content %}
<div class="container-fluid">
    <div class="row">
        <div class="col-md-12">
            <div class="card fade-in">
                <div class="card-header">
                    <i class="fas fa-home me-2"></i>Recent Audit Runs
                </div>
                <div class="card-body">
                    <!-- Content here -->
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
```

**Property Detail Template:**
```html
{% extends "base.html" %}

{% block title %}{{ property_name }} - Lease Audit{% endblock %}

{% block app_title %}Lease File Audit{% endblock %}

{% block action_buttons %}
    <a href="{{ url_for('main.portfolio', run_id=run_id) }}" class="btn btn-sm btn-secondary">
        <i class="fas fa-arrow-left"></i> Back to Portfolio
    </a>
{% endblock %}

{% block content %}
<!-- Property-specific content -->
{% endblock %}
```

---

## Modern UI/UX Design (Enhanced Visual Experience)

**How This Section Works:**
The Standard Base.html template (above) provides the **foundation** (header, buttons, cards, alerts). This section shows **page-specific enhancements** you build ON TOP of that base:

- **Base Template** = Reusable shell (header, nav, footer, common styles)
- **Page Templates** = Extend base.html and add custom content (hero sections, stat cards, charts, grids)
- **Custom CSS** = Additional styling for page-specific components (dashboard hero, progress rings, timeline)

Think of it like: Base.html is the house frame, these enhancements are the custom furniture and decorations for each room.

---

### Design Philosophy

**Goals for New UI:**
- 🎨 Modern, clean aesthetic (not generic Bootstrap tables)
- 📊 Data visualization with charts and graphs
- 🎯 Visual hierarchy guides user attention to important metrics
- 🔄 Smooth animations and transitions
- 📱 Fully responsive (desktop, tablet, mobile)
- ⚡ Interactive components (collapsible sections, sliding tiles)
- 🎭 Professional color scheme matching business context
- 🏢 **Maintain PeakMade brand identity** (your existing app's look and feel)

**PeakMade Visual Identity (From Current App):**

**Brand Colors:**
```css
:root {
    --color-primary: #00a8c8;      /* Teal/Cyan - main brand color */
    --color-secondary: #231f20;     /* Dark gray/black - text */
    --color-accent: #ff6600;        /* Orange - highlights */
    --color-danger: #c20068;        /* Magenta - errors */
    --color-success: #10b981;       /* Emerald - success states */
    --color-warning: #f59e0b;       /* Amber - warnings */
    --color-light: #f4f4f4;         /* Light gray - backgrounds */
}
```

**Header Gradient (Signature Style):**
```css
background: linear-gradient(90deg, #ffb6b6 0%, #1f51ff 50%, #90eeff 100%);
/* Pink → Blue → Cyan gradient - distinctive PeakMade header */
```

**Progress Bar Gradient:**
```css
background: linear-gradient(90deg, #00a8c8 0%, #41c5de 45%, #1f51ff 100%);
/* Cyan → Light Cyan → Blue - used for loading states */
```

**Card Headers:**
```css
background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%);
/* Teal gradient - consistent across all cards */
```

**Typography:**
- Font Family: `'Montserrat', sans-serif` (400, 700 weights)
- Headers: Bold (700), color #231f20
- Body: Regular (400), color #64748b

**Component Styling:**
- Cards: Border-radius 12px, box-shadow `0 4px 15px rgba(0,0,0,0.1)`
- Buttons: Border-radius 8px, gradient backgrounds, hover transform `translateY(-2px)`
- Badges: Border-radius 8px, padding `0.35em 0.65em`
- Header: Sticky top, height 80px, PeakMade logo on left

**Key Design Elements to Preserve:**
1. ✅ Header gradient (pink-blue-cyan) - signature PeakMade look
2. ✅ Teal (#00a8c8) as primary action color
3. ✅ Magenta (#c20068) for critical errors/danger
4. ✅ Orange (#ff6600) for accent elements
5. ✅ Card headers with teal gradient
6. ✅ Montserrat font family
7. ✅ Smooth hover animations with transform
8. ✅ Drawer/sliding panel pattern (1100px width, right side)

**What to Modernize (Keep Brand, Enhance UX):**
- ❌ Plain HTML tables → Card grids with visual indicators
- ❌ Flat KPI displays → Stat cards with icons and trends
- ❌ No data visualization → Add Chart.js graphs
- ❌ Static accordions → Smooth sliding tiles with animations
- ❌ Basic badges → Rich status indicators with colors
- ✅ Keep signature gradients and brand colors throughout

---

### 1. Enhanced Dashboard (Home Page)

**Current Problem:** Simple list of audit runs - boring, hard to scan

**New Dashboard Improvements (Beyond Old App):**
- ✅ **Hero Section**: Eye-catching header with primary CTA button (Start New Audit)
- ✅ **Quick Stats Cards**: 4 KPI cards showing recent audits, avg match rate, undercharges, properties audited
- ✅ **Visual Indicators**: Color-coded status markers (green/yellow/red) based on match rate thresholds
- ✅ **Timeline View**: Enhanced audit history with colored markers and hover effects
- ✅ **Inline Metrics**: Each audit run shows undercharge/overcharge/exceptions without drilling down
- ✅ **Click-to-Navigate**: Entire timeline cards are clickable to portfolio view
- ✅ **Trend Indicators**: Optional trend arrows showing improvement/decline from previous audit
- ✅ **Search/Filter**: (Optional) Quick search box to filter audit runs by date or property

**New Design (With PeakMade Branding):**

```html
<!-- templates/home.html -->
{% extends "base.html" %}

{% block title %}Dashboard - Lease File Audit{% endblock %}

{% block app_title %}Lease File Audit{% endblock %}

{% block action_buttons %}
    <!-- Optional: Add quick action buttons in top-right -->
    <a href="{{ url_for('main.settings') }}" class="btn btn-sm btn-secondary">
        <i class="fas fa-cog"></i> Settings
    </a>
{% endblock %}

{% block content %}
<div class="dashboard-hero">
    <div class="hero-content">
        <h1 class="display-4">Lease Billing Audit System</h1>
        <p class="lead">Automated reconciliation for 94 properties, 120K+ leases</p>
        <button class="btn btn-primary btn-lg" onclick="location.href='/bulk-audit'">
            <i class="fas fa-plus-circle"></i> Start New Audit
        </button>
    </div>
</div>

<!-- Quick Stats Cards (PeakMade style) -->
<div class="row g-4 mb-5">
    <div class="col-md-3">
        <div class="stat-card stat-card-primary">
            <div class="stat-icon">
                <i class="fas fa-play-circle"></i>
            </div>
            <div class="stat-content">
                <h3>{{ recent_audits_count }}</h3>
                <p>Recent Audits</p>
            </div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="stat-card stat-card-success">
            <div class="stat-icon">
                <i class="fas fa-check-circle"></i>
            </div>
            <div class="stat-content">
                <h3>{{ avg_match_rate }}%</h3>
                <p>Avg Match Rate</p>
            </div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="stat-card stat-card-danger">
            <div class="stat-icon">
                <i class="fas fa-exclamation-triangle"></i>
            </div>
            <div class="stat-content">
                <h3>${{ total_undercharge | format_currency }}</h3>
                <p>Total Undercharge</p>
            </div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="stat-card stat-card-accent">
            <div class="stat-icon">
                <i class="fas fa-chart-line"></i>
            </div>
            <div class="stat-content">
                <h3>{{ properties_audited }}</h3>
                <p>Properties Audited</p>
            </div>
        </div>
    </div>
</div>

<!-- Recent Audits Timeline (Enhanced Cards) -->
<div class="card shadow-lg border-0">
    <div class="card-header">
        <h4 class="mb-0"><i class="fas fa-history me-2"></i>Recent Audit Runs</h4>
    </div>
    <div class="card-body p-0">
        <div class="audit-timeline">
            {% for run in recent_runs %}
            <div class="timeline-item" onclick="location.href='/portfolio/{{ run.run_id }}'">
                <div class="timeline-marker 
                    {% if run.match_rate >= 98 %}marker-success
                    {% elif run.match_rate >= 95 %}marker-warning
                    {% else %}marker-danger{% endif %}">
                </div>
                <div class="timeline-content">
                    <div class="d-flex justify-content-between align-items-start">
                        <div>
                            <h5 class="mb-1">{{ run.run_id }}</h5>
                            <p class="text-muted mb-2">
                                <i class="fas fa-calendar"></i> {{ run.run_date | format_date }}
                                <span class="mx-2">•</span>
                                <i class="fas fa-building"></i> {{ run.property_count }} properties
                            </p>
                        </div>
                        <div class="text-end">
                            <div class="match-rate-badge 
                                {% if run.match_rate >= 98 %}bg-success
                                {% elif run.match_rate >= 95 %}bg-warning
                                {% else %}bg-brand-danger{% endif %}">
                                {{ run.match_rate }}% Match
                            </div>
                        </div>
                    </div>
                    <div class="row g-3 mt-2">
                        <div class="col">
                            <small class="text-muted d-block">Undercharge</small>
                            <strong class="text-brand-danger">${{ run.undercharge | format_currency }}</strong>
                        </div>
                        <div class="col">
                            <small class="text-muted d-block">Overcharge</small>
                            <strong class="text-brand-primary">${{ run.overcharge | format_currency }}</strong>
                        </div>
                        <div class="col">
                            <small class="text-muted d-block">Exceptions</small>
                            <strong>{{ run.exception_count }}</strong>
                        </div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
</div>
{% endblock %}
```

**CSS for Dashboard (PeakMade Brand):**
```css
/* static/css/styles.css */

/* Use PeakMade brand colors */
.text-brand-primary { color: #00a8c8 !important; }
.text-brand-danger { color: #c20068 !important; }
.text-brand-accent { color: #ff6600 !important; }
.bg-brand-primary { background-color: #00a8c8 !important; }
.bg-brand-danger { background-color: #c20068 !important; }
.bg-brand-accent { background-color: #ff6600 !important; }

.dashboard-hero {
    background: linear-gradient(90deg, #ffb6b6 0%, #1f51ff 50%, #90eeff 100%);
    /* PeakMade signature header gradient */
    color: white;
    padding: 60px 0;
    border-radius: 12px;
    margin-bottom: 40px;
    box-shadow: 0 10px 30px rgba(30, 58, 138, 0.3);
}

.hero-content {
    text-align: center;
}

.stat-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    /* Match your current card shadow */
    display: flex;
    align-items: center;
    gap: 20px;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
    position: relative;
    overflow: hidden;
}

.stat-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 4px;
    height: 100%;
    background: var(--color-primary);
}

.stat-card-primary::before { background: linear-gradient(180deg, #00a8c8 0%, #0088a8 100%); }
.stat-card-success::before { background: #10b981; }
.stat-card-danger::before { background: #c20068; }
.stat-card-accent::before { background: #ff6600; }

.stat-card:hover {
    transform: translateY(-4px);
    /* Match your button hover effect */
    box-shadow: 0 8px 25px rgba(0, 168, 200, 0.3);
}

.stat-icon {
    width: 64px;
    height: 64px;
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 28px;
    color: white;
    background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%);
    /* PeakMade card header gradient */
}

.stat-card-success .stat-icon {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
}

.stat-card-danger .stat-icon {
    background: linear-gradient(135deg, #c20068 0%, #a00056 100%);
}

.stat-card-accent .stat-icon {
    background: linear-gradient(135deg, #ff6600 0%, #cc5200 100%);
}

.stat-content h3 {
    font-family: 'Montserrat', sans-serif;
    font-size: 32px;
    font-weight: 700;
    margin: 0;
    color: #231f20;
}

.stat-content p {
    margin: 0;
    color: #64748b;
    font-size: 14px;
}

.card {
    border: none;
    border-radius: 12px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    /* Keep your current card styling */
}

.card-header {
    background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%);
    /* PeakMade card header gradient */
    color: white;
    border-radius: 12px 12px 0 0 !important;
    border: none;
    padding: 12px 20px;
    font-family: 'Montserrat', sans-serif;
    font-weight: 700;
}

.audit-timeline {
    padding: 0;
}

.timeline-item {
    padding: 24px;
    border-bottom: 1px solid #e2e8f0;
    display: flex;
    gap: 20px;
    cursor: pointer;
    transition: background 0.2s ease;
}

.timeline-item:hover {
    background: linear-gradient(135deg, #e6f7fb 0%, #d4f1f7 100%);
    /* Light teal gradient on hover */
}

.timeline-marker {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    margin-top: 6px;
    flex-shrink: 0;
}

.marker-success { background: #10b981; }
.marker-warning { background: #f59e0b; }
.marker-danger { background: #c20068; }

.timeline-content {
    flex: 1;
}

.match-rate-badge {
    padding: 8px 16px;
    border-radius: 8px;
    /* Match your badge radius */
    color: white;
    font-weight: 600;
    font-size: 14px;
}
```

**Dashboard Summary:**
The new dashboard uses the **Standard Base.html template** (PeakMade header, card styling, buttons) but adds:
1. Custom hero section with signature gradient background
2. Stat cards with icons and colored left borders
3. Timeline layout with interactive hover states
4. Inline KPI display (no need to click into each audit to see metrics)

This gives you the proven PeakMade look with modern, data-rich dashboard functionality.

---

### 2. Enhanced Portfolio View (Property Grid with PeakMade Styling)

**Current Problem:** Plain table of properties - no visual context

**New Portfolio Improvements (Beyond Old App):**
- ✅ **KPI Cards Row**: Portfolio-level metrics at top (match rate, undercharge, overcharge, exceptions)
- ✅ **Data Visualization**: Chart.js bar chart (match rate by property) + pie chart (exception breakdown)
- ✅ **Property Grid**: Card-based layout instead of boring table
- ✅ **Progress Rings**: Circular SVG progress indicators showing match rate % visually
- ✅ **Color-Coded Cards**: Green/yellow/red based on performance thresholds
- ✅ **Hover Effects**: Cards lift and highlight on hover
- ✅ **Grid/List Toggle**: (Optional) Switch between grid and list views

**New Design:**

```html
<!-- templates/portfolio.html -->
{% extends "base.html" %}

{% block content %}
<!-- Portfolio KPI Cards -->
<div class="row g-4 mb-4">
    <div class="col-md-3">
        <div class="kpi-card border-left-primary">
            <div class="kpi-label">Match Rate</div>
            <div class="kpi-value">{{ kpis.match_rate }}%</div>
            <div class="kpi-trend">
                <i class="bi bi-arrow-up text-success"></i> +2.3% from last audit
            </div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="kpi-card border-left-danger">
            <div class="kpi-label">Total Undercharge</div>
            <div class="kpi-value text-danger">${{ kpis.undercharge | format_currency }}</div>
            <div class="kpi-subtext">{{ kpis.unbilled_count }} unbilled charges</div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="kpi-card border-left-info">
            <div class="kpi-label">Total Overcharge</div>
            <div class="kpi-value text-info">${{ kpis.overcharge | format_currency }}</div>
            <div class="kpi-subtext">{{ kpis.unexpected_count }} unexpected charges</div>
        </div>
    </div>
    
    <div class="col-md-3">
        <div class="kpi-card border-left-warning">
            <div class="kpi-label">Exceptions</div>
            <div class="kpi-value">{{ kpis.exception_count }}</div>
            <div class="kpi-subtext">Require manual review</div>
        </div>
    </div>
</div>

<!-- Charts Row -->
<div class="row g-4 mb-4">
    <div class="col-md-8">
        <div class="card border-0 shadow-sm">
            <div class="card-header bg-white border-bottom">
                <h5 class="mb-0">Match Rate by Property</h5>
            </div>
            <div class="card-body">
                <canvas id="matchRateChart" height="80"></canvas>
            </div>
        </div>
    </div>
    
    <div class="col-md-4">
        <div class="card border-0 shadow-sm">
            <div class="card-header bg-white border-bottom">
                <h5 class="mb-0">Exception Breakdown</h5>
            </div>
            <div class="card-body">
                <canvas id="exceptionPieChart"></canvas>
            </div>
        </div>
    </div>
</div>

<!-- Property Grid (NOT a table) -->
<div class="card border-0 shadow-sm">
    <div class="card-header bg-white border-bottom d-flex justify-content-between align-items-center">
        <h5 class="mb-0">Properties ({{ properties | length }})</h5>
        <div class="btn-group btn-group-sm" role="group">
            <button class="btn btn-outline-secondary active" data-view="grid">
                <i class="bi bi-grid-3x3"></i> Grid
            </button>
            <button class="btn btn-outline-secondary" data-view="list">
                <i class="bi bi-list"></i> List
            </button>
        </div>
    </div>
    <div class="card-body">
        <div class="property-grid" id="propertyGrid">
            {% for property in properties %}
            <div class="property-card" onclick="location.href='/property/{{ property.property_id }}/run_{{ run_id }}'">
                <div class="property-header">
                    <h6>{{ property.property_name }}</h6>
                    <span class="property-id">ID: {{ property.property_id }}</span>
                </div>
                
                <!-- Match Rate Progress Ring -->
                <div class="progress-ring-container">
                    <svg class="progress-ring" width="120" height="120">
                        <circle class="progress-ring-bg" cx="60" cy="60" r="52"></circle>
                        <circle class="progress-ring-fill 
                            {% if property.match_rate >= 98 %}stroke-success
                            {% elif property.match_rate >= 95 %}stroke-warning
                            {% else %}stroke-danger{% endif %}"
                            cx="60" cy="60" r="52"
                            style="stroke-dasharray: 327; stroke-dashoffset: {{ 327 - (327 * property.match_rate / 100) }};"></circle>
                    </svg>
                    <div class="progress-ring-text">
                        <div class="percentage">{{ property.match_rate }}%</div>
                        <div class="label">Match</div>
                    </div>
                </div>
                
                <div class="property-metrics">
                    <div class="metric">
                        <i class="bi bi-house-door"></i>
                        <span>{{ property.lease_count }} leases</span>
                    </div>
                    <div class="metric text-danger">
                        <i class="bi bi-arrow-down-circle"></i>
                        <span>${{ property.undercharge | format_currency }}</span>
                    </div>
                    <div class="metric text-info">
                        <i class="bi bi-arrow-up-circle"></i>
                        <span>${{ property.overcharge | format_currency }}</span>
                    </div>
                </div>
                
                {% if property.exception_count > 0 %}
                <div class="property-alert">
                    <i class="bi bi-exclamation-triangle"></i>
                    {{ property.exception_count }} exceptions
                </div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>
<script>
// Match Rate Bar Chart
const matchRateCtx = document.getElementById('matchRateChart').getContext('2d');
new Chart(matchRateCtx, {
    type: 'bar',
    data: {
        labels: {{ properties | map(attribute='property_name') | list | tojson }},
        datasets: [{
            label: 'Match Rate %',
            data: {{ properties | map(attribute='match_rate') | list | tojson }},
            backgroundColor: function(context) {
                const value = context.parsed.y;
                if (value >= 98) return '#10b981';
                if (value >= 95) return '#f59e0b';
                return '#ef4444';
            }
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { display: false }
        },
        scales: {
            y: {
                beginAtZero: true,
                max: 100
            }
        }
    }
});

// Exception Pie Chart
const exceptionCtx = document.getElementById('exceptionPieChart').getContext('2d');
new Chart(exceptionCtx, {
    type: 'doughnut',
    data: {
        labels: ['Unbilled', 'Unexpected', 'Amount Mismatch'],
        datasets: [{
            data: [
                {{ kpis.unbilled_count }},
                {{ kpis.unexpected_count }},
                {{ kpis.mismatch_count }}
            ],
            backgroundColor: ['#ef4444', '#3b82f6', '#f59e0b']
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: true
    }
});
</script>
{% endblock %}
```

**CSS for Portfolio Grid:**
```css
.kpi-card {
    background: white;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    border-left: 4px solid transparent;
}

.border-left-primary { border-left-color: #1e3a8a !important; }
.border-left-danger { border-left-color: #ef4444 !important; }
.border-left-info { border-left-color: #3b82f6 !important; }
.border-left-warning { border-left-color: #f59e0b !important; }

.kpi-label {
    font-size: 12px;
    color: #64748b;
    text-transform: uppercase;
    font-weight: 600;
    letter-spacing: 0.5px;
}

.kpi-value {
    font-size: 32px;
    font-weight: 700;
    color: #1e293b;
    margin: 8px 0;
}

.kpi-trend, .kpi-subtext {
    font-size: 13px;
    color: #64748b;
}

.property-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 24px;
}

.property-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px;
    cursor: pointer;
    transition: all 0.3s ease;
}

.property-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 24px rgba(0, 0, 0, 0.12);
    border-color: #1e3a8a;
}

.property-header {
    margin-bottom: 16px;
}

.property-header h6 {
    margin: 0;
    font-size: 16px;
    font-weight: 600;
    color: #1e293b;
}

.property-id {
    font-size: 12px;
    color: #64748b;
}

.progress-ring-container {
    position: relative;
    width: 120px;
    height: 120px;
    margin: 20px auto;
}

.progress-ring-bg {
    fill: none;
    stroke: #e2e8f0;
    stroke-width: 8;
}

.progress-ring-fill {
    fill: none;
    stroke-width: 8;
    stroke-linecap: round;
    transform: rotate(-90deg);
    transform-origin: 50% 50%;
    transition: stroke-dashoffset 0.5s ease;
}

.stroke-success { stroke: #10b981; }
.stroke-warning { stroke: #f59e0b; }
.stroke-danger { stroke: #ef4444; }

.progress-ring-text {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    text-align: center;
}

.progress-ring-text .percentage {
    font-size: 24px;
    font-weight: 700;
    color: #1e293b;
}

.progress-ring-text .label {
    font-size: 12px;
    color: #64748b;
}

.property-metrics {
    display: flex;
    justify-content: space-around;
    margin-top: 16px;
    padding-top: 16px;
    border-top: 1px solid #e2e8f0;
}

.property-metrics .metric {
    text-align: center;
    font-size: 13px;
    color: #64748b;
}

.property-metrics .metric i {
    display: block;
    font-size: 18px;
    margin-bottom: 4px;
}

.property-alert {
    margin-top: 12px;
    padding: 8px 12px;
    background: #fef3c7;
    color: #92400e;
    border-radius: 6px;
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 8px;
}
```

---

### 3. Enhanced Lease Detail with Sliding Tiles

**Current Problem:** Flat transaction tables - hard to compare expected vs actual

**New Lease Detail Improvements (Beyond Old App):**
- ✅ **Variance Summary Cards**: Two large cards showing undercharge/overcharge with icons
- ✅ **AR Code Accordion**: Collapsible sections for each AR code (Rent, Utilities, etc.)
- ✅ **Sliding Tiles Animation**: Smooth slide-down reveal when expanding AR code sections
- ✅ **Side-by-Side Tables**: Expected vs Actual transactions in adjacent columns for easy comparison
- ✅ **Color-Coded Rows**: Red for mismatches, green for matched, yellow for variances
- ✅ **Status Badges**: Visual indicators (MATCHED, SCHEDULED_NOT_BILLED, AMOUNT_MISMATCH)
- ✅ **Quick Actions**: "Open in Entrata" button, mark exception resolved
- ✅ **Transaction Deep Links**: Click transaction to see full details in Entrata

**New Design with Collapsible Sections:**

```html
<!-- templates/lease.html -->
{% extends "base.html" %}

{% block content %}
<!-- Lease Header -->
<div class="lease-header">
    <div class="d-flex justify-content-between align-items-center">
        <div>
            <h2>{{ resident_name }}</h2>
            <p class="text-muted">
                Lease ID: {{ lease_id }} 
                <span class="mx-2">•</span>
                Unit: {{ unit_number }}
            </p>
        </div>
        <a href="https://peakmade.entrata.com/users/{{ customer_id }}" 
           target="_blank" 
           class="btn btn-primary btn-lg">
            <i class="bi bi-box-arrow-up-right"></i> Open in Entrata
        </a>
    </div>
</div>

<!-- Variance Summary Cards -->
<div class="row g-4 mb-4">
    <div class="col-md-6">
        <div class="variance-card variance-undercharge">
            <div class="variance-icon">
                <i class="bi bi-arrow-down-circle"></i>
            </div>
            <div class="variance-content">
                <div class="variance-label">Total Undercharge</div>
                <div class="variance-amount">${{ undercharge | format_currency }}</div>
                <div class="variance-detail">Revenue at risk</div>
            </div>
        </div>
    </div>
    
    <div class="col-md-6">
        <div class="variance-card variance-overcharge">
            <div class="variance-icon">
                <i class="bi bi-arrow-up-circle"></i>
            </div>
            <div class="variance-content">
                <div class="variance-label">Total Overcharge</div>
                <div class="variance-amount">${{ overcharge | format_currency }}</div>
                <div class="variance-detail">Potential refund risk</div>
            </div>
        </div>
    </div>
</div>

<!-- AR Code Accordion with Sliding Tiles -->
<div class="ar-code-accordion">
    {% for ar in ar_code_summaries %}
    <div class="ar-code-section">
        <div class="ar-code-header" onclick="toggleArCode('ar-{{ ar.ar_code_id }}')">
            <div class="ar-code-info">
                <h5>
                    {{ ar.ar_code_name }}
                    <span class="badge bg-secondary ms-2">{{ ar.ar_code_id }}</span>
                </h5>
                <div class="ar-code-stats">
                    <span class="stat-pill stat-success">
                        <i class="bi bi-check-circle"></i> {{ ar.matched_count }} Matched
                    </span>
                    {% if ar.discrepancy_count > 0 %}
                    <span class="stat-pill stat-warning">
                        <i class="bi bi-exclamation-triangle"></i> {{ ar.discrepancy_count }} Discrepancies
                    </span>
                    {% endif %}
                </div>
            </div>
            <div class="ar-code-actions">
                <div class="ar-code-variance">
                    {% if ar.undercharge > 0 %}
                    <span class="text-danger">-${{ ar.undercharge | format_currency }}</span>
                    {% endif %}
                    {% if ar.overcharge > 0 %}
                    <span class="text-info">+${{ ar.overcharge | format_currency }}</span>
                    {% endif %}
                </div>
                <i class="bi bi-chevron-down toggle-icon"></i>
            </div>
        </div>
        
        <!-- Sliding Transaction Tiles -->
        <div class="ar-code-body" id="ar-{{ ar.ar_code_id }}" style="display: none;">
            <div class="transaction-comparison">
                <!-- Expected Transactions Tile -->
                <div class="transaction-tile tile-expected">
                    <div class="tile-header">
                        <h6><i class="bi bi-calendar-check"></i> Expected Transactions</h6>
                        <span class="tile-count">{{ ar.expected_transactions | length }} charges</span>
                    </div>
                    <div class="tile-body">
                        {% for txn in ar.expected_transactions %}
                        <div class="transaction-row 
                            {% if txn.status == 'MATCHED' %}txn-matched
                            {% elif txn.status == 'SCHEDULED_NOT_BILLED' %}txn-error
                            {% else %}txn-warning{% endif %}">
                            <div class="txn-main">
                                <div class="txn-period">{{ txn.period }}</div>
                                <div class="txn-amount">${{ txn.amount | format_currency }}</div>
                            </div>
                            <div class="txn-status">
                                <span class="status-badge status-{{ txn.status | lower }}">
                                    {{ txn.status | format_status }}
                                </span>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </div>
                
                <!-- Actual Transactions Tile -->
                <div class="transaction-tile tile-actual">
                    <div class="tile-header">
                        <h6><i class="bi bi-receipt"></i> Actual Transactions</h6>
                        <span class="tile-count">{{ ar.actual_transactions | length }} charges</span>
                    </div>
                    <div class="tile-body">
                        {% for txn in ar.actual_transactions %}
                        <div class="transaction-row 
                            {% if txn.status == 'MATCHED' %}txn-matched
                            {% elif txn.status == 'BILLED_NOT_SCHEDULED' %}txn-error
                            {% else %}txn-warning{% endif %}">
                            <div class="txn-main">
                                <div class="txn-date">
                                    <i class="bi bi-calendar"></i> {{ txn.post_date }}
                                </div>
                                <div class="txn-amount">${{ txn.amount | format_currency }}</div>
                            </div>
                            <div class="txn-meta">
                                <span class="txn-id">{{ txn.transaction_id }}</span>
                                <span class="status-badge status-{{ txn.status | lower }}">
                                    {{ txn.status | format_status }}
                                </span>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>
            
            <!-- Exception Resolution (if discrepancies exist) -->
            {% if ar.discrepancy_count > 0 %}
            <div class="exception-action-bar">
                <button class="btn btn-outline-primary" 
                        onclick="openResolveModal({{ property_id }}, {{ lease_id }}, {{ ar.ar_code_id }})">
                    <i class="bi bi-check-square"></i> Mark as Resolved
                </button>
            </div>
            {% endif %}
        </div>
    </div>
    {% endfor %}
</div>
{% endblock %}

{% block scripts %}
<script>
function toggleArCode(arCodeId) {
    const body = document.getElementById(arCodeId);
    const header = body.previousElementSibling;
    const icon = header.querySelector('.toggle-icon');
    
    if (body.style.display === 'none') {
        // Slide down with animation
        body.style.display = 'block';
        setTimeout(() => {
            body.classList.add('expanded');
        }, 10);
        icon.classList.add('rotated');
    } else {
        // Slide up
        body.classList.remove('expanded');
        icon.classList.remove('rotated');
        setTimeout(() => {
            body.style.display = 'none';
        }, 300);
    }
}
</script>
{% endblock %}
```

**CSS for Sliding Tiles:**
```css
.lease-header {
    background: white;
    padding: 32px;
    border-radius: 12px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    margin-bottom: 24px;
}

.variance-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    display: flex;
    gap: 20px;
    align-items: center;
}

.variance-undercharge { border-left: 4px solid #ef4444; }
.variance-overcharge { border-left: 4px solid #3b82f6; }

.variance-icon {
    width: 64px;
    height: 64px;
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 32px;
}

.variance-undercharge .variance-icon {
    background: #fee2e2;
    color: #ef4444;
}

.variance-overcharge .variance-icon {
    background: #dbeafe;
    color: #3b82f6;
}

.variance-label {
    font-size: 14px;
    color: #64748b;
    font-weight: 600;
}

.variance-amount {
    font-size: 36px;
    font-weight: 700;
    color: #1e293b;
    margin: 4px 0;
}

.variance-detail {
    font-size: 13px;
    color: #64748b;
}

.ar-code-accordion {
    display: flex;
    flex-direction: column;
    gap: 16px;
}

.ar-code-section {
    background: white;
    border-radius: 12px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    overflow: hidden;
}

.ar-code-header {
    padding: 20px 24px;
    cursor: pointer;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: background 0.2s ease;
}

.ar-code-header:hover {
    background: #f8fafc;
}

.ar-code-info h5 {
    margin: 0 0 8px 0;
    font-size: 18px;
    color: #1e293b;
}

.ar-code-stats {
    display: flex;
    gap: 12px;
}

.stat-pill {
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
}

.stat-success {
    background: #d1fae5;
    color: #065f46;
}

.stat-warning {
    background: #fef3c7;
    color: #92400e;
}

.toggle-icon {
    font-size: 20px;
    color: #64748b;
    transition: transform 0.3s ease;
}

.toggle-icon.rotated {
    transform: rotate(180deg);
}

.ar-code-body {
    max-height: 0;
    overflow: hidden;
    transition: max-height 0.3s ease;
}

.ar-code-body.expanded {
    max-height: 2000px;
}

.transaction-comparison {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    padding: 24px;
    background: #f8fafc;
}

.transaction-tile {
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.1);
}

.tile-expected {
    border-top: 3px solid #3b82f6;
}

.tile-actual {
    border-top: 3px solid #10b981;
}

.tile-header {
    padding: 16px;
    background: #f8fafc;
    border-bottom: 1px solid #e2e8f0;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.tile-header h6 {
    margin: 0;
    font-size: 14px;
    font-weight: 600;
    color: #1e293b;
}

.tile-count {
    font-size: 12px;
    color: #64748b;
}

.tile-body {
    max-height: 400px;
    overflow-y: auto;
    padding: 12px;
}

.transaction-row {
    padding: 12px;
    border-radius: 6px;
    margin-bottom: 8px;
    transition: background 0.2s ease;
}

.txn-matched {
    background: #f0fdf4;
    border-left: 3px solid #10b981;
}

.txn-warning {
    background: #fffbeb;
    border-left: 3px solid #f59e0b;
}

.txn-error {
    background: #fef2f2;
    border-left: 3px solid #ef4444;
}

.txn-main {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 4px;
}

.txn-period, .txn-date {
    font-size: 13px;
    color: #64748b;
}

.txn-amount {
    font-size: 16px;
    font-weight: 600;
    color: #1e293b;
}

.txn-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
}

.txn-id {
    color: #64748b;
}

.status-badge {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
}

.status-matched {
    background: #d1fae5;
    color: #065f46;
}

.status-scheduled_not_billed, .status-billed_not_scheduled {
    background: #fee2e2;
    color: #991b1b;
}

.status-amount_mismatch {
    background: #fef3c7;
    color: #92400e;
}

.exception-action-bar {
    padding: 16px 24px;
    border-top: 1px solid #e2e8f0;
    text-align: right;
}

/* Responsive Design */
@media (max-width: 768px) {
    .transaction-comparison {
        grid-template-columns: 1fr;
    }
    
    .property-grid {
        grid-template-columns: 1fr;
    }
}
```

---

### 4. Required Frontend Libraries

**Add to base.html:**
```html
<!-- Bootstrap Icons -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css">

<!-- Chart.js for data visualization -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>

<!-- Optional: Animate.css for smooth animations -->
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css">
```

---

### 5. Design Implementation Checklist

Before building UI, ensure these elements:

- [ ] ✅ **Color System**: Define CSS variables for consistent colors
- [ ] ✅ **Typography Scale**: Define heading sizes (h1-h6) and body text
- [ ] ✅ **Spacing System**: Use consistent padding/margins (4px, 8px, 12px, 16px, 24px, 32px)
- [ ] ✅ **Card Shadows**: Use 3 levels (sm, md, lg) for depth
- [ ] ✅ **Hover States**: All interactive elements have hover effects
- [ ] ✅ **Loading States**: Show spinners during data fetches
- [ ] ✅ **Empty States**: Handle "no data" scenarios gracefully
- [ ] ✅ **Responsive Breakpoints**: Test mobile (375px), tablet (768px), desktop (1280px)
- [ ] ✅ **Chart Colors**: Match brand colors in Chart.js configurations
- [ ] ✅ **Animation Timing**: Use 0.2s for small interactions, 0.3s for larger transitions

---

## What Needs to be Built (Project Structure)

### Core Modules

**1. API Client (`audit_engine/api_ingest.py`)**
- Functions to call Entrata `getLeaseDetails` and `getLeaseArTransactions`
- Handle authentication (X-Api-Key header)
- Parse JSON responses into pandas DataFrames
- Handle API errors, rate limits, and retries
- Support both production and sandbox environments

**2. Data Transformation Pipeline (`audit_engine/`)**
- `mappings.py` - Map raw API fields to canonical field names
- `normalize.py` - Validate and clean data (remove nulls, parse dates, apply filters)
- `expand.py` - Expand recurring scheduled charges to monthly buckets
- `reconcile.py` - Three-tier matching algorithm (exact, amount mismatch, unbilled/unexpected)
- `metrics.py` - Calculate KPIs (match rate, undercharge, overcharge, exception count)
- `canonical_fields.py` - Define standard field names as enums

**3. Storage Layer (`storage/service.py`)**
- Write to SharePoint Lists via Microsoft Graph API
- Batch operations for bulk writes (20 rows per batch)
- Query lists with filters (RunID, PropertyID, LeaseID)
- In-memory caching with Flask-Caching
- Handle authentication tokens

**4. Web Application (`web/views.py`)**
- Flask routes:
  - `/` - Home page (audit run picker)
  - `/portfolio` - Portfolio dashboard (all properties aggregated)
  - `/property/<property_id>/run_<run_id>` - Property detail view
  - `/lease/<lease_id>/run_<run_id>` - Lease detail view with transaction tables
  - `/api/bulk-audit` - Bulk audit job submission
  - `/api/exception-months` - Exception resolution updates
- `execute_audit_run()` function - orchestrates entire audit flow
- Template rendering with Jinja2
- Session management and caching

**5. Configuration Files**
- `config.py` - App configuration, reconciliation settings
- `excluded_ar_codes.json` - AR code whitelist `{"allowed_ar_codes": [154771]}`
- `excluded_properties.json` - Property blacklist `{"excluded_property_ids": []}`
- `ar_code_name_usage_map.json` - AR code ID to name mapping `{"154771": {"name": "Rent"}}`
- `entrata_environment.json` - Environment toggle `{"environment": "prod"}`
- `.env` - Environment variables (API keys, SharePoint URLs, etc.)

### Required Dependencies (Python packages)

```python
Flask==3.1.0
pandas==2.2.3
requests==2.31.0
python-dotenv==1.0.0
Flask-Caching==2.1.0
msal==1.28.0  # Microsoft Authentication Library for Azure AD
openpyxl==3.1.2  # Excel file handling (if needed)
waitress==3.0.0  # Production WSGI server
```

### Required Environment Variables

```bash
# Entrata API
LEASE_API_KEY=<your-entrata-api-key>
LEASE_API_BASE_URL=https://apis.entrata.com/ext/orgs/peakmade/v1
LEASE_API_DETAILS_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/leases
LEASE_API_AR_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions
LEASE_API_DETAILS_METHOD=getLeaseDetails
LEASE_API_AR_METHOD=getLeaseArTransactions

# SharePoint
SHAREPOINT_SITE_URL=https://<tenant>.sharepoint.com/sites/<site-name>
SHAREPOINT_DOCUMENT_LIBRARY=LeaseFileAuditDocuments
SHAREPOINT_RUN_DISPLAY_SNAPSHOTS_LIST=RunDisplaySnapshots
SHAREPOINT_EXCEPTION_MONTHS_LIST=ExceptionMonths
SHAREPOINT_LEASE_TERMS_LIST=LeaseTerms

# Azure AD Authentication
AZURE_CLIENT_ID=<app-registration-client-id>
AZURE_CLIENT_SECRET=<app-registration-secret>
AZURE_TENANT_ID=<tenant-id>

# SharePoint Authentication (App-Only Flow)
# Uses MSAL (Microsoft Authentication Library) to get access tokens
# Requires Azure AD app registration with Sites.ReadWrite.All permission
# Token lifetime: 1 hour (must refresh before expiration)

# Flask
FLASK_SECRET_KEY=<random-secret-key>
FLASK_ENV=development
```

### Data Transformation Flow (What the Code Needs to Do)

```
PHASE 1: API FETCH
  → Call getLeaseDetails(propertyIds=[771903])
  → Call getLeaseArTransactions(propertyIds=[771903], fromDate, toDate)
  → Returns raw JSON

PHASE 2: SOURCE MAPPING (Raw → Canonical)
  → Map API field names to canonical field names
  → Example: "PROPERTY_ID" → "PropertyID", "LEASE_ID" → "LeaseIntervalID"
  → Result: Two DataFrames (ar_canonical, scheduled_canonical)

PHASE 3: NORMALIZATION & VALIDATION
  → Remove deleted/invalid charges
  → Apply AR code whitelist (filter to allowed codes)
  → Filter out API-posted charges
  → Parse dates, clean amounts
  → Result: actual_detail (9,692 rows), scheduled_normalized (2,123 rows)

PHASE 4: EXPANSION (Scheduled → Monthly)
  → Expand recurring charges to individual months
  → Example: 1 charge (08/2024-04/2025, Monthly) → 9 monthly rows
  → Result: expected_detail (8,824 rows)

PHASE 5: RECONCILIATION (Expected vs Actual)
  → Group by (PropertyID, LeaseID, ArCode, AuditMonth)
  → Three-tier matching:
    1. Exact match (all fields match) → MATCHED
    2. IDs match, amount differs → AMOUNT_MISMATCH
    3. Expected with no actual → SCHEDULED_NOT_BILLED
    4. Actual with no expected → BILLED_NOT_SCHEDULED
  → Result: bucket_results (8,971 rows), findings (187 discrepancies)

PHASE 6: METRICS CALCULATION
  → Calculate match rate: matched / total expected
  → Calculate undercharge: sum of unbilled scheduled charges
  → Calculate overcharge: sum of unexpected transactions
  → Calculate exception count: count of high-severity findings

PHASE 7: AGGREGATION & SNAPSHOTS
  → Aggregate to multiple scopes:
    - Portfolio: 1 row (all properties)
    - Property: 1 row per property
    - Lease: 668 rows (one per lease)
    - Month: 5,297 rows (lease × month × AR code)
  → Result: 5,966 snapshot rows for RunDisplaySnapshots

PHASE 8: SAVE TO SHAREPOINT
  → Write transaction detail to new SharePoint list(s)
  → Write aggregated snapshots to RunDisplaySnapshots (299 batches of 20 rows)
  → Populate in-memory cache
  → Data now available for user viewing

PHASE 9: USER VIEWS RESULTS
  → Navigate to property detail page → load from RunDisplaySnapshots
  → Click lease → query transaction detail list by RunID + LeaseID
  → Display expected transactions table and actual transactions table
```

### SharePoint List Schemas (To Be Created)

**RunDisplaySnapshots** (already exists, for aggregated summaries):
- RunId, ScopeType, PropertyId, LeaseIntervalId, ArCodeId, AuditMonth
- ExpectedTotal, ActualTotal, Variance, ExceptionCount, MatchRate

**ExceptionMonths** (already exists, for manual resolution):
- PropertyId, LeaseIntervalId, ArCodeId, AuditMonth
- ExpectedTotal, ActualTotal, Variance, Status, FixLabel, Notes, ResolvedBy, ResolvedAt

**AuditTransactionDetail** (NEW - to be created):
**FINAL SCHEMA (Single combined list):**
- **RunId** (Single line of text) - indexed
- **DetailType** (Choice: EXPECTED, ACTUAL) - indexed  
- **PropertyId** (Number) - indexed
- **PropertyName** (Single line of text)
- **LeaseIntervalId** (Number) - indexed
- **ResidentName** (Single line of text)
- **UnitNumber** (Single line of text)
- **ArCodeId** (Number) - indexed
- **ArCodeName** (Single line of text)
- **AuditMonth** (Date) - indexed
- **Amount** (Currency)
- **PostDate** (Date)
- **TransactionId** (Number) - nullable (null for EXPECTED rows)
- **ScheduledChargeId** (Number) - nullable (null for ACTUAL rows)
- **PeriodStartDate** (Date)
- **PeriodEndDate** (Date)
- **RowKey** (Single line of text) - unique identifier
- **Created** (DateTime) - auto-populated
- **Modified** (DateTime) - auto-populated

**Why single list?**
- Simpler schema: One query to get all lease data, filter by DetailType client-side
- Easier joins: Expected and Actual rows share same RunId + LeaseIntervalId + ArCodeId + AuditMonth
- Fewer lists to manage: Reduces SharePoint list count

**Required Indexes:** RunId, DetailType, PropertyId, LeaseIntervalId, ArCodeId, AuditMonth

### Implementation Build Order (Final)

**Phase 1: Flask App Shell & Base Template** (Day 1-2)
1. Create Flask app factory (`app.py`, `create_app()` pattern)
2. Implement Standard Base.html template (PeakMade branding)
3. Create basic routes: `/`, `/run-audit`, `/portfolio/<run_id>`, `/property/<property_id>/<run_id>`, `/lease/<lease_id>/<run_id>`
4. Test navigation flow (home → audit → results)
5. Set up environment variables (.env file)

**Phase 2: Run Lease Audit Page** (Day 3-4)
1. Build `/run-audit` page with three audit type forms:
   - Run Property Audit (single property selector)
   - Run Bulk Audit (multi-property checkboxes)
   - Run Single Lease Audit (property + lease ID inputs)
2. Add date range options (Academic Year dropdown OR custom From/To dates)
3. Add AR code filter checkboxes (default: [154771] Rent)
4. Test form submission, create RunId on submit

**Phase 3: Entrata API Client** (Day 5-6)
1. Build `audit_engine/api_ingest.py`:
   - `fetch_lease_details(property_ids, lease_id=None)` → calls getLeaseDetails
   - `fetch_ar_transactions(property_ids, from_date, to_date, lease_id=None)` → calls getLeaseArTransactions
2. Handle authentication (X-Api-Key header)
3. Parse JSON responses into pandas DataFrames
4. Test with property 771903 in sandbox environment
5. Verify response structure, log row counts

**Phase 4: Data Normalization** (Day 7-8)
1. Build `audit_engine/mappings.py` - map raw Entrata fields to canonical fields
2. Build `audit_engine/normalize.py`:
   - Parse dates to datetime objects
   - Clean amounts (remove nulls, validate numeric)
   - Standardize AR codes, property IDs, lease IDs
   - Extract resident names from customer arrays
   - Populate unit numbers
3. Apply AR code whitelist from `excluded_ar_codes.json`
4. Test with sample API responses, verify clean data

**Phase 5: Scheduled Charge Expansion** (Day 9)
1. Build `audit_engine/expand.py`:
   - `expand_scheduled_charges(scheduled_df)` → monthly buckets
   - Input: 1 charge (Aug 2024 - Apr 2025, Monthly, $1,234.56)
   - Output: 9 monthly EXPECTED rows
2. Test with various frequencies (Monthly, Semester, Annual)
3. Verify row counts match expectations

**Phase 6: Reconciliation Engine** (Day 10-11)
1. Build `audit_engine/reconcile.py`:
   - `reconcile_buckets(expected_df, actual_df)` → matched/exception buckets
   - Group by: PropertyId, LeaseIntervalId, ArCodeId, ArCodeName, AuditMonth
   - Classify: MATCHED, SCHEDULED_NOT_BILLED, BILLED_NOT_SCHEDULED, AMOUNT_MISMATCH
2. Build `audit_engine/metrics.py`:
   - `calculate_metrics(buckets_df)` → match rate, undercharge, overcharge, exception count
3. Test with known input/output pairs
4. Verify KPI calculations match manual calculations

**Phase 7: SharePoint List Helpers** (Day 12-13)
1. Build `storage/sharepoint_service.py`:
   - `create_audit_run_status(run_id, audit_type, property_ids)` → AuditRunStatus rows
   - `update_audit_run_status(run_id, property_id, status, **kwargs)` → status updates
   - `write_transaction_detail_batch(rows)` → batch write to AuditTransactionDetail
   - `write_snapshots_batch(rows)` → batch write to RunDisplaySnapshots
   - `query_transaction_detail(run_id, lease_id)` → lease detail query
   - `query_snapshots(run_id, scope_type, property_id=None)` → dashboard queries
2. Implement Graph API batch operations (20 rows per batch)
3. Handle token refresh, rate limiting, retries
4. Test writes and reads with test data

**Phase 8: Incremental Transaction Detail Writes** (Day 14-15)
1. Integrate write logic into audit flow:
   - After reconciliation: Split into EXPECTED and ACTUAL rows
   - Write AuditTransactionDetail in batches of 20 rows
   - Update AuditRunStatus: ExpectedRowsWritten, ActualRowsWritten
   - Log progress every 1,000 rows
2. Test with property 771903 (668 leases, ~18,600 rows)
3. Measure write time (~15 minutes expected)
4. Verify all rows written correctly

**Phase 9: Write RunDisplaySnapshots** (Day 16)
1. Build aggregation logic:
   - Portfolio scope: 1 row across all properties
   - Property scope: 1 row per property
   - Lease scope: 1 row per lease
   - Month scope: 1 row per lease × month × AR code
2. Write snapshots after transaction detail completes
3. Update AuditRunStatus: SnapshotRowsWritten
4. Test aggregation math (match rates, totals)

**Phase 10: AuditRunStatus Lifecycle** (Day 17)
1. Implement full status progression:
   - Queued → Fetching → Reconciling → Writing Detail → Viewable → Complete
2. Mark IsViewable = True after snapshots written
3. Set CompletedAt timestamp
4. Test Bulk Audit: Property 1 viewable while Property 2 still processing
5. Test error handling: Status = Failed, ErrorMessage populated

**Phase 11: Portfolio Dashboard** (Day 18-19)
1. Build `/portfolio/<run_id>` view:
   - Query RunDisplaySnapshots by RunId and ScopeType = 'Property'
   - Display property list table with KPIs
   - Show match rate, undercharge, overcharge per property
   - Add click handlers → navigate to property detail
2. Add portfolio-level KPI cards (aggregate across all properties)
3. Test with Bulk Audit results (multiple properties)

**Phase 12: Property Detail View** (Day 20-21)
1. Build `/property/<property_id>/<run_id>` view:
   - Query RunDisplaySnapshots by RunId, PropertyId, ScopeType = 'Lease'
   - Display lease list table with KPIs
   - Show resident name, unit number, match rate, undercharge, overcharge
   - Add click handlers → navigate to lease detail
2. Add property-level KPI cards
3. Test with property 771903 (668 leases)

**Phase 13: Lease Detail View** (Day 22-24)
1. Build `/lease/<lease_id>/<run_id>` view:
   - Query AuditTransactionDetail by RunId, LeaseIntervalId
   - Split by DetailType:
     - EXPECTED rows → Expected Transactions table
     - ACTUAL rows → Actual Transactions table
   - Display side-by-side comparison
   - Color-code rows by Status (green = MATCHED, red = exceptions, yellow = mismatch)
2. Add Entrata deep link button (opens customer in Entrata portal)
3. Add status badges (MATCHED, SCHEDULED_NOT_BILLED, etc.)
4. Test with lease 18296704

**Phase 14: Exception Resolution Modal & Workflow** (Day 25-27)
1. Build resolution dropdown:
   - Context-specific options based on ExceptionType
   - SCHEDULED_NOT_BILLED: 6 options
   - BILLED_NOT_SCHEDULED: 5 options
   - AMOUNT_MISMATCH: 6 options
2. Implement batch resolution:
   - Checkboxes on exception month rows
   - "Apply to Selected" button
   - Single FixLabel + ActionType applied to all selected months
3. Build `/api/exception-months` POST endpoint:
   - Write to ExceptionMonths list
   - Store FixLabel, ActionType, Notes, ResolvedBy, ResolvedAt
4. Test resolution workflow end-to-end

**Phase 15: Historical Resolution Matching** (Day 28-29)
1. Build historical lookup logic:
   - Query ExceptionMonths by PropertyId, LeaseIntervalId, ArCodeId, AuditMonth, ExceptionType
   - Order by ResolvedAt DESC, limit 1 (most recent)
2. Display previous resolution in lease detail:
   - Format: "✓ [FixLabel] - Resolved by [User] on [Date]"
   - Show below exception indicator
3. Test: Resolve exception, run new audit, verify previous resolution shows
4. Test: Ensure new exception still requires explicit resolution (no auto-apply)

**Phase 16: Polish & Testing** (Day 30)
1. Add progress polling for Bulk Audit:
   - `/api/audit-progress/<run_id>` endpoint
   - Returns AuditRunStatus rows with current Status
   - Frontend polls every 2 seconds, updates progress bars
2. Add error handling:
   - Display ErrorMessage on failure
   - Retry logic for transient Graph API errors
   - User-friendly error messages
3. Add caching:
   - Flask-Caching on portfolio, property, lease views (4-hour TTL)
4. End-to-end testing:
   - Run Property Audit → verify viewable immediately
   - Run Bulk Audit → verify properties viewable incrementally
   - Run Single Lease Audit → verify lease detail view
   - Test exception resolution → verify historical matching
5. Performance testing:
   - Property audit (668 leases): Target < 20 minutes
   - Lease detail view load: Target < 2 seconds
   - Query performance with 100K+ rows: Verify indexes working

### Testing Strategy

**Unit Tests:**
- API client: Mock Entrata responses, test parsing
- Mapping functions: Verify field transformations
- Expansion logic: Test recurring charge calculations
- Matching algorithm: Known input/output pairs

**Integration Tests:**
- Full audit flow: Run with test property in sandbox environment
- Storage operations: Write/read from SharePoint lists
- Cache behavior: Verify cache hits/misses

**End-to-End Test (Success Criteria):**
1. Run audit for property 771903 in production
2. Within 2 minutes, navigate to lease 18296704
3. Verify: Resident name, AR code "Rent", transaction tables populated
4. Click "Open in Entrata" → opens external browser
5. Run second audit → both audits still accessible
6. Restart app → data persists

## Key Files in Codebase (Reference Only - For Existing Project)

**⚠️ IMPORTANT: These files contain the OLD problematic architecture. Use them to understand:**
- ✅ **Business logic to PRESERVE**: API calls, reconciliation algorithm, KPI calculations
- ❌ **Storage patterns to AVOID**: AuditRuns2 writes, Parquet file delays, multi-layer storage

**Reference Files:**
- `web/views.py` → `execute_audit_run()` - orchestrates entire audit flow
  - Shows the workflow: API fetch → mapping → reconciliation → metrics
  - **WARNING**: Contains problematic Parquet file writes and AuditRuns2 async writes
- `audit_engine/reconcile.py` → `reconcile_buckets()` - matching algorithm
  - ✅ **KEEP THIS LOGIC**: Three-tier matching, groupby operations
- `storage/service.py` → `save_run()` - persistence logic
  - ❌ **DO NOT REPLICATE**: Async AuditRuns2 writes, Parquet file storage
  - Shows Graph API batch operations (useful reference)
- `SHAREPOINT_DATA_FLOW_EXPLAINED.md` - documents the OLD five-list architecture
  - Explains why AuditRuns2 was a bottleneck (millions of rows, slow writes)
  - Explains the 5K item query degradation problem
- `PROJECT_REQUIREMENTS.md` - detailed business requirements
  - ✅ **USE THIS**: Pure requirements, no implementation details
- `LEASEFILEAUDIT_OVERVIEW.md` - complete technical documentation
  - Comprehensive but describes the OLD architecture with all its problems

## Question for New Project

**⚠️ CRITICAL: UI vs Data Storage Approach**

This new project has TWO completely different design requirements:

**1. UI/Templates (REUSE Standard Pattern):**
- ✅ USE the standard Base.html template shown above
- ✅ USE PeakMade brand colors, gradients, typography
- ✅ USE proven Bootstrap 5 + Font Awesome structure
- This is a **STANDARD** across all PeakMade apps (copy the template exactly)

**2. Data Storage (DESIGN FROM SCRATCH):**
- ❌ DO NOT copy storage architecture from old LeaseFileAudit
- ❌ DO NOT copy from any other app (each has different storage needs)
- ❌ DO NOT use AuditRuns2/CSV/Parquet patterns (all problematic)
- ✅ DESIGN NEW SharePoint list architecture specific to this app's requirements
- Data storage rules are **PROJECT-SPECIFIC**, not reusable like UI templates

---

**I'm starting a NEW LeaseFileAudit project from scratch. The immediate focus is Phase 4 (Storage Architecture).**

**What's the best way to architect SharePoint list-based data persistence so that:**
1. Transaction details are available IMMEDIATELY (within 2 minutes, not after all batches)
2. Queries are fast (lease detail page loads in <2 seconds)
3. Audit history is preserved (no automatic deletion of old runs)
4. Data survives app restarts (durable storage)
5. Architecture is simple (easy to implement, debug, and maintain)

**Given:**
- ~18,000 transaction detail rows per audit (9,000 expected + 9,600 actual)
- ~5,966 aggregated snapshot rows per audit (already handled via RunDisplaySnapshots)
- Multiple audits per week (data accumulates over time)
- SharePoint list item limit: 30M items (but query perf degrades after 5K without indexing)

**DO NOT replicate these old architecture patterns:**
- ❌ **AuditRuns2-style list** - storing millions of rows with async background writes (caused 10-30 min delays)
- ❌ **CSV/Parquet files** - file-based storage written after audit completes (caused 15+ min delays)
- ❌ **Multi-layer storage** - separate CSV + Parquet + SharePoint list (complex, unclear source of truth)
- ❌ **Unindexed queries** - filtering large lists without proper indexing (caused 5K item degradation)

---

## Implementation Guidelines - Do This RIGHT in the New App

### ✅ 1. Write Data INCREMENTALLY (Not After Audit Completes)

**OLD WAY (WRONG):**
```python
# Wait for entire audit to finish, THEN write everything
for property_id in properties:
    results = audit_property(property_id)  # 10-15 minutes
    all_results.append(results)

# User waits 30 minutes before ANY data is available
write_to_sharepoint(all_results)  # Another 10-15 minutes
```

**NEW WAY (RIGHT):**
```python
# Write as you go - data available immediately
for property_id in properties:
    results = audit_property(property_id)  # 10-15 minutes
    write_to_sharepoint_immediately(results)  # Write happens NOW
    # User can view this property's data while next property audits
```

**Key Principle:** Write in batches of 1,000 rows or every 2 minutes (whichever comes first). User sees results as they become available.

---

### ✅ 2. Use Proper SharePoint List INDEXING (Prevent Query Degradation)

**Required Indexed Columns:**
- `RunId` (Text) - Every query filters by this
- `PropertyId` (Number) - Property view queries need this
- `LeaseIntervalId` (Number) - Lease view queries need this
- `AuditMonth` (Text) - Often used in filters

**How to Create Indexes in SharePoint:**
1. Navigate to SharePoint list settings
2. For each column: Click column name → Column settings → "Indexed" → Yes
3. Verify: List settings → "Indexed columns" shows all 4 columns
4. Test query speed: Should remain <2 seconds even with 100K+ items

**Why This Matters:**
- Without indexes: Queries degrade after 5,000 items (10+ seconds)
- With indexes: Queries stay fast with millions of items (<2 seconds)

---

### ✅ 3. Single Source of Truth (One List, Not Three Storage Layers)

**OLD WAY (WRONG):**
- AuditRuns2 SharePoint list (18K rows per audit)
- CSV files in Document Library (intermediate storage)
- Parquet files in Document Library (final storage)
- Cache in memory
- **Problem:** Which one is the "truth"? Where do I query from?

**NEW WAY (RIGHT):**
- ONE `AuditTransactionDetail` SharePoint list (all transaction data)
- ONE `RunDisplaySnapshots` SharePoint list (aggregated summaries)
- In-memory cache (read-through pattern: check cache → if miss, query list → populate cache)
- **Clear hierarchy:** SharePoint list is source of truth, cache is performance optimization

**Query Pattern:**
```python
def get_lease_detail(run_id, lease_id):
    # Try cache first
    cache_key = f"lease_{run_id}_{lease_id}"
    cached = flask_cache.get(cache_key)
    if cached:
        return cached
    
    # Cache miss - query SharePoint list
    data = query_sharepoint_list(
        filter=f"RunId eq '{run_id}' and LeaseIntervalId eq {lease_id}"
    )
    
    # Populate cache for next time (4 hour TTL)
    flask_cache.set(cache_key, data, timeout=14400)
    return data
```

---

### ✅ 4. NO Silent Background Threads (Writes Complete Before User Navigation)

**OLD WAY (WRONG):**
```python
# Start background thread to write AuditRuns2
thread = Thread(target=write_auditruns2, args=(results,))
thread.start()

# Immediately show user "Audit Complete!" message
# But data isn't actually available yet (silent failure)
return "Audit complete! View results."
```

**NEW WAY (RIGHT):**
```python
# Write data synchronously before returning
write_to_sharepoint(results)  # Blocks until complete

# Only show "complete" when data is ACTUALLY available
return "Audit complete! View results."
```

**Alternative (If Async Needed):**
```python
# Use job queue with visible status
job_id = create_audit_job(property_ids)
return {"job_id": job_id, "status": "running"}

# Polling endpoint shows real-time progress
@app.route('/api/audit-status/<job_id>')
def audit_status(job_id):
    return {
        "status": job.status,  # running, writing, complete
        "properties_complete": 45,
        "properties_total": 94,
        "rows_written": 850000
    }
```

**Key Principle:** If data isn't available yet, tell the user clearly. Don't silently fail.

---

### ✅ 5. Batch Size Optimization (20 rows per Graph API batch)

**Graph API Constraints:**
- Maximum 20 requests per `$batch` operation
- Total payload size: ~4MB per batch
- Rate limiting: ~1,000 requests per minute per tenant

**Optimal Write Strategy:**
```python
def write_in_batches(dataframe, batch_size=20):
    total_rows = len(dataframe)
    for i in range(0, total_rows, batch_size):
        batch = dataframe.iloc[i:i+batch_size]
        
        # Build $batch request
        requests = []
        for idx, row in batch.iterrows():
            requests.append({
                "id": str(idx),
                "method": "POST",
                "url": f"/sites/{site_id}/lists/{list_id}/items",
                "body": {"fields": row.to_dict()}
            })
        
        # Send batch (20 rows at once)
        response = graph_api_batch(requests)
        
        # Log progress every 1,000 rows
        if i % 1000 == 0:
            print(f"Written {i}/{total_rows} rows ({i/total_rows*100:.1f}%)")
```

**Performance Math:**
- 18,000 rows ÷ 20 per batch = 900 batches
- 900 batches × 1 second per batch = **~15 minutes total**
- Writing incrementally: User sees first 1,000 rows in **1 minute**

---

### ✅ 6. Cache Strategy (Read-Through Pattern with TTL)

**Pattern:**
1. **On Write:** Write to SharePoint list, invalidate cache for that RunID
2. **On Read:** Check cache → if miss, query SharePoint → populate cache
3. **TTL:** 4 hours (audit data doesn't change after initial write)

**Implementation:**
```python
from flask_caching import Cache

cache = Cache(config={
    'CACHE_TYPE': 'simple',  # In-memory
    'CACHE_DEFAULT_TIMEOUT': 14400  # 4 hours
})

@cache.memoize(timeout=14400)
def get_property_summary(run_id, property_id):
    # Automatically cached based on arguments
    return query_sharepoint_list(
        filter=f"RunId eq '{run_id}' and PropertyId eq {property_id}"
    )

def write_audit_results(run_id, data):
    # Write to SharePoint
    write_to_sharepoint(data)
    
    # Invalidate cache for this run
    cache.delete_memoized(get_property_summary, run_id, '*')
```

**Key Principle:** Cache is for performance, SharePoint list is source of truth.

---

### ✅ 7. Error Handling with Retry Logic (Handle Graph API Failures)

**Common Failure Scenarios:**
- 429 Too Many Requests (rate limiting)
- 503 Service Unavailable (SharePoint throttling)
- Network timeouts
- Token expiration

**Retry Pattern:**
```python
import time
from requests.exceptions import HTTPError

def write_with_retry(batch_data, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.post(
                graph_api_url,
                headers={"Authorization": f"Bearer {token}"},
                json=batch_data,
                timeout=30
            )
            
            if response.status_code == 429:
                # Rate limited - respect Retry-After header
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except HTTPError as e:
            if attempt == max_retries - 1:
                # Final attempt failed - log and re-raise
                print(f"FAILED after {max_retries} attempts: {e}")
                raise
            
            # Exponential backoff: 2^attempt seconds
            wait_time = 2 ** attempt
            print(f"Attempt {attempt+1} failed. Retrying in {wait_time}s...")
            time.sleep(wait_time)
```

**Key Principle:** Always retry transient failures. Log permanent failures for debugging.

---

### ✅ 8. List Size Management (Prevent 30M Item Limit)

**Retention Strategy:**
```python
# Keep audits for 90 days, then archive/delete
RETENTION_DAYS = 90

def cleanup_old_audits():
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    
    # Query old runs
    old_runs = query_sharepoint_list(
        filter=f"RunDate lt '{cutoff_date.isoformat()}'"
    )
    
    # Option 1: Delete (permanent)
    for run in old_runs:
        delete_sharepoint_item(run['Id'])
    
    # Option 2: Archive to separate list (preserves history)
    for run in old_runs:
        copy_to_archive_list(run)
        delete_sharepoint_item(run['Id'])
```

**Growth Projections:**
- 18,000 rows per audit
- 3 audits per week = 54,000 rows/week
- 52 weeks = 2.8M rows/year
- 10 years = 28M rows (approaching 30M limit)
- **Solution:** Implement retention policy after 2 years

---

### ✅ 9. Testing Strategy (Validate Early and Often)

**Test Pyramid:**
1. **Unit Tests** (fast, many):
   - Test mapping functions in isolation
   - Test expansion logic with known inputs
   - Test matching algorithm with sample data

2. **Integration Tests** (medium speed, some):
   - Test API client against sandbox environment
   - Test SharePoint writes/reads with test list
   - Test cache behavior (hits, misses, invalidation)

3. **End-to-End Tests** (slow, few):
   - Run full audit for test property (sandbox)
   - Verify data appears in <2 minutes
   - Verify queries return correct results
   - Test app restart (data persists)

**Key Test Case (Success Criteria):**
```python
def test_transaction_detail_availability():
    # Start audit
    run_id = start_audit(property_ids=[771903])
    
    # Wait 2 minutes
    time.sleep(120)
    
    # Verify lease detail is available
    data = get_lease_detail(run_id, lease_id=18296704)
    assert data is not None, "Data should be available within 2 minutes"
    assert len(data['expected']) > 0, "Expected transactions should exist"
    assert len(data['actual']) > 0, "Actual transactions should exist"
    assert data['resident_name'] != "", "Resident name should be populated"
```

---

### ✅ 10. Monitoring and Debugging (Add Visibility)

**Log Key Milestones:**
```python
import logging

logger = logging.getLogger(__name__)

def execute_audit_run(property_ids):
    run_id = generate_run_id()
    logger.info(f"[{run_id}] Audit started: {len(property_ids)} properties")
    
    # API fetch
    start_time = time.time()
    leases = fetch_lease_details(property_ids)
    logger.info(f"[{run_id}] API fetch complete: {len(leases)} leases in {time.time()-start_time:.1f}s")
    
    # Reconciliation
    start_time = time.time()
    results = reconcile_buckets(expected, actual)
    logger.info(f"[{run_id}] Reconciliation complete: {len(results)} buckets in {time.time()-start_time:.1f}s")
    
    # Write to SharePoint
    start_time = time.time()
    write_to_sharepoint(results)
    logger.info(f"[{run_id}] SharePoint write complete: {len(results)} rows in {time.time()-start_time:.1f}s")
    
    logger.info(f"[{run_id}] Audit complete: Total time {total_time:.1f}s")
```

**Add Progress Indicators in UI:**
```python
# WebSocket or polling endpoint for real-time progress
@app.route('/api/audit-progress/<run_id>')
def audit_progress(run_id):
    return {
        "stage": "writing_data",  # api_fetch, reconciliation, writing_data, complete
        "progress_pct": 67,
        "message": "Writing transaction details: 12,000 / 18,000 rows"
    }
```

**Key Principle:** Make the system observable. User should always know what's happening.

---

## Summary Checklist - Architecture Decisions Final

**✅ Storage Architecture Finalized:**

- ✅ **Single `AuditTransactionDetail` list** with `DetailType` column (EXPECTED/ACTUAL)
  - Chosen over separate lists - simpler schema, single query source
- ✅ **Per-property incremental writes** - each property viewable when its data completes
  - Not bulk writes after entire audit finishes
- ✅ **AuditRunStatus tracking** - controls when properties become viewable
  - Status progression: Queued → Fetching → Reconciling → Writing Detail → Viewable
- ✅ **No silent background threads** - writes complete synchronously before marking viewable
- ✅ **SharePoint lists as source of truth** - no CSV, no Parquet, no AuditRuns2
- ✅ **Proper indexing on all filter columns** - prevents 5K query degradation
- ✅ **20 rows per Graph API batch** - optimal for performance
- ✅ **Read-through cache with 4-hour TTL** - SharePoint for durability, cache for speed
- ✅ **Retry logic for transient failures** - handles 429 rate limiting, 503 throttling
- ✅ **90-day retention strategy** - prevents hitting 30M item limit

**✅ Three Audit Types Supported:**
- Property Audit: Single property, all leases
- Bulk Audit: Multiple properties, each viewable incrementally
- Single Lease Audit: One lease only, fast targeted audit

**✅ SharePoint Lists Created:**
1. **AuditRunStatus** - tracks progress, controls viewability
2. **AuditTransactionDetail** - durable transaction detail (EXPECTED + ACTUAL rows)
3. **RunDisplaySnapshots** - pre-aggregated summaries (Portfolio, Property, Lease, Month scopes)
4. **ExceptionMonths** - resolution workflow with historical matching

**✅ Query Patterns Defined:**
- Portfolio: RunDisplaySnapshots filtered by RunId + ScopeType
- Property: RunDisplaySnapshots filtered by RunId + PropertyId + ScopeType
- Lease: AuditTransactionDetail filtered by RunId + LeaseIntervalId, split by DetailType
- Historical resolutions: ExceptionMonths filtered by PropertyId + LeaseIntervalId + ArCodeId + AuditMonth

**✅ Implementation Build Order (30 Days):**
- Days 1-2: Flask shell + base template
- Days 3-4: Run Lease Audit page (3 audit types)
- Days 5-6: Entrata API client
- Days 7-8: Data normalization
- Day 9: Scheduled charge expansion
- Days 10-11: Reconciliation engine
- Days 12-13: SharePoint list helpers
- Days 14-15: Incremental transaction detail writes
- Day 16: RunDisplaySnapshots aggregation
- Day 17: AuditRunStatus lifecycle
- Days 18-19: Portfolio dashboard
- Days 20-21: Property detail view
- Days 22-24: Lease detail view
- Days 25-27: Exception resolution modal
- Days 28-29: Historical resolution matching
- Day 30: Polish & end-to-end testing

**✅ Expected Performance:**
- Property audit (668 leases): ~15-20 minutes to complete
- Transaction detail writes: ~15 minutes per property (900 batches × 1 sec)
- Lease detail view load: <2 seconds (with proper indexes)
- Data availability: Immediate per property (not waiting for entire audit)
- Query performance: Fast with millions of rows (indexed columns prevent degradation)

**✅ Key Success Metrics:**
- Property becomes viewable within 2 minutes of completing (not 30 minutes)
- Bulk audit: Property 1 viewable while Property 2 still processing
- Lease detail page loads in <2 seconds
- No silent failures (all writes complete before marking viewable)
- Audit history preserved for 90 days (survives app restarts)

---

**This prompt is now production-ready.** All architecture decisions have been made. Begin implementation with Phase 1 (Flask app shell).
