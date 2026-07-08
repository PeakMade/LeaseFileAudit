# Lease Billing Audit System - Core Requirements

## Purpose
Build an automated billing reconciliation system that compares **what we expected to bill** (scheduled charges) against **what we actually billed** (posted transactions) for student housing properties.

## Business Problem
- Property managers schedule recurring charges (rent, parking, pet fees, etc.) for thousands of residents
- Charges may not post correctly: missed entirely, billed at wrong amount, or billed without a schedule
- Manual verification is impossible at scale (20+ properties, 10,000+ leases)
- Need automated detection of billing discrepancies with drill-down to transaction level

---

## Data Sources

### Input 1: Scheduled Charges (from Entrata API `getLeaseDetails`)
**What it is:** The charges we EXPECTED to bill

**Raw fields:**
- Property ID, Property Name
- Lease ID, Unit Number, Resident Names
- AR Code ID (charge type: 154771 = "Rent", etc.)
- Amount ($1,234.56)
- Start Date, End Date (e.g., 08/01/2024 - 04/30/2025)
- Frequency (Monthly, One-time, etc.)

**Key transformations needed:**
- Expand recurring charges to individual months (1 schedule → 9 monthly buckets)
- Filter to audit date range (e.g., 08/2024 - 04/2025)
- Apply AR code whitelist (only audit specific charge types like Rent)
- Remove deleted/invalid charges

### Input 2: Actual Transactions (from Entrata API `getLeaseArTransactions`)
**What it is:** The charges we ACTUALLY billed

**Raw fields:**
- Property ID, Property Name
- Lease ID
- AR Code ID
- Amount ($1,234.56)
- Post Date (when charge hit resident ledger)
- Audit Month (which month the charge applies to)
- Transaction ID (unique identifier like "AR-12345")

**Key transformations needed:**
- Filter to audit date range
- Apply AR code whitelist
- Remove API-posted charges (system-generated, not scheduled)

---

## Core Reconciliation Logic

### Three-Tier Matching Algorithm

**TIER 1: Exact Match** (✅ MATCHED)
- Property ID matches
- Lease ID matches
- AR Code matches
- Audit Month matches
- Amount matches (within $0.01 tolerance)
→ **Result:** Status = MATCHED, Variance = $0.00

**TIER 2: Amount Mismatch** (⚠️ AMOUNT_MISMATCH)
- All identifiers match (property, lease, AR code, month)
- Amount differs by more than $0.01
→ **Result:** Status = AMOUNT_MISMATCH, Variance = actual - expected

**TIER 3: Unbilled/Unexpected**
- **Expected charge has no matching actual** → SCHEDULED_NOT_BILLED (high severity)
- **Actual transaction has no matching expected** → BILLED_NOT_SCHEDULED (high severity)
→ **Result:** High-priority findings requiring manual review

### Matching Keys (Group By)
- Property ID
- Lease ID
- AR Code ID
- Audit Month

---

## Calculated Metrics

For each scope (portfolio, property, lease, month):

1. **Match Rate %**
   - Formula: (matched charges / total expected charges) × 100
   - Target: 98%+ indicates healthy billing

2. **Undercharge $**
   - Sum of scheduled charges that were never billed
   - Represents potential lost revenue

3. **Overcharge $**
   - Sum of unexpected transactions (no matching schedule)
   - Risk of resident disputes

4. **Exception Count**
   - Number of discrepancies requiring manual review
   - Includes both amount mismatches and unbilled/unexpected

5. **Variance $**
   - Total difference: Actual Total - Expected Total
   - Negative = underbilled, Positive = overbilled

---

## User Workflow

### 1. Bulk Audit
1. User selects properties from picklist (e.g., "CLEMSON EDGE", "REDPOINT ATHENS")
2. Selects audit period (e.g., "Current Academic Year" or specific month/year)
3. Clicks "Start Audit"
4. System fetches data from Entrata API
5. System runs reconciliation
6. User can immediately view results

### 2. View Results - Portfolio Dashboard
- Shows aggregated KPIs across all properties
- Lists properties with exception counts
- Click property to drill down

### 3. View Results - Property Detail
- Shows property-level KPIs
- Table of leases with match rates
- List of unresolved exceptions
- Click lease to drill down

### 4. View Results - Lease Detail
**THIS IS THE KEY VIEW - MUST WORK CORRECTLY**

Display for each lease:
- **Lease metadata:** Resident name, unit, lease dates
- **Monthly summary table:** One row per (month + AR code) with Expected/Actual/Variance
- **Expected Transactions table:**
  - Columns: Period (start-end), Amount, Status
  - Example: "08/01/2024 - 08/31/2024 | $1,234.56 | Scheduled"
- **Actual Transactions table:**
  - Columns: Post Date, Amount, Transaction ID, Entrata Link
  - Example: "08/05/2024 | $1,234.56 | AR-12345 | [View in Entrata]"
  - Button to open transaction in Entrata (opens external browser)

### 5. Resolve Exceptions
- User clicks "Resolve Exception" on lease with discrepancy
- Modal opens: select resolution reason, add notes, mark status (Resolved/Acknowledged)
- System saves resolution to tracking list
- Exception count decrements on property view

---

## Critical Requirements

### Must-Haves
1. **Immediate data availability:** Transaction details must be visible as soon as reconciliation completes (no waiting for background saves)
2. **AR code filtering:** Only audit whitelisted AR codes (configurable, default: [154771] for Rent)
3. **Transaction arrays:** Lease view must show individual expected charges and actual transactions (not just summary totals)
4. **Entrata deep links:** "Open in Entrata" button must construct correct URL and open in external browser
5. **Exception tracking:** Manual resolution workflow must persist across audits

### Performance Targets
- **Audit execution:** Complete 668-lease property in <5 minutes
- **Data availability:** Transaction detail visible within 2 minutes of audit start
- **Page load:** Property/lease detail views load in <2 seconds
- **Concurrent audits:** Support multiple properties auditing in parallel

### Data Volume Expectations
- **Single property audit:** 
  - 668 leases
  - ~9,000 expected charge records (after expansion to months)
  - ~9,600 actual transaction records
  - ~5,966 aggregated snapshot rows (portfolio + property + lease + month scopes)

---

## Configuration

### AR Code Whitelist (`excluded_ar_codes.json`)
```json
{
  "allowed_ar_codes": [154771]
}
```
Only audit AR codes in this list. Common codes:
- 154771 = Rent (base)
- 154772 = Parking
- 154773 = Pet Fee
- etc.

### Excluded Properties (`excluded_properties.json`)
```json
{
  "excluded_property_ids": ["123456", "789012"]
}
```
Skip these properties entirely (test properties, closed properties, etc.)

### Entrata Environment (`entrata_environment.json`)
```json
{
  "environment": "prod"
}
```
Toggle between `"prod"` (live data) and `"sandbox"` (test environment)

---

## Technical Constraints

### API Limits
- Entrata API has rate limits (exact limits unknown)
- Must handle API errors gracefully (retry logic, timeout handling)
- Properties with 1,000+ leases may require pagination or batching

### Data Quality Issues
- Some scheduled charges have NULL/invalid dates
- AR transactions may have incorrect AUDIT_MONTH values
- Lease renewals create duplicate lease IDs (must handle interval realignment)

### Authentication
- Azure AD single sign-on (production)
- API key authentication for Entrata
- SharePoint access via Microsoft Graph API (app-only token)

---

## What I DON'T Need Help With (Already Working)
- ✅ Entrata API integration (fetching data works)
- ✅ Three-tier matching algorithm (reconciliation logic works)
- ✅ Flask web framework (routes and templates work)
- ✅ KPI calculation (metrics are correct)
- ✅ Azure AD authentication (SSO works)

## What I DO Need Help With (Focus Areas)
- ❓ **Data persistence strategy:** How to store transaction detail for immediate access without complex file I/O
- ❓ **Incremental availability:** How to make transaction details visible BEFORE entire audit completes
- ❓ **Query performance:** How to load lease detail view fast when there are 18,000+ transaction rows per audit
- ❓ **Audit history:** How to preserve multiple audit runs without deleting old data
- ❓ **Cache strategy:** How to balance in-memory speed with durability across app restarts

---

## Success Criteria

**The system works correctly when:**
1. ✅ User runs audit for property 771903 ("CLEMSON EDGE")
2. ✅ Portfolio view loads showing 1 property with KPIs
3. ✅ Property view loads showing 668 leases in table
4. ✅ User clicks lease 18296704
5. ✅ Lease view loads with:
   - Resident name displayed (not blank)
   - AR code name shows "Rent" (not "-")
   - Expected transactions table shows 9 rows with Period and Amount
   - Actual transactions table shows 9 rows with Post Date, Amount, Transaction ID
   - "Open in Entrata" button is enabled (not greyed out)
6. ✅ User can click "Resolve Exception" and mark status
7. ✅ User can run second audit without deleting first audit
8. ✅ User can navigate back to first audit and still see all transaction details

**The system is FAST when:**
- Audit completes in <5 minutes (668 leases)
- Lease detail view loads in <2 seconds
- Transaction details are visible within 2 minutes of starting audit (not after all saves complete)

---

## Out of Scope (Future Enhancements)
- ❌ Email notifications when audit completes
- ❌ Scheduled/automated audits (cron jobs)
- ❌ Excel export of audit results
- ❌ Multi-tenant support (other organizations)
- ❌ Advanced filtering/search on transaction detail
- ❌ Bulk exception resolution (mark all as resolved)
- ❌ Audit comparison (run 1 vs run 2 diff)

---

## Key Question to Answer
**"How do I store and retrieve ~18,000 transaction detail rows per audit so that:**
- **They're available immediately** (within 2 minutes, not 15 minutes)
- **Queries are fast** (lease view loads in <2 seconds)
- **History is preserved** (can view old audits months later)
- **App restart doesn't lose data** (durable storage)
- **Architecture is simple** (minimal overhead, easy to debug)"
