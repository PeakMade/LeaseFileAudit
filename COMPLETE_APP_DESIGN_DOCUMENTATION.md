# LeaseFileAudit - Complete Application Design Documentation

## Overview
This document provides comprehensive documentation of all screens, functionality, workflows, design elements, and implementation details for the LeaseFileAudit application to enable replication of identical design and functionality in new applications.

---

## Table of Contents
1. [Brand & Design System](#brand--design-system)
2. [Application Architecture](#application-architecture)
3. [Screen-by-Screen Documentation](#screen-by-screen-documentation)
4. [Workflow & Navigation](#workflow--navigation)
5. [Data Model & IDs](#data-model--ids)
6. [Resolution System](#resolution-system)
7. [UI Components Library](#ui-components-library)
8. [Entrata API Error Handling](#entrata-api-error-handling)
9. [Data Storage Architecture](#data-storage-architecture)

---

## Quick Reference: Common Issues

### ❌ "403 Forbidden - App doesn't have permission to the property"
**Problem**: Property audit returns Entrata API error code 311  
**Cause**: Property ID belongs to different environment (Production vs Sandbox)  
**Fix**: Go to Settings → Switch Entrata environment, OR use a property ID from current environment  
**Details**: [Entrata API Error Handling](#entrata-api-error-handling)

### ❌ Property not showing in picklist
**Cause**: Property excluded in `excluded_properties.json`, OR wrong environment  
**Fix**: Check Settings → Exclusion Configuration, OR toggle environment  

### ❌ Audit taking too long
**Cause**: Large property with many leases, SharePoint throttling  
**Fix**: Enable `SHAREPOINT_WRITE_EXCEPTIONS_ONLY=true` to skip MATCHED rows  

### ❌ Resident names showing as "—"
**Cause**: CUSTOMER_NAME field missing in source data  
**Fix**: Ensure Excel upload includes resident name column, OR API returns CustomerName  

### ❌ SharePoint connection failing
**Cause**: Azure AD token expired, credentials invalid  
**Fix**: Refresh credentials, check SHAREPOINT_CLIENT_ID/TENANT_ID in `.env`

---

## Brand & Design System

### Color Palette
```css
--color-primary: #00a8c8      /* Teal - Primary brand color */
--color-secondary: #231f20    /* Dark gray - Secondary */
--color-accent: #ff6600       /* Orange - Accent/warnings */
--color-danger: #c20068       /* Magenta - Errors/undercharge */
--color-light: #f4f4f4        /* Light gray background */
```

### Typography
- **Font Family**: Montserrat (Google Fonts)
- **Headline/Headers**: Montserrat Bold (700)
- **Body Text**: Montserrat Regular (400)
- **Used For**: All text, buttons, badges, tables

### Button Styles

**Primary Button** (Blue gradient):
```css
background: linear-gradient(135deg, #0d6efd 0%, #4a9eff 100%)
border-radius: 8px
padding: 10px 25px
font-weight: 500
hover: translateY(-2px) + shadow
```

**Success Button** (Teal gradient):
```css
background: linear-gradient(135deg, #00a8c8 0%, #33bdd8 100%)
border-radius: 8px
```

**Danger Button** (Magenta gradient):
```css
background: linear-gradient(135deg, #c20068 0%, #d42080 100%)
border-radius: 8px
```

**Secondary Button** (Dark gradient):
```css
background: linear-gradient(135deg, #231f20 0%, #3d3a3b 100%)
color: white
border-radius: 8px
```

### Card Styles
```css
border: none
border-radius: 12px
box-shadow: 0 4px 15px rgba(0,0,0,0.1)
margin-bottom: 20px
```

**Card Header**:
```css
background: linear-gradient(135deg, #00a8c8 0%, #0088a8 100%)
color: white
border-radius: 12px 12px 0 0
padding: 7px 20px
font-size: 1.2rem
```

### Badge Styles
- **Success**: `bg-success` (green) - Matched items
- **Danger**: `bg-danger` (red) - Exceptions/discrepancies
- **Warning**: `bg-warning` (yellow) - Needs review
- **Info**: `bg-info` (blue) - Informational
- **Secondary**: `bg-secondary` (gray) - Neutral status

---

## Application Architecture

### Tech Stack
- **Backend**: Flask (Python)
- **Frontend**: Bootstrap 5.3.0, Font Awesome 6.4.0, Vanilla JavaScript
- **Data Storage**: 
  - CSV files (primary backup)
  - SharePoint Lists (AuditRuns2, RunDisplaySnapshots, Audit Run Metrics, ExceptionMonths)
  - In-memory cache (recent runs)
- **Authentication**: Azure AD (Easy Auth)

### URL Structure
```
/                                           # Home/Upload screen
/portfolio                                  # Portfolio overview (all properties)
/portfolio/<run_id>                         # Portfolio for specific run
/property/<property_id>                     # Property details (latest run)
/property/<property_id>/<run_id>            # Property details for specific run
/lease/<run_id>/<property_id>/<lease_interval_id>  # Lease details
/bucket/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>/<audit_month>  # Bucket drilldown
/bulk-audit                                 # Bulk audit configuration
/settings                                   # Application settings
```

### API Endpoints
```
GET  /api/runs                              # Get available runs
POST /api/admin/clear-cache                 # Clear application cache
GET  /api/property-picklist                 # Get Entrata property list
POST /upload                                # Upload Excel file
POST /upload-api-property                   # Run property audit via API
POST /upload-api-lease                      # Run single lease audit via API
POST /api/bulk-audit                        # Start bulk audit job
GET  /api/bulk-audit/<job_id>               # Get bulk audit status
POST /api/bulk-audit/<job_id>/cancel        # Cancel bulk audit
GET  /api/exception-months/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>  # Get exception month status
POST /api/exception-months                  # Save resolution for exception month
GET  /api/exception-months/ar-status/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>  # Get AR status summary
GET  /api/lease-terms/<run_id>/<property_id>/<lease_interval_id>  # Get lease terms
```

---

## Screen-by-Screen Documentation

### 1. Home / Upload Screen (`/`)

**Layout**: Single column, centered (max-width: 1200px)

**Components**:

#### A) Header
```
Title: "Run Lease Audit"
Icon: fas fa-network-wired
Subtitle: "Launch audits directly from Entrata without using the Excel upload workflow."
```

#### B) Run Property Audit Card (Left, 7 columns)
**Border**: 2px solid #00a8c8
**Background**: #f8fcfd
**Title**: "Run Property Audit" with fas fa-building icon

**Form Fields**:
1. **Property Selector** (autocomplete with datalist)
   - Label: "Property"
   - Placeholder: "Loading properties from Entrata API..." → "Select Property..."
   - Format: "Property Name (Property ID)"
   - Hidden field: `api_property_id` (stores numeric ID)

2. **Academic Year Selector** (dropdown)
   - Label: "Academic Year (Auto-fills date range)"
   - Options: Populated from backend (2024-2025, 2025-2026, 2026-2027)
   - Auto-fills from/to dates when selected
   - Note: "Or manually enter dates below to override"

3. **Transaction From Date** (date picker)
   - Label: "Transaction From Date"
   - Auto-populated by academic year selection

4. **Transaction To Date** (date picker)
   - Label: "Transaction To Date"
   - Auto-populated by academic year selection

**Actions**:
- Primary Button: "Run Property Audit" (blue, with fas fa-search icon)
- Secondary Link: "Go to Portfolio" (small, secondary style)

**POST to**: `/upload-api-property`

#### C) Run Single Lease Audit Card (Right, 5 columns)
**Border**: 2px solid #198754 (green)
**Background**: #fbfefc
**Title**: "Run Single Lease Audit" with fas fa-user-check icon

**Form Fields**:
1. **Lease ID** (number input)
   - Label: "Lease ID"
   - Placeholder: "e.g. 12345"
   - Required, step="1"

2. **Academic Year / Date Range** (same as property audit)

**Actions**:
- Primary Button: "Run Lease Audit" (green)

**POST to**: `/upload-api-lease`

---

### 2. Portfolio Screen (`/portfolio` or `/portfolio/<run_id>`)

**Layout**: Full-width fluid container with 20px top padding

#### A) Header Section
**Left side**:
- Title: "PeakMade Portfolio" (h2)
- Back button: "Back to Home" (small, outline-secondary)
- Metadata:
  - Audit ID: `<code>run_id</code>`
  - Executed: `timestamp` (converted to local timezone)
  - Month(s) Audited: `analysis_period`

**Run Selector** (dropdown):
- Width: 260px
- Font size: 0.85rem
- Format: "MM/DD/YYYY - Manual/Auto"
- onchange → reload page with selected run

#### B) Overall Dashboard KPIs (3-column row)

**Card Layout**: Full-width card with 3 centered columns

**Column 1 - Current Undercharge**:
```
Label: "Current Undercharge" (text-muted)
Value: $X,XXX.XX (color: #c20068, h3)
Subtext: "Historical: $X,XXX.XX" (small, text-muted)
```

**Column 2 - Current Overcharge**:
```
Label: "Current Overcharge" (text-muted)
Value: $X,XXX.XX (color: #00a8c8, h3)
Subtext: "Historical: $X,XXX.XX" (small, text-muted)
```

**Column 3 - Open Discrepancies**:
```
Label: "Open Discrepancies" (text-muted)
Value: XXX (color: #ff6600, h3)
Subtext: "Match Rate: XX.X%" (small, text-muted)
```

#### C) Future Lease Audit KPIs (Conditional)
**Display**: Only if `future_lease_kpis.total_future_leases > 0`
**Border**: `border-info`
**Header**: Info background with white text
- Title: "Future Lease Audit Results" with fas fa-calendar-check icon
- Subtitle: "Lease contract validation for future-status leases"

**5 Columns**:
1. **Total Future Leases** (audited count)
2. **Pass** (green, with percentage)
3. **Needs Review** (yellow, "Missing data")
4. **True Discrepancies** (red, "Action required")
5. **Potential Variance** (orange, with Under/Over breakdown)

#### D) Property Summary Table

**Header Controls** (flex-wrap, gap-3):
- Title: "Properties" (h5)
- Search input: "Search by property name…" (width: 210px)
- Audit Status Filter dropdown: "All statuses", "Not Started", "In Progress", "Complete" (width: 160px)
- Checkbox: "Discrepancies only"
- Property count badge
- "Run Bulk Audit" button (orange #ff6600)

**Table Structure**:
```
Columns (all sortable):
1. Property Name         - Left aligned, clickable sort
2. Property ID          - Numeric, clickable sort
3. Audit Status         - Center aligned, badge (not_started/in_progress/complete)
4. Audited Through      - Date, center aligned
5. Discrepancies        - Center, badge (red if > 0)
6. Undercharge          - Right aligned, currency, red color
7. Overcharge           - Right aligned, currency, teal color
8. Match Rate           - Right aligned, percentage
9. Actions              - "View Details" link
```

**Row Interaction**:
- Entire row clickable → navigates to property detail
- Hover effect: light gray background
- Link format: `/property/<property_id>/<run_id>`

**Sorting**:
- Click header → toggle ascending/descending
- Icon changes: ↕ (neutral) → ↓ (desc) → ↑ (asc)
- Default sort: Property Name (ascending)

**Filtering**:
- Search: Real-time filter by property name (case-insensitive)
- Status filter: Filter by audit status
- Discrepancies only: Show only properties with exceptions > 0
- Updates count badge dynamically

---

### 3. Property Screen (`/property/<property_id>/<run_id>`)

**Layout**: Full-width fluid container with top margin

#### A) Header Section
```
Title: Property Name (h2) or "Property {id}"
Subtitle (text-muted, 0.85rem):
  - Property ID: {id}
  - Audit Run: <code>run_id</code>
  - Executed: timestamp
  - Analyzing Data: analysis_period
```

**Action Buttons** (top right):
- "Back to Portfolio" (secondary, with left arrow icon)

**Run Selector** (dropdown):
- Same as portfolio screen
- Lazy-loaded on first interaction (focus/mousedown/touchstart)

#### B) KPI Summary Cards (3-column row)

**Card Styling**: Height: 80px, 2px border, border-radius: 8px

**Card 1 - Discrepancies**:
```
Border color: #ff6600
Label: "Discrepancies" (0.75rem, text-muted)
Value: Count (h5, #ff6600)
```

**Card 2 - Total Undercharge**:
```
Border color: #c20068
Label: "Total Undercharge" (0.75rem, text-muted)
Value: $X,XXX.XX (h5, #c20068)
```

**Card 3 - Total Overcharge**:
```
Border color: #00a8c8
Label: "Total Overcharge" (0.75rem, text-muted)
Value: $X,XXX.XX (h5, #00a8c8)
```

#### C) Leases Table

**Header Controls** (flex-wrap, py-1):
- Title: "Leases" with fas fa-table icon
- Badge: "{count} Total" (bg-secondary)
- Search input: "Search resident…" (width: 200px)
- Status filter: "All Statuses", "Clean", "Has Discrepancies" (width: 155px)
- Lease count badge (dynamic)

**Table Structure**:
```
Sticky header with light gray background

Columns (sortable):
1. Resident Name        - 180px, sorted desc by default
2. Lease ID            - 150px
3. Lease Interval ID   - 170px
4. Matched             - 100px, center, green badge if > 0
5. Discrepancies       - 110px, center, red badge if > 0, sorted desc by default
6. Undercharge         - Right aligned, $X,XXX.XX, red
7. Overcharge          - Right aligned, $X,XXX.XX, teal
8. Details             - Center, eye icon link
```

**Row Styling**:
- Border: 1px solid #dee2e6
- Hover: light background
- Clickable → navigates to lease detail
- Link: `/lease/<run_id>/<property_id>/<lease_interval_id>`

**Sorting**:
- Default: Resident Name (ascending), Discrepancies (descending as secondary)
- Click any header to toggle sort
- Multi-column sort support

---

### 4. Lease Screen (`/lease/<run_id>/<property_id>/<lease_interval_id>`)

**Layout**: Full-width fluid container with top margin

#### A) Header Section
```
Title: Customer Name (h2) or "Lease Details"
Subtitle: Lease ID: {id} | Lease Interval ID: {id}
```

**Action Buttons** (top right):
- "Open in Entrata" (primary, external link icon) - Opens Entrata resident profile
  - Disabled if Customer ID not available
- "Back to Property" (secondary, left arrow)

#### B) Summary Banner
**Alert Style**: alert-light with border

**Content**:
```
Total Undercharge: $X,XXX.XX (red #c20068)
Total Overcharge: $X,XXX.XX (teal #00a8c8)
```

#### C) Lease-Only Expectations Banner (Conditional)
**Display**: Only if expectations exist without transactions
**Alert Style**: alert-warning with border

**Content**:
```
Title: "Expected in Lease, no scheduled charges or transactions matched:"
List: Bullet list of AR codes/names
```

#### D) Lease Terms Loading Indicator (Conditional)
**Display**: While refreshing from Entrata API
```
Spinner + text: "Refreshing lease documents from Entrata API…"
```

#### E) AR Code Details Table

**Card Header**: "AR Code Details"

**Table Structure**:
```
Columns:
1. AR Code          - 120px, numeric ID
2. AR Code Name     - Text description
3. Matched          - 120px, center, green badge
4. Discrepancies    - 120px, center, red badge
5. Status           - 180px, center, colored badge
6. Details          - 80px, center, eye icon
```

**Row Interaction**:
- Click anywhere → opens side drawer with month-by-month details
- Cursor: pointer
- Hover: light background

**Status Badge Colors**:
- **Not Started**: bg-secondary (gray)
- **In Progress**: bg-warning (yellow)
- **Complete**: bg-success (green)
- Dynamically updates when resolutions saved

#### F) Side Drawer (AR Code Details)

**Drawer Styling**:
```
Position: fixed right side
Width: 600px
Height: 100vh
Background: white
Shadow: large left shadow
Z-index: 1050
Transform: translateX(100%) when closed
Transition: 0.3s ease
```

**Overlay**:
```
Position: fixed
Full viewport
Background: rgba(0,0,0,0.5)
Z-index: 1049
Click → closes drawer
```

**Drawer Header**:
```
Background: #00a8c8
Color: white
Padding: 15px 20px
Title: "AR Code {id} - {name}"
Close button: X button (white, right side)
```

**Drawer Body Content**:

**Month-by-Month Breakdown** (for each audit month):

**Month Card**:
```
Border: 1px solid based on status
Border-left: 4px colored stripe
Margin-bottom: 15px
```

**Month Card Header**:
```
Background: light gray
Bold text
Format: "YYYY-MM (Month Name)"
```

**Month Card Body**:

1. **Status/Amounts Row**:
```
3 columns:
- Status badge (left)
- Expected: $XXX.XX (center, labeled)
- Actual: $XXX.XX (center, labeled)
- Variance: $XXX.XX (right, colored red/teal based on +/-)
```

2. **Details Grid**:
```
Expected Rows: {count} rows
Actual Rows: {count} rows
(Clickable → expands detail table)
```

3. **Resolution Controls** (if discrepancy exists):

**Resolution Dropdown**:
```
Label: "Resolution Status"
Options:
  - Not Started (gray)
  - In Progress (yellow)
  - Complete (green)
  - N/A - Not Actionable
  - N/A - Out of Scope
```

**Fix Action Input**:
```
Label: "Fix Action Taken" (optional)
Placeholder: "Brief note about resolution..."
Textarea
```

**Resolved By / Resolved At** (display only if resolved):
```
Shows: "Resolved by: {user} on {date}"
```

**Save Button**:
```
"Save Resolution" (primary button, green)
Saves to ExceptionMonths SharePoint list
Updates status badge in real-time
Shows toast notification on success
```

**Expected/Actual Detail Tables** (expandable):
```
Columns for Expected:
- Scheduled Charge ID
- Amount
- Start Date
- End Date
- Charge Amount

Columns for Actual:
- Transaction ID
- Amount
- Post Date
- Post Month
- Is Reversal badge
```

#### G) Future Months Section (Conditional)
**Display**: Only if future scheduled charges exist
**Card Border**: border-info
**Card Header**: Info background

**Title**: "Future Lease — Scheduled Charges" with calendar icon
**Badge**: Count of months

**Alert**: Info alert explaining no discrepancy raised for future billing

**Table**:
```
Columns:
- AR Code (numeric)
- AR Code Name
- Month (YYYY-MM-DD)
- Expected Amount (right-aligned)
- Status badge (info, "Scheduled Only")
```

---

### 5. Bucket Drilldown Screen (`/bucket/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>/<audit_month>`)

**Layout**: Container-fluid

**Purpose**: Detailed view of a single bucket (grain: property + lease + AR code + month)

#### A) Header
```
Title: "Bucket Drilldown" (h2)
Subtitle: Run ID: <code>run_id</code>
```

**Action Button**:
- "Back to Property" (secondary)

#### B) Bucket Details Card

**Card Header**: bg-primary with white text, "Bucket Details"

**Bucket Info Grid** (2 rows x 4 columns):

**Row 1**:
```
Property ID: {id}
Lease ID: {lease_interval_id}
AR Code: {ar_code_id}
Audit Month: YYYY-MM-DD
```

**Row 2**:
```
Expected Total: $XXX.XX
Actual Total: $XXX.XX
Variance: $XXX.XX (colored red if ≠ 0)
Status: Badge (teal for MATCHED, magenta for exceptions)
```

#### C) Findings Card (Conditional)
**Display**: Only if findings exist
**Card Header**: bg-warning, "Findings ({count})"

**Finding Alert** (for each finding):
```
Background: #ffe6f2 (high) or #fff3e6 (medium)
Border: 1px solid #c20068 (high) or #ff6600 (medium)

Layout:
- Title (bold)
- Description (paragraph)
- Severity badge (right side, high/medium)
```

#### D) Expected Detail Card

**Card Header**: bg-info with white text
**Title**: "Expected (Scheduled Charges) - {count} records"

**Table**:
```
Columns:
- Scheduled Charge ID
- Expected Amount ($XXX.XX)
- Charge Amount ($XXX.XX)
- Date Start (YYYY-MM-DD)
- Date End (YYYY-MM-DD)

Footer Row (table-info):
- Total: $XXX.XX (bold)
```

**Empty State**:
```
"No scheduled charges for this bucket." (text-muted)
```

#### E) Actual Detail Card

**Card Header**: bg-success with white text
**Title**: "Actual (AR Transactions) - {count} records"

**Table**:
```
Columns:
- Transaction ID
- Actual Amount ($XXX.XX)
- Transaction Amount ($XXX.XX)
- Post Month (YYYY-MM-DD)
- Is Reversal (badge: warning for Yes, secondary for No)

Footer Row (table-success):
- Total: $XXX.XX (bold)
```

**Empty State**:
```
"No AR transactions for this bucket." (text-muted)
```

---

### 6. Bulk Audit Screen (`/bulk-audit`)

**Layout**: Centered container (max-width: 1100px, padding 20px 40px)

#### A) Header
```
Back button: "Back to Portfolio"
Title: "Run Bulk Audit" with fas fa-layer-group icon (#00a8c8)
```

#### B) Recent Bulk Audit Jobs (For each recent job)

**Card Styling**:
```
Border-left: 4px solid #00a8c8
Margin-bottom: 1.5rem
```

**Card Header**:
```
Background: light
Title: "Most Recent Bulk Audit" or "Previous Bulk Audit"
Icon: fas fa-history (#00a8c8)
Right side:
  - Status badge (complete/stopped)
  - "Details" collapse toggle button
```

**Card Body**:
```
Grid (g-3):
- Date Range: "from_date → to_date"
- Started: timestamp
- Properties: "completed / total completed"
- Errors: count (if any, red)
```

**Collapsible Details Table**:
```
Columns:
- Property Name
- Property ID (110px)
- Status (120px, center, colored badge)
- Run (monospace link to property view)

Status badges:
- Done: green #198754
- Error: red bg-danger
- Cancelled: gray bg-secondary
```

#### C) Audit Period Setup Card

**Card Header**: "Audit Period"

**Description**:
```
"Select the date range to audit. AR transactions will be fetched 
within this window; scheduled charges will be windowed to the 
same period automatically."
```

**Form Fields** (3-column grid):

1. **Academic Year Selector** (full width)
   - Label: "Academic Year (Auto-fills date range)"
   - Dropdown with pre-defined years
   - Auto-fills from/to dates
   - Override note

2. **From Date** (6 columns)
   - Label: "From Date"
   - Date input
   - Auto-filled by academic year

3. **To Date** (6 columns)
   - Label: "To Date"
   - Date input
   - Auto-filled by academic year

**JavaScript Behavior**:
- Academic year change → auto-fills dates
- Manual date change → clears academic year selection
- Bidirectional sync

#### D) Property Selection Card

**Card Header**: "Select Properties"

**All Properties Checkbox**:
```
Large checkbox with label
"Select/deselect all"
Toggle state updates all property checkboxes
```

**Property Checklist**:
```
Scrollable container (max-height: 400px)
For each property:
  - Checkbox (name="property_ids[]", value=property_id)
  - Label: "Property Name (Property ID)"
  - Display: block, margin-bottom
```

#### E) Start Bulk Audit Button

**Button**:
```
"Start Bulk Audit" (large, primary)
Icon: fas fa-play-circle
Disabled if no properties selected
POST to /api/bulk-audit
```

#### F) Progress Panel (During Execution)

**Display**: Hidden initially, shows when job starts

**Header**:
```
Title: "Bulk Audit Running"
Icon: fas fa-spinner fa-spin
Cancel button (danger, right side)
```

**Progress Bar**:
```
Bootstrap progress bar
Percentage: "X / Y completed (Z%)"
Striped, animated
Color: success
```

**Live Status Table**:
```
Auto-refreshes every 2 seconds via polling

Columns:
- Property Name
- Property ID
- Status (with colored badge)
- Run ID (link when complete)
- Error message (if failed)

Status sequence:
1. "pending" (gray)
2. "running" (blue with spinner)
3. "done" (green) → shows run_id link
4. "error" (red) → shows error message
5. "cancelled" (gray)
```

**Complete State**:
```
Progress bar → 100%, green
Title changes to "Bulk Audit Complete"
Shows summary: "X of Y succeeded, Z errors"
"View Results in Portfolio" button
```

---

### 7. Settings Screen (`/settings`)

**Layout**: Centered (max-width col-xl-10)

#### A) Entrata Environment Toggle Card

**Card Header**:
```
Left: "Entrata API Environment" with exchange icon
Right: Badge showing current environment
  - SANDBOX: bg-warning text-dark
  - PRODUCTION: bg-success
```

**Card Body**:
```
Description: Switch between environments
All API calls use selected environment

Form (inline):
- Button: "Switch to Production" (success) OR "Switch to Sandbox" (warning)
- Text: Current org info
  - Sandbox: peakmade-test-17291
  - Production: peakmade
```

**POST to**: `/settings` with `entrata_environment=prod|sandbox`

**Critical Use Case - Permission Errors**:
```
Problem: Property audit returns 403 Forbidden "App doesn't have permission"
Root Cause: Property ID belongs to different environment than current setting
Solution: Toggle to correct environment in Settings

Example:
- Property 9601 (sandbox) + Production environment = ❌ 403 Error
- Property 9601 (sandbox) + Sandbox environment = ✅ Success
```

**Implementation Details**: See [Entrata API Error Handling](#entrata-api-error-handling) section for complete code examples of:
- Environment toggle backend logic
- Property access validation
- User-friendly error messages
- Auto-detection of property environment

#### B) Exclusion Configuration Card

**Card Header**: "Exclusion Configuration" with sliders icon

**Sections** (each with `<details>` expandable):

1. **Current Excluded Resident Profile Names**:
```
Textarea (disabled, rows=10)
Shows current list (one per line)
```

2. **Add Resident Profile Names**:
```
Textarea (enabled, rows=3)
Placeholder: "One new name per line, case sensitive"
```

3. **Current Excluded Lease IDs**:
```
Textarea (disabled, rows=3)
Shows current list (one per line)
```

4. **Add Lease IDs**:
```
Textarea (enabled, rows=3)
One ID per line
```

5. **Current Excluded AR Codes**:
```
Textarea (disabled, rows=6, monospace)
Shows current list
```

**Save Button**:
```
"Save Changes" (primary, large)
Updates resident_profile_exclusions.json
Reloads configurations
Shows success message
```

---

## Workflow & Navigation

### Primary User Journeys

#### Journey 1: Excel Upload Audit
```
1. Home (/) 
   └─> Upload Excel → Processing
       └─> Portfolio (/portfolio/<run_id>)
           └─> Click property → Property (/property/<property_id>/<run_id>)
               └─> Click lease → Lease (/lease/<run_id>/<property_id>/<lease_interval_id>)
                   └─> Click AR code → Side drawer with month details
                       └─> Save resolutions → Update status badges
```

#### Journey 2: API Property Audit
```
1. Home (/)
   └─> Select property + date range → Submit
       └─> Processing with progress indicator
           └─> Redirect to Property (/property/<property_id>/<run_id>)
               └─> Same as Journey 1 from property level
```

#### Journey 3: API Lease Audit
```
1. Home (/)
   └─> Enter lease ID + date range → Submit
       └─> Processing
           └─> Redirect to Lease (/lease/<run_id>/<property_id>/<lease_interval_id>)
               └─> Review AR codes and resolve
```

#### Journey 4: Bulk Audit
```
1. Portfolio (/portfolio)
   └─> Click "Run Bulk Audit" → Bulk Audit Screen
       └─> Select properties + date range → Start
           └─> Watch live progress
               └─> Click "View Results" → Portfolio with all new runs
                   └─> Navigate individual properties/leases
```

#### Journey 5: Cross-Run Comparison
```
1. Any screen with run selector
   └─> Change run dropdown → Reload page with new run_id
       └─> Compare metrics/exceptions across runs
```

### Navigation Elements

**Global Navigation** (Present on all screens):
- User info display (top right):
  ```
  Name (email)
  Position: absolute, top: 100%, right: 20px
  Background: white, rounded, shadow
  ```

**Breadcrumb Pattern**:
```
Portfolio → Property → Lease → AR Code Detail
Each level has "Back to [Previous Level]" button
```

**Run Selector** (Portfolio, Property, Lease):
```
Dropdown format: "MM/DD/YYYY - Manual/Auto (run_id)"
Change → preserves current property/lease context with new run
Lazy-loaded on first interaction (performance optimization)
```

---

## Data Model & IDs

### Key Identifiers

#### Run ID
```
Format: run_YYYYMMDD_HHMMSS
Example: run_20260708_123738
Purpose: Unique identifier for each audit run
Used in: URLs, database queries, file paths
```

#### Property ID
```
Type: Integer
Source: Entrata API
Example: 1150907
Purpose: Unique property identifier
```

#### Lease ID
```
Type: Integer
Source: Entrata API
Example: 12345
Purpose: Identifies a lease contract
Note: Different from Lease Interval ID
```

#### Lease Interval ID
```
Type: Integer
Source: Entrata API
Example: 67890
Purpose: Identifies a specific lease period/interval
Key: This is the primary grain for lease-level analysis
```

#### Customer ID
```
Type: Integer
Source: Entrata API
Purpose: Links to Entrata resident profile
Used for: "Open in Entrata" deep link
```

#### AR Code ID
```
Type: Integer (6 digits)
Source: Entrata API
Example: 154771 (Rent), 154777 (Late Charges)
Purpose: Identifies charge/transaction type
Total: 266 codes defined
```

#### Audit Month
```
Format: YYYY-MM-DD (first day of month)
Example: 2026-03-01
Purpose: Monthly grain for reconciliation
```

### Data Hierarchy

```
Portfolio
├── Property (property_id)
│   └── Lease (lease_interval_id)
│       └── AR Code (ar_code_id)
│           └── Month (audit_month)
│               └── Bucket
│                   ├── Expected Detail (scheduled charges)
│                   ├── Actual Detail (AR transactions)
│                   ├── Variance
│                   ├── Status (MATCHED / exception)
│                   └── Findings (rule violations)
```

### Bucket Composite Key
```
Primary grain for reconciliation:
- property_id
- lease_interval_id
- ar_code_id
- audit_month

Unique identifier format:
{run_id}:{property_id}:{lease_interval_id}:{ar_code_id}:{audit_month}
```

### SharePoint List Schemas

#### AuditRuns2 (Primary Results Storage)
```
Fields:
- RunId (Text)
- ResultType (Text): "bucket_result" or "finding"
- PropertyId (Number)
- PropertyName (Text)
- LeaseIntervalId (Number)
- ArCodeId (Number)
- ArCodeName (Text)
- AuditMonth (Date)
- Status (Text): MATCHED, SCHEDULED_NOT_BILLED, etc.
- ExpectedTotal (Currency)
- ActualTotal (Currency)
- Variance (Currency)
- Severity (Text): high, medium, info
- Title (Text): Finding title
- Description (Text): Finding description
- Impact (Text): Financial impact description
```

#### RunDisplaySnapshots (Precomputed Summaries)
```
Fields:
- RunId (Text)
- ScopeType (Text): portfolio, property, lease, month
- PropertyId (Number, optional)
- PropertyName (Text, optional)
- LeaseIntervalId (Number, optional)
- ArCodeId (Number, optional)
- AuditMonth (Date, optional)
- ExceptionCount (Number)
- Undercharge (Currency)
- Overcharge (Currency)
- TotalBuckets (Number)
- MatchedBuckets (Number)
- MatchRate (Number)
- CreatedAt (DateTime)
```

#### ExceptionMonths (Resolution Tracking)
```
Fields:
- run_id (Text)
- property_id (Number)
- lease_interval_id (Number)
- ar_code_id (Number)
- audit_month (Date)
- status (Text): not_started, in_progress, complete, n/a
- fix_label (Text): User note
- resolved_by (Text): User email
- resolved_at (DateTime)
- expected_total (Currency)
- actual_total (Currency)
- variance (Currency)
```

#### Audit Run Metrics (Run-Level KPIs)
```
Fields:
- Title (Text): run_id
- TotalExceptions (Number)
- TotalUndercharge (Currency)
- TotalOvercharge (Currency)
- MatchRate (Number)
- HighSeverityCount (Number)
- MediumSeverityCount (Number)
- InfoSeverityCount (Number)
- TotalBuckets (Number)
- MatchedBuckets (Number)
- CreatedAt (DateTime)
```

---

## Resolution System

### Resolution Status States

**1. Not Started** (Gray badge):
```
Default state
User has not begun resolution
No action taken
```

**2. In Progress** (Yellow badge):
```
User is actively working on resolution
Fix not yet complete
Requires follow-up
```

**3. Complete** (Green badge):
```
Issue fully resolved
Fix implemented and verified
No further action needed
```

**4. N/A - Not Actionable**:
```
Issue cannot be resolved
System limitation or data quality issue
Documented as non-actionable
```

**5. N/A - Out of Scope**:
```
Exception outside audit scope
Not relevant to current analysis
Excluded from metrics
```

### Resolution Workflow

#### Step 1: Identify Discrepancy
```
Location: Lease screen, AR Code row with red exception badge
Action: Click row to open side drawer
Display: Month-by-month breakdown showing variances
```

#### Step 2: Review Month Details
```
For each month with variance:
- View expected vs actual amounts
- Expand detail tables to see individual transactions
- Identify root cause
```

#### Step 3: Record Resolution
```
Fields:
1. Resolution Status (dropdown, required)
   - Select current state
   
2. Fix Action Taken (textarea, optional)
   - Brief note explaining resolution
   - Examples:
     * "Adjusted SC start date to match lease start"
     * "Reversal posted, variance cleared"
     * "Resident moved in late, prorated correctly"

3. Save button
   - Saves to ExceptionMonths SharePoint list
   - Records:
     * resolved_by: current user email
     * resolved_at: current timestamp
     * All bucket context (ids, month, amounts)
```

#### Step 4: Real-Time Updates
```
On successful save:
1. Close drawer (optional, user can continue to next month)
2. Update AR status badge in table
   - Color changes based on new status
   - Label updates
3. Show success toast notification
   - Green background
   - Check icon
   - Message: "Resolution saved."
   - Auto-dismiss after 3 seconds
```

#### Step 5: Status Aggregation
```
AR-level status logic:
- All months complete → AR badge = Complete (green)
- Any month in progress → AR badge = In Progress (yellow)
- No resolutions saved → AR badge = Not Started (gray)
- Mix of complete/N/A → AR badge = Complete (green)
```

### Resolution Data Flow

```
User Input (Lease Screen)
    ↓
JavaScript (AJAX POST /api/exception-months)
    ↓
Flask Backend (web/views.py)
    ↓
StorageService (save_exception_month)
    ↓
SharePoint ExceptionMonths List
    ↓
Response → Frontend
    ↓
Update DOM (badge colors, status text)
    ↓
Show Toast Notification
```

### Batch Status Loading

**Problem**: Individual API calls for each AR code's status = slow

**Solution**: Bulk fetch all exception months for property
```
API: GET /api/exception-months/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>

Returns: All month statuses in single response
Cached: 14400 seconds (4 hours)
Used by: Lease screen on load to populate all AR status badges
```

---

## UI Components Library

### Toast Notifications

**Structure**:
```html
<div class="toast align-items-center text-white bg-success border-0 shadow">
    <div class="d-flex">
        <div class="toast-body">
            <i class="fas fa-check-circle me-1"></i> Message text
        </div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" 
                data-bs-dismiss="toast"></button>
    </div>
</div>
```

**Position**: Fixed bottom-right (bottom: 1.5rem, right: 1.5rem, z-index: 9999)

**Colors**:
- Success: bg-success (green)
- Error: bg-danger (red)
- Warning: bg-warning (yellow)
- Info: bg-info (blue)

**JavaScript**:
```javascript
const toast = new bootstrap.Toast(document.getElementById('toastId'));
toast.show();
```

### Side Drawer

**HTML Structure**:
```html
<!-- Overlay -->
<div id="drawerOverlay" class="drawer-overlay" onclick="closeDrawer()"></div>

<!-- Drawer -->
<div id="detailsDrawer" class="drawer">
    <div class="drawer-content">
        <div class="drawer-header">
            <h5 id="drawerTitle">Title</h5>
            <button type="button" class="btn-close" onclick="closeDrawer()"></button>
        </div>
        <div class="drawer-body" id="drawerBody">
            <!-- Dynamic content -->
        </div>
    </div>
</div>
```

**CSS**:
```css
.drawer {
    position: fixed;
    right: 0;
    top: 0;
    width: 600px;
    height: 100vh;
    background: white;
    box-shadow: -2px 0 10px rgba(0,0,0,0.3);
    transform: translateX(100%);
    transition: transform 0.3s ease;
    z-index: 1050;
    overflow-y: auto;
}

.drawer.open {
    transform: translateX(0);
}

.drawer-overlay {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0,0,0,0.5);
    z-index: 1049;
    display: none;
}

.drawer-overlay.active {
    display: block;
}
```

**JavaScript**:
```javascript
function openDrawer(index) {
    // Populate drawer content
    document.getElementById('drawerBody').innerHTML = content;
    // Show drawer and overlay
    document.getElementById('detailsDrawer').classList.add('open');
    document.getElementById('drawerOverlay').classList.add('active');
}

function closeDrawer() {
    document.getElementById('detailsDrawer').classList.remove('open');
    document.getElementById('drawerOverlay').classList.remove('active');
}
```

### Sortable Tables

**HTML**:
```html
<th class="sortable" data-sort="column_name" style="cursor: pointer;">
    Column Header <span class="sort-icon">↕</span>
</th>
```

**JavaScript**:
```javascript
// Multi-column sort with default sort
let sortState = {
    column: 'default_column',
    direction: 'asc',
    secondary: 'secondary_column',
    secondaryDirection: 'desc'
};

// Sort function
function sortTable(column) {
    if (sortState.column === column) {
        // Toggle direction
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.column = column;
        sortState.direction = 'asc';
    }
    
    // Update icons
    document.querySelectorAll('.sort-icon').forEach(icon => {
        icon.textContent = '↕';
        icon.style.color = '#999';
    });
    
    const activeIcon = document.querySelector(`[data-sort="${column}"] .sort-icon`);
    activeIcon.textContent = sortState.direction === 'asc' ? '↑' : '↓';
    activeIcon.style.color = '#000';
    
    // Perform sort
    renderTable();
}
```

### Search/Filter Pattern

**HTML**:
```html
<input type="text" id="searchInput" class="form-control form-control-sm" 
       placeholder="Search..." style="width: 200px;">
```

**JavaScript**:
```javascript
document.getElementById('searchInput').addEventListener('input', function(e) {
    const query = e.target.value.toLowerCase();
    
    // Filter rows
    const rows = document.querySelectorAll('#tableId tbody tr');
    let visibleCount = 0;
    
    rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        if (text.includes(query)) {
            row.style.display = '';
            visibleCount++;
        } else {
            row.style.display = 'none';
        }
    });
    
    // Update count badge
    document.getElementById('countBadge').textContent = 
        `Showing ${visibleCount} of ${rows.length}`;
});
```

### Loading Spinners

**Inline Spinner**:
```html
<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
Loading text...
```

**Full-Screen Overlay**:
```html
<div class="overlay" id="loadingOverlay">
    <div class="spinner-border text-primary" role="status">
        <span class="visually-hidden">Loading...</span>
    </div>
</div>
```

**CSS**:
```css
.overlay {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(255,255,255,0.9);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 9999;
}
```

### Progress Bar (Bulk Audit)

**HTML**:
```html
<div class="progress" style="height: 30px;">
    <div class="progress-bar progress-bar-striped progress-bar-animated bg-success" 
         role="progressbar" 
         style="width: 0%" 
         id="progressBar">
        0 / 0 completed (0%)
    </div>
</div>
```

**JavaScript Update**:
```javascript
function updateProgress(completed, total) {
    const percentage = Math.round((completed / total) * 100);
    const progressBar = document.getElementById('progressBar');
    
    progressBar.style.width = percentage + '%';
    progressBar.textContent = `${completed} / ${total} completed (${percentage}%)`;
    
    if (completed === total) {
        progressBar.classList.remove('progress-bar-animated', 'progress-bar-striped');
    }
}
```

### Collapsible Details

**HTML**:
```html
<details class="mb-3" open>
    <summary class="fw-bold mb-2">Section Title</summary>
    <div>Content...</div>
</details>
```

**Custom Styling**:
```css
details {
    border: 1px solid #dee2e6;
    border-radius: 8px;
    padding: 15px;
}

summary {
    cursor: pointer;
    user-select: none;
}

summary:hover {
    color: #00a8c8;
}
```

---

## Additional Technical Details

### Caching Strategy

**Flask-Caching** (SimpleCache for single-worker):
```python
@cache.memoize(timeout=14400)  # 4 hours
def cached_load_run(run_id, session_cache_key):
    # Expensive operation
    pass
```

**Cache Keys**:
- Session-scoped: `_session_cache_token()` returns "shared"
- Run data is immutable, survives session restarts
- Clear cache: POST `/api/admin/clear-cache`

**Cached Operations**:
- Load run data (4 hours)
- Load bucket results (4 hours)
- Load findings (4 hours)
- Load property exception months bulk (4 hours)
- Load API property picklist (1 hour)
- Load available runs list (1 hour)

### Date/Time Handling

**Timezone Conversion** (JavaScript):
```javascript
const date = new Date(timestamp);
const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const formatted = date.toLocaleString('en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZoneName: 'short'
});
```

**Format**: `MM/DD/YYYY HH:MM AM/PM TZ`

**Server Timestamps**: UTC ISO format `YYYY-MM-DDTHH:MM:SSZ`

### Academic Year Configuration

**Defined Years**:
```python
academic_years = [
    {
        'label': '2024-2025 Academic Year',
        'value': '2024-2025',
        'start_date': '2024-08-01',
        'end_date': '2025-07-31'
    },
    {
        'label': '2025-2026 Academic Year',
        'value': '2025-2026',
        'start_date': '2025-08-01',
        'end_date': '2026-07-31'
    },
    {
        'label': '2026-2027 Academic Year',
        'value': '2026-2027',
        'start_date': '2026-08-01',
        'end_date': '2027-07-31'
    }
]
```

### Entrata Deep Links

**Format**:
```
https://{org}.entrata.com/residents/view_resident.php?customerid={customer_id}
```

**Org Values**:
- Production: `peakmade`
- Sandbox: `peakmade-test-17291`

**Example**:
```
https://peakmade.entrata.com/residents/view_resident.php?customerid=12345
```

### Error Handling

**User-Facing Errors**:
```python
flash('Error message', 'danger')  # Red alert
flash('Warning message', 'warning')  # Yellow alert
flash('Success message', 'success')  # Green alert
flash('Info message', 'info')  # Blue alert
```

**API Error Responses**:
```json
{
    "error": "Error description",
    "status": "error",
    "details": "Additional context"
}
```

**HTTP Status Codes**:
- 200: Success
- 400: Bad request (validation error)
- 404: Resource not found
- 500: Server error

---

### Entrata API Error Handling

#### 🚀 Quick Implementation Guide

**Problem**: Property 9601 returns `403 Forbidden` with error code 311  
**Solution**: Implement environment toggle to switch between Production and Sandbox Entrata APIs

**3-Step Implementation**:

1. **Add environment configuration** (`.env` file):
   ```env
   # Production Entrata credentials
   ENTRATA_USERNAME_PROD=your_prod_username
   ENTRATA_PASSWORD_PROD=your_prod_password
   ENTRATA_ORG_PROD=peakmade
   
   # Sandbox Entrata credentials
   ENTRATA_USERNAME_SANDBOX=your_sandbox_username
   ENTRATA_PASSWORD_SANDBOX=your_sandbox_password
   ENTRATA_ORG_SANDBOX=peakmade-test-17291
   
   # Default environment
   ENTRATA_DEFAULT_ENVIRONMENT=sandbox
   ```

2. **Store environment in session** (Flask session):
   ```python
   from flask import session
   
   def get_current_entrata_environment():
       return session.get('entrata_environment', 'sandbox')
   
   def set_entrata_environment(env_name):
       session['entrata_environment'] = env_name
   ```

3. **Add toggle button in Settings UI**:
   ```html
   <button type="submit" name="entrata_environment" value="production">
       Switch to Production
   </button>
   ```

**When to use each environment**:
- **Sandbox**: Test properties (e.g., 9601), development, QA testing
- **Production**: Real properties (e.g., 771903), live audits, actual data

**Error detection**: Catch Entrata error code 311 → show user-friendly message suggesting environment switch

---

#### Common Entrata API Errors

**403 Forbidden - Permission Denied**:
```json
{
    "response": {
        "code": 311,
        "result": "error",
        "message": "App doesn't have permission to the property."
    }
}
```

**Cause**: Property ID belongs to different environment (production property with sandbox credentials, or vice versa)

**401 Unauthorized**:
```json
{
    "response": {
        "code": 401,
        "result": "error",
        "message": "Invalid credentials"
    }
}
```

**Cause**: Username/password incorrect or API key expired

**Other Error Codes**:
- **400**: Invalid request format or missing required fields
- **404**: Property/lease not found
- **429**: Rate limit exceeded (too many requests)
- **500**: Entrata internal server error

---

#### Error Handling Flow

```
User submits Property Audit with Property ID 9601
                    ↓
         Call Entrata API
                    ↓
         ┌──────────────────┐
         │ Response Status? │
         └──────────────────┘
                 ↓
         ┌───────┴───────┐
         ↓               ↓
      200 OK          403 Forbidden
         ↓               ↓
   Run audit       Check error code
         ↓               ↓
   Show results    Code 311?
                        ↓
                    ┌───┴───┐
                    ↓       ↓
                  Yes      No
                    ↓       ↓
              Permission  Other
               Denied     Error
                    ↓
         ┌───────────────────────────┐
         │ Show User-Friendly Error: │
         │                           │
         │ Property 9601 not         │
         │ accessible in Production  │
         │                           │
         │ Fix options:              │
         │ 1. Switch to Sandbox      │
         │ 2. Use prod property ID   │
         └───────────────────────────┘
                    ↓
         User clicks "Switch to Sandbox"
                    ↓
         Settings page opens
                    ↓
         User clicks toggle button
                    ↓
         session['entrata_environment'] = 'sandbox'
                    ↓
         Redirect to home page
                    ↓
         User retries same property
                    ↓
         API call succeeds with sandbox credentials
                    ↓
         ✅ Audit runs successfully
```

---

#### Implementing Environment Toggle (Production ↔ Sandbox)

**Backend Configuration**:
```python
# config.py
ENTRATA_ENVIRONMENTS = {
    'production': {
        'base_url': 'https://peakmade.entrata.com/api/v1',
        'org_name': 'peakmade',
        'username': os.getenv('ENTRATA_USERNAME_PROD'),
        'password': os.getenv('ENTRATA_PASSWORD_PROD'),
        'display_name': 'Production'
    },
    'sandbox': {
        'base_url': 'https://peakmade-test-17291.entrata.com/api/v1',
        'org_name': 'peakmade-test-17291',
        'username': os.getenv('ENTRATA_USERNAME_SANDBOX'),
        'password': os.getenv('ENTRATA_PASSWORD_SANDBOX'),
        'display_name': 'Sandbox (Test)'
    }
}

# Default environment (stored in session or database)
ENTRATA_DEFAULT_ENVIRONMENT = 'sandbox'
```

**Session Storage**:
```python
# Store user's environment preference in Flask session
from flask import session

def get_current_entrata_environment():
    """Get current Entrata environment from session."""
    return session.get('entrata_environment', ENTRATA_DEFAULT_ENVIRONMENT)

def set_entrata_environment(env_name):
    """Switch Entrata environment."""
    if env_name not in ENTRATA_ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {env_name}")
    session['entrata_environment'] = env_name
    return ENTRATA_ENVIRONMENTS[env_name]
```

**Settings Route (Toggle Environment)**:
```python
# web/views.py
@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        new_env = request.form.get('entrata_environment')
        
        if new_env in ['production', 'sandbox']:
            set_entrata_environment(new_env)
            flash(f'Switched to {new_env.title()} environment', 'success')
            
            # Clear cached property picklist (different properties per environment)
            cache.delete_memoized(load_entrata_property_picklist)
        else:
            flash('Invalid environment selection', 'danger')
        
        return redirect(url_for('settings'))
    
    current_env = get_current_entrata_environment()
    env_config = ENTRATA_ENVIRONMENTS[current_env]
    
    return render_template('settings.html',
                           current_env=current_env,
                           env_config=env_config,
                           available_environments=ENTRATA_ENVIRONMENTS)
```

**Settings Template (HTML)**:
```html
<!-- templates/settings.html -->
<div class="card mb-4">
    <div class="card-header d-flex justify-content-between align-items-center">
        <div>
            <i class="fas fa-exchange-alt me-2"></i>Entrata API Environment
        </div>
        <span class="badge {{ 'bg-success' if current_env == 'production' else 'bg-warning text-dark' }}">
            {{ env_config.display_name }}
        </span>
    </div>
    <div class="card-body">
        <p class="text-muted">
            Switch between Production and Sandbox Entrata environments. 
            All API calls will use the selected environment's credentials and property access.
        </p>
        
        <form method="POST" class="d-inline-flex align-items-center gap-3">
            {% if current_env == 'sandbox' %}
                <button type="submit" name="entrata_environment" value="production" 
                        class="btn btn-success">
                    <i class="fas fa-rocket me-2"></i>Switch to Production
                </button>
                <span class="text-muted">Currently using: {{ env_config.org_name }}</span>
            {% else %}
                <button type="submit" name="entrata_environment" value="sandbox" 
                        class="btn btn-warning">
                    <i class="fas fa-flask me-2"></i>Switch to Sandbox
                </button>
                <span class="text-muted">Currently using: {{ env_config.org_name }}</span>
            {% endif %}
        </form>
    </div>
</div>
```

---

#### Property Access Validation

**Pre-Flight Property Check** (Optional):
```python
def validate_property_access(property_id):
    """
    Verify app has permission to access property before running audit.
    Returns (is_accessible, error_message).
    """
    current_env = get_current_entrata_environment()
    env_config = ENTRATA_ENVIRONMENTS[current_env]
    
    try:
        # Call Entrata API to fetch property details
        response = call_entrata_api(
            endpoint='/properties',
            method='getProperty',
            params={'PropertyID': property_id}
        )
        
        if response.get('response', {}).get('code') == 311:
            return False, (
                f"Property {property_id} is not accessible in {env_config['display_name']} environment. "
                f"Try switching to {'Sandbox' if current_env == 'production' else 'Production'} in Settings."
            )
        
        return True, None
    
    except Exception as e:
        return False, f"Unable to validate property access: {str(e)}"
```

**Property Audit Route with Validation**:
```python
@app.route('/upload-api-property', methods=['POST'])
def upload_api_property():
    property_id = request.form.get('api_property_id')
    
    # Validate property access before starting audit
    is_accessible, error_msg = validate_property_access(property_id)
    
    if not is_accessible:
        flash(error_msg, 'danger')
        return redirect(url_for('index'))
    
    # Proceed with audit
    run_id = run_property_audit(property_id, from_date, to_date)
    return redirect(url_for('property_view', property_id=property_id, run_id=run_id))
```

---

#### User-Friendly Error Messages

**403 Permission Error**:
```python
if response.get('response', {}).get('code') == 311:
    current_env = get_current_entrata_environment()
    env_display = ENTRATA_ENVIRONMENTS[current_env]['display_name']
    other_env = 'Sandbox' if current_env == 'production' else 'Production'
    
    error_html = f"""
    <div class="alert alert-danger">
        <h5><i class="fas fa-lock me-2"></i>Property Access Denied</h5>
        <p>
            Property <strong>{property_id}</strong> is not accessible in 
            <strong>{env_display}</strong> environment.
        </p>
        <p class="mb-0">
            <strong>To fix this:</strong>
        </p>
        <ol>
            <li>
                <strong>Use a property ID that exists in {env_display}</strong>, OR
            </li>
            <li>
                <strong><a href="{{ url_for('settings') }}">Switch to {other_env} environment</a></strong> 
                if this property belongs to {other_env}.
            </li>
        </ol>
    </div>
    """
    flash(Markup(error_html), 'danger')
```

**Property Picklist Filtering** (Only show accessible properties):
```python
def load_entrata_property_picklist():
    """Load property list from Entrata API."""
    current_env = get_current_entrata_environment()
    env_config = ENTRATA_ENVIRONMENTS[current_env]
    
    response = call_entrata_api(endpoint='/properties', method='getProperties')
    
    properties = response.get('response', {}).get('result', {}).get('PhysicalProperty', {}).get('Property', [])
    
    # Filter out properties without permission (optional)
    accessible_properties = []
    for prop in properties:
        property_id = prop.get('PropertyID')
        # Optionally validate each property (can be slow)
        # is_accessible, _ = validate_property_access(property_id)
        # if is_accessible:
        accessible_properties.append({
            'id': property_id,
            'name': prop.get('Name'),
            'code': prop.get('Code')
        })
    
    return accessible_properties
```

---

#### Environment Indicator (Visible on All Pages)

**Add to Base Template Header**:
```html
<!-- templates/base.html -->
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center py-2">
        <div>
            <!-- App title/logo -->
        </div>
        <div class="d-flex align-items-center gap-3">
            <!-- Entrata Environment Badge -->
            <div class="badge {{ 'bg-success' if current_env == 'production' else 'bg-warning text-dark' }} fs-6">
                <i class="fas fa-server me-1"></i>
                Entrata: {{ env_config.display_name }}
            </div>
            
            <!-- User info -->
            <div class="text-end small">
                <strong>{{ user.name }}</strong><br>
                <span class="text-muted">{{ user.email }}</span>
            </div>
        </div>
    </div>
</div>
```

**Pass Environment to All Templates**:
```python
# app.py or extensions.py
@app.context_processor
def inject_entrata_environment():
    """Make Entrata environment available to all templates."""
    current_env = get_current_entrata_environment()
    return {
        'current_env': current_env,
        'env_config': ENTRATA_ENVIRONMENTS[current_env]
    }
```

---

#### Testing Environment Switching

**Test Cases**:
1. **Production Property in Production Environment**: ✅ Works
2. **Production Property in Sandbox Environment**: ❌ 403 Error (expected)
3. **Sandbox Property in Sandbox Environment**: ✅ Works
4. **Sandbox Property in Production Environment**: ❌ 403 Error (expected)

**Example Property IDs**:
```python
# Document which properties belong to which environment
KNOWN_PROPERTY_IDS = {
    'production': [771903, 1150907, 1150908],  # Real production properties
    'sandbox': [9601, 9602, 12345]             # Sandbox test properties
}
```

**Environment Auto-Detection** (Advanced):
```python
def detect_property_environment(property_id):
    """
    Attempt to determine which environment a property belongs to.
    Returns 'production', 'sandbox', or None if unknown.
    """
    # Try production first
    session['entrata_environment'] = 'production'
    is_prod_accessible, _ = validate_property_access(property_id)
    
    if is_prod_accessible:
        return 'production'
    
    # Try sandbox
    session['entrata_environment'] = 'sandbox'
    is_sandbox_accessible, _ = validate_property_access(property_id)
    
    if is_sandbox_accessible:
        return 'sandbox'
    
    # Property not accessible in either environment
    return None
```

---

### Performance Optimizations

1. **Lazy Loading**: Run selector loads on first interaction
2. **Batch Queries**: Load all exception months in one call
3. **Caching**: Aggressive caching with 4-hour TTL
4. **Pagination**: Not implemented (relies on filtering)
5. **Async Writes**: Background threads for SharePoint writes
6. **In-Memory Cache**: Recent run data for instant access
7. **Parquet Storage**: Compressed format for detail data

---

## Implementation Checklist

### For Replicating This Design:

**✅ Brand & Styling**:
- [ ] Import Montserrat font from Google Fonts
- [ ] Define CSS custom properties for brand colors
- [ ] Implement gradient button styles
- [ ] Apply card shadow and border-radius
- [ ] Set up badge color scheme

**✅ Layout & Navigation**:
- [ ] Create base template with header/footer
- [ ] Implement breadcrumb pattern
- [ ] Add run selector dropdown (lazy-loaded)
- [ ] Build responsive grid layouts
- [ ] Add user info display

**✅ Core Screens**:
- [ ] Home/Upload with property & lease audit forms
- [ ] Portfolio with KPI dashboard and property table
- [ ] Property with lease table and filters
- [ ] Lease with AR code table and side drawer
- [ ] Bucket drilldown with expected/actual tables
- [ ] Bulk audit with progress tracking
- [ ] Settings with environment toggle

**✅ Interactive Components**:
- [ ] Side drawer with overlay
- [ ] Sortable tables (multi-column)
- [ ] Search/filter inputs (real-time)
- [ ] Toast notifications
- [ ] Progress bars
- [ ] Collapsible sections
- [ ] Loading spinners

**✅ Data Integration**:
- [ ] Connect to SharePoint Lists (AuditRuns2, etc.)
- [ ] Implement CSV fallback reads
- [ ] Set up caching layer
- [ ] Configure resolution tracking (ExceptionMonths)
- [ ] Build API endpoints for CRUD operations

**✅ Workflow**:
- [ ] Audit execution pipeline
- [ ] Resolution recording system
- [ ] Status badge updates (real-time)
- [ ] Cross-run navigation
- [ ] Bulk audit orchestration

**✅ Polish**:
- [ ] Timezone conversion for timestamps
- [ ] Currency formatting ($X,XXX.XX)
- [ ] Date formatting (MM/DD/YYYY)
- [ ] Empty state messages
- [ ] Error handling and user feedback
- [ ] Responsive design testing

---

---

## Data Storage Architecture

### Overview

The LeaseFileAudit application uses a **multi-tier storage strategy** with in-memory caching, SharePoint Lists (primary), Parquet files (detail data), and CSV files (fallback). This ensures data persistence, fast access, and resilience.

---

### Storage Tiers

#### Tier 1: In-Memory Cache (`_IN_MEMORY_RESULTS_CACHE`)
**Purpose**: Ultra-fast access for recently-run audits  
**Lifecycle**: Survives until application restart  
**Thread-Safety**: Protected by `_IN_MEMORY_CACHE_LOCK`

**Cached Data** (keyed by `run_id`):
```python
{
    'run_id_123': {
        'bucket_results': pd.DataFrame,      # Core reconciliation results
        'findings': pd.DataFrame,            # Rule violations
        'variance_detail': pd.DataFrame,     # Month-by-month expected/actual breakdown
        'expected_detail': pd.DataFrame,     # Scheduled charges (full detail)
        'actual_detail': pd.DataFrame        # AR transactions (full detail)
    }
}
```

**Load Priority**:
1. Check in-memory cache first
2. Fall back to SharePoint/CSV if not in cache

---

#### Tier 2: SharePoint Lists (Primary Persistence)

**Configuration**:
- Enabled via: `USE_SHAREPOINT_STORAGE=true` (default)
- Access: Azure AD app-only token (auto-refreshed)
- Batch size: 20 items per Graph API batch (Microsoft limit)
- Concurrency: 1-4 parallel batches (configurable)

##### List 1: **AuditRuns2** (Detailed Results)
**Purpose**: Stores individual bucket reconciliation results and findings  
**Row Types**: `bucket_result`, `finding`

**Bucket Result Row Schema**:
```
- RunId (Text)
- ResultType (Text): "bucket_result"
- PropertyId (Number)
- LeaseIntervalId (Number)
- ArCodeId (Text)
- AuditMonth (Date)
- Status (Text): MATCHED, SCHEDULED_NOT_BILLED, BILLED_NOT_SCHEDULED, AMOUNT_MISMATCH
- Severity (Text): high, medium, info
- Variance (Currency)
- ExpectedTotal (Currency)
- ActualTotal (Currency)
- PropertyName (Text, optional)
- ResidentName (Text, optional)
- MatchRule (Text, optional)
- CreatedAt (DateTime)
```

**Finding Row Schema**:
```
- RunId (Text)
- ResultType (Text): "finding"
- PropertyId (Number)
- LeaseIntervalId (Number)
- ArCodeId (Text)
- AuditMonth (Date)
- Category (Text)
- Severity (Text): high, medium, info
- FindingTitle (Text)
- Description (Text)
- ExpectedValue (Text)
- ActualValue (Text)
- Variance (Currency)
- ImpactAmount (Currency)
- FindingId (Text, UUID)
- PropertyName (Text, optional)
- ResidentName (Text, optional)
- CreatedAt (DateTime)
```

**Write Strategy**:
- **Exceptions-Only Mode** (optional): `SHAREPOINT_WRITE_EXCEPTIONS_ONLY=true` skips MATCHED rows (reduces list size by ~60-80%)
- **Async Write** (default): Background thread persists data after page load
- **Fallback**: Single-row POST if batch fails

**Query Strategy**:
```python
# Load all buckets for a run
filter = "fields/RunId eq 'run_20260708_123738' and fields/ResultType eq 'bucket_result'"

# Load property-specific buckets
filter = "fields/RunId eq 'run_20260708_123738' and fields/ResultType eq 'bucket_result' and fields/PropertyId eq 1150907"
```

**Limitations**:
- AuditRuns2 is **not indexed** by PropertyId/LeaseIntervalId (SharePoint limitation)
- Large property queries may be slow → RunDisplaySnapshots is the preferred fast path

---

##### List 2: **RunDisplaySnapshots** (Precomputed Metrics)
**Purpose**: Instant portfolio/property/lease rendering without reading AuditRuns2  
**Row Types**: `portfolio`, `property`, `lease`, `ar_code`, `month`

**Snapshot Row Schema** (all scopes):
```
- Title (Text): Composite key (run:scope:property:lease:ar_code:month)
- SnapshotKey (Text): Same as Title
- RunId (Text)
- ScopeType (Text): portfolio, property, lease, ar_code, month
- PropertyId (Number, optional)
- LeaseIntervalId (Number, optional)
- ArCodeId (Number, optional)
- AuditMonth (Date, optional)
- ExceptionCountStatic (Number): Unresolved exception count
- UnderchargeStatic (Currency)
- OverchargeStatic (Currency)
- TotalVarianceStatic (Currency)
- MatchRateStatic (Number)
- TotalBucketsStatic (Number)
- MatchedBucketsStatic (Number)
- TotalLeaseIntervalStatic (Number): Lease count (property scope only)
- PropertyNameStatic (Text, optional)
- RunScopeType (Text, optional): "property_audit", "lease_audit", "manual", "bulk"
- AuditedThrough (Date, optional): Max AUDIT_MONTH for property
- CreatedAt (DateTime)
```

**Additional Fields (ar_code/month scopes only)**:
```
- Status (Text): MATCHED, SCHEDULED_NOT_BILLED, etc.
- ArCodeName (Text)
- ExpectedTotal (Currency)
- ActualTotal (Currency)
- Variance (Currency)
```

**Snapshot Generation**:
1. **Filter resolved months**: Queries ExceptionMonths list to exclude resolved exceptions from metrics
2. **Apply AR code whitelist**: Only includes allowed AR codes (e.g., 154771 Rent, 154777 Late Charges)
3. **Build hierarchy**: Portfolio → Property → Lease → AR Code → Month
4. **Write batches**: 20 rows per batch, 1-2 batches in parallel

**Query Strategy**:
```python
# Load portfolio snapshot (1 row)
filter = "fields/RunId eq 'run_20260708_123738' and fields/ScopeType eq 'portfolio'"

# Load all property snapshots for a run (~20-100 rows)
filter = "fields/RunId eq 'run_20260708_123738' and fields/ScopeType eq 'property'"

# Load all lease snapshots for a property (~10-200 rows)
filter = "fields/RunId eq 'run_20260708_123738' and fields/ScopeType eq 'lease' and fields/PropertyId eq 1150907"

# Load month-level bucket details (reconstruct bucket_results from snapshots)
filter = "fields/RunId eq 'run_20260708_123738' and fields/ScopeType eq 'month' and fields/PropertyId eq 1150907"
```

**Advantages**:
- **Instant portfolio load**: No need to scan AuditRuns2
- **Indexed queries**: ScopeType is efficient (though not officially indexed)
- **Cross-run aggregation**: Latest snapshot per property across all runs

**Fallback Chain**:
1. Try RunDisplaySnapshots (preferred)
2. Try AuditRuns2 (if snapshots unavailable)
3. Fall back to CSV

---

##### List 3: **ExceptionMonths** (Resolution Tracking)
**Purpose**: Track user resolutions for individual month exceptions  
**Cross-Run Matching**: Preserves historical resolutions across audit runs

**Schema**:
```
- CompositeKey (Text): "{run_id}:{property_id}:{lease_id}:{ar_code_id}:{audit_month}"
- RunId (Text)
- PropertyId (Number)
- LeaseIntervalId (Number)
- ArCodeId (Text)
- AuditMonth (Date): YYYY-MM-DD (first of month)
- ExceptionType (Text): SCHEDULED_NOT_BILLED, AMOUNT_MISMATCH, etc.
- Status (Text): "Open", "Resolved"
- FixLabel (Text): User note describing resolution
- ActionType (Text): "adjusted_schedule", "reversal_posted", etc. (optional)
- Variance (Currency)
- ExpectedTotal (Currency)
- ActualTotal (Currency)
- ResolvedAt (DateTime)
- ResolvedBy (Text): User email
- ResolvedByName (Text): User display name
- Notes (Text, optional)
- UpdatedAt (DateTime)
- UpdatedBy (Text)
```

**Resolution Persistence Logic**:
```python
# Query WITHOUT run_id to find historical resolutions
filter = (
    f"fields/PropertyId eq {property_id} and "
    f"fields/LeaseIntervalId eq {lease_id} and "
    f"fields/ArCodeId eq '{ar_code_id}'"
)

# Deduplication priority:
# 1. RESOLVED records from ANY run (auto-apply historical resolutions)
# 2. CURRENT run records (new/unresolved exceptions)
# 3. HISTORICAL run records (reference only)
```

**Why Cross-Run Matching?**  
If an exception was resolved in Run A, but the same exception appears in Run B (new audit of same property/lease), the resolution from Run A is automatically applied to Run B's metrics. This prevents double-counting resolved exceptions in undercharge/overcharge totals.

**Bulk Fetch Optimization**:
```python
# Instead of N queries (one per AR code), fetch entire property at once
bulk_data = storage.load_property_exception_months_bulk(run_id, property_id)
# Returns: {(lease_id, ar_code_id): [month1, month2, ...], ...}
```

**Write Strategy**:
- **Upsert by CompositeKey**: If record exists, update; otherwise, create
- **Single API call per month**: No batching (low volume)

---

##### List 4: **Audit Run Metrics** (Run-Level KPIs)
**Purpose**: Store run-level summary metrics for quick portfolio dashboard rendering

**Schema**:
```
- Title (Text): run_id
- TotalExceptions (Number)
- TotalUndercharge (Currency)
- TotalOvercharge (Currency)
- MatchRate (Number)
- HighSeverityCount (Number)
- MediumSeverityCount (Number)
- InfoSeverityCount (Number)
- TotalBuckets (Number)
- MatchedBuckets (Number)
- CreatedAt (DateTime)
```

**Write Strategy**: Async background thread

---

#### Tier 3: Parquet Files (Detail Data)
**Purpose**: Persist large detail DataFrames (expected/actual/variance) for later access  
**Location**: SharePoint Document Library or local filesystem  
**Enabled**: `PERSIST_DETAIL_DATAFRAMES=true` (default)

**Stored Files**:
```
{run_id}/inputs_normalized/expected_detail.parquet
{run_id}/inputs_normalized/actual_detail.parquet
{run_id}/inputs_normalized/variance_detail.parquet
```

**Format**: Apache Parquet with Snappy compression (PyArrow engine)

**Why Parquet?**
- **10x smaller** than CSV (compressed column format)
- **Fast reads**: Only reads needed columns
- **Type-safe**: Preserves date/numeric types

**Load Priority**:
1. Check in-memory cache
2. Try Parquet file (if PERSIST_DETAIL_DATAFRAMES=true)
3. Fall back to CSV (legacy)

**Expected Detail Columns**:
```
PROPERTY_ID, PROPERTY_NAME, LEASE_INTERVAL_ID, LEASE_ID, CUSTOMER_NAME,
AR_CODE_ID, AR_CODE_NAME, AUDIT_MONTH, SCHEDULED_CHARGES_ID,
DATE_CHARGE_START, DATE_CHARGE_END, CHARGE_AMOUNT, FREQUENCY
```

**Actual Detail Columns**:
```
PROPERTY_ID, PROPERTY_NAME, LEASE_INTERVAL_ID, LEASE_ID, CUSTOMER_NAME,
AR_CODE_ID, AR_CODE_NAME, POST_MONTH_DATE, TRANSACTION_ID,
TRANSACTION_AMOUNT, IS_REVERSAL
```

**Variance Detail Columns**:
```
PROPERTY_ID, LEASE_INTERVAL_ID, AR_CODE_ID, AUDIT_MONTH, STATUS,
EXPECTED_TOTAL, ACTUAL_TOTAL, VARIANCE, EXPECTED_ROWS, ACTUAL_ROWS,
EXPECTED_TRANSACTIONS (JSON), ACTUAL_TRANSACTIONS (JSON)
```

---

#### Tier 4: CSV Files (Legacy Fallback)
**Purpose**: Local filesystem fallback when SharePoint unavailable  
**Location**: `instance/runs/{run_id}/outputs/`

**Files**:
```
bucket_results.csv  - Core reconciliation results
findings.csv        - Rule violations
variance_detail.csv - Optional, if generated
```

**Write Strategy**:
- Always written locally (even with SharePoint enabled)
- Acts as backup if SharePoint Lists write fails

**Read Strategy**:
- Only used if SharePoint Lists and Parquet both unavailable
- Loaded via `pd.read_csv()` with date parsing

---

### Data Flow: Save Run

**Input**: Audit engine outputs (DataFrames)
```python
- bucket_results (pd.DataFrame)
- findings (pd.DataFrame)
- expected_detail (pd.DataFrame)
- actual_detail (pd.DataFrame)
- metadata (dict)
```

**Save Sequence**:

1. **Store in Memory Cache** (immediate):
   ```python
   _IN_MEMORY_RESULTS_CACHE[run_id] = {
       'bucket_results': bucket_results.copy(),
       'findings': findings.copy(),
       'expected_detail': expected_detail.copy(),
       'actual_detail': actual_detail.copy(),
       'variance_detail': variance_detail  # If generated
   }
   ```

2. **Write to CSV** (immediate, synchronous):
   ```python
   instance/runs/{run_id}/outputs/bucket_results.csv
   instance/runs/{run_id}/outputs/findings.csv
   ```

3. **Write to Parquet** (immediate, if enabled):
   ```python
   {run_id}/inputs_normalized/expected_detail.parquet
   {run_id}/inputs_normalized/actual_detail.parquet
   {run_id}/inputs_normalized/variance_detail.parquet
   ```

4. **Write RunDisplaySnapshots** (async background thread):
   - Filter resolved months from ExceptionMonths
   - Apply AR code whitelist
   - Build portfolio/property/lease/ar_code/month snapshots
   - Batch write to SharePoint List (20 rows per batch)

5. **Write AuditRuns2** (async background thread, optional):
   - Transform bucket_results → bucket_result rows
   - Transform findings → finding rows
   - Batch write to SharePoint List (20 rows per batch)
   - **Exceptions-only mode**: Skip MATCHED rows to reduce size

6. **Write Audit Run Metrics** (async background thread):
   - Calculate run-level KPIs
   - Single row insert to SharePoint List

**Timeline**:
- **T+0s**: User sees "Audit complete, redirecting..." (memory cache populated)
- **T+1s**: Page loads showing data from memory cache
- **T+2-5s**: Background threads write to SharePoint Lists
- **T+5-10s**: All writes complete (user doesn't wait)

**Error Handling**:
- If SharePoint writes fail → CSV files remain as fallback
- If async threads crash → logged but doesn't affect user experience
- Token refresh: Background threads acquire fresh app-only tokens

---

### Data Flow: Load Run

**Request**: User navigates to `/portfolio/{run_id}` or `/property/{property_id}/{run_id}`

**Load Sequence**:

1. **Check In-Memory Cache** (fastest):
   ```python
   if run_id in _IN_MEMORY_RESULTS_CACHE:
       return cached_data
   ```

2. **Load Snapshots from SharePoint** (fast, preferred):
   ```python
   # Portfolio view
   portfolio_snapshot = load_run_display_snapshot_from_sharepoint_list(
       run_id, 'portfolio'
   )
   property_snapshots = load_run_display_snapshots_for_run(
       run_id, 'property'
   )
   
   # Property view
   lease_snapshots = load_run_display_snapshots_for_property(
       run_id, property_id, 'lease'
   )
   ```

3. **Fallback: Load from AuditRuns2** (slower):
   ```python
   bucket_results = load_results_from_sharepoint_list(
       run_id, 'bucket_result', property_id=property_id
   )
   ```

4. **Fallback: Load from CSV** (slowest):
   ```python
   bucket_results = pd.read_csv(
       f'instance/runs/{run_id}/outputs/bucket_results.csv'
   )
   ```

**Attributes Tracking**:
```python
df.attrs['read_source'] = 'sharepoint_list'  # or 'snapshots', 'csv', 'memory'
df.attrs['read_reason'] = 'preferred'        # or 'auditruns2_unavailable', etc.
df.attrs['read_scope'] = f"run={run_id}, property_id={property_id}"
```

---

### Storage Configuration

**Environment Variables**:
```bash
# SharePoint Storage
USE_SHAREPOINT_STORAGE=true                    # Enable SharePoint Document Library
SHAREPOINT_LIBRARY_NAME="LeaseFileAudit Runs"  # Document library name
SHAREPOINT_SITE_URL=https://...                # SharePoint site URL

# SharePoint Lists
SHAREPOINT_AUDIT_RESULTS_LIST_NAME=AuditRuns2  # Always AuditRuns2 (locked)

# Performance Tuning
SHAREPOINT_BATCH_SIZE_AUDITRUNS=20             # Batch size for AuditRuns2 writes
SHAREPOINT_BATCH_SIZE_SNAPSHOTS=20             # Batch size for RunDisplaySnapshots writes
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=2       # Parallel batches for AuditRuns2
SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=2       # Parallel batches for RunDisplaySnapshots

# Async Write Toggles
ASYNC_AUDIT_RESULTS_WRITE=true                 # Write AuditRuns2 in background
ASYNC_METRICS_WRITE=true                       # Write metrics in background
ASYNC_SNAPSHOTS_WRITE=false                    # Snapshots must complete before redirect

# Optimization
SHAREPOINT_WRITE_EXCEPTIONS_ONLY=false         # If true, skip MATCHED rows in AuditRuns2
PERSIST_DETAIL_DATAFRAMES=true                 # Save expected/actual/variance as Parquet

# Caching
SNAPSHOT_COLUMNS_CACHE_TTL_SECONDS=600         # Cache SharePoint column names (10 min)
```

**Recommended Settings**:
- **Small Audits** (<5 properties): All defaults work fine
- **Large Audits** (>20 properties): Set `SHAREPOINT_WRITE_EXCEPTIONS_ONLY=true` to reduce list size
- **High Volume** (bulk audits): Increase `SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=4`

---

### Performance Characteristics

**Write Times** (typical):
- In-memory cache: <1ms
- CSV files: 50-200ms (depends on row count)
- Parquet files: 100-500ms (compression overhead)
- RunDisplaySnapshots: 2-5s (20 properties, 200 snapshots)
- AuditRuns2: 5-15s (1000 buckets + findings)

**Read Times** (typical):
- In-memory cache: <1ms
- RunDisplaySnapshots: 200-800ms (portfolio with 20 properties)
- AuditRuns2: 1-3s (property with 100 leases)
- CSV files: 500ms-2s (depends on size)

**Bottlenecks**:
1. **SharePoint throttling**: 429 responses under heavy load (retries with exponential backoff)
2. **Large property queries**: AuditRuns2 lacks indexes (use RunDisplaySnapshots instead)
3. **Parquet compression**: CPU-intensive (worth it for 10x space savings)

---

### Data Retention

**In-Memory Cache**:
- Cleared on app restart
- No expiration (persists forever if app stays running)
- LRU eviction not implemented (assumes low memory usage)

**SharePoint Lists**:
- **Perpetual**: No automatic deletion
- **Manual cleanup**: Admin can delete old runs from lists
- **Snapshots**: Automatically filtered by RunId in queries

**CSV/Parquet Files**:
- **Perpetual**: No automatic deletion
- **Manual cleanup**: Admin can delete old run folders from `instance/runs/`

**Recommendation**: Archive runs older than 1 year to cold storage (not implemented)

---

### Disaster Recovery

**Scenario 1: SharePoint Lists Unavailable**
- ✅ App continues working (CSV fallback)
- ✅ Portfolio/property views load from CSV
- ❌ Cross-run resolution matching unavailable (ExceptionMonths list required)
- ❌ Portfolio aggregation slow (no snapshots)

**Scenario 2: App Restart (Memory Cache Lost)**
- ✅ All data reloads from SharePoint Lists or CSV
- ✅ No data loss
- ⚠️ First page load after restart is slower (cache cold)

**Scenario 3: SharePoint Credentials Expired**
- ✅ App auto-refreshes token (app-only authentication)
- ❌ If refresh fails → read-only mode (CSV fallback)

**Scenario 4: Batch Write Failure**
- ✅ App retries with exponential backoff (3 attempts)
- ✅ Falls back to single-row POST if batch fails
- ❌ If all retries fail → data persists in CSV only

---

### Migration Notes

**From Legacy Storage (pre-SharePoint)**:
1. Old runs stored only in CSV → still accessible
2. No snapshots → portfolio rendering slow
3. No resolution tracking → no ExceptionMonths data

**To New App**:
1. Export SharePoint Lists to CSV/JSON (PowerShell or Graph API)
2. Transform schemas to match new app (column name changes)
3. Bulk import into new SharePoint Lists (batch API)
4. Rebuild RunDisplaySnapshots from AuditRuns2 data

---

## Conclusion

This document provides a complete reference for replicating the LeaseFileAudit application's design, functionality, and workflow. All screen layouts, color schemes, component structures, data models, and user interactions are documented in detail to enable creation of an application with identical appearance and behavior.

**Key Design Principles**:
1. **Consistent Brand Colors**: Teal, magenta, orange across all elements
2. **Clear Information Hierarchy**: KPIs → Tables → Detail views
3. **Drill-Down Navigation**: Portfolio → Property → Lease → AR Code → Month
4. **Real-Time Feedback**: Toast notifications, badge updates, progress tracking
5. **Responsive Design**: Works on desktop and tablet sizes
6. **Performance First**: Caching, lazy loading, batch queries

**Key Storage Principles**:
1. **Multi-Tier Architecture**: Memory → SharePoint Lists → Parquet → CSV
2. **Fast Reads**: In-memory cache + precomputed snapshots
3. **Async Writes**: Background threads don't block user
4. **Cross-Run Persistence**: Historical resolutions preserved
5. **Resilient Fallbacks**: CSV files if SharePoint unavailable

**For Questions or Clarifications**:
Refer to specific sections above or examine the source code in:
- `templates/` - All HTML templates
- `static/` - CSS and JavaScript
- `web/views.py` - Route handlers and logic
- `storage/service.py` - Data access layer (storage implementation)
