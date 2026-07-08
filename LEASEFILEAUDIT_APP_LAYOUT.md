# LeaseFileAudit App Layout & Structure

This document describes the complete layout, page structure, and routing architecture of the LeaseFileAudit application for replication in new projects.

---

## Application Overview

**LeaseFileAudit** is a Flask-based web application for auditing property lease data against scheduled charges and AR transactions. The app uses a hierarchical drill-down navigation pattern (Portfolio → Property → Lease) with real-time data fetching from Entrata APIs.

---

## Technology Stack

- **Backend:** Python 3.10+, Flask
- **Frontend:** Bootstrap 5.3, Font Awesome 6.4, jQuery
- **Styling:** Custom CSS with Montserrat font family
- **Data:** Pandas, Excel/CSV upload, Entrata REST APIs
- **Storage:** Local filesystem + SharePoint Document Library + SharePoint Lists
- **Cache:** Flask-Caching (SimpleCache)
- **Authentication:** Azure AD Easy Auth (optional, configurable)
- **Server:** Waitress WSGI server

---

## Color Palette & Branding

```css
:root {
    --color-primary: #00a8c8;      /* Teal/Cyan - Primary brand color */
    --color-secondary: #231f20;    /* Dark gray/black - Text */
    --color-accent: #ff6600;       /* Orange - Accent highlights */
    --color-danger: #c20068;       /* Magenta - Errors/warnings */
    --color-light: #f4f4f4;        /* Light gray - Backgrounds */
    --font-headline: 'Montserrat', sans-serif;
    --font-body: 'Montserrat', sans-serif;
}
```

**Typography:**
- All text uses **Montserrat** (Google Fonts)
- Headings: `font-weight: 700` (Bold)
- Body text: `font-weight: 400` (Regular)

---

## Application Routes & Pages

### 1. **Home / Upload Page** (`/`)
- **Route:** `GET /`
- **Template:** `upload.html`
- **Authentication:** Required
- **Purpose:** Main landing page with file upload and recent audit runs

**Features:**
- Excel file upload form (scheduled charges + AR transactions)
- API-based property selector (fetch directly from Entrata)
- API-based single lease selector
- Recent audit runs list with status badges
- Academic year date selector (for audit windows)
- Two upload modes:
  - Manual Excel upload
  - API property fetch (pulls data directly from Entrata)

**Key UI Elements:**
- Property picklist dropdown (loads from Entrata API)
- Date range selector for audit window
- Drag-and-drop file upload zone
- Recent runs table with clickable links to results

---

### 2. **Settings Page** (`/settings`)
- **Route:** `GET/POST /settings`
- **Template:** `settings.html`
- **Authentication:** Required
- **Purpose:** Configure exclusions and environment settings

**Features:**
- **Resident Profile Exclusions:** Append-only text area to exclude specific resident names
- **Lease ID Exclusions:** Append-only text area to exclude specific lease IDs
- **AR Code Exclusions:** Append-only text area to exclude specific AR codes
- **Environment Switcher:** Toggle between Production and Sandbox Entrata environments
- **Configuration Persistence:** Saves exclusions to JSON files

**Configuration Files:**
- `resident_profile_exclusions.json`
- `excluded_ar_codes.json`

---

### 3. **Portfolio View** (`/portfolio` or `/portfolio/<run_id>`)
- **Route:** `GET /portfolio` or `GET /portfolio/<run_id>`
- **Template:** `portfolio.html`
- **Authentication:** Required
- **Purpose:** Portfolio-level aggregated view of all properties

**Features:**
- **Run Selector:** Dropdown to switch between audit runs
- **Property Summary Cards:** Grid of property cards showing:
  - Property name
  - Total exceptions count
  - Exception breakdown by severity (High, Medium, Low)
  - Total AR transaction amount
  - KPI metrics (match rate, etc.)
- **Drill-Down:** Click any property card to view property details
- **Filtering:** Filter by property status, severity, etc.
- **Export:** Download portfolio summary as CSV/Excel

**Data Display:**
- Properties sorted by exception count (descending)
- Color-coded severity badges
- Inline KPI charts

---

### 4. **Property View** (`/property/<property_id>` or `/property/<property_id>/<run_id>`)
- **Route:** `GET /property/<property_id>` or `GET /property/<property_id>/<run_id>`
- **Template:** `property.html`
- **Authentication:** Required
- **Purpose:** Property-level view showing all leases with exceptions

**Features:**
- **Run Selector:** Dropdown to switch between audit runs for this property
- **Property Header:** Property name, ID, total exceptions
- **Lease Exception Table:** List of all leases with exceptions:
  - Lease ID
  - Resident name
  - Exception count by type
  - Total amount variance
  - Severity badge
- **Drill-Down:** Click any lease row to view lease details
- **Filtering:** Filter by exception type, severity, resident name
- **Breadcrumb Navigation:** Portfolio → Property

**Data Display:**
- Leases sorted by exception count (descending)
- Expandable/collapsible exception details
- Month-by-month exception timeline

---

### 5. **Lease View** (`/lease/<property_id>/<lease_interval_id>` or `/lease/<run_id>/<property_id>/<lease_interval_id>`)
- **Route:** `GET /lease/<property_id>/<lease_interval_id>` or `GET /lease/<run_id>/<property_id>/<lease_interval_id>`
- **Template:** `lease.html`
- **Authentication:** Required
- **Purpose:** Lease-level detailed exception view with line-by-line charge analysis

**Features:**
- **Lease Header:** Resident name, lease ID, lease term, move-in/move-out dates
- **Exception Summary:** Total exceptions, total variance amount
- **Line-Item Exception Table:** Detailed charge comparison:
  - AR Code ID and Name
  - Expected amount (scheduled)
  - Actual amount (billed)
  - Variance amount
  - Status (Matched, Missing Billing, Amount Mismatch)
  - Month/date breakdown
- **AR Transaction Details:** Expandable raw AR transaction data
- **Flags/Notes:** User annotations (future enhancement)
- **Breadcrumb Navigation:** Portfolio → Property → Lease

**Data Display:**
- Month-by-month charge reconciliation
- Color-coded status badges (Matched=green, Missing=red, Mismatch=yellow)
- Inline amount formatting ($)
- Drill-down to individual AR transaction details

---

### 6. **Bulk Audit Page** (`/bulk-audit`)
- **Route:** `GET /bulk-audit`
- **Template:** `bulk_audit.html`
- **Authentication:** Required
- **Purpose:** Run audits for multiple properties in parallel

**Features:**
- **Property Multi-Select:** Choose multiple properties from Entrata API picklist
- **Batch Job Execution:** Submit bulk audit job
- **Job Progress Tracker:** Real-time progress bar and status updates
- **Job History:** List of recent bulk audit jobs with status
- **Job Cancellation:** Cancel running bulk audit jobs
- **Result Navigation:** Click any completed property to view results

**Key UI Elements:**
- Property checklist with select-all option
- Live progress indicator (polling)
- Job status badges (Running, Complete, Cancelled, Failed)
- Per-property result links

---

### 7. **Session Ended Page** (`/end-session`)
- **Route:** `GET /end-session`
- **Template:** `session_ended.html`
- **Authentication:** Required
- **Purpose:** Session timeout or logout confirmation page

**Features:**
- User-friendly session expiration message
- Link to return to home page

---

## API Endpoints

### Data APIs

#### `/api/runs` (GET)
- **Purpose:** List all recent audit runs
- **Returns:** JSON array of run metadata

#### `/api/property-picklist` (GET)
- **Purpose:** Fetch property list from Entrata API
- **Returns:** JSON array of properties with ID, name, address

#### `/api/exception-months/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>` (GET)
- **Purpose:** Get month-by-month exception details for a charge
- **Returns:** JSON object with expected/actual amounts per month

#### `/api/exception-months/ar-status/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>` (GET)
- **Purpose:** Get AR transaction status details
- **Returns:** JSON object with transaction history

### Upload APIs

#### `/upload` (POST)
- **Purpose:** Upload Excel file and trigger audit
- **Accepts:** `multipart/form-data` with Excel file
- **Returns:** Redirect to results page

#### `/upload-api-property` (POST)
- **Purpose:** Fetch property data from Entrata API and trigger audit
- **Accepts:** JSON with property_id, date range
- **Returns:** Redirect to results page

#### `/upload-api-lease` (POST)
- **Purpose:** Fetch single lease data from Entrata API and trigger audit
- **Accepts:** JSON with property_id, lease_interval_id, date range
- **Returns:** Redirect to results page

### Bulk Audit APIs

#### `/api/bulk-audit` (POST)
- **Purpose:** Start bulk audit job for multiple properties
- **Accepts:** JSON with array of property_ids
- **Returns:** JSON with job_id

#### `/api/bulk-audit/<job_id>` (GET)
- **Purpose:** Get bulk audit job status
- **Returns:** JSON with job status, progress, completed property count

#### `/api/bulk-audit/<job_id>/cancel` (POST)
- **Purpose:** Cancel running bulk audit job
- **Returns:** JSON with success status

### Admin APIs

#### `/api/admin/clear-cache` (POST)
- **Purpose:** Clear application cache
- **Returns:** JSON with success status

---

## Template Hierarchy

```
templates/
├── base.html              # Base template with navbar, styles, scripts
├── upload.html            # Home page (extends base.html)
├── portfolio.html         # Portfolio view (extends base.html)
├── property.html          # Property view (extends base.html)
├── lease.html             # Lease view (extends base.html)
├── bulk_audit.html        # Bulk audit page (extends base.html)
├── settings.html          # Settings page (extends base.html)
├── session_ended.html     # Session timeout page (extends base.html)
├── bucket.html            # (Legacy/unused)
└── flags.html             # (Future enhancement)
```

---

## Base Template Structure (`base.html`)

All pages extend `base.html`, which provides:

### Header
- **Brand Logo:** Peak Campus logo (inline SVG or image)
- **App Title:** "Lease File Audit"
- **User Info:** Displays logged-in user name and email (top-right)

### Navigation
- **Home:** Link to `/` (upload page)
- **Portfolio:** Link to `/portfolio` (portfolio view)
- **Bulk Audit:** Link to `/bulk-audit` (bulk audit page)
- **Settings:** Link to `/settings` (settings page)
- **Logout:** Link to `/end-session` (session end)

### Footer
- Copyright notice
- Links to documentation (optional)

### Styles
- Bootstrap 5.3 CSS
- Font Awesome 6.4 icons
- Google Fonts (Montserrat)
- Custom CSS variables for brand colors

### Scripts
- Bootstrap 5.3 JS bundle
- jQuery 3.7+
- Custom JavaScript for:
  - Form validation
  - Dynamic dropdowns
  - Progress polling
  - AJAX requests

---

## Page Layout Patterns

### Common UI Components

#### 1. **Run Selector Dropdown**
```html
<select id="runSelector" class="form-select">
    <option value="12345">Run 12345 - 2026-07-08 10:30 AM</option>
    <option value="12346">Run 12346 - 2026-07-07 03:15 PM</option>
</select>
```
- Appears on Portfolio, Property, and Lease pages
- Dynamically loads on page load
- Triggers page reload on change with new run_id

#### 2. **Exception Summary Cards**
```html
<div class="card border-danger">
    <div class="card-header bg-danger text-white">
        <h5>High Severity</h5>
    </div>
    <div class="card-body">
        <p class="display-4">23</p>
        <p class="text-muted">Exceptions</p>
    </div>
</div>
```
- Color-coded by severity (High=red, Medium=yellow, Low=blue, Info=green)
- Shows count and total amount

#### 3. **Breadcrumb Navigation**
```html
<nav aria-label="breadcrumb">
    <ol class="breadcrumb">
        <li class="breadcrumb-item"><a href="/portfolio">Portfolio</a></li>
        <li class="breadcrumb-item"><a href="/property/123">Property Name</a></li>
        <li class="breadcrumb-item active">Lease 456</li>
    </ol>
</nav>
```
- Shows hierarchy: Portfolio → Property → Lease
- Clickable links to navigate back up

#### 4. **Data Tables**
- Responsive Bootstrap tables with fixed headers
- Sortable columns (click header to sort)
- Filterable rows (search box above table)
- Expandable rows for details
- Pagination for large datasets

#### 5. **Status Badges**
```html
<span class="badge bg-success">Matched</span>
<span class="badge bg-danger">Missing Billing</span>
<span class="badge bg-warning">Amount Mismatch</span>
```
- Color-coded status indicators
- Used throughout for exception types, job status, etc.

---

## Data Flow

### 1. **Excel Upload Flow**
```
User uploads Excel → 
/upload POST → 
Parse Excel (scheduled + AR sheets) → 
Run audit engine → 
Save results to storage → 
Redirect to /portfolio/<run_id>
```

### 2. **API Property Fetch Flow**
```
User selects property from picklist → 
/upload-api-property POST → 
Fetch scheduled charges from Entrata API → 
Fetch AR transactions from Entrata API → 
Run audit engine → 
Save results to storage → 
Redirect to /property/<property_id>/<run_id>
```

### 3. **Bulk Audit Flow**
```
User selects multiple properties → 
/api/bulk-audit POST → 
Create background job → 
For each property:
    Fetch data from Entrata API → 
    Run audit engine → 
    Save results → 
Poll /api/bulk-audit/<job_id> for status → 
Display results when complete
```

---

## Storage Architecture

### Local Filesystem
```
instance/
└── runs/
    └── <run_id>/
        ├── inputs_normalized/
        │   ├── expected_detail.csv
        │   └── actual_detail.csv
        ├── outputs/
        │   ├── bucket_results.csv
        │   ├── findings.csv
        │   └── variance_detail.csv
        └── run_meta.json
```

### SharePoint
- **Document Library:** `LeaseFileAudit Runs` (stores uploaded files and CSVs)
- **SharePoint List:** `AuditRuns2` (stores result rows for querying)
- **SharePoint List:** `RunDisplaySnapshots` (stores aggregated property/lease summaries)
- **SharePoint List:** `AuditRunMetrics` (stores KPI metrics per run)

---

## Key Features to Replicate

1. **Hierarchical Drill-Down Navigation:** Portfolio → Property → Lease
2. **Run Selector:** Switch between audit runs on any page
3. **Real-Time Progress:** Polling for bulk audit job status
4. **Color-Coded Severity:** Visual hierarchy for exceptions
5. **Responsive Design:** Mobile-friendly Bootstrap layout
6. **Caching:** Flask-Caching for expensive queries
7. **API Integration:** Fetch data directly from Entrata REST APIs
8. **Excel Upload:** Parse multi-sheet Excel files with pandas
9. **Export Functionality:** Download results as CSV/Excel
10. **Session Management:** User authentication with Azure AD Easy Auth

---

## Responsive Breakpoints

- **Desktop:** 1200px+ (full multi-column layout)
- **Tablet:** 768px - 1199px (2-column cards)
- **Mobile:** <768px (single-column stacked layout)

All tables scroll horizontally on small screens.

---

## JavaScript Libraries & Plugins

- **Bootstrap 5.3:** Core layout and components
- **jQuery 3.7+:** AJAX requests and DOM manipulation
- **Font Awesome 6.4:** Icons
- **Chart.js** (optional): For future KPI charts

---

## Authentication Flow

1. User navigates to app
2. Azure AD Easy Auth intercepts if `REQUIRE_AUTH=true`
3. User logs in with Microsoft account
4. App reads user info from `X-MS-CLIENT-PRINCIPAL` header
5. User info displayed in header
6. SharePoint logging enabled (tracks user activity)

For local dev: Set `REQUIRE_AUTH=false` and use mock user.

---

## Future Enhancements (Not Yet Implemented)

- **Flags Page:** Manual exception flagging/notes
- **KPI Dashboard:** Visual charts for portfolio KPIs
- **Lease Term Editor:** Edit lease term dates inline
- **Batch Exception Resolution:** Mark multiple exceptions as resolved
- **Historical Trending:** Compare audit runs over time

---

## Quick Start for New Apps

### Minimal Pages to Implement
1. **Home/Upload Page** (`/`) - File upload + recent runs
2. **Results List Page** (`/portfolio`) - Summary of audit results
3. **Detail Page** (`/lease`) - Line-item exception details

### Reusable Components
- Copy `base.html` for layout
- Copy color palette CSS
- Copy run selector JavaScript
- Copy exception summary card HTML
- Copy breadcrumb navigation HTML

---

*Last Updated: 2026-07-08*
*App Version: LeaseFileAudit v2.0*
