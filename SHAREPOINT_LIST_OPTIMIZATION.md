# SharePoint List Optimization for Dashboard Performance

## Overview

This optimization reduces Graph API calls for the portfolio dashboard from **50+ calls to 2 calls** by pre-calculating and storing summary metrics in a SharePoint List.

## Problem

Previously, `calculate_cumulative_metrics()` loaded ALL audit runs and their CSVs (bucket_results, findings) from SharePoint to calculate dashboard metrics. With 50+ runs, this resulted in:
- 50+ Graph API calls to download CSVs
- Slow dashboard loading (5-10 seconds)
- Excessive bandwidth usage
- API throttling risk

## Solution

Store pre-calculated summary metrics in SharePoint List **"Audit Run Metrics"** at save time, then query the list (1-2 API calls) for dashboard display.

## SharePoint List Structure

**List Name:** `Audit Run Metrics`  
**Location:** https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/Audit Run Metrics/AllItems.aspx

### Columns

| Column Name | Type | Description |
|------------|------|-------------|
| `Title` | Single line text | Run ID (e.g., run_20260127_135019) |
| `RunDateTime` | Single line text | ISO timestamp of run |
| `UploadedBy` | Single line text | User email who uploaded |
| `FileName` | Single line text | Original Excel filename |
| `TotalScheduled` | Number | Sum of expected_total from all buckets |
| `TotalActual` | Number | Sum of actual_total from all buckets |
| `Matched` | Number | Count of matched buckets |
| `ScheduledNotBilled` | Number | Count of "Scheduled Not Billed" status |
| `BilledNotScheduled` | Number | Count of "Billed Not Scheduled" status |
| `AmountMismatch` | Number | Count of "Amount Mismatch" status |
| `TotalVariances` | Number | Total count of exceptions (non-matched) |
| `HighSeverity` | Number | Count of high severity findings |
| `MediumSeverity` | Number | Count of medium severity findings |
| `Properties` | Multiple lines text | JSON with property-level breakdown |

## Implementation Details

### 1. Writing Metrics to List

**File:** [storage/service.py](storage/service.py)  
**Method:** `_write_metrics_to_sharepoint_list()`

Called automatically at the end of `save_run()` after all CSVs are saved:

```python
def _write_metrics_to_sharepoint_list(self, run_id: str, bucket_results: pd.DataFrame, 
                                      findings: pd.DataFrame, metadata: dict) -> bool:
    # Calculate metrics from bucket_results DataFrame
    # - Count by status (Matched, Scheduled Not Billed, etc.)
    # - Sum totals (expected, actual)
    # - Count findings by severity
    # - Calculate property-level breakdown
    
    # Write to SharePoint List via Graph API
    # POST https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items
```

**Key Features:**
- Calculates 14 summary metrics from bucket_results and findings
- Creates property-level breakdown as JSON
- Uses Graph API to create list item
- Does NOT fail save_run() if list write fails (graceful degradation)
- Logs success/failure

### 2. Reading Metrics from List

**File:** [storage/service.py](storage/service.py)  
**Method:** `load_all_metrics_from_sharepoint_list()`

Retrieves all metrics items from the list:

```python
def load_all_metrics_from_sharepoint_list(self) -> List[Dict[str, Any]]:
    # Query SharePoint List via Graph API
    # GET https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items
    # $expand=fields&$top=1000&$orderby=RunDateTime desc
    
    # Returns list of dictionaries with all metrics
```

**Key Features:**
- Fetches up to 1000 items (configurable)
- Orders by RunDateTime descending (most recent first)
- Parses Properties JSON field
- Returns empty list if list not configured (graceful degradation)

### 3. Updated Dashboard Logic

**File:** [web/views.py](web/views.py)  
**Function:** `calculate_cumulative_metrics()`

Now uses a **fast path** when SharePoint list is available:

```python
def calculate_cumulative_metrics() -> dict:
    storage = get_storage_service()
    
    # Try SharePoint list first (2 API calls)
    all_metrics = storage.load_all_metrics_from_sharepoint_list()
    
    if all_metrics:
        # Use pre-calculated metrics from list (FAST!)
        # Only loads most recent run's bucket_results for detailed undercharge/overcharge
        # Returns metrics in ~1 second
    else:
        # Fallback to old method (50+ API calls)
        # Load all runs and CSVs for full calculation
        # Used in local mode or if list is empty
```

**Performance:**
- **Before:** 50+ Graph API calls, 5-10 seconds load time
- **After:** 2 Graph API calls, <1 second load time

## Fallback Behavior

The implementation includes graceful degradation:

1. **SharePoint list not configured** (local development):
   - `load_all_metrics_from_sharepoint_list()` returns empty list
   - `calculate_cumulative_metrics()` falls back to CSV loading
   
2. **SharePoint list empty** (first run):
   - Returns empty metrics
   - Next run will populate the list
   
3. **List write fails** during `save_run()`:
   - Logs warning but continues
   - CSV files still saved successfully
   - Dashboard can still use old CSV method

## Usage

### For Users
No changes required! The optimization is transparent:
- Upload files as normal
- Dashboard loads faster
- All functionality works the same

### For Developers

**To populate historical runs:**
Run this script to backfill metrics for existing runs:

```python
from storage.service import StorageService
from web.auth import get_access_token

storage = StorageService(
    base_dir='instance/runs',
    use_sharepoint=True,
    sharepoint_site_url='https://peakcampus.sharepoint.com/sites/BaseCampApps',
    library_name='LeaseFileAudit Runs',
    access_token=get_access_token()
)

# Load each run and write metrics
for run in storage.list_runs(limit=1000):
    run_data = storage.load_run(run['run_id'])
    storage._write_metrics_to_sharepoint_list(
        run['run_id'],
        run_data['bucket_results'],
        run_data['findings'],
        run_data['metadata']
    )
```

## Testing

1. **Upload new audit file**: Verify metrics are written to SharePoint list
2. **View dashboard**: Should load in <1 second
3. **Check logs**: Should see "Using SharePoint list data (X runs)"
4. **Verify list**: Open SharePoint list and confirm new item created

## Future Enhancements

1. **Deduplication**: Prevent duplicate metrics if run is re-saved
2. **Update vs. Create**: Update existing list item instead of creating new one
3. **Historical undercharge/overcharge**: Add to list for accurate historical tracking
4. **Pagination**: Handle >1000 runs with paging
5. **Batch operations**: Batch create multiple list items at once

## Related Files

- [storage/service.py](storage/service.py) - Storage service with list methods
- [web/views.py](web/views.py) - Dashboard view using list data
- [AUTHENTICATION_GUIDE.md](AUTHENTICATION_GUIDE.md) - Easy Auth setup (required for Graph API)
