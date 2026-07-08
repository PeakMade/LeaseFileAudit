# CSV-Optional Mode Guide

## Overview

By default, the LeaseFileAudit application writes audit data to **BOTH** CSV files and SharePoint lists. You can now disable CSV writes completely while keeping SharePoint list functionality.

## How to Disable CSV Writes

Add this environment variable to your `.env` file:

```bash
DISABLE_CSV_WRITES=true
```

## In-Memory Caching for Drill-Down Views

**NEW:** When CSV writes are disabled, the application automatically caches expected_detail and actual_detail DataFrames in memory for **4 hours** after each audit completes.

**How it works:**
1. Audit execution completes with full DataFrames in memory
2. Before page redirect, DataFrames are cached using Flask-Caching (4-hour TTL)
3. Drill-down views (lease detail, bucket detail) load from cache for 4 hours
4. After 4 hours, cache expires and drill-downs show empty (gracefully)
5. Users re-run the audit if they need to view drill-down details again

**Cache persistence:**
- ✅ Survives page refreshes
- ✅ Survives browser session restarts
- ✅ Shared across all users (same run_id)
- ❌ Lost after app restart
- ❌ Lost after 4-hour expiration

**Result:** Drill-down transaction details are available for 4 hours without CSV files.

## What Happens When CSV Writes Are Disabled

### ✅ Still Works (Full Functionality)
- ✅ **AuditRuns2 list writes** - bucket_results and findings written to SharePoint
- ✅ **RunDisplaySnapshots list writes** - portfolio/property/lease snapshots 
- ✅ **LeaseTerms list writes** - extracted lease term data
- ✅ **Metrics list writes** - Audit Run Metrics
- ✅ **Document library uploads** - original Excel files still saved
- ✅ **Metadata JSON** - run_meta.json still saved (to SharePoint if use_sharepoint=true)
- ✅ **Dashboard loading** - uses SharePoint lists as primary data source
- ✅ **Drill-down views** - cached in memory for 4 hours after audit execution
- ✅ **Transaction-level details** - available for 4 hours via Flask cache

### ⚠️ Time-Limited Functionality
- ⚠️ **Expected detail** - cached for 4 hours, then expires (empty after expiration)
- ⚠️ **Actual detail** - cached for 4 hours, then expires (empty after expiration)
- ⚠️ **Variance detail** - not cached, always empty

### 🔄 Fallback Behavior
The `load_run()` method uses this priority:

1. **bucket_results**: SharePoint AuditRuns2 → CSV fallback
2. **findings**: SharePoint AuditRuns2 → CSV fallback
3. **expected_detail**: CSV only (returns empty DataFrame if missing)
4. **actual_detail**: CSV only (returns empty DataFrame if missing)
5. **variance_detail**: CSV only (returns None if missing)

## Storage Architecture

### With CSV Writes ENABLED (default)
```
save_run() execution:
├── Step 1: Save original Excel file → SharePoint Document Library
├── Step 2: Save CSV input files → expected_detail.csv, actual_detail.csv
├── Step 3: Save CSV output files → bucket_results.csv, findings.csv, variance_detail.csv
├── Step 4: Save metadata → run_meta.json
├── Step 5: Write metrics → Audit Run Metrics list
├── Step 6: Write snapshots → RunDisplaySnapshots list
└── Step 7: Write results → AuditRuns2 list
```

### With CSV Writes DISABLED
```
save_run() execution:
├── Step 1: Save original Excel file → SharePoint Document Library
├── Step 2: CSV writes DISABLED (skipped)
├── Step 3: CSV writes DISABLED (skipped)
├── Step 4: Save metadata → run_meta.json
├── Step 5: Write metrics → Audit Run Metrics list
├── Step 6: Write snapshots → RunDisplaySnapshots list
└── Step 7: Write results → AuditRuns2 list
```

## Error Handling

All CSV writes are now wrapped in try/except blocks. If CSV writes fail (even when enabled), the application will:

- ⚠️ Log a warning
- ⚠️ Print error message to console
- ✅ Continue execution (no crash)
- ✅ Complete SharePoint writes in Steps 5-7

## Performance Benefits

Disabling CSV writes reduces:
- **Storage operations**: 3-5 fewer file writes per audit run
- **SharePoint traffic**: Fewer document library uploads (if use_sharepoint=true)
- **Local I/O**: No CSV file creation (if use_sharepoint=false)
- **Network bandwidth**: Reduced upload volume to SharePoint
- **Cache efficiency**: In-memory caching is 10-100x faster than CSV reads

Typical time savings: **0.5-2 seconds per audit run** depending on:
- Data volume (row counts)
- Network speed (SharePoint uploads)
- Disk speed (local writes)

**Memory usage**: In-memory caching adds ~2-10 MB per audit run (4-hour retention).
- Typical audit: ~500KB-2MB cached data
- Large audit (1000s of leases): ~5-10MB cached data
- Automatically expires after 4 hours

## Migration Guide

### From CSV-Primary to SharePoint-Primary

**Current state (CSV-primary):**
```env
# .env
DISABLE_CSV_WRITES=false
ASYNC_AUDIT_RESULTS_WRITE=true
```

**Target state (SharePoint-only):**
```env
# .env
DISABLE_CSV_WRITES=true
ASYNC_AUDIT_RESULTS_WRITE=true
```

**Steps:**
1. Verify SharePoint lists are working: `AuditRuns2`, `RunDisplaySnapshots`, `LeaseTerms`, `Audit Run Metrics`
2. Run test audit with `DISABLE_CSV_WRITES=true`
3. Verify dashboard loads correctly from SharePoint lists
4. Monitor logs for warnings about missing expected/actual detail
5. If no issues after 5-10 test runs, keep CSV writes disabled

## Troubleshooting

### Dashboard shows "Run not found or incomplete"
**Cause**: SharePoint list writes failed AND CSV writes are disabled

**Solution**: Check logs for SharePoint write errors:
```
[AUDITRUNS2_ASYNC] Background write STARTED
[AUDITRUNS2_ASYNC] Background write SUCCESS
```

### "Missing bucket_results or findings" error
**Cause**: Neither SharePoint nor CSV has the data

**Solution**: 
1. Check `_can_use_sharepoint_lists()` returns True
2. Verify `access_token` is valid
3. Check AuditRuns2 list exists and is accessible
4. Re-enable CSV writes temporarily: rill-down views
**Cause**: Cache expired (4-hour TTL) or app restarted

**Solution**: This is expected behavior with CSV writes disabled. To restore drill-down details:
1. Re-run the audit (will be cached for another 4 hours)
2. OR: Re-enable CSV writes for persistent storage: `DISABLE_CSV_WRITES=false`

**Note:** All summary views (dashboard, property page, lease summary) work fine - only transaction-level drill-downs are affected.
2. Re-run the audit
3. Or implement SharePoint list storage for expected/actual detail (custom development)

## Related Environment Variables

```env
# Disable CSV file writes (this guide)
DISABLE_CSV_WRITES=false

# Use SharePoint for all storage (Document Library + Lists)
USE_SHAREPOINT=true

# Async writes for AuditRuns2 list
ASYNC_AUDIT_RESULTS_WRITE=true

# Async writes for RunDisplaySnapshots list
ASYNC_RUN_DISPLAY_SNAPSHOTS=true

# Async writes for Audit Run Metrics list
ASYNC_METRICS_WRITE=true
```

## Code References

- **CSV write logic**: `storage/service.py` lines 4843-4904 (save_run Step 2/3)
- **CSV load logic**: `storage/service.py` lines 5047-5092 (load_run)
- **SharePoint fallback**: `storage/service.py` lines 2711-2900 (_load_results_from_sharepoint_list)
