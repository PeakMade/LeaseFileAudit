# Azure Environment Variables - Complete List

## Required Environment Variables (Must Set)

### Azure Authentication
```bash
SHAREPOINT_CLIENT_ID=your-app-registration-client-id
SHAREPOINT_TENANT_ID=your-azure-tenant-id
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=your-client-secret
```

### SharePoint Storage
```bash
SHAREPOINT_SITE_URL=https://yourcompany.sharepoint.com/sites/YourSite
SHAREPOINT_LIBRARY_NAME=LeaseFileAudit Runs
```

### Entrata API (Production)
```bash
LEASE_API_KEY=your-entrata-production-api-key
LEASE_API_BASE_URL=https://yourcompany.entrata.com/api/v1
LEASE_API_DETAILS_URL=https://yourcompany.entrata.com/api/v1/leases
LEASE_API_AR_URL=https://yourcompany.entrata.com/api/v1/accounting
```

---

## Performance Optimization Variables (NEW - Recommended)

### 🚀 Speed Optimizations (Add These for 2-3x Faster Audits)
```bash
# SharePoint Batch Concurrency - 4x faster writes
SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=4
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=4

# Already enabled by default (no action needed):
# EARLY_AUDIT_WINDOW_PREFILTER=true
# ASYNC_AUDIT_RESULTS_WRITE=true
```

**Impact**: Reduces audit time from ~230s to ~110-130s

---

## Optional Environment Variables

### App Configuration
```bash
PORT=8000                              # Default: 8000
FLASK_DEBUG=false                      # Default: true (set false in production)
APP_ENVIRONMENT=Production             # Default: Local
SECRET_KEY=your-random-secret-key      # Generate a random string for production
```

### Authentication & Access
```bash
REQUIRE_AUTH=true                      # Default: true
SESSION_IDLE_TIMEOUT_MINUTES=30        # Default: 30
OPEN_BROWSER=false                     # Default: true (set false in Azure)
```

### SharePoint Advanced
```bash
# SharePoint Logging
ENABLE_SHAREPOINT_LOGGING=true         # Default: true
SHAREPOINT_LOG_SITE_URL=https://yourcompany.sharepoint.com/sites/BaseCampApps
SHAREPOINT_LOG_LIST_NAME=Innovation Use Log

# Batch Sizes (default 20, max 20)
SHAREPOINT_BATCH_SIZE_AUDITRUNS=20     # Default: 20
SHAREPOINT_BATCH_SIZE_SNAPSHOTS=20     # Default: 20
SHAREPOINT_BATCH_SIZE=20               # Default: 20 (global fallback)

# Write Strategies
ASYNC_METRICS_WRITE=true               # Default: true
ASYNC_SNAPSHOTS_WRITE=false            # Default: false (must complete before redirect)
SHAREPOINT_WRITE_EXCEPTIONS_ONLY=false # Default: false (write all rows)

# SharePoint Storage
USE_SHAREPOINT_STORAGE=true            # Default: true
```

### Entrata API Advanced
```bash
# API Headers & Authentication
LEASE_API_KEY_HEADER=X-Api-Key         # Default: X-Api-Key

# API Methods & Versions
LEASE_API_DETAILS_METHOD=getLeaseDetails    # Default: getLeaseDetails
LEASE_API_DETAILS_VERSION=r2                # Default: r2
LEASE_API_AR_METHOD=getAccountReceivables   # Default: getAccountReceivables
LEASE_API_AR_VERSION=r3                     # Default: r3

# API Parameters
LEASE_API_INCLUDE_ADDONS=0             # Default: 0
LEASE_API_INCLUDE_CHARGE=1             # Default: 1
LEASE_API_INCLUDE_BALANCE=1            # Default: 1
LEASE_API_INCLUDE_CONCESSION=1         # Default: 1
LEASE_API_INCLUDE_SCHEDULED=1          # Default: 1
LEASE_API_PARKING_INCLUDE_SCHEDULED=1  # Default: 1

# API Timeouts
LEASE_API_TIMEOUT_SECONDS=300          # Default: 300 (5 minutes)
LEASE_API_MAX_RETRIES=3                # Default: 3
LEASE_API_RETRY_DELAY_SECONDS=2        # Default: 2

# Properties Filter
LEASE_API_PROPERTIES_SHAREPOINT_LIST=Properties_0        # Default: Properties_0
LEASE_API_PROPERTIES_REQUIRE_REPORTABLE=1                # Default: 1

# Debug Flags
LEASE_API_DEBUG_PARKING=false          # Default: false (enables verbose parking logs)
```

### Sandbox/Testing (Optional - for non-prod environments)
```bash
# Sandbox Entrata API
LEASE_API_SANDBOX_KEY=your-sandbox-api-key
LEASE_API_SANDBOX_BASE_URL=https://sandbox.entrata.com/api/v1
LEASE_API_SANDBOX_DETAILS_URL=https://sandbox.entrata.com/api/v1/leases
LEASE_API_SANDBOX_AR_URL=https://sandbox.entrata.com/api/v1/accounting
```

### Local Development Only
```bash
# Local dev user (when REQUIRE_AUTH=false)
LOCAL_DEV_USER_NAME=Local Developer    # Default: Local Developer
LOCAL_DEV_USER_EMAIL=dev@localhost     # Default: dev@localhost
```

### Data Persistence
```bash
# Save expanded dataframes to files for debugging
PERSIST_DETAIL_DATAFRAMES=true         # Default: true
```

---

## Azure App Service Configuration

### How to Set in Azure Portal:
1. Go to Azure Portal → Your App Service
2. Navigate to **Configuration** → **Application settings**
3. Click **+ New application setting**
4. Add each variable name and value
5. Click **Save** at the top

### How to Set via Azure CLI:
```bash
az webapp config appsettings set \
  --resource-group YourResourceGroup \
  --name YourAppServiceName \
  --settings \
    SHAREPOINT_CLIENT_ID="your-value" \
    SHAREPOINT_TENANT_ID="your-value" \
    MICROSOFT_PROVIDER_AUTHENTICATION_SECRET="your-value" \
    SHAREPOINT_SITE_URL="your-value" \
    LEASE_API_KEY="your-value" \
    SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=4 \
    SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=4
```

---

## Priority List for Azure Deployment

### ✅ Tier 1: Must Have (App Won't Work Without These)
```
SHAREPOINT_CLIENT_ID
SHAREPOINT_TENANT_ID
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET
SHAREPOINT_SITE_URL
LEASE_API_KEY
LEASE_API_BASE_URL
```

### 🚀 Tier 2: Performance Optimizations (Highly Recommended)
```
SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=4
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=4
```

### ⚙️ Tier 3: Production Best Practices
```
FLASK_DEBUG=false
SECRET_KEY=<generate-random-string>
APP_ENVIRONMENT=Production
REQUIRE_AUTH=true
OPEN_BROWSER=false
```

### 🔧 Tier 4: Optional Tuning (Use Defaults)
All other variables listed above - only set if you need to override defaults.

---

## Quick Copy-Paste for Azure (Replace Values)

```bash
# Tier 1: Required
SHAREPOINT_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
SHAREPOINT_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=your-secret-value
SHAREPOINT_SITE_URL=https://peakcampus.sharepoint.com/sites/BaseCampApps
LEASE_API_KEY=your-entrata-api-key
LEASE_API_BASE_URL=https://yourcompany.entrata.com/api/v1

# Tier 2: Performance (NEW!)
SHAREPOINT_BATCH_CONCURRENCY_SNAPSHOTS=4
SHAREPOINT_BATCH_CONCURRENCY_AUDITRUNS=4

# Tier 3: Production Settings
FLASK_DEBUG=false
SECRET_KEY=generate-a-random-secret-key-here
APP_ENVIRONMENT=Production
REQUIRE_AUTH=true
OPEN_BROWSER=false
PORT=8000
```

---

## Testing Your Configuration

After setting environment variables in Azure, test with:
```bash
# Check if variables are loaded
curl https://your-app.azurewebsites.net/health

# Monitor application logs
az webapp log tail --name YourAppServiceName --resource-group YourResourceGroup
```

---

## Notes

1. **Secrets**: Never commit secrets to git. Always use Azure environment variables.
2. **Restart Required**: After changing environment variables, restart your Azure App Service.
3. **Validation**: The app logs will show if any required variables are missing on startup.
4. **Performance**: Adding the Tier 2 variables will reduce audit time by 50-60%.

---

## Support

If you see errors related to missing environment variables, check the application logs:
```bash
az webapp log tail --name YourAppServiceName --resource-group YourResourceGroup
```

Look for messages like:
- `[ERROR] Missing required environment variable: SHAREPOINT_CLIENT_ID`
- `[WARNING] Using default value for: LEASE_API_TIMEOUT_SECONDS`
