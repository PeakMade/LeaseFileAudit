# SharePoint Storage Implementation Summary

## ✅ Implementation Complete

Your application now supports persistent storage using SharePoint Document Library!

### Changes Made

1. **config.py**
   - Added `use_sharepoint_storage` setting (from environment variable)
   - Added `sharepoint_library_name` setting
   - Added `is_sharepoint_configured()` method

2. **storage/service.py**
   - Updated constructor to accept SharePoint parameters
   - Added `_get_site_and_drive_id()` - Gets SharePoint site/drive IDs via Graph API
   - Added `_upload_file_to_sharepoint()` - Uploads CSV/JSON files
   - Added `_download_file_from_sharepoint()` - Downloads CSV/JSON files
   - Updated `_save_dataframe()` - Works with both SharePoint and local
   - Updated `_load_dataframe()` - Works with both SharePoint and local
   - Updated `_save_json()` - Works with both SharePoint and local
   - Updated `_load_json()` - Works with both SharePoint and local
   - Updated `list_runs()` - Lists from SharePoint or local
   - Updated `get_run_exists()` - Checks existence in SharePoint or local

3. **web/views.py**
   - Updated `get_storage_service()` to pass SharePoint configuration
   - Imports `get_access_token` from auth module

4. **Documentation**
   - Created `SHAREPOINT_STORAGE_DEPLOYMENT.md` with deployment instructions

### How It Works

**Local Development (Default)**
```python
USE_SHAREPOINT_STORAGE = not set or "false"
→ Uses instance/runs/ folder
→ No SharePoint configuration needed
```

**Production (Azure App Service)**
```python
USE_SHAREPOINT_STORAGE = "true"
SHAREPOINT_LIBRARY_NAME = "LeaseFileAudit Runs"
→ Uses SharePoint Document Library
→ Persists across restarts
```

### Next Steps for Deployment

1. **Test Locally** (current state)
   - Run the app locally
   - Should work exactly as before using local files
   - Log should show: `[STORAGE] Using local filesystem: instance/runs`

2. **Deploy to Azure**
   - Deploy this code to your App Service
   - Add environment variables:
     - `USE_SHAREPOINT_STORAGE=true`
     - `SHAREPOINT_LIBRARY_NAME=LeaseFileAudit Runs`
   - Restart the app

3. **Verify**
   - Run an audit
   - Check SharePoint library for new run folder
   - Check App Service logs for success messages

### Storage Flow

```
User uploads file
    ↓
App processes audit
    ↓
storage.save_run() called
    ↓
IF USE_SHAREPOINT_STORAGE=true:
    → Upload CSVs to SharePoint via Graph API
    → Files persist forever
ELSE:
    → Save to local instance/runs/
    → Files lost on restart (dev only)
```

### API Calls Used

- **GET** `/v1.0/sites/{hostname}:/{site-path}` - Get site ID
- **GET** `/v1.0/sites/{site-id}/drives` - Get drive ID for library
- **PUT** `/v1.0/sites/{site-id}/drives/{drive-id}/root:/{path}:/content` - Upload file
- **GET** `/v1.0/sites/{site-id}/drives/{drive-id}/root:/{path}:/content` - Download file
- **GET** `/v1.0/sites/{site-id}/drives/{drive-id}/root/children` - List folders

All authenticated using the existing Azure AD access token from Easy Auth.

### Testing Checklist

- [ ] Local app still runs without SharePoint config
- [ ] Local app saves to instance/runs/
- [ ] Deployed app connects to SharePoint library
- [ ] Deployed app uploads files successfully
- [ ] Deployed app lists runs from SharePoint
- [ ] Deployed app loads run details from SharePoint
- [ ] Files persist after app restart
- [ ] No errors in Application Insights

### Troubleshooting

If issues occur, check:
1. Application Insights logs for detailed errors
2. SharePoint library permissions
3. Environment variables are set correctly
4. Access token is being retrieved from Easy Auth
5. Library name matches exactly (case-sensitive)
