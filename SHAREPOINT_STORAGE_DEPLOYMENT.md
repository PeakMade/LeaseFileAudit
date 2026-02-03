# SharePoint Document Library Storage - Deployment Guide

This guide explains how to configure SharePoint Document Library storage for persistent audit run data in your deployed Azure App Service.

## What You've Already Done ✅

1. Created Document Library: **LeaseFileAudit Runs**
2. SharePoint Site: `https://peakcampus.sharepoint.com/sites/BaseCampApps`
3. Sample folder structure created

## Deployment Steps

### 1. Configure App Service Environment Variables

Add these environment variables to your Azure App Service:

1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to your App Service
3. Click **Configuration** → **Application settings**
4. Add the following settings:

| Name | Value | Description |
|------|-------|-------------|
| `USE_SHAREPOINT_STORAGE` | `true` | Enable SharePoint Document Library storage |
| `SHAREPOINT_LIBRARY_NAME` | `LeaseFileAudit Runs` | Exact name of your document library |

**Note**: Your existing `SHAREPOINT_SITE_URL` is already configured, so no additional site URL settings needed.

5. Click **Save** at the top
6. Click **Continue** to restart the app

### 2. Verify Permissions

Ensure the app's service principal has access to the SharePoint site:

1. Go to your SharePoint site: `https://peakcampus.sharepoint.com/sites/BaseCampApps`
2. Click **⚙️ Settings** → **Site permissions**
3. Verify the app registration (from `SHAREPOINT_CLIENT_ID`) or admin account has **Edit** permissions
4. If not, add it with **Edit** permissions

### 3. Deploy Updated Code

Deploy this updated code to Azure using your normal deployment process.

## How It Works

### Automatic Fallback
- **Production (Azure)**: Uses SharePoint when `USE_SHAREPOINT_STORAGE=true`
- **Development (Local)**: Uses local filesystem (`instance/runs/`) automatically

### File Structure in SharePoint

```
📁 LeaseFileAudit Runs (Document Library)
├── 📁 run_20260203_120000/
│   ├── 📁 inputs_normalized/
│   │   ├── 📄 expected_detail.csv (30-50 KB)
│   │   └── 📄 actual_detail.csv (20-40 KB)
│   ├── 📁 outputs/
│   │   ├── 📄 bucket_results.csv (15-25 KB)
│   │   ├── 📄 findings.csv (10-20 KB)
│   │   └── 📄 variance_detail.csv (varies)
│   └── 📄 run_meta.json (1 KB)
├── 📁 run_20260203_130000/
│   └── ...
```

### Code Changes Made

1. **config.py**: Added SharePoint storage configuration
2. **storage/service.py**: Implemented dual storage (SharePoint + local fallback)
3. **web/views.py**: Updated to pass access token to storage service

## Verify It's Working

### Check Logs in Azure
1. Go to App Service → **Log stream**
2. After deploying and running an audit, look for:
   ```
   [STORAGE] Using SharePoint Document Library: LeaseFileAudit Runs
   [STORAGE] Saved run run_20260203_120000
   ```

### Check SharePoint
1. Go to `https://peakcampus.sharepoint.com/sites/BaseCampApps`
2. Open **LeaseFileAudit Runs** library
3. After running an audit, you should see new `run_YYYYMMDD_HHMMSS` folders
4. Click into a folder to verify CSV files are present

### Test Local Development
1. Run the app locally (without setting environment variables)
2. It will automatically use `instance/runs/` folder
3. Log should show: `[STORAGE] Using local filesystem: instance/runs`

## Benefits of SharePoint Storage

✅ **Persistent** - Survives app restarts, redeployments, scaling  
✅ **Accessible** - Browse files directly in SharePoint UI  
✅ **Searchable** - SharePoint indexes file contents  
✅ **Shareable** - Easy to share specific runs with stakeholders  
✅ **Versioned** - SharePoint tracks file versions automatically  
✅ **No extra cost** - Uses your existing SharePoint storage  
✅ **Familiar** - Users already know how to navigate SharePoint  

## Storage Capacity

- SharePoint tenant storage: **1 TB+ included**
- Each audit run: **~100-200 KB**
- 1,000 runs = **100-200 MB**
- You can store **thousands** of audit runs with no issues

## Troubleshooting

### Runs still disappear after restart
- Verify `USE_SHAREPOINT_STORAGE=true` in App Service settings (not local)
- Check that environment variables are set in **Configuration**, not just locally
- Review Application Insights logs for storage errors

### "Failed to get site ID" error
- Verify `SHAREPOINT_SITE_URL` is correct: `https://peakcampus.sharepoint.com/sites/BaseCampApps`
- Check that the app has permissions to the SharePoint site
- Ensure access token is being passed correctly

### "Document library not found" error
- Verify library name exactly matches: `LeaseFileAudit Runs` (case-sensitive, includes space)
- Check that library exists at the site URL
- Try accessing the library manually with the same account

### Files upload but can't be read back
- Check library permissions - app needs **Edit** access, not just **Read**
- Verify folder structure is correct (run_YYYYMMDD_HHMMSS format)
- Look for Graph API errors in Application Insights

### Mixed runs (some local, some SharePoint)
This is expected:
- **Before deployment**: Runs saved locally (86 existing runs)
- **After deployment**: Runs saved to SharePoint (new runs)
- App will only show runs from the active storage location
- To see old runs in production, manually upload them to SharePoint

## Manual Migration (Optional)

To move existing local runs to SharePoint:

1. **Option A: Use SharePoint Web UI**
   - Open SharePoint library in browser
   - Drag/drop run folders from `instance/runs/` into the library
   
2. **Option B: Use SharePoint Sync**
   - Sync the library to your computer
   - Copy run folders into synced folder
   - Let SharePoint upload automatically

3. **Option C: Start Fresh**
   - Keep old runs local for reference
   - Only new production runs use SharePoint (recommended)

## Cost

**FREE** - SharePoint Document Library storage uses your tenant's included storage quota. No additional charges.

## Rollback

To disable SharePoint storage and go back to local filesystem:

1. In App Service, set `USE_SHAREPOINT_STORAGE=false`
2. Restart the app
3. App will use local filesystem again (but files will be lost on restart)

**Note**: This is not recommended for production - SharePoint is the persistent solution.
