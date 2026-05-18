#!/usr/bin/env python3
"""
Diagnostic tool to check why your app isn't writing audit results to AuditRuns2.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import config as cfg
from activity_logging.sharepoint import _get_app_only_token


def check_sharepoint_config():
    """Check all SharePoint configuration settings."""
    
    print("\n" + "="*70)
    print("SHAREPOINT CONFIGURATION DIAGNOSTIC")
    print("="*70 + "\n")
    
    auth_cfg = cfg.config.auth
    storage_cfg = cfg.config.storage
    
    # 1. Check basic credentials
    print("1️⃣  CREDENTIALS CHECK")
    print("-" * 70)
    
    tenant_id = os.getenv('SHAREPOINT_TENANT_ID')
    client_id = os.getenv('SHAREPOINT_CLIENT_ID')
    client_secret = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
    
    print(f"   SHAREPOINT_TENANT_ID: {'✅ SET' if tenant_id else '❌ MISSING'}")
    print(f"   SHAREPOINT_CLIENT_ID: {'✅ SET' if client_id else '❌ MISSING'}")
    print(f"   MICROSOFT_PROVIDER_AUTHENTICATION_SECRET: {'✅ SET' if client_secret else '❌ MISSING'}")
    
    if not all([tenant_id, client_id, client_secret]):
        print("\n   ⚠️  CRITICAL: Missing credentials!")
        return False
    
    # 2. Check site URL
    print("\n2️⃣  SHAREPOINT SITE URL CHECK")
    print("-" * 70)
    
    site_url = auth_cfg.sharepoint_site_url
    print(f"   SHAREPOINT_SITE_URL: {site_url or '❌ NOT SET'}")
    
    if not site_url:
        print("\n   ⚠️  CRITICAL: Site URL not configured!")
        return False
    
    # 3. Check audit results list name
    print("\n3️⃣  AUDIT RESULTS LIST CHECK")
    print("-" * 70)
    
    list_name = auth_cfg.audit_results_list_name
    print(f"   SHAREPOINT_AUDIT_RESULTS_LIST_NAME: {list_name or '❌ NOT SET'}")
    
    if not list_name:
        print("\n   ⚠️  List name not configured!")
        return False
    
    # 4. Check storage configuration
    print("\n4️⃣  STORAGE CONFIGURATION CHECK")
    print("-" * 70)
    
    use_sp = storage_cfg.use_sharepoint_storage
    print(f"   USE_SHAREPOINT_STORAGE: {use_sp}")
    print(f"   is_sharepoint_configured(): {storage_cfg.is_sharepoint_configured()}")
    
    if not storage_cfg.is_sharepoint_configured():
        print("\n   ⚠️  SharePoint storage not enabled!")
        print("      Make sure USE_SHAREPOINT_STORAGE=true in .env")
        return False
    
    # 5. Check logging configuration
    print("\n5️⃣  LOGGING CONFIGURATION CHECK")
    print("-" * 70)
    
    enable_logging = auth_cfg.enable_sharepoint_logging
    print(f"   ENABLE_SHAREPOINT_LOGGING: {enable_logging}")
    
    # 6. Test token acquisition
    print("\n6️⃣  TOKEN ACQUISITION TEST")
    print("-" * 70)
    
    token = _get_app_only_token()
    if token:
        print(f"   ✅ Successfully acquired access token")
        print(f"   Token length: {len(token)} characters")
        return True
    else:
        print(f"   ❌ Failed to acquire access token")
        print(f"   Check credentials: {tenant_id}, {client_id[:20]}...")
        return False


def main():
    success = check_sharepoint_config()
    
    print("\n" + "="*70)
    if success:
        print("✅ SHAREPOINT IS PROPERLY CONFIGURED")
        print("="*70)
        print("""
Your app SHOULD be writing audit results to AuditRuns2.

If audits are still not appearing in SharePoint, check:

1. Check the app logs for errors during audit execution
2. Verify that the audit actually completed (check browser for success message)
3. Look for async write operations (results may take 30-60 seconds to appear)
4. Check if the list exists and is accessible
""")
    else:
        print("❌ SHAREPOINT CONFIGURATION INCOMPLETE")
        print("="*70)
        print("""
Your app CANNOT write to SharePoint until these are fixed:

1. Add to your .env file:
   
   USE_SHAREPOINT_STORAGE=true
   ENABLE_SHAREPOINT_LOGGING=true
   SHAREPOINT_SITE_URL=https://peakcampus.sharepoint.com/sites/BaseCampApps
   SHAREPOINT_AUDIT_RESULTS_LIST_NAME=AuditRuns2
   SHAREPOINT_TENANT_ID=<your-tenant-id>
   SHAREPOINT_CLIENT_ID=<your-client-id>
   MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=<your-client-secret>

2. Restart your app after adding these settings

3. Run a new audit - results should now appear in AuditRuns2
""")
    
    print("\nNext steps:")
    print("  - Check .env file")
    print("  - Run: python diagnostic_sharepoint_config.py")
    print("  - Run: python write_sample_findings.py")
    print("  - Run: python query_auditruns2.py")


if __name__ == "__main__":
    main()
