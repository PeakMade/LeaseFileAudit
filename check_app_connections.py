"""
Comprehensive diagnostic check for LeaseFileAudit app connections and functionality.
"""
import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_environment():
    """Check environment configuration."""
    print("\n=== ENVIRONMENT CONFIGURATION ===")
    env_vars = {
        'USE_SHAREPOINT_STORAGE': os.getenv('USE_SHAREPOINT_STORAGE'),
        'DISABLE_CSV_WRITES': os.getenv('DISABLE_CSV_WRITES'),
        'ASYNC_AUDIT_RESULTS_WRITE': os.getenv('ASYNC_AUDIT_RESULTS_WRITE'),
        'SHAREPOINT_WRITE_EXCEPTIONS_ONLY': os.getenv('SHAREPOINT_WRITE_EXCEPTIONS_ONLY'),
        'REQUIRE_AUTH': os.getenv('REQUIRE_AUTH'),
        'ENABLE_SHAREPOINT_LOGGING': os.getenv('ENABLE_SHAREPOINT_LOGGING'),
    }
    
    for key, value in env_vars.items():
        status = '✅' if value else '❌'
        print(f"{status} {key}: {value}")
    
    return all(v is not None for v in [
        os.getenv('SHAREPOINT_CLIENT_ID'),
        os.getenv('SHAREPOINT_TENANT_ID'),
        os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
    ])

def check_sharepoint_auth():
    """Check SharePoint authentication."""
    print("\n=== SHAREPOINT AUTHENTICATION ===")
    try:
        from storage.service import StorageService
        from config import config
        from activity_logging.sharepoint import _get_app_only_token
        
        # Get access token
        access_token = _get_app_only_token()
        
        # Create service instance
        service = StorageService(
            base_dir=config.storage.base_dir,
            use_sharepoint=config.storage.is_sharepoint_configured(),
            sharepoint_site_url=config.auth.sharepoint_site_url,
            library_name=config.storage.sharepoint_library_name,
            access_token=access_token,
        )
        
        # Check token
        if service.access_token:
            print("✅ SharePoint access token obtained")
            print(f"   Token length: {len(service.access_token)} chars")
            
            # Try to get site ID
            site_id = service._get_site_id()
            if site_id:
                print(f"✅ SharePoint site ID resolved: {site_id[:30]}...")
            else:
                print("❌ Failed to resolve SharePoint site ID")
                return False
            
            # Try to get list IDs
            lists_to_check = ['AuditRuns2', 'RunDisplaySnapshots', 'ExceptionMonths', 'Audit Run Metrics']
            for list_name in lists_to_check:
                list_id = service._get_sharepoint_list_id(list_name)
                if list_id:
                    print(f"✅ {list_name}: {list_id[:30]}...")
                else:
                    print(f"❌ {list_name}: NOT FOUND")
            
            return True
        else:
            print("❌ Failed to obtain SharePoint access token")
            return False
            
    except Exception as e:
        print(f"❌ SharePoint authentication error: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_app_running():
    """Check if the Flask app is running and accessible."""
    print("\n=== APPLICATION STATUS ===")
    try:
        response = requests.get('http://127.0.0.1:8000/', timeout=5)
        if response.status_code == 200:
            print("✅ App is running on http://127.0.0.1:8000")
            print(f"   Response size: {len(response.text)} bytes")
            return True
        else:
            print(f"❌ App returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ App is not running (connection refused)")
        return False
    except Exception as e:
        print(f"❌ Error checking app status: {e}")
        return False

def check_data_sources():
    """Check available data sources."""
    print("\n=== DATA SOURCES ===")
    try:
        from storage.service import StorageService
        from config import config
        from activity_logging.sharepoint import _get_app_only_token
        
        # Create service instance
        access_token = _get_app_only_token()
        service = StorageService(
            base_dir=config.storage.base_dir,
            use_sharepoint=config.storage.is_sharepoint_configured(),
            sharepoint_site_url=config.auth.sharepoint_site_url,
            library_name=config.storage.sharepoint_library_name,
            access_token=access_token,
        )
        
        # Check recent runs
        runs = service.list_runs(limit=3)
        if runs:
            print(f"✅ Found {len(runs)} recent audit run(s)")
            for run in runs[:3]:
                print(f"   - {run.get('run_id', 'Unknown')}: {run.get('property_count', 0)} properties")
        else:
            print("⚠️  No recent audit runs found")
        
        # Check CSV files
        csv_dir = Path('instance/runs')
        if csv_dir.exists():
            csv_files = list(csv_dir.glob('**/bucket_results.csv'))
            print(f"✅ Found {len(csv_files)} CSV file(s) in {csv_dir}")
        else:
            print(f"⚠️  CSV directory not found: {csv_dir}")
        
        # Check Parquet files
        parquet_files = list(csv_dir.glob('**/*.parquet')) if csv_dir.exists() else []
        print(f"✅ Found {len(parquet_files)} Parquet file(s)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking data sources: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_api_endpoints():
    """Check if key API endpoints are working."""
    print("\n=== API ENDPOINTS ===")
    if not check_app_running():
        print("⚠️  Skipping API endpoint checks (app not running)")
        return False
    
    endpoints = [
        '/api/property-picklist',
        '/api/recent-runs',
    ]
    
    all_ok = True
    for endpoint in endpoints:
        try:
            response = requests.get(f'http://127.0.0.1:8000{endpoint}', timeout=5)
            if response.status_code == 200:
                print(f"✅ {endpoint}: OK")
            else:
                print(f"❌ {endpoint}: Status {response.status_code}")
                all_ok = False
        except Exception as e:
            print(f"❌ {endpoint}: {e}")
            all_ok = False
    
    return all_ok

def check_entrata_config():
    """Check Entrata API configuration."""
    print("\n=== ENTRATA API CONFIGURATION ===")
    entrata_config_file = Path('entrata_environment.json')
    
    if entrata_config_file.exists():
        import json
        with open(entrata_config_file, 'r') as f:
            config = json.load(f)
        
        required_keys = ['username', 'password', 'base_url']
        all_present = all(key in config for key in required_keys)
        
        if all_present:
            print("✅ Entrata configuration file found and valid")
            print(f"   Base URL: {config.get('base_url', 'N/A')}")
            return True
        else:
            print("❌ Entrata configuration incomplete")
            return False
    else:
        print("⚠️  Entrata configuration file not found")
        return False

def main():
    """Run all diagnostic checks."""
    print("=" * 60)
    print("LEASEFILEAUDIT CONNECTION DIAGNOSTICS")
    print("=" * 60)
    
    results = {
        'Environment': check_environment(),
        'SharePoint Auth': check_sharepoint_auth(),
        'App Running': check_app_running(),
        'Data Sources': check_data_sources(),
        'API Endpoints': check_api_endpoints(),
        'Entrata Config': check_entrata_config(),
    }
    
    print("\n" + "=" * 60)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 60)
    
    for check, passed in results.items():
        status = '✅ PASS' if passed else '❌ FAIL'
        print(f"{status}: {check}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✅ ALL SYSTEMS OPERATIONAL")
    else:
        print("⚠️  SOME ISSUES DETECTED - See details above")
    print("=" * 60)
    
    return 0 if all_passed else 1

if __name__ == '__main__':
    sys.exit(main())
