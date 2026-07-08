"""Check what columns are in expected_detail and actual_detail in memory cache."""
import sys
sys.path.insert(0, '.')

from storage.service import StorageService
from config import config

storage = StorageService(config)
run_id = 'run_20260702_093008'

print(f"Checking run: {run_id}\n")

# Check in-memory cache
with storage._IN_MEMORY_CACHE_LOCK:
    if run_id in storage._IN_MEMORY_RESULTS_CACHE:
        cache_data = storage._IN_MEMORY_RESULTS_CACHE[run_id]
        
        expected_detail = cache_data.get('expected_detail')
        actual_detail = cache_data.get('actual_detail')
        
        print("=" * 80)
        print("EXPECTED DETAIL")
        print("=" * 80)
        if expected_detail is not None and not expected_detail.empty:
            print(f"Rows: {len(expected_detail)}")
            print(f"Columns: {list(expected_detail.columns)}")
            print(f"\nFirst few rows:")
            print(expected_detail.head(2))
        else:
            print("EMPTY OR NONE")
        
        print("\n" + "=" * 80)
        print("ACTUAL DETAIL")
        print("=" * 80)
        if actual_detail is not None and not actual_detail.empty:
            print(f"Rows: {len(actual_detail)}")
            print(f"Columns: {list(actual_detail.columns)}")
            print(f"\nFirst few rows:")
            print(actual_detail.head(2))
        else:
            print("EMPTY OR NONE")
    else:
        print(f"Run {run_id} NOT FOUND in cache")
        print(f"\nAvailable runs: {list(storage._IN_MEMORY_RESULTS_CACHE.keys())}")
