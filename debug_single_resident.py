"""
Single Resident Audit Debugger

This script processes the audit workflow for ONE resident, showing detailed
output at each step so you can understand the complete process.

Usage:
    # From Excel file (uses local data)
    python debug_single_resident.py --lease-id 12345
    python debug_single_resident.py --resident-id 67890
    python debug_single_resident.py --customer-id 67890
    python debug_single_resident.py --customer-name "John Doe"
    
    # From Entrata API (fetches live data for one lease)
    python debug_single_resident.py --api --lease-id 12345
    python debug_single_resident.py --api --lease-id 12345 --property-id 456
"""
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add repo root to path
sys.path.insert(0, str(Path(__file__).parent))

from audit_engine.io import load_excel_sources
from audit_engine.api_ingest import fetch_single_lease_api_sources
from audit_engine.mappings import (
    apply_source_mapping,
    AR_TRANSACTIONS_MAPPING,
    SCHEDULED_CHARGES_MAPPING,
)
from audit_engine.normalize import normalize_ar_transactions, normalize_scheduled_charges
from audit_engine.expand import expand_scheduled_to_months
from audit_engine.reconcile import reconcile_detail
from audit_engine.findings import generate_findings
from audit_engine.canonical_fields import CanonicalField
from audit_engine.lease_term_extraction_rules import get_term_extraction_rules
from config import config

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def print_dataframe_summary(df: pd.DataFrame, name: str):
    """Print summary of a dataframe."""
    print(f"\n{name}:")
    print(f"  Shape: {df.shape}")
    if not df.empty:
        print(f"  Columns: {list(df.columns)}")
        print(f"\n  First few rows:")
        print(df.head(3).to_string())
    else:
        print("  (empty)")


def debug_single_resident(
    excel_path: str = None,
    lease_id: int = None,
    customer_id: int = None,
    resident_id: int = None,
    customer_name: str = None,
    use_api: bool = False,
    property_id: int = None,
):
    """
    Process audit for a single resident with detailed debugging output.
    
    Args:
        excel_path: Path to the Excel file with AR and Scheduled data (for file mode)
        lease_id: Optional Lease ID to filter
        customer_id: Optional Customer ID to filter (file mode only)
        resident_id: Optional Resident ID to filter (file mode only)
        customer_name: Optional Customer Name to filter (file mode only)
        use_api: If True, fetch from Entrata API instead of Excel
        property_id: Optional Property ID (for API mode)
    """
    
    # ===========================================================================
    # STEP 1: Load Raw Sources
    # ===========================================================================
    print_section("STEP 1: Loading Raw Source Data")
    
    if use_api:
        # ===== API MODE =====
        if not lease_id:
            print("ERROR: --lease-id is required when using --api mode")
            return
        
        print(f"Fetching from Entrata API for Lease ID: {lease_id}")
        if property_id:
            print(f"  Property ID: {property_id}")
        
        try:
            api_result = fetch_single_lease_api_sources(
                lease_id=lease_id,
                property_id=property_id,
            )
            
            print(f"✓ API Fetch Successful")
            print(f"  Property: {api_result['property_name']}")
            print(f"  Lease Count: {api_result['lease_count']}")
            
            ar_raw = api_result['ar_raw']
            scheduled_raw = api_result['scheduled_raw']
            
            print(f"✓ Loaded AR Transactions: {ar_raw.shape}")
            print(f"✓ Loaded Scheduled Charges: {scheduled_raw.shape}")
            
        except Exception as e:
            print(f"❌ API Error: {e}")
            print("\nMake sure these environment variables are set:")
            print("  - LEASE_API_DETAILS_URL (or LEASE_API_BASE_URL)")
            print("  - LEASE_API_AR_URL (or LEASE_API_BASE_URL)")
            print("  - LEASE_API_KEY")
            return
    
    else:
        # ===== FILE MODE =====
        if not excel_path:
            print("ERROR: --excel path is required when not using --api mode")
            return
        
        excel_file = Path(excel_path)
        if not excel_file.exists():
            print(f"ERROR: File not found: {excel_path}")
            print("\nAvailable run files:")
            instance_dir = Path("instance/runs")
            if instance_dir.exists():
                for run_dir in sorted(instance_dir.glob("run_*"), reverse=True):
                    data_file = run_dir / "data.xlsx"
                    if data_file.exists():
                        print(f"  {data_file}")
            return
        
        print(f"Loading from: {excel_file}")
        sources = load_excel_sources(excel_file, config.ar_source, config.scheduled_source)
        
        ar_raw = sources[config.ar_source.name]
        scheduled_raw = sources[config.scheduled_source.name]
        
        print(f"✓ Loaded AR Transactions: {ar_raw.shape}")
        print(f"✓ Loaded Scheduled Charges: {scheduled_raw.shape}")
    
    # ===========================================================================
    # STEP 2: Apply Source Mappings (RAW → CANONICAL)
    # ===========================================================================
    print_section("STEP 2: Mapping Raw Columns to Canonical Fields")
    
    print("Applying AR Transactions mapping...")
    ar_canonical = apply_source_mapping(ar_raw, AR_TRANSACTIONS_MAPPING)
    print(f"  Canonical AR shape: {ar_canonical.shape}")
    print(f"  Canonical AR columns: {list(ar_canonical.columns)}")
    
    print("\nApplying Scheduled Charges mapping...")
    scheduled_canonical = apply_source_mapping(scheduled_raw, SCHEDULED_CHARGES_MAPPING)
    print(f"  Canonical Scheduled shape: {scheduled_canonical.shape}")
    print(f"  Canonical Scheduled columns: {list(scheduled_canonical.columns)}")
    
    # ===========================================================================
    # STEP 3: Filter to Single Resident
    # ===========================================================================
    print_section("STEP 3: Filtering to Single Resident")
    
    if use_api:
        # API mode - data is already filtered to one lease
        print(f"✓ API mode - data already filtered to Lease ID {lease_id}")
        ar_canonical_filtered = ar_canonical
        scheduled_canonical_filtered = scheduled_canonical
    else:
        # File mode - need to filter the data
        def filter_to_resident(df: pd.DataFrame, name: str) -> pd.DataFrame:
            """Filter dataframe to single resident."""
            original_count = len(df)
            
            if lease_id is not None and CanonicalField.LEASE_ID.value in df.columns:
                df = df[df[CanonicalField.LEASE_ID.value] == lease_id].copy()
                print(f"  {name}: Filtered by LEASE_ID={lease_id}: {original_count} → {len(df)} rows")
            
            elif resident_id is not None and CanonicalField.RESIDENT_ID.value in df.columns:
                df = df[df[CanonicalField.RESIDENT_ID.value] == resident_id].copy()
                print(f"  {name}: Filtered by RESIDENT_ID={resident_id}: {original_count} → {len(df)} rows")
            
            elif customer_id is not None and CanonicalField.CUSTOMER_ID.value in df.columns:
                df = df[df[CanonicalField.CUSTOMER_ID.value] == customer_id].copy()
                print(f"  {name}: Filtered by CUSTOMER_ID={customer_id}: {original_count} → {len(df)} rows")
            
            elif customer_name is not None and CanonicalField.CUSTOMER_NAME.value in df.columns:
                df = df[df[CanonicalField.CUSTOMER_NAME.value].str.contains(customer_name, case=False, na=False)].copy()
                print(f"  {name}: Filtered by CUSTOMER_NAME containing '{customer_name}': {original_count} → {len(df)} rows")
            
            else:
                print(f"  {name}: No filter applied (no matching ID column)")
            
            return df
        
        ar_canonical_filtered = filter_to_resident(ar_canonical, "AR Transactions")
        scheduled_canonical_filtered = filter_to_resident(scheduled_canonical, "Scheduled Charges")
    
    if ar_canonical_filtered.empty and scheduled_canonical_filtered.empty:
        print("\n❌ No data found for this resident!")
        if not use_api and CanonicalField.CUSTOMER_ID.value in ar_canonical.columns:
            print("\nAvailable residents in AR data:")
            print(ar_canonical[[CanonicalField.CUSTOMER_ID.value, CanonicalField.CUSTOMER_NAME.value]].drop_duplicates().head(10))
        return
    
    # ===========================================================================
    # STEP 4: Normalize Canonical Data
    # ===========================================================================
    print_section("STEP 4: Normalizing and Validating Canonical Data")
    
    print("Normalizing AR transactions...")
    actual_detail = normalize_ar_transactions(ar_canonical_filtered)
    print_dataframe_summary(actual_detail, "Normalized AR (Actual)")
    
    print("\nNormalizing Scheduled charges...")
    scheduled_normalized = normalize_scheduled_charges(scheduled_canonical_filtered)
    print_dataframe_summary(scheduled_normalized, "Normalized Scheduled")
    
    # ===========================================================================
    # STEP 5: Expand Scheduled to Months
    # ===========================================================================
    print_section("STEP 5: Expanding Scheduled Charges to Monthly Expected Detail")
    
    print("Expanding scheduled charges across their date ranges...")
    expected_detail = expand_scheduled_to_months(scheduled_normalized)
    print_dataframe_summary(expected_detail, "Expected Monthly Detail")
    
    if not expected_detail.empty:
        print("\nExpected charges by month:")
        month_summary = expected_detail.groupby(CanonicalField.AUDIT_MONTH.value).agg({
            CanonicalField.AMOUNT.value: ['count', 'sum']
        })
        print(month_summary)
    
    # ===========================================================================
    # STEP 6: Reconcile Expected vs Actual
    # ===========================================================================
    print_section("STEP 6: Reconciling Expected vs Actual Charges")
    
    print("Running bucket-based reconciliation...")
    buckets = reconcile_detail(
        actual_detail=actual_detail,
        expected_detail=expected_detail,
        context=None  # Context not needed for detail reconcile
    )
    
    print(f"\nReconciliation Results:")
    print(f"  Matched Charges: {len(buckets['matched'])}")
    print(f"  Unbilled (Expected but Not Billed): {len(buckets['unbilled'])}")
    print(f"  Unscheduled (Billed but Not Expected): {len(buckets['unscheduled'])}")
    
    if not buckets['matched'].empty:
        print("\n📊 MATCHED CHARGES:")
        print(buckets['matched'][[
            CanonicalField.AUDIT_MONTH.value,
            CanonicalField.AR_CODE_NAME.value,
            CanonicalField.AMOUNT.value,
        ]].to_string())
    
    if not buckets['unbilled'].empty:
        print("\n⚠️  UNBILLED (Missing from AR):")
        print(buckets['unbilled'][[
            CanonicalField.AUDIT_MONTH.value,
            CanonicalField.AR_CODE_NAME.value,
            CanonicalField.AMOUNT.value,
        ]].to_string())
    
    if not buckets['unscheduled'].empty:
        print("\n❌ UNSCHEDULED (Billed without Schedule):")
        print(buckets['unscheduled'][[
            CanonicalField.AUDIT_MONTH.value,
            CanonicalField.AR_CODE_NAME.value,
            CanonicalField.AMOUNT.value,
        ]].to_string())
    
    # ===========================================================================
    # STEP 7: Generate Findings
    # ===========================================================================
    print_section("STEP 7: Generating Audit Findings from Rules")
    
    print("Apply audit rules to identify exceptions...")
    from audit_engine.rules import default_registry
    from audit_engine.schemas import RuleContext
    
    context = RuleContext(
        actual=actual_detail,
        expected=expected_detail,
        scheduled=scheduled_normalized,
        matched=buckets['matched'],
        unbilled=buckets['unbilled'],
        unscheduled=buckets['unscheduled'],
    )
    
    findings = generate_findings(context, default_registry())
    
    print(f"\n✓ Generated {len(findings)} findings")
    
    if not findings.empty:
        print("\n🔍 AUDIT FINDINGS:")
        print(findings[[
            'RULE_NAME',
            'SEVERITY',
            'AUDIT_MONTH',
            'AR_CODE_NAME',
            'EXPECTED_AMOUNT',
            'ACTUAL_AMOUNT',
            'DIFFERENCE',
        ]].to_string())
    
    # ===========================================================================
    # STEP 8: Show Term Extraction Configuration
    # ===========================================================================
    print_section("STEP 8: Lease Term Extraction Configuration")
    
    print("Available term extraction rules:")
    term_rules = get_term_extraction_rules()
    for term_type in term_rules.keys():
        print(f"  ✓ {term_type}")
    
    if "TELECOM_FEE" in term_rules:
        print("\n📡 TELECOM_FEE configuration:")
        telecom_config = term_rules["TELECOM_FEE"]
        print(f"  Include patterns: {telecom_config.get('include_patterns', [])[:3]}...")
        print(f"  One-time signals: {telecom_config.get('one_time_signals', [])}")
    
    # ===========================================================================
    # Summary
    # ===========================================================================
    print_section("SUMMARY")
    
    print(f"Data Source:")
    if use_api:
        print(f"  Mode: Entrata API")
        print(f"  Lease ID: {lease_id}")
        if property_id:
            print(f"  Property ID: {property_id}")
    else:
        print(f"  Mode: Excel File")
        if lease_id:
            print(f"  Filter: LEASE_ID = {lease_id}")
        if resident_id:
            print(f"  Filter: RESIDENT_ID = {resident_id}")
        if customer_id:
            print(f"  Filter: CUSTOMER_ID = {customer_id}")
        if customer_name:
            print(f"  Filter: CUSTOMER_NAME contains '{customer_name}'")
    
    print(f"\nData Processed:")
    print(f"  AR Transactions: {len(actual_detail)}")
    print(f"  Scheduled Charges: {len(scheduled_normalized)}")
    print(f"  Expected Monthly Detail: {len(expected_detail)}")
    
    print(f"\nReconciliation Results:")
    print(f"  ✓ Matched: {len(buckets['matched'])}")
    print(f"  ⚠️  Unbilled: {len(buckets['unbilled'])}")
    print(f"  ❌ Unscheduled: {len(buckets['unscheduled'])}")
    
    print(f"\nFindings: {len(findings)} exceptions identified")
    
    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Debug audit process for a single resident')
    parser.add_argument('--excel', type=str, help='Path to Excel file (for file mode)')
    parser.add_argument('--lease-id', type=int, help='Filter by Lease ID')
    parser.add_argument('--resident-id', type=int, help='Filter by Resident ID (file mode only)')
    parser.add_argument('--customer-id', type=int, help='Filter by Customer ID (file mode only)')
    parser.add_argument('--customer-name', type=str, help='Filter by Customer Name - partial match (file mode only)')
    parser.add_argument('--api', action='store_true', help='Fetch from Entrata API instead of Excel file')
    parser.add_argument('--property-id', type=int, help='Property ID (optional for API mode)')
    
    args = parser.parse_args()
    
    if args.api:
        # ===== API MODE =====
        if not args.lease_id:
            print("ERROR: --lease-id is required when using --api mode")
            print("\nUsage:")
            print("  python debug_single_resident.py --api --lease-id 12345")
            print("  python debug_single_resident.py --api --lease-id 12345 --property-id 456")
            return
        
        debug_single_resident(
            excel_path=None,
            lease_id=args.lease_id,
            use_api=True,
            property_id=args.property_id,
        )
    
    else:
        # ===== FILE MODE =====
        # Find latest run if no excel specified
        excel_path = args.excel
        if not excel_path:
            instance_runs = Path("instance/runs")
            if instance_runs.exists():
                run_dirs = sorted(instance_runs.glob("run_*"), reverse=True)
                for run_dir in run_dirs:
                    data_file = run_dir / "data.xlsx"
                    if data_file.exists():
                        excel_path = str(data_file)
                        break
        
        if not excel_path:
            print("ERROR: No Excel file specified and no runs found in instance/runs/")
            print("\nUsage (File Mode):")
            print("  python debug_single_resident.py --excel path/to/data.xlsx --lease-id 12345")
            print("  python debug_single_resident.py --resident-id 67890")
            print("  python debug_single_resident.py --customer-id 67890")
            print("  python debug_single_resident.py --customer-name 'John Doe'")
            print("\nUsage (API Mode):")
            print("  python debug_single_resident.py --api --lease-id 12345")
            return
        
        if not any([args.lease_id, args.resident_id, args.customer_id, args.customer_name]):
            print("ERROR: Must specify at least one filter: --lease-id, --resident-id, --customer-id, or --customer-name")
            return
        
        debug_single_resident(
            excel_path=excel_path,
            lease_id=args.lease_id,
            resident_id=args.residentth,
            lease_id=args.lease_id,
            customer_id=args.customer_id,
            customer_name=args.customer_name,
            use_api=False,
        )


if __name__ == '__main__':
    main()
