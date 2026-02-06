"""
Flask views for Lease File Audit application.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import logging

from audit_engine import (
    ExcelSourceLoader,
    normalize_ar_transactions,
    normalize_scheduled_charges,
    expand_scheduled_to_months,
    reconcile_buckets,
    RuleContext,
    generate_findings,
    calculate_kpis,
    calculate_property_summary
)
from audit_engine.reconcile import reconcile_detail
from audit_engine.rules import default_registry
from audit_engine.canonical_fields import CanonicalField
from storage.service import StorageService
from config import config
from web.auth import require_auth, optional_auth, get_current_user, get_access_token
from activity_logging.sharepoint import log_user_activity
import os

logger = logging.getLogger(__name__)
bp = Blueprint('main', __name__)


def get_storage_service() -> StorageService:
    """Get storage service instance with SharePoint support."""
    # Get access token if SharePoint storage is enabled
    access_token = None
    if config.storage.is_sharepoint_configured():
        access_token = get_access_token()
    
    return StorageService(
        base_dir=config.storage.base_dir,
        use_sharepoint=config.storage.is_sharepoint_configured(),
        sharepoint_site_url=config.auth.sharepoint_site_url if config.storage.is_sharepoint_configured() else None,
        library_name=config.storage.sharepoint_library_name,
        access_token=access_token
    )


def get_available_runs() -> list:
    """Get all available runs sorted by date (most recent first)."""
    storage = get_storage_service()
    runs = storage.list_runs(limit=1000)  # Get all runs
    
    # Format runs for dropdown
    formatted_runs = []
    for run in runs:
        run_info = {
            'run_id': run['run_id'],
            'timestamp': run.get('timestamp', 'Unknown'),
            'audit_period': run.get('audit_period', {})
        }
        formatted_runs.append(run_info)
    
    return formatted_runs


def calculate_cumulative_metrics() -> dict:
    """Calculate cumulative portfolio metrics across all audit runs."""
    storage = get_storage_service()
    
    # Try to load metrics from SharePoint list first (fast path)
    all_metrics = storage.load_all_metrics_from_sharepoint_list()
    
    if all_metrics:
        # Use metrics from SharePoint list - much faster!
        logger.info(f"[METRICS] Using SharePoint list data ({len(all_metrics)} runs)")
        
        # Get most recent run
        most_recent_metrics = all_metrics[0] if all_metrics else None
        if not most_recent_metrics:
            return _empty_metrics_dict()
        
        # Current state from most recent run
        current_variances = most_recent_metrics['total_variances']
        matched = most_recent_metrics['matched']
        total_buckets = matched + current_variances
        match_rate = (matched / total_buckets * 100) if total_buckets > 0 else 0
        
        # Calculate undercharge/overcharge from most recent run
        # Need to load most recent run's bucket_results for detailed calculation
        # But we can approximate from the variances for now
        most_recent_run_id = most_recent_metrics['run_id']
        try:
            latest_data = storage.load_run(most_recent_run_id)
            latest_buckets = latest_data['bucket_results']
            
            current_exceptions = latest_buckets[
                latest_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
            ]
            
            current_undercharge = current_exceptions.apply(
                lambda row: max(0, row[CanonicalField.EXPECTED_TOTAL.value] - row[CanonicalField.ACTUAL_TOTAL.value]),
                axis=1
            ).sum()
            
            current_overcharge = current_exceptions.apply(
                lambda row: max(0, row[CanonicalField.ACTUAL_TOTAL.value] - row[CanonicalField.EXPECTED_TOTAL.value]),
                axis=1
            ).sum()
            
            total_leases_audited = latest_buckets[CanonicalField.LEASE_INTERVAL_ID.value].nunique()
        except Exception as e:
            logger.warning(f"[METRICS] Error loading most recent run details: {e}")
            # Fall back to approximations
            current_undercharge = 0
            current_overcharge = 0
            total_leases_audited = 0
        
        # Historical metrics - sum across all runs (not deduplicated, but fast)
        # This is an approximation - true deduplication would require loading all CSVs
        total_historical_variances = sum(m['total_variances'] for m in all_metrics)
        total_historical_high_severity = sum(m['high_severity'] for m in all_metrics)
        
        # Simplified recovery calculation - compare current vs. historical averages
        avg_variances_per_run = total_historical_variances / len(all_metrics) if all_metrics else 0
        money_recovered = max(0, avg_variances_per_run - current_variances) * 100  # Rough estimate
        
        current_net_variance = current_overcharge - current_undercharge
        
        return {
            'current_undercharge': float(current_undercharge),
            'current_overcharge': float(current_overcharge),
            'current_variance': float(current_net_variance),
            'open_exceptions': int(current_variances),
            'total_audits': int(total_leases_audited),
            'match_rate': float(match_rate),
            'money_recovered': float(money_recovered),
            'historical_undercharge': 0,  # Would require full CSV analysis
            'historical_overcharge': 0,   # Would require full CSV analysis
            'most_recent_run': {
                'run_id': most_recent_metrics['run_id'],
                'timestamp': most_recent_metrics['timestamp'],
                'uploaded_by': most_recent_metrics['uploaded_by']
            },
            'total_runs': len(all_metrics)
        }
    
    # Fallback to old method if SharePoint list not available (local mode or list empty)
    logger.info(f"[METRICS] SharePoint list not available, using CSV loading (slow)")
    all_runs = storage.list_runs(limit=1000)
    
    if not all_runs:
        return _empty_metrics_dict()
    
    # Get most recent run for current state
    most_recent = all_runs[0]
    latest_data = storage.load_run(most_recent['run_id'])
    latest_buckets = latest_data['bucket_results']
    
    # Current state from most recent audit
    current_exceptions = latest_buckets[
        latest_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
    ]
    
    # Proper undercharge/overcharge calculation:
    # Undercharge = expected > actual (we billed/collected less than scheduled)
    # Overcharge = actual > expected (we billed/collected more than scheduled)
    # Use MAX(0, difference) to get only positive contributions
    
    current_undercharge = current_exceptions.apply(
        lambda row: max(0, row[CanonicalField.EXPECTED_TOTAL.value] - row[CanonicalField.ACTUAL_TOTAL.value]),
        axis=1
    ).sum()
    
    current_overcharge = current_exceptions.apply(
        lambda row: max(0, row[CanonicalField.ACTUAL_TOTAL.value] - row[CanonicalField.EXPECTED_TOTAL.value]),
        axis=1
    ).sum()
    
    # Count unique leases audited (not buckets)
    total_leases_audited = latest_buckets[CanonicalField.LEASE_INTERVAL_ID.value].nunique()
    
    # Count open exceptions (unique buckets with issues)
    open_exceptions_count = len(current_exceptions)
    
    # Calculate match rate based on buckets
    total_buckets = len(latest_buckets)
    matched = total_buckets - open_exceptions_count
    match_rate = (matched / total_buckets * 100) if total_buckets > 0 else 0
    
    # Historical tracking - aggregate all unique exceptions ever found
    # Use dictionary to deduplicate and keep most recent occurrence
    exception_history = {}  # key: exc_key, value: {variance, run_id, timestamp}
    
    for run in all_runs:
        try:
            run_data = storage.load_run(run['run_id'])
            run_buckets = run_data['bucket_results']
            run_timestamp = run.get('timestamp', '')
            run_exceptions = run_buckets[
                run_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
            ]
            
            for _, exc in run_exceptions.iterrows():
                # Create unique key for each exception
                exc_key = (
                    exc[CanonicalField.PROPERTY_ID.value],
                    exc[CanonicalField.LEASE_INTERVAL_ID.value],
                    exc[CanonicalField.AR_CODE_ID.value],
                    exc[CanonicalField.AUDIT_MONTH.value]
                )
                
                # Keep only the most recent occurrence of each exception
                if exc_key not in exception_history or run_timestamp > exception_history[exc_key]['timestamp']:
                    exception_history[exc_key] = {
                        'key': exc_key,
                        'variance': exc[CanonicalField.VARIANCE.value],
                        'run_id': run['run_id'],
                        'timestamp': run_timestamp
                    }
        except Exception as e:
            print(f"Error loading run {run['run_id']}: {e}")
            continue
    
    # Calculate historical totals (all unique exceptions ever found - deduplicated)
    all_exception_data = list(exception_history.values())
    
    # Historical undercharge/overcharge using proper logic
    # Note: We only have variance in history, need to derive expected/actual
    # For historical: if variance < 0, it means actual < expected (undercharged)
    #                 if variance > 0, it means actual > expected (overcharged)
    historical_undercharge = sum(abs(exc['variance']) for exc in all_exception_data if exc['variance'] < 0)
    historical_overcharge = sum(exc['variance'] for exc in all_exception_data if exc['variance'] > 0)
    
    # Calculate recovery - exceptions that existed historically but not in current
    current_exception_keys = set()
    for _, exc in current_exceptions.iterrows():
        exc_key = (
            exc[CanonicalField.PROPERTY_ID.value],
            exc[CanonicalField.LEASE_INTERVAL_ID.value],
            exc[CanonicalField.AR_CODE_ID.value],
            exc[CanonicalField.AUDIT_MONTH.value]
        )
        current_exception_keys.add(exc_key)
    
    # Recovered = historical exceptions not in current
    recovered_exceptions = [exc for exc in all_exception_data if exc['key'] not in current_exception_keys]
    money_recovered = sum(abs(exc['variance']) for exc in recovered_exceptions)
    
    # Calculate true net variance: Overcharge - Undercharge (positive = net over-billed, negative = net under-billed)
    current_net_variance = current_overcharge - current_undercharge
    
    return {
        'current_undercharge': float(current_undercharge),
        'current_overcharge': float(current_overcharge),
        'current_variance': float(current_net_variance),
        'open_exceptions': int(open_exceptions_count),
        'total_audits': int(total_leases_audited),
        'match_rate': float(match_rate),
        'money_recovered': float(money_recovered),
        'historical_undercharge': float(historical_undercharge),
        'historical_overcharge': float(historical_overcharge),
        'most_recent_run': most_recent,
        'total_runs': len(all_runs)
    }


def _empty_metrics_dict():
    """Return empty metrics dictionary."""
    return {
        'current_undercharge': 0,
        'current_overcharge': 0,
        'current_variance': 0,
        'open_exceptions': 0,
        'total_audits': 0,
        'match_rate': 0,
        'money_recovered': 0,
        'historical_undercharge': 0,
        'historical_overcharge': 0,
        'most_recent_run': None,
        'total_runs': 0
    }


def filter_by_audit_period(df: pd.DataFrame, year: int = None, month: int = None) -> pd.DataFrame:
    """
    Filter a DataFrame by audit period (year and/or month).
    
    Args:
        df: DataFrame with AUDIT_MONTH column (datetime)
        year: Optional year to filter (e.g., 2024)
        month: Optional month to filter (1-12)
        
    Returns:
        Filtered DataFrame
    """
    from audit_engine.canonical_fields import CanonicalField
    
    if CanonicalField.AUDIT_MONTH.value not in df.columns:
        raise ValueError(f"DataFrame missing required column: {CanonicalField.AUDIT_MONTH.value}")
    
    result = df.copy()
    
    # Drop any NaT values first (shouldn't happen after normalize, but be safe)
    before_count = len(result)
    result = result[result[CanonicalField.AUDIT_MONTH.value].notna()]
    after_count = len(result)
    
    if before_count > after_count:
        print(f"[FILTER WARNING] Dropped {before_count - after_count} rows with NaT AUDIT_MONTH")
    
    # Filter by year if specified
    if year is not None:
        result = result[result[CanonicalField.AUDIT_MONTH.value].dt.year == year]
        print(f"[FILTER] Filtered to year {year}: {len(result)} rows remaining")
    
    # Filter by month if specified
    if month is not None:
        result = result[result[CanonicalField.AUDIT_MONTH.value].dt.month == month]
        print(f"[FILTER] Filtered to month {month}: {len(result)} rows remaining")
    
    return result


def execute_audit_run(file_path: Path, run_id: str, audit_year: int = None, audit_month: int = None) -> dict:
    """
    Execute complete audit run.
    
    Args:
        file_path: Path to uploaded Excel file
        run_id: Unique run identifier
        audit_year: Optional year to filter audit (e.g., 2024)
        audit_month: Optional month to filter audit (1-12)
    
    Returns:
        Dict with all results and metadata
    """
    # Load RAW data sources from Excel
    from audit_engine.io import load_excel_sources
    from audit_engine.mappings import apply_source_mapping, AR_TRANSACTIONS_MAPPING, SCHEDULED_CHARGES_MAPPING
    
    sources = load_excel_sources(file_path, config.ar_source, config.scheduled_source)
    print(f"\n[EXECUTE_AUDIT_RUN] Loaded raw sources:")
    print(f"  AR Transactions: {sources[config.ar_source.name].shape}")
    print(f"  Scheduled Charges: {sources[config.scheduled_source.name].shape}")
    
    # Apply source mappings to convert RAW -> CANONICAL
    print(f"\n[EXECUTE_AUDIT_RUN] Applying source mappings...")
    ar_canonical = apply_source_mapping(sources[config.ar_source.name], AR_TRANSACTIONS_MAPPING)
    scheduled_canonical = apply_source_mapping(sources[config.scheduled_source.name], SCHEDULED_CHARGES_MAPPING)
    
    # Normalize (validate canonical data)
    print(f"\n[EXECUTE_AUDIT_RUN] Normalizing canonical data...")
    actual_detail = normalize_ar_transactions(ar_canonical)
    scheduled_normalized = normalize_scheduled_charges(scheduled_canonical)
    
    # Expand scheduled to months
    expected_detail = expand_scheduled_to_months(scheduled_normalized)
    
    # Apply period filter if specified
    if audit_year is not None or audit_month is not None:
        print(f"\n[AUDIT PERIOD FILTER] Filtering to Year={audit_year or 'All'}, Month={audit_month or 'All'}")
        print(f"[AUDIT PERIOD FILTER] Before filter - Expected: {len(expected_detail)}, Actual: {len(actual_detail)}")
        
        expected_detail = filter_by_audit_period(expected_detail, audit_year, audit_month)
        actual_detail = filter_by_audit_period(actual_detail, audit_year, audit_month)
        
        print(f"[AUDIT PERIOD FILTER] After filter - Expected: {len(expected_detail)}, Actual: {len(actual_detail)}")
    
    # Filter out API/timed/external charges from actual_detail
    # These codes are expected to be billed without schedule and should not appear in audit
    from audit_engine.mappings import API_POSTED_AR_CODES
    if not actual_detail.empty and CanonicalField.AR_CODE_ID.value in actual_detail.columns:
        original_count = len(actual_detail)
        
        print(f"\n[API CODE FILTER] ========== FILTERING API CODES ==========")
        print(f"[API CODE FILTER] API codes to filter: {API_POSTED_AR_CODES}")
        print(f"[API CODE FILTER] Total AR transactions before filter: {original_count}")
        print(f"[API CODE FILTER] AR_CODE_ID column dtype: {actual_detail[CanonicalField.AR_CODE_ID.value].dtype}")
        print(f"[API CODE FILTER] Sample AR_CODE_ID values: {actual_detail[CanonicalField.AR_CODE_ID.value].unique()[:20].tolist()}")
        
        # Check which API codes are present
        ar_code_col = actual_detail[CanonicalField.AR_CODE_ID.value]
        api_codes_present = actual_detail[ar_code_col.isin(API_POSTED_AR_CODES)]
        
        if not api_codes_present.empty:
            print(f"[API CODE FILTER] Found {len(api_codes_present)} API code transactions to filter:")
            for code in API_POSTED_AR_CODES:
                count = (ar_code_col == code).sum()
                if count > 0:
                    print(f"[API CODE FILTER]   - Code {code}: {count} transactions")
        else:
            print(f"[API CODE FILTER] No API codes found in data (checking data type match...)")
            # Try with type conversion
            try:
                ar_code_int = ar_code_col.astype(int)
                matches = ar_code_int.isin(API_POSTED_AR_CODES).sum()
                print(f"[API CODE FILTER] After int conversion: {matches} matches found")
            except:
                pass
        
        # Apply filter
        actual_detail = actual_detail[~ar_code_col.isin(API_POSTED_AR_CODES)].copy()
        
        filtered_count = original_count - len(actual_detail)
        print(f"[API CODE FILTER] Filtered out: {filtered_count} transactions")
        print(f"[API CODE FILTER] Remaining AR transactions: {len(actual_detail)}")
        print(f"[API CODE FILTER] ==========================================\n")
    
    # Reconcile (detailed row-level with PRIMARY/SECONDARY/TERTIARY matching) - DO THIS FIRST
    # This identifies date mismatches via TERTIARY matching
    # Note: Use the normalized (not expanded) versions for detailed reconciliation
    variance_detail, recon_stats = reconcile_detail(
        scheduled_normalized,  # Use non-expanded scheduled charges
        actual_detail,         # Use normalized AR transactions
        config.reconciliation
    )
    
    print(f"\n[RECONCILIATION STATS]")
    print(f"  Primary matches: {recon_stats['primary_matched_ar']}")
    print(f"  Secondary matches: {recon_stats['secondary_matched_ar']}")
    print(f"  Tertiary matches: {recon_stats['tertiary_matched_ar']}")
    print(f"  Unmatched AR: {recon_stats['unmatched_ar']}")
    print(f"  Unmatched scheduled: {recon_stats['unmatched_scheduled']}")
    print(f"  Total variances: {recon_stats['variances']}")
    
    # Reconcile (bucket-level aggregation)
    # Note: DATE_MISMATCH variances from variance_detail will be displayed separately in lease view
    bucket_results = reconcile_buckets(expected_detail, actual_detail, config.reconciliation)
    
    # Execute rules
    context = RuleContext(
        run_id=run_id,
        expected_detail=expected_detail,
        actual_detail=actual_detail,
        bucket_results=bucket_results
    )
    
    finding_dicts = default_registry.evaluate_all(context)
    findings = generate_findings(finding_dicts, run_id)
    
    return {
        "expected_detail": expected_detail,
        "actual_detail": actual_detail,
        "bucket_results": bucket_results,
        "variance_detail": variance_detail,
        "recon_stats": recon_stats,
        "findings": findings
    }


@bp.route('/')
@optional_auth
def index():
    """Upload form and recent runs."""
    import logging
    logger = logging.getLogger(__name__)
    
    storage = get_storage_service()
    recent_runs = storage.list_runs(limit=10)
    user = get_current_user()
    
    # Log login activity to SharePoint if user is authenticated
    logger.info(f"[INDEX] User present: {user is not None}")
    if user:
        logger.info(f"[INDEX] User keys: {list(user.keys())}")
        logger.info(f"[INDEX] SharePoint logging enabled: {config.auth.enable_sharepoint_logging}")
        logger.info(f"[INDEX] Can log to SharePoint: {config.auth.can_log_to_sharepoint()}")
        logger.info(f"[INDEX] SharePoint site URL: {config.auth.sharepoint_site_url}")
        logger.info(f"[INDEX] SharePoint list name: {config.auth.sharepoint_list_name}")
        
    if user and config.auth.can_log_to_sharepoint():
        logger.info(f"[INDEX] Attempting to log to SharePoint...")
        result = log_user_activity(
            user_info=user,
            activity_type='Start Session',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={'page': 'index', 'user_role': 'user'}
        )
        logger.info(f"[INDEX] SharePoint logging result: {result}")
    else:
        logger.warning(f"[INDEX] SharePoint logging skipped - user: {user is not None}, can_log: {config.auth.can_log_to_sharepoint() if user else 'N/A'}")
    
    return render_template('upload.html', recent_runs=recent_runs, user=user)


@bp.route('/end-session')
@require_auth
def end_session():
    """End the user's session and log activity to SharePoint."""
    import logging
    logger = logging.getLogger(__name__)
    
    user = get_current_user()
    
    # Log session end activity to SharePoint if user is authenticated
    if user and config.auth.can_log_to_sharepoint():
        logger.info(f"[END_SESSION] Logging session end for user: {user.get('name', 'Unknown')}")
        result = log_user_activity(
            user_info=user,
            activity_type='End Session',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={'page': 'end_session', 'user_role': 'user'}
        )
        logger.info(f"[END_SESSION] SharePoint logging result: {result}")
    
    return render_template('session_ended.html', user=user)


@bp.route('/upload', methods=['POST'])
@require_auth
def upload():
    """Handle file upload and execute audit."""
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('main.index'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No file selected', 'danger')
        return redirect(url_for('main.index'))
    
    if not file.filename.endswith('.xlsx'):
        flash('Please upload an Excel (.xlsx) file', 'danger')
        return redirect(url_for('main.index'))
    
    try:
        # Get audit period filters from form
        audit_year = request.form.get('audit_year')
        audit_month = request.form.get('audit_month')
        
        # Convert to int if provided, otherwise None
        audit_year = int(audit_year) if audit_year else None
        audit_month = int(audit_month) if audit_month else None
        
        # Save uploaded file
        storage = get_storage_service()
        run_id = storage.generate_run_id()
        run_dir = storage.create_run_dir(run_id)
        
        filename = secure_filename(file.filename)
        
        # For SharePoint, run_dir might not be a real directory - use temp or ensure it exists
        if storage.use_sharepoint:
            # Create a temporary local directory for processing
            import tempfile
            temp_dir = Path(tempfile.mkdtemp())
            file_path = temp_dir / filename
        else:
            file_path = run_dir / filename
        
        file.save(str(file_path))
        
        # Execute audit with period filter
        results = execute_audit_run(file_path, run_id, audit_year, audit_month)
        
        # Save results
        metadata = storage.create_metadata(run_id, file_path)
        # Add period filter to metadata
        if audit_year or audit_month:
            metadata['audit_period'] = {
                'year': audit_year,
                'month': audit_month
            }
        
        storage.save_run(
            run_id,
            results["expected_detail"],
            results["actual_detail"],
            results["bucket_results"],
            results["findings"],
            metadata,
            results.get("variance_detail"),
            file_path  # Pass the original Excel file path
        )
        
        # Clean up temp file if using SharePoint
        if storage.use_sharepoint:
            import shutil
            try:
                shutil.rmtree(file_path.parent)  # Remove temp directory
            except Exception as e:
                import logging
                logging.warning(f"Failed to cleanup temp directory: {e}")
        
        # Create success message with period info
        period_msg = ""
        if audit_year or audit_month:
            period_parts = []
            if audit_month:
                month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                             'July', 'August', 'September', 'October', 'November', 'December']
                period_parts.append(month_names[audit_month])
            if audit_year:
                period_parts.append(str(audit_year))
            period_msg = f" (Period: {' '.join(period_parts)})"
        
        flash(f'Audit completed successfully! Run ID: {run_id}{period_msg}', 'success')
        
        # Log successful audit completion to SharePoint
        user = get_current_user()
        if user and config.auth.can_log_to_sharepoint():
            log_user_activity(
                user_info=user,
                activity_type='Successful Audit',
                site_url=config.auth.sharepoint_site_url,
                list_name=config.auth.sharepoint_list_name,
                details={
                    'run_id': run_id,
                    'file_name': filename,
                    'audit_year': audit_year,
                    'audit_month': audit_month,
                    'user_role': 'user'
                }
            )
        
        return redirect(url_for('main.portfolio', run_id=run_id))
        
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_trace = traceback.format_exc()
        print(f"\n[ERROR IN UPLOAD] {error_msg}")
        print(f"[ERROR TRACEBACK]\n{error_trace}")
        
        # Log failed audit to SharePoint
        user = get_current_user()
        if user and config.auth.can_log_to_sharepoint():
            log_user_activity(
                user_info=user,
                activity_type='Failed Audit',
                site_url=config.auth.sharepoint_site_url,
                list_name=config.auth.sharepoint_list_name,
                details={
                    'file_name': filename if 'filename' in locals() else 'unknown',
                    'error': error_msg,
                    'user_role': 'user'
                }
            )
        
        flash(f'Error processing file: {error_msg}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/portfolio')
@bp.route('/portfolio/<run_id>')
@require_auth
def portfolio(run_id: str = None):
    """Portfolio view - Cumulative KPIs across all runs."""
    try:
        storage = get_storage_service()
        
        # Calculate cumulative metrics across all runs
        cumulative = calculate_cumulative_metrics()
        
        if not cumulative['most_recent_run']:
            flash('No audit runs available', 'warning')
            return redirect(url_for('main.index'))
        
        # Use most recent run if not specified
        if not run_id:
            run_id = cumulative['most_recent_run']['run_id']
        
        # Get most recent run data for property breakdown
        run_data = storage.load_run(run_id)
        
        # Calculate property summary from most recent run
        property_summary = calculate_property_summary(
            run_data["bucket_results"],
            run_data["findings"],
            run_data["actual_detail"]  # Pass actual_detail to get property names
        )
        
        return render_template(
            'portfolio.html',
            run_id=run_id,
            metadata=run_data["metadata"],
            kpis=cumulative,
            properties=property_summary.to_dict('records'),
            total_runs=cumulative['total_runs']
        )
    except Exception as e:
        import traceback
        print(f"[ERROR] Portfolio view error: {str(e)}")
        print(traceback.format_exc())
        flash(f'Error loading portfolio: {str(e)}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/property/<property_id>')
@bp.route('/property/<property_id>/<run_id>')
@require_auth
def property_view(property_id: str, run_id: str = None):
    """Property view - exceptions grouped by lease with run selector."""
    try:
        storage = get_storage_service()
        
        # Get all available runs
        all_runs = get_available_runs()
        
        # If no run_id specified, use the most recent
        if not run_id and all_runs:
            run_id = all_runs[0]['run_id']
        elif not run_id:
            flash('No audit runs available', 'warning')
            return redirect(url_for('main.index'))
        
        run_data = storage.load_run(run_id)
        
        # Get bucket results for this property (all statuses)
        bucket_results = run_data["bucket_results"]
        all_property_buckets = bucket_results[
            bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)
        ]
        
        # Get property name from actual detail or use hardcoded mapping
        PROPERTY_NAME_MAP = {
            1122966: "48 West",
            100069944: "Bixby Kennesaw"
        }
        
        property_name = None
        actual_detail = run_data["actual_detail"]
        expected_detail = run_data["expected_detail"]
        property_actual = actual_detail[actual_detail[CanonicalField.PROPERTY_ID.value] == float(property_id)]
        if len(property_actual) > 0 and CanonicalField.PROPERTY_NAME.value in property_actual.columns:
            property_name = property_actual[CanonicalField.PROPERTY_NAME.value].iloc[0]
        
        # Fallback to hardcoded names
        if not property_name:
            property_name = PROPERTY_NAME_MAP.get(int(float(property_id)))
        
        # Get all unique leases for this property
        all_lease_ids = sorted(all_property_buckets[CanonicalField.LEASE_INTERVAL_ID.value].unique())
        
        # Filter to only exceptions for grouping
        property_buckets = all_property_buckets[
            all_property_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
        ].copy()
        
        # Group exceptions by lease_interval_id
        lease_groups = {}
        for _, bucket in property_buckets.iterrows():
            lease_id = bucket[CanonicalField.LEASE_INTERVAL_ID.value]
            if lease_id not in lease_groups:
                lease_groups[lease_id] = []
            
            # Add exception details
            exception = {
                'ar_code_id': bucket[CanonicalField.AR_CODE_ID.value],
                'audit_month': bucket[CanonicalField.AUDIT_MONTH.value],
                'status': bucket[CanonicalField.STATUS.value],
                'expected_total': bucket[CanonicalField.EXPECTED_TOTAL.value],
                'actual_total': bucket[CanonicalField.ACTUAL_TOTAL.value],
                'variance': bucket[CanonicalField.VARIANCE.value],
                'status_label': _get_status_label(bucket[CanonicalField.STATUS.value]),
                'status_color': _get_status_color(bucket[CanonicalField.STATUS.value])
            }
            lease_groups[lease_id].append(exception)
        
        # Build comprehensive lease summary (including clean and matched leases)
        lease_summary = []
        for lease_id in all_lease_ids:
            # Get guarantor name and customer name for this lease
            guarantor_name = None
            customer_name = None
            
            # First check actual_detail (posted transactions)
            lease_actual_data = actual_detail[actual_detail[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id]
            if len(lease_actual_data) > 0:
                if CanonicalField.GUARANTOR_NAME.value in lease_actual_data.columns:
                    guarantor_value = lease_actual_data[CanonicalField.GUARANTOR_NAME.value].iloc[0]
                    if pd.notna(guarantor_value):
                        guarantor_name = guarantor_value
                
                if CanonicalField.CUSTOMER_NAME.value in lease_actual_data.columns:
                    customer_value = lease_actual_data[CanonicalField.CUSTOMER_NAME.value].iloc[0]
                    if pd.notna(customer_value):
                        customer_name = customer_value
            
            # If not found in actual, check expected_detail (scheduled charges)
            if not guarantor_name or not customer_name:
                lease_expected_data = expected_detail[expected_detail[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id]
                if len(lease_expected_data) > 0:
                    if not guarantor_name and CanonicalField.GUARANTOR_NAME.value in lease_expected_data.columns:
                        guarantor_value = lease_expected_data[CanonicalField.GUARANTOR_NAME.value].iloc[0]
                        if pd.notna(guarantor_value):
                            guarantor_name = guarantor_value
                    
                    if not customer_name and CanonicalField.CUSTOMER_NAME.value in lease_expected_data.columns:
                        customer_value = lease_expected_data[CanonicalField.CUSTOMER_NAME.value].iloc[0]
                        if pd.notna(customer_value):
                            customer_name = customer_value
            
            # Get all buckets for this lease
            lease_all_buckets = all_property_buckets[
                all_property_buckets[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id
            ]
            matched_count = len(lease_all_buckets[
                lease_all_buckets[CanonicalField.STATUS.value] == config.reconciliation.status_matched
            ])
            
            if lease_id in lease_groups:
                # Lease has exceptions
                exceptions = lease_groups[lease_id]
                total_variance = sum(e['variance'] for e in exceptions)
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'customer_name': customer_name,
                    'guarantor_name': guarantor_name,
                    'has_exceptions': True,
                    'exception_count': len(exceptions),
                    'matched_count': matched_count,
                    'total_variance': total_variance,
                    'exceptions': sorted(exceptions, key=lambda x: abs(x['variance']), reverse=True)
                })
            else:
                # Clean lease - no exceptions
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'customer_name': customer_name,
                    'guarantor_name': guarantor_name,
                    'has_exceptions': False,
                    'exception_count': 0,
                    'matched_count': matched_count,
                    'total_variance': 0,
                    'exceptions': []
                })
        
        # Sort: exceptions first (by variance), then clean leases
        lease_summary = sorted(lease_summary, key=lambda x: (not x['has_exceptions'], -x['total_variance']))
        
        # Calculate property KPIs
        property_kpis = calculate_kpis(
            all_property_buckets,
            run_data["findings"],
            property_id=None  # Already filtered, don't filter again
        )
        
        return render_template(
            'property.html',
            run_id=run_id,
            property_id=property_id,
            property_name=property_name,
            metadata=run_data["metadata"],
            kpis=property_kpis,
            lease_summary=lease_summary,
            exception_count=len(property_buckets),
            all_runs=all_runs,
            current_run_id=run_id
        )
    except Exception as e:
        import traceback
        print(f"[ERROR] Property view error: {str(e)}")
        print(traceback.format_exc())
        flash(f'Error loading property: {str(e)}', 'danger')
        return redirect(url_for('main.portfolio', run_id=run_id))


def _get_status_label(status: str) -> str:
    """Get human-readable status label."""
    labels = {
        "SCHEDULED_NOT_BILLED": "Scheduled Not Billed",
        "BILLED_NOT_SCHEDULED": "Billed Without Schedule",
        "AMOUNT_MISMATCH": "Amount Mismatch"
    }
    return labels.get(status, status)


def _get_status_color(status: str) -> str:
    """Get brand color class for status."""
    colors = {
        "SCHEDULED_NOT_BILLED": "brand-danger",  # magenta
        "BILLED_NOT_SCHEDULED": "brand-accent",  # orange
        "AMOUNT_MISMATCH": "brand-primary"  # cyan
    }
    return colors.get(status, "secondary")


def build_entrata_url(lease_id: str, customer_id: str = None) -> str:
    """Build Entrata resident profile URL.
    
    Opens the resident profile shell (module=customers_systemxxx) with customer[id] and lease[id].
    
    Args:
        lease_id: The lease interval ID
        customer_id: The customer ID (optional)
        
    Returns:
        Entrata URL string. If customer_id is missing, returns customers module.
    """
    base_url = "https://peakmade.entrata.com/"
    
    if customer_id and str(customer_id).strip() and str(customer_id).lower() != 'nan':
        # Open resident profile shell
        return f"{base_url}?module=customers_systemxxx&customer[id]={customer_id}&lease[id]={lease_id}"
    else:
        # Fallback to customers module if customer_id is missing
        return f"{base_url}?module=customers_systemxxx"


@bp.route('/lease/<run_id>/<property_id>/<lease_interval_id>')
@require_auth
def lease_view(run_id: str, property_id: str, lease_interval_id: str):
    """Lease view - detailed exceptions for a specific lease."""
    try:
        storage = get_storage_service()
        run_data = storage.load_run(run_id)
        
        # Get all buckets for this lease - exceptions and matches separately
        bucket_results = run_data["bucket_results"]
        lease_buckets = bucket_results[
            (bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)) &
            (bucket_results[CanonicalField.LEASE_INTERVAL_ID.value] == float(lease_interval_id)) &
            (bucket_results[CanonicalField.STATUS.value] != config.reconciliation.status_matched)
        ].copy()
        
        # Get matched buckets for this lease
        matched_buckets = bucket_results[
            (bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)) &
            (bucket_results[CanonicalField.LEASE_INTERVAL_ID.value] == float(lease_interval_id)) &
            (bucket_results[CanonicalField.STATUS.value] == config.reconciliation.status_matched)
        ].copy()
        
        # Get expected and actual detail for this lease
        expected_detail = run_data["expected_detail"]
        actual_detail = run_data["actual_detail"]
        
        lease_expected = expected_detail[
            expected_detail[CanonicalField.LEASE_INTERVAL_ID.value] == float(lease_interval_id)
        ]
        lease_actual = actual_detail[
            actual_detail[CanonicalField.LEASE_INTERVAL_ID.value] == float(lease_interval_id)
        ]
        
        # Build detailed exception list with actual dates
        exceptions = []
        for _, bucket in lease_buckets.iterrows():
            ar_code = bucket[CanonicalField.AR_CODE_ID.value]
            audit_month = bucket[CanonicalField.AUDIT_MONTH.value]
            
            # Get expected records for this bucket
            expected_records = lease_expected[
                (lease_expected[CanonicalField.AR_CODE_ID.value] == ar_code) &
                (lease_expected[CanonicalField.AUDIT_MONTH.value] == audit_month)
            ]
            
            # Get actual records for this bucket
            actual_records = lease_actual[
                (lease_actual[CanonicalField.AR_CODE_ID.value] == ar_code) &
                (lease_actual[CanonicalField.AUDIT_MONTH.value] == audit_month)
            ]
            
            # Extract dates
            charge_start = None
            charge_end = None
            post_dates = []
            ar_code_name = None
            
            if not expected_records.empty:
                if 'PERIOD_START' in expected_records.columns:
                    charge_start = expected_records['PERIOD_START'].iloc[0]
                    # Convert NaT to None, Timestamp to date string
                    if pd.isna(charge_start):
                        charge_start = None
                    elif isinstance(charge_start, pd.Timestamp):
                        charge_start = charge_start.strftime('%Y-%m-%d')
                if 'PERIOD_END' in expected_records.columns:
                    charge_end = expected_records['PERIOD_END'].iloc[0]
                    # Convert NaT to None, Timestamp to date string
                    if pd.isna(charge_end):
                        charge_end = None
                    elif isinstance(charge_end, pd.Timestamp):
                        charge_end = charge_end.strftime('%Y-%m-%d')
            
            if not actual_records.empty:
                if 'POST_DATE' in actual_records.columns:
                    # Convert Timestamps to date strings
                    post_dates = [pd.Timestamp(d).strftime('%Y-%m-%d') for d in actual_records['POST_DATE'].dropna()]
                # Try to get AR code name from actual records first
                if 'AR_CODE_NAME' in actual_records.columns:
                    name_value = actual_records['AR_CODE_NAME'].iloc[0]
                    if pd.notna(name_value):
                        ar_code_name = name_value
            
            # If not in actual, try expected records
            if not ar_code_name and not expected_records.empty:
                if 'AR_CODE_NAME' in expected_records.columns:
                    name_value = expected_records['AR_CODE_NAME'].iloc[0]
                    if pd.notna(name_value):
                        ar_code_name = name_value
            
            # Build individual transaction details
            expected_transactions = []
            actual_transactions = []
            missing_dates_warning = []
            
            if not expected_records.empty:
                for _, exp_rec in expected_records.iterrows():
                    period_start = exp_rec.get('PERIOD_START')
                    period_end = exp_rec.get('PERIOD_END')
                    
                    # Convert NaT to None, Timestamps to date strings for template compatibility
                    if pd.isna(period_start):
                        missing_dates_warning.append(f"Missing PERIOD_START for expected charge")
                        period_start = None
                    elif isinstance(period_start, pd.Timestamp):
                        period_start = period_start.strftime('%Y-%m-%d')
                    
                    if pd.isna(period_end):
                        missing_dates_warning.append(f"Missing PERIOD_END for expected charge")
                        period_end = None
                    elif isinstance(period_end, pd.Timestamp):
                        period_end = period_end.strftime('%Y-%m-%d')
                    
                    expected_transactions.append({
                        'amount': exp_rec.get('expected_amount', 0),
                        'period_start': period_start,
                        'period_end': period_end,
                        'ar_code_name': exp_rec.get('AR_CODE_NAME', ar_code_name)
                    })
            
            if not actual_records.empty:
                for _, act_rec in actual_records.iterrows():
                    post_date = act_rec.get('POST_DATE')
                    
                    # Convert NaT to None, Timestamps to date strings for template compatibility
                    if pd.isna(post_date):
                        missing_dates_warning.append(f"Missing POST_DATE for actual transaction")
                        post_date = None
                    elif isinstance(post_date, pd.Timestamp):
                        post_date = post_date.strftime('%Y-%m-%d')
                    
                    actual_transactions.append({
                        'amount': act_rec.get('actual_amount', 0),
                        'post_date': post_date,
                        'ar_code_name': act_rec.get('AR_CODE_NAME', ar_code_name),
                        'transaction_id': act_rec.get('AR_TRANSACTION_ID')
                    })
            
            # Convert audit_month Timestamp to date string
            audit_month_str = audit_month.strftime('%Y-%m-%d') if isinstance(audit_month, pd.Timestamp) else audit_month
            
            exception = {
                'ar_code_id': ar_code,
                'ar_code_name': ar_code_name,
                'audit_month': audit_month_str,
                'charge_start': charge_start,
                'charge_end': charge_end,
                'post_dates': post_dates,
                'status': bucket[CanonicalField.STATUS.value],
                'status_label': _get_status_label(bucket[CanonicalField.STATUS.value]),
                'status_color': _get_status_color(bucket[CanonicalField.STATUS.value]),
                'expected_total': bucket[CanonicalField.EXPECTED_TOTAL.value],
                'actual_total': bucket[CanonicalField.ACTUAL_TOTAL.value],
                'variance': bucket[CanonicalField.VARIANCE.value],
                'expected_transactions': expected_transactions,
                'actual_transactions': actual_transactions,
                'missing_dates_warning': missing_dates_warning
            }
            
            # Get detailed reason/description
            if exception['status'] == 'SCHEDULED_NOT_BILLED':
                exception['description'] = f"Expected ${exception['expected_total']:.2f} to be billed based on schedule, but nothing was billed."
                exception['recommendation'] = "Verify if charge should have been billed. Check if lease terms changed or if this is a billing error."
            elif exception['status'] == 'BILLED_NOT_SCHEDULED':
                exception['description'] = f"${exception['actual_total']:.2f} was billed but no scheduled charge exists for this AR code."
                exception['recommendation'] = "Verify if this is a valid one-time charge or if schedule is missing/incomplete."
            elif exception['status'] == 'AMOUNT_MISMATCH':
                exception['description'] = f"Expected ${exception['expected_total']:.2f} but ${exception['actual_total']:.2f} was billed (${exception['variance']:.2f} difference)."
                exception['recommendation'] = "Review if this is due to proration, lease amendment, or billing error."
            
            # Add missing date warning to description if present
            if missing_dates_warning:
                exception['description'] += f" ⚠️ DATA QUALITY ISSUE: {'; '.join(set(missing_dates_warning))}."
            
            exceptions.append(exception)
        
        # Group exceptions by AR code and status
        grouped_exceptions = {}
        for exc in exceptions:
            group_key = (exc['ar_code_id'], exc['status'])
            
            if group_key not in grouped_exceptions:
                # Create new group
                grouped_exceptions[group_key] = {
                    'ar_code_id': exc['ar_code_id'],
                    'ar_code_name': exc['ar_code_name'],
                    'status': exc['status'],
                    'status_label': exc['status_label'],
                    'status_color': exc['status_color'],
                    'total_variance': 0,
                    'total_expected': 0,
                    'total_actual': 0,
                    'month_count': 0,
                    'monthly_details': [],
                    'all_expected_transactions': [],
                    'all_actual_transactions': []
                }
            
            # Aggregate totals
            group = grouped_exceptions[group_key]
            group['total_variance'] += exc['variance']
            group['total_expected'] += exc['expected_total']
            group['total_actual'] += exc['actual_total']
            group['month_count'] += 1
            
            # Store monthly detail
            group['monthly_details'].append({
                'audit_month': exc['audit_month'],
                'charge_start': exc['charge_start'],
                'charge_end': exc['charge_end'],
                'post_dates': exc['post_dates'],
                'expected_total': exc['expected_total'],
                'actual_total': exc['actual_total'],
                'variance': exc['variance'],
                'expected_transactions': exc['expected_transactions'],
                'actual_transactions': exc['actual_transactions'],
                'description': exc['description'],
                'recommendation': exc['recommendation']
            })
            
            # Aggregate all transactions
            group['all_expected_transactions'].extend(exc['expected_transactions'])
            group['all_actual_transactions'].extend(exc['actual_transactions'])
        
        # Convert to list and sort by total variance
        grouped_list = list(grouped_exceptions.values())
        grouped_list = sorted(grouped_list, key=lambda x: abs(x['total_variance']), reverse=True)
        
        # Calculate lease totals with proper undercharge/overcharge logic
        total_expected = sum(g['total_expected'] for g in grouped_list)
        total_actual = sum(g['total_actual'] for g in grouped_list)
        total_variance = total_actual - total_expected
        total_undercharge = max(0, total_expected - total_actual)
        total_overcharge = max(0, total_actual - total_expected)
        
        # Get property name
        PROPERTY_NAME_MAP = {
            1122966: "48 West",
            100069944: "Bixby Kennesaw"
        }
        property_name = None
        if len(lease_actual) > 0 and CanonicalField.PROPERTY_NAME.value in lease_actual.columns:
            property_name = lease_actual[CanonicalField.PROPERTY_NAME.value].iloc[0]
        if not property_name:
            property_name = PROPERTY_NAME_MAP.get(int(float(property_id)))
        
        # Get customer name and IDs from actual records
        customer_name = None
        customer_id = None
        lease_id = None
        
        if len(lease_actual) > 0:
            if CanonicalField.CUSTOMER_NAME.value in lease_actual.columns:
                customer_value = lease_actual[CanonicalField.CUSTOMER_NAME.value].iloc[0]
                if pd.notna(customer_value):
                    customer_name = customer_value
            
            if CanonicalField.CUSTOMER_ID.value in lease_actual.columns:
                customer_id_value = lease_actual[CanonicalField.CUSTOMER_ID.value].iloc[0]
                if pd.notna(customer_id_value):
                    customer_id = int(customer_id_value)
            
            if CanonicalField.LEASE_ID.value in lease_actual.columns:
                lease_id_value = lease_actual[CanonicalField.LEASE_ID.value].iloc[0]
                if pd.notna(lease_id_value):
                    lease_id = int(lease_id_value)
        
        # If not found in actual, check expected records (scheduled charges)
        if not customer_name and len(lease_expected) > 0:
            if CanonicalField.CUSTOMER_NAME.value in lease_expected.columns:
                customer_value = lease_expected[CanonicalField.CUSTOMER_NAME.value].iloc[0]
                if pd.notna(customer_value):
                    customer_name = customer_value
        
        if not customer_id and len(lease_expected) > 0:
            if CanonicalField.CUSTOMER_ID.value in lease_expected.columns:
                customer_id_value = lease_expected[CanonicalField.CUSTOMER_ID.value].iloc[0]
                if pd.notna(customer_id_value):
                    customer_id = int(customer_id_value)
        
        if not lease_id and len(lease_expected) > 0:
            if CanonicalField.LEASE_ID.value in lease_expected.columns:
                lease_id_value = lease_expected[CanonicalField.LEASE_ID.value].iloc[0]
                if pd.notna(lease_id_value):
                    lease_id = int(lease_id_value)
        
        # Build matched details grouped by AR code
        matched_groups = {}
        for _, bucket in matched_buckets.iterrows():
            ar_code = bucket[CanonicalField.AR_CODE_ID.value]
            audit_month = bucket[CanonicalField.AUDIT_MONTH.value]
            
            # Get expected and actual records for this match
            expected_records = lease_expected[
                (lease_expected[CanonicalField.AR_CODE_ID.value] == ar_code) &
                (lease_expected[CanonicalField.AUDIT_MONTH.value] == audit_month)
            ]
            
            actual_records = lease_actual[
                (lease_actual[CanonicalField.AR_CODE_ID.value] == ar_code) &
                (lease_actual[CanonicalField.AUDIT_MONTH.value] == audit_month)
            ]
            
            # Get AR code name
            ar_code_name = None
            if not actual_records.empty and 'AR_CODE_NAME' in actual_records.columns:
                name_value = actual_records['AR_CODE_NAME'].iloc[0]
                if pd.notna(name_value):
                    ar_code_name = name_value
            if not ar_code_name and not expected_records.empty and 'AR_CODE_NAME' in expected_records.columns:
                name_value = expected_records['AR_CODE_NAME'].iloc[0]
                if pd.notna(name_value):
                    ar_code_name = name_value
            
            if ar_code not in matched_groups:
                matched_groups[ar_code] = {
                    'ar_code_id': ar_code,
                    'ar_code_name': ar_code_name,
                    'total_amount': 0,
                    'month_count': 0,
                    'monthly_details': []
                }
            
            # Build transaction details
            expected_transactions = []
            actual_transactions = []
            
            if not expected_records.empty:
                for _, exp_rec in expected_records.iterrows():
                    period_start = exp_rec.get('PERIOD_START')
                    period_end = exp_rec.get('PERIOD_END')
                    
                    # Convert Timestamps to date strings
                    if pd.notna(period_start) and isinstance(period_start, pd.Timestamp):
                        period_start = period_start.strftime('%Y-%m-%d')
                    elif pd.isna(period_start):
                        period_start = None
                    
                    if pd.notna(period_end) and isinstance(period_end, pd.Timestamp):
                        period_end = period_end.strftime('%Y-%m-%d')
                    elif pd.isna(period_end):
                        period_end = None
                    
                    expected_transactions.append({
                        'amount': exp_rec.get('expected_amount', 0),
                        'period_start': period_start,
                        'period_end': period_end,
                    })
            
            if not actual_records.empty:
                for _, act_rec in actual_records.iterrows():
                    post_date = act_rec.get('POST_DATE')
                    
                    # Convert Timestamp to date string
                    if pd.notna(post_date) and isinstance(post_date, pd.Timestamp):
                        post_date = post_date.strftime('%Y-%m-%d')
                    elif pd.isna(post_date):
                        post_date = None
                    
                    actual_transactions.append({
                        'amount': act_rec.get('actual_amount', 0),
                        'post_date': post_date,
                        'transaction_id': act_rec.get('AR_TRANSACTION_ID')
                    })
            
            # Convert audit_month to date string
            audit_month_str = audit_month.strftime('%Y-%m-%d') if isinstance(audit_month, pd.Timestamp) else audit_month
            
            matched_groups[ar_code]['total_amount'] += bucket[CanonicalField.ACTUAL_TOTAL.value]
            matched_groups[ar_code]['month_count'] += 1
            matched_groups[ar_code]['monthly_details'].append({
                'audit_month': audit_month_str,
                'amount': bucket[CanonicalField.ACTUAL_TOTAL.value],
                'expected_transactions': expected_transactions,
                'actual_transactions': actual_transactions
            })
        
        # Convert to sorted list
        matched_list = sorted(matched_groups.values(), key=lambda x: x['total_amount'], reverse=True)
        
        # Create unified list of all AR codes - ONE ROW PER AR CODE
        ar_code_unified = {}
        
        # First, add all matched months by AR code
        for match in matched_list:
            ar_code_id = match['ar_code_id']
            if ar_code_id not in ar_code_unified:
                ar_code_unified[ar_code_id] = {
                    'ar_code_id': ar_code_id,
                    'ar_code_name': match['ar_code_name'],
                    'matched_count': 0,
                    'exception_count': 0,
                    'monthly_details': [],
                    'has_exceptions': False
                }
            
            ar_code_unified[ar_code_id]['matched_count'] = match['month_count']
            
            # Add matched monthly details with status flag
            for monthly in match['monthly_details']:
                ar_code_unified[ar_code_id]['monthly_details'].append({
                    'audit_month': monthly['audit_month'],
                    'expected_transactions': monthly['expected_transactions'],
                    'actual_transactions': monthly['actual_transactions'],
                    'status': 'matched',
                    'status_label': 'Matched',
                    'status_color': 'success',
                    'variance': 0,
                    'expected_total': monthly['amount'],
                    'actual_total': monthly['amount']
                })
        
        # Then, add all exception months by AR code
        for exc in grouped_list:
            ar_code_id = exc['ar_code_id']
            if ar_code_id not in ar_code_unified:
                ar_code_unified[ar_code_id] = {
                    'ar_code_id': ar_code_id,
                    'ar_code_name': exc['ar_code_name'],
                    'matched_count': 0,
                    'exception_count': 0,
                    'monthly_details': [],
                    'has_exceptions': False
                }
            
            ar_code_unified[ar_code_id]['exception_count'] += exc['month_count']
            ar_code_unified[ar_code_id]['has_exceptions'] = True
            
            # Add exception monthly details with status flag
            for monthly in exc['monthly_details']:
                ar_code_unified[ar_code_id]['monthly_details'].append({
                    'audit_month': monthly['audit_month'],
                    'expected_transactions': monthly['expected_transactions'],
                    'actual_transactions': monthly['actual_transactions'],
                    'status': exc['status'],
                    'status_label': exc['status_label'],
                    'status_color': exc['status_color'],
                    'variance': monthly['variance'],
                    'expected_total': monthly['expected_total'],
                    'actual_total': monthly['actual_total'],
                    'description': monthly.get('description'),
                    'recommendation': monthly.get('recommendation')
                })
        
        # Determine overall status for each AR code
        all_ar_codes = []
        for ar_data in ar_code_unified.values():
            # Sort monthly details by audit month
            # Convert None to pd.Timestamp('1900-01-01') so it sorts first
            def _sort_audit_month(item):
                value = item.get('audit_month')
                if value is None:
                    return pd.Timestamp('1900-01-01')
                if isinstance(value, pd.Timestamp):
                    return value
                return pd.to_datetime(value, errors='coerce') or pd.Timestamp('1900-01-01')

            ar_data['monthly_details'] = sorted(
                ar_data['monthly_details'],
                key=_sort_audit_month
            )
            
            # Determine overall status
            if ar_data['has_exceptions']:
                # If has exceptions, show the most critical status
                ar_data['status_label'] = f"{ar_data['matched_count']} Matched, {ar_data['exception_count']} Exception(s)"
                ar_data['status_color'] = 'warning'
            else:
                ar_data['status_label'] = f"{ar_data['matched_count']} Matched"
                ar_data['status_color'] = 'success'
            
            all_ar_codes.append(ar_data)
        
        # Sort by AR code ID
        all_ar_codes = sorted(all_ar_codes, key=lambda x: x['ar_code_id'])
        
        # Convert NaT/NaN values to None for JSON serialization
        def sanitize_for_json(obj):
            """Recursively convert pandas NaT/NaN to None and Timestamps to date strings for JSON serialization."""
            if isinstance(obj, dict):
                return {k: sanitize_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [sanitize_for_json(item) for item in obj]
            elif pd.isna(obj):
                return None
            elif isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
                # Convert to date-only string to prevent JavaScript timezone conversion
                # '2026-02-01' instead of '2026-02-01T00:00:00' avoids UTC interpretation
                return obj.strftime('%Y-%m-%d')
            elif hasattr(obj, 'isoformat') and hasattr(obj, 'date'):
                # Handle Python datetime objects
                return obj.strftime('%Y-%m-%d')
            else:
                return obj
        
        all_ar_codes = sanitize_for_json(all_ar_codes)
        grouped_list = sanitize_for_json(grouped_list)
        matched_list = sanitize_for_json(matched_list)
        
        # Build Entrata URL using LEASE_ID (not LEASE_INTERVAL_ID)
        entrata_url = build_entrata_url(lease_id, customer_id)
        has_customer_id = customer_id is not None and lease_id is not None
        
        return render_template(
            'lease.html',
            run_id=run_id,
            property_id=property_id,
            property_name=property_name,
            customer_name=customer_name,
            customer_id=customer_id,
            lease_id=lease_id,
            lease_interval_id=lease_interval_id,
            entrata_url=entrata_url,
            has_customer_id=has_customer_id,
            metadata=run_data["metadata"],
            exceptions=grouped_list,
            exception_count=len(grouped_list),
            matched_records=matched_list,
            matched_count=len(matched_list),
            all_ar_codes=all_ar_codes,
            total_variance=total_variance,
            total_expected=total_expected,
            total_actual=total_actual,
            total_undercharge=total_undercharge,
            total_overcharge=total_overcharge
        )
    except Exception as e:
        import traceback
        print(f"[ERROR] Lease view error: {str(e)}")
        print(traceback.format_exc())
        flash(f'Error loading lease: {str(e)}', 'danger')
        return redirect(url_for('main.property_view', run_id=run_id, property_id=property_id))


@bp.route('/bucket/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>/<audit_month>')
@require_auth
def bucket_drilldown(run_id: str, property_id: str, lease_interval_id: str, 
                      ar_code_id: str, audit_month: str):
    """Bucket drilldown - expected vs actual detail and findings."""
    try:
        storage = get_storage_service()
        run_data = storage.load_run(run_id)
        
        # Convert audit_month to datetime
        audit_month_dt = pd.to_datetime(audit_month)
        
        # Get bucket info
        bucket_results = run_data["bucket_results"]
        bucket = bucket_results[
            (bucket_results[CanonicalField.PROPERTY_ID.value] == property_id) &
            (bucket_results[CanonicalField.LEASE_INTERVAL_ID.value] == lease_interval_id) &
            (bucket_results[CanonicalField.AR_CODE_ID.value] == ar_code_id) &
            (bucket_results[CanonicalField.AUDIT_MONTH.value] == audit_month_dt)
        ]
        
        if bucket.empty:
            flash('Bucket not found', 'warning')
            return redirect(url_for('main.property_view', run_id=run_id, property_id=property_id))
        
        bucket_info = bucket.iloc[0].to_dict()
        
        # Get expected detail
        expected_detail = run_data["expected_detail"]
        expected_records = expected_detail[
            (expected_detail[CanonicalField.PROPERTY_ID.value] == property_id) &
            (expected_detail[CanonicalField.LEASE_INTERVAL_ID.value] == lease_interval_id) &
            (expected_detail[CanonicalField.AR_CODE_ID.value] == ar_code_id) &
            (expected_detail[CanonicalField.AUDIT_MONTH.value] == audit_month_dt)
        ]
        
        # Get actual detail
        actual_detail = run_data["actual_detail"]
        actual_records = actual_detail[
            (actual_detail[CanonicalField.PROPERTY_ID.value] == property_id) &
            (actual_detail[CanonicalField.LEASE_INTERVAL_ID.value] == lease_interval_id) &
            (actual_detail[CanonicalField.AR_CODE_ID.value] == ar_code_id) &
            (actual_detail[CanonicalField.AUDIT_MONTH.value] == audit_month_dt)
        ]
        
        # Get findings for this bucket
        findings = run_data["findings"]
        bucket_findings = findings[
            (findings["property_id"] == property_id) &
            (findings["lease_interval_id"] == lease_interval_id) &
            (findings["ar_code_id"] == ar_code_id) &
            (findings["audit_month"] == audit_month_dt)
        ]
        
        return render_template(
            'bucket.html',
            run_id=run_id,
            property_id=property_id,
            bucket_info=bucket_info,
            expected_records=expected_records.to_dict('records'),
            actual_records=actual_records.to_dict('records'),
            findings=bucket_findings.to_dict('records'),
            metadata=run_data["metadata"]
        )
    except Exception as e:
        flash(f'Error loading bucket: {str(e)}', 'danger')
        return redirect(url_for('main.property_view', run_id=run_id, property_id=property_id))
