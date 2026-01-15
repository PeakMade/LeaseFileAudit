"""
Flask views for Lease File Audit application.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd

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
from audit_engine.rules import default_registry
from audit_engine.canonical_fields import CanonicalField
from storage.service import StorageService
from config import config

bp = Blueprint('main', __name__)


def get_storage_service() -> StorageService:
    """Get storage service instance."""
    return StorageService(config.storage.base_dir)


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
    
    # Reconcile
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
        "findings": findings
    }


@bp.route('/')
def index():
    """Upload form and recent runs."""
    storage = get_storage_service()
    recent_runs = storage.list_runs(limit=10)
    return render_template('upload.html', recent_runs=recent_runs)


@bp.route('/upload', methods=['POST'])
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
            metadata
        )
        
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
        return redirect(url_for('main.portfolio', run_id=run_id))
        
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_trace = traceback.format_exc()
        print(f"\n[ERROR IN UPLOAD] {error_msg}")
        print(f"[ERROR TRACEBACK]\n{error_trace}")
        flash(f'Error processing file: {error_msg}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/portfolio/<run_id>')
def portfolio(run_id: str):
    """Portfolio view - KPIs and property summary."""
    try:
        storage = get_storage_service()
        run_data = storage.load_run(run_id)
        
        # Calculate portfolio KPIs
        portfolio_kpis = calculate_kpis(
            run_data["bucket_results"],
            run_data["findings"]
        )
        
        # Calculate undercharge/overcharge from variance
        bucket_results = run_data["bucket_results"]
        undercharge = bucket_results[bucket_results[CanonicalField.VARIANCE.value] < 0][CanonicalField.VARIANCE.value].sum()
        overcharge = bucket_results[bucket_results[CanonicalField.VARIANCE.value] > 0][CanonicalField.VARIANCE.value].sum()
        
        portfolio_kpis['total_undercharge'] = abs(undercharge)  # Make positive for display
        portfolio_kpis['total_overcharge'] = overcharge
        portfolio_kpis['total_variance_abs'] = abs(portfolio_kpis['total_variance'])
        
        # Calculate property summary
        property_summary = calculate_property_summary(
            run_data["bucket_results"],
            run_data["findings"]
        )
        
        return render_template(
            'portfolio.html',
            run_id=run_id,
            metadata=run_data["metadata"],
            kpis=portfolio_kpis,
            properties=property_summary.to_dict('records')
        )
    except Exception as e:
        flash(f'Error loading run: {str(e)}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/property/<property_id>')
@bp.route('/property/<property_id>/<run_id>')
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
        
        # Get all unique leases for this property from expected_detail
        expected_detail = run_data["expected_detail"]
        property_expected = expected_detail[
            expected_detail[CanonicalField.PROPERTY_ID.value] == float(property_id)
        ]
        all_lease_ids = property_expected[CanonicalField.LEASE_INTERVAL_ID.value].unique()
        
        # Filter bucket results by property - only exceptions
        bucket_results = run_data["bucket_results"]
        property_buckets = bucket_results[
            (bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)) &
            (bucket_results[CanonicalField.STATUS.value] != config.reconciliation.status_matched)
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
        
        # Build comprehensive lease summary (including clean leases)
        lease_summary = []
        for lease_id in all_lease_ids:
            if lease_id in lease_groups:
                # Lease has exceptions
                exceptions = lease_groups[lease_id]
                total_variance = sum(abs(e['variance']) for e in exceptions)
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'has_exceptions': True,
                    'exception_count': len(exceptions),
                    'total_variance': total_variance,
                    'exceptions': sorted(exceptions, key=lambda x: abs(x['variance']), reverse=True)
                })
            else:
                # Clean lease - no exceptions
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'has_exceptions': False,
                    'exception_count': 0,
                    'total_variance': 0,
                    'exceptions': []
                })
        
        # Sort: exceptions first (by variance), then clean leases
        lease_summary = sorted(lease_summary, key=lambda x: (not x['has_exceptions'], -x['total_variance']))
        
        # Calculate property KPIs
        all_property_buckets = bucket_results[
            bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)
        ]
        property_kpis = calculate_kpis(
            all_property_buckets,
            run_data["findings"],
            property_id=None  # Already filtered, don't filter again
        )
        
        return render_template(
            'property.html',
            run_id=run_id,
            property_id=property_id,
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
    """Get Bootstrap color class for status."""
    colors = {
        "SCHEDULED_NOT_BILLED": "danger",
        "BILLED_NOT_SCHEDULED": "warning",
        "AMOUNT_MISMATCH": "info"
    }
    return colors.get(status, "secondary")


@bp.route('/lease/<run_id>/<property_id>/<lease_interval_id>')
def lease_view(run_id: str, property_id: str, lease_interval_id: str):
    """Lease view - detailed exceptions for a specific lease."""
    try:
        storage = get_storage_service()
        run_data = storage.load_run(run_id)
        
        # Get all buckets for this lease - only exceptions
        bucket_results = run_data["bucket_results"]
        lease_buckets = bucket_results[
            (bucket_results[CanonicalField.PROPERTY_ID.value] == float(property_id)) &
            (bucket_results[CanonicalField.LEASE_INTERVAL_ID.value] == float(lease_interval_id)) &
            (bucket_results[CanonicalField.STATUS.value] != config.reconciliation.status_matched)
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
                if 'PERIOD_END' in expected_records.columns:
                    charge_end = expected_records['PERIOD_END'].iloc[0]
            
            if not actual_records.empty:
                if 'POST_DATE' in actual_records.columns:
                    post_dates = actual_records['POST_DATE'].dropna().tolist()
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
                    
                    # Convert NaT to None for template compatibility
                    if pd.isna(period_start):
                        missing_dates_warning.append(f"Missing PERIOD_START for expected charge")
                        period_start = None
                    if pd.isna(period_end):
                        missing_dates_warning.append(f"Missing PERIOD_END for expected charge")
                        period_end = None
                    
                    expected_transactions.append({
                        'amount': exp_rec.get('expected_amount', 0),
                        'period_start': period_start,
                        'period_end': period_end,
                        'ar_code_name': exp_rec.get('AR_CODE_NAME', ar_code_name)
                    })
            
            if not actual_records.empty:
                for _, act_rec in actual_records.iterrows():
                    post_date = act_rec.get('POST_DATE')
                    
                    # Convert NaT to None for template compatibility
                    if pd.isna(post_date):
                        missing_dates_warning.append(f"Missing POST_DATE for actual transaction")
                        post_date = None
                    
                    actual_transactions.append({
                        'amount': act_rec.get('actual_amount', 0),
                        'post_date': post_date,
                        'ar_code_name': act_rec.get('AR_CODE_NAME', ar_code_name),
                        'transaction_id': act_rec.get('AR_TRANSACTION_ID')
                    })
            
            exception = {
                'ar_code_id': ar_code,
                'ar_code_name': ar_code_name,
                'audit_month': audit_month,
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
        
        # Sort by variance magnitude
        exceptions = sorted(exceptions, key=lambda x: abs(x['variance']), reverse=True)
        
        # Calculate lease totals
        total_variance = sum(abs(e['variance']) for e in exceptions)
        total_expected = sum(e['expected_total'] for e in exceptions)
        total_actual = sum(e['actual_total'] for e in exceptions)
        
        return render_template(
            'lease.html',
            run_id=run_id,
            property_id=property_id,
            lease_interval_id=lease_interval_id,
            metadata=run_data["metadata"],
            exceptions=exceptions,
            exception_count=len(exceptions),
            total_variance=total_variance,
            total_expected=total_expected,
            total_actual=total_actual
        )
    except Exception as e:
        import traceback
        print(f"[ERROR] Lease view error: {str(e)}")
        print(traceback.format_exc())
        flash(f'Error loading lease: {str(e)}', 'danger')
        return redirect(url_for('main.property_view', run_id=run_id, property_id=property_id))


@bp.route('/bucket/<run_id>/<property_id>/<lease_interval_id>/<ar_code_id>/<audit_month>')
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
