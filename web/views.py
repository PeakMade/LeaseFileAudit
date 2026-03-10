"""
Flask views for Lease File Audit application.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session, has_request_context
from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import logging
import json
from datetime import datetime
from time import perf_counter
import tempfile

from audit_engine import (
    ExcelSourceLoader,
    normalize_ar_transactions,
    normalize_scheduled_charges,
    expand_scheduled_to_months,
    reconcile_buckets,
    RuleContext,
    generate_findings,
    calculate_kpis,
    calculate_property_summary,
    build_lease_expectation_overlay,
    refresh_lease_terms_for_lease_interval,
)
from audit_engine.api_ingest import fetch_property_api_sources, fetch_entrata_property_picklist
from audit_engine.reconcile import reconcile_detail
from audit_engine.rules import default_registry
from audit_engine.canonical_fields import CanonicalField
from storage.service import StorageService
from config import config
from web.auth import require_auth, optional_auth, get_current_user, get_access_token
from activity_logging.sharepoint import log_user_activity
from extensions import cache
import os

logger = logging.getLogger(__name__)
bp = Blueprint('main', __name__)


def _session_cache_token() -> str:
    """Return a stable cache key segment that rotates with session lifecycle."""
    if not has_request_context():
        return "no-request"
    return session.get('session_id') or "no-session"


def _safe_seconds_from_iso(started_at_iso: str) -> float:
    if not started_at_iso:
        return 0.0
    try:
        started_at = datetime.fromisoformat(started_at_iso)
        return max(0.0, (datetime.utcnow() - started_at).total_seconds())
    except Exception:
        return 0.0


def _log_and_clear_pending_upload_timing(run_id: str, destination: str, destination_route_seconds: float) -> None:
    pending = session.get('pending_upload_timing')
    if not isinstance(pending, dict):
        return

    pending_run_id = str(pending.get('run_id') or '')
    if pending_run_id != str(run_id):
        return

    end_to_end_seconds = _safe_seconds_from_iso(str(pending.get('request_started_at_utc') or ''))
    logger.info(
        "[AUDIT TIMER][E2E] "
        f"run_id={run_id} "
        f"destination={destination} "
        f"upload_request_seconds={float(pending.get('upload_request_seconds') or 0):.2f} "
        f"file_save_seconds={float(pending.get('file_save_seconds') or 0):.2f} "
        f"api_fetch_seconds={float(pending.get('api_fetch_seconds') or 0):.2f} "
        f"execute_seconds={float(pending.get('execute_seconds') or 0):.2f} "
        f"overlay_seconds={float(pending.get('overlay_seconds') or 0):.2f} "
        f"save_run_seconds={float(pending.get('save_run_seconds') or 0):.2f} "
        f"cache_clear_seconds={float(pending.get('cache_clear_seconds') or 0):.2f} "
        f"cleanup_seconds={float(pending.get('cleanup_seconds') or 0):.2f} "
        f"activity_log_seconds={float(pending.get('activity_log_seconds') or 0):.2f} "
        f"post_upload_route_seconds={destination_route_seconds:.2f} "
        f"end_to_end_seconds={end_to_end_seconds:.2f}"
    )
    session.pop('pending_upload_timing', None)


def get_storage_service() -> StorageService:
    """Get storage service instance with SharePoint support."""
    access_token = get_access_token()
    sharepoint_site_url = config.auth.sharepoint_site_url
    
    return StorageService(
        base_dir=config.storage.base_dir,
        use_sharepoint=config.storage.is_sharepoint_configured(),
        sharepoint_site_url=sharepoint_site_url,
        library_name=config.storage.sharepoint_library_name,
        access_token=access_token
    )


@cache.memoize(timeout=14400)  # Cache for 4 hours per session
def cached_load_run(run_id: str, session_cache_key: str = None):
    """
    Cached wrapper for load_run() to avoid repeated CSV downloads.
    Cache key includes run_id automatically.
    """
    logger.info(f"[CACHE] ⏬ Cache MISS: Loading run {run_id} from storage")
    storage = get_storage_service()
    return storage.load_run(run_id)


@cache.memoize(timeout=14400)  # Cache for 4 hours per session
def cached_load_property_exception_months(run_id: str, property_id: int, session_cache_key: str = None):
    """
    Cached bulk fetch of all exception months for a property.
    Replaces hundreds of individual API calls with ONE cached result.
    """
    logger.info(f"[CACHE] ⏬ Cache MISS: Bulk loading exception months for property {property_id}")
    storage = get_storage_service()
    return storage.load_property_exception_months_bulk(run_id, property_id)


@cache.memoize(timeout=3600)
def cached_load_api_property_picklist(session_cache_key: str = None):
    """Cached Entrata properties picklist for API upload form."""
    logger.info("[CACHE] ⏬ Cache MISS: Loading Entrata property picklist")
    return fetch_entrata_property_picklist()


@cache.memoize(timeout=14400)
def cached_load_bucket_results(run_id: str, property_id=None, lease_interval_id=None, session_cache_key: str = None):
    """Cached wrapper for bucket results by run and optional scope."""
    logger.info(
        f"[CACHE] ⏬ Cache MISS: Loading bucket_results for run={run_id}, "
        f"property_id={property_id}, lease_interval_id={lease_interval_id}"
    )
    storage = get_storage_service()
    return storage.load_bucket_results(run_id, property_id=property_id, lease_interval_id=lease_interval_id)


@cache.memoize(timeout=14400)
def cached_load_findings(run_id: str, property_id=None, lease_interval_id=None, session_cache_key: str = None):
    """Cached wrapper for findings by run and optional scope."""
    logger.info(
        f"[CACHE] ⏬ Cache MISS: Loading findings for run={run_id}, "
        f"property_id={property_id}, lease_interval_id={lease_interval_id}"
    )
    storage = get_storage_service()
    return storage.load_findings(run_id, property_id=property_id, lease_interval_id=lease_interval_id)


@cache.memoize(timeout=14400)
def cached_load_actual_detail(run_id: str, session_cache_key: str = None):
    """Cached wrapper for actual_detail by run."""
    logger.info(f"[CACHE] ⏬ Cache MISS: Loading actual_detail for run={run_id}")
    storage = get_storage_service()
    return storage.load_actual_detail(run_id)


@cache.memoize(timeout=14400)
def cached_load_expected_detail(run_id: str, session_cache_key: str = None):
    """Cached wrapper for expected_detail by run."""
    logger.info(f"[CACHE] ⏬ Cache MISS: Loading expected_detail for run={run_id}")
    storage = get_storage_service()
    return storage.load_expected_detail(run_id)


@cache.memoize(timeout=14400)
def cached_load_metadata(run_id: str, session_cache_key: str = None):
    """Cached wrapper for run metadata by run."""
    logger.info(f"[CACHE] ⏬ Cache MISS: Loading metadata for run={run_id}")
    storage = get_storage_service()
    return storage.load_metadata(run_id)


@cache.memoize(timeout=14400)
def cached_load_run_display_snapshot(run_id: str, scope_type: str, property_id=None, lease_interval_id=None, session_cache_key: str = None):
    """Cached wrapper for run display snapshot lookup."""
    logger.info(
        f"[CACHE] ⏬ Cache MISS: Loading run display snapshot for run={run_id}, "
        f"scope={scope_type}, property_id={property_id}, lease_interval_id={lease_interval_id}"
    )
    storage = get_storage_service()
    return storage.load_run_display_snapshot_from_sharepoint_list(
        run_id=run_id,
        scope_type=scope_type,
        property_id=property_id,
        lease_interval_id=lease_interval_id
    )


@cache.memoize(timeout=14400)
def cached_load_run_display_snapshots_for_property(run_id: str, property_id: int, scope_type: str = 'lease', session_cache_key: str = None):
    """Cached wrapper for all snapshot rows for a property."""
    logger.info(
        f"[CACHE] ⏬ Cache MISS: Loading run display snapshots for run={run_id}, "
        f"property_id={property_id}, scope={scope_type}"
    )
    storage = get_storage_service()
    return storage.load_run_display_snapshots_for_property(
        run_id=run_id,
        property_id=property_id,
        scope_type=scope_type
    )


@cache.memoize(timeout=14400)
def cached_load_run_display_snapshots_for_run(run_id: str, scope_type: str = 'property', session_cache_key: str = None):
    """Cached wrapper for all snapshot rows for a run and scope."""
    logger.info(
        f"[CACHE] ⏬ Cache MISS: Loading run display snapshots for run={run_id}, "
        f"scope={scope_type}"
    )
    storage = get_storage_service()
    return storage.load_run_display_snapshots_for_run(
        run_id=run_id,
        scope_type=scope_type
    )


def _clean_property_name(value: object) -> str | None:
    """Normalize a property-name candidate to a safe non-empty string."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return None
    return text


def _add_property_name_from_row(target: dict[str, str], property_id_value: object, property_name_value: object) -> None:
    property_key = _normalize_property_id_token(property_id_value)
    property_name = _clean_property_name(property_name_value)
    if not property_key or not property_name:
        return
    target.setdefault(property_key, property_name)


def _add_property_names_from_df(target: dict[str, str], df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    property_id_candidates = [CanonicalField.PROPERTY_ID.value, 'property_id', 'PropertyId', 'Property ID']
    property_name_candidates = [CanonicalField.PROPERTY_NAME.value, 'property_name', 'PropertyName', 'Property Name']

    property_id_col = next((col for col in property_id_candidates if col in df.columns), None)
    property_name_col = next((col for col in property_name_candidates if col in df.columns), None)
    if not property_id_col or not property_name_col:
        return

    for _, row in df[[property_id_col, property_name_col]].dropna().iterrows():
        _add_property_name_from_row(target, row.get(property_id_col), row.get(property_name_col))


@cache.memoize(timeout=14400)
def cached_load_property_name_lookup(run_id: str, session_cache_key: str = None) -> dict[str, str]:
    """Build a run-scoped property id -> property name lookup with normalized keys."""
    lookup: dict[str, str] = {}

    try:
        metadata = cached_load_metadata(run_id, session_cache_key)
        if isinstance(metadata, dict):
            _add_property_name_from_row(
                lookup,
                metadata.get('property_id'),
                metadata.get('property_name'),
            )
    except Exception:
        pass

    try:
        for snapshot_row in cached_load_run_display_snapshots_for_run(
            run_id=run_id,
            scope_type='property',
            session_cache_key=session_cache_key,
        ):
            if not isinstance(snapshot_row, dict):
                continue
            _add_property_name_from_row(
                lookup,
                snapshot_row.get('property_id'),
                snapshot_row.get('property_name'),
            )
    except Exception:
        pass

    try:
        _add_property_names_from_df(lookup, cached_load_actual_detail(run_id, session_cache_key))
    except Exception:
        pass

    try:
        _add_property_names_from_df(lookup, cached_load_expected_detail(run_id, session_cache_key))
    except Exception:
        pass

    return lookup


def _resolve_property_name_for_run(run_id: str, property_id: object, session_cache_key: str = None) -> str:
    """Resolve display property name for a run and property id with safe fallback."""
    property_key = _normalize_property_id_token(property_id)
    fallback = f"Property {property_key or property_id}"
    if not property_key:
        return fallback

    try:
        lookup = cached_load_property_name_lookup(run_id, session_cache_key)
        resolved = _clean_property_name((lookup or {}).get(property_key))
        if resolved:
            return resolved
    except Exception:
        pass
    return fallback


def _clear_run_scoped_caches(run_id: str, property_id=None, lease_interval_id=None):
    """Clear memoized caches for a specific run and optional property/lease scope."""
    session_cache_key = _session_cache_token()

    def _safe_delete(func, *args):
        try:
            cache.delete_memoized(func, *args)
        except KeyError as e:
            logger.warning(f"[CACHE] delete_memoized KeyError for {func.__name__}{args}: {e}")
        except Exception as e:
            logger.warning(f"[CACHE] delete_memoized failed for {func.__name__}{args}: {e}")

    _safe_delete(cached_load_run, run_id, session_cache_key)
    _safe_delete(cached_load_bucket_results, run_id, None, None, session_cache_key)
    _safe_delete(cached_load_findings, run_id, None, None, session_cache_key)
    _safe_delete(cached_load_actual_detail, run_id, session_cache_key)
    _safe_delete(cached_load_expected_detail, run_id, session_cache_key)
    _safe_delete(cached_load_metadata, run_id, session_cache_key)
    _safe_delete(cached_load_property_name_lookup, run_id, session_cache_key)
    _safe_delete(cached_load_run_display_snapshot, run_id, 'portfolio', None, None, session_cache_key)
    _safe_delete(cached_load_run_display_snapshots_for_run, run_id, 'property', session_cache_key)
    _safe_delete(cached_load_run_display_snapshots_for_run, run_id, 'lease', session_cache_key)

    if property_id is not None:
        property_id_int = int(float(property_id))
        _safe_delete(cached_load_bucket_results, run_id, property_id_int, None, session_cache_key)
        _safe_delete(cached_load_findings, run_id, property_id_int, None, session_cache_key)
        _safe_delete(cached_load_property_exception_months, run_id, property_id_int, session_cache_key)
        _safe_delete(cached_load_run_display_snapshot, run_id, 'property', property_id_int, None, session_cache_key)
        _safe_delete(cached_load_run_display_snapshots_for_property, run_id, property_id_int, 'lease', session_cache_key)

        if lease_interval_id is not None:
            lease_interval_id_int = int(float(lease_interval_id))
            _safe_delete(cached_load_bucket_results, run_id, property_id_int, lease_interval_id_int, session_cache_key)
            _safe_delete(cached_load_run_display_snapshot, run_id, 'lease', property_id_int, lease_interval_id_int, session_cache_key)

    _safe_delete(calculate_cumulative_metrics)


def _normalize_key_value(value, cast_type=str):
    """Normalize key values for consistent tuple matching across CSV and SharePoint sources."""
    if value is None:
        return ""
    if cast_type is int:
        return int(float(value))
    if cast_type is str:
        return str(value)
    return value


def _normalize_audit_month(value):
    """Normalize audit month to YYYY-MM for key comparisons."""
    if value is None:
        return ''
    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m')
    try:
        parsed = pd.to_datetime(value, errors='coerce')
        if not pd.isna(parsed):
            return parsed.strftime('%Y-%m')
    except Exception:
        pass
    text = str(value).strip()
    if len(text) >= 7:
        return text[:7]
    return text


def _normalize_property_id_token(value):
    """Normalize property id-like values to stable string keys (e.g. 1001.0 -> '1001')."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (int, float)):
        numeric = float(value)
        if pd.isna(numeric):
            return None
        return str(int(numeric)) if numeric.is_integer() else str(numeric)

    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
        if pd.isna(numeric):
            return None
        return str(int(numeric)) if numeric.is_integer() else str(numeric)
    except Exception:
        return text


def _filter_df_to_property_scope(df: pd.DataFrame, property_scope: set) -> pd.DataFrame:
    """Filter DataFrame rows to scoped property ids using normalized key matching."""
    if df is None or df.empty or not property_scope:
        return df

    prop_col = CanonicalField.PROPERTY_ID.value
    if prop_col not in df.columns:
        return df.iloc[0:0].copy()

    normalized = df[prop_col].apply(_normalize_property_id_token)
    return df[normalized.isin(property_scope)].copy()


def _overlay_property_scope_results(scoped_results: dict, baseline_run_data: dict, property_scope: set) -> dict:
    """Overlay scoped property outputs onto baseline run outputs to preserve full portfolio coverage."""
    if not baseline_run_data or not property_scope:
        return scoped_results

    def _merge_dataset(scoped_df: pd.DataFrame, baseline_df: pd.DataFrame, property_col_candidates) -> pd.DataFrame:
        if baseline_df is None or baseline_df.empty:
            return scoped_df.copy() if scoped_df is not None else pd.DataFrame()
        if scoped_df is None:
            scoped_df = pd.DataFrame()

        property_col = next((col for col in property_col_candidates if col in baseline_df.columns), None)
        if not property_col:
            return scoped_df.copy() if not scoped_df.empty else baseline_df.copy()

        baseline_filtered = baseline_df[
            ~baseline_df[property_col].apply(_normalize_property_id_token).isin(property_scope)
        ].copy()

        if scoped_df.empty:
            return baseline_filtered

        return pd.concat([baseline_filtered, scoped_df], ignore_index=True)

    merged = dict(scoped_results)
    merged['expected_detail'] = _merge_dataset(
        scoped_results.get('expected_detail', pd.DataFrame()),
        baseline_run_data.get('expected_detail', pd.DataFrame()),
        [CanonicalField.PROPERTY_ID.value, 'property_id']
    )
    merged['actual_detail'] = _merge_dataset(
        scoped_results.get('actual_detail', pd.DataFrame()),
        baseline_run_data.get('actual_detail', pd.DataFrame()),
        [CanonicalField.PROPERTY_ID.value, 'property_id']
    )
    merged['bucket_results'] = _merge_dataset(
        scoped_results.get('bucket_results', pd.DataFrame()),
        baseline_run_data.get('bucket_results', pd.DataFrame()),
        [CanonicalField.PROPERTY_ID.value, 'property_id']
    )
    merged['findings'] = _merge_dataset(
        scoped_results.get('findings', pd.DataFrame()),
        baseline_run_data.get('findings', pd.DataFrame()),
        ['property_id', CanonicalField.PROPERTY_ID.value]
    )

    # Keep variance_detail scoped (no stable PROPERTY_ID column guaranteed in this dataset).
    merged['variance_detail'] = scoped_results.get('variance_detail', pd.DataFrame())
    
    # Merge property_name_map: baseline properties + scoped property (scoped overrides on conflict)
    merged_property_name_map = {}
    if baseline_run_data.get('property_name_map'):
        logger.info(f"[OVERLAY] Baseline property_name_map: {baseline_run_data['property_name_map']}")
        merged_property_name_map.update(baseline_run_data['property_name_map'])
    if scoped_results.get('property_name_map'):
        logger.info(f"[OVERLAY] Scoped property_name_map: {scoped_results['property_name_map']}")
        merged_property_name_map.update(scoped_results['property_name_map'])
    if merged_property_name_map:
        logger.info(f"[OVERLAY] Merged property_name_map: {merged_property_name_map}")
        merged['property_name_map'] = merged_property_name_map
    
    # Log property IDs in merged bucket_results
    if not merged['bucket_results'].empty:
        property_col = next((col for col in [CanonicalField.PROPERTY_ID.value, 'property_id'] if col in merged['bucket_results'].columns), None)
        if property_col:
            unique_properties = merged['bucket_results'][property_col].unique()
            logger.info(f"[OVERLAY] Properties in merged bucket_results: {unique_properties}")

    merged['property_summary'] = calculate_property_summary(
        merged['bucket_results'],
        merged['findings'],
        merged['actual_detail']
    )
    merged['portfolio_totals'] = calculate_kpis(merged['bucket_results'], merged['findings'])
    return merged


def _build_resolved_key(property_id, lease_id, ar_code_id, audit_month):
    """Create normalized key for resolved month matching (portfolio/metrics)."""
    return (
        _normalize_key_value(property_id, int),
        _normalize_key_value(lease_id, int),
        _normalize_key_value(ar_code_id, str),
        _normalize_audit_month(audit_month)
    )


def _build_property_resolved_key(lease_id, ar_code_id, audit_month):
    """Create normalized key for property-level resolved month matching."""
    return (
        _normalize_key_value(lease_id, int),
        _normalize_key_value(ar_code_id, str),
        _normalize_audit_month(audit_month)
    )


def _calculate_scoped_ar_status(
    storage: StorageService,
    run_id: str,
    property_id: int,
    lease_interval_id: int,
    ar_code_id: str,
) -> dict:
    """Calculate AR status for current run scope only, excluding historical-resolved carry-forward months."""
    bucket_results = storage.load_bucket_results(
        run_id,
        property_id=int(property_id),
        lease_interval_id=int(lease_interval_id)
    )

    if bucket_results is None or bucket_results.empty:
        return {
            'status': 'Passed',
            'total_months': 0,
            'resolved_months': 0,
            'open_months': 0,
            'status_label': 'Passed'
        }

    status_col = CanonicalField.STATUS.value
    ar_col = CanonicalField.AR_CODE_ID.value
    month_col = CanonicalField.AUDIT_MONTH.value

    def _norm_ar(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ''
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric.is_integer():
                return str(int(numeric))
            return str(numeric)
        text = str(value).strip()
        try:
            numeric = float(text)
            if numeric.is_integer():
                return str(int(numeric))
        except Exception:
            pass
        return text

    ar_code_normalized = _norm_ar(ar_code_id)

    exception_rows = bucket_results[
        (bucket_results[status_col] != config.reconciliation.status_matched)
        & (bucket_results[ar_col].apply(_norm_ar) == ar_code_normalized)
    ].copy()

    if exception_rows.empty:
        return {
            'status': 'Passed',
            'total_months': 0,
            'resolved_months': 0,
            'open_months': 0,
            'status_label': 'Passed'
        }

    state_rows = storage.load_exception_months_from_sharepoint_list(
        run_id,
        int(property_id),
        int(lease_interval_id),
        ar_code_normalized
    )
    state_by_month = {}
    for row in state_rows:
        state_by_month[_normalize_audit_month(row.get('audit_month'))] = row

    scoped_month_states = []
    for _, exception_row in exception_rows.iterrows():
        month_key = _normalize_audit_month(exception_row.get(month_col))
        month_state = state_by_month.get(month_key)

        is_historical_resolved = bool(
            month_state
            and str(month_state.get('status', '')).strip().lower() == 'resolved'
            and str(month_state.get('run_id', '')).strip() != str(run_id)
        )
        if is_historical_resolved:
            continue

        month_status = 'Open'
        if month_state and str(month_state.get('status', '')).strip().lower() == 'resolved':
            month_status = 'Resolved'
        scoped_month_states.append(month_status)

    total_months = len(scoped_month_states)
    resolved_months = sum(1 for state in scoped_month_states if state == 'Resolved')
    open_months = total_months - resolved_months

    if total_months == 0:
        return {
            'status': 'Passed',
            'total_months': 0,
            'resolved_months': 0,
            'open_months': 0,
            'status_label': 'Passed'
        }

    if open_months == 0:
        return {
            'status': 'Resolved',
            'total_months': total_months,
            'resolved_months': resolved_months,
            'open_months': 0,
            'status_label': 'Resolved'
        }

    status_label = 'Open'
    if resolved_months > 0:
        status_label = f"Open ({resolved_months} of {total_months} resolved)"

    return {
        'status': 'Open',
        'total_months': total_months,
        'resolved_months': resolved_months,
        'open_months': open_months,
        'status_label': status_label
    }


def clear_run_cache(run_id: str = None):
    """
    Clear cached data, optionally for a specific run.
    Call this after uploads or status updates.
    """
    if run_id:
        logger.info(f"[CACHE] 🧹 Clearing cache for run {run_id}")
        _clear_run_scoped_caches(run_id)
        logger.info(f"[CACHE] ✅ Cleared run-scoped cache entries for run {run_id}")
        return
    else:
        logger.info(f"[CACHE] 🧹 Clearing ALL caches")

    try:
        cache.clear()
        logger.info("[CACHE] ✅ Cleared via cache.clear()")
        return
    except KeyError as e:
        logger.warning(f"[CACHE] cache.clear() KeyError; attempting extension-level clear fallback: {e}")
    except Exception as e:
        logger.warning(f"[CACHE] cache.clear() failed; attempting extension-level clear fallback: {e}")

    try:
        cache_extension = current_app.extensions.get('cache', {}) if current_app else {}
        if not cache_extension:
            logger.warning("[CACHE] No Flask cache extension map found; skipping cache clear")
            return

        backends_cleared = 0
        for backend in cache_extension.values():
            try:
                backend.clear()
                backends_cleared += 1
            except Exception as backend_error:
                logger.warning(f"[CACHE] Failed clearing backend from extension map: {backend_error}")

        logger.info(f"[CACHE] ✅ Fallback clear completed; backends_cleared={backends_cleared}")
    except Exception as e:
        logger.warning(f"[CACHE] Fallback clear failed; continuing without hard failure: {e}")


@cache.memoize(timeout=43200)  # Cache run list for 12 hours per session
def get_available_runs(session_cache_key: str = None) -> list:
    """Get all available runs sorted by date (most recent first)."""
    logger.info("[CACHE] ⏬ Cache MISS: Loading available runs list from SharePoint")
    storage = get_storage_service()
    runs = storage.list_runs(limit=50)  # Only load 50 most recent runs
    
    # Format runs for dropdown
    formatted_runs = []
    for run in runs:
        # Determine if manual or auto based on metadata
        # For now, all runs are manual (uploaded via UI)
        # Future: check run metadata for 'run_type' field
        run_type = run.get('run_type', 'Manual')
        
        run_info = {
            'run_id': run['run_id'],
            'timestamp': run.get('timestamp', 'Unknown'),
            'audit_period': run.get('audit_period', {}),
            'run_type': run_type
        }
        formatted_runs.append(run_info)
    
    logger.info(f"[CACHE] ✅ Loaded {len(formatted_runs)} available runs")
    return formatted_runs


@cache.memoize(timeout=3600)  # Cache latest run lookup for 1 hour per session
def get_latest_run(session_cache_key: str = None) -> dict:
    """Get most recent run metadata without loading full run history."""
    logger.info("[CACHE] ⏬ Cache MISS: Loading latest run metadata")
    storage = get_storage_service()
    runs = storage.list_runs(limit=1)
    if not runs:
        return {}

    run = runs[0]
    return {
        'run_id': run.get('run_id'),
        'timestamp': run.get('timestamp', 'Unknown'),
        'audit_period': run.get('audit_period', {}),
        'run_type': run.get('run_type', 'Manual')
    }


def invalidate_runs_cache() -> None:
    """Invalidate run-picker caches. Call only when runs are added/removed."""
    cache_token = _session_cache_token()
    try:
        cache.delete_memoized(get_available_runs, cache_token)
    except Exception as e:
        logger.warning(f"[CACHE] Failed to clear available runs cache: {e}")
    try:
        cache.delete_memoized(get_latest_run, cache_token)
    except Exception as e:
        logger.warning(f"[CACHE] Failed to clear latest run cache: {e}")


@bp.route('/api/runs', methods=['GET'])
@require_auth
def get_available_runs_api():
    """Return available runs for lazy-loaded run pickers."""
    runs = get_available_runs(_session_cache_token())
    return jsonify({'runs': runs})


@cache.memoize(timeout=3600)  # Cache metrics for 1 hour per session
def calculate_cumulative_metrics(session_cache_key: str = None) -> dict:
    """Calculate cumulative portfolio metrics across all audit runs."""
    logger.info("[CACHE] ⏬ Cache MISS: Calculating cumulative metrics")
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
        # Load most recent run's bucket_results for detailed calculation
        most_recent_run_id = most_recent_metrics['run_id']
        try:
            latest_data = storage.load_run(most_recent_run_id)
            latest_buckets = latest_data['bucket_results']
            
            logger.info(f"[METRICS] Loaded bucket_results with {len(latest_buckets)} rows")
            
            current_exceptions = latest_buckets[
                latest_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
            ]
            
            # 🚀 BULK FETCH: Get resolved exceptions for all properties at once
            resolved_keys = set()
            resolved_exceptions_data = []  # Track variance data for historical calculation
            unique_properties = current_exceptions[CanonicalField.PROPERTY_ID.value].unique()
            
            logger.info(f"[METRICS] Bulk fetching resolved exceptions for {len(unique_properties)} properties")
            
            # Bulk fetch all exception months for all properties
            for property_id in unique_properties:
                # Use cached bulk fetch
                bulk_exception_data = cached_load_property_exception_months(
                    most_recent_run_id, int(float(property_id)), session_cache_key
                )
                
                # Process all resolved exceptions from bulk data
                for (lease_id, ar_code_id), month_records in bulk_exception_data.items():
                    for month_record in month_records:
                        if month_record.get('status') == 'Resolved':
                            audit_month = month_record.get('audit_month')
                            if isinstance(audit_month, str):
                                audit_month = audit_month[:10]
                            
                            # Find the matching bucket to get variance
                            property_exceptions = current_exceptions[
                                current_exceptions[CanonicalField.PROPERTY_ID.value] == property_id
                            ]
                            matching_bucket = property_exceptions[
                                (property_exceptions[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id) &
                                (property_exceptions[CanonicalField.AR_CODE_ID.value] == ar_code_id) &
                                (property_exceptions[CanonicalField.AUDIT_MONTH.value].astype(str).str[:10] == audit_month)
                            ]
                            
                            if not matching_bucket.empty:
                                variance = matching_bucket.iloc[0][CanonicalField.VARIANCE.value]
                                resolved_exceptions_data.append({
                                    'variance': variance,
                                    'audit_month': audit_month
                                })
                                logger.debug(f"[METRICS] Found resolved exception: AR {ar_code_id}, Month {audit_month}, Variance ${variance}")
                            
                            resolved_key = _build_resolved_key(property_id, lease_id, ar_code_id, audit_month)
                            resolved_keys.add(resolved_key)
            
            logger.info(f"[METRICS] Found {len(resolved_keys)} resolved exception months to filter out")
            logger.info(f"[METRICS] Collected {len(resolved_exceptions_data)} resolved exceptions for historical calculation")
            
            def is_unresolved_bucket(row):
                if row[CanonicalField.STATUS.value] == config.reconciliation.status_matched:
                    return False
                
                key = _build_resolved_key(
                    row[CanonicalField.PROPERTY_ID.value],
                    row[CanonicalField.LEASE_INTERVAL_ID.value],
                    row[CanonicalField.AR_CODE_ID.value],
                    row[CanonicalField.AUDIT_MONTH.value]
                )
                return key not in resolved_keys
            
            current_exceptions = current_exceptions[current_exceptions.apply(is_unresolved_bucket, axis=1)]
            
            logger.info(f"[METRICS] Found {len(current_exceptions)} unresolved exception rows")
            
            variances = current_exceptions[CanonicalField.VARIANCE.value]
            current_undercharge = variances[variances < 0].abs().sum()
            current_overcharge = variances[variances > 0].sum()
            
            logger.info(f"[METRICS] Calculated undercharge=${current_undercharge}, overcharge=${current_overcharge}")
            
            total_leases_audited = latest_buckets[CanonicalField.LEASE_INTERVAL_ID.value].nunique()
            open_exceptions_count = len(current_exceptions)
            total_buckets = len(latest_buckets)
            matched = total_buckets - open_exceptions_count
            match_rate = (matched / total_buckets * 100) if total_buckets > 0 else 0
        except Exception as e:
            logger.warning(f"[METRICS] Error loading most recent run details: {e}")
            # Fall back to approximations
            current_undercharge = 0
            current_overcharge = 0
            total_leases_audited = 0
            open_exceptions_count = int(current_variances)
            resolved_exceptions_data = []  # No resolved data available
        
        # Historical metrics - sum across all runs (not deduplicated, but fast)
        # This is an approximation - true deduplication would require loading all CSVs
        total_historical_variances = sum(m['total_variances'] for m in all_metrics)
        total_historical_high_severity = sum(m['high_severity'] for m in all_metrics)
        
        # Simplified recovery calculation - compare current vs. historical averages
        avg_variances_per_run = total_historical_variances / len(all_metrics) if all_metrics else 0
        money_recovered = max(0, avg_variances_per_run - current_variances) * 100  # Rough estimate
        
        current_net_variance = current_overcharge - current_undercharge
        
        # Calculate historical undercharge/overcharge from resolved exceptions
        historical_undercharge = sum(abs(exc['variance']) for exc in resolved_exceptions_data if exc['variance'] < 0)
        historical_overcharge = sum(exc['variance'] for exc in resolved_exceptions_data if exc['variance'] > 0)
        
        logger.info(f"[METRICS] Historical undercharge=${historical_undercharge}, overcharge=${historical_overcharge} (from {len(resolved_exceptions_data)} resolved exceptions)")
        
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
    most_recent_run_id = most_recent['run_id']
    latest_data = storage.load_run(most_recent_run_id)
    latest_buckets = latest_data['bucket_results']
    
    # Current state from most recent audit
    current_exceptions = latest_buckets[
        latest_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
    ]
    
    # Filter out resolved exceptions to match fast path behavior
    resolved_keys = set()
    resolved_exceptions_data = []  # Track variance data for historical calculation
    unique_properties = current_exceptions[CanonicalField.PROPERTY_ID.value].unique()
    
    logger.info(f"[METRICS-SLOW] Checking {len(unique_properties)} properties for resolved exceptions")
    
    for property_id in unique_properties:
        property_exceptions = current_exceptions[
            current_exceptions[CanonicalField.PROPERTY_ID.value] == property_id
        ]
        unique_lease_ids = property_exceptions[CanonicalField.LEASE_INTERVAL_ID.value].unique()
        
        for lease_id in unique_lease_ids:
            lease_exceptions = property_exceptions[
                property_exceptions[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id
            ]
            unique_ar_codes = lease_exceptions[CanonicalField.AR_CODE_ID.value].unique()
            
            for ar_code_id in unique_ar_codes:
                exception_months = storage.load_exception_months_from_sharepoint_list(
                    most_recent_run_id, int(float(property_id)), int(float(lease_id)), ar_code_id
                )
                
                logger.debug(f"[METRICS-SLOW] Property {property_id}, Lease {lease_id}, AR {ar_code_id}: {len(exception_months)} months from SharePoint")
                
                for month_record in exception_months:
                    if month_record.get('status') == 'Resolved':
                        audit_month = month_record.get('audit_month')
                        if isinstance(audit_month, str):
                            audit_month = audit_month[:10]
                        
                        # Find the matching bucket to get variance
                        matching_bucket = lease_exceptions[
                            (lease_exceptions[CanonicalField.AR_CODE_ID.value] == ar_code_id) &
                            (lease_exceptions[CanonicalField.AUDIT_MONTH.value].astype(str).str[:10] == audit_month)
                        ]
                        
                        if not matching_bucket.empty:
                            variance = matching_bucket.iloc[0][CanonicalField.VARIANCE.value]
                            resolved_exceptions_data.append({
                                'variance': variance,
                                'audit_month': audit_month
                            })
                            logger.debug(f"[METRICS-SLOW] Found resolved exception: AR {ar_code_id}, Month {audit_month}, Variance ${variance}")
                        
                        resolved_key = _build_resolved_key(property_id, lease_id, ar_code_id, audit_month)
                        resolved_keys.add(resolved_key)
    
    logger.info(f"[METRICS-SLOW] Found {len(resolved_keys)} resolved exception months to filter out")
    logger.info(f"[METRICS-SLOW] Collected {len(resolved_exceptions_data)} resolved exceptions for historical calculation")
    
    def is_unresolved_bucket(row):
        if row[CanonicalField.STATUS.value] == config.reconciliation.status_matched:
            return False
        
        key = _build_resolved_key(
            row[CanonicalField.PROPERTY_ID.value],
            row[CanonicalField.LEASE_INTERVAL_ID.value],
            row[CanonicalField.AR_CODE_ID.value],
            row[CanonicalField.AUDIT_MONTH.value]
        )
        return key not in resolved_keys
    
    current_exceptions = current_exceptions[current_exceptions.apply(is_unresolved_bucket, axis=1)]
    
    logger.info(f"[METRICS-SLOW] Found {len(current_exceptions)} unresolved exception rows")
    
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
    
    logger.info(f"[METRICS-SLOW] Calculated undercharge=${current_undercharge}, overcharge=${current_overcharge}")
    
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
    # Also include resolved exceptions from current run
    historical_undercharge = sum(abs(exc['variance']) for exc in all_exception_data if exc['variance'] < 0)
    historical_undercharge += sum(abs(exc['variance']) for exc in resolved_exceptions_data if exc['variance'] < 0)
    
    historical_overcharge = sum(exc['variance'] for exc in all_exception_data if exc['variance'] > 0)
    historical_overcharge += sum(exc['variance'] for exc in resolved_exceptions_data if exc['variance'] > 0)
    
    logger.info(f"[METRICS-SLOW] Historical undercharge=${historical_undercharge}, overcharge=${historical_overcharge} (from {len(all_exception_data)} historical + {len(resolved_exceptions_data)} resolved exceptions)")
    
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


def filter_to_current_academic_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter a DataFrame to current academic year-to-date.

    Academic year starts in August and runs through the current month.
    Example (Feb 2026): range is 2025-08 through 2026-02.
    """
    from audit_engine.canonical_fields import CanonicalField

    if CanonicalField.AUDIT_MONTH.value not in df.columns:
        raise ValueError(f"DataFrame missing required column: {CanonicalField.AUDIT_MONTH.value}")

    result = df.copy()
    result = result[result[CanonicalField.AUDIT_MONTH.value].notna()]

    current_month = pd.Timestamp.now().to_period('M').to_timestamp()
    academic_start_year = current_month.year if current_month.month >= 8 else current_month.year - 1
    academic_start = pd.Timestamp(year=academic_start_year, month=8, day=1)

    result = result[
        (result[CanonicalField.AUDIT_MONTH.value] >= academic_start) &
        (result[CanonicalField.AUDIT_MONTH.value] <= current_month)
    ]

    print(
        f"[AUDIT PERIOD FILTER] Current Academic Year applied: "
        f"{academic_start.strftime('%Y-%m')} through {current_month.strftime('%Y-%m')} "
        f"({len(result)} rows)"
    )
    return result


def _resolve_audit_window_bounds(audit_year: int = None, audit_month: int = None) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Resolve inclusive date bounds for audit window filtering."""
    if audit_year is None:
        current_month = pd.Timestamp.now().to_period('M').to_timestamp()
        academic_start_year = current_month.year if current_month.month >= 8 else current_month.year - 1
        start = pd.Timestamp(year=academic_start_year, month=8, day=1)
        end = current_month + pd.offsets.MonthEnd(0)
        return start, end

    if audit_month is not None:
        month_start = pd.Timestamp(year=int(audit_year), month=int(audit_month), day=1)
        month_end = month_start + pd.offsets.MonthEnd(0)
        return month_start, month_end

    year_start = pd.Timestamp(year=int(audit_year), month=1, day=1)
    year_end = pd.Timestamp(year=int(audit_year), month=12, day=31)
    return year_start, year_end


def _normalize_raw_date_series(series: pd.Series) -> pd.Series:
    """
    Normalize mixed raw date values with deterministic numeric handling.

    Numeric rules:
    - 20000101..20991231 => YYYYMMDD
    - >1e12 => epoch milliseconds
    - >1e9 => epoch seconds
    - otherwise => invalid (NaT)
    """
    normalized = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')
    non_null = series[series.notna()]
    if non_null.empty:
        return normalized

    numeric_values = pd.to_numeric(non_null, errors='coerce')

    yyyymmdd_mask = numeric_values.between(20000101, 20991231)
    if yyyymmdd_mask.any():
        yyyymmdd_series = numeric_values[yyyymmdd_mask].astype('Int64').astype(str)
        normalized.loc[yyyymmdd_series.index] = pd.to_datetime(yyyymmdd_series, format='%Y%m%d', errors='coerce')

    epoch_ms_mask = numeric_values > 1_000_000_000_000
    if epoch_ms_mask.any():
        epoch_ms_series = numeric_values[epoch_ms_mask]
        normalized.loc[epoch_ms_series.index] = pd.to_datetime(epoch_ms_series, unit='ms', errors='coerce')

    epoch_s_mask = (numeric_values > 1_000_000_000) & ~epoch_ms_mask
    if epoch_s_mask.any():
        epoch_s_series = numeric_values[epoch_s_mask]
        normalized.loc[epoch_s_series.index] = pd.to_datetime(epoch_s_series, unit='s', errors='coerce')

    # Parse non-numeric strings/datetimes only (never let pandas infer numeric values)
    unresolved_index = normalized.loc[non_null.index][normalized.loc[non_null.index].isna()].index
    if len(unresolved_index) > 0:
        unresolved_values = non_null.loc[unresolved_index]
        unresolved_numeric = pd.to_numeric(unresolved_values, errors='coerce')
        non_numeric_values = unresolved_values[unresolved_numeric.isna()]
        if len(non_numeric_values) > 0:
            normalized.loc[non_numeric_values.index] = pd.to_datetime(non_numeric_values, errors='coerce')

    return normalized


def execute_audit_run(
    file_path: Path = None,
    run_id: str = "",
    audit_year: int = None,
    audit_month: int = None,
    scoped_property_ids=None,
    preloaded_sources: dict = None,
) -> dict:
    """
    Execute complete audit run.
    
    Args:
        file_path: Path to uploaded Excel file
        run_id: Unique run identifier
        audit_year: Optional year to filter audit (e.g., 2024)
        audit_month: Optional month to filter audit (1-12)
        preloaded_sources: Optional pre-loaded RAW source dict keyed by configured source names
    
    Returns:
        Dict with all results and metadata
    """
    # Load RAW data sources from Excel, or use provided in-memory sources.
    from audit_engine.mappings import (
        apply_source_mapping,
        AR_TRANSACTIONS_MAPPING,
        SCHEDULED_CHARGES_MAPPING,
        ARSourceColumns,
        ScheduledSourceColumns,
    )

    if preloaded_sources is not None:
        sources = preloaded_sources
    else:
        if file_path is None:
            raise ValueError("file_path is required when preloaded_sources is not provided")
        from audit_engine.io import load_excel_sources
        sources = load_excel_sources(file_path, config.ar_source, config.scheduled_source)

    if config.ar_source.name not in sources or config.scheduled_source.name not in sources:
        raise ValueError(
            "Missing required source tabs for execution: "
            f"{config.ar_source.name} and/or {config.scheduled_source.name}"
        )

    early_prefilter_enabled = os.getenv('EARLY_AUDIT_WINDOW_PREFILTER', 'false').lower() == 'true'
    if early_prefilter_enabled:
        window_start, window_end = _resolve_audit_window_bounds(audit_year=audit_year, audit_month=audit_month)
        print(
            f"[EARLY WINDOW FILTER] Enabled with canonical bounds: "
            f"{window_start.strftime('%Y-%m-%d')} through {window_end.strftime('%Y-%m-%d')}"
        )

        sources = dict(sources)

        raw_ar_df = sources.get(config.ar_source.name)
        if isinstance(raw_ar_df, pd.DataFrame) and not raw_ar_df.empty:
            ar_date_column = None
            for candidate in [ARSourceColumns.POST_DATE, ARSourceColumns.POST_MONTH_DATE]:
                if candidate in raw_ar_df.columns:
                    ar_date_column = candidate
                    break

            if ar_date_column:
                ar_dates = _normalize_raw_date_series(raw_ar_df[ar_date_column])
                invalid_ar_dates = int(ar_dates.isna().sum())

                # Conservative safety rule: keep rows with invalid dates; only drop proven out-of-window rows.
                in_window_mask = ar_dates.isna() | ((ar_dates >= window_start) & (ar_dates <= window_end))
                before_count = len(raw_ar_df)
                after_df = raw_ar_df[in_window_mask].copy()
                sources[config.ar_source.name] = after_df
                print(
                    f"[EARLY WINDOW FILTER] AR source: {before_count} -> {len(after_df)} rows "
                    f"(invalid_dates_kept={invalid_ar_dates}, date_col={ar_date_column})"
                )

        raw_sched_df = sources.get(config.scheduled_source.name)
        if isinstance(raw_sched_df, pd.DataFrame) and not raw_sched_df.empty:
            start_col = ScheduledSourceColumns.CHARGE_START_DATE
            end_col = ScheduledSourceColumns.CHARGE_END_DATE
            if start_col in raw_sched_df.columns:
                sched_start = _normalize_raw_date_series(raw_sched_df[start_col])
                sched_end = _normalize_raw_date_series(raw_sched_df[end_col]) if end_col in raw_sched_df.columns else pd.Series(pd.NaT, index=raw_sched_df.index, dtype='datetime64[ns]')
                sched_end = sched_end.where(sched_end.notna(), sched_start)

                invalid_sched_dates = int(sched_start.isna().sum())
                overlap_mask = (
                    sched_start.isna()
                    | sched_end.isna()
                    | (
                        (sched_start <= window_end)
                        & (sched_end >= window_start)
                    )
                )
                before_count = len(raw_sched_df)
                after_df = raw_sched_df[overlap_mask].copy()
                sources[config.scheduled_source.name] = after_df
                print(
                    f"[EARLY WINDOW FILTER] Scheduled source: {before_count} -> {len(after_df)} rows "
                    f"(invalid_start_dates_kept={invalid_sched_dates})"
                )

    print(f"\n[EXECUTE_AUDIT_RUN] Loaded raw sources:")
    print(f"  AR Transactions: {sources[config.ar_source.name].shape}")
    print(f"  Scheduled Charges: {sources[config.scheduled_source.name].shape}")

    # Build property name lookup from original uploaded source tabs (authoritative first-pass source).
    # This is passed through to snapshot persistence so portfolio names are stored correctly at write time.
    def _safe_int_property_id(value):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        try:
            return int(float(value))
        except Exception:
            return None

    def _extract_property_names_from_raw(df: pd.DataFrame, target_map: dict) -> None:
        if df is None or df.empty:
            return

        property_id_candidates = ['PROPERTY_ID', 'property_id', 'PropertyId', 'Property ID']
        property_name_candidates = ['PROPERTY_NAME', 'property_name', 'PropertyName', 'Property Name']

        property_id_column = next((col for col in property_id_candidates if col in df.columns), None)
        property_name_column = next((col for col in property_name_candidates if col in df.columns), None)
        if not property_id_column or not property_name_column:
            return

        for _, row in df[[property_id_column, property_name_column]].dropna().iterrows():
            property_id_int = _safe_int_property_id(row.get(property_id_column))
            property_name = str(row.get(property_name_column)).strip()
            if property_id_int is None or not property_name or property_name.lower() == 'nan':
                continue
            if property_id_int not in target_map:
                target_map[property_id_int] = property_name

    property_name_map = {}
    _extract_property_names_from_raw(sources.get(config.ar_source.name), property_name_map)
    _extract_property_names_from_raw(sources.get(config.scheduled_source.name), property_name_map)
    print(f"[EXECUTE_AUDIT_RUN] Upload-time property name map size: {len(property_name_map)}")
    
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

    if early_prefilter_enabled:
        window_start, window_end = _resolve_audit_window_bounds(audit_year=audit_year, audit_month=audit_month)
        pre_late_ar_count = len(actual_detail)
        pre_late_expected_count = len(expected_detail)

        ar_month = actual_detail.get(CanonicalField.AUDIT_MONTH.value)
        expected_month = expected_detail.get(CanonicalField.AUDIT_MONTH.value)
        ar_outside_window = int(((ar_month < window_start) | (ar_month > window_end)).sum()) if ar_month is not None else 0
        expected_outside_window = int(((expected_month < window_start) | (expected_month > window_end)).sum()) if expected_month is not None else 0
        print(
            f"[EARLY WINDOW FILTER] Parity pre-check: actual_rows={pre_late_ar_count}, expected_rows={pre_late_expected_count}, "
            f"actual_outside_window={ar_outside_window}, expected_outside_window={expected_outside_window}"
        )
    
    # Apply period filter.
    # Default behavior when no explicit year is selected: current academic year (Aug -> current month).
    if audit_year is None:
        print("\n[AUDIT PERIOD FILTER] Applying default Current Academic Year filter")
        print(f"[AUDIT PERIOD FILTER] Before filter - Expected: {len(expected_detail)}, Actual: {len(actual_detail)}")

        expected_detail = filter_to_current_academic_year(expected_detail)
        actual_detail = filter_to_current_academic_year(actual_detail)

        # If month is explicitly selected with Current Academic Year, apply month after academic-year scoping.
        if audit_month is not None:
            expected_detail = filter_by_audit_period(expected_detail, year=None, month=audit_month)
            actual_detail = filter_by_audit_period(actual_detail, year=None, month=audit_month)

        print(f"[AUDIT PERIOD FILTER] After filter - Expected: {len(expected_detail)}, Actual: {len(actual_detail)}")
    elif audit_year is not None or audit_month is not None:
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
        api_code_set = {int(code) for code in API_POSTED_AR_CODES}
        api_code_text_set = {str(code) for code in api_code_set}
        ar_code_numeric = pd.to_numeric(ar_code_col, errors='coerce')
        api_code_mask = ar_code_numeric.isin(api_code_set) | ar_code_col.astype(str).str.strip().isin(api_code_text_set)

        api_codes_present = actual_detail[api_code_mask]
        
        if not api_codes_present.empty:
            print(f"[API CODE FILTER] Found {len(api_codes_present)} API code transactions to filter:")
            for code in API_POSTED_AR_CODES:
                count = int((ar_code_numeric == int(code)).sum())
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
        actual_detail = actual_detail[~api_code_mask].copy()
        
        filtered_count = original_count - len(actual_detail)
        print(f"[API CODE FILTER] Filtered out: {filtered_count} transactions")
        print(f"[API CODE FILTER] Remaining AR transactions: {len(actual_detail)}")
        print(f"[API CODE FILTER] ==========================================\n")
    
    # Optionally scope audit execution to selected property IDs.
    scoped_property_set = {
        token for token in (_normalize_property_id_token(v) for v in (scoped_property_ids or [])) if token
    }
    if scoped_property_set:
        print(f"\n[PROPERTY SCOPE] Limiting audit execution to properties: {sorted(scoped_property_set)}")
        expected_detail = _filter_df_to_property_scope(expected_detail, scoped_property_set)
        actual_detail = _filter_df_to_property_scope(actual_detail, scoped_property_set)
        scheduled_normalized = _filter_df_to_property_scope(scheduled_normalized, scoped_property_set)
        print(
            f"[PROPERTY SCOPE] After scope filter - "
            f"Expected: {len(expected_detail)}, Actual: {len(actual_detail)}, Scheduled: {len(scheduled_normalized)}"
        )

    # Execute reconciliation property-by-property (true scoped execution, not post-sort).
    property_column = CanonicalField.PROPERTY_ID.value

    for dataset_name, dataset_df in {
        "expected_detail": expected_detail,
        "actual_detail": actual_detail,
        "scheduled_normalized": scheduled_normalized,
    }.items():
        if property_column not in dataset_df.columns:
            raise ValueError(
                f"{dataset_name} missing required property column '{property_column}' for property-scoped reconciliation"
            )

    def _group_by_property(df: pd.DataFrame) -> dict:
        if df.empty:
            return {}

        property_keys = df[property_column].apply(_normalize_property_id_token)
        valid_mask = property_keys.notna()
        if valid_mask.sum() == 0:
            return {}

        groups = {}
        for key, subset in df[valid_mask].groupby(property_keys[valid_mask], sort=True):
            groups[key] = subset.copy()
        return groups

    expected_by_property = _group_by_property(expected_detail)
    actual_by_property = _group_by_property(actual_detail)
    scheduled_by_property = _group_by_property(scheduled_normalized)

    property_keys = sorted(
        set(expected_by_property.keys()) |
        set(actual_by_property.keys()) |
        set(scheduled_by_property.keys())
    )

    print(f"\n[PROPERTY EXECUTION] Running reconciliation per property ({len(property_keys)} properties)")

    expected_parts = []
    actual_parts = []
    bucket_parts = []
    finding_parts = []
    variance_parts = []
    property_execution_stats = []

    empty_expected_template = expected_detail.iloc[0:0].copy()
    empty_actual_template = actual_detail.iloc[0:0].copy()
    empty_scheduled_template = scheduled_normalized.iloc[0:0].copy()

    for property_key in property_keys:
        expected_prop = expected_by_property.get(property_key, empty_expected_template)
        actual_prop = actual_by_property.get(property_key, empty_actual_template)
        scheduled_prop = scheduled_by_property.get(property_key, empty_scheduled_template)

        print(
            f"[PROPERTY EXECUTION] PROPERTY_ID={property_key}: "
            f"expected={len(expected_prop)}, actual={len(actual_prop)}, scheduled={len(scheduled_prop)}"
        )

        # Reconcile detail for this property only.
        variance_detail_prop, recon_stats_prop = reconcile_detail(
            scheduled_prop,
            actual_prop,
            config.reconciliation
        )

        # Reconcile buckets for this property only.
        bucket_results_prop = reconcile_buckets(expected_prop, actual_prop, config.reconciliation)

        # Run rules scoped to this property only.
        context_prop = RuleContext(
            run_id=run_id,
            expected_detail=expected_prop,
            actual_detail=actual_prop,
            bucket_results=bucket_results_prop
        )
        finding_dicts_prop = default_registry.evaluate_all(context_prop)
        findings_prop = generate_findings(finding_dicts_prop, run_id)

        expected_parts.append(expected_prop)
        actual_parts.append(actual_prop)
        bucket_parts.append(bucket_results_prop)
        finding_parts.append(findings_prop)
        variance_parts.append(variance_detail_prop)

        property_execution_stats.append({
            "property_id": property_key,
            "expected_rows": len(expected_prop),
            "actual_rows": len(actual_prop),
            "scheduled_rows": len(scheduled_prop),
            "bucket_rows": len(bucket_results_prop),
            "finding_rows": len(findings_prop),
            "variance_rows": len(variance_detail_prop),
            "primary_matched_ar": recon_stats_prop.get("primary_matched_ar", 0),
            "secondary_matched_ar": recon_stats_prop.get("secondary_matched_ar", 0),
            "tertiary_matched_ar": recon_stats_prop.get("tertiary_matched_ar", 0),
            "unmatched_ar": recon_stats_prop.get("unmatched_ar", 0),
            "unmatched_scheduled": recon_stats_prop.get("unmatched_scheduled", 0),
            "variances": recon_stats_prop.get("variances", 0),
        })

    def _concat_frames(frames, fallback: pd.DataFrame) -> pd.DataFrame:
        if frames:
            try:
                return pd.concat(frames, ignore_index=True)
            except ValueError:
                pass
        return fallback.iloc[0:0].copy()

    expected_detail = _concat_frames(expected_parts, expected_detail)
    actual_detail = _concat_frames(actual_parts, actual_detail)
    bucket_results = _concat_frames(bucket_parts, pd.DataFrame())
    findings = _concat_frames(finding_parts, pd.DataFrame())

    variance_frames = [df for df in variance_parts if df is not None]
    if variance_frames:
        try:
            variance_detail = pd.concat(variance_frames, ignore_index=True)
        except ValueError:
            variance_detail = pd.DataFrame()
    else:
        variance_detail = pd.DataFrame()

    recon_stats = {
        "total_scheduled": sum(item["scheduled_rows"] for item in property_execution_stats),
        "total_ar": sum(item["actual_rows"] for item in property_execution_stats),
        "primary_matched_ar": sum(item["primary_matched_ar"] for item in property_execution_stats),
        "secondary_matched_ar": sum(item["secondary_matched_ar"] for item in property_execution_stats),
        "tertiary_matched_ar": sum(item["tertiary_matched_ar"] for item in property_execution_stats),
        "unmatched_ar": sum(item["unmatched_ar"] for item in property_execution_stats),
        "unmatched_scheduled": sum(item["unmatched_scheduled"] for item in property_execution_stats),
        "variances": sum(item["variances"] for item in property_execution_stats),
        "properties_processed": len(property_execution_stats),
    }

    print(f"\n[RECONCILIATION STATS - AGGREGATED]")
    print(f"  Properties processed: {recon_stats['properties_processed']}")
    print(f"  Primary matches: {recon_stats['primary_matched_ar']}")
    print(f"  Secondary matches: {recon_stats['secondary_matched_ar']}")
    print(f"  Tertiary matches: {recon_stats['tertiary_matched_ar']}")
    print(f"  Unmatched AR: {recon_stats['unmatched_ar']}")
    print(f"  Unmatched scheduled: {recon_stats['unmatched_scheduled']}")
    print(f"  Total variances: {recon_stats['variances']}")

    # Aggregate property summaries into portfolio totals (computed from concatenated outputs).
    property_summary = calculate_property_summary(bucket_results, findings, actual_detail)
    portfolio_totals = calculate_kpis(bucket_results, findings)

    return {
        "expected_detail": expected_detail,
        "actual_detail": actual_detail,
        "bucket_results": bucket_results,
        "variance_detail": variance_detail,
        "recon_stats": recon_stats,
        "findings": findings,
        "property_summary": property_summary,
        "portfolio_totals": portfolio_totals,
        "property_execution_stats": property_execution_stats,
        "property_name_map": property_name_map,
    }


@bp.route('/')
@require_auth
def index():
    """Upload form and recent runs."""
    import logging
    logger = logging.getLogger(__name__)
    
    storage = get_storage_service()
    recent_runs = storage.list_runs(limit=10)
    api_property_options = []
    try:
        api_property_options = cached_load_api_property_picklist(_session_cache_token())
    except Exception as e:
        logger.warning(f"[INDEX] Failed loading Entrata property picklist: {e}")
    user = get_current_user()
    
    # Session lifecycle logging (Start/timeout End) is handled centrally in app.before_request.
    logger.info(f"[INDEX] User present: {user is not None}")
    if user:
        logger.info(f"[INDEX] User keys: {list(user.keys())}")
        logger.info(f"[INDEX] SharePoint logging enabled: {config.auth.enable_sharepoint_logging}")
        logger.info(f"[INDEX] Can log to SharePoint: {config.auth.can_log_to_sharepoint()}")
        logger.info(f"[INDEX] SharePoint site URL: {config.auth.sharepoint_site_url}")
        logger.info(f"[INDEX] SharePoint list name: {config.auth.sharepoint_list_name}")
    
    return render_template(
        'upload.html',
        recent_runs=recent_runs,
        user=user,
        api_property_options=api_property_options,
    )


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

    session.pop('session_id', None)
    session.pop('session_started_at', None)
    session.pop('last_activity_at', None)
    
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
    
    audit_started_at = None
    request_started_at_utc = None

    try:
        request_started_at_utc = datetime.utcnow()
        audit_started_at = perf_counter()

        # Get audit period filters from form
        audit_year = request.form.get('audit_year')
        audit_month = request.form.get('audit_month')
        scoped_property_id = request.form.get('scoped_property_id')
        scoped_property_ids_raw = request.form.get('scoped_property_ids')

        scoped_property_ids = []
        for value in [scoped_property_id] + (scoped_property_ids_raw.split(',') if scoped_property_ids_raw else []):
            normalized = _normalize_property_id_token(value)
            if normalized and normalized not in scoped_property_ids:
                scoped_property_ids.append(normalized)
        
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
        
        file_save_started = perf_counter()
        file.save(str(file_path))
        file_save_seconds = perf_counter() - file_save_started
        
        # Execute audit with period filter and optional property scope.
        execute_started = perf_counter()
        results = execute_audit_run(
            file_path,
            run_id,
            audit_year,
            audit_month,
            scoped_property_ids=scoped_property_ids
        )
        execute_seconds = perf_counter() - execute_started

        # For property-scoped runs, overlay onto latest baseline run so portfolio remains complete.
        base_run_id = None
        overlay_seconds = 0.0
        if scoped_property_ids:
            overlay_started = perf_counter()
            try:
                # PRIORITY 1: Use the most recently saved run from this session (avoids SharePoint eventual consistency)
                last_saved_run = session.get('last_saved_run_id')
                if last_saved_run and last_saved_run != run_id:
                    base_run_id = last_saved_run
                    logger.info(
                        f"[PROPERTY SCOPE] Using last saved run from session as baseline: {base_run_id}"
                    )
                else:
                    # PRIORITY 2: Fall back to SharePoint list_runs
                    prior_runs = storage.list_runs(limit=2)
                    for run in prior_runs:
                        candidate = run.get('run_id')
                        if candidate and candidate != run_id:
                            base_run_id = candidate
                            logger.info(
                                f"[PROPERTY SCOPE] Using baseline from list_runs: {base_run_id}"
                            )
                            break

                if base_run_id:
                    logger.info(
                        f"[PROPERTY SCOPE] Overlaying scoped results for {scoped_property_ids} "
                        f"onto baseline run {base_run_id}"
                    )
                    baseline_run_data = storage.load_run(base_run_id)
                    results = _overlay_property_scope_results(
                        results,
                        baseline_run_data,
                        set(scoped_property_ids)
                    )
            except Exception as overlay_error:
                logger.warning(f"[PROPERTY SCOPE] Baseline overlay skipped due to error: {overlay_error}")
            finally:
                overlay_seconds = perf_counter() - overlay_started
        
        # Save results
        metadata = storage.create_metadata(run_id, file_path)
        metadata['run_scope'] = {
            'type': 'property' if scoped_property_ids else 'portfolio',
            'property_ids': scoped_property_ids,
            'base_run_id': base_run_id,
        }
        # Add period filter to metadata
        if audit_year or audit_month:
            metadata['audit_period'] = {
                'year': audit_year,
                'month': audit_month
            }
        
        save_run_started = perf_counter()
        storage.save_run(
            run_id,
            results["expected_detail"],
            results["actual_detail"],
            results["bucket_results"],
            results["findings"],
            metadata,
            results.get("variance_detail"),
            file_path,  # Pass the original Excel file path
            property_name_map=results.get("property_name_map"),
        )

        # New run added: invalidate run-picker caches.
        invalidate_runs_cache()
        
        # Store this run_id in session for next property upload to use as baseline
        session['last_saved_run_id'] = run_id
        logger.info(f"[SESSION] Stored run {run_id} as last_saved_run_id for next baseline")
        
        save_run_seconds = perf_counter() - save_run_started
        
        # 🧹 CLEAR CACHE after new upload
        logger.info(f"[CACHE] Clearing cache after new upload (run_id: {run_id})")
        cache_clear_started = perf_counter()
        clear_run_cache(run_id)
        cache_clear_seconds = perf_counter() - cache_clear_started
        
        # Clean up temp file if using SharePoint
        cleanup_seconds = 0.0
        if storage.use_sharepoint:
            cleanup_started = perf_counter()
            import shutil
            try:
                shutil.rmtree(file_path.parent)  # Remove temp directory
            except Exception as e:
                import logging
                logging.warning(f"Failed to cleanup temp directory: {e}")
            finally:
                cleanup_seconds = perf_counter() - cleanup_started
        
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
        
        # Log successful audit completion to SharePoint
        user = get_current_user()
        activity_log_seconds = 0.0
        if user and config.auth.can_log_to_sharepoint():
            activity_started = perf_counter()
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
                    'run_scope': metadata.get('run_scope', {}),
                    'user_role': 'user'
                }
            )
            activity_log_seconds = perf_counter() - activity_started

        if audit_started_at is not None:
            elapsed_seconds = perf_counter() - audit_started_at
            logger.info(
                f"[AUDIT TIMER] SUCCESS run_id={run_id} file={filename} "
                f"elapsed_seconds={elapsed_seconds:.2f} "
                f"file_save_seconds={file_save_seconds:.2f} "
                f"execute_seconds={execute_seconds:.2f} "
                f"overlay_seconds={overlay_seconds:.2f} "
                f"save_run_seconds={save_run_seconds:.2f} "
                f"cache_clear_seconds={cache_clear_seconds:.2f} "
                f"cleanup_seconds={cleanup_seconds:.2f} "
                f"activity_log_seconds={activity_log_seconds:.2f}"
            )

        session['pending_upload_timing'] = {
            'run_id': run_id,
            'request_started_at_utc': request_started_at_utc.isoformat() if request_started_at_utc else '',
            'upload_request_seconds': float(perf_counter() - audit_started_at) if audit_started_at is not None else 0.0,
            'file_save_seconds': float(file_save_seconds),
            'execute_seconds': float(execute_seconds),
            'overlay_seconds': float(overlay_seconds),
            'save_run_seconds': float(save_run_seconds),
            'cache_clear_seconds': float(cache_clear_seconds),
            'cleanup_seconds': float(cleanup_seconds),
            'activity_log_seconds': float(activity_log_seconds),
        }

        if len(scoped_property_ids) == 1:
            return redirect(url_for('main.property_view', property_id=scoped_property_ids[0], run_id=run_id))

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

        if audit_started_at is not None:
            elapsed_seconds = perf_counter() - audit_started_at
            logger.error(
                f"[AUDIT TIMER] FAILED run_id={locals().get('run_id', 'unknown')} "
                f"file={locals().get('filename', 'unknown')} "
                f"elapsed_seconds={elapsed_seconds:.2f} error={error_msg}"
            )
        
        flash(f'Error processing file: {error_msg}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/upload-api-property', methods=['POST'])
@require_auth
def upload_api_property():
    """Run audit from API sources for a single property (parallel flow to Excel upload)."""
    audit_started_at = perf_counter()
    request_started_at_utc = datetime.utcnow()

    try:
        property_id_raw = request.form.get('api_property_id')
        if not property_id_raw:
            flash('Property ID is required for API upload.', 'danger')
            return redirect(url_for('main.index'))

        property_id = int(float(property_id_raw))

        audit_year = request.form.get('audit_year')
        audit_month = request.form.get('audit_month')
        audit_year = int(audit_year) if audit_year else None
        audit_month = int(audit_month) if audit_month else None

        transaction_from_date = request.form.get('api_from_date') or None
        transaction_to_date = request.form.get('api_to_date') or None

        storage = get_storage_service()
        run_id = storage.generate_run_id()

        # Load property name from picklist (authoritative source for property names)
        property_name_from_picklist = None
        try:
            picklist = cached_load_api_property_picklist(_session_cache_token())
            for item in picklist:
                if str(item.get('property_id')) == str(property_id):
                    property_name_from_picklist = item.get('property_name')
                    break
        except Exception as picklist_error:
            logger.warning(f"[API UPLOAD] Failed to load property name from picklist: {picklist_error}")

        api_fetch_started = perf_counter()
        api_sources = fetch_property_api_sources(
            property_id=property_id,
            transaction_from_date=transaction_from_date,
            transaction_to_date=transaction_to_date,
        )
        api_fetch_seconds = perf_counter() - api_fetch_started

        ar_raw = api_sources.get('ar_raw', pd.DataFrame())
        scheduled_raw = api_sources.get('scheduled_raw', pd.DataFrame())

        if ar_raw.empty and scheduled_raw.empty:
            flash('API returned no scheduled charges or AR transactions for that property.', 'warning')
            return redirect(url_for('main.index'))

        # Use picklist property name if available, otherwise use API source name
        property_name = property_name_from_picklist or api_sources.get('property_name') or f"Property {property_id}"
        logger.info(f"[API UPLOAD] Property {property_id} name: {property_name} (source: {'picklist' if property_name_from_picklist else 'API fallback'})")

        execute_started = perf_counter()
        results = execute_audit_run(
            file_path=None,
            run_id=run_id,
            audit_year=audit_year,
            audit_month=audit_month,
            scoped_property_ids=[str(property_id)],
            preloaded_sources={
                config.ar_source.name: ar_raw,
                config.scheduled_source.name: scheduled_raw,
            },
        )
        
        # Override property_name_map with picklist name for snapshot persistence
        if property_name_from_picklist:
            results['property_name_map'] = {property_id: property_name_from_picklist}
        execute_seconds = perf_counter() - execute_started

        base_run_id = None
        overlay_seconds = 0.0
        try:
            # PRIORITY 1: Use the most recently saved run from this session (avoids SharePoint eventual consistency)
            last_saved_run = session.get('last_saved_run_id')
            if last_saved_run and last_saved_run != run_id:
                base_run_id = last_saved_run
                logger.info(
                    f"[PROPERTY API SCOPE] Using last saved run from session as baseline: {base_run_id}"
                )
            else:
                # PRIORITY 2: Fall back to SharePoint list_runs
                prior_runs = storage.list_runs(limit=2)
                for run in prior_runs:
                    candidate = run.get('run_id')
                    if candidate and candidate != run_id:
                        base_run_id = candidate
                        logger.info(
                            f"[PROPERTY API SCOPE] Using baseline from list_runs: {base_run_id}"
                        )
                        break

            if base_run_id:
                overlay_started = perf_counter()
                logger.info(
                    f"[PROPERTY API SCOPE] Overlaying API-scoped results for property {property_id} "
                    f"onto baseline run {base_run_id}"
                )
                baseline_run_data = storage.load_run(base_run_id)
                results = _overlay_property_scope_results(
                    results,
                    baseline_run_data,
                    {str(property_id)}
                )
                overlay_seconds = perf_counter() - overlay_started
        except Exception as overlay_error:
            logger.warning(f"[PROPERTY API SCOPE] Baseline overlay skipped due to error: {overlay_error}")

        metadata = {
            'run_id': run_id,
            'timestamp': datetime.now().isoformat(),
            'config_version': 'v1',
            'file_name': f'api_property_{property_id}.json',
            'file_hash': '',
            'file_size': 0,
        }
        metadata['run_scope'] = {
            'type': 'property',
            'source': 'api_property',
            'property_ids': [str(property_id)],
            'base_run_id': base_run_id,
        }
        metadata['api_ingest'] = {
            'property_id': property_id,
            'property_name': property_name,  # Use property name from picklist or API fallback
            'lease_count': int(api_sources.get('lease_count') or 0),
        }
        if audit_year or audit_month:
            metadata['audit_period'] = {
                'year': audit_year,
                'month': audit_month,
            }

        save_run_started = perf_counter()
        storage.save_run(
            run_id,
            results["expected_detail"],
            results["actual_detail"],
            results["bucket_results"],
            results["findings"],
            metadata,
            results.get("variance_detail"),
            None,
            property_name_map=results.get("property_name_map"),
        )
        save_run_seconds = perf_counter() - save_run_started

        # Store this run_id in session for next property upload to use as baseline
        session['last_saved_run_id'] = run_id
        logger.info(f"[SESSION] Stored run {run_id} as last_saved_run_id for next baseline")

        cache_clear_started = perf_counter()
        invalidate_runs_cache()
        clear_run_cache(run_id)
        cache_clear_seconds = perf_counter() - cache_clear_started
        cleanup_seconds = 0.0

        activity_log_seconds = 0.0
        user = get_current_user()
        if user and config.auth.can_log_to_sharepoint():
            activity_started = perf_counter()
            log_user_activity(
                user_info=user,
                activity_type='Successful Audit',
                site_url=config.auth.sharepoint_site_url,
                list_name=config.auth.sharepoint_list_name,
                details={
                    'run_id': run_id,
                    'source': 'api_property',
                    'property_id': property_id,
                    'audit_year': audit_year,
                    'audit_month': audit_month,
                    'user_role': 'user',
                }
            )
            activity_log_seconds = perf_counter() - activity_started

        elapsed_seconds = perf_counter() - audit_started_at
        logger.info(
            f"[AUDIT TIMER] SUCCESS run_id={run_id} source=api_property property_id={property_id} "
            f"elapsed_seconds={elapsed_seconds:.2f} "
            f"api_fetch_seconds={api_fetch_seconds:.2f} "
            f"execute_seconds={execute_seconds:.2f} "
            f"overlay_seconds={overlay_seconds:.2f} "
            f"save_run_seconds={save_run_seconds:.2f} "
            f"cache_clear_seconds={cache_clear_seconds:.2f} "
            f"cleanup_seconds={cleanup_seconds:.2f} "
            f"activity_log_seconds={activity_log_seconds:.2f}"
        )

        session['pending_upload_timing'] = {
            'run_id': run_id,
            'request_started_at_utc': request_started_at_utc.isoformat(),
            'upload_request_seconds': float(elapsed_seconds),
            'file_save_seconds': 0.0,
            'api_fetch_seconds': float(api_fetch_seconds),
            'execute_seconds': float(execute_seconds),
            'overlay_seconds': float(overlay_seconds),
            'save_run_seconds': float(save_run_seconds),
            'cache_clear_seconds': float(cache_clear_seconds),
            'cleanup_seconds': float(cleanup_seconds),
            'activity_log_seconds': float(activity_log_seconds),
        }

        return redirect(url_for('main.property_view', property_id=str(property_id), run_id=run_id))

    except Exception as e:
        error_msg = str(e)
        logger.exception(f"[PROPERTY API UPLOAD] Failed for property {request.form.get('api_property_id')}: {error_msg}")
        flash(f'Error processing API property upload: {error_msg}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/portfolio')
@bp.route('/portfolio/<run_id>')
@require_auth
def portfolio(run_id: str = None):
    """Portfolio view - Aggregated latest data for each property across all runs."""
    try:
        route_started = perf_counter()
        cache_token = _session_cache_token()
        storage = get_storage_service()
        
        # Load latest snapshot for each property across ALL runs
        property_snapshots = storage.load_latest_property_snapshots_across_runs()
        
        # If no properties found, fall back to showing latest run
        if not property_snapshots:
            if not run_id:
                latest_run = get_latest_run(cache_token)
                run_id = latest_run.get('run_id')
                if not run_id:
                    flash('No audit runs available', 'warning')
                    return redirect(url_for('main.index'))
            
            property_snapshots = cached_load_run_display_snapshots_for_run(
                run_id=run_id,
                scope_type='property',
                session_cache_key=cache_token
            )

        # Aggregate KPIs from all property snapshots
        total_exceptions = sum(p.get('exception_count', 0) for p in property_snapshots)
        total_undercharge = sum(p.get('undercharge', 0) for p in property_snapshots)
        total_overcharge = sum(p.get('overcharge', 0) for p in property_snapshots)
        total_buckets = sum(p.get('total_buckets', 0) for p in property_snapshots)
        matched_buckets = sum(p.get('matched_buckets', 0) for p in property_snapshots)
        match_rate = (matched_buckets / total_buckets * 100) if total_buckets > 0 else 0

        kpis = {
            'current_undercharge': float(total_undercharge),
            'historical_undercharge': 0.0,
            'current_overcharge': float(total_overcharge),
            'historical_overcharge': 0.0,
            'open_exceptions': int(total_exceptions),
            'match_rate': float(match_rate),
            'total_runs': 0,
            'most_recent_run': None,
        }
        
        # Get metadata from the most recent run_id in property snapshots
        if property_snapshots:
            # Find the most recent run_id from the snapshots
            most_recent_run_id = max(p.get('run_id') for p in property_snapshots if p.get('run_id'))
            kpis['most_recent_run'] = {'run_id': most_recent_run_id}
            try:
                metadata = cached_load_metadata(most_recent_run_id, cache_token)
            except Exception:
                metadata = {'timestamp': 'Unknown'}
        else:
            metadata = {'timestamp': 'Unknown'}

        logger.info(
            f"[SNAPSHOT][PORTFOLIO] Aggregated {len(property_snapshots)} properties from latest runs: "
            f"exception_count={total_exceptions}, undercharge={total_undercharge}, "
            f"overcharge={total_overcharge}, match_rate={match_rate:.2f}%"
        )

        # Build property rows for display
        properties = []
        for snapshot_row in property_snapshots:
            property_id = snapshot_row.get('property_id')
            if property_id is None:
                continue

            undercharge = float(snapshot_row.get('undercharge', 0) or 0)
            overcharge = float(snapshot_row.get('overcharge', 0) or 0)
            exception_count = int(snapshot_row.get('exception_count', 0) or 0)
            
            # Use run_id from this specific property's snapshot (for drill-down links)
            property_run_id = snapshot_row.get('run_id')

            properties.append({
                'property_name': (
                    _clean_property_name(snapshot_row.get('property_name'))
                    or f"Property {property_id}"
                ),
                'property_id': property_id,
                'run_id': property_run_id,  # Store which run this property data is from
                'total_lease_intervals': int(snapshot_row.get('total_lease_intervals', 0) or 0),
                'exception_buckets': exception_count,
                'total_undercharge': undercharge,
                'total_overcharge': overcharge,
                'total_variance': float(snapshot_row.get('total_variance', undercharge + overcharge) or (undercharge + overcharge)),
            })

        properties.sort(key=lambda p: p.get('exception_buckets', 0), reverse=True)
        
        # Use the most recent run_id for the render (if available)
        display_run_id = kpis['most_recent_run']['run_id'] if kpis.get('most_recent_run') else None
        
        response = render_template(
            'portfolio.html',
            run_id=display_run_id,
            metadata=metadata,
            kpis=kpis,
            properties=properties,
            total_runs=kpis['total_runs'],
            current_run_id=run_id,
        )
        _log_and_clear_pending_upload_timing(
            run_id=run_id,
            destination='portfolio',
            destination_route_seconds=(perf_counter() - route_started)
        )
        return response
    except Exception as e:
        import traceback
        print(f"[ERROR] Portfolio view error: {str(e)}")
        print(traceback.format_exc())
        flash(f'Error loading portfolio: {str(e)}', 'danger')
        return redirect(url_for('main.index'))


@bp.route('/api/exception-states/<run_id>/<int:property_id>/<int:lease_interval_id>', methods=['GET'])
@require_auth
def get_exception_states(run_id: str, property_id: int, lease_interval_id: int):
    storage = get_storage_service()
    states = storage.load_exception_states_from_sharepoint_list(run_id, property_id, lease_interval_id)
    return jsonify({'states': states})


@bp.route('/api/exception-states', methods=['POST'])
@require_auth
def upsert_exception_state():
    payload = request.get_json(silent=True) or {}
    required = ['run_id', 'property_id', 'lease_interval_id', 'ar_code_id', 'exception_type', 'status']
    missing = [key for key in required if key not in payload]
    if missing:
        return jsonify({'ok': False, 'error': f"Missing fields: {', '.join(missing)}"}), 400

    storage = get_storage_service()
    ok = storage.upsert_exception_state_to_sharepoint_list(payload)
    if ok:
        _clear_run_scoped_caches(
            payload['run_id'],
            property_id=payload.get('property_id'),
            lease_interval_id=payload.get('lease_interval_id')
        )
    return jsonify({'ok': ok})


@bp.route('/api/exception-months/<run_id>/<int:property_id>/<int:lease_interval_id>/<ar_code_id>', methods=['GET'])
@require_auth
def get_exception_months(run_id: str, property_id: int, lease_interval_id: int, ar_code_id: str):
    """Get all exception months for a specific AR code."""
    storage = get_storage_service()
    months = storage.load_exception_months_from_sharepoint_list(
        run_id, property_id, lease_interval_id, ar_code_id
    )
    return jsonify({'months': months})


@bp.route('/api/exception-months', methods=['POST'])
@require_auth
def upsert_exception_month():
    """
    Upsert a single month's exception state.
    
    Expected payload:
    {
        "run_id": "run_20260127_135019",
        "property_id": 101,
        "lease_interval_id": 2345,
        "ar_code_id": "AR001",
        "audit_month": "2024-01",
        "exception_type": "Scheduled Not Billed",
        "status": "Resolved",
        "fix_label": "Add to next billing cycle",
        "action_type": "bill_next_cycle",
        "variance": -500.00,
        "expected_total": 500.00,
        "actual_total": 0.00,
        "resolved_at": "2026-02-09T14:30:00",
        "resolved_by": "user@company.com",
        "resolved_by_name": "User Name"
    }
    """
    payload = request.get_json(silent=True) or {}
    required = ['run_id', 'property_id', 'lease_interval_id', 'ar_code_id', 'audit_month']
    missing = [key for key in required if key not in payload]
    if missing:
        return jsonify({'ok': False, 'error': f"Missing fields: {', '.join(missing)}"}), 400

    # Add timestamp and user info
    user = get_current_user()
    payload['updated_at'] = datetime.now().isoformat()
    payload['updated_by'] = user.get('email', 'unknown') if user else 'unknown'
    
    logger.info(f"[EXCEPTION_MONTH] User info: {user}")
    
    if payload.get('status') == 'Resolved' and not payload.get('resolved_at'):
        payload['resolved_at'] = datetime.now().isoformat()
        payload['resolved_by'] = user.get('email', 'unknown') if user else 'unknown'
        payload['resolved_by_name'] = user.get('name', 'Unknown') if user else 'Unknown'
        logger.info(f"[EXCEPTION_MONTH] Setting resolved_by={payload['resolved_by']}, resolved_by_name={payload['resolved_by_name']}")

    storage = get_storage_service()
    ok = storage.upsert_exception_month_to_sharepoint_list(payload)
    
    # 🧹 CLEAR CACHE after status update
    if ok:
        logger.info(f"[CACHE] Clearing cache after exception status update")
        _clear_run_scoped_caches(
            payload['run_id'],
            property_id=payload.get('property_id'),
            lease_interval_id=payload.get('lease_interval_id')
        )
    
    # Recalculate overall AR code status using scoped current-run logic.
    status_info = _calculate_scoped_ar_status(
        storage,
        payload['run_id'],
        int(payload['property_id']),
        int(payload['lease_interval_id']),
        payload['ar_code_id']
    )
    
    return jsonify({'ok': ok, 'ar_code_status': status_info})


@bp.route('/api/exception-months/ar-status/<run_id>/<int:property_id>/<int:lease_interval_id>/<ar_code_id>', methods=['GET'])
@require_auth
def get_ar_code_status_api(run_id: str, property_id: int, lease_interval_id: int, ar_code_id: str):
    """Get calculated AR code status based on month-level statuses."""
    storage = get_storage_service()
    status_info = _calculate_scoped_ar_status(storage, run_id, property_id, lease_interval_id, ar_code_id)
    return jsonify(status_info)


@bp.route('/property/<property_id>')
@bp.route('/property/<property_id>/<run_id>')
@require_auth
def property_view(property_id: str, run_id: str = None):
    """Property view - exceptions grouped by lease with run selector."""
    try:
        route_started = perf_counter()
        storage = get_storage_service()
        cache_token = _session_cache_token()

        # Resolve selected run without loading full run list unless needed.
        if not run_id:
            latest_runs = get_available_runs(cache_token)
            if latest_runs:
                run_id = latest_runs[0]['run_id']
            else:
                flash('No audit runs available', 'warning')
                return redirect(url_for('main.index'))
        
        try:
            metadata = cached_load_metadata(run_id, cache_token)
        except Exception:
            metadata = {'timestamp': 'Unknown'}

        # Get bucket results for this property from AuditRuns
        all_property_buckets = cached_load_bucket_results(
            run_id,
            property_id=int(float(property_id)),
            session_cache_key=cache_token
        )

        property_snapshot = cached_load_run_display_snapshot(
            run_id=run_id,
            scope_type='property',
            property_id=int(float(property_id)),
            session_cache_key=cache_token
        )
        lease_snapshot_map = cached_load_run_display_snapshots_for_property(
            run_id=run_id,
            property_id=int(float(property_id)),
            scope_type='lease',
            session_cache_key=cache_token
        )
        if property_snapshot:
            logger.info(
                f"[SNAPSHOT][PROPERTY] Using RunDisplaySnapshots for run {run_id}, property {property_id}: "
                f"exception_count={property_snapshot.get('exception_count')}, "
                f"undercharge={property_snapshot.get('undercharge')}, "
                f"overcharge={property_snapshot.get('overcharge')}"
            )
        else:
            logger.warning(
                f"[SNAPSHOT][PROPERTY] Fallback to recalculated property metrics for run {run_id}, "
                f"property {property_id} (snapshot not found or unavailable)"
            )
        
        property_name = _resolve_property_name_for_run(run_id, property_id, cache_token)
        
        # Get all unique leases for this property
        all_lease_ids = sorted(all_property_buckets[CanonicalField.LEASE_INTERVAL_ID.value].unique())

        lease_customer_names = {}
        lease_parent_ids = {}
        try:
            lease_ids_normalized = {int(float(lease_id)) for lease_id in all_lease_ids}

            def _normalize_person_name(value):
                if value is None:
                    return ""
                text = str(value).strip()
                if not text or text.lower() == 'nan':
                    return ""
                return text

            def _row_is_guarantor_like(row):
                customer_value = _normalize_person_name(row.get(CanonicalField.CUSTOMER_NAME.value))
                guarantor_value = _normalize_person_name(row.get(CanonicalField.GUARANTOR_NAME.value))
                if not customer_value or not guarantor_value:
                    return False
                return customer_value.casefold() == guarantor_value.casefold()

            def _build_name_map_for_source(source_df: pd.DataFrame) -> dict:
                name_map = {}
                if source_df is None or source_df.empty or CanonicalField.LEASE_INTERVAL_ID.value not in source_df.columns:
                    return name_map

                non_guarantor_candidates = {lease_key: [] for lease_key in lease_ids_normalized}
                fallback_candidates = {lease_key: [] for lease_key in lease_ids_normalized}

                for _, record in source_df.iterrows():
                    lease_value = record.get(CanonicalField.LEASE_INTERVAL_ID.value)
                    if pd.isna(lease_value):
                        continue
                    lease_key = int(float(lease_value))
                    if lease_key not in lease_ids_normalized:
                        continue

                    customer_value = _normalize_person_name(record.get(CanonicalField.CUSTOMER_NAME.value))
                    if not customer_value:
                        continue

                    fallback_candidates[lease_key].append(customer_value)
                    if not _row_is_guarantor_like(record):
                        non_guarantor_candidates[lease_key].append(customer_value)

                for lease_key in lease_ids_normalized:
                    preferred = non_guarantor_candidates[lease_key] if non_guarantor_candidates[lease_key] else fallback_candidates[lease_key]
                    if not preferred:
                        continue

                    counts = {}
                    first_seen = {}
                    for index, name in enumerate(preferred):
                        key = name.casefold()
                        counts[key] = counts.get(key, 0) + 1
                        if key not in first_seen:
                            first_seen[key] = index

                    winner_key = max(counts.keys(), key=lambda key: (counts[key], -first_seen[key]))
                    for name in preferred:
                        if name.casefold() == winner_key:
                            name_map[lease_key] = name
                            break

                return name_map

            expected_detail = cached_load_expected_detail(run_id, cache_token)
            actual_detail = cached_load_actual_detail(run_id, cache_token)

            # Prefer names from lease-details (expected) first, then fill from AR (actual).
            for source_df in [expected_detail, actual_detail]:
                source_name_map = _build_name_map_for_source(source_df)
                for lease_key, resolved_name in source_name_map.items():
                    if lease_key not in lease_customer_names or not lease_customer_names.get(lease_key):
                        lease_customer_names[lease_key] = resolved_name

            # Prefer LEASE_ID from expected first, then fallback to actual.
            for source_df in [expected_detail, actual_detail]:
                if source_df is None or source_df.empty or CanonicalField.LEASE_INTERVAL_ID.value not in source_df.columns:
                    continue
                if CanonicalField.LEASE_ID.value not in source_df.columns:
                    continue

                for _, record in source_df.iterrows():
                    lease_value = record.get(CanonicalField.LEASE_INTERVAL_ID.value)
                    if pd.isna(lease_value):
                        continue
                    lease_key = int(float(lease_value))
                    if lease_key not in lease_ids_normalized or lease_key in lease_parent_ids:
                        continue

                    lease_parent_value = record.get(CanonicalField.LEASE_ID.value)
                    if pd.notna(lease_parent_value):
                        try:
                            lease_parent_ids[lease_key] = int(float(lease_parent_value))
                        except Exception:
                            pass
        except Exception as name_error:
            logger.warning(f"[PROPERTY_VIEW] Failed to load resident names for lease rows: {name_error}")
        
        # Filter to only exceptions for grouping
        property_buckets = all_property_buckets[
            all_property_buckets[CanonicalField.STATUS.value] != config.reconciliation.status_matched
        ].copy()
        
        # 🚀 BULK FETCH: Load all exception months for this property in ONE call
        logger.info(f"[PROPERTY_VIEW] Bulk fetching exception months for property {property_id}")
        bulk_exception_data = cached_load_property_exception_months(
            run_id,
            int(float(property_id)),
            cache_token
        )
        
        # Build resolved_keys from bulk data
        resolved_keys = set()
        for (lease_id, ar_code_id), month_records in bulk_exception_data.items():
            for month_record in month_records:
                if month_record.get('status') == 'Resolved':
                    audit_month = month_record.get('audit_month')
                    if isinstance(audit_month, str):
                        audit_month = audit_month[:10]
                    resolved_key = _build_property_resolved_key(lease_id, ar_code_id, audit_month)
                    resolved_keys.add(resolved_key)
        
        logger.info(f"[PROPERTY_VIEW] Found {len(resolved_keys)} resolved exceptions (using bulk fetch)")
        
        # Filter property_buckets to exclude resolved ones
        def is_unresolved(row):
            key = _build_property_resolved_key(
                row[CanonicalField.LEASE_INTERVAL_ID.value],
                row[CanonicalField.AR_CODE_ID.value],
                row[CanonicalField.AUDIT_MONTH.value]
            )
            is_unres = key not in resolved_keys
            if not is_unres:
                logger.debug(f"[PROPERTY_VIEW] Filtering out resolved exception: {key}")
            return is_unres
        
        property_buckets = property_buckets[property_buckets.apply(is_unresolved, axis=1)].copy()
        logger.info(f"[PROPERTY_VIEW] After filtering resolved: {len(property_buckets)} unresolved exceptions remain")
        
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
            customer_name = lease_customer_names.get(int(float(lease_id)))
            lease_key = int(float(lease_id))
            lease_snapshot = lease_snapshot_map.get(lease_key)
            resolved_lease_id = lease_parent_ids.get(lease_key)
            if not resolved_lease_id and lease_snapshot:
                snapshot_lease_id = lease_snapshot.get('lease_id')
                if snapshot_lease_id is not None:
                    try:
                        resolved_lease_id = int(float(snapshot_lease_id))
                    except Exception:
                        pass
            
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
                static_exception_count = int(lease_snapshot.get('exception_count', len(exceptions))) if lease_snapshot else len(exceptions)
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'lease_id': resolved_lease_id,
                    'customer_name': customer_name,
                    'guarantor_name': guarantor_name,
                    'has_exceptions': static_exception_count > 0,
                    'exception_count': static_exception_count,
                    'unresolved_exception_count': len(exceptions),
                    'matched_count': matched_count,
                    'total_variance': total_variance,
                    'exceptions': sorted(exceptions, key=lambda x: abs(x['variance']), reverse=True)
                })
            else:
                # Clean lease - no exceptions
                static_exception_count = int(lease_snapshot.get('exception_count', 0)) if lease_snapshot else 0
                lease_summary.append({
                    'lease_interval_id': lease_id,
                    'lease_id': resolved_lease_id,
                    'customer_name': customer_name,
                    'guarantor_name': guarantor_name,
                    'has_exceptions': static_exception_count > 0,
                    'exception_count': static_exception_count,
                    'unresolved_exception_count': 0,
                    'matched_count': matched_count,
                    'total_variance': 0,
                    'exceptions': []
                })

        # Sort: highest exception count first, then unresolved count, then variance.
        lease_summary = sorted(
            lease_summary,
            key=lambda x: (
                -int(x.get('exception_count') or 0),
                -int(x.get('unresolved_exception_count') or 0),
                -abs(float(x.get('total_variance') or 0)),
            )
        )
        
        # Calculate property KPIs
        # Combine matched buckets with unresolved exceptions only (exclude resolved exceptions)
        matched_buckets = all_property_buckets[
            all_property_buckets[CanonicalField.STATUS.value] == config.reconciliation.status_matched
        ]
        kpis_input = pd.concat([matched_buckets, property_buckets], ignore_index=True)
        
        property_kpis = calculate_kpis(
            kpis_input,  # Use filtered dataset (matched + unresolved exceptions only)
            cached_load_findings(run_id, property_id=int(float(property_id)), session_cache_key=cache_token),
            property_id=None  # Already filtered, don't filter again
        )

        property_exception_count = len(property_buckets)
        if property_snapshot:
            property_exception_count = int(property_snapshot.get('exception_count', property_exception_count) or 0)
            property_kpis['total_undercharge'] = float(
                property_snapshot.get('undercharge', property_kpis.get('total_undercharge', 0)) or 0
            )
            property_kpis['total_overcharge'] = float(
                property_snapshot.get('overcharge', property_kpis.get('total_overcharge', 0)) or 0
            )
        
        response = render_template(
            'property.html',
            run_id=run_id,
            property_id=property_id,
            property_name=property_name,
            metadata=metadata,
            kpis=property_kpis,
            lease_summary=lease_summary,
            exception_count=property_exception_count,
            all_runs=[
                {
                    'run_id': run_id,
                    'timestamp': metadata.get('timestamp', 'Unknown'),
                    'audit_period': metadata.get('audit_period', {}),
                    'run_type': metadata.get('run_type', 'Manual')
                }
            ],
            current_run_id=run_id
        )
        _log_and_clear_pending_upload_timing(
            run_id=run_id,
            destination='property',
            destination_route_seconds=(perf_counter() - route_started)
        )
        return response
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


def _calculate_analysis_period(actual_detail: pd.DataFrame, expected_detail: pd.DataFrame) -> str:
    """Calculate the date range of data being analyzed."""
    try:
        # Get earliest and latest dates from both actual and expected data
        dates = []
        
        # Get dates from actual transactions
        if CanonicalField.POST_DATE.value in actual_detail.columns:
            actual_dates = pd.to_datetime(actual_detail[CanonicalField.POST_DATE.value], errors='coerce').dropna()
            if len(actual_dates) > 0:
                dates.extend(actual_dates.tolist())
        
        # Get dates from scheduled charges
        if CanonicalField.PERIOD_START.value in expected_detail.columns:
            expected_dates = pd.to_datetime(expected_detail[CanonicalField.PERIOD_START.value], errors='coerce').dropna()
            if len(expected_dates) > 0:
                dates.extend(expected_dates.tolist())
        
        if not dates:
            return None
        
        min_date = min(dates)
        max_date = max(dates)
        
        # Format as "MM/YYYY - MM/YYYY"
        return f"{min_date.strftime('%m/%Y')} - {max_date.strftime('%m/%Y')}"
    except Exception as e:
        print(f"[WARNING] Could not calculate analysis period: {e}")
        return None


def _get_status_color(status: str) -> str:
    """Get brand color class for status."""
    colors = {
        "SCHEDULED_NOT_BILLED": "secondary",  # grey
        "BILLED_NOT_SCHEDULED": "secondary",  # grey
        "AMOUNT_MISMATCH": "secondary"  # grey
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
        cache_token = _session_cache_token()
        bucket_results = cached_load_bucket_results(
            run_id,
            property_id=int(float(property_id)),
            lease_interval_id=int(float(lease_interval_id)),
            session_cache_key=cache_token
        )
        expected_detail = cached_load_expected_detail(run_id, cache_token)
        actual_detail = cached_load_actual_detail(run_id, cache_token)
        try:
            run_metadata = cached_load_metadata(run_id, cache_token)
        except Exception:
            run_metadata = {'timestamp': 'Unknown'}

        lease_snapshot = cached_load_run_display_snapshot(
            run_id=run_id,
            scope_type='lease',
            property_id=int(float(property_id)),
            lease_interval_id=int(float(lease_interval_id)),
            session_cache_key=cache_token
        )
        if lease_snapshot:
            logger.info(
                f"[SNAPSHOT][LEASE] Using RunDisplaySnapshots for run {run_id}, property {property_id}, "
                f"lease {lease_interval_id}: undercharge={lease_snapshot.get('undercharge')}, "
                f"overcharge={lease_snapshot.get('overcharge')}"
            )
        else:
            logger.warning(
                f"[SNAPSHOT][LEASE] Fallback to recalculated lease header totals for run {run_id}, "
                f"property {property_id}, lease {lease_interval_id} (snapshot not found or unavailable)"
            )
        
        # Get all buckets for this lease - exceptions and matches separately
        lease_buckets = bucket_results[
            (bucket_results[CanonicalField.STATUS.value] != config.reconciliation.status_matched)
        ].copy()
        
        # Get matched buckets for this lease
        matched_buckets = bucket_results[
            (bucket_results[CanonicalField.STATUS.value] == config.reconciliation.status_matched)
        ].copy()
        
        # Keep all exceptions including resolved ones for lease detail view
        # Month-level status information will be merged later to show resolution state
        logger.info(f"[LEASE_VIEW] Found {len(lease_buckets)} exception buckets for lease {lease_interval_id} (including any resolved)")
        
        # Get expected and actual detail for this lease
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
        
        # Calculate lease totals (temporary values; final unresolved totals are
        # recalculated after month-level resolved statuses are merged below)
        total_expected = sum(g['total_expected'] for g in grouped_list)
        total_actual = sum(g['total_actual'] for g in grouped_list)
        total_variance = total_actual - total_expected
        total_undercharge = 0
        total_overcharge = 0
        
        # Resolve from run-scoped lookup so lease pages stay aligned with portfolio/property naming.
        property_name = _resolve_property_name_for_run(run_id, property_id, cache_token)
        
        # Get customer name and IDs from lease records
        customer_name = None
        customer_id = None
        lease_id = None

        def _normalize_person_name(value):
            if value is None:
                return ""
            text = str(value).strip()
            if not text or text.lower() == 'nan':
                return ""
            return text

        def _row_is_guarantor(row):
            customer_value = _normalize_person_name(row.get(CanonicalField.CUSTOMER_NAME.value))
            guarantor_value = _normalize_person_name(row.get(CanonicalField.GUARANTOR_NAME.value))
            if not customer_value or not guarantor_value:
                return False
            return customer_value.casefold() == guarantor_value.casefold()

        def _resolve_primary_customer_name(*frames):
            # Resolve within each source independently, honoring source priority order.
            for frame in frames:
                if frame is None or frame.empty:
                    continue
                if CanonicalField.CUSTOMER_NAME.value not in frame.columns:
                    continue

                ranked_names = []
                fallback_names = []
                for _, row in frame.iterrows():
                    name_value = _normalize_person_name(row.get(CanonicalField.CUSTOMER_NAME.value))
                    if not name_value:
                        continue
                    fallback_names.append(name_value)
                    if not _row_is_guarantor(row):
                        ranked_names.append(name_value)

                candidate_names = ranked_names if ranked_names else fallback_names
                if not candidate_names:
                    continue

                # Mode by occurrence, stable on first appearance for ties.
                counts = {}
                first_seen_index = {}
                for index, name_value in enumerate(candidate_names):
                    key = name_value.casefold()
                    counts[key] = counts.get(key, 0) + 1
                    if key not in first_seen_index:
                        first_seen_index[key] = index

                winner_key = max(counts.keys(), key=lambda key: (counts[key], -first_seen_index[key]))
                for name_value in candidate_names:
                    if name_value.casefold() == winner_key:
                        return name_value
                return candidate_names[0]

            return None

        # Prefer lease-details (scheduled/expected) tenant name over AR-transactions tenant name.
        customer_name = _resolve_primary_customer_name(lease_expected, lease_actual)
        
        if len(lease_actual) > 0:
            if CanonicalField.CUSTOMER_ID.value in lease_actual.columns:
                customer_id_value = lease_actual[CanonicalField.CUSTOMER_ID.value].iloc[0]
                if pd.notna(customer_id_value):
                    customer_id = int(customer_id_value)
            
            if CanonicalField.LEASE_ID.value in lease_actual.columns:
                lease_id_value = lease_actual[CanonicalField.LEASE_ID.value].iloc[0]
                if pd.notna(lease_id_value):
                    lease_id = int(lease_id_value)
        
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
        
        # Load per-month exception states from SharePoint
        # This allows tracking which specific months have been resolved
        ar_month_states = {}  # {ar_code_id: {month: {status, fix_label, ...}}}
        
        for ar_code_id in ar_code_unified.keys():
            exception_months = storage.load_exception_months_from_sharepoint_list(
                run_id, int(float(property_id)), int(float(lease_interval_id)), ar_code_id
            )
            
            logger.info(f"[LEASE_VIEW] Loaded {len(exception_months)} exception months from SharePoint for AR Code {ar_code_id}")
            
            ar_month_states[ar_code_id] = {}
            for month_record in exception_months:
                audit_month = month_record.get('audit_month')
                logger.info(f"[LEASE_VIEW] SharePoint month record: {audit_month} -> Status: {month_record.get('status')}, Fix: {month_record.get('fix_label')}")
                normalized_month = _normalize_audit_month(audit_month)
                ar_month_states[ar_code_id][normalized_month] = month_record
        
        # Merge month-level resolution statuses into monthly details
        for ar_code_id, ar_data in ar_code_unified.items():
            month_states = ar_month_states.get(ar_code_id, {})
            
            logger.info(f"[LEASE_VIEW] Merging states for AR Code {ar_code_id}, has {len(month_states)} states from SharePoint")
            
            for monthly in ar_data['monthly_details']:
                audit_month = monthly['audit_month']
                audit_month_normalized = _normalize_audit_month(audit_month)
                logger.debug(
                    f"[LEASE_VIEW] Looking for match: monthly audit_month={audit_month}, "
                    f"normalized={audit_month_normalized}, type={type(audit_month)}"
                )

                month_state = month_states.get(audit_month_normalized)
                
                # If this month has exception state data, overlay it
                if month_state:
                    monthly['month_status'] = month_state.get('status', 'Open')
                    monthly['month_fix_label'] = month_state.get('fix_label', '')
                    monthly['month_action_type'] = month_state.get('action_type', '')
                    monthly['month_resolved_at'] = month_state.get('resolved_at', '')
                    monthly['month_resolved_by'] = month_state.get('resolved_by', '')
                    monthly['month_resolved_by_name'] = month_state.get('resolved_by_name', '')
                    monthly['is_historical'] = month_state.get('is_historical', False)
                    monthly['resolution_run_id'] = month_state.get('run_id', '')
                    logger.info(f"[LEASE_VIEW] Applied state to {audit_month}: status={monthly['month_status']}, fix={monthly['month_fix_label']}, historical={monthly.get('is_historical', False)}")
                else:
                    # No state saved yet - default to Open for exceptions, N/A for matched
                    if monthly['status'] != 'matched':
                        monthly['month_status'] = 'Open'
                        logger.debug(f"[LEASE_VIEW] No state found for {audit_month}, defaulting to Open")
                    else:
                        monthly['month_status'] = 'N/A'  # Matched months don't need resolution
                    monthly['month_fix_label'] = ''
                    monthly['month_action_type'] = ''
                    monthly['month_resolved_at'] = ''
                    monthly['month_resolved_by'] = ''
                    monthly['month_resolved_by_name'] = ''
                    monthly['is_historical'] = False
                    monthly['resolution_run_id'] = ''
            
            # Static count for this audit run:
            # - Include unresolved months
            # - Include months resolved during THIS run
            # - Exclude months already resolved in PREVIOUS runs (historical carry-forward)
            scoped_exception_months = [
                monthly for monthly in ar_data['monthly_details']
                if monthly.get('status') != 'matched'
                and not (monthly.get('month_status') == 'Resolved' and monthly.get('is_historical'))
            ]

            unresolved_count = sum(
                1 for monthly in scoped_exception_months
                if monthly.get('month_status') != 'Resolved'
            )
            total_exception_count = len(scoped_exception_months)

            ar_data['exception_count'] = total_exception_count
            ar_data['unresolved_exception_count'] = unresolved_count
            ar_data['total_exception_count'] = total_exception_count
            logger.info(
                f"[LEASE_VIEW] AR Code {ar_code_id}: {unresolved_count} unresolved, "
                f"{total_exception_count} static exceptions (historical resolutions excluded)"
            )
        
        # Calculate overall AR code status from scoped current-run exception months.
        # Status remains Open until ALL scoped exceptions for this AR code are resolved.
        ar_status_map = {}
        for ar_code_id, ar_data in ar_code_unified.items():
            total_months = int(ar_data.get('total_exception_count', 0) or 0)
            unresolved_months = int(ar_data.get('unresolved_exception_count', 0) or 0)
            resolved_months = max(0, total_months - unresolved_months)

            if total_months == 0:
                ar_status_map[ar_code_id] = {
                    'status': 'Passed',
                    'total_months': 0,
                    'resolved_months': 0,
                    'open_months': 0,
                    'status_label': 'Passed',
                }
            elif unresolved_months == 0:
                ar_status_map[ar_code_id] = {
                    'status': 'Resolved',
                    'total_months': total_months,
                    'resolved_months': resolved_months,
                    'open_months': 0,
                    'status_label': 'Resolved',
                }
            else:
                status_label = 'Open'
                if resolved_months > 0:
                    status_label = f"Open ({resolved_months} of {total_months} resolved)"

                ar_status_map[ar_code_id] = {
                    'status': 'Open',
                    'total_months': total_months,
                    'resolved_months': resolved_months,
                    'open_months': unresolved_months,
                    'status_label': status_label,
                }
        
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
            
            # Determine overall status from calculated month statuses
            ar_code_id = ar_data['ar_code_id']
            has_exceptions = ar_data.get('has_exceptions', False)
            
            # Only AR codes with NO exceptions should be "Passed"
            if not has_exceptions:
                ar_data['status_label'] = 'Passed'
                ar_data['status_color'] = 'light-success'
                logger.info(f"[AR_STATUS] AR Code {ar_code_id} has no exceptions - setting to Passed")
            elif ar_code_id in ar_status_map:
                status_info = ar_status_map[ar_code_id]
                overall_status = status_info['status']
                
                logger.info(f"[AR_STATUS] AR Code {ar_code_id}: status_info={status_info}, overall_status={overall_status}")
                
                if overall_status == 'Resolved':
                    ar_data['status_label'] = 'Resolved'
                    ar_data['status_color'] = 'success'
                    logger.info(f"[AR_STATUS] Setting AR Code {ar_code_id} to Resolved")
                else:  # Open (with or without progress)
                    # Show progress: "Open" or "Open (2 of 4 resolved)"
                    ar_data['status_label'] = status_info['status_label']
                    ar_data['status_color'] = 'danger'
                    logger.info(f"[AR_STATUS] Setting AR Code {ar_code_id} to {status_info['status_label']}")
            else:
                # Fallback: has exceptions but no status calculated - default to Open
                ar_data['status_label'] = 'Open'
                ar_data['status_color'] = 'danger'
                logger.info(f"[AR_STATUS] AR Code {ar_code_id} has exceptions but no saved months - setting to Open")
            
            all_ar_codes.append(ar_data)
        
        # Sort by status priority (Open > Resolved > Passed), then by exceptions (highest to lowest), then by AR code ID
        status_sort_order = {'Open': 0, 'Resolved': 1, 'Passed': 2}
        all_ar_codes = sorted(all_ar_codes, key=lambda x: (status_sort_order.get(x['status_label'], 99), -x['exception_count'], x['ar_code_id']))

        lease_only_expectations = []
        lease_mapping_diagnostics = {}
        try:
            lease_key = f"{int(float(property_id))}:{int(float(lease_interval_id))}"

            lease_term_period_start = None
            lease_term_period_end = None

            audit_period_meta = run_metadata.get('audit_period') if isinstance(run_metadata, dict) else None
            audit_year = None
            audit_month = None
            if isinstance(audit_period_meta, dict):
                try:
                    audit_year = int(audit_period_meta.get('year')) if audit_period_meta.get('year') else None
                except Exception:
                    audit_year = None
                try:
                    audit_month = int(audit_period_meta.get('month')) if audit_period_meta.get('month') else None
                except Exception:
                    audit_month = None

            if audit_year and audit_month:
                lease_term_period_start = pd.Timestamp(year=audit_year, month=audit_month, day=1)
                lease_term_period_end = lease_term_period_start + pd.offsets.MonthEnd(1)
            elif audit_year:
                lease_term_period_start = pd.Timestamp(year=audit_year, month=1, day=1)
                lease_term_period_end = pd.Timestamp(year=audit_year, month=12, day=31)
            else:
                audit_month_values = []
                for source_df in (lease_expected, lease_actual):
                    if source_df is None or source_df.empty:
                        continue
                    if CanonicalField.AUDIT_MONTH.value not in source_df.columns:
                        continue
                    audit_month_values.append(source_df[CanonicalField.AUDIT_MONTH.value])

                if audit_month_values:
                    combined_months = pd.concat(audit_month_values, ignore_index=True)
                    parsed_months = pd.to_datetime(combined_months, errors='coerce').dropna()
                    if not parsed_months.empty:
                        lease_term_period_start = parsed_months.min().replace(day=1)
                        lease_term_period_end = parsed_months.max() + pd.offsets.MonthEnd(0)

            refresh_ttl_hours = int(os.getenv('LEASE_TERM_REFRESH_TTL_HOURS', '24'))
            force_refresh = os.getenv('LEASE_TERM_FORCE_REFRESH', 'false').lower() == 'true'

            refresh_result = refresh_lease_terms_for_lease_interval(
                storage_service=storage,
                property_id=int(float(property_id)),
                lease_interval_id=int(float(lease_interval_id)),
                lease_id=lease_id,
                run_id=run_id,
                audit_period_start=lease_term_period_start.isoformat() if lease_term_period_start is not None else None,
                audit_period_end=lease_term_period_end.isoformat() if lease_term_period_end is not None else None,
                force_refresh=force_refresh,
                min_recheck_hours=refresh_ttl_hours,
            )

            lease_terms_df = refresh_result.get('terms_df')
            if isinstance(lease_terms_df, pd.DataFrame) and not lease_terms_df.empty:
                lease_term_records = lease_terms_df.to_dict(orient='records')
            else:
                lease_terms_df = storage.load_lease_terms_for_lease_key_from_sharepoint_list(lease_key)
                lease_term_records = lease_terms_df.to_dict(orient='records') if not lease_terms_df.empty else []

            if not lease_term_records:
                lease_term_records = (
                    run_metadata.get('lease_terms_extracted')
                    or run_metadata.get('lease_terms')
                    or run_metadata.get('entrata_lease_terms')
                    or []
                )

            if isinstance(lease_term_records, str):
                try:
                    lease_term_records = json.loads(lease_term_records)
                except Exception:
                    lease_term_records = []

            if str(lease_interval_id) == "18250886":
                try:
                    refresh_status = (refresh_result or {}).get('status') if isinstance(refresh_result, dict) else None
                    logger.info(
                        "[LEASE TRACE] lease_interval_id=18250886 lease_key=%s refresh_status=%s term_count=%s",
                        lease_key,
                        refresh_status,
                        len(lease_term_records) if isinstance(lease_term_records, list) else 0,
                    )

                    if isinstance(lease_term_records, list):
                        parking_terms = []
                        for term in lease_term_records:
                            if not isinstance(term, dict):
                                continue
                            term_type = str(term.get('term_type') or '').upper()
                            mapped_code = str(term.get('mapped_ar_code') or '').strip()
                            if term_type == 'PARKING' or mapped_code in {'155052', '155385'}:
                                parking_terms.append({
                                    'term_key': term.get('term_key'),
                                    'term_type': term.get('term_type'),
                                    'mapped_ar_code': term.get('mapped_ar_code'),
                                    'amount': term.get('amount'),
                                    'frequency': term.get('frequency'),
                                    'term_source_doc_id': term.get('term_source_doc_id'),
                                    'term_source_doc_name': term.get('term_source_doc_name'),
                                    'start_date': term.get('start_date'),
                                    'end_date': term.get('end_date'),
                                })

                        logger.info(
                            "[LEASE TRACE] lease_interval_id=18250886 parking_terms=%s",
                            parking_terms,
                        )
                except Exception as lease_trace_error:
                    logger.warning("[LEASE TRACE] Failed parking trace for 18250886: %s", lease_trace_error)

            overlay = build_lease_expectation_overlay(all_ar_codes, lease_term_records)
            all_ar_codes = overlay.get('ar_groups', all_ar_codes)
            lease_only_expectations = overlay.get('lease_only_expectations', [])
            lease_mapping_diagnostics = overlay.get('mapping_diagnostics', {})
        except Exception as lease_overlay_error:
            logger.warning(f"[LEASE_VIEW] Lease expectation overlay skipped: {lease_overlay_error}")

        # Recalculate lease summary totals from unresolved exception months only.
        # Keep undercharge and overcharge independent (do not net them).
        unresolved_exception_months = []
        for ar_data in all_ar_codes:
            for monthly in ar_data.get('monthly_details', []):
                if monthly.get('status') == 'matched':
                    continue
                if monthly.get('month_status') == 'Resolved':
                    continue
                unresolved_exception_months.append(monthly)

        total_undercharge = sum(
            max(0, float(monthly.get('expected_total', 0) or 0) - float(monthly.get('actual_total', 0) or 0))
            for monthly in unresolved_exception_months
        )
        total_overcharge = sum(
            max(0, float(monthly.get('actual_total', 0) or 0) - float(monthly.get('expected_total', 0) or 0))
            for monthly in unresolved_exception_months
        )

        total_expected = sum(float(monthly.get('expected_total', 0) or 0) for monthly in unresolved_exception_months)
        total_actual = sum(float(monthly.get('actual_total', 0) or 0) for monthly in unresolved_exception_months)
        total_variance = total_actual - total_expected

        if lease_snapshot:
            total_undercharge = float(lease_snapshot.get('undercharge', total_undercharge) or 0)
            total_overcharge = float(lease_snapshot.get('overcharge', total_overcharge) or 0)

        # Variance aligns with unresolved-month totals shown in lease workflow rows.
        
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
            metadata=run_metadata,
            exceptions=grouped_list,
            exception_count=len(grouped_list),
            matched_records=matched_list,
            matched_count=len(matched_list),
            all_ar_codes=all_ar_codes,
            lease_only_expectations=lease_only_expectations,
            lease_mapping_diagnostics=lease_mapping_diagnostics,
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
        run_data = cached_load_run(run_id, _session_cache_token())
        
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
