from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests

from .mappings import ARSourceColumns, ScheduledSourceColumns
from activity_logging.sharepoint import _get_app_only_token

# ---------------------------------------------------------------------------
# Entrata environment switching (prod / sandbox)
# ---------------------------------------------------------------------------
_ENTRATA_ENV_CONFIG_PATH = Path(__file__).parent.parent / "entrata_environment.json"


def get_entrata_environment() -> str:
    """Return the active Entrata environment: 'prod' or 'sandbox'."""
    try:
        data = json.loads(_ENTRATA_ENV_CONFIG_PATH.read_text(encoding="utf-8"))
        env = str(data.get("environment", "prod")).lower()
        return env if env in {"prod", "sandbox"} else "prod"
    except Exception:
        return "prod"


def set_entrata_environment(env: str) -> None:
    """Persist the active Entrata environment ('prod' or 'sandbox') to config."""
    if env not in {"prod", "sandbox"}:
        raise ValueError(f"Invalid environment '{env}'. Must be 'prod' or 'sandbox'.")
    _ENTRATA_ENV_CONFIG_PATH.write_text(
        json.dumps({"environment": env}, indent=2), encoding="utf-8"
    )


def _resolve_api_credentials() -> tuple[str, str, str, str]:
    """Return (details_url, ar_url, api_key, api_key_header) for the active environment."""
    env = get_entrata_environment()

    if env == "sandbox":
        details_url = (
            _to_str(os.getenv("LEASE_API_SANDBOX_DETAILS_URL"))
            or _to_str(os.getenv("LEASE_API_SANDBOX_BASE_URL"))
            or "https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/leases?page_no=1&per_page=100"
        )
        ar_url = (
            _to_str(os.getenv("LEASE_API_SANDBOX_AR_URL"))
            or _to_str(os.getenv("LEASE_API_SANDBOX_BASE_URL"))
            or "https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/artransactions?page_no=1&per_page=100"
        )
        api_key = _to_str(os.getenv("LEASE_API_SANDBOX_KEY"))
    else:
        details_url = (
            _to_str(os.getenv("LEASE_API_DETAILS_URL"))
            or _to_str(os.getenv("LEASE_API_BASE_URL"))
            or "https://apis.entrata.com/ext/orgs/peakmade/v1/leases?page_no=1&per_page=100"
        )
        ar_url = (
            _to_str(os.getenv("LEASE_API_AR_URL"))
            or _to_str(os.getenv("LEASE_API_BASE_URL"))
            or "https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions?page_no=1&per_page=100"
        )
        api_key = _to_str(os.getenv("LEASE_API_KEY"))

    api_key_header = _to_str(os.getenv("LEASE_API_KEY_HEADER") or "X-Api-Key")
    return details_url, ar_url, api_key, api_key_header


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _contains_deleted_never_posted_marker(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_contains_deleted_never_posted_marker(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_deleted_never_posted_marker(item) for item in value)

    text = _to_str(value).lower()
    return bool(text) and ("deleted" in text and "never posted" in text)


def _to_yyyymmdd_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return int(parsed.strftime("%Y%m%d"))


def _to_mmddyyyy(value: Any) -> str:
    if value is None or value == "":
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%m/%d/%Y")


def _parse_money(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if text == "":
        return None

    # Support accounting-style negatives, e.g. "($70.00)" -> -70.00
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1].strip()}"

    try:
        return float(text)
    except Exception:
        return None


def _post_method(
    endpoint_url: str,
    api_key: str,
    api_key_header: str,
    method_name: str,
    version: str,
    params: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    method_payload: dict[str, Any] = {
        "name": method_name,
        "params": params,
    }
    if _to_str(version):
        method_payload["version"] = version

    request_body = {
        "auth": {"type": "apikey"},
        "requestId": str(int(datetime.utcnow().timestamp() * 1000)),
        "method": method_payload,
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if api_key:
        headers[api_key_header] = api_key
        headers.setdefault("X-Api-Key", api_key)

    response = requests.post(endpoint_url, headers=headers, json=request_body, timeout=timeout_seconds)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        response_snippet = (response.text or "")[:500]
        raise ValueError(
            f"{method_name} HTTP {response.status_code} calling {endpoint_url}. "
            f"Response: {response_snippet}"
        ) from exc

    payload = response.json()
    if isinstance(payload, dict):
        top_code = payload.get("code")
        nested_code = ((payload.get("response") or {}).get("code"))
        resolved_code = top_code if top_code is not None else nested_code
        if resolved_code is not None and int(resolved_code or 0) != 200:
            raise ValueError(f"{method_name} failed: {payload}")
    return payload


def _graph_get_json(url: str, access_token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, params=params, timeout=30)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        snippet = (response.text or "")[:500]
        raise ValueError(f"SharePoint Graph GET failed ({response.status_code}) for {url}. Response: {snippet}") from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected Graph payload type for {url}: {type(payload)}")
    return payload


def _resolve_sharepoint_site_id(access_token: str, sharepoint_site_url: str) -> str:
    parsed = urlparse(sharepoint_site_url)
    hostname = _to_str(parsed.hostname)
    site_path = _to_str(parsed.path)
    if not hostname or not site_path:
        raise ValueError("Invalid SHAREPOINT_SITE_URL for property picklist")

    endpoint = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    payload = _graph_get_json(endpoint, access_token)
    site_id = _to_str(payload.get("id"))
    if not site_id:
        raise ValueError("Unable to resolve SharePoint site id for property picklist")
    return site_id


def _resolve_sharepoint_list_id(access_token: str, site_id: str, list_name: str) -> str:
    endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    payload = _graph_get_json(
        endpoint,
        access_token,
        params={"$filter": f"displayName eq '{list_name}'", "$select": "id,displayName"},
    )
    rows = payload.get("value") if isinstance(payload.get("value"), list) else []
    if not rows:
        raise ValueError(f"SharePoint list '{list_name}' not found")
    list_id = _to_str(rows[0].get("id"))
    if not list_id:
        raise ValueError(f"Unable to resolve list id for '{list_name}'")
    return list_id


def _to_legacy_entrata_property_id(value: Any) -> str:
    token = _to_str(value)
    if not token:
        return ""
    try:
        numeric = float(token)
        if numeric.is_integer():
            return str(int(numeric))
    except Exception:
        pass
    return token


def fetch_entrata_property_picklist() -> list[dict[str, str]]:
    """Fetch property id/name pairs from SharePoint Properties_0 for API upload picklist."""
    sharepoint_site_url = _to_str(os.getenv("SHAREPOINT_SITE_URL"))
    list_name = _to_str(os.getenv("LEASE_API_PROPERTIES_SHAREPOINT_LIST") or "Properties_0")
    require_reportable = _to_str(os.getenv("LEASE_API_PROPERTIES_REQUIRE_REPORTABLE") or "1").lower() not in {"0", "false", "no"}

    if not sharepoint_site_url:
        raise ValueError("Missing SHAREPOINT_SITE_URL env var")

    access_token = _get_app_only_token()
    if not access_token:
        raise ValueError("Unable to acquire app-only token for SharePoint property picklist")

    site_id = _resolve_sharepoint_site_id(access_token, sharepoint_site_url)
    list_id = _resolve_sharepoint_list_id(access_token, site_id, list_name)

    select_fields = [
        "PROPERTY_NAME",
        "PropertyName",
        "LEGACY_ENTRATA_ID",
        "LegacyEntrataId",
    ]
    endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {
        "$expand": f"fields($select={','.join(select_fields)})",
        "$top": "5000",
    }

    rows: list[dict[str, str]] = []
    next_url: str | None = endpoint
    next_params: dict[str, Any] | None = params
    while next_url:
        page_payload = _graph_get_json(next_url, access_token, params=next_params)
        for item in page_payload.get("value") or []:
            fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
            property_name = _to_str(fields.get("PROPERTY_NAME") or fields.get("PropertyName"))
            property_id = _to_legacy_entrata_property_id(fields.get("LEGACY_ENTRATA_ID") or fields.get("LegacyEntrataId"))

            # Skip if missing required fields or if require_reportable and no LEGACY_ENTRATA_ID
            if require_reportable and not property_id:
                continue
            if not property_id or not property_name:
                continue

            rows.append({
                "property_id": property_id,
                "property_name": property_name,
            })

        next_url = _to_str(page_payload.get("@odata.nextLink") or "") or None
        next_params = None

    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        pid = _to_str(row.get("property_id"))
        if not pid or pid in deduped:
            continue
        deduped[pid] = {
            "property_id": pid,
            "property_name": _to_str(row.get("property_name")),
        }

    result_rows = list(deduped.values())

    # In sandbox mode, replace property IDs with sandbox-specific IDs where defined.
    if get_entrata_environment() == "sandbox":
        sandbox_map = _load_sandbox_property_id_map()
        if sandbox_map:
            filtered: list[dict[str, str]] = []
            for row in result_rows:
                name = row.get("property_name", "")
                sandbox_id = sandbox_map.get(name)
                if sandbox_id:
                    filtered.append({"property_id": sandbox_id, "property_name": name})
            # Only return properties that have a sandbox ID defined
            result_rows = filtered

    result_rows.sort(key=lambda item: (item.get("property_name", "").lower(), item.get("property_id", "")))
    return result_rows


def _load_sandbox_property_id_map() -> dict[str, str]:
    """Load the property-name -> sandbox-property-id mapping from sandbox_property_ids.json."""
    config_path = Path(__file__).parent.parent / "sandbox_property_ids.json"
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _extract_lease_nodes(details_payload: dict[str, Any]) -> list[dict[str, Any]]:
    root = details_payload.get("response") if isinstance(details_payload.get("response"), dict) else details_payload
    leases = (((root.get("result") or {}).get("leases") or {}).get("lease"))
    return [item for item in _as_list(leases) if isinstance(item, dict)]


def _extract_customer_name(lease_node: dict[str, Any]) -> str:
    customer_name, _, _ = _extract_customer_fields(lease_node)
    return customer_name


def _extract_customer_nodes(lease_node: dict[str, Any]) -> list[dict[str, Any]]:
    customers_node = (((lease_node.get("customers") or {}).get("customer")))
    return [item for item in _as_list(customers_node) if isinstance(item, dict)]


def _customer_full_name(customer: dict[str, Any]) -> str:
    first = _to_str(customer.get("firstName"))
    last = _to_str(customer.get("lastName"))
    full = " ".join(part for part in [first, last] if part).strip()
    if full:
        return full
    return _to_str(customer.get("name"))


def _is_guarantor_customer(customer: dict[str, Any]) -> bool:
    marker_values = [
        _to_str(customer.get("type")).lower(),
        _to_str(customer.get("customerType")).lower(),
        _to_str(customer.get("customerTypeName")).lower(),
        _to_str(customer.get("relationship")).lower(),
        _to_str(customer.get("relationshipType")).lower(),
        _to_str(customer.get("relationshipTypeName")).lower(),
        _to_str(customer.get("occupancyType")).lower(),
        _to_str(customer.get("occupancyStatus")).lower(),
    ]
    marker_text = " ".join(value for value in marker_values if value)
    if "guarant" in marker_text:
        return True

    explicit_flags = [
        customer.get("isGuarantor"),
        customer.get("guarantor"),
        customer.get("isGuarantorSigner"),
    ]
    for flag in explicit_flags:
        if str(flag).strip().lower() in {"1", "true", "yes", "y"}:
            return True
    return False


def _extract_customer_fields(lease_node: dict[str, Any]) -> tuple[str, str, str]:
    """Return (tenant_name, tenant_id, guarantor_name) from lease payload."""
    lease_name = _to_str(lease_node.get("name"))
    customers = _extract_customer_nodes(lease_node)

    if not customers:
        return lease_name, "", ""

    guarantor_customers = [customer for customer in customers if _is_guarantor_customer(customer)]
    tenant_candidates = [customer for customer in customers if not _is_guarantor_customer(customer)]

    tenant_customer = tenant_candidates[0] if tenant_candidates else None
    guarantor_customer = guarantor_customers[0] if guarantor_customers else None

    tenant_name = _customer_full_name(tenant_customer) if tenant_customer else ""
    tenant_id = _to_str((tenant_customer or {}).get("id"))
    guarantor_name = _customer_full_name(guarantor_customer) if guarantor_customer else ""

    if not tenant_name:
        tenant_name = lease_name
    if not tenant_name and customers:
        tenant_name = _customer_full_name(customers[0])
    if not tenant_id and customers:
        tenant_id = _to_str(customers[0].get("id"))

    return tenant_name, tenant_id, guarantor_name


def _extract_charges_from_interval(interval_node: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    active = (((interval_node.get("activeScheduledCharges") or {}).get("activeScheduledCharge")))
    past = (((interval_node.get("pastScheduledCharges") or {}).get("pastScheduledCharge")))
    direct = (((interval_node.get("charges") or {}).get("charge")))

    installment_nodes = [
        item for item in _as_list((((interval_node.get("installments") or {}).get("installment"))))
        if isinstance(item, dict)
    ]
    installment_charges: list[dict[str, Any]] = []
    for installment in installment_nodes:
        charges = (((installment.get("charges") or {}).get("charge")))
        for charge in _as_list(charges):
            if isinstance(charge, dict):
                merged = dict(charge)
                merged.setdefault("installmentStartDate", installment.get("installmentStartDate"))
                merged.setdefault("installmentEndDate", installment.get("installmentEndDate"))
                installment_charges.append(merged)

    seen_signatures: set[tuple[str, str, str, str, str, str, str]] = set()

    def _append_charge(charge: dict[str, Any]) -> None:
        signature = (
            _to_str(charge.get("scheduledChargeId") or charge.get("scheduledChargeID") or charge.get("id")),
            _to_str(charge.get("arCodeId")),
            _to_str(charge.get("chargeCode")),
            _to_str(charge.get("chargeTiming")),
            _to_str(charge.get("amount")),
            _to_str(charge.get("chargeStartDate") or charge.get("installmentStartDate")),
            _to_str(charge.get("chargeEndDate") or charge.get("installmentEndDate")),
        )
        if signature in seen_signatures:
            return
        seen_signatures.add(signature)
        rows.append(charge)

    for source in [active, past, direct]:
        for charge in _as_list(source):
            if isinstance(charge, dict):
                _append_charge(charge)

    for charge in installment_charges:
        _append_charge(charge)
    return rows


def _is_deleted_never_posted(value: Any) -> bool:
    return _contains_deleted_never_posted_marker(value)


def _resolve_scheduled_posting_marker(charge: dict[str, Any], interval_node: dict[str, Any] | None = None) -> str:
    candidates = [
        charge.get("postedThrough"),
        charge.get("posted_through"),
        charge.get("postedThroughDate"),
        charge.get("lastPosted"),
        charge.get("last_posted"),
    ]

    if interval_node:
        candidates.extend([
            interval_node.get("postedThrough"),
            interval_node.get("posted_through"),
            interval_node.get("postedThroughDate"),
            interval_node.get("lastPosted"),
            interval_node.get("last_posted"),
        ])

    for candidate in candidates:
        text = _to_str(candidate)
        if text:
            return text

    return ""


def _charge_is_deleted_never_posted(charge: dict[str, Any], interval_node: dict[str, Any] | None = None) -> bool:
    if _contains_deleted_never_posted_marker(charge):
        return True
    if interval_node and _contains_deleted_never_posted_marker(interval_node):
        return True
    return _is_deleted_never_posted(_resolve_scheduled_posting_marker(charge, interval_node=interval_node))


def _is_one_time_charge(charge: dict[str, Any], interval_node: dict[str, Any] | None = None) -> bool:
    timing = _to_str(charge.get("chargeTiming")).lower()
    if "monthly" in timing:
        return False

    one_time_markers = [
        "one time",
        "one-time",
        "onetime",
        "renewal start",
        "application completed",
        "move in",
        "move-in",
    ]
    if any(marker in timing for marker in one_time_markers):
        return True

    interval_bucket = _to_str((interval_node or {}).get("_interval_charge_bucket")).lower()
    if interval_bucket == "one_time":
        return True
    if interval_bucket == "recurring":
        return False

    # Strong recurring evidence from explicit date span on the charge itself.
    # If a charge has a multi-day start/end range, treat it as recurring even when
    # chargeTiming/interval bucket metadata is inconsistent.
    raw_start = charge.get("chargeStartDate") or charge.get("installmentStartDate")
    raw_end = charge.get("chargeEndDate") or charge.get("installmentEndDate")
    start_dt = pd.to_datetime(raw_start, errors="coerce")
    end_dt = pd.to_datetime(raw_end, errors="coerce")
    if pd.notna(start_dt) and pd.notna(end_dt):
        span_days = (end_dt - start_dt).days
        if span_days >= 7:
            return False

    # Last resort fallback to prior behavior.
    return "monthly" not in timing


def _build_scheduled_df(property_id: int, property_name: str, details_payload: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    output_rows: list[dict[str, Any]] = []
    lease_ids: list[str] = []

    for lease_node in _extract_lease_nodes(details_payload):
        lease_id = _to_str(lease_node.get("leaseId") or lease_node.get("id"))
        if lease_id and lease_id not in lease_ids:
            lease_ids.append(lease_id)

        customer_name, customer_id, guarantor_name = _extract_customer_fields(lease_node)

        scheduled_node = lease_node.get("scheduledCharges") or {}
        interval_nodes: list[dict[str, Any]] = []
        for item in _as_list(scheduled_node.get("recurringCharge")):
            if isinstance(item, dict):
                tagged = dict(item)
                tagged["_interval_charge_bucket"] = "recurring"
                interval_nodes.append(tagged)
        for item in _as_list(scheduled_node.get("oneTimeCharge")):
            if isinstance(item, dict):
                tagged = dict(item)
                tagged["_interval_charge_bucket"] = "one_time"
                interval_nodes.append(tagged)

        for interval_index, interval_node in enumerate(interval_nodes, start=1):
            lease_interval_status = _to_str(interval_node.get("leaseIntervalStatus")).lower()
            if lease_interval_status == "cancelled":
                continue

            lease_interval_id = _to_str(interval_node.get("leaseIntervalId"))
            lease_start = _to_mmddyyyy(interval_node.get("leaseStartDate"))
            lease_end = _to_mmddyyyy(interval_node.get("leaseEndDate"))

            charges = _extract_charges_from_interval(interval_node)
            for charge_index, charge in enumerate(charges, start=1):
                posted_through_value = _resolve_scheduled_posting_marker(charge, interval_node=interval_node)
                if _charge_is_deleted_never_posted(charge, interval_node=interval_node):
                    continue

                amount = _parse_money(charge.get("amount"))
                if amount is None:
                    continue

                charge_start = _to_mmddyyyy(
                    charge.get("chargeStartDate") or charge.get("installmentStartDate") or lease_start
                )

                explicit_charge_end = charge.get("chargeEndDate") or charge.get("installmentEndDate")
                is_one_time = _is_one_time_charge(charge, interval_node=interval_node)
                explicit_charge_end_normalized = _to_mmddyyyy(explicit_charge_end)

                # One-time charges should remain scoped to a single audit month even when
                # Entrata also emits a lease-length explicit end date on the charge record.
                if is_one_time:
                    charge_end = ""
                # Some Entrata payloads use textual sentinels (e.g. "End During Move-Out")
                # for recurring charges. Preserve recurring behavior by falling back to
                # lease_end when the explicit end cannot be parsed as a date.
                elif explicit_charge_end_normalized:
                    charge_end = explicit_charge_end_normalized
                else:
                    charge_end = lease_end

                # Guard bad upstream ranges: recurring charges with end before start
                # should default to lease_end rather than collapsing/vanishing.
                start_dt = pd.to_datetime(charge_start, errors="coerce")
                end_dt = pd.to_datetime(charge_end, errors="coerce")
                if pd.notna(start_dt) and pd.notna(end_dt) and end_dt < start_dt:
                    if is_one_time:
                        charge_end = ""
                    else:
                        charge_end = lease_end

                if str(os.getenv("LEASE_API_DEBUG_PARKING") or "").strip().lower() == "true":
                    ar_code_token = _to_str(charge.get("arCodeId"))
                    if ar_code_token == "155052":
                        debug_payload = {
                            "leaseId": lease_id,
                            "leaseIntervalId": lease_interval_id,
                            "intervalBucket": _to_str(interval_node.get("_interval_charge_bucket")),
                            "chargeId": _to_str(charge.get("id") or charge.get("scheduledChargeId")),
                            "amount": _to_str(charge.get("amount")),
                            "chargeTiming": _to_str(charge.get("chargeTiming")),
                            "chargeStartDate": _to_str(charge.get("chargeStartDate") or charge.get("installmentStartDate")),
                            "chargeEndDateRaw": _to_str(explicit_charge_end),
                            "leaseStartDate": lease_start,
                            "leaseEndDate": lease_end,
                            "postedThrough": _to_str(posted_through_value),
                            "lastPosted": _to_str(charge.get("lastPosted") or interval_node.get("lastPosted")),
                            "isOneTimeDerived": bool(is_one_time),
                            "finalChargeStart": charge_start,
                            "finalChargeEnd": charge_end,
                        }
                        print(f"[LEASE API DEBUG][PARKING] {debug_payload}")

                raw_scheduled_charge_id = _to_str(
                    charge.get("scheduledChargeId")
                    or charge.get("id")
                    or charge.get("scheduledChargeID")
                )

                row_id = (
                    f"api-sc-{property_id}-{lease_id}-{lease_interval_id or interval_index}-"
                    f"{_to_str(charge.get('arCodeId') or '')}-{charge_index}"
                )
                output_rows.append({
                    ScheduledSourceColumns.ID: row_id,
                    ScheduledSourceColumns.SCHEDULED_CHARGE_ID: raw_scheduled_charge_id,
                    ScheduledSourceColumns.PROPERTY_ID: int(property_id),
                    ScheduledSourceColumns.LEASE_ID: lease_id,
                    ScheduledSourceColumns.LEASE_INTERVAL_ID: lease_interval_id,
                    ScheduledSourceColumns.AR_CODE_ID: _to_str(charge.get("arCodeId")),
                    ScheduledSourceColumns.AR_CODE_NAME: _to_str(charge.get("chargeCode") or charge.get("chargeUsage") or "Unknown"),
                    ScheduledSourceColumns.CHARGE_AMOUNT: float(amount),
                    ScheduledSourceColumns.CHARGE_START_DATE: charge_start,
                    ScheduledSourceColumns.CHARGE_END_DATE: charge_end,
                    ScheduledSourceColumns.GUARANTOR_NAME: guarantor_name,
                    ScheduledSourceColumns.CUSTOMER_NAME: customer_name,
                    ScheduledSourceColumns.CUSTOMER_ID: customer_id,
                    ScheduledSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL: 1,
                    ScheduledSourceColumns.POSTED_THROUGH_DATE: _to_str(posted_through_value),
                    "PROPERTY_NAME": property_name,
                })

    required_columns = [
        ScheduledSourceColumns.ID,
        ScheduledSourceColumns.SCHEDULED_CHARGE_ID,
        ScheduledSourceColumns.PROPERTY_ID,
        ScheduledSourceColumns.LEASE_ID,
        ScheduledSourceColumns.LEASE_INTERVAL_ID,
        ScheduledSourceColumns.AR_CODE_ID,
        ScheduledSourceColumns.AR_CODE_NAME,
        ScheduledSourceColumns.CHARGE_AMOUNT,
        ScheduledSourceColumns.CHARGE_START_DATE,
        ScheduledSourceColumns.CHARGE_END_DATE,
        ScheduledSourceColumns.GUARANTOR_NAME,
        ScheduledSourceColumns.CUSTOMER_NAME,
        ScheduledSourceColumns.CUSTOMER_ID,
        ScheduledSourceColumns.POSTED_THROUGH_DATE,
    ]

    df = pd.DataFrame(output_rows)
    if df.empty:
        df = pd.DataFrame(columns=required_columns)
    else:
        for column in required_columns:
            if column not in df.columns:
                df[column] = ""

    return df, lease_ids


def _extract_ar_lease_nodes(ar_payload: dict[str, Any]) -> list[dict[str, Any]]:
    root = ar_payload.get("response") if isinstance(ar_payload.get("response"), dict) else ar_payload
    leases = (((root.get("result") or {}).get("leases") or {}).get("lease"))
    return [item for item in _as_list(leases) if isinstance(item, dict)]


def _build_ar_df(property_id: int, property_name: str, ar_payload: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for lease_node in _extract_ar_lease_nodes(ar_payload):
        lease_id = _to_str(lease_node.get("leaseId") or lease_node.get("id"))

        lease_property_id = _to_str(lease_node.get("propertyId"))
        if lease_property_id:
            try:
                if int(float(lease_property_id)) != int(property_id):
                    continue
            except Exception:
                pass

        customer_name, customer_id, guarantor_name = _extract_customer_fields(lease_node)

        ledger_nodes = [
            item for item in _as_list((((lease_node.get("ledgers") or {}).get("ledger"))))
            if isinstance(item, dict)
        ]

        for ledger in ledger_nodes:
            tx_nodes = _as_list((((ledger.get("transactions") or {}).get("transaction"))) )
            for tx in tx_nodes:
                if not isinstance(tx, dict):
                    continue

                tx_id = _to_str(tx.get("id"))
                ar_code_id = _to_str(tx.get("arCodeId"))
                amount = _parse_money(tx.get("amount"))
                if not tx_id or not ar_code_id or amount is None:
                    continue

                post_date_raw = tx.get("postDate") or tx.get("transactionDate")
                post_month_raw = tx.get("postMonth") or post_date_raw
                post_date = _to_yyyymmdd_int(post_date_raw)
                post_month = _to_yyyymmdd_int(post_month_raw)
                if post_date is None:
                    continue
                if post_month is None:
                    post_month = post_date

                description = _to_str(tx.get("description"))
                is_reversal = 1 if (
                    "reversal" in description.lower()
                    or _to_str(tx.get("originalArTransactionId"))
                    or _to_str(tx.get("reveseArTransactionId"))
                    or _to_str(tx.get("reverseArTransactionId"))
                ) else 0

                rows.append({
                    ARSourceColumns.PROPERTY_ID: int(property_id),
                    ARSourceColumns.PROPERTY_NAME: property_name,
                    ARSourceColumns.LEASE_ID: lease_id,
                    ARSourceColumns.LEASE_INTERVAL_ID: _to_str(tx.get("leaseIntervalId")),
                    ARSourceColumns.AR_CODE_ID: ar_code_id,
                    ARSourceColumns.AR_CODE_NAME: _to_str(tx.get("arCodeName") or "Unknown"),
                    ARSourceColumns.TRANSACTION_AMOUNT: float(amount),
                    ARSourceColumns.POST_DATE: int(post_date),
                    ARSourceColumns.POST_MONTH_DATE: int(post_month),
                    ARSourceColumns.IS_POSTED: 1,
                    ARSourceColumns.IS_DELETED: 0,
                    ARSourceColumns.IS_REVERSAL: is_reversal,
                    ARSourceColumns.ID: tx_id,
                    ARSourceColumns.CUSTOMER_NAME: customer_name,
                    ARSourceColumns.CUSTOMER_ID: customer_id,
                    ARSourceColumns.GUARANTOR_NAME: guarantor_name,
                    ARSourceColumns.SCHEDULED_CHARGE_ID: _to_str(tx.get("scheduledChargeId")),
                    ARSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL: 1,
                })

    required_columns = [
        ARSourceColumns.PROPERTY_ID,
        ARSourceColumns.PROPERTY_NAME,
        ARSourceColumns.LEASE_INTERVAL_ID,
        ARSourceColumns.AR_CODE_ID,
        ARSourceColumns.AR_CODE_NAME,
        ARSourceColumns.TRANSACTION_AMOUNT,
        ARSourceColumns.POST_DATE,
        ARSourceColumns.POST_MONTH_DATE,
        ARSourceColumns.IS_POSTED,
        ARSourceColumns.IS_DELETED,
        ARSourceColumns.IS_REVERSAL,
        ARSourceColumns.ID,
        ARSourceColumns.CUSTOMER_NAME,
        ARSourceColumns.GUARANTOR_NAME,
    ]

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=required_columns)
    else:
        for column in required_columns:
            if column not in df.columns:
                df[column] = ""

    return df


def _fetch_all_lease_details_pages(
    endpoint_url: str,
    api_key: str,
    api_key_header: str,
    method_name: str,
    version: str,
    params: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    """Call getLeaseDetails repeatedly until all pages are collected, then return a
    merged payload whose lease list contains every lease node across all pages."""
    parsed = urlparse(endpoint_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    per_page = int((qs.get("per_page") or ["100"])[0])

    all_lease_nodes: list[dict[str, Any]] = []
    first_payload: dict[str, Any] | None = None
    page_no = 1

    while True:
        qs["page_no"] = [str(page_no)]
        page_url = urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))

        payload = _post_method(
            endpoint_url=page_url,
            api_key=api_key,
            api_key_header=api_key_header,
            method_name=method_name,
            version=version,
            params=params,
            timeout_seconds=timeout_seconds,
        )

        if first_payload is None:
            first_payload = payload

        page_nodes = _extract_lease_nodes(payload)
        all_lease_nodes.extend(page_nodes)
        print(
            f"[LEASE API PAGINATION] page={page_no}, "
            f"leases_on_page={len(page_nodes)}, total_so_far={len(all_lease_nodes)}"
        )

        if len(page_nodes) < per_page:
            break
        page_no += 1

    if first_payload is None:
        return {}

    # Stitch all collected lease nodes back into a single merged payload so the
    # existing _build_scheduled_df / _extract_lease_nodes helpers work unchanged.
    merged = dict(first_payload)
    response = dict(merged.get("response") or {})
    result = dict(response.get("result") or {})
    leases = dict(result.get("leases") or {})
    leases["lease"] = all_lease_nodes
    result["leases"] = leases
    response["result"] = result
    merged["response"] = response
    return merged



def _is_other_income_lease_node(node: dict[str, Any]) -> bool:
    """Return True if this lease node is an Other Income lease.

    Other Income leases in Entrata have only leaseId + name and are missing
    the unit/floorplan fields present on every real residential lease.
    """
    return not node.get("unitTypeId") and not node.get("propertyUnitId")


def _strip_other_income_lease_nodes(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Remove Other Income lease nodes from the merged details payload.
    Returns (filtered_payload, count_removed).
    """
    all_nodes = _extract_lease_nodes(payload)
    kept = [n for n in all_nodes if not _is_other_income_lease_node(n)]
    dropped = len(all_nodes) - len(kept)

    if dropped:
        print(f"[OTHER INCOME FILTER] Removed {dropped} Other Income lease node(s) before processing")

    merged = dict(payload)
    response = dict(merged.get("response") or {})
    result = dict(response.get("result") or {})
    leases = dict(result.get("leases") or {})
    leases["lease"] = kept
    result["leases"] = leases
    response["result"] = result
    merged["response"] = response
    return merged, dropped


def fetch_property_api_sources(
    property_id: int,
    transaction_from_date: str | None = None,
    transaction_to_date: str | None = None,
) -> dict[str, Any]:
    details_url, ar_url, api_key, api_key_header = _resolve_api_credentials()
    env = get_entrata_environment()
    print(f"[API ENV] Using Entrata environment: {env}")

    if not details_url:
        raise ValueError("Missing LEASE_API_DETAILS_URL (or LEASE_API_BASE_URL) env var")
    if not ar_url:
        raise ValueError("Missing LEASE_API_AR_URL (or LEASE_API_BASE_URL) env var")
    if not api_key:
        raise ValueError("Missing LEASE_API_KEY (or LEASE_API_SANDBOX_KEY) env var")

    timeout_seconds = int(_to_str(os.getenv("LEASE_API_TIMEOUT_SECONDS") or "60") or "60")

    # Step 1: Fetch all lease detail pages.
    lease_details_payload = _fetch_all_lease_details_pages(
        endpoint_url=details_url,
        api_key=api_key,
        api_key_header=api_key_header,
        method_name=_to_str(os.getenv("LEASE_API_DETAILS_METHOD") or "getLeaseDetails"),
        version=_to_str(os.getenv("LEASE_API_DETAILS_VERSION") or "r2"),
        timeout_seconds=timeout_seconds,
        params={
            "propertyId": int(property_id),
            "includeAddOns": _to_str(os.getenv("LEASE_API_INCLUDE_ADDONS") or "0"),
            "includeCharge": _to_str(os.getenv("LEASE_API_INCLUDE_CHARGE") or "1"),
            "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_LEASE_STATUS_TYPE_IDS") or "3,4"),
        },
    )

    # Step 2: Strip Other Income lease nodes before anything is parsed or audited.
    lease_details_payload, _ = _strip_other_income_lease_nodes(lease_details_payload)

    lease_nodes = _extract_lease_nodes(lease_details_payload)
    property_name = ""
    if lease_nodes:
        property_name = _to_str(lease_nodes[0].get("propertyName"))
    if not property_name:
        property_name = f"Property {int(property_id)}"

    # Step 3: Build scheduled charges only from the already-filtered lease nodes.
    scheduled_df, lease_ids = _build_scheduled_df(int(property_id), property_name, lease_details_payload)

    # Step 4: Fetch AR transactions scoped to the surviving lease IDs.
    lease_ids_csv = ",".join(_to_str(item) for item in lease_ids if _to_str(item))

    ar_params: dict[str, Any] = {
        "propertyId": int(property_id),
        "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_AR_LEASE_STATUS_TYPE_IDS") or "3,4"),
        "transactionTypeIds": _to_str(os.getenv("LEASE_API_TRANSACTION_TYPE_IDS") or ""),
        "arCodeIds": _to_str(os.getenv("LEASE_API_AR_CODE_IDS") or ""),
        "showFullLedger": _to_str(os.getenv("LEASE_API_SHOW_FULL_LEDGER") or "1"),
        "residentFriendlyMode": _to_str(os.getenv("LEASE_API_RESIDENT_FRIENDLY_MODE") or "0"),
        "includeOtherIncomeLeases": _to_str(os.getenv("LEASE_API_INCLUDE_OTHER_INCOME_LEASES") or "0"),
        "includeReversals": _to_str(os.getenv("LEASE_API_INCLUDE_REVERSALS") or "1"),
        "ledgerIds": _to_str(os.getenv("LEASE_API_LEDGER_IDS") or ""),
    }
    if lease_ids_csv:
        ar_params["leaseIds"] = lease_ids_csv
    if transaction_from_date:
        ar_params["transactionFromDate"] = transaction_from_date
    if transaction_to_date:
        ar_params["transactionToDate"] = transaction_to_date

    ar_payload = _post_method(
        endpoint_url=ar_url,
        api_key=api_key,
        api_key_header=api_key_header,
        method_name=_to_str(os.getenv("LEASE_API_AR_METHOD") or "getLeaseArTransactions"),
        version=_to_str(os.getenv("LEASE_API_AR_VERSION") or "r1"),
        timeout_seconds=timeout_seconds,
        params=ar_params,
    )

    ar_df = _build_ar_df(int(property_id), property_name, ar_payload)

    return {
        "property_name": property_name,
        "scheduled_raw": scheduled_df,
        "ar_raw": ar_df,
        "lease_count": len(lease_ids),
    }


def fetch_single_lease_api_sources(
    lease_id: int,
    property_id: int = None,
    transaction_from_date: str | None = None,
    transaction_to_date: str | None = None,
) -> dict[str, Any]:
    """
    Fetch AR transactions and scheduled charges for a single lease from Entrata API.
    
    This is useful for debugging/analyzing one resident without fetching all property data.
    
    Args:
        lease_id: The lease ID to fetch
        property_id: Optional property ID (will be inferred from lease details if not provided)
        transaction_from_date: Optional start date filter for AR transactions (MM/DD/YYYY)
        transaction_to_date: Optional end date filter for AR transactions (MM/DD/YYYY)
    
    Returns:
        Dict with:
            - property_name: Property name
            - scheduled_raw: DataFrame of scheduled charges
            - ar_raw: DataFrame of AR transactions
            - lease_count: Always 1 for single lease fetch
    """
    details_url, ar_url, api_key, api_key_header = _resolve_api_credentials()
    env = get_entrata_environment()

    if not details_url:
        raise ValueError("Missing LEASE_API_DETAILS_URL (or LEASE_API_BASE_URL) env var")
    if not ar_url:
        raise ValueError("Missing LEASE_API_AR_URL (or LEASE_API_BASE_URL) env var")
    if not api_key:
        raise ValueError("Missing LEASE_API_KEY (or LEASE_API_SANDBOX_KEY) env var")

    timeout_seconds = int(_to_str(os.getenv("LEASE_API_TIMEOUT_SECONDS") or "60") or "60")

    print(f"\n{'='*80}")
    print(f"[SINGLE LEASE API] ===== STARTING API FETCH FOR LEASE {lease_id} (env={env}) =====")
    print(f"{'='*80}")
    print(f"[SINGLE LEASE API] Input Parameters:")
    print(f"  - lease_id: {lease_id}")
    print(f"  - property_id: {property_id or 'None (will be discovered)'}")
    print(f"  - transaction_from_date: {transaction_from_date or 'None'}")
    print(f"  - transaction_to_date: {transaction_to_date or 'None'}")
    print(f"[SINGLE LEASE API] API Endpoints:")
    print(f"  - Details URL: {details_url}")
    print(f"  - AR URL: {ar_url}")
    
    # If property_id not provided, we need to discover it first with a minimal call
    if property_id is None:
        print(f"\n[SINGLE LEASE API] STEP 1: Property ID Discovery")
        print(f"[SINGLE LEASE API] Making discovery API call to find property_id...")
        # Make initial call to discover property_id from the lease
        discovery_params = {
            "leaseIds": str(lease_id),
            "includeAddOns": "0",
            "includeCharge": "0",
            "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_LEASE_STATUS_TYPE_IDS") or "3,4"),
        }
        print(f"[SINGLE LEASE API] Discovery request parameters: {discovery_params}")
        discovery_payload = _post_method(
            endpoint_url=details_url,
            api_key=api_key,
            api_key_header=api_key_header,
            method_name=_to_str(os.getenv("LEASE_API_DETAILS_METHOD") or "getLeaseDetails"),
            version=_to_str(os.getenv("LEASE_API_DETAILS_VERSION") or "r2"),
            timeout_seconds=timeout_seconds,
            params=discovery_params,
        )
        discovery_nodes = _extract_lease_nodes(discovery_payload)
        if not discovery_nodes:
            raise ValueError(f"No lease found with ID {lease_id}")
        property_id = int(discovery_nodes[0].get("propertyId"))
        print(f"[SINGLE LEASE API] ✓ Successfully discovered property_id={property_id} for lease_id={lease_id}")
    else:
        print(f"\n[SINGLE LEASE API] STEP 1: Using provided property_id={property_id}")

    # Fetch lease details with BOTH propertyId and leaseIds
    # Entrata API requires both parameters to properly filter to a single lease
    print(f"\n[SINGLE LEASE API] STEP 2: Fetching Lease Details (Scheduled Charges)")
    lease_details_params: dict[str, Any] = {
        "propertyId": int(property_id),
        "leaseIds": str(lease_id),
        "includeAddOns": _to_str(os.getenv("LEASE_API_INCLUDE_ADDONS") or "0"),
        "includeCharge": _to_str(os.getenv("LEASE_API_INCLUDE_CHARGE") or "1"),
        "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_LEASE_STATUS_TYPE_IDS") or "3,4"),
    }
    print(f"[SINGLE LEASE API] getLeaseDetails request parameters: {lease_details_params}")
    print(f"[SINGLE LEASE API] ✓ Both propertyId AND leaseIds sent together (Entrata requirement)")

    lease_details_payload = _post_method(
        endpoint_url=details_url,
        api_key=api_key,
        api_key_header=api_key_header,
        method_name=_to_str(os.getenv("LEASE_API_DETAILS_METHOD") or "getLeaseDetails"),
        version=_to_str(os.getenv("LEASE_API_DETAILS_VERSION") or "r2"),
        timeout_seconds=timeout_seconds,
        params=lease_details_params,
    )
    print(f"[SINGLE LEASE API] ✓ Received lease details response from Entrata")

    # Extract lease nodes (should only be one)
    lease_nodes = _extract_lease_nodes(lease_details_payload)
    
    if not lease_nodes:
        raise ValueError(f"No lease found with ID {lease_id}")
    
    # Get property info from the lease
    property_name = _to_str(lease_nodes[0].get("propertyName"))
    discovered_property_id = lease_nodes[0].get("propertyId")
    
    if not property_id and discovered_property_id:
        property_id = int(discovered_property_id)
    
    if not property_name:
        property_name = f"Property {int(property_id)}" if property_id else "Unknown Property"

    # Build scheduled charges DataFrame
    print(f"[SINGLE LEASE API] Building scheduled charges DataFrame from API response...")
    scheduled_df, lease_ids_list = _build_scheduled_df(
        int(property_id) if property_id else 0, 
        property_name, 
        lease_details_payload
    )
    print(f"[SINGLE LEASE API] Initial scheduled charges DataFrame: {scheduled_df.shape}")
    if len(lease_ids_list) > 0:
        print(f"[SINGLE LEASE API] Lease IDs in response: {lease_ids_list}")
    
    # Filter scheduled charges to ONLY the requested lease_id
    # (in case API returned multiple leases despite leaseIds parameter)
    if not scheduled_df.empty and 'LEASE_ID' in scheduled_df.columns:
        before_count = len(scheduled_df)
        scheduled_df = scheduled_df[scheduled_df['LEASE_ID'] == str(lease_id)].copy()
        after_count = len(scheduled_df)
        if before_count != after_count:
            print(f"[SINGLE LEASE API] ⚠️  Defensive filtering triggered (API returned multiple leases)")
            print(f"[SINGLE LEASE API] Filtered scheduled charges: {before_count} → {after_count} rows (lease_id={lease_id})")
        else:
            print(f"[SINGLE LEASE API] ✓ API correctly returned only requested lease (no defensive filtering needed)")
    
    print(f"[SINGLE LEASE API] Final scheduled charges DataFrame: {scheduled_df.shape}")
    if not scheduled_df.empty:
        print(f"[SINGLE LEASE API] Scheduled charge columns: {list(scheduled_df.columns)}")

    # Fetch AR transactions with BOTH propertyId and leaseIds
    # Entrata API requires both parameters to properly filter to a single lease
    print(f"\n[SINGLE LEASE API] STEP 3: Fetching AR Transactions")
    ar_params: dict[str, Any] = {
        "propertyId": int(property_id),
        "leaseIds": str(lease_id),
        "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_AR_LEASE_STATUS_TYPE_IDS") or "3,4"),
        "transactionTypeIds": _to_str(os.getenv("LEASE_API_TRANSACTION_TYPE_IDS") or ""),
        "arCodeIds": _to_str(os.getenv("LEASE_API_AR_CODE_IDS") or ""),
        "showFullLedger": _to_str(os.getenv("LEASE_API_SHOW_FULL_LEDGER") or "1"),
        "residentFriendlyMode": _to_str(os.getenv("LEASE_API_RESIDENT_FRIENDLY_MODE") or "0"),
        "includeOtherIncomeLeases": _to_str(os.getenv("LEASE_API_INCLUDE_OTHER_INCOME_LEASES") or "0"),
        "includeReversals": _to_str(os.getenv("LEASE_API_INCLUDE_REVERSALS") or "1"),
        "ledgerIds": _to_str(os.getenv("LEASE_API_LEDGER_IDS") or ""),
    }
    
    if transaction_from_date:
        ar_params["transactionFromDate"] = transaction_from_date
    if transaction_to_date:
        ar_params["transactionToDate"] = transaction_to_date
    
    print(f"[SINGLE LEASE API] getLeaseArTransactions request parameters: {ar_params}")
    print(f"[SINGLE LEASE API] ✓ Both propertyId AND leaseIds sent together (Entrata requirement)")

    ar_payload = _post_method(
        endpoint_url=ar_url,
        api_key=api_key,
        api_key_header=api_key_header,
        method_name=_to_str(os.getenv("LEASE_API_AR_METHOD") or "getLeaseArTransactions"),
        version=_to_str(os.getenv("LEASE_API_AR_VERSION") or "r1"),
        timeout_seconds=timeout_seconds,
        params=ar_params,
    )
    print(f"[SINGLE LEASE API] ✓ Received AR transactions response from Entrata")

    print(f"[SINGLE LEASE API] Building AR transactions DataFrame from API response...")
    ar_df = _build_ar_df(
        int(property_id) if property_id else 0, 
        property_name, 
        ar_payload
    )
    print(f"[SINGLE LEASE API] Initial AR transactions DataFrame: {ar_df.shape}")
    
    # Filter AR transactions to ONLY the requested lease_id
    # (in case API returned multiple leases despite leaseIds parameter)
    if not ar_df.empty and 'LEASE_ID' in ar_df.columns:
        before_count = len(ar_df)
        ar_df = ar_df[ar_df['LEASE_ID'] == str(lease_id)].copy()
        after_count = len(ar_df)
        if before_count != after_count:
            print(f"[SINGLE LEASE API] ⚠️  Defensive filtering triggered (API returned multiple leases)")
            print(f"[SINGLE LEASE API] Filtered AR transactions: {before_count} → {after_count} rows (lease_id={lease_id})")
        else:
            print(f"[SINGLE LEASE API] ✓ API correctly returned only requested lease (no defensive filtering needed)")
    
    print(f"[SINGLE LEASE API] Final AR transactions DataFrame: {ar_df.shape}")
    if not ar_df.empty:
        print(f"[SINGLE LEASE API] AR transaction columns: {list(ar_df.columns)}")
    
    print(f"\n[SINGLE LEASE API] ===== API FETCH COMPLETE =====")
    print(f"[SINGLE LEASE API] Summary for lease {lease_id}:")
    print(f"  - Property: {property_name} (ID: {property_id})")
    print(f"  - Scheduled Charges: {len(scheduled_df)} rows")
    print(f"  - AR Transactions: {len(ar_df)} rows")
    print(f"{'='*80}\n")

    return {
        "property_name": property_name,
        "property_id": property_id,
        "scheduled_raw": scheduled_df,
        "ar_raw": ar_df,
        "lease_count": 1,
        "lease_id": lease_id,
    }
