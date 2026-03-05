from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from .mappings import ARSourceColumns, ScheduledSourceColumns


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
    request_body = {
        "auth": {"type": "apikey"},
        "requestId": str(int(datetime.utcnow().timestamp() * 1000)),
        "method": {
            "name": method_name,
            "version": version,
            "params": params,
        },
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if api_key:
        headers[api_key_header] = api_key

    response = requests.post(endpoint_url, headers=headers, json=request_body, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        top_code = payload.get("code")
        nested_code = ((payload.get("response") or {}).get("code"))
        resolved_code = top_code if top_code is not None else nested_code
        if resolved_code is not None and int(resolved_code or 0) != 200:
            raise ValueError(f"{method_name} failed: {payload}")
    return payload


def _extract_lease_nodes(details_payload: dict[str, Any]) -> list[dict[str, Any]]:
    root = details_payload.get("response") if isinstance(details_payload.get("response"), dict) else details_payload
    leases = (((root.get("result") or {}).get("leases") or {}).get("lease"))
    return [item for item in _as_list(leases) if isinstance(item, dict)]


def _extract_customer_name(lease_node: dict[str, Any]) -> str:
    lease_name = _to_str(lease_node.get("name"))
    if lease_name:
        return lease_name

    customers_node = (((lease_node.get("customers") or {}).get("customer")))
    customers = [item for item in _as_list(customers_node) if isinstance(item, dict)]
    if not customers:
        return ""

    first = _to_str(customers[0].get("firstName"))
    last = _to_str(customers[0].get("lastName"))
    return " ".join(part for part in [first, last] if part)


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

    for source in [active, past, direct]:
        for charge in _as_list(source):
            if isinstance(charge, dict):
                rows.append(charge)

    rows.extend(installment_charges)
    return rows


def _build_scheduled_df(property_id: int, property_name: str, details_payload: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    output_rows: list[dict[str, Any]] = []
    lease_ids: list[str] = []

    for lease_node in _extract_lease_nodes(details_payload):
        lease_id = _to_str(lease_node.get("leaseId") or lease_node.get("id"))
        if lease_id and lease_id not in lease_ids:
            lease_ids.append(lease_id)

        customer_name = _extract_customer_name(lease_node)
        customer_id = ""
        customer_nodes = [
            item for item in _as_list((((lease_node.get("customers") or {}).get("customer"))))
            if isinstance(item, dict)
        ]
        if customer_nodes:
            customer_id = _to_str(customer_nodes[0].get("id"))

        scheduled_node = lease_node.get("scheduledCharges") or {}
        interval_nodes = []
        interval_nodes.extend([item for item in _as_list(scheduled_node.get("recurringCharge")) if isinstance(item, dict)])
        interval_nodes.extend([item for item in _as_list(scheduled_node.get("oneTimeCharge")) if isinstance(item, dict)])

        for interval_index, interval_node in enumerate(interval_nodes, start=1):
            lease_interval_id = _to_str(interval_node.get("leaseIntervalId"))
            lease_start = _to_mmddyyyy(interval_node.get("leaseStartDate"))
            lease_end = _to_mmddyyyy(interval_node.get("leaseEndDate"))

            charges = _extract_charges_from_interval(interval_node)
            for charge_index, charge in enumerate(charges, start=1):
                amount = _parse_money(charge.get("amount"))
                if amount is None:
                    continue

                charge_start = _to_mmddyyyy(
                    charge.get("chargeStartDate") or charge.get("installmentStartDate") or lease_start
                )
                charge_end = _to_mmddyyyy(
                    charge.get("chargeEndDate") or charge.get("installmentEndDate") or lease_end
                )

                row_id = (
                    f"api-sc-{property_id}-{lease_id}-{lease_interval_id or interval_index}-"
                    f"{_to_str(charge.get('arCodeId') or '')}-{charge_index}"
                )
                output_rows.append({
                    ScheduledSourceColumns.ID: row_id,
                    ScheduledSourceColumns.PROPERTY_ID: int(property_id),
                    ScheduledSourceColumns.LEASE_ID: lease_id,
                    ScheduledSourceColumns.LEASE_INTERVAL_ID: lease_interval_id,
                    ScheduledSourceColumns.AR_CODE_ID: _to_str(charge.get("arCodeId")),
                    ScheduledSourceColumns.AR_CODE_NAME: _to_str(charge.get("chargeCode") or charge.get("chargeUsage") or "Unknown"),
                    ScheduledSourceColumns.CHARGE_AMOUNT: float(amount),
                    ScheduledSourceColumns.CHARGE_START_DATE: charge_start,
                    ScheduledSourceColumns.CHARGE_END_DATE: charge_end,
                    ScheduledSourceColumns.GUARANTOR_NAME: "",
                    ScheduledSourceColumns.CUSTOMER_NAME: customer_name,
                    ScheduledSourceColumns.CUSTOMER_ID: customer_id,
                    ScheduledSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL: 1,
                    "PROPERTY_NAME": property_name,
                })

    required_columns = [
        ScheduledSourceColumns.ID,
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

        customer_name = _extract_customer_name(lease_node)
        customer_id = ""
        customer_nodes = [
            item for item in _as_list((((lease_node.get("customers") or {}).get("customer"))))
            if isinstance(item, dict)
        ]
        if customer_nodes:
            customer_id = _to_str(customer_nodes[0].get("id"))

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
                    ARSourceColumns.GUARANTOR_NAME: "",
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


def fetch_property_api_sources(
    property_id: int,
    transaction_from_date: str | None = None,
    transaction_to_date: str | None = None,
) -> dict[str, Any]:
    details_url = _to_str(os.getenv("LEASE_API_DETAILS_URL")) or _to_str(os.getenv("LEASE_API_BASE_URL")) or "https://apis.entrata.com/ext/orgs/peakmade/v1/leases?page_no=1&per_page=100"
    ar_url = _to_str(os.getenv("LEASE_API_AR_URL")) or _to_str(os.getenv("LEASE_API_BASE_URL")) or "https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions?page_no=1&per_page=100"
    api_key = _to_str(os.getenv("LEASE_API_KEY"))
    api_key_header = _to_str(os.getenv("LEASE_API_KEY_HEADER") or "X-Api-Key")

    if not details_url:
        raise ValueError("Missing LEASE_API_DETAILS_URL (or LEASE_API_BASE_URL) env var")
    if not ar_url:
        raise ValueError("Missing LEASE_API_AR_URL (or LEASE_API_BASE_URL) env var")
    if not api_key:
        raise ValueError("Missing LEASE_API_KEY env var")

    timeout_seconds = int(_to_str(os.getenv("LEASE_API_TIMEOUT_SECONDS") or "60") or "60")

    lease_details_payload = _post_method(
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

    lease_nodes = _extract_lease_nodes(lease_details_payload)
    property_name = ""
    if lease_nodes:
        property_name = _to_str(lease_nodes[0].get("propertyName"))
    if not property_name:
        property_name = f"Property {int(property_id)}"

    scheduled_df, lease_ids = _build_scheduled_df(int(property_id), property_name, lease_details_payload)

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
