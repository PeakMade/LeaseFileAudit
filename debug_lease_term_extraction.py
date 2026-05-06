"""
Lease Term Extraction Debugger
==============================
Diagnoses why a lease shows "no mapped lease term found" for Rent (154771).

What it does:
  1. Calls getLeaseDocumentsList  -> shows every document Entrata returns
  2. Selects the primary signed packet + addenda (same logic as audit engine)
  3. Downloads and parses each PDF -> shows extracted text snippet around rent
  4. Runs the term extraction pipeline -> shows what terms were found
  5. Runs build_lease_expectation_overlay -> shows the final mapping result

Usage:
    python debug_lease_term_extraction.py
    python debug_lease_term_extraction.py --lease-id 15177383 --lease-interval-id 18380320 --property-id 100013335
"""
import argparse
import json
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# -- env / config --------------------------------------------------------------
from dotenv import load_dotenv
load_dotenv()

from audit_engine.entrata_lease_terms import (
    fetch_lease_documents_list,
    select_lease_packet_and_addenda,
    parse_pdf_to_text_pack,
    build_lease_expectation_overlay,
    post_entrata,
    get_file_type_name,
    get_doc_id,
    get_doc_code,
    is_signed,
)
from audit_engine.lease_term_rules import get_term_to_ar_code_rules


# -- defaults ------------------------------------------------------------------
DEFAULT_PROPERTY_ID      = 100013335
DEFAULT_LEASE_ID         = 15177383
DEFAULT_LEASE_INTERVAL_ID = 18380320


def sep(title: str = ""):
    width = 78
    if title:
        pad = max(0, width - len(title) - 4)
        print(f"\n{'-' * 2} {title} {'-' * pad}")
    else:
        print("\n" + "-" * width)


def pp(obj):
    """Pretty-print a dict/list."""
    print(json.dumps(obj, indent=2, default=str))


# -- step 1: document list -----------------------------------------------------
def step_document_list(property_id, lease_id):
    sep("STEP 1 — getLeaseDocumentsList")
    docs = fetch_lease_documents_list(property_id, lease_id)
    print(f"Total documents returned: {len(docs)}\n")
    for i, doc in enumerate(docs, 1):
        doc_id    = get_doc_id(doc)
        code      = get_doc_code(doc)
        signed    = is_signed(doc)
        title     = doc.get("Title") or doc.get("title") or "(no title)"
        added_on  = doc.get("AddedOn") or doc.get("addedOn") or ""
        file_type = get_file_type_name(doc.get("FileTypeId") or doc.get("fileTypeId"))
        print(
            f"  [{i:02d}] id={doc_id:<12} code={str(code):<10} signed={str(signed):<6} "
            f"added={str(added_on)[:10]}  type={file_type:<20} title={title}"
        )
    return docs


# -- step 2: document selection ------------------------------------------------
def step_document_selection(docs):
    sep("STEP 2 — select_lease_packet_and_addenda")
    try:
        primary, addenda, reason = select_lease_packet_and_addenda(docs)
        print(f"Selection reason : {reason}")
        print(f"Primary doc      : id={get_doc_id(primary)}  title={primary.get('Title') or primary.get('title')}")
        print(f"Addenda included : {len(addenda)}")
        for a in addenda:
            print(f"  -> id={get_doc_id(a)}  title={a.get('Title') or a.get('title')}")
        return primary, addenda
    except ValueError as exc:
        print(f"  X Document selection failed: {exc}")
        print("  Possible reasons:")
        print("    • No documents have a recognised code (LP, OEP, PACKET, LEASE, LD, OEL)")
        print("    • No signed documents found")
        return None, []


# -- step 3: PDF download + text extraction ------------------------------------
def _download_pdf_bytes(property_id, lease_id, doc_id) -> bytes | None:
    """Fetch raw PDF bytes for one document via getLeaseDocuments."""
    payload = {
        "auth": {"type": "apikey"},
        "requestId": f"doc-get-{doc_id}",
        "method": {
            "name": "getLeaseDocuments",
            "version": "r1",
            "params": {
                "propertyId": property_id,
                "leaseId": lease_id,
                "documentIds": str(doc_id),
                "showDeletedFile": 0,
            },
        },
    }
    import base64
    resp = post_entrata(payload)
    result = resp.get("response", {}).get("result", {})
    lease_docs = result.get("LeaseDocuments") or result.get("leaseDocuments", {})
    lease_doc_obj = lease_docs.get("LeaseDocument") or lease_docs.get("leaseDocument", {})

    filedata = None
    if isinstance(lease_doc_obj, dict):
        for _k, v in lease_doc_obj.items():
            if isinstance(v, dict):
                filedata = v.get("FileData") or v.get("fileData") or v.get("filedata")
                if filedata:
                    break
    if not filedata:
        return None
    try:
        return base64.b64decode(filedata)
    except Exception:
        return None


def step_pdf_extraction(property_id, lease_id, primary, addenda):
    sep("STEP 3 — PDF download & text extraction")
    docs_to_check = ([primary] if primary else []) + list(addenda)
    all_text_parts = []

    for doc in docs_to_check:
        doc_id = get_doc_id(doc)
        title  = doc.get("Title") or doc.get("title") or "(no title)"
        print(f"\n  Document: id={doc_id}  title={title}")

        pdf_bytes = _download_pdf_bytes(property_id, lease_id, doc_id)
        if not pdf_bytes:
            print("    X Could not download PDF bytes")
            continue

        print(f"    ✅ Downloaded {len(pdf_bytes):,} bytes")

        text_pack = parse_pdf_to_text_pack(pdf_bytes)
        if text_pack.get("error"):
            print(f"    X PDF parse error: {text_pack['error']}")
            continue

        full_text = text_pack.get("full_text") or ""
        focus_text = text_pack.get("focus_text") or ""
        print(f"    Pages     : {text_pack.get('page_count', '?')}")
        print(f"    full_text : {len(full_text):,} chars")
        print(f"    focus_text: {len(focus_text):,} chars")

        # Show lines containing "rent" (case-insensitive)
        rent_lines = [
            line.strip() for line in (focus_text or full_text).splitlines()
            if "rent" in line.lower() and line.strip()
        ]
        if rent_lines:
            print(f"\n    Lines mentioning 'rent' ({len(rent_lines)} found):")
            for line in rent_lines[:30]:
                print(f"      {textwrap.shorten(line, 120)}")
        else:
            print("    !️  No lines containing 'rent' found in extracted text")
            # Show first 500 chars so we can see what WAS extracted
            preview = (focus_text or full_text)[:500].replace("\n", "↵")
            print(f"    Text preview: {preview!r}")

        all_text_parts.append(focus_text or full_text)

    return "\n\n".join(all_text_parts)


# -- step 4: term extraction ---------------------------------------------------
def step_term_extraction(property_id, lease_interval_id):
    sep("STEP 4 — refresh_lease_terms_for_lease_interval (cached SP result)")
    print("  Checking SharePoint for cached term set ...")
    from app import create_app
    app = create_app()
    with app.app_context():
        from web.views import get_storage_service
        storage = get_storage_service()
        lease_key = f"{property_id}:{lease_interval_id}"

        term_set = storage.load_lease_term_set_for_lease_key(lease_key)
        print(f"\n  term_set keys: {list((term_set or {}).keys())}")
        if term_set:
            print(f"  last_checked_at    : {term_set.get('last_checked_at')}")
            print(f"  doc_list_fingerprint: {term_set.get('doc_list_fingerprint')}")
            print(f"  extraction_status  : {term_set.get('extraction_status')}")
            print(f"  error_message      : {term_set.get('error_message')}")

        terms_df = storage.load_lease_terms_for_lease_key_from_sharepoint_list(lease_key)
        if terms_df is not None and not terms_df.empty:
            print(f"\n  Cached lease terms ({len(terms_df)} rows):")
            print(terms_df.to_string(index=False))
        else:
            print("\n  !️  No cached lease terms in SharePoint")

        return terms_df


# -- step 5: expectation overlay -----------------------------------------------
def step_expectation_overlay(terms_df):
    sep("STEP 5 — build_lease_expectation_overlay for AR code 154771")
    import pandas as pd
    all_ar_codes = [{"ar_code_id": "154771", "ar_code_name": "Rent"}]
    term_records = terms_df.to_dict("records") if (terms_df is not None and not terms_df.empty) else []
    print(f"  Term records passed to overlay: {len(term_records)}")
    if term_records:
        pp(term_records)

    overlay = build_lease_expectation_overlay(all_ar_codes, term_records)
    sep("Overlay result")
    pp(overlay)

    for group in overlay.get("ar_groups", []):
        exp = group.get("lease_expectation", {})
        has_term = exp.get("has_term", False)
        status   = exp.get("status")
        message  = exp.get("message")
        print(f"\n  AR 154771 -> has_term={has_term}  status={status}")
        if message:
            print(f"  Message: {message}")
        if has_term:
            for t in exp.get("terms", []):
                print(f"  Term: type={t.get('term_type')}  amount={t.get('amount')}  "
                      f"start={t.get('start_date')}  end={t.get('end_date')}")


# -- step 6: show mapping rules ------------------------------------------------
def step_mapping_rules():
    sep("STEP 6 — active term->AR code mapping rules")
    rules = get_term_to_ar_code_rules()
    for rule in rules:
        if "154771" in [str(c) for c in (rule.get("accepted_ar_codes") or [])]:
            print("  Rule covering 154771:")
            pp(rule)


# -- step 7: raw getLeaseDetails charge schedule --------------------------------
def step_getleasedetails_schedule(property_id, lease_id):
    sep("STEP 7 — raw getLeaseDetails scheduled charges from Entrata API")
    from audit_engine.api_ingest import (
        _resolve_api_credentials,
        _post_method,
        _extract_lease_nodes,
        _extract_charges_from_interval,
        _to_str,
        get_entrata_environment,
    )
    import os

    details_url, _, api_key, api_key_header = _resolve_api_credentials()
    print(f"  Environment : {get_entrata_environment()}")
    print(f"  URL         : {details_url}")
    timeout = int(_to_str(os.getenv("LEASE_API_TIMEOUT_SECONDS") or "60") or "60")

    params = {
        "propertyId": int(property_id),
        "leaseIds": str(lease_id),
        "includeAddOns": _to_str(os.getenv("LEASE_API_INCLUDE_ADDONS") or "0"),
        "includeCharge": _to_str(os.getenv("LEASE_API_INCLUDE_CHARGE") or "1"),
        "leaseStatusTypeIds": _to_str(os.getenv("LEASE_API_LEASE_STATUS_TYPE_IDS") or "3,4"),
    }
    print(f"  Params      : {params}\n")

    try:
        payload = _post_method(
            endpoint_url=details_url,
            api_key=api_key,
            api_key_header=api_key_header,
            method_name=_to_str(os.getenv("LEASE_API_DETAILS_METHOD") or "getLeaseDetails"),
            version=_to_str(os.getenv("LEASE_API_DETAILS_VERSION") or "r2"),
            params=params,
            timeout_seconds=timeout,
        )
    except Exception as exc:
        print(f"  X getLeaseDetails call failed: {exc}")
        return

    lease_nodes = _extract_lease_nodes(payload)
    print(f"  Lease nodes returned: {len(lease_nodes)}")

    for ln_idx, lease_node in enumerate(lease_nodes, 1):
        lid = _to_str(lease_node.get("leaseId") or lease_node.get("id"))
        name = _to_str(lease_node.get("customerName") or lease_node.get("name") or "")
        print(f"\n  Lease node [{ln_idx}]: leaseId={lid}  name={name}")

        scheduled_node = lease_node.get("scheduledCharges") or {}
        recurring_raw  = scheduled_node.get("recurringCharge")
        onetime_raw    = scheduled_node.get("oneTimeCharge")

        def _as_list_local(v):
            if v is None:
                return []
            return v if isinstance(v, list) else [v]

        recurring = _as_list_local(recurring_raw)
        onetime   = _as_list_local(onetime_raw)
        print(f"  recurringCharge intervals : {len(recurring)}")
        print(f"  oneTimeCharge intervals   : {len(onetime)}")

        for bucket_label, intervals in [("RECURRING", recurring), ("ONE-TIME", onetime)]:
            for iv_idx, interval in enumerate(intervals, 1):
                iv_id     = _to_str(interval.get("leaseIntervalId"))
                iv_status = _to_str(interval.get("leaseIntervalStatus") or "")
                iv_start  = _to_str(interval.get("leaseStartDate") or "")
                iv_end    = _to_str(interval.get("leaseEndDate") or "")
                print(f"\n    [{bucket_label} interval {iv_idx}] "
                      f"leaseIntervalId={iv_id}  status={iv_status}  "
                      f"start={iv_start}  end={iv_end}")

                charges = _extract_charges_from_interval(interval)
                if not charges:
                    print("      (no charges extracted)")
                    continue
                print(f"      Charges ({len(charges)}):")
                print(f"      {'arCodeId':<10} {'chargeCode':<10} {'amount':<10} "
                      f"{'chargeTiming':<20} {'start':<12} {'end':<12} {'postedThrough'}")
                print("      " + "-" * 90)
                for ch in charges:
                    print(
                        f"      {_to_str(ch.get('arCodeId')):<10} "
                        f"{_to_str(ch.get('chargeCode')):<10} "
                        f"{_to_str(ch.get('amount')):<10} "
                        f"{_to_str(ch.get('chargeTiming')):<20} "
                        f"{_to_str(ch.get('chargeStartDate') or ch.get('installmentStartDate')):<12} "
                        f"{_to_str(ch.get('chargeEndDate') or ch.get('installmentEndDate')):<12} "
                        f"{_to_str(ch.get('postedThrough') or ch.get('postedThroughDate') or '')}"
                    )

        # Also check if the target lease interval ID appears anywhere
        target_iv = str(lease_id)  # reuse as reference; leaseIntervalId checked below
        all_iv_ids = [
            _to_str(iv.get("leaseIntervalId"))
            for iv in _as_list_local(recurring_raw) + _as_list_local(onetime_raw)
        ]
        print(f"\n  All leaseIntervalIds in this node: {all_iv_ids}")


# -- main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Debug lease term extraction for a single lease")
    parser.add_argument("--property-id",        type=int, default=DEFAULT_PROPERTY_ID)
    parser.add_argument("--lease-id",           type=int, default=DEFAULT_LEASE_ID)
    parser.add_argument("--lease-interval-id",  type=int, default=DEFAULT_LEASE_INTERVAL_ID)
    parser.add_argument("--skip-pdf",           action="store_true", help="Skip PDF download/parse (faster)")
    args = parser.parse_args()

    print(f"\n{'=' * 78}")
    print(f"  Lease Term Extraction Debugger")
    print(f"  property_id={args.property_id}  lease_id={args.lease_id}  "
          f"lease_interval_id={args.lease_interval_id}")
    print(f"{'=' * 78}")

    # Steps 1–3: live Entrata API
    docs = step_document_list(args.property_id, args.lease_id)

    if not docs:
        print("\n  X No documents returned — this is the root cause.")
        print("  The audit engine has nothing to extract terms from.")
        step_mapping_rules()
        return

    primary, addenda = step_document_selection(docs)

    if not args.skip_pdf:
        if primary:
            step_pdf_extraction(args.property_id, args.lease_id, primary, addenda)
        else:
            print("\n  !️  Skipping PDF extraction — no primary document selected")
    else:
        print("\n  (PDF extraction skipped via --skip-pdf)")

    # Steps 4–6: SharePoint cached state + overlay logic
    terms_df = step_term_extraction(args.property_id, args.lease_interval_id)
    step_expectation_overlay(terms_df)
    step_mapping_rules()

    # Step 7: raw getLeaseDetails scheduled charges
    step_getleasedetails_schedule(args.property_id, args.lease_id)

    print(f"\n{'=' * 78}")
    print("  Debug complete")
    print(f"{'=' * 78}\n")


if __name__ == "__main__":
    main()
