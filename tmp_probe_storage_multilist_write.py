from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from web.views import get_storage_service


def main() -> None:
    run_id = f"run_probe_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    storage = get_storage_service()
    print(f"storage.audit_results_list_name={storage.audit_results_list_name}")

    bucket_results = pd.DataFrame([
        {
            "PROPERTY_ID": 154771,
            "LEASE_INTERVAL_ID": 15222230,
            "AR_CODE_ID": "RENT",
            "AUDIT_MONTH": "2026-05",
            "status": "MATCHED",
            "severity": "info",
            "variance": 0,
            "expected_total": 1000,
            "actual_total": 1000,
            "title": "Probe bucket row",
            "impact_amount": 0,
            "match_rule": "probe",
            "finding_id": "probe-bucket-1",
            "category": "probe",
            "description": "Probe write through storage service",
            "expected_value": "1000",
            "actual_value": "1000",
        }
    ])

    findings = pd.DataFrame([
        {
            "PROPERTY_ID": 154771,
            "LEASE_INTERVAL_ID": 15222230,
            "AR_CODE_ID": "RENT",
            "AUDIT_MONTH": "2026-05",
            "status": "MATCHED",
            "severity": "info",
            "variance": 0,
            "expected_total": 1000,
            "actual_total": 1000,
            "title": "Probe finding row",
            "impact_amount": 0,
            "match_rule": "probe",
            "finding_id": "probe-finding-1",
            "category": "probe",
            "description": "Probe finding through storage service",
            "expected_value": "1000",
            "actual_value": "1000",
        }
    ])

    ok = storage._write_results_to_sharepoint_list(
        run_id,
        bucket_results,
        findings,
    )

    print(f"write_ok={ok}")
    print(f"probe_run_id={run_id}")


if __name__ == "__main__":
    main()
