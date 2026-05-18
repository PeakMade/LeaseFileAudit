import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

REQUIRED_COLUMNS = [
    ("RunId", "text"),
    ("ResultType", "text"),
    ("PropertyId", "number"),
    ("LeaseIntervalId", "number"),
    ("ArCodeId", "text"),
    ("AuditMonth", "text"),
    ("Status", "text"),
    ("Severity", "text"),
    ("FindingTitle", "text"),
    ("Variance", "number"),
    ("ExpectedTotal", "number"),
    ("ActualTotal", "number"),
    ("ImpactAmount", "number"),
    ("MatchRule", "text"),
    ("FindingId", "text"),
    ("Category", "text"),
    ("Description", "multilineText"),
    ("ExpectedValue", "text"),
    ("ActualValue", "text"),
    ("CreatedAt", "text"),
    ("PropertyName", "text"),
    ("ResidentName", "text"),
]


def column_payload(name: str, kind: str) -> dict:
    payload = {"displayName": name}
    if kind == "text":
        payload["text"] = {}
    elif kind == "number":
        payload["number"] = {"decimalPlaces": "automatic", "displayAs": "number"}
    elif kind == "multilineText":
        payload["text"] = {"allowMultipleLines": True}
    else:
        raise ValueError(f"Unsupported column kind: {kind}")
    return payload


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()
    if not site or not token:
        print("Missing site URL or token")
        return

    host = site.split("/")[2]
    path = "/".join(site.split("/")[3:])

    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    site_resp = sess.get(f"https://graph.microsoft.com/v1.0/sites/{host}:/{path}", timeout=30)
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    list_resp = sess.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
        params={"$filter": "displayName eq 'AuditRuns2'"},
        timeout=30,
    )
    list_resp.raise_for_status()
    values = list_resp.json().get("value", [])
    if not values:
        print("AuditRuns2 list not found")
        return

    list_id = values[0]["id"]
    cols_resp = sess.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns",
        params={"$select": "name,displayName", "$top": 200},
        timeout=30,
    )
    cols_resp.raise_for_status()

    existing = set()
    for c in cols_resp.json().get("value", []):
        n = c.get("name")
        d = c.get("displayName")
        if n:
            existing.add(n)
        if d:
            existing.add(d)

    created = []
    skipped = []
    failed = []

    for name, kind in REQUIRED_COLUMNS:
        if name in existing:
            skipped.append(name)
            continue

        payload = column_payload(name, kind)
        resp = sess.post(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns",
            json=payload,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            created.append(name)
            existing.add(name)
        else:
            failed.append((name, resp.status_code, resp.text[:400]))

    print(f"AuditRuns2 id={list_id}")
    print(f"Created ({len(created)}): {', '.join(created) if created else 'none'}")
    print(f"Skipped ({len(skipped)}): {', '.join(skipped) if skipped else 'none'}")

    if failed:
        print("Failed:")
        for item in failed:
            print(f"  - {item[0]}: {item[1]} {item[2]}")


if __name__ == "__main__":
    main()
