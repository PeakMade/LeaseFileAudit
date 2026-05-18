import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()
    if not site or not token:
        print("Missing site URL or token")
        return

    host = site.split("/")[2]
    path = "/".join(site.split("/")[3:])
    site_id = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{host}:/{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    ).json()["id"]

    print(f"site_id={site_id}\n")

    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}"})

    for list_name in ["AuditRuns2", "AuditRuns"]:
        r = sess.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
            params={"$filter": f"displayName eq '{list_name}'"},
            timeout=30,
        )
        values = r.json().get("value", [])
        if not values:
            print(f"{list_name}: NOT FOUND\n")
            continue

        list_id = values[0]["id"]
        cols = sess.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns",
            params={"$select": "name", "$top": 200},
            timeout=30,
        ).json().get("value", [])
        col_names = sorted([c.get("name") for c in cols if c.get("name")])

        count_resp = sess.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/$count",
            headers={"ConsistencyLevel": "eventual"},
            timeout=30,
        )
        item_count = count_resp.text if count_resp.status_code == 200 else f"count_error_{count_resp.status_code}"

        print(f"{list_name} id={list_id}")
        print(f"count={item_count}")
        print(f"has_RunId={'RunId' in col_names}")
        print(f"columns={', '.join(col_names)}\n")


if __name__ == "__main__":
    main()
