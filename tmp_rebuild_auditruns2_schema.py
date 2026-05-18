"""
Rebuilds AuditRuns2 columns to exactly match AuditRuns:
  1. Deletes all field_* columns from AuditRuns2
  2. Copies each custom column from AuditRuns with the same internal name + type

After this runs, AuditRuns2 will have RunId, ResultType, PropertyId, etc. as proper
internal names — no remapping needed in the app.
"""
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

SKIP_ALWAYS = {
    'Title', 'ID', 'id', 'Created', 'Modified', 'Author', 'Editor',
    'ContentType', 'Attachments', 'Edit', '_UIVersionString', 'DocIcon',
    'LinkTitleNoMenu', 'LinkTitle', 'ItemChildCount', 'FolderChildCount',
    'AppAuthor', 'AppEditor', '_ComplianceFlags', '_ComplianceTag',
    '_ComplianceTagWrittenTime', '_ComplianceTagUserId', '_IsRecord',
    '_ColorTag', 'ComplianceAssetId', 'LinkTitleNoMenu',
}


def get_columns(s, site_id, list_id):
    resp = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
        params={'$top': 200},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get('value', [])


def main():
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()

    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})

    site_id = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30).json()['id']

    all_lists = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$top': 200}, timeout=30,
    ).json().get('value', [])
    list_map = {lst['displayName']: lst['id'] for lst in all_lists}

    source_id = list_map.get('AuditRuns')
    target_id = list_map.get('AuditRuns2')

    if not source_id or not target_id:
        print("ERROR: Could not find AuditRuns or AuditRuns2")
        return

    source_cols = get_columns(s, site_id, source_id)
    target_cols = get_columns(s, site_id, target_id)

    # Step 1: delete field_* columns from AuditRuns2
    field_star_cols = [
        c for c in target_cols
        if c.get('name', '').startswith('field_')
        and not c.get('readOnly', False)
        and not c.get('sealed', False)
    ]
    print(f"Deleting {len(field_star_cols)} field_* columns from AuditRuns2...")
    for col in field_star_cols:
        col_id = col.get('id')
        col_name = col.get('name')
        resp = s.delete(
            f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{target_id}/columns/{col_id}',
            timeout=30,
        )
        if resp.status_code in (200, 204):
            print(f"  Deleted: {col_name}")
        else:
            print(f"  Failed to delete {col_name}: {resp.status_code} - {resp.text[:200]}")
        time.sleep(0.3)

    # Step 2: copy custom columns from AuditRuns with proper names/types
    custom_source_cols = [
        c for c in source_cols
        if c.get('name') not in SKIP_ALWAYS
        and not c.get('readOnly', False)
        and not c.get('sealed', False)
        and not c.get('hidden', False)
    ]
    print(f"\nAdding {len(custom_source_cols)} columns to AuditRuns2 from AuditRuns...")
    cols_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{target_id}/columns'

    for col in custom_source_cols:
        col_name = col.get('name')
        display_name = col.get('displayName') or col_name

        body = {'name': col_name, 'displayName': display_name}

        # Copy type definition from source
        for type_key in ('text', 'number', 'dateTime', 'boolean', 'choice', 'lookup', 'personOrGroup'):
            if type_key in col:
                body[type_key] = col[type_key]
                break
        else:
            body['text'] = {}  # default

        resp = s.post(cols_url, json=body, timeout=30)
        if resp.status_code in (200, 201):
            created = resp.json()
            print(f"  Added: {col_name} ({display_name}) type={list(body.keys()[-1:])} internal={created.get('name')}")
        else:
            print(f"  Failed {col_name}: {resp.status_code} - {resp.text[:300]}")
        time.sleep(0.3)

    print("\nDone. Run tmp_debug_auditruns2_write.py to verify.")


if __name__ == '__main__':
    main()
