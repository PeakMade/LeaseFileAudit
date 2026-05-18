"""
Compare AuditRuns vs AuditRuns2 column schemas and add any missing columns to AuditRuns2.
This makes AuditRuns2 an exact schema match of AuditRuns so the app can write to it
without any field remapping.
"""
import json
import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

# SharePoint column type map for creating columns
# Graph API column types: text, number, dateTime, boolean, choice, etc.
SKIP_INTERNAL_NAMES = {
    'Title', 'ID', 'id', 'Created', 'Modified', 'Author', 'Editor',
    'ContentType', 'Attachments', 'Edit', '_UIVersionString', 'DocIcon',
    'LinkTitleNoMenu', 'LinkTitle', 'ItemChildCount', 'FolderChildCount',
    'AppAuthor', 'AppEditor', '_ComplianceFlags', '_ComplianceTag',
    '_ComplianceTagWrittenTime', '_ComplianceTagUserId', '_IsRecord',
    'FileSizeDisplay', 'SelectTitle', 'SelectFilename', 'MediaServiceOCR',
    'CheckedOutUserId', 'IsCheckedoutToLocal', 'PermMask',
    '_ListSchemaVersion', 'owshiddenversion', 'UniqueId', 'ProgId',
    'ScopeId', 'HTML_x0020_File_x0020_Type', 'File_x0020_Type',
    'MetaInfo', 'Restricted', 'NoExecute',
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
        params={'$top': 200},
        timeout=30,
    ).json().get('value', [])

    list_map = {lst['displayName']: lst['id'] for lst in all_lists}
    print("Available lists:", list(list_map.keys()))

    source_id = list_map.get('AuditRuns')
    target_id = list_map.get('AuditRuns2')

    if not source_id:
        print("ERROR: AuditRuns list not found")
        return
    if not target_id:
        print("ERROR: AuditRuns2 list not found")
        return

    source_cols = get_columns(s, site_id, source_id)
    target_cols = get_columns(s, site_id, target_id)

    print(f"\n=== AuditRuns columns ({len(source_cols)}) ===")
    for c in source_cols:
        print(f"  name={c.get('name'):35s} displayName={c.get('displayName')}")

    print(f"\n=== AuditRuns2 columns ({len(target_cols)}) ===")
    for c in target_cols:
        print(f"  name={c.get('name'):35s} displayName={c.get('displayName')}")

    # Find columns in source that are missing from target (by display name)
    target_display_names = {c.get('displayName') for c in target_cols}
    target_internal_names = {c.get('name') for c in target_cols}

    missing = [
        c for c in source_cols
        if c.get('name') not in SKIP_INTERNAL_NAMES
        and c.get('displayName') not in target_display_names
        and c.get('name') not in target_internal_names
        and not c.get('readOnly', False)
        and not c.get('sealed', False)
        and not c.get('hidden', False)
    ]

    print(f"\n=== Columns in AuditRuns missing from AuditRuns2 ({len(missing)}) ===")
    for c in missing:
        print(f"  name={c.get('name'):35s} displayName={c.get('displayName')}  type={list(c.keys())}")

    if not missing:
        print("\nNo missing columns — schemas already match!")
        return

    print(f"\nAdding {len(missing)} missing column(s) to AuditRuns2...")
    cols_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{target_id}/columns'

    for col in missing:
        col_name = col.get('name')
        display_name = col.get('displayName') or col_name

        # Build column definition based on source column type
        body = {'name': col_name, 'displayName': display_name}

        if 'text' in col:
            body['text'] = col['text']
        elif 'number' in col:
            body['number'] = col['number']
        elif 'dateTime' in col:
            body['dateTime'] = col['dateTime']
        elif 'boolean' in col:
            body['boolean'] = {}
        elif 'choice' in col:
            body['choice'] = col['choice']
        elif 'currency' in col:
            body['number'] = {}  # Graph doesn't support currency directly in column create
        else:
            body['text'] = {}  # Default to text

        resp = s.post(cols_url, json=body, timeout=30)
        if resp.status_code in (200, 201):
            print(f"  ✓ Added: {col_name} ({display_name})")
        else:
            print(f"  ✗ Failed {col_name}: {resp.status_code} - {resp.text[:200]}")

    print("\nDone. AuditRuns2 should now match AuditRuns schema.")


if __name__ == '__main__':
    main()
