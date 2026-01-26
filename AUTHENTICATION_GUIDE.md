# Azure App Service Easy Auth + Microsoft Graph - Implementation Guide

Guide for implementing Azure App Service authentication with Microsoft Graph API for SharePoint logging.

## Prerequisites

- Azure subscription with App Service (Basic tier or higher)
- Azure AD tenant access
- SharePoint site for logging (optional)

---

## 1. Azure AD App Registration & Authentication Setup

### Option A: Let Azure Create Registration (Easiest)

1. **App Service** → **Authentication** → **Add identity provider** → **Microsoft**
2. Choose **Create new app registration**
3. Azure automatically creates and configures the app registration
4. Note the **Client ID** and **Tenant ID** shown after creation

### Option B: Use Existing Registration

If you already have an app registration:

1. **App Service** → **Authentication** → **Add identity provider** → **Microsoft**
2. Choose **Provide details of an existing registration**
3. Enter your **Client ID**, **Client secret**, and **Tenant ID**

### Required: Configure API Permissions

**Regardless of which option**, you must add Microsoft Graph permissions:

1. **Azure Portal** → **Azure Active Directory** → **App registrations** → Your app
2. **API permissions** → **Add a permission** → **Microsoft Graph** → **Delegated permissions**

**Add these permissions:**
- `User.Read` - Read user profile
- `email` - Read email
- `openid` - Sign-in
- `profile` - Read profile
- `Sites.ReadWrite.All` - Write to SharePoint (for logging)

3. Click **Grant admin consent for [your organization]**

---

## 2. App Service Authentication Configuration

1. **App Service** → **Authentication** → Configure your Microsoft identity provider

2. **Key Settings:**
   - **Restrict access**: Require authentication
   - **Unauthenticated requests**: HTTP 302 Found redirect
   - **Token store**: Enabled (checked)

3. **Advanced settings** (if using existing registration):
   - **Issuer URL**: `https://login.microsoftonline.com/{TENANT_ID}/v2.0`
   - **Allowed token audiences**: Leave default or add `https://graph.microsoft.com`

### Environment Variables

Add in **Configuration** → **Application settings**:

```bash
# Required
SECRET_KEY=<random-strong-key>
SHAREPOINT_CLIENT_ID=<your-client-id>
SHAREPOINT_TENANT_ID=<your-tenant-id>
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET=<your-secret>
REQUIRE_AUTH=true

# SharePoint Logging (optional)
ENABLE_SHAREPOINT_LOGGING=true
SHAREPOINT_SITE_URL=https://tenant.sharepoint.com/sites/yoursite
SHAREPOINT_LIST_NAME=YourListName
APP_NAME=YourAppName
```

---

## 3. Understanding Access Tokens & Delegated Permissions

### How It Works

1. User authenticates via Azure AD
2. Easy Auth injects headers into every request:
   - `X-MS-CLIENT-PRINCIPAL`: Base64 JSON with user info and claims
   - `X-MS-TOKEN-AAD-ACCESS-TOKEN`: Access token for Microsoft Graph
3. Your app extracts user info and uses token to call Microsoft Graph API

### Delegated Permissions

- **Delegated** = actions performed on behalf of the signed-in user
- Token contains user's identity and permissions
- User must have access to SharePoint sites your app writes to

### Why Microsoft Graph API?

**Use Microsoft Graph API:**
- Easy Auth tokens work with Graph out of the box
- Token audience is `https://graph.microsoft.com`
- Single API for SharePoint, OneDrive, Teams, etc.
- Modern, well-documented, actively maintained

---

## 4. Code Implementation

### Authentication Module (web/auth.py)

```python
import base64
import json
from flask import request, g
from functools import wraps

def get_easy_auth_user():
    """Extract user from Easy Auth headers."""
    principal_header = request.headers.get('X-MS-CLIENT-PRINCIPAL')
    if not principal_header:
        return None
    
    # Decode base64 JSON
    principal_json = base64.b64decode(principal_header).decode('utf-8')
    principal_data = json.loads(principal_json)
    
    # Extract claims
    claims = {}
    for claim in principal_data.get('claims', []):
        claim_type = claim.get('typ', '')
        claim_value = claim.get('val', '')
        claim_key = claim_type.split('/')[-1] if '/' in claim_type else claim_type
        claims[claim_key] = claim_value
    
    # Get access token
    access_token = request.headers.get('X-MS-TOKEN-AAD-ACCESS-TOKEN')
    
    return {
        'user_id': principal_data.get('user_id', ''),
        'name': claims.get('name', claims.get('displayname', 'Unknown')),
        'email': claims.get('emailaddress', claims.get('email', claims.get('upn', ''))),
        'claims': claims,
        'access_token': access_token,
        'identity_provider': principal_data.get('identity_provider', 'aad')
    }

def require_auth(f):
    """Decorator requiring authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_easy_auth_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        g.user = user
        return f(*args, **kwargs)
    return decorated

def get_current_user():
    """Get current user from Flask g."""
    return getattr(g, 'user', None)
```

### SharePoint Logger via Microsoft Graph (activity_logging/sharepoint.py)

```python
import requests
from urllib.parse import urlparse

class SharePointLogger:
    def __init__(self, site_url: str, list_name: str):
        self.site_url = site_url.rstrip('/')
        self.list_name = list_name
        self._site_id = None
        self._list_id = None
    
    def _get_site_id(self, access_token: str):
        """Resolve SharePoint site URL to Graph API site ID."""
        if self._site_id:
            return self._site_id
        
        parsed = urlparse(self.site_url)
        hostname = parsed.hostname  # e.g., tenant.sharepoint.com
        site_path = parsed.path     # e.g., /sites/MySite
        
        endpoint = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        response = requests.get(endpoint, headers=headers, timeout=10)
        if response.status_code == 200:
            self._site_id = response.json().get('id')
        return self._site_id
    
    def _get_list_id(self, access_token: str, site_id: str):
        """Get list ID by display name."""
        if self._list_id:
            return self._list_id
        
        endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        response = requests.get(endpoint, headers=headers, timeout=10)
        if response.status_code == 200:
            for item in response.json().get('value', []):
                if item.get('displayName') == self.list_name:
                    self._list_id = item.get('id')
                    return self._list_id
        return None
    
    def log_activity(self, access_token: str, user_name: str, 
                     user_email: str, activity_type: str, **fields):
        """Log activity to SharePoint list."""
        site_id = self._get_site_id(access_token)
        if not site_id:
            return False
        
        list_id = self._get_list_id(access_token, site_id)
        if not list_id:
            return False
        
        # Build item data - Graph uses simple format
        item_data = {
            'fields': {
                'Title': f'{activity_type} - {user_name}',
                'UserName': user_name,
                'UserEmail': user_email,
                'ActivityType': activity_type,
                **fields  # Additional columns
            }
        }
        
        endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(endpoint, json=item_data, headers=headers, timeout=10)
        return response.status_code in [200, 201]
```

### Usage Example

```python
from web.auth import require_auth, get_current_user
from activity_logging.sharepoint import SharePointLogger
import os

@app.route('/protected')
@require_auth
def protected_route():
    user = get_current_user()
    
    # Log activity
    if os.getenv('ENABLE_SHAREPOINT_LOGGING') == 'true':
        logger = SharePointLogger(
            site_url=os.getenv('SHAREPOINT_SITE_URL'),
            list_name=os.getenv('SHAREPOINT_LIST_NAME')
        )
        logger.log_activity(
            access_token=user['access_token'],
            user_name=user['name'],
            user_email=user['email'],
            activity_type='Page View',
            Application=os.getenv('APP_NAME', 'MyApp'),
            UserRole='user'
        )
    
    return f"Hello {user['name']}"
```

---

## 5. SharePoint List Setup

Create a SharePoint list with these columns (names must match exactly):

| Column Name | Type |
|------------|------|
| Title | Single line of text |
| UserName | Single line of text |
| UserEmail | Single line of text |
| ActivityType | Single line of text |
| Application | Single line of text |
| UserRole | Single line of text |
| LoginTimestamp | Date and Time |

**Column names in your code must match the SharePoint column names exactly.**

---

## 6. Testing & Troubleshooting

### Local Development

Set `REQUIRE_AUTH=false` in `.env` to bypass authentication locally.

### Verify Token

Decode token at [jwt.ms](https://jwt.ms):
- `aud` claim should be `https://graph.microsoft.com`
- `scp` claim should include your permissions

### Common Issues

**"Access denied":**
- Missing API permissions in Azure AD
- Admin consent not granted
- User doesn't have SharePoint site access

**"List not found":**
- List `displayName` in SharePoint doesn't match `SHAREPOINT_LIST_NAME` env variable

**"Column doesn't exist":**
- Field name in code doesn't match SharePoint column name exactly

**Authentication not working:**
- `REQUIRE_AUTH` not set to `true`
- Easy Auth not enabled in App Service
- Redirect URI mismatch in app registration

---

## 7. Key Points

1. **Use Microsoft Graph API** for all SharePoint operations
2. **Delegated permissions** = actions performed on behalf of signed-in user
3. **Easy Auth handles token acquisition and refresh** automatically
4. **Always grant admin consent** after adding API permissions
5. **Column names in code must match SharePoint columns exactly**
6. **Azure can auto-create app registrations** when enabling auth

---

## Additional Resources

- [Microsoft Graph API](https://learn.microsoft.com/en-us/graph/)
- [App Service Easy Auth](https://learn.microsoft.com/en-us/azure/app-service/overview-authentication-authorization)
- [SharePoint Lists via Graph](https://learn.microsoft.com/en-us/graph/api/resources/list)

---

## Additional Resources

- [Microsoft Graph API Documentation](https://learn.microsoft.com/en-us/graph/)
- [App Service Easy Auth](https://learn.microsoft.com/en-us/azure/app-service/overview-authentication-authorization)
- [SharePoint List API](https://learn.microsoft.com/en-us/graph/api/resources/list)

---

## Additional Resources

- [Microsoft Graph API Documentation](https://learn.microsoft.com/en-us/graph/)
- [App Service Easy Auth](https://learn.microsoft.com/en-us/azure/app-service/overview-authentication-authorization)
- [SharePoint List API](https://learn.microsoft.com/en-us/graph/api/resources/list)


curl https://your-app.azurewebsites.net/portfolio

# Test with mock header (local dev)
curl -H "X-MS-CLIENT-PRINCIPAL: {encoded}" http://localhost:5000/portfolio
```

## Troubleshooting

### Issue: "Authentication required" error

**Solution**: Ensure Easy Auth is properly configured in Azure App Service:
- Check Authentication blade shows "Microsoft" as configured
- Verify client ID and secret are correct
- Confirm redirect URI matches

### Issue: User info not available

**Solution**: Check headers are being injected:
```python
# Add to a test route
@bp.route('/debug')
def debug():
    headers = {k: v for k, v in request.headers.items()}
    return headers
```

Look for `X-MS-CLIENT-PRINCIPAL` and `X-MS-TOKEN-AAD-ACCESS-TOKEN`

### Issue: SharePoint logging fails

**Solutions**:
1. Verify API permissions include SharePoint access
2. Check SharePoint list exists with correct schema
3. Verify site URL is correct
4. Check access token has appropriate scopes
5. Review logs for specific error messages

### Issue: Token expired

**Solution**: App Service automatically refreshes tokens. If issues persist:
- Clear browser cache/cookies
- Sign out and sign in again
- Check token lifetime settings in Azure AD

## Security Best Practices

1. **Never log sensitive information**: Don't log access tokens or secrets
2. **Use HTTPS only**: Ensure your App Service requires HTTPS
3. **Rotate secrets regularly**: Set expiration on client secrets
4. **Principle of least privilege**: Only request necessary API permissions
5. **Monitor authentication**: Review authentication logs regularly
6. **Secure environment variables**: Use App Service Configuration, not code

## Additional Resources

- [Azure App Service Authentication Documentation](https://docs.microsoft.com/en-us/azure/app-service/overview-authentication-authorization)
- [Microsoft Identity Platform](https://docs.microsoft.com/en-us/azure/active-directory/develop/)
- [SharePoint REST API](https://docs.microsoft.com/en-us/sharepoint/dev/sp-add-ins/get-to-know-the-sharepoint-rest-service)
