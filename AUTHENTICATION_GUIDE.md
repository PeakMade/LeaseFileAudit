# Azure App Service Easy Auth - Setup Guide

Complete guide for configuring Azure App Service built-in authentication (Easy Auth) with Microsoft Entra ID.

## Quick Start

**What you need:**
- Azure subscription
- Azure AD tenant
- App Service (Basic tier or higher)
- 30 minutes setup time

## Table of Contents
1. [What Was Implemented](#what-was-implemented)
2. [Azure AD Configuration](#azure-ad-configuration)
3. [App Service Configuration](#app-service-configuration)
4. [Code Usage](#code-usage)
5. [SharePoint Logging (Optional)](#sharepoint-logging-optional)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

## What Was Implemented

### Files Created
- **`web/auth.py`**: Authentication helpers and decorators
- **`logging/sharepoint.py`**: Optional SharePoint activity logging
- **`.env.example`**: Environment variables template

### Files Modified  
- **`config.py`**: Added `AuthConfig` class
- **`app.py`**: Added user context injection
- **`web/views.py`**: Protected routes with `@require_auth` decorator
- **`requirements.txt`**: Added `requests` library

### Key Features
- ✅ User authentication via Microsoft Entra ID
- ✅ Auto-extract user info from Easy Auth headers
- ✅ Route protection with decorators
- ✅ Optional SharePoint activity logging
- ✅ User context available in all templates

## Azure AD Configuration

### Step 1: Create Azure App Registration

1. Go to **Azure Portal** → **Azure Active Directory** → **App registrations**
2. Click **"New registration"**
3. Configure:
   - **Name**: LeaseFileAudit
   - **Supported account types**: Single tenant (your organization only)
   - **Redirect URI**: Web - `https://your-app.azurewebsites.net/.auth/login/aad/callback`
4. Click **Register**

5. Note the following values (you'll need them):
   - **Application (client) ID** → `SHAREPOINT_CLIENT_ID`
   - **Directory (tenant) ID** → `SHAREPOINT_TENANT_ID`

### Step 2: Create Client Secret

1. In your App Registration → **Certificates & secrets**
2. Click **"New client secret"**
3. Add description and expiration
4. Copy the **Value** (not the ID) → `MICROSOFT_PROVIDER_AUTHENTICATION_SECRET`
5. **Important**: Save this immediately; you can't retrieve it later

### Step 3: Configure API Permissions

1. In App Registration → **API permissions**
2. Click **"Add a permission"**
3. Add the following permissions:

#### Microsoft Graph (Delegated)
- `User.Read` - Read user profile
- `email` - Read user email address
- `openid` - OpenID Connect sign-in
- `profile` - Read user profile

#### SharePoint (Optional - only if using SharePoint logging)
- `AllSites.Write` - Write to SharePoint sites
- OR configure specific site permissions

4. Click **"Grant admin consent"** for your organization

### Step 4: Configure Token Claims

1. In App Registration → **Token configuration**
2. Click **"Add optional claim"**
3. Select **ID** token type
4. Add the following claims:
   - `email`
   - `family_name`
   - `given_name`
   - `upn` (User Principal Name)
5. Click **Add**

### Step 5: Enable Easy Auth in App Service

1. Go to **Azure Portal** → **App Services** → Your App
2. In the left menu, click **Authentication**
3. Click **"Add identity provider"**
4. Configure:
   - **Identity provider**: Microsoft
   - **App registration type**: Provide details of an existing registration
   - **Application (client) ID**: Your `SHAREPOINT_CLIENT_ID`
   - **Client secret**: Your `MICROSOFT_PROVIDER_AUTHENTICATION_SECRET`
   - **Issuer URL**: `https://login.microsoftonline.com/{TENANT_ID}/v2.0`
   - **Restrict access**: Require authentication
   - **Unauthenticated requests**: HTTP 302 redirect (recommended for websites)
   - **Token store**: Enabled (default)
5. Click **Add**

### Step 6: Configure Environment Variables

1. In App Service → **Configuration** → **Application settings**
2. Add the following:

```
SHAREPOINT_CLIENT_ID = <your-client-id>
SHAREPOINT_TENANT_ID = <your-tenant-id>
MICROSOFT_PROVIDER_AUTHENTICATION_SECRET = <your-client-secret>
REQUIRE_AUTH = true
SECRET_KEY = <generate-a-strong-random-key>
```

Optional (for SharePoint logging):
```
ENABLE_SHAREPOINT_LOGGING = true
SHAREPOINT_SITE_URL = https://yourtenant.sharepoint.com/sites/yoursite
SHAREPOINT_LIST_NAME = AuditLog
```

3. Click **Save**
4. App will restart automatically

## Application Integration

### Authentication Decorators

#### `@require_auth`
Requires authentication. Returns 401 if user not authenticated.

```python
from web.auth import require_auth, get_current_user

@bp.route('/protected')
@require_auth
def protected_route():
    user = get_current_user()
    return f"Hello {user['name']}"
```

#### `@optional_auth`
Extracts user info if available, but doesn't require it.

```python
from web.auth import optional_auth, get_current_user

@bp.route('/public')
@optional_auth
def public_route():
    user = get_current_user()
    if user:
        return f"Hello {user['name']}"
    return "Hello anonymous user"
```

### Helper Functions

```python
from web.auth import (
    get_easy_auth_user,      # Get user info from headers
    get_current_user,        # Get user from Flask g object
    get_access_token,        # Get access token
    is_authenticated,        # Check if authenticated
    get_user_display_name,   # Get display name or 'Anonymous'
    get_user_email          # Get email or empty string
)
```

### User Information Structure

```python
user = get_current_user()
# Returns:
{
    'user_id': 'unique-user-id',
    'name': 'John Doe',
    'email': 'john.doe@company.com',
    'claims': {
        'name': 'John Doe',
        'emailaddress': 'john.doe@company.com',
        'upn': 'john.doe@company.com',
        # ... other claims
    },
    'access_token': 'eyJ0eXAiOiJKV1QiLCJhbGc...',
    'identity_provider': 'aad'
}
```

### Template Usage

User information is automatically available in all templates:

```html
{% if user %}
    <p>Welcome, {{ user.name }}!</p>
    <p>Email: {{ user.email }}</p>
{% else %}
    <p>Please log in</p>
{% endif %}
```

## SharePoint Logging

### Setup SharePoint List

1. Go to your SharePoint site
2. Create a new list named **"AuditLog"** (or your configured name)
3. Add the following columns:

| Column Name | Type | Required |
|------------|------|----------|
| Title | Single line of text | Yes (auto-created) |
| UserName | Single line of text | No |
| UserEmail | Single line of text | No |
| ActivityType | Single line of text | No |
| AppName | Single line of text | No |
| Timestamp | Date and Time | No |
| IPAddress | Single line of text | No |
| UserAgent | Multiple lines of text | No |
| Details | Multiple lines of text | No |

### Usage in Code

```python
from config import config
from web.auth import get_current_user
from logging.sharepoint import log_user_activity

@bp.route('/upload', methods=['POST'])
@require_auth
def upload():
    user = get_current_user()
    
    # ... upload processing ...
    
    # Log the activity
    if config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type='File Upload',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={'filename': filename, 'size': file_size}
        )
    
    return redirect(url_for('main.index'))
```

## Testing

### Local Development Without Easy Auth

For local development, set in your environment:

```bash
REQUIRE_AUTH=false
```

This allows the app to run without Easy Auth headers.

### Testing with Mock Headers

You can test authentication locally using mock headers:

```python
import base64
import json

# Create mock principal
mock_principal = {
    "user_id": "test-123",
    "identity_provider": "aad",
    "claims": [
        {"typ": "name", "val": "Test User"},
        {"typ": "emailaddress", "val": "test@example.com"}
    ]
}

# Encode
encoded = base64.b64encode(json.dumps(mock_principal).encode()).decode()

# Use in request (e.g., with curl or Postman)
# X-MS-CLIENT-PRINCIPAL: {encoded}
```

### Testing Routes

```bash
# Test without auth (should redirect or return 401)
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
