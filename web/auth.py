"""
Azure App Service Easy Auth (EasyAuth) authentication module.

This module provides helper functions to extract user information from 
Azure App Service built-in authentication headers when deployed to Azure.
"""
import base64
import json
import logging
from functools import wraps
from typing import Optional, Dict, Any
from flask import request, jsonify, g

logger = logging.getLogger(__name__)


def get_easy_auth_user() -> Optional[Dict[str, Any]]:
    """
    Extract user information from Azure App Service Easy Auth headers.
    
    When authentication is enabled in Azure App Service, it injects several
    headers into each request:
    - X-MS-CLIENT-PRINCIPAL: Base64-encoded JSON with user claims
    - X-MS-TOKEN-AAD-ACCESS-TOKEN: Azure AD access token
    
    Returns:
        Dict with user information or None if not authenticated:
        {
            'user_id': str,          # User's unique ID
            'name': str,             # Display name
            'email': str,            # Email address
            'claims': dict,          # All claims from token
            'access_token': str,     # Azure AD access token (if present)
            'identity_provider': str # Identity provider (e.g., 'aad')
        }
    """
    # Get the client principal header
    principal_header = request.headers.get('X-MS-CLIENT-PRINCIPAL')
    
    logger.debug(f"[AUTH] Checking for X-MS-CLIENT-PRINCIPAL header")
    logger.debug(f"[AUTH] Header present: {principal_header is not None}")
    
    if not principal_header:
        logger.debug("No X-MS-CLIENT-PRINCIPAL header found - user not authenticated via Easy Auth")
        return None
    
    try:
        logger.debug(f"[AUTH] Decoding X-MS-CLIENT-PRINCIPAL header (length: {len(principal_header)})")
        # Decode the base64-encoded JSON
        principal_json = base64.b64decode(principal_header).decode('utf-8')
        logger.debug(f"[AUTH] Decoded principal JSON: {principal_json[:200]}...")
        principal_data = json.loads(principal_json)
        logger.debug(f"[AUTH] Principal data keys: {list(principal_data.keys())}")
        
        # Extract user claims
        claims = {}
        logger.debug(f"[AUTH] Extracting {len(principal_data.get('claims', []))} claims from principal data")
        for claim in principal_data.get('claims', []):
            claim_type = claim.get('typ', '')
            claim_value = claim.get('val', '')
            
            # Store claim using the last part of the type (e.g., 'name' from 'http://schemas.../name')
            claim_key = claim_type.split('/')[-1] if '/' in claim_type else claim_type
            claims[claim_key] = claim_value
        
        logger.debug(f"[AUTH] Extracted claim keys: {list(claims.keys())}")
        
        # Get access token from separate header
        access_token = request.headers.get('X-MS-TOKEN-AAD-ACCESS-TOKEN')
        logger.debug(f"[AUTH] Access token present: {access_token is not None}")
        if access_token:
            logger.debug(f"[AUTH] Access token length: {len(access_token)}")
        
        # Build user info dictionary
        extracted_name = claims.get('name', claims.get('displayname', 'Unknown User'))
        extracted_email = claims.get('emailaddress', claims.get('email', claims.get('upn', '')))
        
        logger.debug(f"[AUTH] Extracted name from claims: {extracted_name}")
        logger.debug(f"[AUTH] Extracted email from claims: {extracted_email}")
        logger.debug(f"[AUTH] User ID: {principal_data.get('user_id', 'N/A')}")
        logger.debug(f"[AUTH] Identity provider: {principal_data.get('identity_provider', 'aad')}")
        
        user_info = {
            'user_id': principal_data.get('user_id', ''),
            'name': extracted_name,
            'email': extracted_email,
            'claims': claims,
            'access_token': access_token,
            'identity_provider': principal_data.get('identity_provider', 'aad')
        }
        
        logger.info(f"Authenticated user: {user_info['name']} ({user_info['email']})")
        return user_info
        
    except Exception as e:
        logger.error(f"Error parsing Easy Auth headers: {e}", exc_info=True)
        return None


def get_access_token() -> Optional[str]:
    """
    Extract the Azure AD access token from Easy Auth headers.
    
    This token can be used to authenticate to other Azure services
    like SharePoint, Graph API, etc.
    
    Returns:
        Access token string or None if not available
    """
    return request.headers.get('X-MS-TOKEN-AAD-ACCESS-TOKEN')


def get_current_user() -> Optional[Dict[str, Any]]:
    """
    Get the current authenticated user from Flask's g object.
    
    This should be called after the authentication decorator has run
    and populated g.user.
    
    Returns:
        User info dictionary or None if not authenticated
    """
    return getattr(g, 'user', None)


def require_auth(f):
    """
    Decorator to require Easy Auth authentication for a route.
    
    This decorator:
    1. Extracts user information from Easy Auth headers
    2. Stores user info in Flask's g.user for access in the view
    3. Returns 401 Unauthorized if no valid authentication is found
    
    Usage:
        @bp.route('/protected')
        @require_auth
        def protected_route():
            user = get_current_user()
            return f"Hello {user['name']}"
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_easy_auth_user()
        
        if user is None:
            logger.warning(f"Unauthorized access attempt to {request.path}")
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Authentication required. Please ensure Azure App Service authentication is enabled.'
            }), 401
        
        # Store user in Flask's g object for access in the view
        g.user = user
        
        return f(*args, **kwargs)
    
    return decorated_function


def optional_auth(f):
    """
    Decorator that extracts user information if available but doesn't require it.
    
    This is useful for routes that should work both with and without authentication,
    but should show different content/behavior when authenticated.
    
    Usage:
        @bp.route('/public')
        @optional_auth
        def public_route():
            user = get_current_user()
            if user:
                return f"Hello {user['name']}"
            return "Hello anonymous user"
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_easy_auth_user()
        g.user = user  # Will be None if not authenticated
        return f(*args, **kwargs)
    
    return decorated_function


def is_authenticated() -> bool:
    """
    Check if the current request has a valid authenticated user.
    
    Returns:
        True if user is authenticated, False otherwise
    """
    return get_current_user() is not None


def get_user_display_name() -> str:
    """
    Get the display name of the current user or 'Anonymous' if not authenticated.
    
    Returns:
        User's display name or 'Anonymous'
    """
    user = get_current_user()
    return user['name'] if user else 'Anonymous'


def get_user_email() -> str:
    """
    Get the email of the current user or empty string if not authenticated.
    
    Returns:
        User's email or empty string
    """
    user = get_current_user()
    return user['email'] if user else ''
