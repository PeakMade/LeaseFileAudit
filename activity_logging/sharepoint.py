"""
SharePoint logging module for Azure App Service Easy Auth.

This module logs user activity to a SharePoint list using Microsoft Graph API
with the user's Azure AD access token obtained from Easy Auth headers.
"""
import logging
import requests
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any
from flask import request

logger = logging.getLogger(__name__)


class SharePointLogger:
    """
    Log user activity to SharePoint using Azure AD access tokens.
    
    This class uses the Microsoft Graph API to write audit logs to a
    SharePoint list. The access token from Easy Auth is used for authentication.
    """
    
    def __init__(self, site_url: str, list_name: str = 'AuditLog'):
        """
        Initialize SharePoint logger.
        
        Args:
            site_url: Full SharePoint site URL (e.g., https://contoso.sharepoint.com/sites/audit)
            list_name: Name of the SharePoint list to log to
        """
        self.site_url = site_url.rstrip('/')
        self.list_name = list_name
        self._site_id = None  # Cache for Graph API site ID
        self._list_id = None  # Cache for Graph API list ID
        logger.debug(f"[SHAREPOINT] Initialized SharePoint logger")
        logger.debug(f"[SHAREPOINT] Site URL: {self.site_url}")
        logger.debug(f"[SHAREPOINT] List name: {self.list_name}")
        
    def log_activity(
        self, 
        access_token: str,
        user_name: str,
        user_email: str,
        activity_type: str,
        app_name: str = None,
        user_role: str = 'user',
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Log a user activity to SharePoint.
        
        Args:
            access_token: Azure AD access token from Easy Auth
            user_name: User's display name
            user_email: User's email address
            activity_type: Type of activity (e.g., 'Upload', 'View', 'Export')
            app_name: Name of the application (defaults to APP_NAME env var)
            user_role: User's role (default: 'user')
            details: Optional dictionary of additional details
            
        Returns:
            True if log was successful, False otherwise
        """
        try:
            # Get app name from environment if not provided
            if app_name is None:
                app_name = os.getenv('APP_NAME', 'LeaseFileAudit')
            
            logger.debug(f"[SHAREPOINT] Attempting to log activity: {activity_type}")
            logger.debug(f"[SHAREPOINT] User: {user_name} ({user_email})")
            logger.debug(f"[SHAREPOINT] Access token present: {access_token is not None}")
            if access_token:
                logger.debug(f"[SHAREPOINT] Access token length: {len(access_token)}")
            
            # Get site ID and list ID using Microsoft Graph API
            site_id = self._get_site_id(access_token)
            if not site_id:
                logger.error("Failed to resolve SharePoint site ID")
                return False
            
            list_id = self._get_list_id(access_token, site_id)
            if not list_id:
                logger.error("Failed to resolve SharePoint list ID")
                return False
            
            # Prepare the list item data for Microsoft Graph API
            # Graph API uses a simpler format with fields nested under 'fields' key
            item_data = {
                'fields': {
                    'Title': f'{activity_type} - {user_name}',
                    'UserName': user_name,
                    'UserEmail': user_email,
                    'ActivityType': activity_type,
                    'Application': app_name,
                    'UserRole': user_role,
                    'LoginTimestamp': datetime.utcnow().isoformat() + 'Z',
                    'IPAddress': self._get_client_ip(),
                    'UserAgent': request.headers.get('User-Agent', ''),
                }
            }
            
            # Add details if provided
            if details:
                item_data['fields']['Details'] = str(details)
            
            logger.debug(f"[SHAREPOINT] Item data: {item_data}")
            
            # Get the Microsoft Graph list endpoint
            list_endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            logger.debug(f"[SHAREPOINT] Endpoint: {list_endpoint}")
            
            # Prepare headers for Microsoft Graph API
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            logger.debug(f"[SHAREPOINT] Request headers prepared")
            logger.debug(f"[SHAREPOINT] Authorization header: Bearer {access_token[:20]}...{access_token[-20:] if len(access_token) > 40 else ''}")
            
            # Make the request
            logger.debug(f"[SHAREPOINT] Sending POST request to SharePoint...")
            logger.debug(f"[SHAREPOINT] Full request body: {json.dumps(item_data, indent=2)}")
            
            try:
                response = requests.post(
                    list_endpoint,
                    json=item_data,
                    headers=headers,
                    timeout=10
                )
                logger.debug(f"[SHAREPOINT] Response status code: {response.status_code}")
                logger.debug(f"[SHAREPOINT] Response headers: {dict(response.headers)}")
                logger.debug(f"[SHAREPOINT] Response body: {response.text[:1000]}")
            except Exception as req_error:
                logger.error(f"[SHAREPOINT] Request exception: {req_error}", exc_info=True)
                raise
            
            if response.status_code in [200, 201]:
                logger.info(f"Logged activity to SharePoint: {activity_type} by {user_name}")
                logger.debug(f"[SHAREPOINT] Successfully created list item")
                return True
            else:
                logger.error(
                    f"Failed to log to SharePoint. Status: {response.status_code}, "
                    f"Response: {response.text}"
                )
                logger.debug(f"[SHAREPOINT] Error response body: {response.text[:500]}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"[SHAREPOINT] Network error connecting to SharePoint: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error logging to SharePoint: {e}", exc_info=True)
            return False
    
    def _get_site_id(self, access_token: str) -> Optional[str]:
        """
        Get the Microsoft Graph site ID for the SharePoint site.
        
        Args:
            access_token: Azure AD access token
            
        Returns:
            Site ID if found, None otherwise
        """
        if self._site_id:
            return self._site_id
            
        try:
            # Parse the SharePoint URL to get hostname and site path
            from urllib.parse import urlparse
            parsed = urlparse(self.site_url)
            hostname = parsed.hostname
            site_path = parsed.path
            
            logger.debug(f"[SHAREPOINT] Resolving site ID for {hostname}:{site_path}")
            
            # Use Graph API to get site ID
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
            }
            
            response = requests.get(endpoint, headers=headers, timeout=10)
            
            if response.status_code == 200:
                site_data = response.json()
                self._site_id = site_data.get('id')
                logger.debug(f"[SHAREPOINT] Resolved site ID: {self._site_id}")
                return self._site_id
            else:
                logger.error(f"Failed to get site ID. Status: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting site ID: {e}", exc_info=True)
            return None
    
    def _get_list_id(self, access_token: str, site_id: str) -> Optional[str]:
        """
        Get the Microsoft Graph list ID for the SharePoint list.
        
        Args:
            access_token: Azure AD access token
            site_id: Microsoft Graph site ID
            
        Returns:
            List ID if found, None otherwise
        """
        if self._list_id:
            return self._list_id
            
        try:
            logger.debug(f"[SHAREPOINT] Resolving list ID for '{self.list_name}'")
            
            # Use Graph API to get list by display name
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
            }
            
            response = requests.get(endpoint, headers=headers, timeout=10)
            
            if response.status_code == 200:
                lists_data = response.json()
                for list_item in lists_data.get('value', []):
                    if list_item.get('displayName') == self.list_name:
                        self._list_id = list_item.get('id')
                        logger.debug(f"[SHAREPOINT] Resolved list ID: {self._list_id}")
                        return self._list_id
                
                logger.error(f"List '{self.list_name}' not found in site")
                return None
            else:
                logger.error(f"Failed to get lists. Status: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting list ID: {e}", exc_info=True)
            return None
    
    def _get_client_ip(self) -> str:
        """
        Get the client's IP address, accounting for proxies.
        
        Returns:
            Client IP address
        """
        # Check for forwarded IP (common in Azure App Service)
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            # X-Forwarded-For can contain multiple IPs, take the first one
            return forwarded_for.split(',')[0].strip()
        
        # Check for client IP header (Azure App Service)
        client_ip = request.headers.get('X-Client-IP')
        if client_ip:
            return client_ip
        
        # Fall back to remote_addr
        return request.remote_addr or 'Unknown'
    
    def ensure_list_exists(self, access_token: str) -> bool:
        """
        Check if the SharePoint list exists.
        
        Args:
            access_token: Azure AD access token from Easy Auth
            
        Returns:
            True if list exists, False otherwise
        """
        try:
            # Get site ID
            site_id = self._get_site_id(access_token)
            if not site_id:
                logger.error("Failed to resolve SharePoint site ID")
                return False
            
            # Check if list exists using Microsoft Graph API
            list_id = self._get_list_id(access_token, site_id)
            
            if list_id:
                logger.info(f"SharePoint list '{self.list_name}' exists")
                return True
            else:
                logger.warning(
                    f"SharePoint list '{self.list_name}' not found. "
                    "Please create the list manually with the following columns:\n"
                    "- Title (Single line of text)\n"
                    "- UserName (Single line of text)\n"
                    "- UserEmail (Single line of text)\n"
                    "- ActivityType (Single line of text)\n"
                    "- Application (Single line of text)\n"
                    "- UserRole (Single line of text)\n"
                    "- LoginTimestamp (Date and Time)\n"
                    "- IPAddress (Single line of text)\n"
                    "- UserAgent (Multiple lines of text)\n"
                    "- Details (Multiple lines of text)"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error checking SharePoint list: {e}", exc_info=True)
            return False


def log_user_activity(
    user_info: Dict[str, Any],
    activity_type: str,
    site_url: str,
    list_name: str = 'AuditLog',
    details: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Convenience function to log user activity to SharePoint.
    
    Args:
        user_info: User info dictionary from get_easy_auth_user()
        activity_type: Type of activity (e.g., 'Upload', 'View', 'Export')
        site_url: SharePoint site URL
        list_name: Name of the SharePoint list
        details: Optional additional details
        
    Returns:
        True if logging was successful, False otherwise
    """
    logger.debug(f"[SHAREPOINT] log_user_activity called for activity: {activity_type}")
    logger.debug(f"[SHAREPOINT] User info present: {user_info is not None}")
    
    if not user_info or not user_info.get('access_token'):
        logger.warning("Cannot log to SharePoint: No user info or access token")
        logger.debug(f"[SHAREPOINT] User info keys: {list(user_info.keys()) if user_info else 'None'}")
        return False
    
    logger.debug(f"[SHAREPOINT] Creating SharePointLogger instance")
    logger_instance = SharePointLogger(site_url, list_name)
    
    # Extract user_role from details if present
    user_role = 'user'  # default
    if details and 'user_role' in details:
        user_role = details.pop('user_role')  # Remove from details to avoid duplication
    
    return logger_instance.log_activity(
        access_token=user_info['access_token'],
        user_name=user_info['name'],
        user_email=user_info['email'],
        activity_type=activity_type,
        user_role=user_role,
        details=details
    )
