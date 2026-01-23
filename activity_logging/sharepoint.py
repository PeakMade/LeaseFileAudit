"""
SharePoint logging module for Azure App Service Easy Auth.

This module logs user activity to a SharePoint list using the user's
Azure AD access token obtained from Easy Auth headers.
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
    
    This class uses the Office 365 REST API to write audit logs to a
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
            
            # Prepare the list item data
            # For list metadata type, SharePoint uses a specific format:
            # Remove spaces and special chars, capitalize first letter of each word
            list_type = ''.join([word.capitalize() for word in self.list_name.replace('-', '').replace('_', '').split()])
            logger.debug(f"[SHAREPOINT] List type name: SP.Data.{list_type}ListItem")
            
            item_data = {
                '__metadata': {'type': f'SP.Data.{list_type}ListItem'},
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
            
            logger.debug(f"[SHAREPOINT] Item data: {item_data}")
            
            # Add details if provided
            if details:
                item_data['Details'] = str(details)
            
            # Get the list endpoint
            list_endpoint = f"{self.site_url}/_api/web/lists/getbytitle('{self.list_name}')/items"
            logger.debug(f"[SHAREPOINT] Endpoint: {list_endpoint}")
            logger.debug(f"[SHAREPOINT] List name used in URL: {self.list_name}")
            
            # Prepare headers
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json;odata=verbose',
                'Content-Type': 'application/json;odata=verbose',
            }
            logger.debug(f"[SHAREPOINT] Request headers prepared")
            logger.debug(f"[SHAREPOINT] Authorization header: Bearer {access_token[:20]}...{access_token[-20:] if len(access_token) > 40 else ''}\")")
            
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
                logger.error(f"[SHAREPOINT] Request exception: {req_error}\", exc_info=True)
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
        Check if the SharePoint list exists and create it if needed.
        
        Args:
            access_token: Azure AD access token from Easy Auth
            
        Returns:
            True if list exists or was created, False otherwise
        """
        try:
            # Check if list exists
            list_endpoint = f"{self.site_url}/_api/web/lists/getbytitle('{self.list_name}')"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json;odata=verbose',
            }
            
            response = requests.get(list_endpoint, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"SharePoint list '{self.list_name}' exists")
                return True
            elif response.status_code == 404:
                logger.warning(
                    f"SharePoint list '{self.list_name}' not found. "
                    "Please create the list manually with the following columns:\n"
                    "- Title (Single line of text)\n"
                    "- UserName (Single line of text)\n"
                    "- UserEmail (Single line of text)\n"
                    "- ActivityType (Single line of text)\n"
                    "- AppName (Single line of text)\n"
                    "- Timestamp (Date and Time)\n"
                    "- IPAddress (Single line of text)\n"
                    "- UserAgent (Multiple lines of text)\n"
                    "- Details (Multiple lines of text)"
                )
                return False
            else:
                logger.error(f"Error checking SharePoint list: {response.status_code}")
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
