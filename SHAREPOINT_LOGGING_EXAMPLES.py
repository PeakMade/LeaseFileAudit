"""
Example: Using SharePoint logging in your Flask routes.

This file demonstrates how to integrate SharePoint activity logging
into your Flask application routes.
"""

from flask import Blueprint, request, redirect, url_for, flash
from web.auth import require_auth, get_current_user
from config import config
from logging.sharepoint import log_user_activity

bp = Blueprint('example', __name__)


@bp.route('/example-upload', methods=['POST'])
@require_auth
def example_upload():
    """Example: Log file upload activity."""
    user = get_current_user()
    
    # Get file from request
    file = request.files.get('file')
    if not file:
        flash('No file uploaded', 'danger')
        return redirect(url_for('main.index'))
    
    # Process the file
    filename = file.filename
    file_size = len(file.read())
    file.seek(0)  # Reset file pointer
    
    # ... your file processing logic ...
    
    # Log the upload activity to SharePoint
    if config.auth.can_log_to_sharepoint():
        success = log_user_activity(
            user_info=user,
            activity_type='File Upload',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'filename': filename,
                'size_bytes': file_size,
                'action': 'upload',
                'status': 'success'
            }
        )
        
        if not success:
            # Log failed but don't break the user flow
            print(f"Warning: Failed to log activity for user {user['name']}")
    
    flash(f'File {filename} uploaded successfully', 'success')
    return redirect(url_for('main.index'))


@bp.route('/example-view/<item_id>')
@require_auth
def example_view(item_id):
    """Example: Log item view activity."""
    user = get_current_user()
    
    # ... your view logic ...
    
    # Log the view activity
    if config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type='View Item',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'item_id': item_id,
                'action': 'view'
            }
        )
    
    return f"Viewing item {item_id}"


@bp.route('/example-export', methods=['POST'])
@require_auth
def example_export():
    """Example: Log data export activity."""
    user = get_current_user()
    
    export_type = request.form.get('export_type', 'csv')
    record_count = 1000  # ... your export logic ...
    
    # Log the export
    if config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type='Data Export',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'export_type': export_type,
                'record_count': record_count,
                'action': 'export'
            }
        )
    
    return f"Exported {record_count} records as {export_type}"


@bp.route('/example-settings-change', methods=['POST'])
@require_auth
def example_settings_change():
    """Example: Log configuration change."""
    user = get_current_user()
    
    old_value = request.form.get('old_value')
    new_value = request.form.get('new_value')
    setting_name = request.form.get('setting_name')
    
    # ... update settings logic ...
    
    # Log the configuration change
    if config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type='Settings Change',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'setting_name': setting_name,
                'old_value': old_value,
                'new_value': new_value,
                'action': 'config_change'
            }
        )
    
    flash('Settings updated successfully', 'success')
    return redirect(url_for('main.settings'))


@bp.route('/example-delete/<item_id>', methods=['POST'])
@require_auth
def example_delete(item_id):
    """Example: Log deletion activity."""
    user = get_current_user()
    
    # ... delete logic ...
    
    # Log the deletion (important for audit trail!)
    if config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type='Delete Item',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'item_id': item_id,
                'action': 'delete',
                'severity': 'high'
            }
        )
    
    flash('Item deleted successfully', 'warning')
    return redirect(url_for('main.index'))


# Example: Using SharePointLogger class directly for more control
from logging.sharepoint import SharePointLogger


@bp.route('/example-batch-operation', methods=['POST'])
@require_auth
def example_batch_operation():
    """Example: Log batch operation with custom logger."""
    user = get_current_user()
    
    if not config.auth.can_log_to_sharepoint():
        # SharePoint logging not configured
        return "Batch operation completed (logging disabled)"
    
    # Create logger instance
    sp_logger = SharePointLogger(
        site_url=config.auth.sharepoint_site_url,
        list_name=config.auth.sharepoint_list_name
    )
    
    # Process batch items
    items = ['item1', 'item2', 'item3']
    results = []
    
    for item in items:
        # ... process each item ...
        success = True  # your processing result
        results.append(success)
    
    # Log the batch operation once
    sp_logger.log_activity(
        access_token=user['access_token'],
        user_name=user['name'],
        user_email=user['email'],
        activity_type='Batch Operation',
        app_name='LeaseFileAudit',
        details={
            'total_items': len(items),
            'successful': sum(results),
            'failed': len(results) - sum(results),
            'action': 'batch_process'
        }
    )
    
    return f"Processed {len(items)} items"


# Example: Conditional logging based on activity severity
@bp.route('/example-conditional-logging', methods=['POST'])
@require_auth
def example_conditional_logging():
    """Example: Only log high-severity activities."""
    user = get_current_user()
    
    action = request.form.get('action')
    severity = 'low'  # default
    
    # Determine severity
    high_severity_actions = ['delete', 'export_all', 'config_change']
    if action in high_severity_actions:
        severity = 'high'
    
    # Only log high-severity activities
    if severity == 'high' and config.auth.can_log_to_sharepoint():
        log_user_activity(
            user_info=user,
            activity_type=f'High Severity: {action}',
            site_url=config.auth.sharepoint_site_url,
            list_name=config.auth.sharepoint_list_name,
            details={
                'action': action,
                'severity': severity,
                'requires_review': True
            }
        )
    
    return f"Action '{action}' completed"


# Example: Error handling with logging
@bp.route('/example-with-error-handling', methods=['POST'])
@require_auth
def example_with_error_handling():
    """Example: Log errors and exceptions."""
    user = get_current_user()
    
    try:
        # ... your operation that might fail ...
        risky_operation()
        
        # Log success
        if config.auth.can_log_to_sharepoint():
            log_user_activity(
                user_info=user,
                activity_type='Risky Operation',
                site_url=config.auth.sharepoint_site_url,
                list_name=config.auth.sharepoint_list_name,
                details={'status': 'success'}
            )
        
        return "Operation successful"
        
    except Exception as e:
        # Log the error
        if config.auth.can_log_to_sharepoint():
            log_user_activity(
                user_info=user,
                activity_type='Operation Error',
                site_url=config.auth.sharepoint_site_url,
                list_name=config.auth.sharepoint_list_name,
                details={
                    'status': 'error',
                    'error_message': str(e),
                    'severity': 'high'
                }
            )
        
        flash(f'Operation failed: {str(e)}', 'danger')
        return redirect(url_for('main.index'))


def risky_operation():
    """Placeholder for an operation that might fail."""
    pass


# Best Practices Summary:
#
# 1. Always check config.auth.can_log_to_sharepoint() before logging
# 2. Don't let logging failures break your application flow
# 3. Log high-severity actions (deletes, exports, config changes)
# 4. Include meaningful details in the details dictionary
# 5. Log both successes and failures for complete audit trail
# 6. Use descriptive activity_type values
# 7. Consider batching logs for performance-intensive operations
# 8. Don't log sensitive data (passwords, tokens, PII)
