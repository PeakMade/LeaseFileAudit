"""
Flask application factory for Lease File Audit.
"""
from flask import Flask, g, session, request
from flask_caching import Cache
from pathlib import Path
import logging
import os
from datetime import datetime, timedelta
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Silence verbose Azure SDK logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.monitor.opentelemetry.exporter").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)

# Initialize cache (will be configured in create_app)
cache = Cache()


def create_app(config_name='default'):
    """
    Application factory pattern.
    
    Args:
        config_name: Configuration name (for future environments)
    
    Returns:
        Configured Flask application instance
    """
    app = Flask(__name__)
    
    # App configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
    app.config['UPLOAD_FOLDER'] = Path('instance/runs')
    app.config['TEMPLATES_AUTO_RELOAD'] = True  # Disable template caching
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(
        minutes=int(os.getenv('SESSION_IDLE_TIMEOUT_MINUTES', '30'))
    )
    
    # Cache configuration
    # Use SimpleCache for single-worker deployments (current setup)
    # For multi-worker: switch to Redis or FileSystemCache
    app.config['CACHE_TYPE'] = 'SimpleCache'  # In-memory cache
    app.config['CACHE_DEFAULT_TIMEOUT'] = 600  # 10 minutes default
    
    # Initialize cache with app
    cache.init_app(app)
    
    app.logger.info(f"[CACHE] Initialized {app.config['CACHE_TYPE']} with {app.config['CACHE_DEFAULT_TIMEOUT']}s timeout")
    
    # Ensure instance folder exists
    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)
    
    # Register blueprints
    from web.views import bp as main_bp
    app.register_blueprint(main_bp)
    
    # Register authentication context processor
    from web.auth import get_current_user
    import pandas as pd
    
    @app.template_filter('safe_strftime')
    def safe_strftime(date_value, format_string='%Y-%m-%d'):
        """Safely format datetime, handling NaT and None values."""
        if date_value is None:
            return '-'
        if pd.isna(date_value):
            return '-'
        try:
            return date_value.strftime(format_string)
        except (AttributeError, ValueError):
            return '-'
    
    @app.context_processor
    def inject_user():
        """Make user info available in all templates."""
        # Check if authentication is required
        require_auth_enabled = os.getenv('REQUIRE_AUTH', 'true').lower() == 'true'
        
        if not require_auth_enabled:
            # Local development mode - inject mock user if not already set
            from flask import g
            if not hasattr(g, 'user') or g.user is None:
                g.user = {
                    'user_id': 'local-dev-user',
                    'name': 'Local Developer',
                    'email': 'dev@localhost',
                    'identity_provider': 'local'
                }
        
        return {'user': get_current_user()}
    
    @app.before_request
    def log_request_info():
        """Log request info and maintain app-level session lifecycle for activity logging."""
        from web.auth import get_easy_auth_user
        from config import config
        from activity_logging.sharepoint import log_user_activity

        def _parse_iso_datetime(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(value)
            except Exception:
                return None

        require_auth_enabled = os.getenv('REQUIRE_AUTH', 'true').lower() == 'true'

        if require_auth_enabled:
            user = get_easy_auth_user()
        else:
            user = {
                'user_id': 'local-dev-user',
                'name': os.getenv('LOCAL_DEV_USER_NAME', 'Local Developer'),
                'email': os.getenv('LOCAL_DEV_USER_EMAIL', 'dev@localhost'),
                'identity_provider': 'local'
            }

        if user:
            app.logger.info(
                f"Request: {user['name']} ({user['email']}) -> "
                f"{request.method} {request.path}"
            )

        if not user:
            return

        timeout_minutes = int(os.getenv('SESSION_IDLE_TIMEOUT_MINUTES', '30'))
        now = datetime.utcnow()

        session.permanent = True
        current_session_id = session.get('session_id')
        session_started_at = _parse_iso_datetime(session.get('session_started_at'))
        last_activity_at = _parse_iso_datetime(session.get('last_activity_at'))

        is_expired = False
        if current_session_id and last_activity_at:
            is_expired = now - last_activity_at > timedelta(minutes=timeout_minutes)

        if current_session_id and is_expired:
            app.logger.info(
                f"[SESSION] Expired session for user {user.get('email')} "
                f"(session_id={current_session_id}, idle_minutes={timeout_minutes})"
            )
            if config.auth.can_log_to_sharepoint():
                log_user_activity(
                    user_info=user,
                    activity_type='End Session',
                    site_url=config.auth.sharepoint_site_url,
                    list_name=config.auth.sharepoint_list_name,
                    details={
                        'page': request.path,
                        'user_role': 'user',
                        'session_id': current_session_id,
                        'session_end_reason': 'timeout'
                    }
                )

            session.pop('session_id', None)
            session.pop('session_started_at', None)
            session.pop('last_activity_at', None)
            current_session_id = None
            session_started_at = None

        if not current_session_id:
            current_session_id = str(uuid.uuid4())
            session_started_at = now
            session['session_id'] = current_session_id
            session['session_started_at'] = session_started_at.isoformat()

            app.logger.info(
                f"[SESSION] Started session for user {user.get('email')} "
                f"(session_id={current_session_id})"
            )
            if config.auth.can_log_to_sharepoint():
                log_user_activity(
                    user_info=user,
                    activity_type='Start Session',
                    site_url=config.auth.sharepoint_site_url,
                    list_name=config.auth.sharepoint_list_name,
                    details={
                        'page': request.path,
                        'user_role': 'user',
                        'session_id': current_session_id
                    }
                )

        session['last_activity_at'] = now.isoformat()
        g.session_id = current_session_id
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
