"""
Flask application factory for Lease File Audit.
"""
from flask import Flask, g
from pathlib import Path
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


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
    
    # Ensure instance folder exists
    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)
    
    # Register blueprints
    from web.views import bp as main_bp
    app.register_blueprint(main_bp)
    
    # Register authentication context processor
    from web.auth import get_current_user
    
    @app.context_processor
    def inject_user():
        """Make user info available in all templates."""
        return {'user': get_current_user()}
    
    @app.before_request
    def log_request_info():
        """Log request information for debugging."""
        from web.auth import get_easy_auth_user
        user = get_easy_auth_user()
        if user:
            app.logger.info(
                f"Request: {user['name']} ({user['email']}) -> "
                f"{os.environ.get('REQUEST_METHOD', 'GET')} {os.environ.get('PATH_INFO', '/')}"
            )
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
