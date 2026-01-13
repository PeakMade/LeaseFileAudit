"""
Flask application factory for Lease File Audit.
"""
from flask import Flask
from pathlib import Path


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
    app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'
    app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
    app.config['UPLOAD_FOLDER'] = Path('instance/runs')
    
    # Ensure instance folder exists
    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)
    
    # Register blueprints
    from web.views import bp as main_bp
    app.register_blueprint(main_bp)
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
