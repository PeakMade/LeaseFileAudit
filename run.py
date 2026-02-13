"""
Application entrypoint.
"""
import os
import sys
sys.stdout.flush()  # Ensure prints are shown immediately

from app import create_app

app = create_app()

if __name__ == '__main__':
    # Azure App Service sets PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    # Use debug mode for local development to enable auto-reload
    debug = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=True)
