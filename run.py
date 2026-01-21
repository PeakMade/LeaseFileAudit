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
    port = int(os.environ.get('PORT', 8080))
    # Must bind to 0.0.0.0 for Azure, use debug=False in production
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
