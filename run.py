"""
Application entrypoint.
"""
import sys
sys.stdout.flush()  # Ensure prints are shown immediately

from app import create_app

app = create_app()

if __name__ == '__main__':
    # Force unbuffered output for debugging
    app.run(debug=True, host='127.0.0.1', port=8080, use_reloader=True)
