"""
Application entrypoint.
"""
import os
import sys
import subprocess
import threading
import webbrowser
import time
sys.stdout.flush()  # Ensure prints are shown immediately

from app import create_app

app = create_app()


def _open_external_browser(url: str) -> None:
    """Open URL in the OS default browser (outside VS Code workspace browser)."""
    if os.name == 'nt':
        # Use Windows shell `start` to force opening in the system default browser.
        subprocess.Popen(["cmd", "/c", "start", "", url])
        return

    webbrowser.open_new_tab(url)

if __name__ == '__main__':
    # Azure App Service sets PORT environment variable; default to 8000 for local development
    port = int(os.environ.get('PORT', 8000))
    # Use debug mode for local development to enable auto-reload
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    use_reloader = os.environ.get('FLASK_USE_RELOADER', 'False').lower() == 'true'
    print(f"[STARTUP] port={port} debug={debug} use_reloader={use_reloader}")
    
    # Open external browser after a short delay to allow server to start
    def open_browser():
        time.sleep(2)  # Wait for server to start
        url = f'http://127.0.0.1:{port}'
        _open_external_browser(url)
        print(f"[STARTUP] Opened browser at {url}")
    
    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()
    
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=use_reloader, threaded=True)
