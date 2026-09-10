"""
========================================================================================
STREAMING OPERATIONS CENTER - ONE-CLICK WEB DEMONSTRATION RUNNER
Kafka Ingestion • Avro Serialization • Real-Time Aggregations • Resilient DLQ
========================================================================================

Usage:
    python web_demo.py
    python web_demo.py --port 8000 --no-browser

Opens the interactive real-time operations dashboard in your web browser.
"""

import sys
import time
import argparse
import webbrowser
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BANNER = r"""
  ___ _                          _               ___             
 / __| |_ _ _ ___ __ _ _ __  ___| |___ _ __  ___| _ \_ _ ___ ___ 
 \__ \  _| '_/ -_) _` | '  \/ -_) / _ \ '_ \/ -_)  _/ '_/ -_)___/
 |___/\__|_| \___\__,_|_|_|_\___|_\___/ .__/\___|_| |_| \___|    
                                       |_|                       
 >>> REAL-TIME KAPPA PIPELINE & CHAPTER 3 AVRO DLQ LAB <<<
"""


def main():
    parser = argparse.ArgumentParser(description="Launch Real-Time Streaming Operations Center")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host interface to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind the web server to (default: 8000)")
    parser.add_argument("--no-browser", action="store_true", help="Do not automatically open default web browser")
    args = parser.parse_args()

    print(BANNER)
    url = f"http://{args.host}:{args.port}"
    print(f"  [+] Starting Web Operations Center on: {url}")
    print(f"  [+] WebSocket Endpoint: ws://{args.host}:{args.port}/ws/stream")
    print(f"  [+] Interactive Swagger API Docs: {url}/docs")
    print("  [+] Press Ctrl+C to terminate cleanly.\n")

    if not args.no_browser:
        def open_browser():
            time.sleep(1.2)
            try:
                webbrowser.open(url)
            except Exception:
                pass

        import threading
        threading.Thread(target=open_browser, daemon=True).start()

    try:
        import uvicorn
        uvicorn.run("web.app:app", host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down Web Operations Center gracefully. Goodbye!")
    except Exception as e:
        print(f"\n[ERROR] Failed to start server: {e}")


if __name__ == "__main__":
    main()
