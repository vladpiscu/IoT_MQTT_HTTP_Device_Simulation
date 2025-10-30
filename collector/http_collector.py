import sys
from pathlib import Path

# Add parent directory to path to import storage module
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, request, jsonify
from storage import save_to_csv
from datetime import datetime, UTC


class HttpCollector:
    
    def __init__(self, csv_filename="iot_data.csv", host="127.0.0.1", port=5000, debug=True, threaded=True):
        self.csv_filename = csv_filename
        self.host = host
        self.port = port
        self.debug = debug
        self.threaded = threaded
        self.app = Flask(__name__)
        self._setup_routes()
    
    def _setup_routes(self):

        @self.app.route("/data", methods=["POST"])
        def collect():
            data = request.json
            receive_time = datetime.now(UTC).isoformat()
            
            if data:
                save_to_csv(data, self.csv_filename, receive_time=receive_time)
                return jsonify({"status": "ok"}), 200
            else:
                return jsonify({"status": "error", "message": "No data received"}), 400
    
    def start(self):
        print(f"[HTTP COLLECTOR] Starting on http://{self.host}:{self.port} (threaded={self.threaded})")
        self.app.run(host=self.host, port=self.port, debug=self.debug, threaded=self.threaded)


if __name__ == "__main__":
    collector = HttpCollector()
    collector.start()

