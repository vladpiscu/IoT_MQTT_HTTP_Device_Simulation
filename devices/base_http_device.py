import time
import threading
import requests
from datetime import datetime, UTC


COLLECTOR_URL = "http://127.0.0.1:5000/data"


class BaseHttpDevice:
    
    def __init__(self, device_number, sensor_type, interval=2, collector_url=None):
        self.device_id = f"{sensor_type}_http_{device_number}"
        self.sensor_type = sensor_type
        self.interval = interval
        self.collector_url = collector_url or COLLECTOR_URL
        self.stop_event = threading.Event()
        self.thread = None
        
    def _extract_sensor_value(self, parts):
        raise NotImplementedError("Subclasses must implement _extract_sensor_value method")
        
    def _run(self):
        # Open and read the data file
        with open("data.txt", "r") as f:
            lines = f.readlines()
        
        line_index = 0
        
        while not self.stop_event.is_set():
            # Read the next line from data.txt
            if line_index >= len(lines):
                line_index = 0  # Loop back to the beginning
            
            line = lines[line_index].strip()
            
            # Parse the line to extract sensor value
            parts = line.split()
            if len(parts) >= 4:
                try:
                    sensor_value = self._extract_sensor_value(parts)
                    
                    if sensor_value is not None:
                        reading = {
                            "device_id": self.device_id,
                            "protocol": "http",
                            "timestamp": datetime.now(UTC).isoformat(),
                            "sensor": self.sensor_type,
                            "value": float(sensor_value)
                        }
                        
                        try:
                            requests.post(self.collector_url, json=reading)
                            print(f"[HTTP DEVICE] {self.device_id} - Sent reading: {reading}")
                        except Exception as e:
                            print(f"[HTTP DEVICE] {self.device_id} - Error: {e}")
                except NotImplementedError:
                    print(f"[HTTP DEVICE] {self.device_id} - Error: _extract_sensor_value not implemented")
                    break
            
            line_index += 1
            
            # Sleep for the configured interval, but check stop_event periodically
            self.stop_event.wait(timeout=self.interval)
    
    def start(self):
        if self.thread and self.thread.is_alive():
            print(f"[HTTP DEVICE] {self.device_id} - Already running")
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[HTTP DEVICE] {self.device_id} - Started (interval: {self.interval}s)")
    
    def stop(self):
        if self.thread and self.thread.is_alive():
            print(f"[HTTP DEVICE] {self.device_id} - Stopping...")
            self.stop_event.set()
            self.thread.join(timeout=5)
            print(f"[HTTP DEVICE] {self.device_id} - Stopped")
    
    def is_running(self):
        return self.thread and self.thread.is_alive()
