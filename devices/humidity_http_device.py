import sys
from pathlib import Path

# Add devices directory to path to import base class
sys.path.insert(0, str(Path(__file__).parent))

from base_http_device import BaseHttpDevice


class HumidityHttpDevice(BaseHttpDevice):
    
    def __init__(self, device_number=1, interval=2, collector_url=None):
        super().__init__(device_number, "humidity", interval, collector_url)
    
    def _extract_sensor_value(self, parts):
        return parts[-3]


def start_humidity_device(device_number=1, interval=2, collector_url=None):
    device = HumidityHttpDevice(device_number=device_number, interval=interval, collector_url=collector_url)
    device.start()
    return device


if __name__ == "__main__":
    device = start_humidity_device(device_number=1, interval=4)
    
    try:
        while device.is_running():
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        device.stop()
