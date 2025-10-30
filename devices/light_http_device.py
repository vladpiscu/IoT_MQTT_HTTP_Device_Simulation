import sys
from pathlib import Path

# Add devices directory to path to import base class
sys.path.insert(0, str(Path(__file__).parent))

from base_http_device import BaseHttpDevice


class LightHttpDevice(BaseHttpDevice):
    
    def __init__(self, device_number=1, interval=2, collector_url=None):
        super().__init__(device_number, "light", interval, collector_url)
    
    def _extract_sensor_value(self, parts):
        return parts[-2]


def start_light_device(device_number=1, interval=2, collector_url=None):
    device = LightHttpDevice(device_number=device_number, interval=interval, collector_url=collector_url)
    device.start()
    return device


if __name__ == "__main__":
    device = start_light_device(device_number=1, interval=5)
    
    try:
        while device.is_running():
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        device.stop()
