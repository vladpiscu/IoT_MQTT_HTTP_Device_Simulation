import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from base_mqtt_device import BaseMqttDevice


class LightMqttDevice(BaseMqttDevice):
    
    def __init__(self, device_number=1, interval=2, broker="localhost", port=1883, topic="iot", keepalive=60):
        super().__init__(device_number, "light", interval, broker, port, topic, keepalive)
    
    def _extract_sensor_value(self, parts):
        return parts[-2]


def start_light_mqtt_device(device_number=1, interval=2, broker="localhost", port=1883, topic="iot", keepalive=60):
    device = LightMqttDevice(device_number=device_number, interval=interval, broker=broker, port=port, topic=topic, keepalive=keepalive)
    device.start()
    return device


if __name__ == "__main__":
    device = start_light_mqtt_device(device_number=1, interval=5)
    
    try:
        while device.is_running():
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        device.stop()
