import json
import sys
import threading
import requests
import time
from pathlib import Path

# Add devices and collector directories to path
sys.path.insert(0, str(Path(__file__).parent / "devices"))
sys.path.insert(0, str(Path(__file__).parent / "collector"))
sys.path.insert(0, str(Path(__file__).parent))


def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def generate_csv_filename(config):
    """Generate CSV filename based on configuration."""
    message_frequency = config["message_interval"]
    num_devices = config["num_devices"]
    protocol = config["protocol"].upper()
    
    filename = f"{message_frequency}_{num_devices}_{protocol}.csv"
    return filename


def create_devices(config):
    """Create and start devices based on configuration."""
    num_devices = config["num_devices"]
    interval = config["message_interval"]
    protocol = config["protocol"].upper()
    devices = []
    
    if protocol == "HTTP":
        # Import HTTP device classes
        import importlib
        temperature_module = importlib.import_module("temperature_http_device")
        humidity_module = importlib.import_module("humidity_http_device")
        light_module = importlib.import_module("light_http_device")
        
        TemperatureHttpDevice = temperature_module.TemperatureHttpDevice
        HumidityHttpDevice = humidity_module.HumidityHttpDevice
        LightHttpDevice = light_module.LightHttpDevice
        
        collector_url = config["http_server"]
        
        # Distribute devices across sensor types
        sensor_types = [
            (TemperatureHttpDevice, "temperature"),
            (HumidityHttpDevice, "humidity"),
            (LightHttpDevice, "light")
        ]
        
        for i in range(num_devices):
            device_class, sensor_type = sensor_types[i % len(sensor_types)]
            device = device_class(
                device_number=i + 1,
                interval=interval,
                collector_url=collector_url
            )
            device.start()
            devices.append(device)
            
    elif protocol == "MQTT":
        # Import MQTT device classes
        import importlib
        temperature_module = importlib.import_module("temperature_mqtt_device")
        humidity_module = importlib.import_module("humidity_mqtt_device")
        light_module = importlib.import_module("light_mqtt_device")
        
        TemperatureMqttDevice = temperature_module.TemperatureMqttDevice
        HumidityMqttDevice = humidity_module.HumidityMqttDevice
        LightMqttDevice = light_module.LightMqttDevice
        
        broker = config["mqtt_broker"]
        topic = config["mqtt_topic"]
        
        # Distribute devices across sensor types
        sensor_types = [
            (TemperatureMqttDevice, "temperature"),
            (HumidityMqttDevice, "humidity"),
            (LightMqttDevice, "light")
        ]
        
        for i in range(num_devices):
            device_class, sensor_type = sensor_types[i % len(sensor_types)]
            device = device_class(
                device_number=i + 1,
                interval=interval,
                broker=broker,
                topic=topic
            )
            device.start()
            devices.append(device)
    else:
        raise ValueError(f"Unknown protocol: {protocol}. Must be 'HTTP' or 'MQTT'")
    
    return devices


def _wait_for_http(host: str, port: int, timeout_seconds: int = 20):
    """Wait until an HTTP server is accepting connections on host:port."""
    base_url = f"http://{host}:{port}"
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            # Any response (200/404/405/etc) indicates server is up
            resp = requests.get(base_url, timeout=1)
            return True
        except Exception:
            time.sleep(0.5)
    return False


def start_collector(config, csv_filename):
    protocol = config["protocol"].upper()
    
    if protocol == "HTTP":
        from collector.http_collector import HttpCollector
        
        # Parse URL to get host and port
        http_server = config["http_server"]
        if http_server.startswith("http://"):
            http_server = http_server[7:]
        if ":" in http_server:
            host, port = http_server.split(":")
            port = port.split("/")[0]
            port = int(port)
        else:
            host = http_server
            port = 5000
        
        collector = HttpCollector(csv_filename=csv_filename, host=host, port=port, debug=False)
        
        # Run HTTP collector in a separate thread since it blocks
        collector_thread = threading.Thread(target=collector.start, daemon=True)
        collector_thread.start()
        
        # Wait until server is reachable before proceeding
        if not _wait_for_http(host, port, timeout_seconds=30):
            raise RuntimeError(f"HTTP collector at {host}:{port} did not become ready in time")
        
        return collector, collector_thread
        
    elif protocol == "MQTT":
        from collector.mqtt_collector import MqttCollector
        
        collector = MqttCollector(
            csv_filename=csv_filename,
            broker=config["mqtt_broker"],
            topic=config["mqtt_topic"]
        )
        
        # Run MQTT collector in a separate thread since it blocks
        collector_thread = threading.Thread(target=collector.start, daemon=True)
        collector_thread.start()
        
        # Give it a moment to connect
        time.sleep(1)
        
        return collector, collector_thread
    else:
        raise ValueError(f"Unknown protocol: {protocol}. Must be 'HTTP' or 'MQTT'")


def main():
    """Main demo function."""
    print("=" * 60)
    print("IoT System Demo")
    print("=" * 60)
    
    # Load configuration
    try:
        config = load_config()
        print(f"[DEMO] Loaded configuration from config.json")
        print(f"[DEMO] Protocol: {config['protocol']}")
        print(f"[DEMO] Number of devices: {config['num_devices']}")
        print(f"[DEMO] Message interval: {config['message_interval']}s")
    except FileNotFoundError:
        print("[ERROR] config.json not found!")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in config.json: {e}")
        sys.exit(1)
    
    # Generate CSV filename
    csv_filename = generate_csv_filename(config)
    print(f"[DEMO] Data will be saved to: {csv_filename}")
    
    devices = []
    collector = None
    collector_thread = None
    
    try:
        # Start collector
        print("\n[DEMO] Starting collector...")
        collector, collector_thread = start_collector(config, csv_filename)
        print("[DEMO] Collector started")
        
        # Create and start devices
        print(f"\n[DEMO] Creating {config['num_devices']} {config['protocol']} devices...")
        devices = create_devices(config)
        print(f"[DEMO] All devices started")
        
        print("\n[DEMO] System running. Press Ctrl+C to stop...")
        print("=" * 60)
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[DEMO] Shutting down...")
    except Exception as e:
        print(f"\n[ERROR] {e}")
    finally:
        # Stop all devices
        print("[DEMO] Stopping devices...")
        for device in devices:
            try:
                device.stop()
            except Exception as e:
                print(f"[ERROR] Error stopping device: {e}")
        
        # Stop collector
        print("[DEMO] Stopping collector...")
        try:
            if collector:
                if hasattr(collector, 'stop'):
                    collector.stop()
                else:
                    # For HTTP collector, we can't gracefully stop it from here
                    # It will stop when the daemon thread is killed
                    print("[DEMO] HTTP collector will stop when program exits")
        except Exception as e:
            print(f"[ERROR] Error stopping collector: {e}")
        
        print("[DEMO] Shutdown complete")


if __name__ == "__main__":
    main()

