import time
import threading
import json
import paho.mqtt.client as mqtt
from datetime import datetime, UTC


class BaseMqttDevice:
    
    def __init__(self, device_number, sensor_type, interval=2, broker="localhost", port=1883, topic="iot", keepalive=60):
        self.device_id = f"{sensor_type}_mqtt_{device_number}"
        self.sensor_type = sensor_type
        self.interval = interval
        self.broker = broker
        self.port = port
        self.topic = topic
        self.keepalive = keepalive
        self.stop_event = threading.Event()
        self.thread = None
        self.client = None
        
    def _extract_sensor_value(self, parts):
        raise NotImplementedError("Subclasses must implement _extract_sensor_value method")
        
    def _run(self):
        # Create MQTT client
        self.client = mqtt.Client()
        
        try:
            # Connect to MQTT broker
            self.client.connect(self.broker, self.port, self.keepalive)
            self.client.loop_start()
            print(f"[MQTT DEVICE] {self.device_id} - Connected to {self.broker}:{self.port}")
            
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
                                "protocol": "mqtt",
                                "timestamp": datetime.now(UTC).isoformat(),
                                "sensor": self.sensor_type,
                                "value": float(sensor_value)
                            }
                            
                            try:
                                self.client.publish(self.topic, json.dumps(reading))
                                print(f"[MQTT DEVICE] {self.device_id} - Published reading: {reading}")
                            except Exception as e:
                                print(f"[MQTT DEVICE] {self.device_id} - Error publishing: {e}")
                    except NotImplementedError:
                        print(f"[MQTT DEVICE] {self.device_id} - Error: _extract_sensor_value not implemented")
                        break
                
                line_index += 1
                
                # Sleep for the configured interval, but check stop_event periodically
                self.stop_event.wait(timeout=self.interval)
                
        except Exception as e:
            print(f"[MQTT DEVICE] {self.device_id} - Connection error: {e}")
        finally:
            if self.client:
                self.client.loop_stop()
                self.client.disconnect()
                print(f"[MQTT DEVICE] {self.device_id} - Disconnected")
    
    def start(self):
        if self.thread and self.thread.is_alive():
            print(f"[MQTT DEVICE] {self.device_id} - Already running")
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[MQTT DEVICE] {self.device_id} - Started (interval: {self.interval}s)")
    
    def stop(self):
        """Gracefully stop the device."""
        if self.thread and self.thread.is_alive():
            print(f"[MQTT DEVICE] {self.device_id} - Stopping...")
            self.stop_event.set()
            self.thread.join(timeout=5)
            print(f"[MQTT DEVICE] {self.device_id} - Stopped")
    
    def is_running(self):
        return self.thread and self.thread.is_alive()
