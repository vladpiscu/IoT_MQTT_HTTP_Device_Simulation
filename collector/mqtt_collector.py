import sys
import json
from pathlib import Path

# Add parent directory to path to import storage module
sys.path.insert(0, str(Path(__file__).parent.parent))

import paho.mqtt.client as mqtt
from storage import save_to_csv
from datetime import datetime, UTC


class MqttCollector:
    
    def __init__(self, csv_filename="iot_data.csv", broker="localhost", port=1883, topic="iot", keepalive=60):
        self.csv_filename = csv_filename
        self.broker = broker
        self.port = port
        self.topic = topic
        self.keepalive = keepalive
        self.client = None
    
    def _on_message(self, client, userdata, msg):
        try:
            # Decode the message payload
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            receive_time = datetime.now(UTC).isoformat()
            
            # Save to CSV
            save_to_csv(data, self.csv_filename, receive_time=receive_time)
            
            print(f"[MQTT COLLECTOR] Received message on topic {msg.topic}: {data}")
        except json.JSONDecodeError as e:
            print(f"[MQTT COLLECTOR] Error decoding JSON: {e}")
        except Exception as e:
            print(f"[MQTT COLLECTOR] Error processing message: {e}")
    
    def start(self):
        print(f"[MQTT COLLECTOR] Starting on {self.broker}:{self.port}, subscribing to topic '{self.topic}'")
        
        self.client = mqtt.Client()
        self.client.on_message = self._on_message
        
        try:
            self.client.connect(self.broker, self.port, self.keepalive)
            self.client.subscribe(self.topic)
            print(f"[MQTT COLLECTOR] Connected and subscribed to '{self.topic}'")
            print(f"[MQTT COLLECTOR] Waiting for messages...")
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("\n[MQTT COLLECTOR] Shutting down...")
            self.client.loop_stop()
            self.client.disconnect()
            print("[MQTT COLLECTOR] Disconnected")
    
    def stop(self):
        if self.client:
            print("[MQTT COLLECTOR] Stopping...")
            self.client.loop_stop()
            self.client.disconnect()
            print("[MQTT COLLECTOR] Disconnected")


if __name__ == "__main__":
    collector = MqttCollector()
    collector.start()
