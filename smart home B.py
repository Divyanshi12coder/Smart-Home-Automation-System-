import os
import json
import threading
import paho.mqtt.client as mqtt
from models import save_log


MQTT_URL = os.getenv('MQTT_URL', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASS = os.getenv('MQTT_PASS')


callbacks = []


def on_connect(client, userdata, flags, rc):
print('Connected to MQTT', rc)
client.subscribe('home/#')


def on_message(client, userdata, msg):
payload = msg.payload.decode()
try:
parsed = json.loads(payload)
except Exception:
parsed = payload
# Save in DB (async via thread)
try:
save_log(msg.topic, parsed)
except Exception as e:
print('DB save error', e)
# notify callbacks
for cb in callbacks:
try: cb({'topic': msg.topic, 'payload': parsed})
except: pass


client = mqtt.Client()
if MQTT_USER: client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = on_connect
client.on_message = on_message


def start():
client.connect(MQTT_URL, MQTT_PORT, 60)
thread = threading.Thread(target=client.loop_forever, daemon=True)
thread.start()


def register_callback(cb):
callbacks.append(cb)


if __name__ == '__main__':
start()
import time
while True:
time.sleep(1)