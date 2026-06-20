#!/usr/bin/env python

import pika
import json
import time
import datetime
import os
import paho.mqtt.client as mqtt

"""
This script reads a RFC8428-compliant message from the MQRABBIT queue 
and transforms it to an MQTT message that will be placed on the output EXCHANGE.
"""

mqrabbit_user = os.getenv("MQRABBIT_USER")
mqrabbit_password = os.getenv("MQRABBIT_PASSWORD")
mqrabbit_host = os.getenv("MQRABBIT_HOST")
mqrabbit_vhost = os.getenv("MQRABBIT_VHOST")
mqrabbit_port = os.getenv("MQRABBIT_PORT")
mqrabbit_exchange = os.getenv("MQRABBIT_EXCHANGE")
mqrabbit_rgbexchange = os.getenv("MQRABBIT_RGBEXCHANGE")

mqtt_host = os.getenv("HOME_ASSISTANT_MQTT_HOST")
mqtt_port = os.getenv("HOME_ASSISTANT_MQTT_PORT")
mqtt_user = os.getenv("HOME_ASSISTANT_MQTT_USER")
mqtt_password = os.getenv("HOME_ASSISTANT_MQTT_PASSWORD")
node_id = os.getenv("HOME_ASSISTANT_NODE_ID")
device = json.loads(os.getenv("HOME_ASSISTANT_DEVICE_PAYLOAD"))

temperature_rgbmatrix_namefilter1 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER1")
temperature_rgbmatrix_namefilter2 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER2")
temperature_rgbmatrix_namefilter3 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER3")
temperature_topic_dictionary = json.loads(os.getenv("TEMPERATURE_TOPIC_DICTIONARY"))

everythingfine = True

def handleSenML(body):
    if not isinstance(body, list):
        print("[R] No SenML record")
        return

    basename = ""
    for line in body:
        fullname = ''
        if 'bn' in line:
            basename = line['bn']
        if 'n' in line:
            fullname = basename + line['n']

        if fullname in temperature_topic_dictionary:
            mqtt_topic = temperature_topic_dictionary[fullname]['topic']
            mqtt_message = f"{line['v']:.1f}"
            print(f"[W] Sending message {mqtt_message} to topic {mqtt_topic} for device {fullname} ({line['v']:.1f})")
            mqtt_client.publish(mqtt_topic, mqtt_message, retain=True)

def callback(ch, method, properties, body):
    #print(f"[R] Handling: {body}")
    print(".", end='', flush=True)
    reading = json.loads(body)
    handleSenML(reading)
    ch.basic_ack(delivery_tag = method.delivery_tag)

print("[R] Connecting to mqtt")
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username=mqtt_user, password=mqtt_password)
mqtt_client.connect(mqtt_host, int(mqtt_port))

mqtt_client.loop_start()
print("[R] Connected to MQTT")

for sensor, sensordata in temperature_topic_dictionary.items():
    print(f"[I] Registering sensor {sensor} with {sensordata}")

    payload = {
        "name": sensordata['name'],
        "unique_id": f"{node_id}_{sensordata['id']}",
        "state_topic": sensordata['topic'],
        "unit_of_measurement": "°C",
        "device": {
            "identifiers": [node_id],
            **device
        }
    }
    discovery_topic = f"homeassistant/sensor/{node_id}/{sensordata['id']}/config"

    print(f"[I] Publishing {json.dumps(payload, indent=2)} to topic {discovery_topic}")
    mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True, qos=1)
    
print("[R] Connecting to RabbitMQ")
mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
mqparameters = pika.ConnectionParameters(
    host=mqrabbit_host,
    virtual_host=mqrabbit_vhost,
    port=mqrabbit_port,
    credentials=mqrabbit_credentials)

mqconnection = pika.BlockingConnection(mqparameters)
channel = mqconnection.channel()

queuename = 'temperature_rgbmatrix_' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
q = channel.queue_declare(queue=queuename, exclusive=True, auto_delete=True)
channel.queue_bind(exchange=mqrabbit_exchange, queue=q.method.queue)

channel.basic_consume(queue=queuename, on_message_callback=callback)

print('[R] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()