#!/usr/bin/env python

import pika
import json
import paho.mqtt.client as mqtt
import time
import datetime
import os

mqrabbit_user = os.getenv("MQRABBIT_USER")
mqrabbit_password = os.getenv("MQRABBIT_PASSWORD")
mqrabbit_host = os.getenv("MQRABBIT_HOST")
mqrabbit_vhost = os.getenv("MQRABBIT_VHOST")
mqrabbit_port = os.getenv("MQRABBIT_PORT")
mqrabbit_exchange = os.getenv("MQRABBIT_EXCHANGE")
mqrabbit_destination = os.getenv("MQRABBIT_DESTINATION")

mqtt_host = os.getenv("MQTT_HOST")
mqtt_port = os.getenv("MQTT_PORT")
mqtt_user = os.getenv("MQTT_USER")
mqtt_password = os.getenv("MQTT_PASSWORD")

mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
mqparameters = pika.ConnectionParameters(
    host=mqrabbit_host,
    virtual_host=mqrabbit_vhost,
    port=mqrabbit_port,
    credentials=mqrabbit_credentials)

mqconnection = pika.BlockingConnection(mqparameters)
channel = mqconnection.channel()

def on_message(client, userdata, message):
    payload=str(message.payload.decode("utf-8"))
    print(f"receiving {datetime.datetime.now()}")
    print("message received " , payload)
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)

    try:
        reading = json.loads(payload)

        message = { 'type': 'temperature', 'value': f"{reading['temp_celsius']:.1f}" }
        channel.basic_publish(exchange='', routing_key='nagios_queue', 
                                    body=json.dumps(message))
    except json.JSONDecodeError:
        print("Could not decode payload")

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK Returned code=",rc)
    else:
        print("Bad connection Returned code=",rc)

client = mqtt.Client('temperature filter')
client.on_message=on_message
client.on_connect=on_connect
client.username_pw_set(mqtt_user, mqtt_password)
client.connect(mqtt_host, port=int(mqtt_port))


client.loop_start()
print("Subscribing to IOT/Temperature sensor")
client.subscribe("IOT/Temperature sensor")
print("Publishing to house/main-light")
client.publish("house/main-light","OFF")
while True:
    time.sleep(60)
