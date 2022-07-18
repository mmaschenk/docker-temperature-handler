#!/usr/bin/env python

import pika
import json
import paho.mqtt.client as mqtt
import time
import datetime
import os

"""
This script reads the MQTT queue and transforms the message to an RFC8428-compliant message
that will be placed on the output EXCHANGE.
"""

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
mqtt_queue = os.getenv("MQTT_QUEUE")

everythingfine = True

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
        print(f"Published temperature: {reading['temp_celsius']:.1f}")
    except json.JSONDecodeError:
        print("Could not decode payload")
    except pika.exceptions.StreamLostError:
        everythingfine = False

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK Returned code=",rc)
    else:
        print("Bad connection Returned code=",rc)

while True:

    print("Connecting")
    mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
    mqparameters = pika.ConnectionParameters(
        host=mqrabbit_host,
        virtual_host=mqrabbit_vhost,
        port=mqrabbit_port,
        credentials=mqrabbit_credentials)

    mqconnection = pika.BlockingConnection(mqparameters)
    channel = mqconnection.channel()

    channel.exchange_declare(exchange='temperatures', exchange_type='fanout', durable=True)

    everythingfine = True

    client = mqtt.Client('temperature filter')
    client.on_message=on_message
    client.on_connect=on_connect
    client.username_pw_set(mqtt_user, mqtt_password)
    client.connect(mqtt_host, port=int(mqtt_port))

    client.loop_start()
    print(f"Subscribing to {mqtt_queue}")
    client.subscribe(mqtt_queue)
    while True:
        time.sleep(60)
        if not everythingfine:
            print("Re-initializing")
            break
