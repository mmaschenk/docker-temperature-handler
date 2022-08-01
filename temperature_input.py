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

def handleSenML(message):
    """
    Forward the message to next queue. Check each line with a measurement and add time to that
    """
    outmessage = []
    for line in message:
        if 't' in line and line['t'] == 0:
            line['t'] = int(time.time())

    print(f"[W] Forwarding: {message}")
    channel.basic_publish(exchange=mqrabbit_exchange, routing_key='*', 
                                        body=json.dumps(message))

def on_message(client, userdata, message):
    payload=str(message.payload.decode("utf-8"))
    print(f"[W] receiving {datetime.datetime.now()} =====================")
    print(f"[W] topic={message.topic} qos={message.qos} retain-flag={message.retain}")
    print(f"[W] payload: {payload}")

    try:
        reading = json.loads(payload)
        if isinstance(reading, list):
            handleSenML(reading)
        else:
            print("[W] ignoring unrecognized message")
    except json.JSONDecodeError:
        print("[W] ignoring: could not decode payload")
    except pika.exceptions.StreamLostError:
        print("[W] connection lost. Need restarting")
        everythingfine = False
    except pika.exceptions.ChannelWrongStateError:
        print("[W] channel wron state. Need restarting")
        everythingfine = False

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("[R] connected OK Returned code=",rc)
    else:
        print("[R] Bad connection Returned code=",rc)

while True:

    print("[R] Connecting")
    mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
    mqparameters = pika.ConnectionParameters(
        host=mqrabbit_host,
        virtual_host=mqrabbit_vhost,
        port=mqrabbit_port,
        credentials=mqrabbit_credentials)

    mqconnection = pika.BlockingConnection(mqparameters)
    channel = mqconnection.channel()

    channel.exchange_declare(exchange=mqrabbit_exchange, exchange_type='fanout', durable=True)

    everythingfine = True

    client = mqtt.Client('temperature filter')
    client.on_message=on_message
    client.on_connect=on_connect
    client.username_pw_set(mqtt_user, mqtt_password)
    client.connect(mqtt_host, port=int(mqtt_port))

    client.loop_start()
    print(f"[R] Subscribing to {mqtt_queue}")
    client.subscribe(mqtt_queue)
    while True:
        time.sleep(60)
        if not everythingfine:
            print("[R] Re-initializing")
            break
