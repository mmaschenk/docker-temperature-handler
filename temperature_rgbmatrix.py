#!/usr/bin/env python

import pika
import json
import time
import uuid
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
mqrabbit_rgbmatrix_destination = os.getenv("MQRABBIT_RGBMATRIX_DESTINATION")

temperature_rgbmatrix_topnamefilter = os.getenv("TEMPERATURE_RGBMATRIX_TOPNAMEFILTER")
temperature_rgbmatrix_bottomnamefilter = os.getenv("TEMPERATURE_RGBMATRIX_BOTTOMNAMEFILTER")

everythingfine = True

def handleSenML(body):
    if not isinstance(body, list):
        print("[W] No SenML record")
        return

    basename = ""
    for line in body:
        fullname = ''
        if 'bn' in line:
            basename = line['bn']
        if 'n' in line:
            fullname = basename + line['n']
        if fullname == temperature_rgbmatrix_topnamefilter:
            print(f"[W] Sending top temperature value for device {fullname} ({line['v']:.1f})")
            message = { 'type': 'toptemperature', 'value': f"{line['v']:.1f}" }
            channel.basic_publish(exchange='', routing_key=mqrabbit_rgbmatrix_destination, body=json.dumps(message))
        if fullname == temperature_rgbmatrix_bottomnamefilter:
            print(f"[W] Sending bottom temperature value for device {fullname} ({line['v']:.1f})")
            message = { 'type': 'bottomtemperature', 'value': f"{line['v']:.1f}" }
            channel.basic_publish(exchange='', routing_key=mqrabbit_rgbmatrix_destination, body=json.dumps(message))

def callback(ch, method, properties, body):
    print(f"[W] Handling: {body}")
    reading = json.loads(body)
    handleSenML(reading)
    ch.basic_ack(delivery_tag = method.delivery_tag)

print("[R] Connecting")
mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
mqparameters = pika.ConnectionParameters(
    host=mqrabbit_host,
    virtual_host=mqrabbit_vhost,
    port=mqrabbit_port,
    credentials=mqrabbit_credentials)

mqconnection = pika.BlockingConnection(mqparameters)
channel = mqconnection.channel()

queuename = 'temperature_to_rgbmatrix_'+ str(uuid.uuid1())
q = channel.queue_declare(queue=queuename, exclusive=True)
channel.queue_bind(exchange=mqrabbit_exchange, queue=q.method.queue)

channel.basic_consume(queue=queuename, on_message_callback=callback)

print('[R] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()