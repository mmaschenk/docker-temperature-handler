#!/usr/bin/env python

import pika
import json
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
mqrabbit_rgbexchange = os.getenv("MQRABBIT_RGBEXCHANGE")

temperature_rgbmatrix_namefilter1 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER1")
temperature_rgbmatrix_namefilter2 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER2")
temperature_rgbmatrix_namefilter3 = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER3")

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
        if fullname == temperature_rgbmatrix_namefilter1:
            print(f"[W] Sending temperature1 value for device {fullname} ({line['v']:.1f})")
            message = { 'type': 'temperature1', 'value': f"{line['v']:.1f}" }
            channel.basic_publish(exchange=mqrabbit_rgbexchange, routing_key='*', body=json.dumps(message))
        if fullname == temperature_rgbmatrix_namefilter2:
            print(f"[W] Sending temperature2 value for device {fullname} ({line['v']:.1f})")
            message = { 'type': 'temperature2', 'value': f"{line['v']:.1f}" }
            channel.basic_publish(exchange=mqrabbit_rgbexchange, routing_key='*', body=json.dumps(message))
        if fullname == temperature_rgbmatrix_namefilter3:
            print(f"[W] Sending temperature3 value for device {fullname} ({line['v']:.1f})")
            message = { 'type': 'temperature3', 'value': f"{line['v']:.1f}" }
            channel.basic_publish(exchange=mqrabbit_rgbexchange, routing_key='*', body=json.dumps(message))

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

queuename = 'temperature_rgbmatrix_' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
q = channel.queue_declare(queue=queuename, exclusive=True, auto_delete=True)
channel.queue_bind(exchange=mqrabbit_exchange, queue=q.method.queue)

channel.basic_consume(queue=queuename, on_message_callback=callback)

print('[R] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()