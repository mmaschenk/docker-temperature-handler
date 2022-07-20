#!/usr/bin/env python

import pika
import json
import time
import uuid
import datetime
import os
from elasticsearch import Elasticsearch

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

es_url = os.getenv("ES_URL")
es_user = os.getenv("ES_USER")
es_password = os.getenv("ES_PASSWORD")

temperature_rgbmatrix_namefilter = os.getenv("TEMPERATURE_RGBMATRIX_NAMEFILTER")

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
            del line['bn']
        if 'n' in line:
            print("[W] Found n: ", line['n'])
        if 'n' in line and line['n'] == "temperature": 
            fullname = basename + line['n']        
            print(f"[W] Storing temperature value for device {fullname} ({line['v']:.1f})")
            line['n'] = fullname
            #message = { 'bn': fullname, 'v': line['v'] }
            #channel.basic_publish(exchange='', routing_key=mqrabbit_rgbmatrix_destination, body=json.dumps(message))
            print(f"[W] Storing: {line}")
            es.index(index='temperatures', document=line)

def callback(ch, method, properties, body):
    print(f"[W] Handling: {body}")
    reading = json.loads(body)
    handleSenML(reading)
    ch.basic_ack(delivery_tag = method.delivery_tag)

mapping = { 'properties': {
                't': { 'type': 'date', 'format': 'epoch_second'},
                'v': { 'type': 'float'}}
        }

mappings = { 'mappings': mapping }

es = Elasticsearch( es_url, http_auth=(es_user, es_password))


index = es.indices.create(
    ignore=400,
    index='temperatures',
    mappings=mapping
    )

#es.indices.put_mapping(mapping, 'temperatures')


print("[R] Connecting")
mqrabbit_credentials = pika.PlainCredentials(mqrabbit_user, mqrabbit_password)
mqparameters = pika.ConnectionParameters(
    host=mqrabbit_host,
    virtual_host=mqrabbit_vhost,
    port=mqrabbit_port,
    credentials=mqrabbit_credentials)

mqconnection = pika.BlockingConnection(mqparameters)
channel = mqconnection.channel()

queuename = 'temperature_to_elasticsearch_'+ str(uuid.uuid1())
q = channel.queue_declare(queue=queuename, exclusive=True)
channel.queue_bind(exchange=mqrabbit_exchange, queue=q.method.queue)

channel.basic_consume(queue=queuename, on_message_callback=callback)

print('[R] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()