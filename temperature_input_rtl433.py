#!/usr/bin/env python

import functools
import pika
import json
import paho.mqtt.client as mqtt
import yaml
from yaml.loader import SafeLoader
from dateutil import parser
import datetime
import os
import numbers

"""
This script reads the RTL433-generated MQTT queue and transforms the message to an RFC8428-compliant message
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
mqtt_queue = os.getenv("MQTT_RTL433_EVENT_QUEUE")

mappingfile = os.getenv("MAPPINGFILE")

everythingfine = True

mqconnection = None
channel = None

mapping = {}

def handleRTL433(message):
    """
    Create a RFC8428-compliant message and forward this to next queue. 
    """

    dateobject = parser.parse(message['time'])
    timevalue = datetime.datetime.timestamp(dateobject)

    print(f"Found timevalue = [{timevalue}]")

    model = message.get('model','unknownmodel')
    payload_id = message.get('id', 'unknownid')
    channelid = message.get('channel','unknownchannel')
    basename = f"{model}:{payload_id}:{channelid}:"
    print(f"Basename = [{basename}]")

    message.pop('time', None)
    message.pop('model', None)
    message.pop('id', None)
    message.pop('channel', None)

    baseline = { "bn": basename, "bt": timevalue }
    rfc8428 = [ ]
    extrarecords = []

    for k in message.keys():
        print(f"Doing [{k}] -> [{message[k]}]")

        record = { **baseline }
        if k == 'temperature_C':
            record['n'] = 'temperature'
            record['u'] = 'Celsius'
            record['v'] = message[k]
        else:
            record['n'] = k
            if isinstance(message[k], numbers.Number):
                record['v'] = message[k]
            else:
                record['vs'] = message[k]

        try:
            mapvalue = mapping[model][payload_id][channelid][record['n']]
            print(f"Found mapping: {mapvalue}")
            extrarecord = { **record }
            extrarecord['bn'] = mapvalue + ':'
            extrarecord['bt'] = timevalue
            extrarecords.append(extrarecord)
        except:
            pass
            #print(f"Found no mapping for [{model}][{payload_id}][{channelid}] in {mapping}")

        rfc8428.append( record )
        baseline = {}

    print(f"Result = {rfc8428}")
    
    mqconnection.add_callback_threadsafe(
        functools.partial( 
            channel.basic_publish,
                exchange=mqrabbit_exchange, routing_key='*', 
                body=json.dumps(rfc8428))
    )
    if extrarecords:
        print(f"Outputting extra records: {extrarecords}")
        mqconnection.add_callback_threadsafe(
            functools.partial( 
                channel.basic_publish,
                exchange=mqrabbit_exchange, routing_key='*', 
                body=json.dumps(extrarecords))
        )


def on_message(client, userdata, message):
    global everythingfine

    payload=str(message.payload.decode("utf-8"))
    print(f"[W] receiving {datetime.datetime.now()} =====================")
    print(f"[W] topic={message.topic} qos={message.qos} retain-flag={message.retain}")
    print(f"[W] payload: [{payload}] ({type(payload)}")

    try:
        if payload == "debug: restart":
            print("[W] Debug command found. Restarting")
            everythingfine = False

        reading = json.loads(payload)
        handleRTL433(reading)
        
    except json.JSONDecodeError:
        print("[W] ignoring: could not decode payload")
    except pika.exceptions.StreamLostError:
        print("[W] connection lost. Need restarting")
        print(f"[W] [channel] closed={channel.is_closed} open={channel.is_open}")
        print(f"[W] [connection] closed={mqconnection.is_closed} open={mqconnection.is_open}")
        everythingfine = False
    except pika.exceptions.ChannelWrongStateError:
        print("[W] channel wrong state. Need restarting")
        print(f"[W] [channel] closed={channel.is_closed} open={channel.is_open}")
        print(f"[W] [connection] closed={mqconnection.is_closed} open={mqconnection.is_open}")
        everythingfine = False
    except Exception as e:
        print(f"[W] Maybe not fine: {e}")

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print(f"[R] connected OK Returned code=[{rc}], flags=[{flags}]")
    else:
        print("[R] Bad connection Returned code=",rc)

def on_disconnect(client, userdata, rc=0):
    global everythingfine
    client.loop_stop()
    everythingfine = False


def main():
    global mapping
    with open(mappingfile) as f:
        mapping = yaml.load(f, Loader=SafeLoader)
    print(mapping)

    global mqconnection
    global channel

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

    client = mqtt.Client('temperature rtl433 filter2')
    client.on_message=on_message
    client.on_connect=on_connect
    client.username_pw_set(mqtt_user, mqtt_password)
    client.connect(mqtt_host, port=int(mqtt_port))

    client.loop_start()
    print(f"[R] Subscribing to {mqtt_queue}")
    client.subscribe(mqtt_queue)
    print("[R] Subscribed")
    while True:
        mqconnection.sleep(60)
        if not everythingfine:
            print("[R] Re-initializing")
            break
        else:
            print("[R] everything is fine")

    print("[R] Ending main loop")
    client.loop_stop()

if __name__ == "__main__":
    main()