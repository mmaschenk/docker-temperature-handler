FROM python:3

RUN pip install paho-mqtt pika

COPY temperature_filter.py /

CMD python -u /temperature_filter.py
