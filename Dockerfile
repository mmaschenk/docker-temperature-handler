FROM python:3

RUN pip install paho-mqtt pika

COPY temperature_input.py /
COPY temperature_rgbmatrix.py /

CMD python -u /temperature_input.py
