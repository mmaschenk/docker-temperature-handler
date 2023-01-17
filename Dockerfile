FROM python:3

RUN pip install paho-mqtt pika elasticsearch==7.15.1

COPY temperature_input.py /
COPY temperature_rgbmatrix.py /
COPY temperature_elasticsearch.py /
COPY raw_elasticsearch.py /
COPY temperature_input_rtl433.py /

CMD python -u /temperature_input.py
