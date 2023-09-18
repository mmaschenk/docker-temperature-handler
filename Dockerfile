FROM python:3

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY temperature_input.py /
COPY temperature_rgbmatrix.py /
COPY temperature_elasticsearch.py /
COPY temperature_input_rtl433.py /

CMD python -u /temperature_input.py
