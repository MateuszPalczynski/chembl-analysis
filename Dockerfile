FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y python3-pip

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3