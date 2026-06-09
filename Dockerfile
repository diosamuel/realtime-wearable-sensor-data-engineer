FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        python3-pip \
        unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

COPY requirements.txt /opt/requirements.txt

RUN python3 -m pip install --no-cache-dir -r /opt/requirements.txt

ENV DATASET_URL="https://archive.ics.uci.edu/static/public/256/daily+and+sports+activities.zip"
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

WORKDIR /data

CMD ["sh", "-c", "set -eu; if [ -f /data/.activities_downloaded ]; then echo 'Dataset already downloaded in /data'; else if [ -f /data/activities.zip ]; then echo 'Using existing /data/activities.zip'; elif [ -f /data/activites.zip ]; then echo 'Using existing /data/activites.zip'; cp /data/activites.zip /data/activities.zip; else curl -L \"$DATASET_URL\" -o /data/activities.zip; fi; unzip -q -o /data/activities.zip -d /data; touch /data/.activities_downloaded; fi"]
