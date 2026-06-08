FROM alpine:3.20

RUN apk add --no-cache curl unzip

ENV DATASET_URL="https://archive.ics.uci.edu/static/public/256/daily+and+sports+activities.zip"

WORKDIR /data

CMD ["sh", "-c", "set -eu; if [ ! -f /data/.activities_downloaded ]; then rm -f /data/activities.zip; curl -L \"$DATASET_URL\" -o /data/activities.zip; unzip -q -o /data/activities.zip -d /data; touch /data/.activities_downloaded; else echo 'Dataset already downloaded in /data'; fi"]
