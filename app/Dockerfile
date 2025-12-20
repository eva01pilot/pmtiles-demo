FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    tippecanoe \
    unzip \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends tar; \
    rm -rf /var/lib/apt/lists/*; \
    \
    curl -L --fail \
      https://github.com/protomaps/go-pmtiles/releases/download/v1.29.1/go-pmtiles_1.29.1_Linux_x86_64.tar.gz \
      -o /tmp/pmtiles.tar.gz; \
    \
    tar -xzf /tmp/pmtiles.tar.gz -C /tmp; \
    \
    mv /tmp/pmtiles /usr/local/bin/pmtiles; \
    chmod +x /usr/local/bin/pmtiles; \
    \
    /usr/local/bin/pmtiles --help > /dev/null
RUN ogr2ogr --version \
 && tippecanoe --version \
 && pmtiles --help > /dev/null

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY main.py /app/main.py

RUN mkdir -p /app/data /app/work

ENV BUNDLES_DB=/app/data/bundles.db
ENV WORKDIR=/app/work

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
