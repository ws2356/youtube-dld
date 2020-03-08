FROM python:3.8

ENV http_proxy 'http://192.168.31.185:8118'
ENV https_proxy 'http://192.168.31.185:8118'

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  ffmpeg \
  && rm -rf /var/lib/apt/lists/*

COPY --chown=1000:1000 requirements.txt /app/

WORKDIR /app

ENV http_proxy ''
ENV https_proxy ''

RUN pip install --no-cache-dir -r requirements.txt

USER 1000:1000

COPY --chown=1000:1000 src/main.py src/

CMD [ "python", "src/main.py" ]
