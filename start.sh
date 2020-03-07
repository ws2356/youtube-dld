#!/usr/bin/env bash

docker run --name youtube-dld --restart unless-stopped -d \
    --env rabbitmq_addr=192.168.31.185 \
    --env 'proxy=http://192.168.31.185:8118' \
    -v "${HOME}/youtube-dl/lib:/app/lib" \
    -v "/usr/bin:/host/usr/bin" \
    youtube-dld:0.1
