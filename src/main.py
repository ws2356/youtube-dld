#!/usr/bin/env python
import os
import pika
import youtube_dl
import json
import threading

os.chdir('/app/lib')

rabbitmq_addr = os.environ['rabbitmq_addr']
if rabbitmq_addr is None:
    raise 'missing environ rabbitmq_addr'

connection = pika.BlockingConnection(parameters = pika.ConnectionParameters(host = rabbitmq_addr))
channel = connection.channel()
channel.queue_declare(queue = 'job', durable = True)

def thread_target(ch, method, properties, body):
    class MyLogger(object):
        def debug(self, msg):
            pass
        def warning(self, msg):
            pass
        def error(self, msg):
            print(msg)

    def my_hook(d):
        if d['status'] == 'finished':
            print('Done downloading, now converting ...')
            ch.basic_ack(delivery_tag = method.delivery_tag)
        elif d['status'] == 'error':
            print('Failed downloading: %s' % d)
            ch.basic_nack(delivery_tag = method.delivery_tag)
        elif d['status'] != 'downloading':
            print('Other event: %s' % d)

    print(" [x] Received %r" % body)
    json_body = json.loads(body)
    ydl_opts = {
            'verbose': True,
            'logger': MyLogger(),
            'progress_hooks': [my_hook],
            'nocheckcertificate': True,
            'noplaylist': True,
            'prefer_ffmpeg': True,
            'ffmpeg_location': '/host/usr/bin',
            'cachedir': '/app/lib/cache',
            'cookiefile': '/app/lib/cookiefile',
            'download_archive': '/app/lib/download_archive',
            'sleep_interval': 1,
            'max_sleep_interval': 10
            }
    if os.environ.get('proxy') is not None:
        ydl_opts['proxy'] = os.environ['proxy']
    print('ydl_opts: %s' % ydl_opts)
    ydl_opts.update(json_body)

    try:
        ydl = youtube_dl.YoutubeDL(params = ydl_opts)
        ydl.download([json_body['url']])
    except BaseException as e:
        print("Download failed error: %s" % e)
        ch.basic_nack(delivery_tag = method.delivery_tag)

def callback(ch, method, properties, body):
    th = threading.Thread(target = thread_target, args = (ch, method, properties, body))
    th.start()

channel.basic_consume(queue = 'job', auto_ack = False, on_message_callback = callback)
channel.basic_qos(prefetch_count=3)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
