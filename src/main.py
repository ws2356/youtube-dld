#!/usr/bin/env python
import os
import pika
import youtube_dl
import json
import threading
import signal
import time

os.chdir('/app/lib')

config = {}
with open('/app/lib/config.json', 'r') as ff:
    config = json.load(ff)

rabbitmq_addr = os.environ['rabbitmq_addr']
if rabbitmq_addr is None:
    raise 'missing environ rabbitmq_addr'


msg_table = {}
mutex = threading.RLock()

def thread_target(ch, delivery_tag, properties, json_body):
    class MyLogger(object):
        def debug(self, msg):
            pass
        def warning(self, msg):
            pass
        def error(self, msg):
            print(msg, flush = True)

    last_progress = 0
    def my_hook(d):
        nonlocal last_progress
        if d['status'] == 'finished':
            print('Done downloading, now converting ...', flush = True)
            try:
                ch.basic_ack(delivery_tag = delivery_tag)
            except BaseException as e:
                print('Failed to basic_ack, e: %s' % e, flush = True)
            finally:
                with mutex:
                    msg_table.pop(message_key(json_body), None)
        elif d['status'] == 'error':
            print('Failed downloading: %s' % d, True)
            try:
                ch.basic_nack(delivery_tag = delivery_tag)
            except BaseException as e:
                print('Failed to basic_nack, e: %s' % e, flush = True)
            finally:
                with mutex:
                    msg_table.pop(message_key(json_body), None)
        elif d['status'] == 'downloading':
            report_unit = config['report_unit']
            now_progress = int(d['downloaded_bytes'])
            if now_progress - last_progress >= report_unit:
                print('[progress %dK / %dK]: %s, %s' % (int(now_progress / 1024), int(d.get('total_bytes', -1) / 1024), d['filename'], json_body['url']), flush = True)
                last_progress = now_progress
        else:
            print('Other event: %s' % d, flush = True)

    print(" [x] Received %r" % json_body, flush = True)
    ydl_opts = {
            'verbose': True,
            'logger': MyLogger(),
            'progress_hooks': [my_hook],
            'nocheckcertificate': True,
            'noplaylist': True,
            'prefer_ffmpeg': True,
            'cachedir': '/app/lib/cache',
            'cookiefile': '/app/lib/cookiefile',
            'download_archive': '/app/lib/download_archive',
            'sleep_interval': 1,
            'max_sleep_interval': 3
            }
    if os.environ.get('proxy') is not None:
        ydl_opts['proxy'] = os.environ['proxy']
    print('ydl_opts: %s' % ydl_opts, flush = True)
    ydl_opts.update(json_body)

    try:
        ydl = youtube_dl.YoutubeDL(params = ydl_opts)
        ydl.download([json_body['url']])
    except BaseException as e:
        print("Download failed error: %s" % e, flush = True)
        try:
            ch.basic_nack(delivery_tag = delivery_tag)
        except BaseException as e:
            print("Failed to basic_nack, e: %s" % e, flush = True)
        finally:
            with mutex:
                msg_table.pop(message_key(json_body), None)


def callback(ch, method, properties, body):
    json_body = json.loads(body)
    handle_message(ch, method.delivery_tag, properties, json_body)


# message management
def message_key(msg):
    if msg is None:
        return None
    if msg.get('url') is None:
        return None
    return '%s:%s' % (msg['url'], msg.get('matchtitle', '-'))

def handle_message(ch, delivery_tag, properties, json_body):
    msg_key = message_key(json_body)
    if msg_key is None:
        return
    with mutex:
        if msg_table.get(msg_key) is not None:
            return
        msg_table[msg_key] = delivery_tag
        th = threading.Thread(target = thread_target, args = (ch, delivery_tag, properties, json_body))
        th.start()


def exit_gracefully(self, signum, frame):
    with mutex:
        for k, v in msg_table.items():
            try:
                channel.basic_nack(delivery_tag = v)
            except BaseException as e:
                print('Failed to basic_nack, e: %s' % e, flush = True)


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

print(' [*] Waiting for messages. To exit press CTRL+C', flush = True)
while True:
    try:
        connection = pika.BlockingConnection(parameters = pika.ConnectionParameters(host = rabbitmq_addr))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=config['prefetch_count'])
        channel.queue_declare(queue = 'job', durable = True)
        channel.basic_consume(queue = 'job', auto_ack = False, on_message_callback = callback)
        channel.start_consuming()
    except BaseException as e:
        print('main loop error of type %s: %s' % (type(e), e), flush = True)
        time.sleep(config['retry_wait'])
