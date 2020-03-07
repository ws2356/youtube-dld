#!/usr/bin/env python
import os
import pika
import youtube_dl
import json
import threading
import signal

os.chdir('/app/lib')

rabbitmq_addr = os.environ['rabbitmq_addr']
if rabbitmq_addr is None:
    raise 'missing environ rabbitmq_addr'

connection = pika.BlockingConnection(parameters = pika.ConnectionParameters(host = rabbitmq_addr))
channel = connection.channel()
channel.queue_declare(queue = 'job', durable = True)


msg_table = {}
mutex = threading.RLock()

def thread_target(ch, method, properties, json_body):
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
            try:
                ch.basic_ack(delivery_tag = method.delivery_tag)
            except BaseException as e:
                print('Failed to basic_ack, e: %s' % e)
            finally:
                with mutex:
                    msg_table.pop(message_key(json_body), None)
        elif d['status'] == 'error':
            print('Failed downloading: %s' % d)
            try:
                ch.basic_nack(delivery_tag = method.delivery_tag)
            except BaseException as e:
                print('Failed to basic_nack, e: %s' % e)
            finally:
                with mutex:
                    msg_table.pop(message_key(json_body), None)
        elif d['status'] != 'downloading':
            print('Other event: %s' % d)

    print(" [x] Received %r" % json_body)
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
        try:
            ch.basic_nack(delivery_tag = method.delivery_tag)
        except BaseException as e:
            print("Failed to basic_nack, e: %s" % e)
        finally:
            with mutex:
                msg_table.pop(message_key(json_body), None)


def callback(ch, method, properties, body):
    json_body = json.loads(body)
    handle_message(ch, method, properties, json_body)


# message management
def message_key(msg):
    if msg is None:
        return None
    if msg.get('url') is None:
        return None
    return '%s:%s' % (msg['url'], msg.get('matchtitle', '-'))

def handle_message(ch, method, properties, json_body):
    msg_key = message_key(json_body)
    if msg_key is None:
        return
    with mutex:
        if msg_table.get(msg_key) is not None:
            return
        msg_table[msg_key] = method
        th = threading.Thread(target = thread_target, args = (ch, method, properties, json_body))
        th.start()


def exit_gracefully(self, signum, frame):
    with mutex:
        for k, v in msg_table.items():
            try:
                channel.basic_nack(delivery_tag = v.delivery_tag)
            except BaseException as e:
                print('Failed to basic_nack, e: %s' % e)


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

channel.basic_consume(queue = 'job', auto_ack = False, on_message_callback = callback)
channel.basic_qos(prefetch_count=3)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
