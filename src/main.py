#!/usr/bin/env python
import os
import sys
import pika
import youtube_dl
import json
import threading
import signal
import time
import functools
import traceback

os.chdir('/app/lib')

config = {}
with open('/app/lib/config.json', 'r') as ff:
    config = json.load(ff)

rabbitmq_addr = os.environ['rabbitmq_addr']
if rabbitmq_addr is None:
    raise 'missing environ rabbitmq_addr'

msg_table = {}

def debug_print(*args, **kwargs):
    s = '[tid = %s] %s' % (threading.currentThread().ident, args[0])
    print(s, *args[1:], **kwargs)


def safe_ack(ch, delivery_tag, json_body):
    try:
        ch.basic_ack(delivery_tag = delivery_tag)
        debug_print('Did basic_ack', flush = True)
    except BaseException as e:
        debug_print('Failed to basic_ack, e: %s' % e, flush = True)
        try:
            ch.stop_consuming()
        except BaseException as e2:
            debug_print('Failed to stop_consuming: %s' % e2)
        finally:
            ch.connection.close()
    finally:
        msg_table.pop(message_key(json_body), None)

def safe_nack(ch, delivery_tag, json_body):
    try:
        ch.basic_nack(delivery_tag = delivery_tag)
        debug_print('Did basic_nack', flush = True)
    except BaseException as e:
        debug_print('Failed to basic_nack, e: %s' % e, flush = True)
        try:
            ch.stop_consuming()
        except BaseException as e2:
            debug_print('Failed to stop_consuming: %s' % e2)
        finally:
            ch.connection.close()
    finally:
        msg_table.pop(message_key(json_body), None)

def thread_target(ch, delivery_tag, properties, json_body):
    class MyLogger(object):
        def debug(self, msg):
            pass
        def warning(self, msg):
            pass
        def error(self, msg):
            debug_print(msg, flush = True)

    last_progress = 0
    def my_hook(d):
        nonlocal last_progress
        if d['status'] == 'finished':
            debug_print('Done downloading, now converting ...', flush = True)
            connection.add_callback_threadsafe(functools.partial(safe_ack, ch, delivery_tag, json_body))
        elif d['status'] == 'error':
            debug_print('Failed downloading: %s' % d, True)
            connection.add_callback_threadsafe(functools.partial(safe_nack, ch, delivery_tag, json_body))
        elif d['status'] == 'downloading':
            report_unit = config['report_unit']
            now_progress = int(d['downloaded_bytes'])
            if now_progress - last_progress >= report_unit:
                debug_print('[progress %dK / %dK]: %s, %s' % (int(now_progress / 1024), int(d.get('total_bytes', -1) / 1024), d['filename'], json_body['url']), flush = True)
                last_progress = now_progress
        else:
            debug_print('Other event: %s' % d, flush = True)

    debug_print(" [x] Received %r" % json_body, flush = True)
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
    debug_print('ydl_opts: %s' % ydl_opts, flush = True)
    ydl_opts.update(json_body)

    retry_count = 10
    while retry_count > 0:
        try:
            ydl = youtube_dl.YoutubeDL(params = ydl_opts)
            ydl.download([json_body['url']])
            return
        except BaseException as e:
            debug_print("Download failed error: %s" % e, flush = True)
            time.sleep(config.get('download_retry_wait', 30))
            retry_count -= 1
    connection.add_callback_threadsafe(functools.partial(safe_nack, ch, delivery_tag, json_body))


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
    if msg_table.get(msg_key) is not None:
        return
    msg_table[msg_key] = delivery_tag
    th = threading.Thread(target = thread_target, args = (ch, delivery_tag, properties, json_body))
    th.start()


def stop_all():
    for k, v in msg_table.items():
        try:
            channel.basic_nack(delivery_tag = v)
        except Exception as e:
            debug_print('Failed to basic_nack: ' % e)
    try:
        channel.stop_consuming()
    except Exception as e:
        debug_print('Failed to start_consuming: %s' % e)
    finally:
        connection.close()

def exit_gracefully(signum, frame):
    connection.add_callback_threadsafe(stop_all)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

def health_report_thread():
    while True:
        print('stack trace report')
        print('===================================')
        for ident, frame in sys._current_frames().items():
            traceback.print_stack(frame)
            print('------------------------------------')
        time.sleep(config.get('health_report_interval', 10))

threading.Thread(target = health_report_thread).start()

while True:
    try:
        debug_print(' [*] Waiting for messages. To exit press CTRL+C', flush = True)
        connection = pika.BlockingConnection(parameters = pika.ConnectionParameters(host = rabbitmq_addr))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=config['prefetch_count'])
        channel.queue_declare(queue = 'job', durable = True)
        channel.basic_consume(queue = 'job', auto_ack = False, on_message_callback = callback)
        channel.start_consuming()
    except BaseException as e:
        debug_print('main loop error of type %s: %s' % (type(e), e), flush = True)
        time.sleep(config['retry_wait'])
