#!/usr/bin/env python
import os
import pika

HOST = os.environ['HOST']

if HOST is None:
    raise 'missing environ HOST'
connection = pika.BlockingConnection(parameters = pika.ConnectionParameters(host = HOST))
channel = connection.channel()
channel.queue_declare(queue = 'job', durable = True)

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_consume(queue = 'job', auto_ack = False, on_message_callback = callback)
channel.basic_qos(prefetch_count=1)

print(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()
