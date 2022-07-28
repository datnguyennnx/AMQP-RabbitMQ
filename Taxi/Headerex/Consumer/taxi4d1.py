import pika
from pika.exchange_type import ExchangeType

def on_message_received(ch, method, properties, body):
    print(f'received new message: {body}')

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.exchange_declare('headersexchange', ExchangeType.headers)

channel.queue_declare('4')

bind_args = {
  'x-match': 'any',
  'name': '4seat',
  'dist': 'd1'
}

channel.queue_bind('4', 'headersexchange', arguments=bind_args)

channel.basic_consume(queue='4', auto_ack=True,
    on_message_callback=on_message_received)

print('Starting Consuming')

channel.start_consuming()