import pika
from pika.exchange_type import ExchangeType

def on_message_received(ch, method, properties, body):
    print(f'received new message: {body}')

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.exchange_declare('headersexchange', ExchangeType.headers)

channel.queue_declare('12')

bind_args = {
  'x-match': 'any',
  'name': '12seat',
  'dist': 'd1'
}

channel.queue_bind('12', 'headersexchange', arguments=bind_args)

channel.basic_consume(queue='12', auto_ack=True,
    on_message_callback=on_message_received)

print('Starting Consuming')

channel.start_consuming()