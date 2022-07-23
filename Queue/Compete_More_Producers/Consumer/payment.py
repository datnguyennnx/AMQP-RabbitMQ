import time
import random
import pika
from pika.exchange_type import ExchangeType
#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 
#Declare a queue
queue = channel.queue_declare(queue='payment', exclusive= True)
#Create an explicit exchange
channel.exchange_declare(exchange='mytopicexchange', exchange_type=ExchangeType.topic)
#Create binding key
channel.queue_bind(exchange='mytopicexchange', queue=queue.method.queue, 
                   routing_key='#.payments')
#don't dispatch a new message unless the previous message has not been ackknowledged
channel.basic_qos(prefetch_count=1)
#Receiving messages from the queue
def callback(ch, method, properties, body):
    processing_time = random.randint(4,6)
    time.sleep(processing_time)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f" [x] Payment-service received {body}")        
#we need to tell RabbitMQ that this particular callback function
# should receive messages from our payment queue
channel.basic_consume(queue=queue.method.queue,
                      on_message_callback=callback)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
