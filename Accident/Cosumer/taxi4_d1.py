import pika
import time
import random
from pika.exchange_type import ExchangeType

####################################################################################
#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 
####################################################################################
#Declare a queue
queue = channel.queue_declare(queue='DISTRICT_1_4seat')
####################################################################################
#Create an explicit exchange
channel.exchange_declare(exchange='taxi4-place_exchange', exchange_type=ExchangeType.topic)
####################################################################################
#Create binding key
channel.queue_bind(queue=queue.method.queue, 
                   exchange='taxi4-place_exchange', 
                   routing_key='*.dist1')

####################################################################################
#don't dispatch a new message unless the previous message has not been ackknowledged
channel.basic_qos(prefetch_count=1)
####################################################################################
#Receiving messages from the queue
def callback(ch, method, properties, body):
    driving_time = random.randint(3,6)
    t = driving_time * 10
    print(f" [x] Received {body}, It will take about {t} minutes to get to finish the order")
    time.sleep(driving_time)
    #acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("[*]Finished task")
####################################################################################
#Get message on payload
channel.basic_consume(queue=queue.method.queue,
                      on_message_callback=callback)


print(' [*] Waiting for messages. To exit press CTRL+C')
####################################################################################
channel.start_consuming()
