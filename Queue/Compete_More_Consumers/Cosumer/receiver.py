import pika
import time
import random
#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 
#Declare a queue
channel.queue_declare(queue='Order')
#don't dispatch a new message unless the previous message has not been ackknowledged
channel.basic_qos(prefetch_count=1)
#Receiving messages from the queue
def callback(ch, method, properties, body):
    driving_time = 2
    print(f" [x] Received {body}, It will take about {driving_time} hours to get to finish the order")
    time.sleep(driving_time)
    #acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("[*]Finished task")
#Get message on payload
channel.basic_consume(queue='Order',
                      on_message_callback=callback)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
