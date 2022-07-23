from email import message
import imp
import pika
import time
import random
#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 
#Declare a queue
channel.queue_declare(queue='Order')
#Add message id
messageid = 1
#Set an infinite loop to publish the message
while(True):
    #Send a string Order queque
    message = f" [x] Sending messageid: {messageid}"
    channel.basic_publish(exchange='',  #using default exchange
                      routing_key='Order',
                      body=message)
    print(f"{message}")
    time.sleep(12)
    messageid+=1

