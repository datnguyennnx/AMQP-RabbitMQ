import pika
from pika.exchange_type import ExchangeType
import random
import time
#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 
#Create an explicit exchange
channel.exchange_declare(exchange='mytopicexchange', exchange_type=ExchangeType.topic)

#Set an infinite loop to publish the message
while(True):
    #Make a list for random routing key
    list = ['user.europe.payments','user.asia.payments','business.africa.payments']
    item = random.choice(list)
    #Send a string Hello World! to hello queque
    message1 = "A European paid for sth"
    message2 = "A Asian paid for sth"
    message3 = "A African paid for sth"
    
    if item == 'user.europe.payments':
        channel.basic_publish(exchange='mytopicexchange',  
                      routing_key=item,
                      body=message1)
        time.sleep(random.randint(2,3))
        print(f"{message1}")

    if item == 'user.asia.payments':
        channel.basic_publish(exchange='mytopicexchange',  
                      routing_key=item,
                      body=message2)
        time.sleep(random.randint(2,3))
        print(f"{message2}")
    
    if item == 'business.africa.payments':
        channel.basic_publish(exchange='mytopicexchange',  
                      routing_key=item,
                      body=message3)
        time.sleep(random.randint(2,3))
        print(f"{message3}")
