from email import message
from pika.exchange_type import ExchangeType
import pika
import time
import random
import uuid

#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 

#Create an explicit exchange
channel.exchange_declare(exchange='seatexchange', exchange_type=ExchangeType.topic)
channel.exchange_declare(exchange='taxi7-place_exchange', exchange_type=ExchangeType.topic)

#Binding seatexchange to taxi-place_exchange
channel.exchange_bind("taxi7-place_exchange","seatexchange", routing_key="7.#")

#Add message id
messageid7q1 = 1
messageid7bth = 1
messageid7td = 1

#Set an infinite loop to publish the message
while(True):
    #Make a list for random routing key
    list = ['7.dist1','7.binhthanh','7.thuduc']
    item = random.choice(list)
    #Send orders to  queques
    message1 = "ordered 7seattaxi in district 1"
    message2 = "ordered 7seattaxi in district thuduc"
    message3 = "ordered 7seattaxi in district binhthanh"
  
    if item == '7.dist1':
        messages1 = (f"Client {messageid7q1} {message1}")
        messageid7q1 += 1
        cor_id = str(uuid.uuid4())
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages1)
        time.sleep(random.randint(2,3))
        print(f"{messages1}")
        
       
    if item == '7.thuduc':
        messages2 = (f"Client {messageid7td} {message2}")
        messageid7td += 1
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages2)
        time.sleep(random.randint(2,3))
        print(f"{messages2}")
        #channel.start_consuming()
        
      
    if item == '7.binhthanh':
        messages3 = (f"Client {messageid7bth} {message3}")
        messageid7bth += 1
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages3)
        time.sleep(random.randint(2,3))
        print(f"{messages3}")
