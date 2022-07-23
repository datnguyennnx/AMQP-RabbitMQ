from email import message
from pika.exchange_type import ExchangeType
import pika
import time
import random
import uuid
####################################################################################

#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 

####################################################################################

#Create an explicit exchange
channel.exchange_declare(exchange='seatexchange', exchange_type=ExchangeType.topic)
channel.exchange_declare(exchange='taxi4-place_exchange', exchange_type=ExchangeType.topic)

####################################################################################

#Binding seatexchange to taxi-place_exchange
channel.exchange_bind("taxi4-place_exchange","seatexchange", routing_key="4.#")

####################################################################################
#Add message id
messageid4q1 = 1
messageid4bth = 1
messageid4td = 1
####################################################################################

#Set an infinite loop to publish the message
while(True):
    #Make a list for random routing key
    list = ['4.dist1','4.binhthanh','4.thuduc']
    item = random.choice(list)
    #Send orders to  queques
    message1 = "ordered 4seattaxi in district 1"
    message2 = "ordered 4seattaxi in district thuduc"
    message3 = "ordered 4seattaxi in district binhthanh"
####################################################################################   
    if item == '4.dist1':
        messages1 = (f"Client {messageid4q1} {message1}")
        messageid4q1 += 1
        cor_id = str(uuid.uuid4())
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages1)
        time.sleep(random.randint(2,3))
        print(f"{messages1}")
        
####################################################################################        
    if item == '4.thuduc':
        messages2 = (f"Client {messageid4td} {message2}")
        messageid4td += 1
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages2)
        time.sleep(random.randint(2,3))
        print(f"{messages2}")
        #channel.start_consuming()
        
####################################################################################       
    if item == '4.binhthanh':
        messages3 = (f"Client {messageid4bth} {message3}")
        messageid4bth += 1
        channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                      body=messages3)
        time.sleep(random.randint(2,3))
        print(f"{messages3}")
