from email import message
from pika.exchange_type import ExchangeType
import pika
import time
import random
####################################################################################

#Establish a connection with RabbitMQ server.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel() 

####################################################################################

#Create an explicit exchange
channel.exchange_declare(exchange='accident', exchange_type=ExchangeType.fanout)

####################################################################################

#Declare queques and binding them to accident exchange
channel.queue_declare('DISTRICT_BINH_THANH_4seat')
channel.queue_declare('DISTRICT_1_4seat')
channel.queue_declare('DISTRICT_THU_DUC_4seat')

channel.queue_declare('DISTRICT_BINH_THANH_7seat')
channel.queue_declare('DISTRICT_1_7seat')
channel.queue_declare('DISTRICT_THU_DUC_7seat')

channel.queue_bind("DISTRICT_BINH_THANH_4seat","accident")
channel.queue_bind("DISTRICT_1_4seat","accident")
channel.queue_bind("DISTRICT_THU_DUC_4seat","accident")

channel.queue_bind("DISTRICT_BINH_THANH_7seat","accident")
channel.queue_bind("DISTRICT_1_7seat","accident")
channel.queue_bind("DISTRICT_THU_DUC_7seat","accident")

####################################################################################

#Send warning to taxi drivers
message = "An accident happened around 0:20 on Nguyen Xi Street, Ward 26, Binh Thanh District, Ho Chi Minh City."
channel.basic_publish(exchange='accident',
                      routing_key='',
                      body= message)    
print(f"Sent message: {message}")    

#Closing the connection after message reach the Rabbitmq.
connection.close()
