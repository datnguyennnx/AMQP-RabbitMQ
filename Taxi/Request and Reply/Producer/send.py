from email import message
from pika.exchange_type import ExchangeType
import pika
import time
import random
import uuid

def on_reply_message_received(ch, method, properties, body):
    print(f"reply recieved: {body}")
#Getting access to RabbitMQ server
connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()
#Declare 'mailbox' queue for requesting & replying
reply_queue = channel.queue_declare(queue='', exclusive=True)

channel.basic_consume(queue=reply_queue.method.queue, auto_ack=True,
    on_message_callback=on_reply_message_received)
#Declare queues of another districs

#Create an explicit exchange
channel.exchange_declare(exchange='seatexchange', exchange_type=ExchangeType.topic)
channel.exchange_declare(exchange='taxi4-place_exchange', exchange_type=ExchangeType.topic)

#Binding seatexchange to taxi-place_exchange
channel.exchange_bind("taxi4-place_exchange","seatexchange", routing_key="4.#")

cor_id = str(uuid.uuid4())

#Make a list for random routing key
list = ['4.dist1','4.binhthanh','4.thuduc']
item = random.choice(list)
#Send orders to  queques
message1 = "ordered 4seattaxi in district 1"
message2 = "ordered 4seattaxi in district thuduc"
message3 = "ordered 4seattaxi in district binhthanh"
####################################################################################   
if item == '4.dist1':
    messages1 = (f"A client {message1}")     
    cor_id = str(uuid.uuid4())
    channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                          properties=pika.BasicProperties(
                                reply_to=reply_queue.method.queue,
                                correlation_id=cor_id),
                      body=messages1)
    time.sleep(random.randint(2,3))
    print(f"You successfully {message1}")
        
####################################################################################        
if item == '4.thuduc':
    messages2 = (f"A client {message2}")    
    channel.basic_publish(exchange='seatexchange',  
                      routing_key=item,
                            properties=pika.BasicProperties(
                                reply_to=reply_queue.method.queue,
                                correlation_id=cor_id),
                      body=messages2)
    time.sleep(random.randint(2,3))
    print(f"You successfully {message2}")
        
####################################################################################       
if item == '4.binhthanh':
    messages3 = (f"A client {message3}")
    channel.basic_publish(exchange='seatexchange',  
                      routing_key=item, 
                            properties=pika.BasicProperties(
                                reply_to=reply_queue.method.queue,
                                correlation_id=cor_id),
                      body=messages3)
    time.sleep(random.randint(2,3))
    print(f"You successfully {message3}")

print("Waiting for reply")

channel.start_consuming()
