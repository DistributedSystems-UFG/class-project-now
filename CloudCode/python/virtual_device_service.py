from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
current_temperature = 'void'
current_light_level = 'void'
led_state = {'red': 0, 'green': 0}

devices = [
    {
        'name': 'led_red',
        'red_led_pin': 16,
        'state': 0
    },
    {
        'name': 'led_green',
        'green_led_pin': 18,
        'state': 0
    },
    {
        'name': 'light_sensor_pin',
        'light_sensor_pin': 29,
        'current_light_level': 'void'
    },
    {
        'name': 'current_temperature',
        'current_temperature': 'void'
    },
]

users = [{
    'id': 1,
    'username': 'user',
    'password': 'password',
    'token': '94a08da1fecbb6e8b46990538c7b50b2'
}]


# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER + ':' + KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print('Received Temperature: ', msg.value.decode())
        current_temperature = msg.value.decode()


# Kafka consumer to run on a separate thread
def consume_light_level():
    global current_light_level
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER + ':' + KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print('Received Light Level: ', msg.value.decode())
        current_light_level = msg.value.decode()


def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER + ':' + KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state

def authenticate(username, password):
    global token
    for user in users:
        if user['username'] == username and user['password'] == password:
            token = user['token']
            return token
    raise Exception('Unauthorized')

def validate_token(token):
    for user in users:
        if user['token'] == token:
            return True;
    return False


class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def Login(self, request, context):
        token = authenticate(request.username, request.password)
        print("token: ", token)
        return iot_service_pb2.LoginReply(token=token)


    def SayTemperature(self, request, context):
        if(validate_token(request.token)):
            return iot_service_pb2.TemperatureReply(temperature=current_temperature)
        return 'Unauthorized'


    def BlinkLed(self, request, context):
        if (validate_token(request.token) == False):
            print('Unauthorized')
            return 'Unauthorized'
        print("Blink led ", request.ledname)
        print("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        led_state[request.ledname] = request.state
        return iot_service_pb2.LedReply(ledstate=led_state)

    def SayLightLevel(self, request, context):
        if (validate_token(request.token) == False):
            print('Unauthorized')
            return 'Unauthorized'
        return iot_service_pb2.LightLevelReply(lightLevel=current_light_level)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()

    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command(led_state[color], color)
    serve()
