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
led_state = {'red':0, 'green':0}

users = {'user1': 'pass1', 'user2': 'pass2', 'user3': 'pass3'}

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print ('Received Temperature: ', msg.value.decode())
        current_temperature = msg.value.decode()

# Kafka consumer to run on a separate thread
def consume_light_level():
    global current_light_level
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print ('Received Light Level: ', msg.value.decode())
        current_light_level = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state

def authenticate_user(context):
    # Obtém as credenciais do contexto
    auth_info = context.invocation_metadata()
    # Verifica se as credenciais estão presentes e se o usuário e a senha são válidos
    if auth_info and len(auth_info) == 1:
        user_pass = auth_info[0].split(':')
        if user_pass[0] in users and user_pass[1] == users[user_pass[0]]:
            return True
    return False
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        # Check user authentication
        if not authenticate_user(context):
            return iot_service_pb2.TemperatureReply(temperature='Not authenticated')
        return iot_service_pb2.TemperatureReply(temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        # Check user authentication
        if not authenticate_user(context):
            return iot_service_pb2.LedReply(ledstate={'red': 0, 'green': 0})
        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        led_state[request.ledname] = request.state
        return iot_service_pb2.LedReply(ledstate=led_state)

    def SayLightLevel(self, request, context):
        # Check user authentication
        if not authenticate_user(context):
            return iot_service_pb2.LightLevelReply(lightLevel='Not authenticated')
        return iot_service_pb2.LightLevelReply(lightLevel=current_light_level)


class AuthInterceptor(grpc.ServerInterceptor):
  def __init__(self, auth_func):
    self.auth_func = auth_func

  def intercept_service(self, continuation, handler_call_details):
      metadata = dict(handler_call_details.invocation_metadata)
      if self._auth_func(metadata):
          return continuation(handler_call_details)
      else:
          raise Exception("Unauthorized")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')

    auth_interceptor = AuthInterceptor(authenticate_user)
    server.intercept_service(auth_interceptor)

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
        produce_led_command (led_state[color], color)
    serve()
