#from __future__ import print_function

import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *
import time


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub(channel)

        getToken = stub.Login(iot_service_pb2.LoginRequest(username='user', password='password'))
        print(getToken)

        while(True):
            response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorName='my_sensor'))
            print("Temperature received: " + response.temperature)
            time.sleep(1)

if __name__ == '__main__':
    logging.basicConfig()
    run()