#from __future__ import print_function

import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        metadata = [('username', 'user1'), ('password', 'pass1'),]
        print(type(metadata))
        stub = iot_service_pb2_grpc.IoTServiceStub(channel)
        response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorName='my_sensor', metadata=metadata))

    print("Temperature received: " + response.temperature)

if __name__ == '__main__':
    logging.basicConfig()
    run()
