#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)

        getToken = stub.Login(iot_service_pb2.LoginRequest(username='user', password='password'))
        print(getToken)

        while(True):
            print('##################################')
            print('### 1 - Led red: start')
            print('### 2 - Led red: shutdown')
            print('### 3 - Led green: start')
            print('### 4 - Led green: shutdown')
            print('##################################')
            opcao = int(input('Digite sua opção: '))

            if opcao == 1:
                response = stub.BlinkLed(iot_service_pb2.LedRequest(state=1, ledname='red'))
                print("Led state is on")

            elif opcao == 2:
                response = stub.BlinkLed(iot_service_pb2.LedRequest(state=0, ledname='red'))
                print("Led state is off")

            elif opcao == 3:
                response = stub.BlinkLed(iot_service_pb2.LedRequest(state=1, ledname='green'))
                print("Led state is on")

            elif opcao == 4:
                response = stub.BlinkLed(iot_service_pb2.LedRequest(state=0, ledname='green'))
                print("Led state is off")

            else:
                exit('Até mais...')


#        response = stub.BlinkLed(iot_service_pb2.LedRequest(state=int(sys.argv[1]),ledname=sys.argv[2]))

#    if response.ledstate[sys.argv[2]] == 1:
#        print("Led state is on")
#    else:
#        print("Led state is off")

if __name__ == '__main__':
    logging.basicConfig()
    run()
