from socket import *
import time

HOST = '10.4.4.13'
PORT = 8088
BUFSIZE = 1024

ADDR = (HOST, PORT)

udpCliSock = socket(AF_INET, SOCK_DGRAM)

a = 1
while True:

    a = a + 1
    data = '''cpu_load_short777,host=server07,region=us-west4 value=0.6{2} {1}000000000
cpu_load_short777,host=server0{0},region=us-west value=0.5{2} {1}000000000'''.format( 6,int(time.time()),a )
    if not data:
        break
    udpCliSock.sendto(data,ADDR)
    time.sleep(1)

udpCliSock.close()
