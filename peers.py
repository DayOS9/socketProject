import socket
import pickle

server = "192.168.1.3"
port = 26751
forma = 'utf-8'
#serverP = "192.168.1.6"
serverP = socket.gethostbyname(socket.gethostname())#this will dynamically obtain the ip adress of the machine this is running on 
port1 = 26752

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((serverP, port1))
'''client.sendto(b"register dave 127.0.0.2 5 6", (server, port))
client.sendto(b"register joe 127.0.0.2 7 8", (server, port))
client.sendto(b"register bob 127.0.0.2 9 10", (server, port))
client.sendto(b"setup-dht bob 3 2021", (server, port))'''
#ask user who to connect to
targetIP = input("What is the target IP?\n")
targetPort = input("What is the port number?\n")

while(True):#input options
    print("1 -> register | 2 -> set-up dht | 3 -> complete-dht")
    option = input("What is your option?\n")
    if(option == "1"):
        giveMe = input()
        giveMe = giveMe.encode(forma)
        client.sendto(giveMe, (targetIP, int(targetPort)))
        #print(data.decode(forma))
        break
    if(option == "2"):
        giveMe = input()
        giveMe = giveMe.encode(forma)
        client.sendto(giveMe, (targetIP, int(targetPort)))
        break



while(True):
    data, addr = client.recvfrom(1024)
    print(data.decode(forma))
