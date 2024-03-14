import socket
import pickle
import threading

server = "192.168.1.3"
port = 26751
forma = 'utf-8'
#serverP = "192.168.1.6"
serverP = socket.gethostbyname(socket.gethostname())#this will dynamically obtain the ip adress of the machine this is running on 
port1 = 26752

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((serverP, port1))

#client.sendto(b"register dave 127.0.0.2 5 6", (server, port))
#client.sendto(b"register joe 127.0.0.2 7 8", (server, port))
#client.sendto(b"register bob 127.0.0.2 9 10", (server, port))
#client.sendto(b"setup-dht bob 3 2021", (server, port))

#def finishdht(users):
    

def handle():
    option = input("1 -> Register | 2 -> setupdht\n")
    if(option == "1"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (server, port))
        return 1
    elif(option == "2"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (server, port))
        return 2
    else:
        print("Not a valid command")
        return 0


def start():
    while True:
        option = handle()
        if(option == 2):
            message, addr = client.recvfrom(65535)
            if(message.decode(forma) == "SUCCESS"):
                print(message.decode(forma))
                message, addr = client.recvfrom(65535)
                print(pickle.loads(message))
            else:
                print(message.decode(forma))
                
        else:
            message, addr = client.recvfrom(65535) #response I get from server
            print(message.decode(forma))

print("Starting...")
start()
