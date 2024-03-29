#Name: Daois Sanchez Mora
#ID  : 1221797369
#Date: Sun Feb 18 15:15:57 MST 2024
#Client-server script
import socket
import threading
import random
import pickle

PORT = 26751 #specifying the port number in my given range
#SERVER = socket.gethostbyname(socket.gethostname())#this will dynamically obtain the ip adress of the machine this is running on 
SERVER = "10.159.53.205"
ADDR = (SERVER, PORT) #what we pass in to bind when binding socket
FORMAT = 'utf-8'

server = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)#creating the socket
server.bind(ADDR)#we are binding the socket to the ip adress and port we specified

#dictionary of lists to store the registered users
registrees = {}
dhtMade = False #global var to keep track if a dht was already made
started = False#check if dht setup inititiated

#register command when new connection/user is made
def register(name, ipv4, mport, nport, addr):
    global registrees
    #lets first check user already exits by name
    if name in registrees.keys():
        server.sendto(b"FAILURE", addr)
        return
    #now check if each port is unique
    for key in registrees.keys():
        if(registrees[key][1] == mport or registrees[key][2] == nport):
            server.sendto(b"FAILURE", addr)
            return
    #if none matches, then we can add to the dictionary
    registrees[name] = [ipv4, mport, nport, "Free"]#when adding a person, their state is free
    server.sendto(b"SUCCESS", addr)

#when a user decides to create the dht/initialize
def setdht(name, n, year, addr):
    global registrees
    global started
    if name not in registrees.keys():
        server.sendto(b"FAILURE", addr)
        return
    if(int(n) < 3):
        server.sendto(b"FAILURE", addr)
        return
    if(len(registrees) < 3):
        server.sendto(b"FAILURE", addr)
        return
    if(dhtMade):
        server.sendto(b"FAILURE", addr)
        return
    registrees[name][3] = "Leader"#set the person who initiated construction to be the leader
    #list to store the users chosen, the first user is always the leader
    lister = []
    lister.append((name, registrees[name][0], registrees[name][2]))
    #pick n-1 free users from the list(dictionary)
    for i in range(int(n) - 1):
        hi = list(registrees.keys())
        tmp = random.choice(hi)
        while(registrees[tmp][3] == "Leader" or registrees[tmp][3] == "inDHT"):
            tmp = random.choice(hi)
        registrees[tmp][3] = "inDHT"
        lister.append((tmp, registrees[tmp][0], registrees[tmp][2]))
    #store the exacts ones that were changed into a list of sublists so that it can be passed to the client
    #tell client it was successful before sending list
    server.sendto(b"SUCCESS", addr)
    started = True
    #msg = str(lister)
    #msg = msg.encode(FORMAT)
    msg = pickle.dumps(lister)
    server.sendto(msg, addr)
            
#a function to check if the peer has done all the necessary steps for setting up the dht
def dhtComplete(name, addr):
    global started
    global dhtMade
    if(registrees[name][3] != "Leader" or started == False):
        server.sendto(b"FAILURE", addr)
    else:
        started = False
        dhtMade = True
        server.sendto(b"SUCCESS", addr)

def queryDht(name, addr):
    global registrees
    #check if the dht is made
    if(dhtMade == False):
        server.sendto(b"FAILURE", addr)
    if name not in registrees.keys():
        server.sendto(b"FAILURE", addr)
    if(registrees[name][3] != "Free"):
        server.sendto(b"FAILURE", addr)
    #return a random size 3 tuple of a peer in the dht
    names = list(registrees.keys())
    tmp = random.choice(hi)
    while(registrees[name][3] == "Free"):
        tmp = random.choice(hi)
    lister = (tmp, registrees[tmp][0], registrees[tmp][2])
    server.sendto(b"SUCCESS", addr)
    server.sendto(pickle.dumps(lister), addr)


def handle(message, addr): #this function handles all requests sent by the peers connecting to it
    errorString = "Error, invalid command"
    print(f"This person -> {addr} has connected")
    print(f"They said -> {message.decode(FORMAT)}")
    print()

    message = message.decode(FORMAT)
    try:
        if(message.split(' ')[0] == "register"):
            temp = message.split(' ')
            register(temp[1], temp[2], temp[3], temp[4], addr)#this is passing in the name, address, and two ports
        elif(message.split(' ')[0] == "setup-dht"):
            temp = message.split(' ')
            setdht(temp[1], temp[2], temp[3], addr)
        elif(message.split(' ')[0] == "dht-complete"):
            temp = message.split(' ')
            dhtComplete(temp[1], addr) #name
        elif(message.split(' ')[0] == "query-dht"):
            temp = message.split(' ')[1]
            queryDht(temp, addr)
        elif(message == "leave-dht"):
            server.sendto(b"SUCCESS", addr)
        elif(message == "join-dht"):
            server.sendto(b"SUCCESS", addr)
        elif(message == "dht-rebuilt"):
            server.sendto(b"SUCCESS", addr)
        elif(message == "deregister"):
            server.sendto(b"SUCCESS", addr)
        elif(message == "teardown-dht"):
            server.sendto(b"SUCCESS", addr)
        elif(message == "teardown-complete"):
            server.sendto(b"SUCCESS", addr)
        else:
            server.sendto(errorString.encode(FORMAT), addr)
    except IndexError:
        server.sendto(b"Insufficient amount of arguments", addr)

def start():
    while True:
        message, addr = server.recvfrom(65535) #when accepting new connection, will obtain its address/port along with object
        if started is False:
            thread = threading.Thread(target=handle, args=(message, addr))
            thread.start()
        else:
            server.sendto(b"FAILURE", addr)

print("The server has started and is running...")
print(SERVER)
start()
