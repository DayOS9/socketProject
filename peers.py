import socket
import pickle
import threading

serveraddr = "192.168.1.3"
sport = 26751
forma = 'utf-8'
#ipaddr = "192.168.1.6"
ipaddr = socket.gethostbyname(socket.gethostname())#this will dynamically obtain the ip adress of the machine this is running on 
pport = 26752

#information about our neighbor
rightNeighbour = None #this will be a size three tuple
identifier = None
ringSize = None

#socket where it will listen only for the server
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((ipaddr, sport))

#socket where it will only listen to the peer
peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer.bind((ipaddr, pport))

#client.sendto(b"register dave 127.0.0.2 5 6", (server, port))
#client.sendto(b"register joe 127.0.0.2 7 8", (server, port))
#client.sendto(b"register bob 127.0.0.2 9 10", (server, port))
#client.sendto(b"setup-dht bob 3 2021", (server, port))

def peers():
    #loop to always check if there is a message from another peer
    while True:
        message, addr = peer.recvfrom(65535)
        if(message.decode(forma) == "set-id"):
            message, addr = peer.recvfrom(65535)
            info = pickle.loads(message)
            identifier = info[0];
            ringSize = info[1];
            #perform calc to get right neighbor
            temp = (identifier + 1) % ringSize
            rightNeighbour = info[2][temp]

def finishdht(users):
    rightNeighbor = users[1] #leader will always get the next person in line
    identifier = 0 #leader will always have identifier as 0
    #now loop starting past the leader and will send info to other peers
    for i in range(1, len(users)):
        lister = [i, len(users), users]
        peer.sendto(b"set-id", (users[i][1], int(users[i][2])))
        peer.sendto(pickle.dumps(lister), (users[i][1], int(users[i][2])))

def handle():
    option = input("1 -> Register | 2 -> setupdht\n")
    if(option == "1"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 1
    elif(option == "2"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 2
    else:
        print("Not a valid command")
        return 0


def start():
    #create a thread which will be checking in the background whether they have received
    #a message from a peer
    thread = threading.Thread(target=peers)
    thread.start()
    while True:
        print(rightNeighbour)
        option = handle()
        if(option == 2):
            message, addr = client.recvfrom(65535)
            if(message.decode(forma) == "SUCCESS"):
                print(message.decode(forma))
                message, addr = client.recvfrom(65535)
                print(pickle.loads(message))
                finishdht(pickle.loads(message))
            else:
                print(message.decode(forma))
                
        else:
            message, addr = client.recvfrom(65535) #response I get from server
            print(message.decode(forma))

print("Starting...")
start()
