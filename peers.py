import socket
import pickle
import threading
import csv
import math
import pandas as pd

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
year = None

#socket where it will listen only for the server
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((ipaddr, sport))

#socket where it will only listen to the peer
peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer.bind((ipaddr, pport))

#hash table of weather records
records = {}

#client.sendto(b"register dave 127.0.0.2 5 6", (server, port))
#client.sendto(b"register joe 127.0.0.2 7 8", (server, port))
#client.sendto(b"register bob 127.0.0.2 9 10", (server, port))
#client.sendto(b"setup-dht bob 3 2021", (server, port))

def isPrime(n):
    if(n <= 1):
        return False
    if(n <= 3):
        return True
    if(n % 2 == 0 or n % 3 == 0):
        return False
    for i in range(5, int(math.sqrt(n) + 1), 6):
        if(n % i == 0 or n % (i + 2) == 0):
            return False
    return True

def findPrime(n):
    if(n <= 1):
        return 2

    prime = n
    found = False

    while(not found):
        prime = prime + 1
        if(isPrime(prime) == True):
            found = True
    return prime

def idfind(length, event_id):
    #for size find frist prime number larger than 2 * length
    size = findPrime(length * 2)
    pos = event_id % size
    idd = pos % ringSize
    return idd

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
        if(message.decode(forma) == "store"):
            print("We got a store command!")

def finishdht(users, year):
    rightNeighbor = users[1] #leader will always get the next person in line
    identifier = 0 #leader will always have identifier as 0
    #now loop starting past the leader and will send info to other peers
    for i in range(1, len(users)):
        lister = [i, len(users), users]
        peer.sendto(b"set-id", (users[i][1], int(users[i][2])))
        peer.sendto(pickle.dumps(lister), (users[i][1], int(users[i][2])))
    #now count lines in the details-{year}.csv file
    length = len(results = pd.read_csv(f"details-{year}.csv"))
    #now read csv file line by line (each record) and send to appropriate peer
    with open(f"details-{year}.csv") as file:
        header = next(file) #skip header
        csvreader = csv.reader(file)
        #now iterate record by record
        for record in csvreader:
            idd = idfind(length - 1, int(record[0])) #get hashed id to see which peer it belongs to
            #check if it is leader's own id
            if(idd == identifier):
                records[hash(' '.join(record))] = record
            else:#if not leader, send to right neighbour to have it forwarded
                peer.sendto(b"store", (rightNeighbour[1], rightNeighbour[2]))
                peer.sendto(b"{idd}", (rightNeighbour[1], rightNeighbour[2]))
                peer.sendto(pickle.dumps(record), (rightNeightbour[1], rightNeighbour[2])) 


def handle():
    option = input("1 -> Register | 2 -> setupdht\n")
    if(option == "1"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 1
    elif(option == "2"):
        user = input("Please enter command: ")
        year = user.split(' ')[-1] #year input by user
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
                finishdht(pickle.loads(message), year)
            else:
                year = None #meaning there was an error so remove previous entry
                print(message.decode(forma))
                
        else:
            message, addr = client.recvfrom(65535) #response I get from server
            print(message.decode(forma))

print("Starting...")
start()
