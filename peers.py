import socket
import pickle
import threading
import csv
import math

serveraddr = "192.168.1.3"
sport = 26751
forma = 'utf-8'
#ipaddr = "192.168.1.6"
ipaddr = socket.gethostbyname(socket.gethostname())#this will dynamically obtain the ip adress of the machine this is running on 
pport = 26752

#information about our neighbor
global rightNeighbour
rightNeighbour = None #this will be a size three tuple
global identifier
identifier = None
global ringSize
ringSize = None
global year
year = None
dhtMade = False

#socket where it will listen only for the server
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((ipaddr, sport))

#socket where it will only listen to the peer
peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer.bind((ipaddr, pport))

#hash table of weather records
global records
records = {}

def dhtComp(name):
    if(dhtMade):
        msg = f"dht-complete {name}"
        client.sendto(msg.encode(forma), (serveraddr, sport))
    else:
        print("dht is not complete")

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
    global identifier
    global ringSize
    global rightNeighbour
    global records
    #loop to always check if there is a message from another peer
    while True:
        message, addr = peer.recvfrom(65535)
        if(message.decode(forma) == "set-id"):
            message, addr = peer.recvfrom(65535)
            info = pickle.loads(message)
            identifier = int(info[0])
            ringSize = int(info[1])
            #perform calc to get right neighbor
            temp = (identifier + 1) % ringSize
            rightNeighbour = info[2][temp]
        elif(message.decode(forma) == "store"):
            message, addr = peer.recvfrom(65535) #this is the id
            idd = message.decode(forma)
            idd = int(idd)
            if(idd == identifier):
                message, addr = peer.recvfrom(65535)
                out = pickle.loads(message)
                records[hash(' '.join(out))] = out
            else:
                message, addr = peer.recvfrom(65535)
                out = pickle.loads(message)
                peer.sendto(b"store", (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(idd).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(pickle.dumps(out), (rightNeighbour[1], int(rightNeighbour[2])))
        else:
            continue

def finishdht(users, year):
    #this is to count how many records each node has received
    printer = dict.fromkeys(range(0, len(users)))
    printer = {key: 0 for key in printer}
    global records
    global ringSize
    global rightNeighbour
    global identifier
    global dhtMade
    rightNeighbor = users[1] #leader will always get the next person in line
    identifier = 0 #leader will always have identifier as 0
    ringSize = len(users)
    #now loop starting past the leader and will send info to other peers
    for i in range(1, len(users)):
        lister = [i, len(users), users]
        peer.sendto(b"set-id", (users[i][1], int(users[i][2])))
        peer.sendto(pickle.dumps(lister), (users[i][1], int(users[i][2])))

    #now count lines in the details-{year}.csv file
    length = 0
    with open(f"details-{year}.csv") as file:
        length = sum(1 for line in file)

    #now read csv file line by line (each record) and send to appropriate peer
    with open(f"details-{year}.csv") as file:
        header = next(file) #skip header
        csvreader = csv.reader(file)

        #now iterate record by record
        for record in csvreader:
            idd = idfind(length - 1, int(record[0])) #get hashed id to see which peer it belongs to
            printer[idd] = printer[idd] + 1
            #check if it is leader's own id
            if(idd == identifier):
                records[hash(' '.join(record))] = record
            else:#if not leader, send to right neighbour to have it forwarded
                peer.sendto(b"store", (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(idd).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(pickle.dumps(record), (rightNeightbour[1], int(rightNeighbour[2]))) 
    dhtMade = True
    print(printer)

def handle():
    global year
    option = input("1 -> Register | 2 -> setupdht | 3 -> dht-complete\n")
    if(option == "1"):
        user = input("Please enter command: ")
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 1
    elif(option == "2"):
        user = input("Please enter command: ")
        year = user.split(' ')[-1] #year input by user
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 2
    elif(option == "3"):
        user = input("Please enter command: ")
        name = user.split(' ')[-1]
        dhtComp(name)
    else:
        print("Not a valid command")
        return 0


def start():
    global year
    #create a thread which will be checking in the background whether they have received
    #a message from a peer
    thread = threading.Thread(target=peers)
    thread.start()
    while True:
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
