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
namer = None #will keep track what the name of the 
lengther = None

#socket where it will listen only for the server
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((ipaddr, sport))

#socket where it will only listen to the peer
peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer.bind((ipaddr, pport))

#hash table of weather records
global records
records = {}

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
#both of these functions ^ \/ serve as a way to obtain next closest prime number for id and pos
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
    global records
    #for size find frist prime number larger than 2 * length
    size = findPrime(length * 2)
    pos = event_id % size
    idd = pos % ringSize
    return idd

#function that handles all peer requestions through the peer port
def peers():
    global identifier
    global ringSize
    global rightNeighbour
    global records
    global lengther
    #loop to always check if there is a message from another peer
    while True:
        #check to see what commands were received by other peeres
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
                #if it matches our id, then we can store the event in our hash table
                message, addr = peer.recvfrom(65535)
                length = int(message.decode(forma))
                lengther = length
                message, addr = peer.recvfrom(65535)
                out = pickle.loads(message)
                records[int(out[0]) % (findPrime((length) * 2))] = out
            else:
                message, addr = peer.recvfrom(65535)
                length = int(message.decode(forma))
                lengther = length
                message, addr = peer.recvfrom(65535)
                out = pickle.loads(message)
                peer.sendto(b"store", (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(idd).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(length).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(pickle.dumps(out), (rightNeighbour[1], int(rightNeighbour[2])))
        elif(message.decode(forma) == "find-event"):
            message, addr = peer.recvfrom(65535)
            eventid = int(message.decode(forma))
            message, addr = peer.recvfrom(65535)
            tupler = pickle.loads(message)
            message, addr = peer.recfrom(65535) #this is the id_seq
            #now find pos for hash table location and id to see if it pertains to me
            pos = eventid % (findPrime(lengther * 2))
            ider = findid(lengther, eventid)
            id_seq = ""
            id_seq = id_seq + message.decode(forma)
            id_seq = id_seq + str(ider)
            if(identifier == ider):
                if pos in records:
                    peer.sendto(b"SUCCESS", (tupler[1], tupler[2]))
                    peer.sendto(picke.dumps(records[pos]), (tupler[1], tupler[2])) #this is the record found
                    peer.sendto(id_seq.encode(forma), (tupler[1], tupler[2])) #this is id_seq
                else:
                    peer.sendto(b"FAILURE", (tupler[1], tupler[2])) #send error message too below this statement?
                    peer.sendto(b"The storm event " + eventid + " not found in the DHT", (tupler[1], tupler[2]))
            else:
                #make range of numbers excluding the number ider that was produced
                i = list(range(0, ringSize))
                i.remove(ider)
                nexter = random(i)
                id_seq = id_seq + str(nexter)
                peer.sendto(b"find-event", (rightNeighbour[1], rightNeighbour[2]))
                peer.sendto(eventid.encode(forma), (rightNeighbour[1], rightNeighbour[2]))
                #send three tuple so that when found or not, message is routed to me
                lister = (namer, ipaddr, pport) #send credentials so that when found or not, send back
                peer.sendto(pickle.dumps(lister), (rightNeighbour[1], rightNeighbour[2]))
                peer.sendto(id_seq.encode(forma), (peer[1], (rightNeighbour[1], rightNeighbour[2])))
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
                records[int(record[0]) % (findPrime((length - 1) * 2))] = record
            else:#if not leader, send to right neighbour to have it forwarded
                peer.sendto(b"store", (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(idd).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(str(length - 1).encode(forma), (rightNeighbour[1], int(rightNeighbour[2])))
                peer.sendto(pickle.dumps(record), (rightNeighbour[1], int(rightNeighbour[2]))) 
    dhtMade = True
    print(printer)


def findEvent(peer):
    eventid = input("What is the event-id you want to look for?: ")
    peer.sendto(b"find-event", (peer[1], peer[2]))
    peer.sendto(eventid.encode(forma), (peer[1], peer[2]))
    #send three tuple so that when found or not, message is routed to me
    lister = (namer, ipaddr, pport) #send credentials so that when found or not, send back
    peer.sendto(pickle.dumps(lister), (peer[1], peer[2]))
    peer.sendto(b"", (peer[1], peer[2]))

def handle():
    global year
    option = input("1 -> Register | 2 -> setupdht | 3 -> dht-complete | 4 -> query-dht | 5 -> leave-dht | 6 -> join-dht | 7 -> dht-rebuilt | 8 -> deregister | 9 -> teardown-dht | 10 -> teardown-complete\n")
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
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 3
    elif(option == "4"):
        user = input("Please enter command: ")
        name = user.split(' ')[-1] #get name
        client.sendto(user.encode(forma), (serveraddr, sport))
        return 4
    elif(option == "5"):
        client.sendto(b"leave-dht", (serveraddr, sport))
    elif(option == "6"):
        client.sendto(b"join-dht", (serveraddr, sport))
    elif(option == "7"):
        client.sendto(b"dht-rebuilt", (serveraddr, sport))
    elif(option == "8"):
        client.sendto(b"deregister", (serveraddr, sport))
    elif(option == "9"):
        client.sendto(b"teardown-dht", (serveraddr, sport))
    elif(option == "10"):
        client.sendto(b"teardown-complete", (serveraddr, sport))
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
        elif(option == 4):
            message, addr = client.recvfrom(65535)
            print(message.decode(forma))
            message, addr = client.recvfrom(65535)
            peer = pickle.loads(message)
            findEvent(peer)
                
        else:
            message, addr = client.recvfrom(65535) #response I get from server
            print(message.decode(forma))

print("Starting...")
start()
