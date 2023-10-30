from multiprocessing import Process
import threading
from random import randint
import string
from collections import namedtuple
from socket import *
from enum import IntEnum
import json
import re

class Peer:
    def __init__(self, id, host, port, priority=0):
        self.id = id
        self.addr = namedtuple('Address', ['host', 'port'])(host, port)
        self.priority = priority

    def __str__(self):
        return self.id

class Bully():
    def __init__(self, peer, peers):
        self.id = peer.id
        self.addr = peer.addr
        self.priority = peer.priority
        self.peers = peers
        self.coordinator = None
        self.ok_received = False
        self.elect_lock = threading.Lock()
        self.elect_condition = threading.Condition()
        self.coordinator_lock = threading.Lock()

        self.sockets = {}
        for id in peers.keys():
            self.sockets[id] = socket(AF_INET, SOCK_STREAM)

    def connect(self, peerID):
        if peerID == self.id: return False

        if self.sockets[peerID].connect(self.peers[peerID].addr):
            #print('{id}: Connected to {peer}'.format(id=self.id, peer=peerID))
            return True
        else:
            #print('{id}: Failed to connect to {peer}'.format(id=self.id, peer=peerID))
            return False
            
    def send(self, peerID, type):
        message = json.dumps({'type': type, 'id': self.id, 'addr': self.addr}).encode()

        if not self.sockets[peerID].send(message): 
            self.connect(peerID) & self.sockets[peerID].send(message)
        
        print('{from_}: {type} sent to {to}'.format(from_=self.id, type=type, to=peerID))

    def setCoordinator(self, id):
        self.coordinator_lock.acquire()
        self.coordinator = id
        if self.elect_lock.locked():
            self.elect_lock.release()   # the election ends when a new coordinator is chosen
        self.coordinator_lock.release()
        print('{id}: Coordinator = {coordinator}'.format(id=self.id, coordinator=id))

    def election(self):
        self.elect_condition.acquire()
        self.ok_received = False
        for id in self.active_peers.keys():
            if id > self.id:
                self.send(id, 'ELECTION')
        self.elect_condition.wait_for(lambda : self.ok_received, timeout=1.0)
        lost_election = self.ok_received
        self.elect_condition.release()

        if lost_election:
            return
        
        for peer in self.peers:
            if peer.id == self.id:
                continue
            self.send(peer.id, 'COORDINATOR')
        self.setCoordinator(self.id)

    def holdElection(self):
        if self.elect_lock.acquire(blocking=False):
            threading.Thread(target=self.election).start()

    def listen(self):
        s = socket(AF_INET, SOCK_STREAM) # create TCP socket
        s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # allow socket to be immediately reused
        s.bind(self.addr) # bind socket to host address and port number
        s.listen() # listen for connection requests

        def receive(conn, addr):
            peerID = None
            while True:  
                data = conn.recv(1024)
                if not data:
                    if peerID:
                        print('{id}: Connection closed by {peer}'.format(id=self.id, peer=peerID))
                        del self.active_peers[peerID]
                        if peerID == self.coordinator:
                            self.holdElection()
                    conn.close()
                    return

                msg = json.loads(data.decode())
                
                peerID = IDs(msg['id'])
                if peerID not in self.active_peers:
                    self.connect({peerID: tuple(msg['addr'])})

                if msg['type'] == 'ELECTION':
                    self.send(peerID, 'OK')
                    self.holdElection()
                elif msg['type'] == 'OK':
                    self.elect_condition.acquire()
                    self.ok_received = True
                    self.elect_condition.notify()
                    self.elect_condition.release()
                elif msg['type'] == 'COORDINATOR':
                    self.setCoordinator(peerID)

        while True:                   
            (conn, addr) = s.accept()   # returns new socket and addr. client 
            threading.Thread(target=receive, args=(conn, addr,)).start() # start a new thread to handle the connection

    def start(self):
        print('{id}: Starting'.format(id=self.id))
        threading.Thread(target=self.listen, args=()).start()
        self.connect(self.peers)
        self.holdElection()
        return

class CustomEnum(IntEnum):
    def __str__(self):
        return self.name

IDs = CustomEnum('IDs', list(string.ascii_uppercase), start=0)

if __name__ == '__main__':
    print('Enter the number of processes:')
    n = input()
    n = int(n)
    print('Enter the max priority')
    k = input()
    k = int(k)

    peers = {}
    for i in range(n):
        BASE_PORT = 50070
        id = IDs(i % len(IDs.__members__))
        peers[id] = Peer(
            id=id, 
            host='localhost',
            port=BASE_PORT + i, 
            priority=randint(0, k+1)
        )
        
    processes = {}
    def runProcess(id):
        if id in processes:
            print('Process {id} already running'.format(id=id))
            return
        if id not in peers:
            print('Process {id} does not exist'.format(id=id))
            return
        bully = Bully(id, peers[id], peers)
        p = Process(target=bully.start, args=())
        p.start()
        processes[id] = p

    for id in reversed(peers.keys()):
        runProcess(id)

    kill = re.compile('kill[\s]+(\w)', re.IGNORECASE)
    run = re.compile('run[\s]+(\w)', re.IGNORECASE)

    def killProcess(id):
        processes[id].kill()
        processes[id].join()
        print('Process {id} killed'.format(id=id))
        del processes[id]

    while True:
        command = input()
        if re.match(kill, command):
            id = IDs[re.match(kill, command).group(1)]
            killProcess(id)
        elif re.match(run, command):
            id = IDs[re.match(run, command).group(1)]
            runProcess(id)
        else:
            print('Command not recognized')

    