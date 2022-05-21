import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = '185.12.219.103'
PORTNUM = 5893
AUTHKEY = 'O4sSEvT{+uYm{p?y3orc'
# data = "30049270"
data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]


def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.
    """
    
    job_q = mp.Queue()
    result_q = mp.Queue()
    
    class QueueManager(BaseManager): pass
    
    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)
    
    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print(f'Server started {port}')
    return manager

def runserver(fn, data):
    # Start a shared manager server and access its queues
    manager = server.make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    
    if not data:
        print("Gimme something to do here!")
        return
    
    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})
    
    time.sleep(2)  
    
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)



def make_client_manager(ip, port, authkey):
        """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager): pass
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    manager = ServerQueueManager(address=(IP, PORTNUM), authkey=AUTHKEY)
    manager.connect()
    
    print(f'Client connected to {ip}:{port}')
    return manager



def runclient(fn, data):
    # Start a shared manager server and access its queues
    manager = Client.make_client_manager(IP, PORTNUM, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    
    if not data:
        print("Gimme something to do here!")
        return
    
    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})
    
    time.sleep(2)  
    
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the client!")
    manager.shutdown()
    print(results)
    

     


# if __name__ == '__main__':
#     server.runserver('download_pmids', data)
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
# Python Simple Password Generator

# import string
# from random import *

# characters = string.ascii_letters + string.punctuation  + string.digits
# password =  "".join(choice(characters) for _ in range(randint(16, 25)))

# print(password)