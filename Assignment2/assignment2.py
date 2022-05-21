"""
Assignment 2 Programming 3
Data Sciences for Life Sciences
Author: Daan Steur
"""

import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
from pubmedpickle import *

class PubmedManager:

    def __init__(self, port, authkey, ip):
        self.port = port
        self.authkey = authkey
        self.ip = ip
        self.stopsign = "STOP"
        self.error = "ERROR"

    def make_server_manager(self):
        """ Create a manager for the server, listening on the given port.
            Return a manager object with get_job_q and get_result_q methods.
        """
        job_q = queue.Queue()
        result_q = queue.Queue()

        class QueueManager(BaseManager):
            pass

        QueueManager.register('get_job_q', callable=lambda: job_q)
        QueueManager.register('get_result_q', callable=lambda: result_q)

        manager = QueueManager(address=('', self.port), authkey=self.authkey)
        manager.start()
        print(f'Server started at port {self.port}')
        return manager


    def runserver(self, fn, data):
        # Start a shared manager server and access its queues
        manager = self.make_server_manager()
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
        shared_job_q.put(self.stopsign)
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        print("Aaaaaand we're done for the server!")
        manager.shutdown()
        print(results)


    def make_client_manager(self):
        """ Create a manager for a client. This manager connects to a server on the
            given address and exposes the get_job_q and get_result_q methods for
            accessing the shared queues from the server.
            Return a manager object.
        """
        class ServerQueueManager(BaseManager):
            pass

        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')

        manager = ServerQueueManager(address=(self.ip, self.port), authkey=self.authkey)
        manager.connect()

        print(f'Client connected to {self.ip}:{self.port}')
        return manager


    def runclient(self, num_processes):
        manager = self.make_client_manager()
        job_q = manager.get_job_q()
        result_q = manager.get_result_q()
        self.run_workers(job_q, result_q, num_processes)
        
    def run_workers(self, job_q, result_q, num_processes):
        processes = []
        for _ in range(num_processes):
            temP = mp.Process(target=self.peon, args=(job_q, result_q))
            processes.append(temP)
            temP.start()
        print(f"Started {len(processes)} workers!")
        for temP in processes:
            temP.join()

    def peon(self,job_q, result_q):
        my_name = mp.current_process().name
        while True:
            try:
                job = job_q.get_nowait()
                if job == self.stopsign:
                    job_q.put(self.stopsign)
                    print("Aaaaaaargh", my_name)
                    return
                else:
                    try:
                        result = job['fn'](job['arg'])
                        print(f"Peon {my_name} Workwork on {job['arg']}!")
                        result_q.put({'job': job, 'result' : result})
                    except NameError:
                        print("Can't find yer fun Bob!")
                        result_q.put({'job': job, 'result' : self.error})

            except queue.Empty:
                print("sleepytime for", my_name)
                time.sleep(1)
                
    def capitalize(self):
        """Capitalizes the word you pass in and returns it"""
        return self.upper()
    
if __name__ == "__main__":
    IP = '185.12.219.103'
    PORTNUM = 45263
    AUTHKEY = b'whathasitgotinitspocketsesss?'
    # input_args = Interface()
    pmid = "30049270"
    main_pmid = pubmedpickle.main(pmid)
    ref_ids = main_pmid.ncbi_query()

    server_start = PubmedManager(PORTNUM,AUTHKEY,IP)
    server = mp.Process(target=server_start.runserver, args=(pubmedpickle.main(), ref_ids))
    server.start()
    time.sleep(1)
    client = mp.Process(target=server_start.runclient, args=(4,))
    client.start()
    server.join()
    client.join()


