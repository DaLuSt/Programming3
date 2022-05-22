import argparse as ap
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import os, sys, time, queue
import pubmedpickle as pp

def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager

def runserver(fn, data):
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
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
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager

def runclient(num_processes):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)

def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()

def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result' : result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result' : ERROR})
        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)

if __name__ == '__main__':
    #set up argparser to catch input from command line
    argparser = ap.ArgumentParser(
                                description="Script that saves the authors of refernced articles by the given PubMed ID article")
    argparser.add_argument("STARTING_PUBMED_ID", action="store", nargs=1,  type = str, default=10,
                            help="Pubmed id to get references")
    argparser.add_argument("-n", action="store", type=int, dest = "n",
                            help="Number of peons for each client")
    argparser.add_argument("-a", action="store", type=int, dest = "a",
                             help="Number of articles from which to get the authorlist")
    argparser.add_argument("--port", action="store", type=int,dest = "port", help="the port")
    argparser.add_argument("--host", action="store", type=str,dest = "host", help="the host")
    group = argparser.add_mutually_exclusive_group()
    group.add_argument('-c', action='store_true',  dest="c")
    group.add_argument('-s', action='store_true',  dest="s")
    args = argparser.parse_args()
    print("Getting: ", args.STARTING_PUBMED_ID)
    pmid = args.STARTING_PUBMED_ID
    port = args.port
    host = args.host
    n = args.n
    a = args.a

    POISONPILL = "MEMENTOMORI"
    ERROR = "DOH"
    IP = host
    PORTNUM = port
    AUTHKEY = b'whathasitgotinitspocketsesss?'

    #get references
    references = pp.get_references(pmid)
    #call the function to get the authors from
    #references and index with a(number of authors to extract)
    if args.s:
        server = mp.Process(target=runserver, args=(pp.write_pickle, references[:a]))
        server.start()
        time.sleep(1)
        server.join()

    if args.c:
        client = mp.Process(target=runclient, args=(n, ))
        client.start()
        client.join()