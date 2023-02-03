# Assignment 2 You want more? You get more

Given this basic skeleton of a multithreaded jobserver we discussed in class, modify it to:

Download and analyze the NCBI articles from different "remote" clients
You can test all this using '' as your IP (remember this means : localhost)
For the time being, start clients by hand using the "ssh" command to connect terminals to different computers on the BIN network.

## 1.1. Deliverables

Your script needs to download "numberofarticles" which are referenced by "STARTINGPUBMEDID" using "numberofchildren" processes on the "hosts" computers. (So, like assignment1, but now from multiple computers). Each child can write to the same folder on the NFS filesystem (your home, or somewhere on the /commons).
Your script needs to analyze the XML of each of the references further to extract all the authors of the article. It should save the authors in a Python tuple and use the Pickle module to save it to the disk as output/PUBMED_ID.authors.pickle where PUBMEDID is of course the pubmed ID of the article in question.

`assignment2.py -n <number_of_peons_per_client> [-c | -s] --port <portnumber> --host <serverhost> -a <number_of_articles_to_download> STARTING_PUBMED_ID`
  
- NB1: if you only specify "localhost" then all processes are run on one computer (good for testing)
- NB2: if you want to run truly networked, specify at least two hosts: the first one is assumed to run the server process, all hosts after that are clients
- NB3: you need to both save the downloaded xml files and communicate the reference PUBMEDs back to the server

## Distributed Computing over the Network

## If you can't get enough cores in one computer, get more computers!

- Most serious distributed computing problems can't be solved using the processors in only one computer
- Computer clusters of homogeneous resources (ie. computers of roughly the same type) are used in practice
- In addition to the sheer _number_ of compute resources, _reliability_ is also a key concern

## Network concepts

- You can communicate over the network using TCP/IP "packets": items of data of fixed length
- The TCP/IP protocol guarantees that the packets are routed to the correct destination
- It also makes sure that large datasets are split into multiple packets, and they are guaranteed to arrive, and reassembled
- It presents itself to the programmer as two "sockets" (unique IP address and PORT combination) and acts like it is a file ("IO Stream")
- You can use this abstraction directly using the Python "socket" module and read and write data between Python programs on different computers
- We will be going one step higher in the abstraction hierarchy though

## Defining a Manager

- The Manager is a "proxy object"; it offers access to shared resources under one name
- Confusingly, given this primary function, it is also what you use to communicate over the network!
- It can "listen" and "connect" on a TCP/IP socket; a unique IP and PORT combination
- You need to define it twice: on each "end" of a connection (or "in the middle" if you are using a one-to-many approach as we do here")
- This may be a good point to explain different distributed computing "cluster topologies"...
