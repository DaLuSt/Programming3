
## Assignment 2 You want more? You get more!
Given this basic skeleton of a multithreaded jobserver we discussed in class, modify it to:

Download and analyze the NCBI articles from different "remote" clients
You can test all this using '' as your IP (remember this means : localhost)
For the time being, start clients by hand using the "ssh" command to connect terminals to different computers on the BIN network.

### 1.1. Deliverables
Your script needs to download "numberofarticles" which are referenced by "STARTINGPUBMEDID" using "numberofchildren" processes on the "hosts" computers. (So, like assignment1, but now from multiple computers). Each child can write to the same folder on the NFS filesystem (your home, or somewhere on the /commons).
Your script needs to analyze the XML of each of the references further to extract all the authors of the article. It should save the authors in a Python tuple and use the Pickle module to save it to the disk as output/PUBMED_ID.authors.pickle where PUBMEDID is of course the pubmed ID of the article in question.

`assignment2.py -n <number_of_peons_per_client> [-c | -s] --port <portnumber> --host <serverhost> -a <number_of_articles_to_download> STARTING_PUBMED_ID`
  
 - NB1: if you only specify "localhost" then all processes are run on one computer (good for testing)
 - NB2: if you want to run truly networked, specify at least two hosts: the first one is assumed to run the server process, all hosts after that are clients
 - NB3: you need to both save the downloaded xml files and communicate the reference PUBMEDs back to the server
