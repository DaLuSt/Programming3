#### Assignment 4: assembly
The goal of this assignment is to investigate the optimal settings for an "assembly" process. "Assembly" is the bioinformatics process of combining reads form a de-novo genome sequencing project to form a "complete" genome (prokaryotes) or chromosome (eukaryotes). 

#### deliverables
The deliverable is a Bash script called assignment4.sh and a Python script called assignment4.py in the Assignment4/ directory of your programming3 GitHub repository. (As always, pay attention to capitalization!).

Your bash script should control the job; it should run velveth and velvetg, using GNU parallel of course to parallelize trying different kmer sizes. Your python script should examine the produced assemblies (they will be in FASTA format) and calculate the N50. Your python script should read any number of contigs from sys.stdin and write the N50 of those contigs to sys.stdout (as a simple number). GNU Parallel controls how many lines are sent into your python script using its chunking facilities, and using up to 16 jobs in parallel.

#### run code
assignment4.sh
python assignment4.sh

