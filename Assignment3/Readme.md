#### Assignment 3: SLURM it all around!
In the lectures, we discuss will discuss four systems for concurrently running programs on the network; SLURM, Dask, Spark and GNU Parallel. SLURM and Spark are fully-fledged schedulers while Dask and GNU Parallel are more user-centric. The latter two can hop onto SLURM though, to parallelize their operation. We will explore what this means for the speed of these programs, with an eye towards algorithmic complexity as well. 

#### Deliverables
You need to submit a bash script called assignment3.sh in the Assignment3 folder of your programming3 repository on GitHub. This script tests the blastp program with different "-numthreads" on the query file "MCRA.faa" (an Amino Acid file of the McrA protein involved in biogas production). This script makes an output directory called output when run, and places the run times for each blastp run in a file called timings.txt in the output directory. The timings should be based on the Linux "time" command (/usr/bin/time; make sure you're not using the built-in shell version, use the whole command name with path and all).

Your assignment is to run the program with -num_threads values from 1 to 16 and capture the time spent running in seconds

export BLASTDB=/local-fs/datasets/
blastp -query MCRA.faa -db refseq_protein/refseq_protein -num_threads 1 -outfmt 6 >> blastoutput.txt
The output should be a file called timings.txt and a PNG format matplotlib graph showing number-of-threads on the x axis and time-taken (s) on the y axis, called timings.png. (Note: so you need to run a small python script as well to generate the graph).

#### Run
sbatch assignment3.sh
