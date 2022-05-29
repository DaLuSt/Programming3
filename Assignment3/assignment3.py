"""
Assignment 3 Programming 3
Data Sciences for Life Sciences
Author: Daan Steur
"""

# MapReduce jobs. The jobs are represented as a python script, which implements 3 methods as an interface:

# An iterable data object called "Data"
# A function called "mapper" which takes (inkey, invalue) tuples
# A function called "reducer" which takes (outkey, intermediatevalue) tuples.


#!/bin/bash
#SBATCH --time 2:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
module load Bowtie2
export BOWTIE2_INDEXES=/data/p225083/BOWTIE2_INDEXES
export DATA=/data/p225083
bowtie2 -x human -U$DATA/all.fq -p 16 -S${DATA}/output.sam 

#SBATCH –mail-user=(mail adress) will run the script and sent you updates.
#SBATCH –outpu=somefile /


# The "x" isavariable, which is set consecutively from1to 10
For x in {1..16} ; do something ; done

        
        
    

        














# command line assignment3.py –hosts <list of hosts, first is server> [-s | -c] -n cores -p port -d jobsdirectory -r <retries>