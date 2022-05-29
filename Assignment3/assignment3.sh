#!/bin/bash
#SBATCH --time 2:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=short
#SBATCH --mem=size_in_MB
#SBATCH --output=somefile

# run with -num_threads from 1 to 16 on query file MCRA.faa
export BLASTDB=/local-fs/datasets/
for x in {1..16} ; do time blastp -query MCRA.faa -db refseq_protein/refseq_protein -num_threads $x -outfmt 6 >> timings.txt ; done

# capture time in seconds of runtime in a file
# called timings.txt in output directory
# based on linux time command
# /usr/bin/time

# timings.png file of matplotlib plot
# x-axis: number-if-threads
# y-axis: time taken (s)

# Make sure the python executable is in your PATH environment
# variable then add in your script
# python path/to/the/python_script.py

#!/bin/sh
python python_script.py