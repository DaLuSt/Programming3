#!/bin/bash

mkdir -p  output
echo ' ' > output/time.txt

blast() {
	export BLASTDB=/local-fs/datasets/
	blastp -query MCRA.faa -db refseq_protein/refseq_protein -num_threads $1 -outfmt 6 >> blastoutput.txt
    }

for i in {1..16}
do
echo '------------' >> output/timings.txt
echo "threads $i" >> output/timings.txt
(time blast $i) 2>> output/timings.txt
done

python3 time_plot.py