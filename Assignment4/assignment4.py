"""
Assignment 4 Programming 4
Data Sciences for Life Sciences
Author: Daan Steur
Date:08/06/2022
"""

#!/usr/bin/python

# Check SeqIO for parsing .fa files and calculating the N50

from Bio import SeqIO
import sys, os, re

fileinput = '/homes/dlsteur/Git_repos/programming3/Assignment4/output_dir/contigs.fa'

def contig_parser(file):
    try:
        handle = open(file, 'r')
        SeqRecords = SeqIO.parse(handle, 'fasta')
        len_list = []  # create list to store lengths
        for record in SeqRecords:   # loop through each fasta entry
            length = len(record.seq)    # get sequence length
            # print(length)  # print("%s: %i bp" % (record.id, length))
            len_list.append(length)
    except Exception:
        print("File not found")


def calculate_N50(list_of_lengths):
    """
    Calculate N50 for a sequence of numbers.
    Args:list_of_lengths (list): List of numbers.
    Returns:float: N50 value.
    """
    try:
        tmp = []
        for tmp_number in set(list_of_lengths):
                tmp += [tmp_number] * list_of_lengths.count(tmp_number) * tmp_number
        tmp.sort()
        if (len(tmp) % 2) == 0:
            median = (tmp[len(tmp) // 2 - 1] + tmp[len(tmp) // 2]) / 2
        else:
            median = tmp[len(tmp) // 2]
        return median
    except Exception:
        print("Error in N50 calculation")

def read_fasta(filename):
    global seq, header, dna, sequences 

if __name__ == "__main__":
    input = sys.stdin
    # print(calculate_N50(contig_parser(input)))
    output = sys.stdout
    N50 = str(calculate_N50(contig_parser(input)))
    output.write(f'N50:{N50}, \n')
    # # print(calculate_N50(lenghts))




    

