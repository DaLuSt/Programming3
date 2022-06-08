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
        return print(median)
    except Exception:
        print("Error in N50 calculation")

lengths = contig_parser(fileinput)
calculate_N50(lengths)
# fasta parser 

dna = []
sequences = []

def read_fasta(filename):
    global seq, header, dna, sequences 

#open the file  
    with open(filename) as file:    
        seq = ''        
        #forloop through the lines
        for line in file: 
            header = re.search(r'^>\w+', line)
            #if line contains the header '>' then append it to the dna list 
            if header:
                line = line.rstrip("\n")
                dna.append(line)            
            # in the else statement is where I have problems, what I would like is
            #else: 
                #the proceeding lines before the next '>' is the sequence for each header,
                #concatenate these lines into one string and append to the sequences list 
            else:               
                seq = line.replace('\n', '')  
                sequences.append(seq)      

filename = 'gc.txt'

# read_fasta(filename)




    

