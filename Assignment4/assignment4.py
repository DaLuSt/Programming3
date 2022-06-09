"""
Assignment 4 Programming 4
Data Sciences for Life Sciences
Author: Daan Steur
Date:08/06/2022
"""
#!/usr/bin/python


from Bio import SeqIO
import os
import sys
from sqlalchemy import outparam

fileinput = '/homes/dlsteur/Git_repos/programming3/Assignment4/output_dir/contigs.fa'

def contig_parser(input_stdin):
    # appends each line to a list
    seq_list_raw = [str(line.strip()) for line in input_stdin]
    seq_list_higher = []
    seq_list_tmp = []

    for n in range(len(seq_list_raw)):    
        if not seq_list_raw[n].startswith('>'):
            seq_list_tmp.append(seq_list_raw[n])
        else:
            seq_list_higher.append(seq_list_tmp)
            seq_list_tmp = []
    
    seq_list_lengths = [len("".join(lst)) for lst in seq_list_higher]
    return seq_list_lengths


def calculate_N50(list_of_lengths):
    """
    Calculate N50 for a sequence of numbers.
    Args:list_of_lengths (list): List of numbers.
    Returns:float: N50 value.
    """
    tmp = []
    for tmp_number in set(list_of_lengths):
            tmp += [tmp_number] * list_of_lengths.count(tmp_number) * tmp_number
    tmp.sort()
    if (len(tmp) % 2) == 0:
        median = (tmp[int(len(tmp) / 2) - 1] + tmp[int(len(tmp) / 2)]) / 2
    else:
        median = tmp[int(len(tmp) / 2)]
 
    return median


if __name__ == "__main__":
    input = sys.stdin
    output = sys.stdout
    N50 = str(calculate_N50(contig_parser(input)))
    output.write(f'N50:{N50}, \n')




    

