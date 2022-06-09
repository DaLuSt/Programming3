"""
Assignment 4 Programming 4
Check SeqIO for parsing .fa files and calculating the N50
Data Sciences for Life Sciences
Author: Daan Steur
Date:08/06/2022
"""
#!/usr/bin/python
import sys

def contig_parser(input_stdin):
    """ 
    Parse the input file and return a list of contig lengths.
    Args:input_stdin (str): Input file.
    returns:list: List of contig lengths.
    """
    try: 
        # appends each line to a list
        seq_list_raw = [str(line.strip()) for line in input_stdin]
        seq_list_higher = []
        seq_list_tmp = []

        for item in seq_list_raw:
            if not item.startswith('>'):
                seq_list_tmp.append(item)
            else:
                seq_list_higher.append(seq_list_tmp)
                seq_list_tmp = []

        return [len("".join(lst)) for lst in seq_list_higher]
    except Exception:
        print("Error: Input file is not in fasta format.")


def calculate_N50(lengths_list):
    """
    Calculate N50 for a sequence of numbers.
    Args:list_of_lengths (list): List of numbers.
    Returns:float: N50 value.
    """
    try: 
        tmp = []
        for tmp_number in set(lengths_list):
                tmp += [tmp_number] * lengths_list.count(tmp_number) * tmp_number
        tmp.sort()
        return (tmp[len(tmp) // 2 - 1] + tmp[len(tmp) // 2]) / 2 if (len(tmp) % 2) == 0 else tmp[len(tmp) // 2]
    except Exception:
        print("Error: Input is not a list of numbers.")


if __name__ == "__main__":
    input = sys.stdin
    output = sys.stdout
    N50 = str(calculate_N50(contig_parser(input)))
    output.write(f'N50:{N50}, \n')




    

