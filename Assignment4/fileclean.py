
import pandas as pd
import shutil

def get_kmer(file):
    try:
        out = pd.read_csv(file, names=['N50', 'Kmer_size'], header=None)
        best_kmer = out.sort_values('N50', ascending=False).iloc[0, 1]
        return best_kmer
    except Exception:
        print("Error: Input file is not in csv format.")

def copy_file(src, dst):
    try:
        shutil.copyfile(src, dst)
    except Exception:
        print("Error: Input file is not in the correct format.")


if __name__ == "__main__":
    print(get_kmer('output/output.csv'))
    best_k = get_kmer('output/output.csv')
    best_k_path = f'/students/2021-2022/master/Martin_DSLS/output/{best_k}/contigs.fa'
    output_path = 'output/contigs.fa'
    print('file copy complete')
    copy_file(best_k_path, output_path)