
import pandas as pd

def get_best(file):
    data = pd.read_csv(file, sep=':', header=None)
    data.rename(columns={0:'name', 1:'value'}, inplace=True)

    # get all rows with n50 string
    n50 = data.loc[data['name'].str.contains('N50')]
    n50 = n50.value
    n50 = n50.max()

    # get the kmer size
    kmer = data.loc[data['name'].str.contains('Kmer_size')]
    kmer = kmer.value
    kmer = kmer.max()

    # create dataframe with n50 and kmer size
    data = pd.DataFrame({'n50': [n50], 'kmer': [kmer]})
    data.to_csv('output/best_kmer.csv', index=False)
    print(f"best kmer is: {kmer} and best n50 is: {n50}")

if __name__ == "__main__":
    get_best('output/output.csv')
