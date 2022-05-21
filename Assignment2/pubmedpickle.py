"""
get authors pmids class
Data Sciences for Life Sciences
Author: Daan Steur
"""

import multiprocessing
from Bio import Entrez
import time, sys, os, pickle
class pubmedpickle():
    def __init__(self, pmid):
        self.pmid = pmid
        self.create_dir('output')
        self.write_pickle(pmid)

    def get_citations(self):
        try:
            Entrez.email = "dalust1997@gmail.com"
            results = Entrez.read(Entrez.elink(dbfrom="pubmed", db="pmc", LinkName="pubmed_pmc_refs", id=self, api_key='5465528a46551838834940b5006829e8e307'))

            references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
            references = references[:]
            return references
        except Exception:
            print('Error: Getting citations.')

    def create_dir(self):
        try:
            if not os.path.exists(self):
                os.makedirs(self)
        except OSError:
            print(f'Error: Creating directory. {self}')

    def download_pmids(self):
        try:
            pmids = list(pubmedpickle.get_citations(self))
            for self in pmids:
                handle = Entrez.efetch(db="pmc", id=self, rettype="XML", retmode="text", api_key='5465528a46551838834940b5006829e8e307')


                with open(f'output/{self}.xml', 'wb') as file:
                    file.write(handle.read())
        except Exception:
            print('Error: Downloading PMIDs.')
            

    def get_authors(self):
        '''funtion to get authors of the referenced articles
        '''
        Entrez.email = "dalust1997@gmail.com"
        results = Entrez.read(Entrez.esummary(db="pubmed", id=self, api_key='5465528a46551838834940b5006829e8e307'))

        author_list = []
        author_list = list(results[0]["AuthorList"])
        return tuple(author_list)

    def write_pickle(self):
        '''function to write out the authors list as a tuple into a pickle 
        file
        input: pmid
        output: file(s) containing authors as a tuple
        '''
        references = pubmedpickle.get_citations(self)
        for self in references:
            author_tup = pubmedpickle.get_authors(self)
            with open(f'output/{self}.authors.pickle', 'wb') as file:
                pickle.dump(author_tup, file)

    def main():
        try:
            pmid = sys.argv[1]
            pubmedpickle.create_dir('output')
            pubmedpickle.write_pickle(pmid)
        except Exception:
            print('Error: Main function.')

# if __name__ == '__main__':
#     # 30049270
    
#     start = time.perf_counter()
#     process = multiprocessing.Process(target=pubmedpickle.main)
#     process.start()
#     end = time.perf_counter()
#     print(f'Multi finished in {round(end-start, 2)} second(s)') 
#     print('Process completed')