import multiprocessing
from Bio import Entrez
import time
import sys
import os


def get_citations(pmid):
    Entrez.email = "dalust1997@gmail.com"
    results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                    db="pmc",
                                    LinkName="pubmed_pmc_refs",
                                    id=pmid,
                                    api_key='5465528a46551838834940b5006829e8e307'))
    references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
    references = references[:10]
    return references

def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def download_pmids(pmid):
    pmids = list(get_citations(pmid))
    for pmid in pmids:
        handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                               api_key='5465528a46551838834940b5006829e8e307')
    
        with open(f'output/{pmid}.xml', 'wb') as file:
            file.write(handle.read())
            
def main():
    create_dir('output')
    pmid = sys.argv[1]
    download_pmids(pmid)

if __name__ == '__main__':
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print(f'Normal finished in {round(end-start, 2)} second(s)')
    
    start = time.perf_counter()
    process = multiprocessing.Process(target=main)
    process.start()
    end = time.perf_counter()
    print(f'Multi finished in {round(end-start, 2)} second(s)') 
    print('Completed download')




    
    




    
    
    
    
    






    
    

