import multiprocessing
from Bio import Entrez
import time, sys, os


def get_citations(pmid):
    try:
        Entrez.email = "dalust1997@gmail.com"
        results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                        db="pmc",
                                        LinkName="pubmed_pmc_refs",
                                        id=pmid,
                                        api_key='5465528a46551838834940b5006829e8e307'))
        references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
        references = references[:10]
        return references
    except Exception:
        print('Error: Getting citations.')

def create_dir(dir_name):
    try:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
    except OSError:
        print(f'Error: Creating directory. {dir_name}')

def download_pmids(pmid):
    try:
        pmids = list(get_citations(pmid))
        for pmid in pmids:
            handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                                   api_key='5465528a46551838834940b5006829e8e307')

            with open(f'output/{pmid}.xml', 'wb') as file:
                file.write(handle.read())
    except Exception:
        print('Error: Downloading PMIDs.')
        
def main():
    try:
        pmid = sys.argv[1]
        create_dir('output')
        download_pmids(pmid)
    except Exception:
        print('Error: Main function.')


if __name__ == '__main__':
    # 30049270
    start = time.perf_counter()
    process = multiprocessing.Process(target=main)
    process.start()
    end = time.perf_counter()
    print(f'Multi finished in {round(end-start, 2)} second(s)') 
    print('Completed download')