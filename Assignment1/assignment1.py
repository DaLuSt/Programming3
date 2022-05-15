# The goal of this assignment is to get used to programmatically querying NCBI.
# You will need to use the Biopython Querying facilities (see: Chapter 9 Biopython Tutorial) to download the XML data for 10 articles.
# NB: To do this succesfully, be sure to make an NCBI account and set up API tokens!
# See: New API keys Rate-limit your script to below the required requests per second using time.sleep() if necessary.
# (May require some experimentation.)

# A script that downloads 10 articles from PubMed using the Biopython API.

# Your script needs to be called "assignment1.py" in the "Assignment1" folder of your "Programming2" Bitbucket repo.
# The only command-line argument you need is a query (PubMed ID) to ask Entrez about an article.
# Your script should then download 10 articles cited by this one _concurrently_ using the multiprocessing.Pool() and map() constructs! The articles should be saved as PUBMED_ID.xml in the directory "output" in the directory your script is run from.

from Bio import Entrez
import sys

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


def download_pmids(pmid):
    pmids = list(get_citations(pmid))
    for pmid in pmids:
        handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                               api_key='5465528a46551838834940b5006829e8e307')
    
        with open(f'output/{pmid}.xml', 'wb') as file:
            file.write(handle.read())

if __name__ == '__main__':
    pmid = sys.argv[1]
    download_pmids(pmid)




    
    




    
    
    
    
    






    
    

