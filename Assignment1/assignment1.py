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
from Bio import Medline

api_key = "5465528a46551838834940b5006829e8e307"
Entrez.api_key = api_key

pmid = "35512704"

# Download the XML data for the article with the given PubMed ID.
def get_xml_data(pmid):
    

# get 10 articles refrences to the given article
def get_refrences(pmid):

# download the 10 articles
def download_refrenced_articles(pmid):
    
    




    
    
    
    
    






    
    

