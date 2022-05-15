## 1. Assignment 1
### 1.1. Goal
The goal of this assignment is to get used to programmatically querying NCBI and use the multiprocessing.Pool construct for concurrent programming in Python.

You will need to use the Biopython Querying facilities (see: Chapter 9 Biopython Tutorial to download the XML data for 10 articles. In particular, I recommend using the esearch, elink, efetch functions.

### 1,2. getting started
The only command-line argument you need is a query PubMed ID to ask Entrez about an article. the script can be called as follows:
```
python assignment1.py pubmed_id
```
### 1.3. Output
The script script will run, given a valid Pubmed id, and will return a folder called output with the first 10 citations of the input Pubmed id.


