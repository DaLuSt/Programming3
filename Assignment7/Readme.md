## Assignment 7: XML Parsing with PySpark and key information extraction from research articles.
In the ever-expanding realm of bioinformatics and biomedical research, extracting information from vast repositories of scientific literature is a crucial task. Assignment 7 is set on an exciting journey into the world of data science and natural language processing. The objective being to use PySpark to parse PubMed XML files and extract key information from research articles. 

### Introduction
Scientific literature, especially in the domain of molecular biology and biochemistry, is a goldmine of knowledge. PubMed, as one of the largest repositories of biomedical literature, offers a treasure trove of research articles. However, making sense of this wealth of information can be daunting. This assignment addresses this challenge by developing a script capable of processing PubMed XML files and organizing the data into a PySpark dataframe.

The key information that will be extracted includes:

- PubMed ID
- First Author
- Last Author
- Year published
- Title
- Journal Title
- Length of Abstract (if Abstract text is present).
- A column of references in a list variable, if references are present for the article.

Furthermore, this assignment also involves the creation of a second dataframe to answer specific questions such as:

- Number of articles per First Author
- Number of articles per Year
- Minimum, maximum, Average length of an abstract
- Average Number of articles per Journal Title per Year

### Deliverables
To successfully complete this assignment, this script should be able to take one or more XML files as input and perform the following tasks:

- Parse PubMed XML files into a PySpark dataframe.
- Extract and organize the specified information from the articles.
- Create a secondary dataframe to answer the provided questions.

### Run code
Clone the repository to your local machine using the following command:

```
git clone https://github.com/DaLuSt/Programming3.git
```

Navigate to the Assignment7 folder:
```
cd ../Programming3/Assignment7
```

Install the required packages using the following command:
```
pip install -r requirements.txt
```

To execute the python script, navigate to your terminal and use the following command:

```
python3 Assignment7.py [file_limit] (file_limit is optional / default is 1)
```

### output[https://github.com/DaLuSt/Programming3/tree/main/Assignment7/output]
Upon running the code, the script will generate multiple CSV files. The first CSV file will contain the parsed data from the PubMed XML files, while the remaining CSV files will contain the answers to the questions posed in this assignment. All output files will be located in an "output" folder, which the script will create for your convenience.

- parsed_data folder (combined_data.csv) : contains the parsed data from the PubMed XML files.
- abstract_length folder (abstract_length.csv) : contains the answer to the question "What is the minimum, maximum, and average length of an abstract?" 
- author_count folder (author_count.csv)  : contains the answer to the question "How many articles were published per author?"
- journal_year_count folder (journal_year_count.csv) : contains the answer to the question "How many articles were published per journal per year?" 
- year_count folder (year_count.csv) : contains the answer to the question "How many articles were published per year?" 
