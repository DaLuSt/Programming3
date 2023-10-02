#### Assignment 7: Portein function prediction with supervised learning
In the ever-expanding realm of bioinformatics and biomedical research, extracting information from vast repositories of scientific literature is a crucial task. Assignment 7 is set on an exciting journey into the world of data science and natural language processing. The objective being to use PySpark to parse PubMed XML files and extract key information from research articles. 

#### Introduction
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

#### Deliverables
To successfully complete this assignment, this script should be able to take one or more XML files as input and perform the following tasks:

- Parse PubMed XML files into a PySpark dataframe.
- Extract and organize the specified information from the articles.
- Create a secondary dataframe to answer the provided questions.


#### Run code
To execute the script, navigate to your terminal and use the following command:

```
python3 Assignment7.py
```

#### output
Upon running the code, the script will generate two CSV files. The first CSV file will contain the parsed data from the PubMed XML files, while the second CSV file will contain the answers to the questions posed in this assignment. Both output files will be located in an "output" folder, which the script will create for your convenience.
