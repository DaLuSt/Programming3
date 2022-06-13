#### Assignment 5: Pyspark
The goal of this assignment is to read in a large dataset of protein annotation information and to manipulate, summarize and analyze it using PySpark Dataframes.

#### Deliverables
You should use the PySpark Dataframe interface to read in and manipulate this file. This file contains ~4,200,000 protein annotations. You need to use the PySpark dataframe functions to answer the following questions:
1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
2. How many annotations does a protein have on average?
3. What is the most common GO Term found?
4. What is the average size of an InterPRO feature found in the dataset?
5. What is the top 10 most common InterPRO features?
6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
8. And the top 10 least common?
9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?

Your output should be a CSV file with 3 columns;
1. in the first column the question number
2. in the second column the answer(s) to the question
3. in the third column the output of the scheduler's physical plan (using the `.explain()` PySpark method) as a string

#### Run code

