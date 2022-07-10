#### status: in progress (Will do in summer)
#### Assignment 5: Portein function prediction with supervised learning
The goal of this assignment is to read in a large dataset of protein annotation information and to manipulate, summarize and analyze it using PySpark Dataframes.

#### Introduction
In the world of microbiology, exploration of new species and the functions of those species can be rather time consuming. Let say a new species is discovered and you want to know the specific protein functions inside that species. To get to that answer, the bacteria need to be grown, induced, cells need to be extracted, protein needs to be extracted and purified, that protein eventually needs to be sequenced and you in the end you align its DNA against tools like BLAST, to see if any useful results pop up. How wonderfully refreshing it would be to simply this by a large number of steps. You grow the bacteria, you sequence the bacteria, you run a model over that data and a list potential active proteins and the functions of those proteins gets printed onto a csv. The problem however is not the lack of data, the problem is the lack of accurate predictive models. In this report an attempt is made use supervised learning techniques to generate an accurate model to predict the protein function based on feature characteristics. 

#### Deliverables
The script should, given one or more of these TSV files with InterPROscan annotations, produce models (Pickle them to save them!) which predict protein function, and a file of training-data on which you trained them.

#### Run code
code is run in terminal using the following command:
```
python Main.py
```

#### output
The output is a csv file and can be found in the folder called output.
The pickled models are also saved in the output folder, along with the data used to train the model.