"""
Assignment 5 Programming 3
Anwser bacterial annotation questions by using spark
Data Sciences for Life Sciences
Author: Daan Steur
Date:13/06/2022
"""  

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
import pandas as pd
import os, sys




def explain(data):
    return data._sc._jvm.PythonSQLUtils.explainString(data._jdf.queryExecution(), 'simple')

def spark_df(path):
    sc = SparkContext('local[16]')
    sqlContext = SQLContext(sc)
    spark_df = sqlContext.read.options(delimiter="\t", header=False).csv(SparkFiles.get(path))
    spark_df = spark_df.withColumnRenamed("_c0","protein_accession")\
                    .withColumnRenamed("_c1","seq_MD5_digest")\
                    .withColumnRenamed("_c2","seq_length")\
                    .withColumnRenamed("_c3","analysis_method")\
                    .withColumnRenamed("_c4","sig_accession")\
                    .withColumnRenamed("_c5","sig_description")\
                    .withColumnRenamed("_c6","start_location")\
                    .withColumnRenamed("_c7","stop_location")\
                    .withColumnRenamed("_c8","score")\
                    .withColumnRenamed("_c9","status_match")\
                    .withColumnRenamed("_c10","date")\
                    .withColumnRenamed("_c11","interPRO_accession")\
                    .withColumnRenamed("_c12","interPRO_description")\
                    .withColumnRenamed("_c13","GO_annots")\
                    .withColumnRenamed("_c14","pathway_annots")

    return spark_df


# 1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
def q1(df):
    print("You will get the answer for question 1 later.")
    data1 = df.select('InterPro_annotations_accession')\
          .filter(df.InterPro_annotations_accession != "-")\
          .distinct()
    epr1 = explain(data1)
    data1 = data1.count()
    return data1, epr1

# 2. How many annotations does a protein have on average?
def q2(df):
    print("You will get the answer for question 2 later.")
    data2 = df.select("Protein_accession",'InterPro_annotations_accession')\
                .filter(df.InterPro_annotations_accession != "-")\
                .groupBy("Protein_accession")\
                .count()\
                .select(mean("count"))
    epr2 = explain(data2)        
    data2 = data2.collect()[0].__getitem__(0)
    return data2, epr2

# 3. What is the most common GO Term found?
def q3(df):
    print("You will get the answer for question 3 later.")
    data3 = df.select(df.GO_annotations, explode(split(col("GO_annotations"),"\|"))\
                        .alias("Split_col"))
    data3 = data3.filter(data3.Split_col != "-")\
                .select("Split_col")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=False)
    epr3 = explain(data3)
    data3 = [data[0] for data in data3.take(1)]
    data3 = data3[0]
    return data3, epr3

# 4. What is the average size of an InterPRO feature found in the dataset?
def q4(df):
    print("You will get the answer for question 4 later.")
    data4 = df.withColumn('Sub', ( df['Stop_location'] - df['Start_location'])).summary("mean")
    epr4 = explain(data4) 
    data4 = data4.collect()[0].__getitem__(-1)
    return data4, epr4

# 5. What is the top 10 most common InterPRO features?
def q15(df):
    print("You will get the answer for question 5 later.")
    data5 = df.select('InterPro_annotations_accession')\
                .filter(df.InterPro_annotations_accession != "-")\
                .groupBy('InterPro_annotations_accession')\
                .count()\
                .sort("count",ascending=False)\
                .select("InterPro_annotations_accession")
    epr5 = explain(data5)
    data5 = [data[0] for data in data5.take(10)]
    return data5, epr5

# 6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
def q6(df):
    print("You will get the answer for question 6 later.")
    data6 = df.select('InterPro_annotations_accession',"Sequence_length",'Stop_location','Start_location')\
                .filter((df['Stop_location'] - df['Start_location'])/df["Sequence_length"]>=0.9)\
                .filter(df.InterPro_annotations_accession != "-")\
                .groupBy('InterPro_annotations_accession')\
                .count()\
                .sort("count",ascending=False)\
                .select("InterPro_annotations_accession")
    epr6 = explain(data6)
    data6 = [data[0] for data in data6.take(10)]
    return data6, epr6

# 7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
def q7(df):
    print("You will get the answer for question 7 later.")
    data7 = df.select(df.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,"))\
                .alias("Split_col"))
    data7 = data7.select("Split_col")\
                .filter(data7.Split_col != "")\
                .filter(data7.Split_col != "-")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=False)\
                .select("Split_col")
    epr7 = explain(data7)
    data7 = [data[0] for data in data7.take(10)]
    return data7,epr7

# 8. And the top 10 least common?
def q8(df):
    print("You will get the answer for question 8 later.")
    data8 = df.select(df.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,"))\
                .alias("Split_col"))
    data8 = data8.select("Split_col")\
                .filter(data8.Split_col != "")\
                .filter(data8.Split_col != "-")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=True)\
                .select("Split_col")
    epr8 = explain(data8)
    data8 = [data[0] for data in data8.take(10)]
    return data8, epr8

# 9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
def q9(df):
    print("You will get the answer for question 9 later.")
    data9 = df.select(df.InterPro_annotations_accession,df.InterPro_annotations_description)\
                .filter(df.InterPro_annotations_accession.isin(data6))\
                .distinct()
    data9 = data9.select(data9.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,")))\
                    .groupby("col")\
                    .count()
    data9 = data9.select(data9["col"], data9["count"])\
                    .filter(data9["col"] != "")\
                    .sort("count",ascending=False)
    epr9 = explain(data9)
    data9 = [data[0] for data in data9.take(10)]
    return data9, epr9

# 10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?
def q10(df):
    print("You will get the answer for question 10 later.")
    data10=df.select(df.Protein_accession,df.InterPro_annotations_accession,df.Sequence_length)\
                .filter(df.InterPro_annotations_accession != "-")\
                .groupby(df.Protein_accession,"Sequence_length")\
                .count()
    epr10 = explain(data10)
    data10 = data10.corr('Sequence_length', 'count')**2
    return data10, epr10

def output_csv(column1,column2,column3):
    d = {'Question': column1, 'Answer': column2,"Explain":column3}
    df = pd.DataFrame(data = d)
    if not os.path.exists("output"):
        os.makedirs("output")
    df.to_csv("output/assignment5.csv",index=False)
    df.to_csv("output/output.csv",index=False)
    return print("Finish the assignment! Good Job!")



if __name__ == "__main__":
    path = "/data/dataprocessing/interproscan/all_bacilli.tsv"
    df = spark_df(path)
    data1, epr1 =  q1(df)
    print(data1) ;print(epr1)
    data2, epr2 =  q2(df)
    print(data2) ;print(epr2)
    data3, epr3 =  q3(df)
    print(data3) ;print(epr3)
    data4, epr4 =  q4(df)
    print(data4) ;print(epr4)
    data5, epr5 =  q5(df)
    print(data5) ;print(epr5)
    data6, epr6 =  q6(df)
    print(data6) ;print(epr6)
    data7, epr7 =  q7(df)
    print(data7) ;print(epr7)
    data8, epr8 =  q8(df)
    print(data8) ;print(epr8)
    data9, epr9 =  q9(df)
    print(data9) ;print(epr9)
    data10,epr10 = q10(df)
    print(data10) ;print(epr10)
    column1 = list(range(1,11))
    column2 = [data1,data2,data3,data4,data5,data6,data7,data8,data9,data10]
    column3 = [epr1,epr2,epr3,epr4,epr5,epr6,epr7,epr8,epr9,epr10]
    output_csv(column1,column2,column3)