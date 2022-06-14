"""
Assignment 5 Programming 3
Anwser bacterial annotation questions by using spark
Data Sciences for Life Sciences
Author: Daan Steur
Date:13/06/2022
"""   

from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql import SQLContext


def create_spark_df(path):
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

if __name__ == "__main__":
    spark_df = create_spark_df("bacterial_proteins.txt")
