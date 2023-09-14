"""
Description: This program is used to create and train a machine learning model to predict the InterPro annotations of small proteins.
data source : path = "/data/dataprocessing/interproscan/all_bacilli.tsv"
Author: Daan Steur
date: 25-06-2023
"""
# pyspark
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark_dist_explore import hist

# pyspark ML
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes, GBTClassifier, MultilayerPerceptronClassifier, LinearSVC
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 

# ETL & visualization
import numpy as np
import warnings
import time
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def create_dataframe(path, num_rows=None):
    """
    Create a Spark DataFrame from a file with the specified schema.
    
    Args:
        path (str): The file path.
        num_rows (int): The number of rows to select from the DataFrame (default is 5000).
        
    Returns:
        pyspark.sql.DataFrame: The Spark DataFrame.
    """
    schema = StructType([
        StructField("Protein_accession", StringType(), True),
        StructField("Sequence_MD5_digest", StringType(), True),
        StructField("Sequence_length", IntegerType(), True),
        StructField("Analysis", StringType(), True),
        StructField("Signature_accession", StringType(), True),
        StructField("Signature_description", StringType(), True),
        StructField("Start_location", IntegerType(), True),
        StructField("Stop_location", IntegerType(), True),
        StructField("Score", FloatType(), True),
        StructField("Status", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("InterPro_annotations_accession", StringType(), True),
        StructField("InterPro_annotations_description", StringType(), True),
        StructField("GO_annotations", StringType(), True),
        StructField("Pathways_annotations", StringType(), True)
    ])
    
    spark = SparkSession.builder.master("local[16]") \
        .config('spark.driver.memory', '128g') \
        .config('spark.executor.memory', '128g') \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .appName("InterPro").getOrCreate()
    
    df = spark.read.option("sep", "\t").option("header", "False").csv(path, schema=schema)
    
    # Select the first num_rows rows
    df = df.limit(num_rows)
    
    return df





main():
    path = "/data/dataprocessing/interproscan/all_bacilli.tsv"
    data = create_dataframe(path, num_rows=None)

    
    


if __name__ == '__main__':
    main()