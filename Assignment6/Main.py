
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
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 

# ETL & visualization
import numpy as np
import warnings
import time
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Create data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def create_dataframe(path, num_rows=5000):
    """
    Create a Spark DataFrame from a file with the specified schema.
    
    Args:
        path (str): The file path.
        num_rows (int): The number of rows to select from the DataFrame (default is 5000).
        
    Returns:
        pyspark.sql.DataFrame: The Spark DataFrame.
    """
    # Define the schema
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
    
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("InterPro") \
        .config("spark.driver.memory", "128g") \
        .config("spark.executor.memory", "128g") \
        .config("spark.sql.debug.maxToStringFields", "25") \
        .master("local[16]") \
        .getOrCreate()
    
    # Read the CSV file into a DataFrame
    df = spark.read \
        .option("sep", "\t") \
        .option("header", "False") \
        .csv(path, schema=schema)
    
    # Select the first num_rows rows
    df = df.limit(num_rows)
    print("DataFrame created")
    
    return df


# data preprocessing
def data_preprocessing(df):
    """
    It will help you to finish preprocessing data.
    df: spark df
    return small_df,large_df
    """
    # remove InterPro_annotations_accession == "-"
    # get the length of protein
    # get the ratio to distinguish them to large and small InterPro_annotations_accession
    # 1 for large, 0 for small InterPro_annotations_accession
    df = df.filter(df.InterPro_annotations_accession != "-")\
        .withColumn("Ratio", (abs(df["Stop_location"] - df["Start_location"])/df["Sequence_length"]))\
        .withColumn("Size", when((abs(df["Stop_location"] - df["Start_location"])/df["Sequence_length"])>0.9,1).otherwise(0))

    # get the intersection to make sure there is a match of large and small InterPro_annotations_accession(at least one large and one small InterPro_annotations_accession)
    intersection = df.filter(df.Size == 0).select("Protein_accession").intersect(df.filter(df.Size == 1).select("Protein_accession"))
    intersection_df = intersection.join(df,["Protein_accession"])

    # get the number of small InterPro_annotations_accession in each Protein_accession
    small_df = intersection_df.filter(df.Size == 0).groupBy(["Protein_accession"]).pivot("InterPro_annotations_accession").count()

    # There are several InterPro_annotations_accession with the same Protein_accession. I only choose the largest one.
    large_df = intersection_df.filter(df.Size == 1).groupby(["Protein_accession"]).agg(max("Ratio").alias("Ratio"))
    large_df = large_df.join(intersection_df,["Protein_accession","Ratio"],"inner").dropDuplicates(["Protein_accession"])

    # Drop the useless columns
    columns = ("Sequence_MD5_digest","Analysis","Signature_accession","Signature_description",
        "Score","Status","Date","InterPro_annotations_description","GO_annotations",
        "Pathways_annotations","Ratio","Size","Stop_location","Start_location","Sequence_length")
    large_df = large_df.drop(*columns)
    print("data preprocessing finished")
    
    return small_df, large_df


def ML_df_create(small_df,large_df):
    """
    It will help you to create a correct ML dataframe.
    small_df: spark df, preprocessing to fit the criteria ratio<=0.9
    large_df: spark df, preprocessing to fit the criteria ratio>0.9
    return ML_df
    """
    # Create the df for ML, we do not need Protein_accession anymore.
    ML_df = large_df.join(small_df,["Protein_accession"],"outer").fillna(0).drop("Protein_accession")

    # catalogize y variable
    Label = StringIndexer(inputCol="InterPro_annotations_accession", outputCol="InterPro_index")

    # catalogize X variable
    input_columns = ML_df.columns[1:]
    assembler = VectorAssembler(inputCols=input_columns,outputCol="InterPro_features")

    pipeline = Pipeline(stages=[Label,assembler])
    ML_final = pipeline.fit(ML_df).transform(ML_df)
    print("ML dataframe created")
    
    return ML_final


# creating a training and testing dataset with a 70/30 split
def split_data(ML_final, percentage=0.7):
    """
    it can help you split the data to trainning data and test data.
    ML_final: df
    percentage:int, you can set another value.
    return: trainData, df; testData,df
    """
    (trainData, testData) = ML_final.randomSplit([percentage, 1-percentage],seed=42)
    
    print("Data split into training and test sets")
    
    return trainData, testData



def train_and_evaluate_naive_bayes_with_cv(model_type, train_data, test_data, output_file):
    # Create the NaiveBayes model with the specified model type
    nb_model = NaiveBayes(modelType=model_type, labelCol="InterPro_index",
                          featuresCol="InterPro_features", predictionCol="prediction")

    # Define hyperparameter grid for smoothing
    param_grid = (ParamGridBuilder()
                  .addGrid(nb_model.smoothing, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.0])
                  .build())

    # Create a MulticlassClassificationEvaluator
    nb_evaluator = MulticlassClassificationEvaluator(labelCol='InterPro_index',
                                                    predictionCol='prediction',
                                                    metricName='accuracy')

    # Create a CrossValidator
    cv = CrossValidator(estimator=nb_model,
                        evaluator=nb_evaluator,
                        estimatorParamMaps=param_grid,
                        numFolds=5,
                        parallelism=10,
                        seed=42)

    # Train the model and measure the time taken
    start_time = time.time()
    cv_model = cv.fit(train_data)
    end_time = time.time()
    training_time_hours = (end_time - start_time) / 3600

    # Make predictions on the test data using the best model from cross-validation
    nb_cv_predictions = cv_model.transform(test_data)

    # Evaluate the model's accuracy
    nb_cv_accuracy = nb_evaluator.evaluate(nb_cv_predictions)

    # Train a separate model on the training data (without cross-validation) to get the initial accuracy
    nb_initial_model = nb_model.fit(train_data)
    nb_initial_predictions = nb_initial_model.transform(test_data)
    nb_initial_accuracy = nb_evaluator.evaluate(nb_initial_predictions)

    # Write accuracies and time taken to a text file
    with open(output_file, 'w') as f:
        f.write(f"Model Type: {model_type}\n")
        f.write(f"Initial Accuracy: {nb_initial_accuracy}\n")
        f.write(f"Cross-Validated Accuracy: {nb_cv_accuracy}\n")
        f.write(f"Time Taken (hours): {training_time_hours}\n")

    print("Model Type:", model_type)
    print("Initial Accuracy:", nb_initial_accuracy)
    print("Cross-Validated Accuracy:", nb_cv_accuracy)
    print("Time Taken (hours):", training_time_hours)
    
    return cv_model, nb_model


def save_spark_model(model, file_path):
    """
    Save a Spark MLlib model to a file.
    
    Args:
        model: Trained Spark MLlib model.
        file_path (str): File path where the model will be saved.
    
    Returns:
        None
    """
    model.save(file_path)
        
        
def save_dataframe_as_csv(dataframe, file_path):
    """
    Save a Spark DataFrame as a CSV file.
    
    Args:
        dataframe: Spark DataFrame to be saved.
        file_path (str): File path where the CSV file will be saved.
    
    Returns:
        None
    """
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = dataframe.toPandas()
    
    # Save Pandas DataFrame as CSV
    pandas_df.to_csv(file_path, index=False)


def main():
    path = "/data/dataprocessing/interproscan/all_bacilli.tsv"
    data = create_dataframe(path, num_rows=100000)
    small_df, large_df = data_preprocessing(data)
    ml_final = ML_df_create(small_df, large_df)
    train_data, test_data = split_data(ml_final,percentage=0.7)
    cv_model, nb_model = train_and_evaluate_naive_bayes_with_cv("multinomial", train_data, test_data, "/students/2021-2022/master/DaanSteur_DSLS/nb_multinomial_cv_results.txt")
    save_spark_model(nb_model, '/students/2021-2022/master/DaanSteur_DSLS/nb_model_multinomial.pkl')
    save_spark_model(cv_model, '/students/2021-2022/master/DaanSteur_DSLS/cv_model_multinomial.pkl')
    
    cv_model, nb_model = train_and_evaluate_naive_bayes_with_cv("gaussian", train_data, test_data, "/students/2021-2022/master/DaanSteur_DSLS/nb_gaussian_cv_results.txt")
    save_spark_model(nb_model, '/students/2021-2022/master/DaanSteur_DSLS/nb_model_gaussian.pkl')
    save_spark_model(cv_model, '/students/2021-2022/master/DaanSteur_DSLS/cv_model_gaussian.pkl')
    
    save_dataframe_as_csv(train_data, '/students/2021-2022/master/DaanSteur_DSLS/train_data.csv')
    save_dataframe_as_csv(test_data, '/students/2021-2022/master/DaanSteur_DSLS/test_data.csv')



if __name__ == '__main__':
    main()