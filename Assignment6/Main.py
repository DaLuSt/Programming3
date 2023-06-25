"""
description: This program is used to create and train a machine learning model to predict the InterPro annotations of proteins.
Author: Daan Steur
date: 25-06-2023
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import abs, when, max
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, NaiveBayes, LinearSVC, MultilayerPerceptronClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import time
from pathlib import Path
from pyspark.ml import Model


def create_df(path):
    """
    Create a Spark DataFrame from a file path.
    
    Args:
        path (str): The correct file path.
    
    Returns:
        df (DataFrame): Spark DataFrame.
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
    
    spark = SparkSession.builder \
        .appName("InterPro") \
        .config("spark.driver.memory", "128g") \
        .config("spark.executor.memory", "128g") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .master("local[16]") \
        .getOrCreate()
    
    df = spark.read \
        .option("sep", "\t") \
        .option("header", "False") \
        .csv(path, schema=schema)
    
    return df




def data_preprocessing(df):
    """
    Perform data preprocessing on the input DataFrame.
    
    Args:
        df (DataFrame): Spark DataFrame.
    
    Returns:
        small_df (DataFrame): Processed DataFrame containing small InterPro_annotations_accession counts.
        large_df (DataFrame): Processed DataFrame containing selected large InterPro_annotations_accession data.
    """
    df = df.filter(df.InterPro_annotations_accession != "-") \
        .withColumn("Ratio", abs(df["Stop_location"] - df["Start_location"]) / df["Sequence_length"]) \
        .withColumn("Size", when((abs(df["Stop_location"] - df["Start_location"]) / df["Sequence_length"]) > 0.9, 1).otherwise(0))

    intersection = df.filter(df.Size == 0).select("Protein_accession").intersect(
        df.filter(df.Size == 1).select("Protein_accession"))
    intersection_df = intersection.join(df, ["Protein_accession"])

    small_df = intersection_df.filter(df.Size == 0).groupBy("Protein_accession").pivot("InterPro_annotations_accession").count()

    large_df = intersection_df.filter(df.Size == 1).groupby("Protein_accession").agg(max("Ratio").alias("Ratio"))
    large_df = large_df.join(intersection_df, ["Protein_accession", "Ratio"]).dropDuplicates(["Protein_accession"])

    columns_to_drop = ["Sequence_MD5_digest", "Analysis", "Signature_accession", "Signature_description",
                       "Score", "Status", "Date", "InterPro_annotations_description", "GO_annotations",
                       "Pathways_annotations", "Ratio", "Size", "Stop_location", "Start_location", "Sequence_length"]
    large_df = large_df.drop(*columns_to_drop)

    return small_df, large_df



def ML_df_create(small_df, large_df):
    """
    Create the correct ML dataframe for further analysis.
    
    Args:
        small_df (DataFrame): Spark DataFrame with preprocessed small InterPro_annotations_accession counts.
        large_df (DataFrame): Spark DataFrame with preprocessed large InterPro_annotations_accession data.
    
    Returns:
        ML_df (DataFrame): ML dataframe ready for analysis.
    """
    ML_df = large_df.join(small_df, ["Protein_accession"], "outer").fillna(0).drop("Protein_accession")

    label_indexer = StringIndexer(inputCol="InterPro_annotations_accession", outputCol="InterPro_index")
    input_columns = ML_df.columns[1:]

    assembler = VectorAssembler(inputCols=input_columns, outputCol="InterPro_features")

    pipeline = Pipeline(stages=[label_indexer, assembler])
    ML_final = pipeline.fit(ML_df).transform(ML_df)

    return ML_final



def split_data(ML_final, percentage=0.7):
    """
    Split the data into training and test sets.
    
    Args:
        ML_final (DataFrame): The ML dataframe.
        percentage (float): The percentage of data to be allocated for training (default: 0.7).
    
    Returns:
        trainData (DataFrame): Training data.
        testData (DataFrame): Test data.
    """
    (trainData, testData) = ML_final.randomSplit([percentage, 1 - percentage], seed=42)
    return trainData, testData



def ML_model(trainData, testData, model_type="rf"):
    """
    Create and train the specified ML model.
    
    Args:
        trainData (DataFrame): Spark DataFrame of the training data.
        testData (DataFrame): Spark DataFrame of the test data.
        model_type (str): Type of ML model to create (default: "rf").
            Available options: "rf" (random forest), "dtc" (decision tree), "nb" (naive bayes), 
            "lsvc" (linear SVC), "mlp" (multilayer perceptron), "gbt" (gradient boosted trees).
    
    Returns:
        model: Trained ML model.
    """
    if model_type == "rf":
        model = RandomForestClassifier(labelCol="InterPro_index",
                                       featuresCol="InterPro_features",
                                       predictionCol="prediction")
    elif model_type == "dtc":
        model = DecisionTreeClassifier(labelCol="InterPro_index",
                                       featuresCol="InterPro_features",
                                       predictionCol="prediction")
    elif model_type == "nb":
        model = NaiveBayes(modelType="multinomial",
                           labelCol="InterPro_index",
                           featuresCol="InterPro_features",
                           predictionCol="prediction")
    elif model_type == "lsvc":
        model = LinearSVC(labelCol="InterPro_index",
                          featuresCol="InterPro_features",
                          predictionCol="prediction")
    elif model_type == "mlp":
        model = MultilayerPerceptronClassifier(labelCol="InterPro_index",
                                               featuresCol="InterPro_features",
                                               predictionCol="prediction")
    elif model_type == "gbt":
        model = GBTClassifier(labelCol="InterPro_index",
                              featuresCol="InterPro_features",
                              predictionCol="prediction")
    else:
        raise ValueError("Invalid model_type. Available options: 'rf', 'dtc', 'nb', 'lsvc', 'mlp', 'gbt'")

    model = model.fit(trainData)
    predictions = model.transform(testData)

    evaluator = MulticlassClassificationEvaluator(labelCol='InterPro_index',
                                                  predictionCol='prediction',
                                                  metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy is {accuracy}.")

    return model


def tuning_model(trainData, testData, model_type="rf"):
    """
    Tune the specified ML model for better performance.
    
    Args:
        trainData (DataFrame): Spark DataFrame of the training data.
        testData (DataFrame): Spark DataFrame of the test data.
        model_type (str): Type of ML model to tune (default: "rf").
            Available options: "rf" (random forest), "dtc" (decision tree), "nb" (naive bayes), 
            "lsvc" (linear SVC), "mlp" (multilayer perceptron), "gbt" (gradient boosted trees).
    
    Returns:
        cvModel: Tuned ML model.
    """
    if model_type == "rf":
        model = RandomForestClassifier(labelCol="InterPro_index",
                                       featuresCol="InterPro_features",
                                       predictionCol="prediction",
                                       seed=42,
                                       maxMemoryInMB=256)
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.maxDepth, [5, 10, 20])
                     .addGrid(model.numTrees, [20, 100])
                     .build())
    elif model_type == "dtc":
        model = DecisionTreeClassifier(labelCol="InterPro_index",
                                       featuresCol="InterPro_features",
                                       predictionCol="prediction")
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.maxDepth, [2, 4, 6, 8, 10, 12])
                     .build())
    elif model_type == "nb":
        model = NaiveBayes(modelType="multinomial",
                           labelCol="InterPro_index",
                           featuresCol="InterPro_features",
                           predictionCol="prediction")
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.smoothing, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.0])
                     .build())
    elif model_type == "lsvc":
        model = LinearSVC(labelCol="InterPro_index",
                          featuresCol="InterPro_features",
                          predictionCol="prediction")
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.maxIter, [10, 100, 1000])
                     .addGrid(model.regParam, [0.0, 0.1, 0.2])
                     .build())
    elif model_type == "mlp":
        model = MultilayerPerceptronClassifier(labelCol="InterPro_index",
                                               featuresCol="InterPro_features",
                                               predictionCol="prediction",
                                               layers=[100, 50, 2])
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.blockSize, [128, 256])
                     .addGrid(model.maxIter, [50, 100])
                     .build())
    elif model_type == "gbt":
        model = GBTClassifier(labelCol="InterPro_index",
                              featuresCol="InterPro_features",
                              predictionCol="prediction")
        paramGrid = (ParamGridBuilder()
                     .addGrid(model.maxDepth, [5, 10, 20])
                     .addGrid(model.maxIter, [20, 100])
                     .build())
    else:
        raise ValueError("Invalid model_type. Available options: 'rf', 'dtc', 'nb', 'lsvc', 'mlp', 'gbt'")

    evaluator = MulticlassClassificationEvaluator(labelCol='InterPro_index',
                                                  predictionCol='prediction',
                                                  metricName='accuracy')

    cv = CrossValidator(estimator=model,
                        evaluator=evaluator,
                        estimatorParamMaps=paramGrid,
                        numFolds=5,
                        parallelism=10,
                        seed=42)

    cvModel = cv.fit(trainData)
    cvPredictions = cvModel.transform(testData)

    print(f"The model accuracy after tuning is {evaluator.evaluate(cvPredictions)}.")

    return cvModel


def save_file(df, path):
    """
    Save the Spark DataFrame to a file.
    
    Args:
        df (DataFrame): Spark DataFrame to be saved.
        path (str): The file path to save the DataFrame.
    
    Returns:
        str: Message indicating the file has been saved.
    """
    df.toPandas().set_index('InterPro_annotations_accession').to_pickle(path)
    return f"File already saved in {path}"



def save_model(model, path, model_type="normal"):
    """
    Save the Spark model to a file.

    Args:
        model (Model): Spark model to be saved.
        path (str): The file path to save the model.
        model_type (str): Type of model to save (default: "normal").
            Available options: "normal" (basic model), "best" (tuned model).

    Returns:
        str: Message indicating the model has been saved.
    """
    if model_type == "normal":
        model.write().overwrite().save(path)
    elif model_type == "best" and hasattr(model, "bestModel") and isinstance(model.bestModel, Model):
        model.bestModel.write().overwrite().save(path)
    else:
        raise ValueError("Invalid model_type or bestModel not found.")

    return f"{model_type} model already saved in {path}"


def main(model_type="nb"):
    """
    Run the model pipeline and save the models and data.

    Args:
        model_type (str): Type of model to run (default: "nb").
            Available options: "rf" (random forest), "dtc" (decision tree), "nb" (naive bayes).
    """
    start = time.time()
    df = create_df("/data/dataprocessing/interproscan/all_bacilli.tsv")
    small_df, large_df = data_preprocessing(df)
    ML_final = ML_df_create(small_df, large_df)
    trainData, testData = split_data(ML_final, 0.7)
    model = ML_model(trainData, testData, model_type)
    cvModel = tuning_model(trainData, testData, model_type)
    end = time.time()
    print(f"Run time is {(end-start)/60/60} hr.")

    trainData_path = "/students/2021-2022/master/DLS_DSLS/trainData.pkl"
    testData_path = "/students/2021-2022/master/DLS_DSLS/testData.pkl"
    model_path = f"/students/2021-2022/master/DLS_DSLS/{model_type}Model"
    best_model_path = f"/students/2021-2022/master/DLS_DSLS/{model_type}BestModel"

    if not Path(trainData_path).is_file():
        save_file(trainData, trainData_path)
    
    if not Path(testData_path).is_file():
        save_file(testData, testData_path)

    save_model(model, model_path, "normal")
    save_model(cvModel, best_model_path, "best")

if __name__ == '__main__':
    main("gbt")
