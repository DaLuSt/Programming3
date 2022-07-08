from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.ml.feature import HashingTF
import numpy as np
import os

class DataLoader:
    def __init__(self):
        print('Creating output folder')
        if os.path.exists('output'):
            pass
        else:
            os.makedirs('output')

    def pyspark_session(self, host_location):
        """
        Creates and returns spark session object
        """
        print('Starting session')
        sc = SparkContext(host_location)  # Create spark context
        spark = SparkSession(sc)  # Create session
        return spark

    def file_loader(self, path, delim, spark_obj, schema):
        print('Loading in file')
        data = spark_obj.read.options(delimiter=delim).option("header", "False").csv(path, schema=schema)
        print('File loaded')
        return data

    def cleaner(self, data, maxsize=0.9):
        # Take out missing values
        print('Removing entries with missing values')
        df_filt = data.filter(data["InterPro_annotations_accession"] != '-')\
            .filter(data['Signature_description'] != '-')
        print('Getting relative sizes')
        df_sizes = df_filt.withColumn('perc',
                                      abs(data.Start_location - data.Stop_location) / data.Sequence_length)\
            .sort('perc')
        # Remove large features as those should be predicted by the small features
        df_sizes = df_sizes.filter(df_sizes.perc < maxsize)
        df_sizes.show()
        return df_sizes

    def get_array_from_df(self, data, column_list):
        return np.array(data.select(column_list).collect())

    def words_to_array(self, data, column):
        df_array = data.select(split(data[column], " ").alias(f'{column}_array'))
        df_copy = df_array.select('*')
        print('Using HashingTF')
        ht = HashingTF(inputCol=f'{column}_array', outputCol=f'{column}_features')
        result = ht.transform(df_copy)
        result.show()
        print('Getting into array form')
        x_array = np.array(result.select(f'{column}_features').collect())
        x_reshaped = x_array.reshape(x_array.shape[0], -1)
        return x_reshaped


if __name__ == "__main__":
    DataLoader = DataLoader()  # Instanciate object
    spk = DataLoader.pyspark_session('local[16]')  # start session
    # load data
    #     path = '/data/dataprocessing/interproscan/all_bacilli.tsv'
    file_path = '/data/dataprocessing/interproscan/all_bacilli.tsv'
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
        StructField("Pathways_annotations", StringType(), True)])

    all_cols = ['Protein_accession', 'Sequence_MD5_digest', 'Sequence_length', 'Analysis',
                'Signature_accession', 'Signature_description', 'Start_location', 'Stop_location',
                'Score', 'Status', 'Date', 'InterPro_annotations_accession', 'InterPro_annotations_description',
                'GO_annotations', 'Pathways_annotations']

    num_cols = ['Sequence_length', 'Start_location', 'Stop_location', 'Score']

    df = DataLoader.file_loader(file_path, '\t', spk, schema)
    df_munged = DataLoader.cleaner(df, maxsize=0.9)
    numeric_cols = DataLoader.get_array_from_df(df_munged, num_cols)

    sign_desc_array = DataLoader.words_to_array(df_munged, 'Signature_description')
    print(sign_desc_array)
    IP_annot_desc_array = DataLoader.words_to_array(df_munged, 'InterPro_annotations_description')
    print(IP_annot_desc_array)
    final_array = np.concatenate([numeric_cols, sign_desc_array, IP_annot_desc_array], axis=1)
    print(final_array)
    print('Closing spark session')
    spk.sparkContext.stop()