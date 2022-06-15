"""
Assignment 5 Programming 3
Anwser bacterial annotation questions by using spark
Data Sciences for Life Sciences
Author: Daan Steur
Date:13/06/2022
"""   

# packages
from email import header
from enum import unique

from sympy import bottom_up
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os, sys


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



class InspectSparkDF:

    def __init__(self, spark_df):
        self.df = spark_df

    # 1. How many distinct protein annotations are found in the dataset? I.e. how many distinct InterPRO numbers are there?
    def unique_pro_annots(self):
        unique_pro_annots = self.df.select('interPRO_accession').distinct().count()
        return unique_pro_annots
# df[column].unique()

    # 2. How many annotations does a protein have on average?
    def avg_pro_annots(self):

        avg = self.df.select('interPRO_accession').distinct().count() / self.df.count()
        return avg

     # 3. What is the most common GO Term found?
    def max_go_term(self):
        max = self.df.select('GO_annots').distinct().count().max()
        return max
# argmax(df[column]) / # df[column].sum().max()
        pass

    # 4. What is the average size of an InterPRO feature found in the dataset?
    def avg_feature_size(self):

        
        
# df[column].mean() > COMBINE WITH Q2 INTO 1 FUNCITON,S THEN CREATE AN EXTRA FUNCTION
# TO ANSWER THE ASSIGNMENT QUESTIONS
        pass

    # 5. What is the top 10 most common InterPRO features?
    def top_10_common_features(self):
        top_10 = self.df.select('interPRO_accession').distinct().count().top(10)
        return top_10
# df.value_counts(ascending=True)[0:10]
        

     # 6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
    def top_10_percent(self):
   
        top_10_percent = self.df.select('interPRO_accession').distinct().count().top(10) / self.df.count()
        return top_10_percent
        
# top10percent = len(df)*0.1 > top10percent_df = df.iloc[length > top10percent] > df.value_counts(ascending=True)[0:10]

    # 7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
    def common_words(self):
        common_words = self.df.select('interPRO_accession').distinct().count().top(10)
        return common_words

# df.value_counts(ascending=True)[0:10]

    # 8. And the top 10 least common?
    def top_10_least_common_features(self):
        least_common_words = self.df.select('interPRO_accession').distinct().count().bottom(10)
        return least_common_words


# df.value_counts(ascending=False)[0:10] / df.value_counts(ascending=True)[-10:]
        pass

    # 9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
    def top_10_largest_inter_feat(self):
        pass
# use top10percent_df > df.value_counts(ascending=True)[0:10]


    def cor_coef(self):
        # 10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?
        coef = self.df.select('seq_length').corr(self.df.select('interPRO_accession').distinct().count())
        return coef
# coeffcor = (stuff)^2 / (len(df) * (df.seq_length.std()^2 * df.interPRO_accession.std()^2))


# Your output should be a CSV file with 3 columns;
# 1. in the first column the question number
# 2. in the second column the answer(s) to the question
# 3. in the third column the output of the scheduler's physical plan (using the `.explain()` PySpark method) as a string

# NB3: Use the `csv` Python module to make the CSV file in "excel" format; this makes it easier to deal with the different answer types (number, string, list etc.)
    
    def create_outpuy(self):
        # create a dataframe with the question number, the answer and the output of the scheduler's physical plan
        data = [1, self.unique_pro_annots(), self.df.select('interPRO_accession').distinct().count().explain(),
                2, self.avg_pro_annots(), self.df.select('interPRO_accession').distinct().count().explain(),
                3, self.max_go_term(), self.df.select('GO_annots').distinct().count().explain(),
                4, self.avg_feature_size(), self.df.select('interPRO_accession').distinct().count().explain(),
                5, self.top_10_common_features(), self.df.select('interPRO_accession').distinct().count().explain(),
                6, self.top_10_percent(), self.df.select('interPRO_accession').distinct().count().explain(),
                7, self.common_words(), self.df.select('interPRO_accession').distinct().count().explain(),
                8, self.top_10_least_common_features(), self.df.select('interPRO_accession').distinct().count().explain(),
                9, self.top_10_largest_inter_feat(), self.df.select('interPRO_accession').distinct().count().explain(),
                10, self.cor_coef(), self.df.select('seq_length').corr(self.df.select('interPRO_accession').distinct().count()).explain()]
    
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(data)
        # turn spark data frame into cvs
        df.write.csv('output/output.csv')
        
        
    def create_dir():
        if not os.path.exists('output'):
            os.makedirs('output')        
        
        
    
    
    def main():
        pass
        
 
        
        
    

if __name__ == "__main__":
    path = '/data/dataprocessing/interproscan/all_bacilli.tsv'
    InspectSparkDF.create_dir()
    df = spark_df(path)
    inspect = InspectSparkDF(df)
    inspect.create_outpuy()
    # print(inspect.unique_pro_annots())
    print(inspect.avg_pro_annots().explain())

   
    # print(df_test)

    




# column names
# ['protein_accession',
#  'seq_MD5_digest',
#   'seq_length',
#    'analysis_method',
#     'sig_accession',
#      'sig_description',
#       'start_location',
#        'stop_location',
#         'score',
#          'status_match',
#           'date',
#            'interPRO_accession',
#             'interPRO_description',
#              'GO_annots',
#               'pathway_annots']