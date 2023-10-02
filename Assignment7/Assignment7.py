#!/usr/bin/env python3
"""
Title: Replacement Assignment
Author: Daan Steur
Date: 05/10/2023
Description: This script parses a PubMed XML file using PySpark and processes the data to obtain key information from research articles.
Usage: 
"""

import os
import sys
import pandas as pd
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_articles_dataframe(spark, input_directory, file_limit=None):
    # Define the schema for the articles DataFrame
    schema = StructType([
        StructField("PubMedID", StringType(), nullable=True),
        StructField("FirstAuthor", StringType(), nullable=True),
        StructField("LastAuthor", StringType(), nullable=True),
        StructField("Year", StringType(), nullable=True),
        StructField("Title", StringType(), nullable=True),
        StructField("JournalTitle", StringType(), nullable=True),
        StructField("Abstract", StringType(), nullable=True),
        StructField("References", StringType(), nullable=True)
    ])

    # Create an empty DataFrame for articles with the defined schema
    articles_df = spark.createDataFrame([], schema=schema)

    # Initialize a counter
    file_count = 0

    # Iterate over XML files in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".xml"):
            filepath = os.path.join(input_directory, filename)
            
            # Parse the XML file
            tree = ET.parse(filepath)
            root = tree.getroot()

            # Extract data from XML and create rows
            for article in root.findall(".//PubmedArticle"):
                pubmed_id = article.find(".//PMID").text
                first_author = article.find(".//AuthorList/Author[1]/LastName").text
                last_author = article.find(".//AuthorList/Author[last()]/LastName").text
                pub_year = article.find(".//PubDate/Year").text if article.find(".//PubDate/Year") is not None else "Unknown"
                title = article.find(".//ArticleTitle").text
                journal_title = article.find(".//Journal/Title").text
                abstract = article.find(".//Abstract/AbstractText")
                abstract_text = abstract.text if abstract is not None else ""
                
                # Extract references if available
                references = [ref.text for ref in article.findall(".//PubmedData/ReferenceList/Reference/ArticleIdList/ArticleId[@IdType='pubmed']")]

                # Append the data to the DataFrame
                articles_df = articles_df.union(
                    spark.createDataFrame([(pubmed_id, first_author, last_author, pub_year, title, journal_title, abstract_text, references)], 
                                         schema=schema)
                )

                # Increment the file count
                file_count += 1

                # Check if the file limit has been reached
                if file_limit is not None and file_count >= file_limit:
                    break
            
            # Check if the file limit has been reached
            if file_limit is not None and file_count >= file_limit:
                break

    return articles_df

def save_dataframe_as_csv(dataframe, output_path):
    dataframe.write.csv(output_path, mode="overwrite", header=True)

def create_and_save_analysis_dataframes(articles_df):
    # Create a second DataFrame to answer questions
    author_counts = articles_df.groupBy("FirstAuthor").count().alias("ArticleCountPerAuthor")
    year_counts = articles_df.groupBy("Year").count().alias("ArticleCountPerYear")
    abstract_lengths = articles_df.groupBy().agg(avg(col("Abstract").cast("string").cast(IntegerType())).alias("AvgAbstractLength"))
    journal_year_counts = articles_df.groupBy("JournalTitle", "Year").count().alias("ArticleCountPerJournalTitlePerYear")

    # Save the second DataFrames to CSV files
    save_dataframe_as_csv(author_counts, "output/author_counts.csv")
    save_dataframe_as_csv(year_counts, "output/year_counts.csv")
    save_dataframe_as_csv(abstract_lengths, "output/abstract_lengths.csv")
    save_dataframe_as_csv(journal_year_counts, "output/journal_year_counts.csv")
    
    
def combine_csv_files(input_folder, output_file):
    # Get a list of all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in the input folder.")
        return

    # Read the first CSV file to get the header
    first_csv = pd.read_csv(os.path.join(input_folder, csv_files[0]))
    header = list(first_csv.columns)

    # Create an empty DataFrame to store the combined data
    combined_df = pd.DataFrame(columns=header)

    # Append data from each CSV file to the combined DataFrame
    for csv_file in csv_files:
        csv_path = os.path.join(input_folder, csv_file)
        df = pd.read_csv(csv_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Save the combined DataFrame to the output CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined data saved to {output_file}")
    
def delete_files_except_combined_csv(folder_path, combined_csv_filename):
    try:
        for filename in os.listdir(folder_path):
            if filename != combined_csv_filename:
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        print(f"Deleted all files except {combined_csv_filename}")
    except Exception as e:
        print(f"An error occurred while deleting files: {str(e)}")


def main(input_directory, file_limit=None):
    # Initialize a Spark session
    spark = create_spark_session("PubMedParser")

    # Create articles DataFrame
    articles_df = create_articles_dataframe(spark, input_directory, file_limit)

    # Save the entire DataFrame to a single CSV file
    save_dataframe_as_csv(articles_df, "output/parsed_data.csv")

    # Create and save analysis DataFrames
    create_and_save_analysis_dataframes(articles_df)
    
    input_folder = "output/parsed_data.csv"
    output_file = "output/parsed_data.csv/combined_data.csv"
    combine_csv_files(input_folder, output_file)
    
    folder_path = "output/parsed_data.csv"
    combined_csv_filename = "combined_data.csv"
    delete_files_except_combined_csv(folder_path, combined_csv_filename)

    # Stop the Spark session
    spark.stop()
    

if __name__ == "__main__":
    input_dir = "/data/datasets/NCBI/PubMed/"
    
    # Check if the file limit is provided as a command line argument
    if len(sys.argv) > 1:
        try:
            file_limit = int(sys.argv[1])
        except ValueError:
            print("Invalid file limit. Please provide an integer.")
            sys.exit(1)
    else:
        # If no file limit is provided, set it to None to parse all files
        file_limit = None

    main(input_dir, file_limit)
