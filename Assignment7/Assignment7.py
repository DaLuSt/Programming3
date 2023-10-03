#!/usr/bin/env python3
"""
Title: Replacement Assignment XML parser
Author: Daan Steur
Date: 05/10/2023
Description: This script parses a PubMed XML file using PySpark and processes the data to obtain key information from research articles.
Usage: python3 Assignment7.py [file_limit] (file_limit is optional)
"""

import os
import sys
import pandas as pd
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session(app_name, num_executors=16, executor_cores=16, executor_memory='128g', driver_memory='128g'):
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.executor.instances", num_executors) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.driver.memory", driver_memory) \
            .getOrCreate()
        
        return spark
    except Exception as e:
        print(f"An error occurred while creating the Spark session: {str(e)}")
        return None

def create_articles_dataframe(spark, input_directory, file_limit=None):
    try:
        schema = StructType([
            StructField("PubMedID", StringType(), nullable=True),
            StructField("FirstAuthor", StringType(), nullable=True),
            StructField("LastAuthor", StringType(), nullable=True),
            StructField("Year", StringType(), nullable=True),
            StructField("Title", StringType(), nullable=True),
            StructField("JournalTitle", StringType(), nullable=True),
            StructField("Abstract", StringType(), nullable=True),
            StructField("AbstractLength", IntegerType(), nullable=True),
            StructField("References", StringType(), nullable=True)
        ])

        articles_df = spark.createDataFrame([], schema=schema)

        file_count = 0

        for filename in os.listdir(input_directory):
            if filename.endswith(".xml"):
                filepath = os.path.join(input_directory, filename)
                
                tree = ET.parse(filepath)
                root = tree.getroot()

                article_data = []

                for article in root.findall(".//PubmedArticle"):
                    pubmed_id = article.find(".//PMID")
                    first_author = article.find(".//AuthorList/Author[1]/LastName")
                    last_author = article.find(".//AuthorList/Author[last()]/LastName")
                    pub_year = article.find(".//PubDate/Year")
                    title = article.find(".//ArticleTitle")
                    journal_title = article.find(".//Journal/Title")
                    abstract = article.find(".//Abstract/AbstractText")
                    
                    pubmed_id_text = pubmed_id.text if pubmed_id is not None else "Unknown"
                    first_author_text = first_author.text if first_author is not None else "Unknown"
                    last_author_text = last_author.text if last_author is not None else "Unknown"
                    pub_year_text = pub_year.text if pub_year is not None else "Unknown"
                    title_text = title.text if title is not None else "Unknown"
                    journal_title_text = journal_title.text if journal_title is not None else "Unknown"
                    abstract_text = abstract.text if abstract is not None else ""
                    
                    # Calculate abstract length
                    abstract_length = len(abstract_text)

                    references = [ref.text for ref in article.findall(".//PubmedData/ReferenceList/Reference/ArticleIdList/ArticleId[@IdType='pubmed']")]

                    article_data.append((pubmed_id_text, first_author_text, last_author_text, pub_year_text, title_text, journal_title_text, abstract_text, abstract_length, references))

                articles_df = articles_df.union(
                    spark.createDataFrame(article_data, schema=schema)
                )

                file_count += 1

                if file_limit is not None and file_count >= file_limit:
                    break
                
        return articles_df
    except Exception as e:
        print(f"An error occurred while creating the DataFrame: {str(e)}")
        return None

def save_dataframe_as_csv(dataframe, output_path):
    try:
        dataframe.write.csv(output_path, mode="overwrite", header=True)
        print(f"DataFrame saved as CSV to {output_path}")
    except Exception as e:
        print(f"Error saving DataFrame as CSV: {str(e)}")
        
        
def create_and_save_analysis_dataframes(articles_df):
    """
    Creates and saves analysis DataFrames based on the provided article DataFrame.

    Args:
        articles_df (DataFrame): The PySpark DataFrame containing article data.
    """
    try:
        # Create the DataFrame to answer questions
        author_counts = articles_df.groupBy("FirstAuthor").count().alias("ArticleCountPerAuthor")
        year_counts = articles_df.groupBy("Year").count().alias("ArticleCountPerYear")
        
        # Calculate min, max, and average of AbstractLength column
        abstract_lengths = articles_df.agg(
            min(col("AbstractLength")).alias("MinAbstractLength"),
            max(col("AbstractLength")).alias("MaxAbstractLength"),
            avg(col("AbstractLength")).alias("AvgAbstractLength")
        )
        
        journal_year_counts = articles_df.groupBy("JournalTitle", "Year").count().alias("ArticleCountPerJournalTitlePerYear")

        # Save the DataFrames to CSV files
        save_dataframe_as_csv(author_counts, "output/author_counts")
        save_dataframe_as_csv(year_counts, "output/year_counts")
        save_dataframe_as_csv(abstract_lengths, "output/abstract_lengths")
        save_dataframe_as_csv(journal_year_counts, "output/journal_year_counts")

        print("Analysis DataFrames created and saved successfully.")
    except Exception as e:
        print(f"Error creating and saving analysis DataFrames: {str(e)}")

    
def combine_and_delete_files(input_folder, output_file, combined_csv_filename):
    """
    Combines multiple CSV files into a single CSV file and deletes all files in a folder except the combined CSV file.

    Args:
        input_folder (str): Path to the folder containing CSV files to be combined.
        output_file (str): Path to the output combined CSV file.
        combined_csv_filename (str): Name of the combined CSV file to be retained.
    """
    try:
        # Get a list of all CSV files in the input folder
        csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

        if not csv_files:
            print("No CSV files found in the input folder.")
            return

        # Initialize an empty DataFrame to store the combined data
        combined_df = pd.DataFrame()

        # Iterate over each CSV file and concatenate them
        for csv_file in csv_files:
            csv_path = os.path.join(input_folder, csv_file)
            try:
                df = pd.read_csv(csv_path)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except pd.errors.ParserError:
                print(f"Skipping file {csv_file} due to parsing error.")

        # Save the combined DataFrame to the output CSV file
        combined_df.to_csv(output_file, index=False)
        print(f"Combined data saved to {output_file}")

        # Delete all files in the folder except the combined CSV file
        for filename in os.listdir(input_folder):
            if filename != combined_csv_filename:
                file_path = os.path.join(input_folder, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        print(f"Deleted all files except {combined_csv_filename}")
    except Exception as e:
        print(f"Error combining and deleting files: {str(e)}")

def csv_name_change_logfile_delete(folder, new_name, confirm_logfile_delete="y"):
    """
    Rename CSV files in the specified folder to the new name and optionally delete other files.

    Args:
        folder (str): The path to the folder containing CSV files.
        new_name (str): The new name to assign to CSV files.
        confirm_logfile_delete (str, optional): A confirmation prompt for deleting other files.
            Defaults to "y". Set to "n" to skip deletion.

    Returns:
        None
    """
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            old_path = os.path.join(folder, filename)
            new_path = os.path.join(folder, new_name)
            os.rename(old_path, new_path)
            print(f"Renamed {old_path} to {new_path}")

    # Ask the user for confirmation before deleting files
    if confirm_logfile_delete == "y":
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) and filename != new_name:
                os.remove(file_path)
        print(f"Deleted all files except {new_name}")
    else:
        print("Files were not deleted.")


def main(input_directory, file_limit = 1):
    try:
        # create output folder if it does not exist
        if not os.path.exists("output"):
            os.makedirs("output")

        # Define folder paths and filenames
        parsed_data_folder = "output/parsed_data"
        output_file = os.path.join(parsed_data_folder, "combined_data.csv")
        combined_csv_filename = "combined_data.csv"

        # Initialize a Spark session
        spark = create_spark_session("PubMedXMLParser")

        # Create articles DataFrame
        articles_df = create_articles_dataframe(spark, input_directory, file_limit)

        # Save the entire DataFrame to a single CSV file
        save_dataframe_as_csv(articles_df, parsed_data_folder)

        # Create and save analysis DataFrames
        create_and_save_analysis_dataframes(articles_df)

        # Combine output CSV files and delete all files except the combined CSV file
        combine_and_delete_files(parsed_data_folder, output_file, combined_csv_filename)
        print(f"Combined data saved to {output_file}.")
        
        # Rename the CSV files and delete all files except the renamed CSV files
        csv_name_change_logfile_delete("output/abstract_lengths", "abstract_lengths.csv", confirm_logfile_delete="y")
        csv_name_change_logfile_delete("output/author_counts", "author_counts.csv", confirm_logfile_delete="y")
        csv_name_change_logfile_delete("output/journal_year_counts", "journal_year_counts.csv", confirm_logfile_delete="y")
        csv_name_change_logfile_delete("output/year_counts", "year_counts.csv", confirm_logfile_delete="y")

        # Stop the Spark session
        spark.stop()
    except Exception as e:
        print(f"An error occurred in the main function: {str(e)}")

if __name__ == "__main__":

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

    main("/data/datasets/NCBI/PubMed/", file_limit)
