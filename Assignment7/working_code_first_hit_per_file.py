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
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session(app_name):
    """
    Creates a Spark session.

    Args:
        app_name (str): Name for the Spark application.

    Returns:
        SparkSession: A Spark session, or None if an error occurs.
    """
    try:
        # Create a Spark session with the provided app name
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark
    except Exception as e:
        # Handle any exceptions that may occur during Spark session creation
        print(f"An error occurred while creating the Spark session: {str(e)}")
        return None

def create_articles_dataframe(spark, input_directory, file_limit=None, articles_per_file=10):
    try:
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

        # Initialize a counter for articles found in each file
        articles_count = 0

        # Initialize a counter for processed files
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
                    if articles_count >= articles_per_file:
                        break  # Stop processing this file if we've found enough articles

                    pubmed_id = article.find(".//PMID").text
                    first_author = article.find(".//AuthorList/Author[1]/LastName").text
                    last_author = article.find(".//AuthorList/Author[last()]/LastName").text
                    pub_year = article.find(".//PubDate/Year").text if article.find(".//PubDate/Year") is not None else "Unknown"
                    title = article.find(".//ArticleTitle").text
                    journal_title = article.find(".//Journal/Title").text
                    abstract = article.find(".//AbstractText")
                    abstract_text = abstract.text if abstract is not None else ""
                    
                    # Extract references if available
                    references = [ref.text for ref in article.findall(".//PubmedData/ReferenceList/Reference/ArticleIdList/ArticleId[@IdType='pubmed']")]

                    # Append the data to the DataFrame
                    articles_df = articles_df.union(
                        spark.createDataFrame([(pubmed_id, first_author, last_author, pub_year, title, journal_title, abstract_text, references)], 
                                             schema=schema)
                    )

                    articles_count += 1

                # Check if the file limit has been reached
                file_count += 1
                if file_limit is not None and file_count >= file_limit:
                    break

        return articles_df
    except Exception as e:
        # Handle any exceptions that may occur during DataFrame creation
        print(f"An error occurred while creating the DataFrame: {str(e)}")
        return None


def save_dataframe_as_csv(dataframe, output_path):
    """
    Saves a PySpark DataFrame as a CSV file.

    Args:
        dataframe (DataFrame): The DataFrame to be saved.
        output_path (str): Path to the output CSV file.
    """
    try:
        dataframe.write.csv(output_path, mode="overwrite", header=True)
        print(f"DataFrame saved as CSV to {output_path}")
    except Exception as e:
        print(f"Error saving DataFrame as CSV: {str(e)}")


def create_and_save_questions_dataframes(articles_df):
    """
    Creates and saves analysis DataFrames based on the provided article DataFrame.

    Args:
        articles_df (DataFrame): The PySpark DataFrame containing article data.
    """
    try:
        # Create the DataFrame to answer questions
        author_counts = articles_df.groupBy("FirstAuthor").count().alias("ArticleCountPerAuthor")
        year_counts = articles_df.groupBy("Year").count().alias("ArticleCountPerYear")
        abstract_lengths = articles_df.groupBy().agg(avg(col("Abstract").cast("string").cast(IntegerType())).alias("AvgAbstractLength"))
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


def main(input_directory, file_limit=None):
    try:
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
        print("Parsed data saved to CSV files.")

        # Create and save analysis DataFrames
        create_and_save_questions_dataframes(articles_df)
        print("Analysis DataFrames created and saved.")

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
    input_dir = "/data/datasets/NCBI/PubMed/"

    # # Check if the file limit is provided as a command line argument
    # if len(sys.argv) > 1:
    #     try:
    #         file_limit = int(sys.argv[1])
    #     except ValueError:
    #         print("Invalid file limit. Please provide an integer.")
    #         sys.exit(1)
    # else:
    #     # If no file limit is provided, set it to None to parse all files
    #     file_limit = None

    file_limit = 1
    main(input_dir, file_limit)
