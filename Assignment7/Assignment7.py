#!/usr/bin/env python3
"""
Title: Replacement Assignment
Author: Daan Steur
Date: 05/10/2023
Description: This script parses a PubMed XML file using PySpark and processes the data to obtain key information from research articles.
Usage: 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, length, avg
from pyspark.sql.types import ArrayType, StringType
import csv








if __name__ == "__main__":
    main()