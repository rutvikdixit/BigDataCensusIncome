# BigDataCensusIncome
Big Data Analysis of Census Income Dataset using AWS EMR, S3, PySpark, Pig and Hive

Data Source: https://archive.ics.uci.edu/ml/datasets/Census+Income
The goal of the project is to predict if the income is greater than or less than 50K annually.

Raw Data Files: adult.data (32K records), adult.test (16K records)

Uploaded adult.data and adult.test into S3 cluster

Data Cleaning:
Used census_cleaning.pig to remove missing records, and merge the two sets into one as well as clean up a few other inconsistencies in the data.
Commands: 
pig s3://bucket-name/census_cleaning.pig

Used census_hive.hql to give structure to the data, and export as clean csv file.
Commands:
hive -f s3://bucket-name/census_hive.hql
hive -e 'select * from Census' | sed 's/[\t]/,/g' > census_full.csv
aws s3 cp census_full.csv s3://bucket-name/Processed-data/

Running Spark ML:
Used pyspark package in a python script to fit 4 machine learning models on the data, using a 70-30 split.
Tried Logistic Regression, SVM, Random Forest and Gradient Boosted Trees.

Achieved Accuracy:
Logistic Regression:    87.49%\n
SVM:                    86.39%\n
Random Forest:          85.54%\n
Gradient Boosted Trees: 87.89%\n

Command:
spark-submit spark-census.py
