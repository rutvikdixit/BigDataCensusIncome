-- Creating Hive table with adult.data

CREATE EXTERNAL TABLE IF NOT EXISTS Census 
(age string,
workclass string,
fnlwgt string,
education string,
education_num string,
marital_status string,
occupation string,
relationship string,
race string,
sex string,
capital_gain string,
capital_loss string,
hours_per_week string,
native_country string,
income string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION 's3://bigdata-cdc-kshitij/census_final.csv/';

hive -e 'select * from Census' | sed 's/[\t]/,/g' > census_full.csv

aws s3 cp census_full.csv s3://bigdata-cdc-kshitij/