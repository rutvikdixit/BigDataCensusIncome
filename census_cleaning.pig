-- Importing train data from S3

raw_data_train = LOAD 's3://bigdata-rutvik/adult.data' using PigStorage(',') AS (age:int,workclass:chararray,fnlwgt:int,education:chararray,education_num:int,marital_status:chararray,occupation:chararray,relationship:chararray,race:chararray,sex:chararray,capital_gain:int,capital_loss:int,hours_per_week:int,native_country:chararray,income:chararray);

-- Importing test data from S3

raw_data_test = LOAD 's3://bigdata-rutvik/adult.test' using PigStorage(',') AS (age:int,workclass:chararray,fnlwgt:int,education:chararray,education_num:int,marital_status:chararray,occupation:chararray,relationship:chararray,race:chararray,sex:chararray,capital_gain:int,capital_loss:int,hours_per_week:int,native_country:chararray,income:chararray);

raw_data_test2 = FOREACH raw_data_test GENERATE age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,REPLACE(income, '<=[a-zA-Z0-9]*.$', '<=50K') AS income;

raw_data_test3 = FOREACH raw_data_test2 GENERATE age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,REPLACE(income, '>[a-zA-Z0-9]*.', '>50K') AS income;

-- Merging Data for Exploratory Data Analysis

census_full = UNION raw_data_train, raw_data_test3;

-- Removing ? records

census_full2 = FILTER census_full BY NOT(workclass MATCHES '.?.' OR education MATCHES '.?.' OR marital_status MATCHES '.?.' OR occupation MATCHES '.?.' OR relationship MATCHES '.?.' OR race MATCHES '.?.' OR sex MATCHES '.?.' OR native_country MATCHES '.?.' OR income MATCHES '.?.');  


-- Generating final dataset

census_final = FOREACH census_full2 GENERATE age,workclass,occupation,education,marital_status,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,income;

-- Exporting output to csv on S3

STORE census_final INTO 's3://bigdata-rutvik/censusattempt3';