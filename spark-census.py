from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sql = SQLContext(sc)

lines = sc.textFile("s3://bigdata-rutvik/clean.csv")
parts = lines.map(lambda l: l.split(','))

df = sql.createDataFrame(parts, ['age','workclass','occupation','education','marital','relationship','race','sex','capital_gain','capital_loss','hours_week','native_country','income'])

df.show(15)


df_num =df.select(df.age.cast("int"), df.workclass.cast("string"), df.education.cast("string"), df.marital.cast("string"), df.relationship.cast("string"), df.race.cast("string"), df.sex.cast("string"), df.capital_gain.cast("int"), df.capital_loss.cast("int"), df.hours_week.cast("int"), df.income.cast("string"))

df = df_num

df = df.na.drop()

df_selected = df.select("age", "workclass", "education", "marital", "relationship", "race", "sex", "hours_week", "income")

df = df_selected

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

wc_indexer = StringIndexer(inputCol = "workclass", outputCol = "wc_index")
wc_encoder = OneHotEncoder(inputCol = "wc_index", outputCol = "wc_fact")

ed_indexer = StringIndexer(inputCol = "education", outputCol = "ed_index")
ed_encoder = OneHotEncoder(inputCol = "ed_index", outputCol = "ed_fact")

marital_indexer = StringIndexer(inputCol = "marital", outputCol = "marital_index")
marital_encoder = OneHotEncoder(inputCol = "marital_index", outputCol = "marital_fact")

r_indexer = StringIndexer(inputCol = "relationship", outputCol = "r_index")
r_encoder = OneHotEncoder(inputCol = "r_index", outputCol = "r_fact")

race_indexer = StringIndexer(inputCol = "race", outputCol = "race_index")
race_encoder = OneHotEncoder(inputCol = "race_index", outputCol = "race_fact")

sex_indexer = StringIndexer(inputCol = "sex", outputCol = "sex_index")
sex_encoder = OneHotEncoder(inputCol = "sex_index", outputCol = "sex_fact")

vec_assembler = VectorAssembler(inputCols = ["age", "wc_fact", "ed_fact", "marital_fact", "r_fact", "race_fact", "sex_fact", "hours_week"], outputCol = "features")

income_indexer = StringIndexer(inputCol = "income", outputCol = "label")
# income_encoder = OneHotEncoder(inputCol = "income_index", outputCol = "income_fact")

from pyspark.ml import Pipeline

df_pipe = Pipeline(stages = [wc_indexer, wc_encoder, ed_indexer, ed_encoder, marital_indexer, marital_encoder, r_indexer, r_encoder, race_indexer, race_encoder, sex_indexer, sex_encoder, vec_assembler, income_indexer])

piped_df = df_pipe.fit(df).transform(df)

training_df, testing_df = piped_df.randomSplit([.7, .3])

##########################
######	Linear SVM	######
##########################
from pyspark.ml.classification import LinearSVC
svm = LinearSVC(labelCol = "label", featuresCol = "features", maxIter = 100)

svm_model = svm.fit(training_df)
pred = svm_model.transform(testing_df)

# rawPred = pred.rawPrediction

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

svmAccuracy = evaluator.evaluate(pred)
print(svmAccuracy)


##########################
######	XGB	##############
##########################


from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(labelCol = "label", featuresCol = "features")
gbtModel = gbt.fit(training_df)

pred2 = gbtModel.transform(testing_df)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

gbtAccuracy = evaluator.evaluate(pred2);
print(gbtAccuracy)


##########################
######	Logistic #########
##########################

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(labelCol="label",featuresCol="features", maxIter=100)
lrModel = lr.fit(training_df)

pred3 = lrModel.transform(testing_df)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
lrAccuracy = evaluator.evaluate(pred3)

print(lrAccuracy)

##########################
######	Random Forest ####
##########################

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(featuresCol="features",labelCol="label",predictionCol="Prediction_RF",seed=3)

rfModel = rf.fit(training_df)
pred4 = rfModel.transform(testing_df)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
rfAccuracy = evaluator.evaluate(pred4);

print(rfAccuracy)

##########################

#print('Accuracy: \nSVM:\t', svmAccuracy, '\nGBT:\t', gbtAccuracy, '\nLogistic:\t', lrAccuracy, '\nRandom Forest:\t', rfAccuracy, '\n\n')

print("Accuracy")
print("SVM: ", svmAccuracy)
print("GBT: ", gbtAccuracy)
print("Logistic: ", lrAccuracy)
print("Random Forest: ", rfAccuracy)

