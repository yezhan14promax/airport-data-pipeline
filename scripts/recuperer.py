from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler as va
from pyspark.ml.regression import LinearRegression as linreg
from pyspark.ml.regression import DecisionTreeRegressor as dtr
from pyspark.ml.evaluation import RegressionEvaluator as re



'''1. Ingérer les données en tant que pyspark dataframe object'''
def jiazai():
# Créer une session Spark
    sc = SparkContext("local", "airports")
    spark = SparkSession(sc)
# Charger le jeu de données csv 
    df_air = spark.read \
    .option('header', 'True') \
    .option('delimiter', ',') \
    .option('inferSchema', 'True') \
    .csv('./data/airports.csv')
    df_fli = spark.read \
    .option('header', 'True') \
    .option('delimiter', ',') \
    .option('inferSchema', 'True') \
    .csv('./data/flights.csv')
    df_raw = spark.read \
    .option('header', 'True') \
    .option('delimiter', ',') \
    .option('inferSchema', 'True') \
    .csv('./data/raw-flight-data.csv')
    return df_air,df_fli,df_raw
df_air,df_fli,df_raw=jiazai()
