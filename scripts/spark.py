from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType, TimestampType


if __name__ == '__main__':
# Créer une session Spark
    spark = SparkSession.builder.\
        appName('tp').\
        getOrCreate()
    sc = spark.sparkContext
    
# Charger le jeu de données csv 
    df_air = spark.read.csv('./data/airports.csv', sep=',', header=True)
    df_fli = spark.read.csv('./data/flights.csv', sep=',', header=True)
    df_raw=spark.read.csv('./data/raw-flight-data.csv', sep=',', header=True)
    
# Afficher le schéma du dataframe
def dayingbiao():
    return df_air.show(5),df_fli.show(5),df_raw.show(5)
#dayingbiao()

# Afficher le nombre d'enregistrements
def dayingshuliang():
    return  print("aéroports: ", df_air.count()),\
            print("flights: ", df_fli.count()),\
            print("row flights: ", df_raw.count())
#dayingshuliang()

# Afficher les colonnes du dataframe
def dayingbiaotou():
    
    return print("airports dataset: ", df_air.columns),\
            print("lights dataset: ", df_fli.columns),\
            print("raw-flight-data dataset: ", df_raw.columns)
#dayingbiaotou()

def zhunshi(zhonglei):
    """ Cette fonction prend en entrée un type de retard ('dep', 'arr' ou 'all') et retourne le nombre de vols sans retard 
    et la proportion de ces vols par rapport au nombre total de vols.
    Args:zhonglei (str): Le type de retard ('dep', 'arr' ou 'all')
    Returns:tuple: Le nombre de vols sans retard et la proportion de ces vols par rapport au nombre total de vols."""
    if zhonglei == 'dep':
        # Vols de départ sans retard, et la proportion.
        count = df_fli.filter(df_fli['DepDelay'] == 0).count()
        prop = count / df_fli.count()
        return count, prop

    elif zhonglei == 'arr':
        # Vols d'arrivée sans retard, et la proportion.
        count = df_fli.filter(df_fli['ArrDelay'] == 0).count()
        prop = count / df_fli.count()
        return count, prop

    elif zhonglei == 'all':
        # Vols sans retard de départ et d'arrivée, et la proportion.
        count = df_fli.filter((df_fli['DepDelay'] == 0) & (df_fli['ArrDelay'] == 0)).count()
        prop = count / df_fli.count()
        return count, prop

    else:
        return "'dep', 'arr', ou 'all'？"

#print(zhunshi('dep'))
#print(zhunshi('arr'))
#print(zhunshi('all'))

#Trouver la moyenne
#La première moyenne est le retard de départ, la seconde le retard d'arrivée et la troisième le retard total.
def pingjunzhi(df):
    avg1=df.select(F.avg('DepDelay')).collect()[0][0]
    avg2=df.select(F.avg('ArrDelay')).collect()[0][0]
    df1=df.withColumn('totaldelay',F.col('DepDelay')+F.col('ArrDelay'))
    avg3=df1.select(F.avg('totaldelay')).collect()[0][0]
    return avg1,avg2,avg3
print(pingjunzhi(df_fli))

def hangban_info(start_airport_id, end_airport_id):
    """Cette fonction prend deux identifiants d'aéroport en entrée et renvoie toutes les informations de vol entre ces deux aéroports.
    Args:L'identifiant de l'aéroport de départ.L'identifiant de l'aéroport de destination.
    Returns:Un dataframe contenant toutes les informations de vol entre les deux aéroports."""
    df = df_raw.filter((df_raw['OriginAirportID'] == start_airport_id) & (df_raw['DestAirportID'] == end_airport_id))
    # drop() returns a new dataframe, so we need to assign it to df
    df = df.drop('OriginAirportID', 'DestAirportID') 
    df.show() 
    return df
#hangban_info(11433,13303)

# Afficher les informations de vol pour un aéroport donné
def diqv_info(location_type):
    """ Cette fonction prend en entrée un type de localisation ('city' ou 'state') et retourne le nombre d'aéroports et la liste des noms d'aéroports."""
    if location_type == 'city':
        return df_air.groupBy('City').agg(F.count('Name').alias('num_air'),\
               F.collect_list('Name').alias('name_air'))
    elif location_type == 'state':
        return df_air.groupBy('State').agg(F.count('Name').alias('num_air'),\
               F.collect_list('Name').alias('name_air'))
    else:
        return "'city' ou 'state'？"
    
#diqv_info().show()

# Afficher les aéroports qui ont le plus de vols
def queshizhi(df):
    queshi_cols = []
    for i in df.columns:
        if df.where(F.col(i).isNull()).count() > 0:
            queshi_cols.append(i)
    return queshi_cols
#print(queshizhi(df_raw))

# Afficher les aéroports qui ont le plus de vols
def dayingqueshizhi(df):
    for i in df.columns:
        print(i, df.where(F.col(i).isNull()).count())
#dayingqueshizhi(df_raw)

# Supprimer les aéroports qui ont le plus de vols
def shanchuqueshizhi(df):
    for i in queshizhi(df):
        df = df.dropna(subset=[i])
    return df
df_raw=shanchuqueshizhi(df_raw)
df_fli=shanchuqueshizhi(df_fli)
df_air=shanchuqueshizhi(df_air)


# Supprimer les doublons
df_raw.dropDuplicates()
df_fli.dropDuplicates()
df_air.dropDuplicates()

# Afficher les types de données
def print_dtypes(df):
    for column, dtype in df.dtypes:
        print(f'{column}: {dtype}')
        

#Convertir le type de données
#print_dtypes(df_air)
df_air = df_air.withColumn('airport_id', df_air['airport_id'].cast(IntegerType()))
#print_dtypes(df_air)

#print_dtypes(df_fli)
for i in df_fli.columns:
    if i != 'Carrier':
        df_fli = df_fli.withColumn(i, df_fli[i].cast(IntegerType()))
#print_dtypes(df_fli)
        
#print_dtypes(df_raw)
for i in df_raw.columns:
    if i != 'Carrier':
        df_raw = df_raw.withColumn(i, df_raw[i].cast(IntegerType()))
#print_dtypes(df_raw)

