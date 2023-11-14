from recuperer import *
import matplotlib.pyplot as plt
import seaborn as sns

'''2. Effectuer des analyses des données tout en utilisant les opérations à disposition'''
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
#print(pingjunzhi(df_fli))

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

#visualisation des données
def keshihua():
    # Convertir
    pandas_df = df_fli.toPandas()
    # 
    sns.distplot(pandas_df['ArrDelay'])
    plt.show()
    # 
    for col in pandas_df.columns:
        if col != 'ArrDelay':
            sns.scatterplot(x='ArrDelay', y=col, data=pandas_df)
            plt.show()
#keshihua()