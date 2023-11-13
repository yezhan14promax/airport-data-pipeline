from nettoyage import *

'''4. Concevoir un modèle de Machine Learning, en utilisant sparkML qui vous permettra de 
prédire si un vol a eu du retard ou pas (entrainement + Test)'''
#régression linéaire
def xianxinghuigui(df,f_col,labcol):
    '''Modèle de régression linéaire
    param df: DataFrame contenant les données d'entraînement et de test
    param f_col: nom de la colonne contenant les caractéristiques
    param labcol: nom de la colonne contenant les étiquettes
    return: DataFrame contenant les prédictions
    '''
    a = va(inputCols=f_col, outputCol='features')
    df= a.transform(df)
    train, test = df.randomSplit([0.8, 0.2])
    lr= linreg(featuresCol='features', labelCol=labcol)
    lr_mod = lr.fit(train)
    lr_eva = lr_mod.evaluate(test)
    lr_eva_r = lr_eva.rootMeanSquaredError
    predictions = lr_mod.transform(test)
    return lr_mod,lr_eva_r,predictions

#arbre de décision
def jueceshu(df,f_col,labcol):
    """
    param df: dataframe contenant les données d'entraînement et de test
    param f_col: liste de noms de colonnes de fonctionnalités
    param labcol: nom de la colonne cible
    return: dataframe contenant les prédictions sur les données de test
    """
    a=va(inputCols=f_col,outputCol='features')
    df=a.transform(df)
    train, test = df.randomSplit([0.8, 0.2])
    dt=dtr(featuresCol='features',labelCol=labcol)
    dt_mod=dt.fit(train)
    predictions = dt_mod.transform(test)
    dt_eva=re(predictionCol='prediction', labelCol=labcol, metricName='rmse')
    rmse=dt_eva.evaluate(predictions)
    return dt_mod,rmse,predictions

if __name__ == '__main__':
    #avec tous les colonnes suaf carrier
    lr1=xianxinghuigui(df_fli,[c for c in df_fli.columns if c not in ['Carrier']],'ArrDelay')[2].show(5)
    #sans aéroport
    lr2=xianxinghuigui(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[2].show(5)
    #que DepDelay
    lr3=xianxinghuigui(df_fli,['DepDelay'],'ArrDelay')[2].show(5)
    #avec tous les colonnes suaf carrier
    dt1=jueceshu(df_fli,[c for c in df_fli.columns if c not in ['Carrier']],'ArrDelay')[2].show(5)
    #sans aéroport
    dt2=jueceshu(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[2].show(5)
    #que DepDelay
    dt3=jueceshu(df_fli,['DepDelay'],'ArrDelay')[2].show(5)
