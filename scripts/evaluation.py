from formation import *
lr1_eva_r=xianxinghuigui(df_fli,[c for c in df_fli.columns if c not in ['Carrier']],'ArrDelay')[1]
'''lr2_eva_r=xianxinghuigui(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[1]
lr3_eva_r=xianxinghuigui(df_fli,['DepDelay'],'ArrDelay')[1]'''
dt1_eva_r=jueceshu(df_fli,[c for c in df_fli.columns if c not in ['Carrier']],'ArrDelay')[1]
'''dt2_eva_r=jueceshu(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[1]
dt3_eva_r=jueceshu(df_fli,['DepDelay'],'ArrDelay')[1]'''

if __name__ == '__main__':
    print("régression linéaire suaf carrier: ", lr1_eva_r, "\n"
        "arbre de décision suaf carrier: ", dt1_eva_r, "\n"
        )
