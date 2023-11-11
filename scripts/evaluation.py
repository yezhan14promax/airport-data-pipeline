from formation import *
lr1_eva_r=xianxinghuigui(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[1]
lr2_eva_r=xianxinghuigui(df_fli,['DepDelay'],'ArrDelay')[1]
dt1_eva_r=jueceshu(df_fli,[c for c in df_fli.columns if c not in ['Carrier','OriginAirportID','DestAirportID']],'ArrDelay')[1]
dt2_eva_r=jueceshu(df_fli,['DepDelay'],'ArrDelay')[1]

if __name__ == '__main__':
    print(lr1_eva_r,lr2_eva_r,dt1_eva_r,dt2_eva_r)