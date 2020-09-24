import talib


def create_Indicators(data):
    dfs = data
    
    df = dfs.copy()
    
    
    
    df['ADX'] = talib.ADX(df.high, df.low, df.close, timeperiod=14)  
    df['ADXR'] = talib.ADXR(df.high, df.low, df.close, timeperiod=14)
    df['APO'] = talib.APO(df.close, fastperiod=12 , slowperiod=26, matype=0)
    df['AROONOSC'] = talib.AROONOSC(df.high,df.low,timeperiod=14)
    df['BOP'] = talib.BOP(df.open,df.high,df.low,df.close) #  (Close price – Open price) / (High price – Low price) 
    df["CCI"] = talib.CCI(df.high, df.low, df.close, timeperiod=14)
    df["CMO"] = talib.CMO(df.close, timeperiod=14)
    df["DX"] = talib.DX(df.high, df.low, df.close, timeperiod=14)
    #df["MFI"] = talib.MFI(df.high, df.low, df.close, df.volume, timeperiod=14).pct_change()
    df["MINUS_DI"] = talib.MINUS_DI(df.high, df.low, df.close, timeperiod=14)
    df["MINUS_DM"] = talib.MINUS_DM(df.high, df.low, timeperiod=14)
    df["MOM"] = talib.MOM(df.close, timeperiod=10)
    df["PLUS_DI"] = talib.PLUS_DI(df.high, df.low, df.close, timeperiod=14)
    df["PLUS_DM"] = talib.PLUS_DM(df.high, df.low, timeperiod=14)
    #X_corr["PPO"] = PPO(df.close, fastperiod=12, sdf.lowperiod=26, matype=0)
    df["ROC"] = talib.ROC(df.close, timeperiod=10)
    df["ROCP"] = talib.ROCP(df.close, timeperiod=10)
    df["ROCR"] = talib.ROCR(df.close, timeperiod=10)
    df["ROCR100"] = talib.ROCR100(df.close, timeperiod=10)
    df["RSI"] = talib.RSI(df.close, timeperiod=14)
    #sdf.lowk, sdf.lowd = STOCH(df.high, df.low, df.close, fastk_period=5, sdf.lowk_period=3, sdf.lowk_matype=0, sdf.lowd_period=3, sdf.lowd_matype=0)
    df["TRIX"] = talib.TRIX(df.close, timeperiod=30)
    df["ULTOSC"] = talib.ULTOSC(df.high, df.low, df.close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
    df["WILLR"] = talib.WILLR(df.high, df.low, df.close, timeperiod=14)
 
    return df