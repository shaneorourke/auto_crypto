from pybit.usdt_perpetual import HTTP
import pandas as pd
import datetime as dt
import ta

def get_timestamp(lookback:int):
    dt_now = dt.datetime.now()
    dt_now = dt_now + dt.timedelta(days=-lookback)
    startTime = dt.datetime(dt_now.year, dt_now.month, dt_now.day)
    startTime = str(int(startTime.timestamp()))
    return startTime

def int_session(symbol_pair):
    int_session = HTTP("https://api.bybit.com", api_key="", api_secret="", request_timeout=30)
    try:
        int_session.set_leverage(symbol=f'{symbol_pair}',buy_leverage=1,sell_leverage=1)
    except Exception as e:
        error = e
    finally:
        return int_session

def apply_technicals(df:object):
    df['FastSMA'] = df.close.rolling(7).mean()
    df['SlowSMA'] = df.close.rolling(25).mean()
    df['%K'] = ta.momentum.stoch(df.high,df.low,df.close,window=14,smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.close,window=14)
    df['macd'] = ta.trend.macd_diff(df.close)
    df.dropna(inplace=True)

def get_bybit_bars(starttime:str,symbol_pair:str,session_interval:int,session:object):
    response = session.query_kline(symbol=symbol_pair,interval=session_interval,from_time=starttime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at,unit='s') + pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time,unit='s') + pd.DateOffset(hours=1)
    apply_technicals(df)
    return df

if __name__ == '__main__':
    lookback_days = 6
    pair = 'SOLUSDT'
    session_interval = 60
    session = int_session(pair)
    bars = get_bybit_bars(get_timestamp(lookback_days),pair,session_interval,session)
    print(bars)