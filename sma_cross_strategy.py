from pybit.usdt_perpetual import HTTP
import pandas as pd
import datetime as dt
import ta
import time
import os

def datetime_now():
    now = dt.datetime.now()
    stamp = f'{now.day}/{now.month}/{now.year} {now.hour}:{now.minute}:{now.second}.{now.microsecond}'
    return stamp

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
    df.sort_index(ascending=False,inplace=True)
    return df

def get_order_dict(trading_sybol:str,side:str,quantity:float,current_price:float,tp:float,sl:float):
    return {'trading_symbol':trading_sybol,'side':side,'order_type':'Market','quantity':quantity,'order_price':current_price,'time_in_force':'ImmediateOrCancel','reduce_only':False,'close_on_trigger':False,'take_profit':tp,'stop_loss':sl,'timestamp':datetime_now()}

def sma_cross_last_cross(dataframe:object):
    trend = ''
    cross = ''
    for index, row in dataframe.iterrows():
        fastsma = row['FastSMA']
        slowsma = row['SlowSMA']
        if trend == '':
            if fastsma > slowsma:
                    trend = 'up'
            if fastsma < slowsma:
                    trend = 'down'
        if cross == '':
            if trend == 'down':
                if fastsma > slowsma:
                    return 'down' #This is the inverse of the truth - as working backwards
            if trend == 'up':
                if fastsma < slowsma:
                    return 'up' #This is the inverse of the truth - as working backwards

def sma_cross_strategy(dataframe:object,trading_sybol:str,quantity:int,tp_percentage:float,sl_percentage:float):
    last_cross = sma_cross_last_cross(dataframe)
    side = ''
    fastsma = dataframe['FastSMA'].values[:1]
    slowsma = dataframe['SlowSMA'].values[:1]
    current_price = float(dataframe['close'].values[:1][0])
    if last_cross == 'up':
        if fastsma < slowsma:
            side = 'SHORT'
            tp = current_price - (current_price * take_prof_perc)
            sl = current_price + (current_price * stop_loss_perc)
    if last_cross == 'down':
        if fastsma > slowsma:
            side = 'LONG'
            tp = current_price + (current_price * take_prof_perc)
            sl = current_price - (current_price * stop_loss_perc)
    if side in ['SHORT','LONG']:
        order_dict = get_order_dict(trading_sybol,side,quantity,current_price,tp,sl)
        print(order_dict)
        order_log_file = 'order_log.csv'
        if os.path.exists(order_log_file):
            header = False
        else:
            header = True
        df = pd.DataFrame([order_dict]).to_csv('order_log.csv',index=False,mode='a',header=header)
        side = pd.DataFrame([{'symbol_pair':pair,'order':'OPEN'}]).to_csv(f'{pair}_order_status.csv',mode='w')

def check_open_order(symbol_pair:str):
    filename = f'{symbol_pair}_order_status.csv'
    if not os.path.exists(filename):
        df = pd.DataFrame([{'symbol_pair':pair,'order':'CLOSED'}]).to_csv(f'{pair}_order_status.csv',mode='w')
    df = pd.read_csv(filename)        
    return df['order'].values[:1][0]

def get_order_details(symbol:str):
    df = pd.read_csv(f'order_log.csv')
    filter = df['trading_symbol']==symbol
    df2 = df.where(filter, inplace = False)
    df2 = df2.dropna()
    df2 = df2.sort_values(by=['timestamp'],ascending=False)
    side = df2['side'].values[:1][0]
    order_price = df2['order_price'].values[:1][0]
    take_profit = df2['take_profit'].values[:1][0]
    stop_loss = df2['stop_loss'].values[:1][0]
    return side, order_price, take_profit, stop_loss

def exist_strategy_stoploss(symbol:str,dataframe:object):
    side, order_price, take_profit, stop_loss = get_order_details(symbol)
    current_price = dataframe['close'].values[:1]
    if side == 'LONG':
        if current_price > take_profit:
            side = 'LONG_CLOSED_TP'
        if current_price < stop_loss:
            side = 'LONG_CLOSED_SL'
        order_dict = get_order_dict(symbol,side,quantity,current_price,take_profit,stop_loss)
        df = pd.DataFrame([order_dict]).to_csv('order_log.csv',index=False,mode='a',header=False)
    if side == 'SHORT':
        if current_price < take_profit:
            side = 'SHORT_CLOSED_TP'
        if current_price > stop_loss:
            side = 'SHORT_CLOSED_SL'
        order_dict = get_order_dict(symbol,side,quantity,current_price,take_profit,stop_loss)
        df = pd.DataFrame([order_dict]).to_csv('order_log.csv',index=False,mode='a',header=False)

if __name__ == '__main__':
    while True:
        time.sleep(60)
        lookback_days = 6
        pair = 'BTCUSDT'
        quantity = 0.001
        session_interval = 5
        take_prof_perc = 0.02
        stop_loss_perc = 0.005
        order_status = check_open_order(pair)
        session = int_session(pair)
        bars = get_bybit_bars(get_timestamp(lookback_days),pair,session_interval,session)
        if order_status != 'OPEN':
            sma_cross_strategy(bars,pair,quantity,take_prof_perc,stop_loss_perc)
        else:
            exist_strategy_stoploss(pair,bars)
