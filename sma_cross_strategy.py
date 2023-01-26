from pybit.usdt_perpetual import HTTP
import pandas as pd
import datetime as dt
import ta
import time
import os
import math

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

def apply_technicals(df:object,session_interval:int):
    df['FastSMA'] = df.close.rolling(7).mean()
    df['SlowSMA'] = df.close.rolling(25).mean()
    df['%K'] = ta.momentum.stoch(df.high,df.low,df.close,window=14,smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.close,window=14)
    df['macd'] = ta.trend.macd_diff(df.close)
    df['force_index'] = ta.volume.force_index(df.close,df.volume,window=25,fillna=False)
    df.dropna(inplace=True)

def get_bybit_bars(starttime:str,symbol_pair:str,session_interval:int,session:object):
    response = session.query_kline(symbol=symbol_pair,interval=session_interval,from_time=starttime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at,unit='s') #+ pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time,unit='s') #+ pd.DateOffset(hours=1)
    apply_technicals(df,session_interval)
    df.sort_index(ascending=False,inplace=True)
    return df

def stoploss_sleep_time_calculator(open_time:str,interval:int):
    dt_time_now = dt.datetime.strptime(str(dt.datetime.now()),'%Y-%m-%d %H:%M:%S.%f')
    open_at_time = dt.datetime.strptime(str(open_time)[:-3],'%Y-%m-%dT%H:%M:%S.%f')
    difference = dt_time_now - open_at_time
    seconds_in_day = 24 * 60 * 60
    min_delay = divmod(difference.days * seconds_in_day + difference.seconds, 60)[0]
    sec_delay = divmod(difference.days * seconds_in_day + difference.seconds, 60)[1]
    delay = (interval - (min_delay + sec_delay/60) + 30/60) * 60
    return delay


def truncate(number:float, decimal_places:int):
    if decimal_places < 0:
        return 0
    elif decimal_places == 0:
        return round(number)
    elif decimal_places > 0:
        factor = 10.0 ** decimal_places
        return math.trunc(number * factor) / factor

def get_quantity(current_price:float):
    wallet_usdt = float(30.00)
    qty = float(wallet_usdt / current_price)
    if current_price < float(10000):
        decimals = 0 if len(str(round(current_price)))-1 == 0 else len(str(round(current_price)))-1
    else:
        decimals = 0 if len(str(round(current_price)))-1 == 0 else len(str(round(current_price)))-2
    return truncate(qty,decimals)

def place_order(order_dict:dict,header:bool,symbol_pair):
    df = pd.DataFrame([order_dict]).to_csv('order_log.csv',index=False,mode='a',header=header)
    side = pd.DataFrame([{'symbol_pair':symbol_pair,'order':'OPEN'}]).to_csv(f'order_status/{symbol_pair}_order_status.csv',mode='w')

def get_order_dict(trading_sybol:str,side:str,quantity:float,current_price:float,tp:float,sl:float):
    return {'trading_symbol':trading_sybol,'side':side,'order_type':'Market','quantity':quantity,'order_price':current_price,'time_in_force':'ImmediateOrCancel','reduce_only':False,'close_on_trigger':False,'take_profit':tp,'stop_loss':sl,'timestamp':datetime_now()}

def dict_format_info(trading_sybol:str,interval:int,order_status:str,last_cross:str,side:str,fastsma:float,slowsma:float,current_price:float,qty:float,take_profit:float,stop_loss:float,volume:float,force_index:float):
    return {'trading_sybol':trading_sybol,'interval':interval,'order_status':order_status,'last_cross':last_cross,'side':side,'fastsma':fastsma,'slowsma':slowsma,'current_price':current_price,'take_profit':take_profit,'stop_loss':stop_loss,'quantity':qty,'volume':volume,'force_index':force_index,'timestamp':datetime_now()}

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

def sma_cross_last_cross(dataframe:object):
    dataframe.drop(index=dataframe.index[0], axis=0, inplace=True)
    for index, row in dataframe.iterrows():
        fastsma = row['FastSMA']
        slowsma = row['SlowSMA']
        if fastsma > slowsma:
            return 'up'
        if fastsma < slowsma:
            return 'down'

def take_profit_stop_loss(side:str,current_price:float,tp_percentage:float,sl_percentage:float):
    if side == 'SHORT':
        tp = current_price - (current_price * tp_percentage)
        sl = current_price + (current_price * sl_percentage)
        return tp, sl
    if side == 'LONG':
        tp = current_price + (current_price * tp_percentage)
        sl = current_price - (current_price * sl_percentage)
        return tp, sl

def sma_cross_strategy(all_bars:object,candle:object,trading_sybol:str,tp_percentage:float,sl_percentage:float,interval:int):
    last_cross, side, fastsma, slowsma, current_price, volume, qty, force_index, stoploss_sleep_time = get_candle_details(all_bars,candle,interval)
    tp = float(0)
    sl = float(0)
    order_status = 'NOT OPEN'
    if last_cross == 'up':
        if fastsma < slowsma:
            if force_index < 0:
                side = 'SHORT'
                tp, sl = take_profit_stop_loss(side,current_price,tp_percentage,sl_percentage)
    if last_cross == 'down':
        if fastsma > slowsma:
            if force_index > 0:
                side = 'LONG'
                tp, sl = take_profit_stop_loss(side,current_price,tp_percentage,sl_percentage)
    if side in ['SHORT','LONG']:
        order_dict = get_order_dict(trading_sybol,side,qty,current_price,tp,sl)
        order_log_file = 'order_log.csv'
        if os.path.exists(order_log_file):
            header = False
        else:
            header = True
        ## -- Place Order -- ##
        place_order(order_dict,header,trading_sybol)
        order_status = 'OPEN'
    return dict_format_info(trading_sybol,interval,order_status,last_cross,side,fastsma,slowsma,current_price,qty,tp,sl,volume,force_index)

def check_open_order(symbol_pair:str):
    filename = f'order_status/{symbol_pair}_order_status.csv'
    if not os.path.exists(filename):
        df = pd.DataFrame([{'symbol_pair':symbol_pair,'order':'CLOSED'}]).to_csv(f'{filename}',mode='w')
    df = pd.read_csv(filename)        
    return df['order'].values[:1][0]

def close_order(symbol_pair:str):
    filename = f'order_status/{symbol_pair}_order_status.csv'
    df = pd.DataFrame([{'symbol_pair':symbol_pair,'order':'CLOSED'}]).to_csv(f'{filename}',mode='w')

def get_candle_details(df_history:object,df_current:object,interval:int):
    last_cross = sma_cross_last_cross(df_history)
    side = ''
    fastsma = df_current['FastSMA'].values[:1][0]
    slowsma = df_current['SlowSMA'].values[:1][0]
    current_price = float(df_current['close'].values[:1][0])
    volume = df_current['volume'].values[:1][0]
    force_index = df_current['force_index'].values[:1][0]
    open_time = df_current['open_time'].values[:1][0]
    stoploss_sleep_time = stoploss_sleep_time_calculator(open_time,interval)
    qty = get_quantity(current_price)
    return last_cross, side, fastsma, slowsma, current_price, volume, qty, force_index, stoploss_sleep_time


def exit_strategy_stoploss(symbol:str,dataframe:object,history_df:object,tp_percentage:float,sl_percentage:float,interval:int):
    last_cross, side, fastsma, slowsma, current_price, volume, qty, force_index, stoploss_sleep_time = get_candle_details(history_df,dataframe,interval)
    side, order_price, take_profit, stop_loss = get_order_details(symbol)
    close = False
    close_side = ''
    order_status = 'OPEN'
    if side == 'LONG':
        if current_price > take_profit:
            close_side = 'LONG_CLOSED_TP'
            close = True
        if current_price < stop_loss:
            close_side = 'LONG_CLOSED_SL'
            close = True
    if side == 'SHORT':
        if current_price < take_profit:
            close_side = 'SHORT_CLOSED_TP'
            close = True
        if current_price > stop_loss:
            close_side = 'SHORT_CLOSED_SL'
            close = True
    if close:
        ## -- Close Order -- ##
        order_dict = get_order_dict(symbol,close_side,qty,order_price,take_profit,stop_loss)
        df = pd.DataFrame([order_dict]).to_csv('order_log.csv',index=False,mode='a',header=False)
        close_order(symbol)
        order_status = close_side
        if close_side in ['SHORT_CLOSED_TP','LONG_CLOSED_TP']:
            ## -- TP - ORDER AGAIN -- ##
            tp, sl = take_profit_stop_loss(side,current_price,tp_percentage,sl_percentage)
            order_dict = get_order_dict(symbol,close_side,qty,order_price,tp,sl)
            place_order(order_dict,True,symbol)
            order_status = 'OPEN'
        else:
            #print(f'Stop loss sleep - {stoploss_sleep_time*60} minutes')
            time.sleep(stoploss_sleep_time)
    return dict_format_info(symbol,interval,order_status,last_cross,side,fastsma,slowsma,current_price,qty,take_profit,stop_loss,volume,force_index)

def if_order_open(pair_list:list):
    not_open = []
    open = []
    for currency in pair_list:
        order_status = check_open_order(currency)
        if order_status == 'OPEN':
            return [currency]
        else:
            not_open.append(currency)
    return not_open
        
def main_funtion():
    if not os.path.exists('order_status'):
        os.mkdir('order_status')
    pairs = ['BTCUSDT','ETHUSDT','SOLUSDT','ADAUSDT','DOGEUSDT','DOTUSDT']
    for pair in if_order_open(pairs):
        session_interval = 30 #minutes
        if session_interval > 60:
            lookback_days = 3
            take_prof_perc = 0.02
            stop_loss_perc = 0.01
        elif session_interval <= 60 and session_interval > 30:
            lookback_days = 3
            take_prof_perc = 0.02
            stop_loss_perc = 0.01
        elif session_interval <= 30 and session_interval >= 15:
            lookback_days = 1
            take_prof_perc = 0.01
            stop_loss_perc = 0.005
        elif session_interval < 15:
            lookback_days = 0.25
            take_prof_perc = 0.005
            stop_loss_perc = 0.0025
        order_status = check_open_order(pair)
        session = int_session(pair)
        bars = get_bybit_bars(get_timestamp(lookback_days),pair,session_interval,session)
        latest_candle = pd.DataFrame(bars.iloc[0:1])
        if order_status != 'OPEN':
            current_details = sma_cross_strategy(bars,latest_candle,pair,take_prof_perc,stop_loss_perc,session_interval)
            print(current_details)
            if True in [True if value == 'OPEN' else False for value in current_details.values()]:
                break
        else:
            current_details = exit_strategy_stoploss(pair,latest_candle,bars,take_prof_perc,stop_loss_perc,session_interval)
            print(current_details)

if __name__ == '__main__':
    main_funtion()
