import pandas as pd
import os

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

def read_cronLog_file_into_list(file_name:str):
    df = pd.read_csv(file_name,header=None)
    rows_dict_list = []
    for row in df.index:
        symbol = str(df[0].values[row]).replace("{'trading_sybol': ",'')
        interval = str(df[1].values[row]).replace(" 'interval': ",'')
        order_status = str(df[2].values[row]).replace(" 'order_status': ",'')
        last_cross = str(df[3].values[row]).replace(" 'last_cross': ",'')
        side = str(df[4].values[row]).replace(" 'side': ",'')
        fastsma = str(df[5].values[row]).replace(" 'fastsma': ",'')
        slowsma = str(df[6].values[row]).replace(" 'slowsma': ",'')
        current_price = str(df[7].values[row]).replace(" 'current_price': ",'')
        take_profit = str(df[8].values[row]).replace(" 'take_profit': ",'')
        stop_loss = str(df[9].values[row]).replace(" 'stop_loss': ",'')
        quantity = str(df[10].values[row]).replace(" 'quantity': ",'')
        timestamp = str(df[11].values[row]).replace(" 'timestamp': ",'')
        rows_dict_list.append({'symbol':symbol,'interval':interval,'order_status':order_status,'last_cross':last_cross,'side':side,'fastsma':fastsma,'slowsma':slowsma,'current_price':current_price,'take_profit':take_profit,'stop_loss':stop_loss,'quantity':quantity,'timestamp':timestamp})

    return rows_dict_list

def read_orderLog_file_into_list(file_name:str):
    df = pd.read_csv(file_name,header=0)
    df_dict = df.to_dict()
    return df_dict

def read_files_into_df(filename:str):
    files_row_list = []
    for file in os.listdir():
        if f'{filename}' in file:
            if filename == 'auto_crypto_cron_script_log_':
                files_row_list = read_cronLog_file_into_list(file)
            if filename == 'order_log':
                files_row_list = read_orderLog_file_into_list(file)
    realData = pd.DataFrame.from_dict(files_row_list, orient='columns')
    return realData
    
def get_order_log_live_data():
    order_log = read_files_into_df('order_log')

if __name__ == '__main__':
    cron_log_df = read_files_into_df('auto_crypto_cron_script_log_')
    #print(cron_log_df)
    order_log = read_files_into_df('order_log')
    session_interval = 60
    lookback_days = 3
    pairs = order_log['trading_symbol'].unique()
    total_profit = float(0.00)
    for pair in pairs:
        LONG_CLOSE_TP_QUANTITY = sum(order_log['quantity'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_TP')])
        LONG_CLOSED_TP = sum(order_log['take_profit'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_TP')])
        LONG_CLOSED_TP_ORDER_PRICE = sum(order_log['order_price'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_TP')])
        LONG_CLOSED_TP_PROFIT = (LONG_CLOSED_TP - LONG_CLOSED_TP_ORDER_PRICE) * LONG_CLOSE_TP_QUANTITY

        LONG_CLOSE_SL_QUANTITY = sum(order_log['quantity'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_SL')])
        LONG_CLOSED_SL = sum(order_log['take_profit'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_SL')])
        LONG_CLOSED_SL_ORDER_PRICE = sum(order_log['order_price'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'LONG_CLOSED_SL')])
        LONG_CLOSED_SL_LOSS = (LONG_CLOSED_SL_ORDER_PRICE - LONG_CLOSED_SL) * LONG_CLOSE_SL_QUANTITY

        SHORT_CLOSE_TP_QUANTITY = sum(order_log['quantity'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_TP')])
        SHORT_CLOSED_TP = sum(order_log['take_profit'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_TP')])
        SHORT_CLOSED_ORDER_PRICE = sum(order_log['order_price'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_TP')])
        SHORT_CLOSED_PROFIT = (SHORT_CLOSED_ORDER_PRICE - SHORT_CLOSED_TP) * SHORT_CLOSE_TP_QUANTITY

        SHORT_CLOSE_SL_QUANTITY = sum(order_log['quantity'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_SL')])
        SHORT_CLOSED_SL = sum(order_log['take_profit'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_SL')])
        SHORT_CLOSED_SL_ORDER_PRICE = sum(order_log['order_price'][(order_log['trading_symbol'] == pair) & (order_log['side'] == 'SHORT_CLOSED_SL')])
        SHORT_CLOSED_SL_LOSS = (SHORT_CLOSED_SL - SHORT_CLOSED_SL_ORDER_PRICE) * SHORT_CLOSE_SL_QUANTITY

        total_profit = total_profit + LONG_CLOSED_TP_PROFIT + LONG_CLOSED_SL_LOSS + SHORT_CLOSED_PROFIT + SHORT_CLOSED_SL_LOSS
        print(pair)
        print(f'Long Profit:{round(LONG_CLOSED_TP_PROFIT,2)}')
        print(f'Short Profit:{round(SHORT_CLOSED_PROFIT,2)}')
        print(f'Long Losses:{round(LONG_CLOSED_SL_LOSS,2)}')
        print(f'Short Losses:{round(SHORT_CLOSED_PROFIT,2)}')
        print()

    print(f'Total Profit:{round(total_profit,2)}')

