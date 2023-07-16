import os
import sys
from bybit_secrets import API_KEY, API_SECRET
from pybit.unified_trading import HTTP
import ta
import logging
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

# Initialize the Bybit REST client
client = HTTP(
    testnet=False,
    api_key=API_KEY,
    api_secret=API_SECRET,
)

# Define the number of periods for the short and long SMAs
short_sma_period: int = 7
long_sma_period: int = 25

# Define the symbol and timeframe for the historical data
symbol: str = 'SOLUSDT'
timeframe: int = 60

# Define the risk percentage (e.g., 10%)
risk_percentage: float = 100.0

# Define the risk-reward ratio (e.g., 1:4)
risk_reward_ratio: float = 4.0

# Define the minimum time interval in minutes before placing a new order after a stop loss event
min_time_interval: int = timeframe

# Create a folder for log files if it doesn't exist
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)

# Configure logging
log_file = str(datetime.now().strftime("%Y-%m-%d")) + '.log'
log_file_path = os.path.join(log_folder, log_file)
logging.basicConfig(level=logging.INFO, filename=log_file_path, format='%(asctime)s - %(levelname)s - %(message)s')

def datetime_now() -> str:
    """
    Get the current timestamp as a formatted string.
    
    :return: The current timestamp as a formatted string.
    :rtype: str
    """
    now = datetime.now()
    stamp = now.strftime('%d/%m/%Y %H:%M:%S.%f')
    return stamp


def get_start_end_times() -> tuple:
    """
    Get the start and end times in milliseconds for fetching historical data.
    
    :return: A tuple containing the start time and end time in milliseconds.
    :rtype: tuple
    """
    endtime_date: datetime = datetime.now()  # current time
    starttime_date: datetime = endtime_date - timedelta(days=3)  # current time minus X
    starttime_date_ms: int = int(starttime_date.timestamp() * 1000)
    current_time_ms: int = int(endtime_date.timestamp() * 1000)
    return starttime_date_ms, current_time_ms


def apply_technicals(df: pd.DataFrame) -> None:
    """
    Apply technical indicators to the DataFrame.
    
    :param df: The DataFrame to apply the technical indicators to.
    :type df: pd.DataFrame
    """
    df['close'] = pd.to_numeric(df['close'])
    df['short_sma'] = ta.trend.sma_indicator(df['close'], window=short_sma_period)
    df['short_sma'] = pd.to_numeric(df['short_sma'])
    df['long_sma'] = ta.trend.sma_indicator(df['close'], window=long_sma_period)
    df['long_sma'] = pd.to_numeric(df['long_sma'])
    df['volume_sma'] = ta.trend.sma_indicator(df['volume'], window=30)
    df['volume'] = pd.to_numeric(df['volume'])
    df['volume_sma'] = pd.to_numeric(df['volume_sma'])

def fetch_historical_data(symbol: str, timeframe: int) -> pd.DataFrame:
    """
    Fetch historical OHLCV data for a symbol and timeframe using the REST API.
    
    :param symbol: The symbol to fetch the data for.
    :type symbol: str
    :param timeframe: The timeframe for the historical data.
    :type timeframe: int
    :return: The fetched historical OHLCV data as a DataFrame.
    :rtype: pd.DataFrame
    """
    result = client.get_kline(category="inverse",
                              symbol=symbol,
                              interval=timeframe,
                              start=get_start_end_times()[0],
                              end=get_start_end_times()[1],
                              timestamp=datetime_now())
    data = result['result']
    header = [value for key, value in data.items() if key != 'list']

    inner_list = []
    for row in data['list']:
        inner_list.append({'symbol': header[0], 'category': header[1], 'startTime': int(row[0]), 'openPrice': row[1],
                           'highPrice': row[2], 'lowPrice': row[3], 'close': row[4], 'volume': row[5],
                           'turnover': row[6]})

    kline = pd.DataFrame(inner_list)
    kline = kline.reindex(index=kline.index[::-1])
    kline.reset_index(inplace=True, drop=True)
    kline.startTime = pd.to_datetime(kline.startTime, unit='ms') + pd.DateOffset(hours=1)
    apply_technicals(kline)
    return kline

def check_sma_crossover(data: pd.DataFrame) -> str:
    """
    Check for a SMA crossover and return the signal ('buy' or 'sell').
    
    :param data: The historical data containing SMA values.
    :type data: pd.DataFrame
    :return: The signal indicating whether to 'buy', 'sell', or None.
    :rtype: str
    """
    if float(data['short_sma'].iloc[-2]) < float(data['long_sma'].iloc[-2]) and \
        float(data['short_sma'].iloc[-1]) > float(data['long_sma'].iloc[-1]) and \
        float(data['volume'].iloc[-1]) > float(data['volume_sma'].iloc[-1]):
        return 'Buy'

    elif float(data['short_sma'].iloc[-2]) > float(data['long_sma'].iloc[-2]) and \
            float(data['short_sma'].iloc[-1]) < float(data['long_sma'].iloc[-1]) and \
            float(data['volume'].iloc[-1]) > float(data['volume_sma'].iloc[-1]):
        return 'Sell'
    else:
        return None

def round_down_truncate(value: float) -> Decimal:
    """
    Round down a float value to two decimal places without rounding.
    
    :param value: The value to round down.
    :type value: float
    :return: The rounded down value.
    :rtype: Decimal
    """
    return Decimal(value).quantize(Decimal('0.00'), rounding=ROUND_DOWN)

def calculate_order_size(risk_percentage: float, usdt_held: float, current_price: float) -> Decimal:
    """
    Calculate the order size based on the risk percentage and current price.
    
    :param risk_percentage: The risk percentage.
    :type risk_percentage: float
    :param usdt_held: The amount of USDT held.
    :type usdt_held: float
    :param current_price: The current price of the symbol.
    :type current_price: float
    :return: The calculated order size.
    :rtype: Decimal
    """
    risk_amount = (float(risk_percentage) / float(100.00)) * float(usdt_held)
    order_size = float(risk_amount) / float(current_price)
    logging.info(f"Risk Amount: {risk_amount}")
    return round_down_truncate(order_size)

def calculate_stop_loss_take_profit(entry_price: float, risk_reward_ratio: float, side: str) -> tuple:
    """
    Calculate the stop loss and take profit levels based on the entry price and risk-reward ratio.
    
    :param entry_price: The entry price.
    :type entry_price: float
    :param risk_reward_ratio: The risk-reward ratio.
    :type risk_reward_ratio: float
    :param side: The side of the trade ('Buy' or 'Sell').
    :type side: str
    :return: A tuple containing the stop loss and take profit levels.
    :rtype: tuple
    """
    if side == 'Buy':
        stop_loss = float(entry_price) - (float(entry_price) * (float(risk_reward_ratio) * 0.01))
        take_profit = float(entry_price) + (float(entry_price) * (float(risk_reward_ratio) * 0.01))
    elif side == 'Sell':
        stop_loss = float(entry_price) + (float(entry_price) * (float(risk_reward_ratio) * 0.01))
        take_profit = float(entry_price) - (float(entry_price) * (float(risk_reward_ratio) * 0.01))
    return round_down_truncate(stop_loss), round_down_truncate(take_profit)

def execute_trade(side: str, risk_percentage: float, current_price: float) -> None:
    """
    Execute a trade based on the given side, risk percentage, and current price.
    
    :param side: The side of the trade ('Buy' or 'Sell').
    :type side: str
    :param risk_percentage: The risk percentage.
    :type risk_percentage: float
    :param current_price: The current price of the symbol.
    :type current_price: float
    """
    # Get USDT balance from the Bybit wallet
    wallet_balance = client.get_wallet_balance(accountType="CONTRACT",coin="USDT")
    usdt_held = wallet_balance['result']['list'][0]['coin'][0]['equity']
    logging.info(f"USDT Held: {usdt_held}")

    # Check if there is an open position
    open_positions = client.get_positions(category="linear", symbol=symbol)
    if float(open_positions['result']['list'][0]['size']) > float(0.0):
        logging.info("There is an open order. Skipping new order placement.")
        return
    
    last_order_time = pd.to_datetime(int(client.get_order_history(category="linear", symbol=symbol, limit=1)['result']['list'][0]['createdTime']),unit='ms') + pd.DateOffset(hours=1)
    
    # Check if the minimum time interval has passed since the last order
    if last_order_time:
        elapsed_time = datetime.now() - last_order_time
        if elapsed_time.total_seconds() / 60 < min_time_interval:
            logging.info(f"Not placing a new order. Minimum time interval not elapsed. Elapsed Time: {elapsed_time.total_seconds() / 60} minutes")
            return
        
    # Calculate order size based on risk percentage and current price
    order_size = calculate_order_size(risk_percentage, usdt_held, current_price)
    logging.info(f"Order Size: {order_size}")
    
    # Calculate stop loss and take profit levels based on entry price and risk-reward ratio
    stop_loss, take_profit = calculate_stop_loss_take_profit(current_price, risk_reward_ratio, side)
    logging.info(f"Stop Loss: {stop_loss} | Take Profit: {take_profit}")

    #"""
    # Place an order on Bybit using the REST API
    order = client.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        order_type="Market",
        qty=order_size,
        price=current_price,
        time_in_force="ImmediateOrCancel",
        reduce_only=False,
        close_on_trigger=False,
        positionIdx=1,
        takeProfit=take_profit,
        stopLoss=stop_loss,
    )
    if order['retCode'] == 0:
        logging.info(f"Order executed: {side} | Order Size: {order_size}")
        last_order_time = datetime.now()
    else:
        logging.error(f"Failed to execute order: {order['ret_msg']}")
    #"""

# Fetch historical data
data = fetch_historical_data(symbol, timeframe)

# Logging information
logging.info(f"Symbol: {symbol} | Timeframe: {timeframe} | Close: {data['close'].iloc[-1]} | Short SMA: {data['short_sma'].iloc[-1]} | Long SMA: {data['long_sma'].iloc[-1]}")

# Check SMA crossover
signal = check_sma_crossover(data)

# Execute trade
if signal:
    execute_trade(signal, risk_percentage, data['close'].iloc[-1])
