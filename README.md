# Trading Bot

This repository contains a trading bot that utilizes technical analysis indicators to execute trades on the Bybit exchange. The bot fetches historical data, checks for specific conditions using SMA crossover, calculates order size based on risk percentage, and executes trades based on predefined parameters.

## Features

- Fetches historical OHLCV data for a symbol and timeframe using the Bybit REST API.
- Applies technical indicators (SMA) to the historical data.
- Checks for a SMA crossover to generate trade signals.
- Calculates order size based on risk percentage and current price.
- Calculates stop loss and take profit levels based on entry price and risk-reward ratio.
- Executes trades on Bybit using the REST API.

## Prerequisites

Before running the trading bot, make sure you have the following prerequisites installed:

- Python 3.x
- Required Python packages (install via `pip install package_name`):
  - `pybit`
  - `ta`
  - `pandas`

## Configuration

To use the trading bot, you need to provide your Bybit API key and secret. Follow these steps to configure the bot:

1. Create a file named `bybit_secrets.py` in the root directory.
2. Open `bybit_secrets.py` and define the following constants:
   ```python
   API_KEY = 'your_api_key'
   API_SECRET = 'your_api_secret'
