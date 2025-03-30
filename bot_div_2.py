import os
import time
import asyncio
import logging
import threading
import signal
import sqlite3
import ccxt.async_support as ccxt
import numpy as np
import pandas as pd
import tenacity
import requests
import json
from telegram import Bot
from telegram.request import HTTPXRequest
from datetime import datetime
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from telegram import Bot
from matplotlib import pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from ccxt import NetworkError, RequestTimeout
from requests.exceptions import RequestException, Timeout
from asyncio import SelectorEventLoop


# Глобальные переменные
RUNNING = True
start_time = time.time()
processed_pairs = 0
signals_found = {"Bullish": 0, "Bearish": 0}
signals_by_pair = {}
successful_signals_by_pair = {}
errors_count = 0
last_error = "None"
last_telegram_sent = None
last_sheets_write = None
last_binance_fetch = None
last_sqlite_update = None
restarts = []
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# # Глобальные переменные
# RUNNING = True
# start_time = time.time()
# signals_found = {'Bullish': 0, 'Bearish': 0}
# processed_pairs = 0
# errors_count = 0
# last_error = "None"
# last_binance_fetch = 0
# last_telegram_sent = 0
# last_sheets_write = 0
# last_sqlite_update = 0
# restarts = []
# signals_by_pair = {}
# successful_signals_by_pair = {}
# binance_markets = None

# Обработчик сигнала
def signal_handler(sig, frame):
    global RUNNING
    print("Останавливаю скрипт...")
    RUNNING = False
    if 'executor' in globals():
        executor.shutdown(wait=False)
    if 'loop' in globals():
        loop.stop()

signal.signal(signal.SIGINT, signal_handler)

# Настройка логирования
logging.basicConfig(filename='binance_tvh_script.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
                    
# Загрузка конфигурации
with open('config.json', 'r') as f:
    CONFIG = json.load(f)

# Извлечение настроек
TELEGRAM_TOKEN = CONFIG['telegram_token']
TELEGRAM_CHAT_ID = CONFIG['telegram_chat_id']
GOOGLE_SHEET_ID = CONFIG['google_sheet_id']
GOOGLE_CREDENTIALS_FILE = CONFIG['google_credentials_file']
VOLUME_24H_THRESHOLD = CONFIG['volume_24h_threshold']
VOLUME_4CANDLES_THRESHOLD = CONFIG['volume_4candles_threshold']
DIVERGENCE_THRESHOLD = CONFIG['divergence_threshold']
RSI_PERIOD = CONFIG['rsi_period']
RSI_OVERSOLD = tuple(CONFIG['rsi_oversold'])
RSI_OVERBOUGHT = tuple(CONFIG['rsi_overbought'])
MACD_FAST = CONFIG['macd_fast']
MACD_SLOW = CONFIG['macd_slow']
MACD_SIGNAL = CONFIG['macd_signal']
MACD_HIST_THRESHOLD = CONFIG['macd_hist_threshold']
USE_TVH_FILTER = CONFIG['use_tvh_filter']
USE_MACD_FILTER = CONFIG['use_macd_filter']
USE_MACD_HIST_FILTER = CONFIG['use_macd_hist_filter']
USE_HORIZONTAL_LEVELS = CONFIG['use_horizontal_levels']
USE_DIAGONAL_LEVELS = CONFIG['use_diagonal_levels']
RESULT_WAIT_TIME = CONFIG['result_wait_time']
CHART_CANDLES = CONFIG['chart_candles']
DIVERGENCE_CANDLES_RSI = CONFIG['divergence_candles_rsi']
DIVERGENCE_CANDLES_MACD = CONFIG['divergence_candles_macd']
TIMEFRAMES = CONFIG['timeframes']
RESTART_WINDOW = CONFIG['restart_window']
MIN_SUCCESS_RATE = CONFIG['min_success_rate']
SCREENSHOT_DIR = CONFIG['screenshot_dir']
SCREENSHOT_LIFETIME = CONFIG['screenshot_lifetime']
DELETE_SCREENSHOTS = CONFIG['delete_screenshots']
LEVEL_TOLERANCE = CONFIG['level_tolerance']
USE_LOWER_TF_LEVELS = CONFIG['use_lower_tf_levels']
LEVEL_ANY_CONDITION = CONFIG['level_any_condition']

# # Подключение к Binance
# @tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(5))
# def connect_to_binance():
    # global binance_markets
    # binance = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    # binance_markets = binance.load_markets()
    # return binance

# Инициализация Binance
binance = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
        'defaultSubType': 'linear'
    }
})
# Telegram
request = HTTPXRequest(connection_pool_size=30, connect_timeout=60, read_timeout=60)
bot = Bot(token=TELEGRAM_TOKEN, request=request)

from google.oauth2 import service_account
from google.auth.transport.requests import Request
import requests

creds = service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
session = requests.Session()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def append_to_google_sheets(data, sheet_name='Sheet1'):
    global last_sheets_write
    if creds.expired or not creds.valid:
        creds.refresh(Request())
    session.headers.update({'Authorization': f'Bearer {creds.token}'})
    cleaned_data = [None if isinstance(x, float) and np.isnan(x) else x for x in data]
    values = [cleaned_data]
    body = {'values': values}
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{sheet_name}!A1:append?valueInputOption=RAW'
    response = session.post(url, json=body)
    response.raise_for_status()
    last_sheets_write = time.time()

# @tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
# def update_google_sheets(symbol, result):
    # global last_sheets_write, successful_signals_by_pair
    # if creds.expired or not creds.valid:
        # creds.refresh(Request())
    # session.headers.update({'Authorization': f'Bearer {creds.token}'})
    # url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1'
    # response = session.get(url)
    # response.raise_for_status()
    # values = response.json().get('values', [])
    # for i, row in enumerate(values):
        # if len(row) > 1 and row[1] == symbol and row[-1] == 'Pending':
            # values[i][-1] = result
            # if result == "Positive":
                # successful_signals_by_pair[symbol] = successful_signals_by_pair.get(symbol, 0) + 1
            # break
    # body = {'values': values}
    # update_url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1?valueInputOption=RAW'
    # response = session.put(update_url, json=body)
    # response.raise_for_status()
    # last_sheets_write = time.time()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def update_stats_sheet():
    if creds.expired or not creds.valid:
        creds.refresh(Request())
    session.headers.update({'Authorization': f'Bearer {creds.token}'})
    total_signals = sum(signals_found.values())
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1'
    response = session.get(url)
    response.raise_for_status()
    values = response.json().get('values', [])
    positive_signals = sum(1 for row in values if len(row) > 12 and row[-1] == "Positive")
    success_rate = (positive_signals / total_signals * 100) if total_signals > 0 else 0
    
    top_data = [(pair, signals_by_pair.get(pair, 0), successful_signals_by_pair.get(pair, 0),
                 (successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100) if signals_by_pair.get(pair, 0) > 0 else 0)
                for pair in signals_by_pair if signals_by_pair.get(pair, 0) > 0 and successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100 >= MIN_SUCCESS_RATE]
    
    top_signals = sorted(top_data, key=lambda x: x[1], reverse=True)[:5]
    top_successful = sorted(top_data, key=lambda x: x[2], reverse=True)[:5]
    
    stats_data = [
        ["Stat", "Value"],
        ["Total Signals", total_signals],
        ["Positive Signals", positive_signals],
        ["Success Rate (%)", f"{success_rate:.2f}"],
        ["", ""],
        ["Top 5 Pairs by Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_signals] + [
        ["", "", "", ""],
        ["Top 5 Pairs by Successful Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_successful]
    
    body = {'values': stats_data}
    update_url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Stats!A1?valueInputOption=RAW'
    response = session.put(update_url, json=body)
    response.raise_for_status()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def update_google_sheets(symbol, result):
    global last_sheets_write, successful_signals_by_pair
    if creds.expired or not creds.valid:
        creds.refresh(Request())
    session.headers.update({'Authorization': f'Bearer {creds.token}'})
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1'
    response = session.get(url)
    response.raise_for_status()
    values = response.json().get('values', [])
    for i, row in enumerate(values):
        if len(row) > 1 and row[1] == symbol and row[-1] == 'Pending':
            values[i][-1] = result
            if result == "Positive":
                successful_signals_by_pair[symbol] = successful_signals_by_pair.get(symbol, 0) + 1
            break
    body = {'values': values}
    update_url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1?valueInputOption=RAW'
    response = session.put(update_url, json=body)
    response.raise_for_status()
    last_sheets_write = time.time()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def update_stats_sheet():
    if creds.expired or not creds.valid:
        creds.refresh(Request())
    session.headers.update({'Authorization': f'Bearer {creds.token}'})
    total_signals = sum(signals_found.values())
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1'
    response = session.get(url)
    response.raise_for_status()
    values = response.json().get('values', [])
    positive_signals = sum(1 for row in values if len(row) > 12 and row[-1] == "Positive")
    success_rate = (positive_signals / total_signals * 100) if total_signals > 0 else 0
    
    top_data = [(pair, signals_by_pair.get(pair, 0), successful_signals_by_pair.get(pair, 0),
                 (successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100) if signals_by_pair.get(pair, 0) > 0 else 0)
                for pair in signals_by_pair if signals_by_pair.get(pair, 0) > 0 and successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100 >= MIN_SUCCESS_RATE]
    
    top_signals = sorted(top_data, key=lambda x: x[1], reverse=True)[:5]
    top_successful = sorted(top_data, key=lambda x: x[2], reverse=True)[:5]
    
    stats_data = [
        ["Stat", "Value"],
        ["Total Signals", total_signals],
        ["Positive Signals", positive_signals],
        ["Success Rate (%)", f"{success_rate:.2f}"],
        ["", ""],
        ["Top 5 Pairs by Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_signals] + [
        ["", "", "", ""],
        ["Top 5 Pairs by Successful Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_successful]
    
    body = {'values': stats_data}
    update_url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Stats!A1?valueInputOption=RAW'
    response = session.put(update_url, json=body)
    response.raise_for_status()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def get_24h_volume(symbol):
    try:
        ticker = binance.fetch_ticker(symbol)
        volume = ticker.get('quoteVolume', 0)
        if volume == 0:
            logging.error(f"Zero volume for {symbol}")
        return volume
    except Exception as e:
        logging.error(f"Error fetching volume for {symbol}: {str(e)}")
        return 0

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def fetch_candles(symbol, timeframe, limit=26):
    global last_binance_fetch, last_sqlite_update
    with sqlite3.connect('binance_data.db') as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM candles WHERE symbol=? ORDER BY timestamp DESC LIMIT ?", (symbol, limit))
            data = cursor.fetchall()
            if len(data) < limit or (int(time.time() * 1000) - data[0][1]) > get_timeframe_seconds(timeframe) * 1000:
                logging.info(f"Fetching new data for {symbol} from Binance on {timeframe}")
                candles = binance.fetch_ohlcv(symbol, timeframe, limit=limit)
                if not candles or len(candles) < 14:
                    logging.warning(f"Insufficient candle data for {symbol}: {len(candles)} candles")
                    return None
                for candle in candles:
                    cursor.execute("INSERT OR REPLACE INTO candles VALUES (?, ?, ?, ?, ?, ?, ?)",
                                   (symbol, candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]))
                conn.commit()
                last_binance_fetch = time.time()
                last_sqlite_update = time.time()
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['symbol'] = symbol
                return df
            else:
                logging.info(f"Using cached data for {symbol}")
                last_sqlite_update = time.time()
                df = pd.DataFrame(data, columns=['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume'])
                return df if len(df) >= 14 else None
        except Exception as e:
            logging.error(f"Error fetching candles for {symbol}: {str(e)}")
            return None

def get_timeframe_seconds(timeframe):
    tf_map = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400}
    return tf_map.get(timeframe, 900)

def clean_screenshots():
    while True:
        now = time.time()
        for filename in os.listdir(SCREENSHOT_DIR):
            filepath = os.path.join(SCREENSHOT_DIR, filename)
            if os.path.isfile(filepath) and now - os.path.getmtime(filepath) > SCREENSHOT_LIFETIME:
                os.remove(filepath)
                logging.info(f"Deleted old screenshot: {filepath}")
        time.sleep(3600)

def is_pinbar(open, high, low, close, ratio):
    body = abs(close - open)
    upper_shadow = high - max(open, close)
    lower_shadow = min(open, close) - low
    return (upper_shadow > body * ratio and lower_shadow < body) or (lower_shadow > body * ratio and upper_shadow < body)

def get_price_precision(df):
    price_range = df['high'].max() - df['low'].min()
    return max(2, int(-np.log10(price_range / 100)))

def check_horizontal_levels(df, candles, by_body, min_touches, mirror, trade_check, trade_candles, pinbar_check, pinbar_ratio, volume_check, volume_increase, volume_candles, lower_tf=False):
    if lower_tf and USE_LOWER_TF_LEVELS and len(TIMEFRAMES) > 1:
        df = fetch_candles(df['symbol'].iloc[0], TIMEFRAMES[0], candles)
    if df is None or len(df) < candles:
        return False, None, []
    df = df.tail(candles)
    precision = get_price_precision(df)
    prices = df['close'] if by_body else df[['high', 'low']].values.flatten()
    
    levels = []
    for price in np.unique(np.round(prices, precision)):
        touches = sum(1 for i in range(len(df)) if (by_body and abs(df['close'].iloc[i] - price) < LEVEL_TOLERANCE * price / 100) or
                      (not by_body and df['high'].iloc[i] >= price >= df['low'].iloc[i]))
        if touches >= min_touches:
            levels.append(price)
    
    if not levels:
        return False, None, []
    
    latest_price = df['close'].iloc[-1]
    for level in levels:
        if abs(latest_price - level) < LEVEL_TOLERANCE * level / 100:
            conditions, descriptions = [], []
            if mirror:
                mirror_touches = sum(1 for i in range(len(df)-1) if df['high'].iloc[i] >= level >= df['low'].iloc[i] and df['close'].iloc[i+1] < level)
                conditions.append(mirror_touches >= min_touches // 2)
                if conditions[-1]:
                    descriptions.append("Mirror")
            if trade_check and len(TIMEFRAMES) > 1:
                lower_tf_df = fetch_candles(df['symbol'].iloc[0], TIMEFRAMES[0], trade_candles * 3)
                if lower_tf_df is not None:
                    trade_touches = sum(1 for i in range(len(lower_tf_df)) if lower_tf_df['high'].iloc[i] >= level >= lower_tf_df['low'].iloc[i])
                    conditions.append(trade_touches >= trade_candles)
                    if conditions[-1]:
                        descriptions.append("Trade on lower TF")
            if pinbar_check:
                last_candle = df.iloc[-1]
                conditions.append(is_pinbar(last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close'], pinbar_ratio))
                if conditions[-1]:
                    descriptions.append("Pinbar")
            if volume_check:
                recent_volume = df['volume'].iloc[-volume_candles:].mean()
                prev_volume = df['volume'].iloc[-volume_candles-5:-volume_candles].mean()
                conditions.append(recent_volume > prev_volume * (1 + volume_increase / 100))
                if conditions[-1]:
                    descriptions.append("Volume increase")
            if LEVEL_ANY_CONDITION and any(conditions) or all(conditions):
                return True, level, descriptions
    return False, None, []

def check_diagonal_levels(df, candles, by_body, min_touches, mirror, trade_check, trade_candles, pinbar_check, pinbar_ratio, volume_check, volume_increase, volume_candles, slope_threshold, lower_tf=False):
    if lower_tf and USE_LOWER_TF_LEVELS and len(TIMEFRAMES) > 1:
        df = fetch_candles(df['symbol'].iloc[0], TIMEFRAMES[0], candles)
    if df is None or len(df) < candles:
        return False, None, [], None
    df = df.tail(candles)
    prices = df['close'] if by_body else df[['high', 'low']].values.flatten()
    x = np.arange(len(df))
    
    slope, intercept = np.polyfit(x, prices, 1)
    if abs(slope) < slope_threshold:
        return False, None, [], None
    
    line = slope * x + intercept
    touches = sum(1 for i in range(len(df)) if abs(prices[i] - line[i]) < LEVEL_TOLERANCE * prices[i] / 100)
    if touches < min_touches:
        return False, None, [], None
    
    latest_price = df['close'].iloc[-1]
    latest_line_price = slope * (len(df) - 1) + intercept
    if abs(latest_price - latest_line_price) < LEVEL_TOLERANCE * latest_price / 100:
        conditions, descriptions = [], []
        if mirror:
            mirror_touches = sum(1 for i in range(len(df)-1) if prices[i] >= line[i] and df['close'].iloc[i+1] < line[i])
            conditions.append(mirror_touches >= min_touches // 2)
            if conditions[-1]:
                descriptions.append("Mirror")
        if trade_check and len(TIMEFRAMES) > 1:
            lower_tf_df = fetch_candles(df['symbol'].iloc[0], TIMEFRAMES[0], trade_candles * 3)
            if lower_tf_df is not None:
                lower_x = np.arange(len(lower_tf_df))
                lower_line = slope * lower_x + intercept
                trade_touches = sum(1 for i in range(len(lower_tf_df)) if abs(lower_tf_df['close'].iloc[i] - lower_line[i]) < LEVEL_TOLERANCE * lower_tf_df['close'].iloc[i] / 100)
                conditions.append(trade_touches >= trade_candles)
                if conditions[-1]:
                    descriptions.append("Trade on lower TF")
        if pinbar_check:
            last_candle = df.iloc[-1]
            conditions.append(is_pinbar(last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close'], pinbar_ratio))
            if conditions[-1]:
                descriptions.append("Pinbar")
        if volume_check:
            recent_volume = df['volume'].iloc[-volume_candles:].mean()
            prev_volume = df['volume'].iloc[-volume_candles-5:-volume_candles].mean()
            conditions.append(recent_volume > prev_volume * (1 + volume_increase / 100))
            if conditions[-1]:
                descriptions.append("Volume increase")
        if LEVEL_ANY_CONDITION and any(conditions) or all(conditions):
            return True, latest_line_price, descriptions, (slope, intercept)
    return False, None, [], None

def calculate_indicators(df):
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    rsi_value = rsi.iloc[-1]
    
    # Простая дивергенция
    price_highs = df['high'][-5:].max()
    rsi_highs = rsi[-5:].max()
    price_lows = df['low'][-5:].min()
    rsi_lows = rsi[-5:].min()
    
    divergence = 0
    direction = "None"
    if price_highs > df['high'].iloc[-1] and rsi_highs < rsi.iloc[-1]:
        divergence = rsi.iloc[-1] - rsi_highs
        direction = "Bullish"
    elif price_lows < df['low'].iloc[-1] and rsi_lows > rsi.iloc[-1]:
        divergence = rsi_lows - rsi.iloc[-1]
        direction = "Bearish"
    
    macd_data = np.zeros(len(df))  # Заглушка
    signal_data = np.zeros(len(df))  # Заглушка
    hist_data = np.zeros(len(df))  # Заглушка
    triggered_filters = ["RSI Divergence"]
    horiz_level, diag_level, diag_line = None, None, None
    
    return divergence, direction, rsi_value, 0, 0, macd_data, signal_data, hist_data, triggered_filters, horiz_level, diag_level, diag_line

def create_screenshot(df, symbol, rsi_value, macd_data, signal_data, hist_data, timeframe, horiz_level, diag_level, diag_line):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), height_ratios=[2, 1], sharex=True)
    ax1.plot(df['close'][-CHART_CANDLES:], label='Close Price', color='blue')
    if horiz_level:
        ax1.axhline(y=horiz_level, color='green', linestyle='--', label=f'Horiz Level ({horiz_level:.2f})')
    if diag_level and diag_line:
        x = np.arange(CHART_CANDLES)
        slope, intercept = diag_line
        line = slope * x + (intercept - slope * (len(df) - CHART_CANDLES))
        ax1.plot(x, line, color='red', linestyle='--', label=f'Diag Level ({diag_level:.2f})')
    ax1.set_title(f'{symbol} {timeframe} Chart\nRSI: {rsi_value:.2f}')
    ax1.set_ylabel('Price')
    ax1.legend()
    ax1.grid()
    
    ax2.plot(macd_data, label='MACD', color='orange')
    ax2.plot(signal_data, label='Signal', color='red')
    ax2.bar(range(len(hist_data)), hist_data, label='Histogram', color='gray', alpha=0.5)
    ax2.set_ylabel('MACD')
    ax2.legend()
    ax2.grid()
    plt.xlabel('Time')
    
    if not os.path.exists(SCREENSHOT_DIR):
        os.makedirs(SCREENSHOT_DIR)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{SCREENSHOT_DIR}/{symbol.replace('/', '_')}_{timestamp}.png"
    plt.savefig(filename)
    plt.close()
    return filename

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def append_to_google_sheets(data, sheet_name='Sheet1'):
    global last_sheets_write
    cleaned_data = [None if isinstance(x, float) and np.isnan(x) else x for x in data]
    values = [cleaned_data]
    body = {'values': values}
    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID, range=f'{sheet_name}!A1',
        valueInputOption='RAW', body=body
    ).execute()
    last_sheets_write = time.time()

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def update_google_sheets(symbol, result):
    global last_sheets_write, successful_signals_by_pair
    sheet = service.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range='Sheet1').execute()
    values = sheet.get('values', [])
    for i, row in enumerate(values):
        if len(row) > 1 and row[1] == symbol and row[-1] == 'Pending':
            values[i][-1] = result
            if result == "Positive":
                successful_signals_by_pair[symbol] = successful_signals_by_pair.get(symbol, 0) + 1
            break
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range='Sheet1', valueInputOption='RAW', body=body).execute()
    last_sheets_write = time.time()

def check_alert_result(symbol, trigger_price, direction, timestamp):
    time.sleep(RESULT_WAIT_TIME)
    df = fetch_candles(symbol, TIMEFRAMES[0], limit=4)
    if df is not None:
        latest_price = df['close'].iloc[-1]
        result = "Positive" if (direction == "Bullish" and latest_price > trigger_price) or (direction == "Bearish" and latest_price < trigger_price) else "Negative"
        logging.info(f"Alert result for {symbol}: {result}")
        update_google_sheets(symbol, result)

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), stop=tenacity.stop_after_attempt(3))
def update_stats_sheet():
    total_signals = sum(signals_found.values())
    sheet = service.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range='Sheet1').execute()
    values = sheet.get('values', [])
    positive_signals = sum(1 for row in values if len(row) > 12 and row[-1] == "Positive")
    success_rate = (positive_signals / total_signals * 100) if total_signals > 0 else 0
    
    top_data = [(pair, signals_by_pair.get(pair, 0), successful_signals_by_pair.get(pair, 0),
                 (successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100) if signals_by_pair.get(pair, 0) > 0 else 0)
                for pair in signals_by_pair if signals_by_pair.get(pair, 0) > 0 and successful_signals_by_pair.get(pair, 0) / signals_by_pair.get(pair, 0) * 100 >= MIN_SUCCESS_RATE]
    
    top_signals = sorted(top_data, key=lambda x: x[1], reverse=True)[:5]
    top_successful = sorted(top_data, key=lambda x: x[2], reverse=True)[:5]
    
    stats_data = [
        ["Stat", "Value"],
        ["Total Signals", total_signals],
        ["Positive Signals", positive_signals],
        ["Success Rate (%)", f"{success_rate:.2f}"],
        ["", ""],
        ["Top 5 Pairs by Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_signals] + [
        ["", "", "", ""],
        ["Top 5 Pairs by Successful Signals", "Signals", "Successful", "Success Rate (%)"],
    ] + [[pair, signals, successful, f"{rate:.2f}"] for pair, signals, successful, rate in top_successful]
    
    body = {'values': stats_data}
    service.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range='Stats!A1', valueInputOption='RAW', body=body).execute()

def print_status():
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        uptime = int(time.time() - start_time)
        uptime_str = f"{uptime // 3600:02d}:{(uptime % 3600) // 60:02d}:{uptime % 60:02d}"
        last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        active_filters = ["RSI Divergence"]
        
        try:
            if creds.expired or not creds.valid:
                creds.refresh(Request())
            session.headers.update({'Authorization': f'Bearer {creds.token}'})
            url = f'https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1'
            response = session.get(url)
            response.raise_for_status()
            values = response.json().get('values', [])
            positive_signals = sum(1 for row in values if len(row) > 12 and row[-1] == "Positive")
        except Exception as e:
            logging.error(f"Google Sheets error in print_status: {str(e)}")
            positive_signals = 0
        
        total_signals = sum(signals_found.values())
        success_rate = (positive_signals / total_signals * 100) if total_signals > 0 else 0
        
        last_fetch_str = f"{int(time.time() - last_binance_fetch) if last_binance_fetch else 'N/A'}s ago"
        last_telegram_str = f"{int(time.time() - last_telegram_sent) if last_telegram_sent else 'N/A'}s ago"
        last_sheets_str = f"{int(time.time() - last_sheets_write) if last_sheets_write else 'N/A'}s ago"
        last_sqlite_str = f"{int(time.time() - last_sqlite_update) if last_sqlite_update else 'N/A'}s ago"
        
        print(f"Binance TVH Divergence Bot       [Running]\n"
              f"----------------------------------------\n"
              f"Uptime: {uptime_str}\n"
              f"Last Update: {last_update}\n"
              f"Timeframes: {', '.join(TIMEFRAMES)}\n"
              f"\nModules:\n"
              f"  Binance Data: OK (Last fetch: {last_fetch_str})\n"
              f"  Telegram: OK (Last sent: {last_telegram_str})\n"
              f"  Google Sheets: OK (Last write: {last_sheets_str})\n"
              f"  SQLite: OK (Last update: {last_sqlite_str})\n"
              f"\nStats:\n"
              f"  Signals Found: {total_signals} (Bullish: {signals_found.get('Bullish', 0)}, Bearish: {signals_found.get('Bearish', 0)})\n"
              f"  Success Rate: {success_rate:.2f}% ({positive_signals}/{total_signals})\n"
              f"  Processed Pairs: {processed_pairs}\n"
              f"  Errors: {errors_count} (Last error: {last_error})\n"
              f"  Restarts (last {RESTART_WINDOW//60}m): {len(restarts)}\n"
              f"\nTop 5 Pairs by Signals (Min {MIN_SUCCESS_RATE}% success):\n"
              f"...\n"
              f"\nFilters Active: {', '.join(active_filters)}\n"
              f"----------------------------------------")
        time.sleep(5)

def restart_script(error_reason):
    global restarts
    restarts.append((time.time(), error_reason))
    logging.critical(f"Restarting script due to: {error_reason}")
    asyncio.run(send_telegram_alert(None, f"Script restarted due to: {error_reason}", "System"))
    time.sleep(5)
    os.execv(sys.executable, [sys.executable] + sys.argv)

#@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=2, max=10), stop=tenacity.stop_after_attempt(3), retry=tenacity.retry_if_exception_type((NetworkError, TimedOut)), reraise=True)
@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), 
                stop=tenacity.stop_after_attempt(3), 
                retry=tenacity.retry_if_exception_type((NetworkError, RequestTimeout, RequestException, Timeout)))
async def send_telegram_alert(df, message, symbol):
    global last_telegram_sent
    if time.time() - last_telegram_sent < 2:
        await asyncio.sleep(2)
    screenshot_path = None
    try:
        screenshot_path = create_screenshot(df, symbol, 0, [], [], [], TIMEFRAMES[0], None, None, None) if df is not None else None
        if screenshot_path:
            with open(screenshot_path, 'rb') as photo:
                await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=photo, caption=message[:1024])
            if DELETE_SCREENSHOTS and os.path.exists(screenshot_path):
                os.remove(screenshot_path)
        else:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message[:1024])
    except FileNotFoundError as e:
        logging.error(f"Telegram alert for {symbol} failed: Screenshot file not found - {str(e)}")
    except TimedOut as e:
        logging.error(f"Telegram alert for {symbol} failed: Timeout error - {str(e)}")
    except NetworkError as e:
        logging.error(f"Telegram alert for {symbol} failed: Network issue - {str(e)}")
    except TelegramError as e:
        logging.error(f"Telegram alert for {symbol} failed: Telegram API error - {str(e)}")
    except Exception as e:
        logging.error(f"Telegram alert for {symbol} failed: Unexpected error - {str(e)}")
    finally:
        last_telegram_sent = time.time()
        if screenshot_path and os.path.exists(screenshot_path) and DELETE_SCREENSHOTS:
            os.remove(screenshot_path)
        
async def process_symbol(symbol, timeframe, executor):
    global processed_pairs, signals_found, errors_count, last_error, last_telegram_sent, signals_by_pair
    try:
        logging.debug(f"Processing {symbol} on {timeframe}: Fetching volume")
        volume_24h = await asyncio.get_event_loop().run_in_executor(executor, get_24h_volume, symbol)
        if volume_24h is None or not isinstance(volume_24h, (int, float)):
            logging.info(f"Skipping {symbol} on {timeframe}: Invalid volume_24h = {volume_24h}")
            return
        if volume_24h < VOLUME_24H_THRESHOLD:
            logging.info(f"Skipping {symbol} on {timeframe}: volume_24h = {volume_24h} < {VOLUME_24H_THRESHOLD}")
            return
        
        logging.debug(f"Processing {symbol} on {timeframe}: Fetching candles")
        df = await asyncio.get_event_loop().run_in_executor(executor, fetch_candles, symbol, timeframe, 26)
        if df is None or len(df) < 26:
            logging.info(f"Skipping {symbol} on {timeframe}: Insufficient candle data (len={len(df) if df is not None else 'None'})")
            return
        volume_4candles = df['volume'][-4:].mean()
        if volume_4candles < VOLUME_4CANDLES_THRESHOLD:
            logging.info(f"Skipping {symbol} on {timeframe}: volume_4candles = {volume_4candles} < {VOLUME_4CANDLES_THRESHOLD}")
            return
        
        logging.debug(f"Processing {symbol} on {timeframe}: Calculating indicators")
        divergence, direction, rsi_value, macd_value, signal_value, macd_data, signal_data, hist_data, triggered_filters, horiz_level, diag_level, diag_line = calculate_indicators(df)
        if direction == "None":
            logging.info(f"No alert for {symbol} on {timeframe}: direction = None")
            return
        
        signals_found[direction] += 1
        signals_by_pair[symbol] = signals_by_pair.get(symbol, 0) + 1
        logging.info(f"Alert triggered for {symbol} on {timeframe}: {direction}")
        screenshot = create_screenshot(df, symbol, rsi_value, macd_data, signal_data, hist_data, timeframe, horiz_level, diag_level, diag_line)
        filters_str = ", ".join(triggered_filters)
        caption = f"{symbol} ({timeframe}): {direction} Divergence {divergence:.4f}, RSI {rsi_value:.2f}\nFilters: {filters_str}"
        await send_telegram_alert(df, caption, symbol)
        
        trigger_price = df['close'].iloc[-1]
        alert_data = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), symbol, trigger_price,
                      volume_4candles, volume_24h, divergence, direction,
                      df['high'].iloc[-1], df['low'].iloc[-1], rsi_value, macd_value, signal_value, 'Pending']
        append_to_google_sheets(alert_data)
        threading.Thread(target=check_alert_result, args=(symbol, trigger_price, direction, df['timestamp'].iloc[-1])).start()
    except Exception as e:
        errors_count += 1
        last_error = f"Processing error: {str(e)}"
        logging.error(f"Error processing {symbol} on {timeframe}: {str(e)}", exc_info=True)

async def main():
    global processed_pairs, executor, loop
    executor = ThreadPoolExecutor(max_workers=5)
    loop = asyncio.get_running_loop()
    batch_size = 50
    
    threading.Thread(target=print_status, daemon=True).start()
    threading.Thread(target=clean_screenshots, daemon=True).start()
    
    while RUNNING:
        try:
            binance_markets = await binance.load_markets()  # Добавлен await
            all_markets = [
                market for market in binance_markets.keys()
                if binance_markets[market]['active'] and
                   binance_markets[market]['type'] == 'swap' and
                   binance_markets[market]['linear'] and
                   market.endswith('/USDT')
            ]
            logging.info(f"Loaded {len(all_markets)} USDT-M perpetual markets: {all_markets[:5]}...")
            if not all_markets:
                logging.warning("No USDT-M perpetual markets found!")
                await asyncio.sleep(30)
                continue
            
            for i in range(0, len(all_markets), batch_size):
                if not RUNNING:
                    break
                futures_markets = all_markets[i:i + batch_size]
                logging.debug(f"Processing batch: {futures_markets}")
                for timeframe in TIMEFRAMES:
                    tasks = []
                    for symbol in futures_markets:
                        tasks.append(process_symbol(symbol, timeframe, executor))
                    try:
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        logging.error(f"Error in batch processing: {str(e)}")
                        continue
                    processed_pairs += len(futures_markets)
                try:
                    update_stats_sheet()
                except Exception as e:
                    logging.error(f"Failed to update stats sheet: {str(e)}")
                await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Main loop error: {str(e)}")
            await asyncio.sleep(30)
    
    while RUNNING:
        try:
            binance_markets = binance.load_markets()
            all_markets = [
                market for market in binance_markets.keys()
                if binance_markets[market]['active'] and
                   binance_markets[market]['type'] == 'swap' and
                   binance_markets[market]['linear'] and
                   market.endswith('/USDT')
            ]
            logging.info(f"Loaded {len(all_markets)} USDT-M perpetual markets: {all_markets[:5]}...")
            if not all_markets:
                logging.warning("No USDT-M perpetual markets found!")
                await asyncio.sleep(30)
                continue
            
            for i in range(0, len(all_markets), batch_size):
                if not RUNNING:
                    break
                futures_markets = all_markets[i:i + batch_size]
                logging.debug(f"Processing batch: {futures_markets}")
                for timeframe in TIMEFRAMES:
                    tasks = []
                    for symbol in futures_markets:
                        tasks.append(process_symbol(symbol, timeframe, executor))
                    try:
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        logging.error(f"Error in batch processing: {str(e)}")
                        continue
                    processed_pairs += len(futures_markets)
                try:
                    update_stats_sheet()
                except Exception as e:
                    logging.error(f"Failed to update stats sheet: {str(e)}")
                await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Main loop error: {str(e)}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    with sqlite3.connect('binance_data.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS candles
                          (symbol TEXT, timestamp INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL)''')
        conn.commit()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script stopped by user")
    finally:
        if 'executor' in globals():
            executor.shutdown(wait=True)