# trend_following.py

import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime
from market_data_loader import MarketDataLoader

def run_backtest(symbol: str, interval: str, period: str, short_window: int, long_window: int) -> (pd.DataFrame, list, dict):
    """
    
    """
    market_data_loader = MarketDataLoader(interval=interval, period=period)
    history = market_data_loader.get_history(symbol)

    history["ma_short"] = history["last_price"].rolling(window=short_window).mean()
    history["ma_long"] = history["last_price"].rolling(window=long_window).mean()

    # Assume no trade at first
    signal = 0
    if history['ma_short'] > history['ma_long']:
        # Generate buy signal
        signal = 1
    elif history['ma_short'] < history['ma_long']:
        # Generate sell signal
        signals = -1