# Part 1: Market Data Loader
# This module will fetch OHLCV (Open, High, Low, Close, Volume) data for equities, ETFs, FX, crypto, bonds/futures, plus options chains, either by period or explicit start/end

import yfinance as yf
import pandas as pd
from typing import Dict, Any

# Create MarketDataLoader class

class MarketDataLoader:
    """
    A class to load and cache market data from Yahoo Finance.
    """
    def __init__(self, interval: str = "1d", period: str = "1y"):
        self.interval = interval
        self.period = period
        self.ohlcv_cache: Dict[str, pd.DataFrame] = {}
        self.options_cache: Dict[str, pd.DataFrame] = {}

    def _rename_and_tz(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns and ensure timezone is set to UTC.
        """
        # check if the DataFrame is empty
        if df.empty:
            return df
        
        # Rename columns to lowercase and set timezone to UTC
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'last_price',
            'Volume': 'volume'
        })
        df.index = df.index.tz_localize('UTC')
        return df
    
    def _load_period(self, symbol: str) -> pd.DataFrame:
        """
        Method to download data for the default period and interval.
        """
        print(f"Downloading data for {symbol} for period {self.period} and interval {self.interval}")
        df = yf.download(symbol, 
                         period=self.period, 
                         interval=self.interval, 
                         auto_adjust=True)

        # Pass through processing function
        df = self._rename_and_tz(df)

        # Cache the result
        self.ohlcv_cache[symbol] = df
        return df

    def get_history(self, symbol: str, start: str = None, end: str = None) -> pd.DataFrame:
        """
        Fetch historical market data for a given symbol.
        If start and end are provided, fetch data for that range.
        Otherwise, fetch data for the period specified during initialization.
        """
        if start is None and end is None:
            # If no start and end, use the default period
            
            # Check if the data is already cached
            if symbol in self.ohlcv_cache:
                print(f"Using cached data for {symbol}")
                return self.ohlcv_cache[symbol]
            else: # if not cached, download data for the default period
                return self._load_period(symbol)
            
        else: # if a specific date range is provided, download data for that range
            print(f"Downloading data for {symbol} from {start} to {end} with interval {self.interval}")
            # Download the data
            df = yf.download(symbol, 
                             start=start, 
                             end=end, 
                             interval=self.interval, 
                             auto_adjust=True)
            return df
        
