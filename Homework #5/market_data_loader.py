# Part 1: Market Data Loader
# This module will fetch OHLCV (Open, High, Low, Close, Volume) data for equities, ETFs, FX, crypto, bonds/futures, plus options chains, either by period or explicit start/end

import yfinance as yf
import pandas as pd
from typing import Dict, Any, Tuple

# Create MarketDataLoader class

class MarketDataLoader:
    """
    A class to load and cache market data from Yahoo Finance.
    """
    def __init__(self, interval: str = "1d", period: str = "5y"):
        self.interval = interval
        self.period = period
        self.ohlcv_cache: Dict[str, pd.DataFrame] = {}
        self.options_cache: Dict[str, pd.DataFrame] = {}

    def _rename_and_tz(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes DataFrame column names and converts timezone to UTC.

        Arguments:
            df (pd.DataFrame): Raw DataFrame from Yahoo Finance with original column names

        Returns:
            df (pd.DataFrame): Processed DataFrame with standardized column names and UTC timezone
        """
        # check if the DataFrame is empty
        if df.empty:
            return df
        
        # Flatten multi-level columns if they exist
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        
        # Rename columns to lowercase and set timezone to UTC
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'last_price',
            'Volume': 'volume'
        })

        if df.index.tzinfo is None:
            # No timezone info, localize to UTC
            df.index = df.index.tz_localize('UTC')
        else:
            # Already has timezone info, convert to UTC
            df.index = df.index.tz_convert('UTC')
        return df
    
    def _load_period(self, symbol: str) -> pd.DataFrame:
        """
        Downloads market data for the default period and interval, then caches the result.

        Arguments:
            symbol (str): The stock symbol to download data for

        Returns:
            df (pd.DataFrame): DataFrame containing the OHLCV data for the specified symbol
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
        Fetches historical market data for a symbol, either from cache or by downloading.

        Arguments:
            symbol (str): The stock symbol to fetch data for
            start (str): Start date in 'YYYY-MM-DD' format (optional)
            end (str): End date in 'YYYY-MM-DD' format (optional)
        
        Returns:
            df (pd.DataFrame): Historical OHLCV data, processed if using default period, raw if using custom range
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
            df = self._rename_and_tz(df)
            return df

    def _locate_timestamp(self, df: pd.DataFrame, ts: pd.Timestamp) -> pd.Timestamp:
        """
        Finds the nearest prior timestamp in the DataFrame index, handling timezone conversion.

        Arguments:
            df (pd.DataFrame): DataFrame with datetime index to search within
            ts (pd.Timestamp): Target timestamp to locate or find nearest prior match

        Returns:
            pd.Timestamp: The nearest prior timestamp in the DataFrame index    
        """
        # Convert datetime to pandas Timestamp if needed
        if not isinstance(ts, pd.Timestamp):
            ts = pd.Timestamp(ts)

        # Convert the timestamp to the timezone of the DataFrame if the timezone has timezone info
        if ts.tzinfo and df.index.tzinfo:
            ts = ts.tz_convert(df.index.tzinfo)
        elif ts.tzinfo is None and df.index.tzinfo:
            ts = ts.tz_localize(df.index.tzinfo)

        idx = df.index.get_indexer([ts], method="ffill")
        if idx[0] == -1:
            raise ValueError(f"Timestamp {ts} not found in DataFrame index.")
        return df.index[idx[0]]
    
    def _scalar_to_float(self, x: Any) -> float:
        """
        Converts a scalar value to a float, handling None and NaN values.

        Arguments:
            x (Any): The value to convert

        Returns:
            float: The converted float value, or NaN if the input is None or NaN
        """
        if pd.isna(x) or x is None:
            return float('nan')
        try:
            return float(x)
        except (ValueError, TypeError):
            return float('nan')

    def _scalar_to_int(self, x: Any) -> int:
        """
        Converts a scalar value to an integer, handling None and NaN values.

        Arguments:
            x (Any): The value to convert

        Returns:
            int: The converted integer value, or NaN if the input is None or NaN
        """
        if pd.isna(x) or x is None:
            raise ValueError("Cannot convert None or NaN to int")
        try:
            return int(x)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert {x} to int")

    def get_price(self, symbol: str, timestamp: pd.Timestamp) -> float:
        """
        Gets the price (last price) for a symbol at a specific timestamp.

        Arguments:
            symbol (str): The stock symbol to fetch the price for
            timestamp (pd.Timestamp): The timestamp to fetch the price at

        Returns:
            float: The last price at the specified timestamp, or None if not found
        """
        df = self.get_history(symbol)
        located_ts = self._locate_timestamp(df, timestamp)
        price = self._scalar_to_float(df.loc[located_ts, 'last_price'])
        if price is None:
            return None
        return price

    def get_bid_ask(self, symbol: str, timestamp: pd.Timestamp) -> Tuple[float, float]:
        """
        Gets the bid-ask spread for a symbol at a specific timestamp.

        Arguments:
            symbol (str): The stock symbol to fetch the bid-ask spread for
            timestamp (pd.Timestamp): The timestamp to fetch the bid-ask spread at

        Returns:
            Tuple[float, float]: The bid and ask prices at the specified timestamp, or (None, None) if not found
        """
        ticker = yf.Ticker(symbol)

        bid = ticker.info.get('bid', None)
        ask = ticker.info.get('ask', None)
        
        if bid is None or ask is None:
            return None, None
        return bid, ask

    def get_volume(self, symbol: str, start: str, end: str) -> int:
        """
        Gets the volume for a symbol over a specified date range.

        Arguments:
            symbol (str): The stock symbol to fetch the volume for
            start (str): Start date in 'YYYY-MM-DD' format
            end (str): End date in 'YYYY-MM-DD' format

        Returns:
            int: The total volume over the specified date range, or None if not found
        """
        df = self.get_history(symbol, start, end)
        total_volume = self._scalar_to_int(df['volume'].sum())
        if total_volume is None:
            return None
        return total_volume

    def get_options_chain(self, symbol: str, expiry=None) -> Dict[str, pd.DataFrame]:
        """
        Gets the options chain for a symbol with optional expiry date.

        Arguments:
            symbol (str): The stock symbol to fetch the options chain for
            expiry (str): Optional expiry date in 'YYYY-MM-DD' format to filter options

        Returns:
            Dict[str, pd.DataFrame]: A dictionary with 'calls' and 'puts' DataFrames, or None if not found
        """
        # Create a cache key based on symbol and expiry (if specified)
        cache_key = f"({symbol}_{expiry})" if expiry else symbol

        # Check if options data is already cached
        if cache_key in self.options_cache:
            print(f"Using cached options chain for {symbol}")
            return self.options_cache[cache_key]
        
        # Get the options for specific expiry date
        print(f"Downloading options chain for {symbol} with expiry {expiry}")

        if expiry:
            options = yf.Ticker(symbol).option_chain(expiry)
        else:
            options = yf.Ticker(symbol).options

        # Create the result
        options_data = {
            "calls": options.calls,
            "puts": options.puts
        }

        # Cache the result
        self.options_cache[cache_key] = options_data
        
        return options_data
