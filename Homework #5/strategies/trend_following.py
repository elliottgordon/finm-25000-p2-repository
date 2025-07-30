# trend_following.py

import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime
from market_data_loader import MarketDataLoader
from typing import List, Dict, Tuple

def run_backtest(
        history: pd.DataFrame,
        short_window: int = 20,
        long_window: int = 50,
        risk_params: dict,
):
    """
    Runs a backtest for a trend-following strategy using moving averages.

    Args:
        history (pd.DataFrame): Historical market data with 'last_price' column.
        risk_params (dict): Risk parameters including 'symbol', 'quantity', etc.
        short_window (int): Short moving average window.
        long_window (int): Long moving average window.
    """
    # Load data and compute moving averages
    market_data_loader = MarketDataLoader(interval=interval, period=period)
    history = market_data_loader.get_history(symbol)

    history["ma_short"] = history["last_price"].rolling(window=short_window).mean()
    history["ma_long"] = history["last_price"].rolling(window=long_window).mean()

    # Create signals_df (columns: timestemp, signal)
    signals_df = pd.DataFrame(index=history.index)
    signals_df.reset_index(inplace=True)
    signals_df['signal'] = 0 # no signal by default
    
    # Generate signals based on moving average crossovers
    # When ma_short crosses above ma_long, we buy


    # When ma_short crosses above ma_long, we buy
    history.loc[history["ma_short"] > history["ma_long"], "signal"] = 1
    # When ma_short crosses below ma_long, we sell
    history.loc[history["ma_short"] < history["ma_long"], "signal"] = -1

    # Create signals_df
    signals_df = history[history['clean_signal'] != 0].copy()
    signals_df = signals_df.reset_index() # Make timestamp a column
    signals_df = signals_df[['timestamp', 'last_price', 'ma_short', 'ma_long', 'clean_signal']].rename(columns={'clean_signal': 'signal'})
    # Clean signals - only trade on actual crossovers using .shift()
    history['prev_signal'] = history['signal'].shift(1).fillna(0)
    history['clean_signal'] = 0 

    # Only signal when there's a change from previous signal
    mask = history['signal'] != history['prev_signal']
    history.loc[mask, 'clean_signal'] = history['signal']

    # Initialize trading components
    oms = OrderManagementSystem()
    book = LimitOrderBook(symbol)
    tracker = PositionTracker(starting_cash=100000)
    trades_list = []

    # Backtest loop
    signals_df = history[history['clean_signal'] != 0].copy()
    for timestamp, row in signals_df.iterrows():
        signal = row['clean_signal']
        price = row['last_price']
        
        # Determine trade side and quantity
        if signal == 1: # Buy signal
            side = "buy"
            quantity = 100
        elif signal == -1: # Sell signal
            side = "sell"
            quantity = 100
        else:
            continue

        # Create order 
        order = Order(
            id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            quantity=quantity, # from risk_params
            type="market" or "limit",
            timestamp=timestamp
        )

        # Submit to Order Management System
        ack = oms.new_order(order)

        # Route to book and tracker
        reports = book.add_order(order)
        for report in reports:
            tracker.update(report)

        trades_list.extend(reports)

    # Calculate final metrics
    current_prices = {symbol: history['last_price'].iloc[-1]}
    summary = tracker.get_pnl_summary(current_prices)

    # Compute additional metrics
    blotter_df = tracker.get_blotter()
    if not blotter_df.empty:
        # Equity curve calculation
        blotter_df['cumulative cash'] = blotter_df['cash_flow'].cumsum() + tracker.cash
        equity_curve = blotter_df['cumulative cash']
        returns = blotter_df['cumulative cash'].pct_change().fillna(0)

        sharpe_ratio = returns.mean() / returns.std() * (252 ** 0.5)  # Annualized Sharpe Ratio
        max_drawdown = (equity_curve - equity_curve.cummax()).min()

    else:
        sharpe_ratio = 0
        max_drawdown = 0
    
    metrics_dict = {
        "total_return": summary['total_pnl'] / tracker.starting_cash,
        "max_drawdown": max_drawdown, # computed from tracker.blotter or equity curve
        "sharpe_ratio": sharpe_ratio, # compute returns.std() etc.
    }
    return history, trades_list, metrics_dict


# run the script

    