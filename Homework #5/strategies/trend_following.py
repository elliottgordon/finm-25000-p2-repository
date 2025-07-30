# trend_following.py

import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime

def run_backtest(
    history: pd.DataFrame,
    symbol: str = "AAPL",
    short_win: int = 20,
    long_win: int = 50,
    risk_params: dict = None
) -> tuple:
    """
    Runs a backtest for a trend-following strategy using moving averages.

    Args:
        history (pd.DataFrame): Historical price data with 'timestamp' and 'last_price' columns
        short_win (int): Short moving average window
        long_win (int): Long moving average window
        risk_params (dict): Risk parameters including max_pos, etc.

    Returns:
        Tuple[pd.DataFrame, list, dict]: signals dataframe, trades list, metrics dict
    """
    # Default risk parameters
    if risk_params is None:
        risk_params = {'max_pos': 100}

    # Compute moving averages
    history = history.copy()
    history["ma_short"] = history["last_price"].rolling(window=short_win).mean()
    history["ma_long"] = history["last_price"].rolling(window=long_win).mean()

    # Generate signals based on moving average crossovers
    history['signal'] = 0
    
    # When ma_short crosses above ma_long: signal = +1
    history.loc[history["ma_short"] > history["ma_long"], "signal"] = 1
    # When ma_short crosses below ma_long: signal = -1  
    history.loc[history["ma_short"] < history["ma_long"], "signal"] = -1
    
    # Clean signals - only trade on actual crossovers using .shift()
    history['prev_signal'] = history['signal'].shift(1).fillna(0)
    
    # Only signal when there's a change from previous signal
    mask = (history['signal'] != history['prev_signal']) & (history['signal'] != 0)
    signals_df = history[mask].copy()

    # Initialize trading components
    oms = OrderManagementSystem()
    book = LimitOrderBook(symbol)
    tracker = PositionTracker(starting_cash=100000)
    trades_list = []

    # Backtest loop - iterate through non-zero signals in chronological order
    for timestamp, row in signals_df.iterrows():
        signal = row['signal']
        price = row['last_price']
        
        # Determine trade side and quantity based on signal
        if signal == 1:  # Buy signal - go long
            side = "buy"
            quantity = risk_params.get('max_pos', 100)
        elif signal == -1:  # Sell signal - go short or close long
            side = "sell"  
            quantity = risk_params.get('max_pos', 100)
        else:
            continue

        # Create order - use limit order at market price for backtesting
        order = Order(
            id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            quantity=quantity,
            type="limit",  # Changed from "market" to "limit"
            price=price,   # Set limit price to current market price
            timestamp=timestamp
        )

        # Submit to OMS
        ack = oms.new_order(order)

        # For backtesting, create a direct execution report instead of using the order book
        # since there's no existing liquidity to trade against
        execution_report = {
            "order_id": order.id,
            "symbol": order.symbol,
            "side": order.side,
            "filled_qty": order.quantity,
            "price": price,
            "timestamp": timestamp,
            "status": "filled"
        }
        
        # Update tracker directly
        tracker.update(execution_report)
        trades_list.append(execution_report)

    # After processing all signals, calculate metrics
    current_prices = {symbol: history['last_price'].iloc[-1]}
    summary = tracker.get_pnl_summary(current_prices)

    # Compute equity curve from tracker.blotter 
    equity_curve = tracker.get_blotter()
    if not equity_curve.empty:
        returns = equity_curve['cash_flow'].fillna(0)
        returns = returns.pct_change().fillna(0)
        sharpe_ratio = returns.mean() / returns.std() * (252**0.5) if returns.std() > 0 else 0
        
        # Calculate max drawdown from cumulative returns
        cumulative_cash = equity_curve['cash_flow'].cumsum() + tracker.starting_cash
        running_max = cumulative_cash.cummax()
        drawdown = (cumulative_cash - running_max) / running_max
        max_drawdown = drawdown.min()
    else:
        sharpe_ratio = 0
        max_drawdown = 0
    
    metrics_dict = {
        "total_return": summary['total_pnl'] / tracker.starting_cash,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
    }
    
    return (signals_df, trades_list, metrics_dict)

    