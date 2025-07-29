import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime
from market_data_loader import MarketDataLoader
from typing import List, Dict, Tuple
import numpy as np

def run_backtest(symbol: str, 
                 interval: str, 
                 period: str, 
                 bollinger_win: int, 
                 num_std: float,
                 risk_params: dict = None) -> Tuple[pd.DataFrame, list, dict]:
    """
    Mean reversion strategy using Bollinger Bands.
    
    Args:
        symbol: Stock symbol to trade
        interval: Data interval (e.g., '1d', '1h')
        period: Data period (e.g., '5y', '1y')
        bollinger_win: Window for Bollinger Bands calculation
        num_std: Number of standard deviations for bands
        risk_params: Risk parameters for position sizing
        
    Returns:
        Tuple of (signals_df, trades_list, metrics_dict)
    """
    
    # Default risk parameters
    if risk_params is None:
        risk_params = {
            'starting_cash': 100000,
            'position_fraction': 0.1  # Use 10% of cash per trade
        }
    
    # Load market data
    market_data_loader = MarketDataLoader(interval=interval, period=period)
    history = market_data_loader.get_history(symbol)
    
    # Compute Bollinger Bands
    m = history["last_price"].rolling(bollinger_win).mean()
    s = history["last_price"].rolling(bollinger_win).std()
    history["upper"] = m + num_std * s
    history["lower"] = m - num_std * s
    history["mid"] = m
    
    # Initialize signal column
    history['signal'] = 0
    
    # Generate signals based on Bollinger Band crossings
    # Track position state to manage entry/exit logic
    history['position'] = 0  # 0 = no position, 1 = long, -1 = short
    
    for i in range(1, len(history)):
        current_price = history['last_price'].iloc[i]
        prev_price = history['last_price'].iloc[i-1]
        upper = history['upper'].iloc[i]
        lower = history['lower'].iloc[i]
        mid = history['mid'].iloc[i]
        prev_position = history['position'].iloc[i-1]
        
        # Skip if Bollinger Bands not yet calculated
        if pd.isna(upper) or pd.isna(lower) or pd.isna(mid):
            history.loc[history.index[i], 'position'] = prev_position
            continue
            
        # Entry signals
        if prev_position == 0:  # No current position
            if current_price <= lower and prev_price > lower:
                # Price crossed below lower band - enter long
                history.loc[history.index[i], 'signal'] = 1
                history.loc[history.index[i], 'position'] = 1
            elif current_price >= upper and prev_price < upper:
                # Price crossed above upper band - enter short
                history.loc[history.index[i], 'signal'] = -1
                history.loc[history.index[i], 'position'] = -1
            else:
                history.loc[history.index[i], 'position'] = prev_position
                
        # Exit signals
        elif prev_position == 1:  # Currently long
            if current_price >= mid and prev_price < mid:
                # Price crossed back to mid - exit long
                history.loc[history.index[i], 'signal'] = 0
                history.loc[history.index[i], 'position'] = 0
            else:
                history.loc[history.index[i], 'position'] = prev_position
                
        elif prev_position == -1:  # Currently short
            if current_price <= mid and prev_price > mid:
                # Price crossed back to mid - exit short
                history.loc[history.index[i], 'signal'] = 0
                history.loc[history.index[i], 'position'] = 0
            else:
                history.loc[history.index[i], 'position'] = prev_position
        else:
            history.loc[history.index[i], 'position'] = prev_position
    
    # Create signals DataFrame with only non-zero signals
    signals_df = history[history['signal'] != 0].copy()
    signals_df = signals_df.reset_index()
    signals_df = signals_df[['timestamp', 'last_price', 'upper', 'lower', 'mid', 'signal']]
    
    # Initialize trading components
    oms = OrderManagementSystem()
    book = LimitOrderBook(symbol)
    tracker = PositionTracker(starting_cash=risk_params['starting_cash'])
    trades_list = []
    
    # Backtest loop - process signals chronologically
    for _, row in signals_df.iterrows():
        timestamp = row['timestamp']
        signal = row['signal']
        price = row['last_price']
        
        # Calculate position size based on available cash and risk parameters
        if signal == 1:  # Enter long
            side = "buy"
            max_investment = tracker.cash * risk_params['position_fraction']
            quantity = int(max_investment / price)
        elif signal == -1:  # Enter short
            side = "sell"
            # For short selling, use same position sizing logic
            max_investment = tracker.cash * risk_params['position_fraction']
            quantity = int(max_investment / price)
        elif signal == 0:  # Exit position
            # Determine exit direction based on current position
            current_position = tracker.positions.get(symbol, 0)
            if current_position > 0:  # Exit long position
                side = "sell"
                quantity = current_position
            elif current_position < 0:  # Exit short position
                side = "buy"
                quantity = abs(current_position)
            else:
                continue  # No position to exit
        else:
            continue
            
        # Skip if quantity is 0
        if quantity <= 0:
            continue
        
        # Create order
        order = Order(
            id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            quantity=quantity,
            type="market",
            price=None,  # Market order
            timestamp=timestamp
        )
        
        # Submit to OMS
        ack = oms.new_order(order)
        
        # Route to book and tracker
        reports = book.add_order(order)
        for report in reports:
            tracker.update(report)
            trades_list.append(report)
    
    # Calculate final metrics
    current_prices = {symbol: history['last_price'].iloc[-1]}
    summary = tracker.get_pnl_summary(current_prices)
    
    # Compute additional metrics
    blotter_df = tracker.get_blotter()
    if not blotter_df.empty and len(blotter_df) > 1:
        # Calculate equity curve
        blotter_df = blotter_df.copy()
        blotter_df['cumulative_pnl'] = blotter_df['pnl'].cumsum()
        blotter_df['equity'] = risk_params['starting_cash'] + blotter_df['cumulative_pnl']
        
        # Calculate returns
        returns = blotter_df['equity'].pct_change().dropna()
        
        if len(returns) > 1:
            # Sharpe ratio (annualized)
            if returns.std() > 0:
                sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
            else:
                sharpe_ratio = 0
                
            # Maximum drawdown
            peak = blotter_df['equity'].expanding().max()
            drawdown = (blotter_df['equity'] - peak) / peak
            max_drawdown = drawdown.min()
        else:
            sharpe_ratio = 0
            max_drawdown = 0
    else:
        sharpe_ratio = 0
        max_drawdown = 0
    
    # Metrics dictionary
    metrics_dict = {
        "total_return": summary['total_pnl'] / risk_params['starting_cash'],
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "total_pnl": summary['total_pnl'],
        "num_trades": len(trades_list),
        "final_cash": tracker.cash,
        "final_positions": dict(tracker.positions)
    }
    
    return signals_df, trades_list, metrics_dict


# Run the script

