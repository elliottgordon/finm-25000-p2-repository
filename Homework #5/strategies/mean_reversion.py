import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime

def run_backtest(history: pd.DataFrame, 
                 symbol: str = "AAPL",
                 bollinger_win: int = 20, 
                 num_std: float = 2.0,
                 risk_params: dict = None) -> tuple:
    """
    Mean reversion strategy using Bollinger Bands.
    
    Args:
        history: Historical price data DataFrame with 'timestamp' and 'last_price' columns
        symbol: Stock symbol to trade (e.g., "AAPL", "MSFT", "GOOGL")
        bollinger_win: Window for Bollinger Bands calculation
        num_std: Number of standard deviations for bands
        risk_params: Risk parameters for position sizing
        
    Returns:
        Tuple of (signals_df, trades_list, metrics_dict)
    """
    
    # Default risk parameters
    if risk_params is None:
        risk_params = {'max_pos': 100}
    
    # Copy history to avoid modifying original
    history = history.copy()
    
    # Compute Bollinger Bands
    m = history["last_price"].rolling(bollinger_win).mean()
    s = history["last_price"].rolling(bollinger_win).std()
    history["upper"] = m + num_std * s
    history["lower"] = m - num_std * s
    history["mid"] = m
    
    # Initialize signal column
    history['signal'] = 0
    
    # Generate signals based on Bollinger Band crossings
    # Use shift() to detect actual crossovers
    prev_price = history['last_price'].shift(1)
    
    # If price crosses below lower: signal = +1 (enter long)
    cross_below_lower = (history['last_price'] <= history['lower']) & (prev_price > history['lower'].shift(1))
    history.loc[cross_below_lower, 'signal'] = 1
    
    # If price crosses above upper: signal = -1 (enter short)  
    cross_above_upper = (history['last_price'] >= history['upper']) & (prev_price < history['upper'].shift(1))
    history.loc[cross_above_upper, 'signal'] = -1
    
    # If price crosses back to mid: signal = 0 (exit position)
    cross_to_mid_from_below = (history['last_price'] >= history['mid']) & (prev_price < history['mid'].shift(1))
    cross_to_mid_from_above = (history['last_price'] <= history['mid']) & (prev_price > history['mid'].shift(1))
    history.loc[cross_to_mid_from_below | cross_to_mid_from_above, 'signal'] = 0
    
    # Only keep non-zero signals with valid Bollinger Bands
    valid_bands = ~(history['upper'].isna() | history['lower'].isna() | history['mid'].isna())
    signals_mask = (history['signal'] != 0) & valid_bands
    signals_df = history[signals_mask].copy()
    
    # Initialize trading components
    oms = OrderManagementSystem()
    book = LimitOrderBook(symbol)
    tracker = PositionTracker(starting_cash=100000)
    trades_list = []
    
    # Backtest loop - iterate through signals in chronological order
    for timestamp, row in signals_df.iterrows():
        signal = row['signal']
        price = row['last_price']
        
        # Determine trade side and quantity based on signal
        if signal == 1:  # Buy signal - price hit lower band
            side = "buy"
            quantity = risk_params.get('max_pos', 100)
        elif signal == -1:  # Sell signal - price hit upper band
            side = "sell"
            quantity = risk_params.get('max_pos', 100)
        else:
            continue

        # Create order
        order = Order(
            id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            quantity=quantity,
            type="limit",
            price=price,
            timestamp=timestamp
        )

        # Submit to OMS
        ack = oms.new_order(order)

        # For backtesting, create direct execution report
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
