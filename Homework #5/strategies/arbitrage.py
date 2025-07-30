import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
from market_data_loader import MarketDataLoader
import uuid
from datetime import datetime
import numpy as np

def run_backtest(history: pd.DataFrame, 
                 symbol1: str = "AAPL",
                 symbol2: str = "AMZN", 
                 threshold: float = 0.02,
                 risk_params: dict = None) -> tuple:
    """
    Cross-asset arbitrage strategy using two correlated assets.
    Trades the spread between two highly correlated assets when it diverges beyond threshold.
    
    Args:
        history: Historical price data DataFrame for the primary asset (symbol1)
        symbol1: Primary asset symbol (e.g., "AAPL", "MSFT", "GOOGL")
        symbol2: Secondary asset symbol for arbitrage pair (e.g., "AMZN", "MSFT", "META")
        threshold: Threshold for arbitrage signal (z-score)
        risk_params: Risk parameters
        
    Returns:
        Tuple of (signals_df, trades_list, metrics_dict)
    """
    
    # Default risk parameters
    if risk_params is None:
        risk_params = {'max_pos': 100}
    
    # Load second asset data to create cross-asset arbitrage
    loader = MarketDataLoader(interval="1d", period="1mo")
    
    # Get second asset history for the same date range as first asset
    start_date = history.index[0].strftime('%Y-%m-%d')
    end_date = history.index[-1].strftime('%Y-%m-%d')
    
    try:
        symbol2_history = loader.get_history(symbol2, start=start_date, end=end_date)
        print(f"Loaded {symbol2} data for arbitrage pair {symbol1}-{symbol2}")
    except Exception as e:
        print(f"Could not load {symbol2} data: {e}")
        # Fallback to creating synthetic second asset
        history = history.copy()
        history['p1'] = history['last_price']
        history['p2'] = history['last_price'].shift(5) + np.random.normal(0, history['last_price'].std()*0.1, len(history))
        df = history.dropna().copy()
    else:
        # Align the two datasets
        asset1_prices = history['last_price']
        asset2_prices = symbol2_history['last_price']
        
        # Create aligned DataFrame with both assets
        df = pd.DataFrame({
            'p1': asset1_prices,  # Primary asset prices
            'p2': asset2_prices   # Secondary asset prices
        }).dropna()
    
    print(f"Loaded {len(df)} aligned data points for {symbol1}-{symbol2} arbitrage")
    
    # Estimate hedge ratio (beta) via linear regression
    if len(df) > 10:
        beta = np.polyfit(df['p2'], df['p1'], 1)[0]
        print(f"Estimated hedge ratio (beta): {beta:.4f}")
    else:
        beta = 1.0
    
    # Compute spread: Asset1 - beta * Asset2
    df['spread'] = df['p1'] - beta * df['p2']
    
    # Calculate rolling mean and std for spread normalization
    spread_mean = df['spread'].rolling(window=20, min_periods=10).mean()
    spread_std = df['spread'].rolling(window=20, min_periods=10).std()
    
    # Generate signals based on spread deviation
    df['signal'] = 0
    
    # Calculate z-score of spread
    z_score = (df['spread'] - spread_mean) / spread_std
    
    # Generate trading signals
    # If spread > +threshold: signal = -1 (sell Asset1, buy Asset2)
    df.loc[z_score > threshold, 'signal'] = -1
    
    # If spread < -threshold: signal = +1 (buy Asset1, sell Asset2)  
    df.loc[z_score < -threshold, 'signal'] = 1
    
    # Exit condition: when spread reverts to within threshold/2
    prev_z_score = z_score.shift(1)
    exit_condition = (abs(z_score) <= threshold/2) & (abs(prev_z_score) > threshold/2)
    df.loc[exit_condition, 'signal'] = 0
    
    print(f"Generated {len(df[df['signal'] != 0])} arbitrage signals")
    
    # Only keep signal changes
    df['signal_change'] = df['signal'] != df['signal'].shift(1)
    signals_df = df[df['signal_change'] & (df['signal'] != 0)].copy()
    
    # Initialize trading components
    oms = OrderManagementSystem()
    tracker = PositionTracker(starting_cash=100000)
    trades_list = []
    
    # Backtest loop - implement both legs of arbitrage
    for timestamp, row in signals_df.iterrows():
        signal = row['signal']
        asset1_price = row['p1']
        asset2_price = row['p2']
        
        if signal == -1:  # Sell spread: sell Asset1, buy Asset2
            # Leg 1: Sell Asset1
            asset1_order = Order(
                id=str(uuid.uuid4()),
                symbol=symbol1,
                side="sell",
                quantity=risk_params.get('max_pos', 100),
                type="limit",
                price=asset1_price,
                timestamp=timestamp
            )
            
            # Leg 2: Buy Asset2 (hedge ratio adjusted)
            asset2_quantity = int(beta * risk_params.get('max_pos', 100))
            asset2_order = Order(
                id=str(uuid.uuid4()),
                symbol=symbol2, 
                side="buy",
                quantity=asset2_quantity,
                type="limit",
                price=asset2_price,
                timestamp=timestamp
            )
            
        elif signal == 1:  # Buy spread: buy Asset1, sell Asset2
            # Leg 1: Buy Asset1
            asset1_order = Order(
                id=str(uuid.uuid4()),
                symbol=symbol1,
                side="buy",
                quantity=risk_params.get('max_pos', 100),
                type="limit",
                price=asset1_price,
                timestamp=timestamp
            )
            
            # Leg 2: Sell Asset2 (hedge ratio adjusted)
            asset2_quantity = int(beta * risk_params.get('max_pos', 100))
            asset2_order = Order(
                id=str(uuid.uuid4()),
                symbol=symbol2,
                side="sell", 
                quantity=asset2_quantity,
                type="limit",
                price=asset2_price,
                timestamp=timestamp
            )
            
        elif signal == 0:  # Exit both positions
            # Close Asset1 position
            asset1_pos = tracker.positions.get(symbol1, 0)
            if asset1_pos != 0:
                asset1_order = Order(
                    id=str(uuid.uuid4()),
                    symbol=symbol1,
                    side="sell" if asset1_pos > 0 else "buy",
                    quantity=abs(asset1_pos),
                    type="limit",
                    price=asset1_price,
                    timestamp=timestamp
                )
            else:
                asset1_order = None
                
            # Close Asset2 position  
            asset2_pos = tracker.positions.get(symbol2, 0)
            if asset2_pos != 0:
                asset2_order = Order(
                    id=str(uuid.uuid4()),
                    symbol=symbol2,
                    side="sell" if asset2_pos > 0 else "buy",
                    quantity=abs(asset2_pos),
                    type="limit", 
                    price=asset2_price,
                    timestamp=timestamp
                )
            else:
                asset2_order = None
        else:
            continue

        # Execute both legs
        for order in [asset1_order, asset2_order]:
            if order is not None:
                # Submit to OMS
                ack = oms.new_order(order)

                # Create execution report
                execution_report = {
                    "order_id": order.id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "filled_qty": order.quantity,
                    "price": order.price,
                    "timestamp": timestamp,
                    "status": "filled"
                }
                
                # Update tracker and add to trades
                tracker.update(execution_report)
                trades_list.append(execution_report)

    # After processing all signals, calculate metrics
    current_prices = {
        symbol1: df['p1'].iloc[-1] if len(df) > 0 else 0,
        symbol2: df['p2'].iloc[-1] if len(df) > 0 else 0
    }
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
    
    # Return signals with additional spread info for analysis
    signals_with_spread = signals_df.copy()
    if 'spread' in df.columns:
        signals_with_spread['spread'] = df.loc[signals_df.index, 'spread']
    
    print(f"{symbol1}-{symbol2} arbitrage backtest complete: {len(trades_list)} total trades executed")
    
    return (signals_with_spread, trades_list, metrics_dict)