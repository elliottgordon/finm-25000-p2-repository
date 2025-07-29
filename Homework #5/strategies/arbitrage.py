import pandas as pd
from order import Order
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
import uuid
from datetime import datetime
from market_data_loader import MarketDataLoader
import numpy as np
from typing import Tuple

def run_backtest(
    symbol1: str,
    symbol2: str,
    interval: str,
    period: str,
    threshold: float,
    risk_params: dict = None,
    transaction_cost: float = 0.0
) -> Tuple[pd.DataFrame, list, dict]:
    """
    Cross-asset arbitrage strategy: trade the spread between two correlated assets.
    """
    if risk_params is None:
        risk_params = {
            'starting_cash': 100000,
            'position_fraction': 0.1  # Use 10% of cash per trade
        }

    # Load market data for both assets
    mdl = MarketDataLoader(interval=interval, period=period)
    hist1 = mdl.get_history(symbol1)
    hist2 = mdl.get_history(symbol2)

    # Align histories
    df = pd.DataFrame({
        'p1': hist1['last_price'],
        'p2': hist2['last_price']
    }).dropna()

    # Estimate hedge ratio (beta)
    beta = np.polyfit(df['p2'], df['p1'], 1)[0]
    df['spread'] = df['p1'] - beta * df['p2']

    # Generate signals
    df['signal'] = 0
    df['position'] = 0  # 0 = flat, 1 = long spread, -1 = short spread
    for i in range(1, len(df)):
        spread = df['spread'].iloc[i]
        prev_spread = df['spread'].iloc[i-1]
        prev_position = df['position'].iloc[i-1]

        # Entry signals
        if prev_position == 0:
            if spread > threshold and prev_spread <= threshold:
                # Sell spread: sell asset1, buy asset2
                df.iloc[i, df.columns.get_loc('signal')] = -1
                df.iloc[i, df.columns.get_loc('position')] = -1
            elif spread < -threshold and prev_spread >= -threshold:
                # Buy spread: buy asset1, sell asset2
                df.iloc[i, df.columns.get_loc('signal')] = 1
                df.iloc[i, df.columns.get_loc('position')] = 1
            else:
                df.iloc[i, df.columns.get_loc('position')] = prev_position
        # Exit signals
        elif prev_position == 1:
            if abs(spread) <= threshold and abs(prev_spread) > threshold:
                # Exit long spread
                df.iloc[i, df.columns.get_loc('signal')] = 0
                df.iloc[i, df.columns.get_loc('position')] = 0
            else:
                df.iloc[i, df.columns.get_loc('position')] = prev_position
        elif prev_position == -1:
            if abs(spread) <= threshold and abs(prev_spread) > threshold:
                # Exit short spread
                df.iloc[i, df.columns.get_loc('signal')] = 0
                df.iloc[i, df.columns.get_loc('position')] = 0
            else:
                df.iloc[i, df.columns.get_loc('position')] = prev_position
        else:
            df.iloc[i, df.columns.get_loc('position')] = prev_position

    # Only keep non-zero signals
    signals_df = df[df['signal'] != 0].copy()
    signals_df = signals_df.reset_index()

    # Initialize trading components for both assets
    oms1 = OrderManagementSystem()
    oms2 = OrderManagementSystem()
    book1 = LimitOrderBook(symbol1)
    book2 = LimitOrderBook(symbol2)
    tracker = PositionTracker(starting_cash=risk_params['starting_cash'])
    trades_list = []

    # Backtest loop
    for _, row in signals_df.iterrows():
        timestamp = row['timestamp']
        signal = row['signal']
        price1 = row['p1']
        price2 = row['p2']

        # Position sizing: use same cash fraction for both legs
        max_investment = tracker.cash * risk_params['position_fraction']
        qty1 = int(max_investment / price1)
        qty2 = int(max_investment / price2)
        if qty1 <= 0 or qty2 <= 0:
            continue

        # Determine order sides
        if signal == 1:  # Buy spread: buy asset1, sell asset2
            side1 = 'buy'
            side2 = 'sell'
        elif signal == -1:  # Sell spread: sell asset1, buy asset2
            side1 = 'sell'
            side2 = 'buy'
        elif signal == 0:  # Exit both
            pos1 = tracker.positions.get(symbol1, 0)
            pos2 = tracker.positions.get(symbol2, 0)
            # Exit both legs
            if pos1 > 0:
                side1 = 'sell'
                qty1 = pos1
            elif pos1 < 0:
                side1 = 'buy'
                qty1 = abs(pos1)
            else:
                side1 = None
                qty1 = 0
            if pos2 > 0:
                side2 = 'sell'
                qty2 = pos2
            elif pos2 < 0:
                side2 = 'buy'
                qty2 = abs(pos2)
            else:
                side2 = None
                qty2 = 0
            if (qty1 == 0 and qty2 == 0):
                continue
        else:
            continue

        # Create and submit orders for both legs
        orders = []
        if qty1 > 0 and side1:
            order1 = Order(
                id=str(uuid.uuid4()),
                symbol=symbol1,
                side=side1,
                quantity=qty1,
                type="market",
                price=None,
                timestamp=timestamp
            )
            ack1 = oms1.new_order(order1)
            reports1 = book1.add_order(order1)
            for rpt in reports1:
                # Subtract transaction cost from P&L
                if 'pnl' in rpt:
                    rpt['pnl'] -= transaction_cost
                tracker.update(rpt)
                trades_list.append(rpt)
        if qty2 > 0 and side2:
            order2 = Order(
                id=str(uuid.uuid4()),
                symbol=symbol2,
                side=side2,
                quantity=qty2,
                type="market",
                price=None,
                timestamp=timestamp
            )
            ack2 = oms2.new_order(order2)
            reports2 = book2.add_order(order2)
            for rpt in reports2:
                if 'pnl' in rpt:
                    rpt['pnl'] -= transaction_cost
                tracker.update(rpt)
                trades_list.append(rpt)

    # Calculate final metrics
    last_price1 = df['p1'].iloc[-1]
    last_price2 = df['p2'].iloc[-1]
    current_prices = {symbol1: last_price1, symbol2: last_price2}
    summary = tracker.get_pnl_summary(current_prices)

    # Compute additional metrics
    blotter_df = tracker.get_blotter()
    if not blotter_df.empty and len(blotter_df) > 1:
        blotter_df = blotter_df.copy()
        blotter_df['cumulative_pnl'] = blotter_df['pnl'].cumsum()
        blotter_df['equity'] = risk_params['starting_cash'] + blotter_df['cumulative_pnl']
        returns = blotter_df['equity'].pct_change().dropna()
        if len(returns) > 1 and returns.std() > 0:
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
            peak = blotter_df['equity'].expanding().max()
            drawdown = (blotter_df['equity'] - peak) / peak
            max_drawdown = drawdown.min()
        else:
            sharpe_ratio = 0
            max_drawdown = 0
    else:
        sharpe_ratio = 0
        max_drawdown = 0

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