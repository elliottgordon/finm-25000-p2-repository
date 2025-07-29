from market_data_loader import MarketDataLoader
from oms import OrderManagementSystem
from order_book import LimitOrderBook
from position_tracker import PositionTracker
from strategies.trend_following   import run_backtest as tf_backtest
from strategies.mean_reversion    import run_backtest as mr_backtest
from strategies.arbitrage         import run_backtest as arb_backtest

loader = MarketDataLoader(interval="5m", period="1mo")
oms = OrderManagementSystem()
tracker = PositionTracker()
book = LimitOrderBook("AAPL")
hist = loader.get_history("AAPL", start="2025-06-01", end="2025-07-01")
signals, trades, metrics = tf_backtest(hist, short_win=10, long_win=50, risk_params={"max_pos":100})
for trade in trades:
    # trade = {"timestamp":..., "symbol":..., "side":..., "qty":..., "price":...}
    order = Order(**trade)
    oms_ack = oms.new_order(order)
    exec_reports = book.add_order(order)
    for rpt in exec_reports:
        tracker.update(rpt)
blotter = tracker.get_blotter()  # DataFrame with columns: timestamp, symbol, side, qty, price, pnl
blotter["cum_pnl"] = blotter["pnl"].cumsum()
blotter.set_index("timestamp")["cum_pnl"].plot(figsize=(10,5), title="Equity Curve")
print(f"**Total Return:**  {metrics['total_return']:.2%}")
print(f"**Max Drawdown:**  {metrics['max_drawdown']:.2%}")
print(f"**Sharpe Ratio:**  {metrics['sharpe_ratio']:.2f}")