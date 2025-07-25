from position_tracker import PositionTracker
from datetime import datetime, timezone

# Simulate a buy
print("=== Simulating a Buy ===")
pt = PositionTracker(starting_cash=10000)
rpt = {
    "order_id":     "1",
    "symbol":       "AAPL",
    "side":         "buy",
    "filled_qty":   10,
    "price":        150.0,
    "timestamp":    datetime.now(tz=timezone.utc)
}

pt.update(rpt)
print(pt.cash)      # 10000 - 1500 = 8500
print(pt.positions) # {'AAPL': 10}

# Simulate a sell
print("\n=== Simulating a Sell ===")

rpt2 = rpt.copy()
rpt2["side"] = "sell"
pt.update(rpt2)
print(pt.cash)        # 8500 + 1500 = 10000
print(pt.positions)   # {'AAPL': 0}

# Get P&L summary
summary = pt.get_pnl_summary(current_prices={'AAPL': 155.0})
print(summary)