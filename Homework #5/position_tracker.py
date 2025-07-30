import pandas as pd
from typing import List, Dict
from datetime import datetime

class PositionTracker:
    """
    Tracks positions and cash, records trades, and compute P&L.
    """
    def __init__(self, starting_cash: float = 0.0):
        # Net position per symbol (symbol -> shares/contracts)
        self.positions: Dict[str, int] = {}
        # Cash balance
        self.cash: float = starting_cash
        # Starting cash for reference
        self.starting_cash: float = starting_cash
        # List of trade records (one dict per fill)
        self.blotter: List[Dict] = []

    def update(self, report: Dict) -> None:
        """
        Process one execution report with keys:
        - order_id
        - symbol
        - side ('buy' or 'sell')
        - filled_qty (int)
        - price (float)
        - timestamp (datetime)

        Updates positions, cash, and append to the blotter list
        """
        symbol      = report['symbol']
        qty         = report['filled_qty']
        price       = report['price']
        side        = report['side']
        timestamp   = report['timestamp']

        # Update net position
        # A 'buy increases your holding and 'sell' decreases it
        delta = qty if side == 'buy' else -qty
        self.positions[symbol] = self.positions.get(symbol, 0) + delta

        # Update cash
        # Cash flows negative for buys (cash out), positive for sells
        cash_flow = -qty * price if side == 'buy' else qty * price
        self.cash += cash_flow

        # Record in blotter
        # Compute realized P&L later in summary
        self.blotter.append({
            "timestamp":    timestamp,
            "symbol":       symbol,
            "side":         side,
            "quantity":     qty,
            "price":        price,
            "cash_flow":    cash_flow
        })

    def get_blotter(self) -> pd.DataFrame:
        """
        Returns a DataFrame of all fills with columns:
            timestamp, symbol, side, quantity, price, cash_flow
        """
        if not self.blotter:
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=["timestamp", "symbol", "side", "quantity", "price", "cash_flow"])
        return pd.DataFrame(self.blotter)
    
    def get_pnl_summary(self, current_prices: Dict[str, float] = None) -> Dict:
        """
        Returns a dict with:
        - 'realized_pnl': float <- profit/loss from completed trades
        - 'unrealized_pnl': float (0 if no current_prices given) <- mark-to-market value of current holdings
        - 'total_pnl': sum of realized + unrealized
        - 'current_cash': self.cash
        - 'positions': copy of self.positions dict
        """
        blotter_df = self.get_blotter()
        
        # Handle empty blotter case
        if blotter_df.empty:
            realized_pnl = 0.0
        else:
            realized_pnl = blotter_df["cash_flow"].sum()

        unrealized_pnl = 0.0
        if current_prices:
            for sym, pos in self.positions.items():
                if pos != 0:
                    current_price = current_prices.get(sym, 0.0)
                    market_value = pos * current_price  # Fix: initialize market_value

                    # Cost basis = sum of cash flows for this symbol
                    if not blotter_df.empty:
                        trades = blotter_df[blotter_df['symbol'] == sym]
                        cost_basis = -trades['cash_flow'].sum()
                    else:
                        cost_basis = 0

                    unrealized_pnl += market_value - cost_basis

        total_pnl = realized_pnl + unrealized_pnl

        return {
            "realized_pnl":     realized_pnl,
            "unrealized_pnl":   unrealized_pnl,
            "total_pnl":        total_pnl,
            "current_cash":     self.cash,
            "positions":        dict(self.positions)
        }

