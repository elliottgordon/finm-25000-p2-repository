# demo_market_data_report_no_tabulate.py

"""
Demo script to showcase MarketDataLoader outputs in a clean, readable format
without relying on tabulate or markdown. Uses pandas.DataFrame.to_string().
"""

import pandas as pd
from datetime import datetime
from market_data_loader import MarketDataLoader


def print_section(title: str):
    """Prints a section header surrounded by underlines."""
    print(f"\n{title}")
    print("-" * len(title))


def pretty_table(
    df: pd.DataFrame,
    caption: str = None,
    float_format: str = "{:.6f}"
):
    """
    Prints DataFrame as an aligned ASCII table.
    - df: must have reset_index() applied if you want the index shown as a column.
    - caption: optional text printed above the table.
    - float_format: Python format string for floats.
    """
    if caption:
        print(f"\n{caption}")
    fmtters = {
        col: (lambda x, fmt=float_format: fmt.format(x))
        for col in df.select_dtypes(include=["float", "float64"]).columns
    }
    print(
        df.to_string(
            index=False,
            formatters=fmtters,
            na_rep="",
            justify="left"
        )
    )


def main():
    loader = MarketDataLoader(interval="5m", period="1mo")

    # 1) Fixed-period history (last 1 month) for AAPL
    print_section("1) Fixed-period History: AAPL (Last 1 Month)")
    hist = loader.get_history("AAPL")
    df1 = hist.head(5).reset_index().rename(columns={"index": "timestamp"})
    df2 = hist.tail(5).reset_index().rename(columns={"index": "timestamp"})
    pretty_table(df1, caption="First 5 rows of AAPL history")
    pretty_table(df2, caption="Last 5 rows of AAPL history")

    # 2) Explicit date range for AAPL
    print_section("2) Explicit Date Range: 2025-06-15 → 2025-07-01 for AAPL")
    start, end = "2025-06-15", "2025-07-01"
    hist_range = loader.get_history("AAPL", start=start, end=end)
    print(f"\nRange Start: {hist_range.index.min()}  Range End: {hist_range.index.max()}\n")
    df3 = hist_range.head(5).reset_index().rename(columns={"index": "timestamp"})
    df4 = hist_range.tail(5).reset_index().rename(columns={"index": "timestamp"})
    pretty_table(df3, caption="First 5 rows of range")
    pretty_table(df4, caption="Last 5 rows of range")

    # 3) EURUSD Price & Bid/Ask lookup
    print_section("3) EURUSD=X Price & Bid/Ask @ 2025-07-01 15:30 UTC")
    ts = datetime(2025, 7, 1, 15, 30)
    price = loader.get_price("EURUSD=X", ts)
    bid, ask = loader.get_bid_ask("EURUSD=X", ts)
    print(f"\nPrice:  {price:.6f}")
    print(f"Bid:    {bid:.6f}")
    print(f"Ask:    {ask:.6f}\n")

    # 4) BTC-USD Volume over custom window
    print_section("4) BTC-USD Volume: 2025-06-30 → 2025-07-01")
    vol = loader.get_volume("BTC-USD", start="2025-06-30", end="2025-07-01")
    print(f"\nTotal Volume (UTC): {vol:,}\n")

    # 5) AAPL Option Chain (nearest expiry)
    print_section("5) AAPL Option Chain (nearest available expiry)")
    opts = loader.get_options_chain("AAPL", expiry="2025-07-25")
    calls = opts["calls"].sort_values("strike").head(5).reset_index(drop=True)
    puts  = opts["puts"].sort_values("strike").head(5).reset_index(drop=True)
    pretty_table(calls, caption="Top 5 AAPL Calls")
    pretty_table(puts,  caption="Top 5 AAPL Puts")


if __name__ == "__main__":
    main()