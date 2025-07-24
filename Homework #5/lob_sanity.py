from order import Order
from order_book import LimitOrderBook

print("=== 1. Basic Sanity ===")
lob = LimitOrderBook("AAPL")
buy = Order("1", "AAPL", "buy", 10, "limit", 150.0)  # Fixed typo: APPL -> AAPL
sell = Order("2", "AAPL", "sell", 5, "limit", 149.0)
print("Adding buy order:", lob.add_order(buy))   # no match, book holds the buy
print("Adding sell order:", lob.add_order(sell))  # matches 5@149.0, leaves 5 buy
print("Book state - bids:", [(o.id, o.price, o.quantity) for o in lob.bids], "asks:", [(o.id, o.price, o.quantity) for o in lob.asks])

print("\n=== 2. Market Order ===")
mk = Order("3","AAPL","sell",5,"market")  # Changed to sell to consume the remaining bid
print("Adding market sell order:", lob.add_order(mk))   # eats the remaining 5@150.0; then book empty
print("Book state after market order - bids:", lob.bids, "asks:", lob.asks)

print("\n=== 3. Edge Case: Multiple Resting Orders at Same Price (Time Priority) ===")
lob2 = LimitOrderBook("AAPL")

# Add multiple buy orders at the same price - should maintain time priority
buy1 = Order("B1", "AAPL", "buy", 20, "limit", 100.0)
buy2 = Order("B2", "AAPL", "buy", 30, "limit", 100.0)  # Same price, later time
buy3 = Order("B3", "AAPL", "buy", 15, "limit", 100.0)  # Same price, even later

print("Adding first buy order at $100:", lob2.add_order(buy1))
print("Adding second buy order at $100:", lob2.add_order(buy2))
print("Adding third buy order at $100:", lob2.add_order(buy3))
print("Bid book (should be in time order):", [(o.id, o.price, o.quantity) for o in lob2.bids])

# Now add a sell order that partially fills multiple orders
sell_big = Order("S1", "AAPL", "sell", 35, "limit", 100.0)  # Should fill B1 completely (20) and B2 partially (15)
print("\nAdding sell order for 35 shares at $100:")
reports = lob2.add_order(sell_big)
for i, report in enumerate(reports):
    print(f"  Report {i+1}: Order {report['order_id']} ({report['side']}) filled {report['filled_qty']} at ${report['price']} - {report['status']}")

print("Bid book after partial fill:", [(o.id, o.price, o.quantity) for o in lob2.bids])
print("Ask book:", [(o.id, o.price, o.quantity) for o in lob2.asks])

print("\n=== 4. Edge Case: Partial Fills on Both Sides ===")
lob3 = LimitOrderBook("AAPL")

# Set up a more complex scenario with multiple orders on both sides
asks = [
    Order("A1", "AAPL", "sell", 10, "limit", 105.0),
    Order("A2", "AAPL", "sell", 15, "limit", 106.0),
    Order("A3", "AAPL", "sell", 20, "limit", 107.0)
]

bids = [
    Order("B1", "AAPL", "buy", 25, "limit", 104.0),
    Order("B2", "AAPL", "buy", 30, "limit", 103.0)
]

# Add all resting orders
for ask in asks:
    lob3.add_order(ask)
for bid in bids:
    lob3.add_order(bid)

print("Initial book state:")
print("  Bids:", [(o.id, o.price, o.quantity) for o in lob3.bids])
print("  Asks:", [(o.id, o.price, o.quantity) for o in lob3.asks])

# Add a large buy order that will cross multiple ask levels
big_buy = Order("BB1", "AAPL", "buy", 40, "limit", 106.5)  # Should fill A1 completely, A2 completely, and partially fill A3
print(f"\nAdding large buy order: 40 shares at $106.50")
reports = lob3.add_order(big_buy)

print("Execution reports:")
for i, report in enumerate(reports):
    print(f"  Report {i+1}: Order {report['order_id']} ({report['side']}) filled {report['filled_qty']} at ${report['price']} - {report['status']}")

print("\nFinal book state:")
print("  Bids:", [(o.id, o.price, o.quantity) for o in lob3.bids])
print("  Asks:", [(o.id, o.price, o.quantity) for o in lob3.asks])

print("\n=== 5. Edge Case: Large Market Order Consuming Multiple Levels ===")
lob4 = LimitOrderBook("AAPL")

# Set up deep book
deep_asks = [
    Order("DA1", "AAPL", "sell", 100, "limit", 200.0),
    Order("DA2", "AAPL", "sell", 200, "limit", 200.5),
    Order("DA3", "AAPL", "sell", 150, "limit", 201.0),
    Order("DA4", "AAPL", "sell", 300, "limit", 201.5)
]

for ask in deep_asks:
    lob4.add_order(ask)

print("Deep ask book setup:")
print("  Asks:", [(o.id, o.price, o.quantity) for o in lob4.asks])

# Large market buy that consumes multiple levels
huge_market_buy = Order("HMB1", "AAPL", "buy", 500, "market")  # Should consume multiple price levels
print(f"\nAdding huge market buy order: 500 shares")
reports = lob4.add_order(huge_market_buy)

print("Execution reports:")
for i, report in enumerate(reports):
    print(f"  Report {i+1}: Order {report['order_id']} ({report['side']}) filled {report['filled_qty']} at ${report['price']} - {report['status']}")

print("\nFinal book state:")
print("  Bids:", [(o.id, o.price, o.quantity) for o in lob4.bids])
print("  Asks:", [(o.id, o.price, o.quantity) for o in lob4.asks])