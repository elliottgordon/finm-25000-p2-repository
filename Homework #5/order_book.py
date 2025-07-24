from datetime import datetime, timezone
from typing import List, Dict
from order import Order

class LimitOrderBook:
    """
    A simple price-time priority limit order book.
    """
    def __init__(self, symbol: str):
        """
        Key Note:
            self.bids: resting buy orders, sorted by price, then time of entry
            self.asks: resting sell orders, sorted by ascending price, then time of entry
        """
        self.symbol = symbol
        # bids: list of resting buy orders, highest price first, then earliest timestamp
        self.bids: List[Order] = []
        # asks: list of resting sell orders, lowest price first, then earliest timestamp
        self.asks: List[Order] = []
        self.last_update: datetime = datetime.now(timezone.utc)

    def add_order(self, order: Order) -> List[Dict]:
        """
        Handle a new incoming order (market, limit, or stop).
            market orders: consume liquidity until filled or book empties
            limit orders: first match, then any unfilled remainder joins the book
            stop orders: left as an exercise (trigger them externally and then tream them as limit)

        Returns a list of execution report dictionaries.
        """
        # Ensure order has a timestamp for time priority
        if order.timestamp is None:
            order.timestamp = datetime.now(timezone.utc)
            
        reports = []

        if order.type == "market":
            reports += self._execute_market(order)

        elif order.type == "limit":
            # try to match immediately
            reports += self._match_limit(order)
            # if there's leftover quantity, add to book
            if order.quantity > 0:
                self._insert_resting(order)
        
        else:
            # for now, treat stop orders as plain limit orders
            # once triggered by strategy logic
            reports += self._match_limit(order)
            if order.quantity > 0:
                self._insert_resting(order)

        return reports
    
    def _match_limit(self, order: Order) -> List[Dict]:
        """
        Match a limit order against the book.
        Fill as much as possible as prices satisfying the limit.

        Key points:
            always look at opposite[0], the best price
            stop when either the incoming order is filled or the price condition fails
            generate two execution reports for each match: one for the aggressor, one for the resting order
            remove fully filled resting orders from the book
        """
        reports = []

        # choose opposite side
        opposite = self.asks if order.side == "buy" else self.bids

        # continue matching while we still have quantity
        # and there is a resting order that satisfies the price
        while order.quantity > 0 and opposite:
            best = opposite[0]

            ## price check that is unique to a limit order
            # buy order matches if best ask <= order.price
            if order.side == "buy" and best.price > order.price:
                break
            # sell order matches if best bid >= order.price
            if order.side == "sell" and best.price < order.price:
                break

            # a fill occurs: trade quantity = min(incoming, resting)
            fill_qty = min(order.quantity, best.quantity)
            trade_price = best.price
            timestamp = datetime.now(timezone.utc)

            # build exectution report for the incoming order
            reports.append({
                "order_id":     order.id,
                "symbol":       order.symbol,
                "side":         order.side,
                "filled_qty":   fill_qty,
                "price":        trade_price,
                "timestamp":    timestamp,
                "status":       "filled" if fill_qty == order.quantity else "partial_fill"       
            })

            #also build report for the resting order
            reports.append({
                "order_id":      best.id,
                "symbol":        best.symbol,
                "side":          best.side,
                "filled_qty":    fill_qty,
                "price":         trade_price,
                "timestamp":     timestamp,
                "status":        "filled" if fill_qty == best.quantity else "partial_fill"
            })

            # decrement quantities
            order.quantity -= fill_qty
            best.quantity -= fill_qty

            # remove resting order if fully filled
            if best.quantity == 0:
                opposite.pop(0)

        return reports
    
    def _execute_market(self, order: Order) -> List[Dict]:
        """
        Fill a market order agains the full depth of the book

        Market orders are indentical to limit orders but without a price constraint
        """
        reports = []
        # opposite side = asks if buy; bids if sell
        opposite = self.asks if order.side == "buy" else self.bids

        while order.quantity > 0 and opposite:
            best = opposite[0]
            fill_qty = min(order.quantity, best.quantity)
            trade_price = best.price
            timestamp = datetime.now(timezone.utc)

            reports.append({
                "order_id":   order.id,
                "symbol":     order.symbol,
                "side":       order.side,
                "filled_qty": fill_qty,
                "price":      trade_price,
                "timestamp":  timestamp,
                "status":     "filled" if fill_qty == order.quantity else "partial_fill"
            })

            reports.append({
                "order_id":   best.id,
                "symbol":     best.symbol,
                "side":       best.side,
                "filled_qty": fill_qty,
                "price":      trade_price,
                "timestamp":  timestamp,
                "status":     "filled" if fill_qty == best.quantity else "partial_fill"
            })

            order.quantity -= fill_qty
            best.quantity -= fill_qty

            if best.quantity == 0:
                opposite.pop(0)
        
        return reports

    def _insert_resting(self, order: Order):
        """
        Place a remainder limit order into bids or asks,
        maintaining sorted order by price, then time.
        """
        book = self.bids if order.side == "buy" else self.asks

        # find insertion index
        idx = 0
        while idx < len(book):
            # for bids: descending price; for asks: ascending price
            better_price = (book[idx].price > order.price) if order.side == "buy" \
                            else (book[idx].price < order.price)
            
            # same price: check time priority (earlier timestamp goes first)
            same_price = (book[idx].price == order.price)
            
            if better_price:
                idx += 1
            elif same_price:
                # For same price, maintain time priority: earlier orders go first
                # Keep looking for the right position among same-price orders
                while (idx < len(book) and 
                       book[idx].price == order.price and 
                       (book[idx].timestamp is None or 
                        order.timestamp is None or 
                        book[idx].timestamp <= order.timestamp)):
                    idx += 1
                break
            else:
                break

        # insert at idx
        book.insert(idx, order)