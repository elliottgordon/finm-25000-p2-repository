# oms.py

from order import Order
from datetime import datetime
from typing import Dict, Optional

class OrderManagementSystem:
    """
    Validates, tracks, and optionally routes orders.
    """
    def __init__(self, matching_engine=None):
        # store orders (Order objects) and statuses by order ID
        self._orders: Dict[str, Order]  = {}
        self._statuses: Dict[str, str]  = {}
        # optional matching engine to forward orders
        self.matching_engine = matching_engine

    def new_order(self, order: Order) -> str:
        """
        Validates and stores a new order.

        Arguments:
            order (Order): The order to be processed

        Returns:
            str: The unique identifier for the order
        """
        # Basic field checks
        if order.side not in ['buy', 'sell']:
            raise ValueError("Order side must be 'buy' or 'sell'")
        if order.quantity <= 0:
            raise ValueError("Order quantity must be greater than 0")
        if order.type not in ['market', 'limit', 'stop']:
            raise ValueError("Order type must be 'market', 'limit', or 'stop'")
        if order.type in ("limit", "stop") and order.price is None:
            raise ValueError("Limit/stop orders must have a price specified")
        
        # Timestamp if not provided
        now = datetime.utcnow()
        order.timestamp = order.timestamp or now

        # Save order & status
        self._orders[order.id] = order
        self._statuses[order.id] = 'accepted'

        # Forward to matching engine
        if self.matching_engine:
            self.matching_engine.add_order(order)

        return {"order_id": order.id, 
                "status": "accepted",
                "timestamp": order.timestamp
                }