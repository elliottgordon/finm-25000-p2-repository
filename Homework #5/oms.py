# oms.py

from order import Order
from datetime import datetime, timezone
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

    def new_order(self, order: Order) -> dict:
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
        now = datetime.now(tz=timezone.utc)
        order.timestamp = order.timestamp or now

        # Save order & status
        self._orders[order.id] = order
        self._statuses[order.id] = 'accepted'

        # Forward to matching engine
        if self.matching_engine:
            self.matching_engine.add_order(order)

        # Acknowledge order
        return {"order_id": order.id, 
                "status": "accepted",
                "timestamp": order.timestamp
                }

    def cancel_order(self, order_id: str) -> dict:
        """
        Cancels an existing order.

        Arguments:
            order_id (str): The unique identifier for the order to be canceled

        Returns:
            Dict[str, str]: Acknowledgment of the cancellation
        """
        if order_id not in self._orders:
            raise KeyError("Order not found")

        status = self._statuses[order_id]
        if status in ("canceled", "filled"):
            raise ValueError(f"Cannot cancel order in status {status}")
        self._statuses[order_id] = 'canceled'

        return{
            "order_id": order_id,
            "status": "canceled",
            "timestamp": datetime.now(tz=timezone.utc)
        }

    def amend_order(self, order_id: str, new_qty: Optional[int] = None, new_price: Optional[float] = None) -> dict:
        """
        Amends an existing order.

        Arguments:
            order_id (str): The unique identifier for the order to be amended
            new_order (Order): The new order details

        Returns:
            Dict[str, str]: Acknowledgment of the amendment
        """
        if order_id not in self._orders:
            raise KeyError("Order not found")

        status = self._statuses[order_id]

        if status not in ("accepted"):
            raise ValueError(f"Cannot amend order in status {status}")

        order = self._orders[order_id]

        if new_qty is not None:
            if new_qty <= 0:
                raise ValueError("Quantity must be greater than 0")
            order.quantity = new_qty
        if new_price is not None:
            if order.type not in ("limit", "stop"):
                raise ValueError("Only limit or stop orders can change price")
            order.price = new_price
        
        order.timestamp = datetime.now(tz=timezone.utc)

        return {
            "order_id": order.id,
            "status": "amended",
            "timestamp": order.timestamp
        }