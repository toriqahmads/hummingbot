import copy

from async_timeout import timeout
from decimal import Decimal
from typing import Any, Optional, Tuple

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate


GET_GATEWAY_EX_ORDER_ID_TIMEOUT = 30  # seconds


class GatewayInFlightOrder(InFlightOrder):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 creation_timestamp: float,
                 gas_price: Optional[Decimal] = Decimal("0"),
                 initial_state: str = OrderState.PENDING_CREATE):
        super().__init__(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            creation_timestamp=creation_timestamp,
            initial_state=initial_state
        )
        self._gas_price = gas_price
        self._nonce: int = 0
        self._cancel_tx_hash: Optional[str] = None

    @property
    def gas_price(self) -> Decimal:
        return self._gas_price

    @gas_price.setter
    def gas_price(self, gas_price: Decimal):
        self._gas_price = gas_price

    @property
    def nonce(self) -> int:
        return self._nonce

    @nonce.setter
    def nonce(self, nonce):
        self._nonce = nonce

    @property
    def cancel_tx_hash(self) -> Optional[str]:
        return self._cancel_tx_hash

    @cancel_tx_hash.setter
    def cancel_tx_hash(self, cancel_tx_hash):
        self._cancel_tx_hash = cancel_tx_hash

    async def get_exchange_order_id(self) -> Optional[str]:
        """
        Overridden from parent class because blockchain orders take more time than ones from CEX.
        """
        if self.exchange_order_id is None:
            async with timeout(GET_GATEWAY_EX_ORDER_ID_TIMEOUT):
                await self.exchange_order_id_update_event.wait()
        return self.exchange_order_id

    @property
    def attributes(self) -> Tuple[Any]:
        return copy.deepcopy(
            (
                self.client_order_id,
                self.trading_pair,
                self.order_type,
                self.trade_type,
                self.price,
                self.amount,
                self.exchange_order_id,
                self.current_state,
                self.leverage,
                self.position,
                self.executed_amount_base,
                self.executed_amount_quote,
                self.creation_timestamp,
                self.last_update_timestamp,
                self.nonce,
                self.gas_price,
                self.cancel_tx_hash,
            )
        )

    @property
    def is_pending_approval(self) -> bool:
        return self.current_state in {OrderState.PENDING_APPROVAL}

    @property
    def is_approval_request(self) -> bool:
        """
        A property attribute that returns `True` if this `GatewayInFlightOrder` is in fact a token approval request.

        :return: True if this `GatewayInFlightOrder` is in fact a token approval request, otherwise it returns False
        :rtype: bool
        """
        return self.current_state in {OrderState.PENDING_APPROVAL, OrderState.APPROVED}

    def update_with_order_update(self, order_update: OrderUpdate) -> bool:
        """
        Updates the in flight order with an order update
        return: True if the order gets updated otherwise False
        """
        if (order_update.client_order_id != self.client_order_id
                and order_update.exchange_order_id != self.exchange_order_id):
            return False

        prev_data = self.attributes

        if self.exchange_order_id is None and order_update.exchange_order_id is not None:
            self.update_exchange_order_id(order_update.exchange_order_id)

        self.current_state = order_update.new_state
        self.nonce = order_update.misc_updates.get("nonce", None)
        self.fee_asset = order_update.misc_updates.get("fee_asset", None)
        self.gas_price = order_update.misc_updates.get("gas_price", None)

        updated: bool = prev_data != self.attributes

        if updated:
            self.last_update_timestamp = order_update.update_timestamp

        return updated
