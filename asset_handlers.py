#!/usr/bin/env python3
"""
Asset Handlers for Trading Engine

Concrete implementations of AssetHandler interface that wrap existing
order handlers to work with the universal trading engine.
"""

import logging
from typing import List, Optional, Dict
from datetime import datetime

from trading_engine import (
    AssetHandler, AssetType, TradingSignal, Position, OrderResult,
    OrderAction, OrderType
)
from order_handler import OrderHandler
from options_data_handler import OptionsDataHandler
from connection_manager import get_all_positions


class EquityHandler(AssetHandler):
    """
    Asset handler for equity trading
    Wraps the existing OrderHandler for equity operations
    """
    
    def __init__(self):
        self.order_handler = OrderHandler()
        self.logger = logging.getLogger("equity_handler")
        self.logger.info("EquityHandler initialized")
    
    def get_asset_type(self) -> AssetType:
        """Return the asset type this handler manages"""
        return AssetType.EQUITY
    
    def validate_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        """Validate if signal is valid for equity trading"""
        try:
            # Check asset type
            if signal.asset_type != AssetType.EQUITY:
                return False, f"Invalid asset type: {signal.asset_type.value}"
            
            # Check symbol
            if not signal.symbol or len(signal.symbol) > 10:
                return False, "Invalid or missing symbol"
            
            # Check quantity
            if signal.quantity <= 0:
                return False, "Quantity must be positive"
            
            # Check action is valid for equities
            valid_actions = {
                OrderAction.BUY, OrderAction.SELL, 
                OrderAction.SELL_SHORT, OrderAction.BUY_TO_COVER
            }
            if signal.action not in valid_actions:
                return False, f"Invalid equity action: {signal.action.value}"
            
            # Check order type
            if signal.order_type == OrderType.LIMIT and signal.limit_price is None:
                return False, "Limit price required for limit orders"
            
            if signal.order_type == OrderType.STOP and signal.stop_price is None:
                return False, "Stop price required for stop orders"
            
            return True, "Equity signal validation passed"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def execute_order(self, signal: TradingSignal) -> OrderResult:
        """Execute an equity order"""
        try:
            self.logger.info(f"Executing equity order: {signal.symbol} {signal.action.value} {signal.quantity}")
            
            # Map signal to order handler method
            if signal.order_type == OrderType.MARKET:
                result = self._execute_market_order(signal)
            elif signal.order_type == OrderType.LIMIT:
                result = self._execute_limit_order(signal)
            elif signal.order_type == OrderType.STOP:
                result = self._execute_stop_order(signal)
            elif signal.order_type == OrderType.STOP_LIMIT:
                result = self._execute_stop_limit_order(signal)
            elif signal.order_type == OrderType.TRAILING_STOP:
                result = self._execute_trailing_stop_order(signal)
            else:
                return OrderResult(
                    success=False,
                    message=f"Unsupported order type: {signal.order_type.value}"
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing equity order: {e}")
            return OrderResult(
                success=False,
                message=f"Execution error: {str(e)}"
            )
    
    def _execute_market_order(self, signal: TradingSignal) -> OrderResult:
        """Execute a market order"""
        try:
            # Use existing order handler methods
            result = self.order_handler.place_market_order(
                action_type=signal.action.value,
                symbol=signal.symbol,
                shares=int(signal.quantity),
                current_price=None,  # Market order - no price needed
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _execute_limit_order(self, signal: TradingSignal) -> OrderResult:
        """Execute a limit order"""
        try:
            result = self.order_handler.place_limit_order(
                action_type=signal.action.value,
                symbol=signal.symbol,
                shares=int(signal.quantity),
                limit_price=signal.limit_price,
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _execute_stop_order(self, signal: TradingSignal) -> OrderResult:
        """Execute a stop order"""
        try:
            result = self.order_handler.place_stop_order(
                action_type=signal.action.value,
                symbol=signal.symbol,
                shares=int(signal.quantity),
                stop_price=signal.stop_price,
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _execute_stop_limit_order(self, signal: TradingSignal) -> OrderResult:
        """Execute a stop-limit order"""
        try:
            result = self.order_handler.place_stop_limit_order(
                action_type=signal.action.value,
                symbol=signal.symbol,
                shares=int(signal.quantity),
                stop_price=signal.stop_price,
                limit_price=signal.limit_price,
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _execute_trailing_stop_order(self, signal: TradingSignal) -> OrderResult:
        """Execute a trailing stop order"""
        try:
            result = self.order_handler.place_trailing_stop_order(
                action_type=signal.action.value,
                symbol=signal.symbol,
                shares=int(signal.quantity),
                stop_price_offset=signal.trailing_amount,
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _convert_order_result(self, order_handler_result: Dict) -> OrderResult:
        """Convert order handler result to OrderResult"""
        success = order_handler_result.get('status') in ['submitted', 'filled']
        
        return OrderResult(
            success=success,
            order_id=order_handler_result.get('order_id'),
            fill_price=order_handler_result.get('fill_price'),
            fill_quantity=order_handler_result.get('shares'),
            message=order_handler_result.get('reason', 'Order processed'),
            timestamp=order_handler_result.get('timestamp', datetime.now()),
            raw_response=order_handler_result
        )
    
    def get_positions(self) -> List[Position]:
        """Get current equity positions"""
        try:
            positions_data = get_all_positions()
            positions = []
            
            for account_number, account_positions in positions_data.items():
                for pos in account_positions:
                    # Only include equity positions (filter out options)
                    if pos.get('asset_type', 'EQUITY') == 'EQUITY':
                        position = Position(
                            symbol=pos.get('symbol', ''),
                            asset_type=AssetType.EQUITY,
                            quantity=pos.get('quantity', 0),
                            avg_cost=pos.get('cost_basis', 0) / max(abs(pos.get('quantity', 1)), 1),
                            market_value=pos.get('market_value', 0),
                            unrealized_pnl=pos.get('unrealized_pl', 0),
                            unrealized_pnl_percent=pos.get('unrealized_pl_percent', 0),
                            account=account_number
                        )
                        positions.append(position)
            
            return positions
            
        except Exception as e:
            self.logger.error(f"Error getting equity positions: {e}")
            return []
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get current quote for an equity symbol"""
        try:
            # Use options data handler for quotes (it handles all symbols)
            options_handler = OptionsDataHandler()
            quote = options_handler.get_quote(symbol)
            return quote
            
        except Exception as e:
            self.logger.error(f"Error getting quote for {symbol}: {e}")
            return None


class OptionHandler(AssetHandler):
    """
    Asset handler for options trading
    Wraps the existing options handlers for options operations
    """
    
    def __init__(self):
        self.order_handler = OrderHandler()
        self.options_data_handler = OptionsDataHandler()
        self.logger = logging.getLogger("option_handler")
        self.logger.info("OptionHandler initialized")
    
    def get_asset_type(self) -> AssetType:
        """Return the asset type this handler manages"""
        return AssetType.OPTION
    
    def validate_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        """Validate if signal is valid for options trading"""
        try:
            # Check asset type
            if signal.asset_type != AssetType.OPTION:
                return False, f"Invalid asset type: {signal.asset_type.value}"
            
            # Check symbol format (options have longer symbols)
            if not signal.symbol or len(signal.symbol) < 10:
                return False, "Invalid option symbol format"
            
            # Check quantity (options are in contracts)
            if signal.quantity <= 0 or signal.quantity != int(signal.quantity):
                return False, "Option quantity must be positive integer (contracts)"
            
            # Check action is valid for options
            valid_actions = {
                OrderAction.BUY_TO_OPEN, OrderAction.SELL_TO_OPEN,
                OrderAction.BUY_TO_CLOSE, OrderAction.SELL_TO_CLOSE
            }
            if signal.action not in valid_actions:
                return False, f"Invalid option action: {signal.action.value}"
            
            # Options typically require limit orders
            if signal.order_type == OrderType.LIMIT and signal.limit_price is None:
                return False, "Limit price required for option orders"
            
            return True, "Option signal validation passed"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def execute_order(self, signal: TradingSignal) -> OrderResult:
        """Execute an options order"""
        try:
            self.logger.info(f"Executing option order: {signal.symbol} {signal.action.value} {signal.quantity}")
            
            # Options typically use limit orders
            if signal.order_type == OrderType.LIMIT:
                result = self._execute_option_limit_order(signal)
            else:
                return OrderResult(
                    success=False,
                    message=f"Unsupported option order type: {signal.order_type.value}"
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing option order: {e}")
            return OrderResult(
                success=False,
                message=f"Execution error: {str(e)}"
            )
    
    def _execute_option_limit_order(self, signal: TradingSignal) -> OrderResult:
        """Execute an option limit order"""
        try:
            result = self.order_handler.place_option_limit_order(
                action_type=signal.action.value,
                option_symbol=signal.symbol,
                contracts=int(signal.quantity),
                limit_price=signal.limit_price,
                timestamp=signal.timestamp
            )
            
            return self._convert_order_result(result)
            
        except Exception as e:
            return OrderResult(success=False, message=str(e))
    
    def _convert_order_result(self, order_handler_result: Dict) -> OrderResult:
        """Convert order handler result to OrderResult"""
        success = order_handler_result.get('status') in ['submitted', 'filled']
        
        return OrderResult(
            success=success,
            order_id=order_handler_result.get('order_id'),
            fill_price=order_handler_result.get('limit_price'),
            fill_quantity=order_handler_result.get('contracts'),
            message=order_handler_result.get('reason', 'Option order processed'),
            timestamp=order_handler_result.get('timestamp', datetime.now()),
            raw_response=order_handler_result
        )
    
    def get_positions(self) -> List[Position]:
        """Get current option positions"""
        try:
            positions_data = get_all_positions()
            positions = []
            
            for account_number, account_positions in positions_data.items():
                for pos in account_positions:
                    # Only include option positions
                    if pos.get('asset_type') == 'OPTION':
                        position = Position(
                            symbol=pos.get('symbol', ''),
                            asset_type=AssetType.OPTION,
                            quantity=pos.get('quantity', 0),
                            avg_cost=pos.get('cost_basis', 0) / max(abs(pos.get('quantity', 1)), 1),
                            market_value=pos.get('market_value', 0),
                            unrealized_pnl=pos.get('unrealized_pl', 0),
                            unrealized_pnl_percent=pos.get('unrealized_pl_percent', 0),
                            account=account_number
                        )
                        positions.append(position)
            
            return positions
            
        except Exception as e:
            self.logger.error(f"Error getting option positions: {e}")
            return []
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get current quote for an option symbol"""
        try:
            # For options, we might need to get the options chain
            # This is a simplified implementation
            quote = self.options_data_handler.get_quote(symbol)
            return quote
            
        except Exception as e:
            self.logger.error(f"Error getting option quote for {symbol}: {e}")
            return None


class FutureHandler(AssetHandler):
    """
    Placeholder for futures trading
    Can be implemented when futures support is needed
    """
    
    def __init__(self):
        self.logger = logging.getLogger("future_handler")
        self.logger.info("FutureHandler initialized (placeholder)")
    
    def get_asset_type(self) -> AssetType:
        return AssetType.FUTURE
    
    def validate_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        return False, "Futures trading not yet implemented"
    
    def execute_order(self, signal: TradingSignal) -> OrderResult:
        return OrderResult(success=False, message="Futures trading not yet implemented")
    
    def get_positions(self) -> List[Position]:
        return []
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        return None


class CryptoHandler(AssetHandler):
    """
    Placeholder for cryptocurrency trading
    Can be implemented when crypto support is needed
    """
    
    def __init__(self):
        self.logger = logging.getLogger("crypto_handler")
        self.logger.info("CryptoHandler initialized (placeholder)")
    
    def get_asset_type(self) -> AssetType:
        return AssetType.CRYPTO
    
    def validate_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        return False, "Crypto trading not yet implemented"
    
    def execute_order(self, signal: TradingSignal) -> OrderResult:
        return OrderResult(success=False, message="Crypto trading not yet implemented")
    
    def get_positions(self) -> List[Position]:
        return []
    
    def get_quote(self, symbol: str) -> Optional[Dict]:
        return None


def main():
    """Test the asset handlers"""
    import logging
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Test equity handler
    print("Testing EquityHandler...")
    equity_handler = EquityHandler()
    print(f"Asset type: {equity_handler.get_asset_type()}")
    
    # Test option handler
    print("\nTesting OptionHandler...")
    option_handler = OptionHandler()
    print(f"Asset type: {option_handler.get_asset_type()}")
    
    # Test validation
    from trading_engine import TradingSignal, AssetType, OrderAction, OrderType
    
    equity_signal = TradingSignal(
        asset_type=AssetType.EQUITY,
        symbol="AAPL",
        action=OrderAction.BUY,
        quantity=100,
        order_type=OrderType.LIMIT,
        limit_price=150.0
    )
    
    is_valid, msg = equity_handler.validate_signal(equity_signal)
    print(f"\nEquity signal validation: {is_valid} - {msg}")
    
    option_signal = TradingSignal(
        asset_type=AssetType.OPTION,
        symbol="AAPL  251011C00260000",
        action=OrderAction.BUY_TO_OPEN,
        quantity=1,
        order_type=OrderType.LIMIT,
        limit_price=2.50
    )
    
    is_valid, msg = option_handler.validate_signal(option_signal)
    print(f"Option signal validation: {is_valid} - {msg}")


if __name__ == "__main__":
    main()
