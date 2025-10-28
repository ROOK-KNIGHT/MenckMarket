#!/usr/bin/env python3
"""
Exceedance Trading Engine - Modular & Atomic Methods

Refactored trading engine with atomic, modular methods for easy debugging:

ATOMIC DATA PROCESSING:
- load_trading_config() - Load and validate configuration
- load_positions_data() - Load current positions data
- validate_trade_params() - Validate trade parameters
- format_price() - Format prices for orders

ATOMIC SIGNAL PROCESSING:
- calculate_position_size() - Calculate position sizing
- calculate_profit_target() - Calculate profit targets
- generate_order_params() - Generate order parameters

ATOMIC TRADE EXECUTION:
- place_market_order() - Place market orders
- place_limit_order() - Place limit orders
- cancel_single_order() - Cancel individual orders
- check_single_order_status() - Check order status

ATOMIC RISK MANAGEMENT:
- validate_position_limits() - Check position limits
- calculate_scale_in_params() - Calculate scale-in parameters
- check_existing_orders() - Check for existing orders

COMPOSITE OPERATIONS:
- execute_complete_trade() - Orchestrate full trade execution
- execute_scale_in_trade() - Handle scale-in trades
- execute_new_position_trade() - Handle new position trades

Each atomic method has a single responsibility and can be tested/debugged independently.
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging

# Import existing handlers
from order_handler import OrderHandler
from current_positions_handler import CurrentPositionsHandler

class ExceedanceTradingEngine:
    """
    Modular trading engine with atomic methods for exceedance strategy
    """
    
    def __init__(self):
        """Initialize the trading engine with atomic components"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize handlers
        self.order_handler = OrderHandler()
        self.position_handler = CurrentPositionsHandler()
        
        # Configuration
        self.config = self.load_trading_config()
        
        # Profit target settings (same as exceedence_strategy.py)
        self.profit_target_pct = 0.00045  # 0.045% profit target

        self.logger.info("🎯 Exceedance Trading Engine initialized (Modular)")
        self.logger.info(f"💰 Profit target: {self.profit_target_pct*100:.3f}%")

    # ============================================================================
    # ATOMIC DATA PROCESSING METHODS
    # ============================================================================
    
    def load_trading_config(self) -> Dict[str, Any]:
        """
        ATOMIC: Load and validate trading configuration from PML strategy
        
        Returns:
            Dict with validated configuration or defaults
        """
        try:
            if not os.path.exists('trading_config_live.json'):
                self.logger.error("❌ trading_config_live.json not found")
                return self.get_default_config()
            
            with open('trading_config_live.json', 'r') as f:
                trading_config = json.load(f)
            
            # Extract PML strategy configuration
            pml_config = trading_config.get('strategies', {}).get('pml', {})
            if not pml_config:
                self.logger.error("❌ No PML strategy configuration found")
                return self.get_default_config()
            
            # Build configuration
            config = {
                "auto_approve": pml_config.get('auto_approve', False),
                "profit_target_pct": 0.00045,  # 0.045%
                "position_check_timeout": 60,  # Wait up to 60 seconds for position
                "position_check_interval": 2   # Check every 2 seconds
            }
            
            self.logger.info("✅ Loaded trading configuration from PML strategy")
            return config
            
        except Exception as e:
            self.logger.error(f"❌ Error loading trading config: {e}")
            return self.get_default_config()

    def get_default_config(self) -> Dict[str, Any]:
        """
        ATOMIC: Get default configuration values
        
        Returns:
            Dict with default configuration
        """
        return {
            "auto_approve": False,
            "profit_target_pct": 0.00045,
            "position_check_timeout": 60,
            "position_check_interval": 2
        }

    def load_positions_data(self) -> Dict[str, Any]:
        """
        ATOMIC: Load current positions data from file
        
        Returns:
            Dict with positions data or empty dict if failed
        """
        try:
            positions_file = 'current_positions.json'
            if not os.path.exists(positions_file):
                self.logger.debug(f"📄 {positions_file} not found")
                return {}
            
            with open(positions_file, 'r') as f:
                data = json.load(f)
            
            self.logger.debug(f"✅ Loaded positions data with {data.get('total_positions', 0)} positions")
            return data
            
        except Exception as e:
            self.logger.error(f"❌ Error loading positions data: {e}")
            return {}

    def validate_trade_params(self, symbol: str, quantity: int, current_price: float) -> Dict[str, Any]:
        """
        ATOMIC: Validate trade parameters
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            current_price: Current market price
            
        Returns:
            Dict with validation result and details
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Validate symbol
        if not symbol or not isinstance(symbol, str) or len(symbol.strip()) == 0:
            validation_result['valid'] = False
            validation_result['errors'].append('Invalid symbol')
        
        # Validate quantity
        if not isinstance(quantity, int) or quantity <= 0:
            validation_result['valid'] = False
            validation_result['errors'].append(f'Invalid quantity: {quantity}')
        
        # Validate price
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            validation_result['valid'] = False
            validation_result['errors'].append(f'Invalid price: {current_price}')
        
        # Add warnings for edge cases
        if quantity > 10000:
            validation_result['warnings'].append(f'Large quantity: {quantity} shares')
        
        if current_price > 1000:
            validation_result['warnings'].append(f'High price stock: ${current_price:.2f}')
        
        return validation_result

    def format_price(self, price: float) -> float:
        """
        ATOMIC: Format price for order submission
        
        Args:
            price: Raw price value
            
        Returns:
            Properly formatted price (2 decimals for >$1, 4 decimals for <$1)
        """
        if price >= 1.0:
            return round(price, 2)
        else:
            return round(price, 4)

    # ============================================================================
    # ATOMIC SIGNAL PROCESSING METHODS
    # ============================================================================
    
    def calculate_position_size_atomic(self, current_price: float, account_equity: float, 
                                     strategy_allocation_pct: float, position_size_pct: float, 
                                     max_shares: int) -> Dict[str, Any]:
        """
        ATOMIC: Calculate position size based on risk parameters
        
        Args:
            current_price: Current stock price
            account_equity: Account equity value
            strategy_allocation_pct: Strategy allocation percentage (0.0-1.0)
            position_size_pct: Position size percentage (0.0-1.0)
            max_shares: Maximum shares allowed
            
        Returns:
            Dict with position size calculation details
        """
        try:
            if account_equity <= 0 or current_price <= 0:
                return {
                    'success': False,
                    'error': f'Invalid equity (${account_equity:.2f}) or price (${current_price:.2f})',
                    'shares': 1
                }
            
            # Calculate position value based on strategy allocation and position size percentage
            strategy_allocation = account_equity * strategy_allocation_pct
            position_value = strategy_allocation * position_size_pct
            
            # Calculate shares
            shares = int(position_value / current_price)
            
            # Apply max shares limit
            shares = min(shares, max_shares)
            
            # Ensure at least 1 share
            shares = max(1, shares)
            
            return {
                'success': True,
                'shares': shares,
                'position_value': position_value,
                'strategy_allocation': strategy_allocation,
                'calculation_details': {
                    'equity': account_equity,
                    'strategy_allocation_pct': strategy_allocation_pct * 100,
                    'position_size_pct': position_size_pct * 100,
                    'max_shares': max_shares,
                    'current_price': current_price
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'shares': 1
            }

    def calculate_profit_target_atomic(self, entry_price: float, profit_target_pct: float) -> Dict[str, Any]:
        """
        ATOMIC: Calculate profit target price
        
        Args:
            entry_price: Entry price
            profit_target_pct: Profit target percentage (0.0-1.0)
            
        Returns:
            Dict with profit target calculation details
        """
        try:
            if entry_price <= 0 or profit_target_pct <= 0:
                return {
                    'success': False,
                    'error': f'Invalid entry price (${entry_price:.2f}) or profit pct ({profit_target_pct:.4f})',
                    'profit_target': entry_price
                }
            
            profit_target = entry_price + (entry_price * profit_target_pct)
            formatted_target = self.format_price(profit_target)
            profit_amount = formatted_target - entry_price
            
            return {
                'success': True,
                'profit_target': formatted_target,
                'profit_amount': profit_amount,
                'profit_pct': profit_target_pct * 100,
                'entry_price': entry_price,
                'calculation_details': {
                    'raw_target': profit_target,
                    'formatted_target': formatted_target,
                    'profit_target_pct': profit_target_pct
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'profit_target': entry_price
            }

    def generate_order_params(self, symbol: str, action: str, quantity: int, 
                            price: Optional[float] = None, order_type: str = 'MARKET') -> Dict[str, Any]:
        """
        ATOMIC: Generate standardized order parameters
        
        Args:
            symbol: Stock symbol
            action: Order action (BUY, SELL, etc.)
            quantity: Number of shares
            price: Price for limit orders (optional)
            order_type: Order type (MARKET, LIMIT)
            
        Returns:
            Dict with standardized order parameters
        """
        try:
            # Validate inputs
            validation = self.validate_trade_params(symbol, quantity, price or 1.0)
            if not validation['valid']:
                return {
                    'success': False,
                    'error': f"Invalid parameters: {', '.join(validation['errors'])}",
                    'order_params': {}
                }
            
            order_params = {
                'symbol': symbol.upper().strip(),
                'action': action.upper(),
                'quantity': quantity,
                'order_type': order_type.upper(),
                'timestamp': datetime.now()
            }
            
            # Add price for limit orders
            if order_type.upper() == 'LIMIT' and price is not None:
                order_params['price'] = self.format_price(price)
            elif order_type.upper() == 'MARKET' and price is not None:
                order_params['current_price'] = self.format_price(price)
            
            return {
                'success': True,
                'order_params': order_params,
                'warnings': validation.get('warnings', [])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'order_params': {}
            }

    # ============================================================================
    # ATOMIC TRADE EXECUTION METHODS
    # ============================================================================
    
    def place_market_order_atomic(self, symbol: str, action: str, quantity: int, current_price: float) -> Dict[str, Any]:
        """
        ATOMIC: Place a market order
        
        Args:
            symbol: Stock symbol
            action: Order action (BUY, SELL)
            quantity: Number of shares
            current_price: Current market price
            
        Returns:
            Dict with order result
        """
        try:
            # Generate order parameters
            params_result = self.generate_order_params(symbol, action, quantity, current_price, 'MARKET')
            if not params_result['success']:
                return {
                    'success': False,
                    'error': f"Parameter generation failed: {params_result['error']}",
                    'order_id': None
                }
            
            order_params = params_result['order_params']
            
            self.logger.info(f"📤 Placing {action} MARKET order: {symbol} {quantity}@${current_price:.2f}")
            
            # Submit market order
            result = self.order_handler.place_market_order(
                action_type=action,
                symbol=symbol,
                shares=quantity,
                current_price=self.format_price(current_price),
                timestamp=order_params['timestamp']
            )
            
            if result.get('status') == 'submitted':
                order_id = result.get('order_id', 'N/A')
                self.logger.info(f"✅ {action} MARKET order submitted: {symbol} - Order ID: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': 'submitted',
                    'symbol': symbol,
                    'action': action,
                    'quantity': quantity,
                    'order_type': 'MARKET',
                    'current_price': current_price
                }
            else:
                error_msg = result.get('reason', 'Order rejected')
                self.logger.error(f"❌ {action} MARKET order rejected: {symbol} - {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'symbol': symbol,
                    'action': action,
                    'quantity': quantity,
                    'order_id': None
                }
            
        except Exception as e:
            self.logger.error(f"❌ Error placing {action} MARKET order for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'order_id': None
            }

    def place_limit_order_atomic(self, symbol: str, action: str, quantity: int, limit_price: float) -> Dict[str, Any]:
        """
        ATOMIC: Place a limit order
        
        Args:
            symbol: Stock symbol
            action: Order action (BUY, SELL)
            quantity: Number of shares
            limit_price: Limit price
            
        Returns:
            Dict with order result
        """
        try:
            # Generate order parameters
            params_result = self.generate_order_params(symbol, action, quantity, limit_price, 'LIMIT')
            if not params_result['success']:
                return {
                    'success': False,
                    'error': f"Parameter generation failed: {params_result['error']}",
                    'order_id': None
                }
            
            order_params = params_result['order_params']
            formatted_price = self.format_price(limit_price)
            
            self.logger.info(f"📤 Placing {action} LIMIT order: {symbol} {quantity}@${formatted_price:.2f}")
            
            # Submit limit order
            result = self.order_handler.place_limit_order(
                action_type=action,
                symbol=symbol,
                shares=quantity,
                limit_price=formatted_price,
                timestamp=order_params['timestamp']
            )
            
            if result.get('status') == 'submitted':
                order_id = result.get('order_id', 'N/A')
                self.logger.info(f"✅ {action} LIMIT order submitted: {symbol} @ ${formatted_price:.2f} - Order ID: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': 'submitted',
                    'symbol': symbol,
                    'action': action,
                    'quantity': quantity,
                    'order_type': 'LIMIT',
                    'limit_price': formatted_price
                }
            else:
                error_msg = result.get('reason', 'Order rejected')
                self.logger.error(f"❌ {action} LIMIT order rejected: {symbol} - {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'symbol': symbol,
                    'action': action,
                    'quantity': quantity,
                    'limit_price': formatted_price,
                    'order_id': None
                }
            
        except Exception as e:
            self.logger.error(f"❌ Error placing {action} LIMIT order for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'limit_price': limit_price,
                'order_id': None
            }

    def cancel_single_order_atomic(self, order_id: str) -> Dict[str, Any]:
        """
        ATOMIC: Cancel a single order
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            Dict with cancellation result
        """
        try:
            if not order_id or not isinstance(order_id, str):
                return {
                    'success': False,
                    'error': 'Invalid order ID',
                    'order_id': order_id
                }
            
            self.logger.info(f"🚫 Cancelling order: {order_id}")
            
            # Cancel order using order handler
            result = self.order_handler.cancel_order(order_id)
            
            # API returns 200 with empty body on success, or error dict on failure
            if 'error' not in result:
                self.logger.info(f"✅ Order cancelled: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': 'cancelled'
                }
            else:
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"❌ Failed to cancel order {order_id}: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'order_id': order_id
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error cancelling order: {e}")
            return {
                'success': False,
                'error': str(e),
                'order_id': order_id
            }

    def check_single_order_status_atomic(self, order_id: str) -> Dict[str, Any]:
        """
        ATOMIC: Check the status of a single order
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Dict with order status details
        """
        try:
            if not order_id or not isinstance(order_id, str):
                return {
                    'success': False,
                    'error': 'Invalid order ID',
                    'order_id': order_id
                }
            
            self.logger.debug(f"🔍 Checking order status: {order_id}")
            
            # Get order status from Schwab
            status_result = self.order_handler.get_order_status(order_id)
            
            if 'error' not in status_result:
                status = status_result.get('status', '').upper()
                fill_price = status_result.get('fill_price', 0.0)
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': status,
                    'fill_price': fill_price,
                    'is_filled': status == 'FILLED',
                    'is_cancelled': status in ['CANCELED', 'CANCELLED'],
                    'is_rejected': status == 'REJECTED',
                    'is_working': status == 'WORKING',
                    'raw_result': status_result
                }
            else:
                error_msg = status_result.get('error', 'Unknown error')
                return {
                    'success': False,
                    'error': error_msg,
                    'order_id': order_id
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'order_id': order_id
            }

    # ============================================================================
    # ATOMIC RISK MANAGEMENT METHODS
    # ============================================================================
    
    def validate_position_limits(self, symbol: str, quantity: int, current_price: float, 
                               max_position_value: float = 50000.0, max_shares: int = 10000) -> Dict[str, Any]:
        """
        ATOMIC: Validate position limits and risk constraints
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            current_price: Current market price
            max_position_value: Maximum position value allowed
            max_shares: Maximum shares allowed
            
        Returns:
            Dict with validation result and risk assessment
        """
        try:
            position_value = quantity * current_price
            
            validation_result = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'risk_metrics': {
                    'position_value': position_value,
                    'max_position_value': max_position_value,
                    'quantity': quantity,
                    'max_shares': max_shares,
                    'position_value_pct': (position_value / max_position_value) * 100 if max_position_value > 0 else 0
                }
            }
            
            # Check position value limit
            if position_value > max_position_value:
                validation_result['valid'] = False
                validation_result['errors'].append(f'Position value ${position_value:.2f} exceeds limit ${max_position_value:.2f}')
            
            # Check shares limit
            if quantity > max_shares:
                validation_result['valid'] = False
                validation_result['errors'].append(f'Quantity {quantity} exceeds limit {max_shares}')
            
            # Add warnings for high risk positions
            if position_value > max_position_value * 0.8:
                validation_result['warnings'].append(f'High position value: ${position_value:.2f} (80%+ of limit)')
            
            if quantity > max_shares * 0.8:
                validation_result['warnings'].append(f'High share count: {quantity} (80%+ of limit)')
            
            return validation_result
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [str(e)],
                'warnings': [],
                'risk_metrics': {}
            }

    def calculate_scale_in_params(self, symbol: str, current_price: float, 
                                target_tolerance_pct: float = 0.02) -> Dict[str, Any]:
        """
        ATOMIC: Calculate scale-in parameters for existing position
        
        Args:
            symbol: Stock symbol
            current_price: Current market price
            target_tolerance_pct: Target tolerance percentage (default 0.02%)
            
        Returns:
            Dict with scale-in calculation parameters
        """
        try:
            # Get current position data
            positions_result = self.get_current_positions()
            if not positions_result.get('success', False):
                return {
                    'success': False,
                    'error': 'Failed to get current positions',
                    'can_scale_in': False
                }
            
            current_position = positions_result.get('positions', {}).get(symbol, {})
            if not current_position or current_position.get('quantity', 0) == 0:
                return {
                    'success': False,
                    'error': 'No existing position found',
                    'can_scale_in': False
                }
            
            current_quantity = current_position.get('quantity', 0)
            current_avg_price = current_position.get('average_price', 0.0)
            
            if current_avg_price <= 0:
                return {
                    'success': False,
                    'error': 'Invalid average price',
                    'can_scale_in': False
                }
            
            # Calculate price difference and tolerance
            price_difference = abs(current_price - current_avg_price)
            price_difference_pct = (price_difference / current_avg_price) * 100
            target_tolerance = current_price * (target_tolerance_pct / 100)
            
            # Determine if scale-in is beneficial
            can_scale_in = price_difference > target_tolerance
            
            # Calculate optimal scale-in quantity if beneficial
            optimal_quantity = 0
            if can_scale_in and current_price < current_avg_price:
                # Averaging down calculation
                target_avg = current_price + target_tolerance
                if target_avg < current_avg_price:
                    optimal_quantity = max(1, int((current_quantity * (current_avg_price - target_avg)) / (target_avg - current_price)))
            
            return {
                'success': True,
                'can_scale_in': can_scale_in,
                'current_position': {
                    'quantity': current_quantity,
                    'average_price': current_avg_price
                },
                'market_data': {
                    'current_price': current_price,
                    'price_difference': price_difference,
                    'price_difference_pct': price_difference_pct,
                    'target_tolerance': target_tolerance,
                    'target_tolerance_pct': target_tolerance_pct
                },
                'scale_in_recommendation': {
                    'optimal_quantity': optimal_quantity,
                    'direction': 'averaging_down' if current_price < current_avg_price else 'averaging_up',
                    'beneficial': can_scale_in and optimal_quantity > 0
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'can_scale_in': False
            }

    def check_existing_orders(self, symbol: str, order_types: List[str] = None) -> Dict[str, Any]:
        """
        ATOMIC: Check for existing orders for a symbol
        
        Args:
            symbol: Stock symbol
            order_types: List of order types to check for (optional)
            
        Returns:
            Dict with existing orders information
        """
        try:
            if order_types is None:
                order_types = ['LIMIT', 'MARKET', 'STOP']
            
            # Load current positions data to get open orders
            positions_data = self.load_positions_data()
            if not positions_data:
                return {
                    'success': False,
                    'error': 'Failed to load positions data',
                    'existing_orders': []
                }
            
            existing_orders = []
            positions = positions_data.get('positions', {})
            
            # Look for the symbol in positions
            for position_key, position_data in positions.items():
                position_symbol = position_data.get('symbol', '')
                
                if position_symbol == symbol:
                    open_orders = position_data.get('open_orders', [])
                    
                    for order in open_orders:
                        order_type = order.get('order_type', '')
                        status = order.get('status', '')
                        
                        # Filter by order types and active status
                        if order_type in order_types and status == 'WORKING':
                            existing_orders.append({
                                'order_id': order.get('order_id', ''),
                                'instruction': order.get('instruction', ''),
                                'order_type': order_type,
                                'quantity': order.get('quantity', 0),
                                'price': order.get('price', 0.0),
                                'status': status
                            })
                    break
            
            return {
                'success': True,
                'symbol': symbol,
                'existing_orders': existing_orders,
                'order_count': len(existing_orders),
                'has_orders': len(existing_orders) > 0,
                'order_types_found': list(set([order['order_type'] for order in existing_orders]))
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'existing_orders': []
            }

    def assess_trade_risk(self, symbol: str, quantity: int, current_price: float, 
                         account_equity: float) -> Dict[str, Any]:
        """
        ATOMIC: Assess overall trade risk
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            current_price: Current market price
            account_equity: Account equity value
            
        Returns:
            Dict with comprehensive risk assessment
        """
        try:
            position_value = quantity * current_price
            position_pct_of_equity = (position_value / account_equity) * 100 if account_equity > 0 else 0
            
            # Risk categories
            risk_level = 'LOW'
            risk_factors = []
            
            # Position size risk
            if position_pct_of_equity > 10:
                risk_level = 'HIGH'
                risk_factors.append(f'Large position: {position_pct_of_equity:.1f}% of equity')
            elif position_pct_of_equity > 5:
                risk_level = 'MEDIUM'
                risk_factors.append(f'Moderate position: {position_pct_of_equity:.1f}% of equity')
            
            # Price risk
            if current_price > 500:
                risk_factors.append(f'High price stock: ${current_price:.2f}')
                if risk_level == 'LOW':
                    risk_level = 'MEDIUM'
            
            # Quantity risk
            if quantity > 5000:
                risk_factors.append(f'Large quantity: {quantity} shares')
                if risk_level == 'LOW':
                    risk_level = 'MEDIUM'
            
            # Check existing positions for concentration risk
            positions_result = self.get_current_positions()
            if positions_result.get('success', False):
                existing_position = positions_result.get('positions', {}).get(symbol, {})
                if existing_position:
                    existing_value = existing_position.get('market_value', 0.0)
                    total_value = existing_value + position_value
                    total_pct = (total_value / account_equity) * 100 if account_equity > 0 else 0
                    
                    if total_pct > 15:
                        risk_level = 'HIGH'
                        risk_factors.append(f'High concentration: {total_pct:.1f}% after trade')
            
            return {
                'success': True,
                'risk_assessment': {
                    'risk_level': risk_level,
                    'risk_factors': risk_factors,
                    'position_value': position_value,
                    'position_pct_of_equity': position_pct_of_equity,
                    'recommended_action': 'PROCEED' if risk_level in ['LOW', 'MEDIUM'] else 'REVIEW'
                },
                'metrics': {
                    'symbol': symbol,
                    'quantity': quantity,
                    'current_price': current_price,
                    'account_equity': account_equity
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'risk_assessment': {
                    'risk_level': 'UNKNOWN',
                    'recommended_action': 'REVIEW'
                }
            }

    # ============================================================================
    # COMPOSITE OPERATIONS (Orchestrate atomic methods)
    # ============================================================================
    
    def execute_new_position_trade(self, symbol: str, quantity: int, current_price: float) -> Dict[str, Any]:
        """
        COMPOSITE: Execute a new position trade using atomic methods
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            current_price: Current market price
            
        Returns:
            Dict with trade execution result
        """
        try:
            self.logger.info(f"🎯 Executing NEW position trade: {symbol} {quantity}@${current_price:.2f}")
            
            # Step 1: Validate trade parameters
            validation = self.validate_trade_params(symbol, quantity, current_price)
            if not validation['valid']:
                return {
                    'success': False,
                    'error': f"Parameter validation failed: {', '.join(validation['errors'])}",
                    'step': 'parameter_validation'
                }
            
            # Step 2: Calculate profit target
            profit_calc = self.calculate_profit_target_atomic(current_price, self.profit_target_pct)
            if not profit_calc['success']:
                return {
                    'success': False,
                    'error': f"Profit target calculation failed: {profit_calc['error']}",
                    'step': 'profit_calculation'
                }
            
            profit_target = profit_calc['profit_target']
            
            # Step 3: Place market order
            market_result = self.place_market_order_atomic(symbol, 'BUY', quantity, current_price)
            if not market_result['success']:
                return {
                    'success': False,
                    'error': f"Market order failed: {market_result['error']}",
                    'step': 'market_order'
                }
            
            market_order_id = market_result['order_id']
            
            # Step 4: Place profit target order
            profit_result = self.place_limit_order_atomic(symbol, 'SELL', quantity, profit_target)
            if not profit_result['success']:
                self.logger.warning(f"⚠️ Profit target order failed: {profit_result['error']}")
                # Continue - market order was successful
                return {
                    'success': True,
                    'symbol': symbol,
                    'quantity': quantity,
                    'market_order_id': market_order_id,
                    'profit_order_id': None,
                    'profit_target': profit_target,
                    'warning': 'Market order successful but profit target failed',
                    'step': 'completed_with_warning'
                }
            
            profit_order_id = profit_result['order_id']
            
            self.logger.info(f"✅ NEW position trade completed: {symbol}")
            self.logger.info(f"   Market Order ID: {market_order_id}")
            self.logger.info(f"   Profit Order ID: {profit_order_id}")
            
            return {
                'success': True,
                'symbol': symbol,
                'quantity': quantity,
                'current_price': current_price,
                'profit_target': profit_target,
                'market_order_id': market_order_id,
                'profit_order_id': profit_order_id,
                'order_type': 'new_position_atomic',
                'step': 'completed'
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error executing new position trade: {e}")
            return {
                'success': False,
                'error': str(e),
                'step': 'exception'
            }

    def execute_scale_in_trade(self, symbol: str, requested_quantity: int, current_price: float) -> Dict[str, Any]:
        """
        COMPOSITE: Execute a scale-in trade using atomic methods
        
        Args:
            symbol: Stock symbol
            requested_quantity: Requested number of shares
            current_price: Current market price
            
        Returns:
            Dict with trade execution result
        """
        try:
            self.logger.info(f"🔄 Executing SCALE-IN trade: {symbol} {requested_quantity}@${current_price:.2f}")
            
            # Step 1: Calculate scale-in parameters
            scale_params = self.calculate_scale_in_params(symbol, current_price)
            if not scale_params['success']:
                return {
                    'success': False,
                    'error': f"Scale-in calculation failed: {scale_params['error']}",
                    'step': 'scale_in_calculation'
                }
            
            if not scale_params['can_scale_in']:
                return {
                    'success': False,
                    'error': 'Scale-in not beneficial at current price',
                    'step': 'scale_in_assessment',
                    'scale_params': scale_params
                }
            
            # Use optimal quantity from calculation
            optimal_quantity = scale_params['scale_in_recommendation']['optimal_quantity']
            if optimal_quantity <= 0:
                optimal_quantity = requested_quantity  # Fallback to requested
            
            # Step 2: Check and cancel existing orders
            existing_orders = self.check_existing_orders(symbol, ['LIMIT'])
            if existing_orders['has_orders']:
                self.logger.info(f"🚫 Cancelling {existing_orders['order_count']} existing orders")
                for order in existing_orders['existing_orders']:
                    cancel_result = self.cancel_single_order_atomic(order['order_id'])
                    if cancel_result['success']:
                        self.logger.info(f"✅ Cancelled order {order['order_id']}")
                
                # Wait after cancellations
                time.sleep(1)
            
            # Step 3: Calculate total position size after scale-in
            current_position = scale_params['current_position']
            current_quantity = current_position['quantity']
            total_position_size = current_quantity + optimal_quantity
            
            # Step 4: Calculate profit target
            profit_calc = self.calculate_profit_target_atomic(current_price, self.profit_target_pct)
            if not profit_calc['success']:
                return {
                    'success': False,
                    'error': f"Profit target calculation failed: {profit_calc['error']}",
                    'step': 'profit_calculation'
                }
            
            profit_target = profit_calc['profit_target']
            
            # Step 5: Place market order for additional shares
            market_result = self.place_market_order_atomic(symbol, 'BUY', optimal_quantity, current_price)
            if not market_result['success']:
                return {
                    'success': False,
                    'error': f"Scale-in market order failed: {market_result['error']}",
                    'step': 'market_order'
                }
            
            market_order_id = market_result['order_id']
            
            # Step 6: Place profit target for TOTAL position
            profit_result = self.place_limit_order_atomic(symbol, 'SELL', total_position_size, profit_target)
            if not profit_result['success']:
                self.logger.warning(f"⚠️ Scale-in profit target failed: {profit_result['error']}")
                return {
                    'success': True,
                    'symbol': symbol,
                    'quantity': optimal_quantity,
                    'total_position_size': total_position_size,
                    'market_order_id': market_order_id,
                    'profit_order_id': None,
                    'profit_target': profit_target,
                    'warning': 'Scale-in successful but profit target failed',
                    'step': 'completed_with_warning',
                    'scale_params': scale_params
                }
            
            profit_order_id = profit_result['order_id']
            
            self.logger.info(f"✅ SCALE-IN trade completed: {symbol}")
            self.logger.info(f"   Added: {optimal_quantity} shares")
            self.logger.info(f"   Total position: {total_position_size} shares")
            self.logger.info(f"   Market Order ID: {market_order_id}")
            self.logger.info(f"   Profit Order ID: {profit_order_id}")
            
            return {
                'success': True,
                'symbol': symbol,
                'quantity': optimal_quantity,
                'requested_quantity': requested_quantity,
                'total_position_size': total_position_size,
                'current_price': current_price,
                'profit_target': profit_target,
                'market_order_id': market_order_id,
                'profit_order_id': profit_order_id,
                'order_type': 'scale_in_atomic',
                'scale_params': scale_params,
                'step': 'completed'
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error executing scale-in trade: {e}")
            return {
                'success': False,
                'error': str(e),
                'step': 'exception'
            }

    # ============================================================================
    # LEGACY METHODS (Maintained for backward compatibility)
    # ============================================================================
    
    def place_buy_order(self, symbol: str, quantity: int, current_price: float) -> Dict[str, Any]:
        """
        Place a market buy order
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            current_price: Current market price
            
        Returns:
            Dict with order result
        """
        try:
            self.logger.info(f"📤 Placing BUY order: {symbol} {quantity}@${current_price:.2f}")
            
            # Submit market order
            result = self.order_handler.place_market_order(
                action_type='BUY',
                symbol=symbol,
                shares=quantity,
                current_price=round(current_price, 2),
                timestamp=datetime.now()
            )
            
            if result.get('status') == 'submitted':
                self.logger.info(f"✅ BUY order submitted: {symbol} - Order ID: {result.get('order_id', 'N/A')}")
                return {
                    'success': True,
                    'order_id': result.get('order_id'),
                    'status': 'submitted',
                    'symbol': symbol,
                    'quantity': quantity,
                    'price': current_price
                }
            else:
                self.logger.error(f"❌ BUY order rejected: {symbol} - {result.get('reason', 'Unknown')}")
                return {
                    'success': False,
                    'error': result.get('reason', 'Order rejected'),
                    'symbol': symbol,
                    'quantity': quantity
                }
            
        except Exception as e:
            self.logger.error(f"❌ Error placing BUY order for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbol': symbol,
                'quantity': quantity
            }

    def place_take_profit(self, symbol: str, quantity: int, entry_price: float) -> Dict[str, Any]:
        """
        Place a take profit (limit sell) order
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares
            entry_price: Entry price for calculating profit target
            
        Returns:
            Dict with order result
        """
        try:
            # Calculate profit target
            profit_target = self.calculate_profit_target(entry_price)
            
            self.logger.info(f"📤 Placing TAKE PROFIT: {symbol} {quantity}@${profit_target:.2f}")
            
            # Submit limit sell order
            result = self.order_handler.place_limit_order(
                action_type='SELL',
                symbol=symbol,
                shares=quantity,
                limit_price=round(profit_target, 2),
                timestamp=datetime.now()
            )
            
            if result.get('status') == 'submitted':
                self.logger.info(f"✅ TAKE PROFIT submitted: {symbol} @ ${profit_target:.2f} - Order ID: {result.get('order_id', 'N/A')}")
                return {
                    'success': True,
                    'order_id': result.get('order_id'),
                    'status': 'submitted',
                    'symbol': symbol,
                    'quantity': quantity,
                    'profit_target': profit_target,
                    'profit_pct': self.profit_target_pct * 100
                }
            else:
                self.logger.error(f"❌ TAKE PROFIT rejected: {symbol} - {result.get('reason', 'Unknown')}")
                return {
                    'success': False,
                    'error': result.get('reason', 'Order rejected'),
                    'symbol': symbol,
                    'quantity': quantity,
                    'profit_target': profit_target
                }
            
        except Exception as e:
            self.logger.error(f"❌ Error placing TAKE PROFIT for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbol': symbol,
                'quantity': quantity
            }

    def get_current_positions(self) -> Dict[str, Any]:
        """
        Get current positions from account
        
        Returns:
            Dict with positions data
        """
        try:
            # Get positions from position handler
            positions_data = self.position_handler.fetch_current_positions()
            
            if positions_data.get('fetch_success', False):
                positions = positions_data.get('positions', {})
                
                # Convert to simpler format
                simplified_positions = {}
                for position_key, position in positions.items():
                    symbol = position.get('symbol')
                    if symbol:
                        simplified_positions[symbol] = {
                            'quantity': position.get('quantity', 0),
                            'average_price': position.get('average_price', 0.0),
                            'market_value': position.get('market_value', 0.0),
                            'position_type': 'LONG' if position.get('quantity', 0) > 0 else 'SHORT'
                        }
                
                self.logger.info(f"📊 Retrieved {len(simplified_positions)} positions")
                return {
                    'success': True,
                    'positions': simplified_positions,
                    'count': len(simplified_positions)
                }
            else:
                self.logger.error("❌ Failed to fetch current positions")
                return {
                    'success': False,
                    'error': 'Failed to fetch positions',
                    'positions': {}
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error getting current positions: {e}")
            return {
                'success': False,
                'error': str(e),
                'positions': {}
            }

    def check_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Check the status of an order
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Dict with order status
        """
        try:
            self.logger.info(f"🔍 Checking order status: {order_id}")
            
            # Get order status from Schwab
            status_result = self.order_handler.get_order_status(order_id)
            
            if 'error' not in status_result:
                status = status_result.get('status', '').upper()
                fill_price = status_result.get('fill_price', 0.0)
                
                self.logger.info(f"📊 Order {order_id} status: {status}")
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': status,
                    'fill_price': fill_price,
                    'is_filled': status == 'FILLED',
                    'is_cancelled': status in ['CANCELED', 'CANCELLED'],
                    'is_rejected': status == 'REJECTED'
                }
            else:
                error_msg = status_result.get('error', 'Unknown error')
                self.logger.error(f"❌ Error checking order {order_id}: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'order_id': order_id
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error checking order status: {e}")
            return {
                'success': False,
                'error': str(e),
                'order_id': order_id
            }

    def calculate_profit_target(self, entry_price: float) -> float:
        """
        Calculate profit target price with proper rounding
        
        Args:
            entry_price: Entry price
            
        Returns:
            Profit target price (rounded to 2 decimals for orders above $1)
        """
        profit_target = entry_price + (entry_price * self.profit_target_pct)
        
        # Round to 2 decimals for orders above $1, 4 decimals for orders below $1
        if profit_target >= 1.0:
            return round(profit_target, 2)
        else:
            return round(profit_target, 4)
        
    def scale_in_shares(self, symbol: str, additional_quantity: int, current_price: float) -> Dict[str, Any]:
        """
        Calculate new total shares to buy based on existing position
        and new shares needed to bring average price to within 0.02% of current price.
        
        Args:
            symbol: Stock symbol
            additional_quantity: Additional shares to consider buying
            current_price: Current market price
            
        Returns:
            Dict with calculation results including optimal shares to buy
        """
        try:
            # Get current positions using comprehensive data
            positions_result = self.get_current_positions()
            price_tolerance = current_price * 0.0002  # 0.02% price tolerance
            
            if not positions_result.get('success', False):
                self.logger.error(f"❌ Cannot scale in, failed to get current positions for {symbol}")
                return {
                    'success': False,
                    'error': 'Failed to get current positions',
                    'symbol': symbol
                }

            current_position = positions_result.get('positions', {}).get(symbol, {})
            current_quantity = current_position.get('quantity', 0)
            current_average_price = current_position.get('average_price', 0.0)
            current_cost_basis = current_position.get('cost_basis', 0.0)

            if current_quantity == 0:
                self.logger.error(f"❌ Cannot scale in, no existing position for {symbol}")
                return {
                    'success': False,
                    'error': 'No existing position',
                    'symbol': symbol
                }

            if current_average_price <= 0:
                self.logger.error(f"❌ Cannot scale in, invalid average price for {symbol}: {current_average_price}")
                return {
                    'success': False,
                    'error': f'Invalid average price: {current_average_price}',
                    'symbol': symbol
                }

            # Calculate target average price (within tolerance of current price)
            target_avg_price_min = current_price - price_tolerance
            target_avg_price_max = current_price + price_tolerance
            
            # Use the midpoint as our target
            target_avg_price = current_price
            
            # Current total cost of existing position
            current_total_cost = current_cost_basis
            
            # Calculate optimal additional shares needed to reach target average
            # Formula: (current_cost + additional_shares * current_price) / (current_quantity + additional_shares) = target_avg_price
            # Solving for additional_shares:
            # additional_shares = (target_avg_price * current_quantity - current_cost) / (current_price - target_avg_price)
            
            if abs(current_price - target_avg_price) < 0.0001:  # Avoid division by zero
                # Current price is already very close to target, minimal shares needed
                optimal_additional_shares = 1
            else:
                optimal_additional_shares = (target_avg_price * current_quantity - current_total_cost) / (current_price - target_avg_price)
            
            # Ensure we get a positive number of shares
            optimal_additional_shares = max(1, int(round(optimal_additional_shares)))
            
            # Calculate what the new average would be with optimal shares
            new_total_cost = current_total_cost + (optimal_additional_shares * current_price)
            new_total_quantity = current_quantity + optimal_additional_shares
            new_average_price = new_total_cost / new_total_quantity
            
            # Calculate the difference from target
            price_difference = abs(new_average_price - current_price)
            price_difference_pct = (price_difference / current_price) * 100
            
            # Check if we're within tolerance
            within_tolerance = price_difference <= price_tolerance
            
            # Compare with the originally requested additional quantity
            if additional_quantity > 0:
                # Calculate what average would be with requested quantity
                requested_total_cost = current_total_cost + (additional_quantity * current_price)
                requested_total_quantity = current_quantity + additional_quantity
                requested_new_average = requested_total_cost / requested_total_quantity
                requested_difference = abs(requested_new_average - current_price)
                requested_difference_pct = (requested_difference / current_price) * 100
                requested_within_tolerance = requested_difference <= price_tolerance
            else:
                requested_new_average = current_average_price
                requested_difference = abs(current_average_price - current_price)
                requested_difference_pct = (requested_difference / current_price) * 100
                requested_within_tolerance = False

            self.logger.info(f"📊 Scale-in analysis for {symbol}:")
            self.logger.info(f"   Current: {current_quantity} shares @ ${current_average_price:.4f} avg")
            self.logger.info(f"   Current price: ${current_price:.4f}")
            self.logger.info(f"   Price tolerance: ±${price_tolerance:.4f} (0.02%)")
            self.logger.info(f"   Optimal additional shares: {optimal_additional_shares}")
            self.logger.info(f"   New average with optimal: ${new_average_price:.4f} (diff: {price_difference_pct:.4f}%)")
            
            if additional_quantity > 0:
                self.logger.info(f"   Requested additional shares: {additional_quantity}")
                self.logger.info(f"   New average with requested: ${requested_new_average:.4f} (diff: {requested_difference_pct:.4f}%)")

            return {
                'success': True,
                'symbol': symbol,
                'current_position': {
                    'quantity': current_quantity,
                    'average_price': current_average_price,
                    'total_cost': current_total_cost
                },
                'market_data': {
                    'current_price': current_price,
                    'price_tolerance': price_tolerance,
                    'target_average_price': target_avg_price
                },
                'optimal_calculation': {
                    'additional_shares': optimal_additional_shares,
                    'new_total_shares': new_total_quantity,
                    'new_average_price': new_average_price,
                    'price_difference': price_difference,
                    'price_difference_pct': price_difference_pct,
                    'within_tolerance': within_tolerance
                },
                'requested_calculation': {
                    'additional_shares': additional_quantity,
                    'new_total_shares': requested_total_quantity if additional_quantity > 0 else current_quantity,
                    'new_average_price': requested_new_average,
                    'price_difference': requested_difference,
                    'price_difference_pct': requested_difference_pct,
                    'within_tolerance': requested_within_tolerance
                } if additional_quantity > 0 else None,
                'recommendation': {
                    'use_optimal': within_tolerance,
                    'use_requested': requested_within_tolerance if additional_quantity > 0 else False,
                    'shares_to_buy': optimal_additional_shares if within_tolerance else (additional_quantity if requested_within_tolerance else 0)
                }
            }

        except Exception as e:
            self.logger.error(f"❌ Error calculating scale-in shares: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbol': symbol
            }

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an order
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            Dict with result
        """
        try:
            self.logger.info(f"🚫 Cancelling order: {order_id}")
            
            # Cancel order using order handler
            result = self.order_handler.cancel_order(order_id)
            
            if result.get('status') == 'cancelled':
                self.logger.info(f"✅ Order cancelled: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'status': 'cancelled'
                }
            else:
                error_msg = result.get('reason', 'Unknown error')
                self.logger.error(f"❌ Failed to cancel order {order_id}: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'order_id': order_id
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error cancelling order: {e}")
            return {
                'success': False,
                'error': str(e),
                'order_id': order_id
            }

    def execute_complete_trade(self, symbol: str, quantity: int, current_price: float, is_scale_in: bool = False) -> Dict[str, Any]:
        """
        Execute a complete trade using OTA MARKET order with LIMIT profit target
        
        This method uses the new OTA market order type that:
        1. Cancels ALL pre-existing orders before submitting new ones
        2. Places a MARKET buy order for immediate execution at current price
        3. Automatically places a LIMIT take profit order when the market order fills
        
        For scale-in orders, this method will:
        1. First calculate the optimal share amount using scale_in_shares()
        2. Use the calculated optimal amount instead of the provided quantity
        3. Cancel any existing profit targets
        4. Place a new profit target for the TOTAL position size (existing + new shares)
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares to buy (ignored for scale-in, calculated automatically)
            current_price: Current market price (used for profit target calculation)
            is_scale_in: Whether this is a scale-in to existing position
            
        Returns:
            Dict with complete trade result
        """
        try:
            # STEP 1: Cancel ALL pre-existing profit target orders before submitting any new orders
            self.logger.info("🚫 Cancelling all pre-existing profit target orders before submitting new orders")
            try:
                # Cancel existing profit targets for this symbol (which covers all our order types)
                cancel_result = self.cancel_existing_profit_targets(symbol)
                if cancel_result.get('cancelled_orders', 0) > 0:
                    self.logger.info(f"✅ Cancelled {cancel_result['cancelled_orders']} pre-existing profit target orders for {symbol}")
                    # Small wait after cancellation before placing new orders
                    import time
                    time.sleep(1)
                else:
                    self.logger.info(f"ℹ️ No pre-existing profit target orders found for {symbol}")
            except Exception as e:
                self.logger.error(f"❌ Error cancelling pre-existing profit targets for {symbol}: {e}")
                # Continue with processing but log the error
            total_position_size = quantity  # Default for new positions
            actual_quantity_to_buy = quantity  # Default quantity
            
            if is_scale_in:
                print(f"\n🔄 DEBUG: Executing SCALE-IN trade for {symbol} - calculating optimal shares...")
                self.logger.info(f"🔄 Executing SCALE-IN trade: {symbol} - calculating optimal share amount")
                
                # Step 1: Calculate optimal share amount using scale_in_shares function
                scale_calculation = self.scale_in_shares(symbol, quantity, current_price)
                
                if not scale_calculation.get('success', False):
                    error_msg = scale_calculation.get('error', 'Scale-in calculation failed')
                    self.logger.error(f"❌ Scale-in calculation failed for {symbol}: {error_msg}")
                    return {
                        'success': False,
                        'error': f"Scale-in calculation failed: {error_msg}",
                        'step': 'scale_in_calculation'
                    }
                
                # Extract the recommended share amount
                recommendation = scale_calculation.get('recommendation', {})
                optimal_shares = recommendation.get('shares_to_buy', 0)
                
                if optimal_shares <= 0:
                    self.logger.warning(f"⚠️ Scale-in calculation suggests 0 shares for {symbol} - position may already be within tolerance")
                    return {
                        'success': False,
                        'error': 'Scale-in calculation suggests 0 shares - position may already be within tolerance',
                        'step': 'scale_in_calculation',
                        'scale_calculation': scale_calculation
                    }
                
                # Use the calculated optimal amount instead of the provided quantity
                actual_quantity_to_buy = optimal_shares
                
                # Calculate total position size after scale-in
                current_position = scale_calculation.get('current_position', {})
                current_quantity = current_position.get('quantity', 0)
                total_position_size = current_quantity + actual_quantity_to_buy
                
                print(f"📊 DEBUG: Scale-in calculation results:")
                print(f"   Current position: {current_quantity} shares")
                print(f"   Requested quantity: {quantity} shares")
                print(f"   Optimal quantity: {actual_quantity_to_buy} shares")
                print(f"   Total position after scale-in: {total_position_size} shares")
                
                self.logger.info(f"📊 Scale-in calculation for {symbol}:")
                self.logger.info(f"   Current position: {current_quantity} shares")
                self.logger.info(f"   Requested quantity: {quantity} shares")
                self.logger.info(f"   Optimal quantity to buy: {actual_quantity_to_buy} shares")
                self.logger.info(f"   Total position after scale-in: {total_position_size} shares")
                
                # Log the calculation details
                optimal_calc = scale_calculation.get('optimal_calculation', {})
                new_avg_price = optimal_calc.get('new_average_price', 0)
                price_diff_pct = optimal_calc.get('price_difference_pct', 0)
                within_tolerance = optimal_calc.get('within_tolerance', False)
                
                self.logger.info(f"   New average price: ${new_avg_price:.4f}")
                self.logger.info(f"   Price difference: {price_diff_pct:.4f}%")
                self.logger.info(f"   Within tolerance: {within_tolerance}")
                
                self.logger.info(f"🔄 Executing SCALE-IN OTA MARKET trade: {symbol} {actual_quantity_to_buy} shares at MARKET")
                
                # Note: total_position_size is already calculated correctly from scale_in_shares() above
                # No need to recalculate - use the value from the scale calculation
                print(f"📊 DEBUG: Using total_position_size from scale calculation: {total_position_size} shares")
                self.logger.info(f"📊 Using total_position_size from scale calculation: {total_position_size} shares")
                
            else:
                self.logger.info(f"🎯 Executing NEW OTA MARKET trade: {symbol} {quantity} shares at MARKET")
            
            # Round current price properly (2 decimals for prices above $1, 4 decimals below $1)
            if current_price >= 1.0:
                rounded_price = round(current_price, 2)
            else:
                rounded_price = round(current_price, 4)
            
            # Calculate profit target based on rounded current price
            profit_target = self.calculate_profit_target(rounded_price)
            
            # For scale-in orders, we need to use a different approach since OTA orders
            # automatically set profit targets for the order quantity, not total position
            if is_scale_in:
                # Place separate market order and then manual profit target for total position
                self.logger.info(f"🔄 Scale-in: Placing separate MARKET order and profit target for total position")
                
                # Place market buy order using the calculated optimal quantity
                market_result = self.order_handler.place_market_order(
                    action_type='BUY',
                    symbol=symbol,
                    shares=actual_quantity_to_buy,
                    current_price=rounded_price,
                    timestamp=datetime.now()
                )
                
                if market_result.get('status') == 'submitted':
                    market_order_id = market_result.get('order_id')
                    self.logger.info(f"✅ SCALE-IN MARKET order submitted: {symbol} - Order ID: {market_order_id}")
                    
                    # Small wait after market order before placing profit target
                    print(f"⏳ DEBUG: Waiting 2 seconds after market order before placing profit target...")
                    time.sleep(1)
                    
                    # Place profit target for TOTAL position size
                    profit_result = self.order_handler.place_limit_order(
                        action_type='SELL',
                        symbol=symbol,
                        shares=total_position_size,
                        limit_price=profit_target,
                        timestamp=datetime.now()
                    )
                    
                    if profit_result.get('status') == 'submitted':
                        profit_order_id = profit_result.get('order_id')
                        self.logger.info(f"✅ SCALE-IN profit target submitted: SELL {total_position_size} {symbol} @ ${profit_target:.2f} - Order ID: {profit_order_id}")
                        
                        return {
                            'success': True,
                            'symbol': symbol,
                            'quantity': actual_quantity_to_buy,  # Use actual quantity bought
                            'requested_quantity': quantity,  # Include original requested quantity
                            'total_position_size': total_position_size,
                            'entry_type': 'MARKET',
                            'profit_target': profit_target,
                            'profit_pct': self.profit_target_pct * 100,
                            'market_order_id': market_order_id,
                            'profit_order_id': profit_order_id,
                            'order_type': 'scale_in_market_with_total_profit_target',
                            'estimated_entry_price': current_price,
                            'is_scale_in': is_scale_in,
                            'scale_calculation': scale_calculation  # Include full calculation details
                        }
                    else:
                        error_msg = profit_result.get('reason', 'Profit target order failed')
                        self.logger.error(f"❌ SCALE-IN profit target failed: {symbol} - {error_msg}")
                        return {
                            'success': False,
                            'error': f"Profit target order failed: {error_msg}",
                            'step': 'profit_target_order',
                            'market_order_id': market_order_id
                        }
                else:
                    error_msg = market_result.get('reason', 'Market order failed')
                    self.logger.error(f"❌ SCALE-IN MARKET order failed: {symbol} - {error_msg}")
                    return {
                        'success': False,
                        'error': f"Market order failed: {error_msg}",
                        'step': 'market_order'
                    }
            else:
                # For new positions, use OTA MARKET order as before
                ota_result = self.order_handler.buy_stock_market_with_profit_target(
                    symbol=symbol,
                    shares=quantity,
                    profit_target=profit_target
                )
                
                if ota_result.get('status') == 'submitted':
                    order_id = ota_result.get('order_id')
                    self.logger.info(f"✅ NEW OTA MARKET order submitted: {symbol} - Order ID: {order_id}")
                    self.logger.info(f"   Entry: BUY {quantity} shares at MARKET")
                    self.logger.info(f"   Auto Take Profit: SELL {quantity} @ ${profit_target:.2f}")
                    
                    return {
                        'success': True,
                        'symbol': symbol,
                        'quantity': quantity,
                        'total_position_size': quantity,
                        'entry_type': 'MARKET',
                        'profit_target': profit_target,
                        'profit_pct': self.profit_target_pct * 100,
                        'order_id': order_id,
                        'order_type': 'ota_market_with_profit_target',
                        'estimated_entry_price': current_price,
                        'is_scale_in': is_scale_in
                    }
                else:
                    error_msg = ota_result.get('reason', 'OTA MARKET order failed')
                    self.logger.error(f"❌ OTA MARKET order failed: {symbol} - {error_msg}")
                    return {
                        'success': False,
                        'error': f"OTA MARKET order failed: {error_msg}",
                        'step': 'ota_market_order'
                    }
            
        except Exception as e:
            self.logger.error(f"❌ Error executing complete trade: {e}")
            return {
                'success': False,
                'error': str(e),
                'step': 'exception'
            }

    def cancel_existing_profit_targets(self, symbol: str) -> Dict[str, Any]:
        """
        Cancel existing profit target (SELL limit) orders for a symbol using current_positions.json
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dict with cancellation results
        """
        try:
            print(f"\n🔍 DEBUG: Starting order cancellation process for {symbol}")
            self.logger.info(f"🔍 Looking for existing profit target orders for {symbol}")
            
            # Load current positions data to get open orders
            positions_data = self._load_current_positions_data()
            if not positions_data:
                print(f"❌ DEBUG: Failed to load current positions data")
                return {
                    'success': False,
                    'error': 'Failed to load current positions data',
                    'cancelled_orders': 0
                }
            
            cancelled_count = 0
            positions = positions_data.get('positions', {})
            
            print(f"🔍 DEBUG: Found {len(positions)} positions to check")
            
            # Look for the symbol in positions
            symbol_found = False
            for position_key, position_data in positions.items():
                position_symbol = position_data.get('symbol', '')
                
                if position_symbol == symbol:
                    symbol_found = True
                    open_orders = position_data.get('open_orders', [])
                    print(f"🔍 DEBUG: Found {len(open_orders)} open orders for {symbol}")
                    
                    # Look for SELL limit orders (profit targets)
                    for order in open_orders:
                        try:
                            order_id = order.get('order_id', '')
                            instruction = order.get('instruction', '')
                            order_type = order.get('order_type', '')
                            status = order.get('status', '')
                            price = order.get('price', 0)
                            quantity = order.get('quantity', 0)
                            
                            print(f"🔍 DEBUG: Order details - ID:{order_id} {instruction} {order_type} {quantity}@${price} Status:{status}")
                            
                            # Look for profit target orders (SELL for long positions, BUY_TO_COVER for short positions) that are WORKING
                            if (instruction in ['SELL', 'BUY_TO_COVER'] and 
                                order_type == 'LIMIT' and
                                status == 'WORKING'):
                                
                                print(f"🚫 DEBUG: Found matching profit target to cancel: Order {order_id} - SELL {quantity} {symbol} @ ${price}")
                                self.logger.info(f"🚫 Cancelling profit target: Order {order_id} - SELL {quantity} {symbol} @ ${price}")
                                
                                cancel_result = self.order_handler.cancel_order(str(order_id))
                                print(f"🚫 DEBUG: Cancel result: {cancel_result}")
                                
                                # API returns 200 with empty body on success, or error dict on failure
                                if 'error' not in cancel_result:
                                    cancelled_count += 1
                                    print(f"✅ DEBUG: Successfully cancelled order {order_id}")
                                    self.logger.info(f"✅ Cancelled order {order_id}")
                                else:
                                    print(f"❌ DEBUG: Failed to cancel order {order_id}: {cancel_result.get('error', 'Unknown')}")
                                    self.logger.warning(f"⚠️ Failed to cancel order {order_id}: {cancel_result.get('error', 'Unknown')}")
                        
                        except Exception as e:
                            print(f"❌ DEBUG: Error processing order: {e}")
                            self.logger.warning(f"⚠️ Error processing order: {e}")
                            continue
                    
                    break  # Found the symbol, no need to continue
            
            if not symbol_found:
                print(f"ℹ️ DEBUG: Symbol {symbol} not found in current positions")
                self.logger.info(f"ℹ️ Symbol {symbol} not found in current positions")
            
            print(f"🔍 DEBUG: Cancellation complete. Cancelled {cancelled_count} orders for {symbol}")
            
            if cancelled_count > 0:
                self.logger.info(f"✅ Successfully cancelled {cancelled_count} profit target orders for {symbol}")
            else:
                self.logger.info(f"ℹ️ No profit target orders found to cancel for {symbol}")
            
            return {
                'success': True,
                'cancelled_orders': cancelled_count,
                'symbol': symbol
            }
            
        except Exception as e:
            print(f"❌ DEBUG: Exception in cancel_existing_profit_targets: {e}")
            self.logger.error(f"❌ Error cancelling existing profit targets: {e}")
            return {
                'success': False,
                'error': str(e),
                'cancelled_orders': 0
            }

    def _load_current_positions_data(self) -> Dict[str, Any]:
        """Load current positions data from current_positions.json"""
        try:
            positions_file = 'current_positions.json'
            if not os.path.exists(positions_file):
                print(f"❌ DEBUG: {positions_file} not found")
                return {}
            
            with open(positions_file, 'r') as f:
                data = json.load(f)
            
            print(f"✅ DEBUG: Loaded current positions data with {data.get('total_positions', 0)} positions")
            return data
            
        except Exception as e:
            print(f"❌ DEBUG: Error loading current positions data: {e}")
            return {}

    def _execute_exceedance_trade(self, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """
        Execute exceedance trade (called by strategy script)
        
        Args:
            symbol: Stock symbol
            signal_data: Signal data from strategy
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract trade parameters
            quantity = signal_data.get('position_size', 0)
            current_price = signal_data.get('current_price', 0.0)
            auto_approve = signal_data.get('auto_approve', False)
            
            # Validate parameters
            if not auto_approve:
                self.logger.debug(f"🔒 Auto-approve disabled for {symbol}")
                return False
            
            if quantity <= 0 or current_price <= 0:
                self.logger.error(f"❌ Invalid trade parameters: {symbol} {quantity}@${current_price:.2f}")
                return False
            
            # Execute complete trade
            result = self.execute_complete_trade(symbol, quantity, current_price)
            
            if result.get('success', False):
                self.logger.info(f"🎉 Exceedance trade completed: {symbol}")
                return True
            else:
                self.logger.error(f"❌ Exceedance trade failed: {symbol} - {result.get('error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error in exceedance trade execution: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get current engine status"""
        return {
            'initialized': True,
            'profit_target_pct': self.profit_target_pct * 100,
            'config': self.config
        }

if __name__ == "__main__":
    # Initialize engine when run directly
    engine = ExceedanceTradingEngine()
