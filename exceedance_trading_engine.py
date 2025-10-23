#!/usr/bin/env python3
"""
Exceedance Trading Engine - Callable Methods

Clean trading engine for exceedance strategy with callable methods:
1. place_buy_order() - Place market buy orders
2. place_take_profit() - Place profit target orders  
3. get_current_positions() - Get current positions
4. check_order_status() - Check order status
5. calculate_profit_target() - Calculate profit target price
6. wait_for_position() - Wait for position to appear
7. cancel_order() - Cancel orders

Simple, direct methods that the strategy script can call as needed.
No file watchers, no event handling - just clean trading functions.
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
    Simple trading engine with callable methods for exceedance strategy
    """
    
    def __init__(self):
        """Initialize the trading engine"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize handlers
        self.order_handler = OrderHandler()
        self.position_handler = CurrentPositionsHandler()
        
        # Configuration
        self.config = self._load_trading_config()
        
        # Profit target settings (same as exceedence_strategy.py)
        self.profit_target_pct = 0.00045  # 0.045% profit target

        self.logger.info("🎯 Exceedance Trading Engine initialized")
        self.logger.info(f"💰 Profit target: {self.profit_target_pct*100:.3f}%")

    def _load_trading_config(self) -> Dict[str, Any]:
        """Load trading configuration from PML strategy"""
        try:
            if not os.path.exists('trading_config_live.json'):
                self.logger.error("❌ trading_config_live.json not found")
                return self._get_default_config()
            
            with open('trading_config_live.json', 'r') as f:
                trading_config = json.load(f)
            
            # Extract PML strategy configuration
            pml_config = trading_config.get('strategies', {}).get('pml', {})
            if not pml_config:
                self.logger.error("❌ No PML strategy configuration found")
                return self._get_default_config()
            
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
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "auto_approve": False,
            "profit_target_pct": 0.00045,
            "position_check_timeout": 60,
            "position_check_interval": 2
        }

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
                
                # Step 1: Get current position from current_positions.json to calculate total size after scale-in
                positions_data = self._load_current_positions_data()
                if positions_data:
                    positions = positions_data.get('positions', {})
                    symbol_found = False
                    
                    for position_key, position_data in positions.items():
                        position_symbol = position_data.get('symbol', '')
                        if position_symbol == symbol:
                            symbol_found = True
                            existing_quantity = position_data.get('quantity', 0)
                            total_position_size = existing_quantity + quantity
                            print(f"📊 DEBUG: Current position: {existing_quantity} shares, adding {quantity}, total will be: {total_position_size}")
                            self.logger.info(f"📊 Current position: {existing_quantity} shares, adding {quantity}, total will be: {total_position_size}")
                            break
                    
                    if not symbol_found:
                        print(f"⚠️ DEBUG: Scale-in requested but no existing position found for {symbol}")
                        self.logger.warning(f"⚠️ Scale-in requested but no existing position found for {symbol}")
                else:
                    print(f"❌ DEBUG: Failed to load current positions data for scale-in calculation")
                    self.logger.warning(f"⚠️ Failed to load current positions data for scale-in calculation")
                
                # Step 2: Cancel existing profit target orders for this symbol
                print(f"🚫 DEBUG: Cancelling existing profit targets for {symbol}")
                cancel_result = self.cancel_existing_profit_targets(symbol)
                if cancel_result.get('cancelled_orders', 0) > 0:
                    print(f"✅ DEBUG: Cancelled {cancel_result['cancelled_orders']} existing profit target(s) for {symbol}")
                    self.logger.info(f"✅ Cancelled {cancel_result['cancelled_orders']} existing profit target(s) for {symbol}")
                    
                    # Small wait after cancellation before placing new orders
                    print(f"⏳ DEBUG: Waiting 1 second after order cancellation before placing new orders...")
                    time.sleep(1)
                else:
                    print(f"ℹ️ DEBUG: No existing profit targets found to cancel for {symbol}")
                
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
