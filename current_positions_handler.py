#!/usr/bin/env python3
"""
Current Positions Handler

This handler manages real-time position data from Schwab accounts:
1. Fetches current positions from Schwab API with full response handling
2. Processes and formats position data including all API fields
3. Calculates position summaries and metrics
4. Produces current_positions.json for realtime_monitor consumption
5. Inserts data into PostgreSQL with truncation

Key Features:
- Real-time position tracking from Schwab API with comprehensive data
- Full API response handling including balances, account info, and positions
- Comprehensive position metrics and summaries
- JSON output for realtime_monitor integration
- Database integration with truncation for fresh data
- Error handling and logging
- Account-level aggregation
- Symbol-based position tracking
- Options contract details and analysis
"""

import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import connection manager for Schwab API
from connection_manager import (
    get_all_positions, 
    get_comprehensive_account_data,
    extract_account_balances,
    extract_detailed_positions
)
from order_handler import OrderHandler

class CurrentPositionsHandler:
    """
    Current Positions Handler for real-time position tracking and database management
    """
    
    def __init__(self):
        """Initialize the Current Positions handler."""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize order handler for open orders functionality
        try:
            self.order_handler = OrderHandler()
            self.logger.info("OrderHandler initialized successfully")
        except Exception as e:
            self.logger.warning(f"Could not initialize OrderHandler: {e}")
            self.order_handler = None
        
        # Database connection parameters
        self.db_config = {
            'host': 'localhost',
            'database': 'volflow_options',
            'user': 'isaac',
            'password': None  # Will use peer authentication
        }
        
        # Database insertion throttling (30-second intervals)
        self.last_db_insertion = 0
        self.db_insertion_interval = 30  # seconds
        
        self.logger.info("CurrentPositionsHandler initialized")

    def fetch_open_orders(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch open orders from Schwab API and group them by symbol.
        
        Returns:
            Dictionary with symbols as keys and lists of open orders as values
        """
        try:
            if not self.order_handler:
                self.logger.warning("OrderHandler not available - cannot fetch open orders")
                return {}
            
            self.logger.info("🔄 Fetching open orders from Schwab API...")
            
            # Calculate date range for orders (today only to get current open orders)
            today = datetime.now()
            from_date = today.strftime('%Y-%m-%dT00:00:00.000Z')
            to_date = today.strftime('%Y-%m-%dT23:59:59.000Z')
            
            orders_by_symbol = {}
            total_open_orders = 0
            
            # Get WORKING orders (open orders) using the correct API approach
            try:
                self.logger.info(f"Fetching WORKING orders from {from_date} to {to_date}")
                orders_response = self.order_handler.get_all_orders(
                    from_entered_time=from_date,
                    to_entered_time=to_date,
                    status="WORKING",
                    max_results=1000
                )
                
                # The API returns a direct list of orders
                if isinstance(orders_response, list):
                    working_orders = orders_response
                    self.logger.info(f"Retrieved {len(working_orders)} WORKING orders")
                    
                    for order in working_orders:
                        # Extract symbol from order
                        symbol = self._extract_symbol_from_order(order)
                        
                        if symbol:
                            if symbol not in orders_by_symbol:
                                orders_by_symbol[symbol] = []
                            
                            # Add processed order information
                            processed_order = self._process_order_data(order)
                            orders_by_symbol[symbol].append(processed_order)
                            total_open_orders += 1
                            
                            self.logger.info(f"Found WORKING order for {symbol}: {order.get('orderType')} {order.get('instruction', 'N/A')} @ ${order.get('price', 'N/A')}")
                
                elif isinstance(orders_response, dict) and 'error' in orders_response:
                    self.logger.error(f"Error in API response: {orders_response['error']}")
                else:
                    self.logger.warning(f"Unexpected response format: {type(orders_response)}")
                
            except Exception as e:
                self.logger.error(f"Error fetching WORKING orders: {e}")
            
            # Also try to get other open order statuses
            other_open_statuses = ["QUEUED", "ACCEPTED", "AWAITING_PARENT_ORDER", 
                                 "AWAITING_CONDITION", "AWAITING_STOP_CONDITION", "PENDING_ACTIVATION"]
            
            for status in other_open_statuses:
                try:
                    self.logger.info(f"Fetching {status} orders...")
                    orders_response = self.order_handler.get_all_orders(
                        from_entered_time=from_date,
                        to_entered_time=to_date,
                        status=status,
                        max_results=1000
                    )
                    
                    if isinstance(orders_response, list) and len(orders_response) > 0:
                        self.logger.info(f"Retrieved {len(orders_response)} {status} orders")
                        
                        for order in orders_response:
                            symbol = self._extract_symbol_from_order(order)
                            
                            if symbol:
                                if symbol not in orders_by_symbol:
                                    orders_by_symbol[symbol] = []
                                
                                processed_order = self._process_order_data(order)
                                orders_by_symbol[symbol].append(processed_order)
                                total_open_orders += 1
                                
                                self.logger.info(f"Found {status} order for {symbol}")
                    
                except Exception as e:
                    self.logger.warning(f"Error fetching {status} orders: {e}")
                    continue
            
            self.logger.info(f"✅ Successfully fetched {total_open_orders} open orders across {len(orders_by_symbol)} symbols")
            
            return orders_by_symbol
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching open orders: {e}")
            return {}

    def _extract_symbol_from_order(self, order: Dict[str, Any]) -> Optional[str]:
        """
        Extract symbol from order data.
        
        Args:
            order: Order data from Schwab API
            
        Returns:
            Symbol string or None if not found
        """
        try:
            # Check orderLegCollection for symbol
            if 'orderLegCollection' in order and len(order['orderLegCollection']) > 0:
                leg = order['orderLegCollection'][0]
                if 'instrument' in leg and 'symbol' in leg['instrument']:
                    return leg['instrument']['symbol']
            
            # Fallback: check if there's a direct symbol field
            if 'symbol' in order:
                return order['symbol']
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error extracting symbol from order: {e}")
            return None

    def _process_order_data(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process raw order data into structured format.
        
        Args:
            order: Raw order data from Schwab API
            
        Returns:
            Processed order data
        """
        try:
            # Extract basic order information
            processed_order = {
                'order_id': order.get('orderId', ''),
                'status': order.get('status', ''),
                'order_type': order.get('orderType', ''),
                'session': order.get('session', ''),
                'duration': order.get('duration', ''),
                'entered_time': order.get('enteredTime', ''),
                'close_time': order.get('closeTime', ''),
                'price': order.get('price', 0),
                'stop_price': order.get('stopPrice', 0),
                'quantity': 0,
                'filled_quantity': order.get('filledQuantity', 0),
                'remaining_quantity': order.get('remainingQuantity', 0),
                'instruction': '',
                'asset_type': '',
                'symbol': '',
                'order_source': 'schwab_api'
            }
            
            # Extract leg information
            if 'orderLegCollection' in order and len(order['orderLegCollection']) > 0:
                leg = order['orderLegCollection'][0]
                processed_order['instruction'] = leg.get('instruction', '')
                processed_order['quantity'] = leg.get('quantity', 0)
                
                if 'instrument' in leg:
                    instrument = leg['instrument']
                    processed_order['symbol'] = instrument.get('symbol', '')
                    processed_order['asset_type'] = instrument.get('assetType', '')
            
            # Calculate remaining quantity if not provided
            if processed_order['remaining_quantity'] == 0 and processed_order['quantity'] > 0:
                processed_order['remaining_quantity'] = processed_order['quantity'] - processed_order['filled_quantity']
            
            # Add order strategy information
            processed_order['order_strategy_type'] = order.get('orderStrategyType', '')
            processed_order['complex_order_strategy_type'] = order.get('complexOrderStrategyType', '')
            
            # Add child orders information if present
            if 'childOrderStrategies' in order:
                processed_order['has_child_orders'] = True
                processed_order['child_orders_count'] = len(order['childOrderStrategies'])
            else:
                processed_order['has_child_orders'] = False
                processed_order['child_orders_count'] = 0
            
            return processed_order
            
        except Exception as e:
            self.logger.warning(f"Error processing order data: {e}")
            return {
                'order_id': order.get('orderId', 'unknown'),
                'status': order.get('status', 'unknown'),
                'error': f'Processing error: {str(e)}',
                'order_source': 'schwab_api'
            }

    def integrate_schwab_open_orders_only(self, positions_data: Dict[str, Any], schwab_open_orders: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Integrate only Schwab open orders into the positions data structure (no local orders).
        
        Args:
            positions_data: Current positions data
            schwab_open_orders: Open orders from Schwab API grouped by symbol
            
        Returns:
            Enhanced positions data with Schwab open orders integrated
        """
        try:
            # Combine all symbols that have either positions or Schwab orders
            all_symbols = set(positions_data.get('positions', {}).keys())
            all_symbols.update(schwab_open_orders.keys())
            
            # Extract symbols from position keys (remove account suffixes)
            position_symbols = set()
            for position_key in positions_data.get('positions', {}):
                symbol = positions_data['positions'][position_key].get('symbol', '')
                if symbol:
                    position_symbols.add(symbol)
                    all_symbols.add(symbol)
            
            # Process each symbol
            for symbol in all_symbols:
                # Find existing position entries for this symbol
                matching_positions = []
                for position_key, position in positions_data.get('positions', {}).items():
                    if position.get('symbol') == symbol:
                        matching_positions.append((position_key, position))
                
                # Get Schwab orders for this symbol
                schwab_orders = schwab_open_orders.get(symbol, [])
                
                if matching_positions:
                    # Add Schwab orders to existing positions
                    for position_key, position in matching_positions:
                        position['open_orders'] = schwab_orders
                        position['total_orders'] = len(schwab_orders)
                else:
                    # Create new entry for symbols with only Schwab orders (no current position)
                    if schwab_orders:
                        position_key = f"{symbol}_orders_only"
                        positions_data['positions'][position_key] = {
                            'symbol': symbol,
                            'account': 'multiple',
                            'quantity': 0,
                            'current_price': 0,
                            'market_value': 0,
                            'cost_basis': 0,
                            'unrealized_pl': 0,
                            'unrealized_pl_percent': 0,
                            'day_pl': 0,
                            'position_type': 'FLAT',
                            'instrument_type': 'UNKNOWN',
                            'last_updated': datetime.now().isoformat(),
                            'open_orders': schwab_orders,
                            'total_orders': len(schwab_orders)
                        }
            
            # Ensure all existing positions have order fields (even if empty)
            for position_key, position in positions_data.get('positions', {}).items():
                if 'open_orders' not in position:
                    position['open_orders'] = []
                if 'total_orders' not in position:
                    position['total_orders'] = 0
            
            # Update summary to include Schwab order counts only
            total_schwab_orders = sum(len(orders) for orders in schwab_open_orders.values())
            
            if 'summary' not in positions_data:
                positions_data['summary'] = {}
            
            positions_data['summary']['total_open_orders'] = total_schwab_orders
            positions_data['summary']['symbols_with_orders'] = len(schwab_open_orders)
            
            self.logger.info(f"✅ Integrated {total_schwab_orders} Schwab open orders (no local orders)")
            
            return positions_data
            
        except Exception as e:
            self.logger.error(f"❌ Error integrating Schwab open orders: {e}")
            return positions_data

    def _create_empty_positions_response(self) -> Dict[str, Any]:
        """Create an empty positions response structure with all comprehensive fields."""
        return {
            'positions': {},
            'summary': {
                'total_positions': 0,
                'total_market_value': 0.0,
                'total_unrealized_pl': 0.0,
                'total_day_pl': 0.0,
                'total_cost_basis': 0.0,
                'accounts_count': 0,
                'symbols_count': 0,
                'winning_positions': 0,
                'losing_positions': 0,
                'break_even_positions': 0,
                'options_summary': {
                    'total_options_positions': 0,
                    'options_market_value': 0.0,
                    'options_unrealized_pl': 0.0,
                    'options_percentage_of_portfolio': 0.0
                },
                'equity_summary': {
                    'total_equity_positions': 0,
                    'equity_market_value': 0.0,
                    'equity_unrealized_pl': 0.0,
                    'equity_percentage_of_portfolio': 0.0
                }
            },
            'accounts': {},
            'account_balances': {},
            'symbols': [],
            'timestamp': datetime.now().isoformat(),
            'comprehensive_data': None,
            'data_source': 'schwab_api',
            'fetch_success': False
        }

    def load_positions_from_json(self) -> Optional[Dict[str, Any]]:
        """
        Load positions data from JSON file.
        
        Returns:
            Dictionary containing positions data or None if not found
        """
        try:
            import os
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            positions_file_path = os.path.join(script_dir, 'current_positions.json')
            
            if not os.path.exists(positions_file_path):
                return None
            
            with open(positions_file_path, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            self.logger.error(f"❌ Error loading positions from JSON: {e}")
            return None

    def fetch_current_positions(self) -> Dict[str, Any]:
        """
        Fetch current positions from Schwab API with comprehensive response handling.
        
        Returns:
            Dictionary containing comprehensive positions data, balances, and summaries
        """
        try:
            self.logger.info("🔄 Fetching comprehensive account data from Schwab API...")
            
            # Get comprehensive account data (full API response)
            comprehensive_data = get_comprehensive_account_data()
            
            if not comprehensive_data:
                self.logger.warning("No comprehensive account data received from API")
                return self._create_empty_positions_response()
            
            # Extract detailed balances and positions
            account_balances = extract_account_balances(comprehensive_data)
            detailed_positions = extract_detailed_positions(comprehensive_data)
            
            # Also get the legacy format for backward compatibility
            legacy_positions_data = get_all_positions()
            
            if not legacy_positions_data and not detailed_positions:
                self.logger.warning("No positions data received from any API method")
                return self._create_empty_positions_response()
            
            # Use the enhanced data from get_all_positions (which now includes comprehensive data)
            positions_data = legacy_positions_data if legacy_positions_data else {}
            
            # Process and format positions data with ALL API fields
            formatted_positions = {}
            account_summaries = {}
            symbols_set = set()
            
            # Totals across all accounts
            total_market_value = 0.0
            total_unrealized_pl = 0.0
            total_day_pl = 0.0
            total_cost_basis = 0.0
            total_positions_count = 0
            
            # Process each account with comprehensive data
            for account_number, account_data in positions_data.items():
                # Handle both old and new data structures
                if isinstance(account_data, dict) and 'positions' in account_data:
                    # New comprehensive structure
                    positions = account_data['positions']
                    account_info = account_data
                else:
                    # Legacy structure - list of positions
                    positions = account_data if isinstance(account_data, list) else []
                    account_info = {'account_number': account_number}
                
                account_market_value = 0.0
                account_unrealized_pl = 0.0
                account_day_pl = 0.0
                account_cost_basis = 0.0
                account_positions_count = len(positions)
                
                # Process each position with ALL available fields
                for position in positions:
                    symbol = position.get('symbol', '')
                    if not symbol:
                        continue
                    
                    symbols_set.add(symbol)
                    
                    # Extract ALL position data fields from the comprehensive API response
                    position_data = {
                        # Basic position info
                        'symbol': symbol,
                        'account': account_number,
                        'cusip': position.get('cusip', ''),
                        'asset_type': position.get('asset_type', position.get('instrument_type', 'EQUITY')),
                        'net_change': position.get('net_change', 0),
                        
                        # Quantities - ALL quantity fields from API
                        'short_quantity': position.get('short_quantity', 0),
                        'long_quantity': position.get('long_quantity', 0),
                        'settled_long_quantity': position.get('settled_long_quantity', 0),
                        'settled_short_quantity': position.get('settled_short_quantity', 0),
                        'previous_session_long_quantity': position.get('previous_session_long_quantity', 0),
                        'quantity': position.get('quantity', position.get('long_quantity', 0) - position.get('short_quantity', 0)),
                        
                        # Pricing and values - ALL pricing fields from API
                        'average_price': position.get('average_price', 0),
                        'average_long_price': position.get('average_long_price', 0),
                        'tax_lot_average_long_price': position.get('tax_lot_average_long_price', 0),
                        'market_value': position.get('market_value', 0),
                        'maintenance_requirement': position.get('maintenance_requirement', 0),
                        'current_price': position.get('current_price', 0),
                        'cost_basis': position.get('cost_basis', position.get('average_price', 0)),
                        
                        # P&L calculations - ALL P&L fields from API
                        'current_day_profit_loss': position.get('current_day_profit_loss', 0),
                        'current_day_profit_loss_percentage': position.get('current_day_profit_loss_percentage', 0),
                        'long_open_profit_loss': position.get('long_open_profit_loss', 0),
                        'current_day_cost': position.get('current_day_cost', 0),
                        'unrealized_pl': position.get('unrealized_pl', position.get('long_open_profit_loss', 0)),
                        'unrealized_pl_percent': position.get('unrealized_pl_percent', position.get('current_day_profit_loss_percentage', 0)),
                        'day_pl': position.get('day_pl', position.get('current_day_profit_loss', 0)),
                        
                        # Options-specific fields - ALL options fields from API
                        'underlying_symbol': position.get('underlying_symbol', ''),
                        'option_deliverables': position.get('option_deliverables', []),
                        'option_multiplier': position.get('option_multiplier', 100),
                        'put_call': position.get('put_call', ''),
                        'strike_price': position.get('strike_price', 0),
                        'expiration_date': position.get('expiration_date', ''),
                        
                        # Calculated fields
                        'position_type': self._determine_position_type(position.get('quantity', position.get('long_quantity', 0) - position.get('short_quantity', 0))),
                        'instrument_type': position.get('asset_type', position.get('instrument_type', 'EQUITY')),
                        'last_updated': datetime.now().isoformat(),
                    }
                    
                    # Calculate current price if not provided
                    if position_data['current_price'] == 0:
                        total_quantity = abs(position_data['quantity'])
                        if total_quantity > 0 and position_data['market_value'] != 0:
                            position_data['current_price'] = abs(position_data['market_value'] / total_quantity)
                    
                    # Enhanced options contract handling with ALL fields
                    if position_data['instrument_type'] == 'OPTION':
                        options_data = self._extract_comprehensive_options_data(position)
                        position_data.update(options_data)
                    
                    # Create unique position key
                    if position_data['instrument_type'] == 'OPTION':
                        position_key = f"{symbol}_{account_number}_{position_data.get('strike_price', '')}_{position_data.get('put_call', '')}_{position_data.get('expiration_date', '')}"
                    else:
                        position_key = f"{symbol}_{account_number}"
                    
                    formatted_positions[position_key] = position_data
                    
                    # Add to account totals
                    account_market_value += position_data['market_value']
                    account_unrealized_pl += position_data['unrealized_pl']
                    account_day_pl += position_data['day_pl']
                    account_cost_basis += position_data['cost_basis']
                
                # Store comprehensive account summary with ALL account fields
                account_summary = {
                    'account_number': account_number,
                    'account_type': account_info.get('account_type', 'UNKNOWN'),
                    'round_trips': account_info.get('round_trips', 0),
                    'is_day_trader': account_info.get('is_day_trader', False),
                    'is_closing_only_restricted': account_info.get('is_closing_only_restricted', False),
                    'pfcb_flag': account_info.get('pfcb_flag', False),
                    'positions_count': account_positions_count,
                    'market_value': account_market_value,
                    'unrealized_pl': account_unrealized_pl,
                    'day_pl': account_day_pl,
                    'cost_basis': account_cost_basis,
                    'unrealized_pl_percent': (account_unrealized_pl / account_cost_basis * 100) if account_cost_basis != 0 else 0,
                    'symbols': [pos.get('symbol') for pos in positions if pos.get('symbol')],
                    
                    # Include ALL balance information from API
                    'initial_balances': account_info.get('initial_balances', {}),
                    'current_balances': account_info.get('current_balances', {}),
                    'projected_balances': account_info.get('projected_balances', {}),
                    'aggregated_balance': account_info.get('aggregated_balance', {})
                }
                
                account_summaries[account_number] = account_summary
                
                # Add to overall totals
                total_market_value += account_market_value
                total_unrealized_pl += account_unrealized_pl
                total_day_pl += account_day_pl
                total_cost_basis += account_cost_basis
                total_positions_count += account_positions_count
            
            # Enhanced summary with comprehensive options-specific metrics
            options_positions = [p for p in formatted_positions.values() if p.get('instrument_type') == 'OPTION']
            equity_positions = [p for p in formatted_positions.values() if p.get('instrument_type') != 'OPTION']
            
            # Options-specific calculations
            options_market_value = sum(p.get('market_value', 0) for p in options_positions)
            options_unrealized_pl = sum(p.get('unrealized_pl', 0) for p in options_positions)
            
            # Categorize options by type and moneyness
            call_positions = [p for p in options_positions if p.get('put_call') == 'CALL']
            put_positions = [p for p in options_positions if p.get('put_call') == 'PUT']
            itm_positions = [p for p in options_positions if p.get('moneyness') == 'ITM']
            otm_positions = [p for p in options_positions if p.get('moneyness') == 'OTM']
            
            # Days to expiration analysis
            expiring_soon = [p for p in options_positions if p.get('days_to_expiration', 999) <= 7]
            expiring_this_month = [p for p in options_positions if 7 < p.get('days_to_expiration', 999) <= 30]
            
            # Comprehensive summary with ALL metrics
            summary = {
                'total_positions': total_positions_count,
                'total_market_value': total_market_value,
                'total_unrealized_pl': total_unrealized_pl,
                'total_day_pl': total_day_pl,
                'total_cost_basis': total_cost_basis,
                'total_unrealized_pl_percent': (total_unrealized_pl / total_cost_basis * 100) if total_cost_basis != 0 else 0,
                'accounts_count': len(account_summaries),
                'symbols_count': len(symbols_set),
                'winning_positions': len([p for p in formatted_positions.values() if p['unrealized_pl'] > 0]),
                'losing_positions': len([p for p in formatted_positions.values() if p['unrealized_pl'] < 0]),
                'break_even_positions': len([p for p in formatted_positions.values() if p['unrealized_pl'] == 0]),
                
                # Comprehensive options-specific metrics
                'options_summary': {
                    'total_options_positions': len(options_positions),
                    'options_market_value': options_market_value,
                    'options_unrealized_pl': options_unrealized_pl,
                    'options_percentage_of_portfolio': (options_market_value / total_market_value * 100) if total_market_value != 0 else 0,
                    
                    'by_type': {
                        'call_positions': len(call_positions),
                        'put_positions': len(put_positions),
                        'call_market_value': sum(p.get('market_value', 0) for p in call_positions),
                        'put_market_value': sum(p.get('market_value', 0) for p in put_positions)
                    },
                    
                    'by_moneyness': {
                        'itm_positions': len(itm_positions),
                        'otm_positions': len(otm_positions),
                        'itm_market_value': sum(p.get('market_value', 0) for p in itm_positions),
                        'otm_market_value': sum(p.get('market_value', 0) for p in otm_positions)
                    },
                    
                    'expiration_analysis': {
                        'expiring_within_week': len(expiring_soon),
                        'expiring_this_month': len(expiring_this_month),
                        'expiring_soon_value': sum(p.get('market_value', 0) for p in expiring_soon),
                        'theta_risk': sum(p.get('theta', 0) for p in options_positions if p.get('theta'))
                    }
                },
                
                # Comprehensive equity summary
                'equity_summary': {
                    'total_equity_positions': len(equity_positions),
                    'equity_market_value': sum(p.get('market_value', 0) for p in equity_positions),
                    'equity_unrealized_pl': sum(p.get('unrealized_pl', 0) for p in equity_positions),
                    'equity_percentage_of_portfolio': ((total_market_value - options_market_value) / total_market_value * 100) if total_market_value != 0 else 0
                }
            }
            
            # Comprehensive result with ALL data
            result = {
                'positions': formatted_positions,
                'summary': summary,
                'accounts': account_summaries,
                'account_balances': account_balances,
                'symbols': sorted(list(symbols_set)),
                'timestamp': datetime.now().isoformat(),
                'comprehensive_data': comprehensive_data,
                'data_source': 'schwab_api',
                'fetch_success': True
            }
            
            self.logger.info(f"✅ Successfully fetched {total_positions_count} positions across {len(account_summaries)} accounts")
            self.logger.info(f"📊 Total Market Value: ${total_market_value:,.2f}, Unrealized P&L: ${total_unrealized_pl:,.2f}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching current positions: {e}")
            return {
                'positions': {},
                'summary': {
                    'total_positions': 0,
                    'total_market_value': 0.0,
                    'total_unrealized_pl': 0.0,
                    'total_day_pl': 0.0,
                    'total_cost_basis': 0.0,
                    'accounts_count': 0,
                    'symbols_count': 0
                },
                'accounts': {},
                'account_balances': {},
                'symbols': [],
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'fetch_success': False
            }

    def _determine_position_type(self, quantity: float) -> str:
        """Determine position type based on quantity."""
        if quantity > 0:
            return 'LONG'
        elif quantity < 0:
            return 'SHORT'
        else:
            return 'FLAT'

    def _extract_comprehensive_options_data(self, position: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive options-specific data from position with ALL API fields.
        
        Args:
            position: Position data from Schwab API
            
        Returns:
            Dictionary with comprehensive options-specific fields
        """
        try:
            options_data = {}
            
            # Strike price
            strike = position.get('strike_price', 0)
            if strike:
                options_data['strike'] = float(strike)
            
            # Option type (CALL/PUT)
            option_type = position.get('put_call', '')
            if option_type:
                options_data['option_type'] = option_type.upper()
            
            # Expiration date
            expiration = position.get('expiration_date', '')
            if expiration:
                options_data['expiration_date'] = str(expiration)
                
                # Days to expiration
                try:
                    if 'T' in expiration:
                        exp_date = datetime.fromisoformat(expiration.replace('Z', '+00:00'))
                    else:
                        exp_date = datetime.strptime(expiration, '%Y-%m-%d')
                    today = datetime.now()
                    dte = (exp_date - today).days
                    options_data['days_to_expiration'] = dte
                except:
                    pass
            
            # Underlying symbol
            underlying_symbol = position.get('underlying_symbol', '')
            if underlying_symbol:
                options_data['underlying_symbol'] = underlying_symbol
            elif 'symbol' in position:
                # Try to extract underlying from option symbol
                symbol = position['symbol']
                if len(symbol) > 6:
                    import re
                    match = re.match(r'^([A-Z]+)', symbol)
                    if match:
                        options_data['underlying_symbol'] = match.group(1)
            
            # Contract multiplier
            multiplier = position.get('option_multiplier', 100)
            options_data['multiplier'] = int(multiplier)
            
            # Option deliverables
            deliverables = position.get('option_deliverables', [])
            options_data['option_deliverables'] = deliverables
            
            # Moneyness calculation
            if options_data.get('option_type') and options_data.get('strike'):
                current_price = position.get('current_price', 0)
                strike = options_data['strike']
                option_type = options_data['option_type']
                
                if option_type == 'CALL':
                    if current_price > strike:
                        options_data['moneyness'] = 'ITM'
                    elif current_price < strike:
                        options_data['moneyness'] = 'OTM'
                    else:
                        options_data['moneyness'] = 'ATM'
                elif option_type == 'PUT':
                    if current_price < strike:
                        options_data['moneyness'] = 'ITM'
                    elif current_price > strike:
                        options_data['moneyness'] = 'OTM'
                    else:
                        options_data['moneyness'] = 'ATM'
            
            # Intrinsic and extrinsic value
            if options_data.get('option_type') and options_data.get('strike'):
                current_price = position.get('current_price', 0)
                strike = options_data['strike']
                option_type = options_data['option_type']
                market_price = position.get('current_price', 0)
                
                if option_type == 'CALL':
                    intrinsic = max(0, current_price - strike)
                elif option_type == 'PUT':
                    intrinsic = max(0, strike - current_price)
                else:
                    intrinsic = 0
                
                options_data['intrinsic_value'] = intrinsic
                if market_price > 0:
                    options_data['extrinsic_value'] = market_price - intrinsic
            
            return options_data
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting comprehensive options data: {e}")
            return {}

    def insert_positions_to_db(self, positions_data: Dict[str, Any]) -> bool:
        """
        Insert current positions into PostgreSQL database with truncation.
        
        Args:
            positions_data: Dictionary containing positions data
            
        Returns:
            Boolean indicating success
        """
        try:
            # Connect to database
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Truncate the existing positions table first
            self.logger.info("🗑️  Truncating positions table...")
            cur.execute("DELETE FROM positions;")
            
            # Insert new positions data
            positions = positions_data.get('positions', {})
            insert_count = 0
            
            for position_key, position in positions.items():
                try:
                    # Prepare insert statement for existing positions table structure
                    insert_sql = """
                    INSERT INTO positions (
                        timestamp, symbol, quantity, market_value, cost_basis,
                        unrealized_pl, unrealized_pl_percent, account
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    # Prepare values to match existing positions table structure
                    values = (
                        datetime.now(),
                        position.get('symbol'),
                        position.get('quantity', 0),
                        position.get('market_value', 0),
                        position.get('cost_basis', 0),
                        position.get('unrealized_pl', 0),
                        position.get('unrealized_pl_percent', 0),
                        position.get('account')
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Error inserting position {position_key}: {e}")
                    continue
            
            # Commit the transaction
            conn.commit()
            
            # Close connections
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Successfully inserted {insert_count} current positions into database")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting positions to database: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def save_positions_to_json(self, positions_data: Dict[str, Any]) -> bool:
        """
        Save current positions data to JSON file for realtime_monitor consumption.
        
        Args:
            positions_data: Dictionary containing positions data
            
        Returns:
            Boolean indicating success
        """
        try:
            import os
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            positions_file_path = os.path.join(script_dir, 'current_positions.json')
            
            # Create comprehensive positions data for realtime_monitor consumption
            positions_json_data = {
                'handler_name': 'Current_Positions_Handler',
                'last_updated': datetime.now().isoformat(),
                'total_positions': positions_data.get('summary', {}).get('total_positions', 0),
                'total_market_value': positions_data.get('summary', {}).get('total_market_value', 0),
                'total_unrealized_pl': positions_data.get('summary', {}).get('total_unrealized_pl', 0),
                'fetch_success': positions_data.get('fetch_success', False),
                'analysis_summary': {
                    'winning_positions': positions_data.get('summary', {}).get('winning_positions', 0),
                    'losing_positions': positions_data.get('summary', {}).get('losing_positions', 0),
                    'break_even_positions': positions_data.get('summary', {}).get('break_even_positions', 0),
                    'accounts_tracked': positions_data.get('summary', {}).get('accounts_count', 0),
                    'symbols_tracked': positions_data.get('summary', {}).get('symbols_count', 0)
                },
                'positions': positions_data.get('positions', {}),
                'summary': positions_data.get('summary', {}),
                'accounts': positions_data.get('accounts', {}),
                'account_balances': positions_data.get('account_balances', {}),
                'symbols': positions_data.get('symbols', []),
                'comprehensive_data': positions_data.get('comprehensive_data'),
                'metadata': {
                    'data_source': 'schwab_api',
                    'handler_type': 'real_time_positions_tracking',
                    'update_frequency': 'on_demand',
                    'database_integration': True,
                    'truncation_enabled': True,
                    'realtime_monitor_compatible': True,
                    'comprehensive_api_response': True
                }
            }
            
            # Write to positions JSON file for realtime_monitor
            with open(positions_file_path, 'w') as f:
                json.dump(positions_json_data, f, indent=2, default=str)
            
            summary = positions_data.get('summary', {})
            self.logger.info(f"✅ Saved comprehensive positions to {positions_file_path}")
            self.logger.info(f"📊 Summary: {summary.get('total_positions', 0)} positions, ${summary.get('total_market_value', 0):,.2f} market value")
            self.logger.info(f"🔄 JSON ready for realtime_monitor consumption with full API data")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error saving positions to JSON: {e}")
            return False

    def get_positions_summary(self) -> Dict[str, Any]:
        """
        Get a quick summary of current positions.
        
        Returns:
            Dictionary with positions summary
        """
        try:
            positions_data = self.fetch_current_positions()
            
            if not positions_data.get('fetch_success', False):
                return {
                    'error': 'Failed to fetch positions data',
                    'timestamp': datetime.now().isoformat()
                }
            
            summary = positions_data.get('summary', {})
            accounts = positions_data.get('accounts', {})
            symbols = positions_data.get('symbols', [])
            
            # Create detailed summary
            detailed_summary = {
                'timestamp': datetime.now().isoformat(),
                'overall_summary': summary,
                'account_breakdown': accounts,
                'symbols_tracked': symbols,
                'performance_metrics': {
                    'win_rate': (summary.get('winning_positions', 0) / max(1, summary.get('total_positions', 1))) * 100,
                    'avg_position_value': summary.get('total_market_value', 0) / max(1, summary.get('total_positions', 1)),
                    'portfolio_diversity': len(symbols),
                    'account_diversity': len(accounts)
                },
                'risk_metrics': {
                    'total_exposure': summary.get('total_market_value', 0),
                    'unrealized_risk': abs(summary.get('total_unrealized_pl', 0)) if summary.get('total_unrealized_pl', 0) < 0 else 0,
                    'day_performance': summary.get('total_day_pl', 0)
                }
            }
            
            return detailed_summary
            
        except Exception as e:
            self.logger.error(f"❌ Error getting positions summary: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def run_positions_analysis(self) -> bool:
        """
        Run complete positions analysis: fetch positions, fetch open orders, process, save JSON, and insert to database.
        
        Returns:
            Boolean indicating overall success
        """
        try:
            self.logger.info("🔄 Starting comprehensive positions analysis with open orders...")
            
            # Fetch current positions with comprehensive data
            positions_data = self.fetch_current_positions()
            
            # Fetch open orders from Schwab API only
            schwab_open_orders = self.fetch_open_orders()
            
            if not positions_data.get('fetch_success', False):
                self.logger.warning("⚠️  No positions data available - creating empty dataset")
                positions_data = {
                    'positions': {},
                    'summary': {
                        'total_positions': 0,
                        'total_market_value': 0.0,
                        'total_unrealized_pl': 0.0,
                        'total_day_pl': 0.0,
                        'total_cost_basis': 0.0,
                        'accounts_count': 0,
                        'symbols_count': 0,
                        'winning_positions': 0,
                        'losing_positions': 0,
                        'break_even_positions': 0
                    },
                    'accounts': {},
                    'account_balances': {},
                    'symbols': [],
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'schwab_api',
                    'fetch_success': True
                }
            
            # Integrate Schwab open orders into positions data
            positions_data = self.integrate_schwab_open_orders_only(positions_data, schwab_open_orders)
            
            # Always save to JSON file for realtime_monitor consumption
            json_success = self.save_positions_to_json(positions_data)
            
            # Insert to database with throttling
            current_time = time.time()
            db_success = True
            
            if current_time - self.last_db_insertion >= self.db_insertion_interval:
                self.logger.info(f"⏰ Database insertion interval reached ({self.db_insertion_interval}s), inserting to database...")
                db_success = self.insert_positions_to_db(positions_data)
                self.last_db_insertion = current_time
            else:
                time_remaining = self.db_insertion_interval - (current_time - self.last_db_insertion)
                self.logger.info(f"⏳ Database insertion throttled - {time_remaining:.1f}s remaining until next insertion")
            
            # Log results
            summary = positions_data.get('summary', {})
            self.logger.info("✅ Comprehensive positions analysis completed with Schwab open orders integration")
            self.logger.info(f"📊 Results: {summary.get('total_positions', 0)} positions, ${summary.get('total_market_value', 0):,.2f} value")
            self.logger.info(f"📋 Open Orders: {summary.get('total_open_orders', 0)} Schwab orders")
            self.logger.info(f"💾 JSON saved: {json_success}, Database inserted: {db_success}")
            
            return json_success and db_success
            
        except Exception as e:
            self.logger.error(f"❌ Error in comprehensive positions analysis: {e}")
            return False


def main():
    """Main function to run comprehensive current positions analysis"""
    print("🔄 Starting Comprehensive Current Positions Handler Analysis...")
    
    # Create positions handler
    handler = CurrentPositionsHandler()
    
    # Run complete analysis
    success = handler.run_positions_analysis()
    
    if success:
        print("✅ Comprehensive current positions analysis completed successfully")
        
        # Get and display summary
        summary = handler.get_positions_summary()
        if 'error' not in summary:
            overall = summary.get('overall_summary', {})
            print(f"📊 Portfolio Summary:")
            print(f"   • Total Positions: {overall.get('total_positions', 0)}")
            print(f"   • Market Value: ${overall.get('total_market_value', 0):,.2f}")
            print(f"   • Unrealized P&L: ${overall.get('total_unrealized_pl', 0):,.2f}")
            print(f"   • Day P&L: ${overall.get('total_day_pl', 0):,.2f}")
            print(f"   • Symbols Tracked: {overall.get('symbols_count', 0)}")
            print(f"   • Accounts: {overall.get('accounts_count', 0)}")
            print(f"🔄 Comprehensive JSON file ready for realtime_monitor consumption")
    else:
        print("❌ Comprehensive current positions analysis failed")


if __name__ == "__main__":
    main()
