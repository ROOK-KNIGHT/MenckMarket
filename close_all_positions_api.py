#!/usr/bin/env python3
"""
Close All Positions API Handler

Clean, streamlined version for direct frontend calls.
No prompts, no fluff - just pure core logic.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging

# Import necessary handlers
from order_handler import OrderHandler

class CloseAllPositionsAPI:
    """Streamlined API handler for closing all positions and cancelling all orders."""
    
    def __init__(self):
        """Initialize the handler."""
        self.order_handler = OrderHandler()
        self.positions_file = 'current_positions.json'
        
        # Setup minimal logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def load_positions_data(self) -> Optional[Dict[str, Any]]:
        """Load current positions data from JSON file."""
        try:
            if not os.path.exists(self.positions_file):
                return None
            
            with open(self.positions_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            self.logger.error(f"Error loading positions: {e}")
            return None
    
    def cancel_all_orders(self, positions_data: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel all open orders."""
        cancelled_count = 0
        failed_count = 0
        cancelled_orders = []
        
        try:
            positions = positions_data.get('positions', {})
            
            for position_data in positions.values():
                open_orders = position_data.get('open_orders', [])
                
                for order in open_orders:
                    try:
                        order_id = order.get('order_id', '')
                        status = order.get('status', '')
                        
                        # Only cancel working orders
                        if status in ['WORKING', 'QUEUED', 'PENDING_ACTIVATION']:
                            cancel_result = self.order_handler.cancel_order(str(order_id))
                            
                            if 'error' not in cancel_result:
                                cancelled_count += 1
                                cancelled_orders.append({
                                    'order_id': order_id,
                                    'symbol': order.get('symbol', ''),
                                    'instruction': order.get('instruction', ''),
                                    'quantity': order.get('quantity', 0),
                                    'price': order.get('price', 0)
                                })
                            else:
                                failed_count += 1
                    
                    except Exception:
                        failed_count += 1
                        continue
            
            return {
                'success': True,
                'cancelled_count': cancelled_count,
                'failed_count': failed_count,
                'cancelled_orders': cancelled_orders
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'cancelled_count': 0,
                'failed_count': 0
            }
    
    def close_all_positions(self, positions_data: Dict[str, Any]) -> Dict[str, Any]:
        """Close all positions via market orders."""
        closed_count = 0
        failed_count = 0
        closed_positions = []
        
        try:
            positions = positions_data.get('positions', {})
            
            for position_data in positions.values():
                try:
                    symbol = position_data.get('symbol', '')
                    quantity = position_data.get('quantity', 0)
                    instrument_type = position_data.get('instrument_type', '')
                    account = position_data.get('account', '')
                    
                    # Skip if no quantity
                    if quantity == 0:
                        continue
                    
                    # Determine instruction
                    if quantity > 0:
                        instruction = 'SELL'
                        close_quantity = abs(quantity)
                    else:
                        instruction = 'BUY_TO_COVER'
                        close_quantity = abs(quantity)
                    
                    # Create market order
                    order_data = {
                        'orderType': 'MARKET',
                        'session': 'NORMAL',
                        'duration': 'DAY',
                        'orderStrategyType': 'SINGLE',
                        'orderLegCollection': [
                            {
                                'instruction': instruction,
                                'quantity': close_quantity,
                                'instrument': {
                                    'symbol': symbol,
                                    'assetType': instrument_type
                                }
                            }
                        ]
                    }
                    
                    # Place market order using the correct method
                    order_result = self.order_handler.place_market_order(instruction, symbol, close_quantity)
                    
                    if order_result.get('status') == 'submitted':
                        closed_count += 1
                        closed_positions.append({
                            'symbol': symbol,
                            'instruction': instruction,
                            'quantity': close_quantity,
                            'order_id': order_result.get('order_id', 'unknown')
                        })
                    else:
                        failed_count += 1
                
                except Exception:
                    failed_count += 1
                    continue
            
            return {
                'success': True,
                'closed_count': closed_count,
                'failed_count': failed_count,
                'closed_positions': closed_positions
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'closed_count': 0,
                'failed_count': 0
            }
    
    def execute(self) -> Dict[str, Any]:
        """Execute close all positions - main entry point."""
        try:
            # Load positions data
            positions_data = self.load_positions_data()
            
            if not positions_data:
                return {
                    'success': False,
                    'error': 'Failed to load positions data'
                }
            
            total_positions = positions_data.get('total_positions', 0)
            total_orders = positions_data.get('summary', {}).get('total_open_orders', 0)
            
            if total_positions == 0 and total_orders == 0:
                return {
                    'success': True,
                    'message': 'No positions or orders to close',
                    'positions_closed': 0,
                    'orders_cancelled': 0
                }
            
            # Cancel orders
            cancel_result = self.cancel_all_orders(positions_data)
            
            # Close positions
            close_result = self.close_all_positions(positions_data)
            
            # Return combined results
            return {
                'success': True,
                'execution_time': datetime.now().isoformat(),
                'orders_cancelled': cancel_result.get('cancelled_count', 0),
                'orders_cancel_failed': cancel_result.get('failed_count', 0),
                'positions_closed': close_result.get('closed_count', 0),
                'positions_close_failed': close_result.get('failed_count', 0),
                'cancelled_orders': cancel_result.get('cancelled_orders', []),
                'closed_positions': close_result.get('closed_positions', [])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Main entry point for direct execution."""
    try:
        handler = CloseAllPositionsAPI()
        result = handler.execute()
        
        # Output JSON for frontend consumption
        print(json.dumps(result, indent=2))
        
        return 0 if result.get('success', False) else 1
        
    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e)
        }
        print(json.dumps(error_result, indent=2))
        return 1


if __name__ == "__main__":
    exit(main())
