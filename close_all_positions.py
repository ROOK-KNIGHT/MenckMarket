#!/usr/bin/env python3
"""
Close All Positions Script

This script will:
1. Load all current positions and open orders from current_positions.json
2. Cancel all open orders
3. Close all positions via market orders

Usage: python3 close_all_positions.py
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# Import necessary handlers
from order_handler import OrderHandler
from connection_manager import make_authenticated_request

class CloseAllPositionsHandler:
    """Handler for closing all positions and cancelling all orders."""
    
    def __init__(self):
        """Initialize the close all positions handler."""
        self.setup_logging()
        self.order_handler = OrderHandler()
        self.positions_file = 'current_positions.json'
        
        self.logger.info("🚀 Close All Positions Handler initialized")
    
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/close_all_positions.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_current_positions_data(self) -> Optional[Dict[str, Any]]:
        """
        Load current positions data from JSON file.
        
        Returns:
            Dict containing positions data or None if failed
        """
        try:
            if not os.path.exists(self.positions_file):
                self.logger.error(f"❌ Positions file not found: {self.positions_file}")
                return None
            
            with open(self.positions_file, 'r') as f:
                data = json.load(f)
            
            self.logger.info(f"✅ Loaded positions data from {self.positions_file}")
            return data
            
        except Exception as e:
            self.logger.error(f"❌ Error loading positions data: {e}")
            return None
    
    def cancel_all_open_orders(self, positions_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cancel all open orders found in positions data.
        
        Args:
            positions_data: Positions data containing open orders
            
        Returns:
            Dict with cancellation results
        """
        try:
            self.logger.info("🚫 Starting cancellation of all open orders...")
            
            cancelled_count = 0
            failed_count = 0
            cancelled_orders = []
            failed_orders = []
            
            positions = positions_data.get('positions', {})
            
            for position_key, position_data in positions.items():
                symbol = position_data.get('symbol', '')
                open_orders = position_data.get('open_orders', [])
                
                self.logger.info(f"🔍 Checking {len(open_orders)} open orders for {symbol}")
                
                for order in open_orders:
                    try:
                        order_id = order.get('order_id', '')
                        instruction = order.get('instruction', '')
                        order_type = order.get('order_type', '')
                        status = order.get('status', '')
                        price = order.get('price', 0)
                        quantity = order.get('quantity', 0)
                        
                        self.logger.info(f"🔍 Order: {order_id} - {instruction} {quantity} {symbol} @ ${price} ({status})")
                        
                        # Only cancel orders that are still working
                        if status in ['WORKING', 'QUEUED', 'PENDING_ACTIVATION']:
                            self.logger.info(f"🚫 Cancelling order {order_id}: {instruction} {quantity} {symbol} @ ${price}")
                            
                            cancel_result = self.order_handler.cancel_order(str(order_id))
                            
                            # API returns 200 with empty body on success, or error dict on failure
                            if 'error' not in cancel_result:
                                cancelled_count += 1
                                cancelled_orders.append({
                                    'order_id': order_id,
                                    'symbol': symbol,
                                    'instruction': instruction,
                                    'quantity': quantity,
                                    'price': price
                                })
                                self.logger.info(f"✅ Successfully cancelled order {order_id}")
                            else:
                                failed_count += 1
                                failed_orders.append({
                                    'order_id': order_id,
                                    'symbol': symbol,
                                    'error': cancel_result.get('error', 'Unknown error')
                                })
                                self.logger.warning(f"❌ Failed to cancel order {order_id}: {cancel_result.get('error', 'Unknown')}")
                        else:
                            self.logger.info(f"ℹ️ Skipping order {order_id} with status: {status}")
                    
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"❌ Error processing order {order.get('order_id', 'unknown')}: {e}")
                        continue
            
            self.logger.info(f"🚫 Order cancellation complete: {cancelled_count} cancelled, {failed_count} failed")
            
            return {
                'success': True,
                'cancelled_count': cancelled_count,
                'failed_count': failed_count,
                'cancelled_orders': cancelled_orders,
                'failed_orders': failed_orders
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error cancelling orders: {e}")
            return {
                'success': False,
                'error': str(e),
                'cancelled_count': 0,
                'failed_count': 0
            }
    
    def close_all_positions(self, positions_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Close all positions via market orders.
        
        Args:
            positions_data: Positions data containing current positions
            
        Returns:
            Dict with closing results
        """
        try:
            self.logger.info("🔄 Starting closure of all positions...")
            
            closed_count = 0
            failed_count = 0
            closed_positions = []
            failed_positions = []
            
            positions = positions_data.get('positions', {})
            
            for position_key, position_data in positions.items():
                try:
                    symbol = position_data.get('symbol', '')
                    quantity = position_data.get('quantity', 0)
                    position_type = position_data.get('position_type', '')
                    instrument_type = position_data.get('instrument_type', '')
                    account = position_data.get('account', '')
                    
                    # Skip if no quantity (already closed)
                    if quantity == 0:
                        self.logger.info(f"ℹ️ Skipping {symbol} - no quantity to close")
                        continue
                    
                    self.logger.info(f"🔄 Closing position: {symbol} - {position_type} {abs(quantity)} shares")
                    
                    # Determine the instruction based on current position
                    if quantity > 0:
                        # Long position - need to SELL
                        instruction = 'SELL'
                        close_quantity = abs(quantity)
                    else:
                        # Short position - need to BUY_TO_COVER
                        instruction = 'BUY_TO_COVER'
                        close_quantity = abs(quantity)
                    
                    # Create market order to close position
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
                    
                    self.logger.info(f"📤 Placing market order: {instruction} {close_quantity} {symbol}")
                    
                    # Place the order
                    order_result = self.order_handler.place_order(account, order_data)
                    
                    if order_result.get('success', False):
                        closed_count += 1
                        closed_positions.append({
                            'symbol': symbol,
                            'instruction': instruction,
                            'quantity': close_quantity,
                            'order_id': order_result.get('order_id', 'unknown'),
                            'position_type': position_type
                        })
                        self.logger.info(f"✅ Successfully placed closing order for {symbol}")
                    else:
                        failed_count += 1
                        failed_positions.append({
                            'symbol': symbol,
                            'quantity': quantity,
                            'error': order_result.get('error', 'Unknown error')
                        })
                        self.logger.error(f"❌ Failed to close position {symbol}: {order_result.get('error', 'Unknown')}")
                
                except Exception as e:
                    failed_count += 1
                    symbol = position_data.get('symbol', 'unknown')
                    failed_positions.append({
                        'symbol': symbol,
                        'error': str(e)
                    })
                    self.logger.error(f"❌ Error processing position {symbol}: {e}")
                    continue
            
            self.logger.info(f"🔄 Position closing complete: {closed_count} closed, {failed_count} failed")
            
            return {
                'success': True,
                'closed_count': closed_count,
                'failed_count': failed_count,
                'closed_positions': closed_positions,
                'failed_positions': failed_positions
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error closing positions: {e}")
            return {
                'success': False,
                'error': str(e),
                'closed_count': 0,
                'failed_count': 0
            }
    
    def execute_close_all(self) -> Dict[str, Any]:
        """
        Execute the complete close all positions process.
        
        Returns:
            Dict with complete execution results
        """
        try:
            self.logger.info("🚀 Starting Close All Positions execution...")
            
            # Step 1: Load current positions data
            self.logger.info("📋 Step 1: Loading current positions data...")
            positions_data = self.load_current_positions_data()
            
            if not positions_data:
                return {
                    'success': False,
                    'error': 'Failed to load positions data',
                    'step_failed': 'load_data'
                }
            
            total_positions = positions_data.get('total_positions', 0)
            total_orders = positions_data.get('summary', {}).get('total_open_orders', 0)
            
            self.logger.info(f"📊 Found {total_positions} positions and {total_orders} open orders")
            
            if total_positions == 0 and total_orders == 0:
                self.logger.info("ℹ️ No positions or orders to close")
                return {
                    'success': True,
                    'message': 'No positions or orders to close',
                    'positions_closed': 0,
                    'orders_cancelled': 0
                }
            
            # Step 2: Cancel all open orders
            self.logger.info("🚫 Step 2: Cancelling all open orders...")
            cancel_result = self.cancel_all_open_orders(positions_data)
            
            if not cancel_result.get('success', False):
                self.logger.warning(f"⚠️ Order cancellation had issues: {cancel_result.get('error', 'Unknown')}")
            
            # Step 3: Close all positions
            self.logger.info("🔄 Step 3: Closing all positions...")
            close_result = self.close_all_positions(positions_data)
            
            if not close_result.get('success', False):
                self.logger.error(f"❌ Position closing failed: {close_result.get('error', 'Unknown')}")
                return {
                    'success': False,
                    'error': close_result.get('error', 'Position closing failed'),
                    'step_failed': 'close_positions',
                    'cancel_result': cancel_result
                }
            
            # Compile final results
            final_result = {
                'success': True,
                'execution_time': datetime.now().isoformat(),
                'orders_cancelled': cancel_result.get('cancelled_count', 0),
                'orders_cancel_failed': cancel_result.get('failed_count', 0),
                'positions_closed': close_result.get('closed_count', 0),
                'positions_close_failed': close_result.get('failed_count', 0),
                'cancelled_orders': cancel_result.get('cancelled_orders', []),
                'closed_positions': close_result.get('closed_positions', []),
                'failed_orders': cancel_result.get('failed_orders', []),
                'failed_positions': close_result.get('failed_positions', [])
            }
            
            self.logger.info("🎉 Close All Positions execution completed!")
            self.logger.info(f"📊 Summary: {final_result['orders_cancelled']} orders cancelled, {final_result['positions_closed']} positions closed")
            
            return final_result
            
        except Exception as e:
            self.logger.error(f"❌ Critical error in close all execution: {e}")
            return {
                'success': False,
                'error': str(e),
                'step_failed': 'execution'
            }
    
    def save_execution_log(self, result: Dict[str, Any]) -> None:
        """
        Save execution results to a log file.
        
        Args:
            result: Execution results to save
        """
        try:
            log_filename = f"logs/close_all_positions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Ensure logs directory exists
            os.makedirs('logs', exist_ok=True)
            
            with open(log_filename, 'w') as f:
                json.dump(result, f, indent=2)
            
            self.logger.info(f"💾 Execution log saved to: {log_filename}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save execution log: {e}")


def main():
    """Main execution function."""
    print("🚨 CLOSE ALL POSITIONS SCRIPT")
    print("=" * 50)
    print("This script will:")
    print("1. Cancel ALL open orders")
    print("2. Close ALL positions via market orders")
    print("=" * 50)
    
    # Confirmation prompt
    confirmation = input("\n⚠️ Are you sure you want to proceed? This action cannot be undone! (yes/no): ")
    
    if confirmation.lower() != 'yes':
        print("❌ Operation cancelled by user")
        return
    
    # Final confirmation
    final_confirmation = input("\n🚨 FINAL CONFIRMATION: Type 'CLOSE ALL' to proceed: ")
    
    if final_confirmation != 'CLOSE ALL':
        print("❌ Operation cancelled - incorrect confirmation")
        return
    
    print("\n🚀 Starting Close All Positions execution...")
    
    try:
        # Create handler and execute
        handler = CloseAllPositionsHandler()
        result = handler.execute_close_all()
        
        # Save execution log
        handler.save_execution_log(result)
        
        # Display results
        print("\n" + "=" * 50)
        print("📊 EXECUTION RESULTS")
        print("=" * 50)
        
        if result.get('success', False):
            print("✅ Execution completed successfully!")
            print(f"🚫 Orders cancelled: {result.get('orders_cancelled', 0)}")
            print(f"🔄 Positions closed: {result.get('positions_closed', 0)}")
            
            if result.get('orders_cancel_failed', 0) > 0:
                print(f"⚠️ Orders failed to cancel: {result.get('orders_cancel_failed', 0)}")
            
            if result.get('positions_close_failed', 0) > 0:
                print(f"⚠️ Positions failed to close: {result.get('positions_close_failed', 0)}")
            
            # Show details
            if result.get('closed_positions'):
                print("\n📋 Closed Positions:")
                for pos in result['closed_positions']:
                    print(f"  • {pos['symbol']}: {pos['instruction']} {pos['quantity']} shares (Order: {pos['order_id']})")
            
            if result.get('cancelled_orders'):
                print("\n📋 Cancelled Orders:")
                for order in result['cancelled_orders']:
                    print(f"  • {order['symbol']}: {order['instruction']} {order['quantity']} @ ${order['price']} (ID: {order['order_id']})")
        
        else:
            print("❌ Execution failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
            print(f"Failed at step: {result.get('step_failed', 'unknown')}")
        
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
