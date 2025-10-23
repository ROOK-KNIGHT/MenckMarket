#!/usr/bin/env python3
"""
PostgreSQL Data Stream Handler
Handles real-time streaming of data from PostgreSQL database
"""

import asyncio
import json
from datetime import datetime
import logging
import threading
import time
from typing import Dict, List, Any

# Import the database query handler
from db_query_handler import DatabaseQueryHandler

logger = logging.getLogger(__name__)

class DataStreamHandler:
    """Handler for PostgreSQL database streaming"""
    
    def __init__(self, broadcast_callback):
        """Initialize the data stream handler"""
        self.broadcast_callback = broadcast_callback
        self.running = False
        self.latest_data = {}
        
        # Initialize database query handler
        self.db_query_handler = DatabaseQueryHandler()
        
        # Data polling interval (seconds)
        self.polling_interval = 3
        
        # Start data polling thread
        self.data_thread = None
    
    def start_data_polling(self):
        """Start background thread to poll database for updates"""
        def poll_data():
            logger.info("🔄 Starting PostgreSQL database polling thread")
            while self.running:
                try:
                    # Get latest data from database
                    new_data = self.get_latest_data()
                    
                    # Check if data content has actually changed
                    if new_data != self.latest_data:
                        self.latest_data = new_data
                        
                        # Broadcast to all connected clients via callback
                        if self.broadcast_callback:
                            asyncio.run(self.broadcast_callback(new_data))
                    
                    time.sleep(self.polling_interval)  # Poll every 3 seconds
                    
                except Exception as e:
                    logger.error(f"❌ Error in database polling: {e}")
                    time.sleep(5)  # Wait longer on error
            
            logger.info("🛑 Database polling thread stopped")
        
        self.running = True
        self.data_thread = threading.Thread(target=poll_data, daemon=True)
        self.data_thread.start()
    
    def get_latest_data(self) -> Dict[str, Any]:
        """Get latest data from PostgreSQL database"""
        try:
            logger.info("🔄 Fetching latest data from PostgreSQL database...")
            
            # Get comprehensive dashboard data from database
            dashboard_data = self.db_query_handler.get_comprehensive_dashboard_data()
            
            if dashboard_data.get('error'):
                logger.error(f"❌ Database error: {dashboard_data.get('metadata', {}).get('error', 'Unknown error')}")
                return {
                    'timestamp': datetime.now().isoformat(),
                    'error': 'Database connection failed',
                    'data_types': [],
                    'data_source': 'postgresql'
                }
            
            # Transform database data to WebSocket format
            data = {
                'timestamp': datetime.now().isoformat(),
                'data_source': 'postgresql',
                'data_types': []
            }
            
            # Trading statistics (for analytics)
            if dashboard_data.get('trading_statistics'):
                data['trading_statistics'] = dashboard_data['trading_statistics']
                data['data_types'].append('trading_statistics')
                logger.info(f"📊 Trading stats: {dashboard_data['trading_statistics'].get('total_trades', 0)} trades")
            
            # Current positions
            if dashboard_data.get('positions'):
                positions_data = dashboard_data['positions']
                if positions_data.get('positions'):
                    data['positions'] = list(positions_data['positions'].values())
                    data['data_types'].append('positions')
                    logger.info(f"📈 Positions: {len(data['positions'])} symbols")
            
            # Strategy signals
            if dashboard_data.get('iron_condor_signals'):
                data['iron_condor_signals'] = dashboard_data['iron_condor_signals']
                data['data_types'].append('iron_condor_signals')
                logger.info(f"🎯 Iron Condor signals: {len(data['iron_condor_signals'])}")
            
            if dashboard_data.get('pml_signals'):
                data['pml_signals'] = dashboard_data['pml_signals']
                data['data_types'].append('pml_signals')
                logger.info(f"📊 PML signals: {len(data['pml_signals'])}")
            
            if dashboard_data.get('divergence_signals'):
                data['divergence_signals'] = dashboard_data['divergence_signals']
                data['data_types'].append('divergence_signals')
                logger.info(f"📈 Divergence signals: {len(data['divergence_signals'])}")
            
            # Technical indicators
            if dashboard_data.get('technical_indicators'):
                data['technical_indicators'] = dashboard_data['technical_indicators']
                data['data_types'].append('technical_indicators')
                logger.info(f"📊 Technical indicators: {len(data['technical_indicators'])} symbols")
            
            # Account data
            if dashboard_data.get('account_data'):
                data['account_data'] = dashboard_data['account_data']
                data['data_types'].append('account_data')
                logger.info(f"💰 Account data: {len(dashboard_data['account_data'].get('accounts', {}))} accounts")
            
            # Watchlist data
            if dashboard_data.get('watchlist_data'):
                data['watchlist_data'] = dashboard_data['watchlist_data']
                data['data_types'].append('watchlist_data')
                logger.info(f"📋 Watchlist: {len(data['watchlist_data'])} symbols")
            
            # Recent transactions
            if dashboard_data.get('recent_transactions'):
                data['recent_transactions'] = dashboard_data['recent_transactions']
                data['data_types'].append('recent_transactions')
                logger.info(f"💳 Recent transactions: {len(data['recent_transactions'])}")
            
            # Market status
            if dashboard_data.get('market_status'):
                data['market_status'] = dashboard_data['market_status']
                data['data_types'].append('market_status')
            
            # Add summary statistics
            data['summary'] = {
                'total_pml_signals': len(data.get('pml_signals', [])),
                'pml_strong_buy_count': len([s for s in data.get('pml_signals', []) if s.get('signal_type') == 'STRONG_BUY']),
                'total_iron_condor_signals': len(data.get('iron_condor_signals', [])),
                'iron_condor_strong_buy_count': len([s for s in data.get('iron_condor_signals', []) if s.get('signal_type') == 'STRONG_BUY']),
                'total_divergence_signals': len(data.get('divergence_signals', [])),
                'divergence_strong_buy_count': len([s for s in data.get('divergence_signals', []) if s.get('signal_type') == 'STRONG_BUY']),
                'total_watchlist_symbols': len(data.get('watchlist_data', [])),
                'total_positions': len(data.get('positions', [])),
                'total_recent_transactions': len(data.get('recent_transactions', [])),
                'database_connected': True
            }
            
            logger.info(f"✅ Successfully fetched data from PostgreSQL: {len(data['data_types'])} data types")
            return data
                
        except Exception as e:
            logger.error(f"❌ Error getting latest data from PostgreSQL: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'data_types': [],
                'data_source': 'postgresql',
                'database_connected': False
            }
    
    def get_initial_data(self):
        """Get initial data for new clients"""
        if self.latest_data:
            return self.latest_data
        else:
            return self.get_latest_data()
    
    def stop(self):
        """Stop the data polling"""
        logger.info("🛑 Stopping data stream handler...")
        self.running = False
        if self.data_thread and self.data_thread.is_alive():
            self.data_thread.join(timeout=5)
