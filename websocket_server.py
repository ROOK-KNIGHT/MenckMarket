#!/usr/bin/env python3
"""
WebSocket Server for Streaming JSON File Data
Streams real-time trading data from JSON files to web clients
"""

import asyncio
import websockets
import json
from datetime import datetime
import logging
import threading
import time
import os
from typing import Dict, List, Any

# Import the new database query handler
from db_query_handler import DatabaseQueryHandler

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseWebSocketStreamer:
    """WebSocket server that streams data from PostgreSQL database in real-time"""
    
    def __init__(self, port=8765):
        """Initialize the WebSocket streamer"""
        self.port = port
        self.clients = set()
        self.running = False
        self.latest_data = {}
        
        # Initialize database query handler
        self.db_query_handler = DatabaseQueryHandler()
        
        # Data polling interval (seconds)
        self.polling_interval = 3
        
        # Start data polling thread
        self.data_thread = None
        self.start_data_polling()
    
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
                        
                        # Send to all connected clients
                        if self.clients:
                            asyncio.run(self.broadcast_data(new_data))
                    
                    time.sleep(self.polling_interval)  # Poll every 3 seconds
                    
                except Exception as e:
                    logger.error(f"❌ Error in database polling: {e}")
                    time.sleep(5)  # Wait longer on error
            
            logger.info("🛑 Database polling thread stopped")
        
        self.running = True
        self.data_thread = threading.Thread(target=poll_data, daemon=True)
        self.data_thread.start()
    
    async def broadcast_data(self, data):
        """Broadcast data to all connected clients"""
        if not self.clients:
            return
        
        message = json.dumps(data, default=str)
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error sending to client: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        if disconnected_clients:
            logger.info(f"🔌 Removed {len(disconnected_clients)} disconnected clients")
    
    async def handle_client(self, websocket):
        """Handle new WebSocket client connection"""
        client_addr = websocket.remote_address
        logger.info(f"🔗 New client connected: {client_addr}")
        
        # Add client to set
        self.clients.add(websocket)
        
        try:
            # Send initial data immediately
            if self.latest_data:
                await websocket.send(json.dumps(self.latest_data, default=str))
            else:
                # Get fresh data if none cached
                initial_data = self.get_latest_data()
                await websocket.send(json.dumps(initial_data, default=str))
            
            # Keep connection alive and handle messages
            async for message in websocket:
                try:
                    # Parse client message
                    client_msg = json.loads(message)
                    
                    if client_msg.get('type') == 'ping':
                        # Respond to ping
                        await websocket.send(json.dumps({
                            'type': 'pong',
                            'timestamp': datetime.now().isoformat()
                        }))
                    
                    elif client_msg.get('type') == 'request_data':
                        # Send latest data
                        fresh_data = self.get_latest_data()
                        await websocket.send(json.dumps(fresh_data, default=str))
                    
                    elif client_msg.get('type') == 'subscribe':
                        # Handle subscription to specific data types
                        data_types = client_msg.get('data_types', [])
                        logger.info(f"📡 Client {client_addr} subscribed to: {data_types}")
                        
                        # Send acknowledgment
                        await websocket.send(json.dumps({
                            'type': 'subscription_ack',
                            'subscribed_to': data_types,
                            'timestamp': datetime.now().isoformat()
                        }))
                    
                    elif client_msg.get('type') == 'save_risk_config':
                        # Handle risk configuration save
                        await self.handle_risk_config_save(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'get_risk_config':
                        # Handle risk configuration request
                        await self.handle_risk_config_get(websocket)
                    
                    elif client_msg.get('type') == 'save_strategy_config':
                        # Handle strategy configuration save
                        logger.info(f"📨 Received save_strategy_config message from {client_addr}")
                        await self.handle_strategy_config_save(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'get_strategy_config':
                        # Handle strategy configuration request
                        logger.info(f"📨 Received get_strategy_config message from {client_addr}")
                        await self.handle_strategy_config_get(websocket)
                    
                    elif client_msg.get('type') == 'add_watchlist_symbol':
                        # Handle adding symbol to watchlist
                        logger.info(f"📨 Received add_watchlist_symbol message from {client_addr}")
                        await self.handle_add_watchlist_symbol(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'remove_watchlist_symbol':
                        # Handle removing symbol from watchlist
                        logger.info(f"📨 Received remove_watchlist_symbol message from {client_addr}")
                        await self.handle_remove_watchlist_symbol(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'get_watchlist':
                        # Handle getting current watchlist
                        logger.info(f"📨 Received get_watchlist message from {client_addr}")
                        await self.handle_get_watchlist(websocket)
                    
                    elif client_msg.get('type') == 'refresh_watchlist_with_cleanup':
                        # Handle refreshing watchlist with database cleanup
                        logger.info(f"📨 Received refresh_watchlist_with_cleanup message from {client_addr}")
                        await self.handle_refresh_watchlist_with_cleanup(websocket)
                    
                    elif client_msg.get('type') == 'check_auth_status':
                        # Handle authentication status check
                        logger.info(f"📨 Received check_auth_status message from {client_addr}")
                        await self.handle_check_auth_status(websocket)
                    
                    elif client_msg.get('type') == 'exchange_tokens':
                        # Handle token exchange
                        logger.info(f"📨 Received exchange_tokens message from {client_addr}")
                        await self.handle_exchange_tokens(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'start_trading':
                        # Handle starting trading engine
                        logger.info(f"📨 Received start_trading message from {client_addr}")
                        await self.handle_start_trading(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'stop_trading':
                        # Handle stopping trading engine
                        logger.info(f"📨 Received stop_trading message from {client_addr}")
                        await self.handle_stop_trading(websocket, client_msg)
                    
                    elif client_msg.get('type') == 'get_trading_status':
                        # Handle trading status request
                        logger.info(f"📨 Received get_trading_status message from {client_addr}")
                        await self.handle_get_trading_status(websocket)
                    
                    elif client_msg.get('type') == 'emergency_stop':
                        # Handle emergency stop
                        logger.info(f"📨 Received emergency_stop message from {client_addr}")
                        await self.handle_emergency_stop(websocket, client_msg)
                
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ Invalid JSON from client {client_addr}: {message}")
                except Exception as e:
                    logger.error(f"❌ Error handling message from {client_addr}: {e}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"🔌 Client {client_addr} disconnected")
        except Exception as e:
            logger.error(f"❌ Error with client {client_addr}: {e}")
        finally:
            # Remove client from set
            self.clients.discard(websocket)
            logger.info(f"👋 Client {client_addr} removed. Active clients: {len(self.clients)}")
    
    async def start_server(self):
        """Start the WebSocket server"""
        logger.info(f"🚀 Starting WebSocket server on port {self.port}")
        
        try:
            server = await websockets.serve(
                self.handle_client,
                "localhost",
                self.port,
                ping_interval=30,
                ping_timeout=10
            )
            
            logger.info(f"✅ WebSocket server running on ws://localhost:{self.port}")
            logger.info("📡 Streaming PostgreSQL database data in real-time")
            logger.info(f"🔄 Database polling every {self.polling_interval} seconds")
            
            # Keep server running
            await server.wait_closed()
            
        except Exception as e:
            logger.error(f"❌ Error starting WebSocket server: {e}")
            raise
        finally:
            self.running = False
            logger.info("🔌 WebSocket server stopped")
    
    async def handle_risk_config_save(self, websocket, client_msg):
        """Handle saving risk configuration to JSON file"""
        try:
            config_data = client_msg.get('config', {})
            config_file = 'risk_config_live.json'
            
            # Load existing config or create new one
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"risk_management": {}}
            
            # Update risk management configuration
            risk_config = existing_config.get('risk_management', {})
            
            # Update account limits
            if 'account_limits' in config_data:
                if 'account_limits' not in risk_config:
                    risk_config['account_limits'] = {}
                risk_config['account_limits'].update(config_data['account_limits'])
            
            # Update position sizing
            if 'position_sizing' in config_data:
                if 'position_sizing' not in risk_config:
                    risk_config['position_sizing'] = {}
                risk_config['position_sizing'].update(config_data['position_sizing'])
            
            # Update stop loss settings
            if 'stop_loss_settings' in config_data:
                if 'stop_loss_settings' not in risk_config:
                    risk_config['stop_loss_settings'] = {}
                risk_config['stop_loss_settings'].update(config_data['stop_loss_settings'])
            
            # Update parameter states
            if 'parameter_states' in config_data:
                if 'parameter_states' not in risk_config:
                    risk_config['parameter_states'] = {}
                risk_config['parameter_states'].update(config_data['parameter_states'])
            
            # Update metadata
            risk_config['metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'updated_by': config_data.get('updated_by', 'web_interface'),
                'version': '1.0.0'
            }
            
            # Save updated configuration
            existing_config['risk_management'] = risk_config
            
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"💾 Risk configuration saved by {config_data.get('updated_by', 'web_interface')}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'risk_config_saved',
                'success': True,
                'message': 'Risk configuration saved successfully',
                'config': existing_config,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_risk_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving risk config: {e}")
            await websocket.send(json.dumps({
                'type': 'risk_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_risk_config_get(self, websocket):
        """Handle getting current risk configuration"""
        try:
            config_file = 'risk_config_live.json'
            
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
            else:
                # Return default config if file doesn't exist
                config = {
                    "risk_management": {
                        "account_limits": {
                            "max_account_risk": 25.0,
                            "daily_loss_limit": 5.0,
                            "equity_buffer": 10000.0
                        },
                        "position_sizing": {
                            "max_position_size": 5.0,
                            "max_positions": 15
                        },
                        "stop_loss_settings": {
                            "method": "atr",
                            "value": 2.0,
                            "take_profit_ratio": 2.0
                        },
                        "parameter_states": {
                            "enable_max_account_risk": True,
                            "enable_daily_loss_limit": True,
                            "enable_equity_buffer": True,
                            "enable_max_position_size": True,
                            "enable_max_positions": True,
                            "enable_stop_loss": True,
                            "enable_stop_loss_value": True,
                            "enable_risk_reward_ratio": True
                        },
                        "metadata": {
                            "last_updated": datetime.now().isoformat(),
                            "updated_by": "system_default",
                            "version": "1.0.0"
                        }
                    }
                }
            
            # Send config response
            await websocket.send(json.dumps({
                'type': 'risk_config_data',
                'success': True,
                'config': config,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting risk config: {e}")
            await websocket.send(json.dumps({
                'type': 'risk_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def broadcast_risk_config_update(self, config):
        """Broadcast risk configuration update to all clients"""
        if not self.clients:
            return
        
        message = json.dumps({
            'type': 'risk_config_updated',
            'config': config,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting risk config update: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        logger.info(f"📡 Risk config update broadcasted to {len(self.clients)} clients")

    async def handle_strategy_config_save(self, websocket, client_msg):
        """Handle saving strategy configuration to centralized auto_approve_config.json"""
        logger.info("🔧 Strategy config save request received")
        logger.info(f"📋 Client message: {client_msg}")
        
        try:
            config_data = client_msg.get('config', {})
            logger.info(f"📊 Config data received: {config_data}")
            logger.info(f"🔍 Config data keys: {list(config_data.keys())}")
            logger.info(f"🔍 Config data type: {type(config_data)}")
            
            # Load existing auto_approve_config.json or create new one
            config_file = 'auto_approve_config.json'
            
            if os.path.exists(config_file):
                logger.info(f"📂 Loading existing auto_approve config: {config_file}")
                with open(config_file, 'r') as f:
                    auto_approve_config = json.load(f)
            else:
                logger.info(f"🆕 Creating new auto_approve config: {config_file}")
                auto_approve_config = {
                    "strategy_controls": {},
                    "metadata": {}
                }
            
            # Ensure strategy_controls exists
            if 'strategy_controls' not in auto_approve_config:
                auto_approve_config['strategy_controls'] = {}
            
            updated_strategies = []
            valid_strategies = ['iron_condor', 'pml', 'divergence']
            
            # Only process strategies that are valid
            strategies_to_update = [strategy for strategy in config_data.keys() if strategy in valid_strategies]
            logger.info(f"🎯 Strategies to update: {strategies_to_update}")
            
            for strategy_name in strategies_to_update:
                strategy_config = config_data[strategy_name]
                logger.info(f"🔍 Processing strategy: {strategy_name}")
                logger.info(f"📝 Strategy config for {strategy_name}: {strategy_config}")
                
                # Determine auto_approve status based on toggle states
                auto_approve = False
                if strategy_config.get('auto_execute', False):
                    auto_approve = True
                    logger.info(f"✅ {strategy_name}: auto_execute=True, setting auto_approve=True")
                elif strategy_config.get('manual_approval', False):
                    auto_approve = False  # Manual approval means no auto approval
                    logger.info(f"👤 {strategy_name}: manual_approval=True, setting auto_approve=False")
                else:
                    logger.info(f"⚪ {strategy_name}: no toggles enabled, setting auto_approve=False")
                
                # Update strategy controls in centralized config
                auto_approve_config['strategy_controls'][strategy_name] = {
                    'auto_execute': strategy_config.get('auto_execute', False),
                    'manual_approval': strategy_config.get('manual_approval', False),
                    'auto_approve': auto_approve
                }
                
                updated_strategies.append(strategy_name)
                logger.info(f"✅ Updated {strategy_name} in centralized config with auto_approve: {auto_approve}")
            
            # Update metadata
            auto_approve_config['metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'updated_by': 'web_interface',
                'version': '1.0.0',
                'description': 'Centralized auto-approve configuration for all trading strategies'
            }
            
            # Save updated centralized config
            logger.info(f"💾 Saving centralized auto_approve config to {config_file}")
            with open(config_file, 'w') as f:
                json.dump(auto_approve_config, f, indent=2)
            
            logger.info(f"✅ Successfully updated centralized auto_approve config for strategies: {updated_strategies}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'strategy_config_saved',
                'success': True,
                'message': f'Strategy configuration saved for {", ".join(updated_strategies)}',
                'updated_strategies': updated_strategies,
                'config': config_data,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast strategy config update to all clients
            await self.broadcast_strategy_config_update(config_data)
            
        except Exception as e:
            logger.error(f"❌ Error saving strategy config: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_strategy_config_get(self, websocket):
        """Handle getting current strategy configuration from centralized auto_approve_config.json"""
        try:
            config_file = 'auto_approve_config.json'
            
            if os.path.exists(config_file):
                logger.info(f"📂 Loading strategy config from centralized file: {config_file}")
                with open(config_file, 'r') as f:
                    auto_approve_config = json.load(f)
                
                # Extract strategy controls from centralized config
                strategy_controls = auto_approve_config.get('strategy_controls', {})
                
                config = {}
                for strategy_name in ['iron_condor', 'pml', 'divergence']:
                    if strategy_name in strategy_controls:
                        config[strategy_name] = {
                            'auto_execute': strategy_controls[strategy_name].get('auto_execute', False),
                            'manual_approval': strategy_controls[strategy_name].get('manual_approval', False)
                        }
                    else:
                        # Default configuration if strategy not in centralized config
                        config[strategy_name] = {
                            'auto_execute': False,
                            'manual_approval': False
                        }
                
                logger.info(f"✅ Loaded strategy config from centralized file: {config}")
            else:
                logger.warning(f"⚠️ Centralized config file not found: {config_file}, using defaults")
                # Default configuration if centralized file doesn't exist
                config = {
                    'iron_condor': {
                        'auto_execute': False,
                        'manual_approval': False
                    },
                    'pml': {
                        'auto_execute': True,
                        'manual_approval': False
                    },
                    'divergence': {
                        'auto_execute': True,
                        'manual_approval': False
                    }
                }
            
            # Send config response
            await websocket.send(json.dumps({
                'type': 'strategy_config_data',
                'success': True,
                'config': config,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting strategy config: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def broadcast_strategy_config_update(self, config):
        """Broadcast strategy configuration update to all clients"""
        if not self.clients:
            return
        
        message = json.dumps({
            'type': 'strategy_config_updated',
            'config': config,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting strategy config update: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        logger.info(f"📡 Strategy config update broadcasted to {len(self.clients)} clients")

    async def handle_add_watchlist_symbol(self, websocket, client_msg):
        """Handle adding a symbol to the watchlist"""
        try:
            logger.info(f"🔍 DEBUG: Received add_watchlist_symbol request: {client_msg}")
            
            symbol = client_msg.get('symbol', '').strip().upper()
            logger.info(f"🔍 DEBUG: Extracted symbol: '{symbol}'")
            
            if not symbol:
                logger.warning(f"🔍 DEBUG: Symbol is empty or invalid")
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': 'Symbol is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"📋 Adding symbol {symbol} to watchlist")
            
            # Load current watchlist
            watchlist_file = 'api_watchlist.json'
            logger.info(f"🔍 DEBUG: Watchlist file path: {watchlist_file}")
            logger.info(f"🔍 DEBUG: File exists: {os.path.exists(watchlist_file)}")
            
            if os.path.exists(watchlist_file):
                logger.info(f"🔍 DEBUG: Loading existing watchlist file")
                with open(watchlist_file, 'r') as f:
                    watchlist_data = json.load(f)
                logger.info(f"🔍 DEBUG: Loaded watchlist data: {watchlist_data}")
            else:
                logger.info(f"🔍 DEBUG: Creating new watchlist data structure")
                watchlist_data = {
                    "watchlist": {
                        "symbols": [],
                        "metadata": {
                            "created": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                            "total_symbols": 0,
                            "managed_by": "websocket_server"
                        }
                    }
                }
                logger.info(f"🔍 DEBUG: Created new watchlist data: {watchlist_data}")
            
            # Check if symbol already exists
            current_symbols = watchlist_data.get('watchlist', {}).get('symbols', [])
            logger.info(f"🔍 DEBUG: Current symbols in watchlist: {current_symbols}")
            
            if symbol in current_symbols:
                logger.warning(f"🔍 DEBUG: Symbol {symbol} already exists in watchlist")
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': f'{symbol} is already in the watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Add symbol to watchlist
            logger.info(f"🔍 DEBUG: Adding {symbol} to symbols list")
            current_symbols.append(symbol)
            logger.info(f"🔍 DEBUG: Updated symbols list: {current_symbols}")
            
            # Update metadata
            watchlist_data['watchlist']['symbols'] = current_symbols
            watchlist_data['watchlist']['metadata']['last_updated'] = datetime.now().isoformat()
            watchlist_data['watchlist']['metadata']['total_symbols'] = len(current_symbols)
            watchlist_data['watchlist']['metadata']['managed_by'] = 'websocket_server'
            
            logger.info(f"🔍 DEBUG: Updated watchlist data structure: {watchlist_data}")
            
            # Save updated watchlist
            logger.info(f"🔍 DEBUG: Attempting to save watchlist to {watchlist_file}")
            try:
                with open(watchlist_file, 'w') as f:
                    json.dump(watchlist_data, f, indent=2)
                logger.info(f"🔍 DEBUG: Successfully wrote watchlist file")
                
                # Verify the file was written correctly
                with open(watchlist_file, 'r') as f:
                    verification_data = json.load(f)
                logger.info(f"🔍 DEBUG: Verification read of saved file: {verification_data}")
                
            except Exception as file_error:
                logger.error(f"🔍 DEBUG: Error writing watchlist file: {file_error}")
                raise file_error
            
            logger.info(f"✅ Successfully added {symbol} to watchlist. Total symbols: {len(current_symbols)}")
            
            # Send success response
            response_data = {
                'type': 'watchlist_symbol_added',
                'success': True,
                'symbol': symbol,
                'message': f'Successfully added {symbol} to watchlist',
                'total_symbols': len(current_symbols),
                'watchlist': watchlist_data['watchlist'],
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"🔍 DEBUG: Sending success response: {response_data}")
            
            await websocket.send(json.dumps(response_data))
            
            # Broadcast watchlist update to all clients
            logger.info(f"🔍 DEBUG: Broadcasting watchlist update to all clients")
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error adding symbol to watchlist: {e}")
            logger.error(f"🔍 DEBUG: Exception details: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_remove_watchlist_symbol(self, websocket, client_msg):
        """Handle removing a symbol from the watchlist"""
        try:
            logger.info(f"🔍 DEBUG: Received remove_watchlist_symbol request: {client_msg}")
            
            symbol = client_msg.get('symbol', '').strip().upper()
            logger.info(f"🔍 DEBUG: Extracted symbol: '{symbol}'")
            
            if not symbol:
                logger.warning(f"🔍 DEBUG: Symbol is empty or invalid")
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': 'Symbol is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"📋 Removing symbol {symbol} from watchlist")
            
            # Load current watchlist
            watchlist_file = 'api_watchlist.json'
            logger.info(f"🔍 DEBUG: Watchlist file path: {watchlist_file}")
            logger.info(f"🔍 DEBUG: File exists: {os.path.exists(watchlist_file)}")
            
            if not os.path.exists(watchlist_file):
                logger.error(f"🔍 DEBUG: Watchlist file does not exist")
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': 'Watchlist file not found',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"🔍 DEBUG: Loading existing watchlist file")
            with open(watchlist_file, 'r') as f:
                watchlist_data = json.load(f)
            logger.info(f"🔍 DEBUG: Loaded watchlist data: {watchlist_data}")
            
            # Check if symbol exists
            current_symbols = watchlist_data.get('watchlist', {}).get('symbols', [])
            logger.info(f"🔍 DEBUG: Current symbols in watchlist: {current_symbols}")
            
            if symbol not in current_symbols:
                logger.warning(f"🔍 DEBUG: Symbol {symbol} not found in watchlist")
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': f'{symbol} is not in the watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Remove symbol from watchlist
            logger.info(f"🔍 DEBUG: Removing {symbol} from symbols list")
            current_symbols.remove(symbol)
            logger.info(f"🔍 DEBUG: Updated symbols list: {current_symbols}")
            
            # Update metadata
            watchlist_data['watchlist']['symbols'] = current_symbols
            watchlist_data['watchlist']['metadata']['last_updated'] = datetime.now().isoformat()
            watchlist_data['watchlist']['metadata']['total_symbols'] = len(current_symbols)
            watchlist_data['watchlist']['metadata']['managed_by'] = 'websocket_server'
            
            logger.info(f"🔍 DEBUG: Updated watchlist data structure: {watchlist_data}")
            
            # Save updated watchlist
            logger.info(f"🔍 DEBUG: Attempting to save watchlist to {watchlist_file}")
            try:
                with open(watchlist_file, 'w') as f:
                    json.dump(watchlist_data, f, indent=2)
                logger.info(f"🔍 DEBUG: Successfully wrote watchlist file")
                
                # Verify the file was written correctly
                with open(watchlist_file, 'r') as f:
                    verification_data = json.load(f)
                logger.info(f"🔍 DEBUG: Verification read of saved file: {verification_data}")
                
            except Exception as file_error:
                logger.error(f"🔍 DEBUG: Error writing watchlist file: {file_error}")
                raise file_error
            
            logger.info(f"✅ Successfully removed {symbol} from watchlist. Total symbols: {len(current_symbols)}")
            
            # Truncate integrated watchlist database table and restart real-time monitor
            logger.info(f"🗄️ Truncating integrated watchlist database table for fresh data set...")
            await self.truncate_watchlist_database_and_restart_monitor()
            
            # Send success response
            response_data = {
                'type': 'watchlist_symbol_removed',
                'success': True,
                'symbol': symbol,
                'message': f'Successfully removed {symbol} from watchlist',
                'total_symbols': len(current_symbols),
                'watchlist': watchlist_data['watchlist'],
                'database_refreshed': True,
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"🔍 DEBUG: Sending success response: {response_data}")
            
            await websocket.send(json.dumps(response_data))
            
            # Broadcast watchlist update to all clients
            logger.info(f"🔍 DEBUG: Broadcasting watchlist update to all clients")
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error removing symbol from watchlist: {e}")
            logger.error(f"🔍 DEBUG: Exception details: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_get_watchlist(self, websocket):
        """Handle getting current watchlist"""
        try:
            watchlist_file = 'api_watchlist.json'
            
            if os.path.exists(watchlist_file):
                with open(watchlist_file, 'r') as f:
                    watchlist_data = json.load(f)
            else:
                watchlist_data = {
                    "watchlist": {
                        "symbols": [],
                        "metadata": {
                            "created": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                            "total_symbols": 0,
                            "managed_by": "websocket_server"
                        }
                    }
                }
            
            # Send watchlist response
            await websocket.send(json.dumps({
                'type': 'watchlist_data',
                'success': True,
                'watchlist': watchlist_data['watchlist'],
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_refresh_watchlist_with_cleanup(self, websocket):
        """Handle refreshing watchlist with database cleanup and monitor restart"""
        try:
            logger.info("🔄 Processing refresh watchlist with cleanup request...")
            
            # Step 1: Truncate integrated watchlist database table and restart real-time monitor
            logger.info("🗄️ Performing database cleanup and monitor restart...")
            await self.truncate_watchlist_database_and_restart_monitor()
            
            # Step 2: Get current watchlist data
            watchlist_file = 'api_watchlist.json'
            
            if os.path.exists(watchlist_file):
                with open(watchlist_file, 'r') as f:
                    watchlist_data = json.load(f)
            else:
                watchlist_data = {
                    "watchlist": {
                        "symbols": [],
                        "metadata": {
                            "created": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                            "total_symbols": 0,
                            "managed_by": "websocket_server"
                        }
                    }
                }
            
            logger.info(f"✅ Watchlist refresh with cleanup completed. Total symbols: {len(watchlist_data.get('watchlist', {}).get('symbols', []))}")
            
            # Send success response with refresh confirmation
            await websocket.send(json.dumps({
                'type': 'watchlist_refreshed_with_cleanup',
                'success': True,
                'message': 'Watchlist refreshed with database cleanup completed',
                'watchlist': watchlist_data['watchlist'],
                'database_refreshed': True,
                'monitor_restarted': True,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast watchlist update to all clients
            logger.info("📡 Broadcasting refreshed watchlist to all clients")
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error refreshing watchlist with cleanup: {e}")
            logger.error(f"🔍 DEBUG: Exception details: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': f'Failed to refresh watchlist with cleanup: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }))
    
    async def broadcast_watchlist_update(self, watchlist):
        """Broadcast watchlist update to all clients"""
        if not self.clients:
            return
        
        message = json.dumps({
            'type': 'watchlist_updated',
            'watchlist': watchlist,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting watchlist update: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        logger.info(f"📡 Watchlist update broadcasted to {len(self.clients)} clients")

    async def truncate_watchlist_database_and_restart_monitor(self):
        """Truncate integrated watchlist database table and restart real-time monitor for fresh data"""
        try:
            logger.info("🗄️ Starting database cleanup and monitor restart process...")
            
            # Step 1: Truncate integrated watchlist database table
            logger.info("🗄️ Truncating integrated_watchlist table...")
            try:
                # Use the database query handler to execute truncate
                truncate_result = self.db_query_handler.execute_query(
                    "TRUNCATE TABLE integrated_watchlist RESTART IDENTITY CASCADE;",
                    fetch_results=False
                )
                
                if truncate_result.get('success', False):
                    logger.info("✅ Successfully truncated integrated_watchlist table")
                else:
                    logger.warning(f"⚠️ Truncate may have failed: {truncate_result}")
                    
            except Exception as db_error:
                logger.error(f"❌ Error truncating database table: {db_error}")
                # Continue with restart even if truncate fails
            
            # Step 2: Restart real-time monitor process
            logger.info("🔄 Restarting real-time monitor process...")
            try:
                import subprocess
                import signal
                import psutil
                
                # Find and terminate existing realtime_monitor.py processes
                logger.info("🔍 Looking for existing realtime_monitor.py processes...")
                terminated_processes = []
                
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        cmdline = proc.info['cmdline']
                        if cmdline and any('realtime_monitor.py' in arg for arg in cmdline):
                            logger.info(f"🛑 Terminating existing realtime_monitor process: PID {proc.info['pid']}")
                            proc.terminate()
                            terminated_processes.append(proc.info['pid'])
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue
                
                # Wait for processes to terminate gracefully
                if terminated_processes:
                    logger.info(f"⏳ Waiting for {len(terminated_processes)} processes to terminate...")
                    import time
                    time.sleep(2)  # Give processes time to shut down gracefully
                
                # Start new realtime_monitor.py process
                logger.info("🚀 Starting new realtime_monitor.py process...")
                monitor_process = subprocess.Popen([
                    'python3', 'realtime_monitor.py'
                ], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                cwd=os.getcwd()
                )
                
                logger.info(f"✅ Started new realtime_monitor.py process with PID: {monitor_process.pid}")
                
                # Give the new process a moment to initialize
                import time
                time.sleep(1)
                
                # Check if the process is still running
                if monitor_process.poll() is None:
                    logger.info("✅ Real-time monitor process is running successfully")
                else:
                    logger.error("❌ Real-time monitor process failed to start")
                    
            except ImportError as import_error:
                logger.error(f"❌ Missing required module for process management: {import_error}")
                logger.info("💡 Install psutil: pip install psutil")
            except Exception as restart_error:
                logger.error(f"❌ Error restarting real-time monitor: {restart_error}")
            
            logger.info("🔄 Database cleanup and monitor restart process completed")
            
        except Exception as e:
            logger.error(f"❌ Error in truncate_watchlist_database_and_restart_monitor: {e}")
            import traceback
            logger.error(f"🔍 Full traceback: {traceback.format_exc()}")

    async def handle_check_auth_status(self, websocket):
        """Handle authentication status check using connection_manager"""
        try:
            logger.info("🔐 Checking authentication status via connection_manager...")
            
            # Import connection_manager to access tokens
            import connection_manager
            
            # Load tokens directly from AWS Secrets Manager
            tokens = connection_manager.load_tokens()
            
            if not tokens:
                await websocket.send(json.dumps({
                    'type': 'auth_status_response',
                    'authenticated': False,
                    'error': 'No tokens found in AWS Secrets Manager',
                    'expires_at': None,
                    'account_info': None,
                    'last_check': datetime.now().isoformat(),
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Check if tokens are valid and not expired
            expires_at = tokens.get('expires_at')
            if expires_at:
                try:
                    expires_datetime = datetime.fromisoformat(expires_at)
                    current_time = datetime.now()
                    
                    if current_time >= expires_datetime:
                        await websocket.send(json.dumps({
                            'type': 'auth_status_response',
                            'authenticated': False,
                            'error': 'Tokens expired',
                            'expires_at': expires_at,
                            'account_info': None,
                            'last_check': current_time.isoformat(),
                            'timestamp': datetime.now().isoformat()
                        }))
                        return
                    
                    # Tokens are valid - return full status
                    await websocket.send(json.dumps({
                        'type': 'auth_status_response',
                        'authenticated': True,
                        'expires_at': expires_at,
                        'account_info': {
                            'account_number': 'Connected',
                            'status': 'Active',
                            'token_type': tokens.get('token_type', 'Bearer')
                        },
                        'last_check': current_time.isoformat(),
                        'time_until_expiry': str(expires_datetime - current_time),
                        'refresh_token_available': bool(tokens.get('refresh_token')),
                        'timestamp': datetime.now().isoformat()
                    }))
                    
                    logger.info("✅ Authentication status: Valid tokens found")
                    return
                    
                except ValueError:
                    await websocket.send(json.dumps({
                        'type': 'auth_status_response',
                        'authenticated': False,
                        'error': 'Invalid token expiration format',
                        'expires_at': None,
                        'account_info': None,
                        'last_check': datetime.now().isoformat(),
                        'timestamp': datetime.now().isoformat()
                    }))
                    return
            else:
                await websocket.send(json.dumps({
                    'type': 'auth_status_response',
                    'authenticated': False,
                    'error': 'No expiration time in tokens',
                    'expires_at': None,
                    'account_info': None,
                    'last_check': datetime.now().isoformat(),
                    'timestamp': datetime.now().isoformat()
                }))
                return
                
        except Exception as e:
            logger.error(f"❌ Error checking auth status: {e}")
            await websocket.send(json.dumps({
                'type': 'auth_status_response',
                'authenticated': False,
                'error': f'Error checking auth status: {str(e)}',
                'expires_at': None,
                'account_info': None,
                'last_check': datetime.now().isoformat(),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_exchange_tokens(self, websocket, client_msg):
        """Handle token exchange using connection_manager"""
        try:
            logger.info("🔐 Processing token exchange request...")
            
            auth_code = client_msg.get('code', '').strip()
            
            if not auth_code:
                await websocket.send(json.dumps({
                    'type': 'token_exchange_response',
                    'success': False,
                    'error': 'No authorization code provided',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"🔄 Processing authorization code: {auth_code[:20]}...")
            
            # Import connection_manager to exchange tokens
            import connection_manager
            
            # Use connection_manager directly to exchange tokens
            tokens = connection_manager.get_tokens(auth_code)
            
            if tokens:
                logger.info("✅ Tokens exchanged and saved successfully via WebSocket")
                await websocket.send(json.dumps({
                    'type': 'token_exchange_response',
                    'success': True,
                    'message': 'Authentication completed successfully',
                    'expires_at': tokens.get('expires_at'),
                    'timestamp': datetime.now().isoformat()
                }))
                
                # Broadcast auth status update to all clients
                await self.broadcast_auth_status_update(True)
                
            else:
                logger.error("❌ Token exchange failed via WebSocket")
                await websocket.send(json.dumps({
                    'type': 'token_exchange_response',
                    'success': False,
                    'error': 'Failed to exchange code for tokens',
                    'timestamp': datetime.now().isoformat()
                }))
                
        except Exception as e:
            error_msg = f'Error processing token exchange: {str(e)}'
            logger.error(f"❌ {error_msg}")
            await websocket.send(json.dumps({
                'type': 'token_exchange_response',
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }))

    async def broadcast_auth_status_update(self, authenticated):
        """Broadcast authentication status update to all clients"""
        if not self.clients:
            return
        
        message = json.dumps({
            'type': 'auth_status_updated',
            'authenticated': authenticated,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting auth status update: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        logger.info(f"📡 Auth status update broadcasted to {len(self.clients)} clients")

    async def handle_start_trading(self, websocket, client_msg):
        """Handle starting the trading engine"""
        try:
            logger.info("🚀 Starting trading engine via WebSocket...")
            
            # Import and start trading engine
            import subprocess
            import psutil
            
            # Check if trading engine is already running
            trading_engine_running = False
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any('trading_engine.py' in arg for arg in cmdline):
                        trading_engine_running = True
                        logger.info(f"Trading engine already running: PID {proc.info['pid']}")
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            if not trading_engine_running:
                # Start new trading engine process
                logger.info("🚀 Starting new trading_engine.py process...")
                trading_process = subprocess.Popen([
                    'python3', 'trading_engine.py'
                ], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                cwd=os.getcwd()
                )
                
                logger.info(f"✅ Started trading engine process with PID: {trading_process.pid}")
                
                # Give the process a moment to initialize
                import time
                time.sleep(2)
                
                # Check if the process is still running
                if trading_process.poll() is None:
                    logger.info("✅ Trading engine is running successfully")
                    success = True
                    message = "Trading engine started successfully"
                else:
                    logger.error("❌ Trading engine failed to start")
                    success = False
                    message = "Trading engine failed to start"
            else:
                success = True
                message = "Trading engine is already running"
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'trading_start_response',
                'success': success,
                'message': message,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast trading status update to all clients
            if success:
                await self.broadcast_trading_status_update('running')
            
        except Exception as e:
            error_msg = f'Error starting trading engine: {str(e)}'
            logger.error(f"❌ {error_msg}")
            await websocket.send(json.dumps({
                'type': 'trading_start_response',
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_stop_trading(self, websocket, client_msg):
        """Handle stopping the trading engine"""
        try:
            logger.info("🛑 Stopping trading engine via WebSocket...")
            
            import psutil
            import signal
            
            # Find and terminate trading engine processes
            terminated_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any('trading_engine.py' in arg for arg in cmdline):
                        logger.info(f"🛑 Terminating trading engine process: PID {proc.info['pid']}")
                        proc.terminate()
                        terminated_processes.append(proc.info['pid'])
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            if terminated_processes:
                # Wait for processes to terminate gracefully
                logger.info(f"⏳ Waiting for {len(terminated_processes)} processes to terminate...")
                import time
                time.sleep(2)
                
                success = True
                message = f"Trading engine stopped successfully ({len(terminated_processes)} processes terminated)"
            else:
                success = True
                message = "Trading engine was not running"
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'trading_stop_response',
                'success': success,
                'message': message,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast trading status update to all clients
            await self.broadcast_trading_status_update('stopped')
            
        except Exception as e:
            error_msg = f'Error stopping trading engine: {str(e)}'
            logger.error(f"❌ {error_msg}")
            await websocket.send(json.dumps({
                'type': 'trading_stop_response',
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_get_trading_status(self, websocket):
        """Handle getting trading engine status"""
        try:
            logger.info("🔍 Checking trading engine status via WebSocket...")
            
            import psutil
            
            # Check if trading engine is running
            trading_engine_running = False
            active_processes = []
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any('trading_engine.py' in arg for arg in cmdline):
                        trading_engine_running = True
                        active_processes.append({
                            'pid': proc.info['pid'],
                            'create_time': datetime.fromtimestamp(proc.info['create_time']).isoformat()
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Check for processed_signals.json
            processed_signals_exists = os.path.exists('processed_signals.json')
            processed_signals_count = 0
            
            if processed_signals_exists:
                try:
                    with open('processed_signals.json', 'r') as f:
                        processed_signals = json.load(f)
                        processed_signals_count = len(processed_signals)
                except Exception:
                    pass
            
            # Check for active orders
            active_orders_count = 0
            if os.path.exists('orders/active_orders.json'):
                try:
                    with open('orders/active_orders.json', 'r') as f:
                        active_orders = json.load(f)
                        active_orders_count = len(active_orders)
                except Exception:
                    pass
            
            # Determine status
            if trading_engine_running:
                status = 'running'
                status_message = f'Trading engine is running ({len(active_processes)} processes)'
            else:
                status = 'stopped'
                status_message = 'Trading engine is not running'
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'trading_status_response',
                'success': True,
                'running': trading_engine_running,
                'status': status,
                'message': status_message,
                'active_processes': active_processes,
                'processed_signals_count': processed_signals_count,
                'active_orders_count': active_orders_count,
                'processed_signals_file_exists': processed_signals_exists,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            error_msg = f'Error getting trading status: {str(e)}'
            logger.error(f"❌ {error_msg}")
            await websocket.send(json.dumps({
                'type': 'trading_status_response',
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_emergency_stop(self, websocket, client_msg):
        """Handle emergency stop of trading engine"""
        try:
            logger.info("🚨 EMERGENCY STOP triggered via WebSocket...")
            
            import psutil
            import signal
            
            # Find and forcefully kill all trading-related processes
            terminated_processes = []
            process_types = ['trading_engine.py', 'iron_condor_strategy.py', 'pml_strategy.py', 'divergence_strategy.py']
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(process_type in ' '.join(cmdline) for process_type in process_types):
                        logger.info(f"🚨 EMERGENCY STOP: Killing process PID {proc.info['pid']}")
                        proc.kill()  # Use kill() instead of terminate() for emergency stop
                        terminated_processes.append(proc.info['pid'])
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Wait briefly for processes to die
            import time
            time.sleep(1)
            
            success = True
            message = f"EMERGENCY STOP executed - {len(terminated_processes)} processes terminated"
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'emergency_stop_response',
                'success': success,
                'message': message,
                'terminated_processes': terminated_processes,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast emergency stop to all clients
            await self.broadcast_trading_status_update('emergency_stopped')
            
        except Exception as e:
            error_msg = f'Error executing emergency stop: {str(e)}'
            logger.error(f"❌ {error_msg}")
            await websocket.send(json.dumps({
                'type': 'emergency_stop_response',
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }))

    async def broadcast_trading_status_update(self, status):
        """Broadcast trading status update to all clients"""
        if not self.clients:
            return
        
        message = json.dumps({
            'type': 'trading_status_updated',
            'status': status,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting trading status update: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        logger.info(f"📡 Trading status update ({status}) broadcasted to {len(self.clients)} clients")

    def stop(self):
        """Stop the server and cleanup"""
        logger.info("🛑 Stopping WebSocket server...")
        self.running = False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL Database WebSocket Streamer')
    parser.add_argument('--port', type=int, default=8765, help='WebSocket port (default: 8765)')
    
    args = parser.parse_args()
    
    # Create and start WebSocket streamer
    streamer = DatabaseWebSocketStreamer(args.port)
    
    try:
        # Run the WebSocket server
        asyncio.run(streamer.start_server())
    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt")
    finally:
        streamer.stop()
        logger.info("👋 WebSocket server stopped")

if __name__ == "__main__":
    main()
