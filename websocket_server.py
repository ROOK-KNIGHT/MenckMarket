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
        """Handle getting current strategy configuration"""
        try:
            strategy_files = {
                'iron_condor': 'iron_condor_signals.json',
                'pml': 'pml_signals.json',
                'divergence': 'divergence_signals.json'
            }
            
            config = {}
            
            for strategy_name, filename in strategy_files.items():
                if os.path.exists(filename):
                    with open(filename, 'r') as f:
                        strategy_data = json.load(f)
                    
                    # Extract strategy controls from metadata
                    strategy_controls = strategy_data.get('metadata', {}).get('strategy_controls', {})
                    
                    config[strategy_name] = {
                        'auto_execute': strategy_controls.get('auto_execute', False),
                        'manual_approval': strategy_controls.get('manual_approval', False)
                    }
                else:
                    # Default configuration if file doesn't exist
                    config[strategy_name] = {
                        'auto_execute': True,  # Default to auto execute
                        'manual_approval': False
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
