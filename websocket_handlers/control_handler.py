#!/usr/bin/env python3
"""
Control Handler
Handles all WebSocket control operations (risk management, strategies, watchlist, trading, auth)
"""

import asyncio
import json
from datetime import datetime
import logging
import os
import subprocess
import psutil
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ControlHandler:
    """Handler for all control operations"""
    
    def __init__(self, broadcast_callback, db_query_handler):
        """Initialize the control handler"""
        self.broadcast_callback = broadcast_callback
        self.db_query_handler = db_query_handler
    
    async def handle_message(self, websocket, client_msg, client_addr):
        """Route control messages to appropriate handlers"""
        message_type = client_msg.get('type')
        
        try:
            if message_type == 'save_risk_config':
                await self.handle_risk_config_save(websocket, client_msg)
            
            elif message_type == 'get_risk_config':
                await self.handle_risk_config_get(websocket)
            
            elif message_type == 'save_strategy_config':
                logger.info(f"📨 Received save_strategy_config message from {client_addr}")
                await self.handle_strategy_config_save(websocket, client_msg)
            
            elif message_type == 'get_strategy_config':
                logger.info(f"📨 Received get_strategy_config message from {client_addr}")
                await self.handle_strategy_config_get(websocket)
            
            elif message_type == 'add_watchlist_symbol':
                logger.info(f"📨 Received add_watchlist_symbol message from {client_addr}")
                await self.handle_add_watchlist_symbol(websocket, client_msg)
            
            elif message_type == 'remove_watchlist_symbol':
                logger.info(f"📨 Received remove_watchlist_symbol message from {client_addr}")
                await self.handle_remove_watchlist_symbol(websocket, client_msg)
            
            elif message_type == 'get_watchlist':
                logger.info(f"📨 Received get_watchlist message from {client_addr}")
                await self.handle_get_watchlist(websocket)
            
            elif message_type == 'refresh_watchlist_with_cleanup':
                logger.info(f"📨 Received refresh_watchlist_with_cleanup message from {client_addr}")
                await self.handle_refresh_watchlist_with_cleanup(websocket)
            
            elif message_type == 'check_auth_status':
                logger.info(f"📨 Received check_auth_status message from {client_addr}")
                await self.handle_check_auth_status(websocket)
            
            elif message_type == 'exchange_tokens':
                logger.info(f"📨 Received exchange_tokens message from {client_addr}")
                await self.handle_exchange_tokens(websocket, client_msg)
            
            elif message_type == 'save_trading_config':
                logger.info(f"📨 Received save_trading_config message from {client_addr}")
                await self.handle_trading_config_save(websocket, client_msg)
            
            elif message_type == 'get_trading_config':
                logger.info(f"📨 Received get_trading_config message from {client_addr}")
                await self.handle_trading_config_get(websocket)
            
            elif message_type == 'save_risk_settings':
                logger.info(f"📨 Received save_risk_settings message from {client_addr}")
                await self.handle_risk_settings_save(websocket, client_msg)
            
            elif message_type == 'save_strategy_watchlist':
                logger.info(f"📨 Received save_strategy_watchlist message from {client_addr}")
                await self.handle_strategy_watchlist_save(websocket, client_msg)
            
            elif message_type == 'get_strategy_watchlist':
                logger.info(f"📨 Received get_strategy_watchlist message from {client_addr}")
                await self.handle_strategy_watchlist_get(websocket, client_msg)
            
            elif message_type == 'add_strategy_watchlist_symbol':
                logger.info(f"📨 Received add_strategy_watchlist_symbol message from {client_addr}")
                await self.handle_add_strategy_watchlist_symbol(websocket, client_msg)
            
            elif message_type == 'remove_strategy_watchlist_symbol':
                logger.info(f"📨 Received remove_strategy_watchlist_symbol message from {client_addr}")
                await self.handle_remove_strategy_watchlist_symbol(websocket, client_msg)
            
            elif message_type == 'run_strategy':
                logger.info(f"📨 Received run_strategy message from {client_addr}")
                await self.handle_run_strategy(websocket, client_msg)
            
            elif message_type == 'stop_strategy':
                logger.info(f"📨 Received stop_strategy message from {client_addr}")
                await self.handle_stop_strategy(websocket, client_msg)
            
            elif message_type == 'execute_python_script':
                logger.info(f"📨 Received execute_python_script message from {client_addr}")
                await self.handle_execute_python_script(websocket, client_msg)
            
            elif message_type == 'stop_python_script':
                logger.info(f"📨 Received stop_python_script message from {client_addr}")
                await self.handle_stop_python_script(websocket, client_msg)
            
            elif message_type == 'get_strategy_status':
                logger.info(f"📨 Received get_strategy_status message from {client_addr}")
                await self.handle_get_strategy_status(websocket, client_msg)
            
            elif message_type == 'save_timing_settings':
                logger.info(f"📨 Received save_timing_settings message from {client_addr}")
                await self.handle_timing_settings_save(websocket, client_msg)
            
            elif message_type == 'get_timing_settings':
                logger.info(f"📨 Received get_timing_settings message from {client_addr}")
                await self.handle_timing_settings_get(websocket, client_msg)
            
            elif message_type == 'close_all_positions':
                logger.info(f"📨 Received close_all_positions message from {client_addr}")
                await self.handle_close_all_positions(websocket, client_msg)
            
            elif message_type == 'request_trading_statistics':
                logger.info(f"📨 Received request_trading_statistics message from {client_addr}")
                await self.handle_request_trading_statistics(websocket, client_msg)
            
            else:
                logger.warning(f"⚠️ Unknown message type: {message_type}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error handling {message_type} message: {e}")
            await websocket.send(json.dumps({
                'type': f'{message_type}_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
            return False

    # Risk Management Handlers
    async def handle_risk_config_save(self, websocket, client_msg):
        """Handle saving risk configuration to JSON file"""
        try:
            config_data = client_msg.get('config', {})
            config_file = 'trading_config_live.json'
            
            # Load existing config or create new one
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"risk_management": {}, "strategies": {}}
            
            # Update risk management configuration
            risk_config = existing_config.get('risk_management', {})

            trading_config = existing_config.get('trading_mode', {})

            emergency_config = existing_config.get('emergency_settings', {})
            if 'emergency_stop_trading' in config_data:
                emergency_config['emergency_stop_trading'] = config_data['emergency_stop_trading']
            if 'pause_new_positions' in config_data:
                emergency_config['pause_new_positions'] = config_data['pause_new_positions']
            if 'close_all_positions' in config_data:
                emergency_config['close_all_positions'] = config_data['close_all_positions']


            # Update trading mode configuration
            if 'trading_enabled' in config_data:
                trading_config['trading_enabled'] = config_data['trading_enabled']
            if 'sandbox_trading' in config_data:
                trading_config['sandbox_trading'] = config_data['sandbox_trading']
            


            # Update account limits
            if 'account_limits' in config_data:
                if 'account_limits' not in risk_config:
                    risk_config['account_limits'] = {}
                risk_config['account_limits'].update(config_data['account_limits'])
                
                # Add timestamp and updated_by for target daily profit settings
                if 'max_target_daily_profit_percent' in config_data['account_limits'] or 'min_target_daily_profit_percent' in config_data['account_limits']:
                    risk_config['account_limits']['last_updated'] = datetime.now().isoformat()
                    risk_config['account_limits']['updated_by'] = config_data.get('updated_by', 'web_interface')
            
            # Update parameter states
            if 'parameter_states' in config_data:
                if 'parameter_states' not in risk_config:
                    risk_config['parameter_states'] = {}
                risk_config['parameter_states'].update(config_data['parameter_states'])
            
            # Update metadata
            risk_config['metadata'] = {
                'updated_by': config_data.get('updated_by', 'web_interface'),
                'version': '1.1.0',
                'last_updated': datetime.now().isoformat()
            }
            
            # Update strategy-specific configurations
            if 'strategies' in config_data:
                if 'strategies' not in existing_config:
                    existing_config['strategies'] = {}
                
                for strategy_name, strategy_config in config_data['strategies'].items():
                    if strategy_name not in existing_config['strategies']:
                        existing_config['strategies'][strategy_name] = {}
                    
                    # Update auto_approve setting
                    if 'auto_approve' in strategy_config:
                        existing_config['strategies'][strategy_name]['auto_approve'] = strategy_config['auto_approve']
                    
                    # Update risk management settings
                    if 'risk_management' in strategy_config:
                        existing_config['strategies'][strategy_name]['risk_management'] = strategy_config['risk_management']
            
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
            config_file = 'trading_config_live.json'
            
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
            else:
                # Return default config if file doesn't exist
                config = {
                    "risk_management": {
                        "account_limits": {
                            "daily_loss_limit": 2,
                            "max_account_risk": 25,
                            "equity_buffer": 10000,
                            "max_positions": 10
                        },
                        "parameter_states": {
                            "enable_max_account_risk": True,
                            "enable_daily_loss_limit": True,
                            "enable_equity_buffer": True,
                            "enable_max_positions": True,
                            "enable_target_daily_profit": True
                        },
                        "metadata": {
                            "updated_by": "system_default",
                            "version": "1.1.0",
                            "last_updated": "2024-06-15T10:00:00Z"
                        }
                    },
                    "strategies": {
                        "pml": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 15,
                                "position_size": 20,
                                "max_contracts": 8
                            }
                        },
                        "iron_condor": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 10,
                                "position_size": 25,
                                "max_contracts": 10
                            }
                        },
                        "divergence": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 20,
                                "position_size": 15,
                                "max_shares": 1000
                            }
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
        message = {
            'type': 'risk_config_updated',
            'config': config,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Risk config update broadcasted to all clients")

    # Strategy Configuration Handlers
    async def handle_strategy_config_save(self, websocket, client_msg):
        """Handle saving strategy configuration to trading_config_live.json"""
        logger.info("🔧 Strategy config save request received")
        
        try:
            config_data = client_msg.get('config', {})
            config_file = 'trading_config_live.json'
            
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    auto_approve_config = json.load(f)
            else:
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
            
            for strategy_name in strategies_to_update:
                strategy_config = config_data[strategy_name]
                
                # Determine auto_approve status based on toggle states
                auto_approve = False
                if strategy_config.get('auto_approve', False):
                    auto_approve = False  # Auto-approve disabled
                elif strategy_config.get('auto_approve', True):
                    auto_approve = True  # Auto-approve enabled

                # Update strategy controls in centralized config
                auto_approve_config['strategy_controls'][strategy_name] = {
                    'auto_approve': auto_approve
                }
                
                updated_strategies.append(strategy_name)
            
            # Update metadata
            auto_approve_config['metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'updated_by': 'web_interface',
                'version': '1.0.0',
            }
            
            # Save updated centralized config
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
        """Handle getting current strategy configuration from trading_config_live.json"""
        try:
            config_file = 'trading_config_live.json'
            
            if os.path.exists(config_file):
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
            else:
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
        message = {
            'type': 'strategy_config_updated',
            'config': config,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Strategy config update broadcasted to all clients")

    # Watchlist Handlers
    async def handle_add_watchlist_symbol(self, websocket, client_msg):
        """Handle adding a symbol to the watchlist"""
        try:
            symbol = client_msg.get('symbol', '').strip().upper()
            
            if not symbol:
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
            
            # Check if symbol already exists
            current_symbols = watchlist_data.get('watchlist', {}).get('symbols', [])
            
            if symbol in current_symbols:
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': f'{symbol} is already in the watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Add symbol to watchlist
            current_symbols.append(symbol)
            
            # Update metadata
            watchlist_data['watchlist']['symbols'] = current_symbols
            watchlist_data['watchlist']['metadata']['last_updated'] = datetime.now().isoformat()
            watchlist_data['watchlist']['metadata']['total_symbols'] = len(current_symbols)
            watchlist_data['watchlist']['metadata']['managed_by'] = 'websocket_server'
            
            # Save updated watchlist
            with open(watchlist_file, 'w') as f:
                json.dump(watchlist_data, f, indent=2)
            
            logger.info(f"✅ Successfully added {symbol} to watchlist. Total symbols: {len(current_symbols)}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'watchlist_symbol_added',
                'success': True,
                'symbol': symbol,
                'message': f'Successfully added {symbol} to watchlist',
                'total_symbols': len(current_symbols),
                'watchlist': watchlist_data['watchlist'],
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast watchlist update to all clients
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error adding symbol to watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_remove_watchlist_symbol(self, websocket, client_msg):
        """Handle removing a symbol from the watchlist"""
        try:
            symbol = client_msg.get('symbol', '').strip().upper()
            
            if not symbol:
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
            
            if not os.path.exists(watchlist_file):
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': 'Watchlist file not found',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            with open(watchlist_file, 'r') as f:
                watchlist_data = json.load(f)
            
            # Check if symbol exists
            current_symbols = watchlist_data.get('watchlist', {}).get('symbols', [])
            
            if symbol not in current_symbols:
                await websocket.send(json.dumps({
                    'type': 'watchlist_error',
                    'success': False,
                    'error': f'{symbol} is not in the watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Remove symbol from watchlist
            current_symbols.remove(symbol)
            
            # Update metadata
            watchlist_data['watchlist']['symbols'] = current_symbols
            watchlist_data['watchlist']['metadata']['last_updated'] = datetime.now().isoformat()
            watchlist_data['watchlist']['metadata']['total_symbols'] = len(current_symbols)
            watchlist_data['watchlist']['metadata']['managed_by'] = 'websocket_server'
            
            # Save updated watchlist
            with open(watchlist_file, 'w') as f:
                json.dump(watchlist_data, f, indent=2)
            
            logger.info(f"✅ Successfully removed {symbol} from watchlist. Total symbols: {len(current_symbols)}")
            
            # Truncate integrated watchlist database table and restart real-time monitor
            await self.truncate_watchlist_database_and_restart_monitor()
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'watchlist_symbol_removed',
                'success': True,
                'symbol': symbol,
                'message': f'Successfully removed {symbol} from watchlist',
                'total_symbols': len(current_symbols),
                'watchlist': watchlist_data['watchlist'],
                'database_refreshed': True,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast watchlist update to all clients
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error removing symbol from watchlist: {e}")
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
            
            # Truncate integrated watchlist database table and restart real-time monitor
            await self.truncate_watchlist_database_and_restart_monitor()
            
            # Get current watchlist data
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
            await self.broadcast_watchlist_update(watchlist_data['watchlist'])
            
        except Exception as e:
            logger.error(f"❌ Error refreshing watchlist with cleanup: {e}")
            await websocket.send(json.dumps({
                'type': 'watchlist_error',
                'success': False,
                'error': f'Failed to refresh watchlist with cleanup: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }))
    
    async def broadcast_watchlist_update(self, watchlist):
        """Broadcast watchlist update to all clients"""
        message = {
            'type': 'watchlist_updated',
            'watchlist': watchlist,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Watchlist update broadcasted to all clients")

    async def truncate_watchlist_database_and_restart_monitor(self):
        """Truncate integrated watchlist database table and restart real-time monitor for fresh data"""
        try:
            logger.info("🗄️ Starting database cleanup and monitor restart process...")
            
            # Truncate integrated watchlist database table
            try:
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
            
            # Restart real-time monitor process
            try:
                # Find and terminate existing realtime_monitor.py processes
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
                    time.sleep(2)
                
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
                    
            except Exception as restart_error:
                logger.error(f"❌ Error restarting real-time monitor: {restart_error}")
            
            logger.info("🔄 Database cleanup and monitor restart process completed")
            
        except Exception as e:
            logger.error(f"❌ Error in truncate_watchlist_database_and_restart_monitor: {e}")

    # Authentication Handlers
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
        message = {
            'type': 'auth_status_updated',
            'authenticated': authenticated,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Auth status update broadcasted to all clients")


    # Trading Configuration Handlers
    async def handle_trading_config_save(self, websocket, client_msg):
        """Handle saving trading configuration to trading_config_live.json"""
        try:
            config_data = client_msg.get('config', {})
            config_file = 'trading_config_live.json'
            
            logger.info(f"💾 Saving trading configuration: {list(config_data.keys())}")
            
            # Load existing config or create new one
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {
                    "trading_mode": {
                        "trading_enabled": True,
                        "sandbox_trading": False
                    },
                    "emergency_controls": {
                        "emergency_stop_trading": False,
                        "pause_new_positions": False,
                        "close_all_positions": False
                    },
                    "risk_management": {
                        "account_limits": {
                            "daily_loss_limit": 2,
                            "max_account_risk": 25,
                            "equity_buffer": 10000,
                            "max_positions": 10
                        },
                        "parameter_states": {
                            "enable_max_account_risk": False,
                            "enable_daily_loss_limit": False,
                            "enable_equity_buffer": False,
                            "enable_max_positions": False
                        },
                        "metadata": {
                            "updated_by": "web_interface",
                            "version": "1.1.0",
                            "last_updated": "2024-06-15T10:00:00Z"
                        }
                    },
                    "strategies": {
                        "pml": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 15,
                                "position_size": 20,
                                "max_contracts": 8
                            }
                        },
                        "iron_condor": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 10,
                                "position_size": 25,
                                "max_contracts": 10
                            }
                        },
                        "divergence": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 20,
                                "position_size": 15,
                                "max_shares": 1000
                            }
                        }
                    }
                }
            
            # Update trading mode settings
            if 'trading_mode' in config_data:
                if 'trading_mode' not in existing_config:
                    existing_config['trading_mode'] = {}
                existing_config['trading_mode'].update(config_data['trading_mode'])
                logger.info(f"📊 Updated trading mode: {config_data['trading_mode']}")
            
            # Update emergency controls
            if 'emergency_controls' in config_data:
                if 'emergency_controls' not in existing_config:
                    existing_config['emergency_controls'] = {}
                existing_config['emergency_controls'].update(config_data['emergency_controls'])
                logger.info(f"🚨 Updated emergency controls: {config_data['emergency_controls']}")
            
            # Update risk management settings
            if 'risk_management' in config_data:
                if 'risk_management' not in existing_config:
                    existing_config['risk_management'] = {}
                
                risk_config = existing_config['risk_management']
                
                # Update account limits
                if 'account_limits' in config_data['risk_management']:
                    if 'account_limits' not in risk_config:
                        risk_config['account_limits'] = {}
                    risk_config['account_limits'].update(config_data['risk_management']['account_limits'])
                
                # Update parameter states
                if 'parameter_states' in config_data['risk_management']:
                    if 'parameter_states' not in risk_config:
                        risk_config['parameter_states'] = {}
                    risk_config['parameter_states'].update(config_data['risk_management']['parameter_states'])
                
                # Update metadata
                risk_config['metadata'] = {
                    'updated_by': config_data.get('updated_by', 'web_interface'),
                    'version': '1.1.0'
                }
                
                logger.info(f"⚖️ Updated risk management settings")
            
            # Update strategy-specific configurations
            if 'strategies' in config_data:
                if 'strategies' not in existing_config:
                    existing_config['strategies'] = {}
                
                for strategy_name, strategy_config in config_data['strategies'].items():
                    if strategy_name not in existing_config['strategies']:
                        existing_config['strategies'][strategy_name] = {}
                    
                    # Update auto_approve setting
                    if 'auto_approve' in strategy_config:
                        existing_config['strategies'][strategy_name]['auto_approve'] = strategy_config['auto_approve']
                    
                    # Update auto_timer setting
                    if 'auto_timer' in strategy_config:
                        existing_config['strategies'][strategy_name]['auto_timer'] = strategy_config['auto_timer']
                    
                    # Update risk management settings
                    if 'risk_management' in strategy_config:
                        existing_config['strategies'][strategy_name]['risk_management'] = strategy_config['risk_management']
                
                logger.info(f"🎯 Updated strategies: {list(config_data['strategies'].keys())}")
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Trading configuration saved successfully by {config_data.get('updated_by', 'web_interface')}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'trading_config_saved',
                'success': True,
                'message': 'Trading configuration saved successfully',
                'config': existing_config,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving trading config: {e}")
            await websocket.send(json.dumps({
                'type': 'trading_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_trading_config_get(self, websocket):
        """Handle getting current trading configuration from trading_config_live.json"""
        try:
            config_file = 'trading_config_live.json'
            
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
                logger.info(f"📋 Retrieved trading configuration from {config_file}")
            else:
                # Return default config if file doesn't exist
                config = {
                    "trading_mode": {
                        "trading_enabled": True,
                        "sandbox_trading": False
                    },
                    "emergency_controls": {
                        "emergency_stop_trading": False,
                        "pause_new_positions": False,
                        "close_all_positions": False
                    },
                    "risk_management": {
                        "account_limits": {
                            "daily_loss_limit": 2,
                            "max_account_risk": 25,
                            "equity_buffer": 10000,
                            "max_positions": 10
                        },
                        "parameter_states": {
                            "enable_max_account_risk": False,
                            "enable_daily_loss_limit": False,
                            "enable_equity_buffer": False,
                            "enable_max_positions": False
                        },
                        "metadata": {
                            "updated_by": "system_default",
                            "version": "1.1.0"
                        }
                    },
                    "strategies": {
                        "pml": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 15,
                                "position_size": 20,
                                "max_contracts": 8
                            }
                        },
                        "iron_condor": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 10,
                                "position_size": 25,
                                "max_contracts": 10
                            }
                        },
                        "divergence": {
                            "auto_approve": True,
                            "risk_management": {
                                "strategy_allocation": 20,
                                "position_size": 15,
                                "max_shares": 1000
                            }
                        }
                    }
                }
                logger.info("📋 Using default trading configuration")
            
            # Send config response
            await websocket.send(json.dumps({
                'type': 'trading_config_data',
                'success': True,
                'config': config,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting trading config: {e}")
            await websocket.send(json.dumps({
                'type': 'trading_config_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def broadcast_trading_config_update(self, config):
        """Broadcast trading configuration update to all clients"""
        message = {
            'type': 'trading_config_updated',
            'config': config,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Trading config update broadcasted to all clients")

    # Risk Settings Handler
    async def handle_risk_settings_save(self, websocket, client_msg):
        """Handle saving risk settings for a specific strategy to trading_config_live.json"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            settings = client_msg.get('settings', {})
            config_file = 'trading_config_live.json'
            
            logger.info(f"💾 Saving risk settings for strategy {strategy_id}: {settings}")
            
            if not strategy_id or not settings:
                await websocket.send(json.dumps({
                    'type': 'risk_settings_error',
                    'success': False,
                    'error': 'Strategy ID and settings are required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config or create new one
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {
                    "trading_mode": {
                        "trading_enabled": True,
                        "sandbox_trading": False
                    },
                    "emergency_controls": {
                        "emergency_stop_trading": False,
                        "pause_new_positions": False,
                        "close_all_positions": False
                    },
                    "risk_management": {
                        "account_limits": {
                            "daily_loss_limit": 2,
                            "max_account_risk": 25,
                            "equity_buffer": 10000,
                            "max_positions": 10
                        },
                        "parameter_states": {
                            "enable_max_account_risk": False,
                            "enable_daily_loss_limit": False,
                            "enable_equity_buffer": False,
                            "enable_max_positions": False
                        },
                        "metadata": {
                            "updated_by": "web_interface",
                            "version": "1.1.0"
                        }
                    },
                    "strategies": {}
                }
            
            # Ensure strategies section exists
            if 'strategies' not in existing_config:
                existing_config['strategies'] = {}
            
            # Ensure the specific strategy exists
            if strategy_id not in existing_config['strategies']:
                existing_config['strategies'][strategy_id] = {
                    "auto_approve": False,
                    "risk_management": {}
                }
            
            # Update the risk management settings for this strategy
            if 'risk_management' not in existing_config['strategies'][strategy_id]:
                existing_config['strategies'][strategy_id]['risk_management'] = {}
            
            risk_mgmt = existing_config['strategies'][strategy_id]['risk_management']
            
            # Map the settings from the frontend to the config structure
            if 'allocation_percentage' in settings:
                risk_mgmt['strategy_allocation'] = settings['allocation_percentage']
            
            if 'position_size_percentage' in settings:
                risk_mgmt['position_size'] = settings['position_size_percentage']
            
            if 'max_contracts' in settings:
                risk_mgmt['max_contracts'] = settings['max_contracts']
            
            if 'max_shares' in settings:
                risk_mgmt['max_shares'] = settings['max_shares']
            
            # Add metadata
            risk_mgmt['last_updated'] = datetime.now().isoformat()
            risk_mgmt['updated_by'] = 'strategy_manager'
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Risk settings saved successfully for strategy {strategy_id}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'risk_settings_saved',
                'success': True,
                'message': f'Risk settings saved successfully for {strategy_id}',
                'strategy_id': strategy_id,
                'settings': settings,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving risk settings: {e}")
            await websocket.send(json.dumps({
                'type': 'risk_settings_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    # Strategy Watchlist Handlers
    async def handle_strategy_watchlist_save(self, websocket, client_msg):
        """Handle saving strategy watchlist to trading_config_live.json"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            symbols = client_msg.get('symbols', [])
            config_file = 'trading_config_live.json'
            
            logger.info(f"💾 Saving strategy watchlist for {strategy_id}: {symbols}")
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Ensure strategies section exists
            if 'strategies' not in existing_config:
                existing_config['strategies'] = {}
            
            # Ensure the specific strategy exists
            if strategy_id not in existing_config['strategies']:
                existing_config['strategies'][strategy_id] = {
                    "auto_approve": False,
                    "risk_management": {}
                }
            
            # Generate watchlist key based on strategy
            watchlist_key = f"{strategy_id}_strategy_watchlist"
            if strategy_id == 'pml':
                watchlist_key = "pmlstrategy_watchlist"
            
            # Update the watchlist for this strategy
            existing_config['strategies'][strategy_id][watchlist_key] = symbols
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Strategy watchlist saved successfully for {strategy_id}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_saved',
                'success': True,
                'message': f'Strategy watchlist saved successfully for {strategy_id}',
                'strategy_id': strategy_id,
                'symbols': symbols,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving strategy watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_strategy_watchlist_get(self, websocket, client_msg):
        """Handle getting strategy watchlist from trading_config_live.json"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            config_file = 'trading_config_live.json'
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Get watchlist for the strategy
            symbols = []
            if 'strategies' in existing_config and strategy_id in existing_config['strategies']:
                strategy_config = existing_config['strategies'][strategy_id]
                
                # Try different watchlist key formats
                watchlist_key = f"{strategy_id}_strategy_watchlist"
                if strategy_id == 'pml':
                    watchlist_key = "pmlstrategy_watchlist"
                
                symbols = strategy_config.get(watchlist_key, [])
            
            logger.info(f"📋 Retrieved strategy watchlist for {strategy_id}: {symbols}")
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_data',
                'success': True,
                'strategy_id': strategy_id,
                'symbols': symbols,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting strategy watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_add_strategy_watchlist_symbol(self, websocket, client_msg):
        """Handle adding a symbol to strategy watchlist"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            symbol = client_msg.get('symbol', '').strip().upper()
            config_file = 'trading_config_live.json'
            
            logger.info(f"📋 Adding symbol {symbol} to {strategy_id} watchlist")
            
            if not strategy_id or not symbol:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': 'Strategy ID and symbol are required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Ensure strategies section exists
            if 'strategies' not in existing_config:
                existing_config['strategies'] = {}
            
            # Ensure the specific strategy exists
            if strategy_id not in existing_config['strategies']:
                existing_config['strategies'][strategy_id] = {
                    "auto_approve": False,
                    "risk_management": {}
                }
            
            # Generate watchlist key based on strategy
            watchlist_key = f"{strategy_id}_strategy_watchlist"
            if strategy_id == 'pml':
                watchlist_key = "pmlstrategy_watchlist"
            
            # Get current symbols
            current_symbols = existing_config['strategies'][strategy_id].get(watchlist_key, [])
            
            # Check if symbol already exists
            if symbol in current_symbols:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': f'{symbol} is already in {strategy_id} watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Add symbol to watchlist
            current_symbols.append(symbol)
            existing_config['strategies'][strategy_id][watchlist_key] = current_symbols
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Successfully added {symbol} to {strategy_id} watchlist")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_symbol_added',
                'success': True,
                'strategy_id': strategy_id,
                'symbol': symbol,
                'symbols': current_symbols,
                'message': f'Successfully added {symbol} to {strategy_id} watchlist',
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error adding symbol to strategy watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_remove_strategy_watchlist_symbol(self, websocket, client_msg):
        """Handle removing a symbol from strategy watchlist"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            symbol = client_msg.get('symbol', '').strip().upper()
            config_file = 'trading_config_live.json'
            
            logger.info(f"📋 Removing symbol {symbol} from {strategy_id} watchlist")
            
            if not strategy_id or not symbol:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': 'Strategy ID and symbol are required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': 'Configuration file not found',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Check if strategy exists
            if 'strategies' not in existing_config or strategy_id not in existing_config['strategies']:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': f'Strategy {strategy_id} not found',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Generate watchlist key based on strategy
            watchlist_key = f"{strategy_id}_strategy_watchlist"
            if strategy_id == 'pml':
                watchlist_key = "pmlstrategy_watchlist"
            
            # Get current symbols
            current_symbols = existing_config['strategies'][strategy_id].get(watchlist_key, [])
            
            # Check if symbol exists
            if symbol not in current_symbols:
                await websocket.send(json.dumps({
                    'type': 'strategy_watchlist_error',
                    'success': False,
                    'error': f'{symbol} is not in {strategy_id} watchlist',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Remove symbol from watchlist
            current_symbols.remove(symbol)
            existing_config['strategies'][strategy_id][watchlist_key] = current_symbols
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Successfully removed {symbol} from {strategy_id} watchlist")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_symbol_removed',
                'success': True,
                'strategy_id': strategy_id,
                'symbol': symbol,
                'symbols': current_symbols,
                'message': f'Successfully removed {symbol} from {strategy_id} watchlist',
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error removing symbol from strategy watchlist: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_watchlist_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    # Strategy Execution Handlers
    async def handle_run_strategy(self, websocket, client_msg):
        """Handle running a strategy"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            action = client_msg.get('action', 'start')
            
            logger.info(f"🚀 Running strategy {strategy_id} with action: {action}")
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'strategy_run_error',
                    'success': False,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Special handling for PML strategy - managed by realtime monitor
            if strategy_id == 'pml':
                logger.info(f"🎯 PML strategy start requested - updating JSON for realtime monitor")
                await self.update_strategy_running_state(strategy_id, True)
                await websocket.send(json.dumps({
                    'type': 'strategy_run_started',
                    'success': True,
                    'strategy_id': strategy_id,
                    'message': 'PML strategy start signal sent to realtime monitor',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Map strategy ID to script name for other strategies
            strategy_scripts = {
                'iron_condor': 'iron_condor_strategy.py',
                'divergence': 'divergence_strategy_multi_timeframe.py'
            }
            
            script_name = strategy_scripts.get(strategy_id)
            if not script_name:
                await websocket.send(json.dumps({
                    'type': 'strategy_run_error',
                    'success': False,
                    'error': f'No script configured for strategy: {strategy_id}',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Execute the strategy script
            await self.execute_strategy_script(websocket, strategy_id, script_name)
            
        except Exception as e:
            logger.error(f"❌ Error running strategy: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_run_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_stop_strategy(self, websocket, client_msg):
        """Handle stopping a strategy"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            
            logger.info(f"🛑 Stopping strategy: {strategy_id}")
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'strategy_stop_error',
                    'success': False,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Special handling for PML strategy - managed by realtime monitor
            if strategy_id == 'pml':
                logger.info(f"🎯 PML strategy stop requested - updating JSON for realtime monitor")
                await self.update_strategy_running_state(strategy_id, False)
                await websocket.send(json.dumps({
                    'type': 'strategy_stop_response',
                    'success': True,
                    'strategy_id': strategy_id,
                    'message': 'PML strategy stop signal sent to realtime monitor',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Map strategy ID to script name for other strategies
            strategy_scripts = {
                'iron_condor': 'iron_condor_strategy.py',
                'divergence': 'divergence_strategy_multi_timeframe.py'
            }
            
            script_name = strategy_scripts.get(strategy_id)
            if not script_name:
                await websocket.send(json.dumps({
                    'type': 'strategy_stop_error',
                    'success': False,
                    'error': f'No script configured for strategy: {strategy_id}',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Stop the strategy process
            await self.stop_strategy_process(websocket, strategy_id, script_name)
            
        except Exception as e:
            logger.error(f"❌ Error stopping strategy: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_stop_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_execute_python_script(self, websocket, client_msg):
        """Handle executing a Python script"""
        try:
            script_name = client_msg.get('script_name', '')
            strategy_id = client_msg.get('strategy_id', '')
            execution_id = client_msg.get('execution_id', '')
            
            logger.info(f"🐍 Executing Python script: {script_name} for strategy: {strategy_id}")
            
            if not script_name:
                await websocket.send(json.dumps({
                    'type': 'python_script_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'error': 'Script name is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Execute the script
            await self.execute_strategy_script(websocket, strategy_id, script_name, execution_id)
            
        except Exception as e:
            logger.error(f"❌ Error executing Python script: {e}")
            await websocket.send(json.dumps({
                'type': 'python_script_error',
                'success': False,
                'strategy_id': strategy_id,
                'execution_id': execution_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_stop_python_script(self, websocket, client_msg):
        """Handle stopping a Python script"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            execution_id = client_msg.get('execution_id', '')
            
            logger.info(f"🛑 Stopping Python script for strategy: {strategy_id}, execution: {execution_id}")
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'python_script_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Map strategy ID to script name for process identification
            strategy_scripts = {
                'iron_condor': 'iron_condor_strategy.py',
                'divergence': 'divergence_strategy_multi_timeframe.py'
            }
            
            script_name = strategy_scripts.get(strategy_id)
            if not script_name:
                await websocket.send(json.dumps({
                    'type': 'python_script_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'error': f'No script configured for strategy: {strategy_id}',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Stop the script process
            await self.stop_strategy_process(websocket, strategy_id, script_name, execution_id)
            
        except Exception as e:
            logger.error(f"❌ Error stopping Python script: {e}")
            await websocket.send(json.dumps({
                'type': 'python_script_error',
                'success': False,
                'strategy_id': strategy_id,
                'execution_id': execution_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def execute_strategy_script(self, websocket, strategy_id, script_name, execution_id=None):
        """Execute a strategy script"""
        try:
            logger.info(f"🚀 Starting {script_name} process...")
            
            # Check if script is already running
            script_running = False
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(script_name in arg for arg in cmdline):
                        script_running = True
                        logger.info(f"{script_name} already running: PID {proc.info['pid']}")
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            if not script_running:
                # Start new script process
                logger.info(f"🚀 Starting new {script_name} process...")
                script_process = subprocess.Popen([
                    'python3', script_name
                ], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                cwd=os.getcwd()
                )
                
                logger.info(f"✅ Started {script_name} process with PID: {script_process.pid}")
                
                # Give the process a moment to initialize
                import time
                time.sleep(2)
                
                # Check if the process is still running
                if script_process.poll() is None:
                    logger.info(f"✅ {script_name} is running successfully")
                    success = True
                    message = f"{script_name} started successfully"
                    
                    # Update running state in trading config
                    await self.update_strategy_running_state(strategy_id, True)
                else:
                    logger.error(f"❌ {script_name} failed to start")
                    success = False
                    message = f"{script_name} failed to start"
            else:
                success = True
                message = f"{script_name} is already running"
                
                # Update running state in trading config (already running)
                await self.update_strategy_running_state(strategy_id, True)
            
            # Send response
            if execution_id:
                # For execute_python_script messages
                if success:
                    await websocket.send(json.dumps({
                        'type': 'python_script_started',
                        'success': True,
                        'strategy_id': strategy_id,
                        'execution_id': execution_id,
                        'script_name': script_name,
                        'message': message,
                        'timestamp': datetime.now().isoformat()
                    }))
                    
                    # Don't simulate completion - let the actual script run continuously
                    # The script will handle its own lifecycle and completion
                    logger.info(f"✅ {script_name} started successfully for {strategy_id}")
                else:
                    await websocket.send(json.dumps({
                        'type': 'python_script_error',
                        'success': False,
                        'strategy_id': strategy_id,
                        'execution_id': execution_id,
                        'script_name': script_name,
                        'error': message,
                        'timestamp': datetime.now().isoformat()
                    }))
            else:
                # For run_strategy messages
                await websocket.send(json.dumps({
                    'type': 'strategy_run_started' if success else 'strategy_run_error',
                    'success': success,
                    'strategy_id': strategy_id,
                    'script_name': script_name,
                    'message': message,
                    'timestamp': datetime.now().isoformat()
                }))
            
        except Exception as e:
            error_msg = f'Error executing {script_name}: {str(e)}'
            logger.error(f"❌ {error_msg}")
            
            if execution_id:
                await websocket.send(json.dumps({
                    'type': 'python_script_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'script_name': script_name,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                }))
            else:
                await websocket.send(json.dumps({
                    'type': 'strategy_run_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'script_name': script_name,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                }))

    async def stop_strategy_process(self, websocket, strategy_id, script_name, execution_id=None):
        """Stop a strategy script process"""
        try:
            logger.info(f"🛑 Stopping {script_name} via WebSocket...")
            
            # Find and terminate script processes
            terminated_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(script_name in arg for arg in cmdline):
                        logger.info(f"🛑 Terminating {script_name} process: PID {proc.info['pid']}")
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
                message = f"{script_name} stopped successfully ({len(terminated_processes)} processes terminated)"
                
                # Update running state in trading config
                await self.update_strategy_running_state(strategy_id, False)
            else:
                success = True
                message = f"{script_name} was not running"
                
                # Update running state in trading config (ensure it's marked as stopped)
                await self.update_strategy_running_state(strategy_id, False)
            
            # Send response
            if execution_id:
                # For stop_python_script messages
                await websocket.send(json.dumps({
                    'type': 'python_script_stopped',
                    'success': success,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'script_name': script_name,
                    'message': message,
                    'timestamp': datetime.now().isoformat()
                }))
            else:
                # For stop_strategy messages
                await websocket.send(json.dumps({
                    'type': 'strategy_stop_response',
                    'success': success,
                    'strategy_id': strategy_id,
                    'script_name': script_name,
                    'message': message,
                    'timestamp': datetime.now().isoformat()
                }))
            
        except Exception as e:
            error_msg = f'Error stopping {script_name}: {str(e)}'
            logger.error(f"❌ {error_msg}")
            
            if execution_id:
                await websocket.send(json.dumps({
                    'type': 'python_script_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'execution_id': execution_id,
                    'script_name': script_name,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                }))
            else:
                await websocket.send(json.dumps({
                    'type': 'strategy_stop_error',
                    'success': False,
                    'strategy_id': strategy_id,
                    'script_name': script_name,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                }))

    # Strategy Running State Management
    async def update_strategy_running_state(self, strategy_id, is_running):
        """Update the running state of a strategy in the trading configuration"""
        try:
            config_file = 'trading_config_live.json'
            
            logger.info(f"📊 Updating running state for {strategy_id}: is_running={is_running}")
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Ensure strategies section exists
            if 'strategies' not in existing_config:
                existing_config['strategies'] = {}
            
            # Ensure the specific strategy exists
            if strategy_id not in existing_config['strategies']:
                existing_config['strategies'][strategy_id] = {
                    "auto_approve": False,
                    "risk_management": {}
                }
            
            # Update or create running_state section
            if 'running_state' not in existing_config['strategies'][strategy_id]:
                existing_config['strategies'][strategy_id]['running_state'] = {}
            
            # Update the running state
            existing_config['strategies'][strategy_id]['running_state']['is_running'] = is_running
            existing_config['strategies'][strategy_id]['running_state']['last_updated'] = datetime.now().isoformat()
            existing_config['strategies'][strategy_id]['running_state']['updated_by'] = 'websocket_control_handler'
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Successfully updated running state for {strategy_id}: is_running={is_running}")
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error updating running state for {strategy_id}: {e}")

    async def handle_get_strategy_status(self, websocket, client_msg):
        """Handle getting strategy status"""
        try:
            logger.info("🔍 Getting strategy status...")
            
            config_file = 'trading_config_live.json'
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Get status for all strategies
            strategy_status = {}
            for strategy_id in ['iron_condor', 'pml', 'divergence']:
                if 'strategies' in existing_config and strategy_id in existing_config['strategies']:
                    strategy_config = existing_config['strategies'][strategy_id]
                    running_state = strategy_config.get('running_state', {})
                    
                    strategy_status[strategy_id] = {
                        'is_running': running_state.get('is_running', False),
                        'last_updated': running_state.get('last_updated'),
                        'updated_by': running_state.get('updated_by')
                    }
                else:
                    strategy_status[strategy_id] = {
                        'is_running': False,
                        'last_updated': None,
                        'updated_by': None
                    }
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'strategy_status_response',
                'success': True,
                'strategy_status': strategy_status,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting strategy status: {e}")
            await websocket.send(json.dumps({
                'type': 'strategy_status_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    # Market Timing Settings Handlers
    async def handle_timing_settings_save(self, websocket, client_msg):
        """Handle saving market timing settings for a specific strategy to trading_config_live.json"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            settings = client_msg.get('settings', {})
            config_file = 'trading_config_live.json'
            
            logger.info(f"⏰ Saving timing settings for strategy {strategy_id}: {settings}")
            
            if not strategy_id or not settings:
                await websocket.send(json.dumps({
                    'type': 'timing_settings_error',
                    'success': False,
                    'error': 'Strategy ID and settings are required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config or create new one
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {
                    "trading_mode": {
                        "trading_enabled": True,
                        "sandbox_trading": False
                    },
                    "emergency_controls": {
                        "emergency_stop_trading": False,
                        "pause_new_positions": False,
                        "close_all_positions": False
                    },
                    "risk_management": {
                        "account_limits": {
                            "daily_loss_limit": 2,
                            "max_account_risk": 25,
                            "equity_buffer": 10000,
                            "max_positions": 10
                        },
                        "parameter_states": {
                            "enable_max_account_risk": False,
                            "enable_daily_loss_limit": False,
                            "enable_equity_buffer": False,
                            "enable_max_positions": False
                        },
                        "metadata": {
                            "updated_by": "web_interface",
                            "version": "1.1.0"
                        }
                    },
                    "strategies": {}
                }
            
            # Ensure strategies section exists
            if 'strategies' not in existing_config:
                existing_config['strategies'] = {}
            
            # Ensure the specific strategy exists
            if strategy_id not in existing_config['strategies']:
                existing_config['strategies'][strategy_id] = {
                    "auto_approve": False,
                    "auto_timer": False,
                    "risk_management": {},
                    "market_config": {
                        "market_hours_only": True,
                        "market_open": "09:30",
                        "market_close": "16:00"
                    }
                }
            
            # Update or create market_config section
            if 'market_config' not in existing_config['strategies'][strategy_id]:
                existing_config['strategies'][strategy_id]['market_config'] = {
                    "market_hours_only": True,
                    "market_open": "09:30",
                    "market_close": "16:00"
                }
            
            market_config = existing_config['strategies'][strategy_id]['market_config']
            
            # Map the settings from the frontend to the config structure
            if 'start_time' in settings:
                market_config['market_open'] = settings['start_time']
            
            if 'stop_time' in settings:
                market_config['market_close'] = settings['stop_time']
            
            if 'market_hours_only' in settings:
                market_config['market_hours_only'] = settings['market_hours_only']
            
            # Add metadata
            market_config['last_updated'] = datetime.now().isoformat()
            market_config['updated_by'] = 'strategy_manager'
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Timing settings saved successfully for strategy {strategy_id}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'timing_settings_saved',
                'success': True,
                'message': f'Timing settings saved successfully for {strategy_id}',
                'strategy_id': strategy_id,
                'settings': settings,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast config update to all clients
            await self.broadcast_trading_config_update(existing_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving timing settings: {e}")
            await websocket.send(json.dumps({
                'type': 'timing_settings_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    async def handle_timing_settings_get(self, websocket, client_msg):
        """Handle getting market timing settings for a specific strategy from trading_config_live.json"""
        try:
            strategy_id = client_msg.get('strategy_id', '')
            config_file = 'trading_config_live.json'
            
            if not strategy_id:
                await websocket.send(json.dumps({
                    'type': 'timing_settings_error',
                    'success': False,
                    'error': 'Strategy ID is required',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {"strategies": {}}
            
            # Get timing settings for the strategy
            settings = {
                'start_time': '09:30',
                'stop_time': '16:00',
                'market_hours_only': True
            }
            
            if ('strategies' in existing_config and 
                strategy_id in existing_config['strategies'] and 
                'market_config' in existing_config['strategies'][strategy_id]):
                
                market_config = existing_config['strategies'][strategy_id]['market_config']
                
                settings['start_time'] = market_config.get('market_open', '09:30')
                settings['stop_time'] = market_config.get('market_close', '16:00')
                settings['market_hours_only'] = market_config.get('market_hours_only', True)
            
            logger.info(f"📋 Retrieved timing settings for {strategy_id}: {settings}")
            
            # Send response
            await websocket.send(json.dumps({
                'type': 'timing_settings_data',
                'success': True,
                'strategy_id': strategy_id,
                'settings': settings,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting timing settings: {e}")
            await websocket.send(json.dumps({
                'type': 'timing_settings_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    # Close All Positions Handler
    async def handle_close_all_positions(self, websocket, client_msg):
        """Handle close all positions request by executing the close_all_positions_api.py script"""
        try:
            logger.info("🔍 DEBUG: handle_close_all_positions called")
            logger.info(f"🔍 DEBUG: client_msg received: {client_msg}")
            
            reason = client_msg.get('reason', 'User requested close all positions')
            timestamp = client_msg.get('timestamp', 'No timestamp provided')
            
            logger.info(f"🚨 DEBUG: Close all positions requested at {timestamp}")
            logger.info(f"🚨 DEBUG: Reason: {reason}")
            
            # Execute the close all positions API script
            logger.info("🐍 DEBUG: About to execute close_all_positions_api.py script...")
            logger.info(f"🐍 DEBUG: Current working directory: {os.getcwd()}")
            logger.info(f"🐍 DEBUG: Script exists: {os.path.exists('close_all_positions_api.py')}")
            
            close_process = subprocess.Popen([
                'python3', 'close_all_positions_api.py'
            ], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            cwd=os.getcwd(),
            text=True
            )
            
            # Wait for the process to complete and get the output
            stdout, stderr = close_process.communicate(timeout=120)  # 2 minute timeout
            
            if close_process.returncode == 0:
                # Parse the JSON output from the script
                try:
                    result = json.loads(stdout)
                    logger.info(f"✅ Close all positions completed successfully: {result}")
                    
                    # Send success response with the script results
                    await websocket.send(json.dumps({
                        'type': 'close_all_positions_response',
                        'success': True,
                        'orders_cancelled': result.get('orders_cancelled', 0),
                        'orders_cancel_failed': result.get('orders_cancel_failed', 0),
                        'positions_closed': result.get('positions_closed', 0),
                        'positions_close_failed': result.get('positions_close_failed', 0),
                        'cancelled_orders': result.get('cancelled_orders', []),
                        'closed_positions': result.get('closed_positions', []),
                        'execution_time': result.get('execution_time'),
                        'message': result.get('message', 'Close all positions completed'),
                        'timestamp': datetime.now().isoformat()
                    }))
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse script output as JSON: {e}")
                    logger.error(f"Script stdout: {stdout}")
                    
                    await websocket.send(json.dumps({
                        'type': 'close_all_positions_response',
                        'success': False,
                        'error': f'Failed to parse script output: {str(e)}',
                        'script_output': stdout,
                        'timestamp': datetime.now().isoformat()
                    }))
            else:
                # Script failed
                logger.error(f"❌ Close all positions script failed with return code {close_process.returncode}")
                logger.error(f"Script stderr: {stderr}")
                
                # Try to parse error output as JSON
                error_result = None
                try:
                    if stdout:
                        error_result = json.loads(stdout)
                except json.JSONDecodeError:
                    pass
                
                if error_result:
                    await websocket.send(json.dumps({
                        'type': 'close_all_positions_response',
                        'success': False,
                        'error': error_result.get('error', 'Script execution failed'),
                        'script_output': stdout,
                        'script_error': stderr,
                        'timestamp': datetime.now().isoformat()
                    }))
                else:
                    await websocket.send(json.dumps({
                        'type': 'close_all_positions_response',
                        'success': False,
                        'error': f'Script execution failed with return code {close_process.returncode}',
                        'script_output': stdout,
                        'script_error': stderr,
                        'timestamp': datetime.now().isoformat()
                    }))
            
        except subprocess.TimeoutExpired:
            logger.error("❌ Close all positions script timed out")
            close_process.kill()
            await websocket.send(json.dumps({
                'type': 'close_all_positions_response',
                'success': False,
                'error': 'Script execution timed out (2 minutes)',
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error executing close all positions: {e}")
            await websocket.send(json.dumps({
                'type': 'close_all_positions_response',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))

    # Trading Statistics Handler
    async def handle_request_trading_statistics(self, websocket, client_msg):
        """Handle request for trading statistics with specific time period - updates config only"""
        try:
            time_period = client_msg.get('time_period', '7days')
            timestamp = client_msg.get('timestamp', datetime.now().isoformat())
            
            logger.info(f"📊 Processing trading statistics request for time period: {time_period}")
            
            # Map time periods to days for transaction handler lookback
            time_period_mapping = {
                'today': 1,
                '7days': 7,
                '1month': 30,
                '3months': 90,
                '6months': 180,
                '1year': 365,
                'all': 1095  # 3 years for "all time"
            }
            
            lookback_days = time_period_mapping.get(time_period, 30)  # Default to 30 days
            
            # Update the transaction handler lookback period in trading config
            success = await self.update_transaction_handler_lookback(lookback_days)
            
            if success:
                logger.info(f"✅ Updated transaction handler lookback for {time_period} ({lookback_days} days)")
                
                # Send success response
                await websocket.send(json.dumps({
                    'type': 'trading_statistics_response',
                    'success': True,
                    'time_period': time_period,
                    'lookback_days': lookback_days,
                    'message': f'Transaction handler lookback updated for {time_period}',
                    'timestamp': datetime.now().isoformat()
                }))
                
            else:
                logger.error(f"❌ Failed to update transaction handler lookback for {time_period}")
                
                await websocket.send(json.dumps({
                    'type': 'trading_statistics_error',
                    'success': False,
                    'error': 'Failed to update transaction handler lookback period',
                    'time_period': time_period,
                    'timestamp': datetime.now().isoformat()
                }))
            
        except Exception as e:
            logger.error(f"❌ Error handling trading statistics request: {e}")
            await websocket.send(json.dumps({
                'type': 'trading_statistics_error',
                'success': False,
                'error': str(e),
                'time_period': client_msg.get('time_period', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }))

    async def update_transaction_handler_lookback(self, lookback_days):
        """Update the transaction handler lookback period in trading config"""
        try:
            config_file = 'trading_config_live.json'
            
            logger.info(f"📊 Updating transaction handler lookback to {lookback_days} days")
            
            # Load existing config
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    existing_config = json.load(f)
            else:
                existing_config = {}
            
            # Ensure transaction_handler_lookback section exists
            if 'transaction_handler_lookback' not in existing_config:
                existing_config['transaction_handler_lookback'] = {}
            
            # Update the lookback period
            existing_config['transaction_handler_lookback']['lookback_period_days'] = lookback_days
            existing_config['transaction_handler_lookback']['last_updated'] = datetime.now().isoformat()
            existing_config['transaction_handler_lookback']['updated_by'] = 'analytics_manager'
            
            # Save updated configuration
            with open(config_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"✅ Successfully updated transaction handler lookback to {lookback_days} days")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating transaction handler lookback: {e}")
            return False
