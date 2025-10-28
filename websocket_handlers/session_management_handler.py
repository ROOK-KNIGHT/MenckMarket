#!/usr/bin/env python3
"""
Session Management Handler
Handles all WebSocket session management operations
"""

import asyncio
import json
from datetime import datetime
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SessionManagementHandler:
    """Handler for all session management operations"""
    
    def __init__(self, broadcast_callback, db_query_handler):
        """Initialize the session management handler"""
        self.broadcast_callback = broadcast_callback
        self.db_query_handler = db_query_handler
        self.trading_config_file = 'trading_config_live.json'
        self.session_settings = None
        self.load_session_settings()
    
    def load_session_settings(self):
        """Load session management settings from trading_config_live.json"""
        try:
            if os.path.exists(self.trading_config_file):
                with open(self.trading_config_file, 'r') as f:
                    config = json.load(f)
                    self.session_settings = config.get('session_management_settings', {})
                logger.info("✅ Session management settings loaded successfully")
            else:
                logger.warning("⚠️ Trading config file not found, using defaults")
                self.session_settings = self.get_default_session_settings()
        except Exception as e:
            logger.error(f"❌ Error loading session settings: {e}")
            self.session_settings = self.get_default_session_settings()
    
    def get_default_session_settings(self):
        """Get default session management settings"""
        return {
            "enable_auto_timeout": True,
            "inactactivity_timeout": 30,
            "current_session": "logged_in",
            "last_updated": datetime.now().isoformat(),
            "updated_by": "system_admin"
        }
    
    async def handle_message(self, websocket, client_msg, client_addr):
        """Route session management messages to appropriate handlers"""
        message_type = client_msg.get('type')
        
        try:
            if message_type == 'save_session_settings':
                await self.handle_session_settings_save(websocket, client_msg)
            
            elif message_type == 'get_session_settings':
                await self.handle_session_settings_get(websocket)
            
            elif message_type == 'update_session_timeout':
                await self.handle_session_timeout_update(websocket, client_msg)
            
            elif message_type == 'toggle_auto_timeout':
                await self.handle_auto_timeout_toggle(websocket, client_msg)
            
            elif message_type == 'extend_session':
                await self.handle_session_extension(websocket, client_msg)
            
            elif message_type == 'get_session_status':
                await self.handle_session_status_get(websocket)
            
            elif message_type == 'force_logout':
                await self.handle_force_logout(websocket, client_msg)
            
            elif message_type == 'session_heartbeat':
                await self.handle_session_heartbeat(websocket, client_msg)
            
            else:
                logger.warning(f"⚠️ Unknown session management message type: {message_type}")
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
    
    # Session Settings Handlers
    async def handle_session_settings_save(self, websocket, client_msg):
        """Handle saving session management settings to trading_config_live.json"""
        try:
            settings_data = client_msg.get('settings', {})
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.info(f"💾 Saving session management settings: {list(settings_data.keys())}")
            
            # Load existing config
            if os.path.exists(self.trading_config_file):
                with open(self.trading_config_file, 'r') as f:
                    config = json.load(f)
            else:
                config = {}
            
            # Update session management settings
            if 'session_management_settings' not in config:
                config['session_management_settings'] = {}
            
            session_config = config['session_management_settings']
            
            # Update individual settings
            if 'enable_auto_timeout' in settings_data:
                session_config['enable_auto_timeout'] = settings_data['enable_auto_timeout']
            
            if 'inactactivity_timeout' in settings_data:
                session_config['inactactivity_timeout'] = int(settings_data['inactactivity_timeout'])
            
            if 'current_session' in settings_data:
                session_config['current_session'] = settings_data['current_session']
            
            # Update metadata
            session_config['last_updated'] = datetime.now().isoformat()
            session_config['updated_by'] = updated_by
            
            # Save updated configuration
            with open(self.trading_config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Update internal settings
            self.session_settings = session_config
            
            logger.info(f"✅ Session management settings saved by {updated_by}")
            
            # Send success response
            await websocket.send(json.dumps({
                'type': 'session_settings_saved',
                'success': True,
                'message': 'Session management settings saved successfully',
                'settings': session_config,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast settings update to all clients
            await self.broadcast_session_settings_update(session_config)
            
        except Exception as e:
            logger.error(f"❌ Error saving session settings: {e}")
            await websocket.send(json.dumps({
                'type': 'session_settings_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_session_settings_get(self, websocket):
        """Handle getting current session management settings"""
        try:
            # Reload settings to ensure we have the latest
            self.load_session_settings()
            
            # Send settings response
            await websocket.send(json.dumps({
                'type': 'session_settings_data',
                'success': True,
                'settings': self.session_settings,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting session settings: {e}")
            await websocket.send(json.dumps({
                'type': 'session_settings_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_session_timeout_update(self, websocket, client_msg):
        """Handle updating session timeout value"""
        try:
            timeout_minutes = client_msg.get('timeout_minutes', 30)
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.info(f"⏰ Updating session timeout to {timeout_minutes} minutes")
            
            # Update settings
            settings_data = {
                'inactactivity_timeout': timeout_minutes
            }
            
            # Use the save handler to update the config
            await self.handle_session_settings_save(websocket, {
                'settings': settings_data,
                'updated_by': updated_by
            })
            
        except Exception as e:
            logger.error(f"❌ Error updating session timeout: {e}")
            await websocket.send(json.dumps({
                'type': 'session_timeout_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_auto_timeout_toggle(self, websocket, client_msg):
        """Handle toggling auto timeout on/off"""
        try:
            enable_auto_timeout = client_msg.get('enable_auto_timeout', True)
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.info(f"🔄 Toggling auto timeout: {enable_auto_timeout}")
            
            # Update settings
            settings_data = {
                'enable_auto_timeout': enable_auto_timeout
            }
            
            # Use the save handler to update the config
            await self.handle_session_settings_save(websocket, {
                'settings': settings_data,
                'updated_by': updated_by
            })
            
        except Exception as e:
            logger.error(f"❌ Error toggling auto timeout: {e}")
            await websocket.send(json.dumps({
                'type': 'auto_timeout_toggle_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_session_extension(self, websocket, client_msg):
        """Handle extending current session"""
        try:
            extension_minutes = client_msg.get('extension_minutes', 30)
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.info(f"⏰ Extending session by {extension_minutes} minutes")
            
            # Update last activity timestamp
            settings_data = {
                'current_session': 'logged_in',
                'last_updated': datetime.now().isoformat()
            }
            
            # Use the save handler to update the config
            await self.handle_session_settings_save(websocket, {
                'settings': settings_data,
                'updated_by': updated_by
            })
            
            # Send specific extension response
            await websocket.send(json.dumps({
                'type': 'session_extended',
                'success': True,
                'message': f'Session extended by {extension_minutes} minutes',
                'extension_minutes': extension_minutes,
                'new_expiry': (datetime.now().timestamp() + (extension_minutes * 60)),
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error extending session: {e}")
            await websocket.send(json.dumps({
                'type': 'session_extension_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_session_status_get(self, websocket):
        """Handle getting current session status"""
        try:
            # Reload settings to ensure we have the latest
            self.load_session_settings()
            
            # Calculate session info
            last_updated = self.session_settings.get('last_updated', datetime.now().isoformat())
            timeout_minutes = self.session_settings.get('inactactivity_timeout', 30)
            enable_auto_timeout = self.session_settings.get('enable_auto_timeout', True)
            current_session = self.session_settings.get('current_session', 'logged_in')
            
            # Calculate time remaining if auto timeout is enabled
            time_remaining = None
            if enable_auto_timeout and last_updated:
                try:
                    last_activity = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    timeout_seconds = timeout_minutes * 60
                    elapsed_seconds = (datetime.now() - last_activity).total_seconds()
                    time_remaining = max(0, timeout_seconds - elapsed_seconds)
                except Exception as e:
                    logger.warning(f"⚠️ Error calculating time remaining: {e}")
            
            session_status = {
                'current_session': current_session,
                'enable_auto_timeout': enable_auto_timeout,
                'timeout_minutes': timeout_minutes,
                'last_activity': last_updated,
                'time_remaining_seconds': time_remaining,
                'is_active': current_session == 'logged_in',
                'will_expire': enable_auto_timeout and time_remaining is not None and time_remaining > 0
            }
            
            # Send status response
            await websocket.send(json.dumps({
                'type': 'session_status_data',
                'success': True,
                'status': session_status,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error getting session status: {e}")
            await websocket.send(json.dumps({
                'type': 'session_status_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_force_logout(self, websocket, client_msg):
        """Handle forcing a logout"""
        try:
            reason = client_msg.get('reason', 'Force logout requested')
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.info(f"🚪 Force logout requested: {reason}")
            
            # Update session to logged out
            settings_data = {
                'current_session': 'logged_out',
                'last_updated': datetime.now().isoformat()
            }
            
            # Use the save handler to update the config
            await self.handle_session_settings_save(websocket, {
                'settings': settings_data,
                'updated_by': updated_by
            })
            
            # Send logout response
            await websocket.send(json.dumps({
                'type': 'force_logout_response',
                'success': True,
                'message': 'User logged out successfully',
                'reason': reason,
                'timestamp': datetime.now().isoformat()
            }))
            
            # Broadcast logout to all clients
            await self.broadcast_callback({
                'type': 'session_logout_broadcast',
                'reason': reason,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Error forcing logout: {e}")
            await websocket.send(json.dumps({
                'type': 'force_logout_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def handle_session_heartbeat(self, websocket, client_msg):
        """Handle session heartbeat to keep session alive"""
        try:
            updated_by = client_msg.get('updated_by', 'web_interface')
            
            logger.debug(f"💓 Session heartbeat received from {updated_by}")
            
            # Update last activity timestamp
            settings_data = {
                'current_session': 'logged_in',
                'last_updated': datetime.now().isoformat()
            }
            
            # Use the save handler to update the config (but don't broadcast this minor update)
            await self.update_session_settings_silent(settings_data, updated_by)
            
            # Send heartbeat acknowledgment
            await websocket.send(json.dumps({
                'type': 'session_heartbeat_ack',
                'success': True,
                'timestamp': datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"❌ Error handling session heartbeat: {e}")
            await websocket.send(json.dumps({
                'type': 'session_heartbeat_error',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def update_session_settings_silent(self, settings_data, updated_by):
        """Update session settings without broadcasting (for heartbeats)"""
        try:
            # Load existing config
            if os.path.exists(self.trading_config_file):
                with open(self.trading_config_file, 'r') as f:
                    config = json.load(f)
            else:
                config = {}
            
            # Update session management settings
            if 'session_management_settings' not in config:
                config['session_management_settings'] = {}
            
            session_config = config['session_management_settings']
            
            # Update settings
            for key, value in settings_data.items():
                session_config[key] = value
            
            # Update metadata
            session_config['updated_by'] = updated_by
            
            # Save updated configuration
            with open(self.trading_config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Update internal settings
            self.session_settings = session_config
            
        except Exception as e:
            logger.error(f"❌ Error updating session settings silently: {e}")
    
    async def broadcast_session_settings_update(self, settings):
        """Broadcast session settings update to all clients"""
        message = {
            'type': 'session_settings_updated',
            'settings': settings,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast_callback(message)
        logger.info("📡 Session settings update broadcasted to all clients")
    
    # Utility methods for session management
    def is_session_expired(self):
        """Check if current session is expired"""
        try:
            if not self.session_settings.get('enable_auto_timeout', True):
                return False
            
            last_updated = self.session_settings.get('last_updated')
            timeout_minutes = self.session_settings.get('inactactivity_timeout', 30)
            
            if not last_updated:
                return True
            
            last_activity = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            timeout_seconds = timeout_minutes * 60
            elapsed_seconds = (datetime.now() - last_activity).total_seconds()
            
            return elapsed_seconds > timeout_seconds
            
        except Exception as e:
            logger.error(f"❌ Error checking session expiry: {e}")
            return True
    
    def get_session_time_remaining(self):
        """Get remaining time in current session"""
        try:
            if not self.session_settings.get('enable_auto_timeout', True):
                return None
            
            last_updated = self.session_settings.get('last_updated')
            timeout_minutes = self.session_settings.get('inactactivity_timeout', 30)
            
            if not last_updated:
                return 0
            
            last_activity = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            timeout_seconds = timeout_minutes * 60
            elapsed_seconds = (datetime.now() - last_activity).total_seconds()
            
            return max(0, timeout_seconds - elapsed_seconds)
            
        except Exception as e:
            logger.error(f"❌ Error calculating session time remaining: {e}")
            return 0
    
    async def check_and_expire_session(self):
        """Check if session should be expired and handle it"""
        try:
            if self.is_session_expired() and self.session_settings.get('current_session') == 'logged_in':
                logger.info("⏰ Session expired due to inactivity")
                
                # Update session to expired
                settings_data = {
                    'current_session': 'expired',
                    'last_updated': datetime.now().isoformat()
                }
                
                await self.update_session_settings_silent(settings_data, 'session_timeout_monitor')
                
                # Broadcast session expiry
                await self.broadcast_callback({
                    'type': 'session_expired',
                    'reason': 'Inactivity timeout',
                    'timestamp': datetime.now().isoformat()
                })
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking session expiry: {e}")
            return False
