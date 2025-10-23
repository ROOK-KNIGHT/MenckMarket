"""
API Connection Status WebSocket Handler
Streams real-time status of API token connections using api_status_exporter
"""

import asyncio
import json
import logging
from datetime import datetime
import websockets
from typing import Dict, Any, Set
import os
import sys
from pathlib import Path

# Add the parent directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

try:
    from api_status_exporter import APIStatusExporter
    print("✅ API Status Exporter imported successfully")
except ImportError as e:
    print(f"❌ Failed to import API Status Exporter: {e}")
    APIStatusExporter = None

logger = logging.getLogger(__name__)

class APIConnectionHandler:
    def __init__(self):
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.api_status_data = {}
        self.monitoring = False
        self.monitor_task = None
        self.status_exporter = APIStatusExporter() if APIStatusExporter else None
        
        if not self.status_exporter:
            logger.error("API Status Exporter not available - falling back to basic status")
        
    async def register_client(self, websocket: websockets.WebSocketServerProtocol):
        """Register a new WebSocket client"""
        self.clients.add(websocket)
        logger.info(f"API connection client registered. Total clients: {len(self.clients)}")
        
        # Send current status to new client
        if self.api_status_data:
            await self.send_to_client(websocket, {
                'type': 'api_status_update',
                'data': self.api_status_data,
                'timestamp': datetime.now().isoformat()
            })
        else:
            # Get initial status
            await self.get_api_status()
        
        # Start monitoring if this is the first client
        if len(self.clients) == 1 and not self.monitoring:
            await self.start_monitoring()
    
    async def unregister_client(self, websocket: websockets.WebSocketServerProtocol):
        """Unregister a WebSocket client"""
        self.clients.discard(websocket)
        logger.info(f"API connection client unregistered. Total clients: {len(self.clients)}")
        
        # Stop monitoring if no clients remain
        if len(self.clients) == 0 and self.monitoring:
            await self.stop_monitoring()
    
    async def send_to_client(self, websocket: websockets.WebSocketServerProtocol, message: Dict[str, Any]):
        """Send message to a specific client"""
        try:
            await websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            await self.unregister_client(websocket)
        except Exception as e:
            logger.error(f"Error sending message to client: {e}")
    
    async def broadcast_to_clients(self, message: Dict[str, Any]):
        """Broadcast message to all connected clients"""
        if not self.clients:
            return
            
        # Create a copy of clients to avoid modification during iteration
        clients_copy = self.clients.copy()
        
        for client in clients_copy:
            await self.send_to_client(client, message)
    
    async def get_api_status(self):
        """Get API status using the status exporter"""
        try:
            if not self.status_exporter:
                logger.error("Status exporter not available")
                return
            
            # Run the status export in a thread to avoid blocking
            loop = asyncio.get_event_loop()
            status_data = await loop.run_in_executor(None, self.status_exporter.export_status)
            
            # Update our cached data
            self.api_status_data = status_data
            
            # Broadcast to all clients
            await self.broadcast_to_clients({
                'type': 'api_status_update',
                'data': status_data,
                'timestamp': datetime.now().isoformat()
            })
            
            logger.info(f"API status updated - Auth: {status_data.get('authentication', {}).get('status', 'unknown')}, Schwab: {status_data.get('connections', {}).get('schwab', {}).get('status', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Error getting API status: {e}")
            
            # Send error status to clients
            error_status = {
                'authentication': {'status': 'error', 'authenticated': False},
                'connections': {'schwab': {'status': 'error', 'connected': False}},
                'frontend_fields': {
                    'auth-status': 'Error',
                    'schwab-status': 'Error',
                    'auth-indicator': 'error',
                    'schwab-indicator': 'error'
                },
                'error': str(e)
            }
            
            await self.broadcast_to_clients({
                'type': 'api_status_update',
                'data': error_status,
                'timestamp': datetime.now().isoformat()
            })
    
    async def start_monitoring(self):
        """Start monitoring API connections"""
        if self.monitoring:
            return
            
        self.monitoring = True
        logger.info("Starting API connection monitoring")
        
        # Initial check
        await self.get_api_status()
        
        # Start monitoring task
        self.monitor_task = asyncio.create_task(self.monitor_loop())
    
    async def stop_monitoring(self):
        """Stop monitoring API connections"""
        if not self.monitoring:
            return
            
        self.monitoring = False
        logger.info("Stopping API connection monitoring")
        
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            self.monitor_task = None
    
    async def monitor_loop(self):
        """Main monitoring loop"""
        try:
            while self.monitoring:
                await self.get_api_status()
                await asyncio.sleep(30)  # Check every 30 seconds
        except asyncio.CancelledError:
            logger.info("API connection monitoring cancelled")
        except Exception as e:
            logger.error(f"Error in API connection monitoring loop: {e}")
    
    async def handle_message(self, websocket: websockets.WebSocketServerProtocol, message: Dict[str, Any]):
        """Handle incoming WebSocket messages"""
        try:
            msg_type = message.get('type')
            
            if msg_type == 'get_status':
                # Send current status
                if self.api_status_data:
                    await self.send_to_client(websocket, {
                        'type': 'api_status_update',
                        'data': self.api_status_data,
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    await self.get_api_status()
            
            elif msg_type == 'refresh_status':
                # Force refresh of connection status
                await self.get_api_status()
            
            elif msg_type == 'test_connection':
                # Test connection by refreshing status
                await self.get_api_status()
            
            else:
                logger.warning(f"Unknown message type: {msg_type}")
                
        except Exception as e:
            logger.error(f"Error handling API connection message: {e}")
            await self.send_to_client(websocket, {
                'type': 'error',
                'message': f'Error processing request: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })

# Global handler instance
api_connection_handler = APIConnectionHandler()

async def handle_api_connection_websocket(websocket, path):
    """WebSocket handler for API connection status"""
    await api_connection_handler.register_client(websocket)
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                await api_connection_handler.handle_message(websocket, data)
            except json.JSONDecodeError:
                logger.error("Invalid JSON received")
                await api_connection_handler.send_to_client(websocket, {
                    'type': 'error',
                    'message': 'Invalid JSON format',
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await api_connection_handler.send_to_client(websocket, {
                    'type': 'error',
                    'message': f'Error processing message: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                })
    
    except websockets.exceptions.ConnectionClosed:
        logger.info("API connection WebSocket client disconnected")
    except Exception as e:
        logger.error(f"Error in API connection WebSocket handler: {e}")
    finally:
        await api_connection_handler.unregister_client(websocket)
