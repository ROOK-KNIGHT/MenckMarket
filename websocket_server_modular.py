#!/usr/bin/env python3
"""
Modular WebSocket Server
Main coordinator that manages PostgreSQL streaming and control operations
"""

import asyncio
import websockets
import json
import subprocess
import os
from datetime import datetime
import logging
from typing import Dict, List, Any

# Import modular handlers
from websocket_handlers.data_stream_handler import DataStreamHandler
from websocket_handlers.control_handler import ControlHandler
from websocket_handlers.alerts_handler import AlertsHandler
from websocket_handlers.api_connection_handler import handle_api_connection_websocket

# Import the database query handler
from db_query_handler import DatabaseQueryHandler

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ModularWebSocketServer:
    """Modular WebSocket server that coordinates between data streaming and control operations"""
    
    def __init__(self, port=8765):
        """Initialize the modular WebSocket server"""
        self.port = port
        self.clients = set()
        self.running = False
        
        # Initialize database query handler
        self.db_query_handler = DatabaseQueryHandler()
        
        # Initialize handlers with broadcast callback
        self.data_handler = DataStreamHandler(self.broadcast_data)
        self.control_handler = ControlHandler(self.broadcast_message, self.db_query_handler)
        self.alerts_handler = AlertsHandler(self.broadcast_message, self.db_query_handler)
    
    async def handle_close_all_positions(self, websocket, client_msg):
        """Handle close all positions request directly in main server"""
        try:
            logger.info("🔍 DEBUG: handle_close_all_positions called in main server")
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
            
            logger.info("🐍 DEBUG: Process started, waiting for completion...")
            
            # Wait for the process to complete and get the output
            stdout, stderr = close_process.communicate(timeout=120)  # 2 minute timeout
            
            logger.info(f"🐍 DEBUG: Process completed with return code: {close_process.returncode}")
            logger.info(f"🐍 DEBUG: stdout length: {len(stdout) if stdout else 0}")
            logger.info(f"🐍 DEBUG: stderr length: {len(stderr) if stderr else 0}")
            
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
                logger.error(f"❌ Error sending data to client: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        if disconnected_clients:
            logger.info(f"🔌 Removed {len(disconnected_clients)} disconnected clients")
    
    async def broadcast_message(self, message_dict):
        """Broadcast control message to all connected clients"""
        if not self.clients:
            return
        
        message = json.dumps(message_dict, default=str)
        disconnected_clients = set()
        
        for client in self.clients.copy():
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"❌ Error broadcasting message to client: {e}")
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        self.clients -= disconnected_clients
        
        if disconnected_clients:
            logger.info(f"🔌 Removed {len(disconnected_clients)} disconnected clients during broadcast")
    
    async def handle_client(self, websocket):
        """Handle new WebSocket client connection"""
        client_addr = websocket.remote_address
        logger.info(f"🔗 New client connected: {client_addr}")
        
        # Add client to set
        self.clients.add(websocket)
        
        try:
            # Send initial data immediately
            initial_data = self.data_handler.get_initial_data()
            await websocket.send(json.dumps(initial_data, default=str))
            
            # Keep connection alive and handle messages
            async for message in websocket:
                try:
                    # Parse client message
                    client_msg = json.loads(message)
                    message_type = client_msg.get('type')
                    
                    # Handle basic connection messages
                    if message_type == 'ping':
                        # Respond to ping
                        await websocket.send(json.dumps({
                            'type': 'pong',
                            'timestamp': datetime.now().isoformat()
                        }))
                    
                    elif message_type == 'request_data':
                        # Send latest data
                        fresh_data = self.data_handler.get_latest_data()
                        await websocket.send(json.dumps(fresh_data, default=str))
                    
                    elif message_type == 'subscribe':
                        # Handle subscription to specific data types
                        data_types = client_msg.get('data_types', [])
                        logger.info(f"📡 Client {client_addr} subscribed to: {data_types}")
                        
                        # Send acknowledgment
                        await websocket.send(json.dumps({
                            'type': 'subscription_ack',
                            'subscribed_to': data_types,
                            'timestamp': datetime.now().isoformat()
                        }))
                    
                    elif message_type == 'close_all_positions':
                        # Handle close all positions directly in main server
                        logger.info(f"🚨 Received close_all_positions message from {client_addr}")
                        await self.handle_close_all_positions(websocket, client_msg)
                    
                    elif message_type in ['get_status', 'refresh_status', 'test_connection']:
                        # Route API connection messages to API connection handler
                        logger.info(f"🔌 Routing API connection message {message_type} to handler")
                        from websocket_handlers.api_connection_handler import api_connection_handler
                        
                        # Register client with API connection handler if not already registered
                        if websocket not in api_connection_handler.clients:
                            await api_connection_handler.register_client(websocket)
                        
                        await api_connection_handler.handle_message(websocket, client_msg)
                    
                    elif message_type in [
                        'save_alerts_config', 'get_alerts_config', 'test_notifications',
                        'update_all_settings', 'trigger_alert', 'get_alert_history',
                        'clear_alert_history', 'change_password', 'setup_2fa',
                        'get_login_history', 'logout'
                    ]:
                        # Route alerts and settings messages to alerts handler
                        logger.info(f"🔔 Routing alerts message {message_type} to alerts handler")
                        handled = await self.alerts_handler.handle_message(websocket, client_msg, client_addr)
                        
                        if not handled:
                            logger.warning(f"⚠️ Alerts handler could not process message type: {message_type}")
                    
                    else:
                        # Route all other messages to control handler
                        handled = await self.control_handler.handle_message(websocket, client_msg, client_addr)
                        
                        if not handled:
                            logger.warning(f"⚠️ Unhandled message type: {message_type} from {client_addr}")
                
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
            
            # Also unregister from API connection handler if registered
            try:
                from websocket_handlers.api_connection_handler import api_connection_handler
                if websocket in api_connection_handler.clients:
                    await api_connection_handler.unregister_client(websocket)
            except Exception as e:
                logger.error(f"Error unregistering client from API handler: {e}")
            
            logger.info(f"👋 Client {client_addr} removed. Active clients: {len(self.clients)}")
    
    async def start_server(self):
        """Start the modular WebSocket server"""
        logger.info(f"🚀 Starting Modular WebSocket Server on port {self.port}")
        
        try:
            # Start data polling
            self.data_handler.start_data_polling()
            
            # Start WebSocket server
            server = await websockets.serve(
                self.handle_client,
                "localhost",
                self.port,
                ping_interval=30,
                ping_timeout=10
            )
            
            logger.info(f"✅ Modular WebSocket server running on ws://localhost:{self.port}")
            logger.info("📡 PostgreSQL data streaming: ACTIVE")
            logger.info("🎛️ Control operations: ACTIVE")
            logger.info("🔔 Alerts & notifications: ACTIVE")
            logger.info("🔄 Database polling every 3 seconds")
            
            # Keep server running
            await server.wait_closed()
            
        except Exception as e:
            logger.error(f"❌ Error starting modular WebSocket server: {e}")
            raise
        finally:
            self.running = False
            self.data_handler.stop()
            logger.info("🔌 Modular WebSocket server stopped")
    
    def stop(self):
        """Stop the server and cleanup"""
        logger.info("🛑 Stopping Modular WebSocket server...")
        self.running = False
        self.data_handler.stop()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Modular PostgreSQL WebSocket Server')
    parser.add_argument('--port', type=int, default=8765, help='WebSocket port (default: 8765)')
    
    args = parser.parse_args()
    
    # Create and start modular WebSocket server
    server = ModularWebSocketServer(args.port)
    
    try:
        # Run the WebSocket server
        asyncio.run(server.start_server())
    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt")
    finally:
        server.stop()
        logger.info("👋 Modular WebSocket server stopped")

if __name__ == "__main__":
    main()
