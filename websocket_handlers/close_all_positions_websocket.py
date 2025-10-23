#!/usr/bin/env python3
"""
Close All Positions WebSocket Server
Dedicated WebSocket server for handling close all positions requests
"""

import asyncio
import websockets
import json
import subprocess
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CloseAllPositionsWebSocketServer:
    """Dedicated WebSocket server for close all positions functionality"""
    
    def __init__(self, port=8766):
        """Initialize the close all positions WebSocket server"""
        self.port = port
        self.clients = set()
        self.running = False
    
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
    
    async def handle_client(self, websocket):
        """Handle new WebSocket client connection"""
        client_addr = websocket.remote_address
        logger.info(f"🔗 New close all positions client connected: {client_addr}")
        
        # Add client to set
        self.clients.add(websocket)
        
        try:
            # Send connection acknowledgment
            await websocket.send(json.dumps({
                'type': 'connection_ack',
                'message': 'Connected to Close All Positions WebSocket Server',
                'timestamp': datetime.now().isoformat()
            }))
            
            # Keep connection alive and handle messages
            async for message in websocket:
                try:
                    # Parse client message
                    client_msg = json.loads(message)
                    message_type = client_msg.get('type')
                    
                    logger.info(f"📨 Received message type: {message_type} from {client_addr}")
                    
                    # Handle ping messages
                    if message_type == 'ping':
                        await websocket.send(json.dumps({
                            'type': 'pong',
                            'timestamp': datetime.now().isoformat()
                        }))
                    
                    # Handle close all positions requests
                    elif message_type == 'close_all_positions':
                        logger.info(f"🚨 Processing close all positions request from {client_addr}")
                        await self.handle_close_all_positions(websocket, client_msg)
                    
                    else:
                        logger.warning(f"⚠️ Unknown message type: {message_type} from {client_addr}")
                        await websocket.send(json.dumps({
                            'type': 'error',
                            'error': f'Unknown message type: {message_type}',
                            'timestamp': datetime.now().isoformat()
                        }))
                
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ Invalid JSON from client {client_addr}: {message}")
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'error': 'Invalid JSON message',
                        'timestamp': datetime.now().isoformat()
                    }))
                except Exception as e:
                    logger.error(f"❌ Error handling message from {client_addr}: {e}")
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    }))
        
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"🔌 Close all positions client {client_addr} disconnected")
        except Exception as e:
            logger.error(f"❌ Error with close all positions client {client_addr}: {e}")
        finally:
            # Remove client from set
            self.clients.discard(websocket)
            logger.info(f"👋 Close all positions client {client_addr} removed. Active clients: {len(self.clients)}")
    
    async def start_server(self):
        """Start the close all positions WebSocket server"""
        logger.info(f"🚀 Starting Close All Positions WebSocket Server on port {self.port}")
        
        try:
            # Start WebSocket server
            server = await websockets.serve(
                self.handle_client,
                "localhost",
                self.port,
                ping_interval=30,
                ping_timeout=10
            )
            
            logger.info(f"✅ Close All Positions WebSocket server running on ws://localhost:{self.port}")
            logger.info("🚨 Ready to handle close all positions requests")
            
            # Keep server running
            await server.wait_closed()
            
        except Exception as e:
            logger.error(f"❌ Error starting close all positions WebSocket server: {e}")
            raise
        finally:
            self.running = False
            logger.info("🔌 Close All Positions WebSocket server stopped")
    
    def stop(self):
        """Stop the server and cleanup"""
        logger.info("🛑 Stopping Close All Positions WebSocket server...")
        self.running = False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Close All Positions WebSocket Server')
    parser.add_argument('--port', type=int, default=8766, help='WebSocket port (default: 8766)')
    
    args = parser.parse_args()
    
    # Create and start close all positions WebSocket server
    server = CloseAllPositionsWebSocketServer(args.port)
    
    try:
        # Run the WebSocket server
        asyncio.run(server.start_server())
    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt")
    finally:
        server.stop()
        logger.info("👋 Close All Positions WebSocket server stopped")

if __name__ == "__main__":
    main()
