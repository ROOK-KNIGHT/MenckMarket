#!/usr/bin/env python3
"""
Simple API server for watchlist management
Bridges web interface with symbols monitor handler
"""

import json
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import time

from symbols_monitor_handler import SymbolsMonitorHandler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WatchlistAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for watchlist API"""
    
    def __init__(self, *args, symbols_monitor=None, **kwargs):
        self.symbols_monitor = symbols_monitor
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/api/watchlist/status':
            self.handle_get_status()
        elif path == '/api/watchlist/symbols':
            self.handle_get_symbols()
        elif path == '/api/watchlist/live-monitor-status':
            self.handle_get_live_monitor_status()
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/api/watchlist/add':
            self.handle_add_symbol()
        elif path == '/api/watchlist/remove':
            self.handle_remove_symbol()
        else:
            self.send_error(404, "Not Found")
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS"""
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()
    
    def send_cors_headers(self):
        """Send CORS headers"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
    
    def send_json_response(self, data, status_code=200):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_cors_headers()
        self.end_headers()
        
        json_data = json.dumps(data, indent=2, default=str)
        self.wfile.write(json_data.encode('utf-8'))
    
    def handle_get_status(self):
        """Handle GET /api/watchlist/status"""
        try:
            symbols = self.symbols_monitor.get_watchlist_symbols()
            watchlist_data = self.symbols_monitor.get_watchlist_data()
            live_monitor_status = self.symbols_monitor.get_live_monitor_status()
            
            response = {
                'success': True,
                'symbols': symbols,
                'watchlist_data': watchlist_data,
                'total_symbols': len(symbols),
                'live_monitor_status': live_monitor_status,
                'data_source': 'live_monitor.json',
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_json_response(response)
            
        except Exception as e:
            logger.error(f"Error getting watchlist status: {e}")
            self.send_json_response({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500)
    
    def handle_get_symbols(self):
        """Handle GET /api/watchlist/symbols"""
        try:
            symbols = self.symbols_monitor.get_watchlist_symbols()
            
            response = {
                'success': True,
                'symbols': symbols,
                'count': len(symbols),
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_json_response(response)
            
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            self.send_json_response({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500)
    
    def handle_get_live_monitor_status(self):
        """Handle GET /api/watchlist/live-monitor-status"""
        try:
            status = self.symbols_monitor.get_live_monitor_status()
            
            response = {
                'success': True,
                'live_monitor_status': status,
                'timestamp': datetime.now().isoformat()
            }
            
            self.send_json_response(response)
            
        except Exception as e:
            logger.error(f"Error getting live monitor status: {e}")
            self.send_json_response({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500)
    
    def handle_add_symbol(self):
        """Handle POST /api/watchlist/add"""
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            # Parse JSON data
            data = json.loads(post_data.decode('utf-8'))
            symbol = data.get('symbol', '').upper().strip()
            
            if not symbol:
                self.send_json_response({
                    'success': False,
                    'error': 'Symbol is required',
                    'timestamp': datetime.now().isoformat()
                }, 400)
                return
            
            # Add symbol to API-managed watchlist only
            result = self._add_symbol_to_api_watchlist(symbol)
            
            if result['success']:
                logger.info(f"Successfully added {symbol} to API watchlist")
            else:
                logger.warning(f"Failed to add {symbol} to API watchlist: {result.get('error')}")
            
            self.send_json_response(result)
            
        except json.JSONDecodeError:
            self.send_json_response({
                'success': False,
                'error': 'Invalid JSON data',
                'timestamp': datetime.now().isoformat()
            }, 400)
        except Exception as e:
            logger.error(f"Error adding symbol: {e}")
            self.send_json_response({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500)
    
    def handle_remove_symbol(self):
        """Handle POST /api/watchlist/remove"""
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            # Parse JSON data
            data = json.loads(post_data.decode('utf-8'))
            symbol = data.get('symbol', '').upper().strip()
            
            if not symbol:
                self.send_json_response({
                    'success': False,
                    'error': 'Symbol is required',
                    'timestamp': datetime.now().isoformat()
                }, 400)
                return
            
            # Remove symbol from API-managed watchlist only
            result = self._remove_symbol_from_api_watchlist(symbol)
            
            if result['success']:
                logger.info(f"Successfully removed {symbol} from API watchlist")
            else:
                logger.warning(f"Failed to remove {symbol} from API watchlist: {result.get('error')}")
            
            self.send_json_response(result)
            
        except json.JSONDecodeError:
            self.send_json_response({
                'success': False,
                'error': 'Invalid JSON data',
                'timestamp': datetime.now().isoformat()
            }, 400)
        except Exception as e:
            logger.error(f"Error removing symbol: {e}")
            self.send_json_response({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500)
    
    def _add_symbol_to_api_watchlist(self, symbol: str) -> dict:
        """Add symbol to API-managed watchlist JSON file"""
        try:
            symbol = symbol.upper().strip()
            
            # Load existing API watchlist
            try:
                with open('api_watchlist.json', 'r') as f:
                    api_data = json.load(f)
            except FileNotFoundError:
                # Create new structure if file doesn't exist
                api_data = {
                    'watchlist': {
                        'symbols': [],
                        'metadata': {
                            'created': datetime.now().isoformat(),
                            'last_updated': datetime.now().isoformat(),
                            'total_symbols': 0,
                            'managed_by': 'watchlist_api_server'
                        }
                    }
                }
            
            # Ensure watchlist structure exists
            if 'watchlist' not in api_data:
                api_data['watchlist'] = {
                    'symbols': [],
                    'metadata': {
                        'created': datetime.now().isoformat(),
                        'last_updated': datetime.now().isoformat(),
                        'total_symbols': 0,
                        'managed_by': 'watchlist_api_server'
                    }
                }
            
            # Check if symbol already exists
            current_symbols = api_data['watchlist'].get('symbols', [])
            if symbol in current_symbols:
                return {
                    'success': False,
                    'error': f'Symbol {symbol} already exists in API watchlist',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Add symbol to symbols list
            current_symbols.append(symbol)
            api_data['watchlist']['symbols'] = current_symbols
            
            # Update metadata
            api_data['watchlist']['metadata'] = {
                'created': api_data['watchlist']['metadata'].get('created', datetime.now().isoformat()),
                'last_updated': datetime.now().isoformat(),
                'total_symbols': len(current_symbols),
                'managed_by': 'watchlist_api_server'
            }
            
            # Save updated data
            with open('api_watchlist.json', 'w') as f:
                json.dump(api_data, f, indent=2)
            
            logger.info(f"✅ Added {symbol} to API watchlist (api_watchlist.json)")
            
            return {
                'success': True,
                'symbol': symbol,
                'message': f'Successfully added {symbol} to API watchlist',
                'total_symbols': len(current_symbols),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error adding {symbol} to API watchlist: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _remove_symbol_from_api_watchlist(self, symbol: str) -> dict:
        """Remove symbol from API-managed watchlist JSON file"""
        try:
            symbol = symbol.upper().strip()
            
            # Load existing API watchlist
            try:
                with open('api_watchlist.json', 'r') as f:
                    api_data = json.load(f)
            except FileNotFoundError:
                return {
                    'success': False,
                    'error': 'api_watchlist.json not found',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Check if watchlist exists
            if 'watchlist' not in api_data:
                return {
                    'success': False,
                    'error': 'watchlist not found in api_watchlist.json',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Check if symbol exists
            current_symbols = api_data['watchlist'].get('symbols', [])
            if symbol not in current_symbols:
                return {
                    'success': False,
                    'error': f'Symbol {symbol} not found in API watchlist',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Remove symbol from symbols list
            current_symbols.remove(symbol)
            api_data['watchlist']['symbols'] = current_symbols
            
            # Update metadata
            api_data['watchlist']['metadata'] = {
                'created': api_data['watchlist']['metadata'].get('created', datetime.now().isoformat()),
                'last_updated': datetime.now().isoformat(),
                'total_symbols': len(current_symbols),
                'managed_by': 'watchlist_api_server'
            }
            
            # Save updated data
            with open('api_watchlist.json', 'w') as f:
                json.dump(api_data, f, indent=2)
            
            logger.info(f"🗑️ Removed {symbol} from API watchlist (api_watchlist.json)")
            
            return {
                'success': True,
                'symbol': symbol,
                'message': f'Successfully removed {symbol} from API watchlist',
                'total_symbols': len(current_symbols),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error removing {symbol} from API watchlist: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _remove_symbol_from_live_monitor_direct(self, symbol: str) -> dict:
        """Remove symbol directly from live_monitor.json integrated_watchlist"""
        try:
            symbol = symbol.upper().strip()
            
            # Load existing live_monitor.json
            try:
                with open('live_monitor.json', 'r') as f:
                    live_data = json.load(f)
            except FileNotFoundError:
                return {
                    'success': False,
                    'error': 'live_monitor.json not found',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Check if integrated_watchlist exists
            if 'integrated_watchlist' not in live_data:
                return {
                    'success': False,
                    'error': 'integrated_watchlist not found in live_monitor.json',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Check if symbol exists
            current_symbols = live_data['integrated_watchlist'].get('symbols', [])
            if symbol not in current_symbols:
                return {
                    'success': False,
                    'error': f'Symbol {symbol} not found in watchlist',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Remove symbol from symbols list
            current_symbols.remove(symbol)
            live_data['integrated_watchlist']['symbols'] = current_symbols
            
            # Remove symbol data from watchlist_data
            if symbol in live_data['integrated_watchlist'].get('watchlist_data', {}):
                del live_data['integrated_watchlist']['watchlist_data'][symbol]
            
            # Update metadata
            live_data['integrated_watchlist']['metadata'] = {
                'total_symbols': len(current_symbols),
                'last_updated': datetime.now().isoformat(),
                'update_source': 'api_server_direct'
            }
            
            # Update main metadata
            live_data['metadata']['timestamp'] = datetime.now().isoformat()
            live_data['metadata']['symbols_monitored'] = current_symbols
            
            # Save updated data
            with open('live_monitor.json', 'w') as f:
                json.dump(live_data, f, indent=2)
            
            logger.info(f"🗑️ Removed {symbol} directly from live_monitor.json integrated_watchlist")
            
            return {
                'success': True,
                'symbol': symbol,
                'message': f'Successfully removed {symbol} from integrated watchlist',
                'total_symbols': len(current_symbols),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error removing {symbol} directly from live monitor: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.info(f"{self.address_string()} - {format % args}")

class WatchlistAPIServer:
    """Simple API server for watchlist management"""
    
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.symbols_monitor = SymbolsMonitorHandler()
        self.server = None
        self.server_thread = None
        self.running = False
    
    def create_handler(self):
        """Create request handler with symbols monitor instance"""
        def handler(*args, **kwargs):
            return WatchlistAPIHandler(*args, symbols_monitor=self.symbols_monitor, **kwargs)
        return handler
    
    def start(self):
        """Start the API server"""
        try:
            # Create HTTP server
            handler_class = self.create_handler()
            self.server = HTTPServer((self.host, self.port), handler_class)
            
            # Start server in a separate thread
            self.server_thread = threading.Thread(target=self._run_server)
            self.server_thread.daemon = True
            self.server_thread.start()
            
            self.running = True
            logger.info(f"Watchlist API server started on http://{self.host}:{self.port}")
            logger.info("Available endpoints:")
            logger.info("  GET  /api/watchlist/status - Get watchlist status")
            logger.info("  GET  /api/watchlist/symbols - Get watchlist symbols")
            logger.info("  GET  /api/watchlist/live-monitor-status - Get live monitor status")
            logger.info("  POST /api/watchlist/add - Add symbol to watchlist")
            logger.info("  POST /api/watchlist/remove - Remove symbol from watchlist")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start API server: {e}")
            return False
    
    def _run_server(self):
        """Run the server (internal method)"""
        try:
            self.server.serve_forever()
        except Exception as e:
            logger.error(f"Server error: {e}")
    
    def stop(self):
        """Stop the API server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.running = False
            logger.info("Watchlist API server stopped")
    
    def is_running(self):
        """Check if server is running"""
        return self.running

def main():
    """Main function to run the API server"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Watchlist API Server')
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=8080, help='Server port (default: 8080)')
    
    args = parser.parse_args()
    
    # Create and start server
    server = WatchlistAPIServer(host=args.host, port=args.port)
    
    if server.start():
        try:
            # Keep server running
            while server.is_running():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            server.stop()
    else:
        logger.error("Failed to start server")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
