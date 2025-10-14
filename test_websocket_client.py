#!/usr/bin/env python3
"""
Python WebSocket Client Test Script
Tests the WebSocket server connection and watchlist functionality directly
"""

import asyncio
import websockets
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebSocketTester:
    def __init__(self, uri="ws://localhost:8765"):
        self.uri = uri
        self.websocket = None
        self.connected = False
        
    async def connect(self):
        """Connect to WebSocket server"""
        try:
            logger.info(f"🔄 Connecting to WebSocket server: {self.uri}")
            self.websocket = await websockets.connect(self.uri)
            self.connected = True
            logger.info("✅ WebSocket connected successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to WebSocket server: {e}")
            self.connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from WebSocket server"""
        if self.websocket:
            await self.websocket.close()
            self.connected = False
            logger.info("🔌 WebSocket disconnected")
    
    async def send_message(self, message):
        """Send a message to the WebSocket server"""
        if not self.connected or not self.websocket:
            logger.error("❌ WebSocket not connected")
            return None
        
        try:
            logger.info(f"📤 Sending message: {message}")
            await self.websocket.send(json.dumps(message))
            
            # Wait for response with timeout
            response = await asyncio.wait_for(self.websocket.recv(), timeout=10.0)
            response_data = json.loads(response)
            logger.info(f"📡 Received response: {response_data}")
            return response_data
            
        except asyncio.TimeoutError:
            logger.error("❌ Timeout waiting for WebSocket response")
            return None
        except Exception as e:
            logger.error(f"❌ Error sending/receiving WebSocket message: {e}")
            return None
    
    async def test_add_symbol(self, symbol):
        """Test adding a symbol to watchlist"""
        logger.info(f"🧪 Testing add symbol: {symbol}")
        
        message = {
            "type": "add_watchlist_symbol",
            "symbol": symbol,
            "timestamp": datetime.now().isoformat()
        }
        
        response = await self.send_message(message)
        
        if response:
            if response.get('type') == 'watchlist_symbol_added' and response.get('success'):
                logger.info(f"✅ Successfully added {symbol} to watchlist")
                return True
            elif response.get('type') == 'watchlist_error':
                logger.error(f"❌ Error adding {symbol}: {response.get('error')}")
                return False
        
        logger.error(f"❌ Failed to add {symbol} - no valid response")
        return False
    
    async def test_remove_symbol(self, symbol):
        """Test removing a symbol from watchlist"""
        logger.info(f"🧪 Testing remove symbol: {symbol}")
        
        message = {
            "type": "remove_watchlist_symbol",
            "symbol": symbol,
            "timestamp": datetime.now().isoformat()
        }
        
        response = await self.send_message(message)
        
        if response:
            if response.get('type') == 'watchlist_symbol_removed' and response.get('success'):
                logger.info(f"✅ Successfully removed {symbol} from watchlist")
                return True
            elif response.get('type') == 'watchlist_error':
                logger.error(f"❌ Error removing {symbol}: {response.get('error')}")
                return False
        
        logger.error(f"❌ Failed to remove {symbol} - no valid response")
        return False
    
    async def test_get_watchlist(self):
        """Test getting current watchlist"""
        logger.info("🧪 Testing get watchlist")
        
        message = {
            "type": "get_watchlist",
            "timestamp": datetime.now().isoformat()
        }
        
        response = await self.send_message(message)
        
        if response:
            if response.get('type') == 'watchlist_data' and response.get('success'):
                watchlist = response.get('watchlist', {})
                symbols = watchlist.get('symbols', [])
                logger.info(f"✅ Current watchlist: {symbols} ({len(symbols)} symbols)")
                return symbols
            elif response.get('type') == 'watchlist_error':
                logger.error(f"❌ Error getting watchlist: {response.get('error')}")
                return None
        
        logger.error("❌ Failed to get watchlist - no valid response")
        return None

async def run_tests():
    """Run comprehensive WebSocket tests for adding different symbols"""
    logger.info("🚀 Starting WebSocket server tests for adding different symbols...")
    
    tester = WebSocketTester()
    
    # Test 1: Connection
    logger.info("\n" + "="*50)
    logger.info("TEST 1: WebSocket Connection")
    logger.info("="*50)
    
    connected = await tester.connect()
    if not connected:
        logger.error("❌ Connection test failed - cannot proceed with other tests")
        return
    
    # Test 2: Get initial watchlist
    logger.info("\n" + "="*50)
    logger.info("TEST 2: Get Initial Watchlist")
    logger.info("="*50)
    
    initial_watchlist = await tester.test_get_watchlist()
    logger.info(f"📋 Initial watchlist: {initial_watchlist}")
    
    # Test 3: Add multiple different symbols
    test_symbols = ["GOOGL", "AMZN", "NVDA", "SPY", "QQQ", "META", "TSLA"]
    successful_adds = []
    failed_adds = []
    
    for i, symbol in enumerate(test_symbols, 3):
        logger.info("\n" + "="*50)
        logger.info(f"TEST {i}: Add Symbol {symbol}")
        logger.info("="*50)
        
        try:
            success = await tester.test_add_symbol(symbol)
            if success:
                successful_adds.append(symbol)
                logger.info(f"✅ Successfully added {symbol}")
            else:
                failed_adds.append(symbol)
                logger.error(f"❌ Failed to add {symbol}")
        except Exception as e:
            failed_adds.append(symbol)
            logger.error(f"❌ Exception adding {symbol}: {e}")
        
        # Small delay between requests
        await asyncio.sleep(0.5)
    
    # Test: Get final watchlist
    logger.info("\n" + "="*50)
    logger.info("TEST: Final Watchlist Check")
    logger.info("="*50)
    
    final_watchlist = await tester.test_get_watchlist()
    logger.info(f"📋 Final watchlist: {final_watchlist}")
    
    # Test Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    
    logger.info(f"✅ Connection: {'SUCCESS' if connected else 'FAILED'}")
    logger.info(f"📋 Initial watchlist: {initial_watchlist}")
    logger.info(f"✅ Successful additions: {successful_adds} ({len(successful_adds)} symbols)")
    logger.info(f"❌ Failed additions: {failed_adds} ({len(failed_adds)} symbols)")
    logger.info(f"📋 Final watchlist: {final_watchlist}")
    
    if final_watchlist:
        logger.info(f"📊 Total symbols in watchlist: {len(final_watchlist)}")
        logger.info(f"🆕 New symbols added this session: {[s for s in successful_adds if s in final_watchlist]}")
    
    # Disconnect
    await tester.disconnect()
    
    logger.info("\n🏁 WebSocket server tests completed!")

def main():
    """Main function"""
    try:
        asyncio.run(run_tests())
    except KeyboardInterrupt:
        logger.info("⏹️ Tests interrupted by user")
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")

if __name__ == "__main__":
    main()
