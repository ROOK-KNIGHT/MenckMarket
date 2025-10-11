#!/usr/bin/env python3
"""
Symbols Monitor Handler for VolFlow Options Breakout
Handles watchlist symbols and fetches current market prices
"""

import json
import logging
import asyncio
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import aiohttp
from connection_manager import ensure_valid_tokens
from config_loader import ConfigLoader
from historical_data_handler import HistoricalDataHandler
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class WatchlistSymbol:
    """Data class for watchlist symbol information"""
    symbol: str
    current_price: float
    price_change: float
    price_change_percent: float
    volume: int
    market_cap: Optional[float] = None
    last_updated: str = ""
    
    def __post_init__(self):
        if not self.last_updated:
            self.last_updated = datetime.now().isoformat()

class SymbolsMonitorHandler:
    """Handler for monitoring watchlist symbols and fetching current prices"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the symbols monitor handler"""
        self.config_loader = ConfigLoader(config_path)
        self.live_monitor_file = "live_monitor.json"
        self.symbols_to_monitor: List[str] = []
        self.watchlist_data: Dict[str, WatchlistSymbol] = {}
        
        
        # Initialize historical data handler
        self.historical_handler = HistoricalDataHandler()
        
        # Load existing watchlist from live monitor
        self.load_watchlist()
    
    def load_watchlist(self) -> None:
        """Load watchlist from both api_watchlist.json and current_positions.json"""
        try:
            # Load from api_watchlist.json
            api_symbols = self._load_api_watchlist_symbols()
            
            # Load from current_positions.json
            position_symbols = self._load_position_symbols()
            
            # Combine both sources
            all_symbols = list(set(api_symbols + position_symbols))
            self.symbols_to_monitor = all_symbols
            
            # Initialize watchlist data for all symbols
            self.watchlist_data = {}
            for symbol in all_symbols:
                self.watchlist_data[symbol] = WatchlistSymbol(
                    symbol=symbol,
                    current_price=0.0,
                    price_change=0.0,
                    price_change_percent=0.0,
                    volume=0
                )
                    
            logger.info(f"📊 Loaded {len(self.symbols_to_monitor)} symbols from integrated sources:")
            logger.info(f"   API Watchlist: {len(api_symbols)} symbols: {api_symbols}")
            logger.info(f"   Current Positions: {len(position_symbols)} symbols: {position_symbols}")
            logger.info(f"   Total Integrated: {len(all_symbols)} symbols: {sorted(all_symbols)}")
            
        except Exception as e:
            logger.error(f"Error loading integrated watchlist: {e}")
            self.symbols_to_monitor = []
            self.watchlist_data = {}

    def _load_api_watchlist_symbols(self) -> List[str]:
        """Load symbols from api_watchlist.json"""
        try:
            with open('api_watchlist.json', 'r') as f:
                data = json.load(f)
                symbols = data.get('watchlist', {}).get('symbols', [])
                logger.info(f"📋 Loaded {len(symbols)} symbols from api_watchlist.json")
                return symbols
        except FileNotFoundError:
            logger.info("api_watchlist.json not found")
            return []
        except Exception as e:
            logger.error(f"Error loading api_watchlist.json: {e}")
            return []

    def _load_position_symbols(self) -> List[str]:
        """Load symbols from current_positions.json"""
        try:
            with open('current_positions.json', 'r') as f:
                data = json.load(f)
                symbols = data.get('symbols', [])
                logger.info(f"💼 Loaded {len(symbols)} symbols from current_positions.json")
                return symbols
        except FileNotFoundError:
            logger.info("current_positions.json not found")
            return []
        except Exception as e:
            logger.error(f"Error loading current_positions.json: {e}")
            return []
    
    def save_watchlist(self) -> None:
        """Save watchlist to live monitor file"""
        try:
            # Load existing live monitor data
            live_data = {}
            try:
                with open(self.live_monitor_file, 'r') as f:
                    live_data = json.load(f)
            except FileNotFoundError:
                # Initialize basic structure if file doesn't exist
                live_data = {
                    'metadata': {},
                    'integrated_watchlist': {}
                }
            
            # Convert WatchlistSymbol objects to dictionaries
            watchlist_raw = {}
            for symbol, symbol_obj in self.watchlist_data.items():
                watchlist_raw[symbol] = asdict(symbol_obj)
            
            # Update integrated_watchlist section
            live_data['integrated_watchlist'] = {
                'symbols': self.symbols_to_monitor,
                'watchlist_data': watchlist_raw,
                'metadata': {
                    'total_symbols': len(self.symbols_to_monitor),
                    'last_updated': datetime.now().isoformat(),
                    'update_source': 'symbols_monitor_handler'
                }
            }
            
            # Update main metadata
            if 'metadata' not in live_data:
                live_data['metadata'] = {}
            
            live_data['metadata']['symbols_monitored'] = self.symbols_to_monitor
            live_data['metadata']['timestamp'] = datetime.now().isoformat()
            
            # Save updated live monitor data
            with open(self.live_monitor_file, 'w') as f:
                json.dump(live_data, f, indent=2)
                
            logger.info(f"Saved watchlist with {len(self.symbols_to_monitor)} symbols to live monitor")
        except Exception as e:
            logger.error(f"Error saving watchlist to live monitor: {e}")
    
    def add_symbol(self, symbol: str) -> bool:
        """Add a symbol to the watchlist"""
        symbol = symbol.upper().strip()
        
        if not symbol:
            logger.warning("Cannot add empty symbol")
            return False
            
        if symbol in self.symbols_to_monitor:
            logger.info(f"Symbol {symbol} already in watchlist")
            return False
        
        # Add to symbols list
        self.symbols_to_monitor.append(symbol)
        
        # Initialize with placeholder data
        self.watchlist_data[symbol] = WatchlistSymbol(
            symbol=symbol,
            current_price=0.0,
            price_change=0.0,
            price_change_percent=0.0,
            volume=0
        )
        
        # Save to file
        self.save_watchlist()
        
        logger.info(f"Added {symbol} to watchlist")
        return True
    
    def remove_symbol(self, symbol: str) -> bool:
        """Remove a symbol from the watchlist"""
        symbol = symbol.upper().strip()
        
        if symbol not in self.symbols_to_monitor:
            logger.warning(f"Symbol {symbol} not in watchlist")
            return False
        
        # Remove from symbols list
        self.symbols_to_monitor.remove(symbol)
        
        # Remove from watchlist data
        if symbol in self.watchlist_data:
            del self.watchlist_data[symbol]
        
        # Save to file
        self.save_watchlist()
        
        logger.info(f"Removed {symbol} from watchlist")
        return True
    
    def get_watchlist_symbols(self) -> List[str]:
        """Get list of symbols in watchlist"""
        return self.symbols_to_monitor.copy()
    
    def get_watchlist_data(self) -> Dict[str, Dict[str, Any]]:
        """Get watchlist data as dictionary"""
        result = {}
        for symbol, symbol_obj in self.watchlist_data.items():
            result[symbol] = asdict(symbol_obj)
        return result
    
    def fetch_quote_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch current quote data for a single symbol using HistoricalDataHandler"""
        try:
            # Get recent price data from historical data handler
            historical_data = self.historical_handler.get_historical_data(
                symbol=symbol,
                periodType='day',
                period=1,
                frequencyType='minute',
                freq=1,
                needExtendedHoursData=True
            )
            
            if historical_data and historical_data.get('candles'):
                candles = historical_data['candles']
                if candles:
                    # Get the most recent candle for current price
                    latest_candle = candles[-1]
                    previous_candle = candles[-2] if len(candles) > 1 else latest_candle
                    
                    current_price = latest_candle.get('close', 0.0)
                    previous_close = previous_candle.get('close', current_price)
                    volume = latest_candle.get('volume', 0)
                    high = latest_candle.get('high', current_price)
                    low = latest_candle.get('low', current_price)
                    open_price = latest_candle.get('open', current_price)
                    
                    # Calculate price change
                    price_change = current_price - previous_close
                    price_change_percent = (price_change / previous_close * 100) if previous_close > 0 else 0.0
                    
                    # Create a quote-like structure for compatibility
                    quote_data = {
                        symbol: {
                            'quote': {
                                'lastPrice': current_price,
                                'closePrice': previous_close,
                                'totalVolume': volume,
                                'high': high,
                                'low': low,
                                'open': open_price,
                                'netChange': price_change,
                                'netPercentChangeInDouble': price_change_percent
                            },
                            'fundamental': {
                                'marketCap': None  # Not available from historical data
                            }
                        }
                    }
                    
                    logger.info(f"Successfully fetched quote data for {symbol}: ${current_price}")
                    return quote_data
                else:
                    logger.warning(f"No candle data available for {symbol}")
                    return None
            else:
                logger.warning(f"No historical data available for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching quote data for {symbol}: {e}")
            return None
    
    def parse_quote_data(self, symbol: str, quote_data: Dict[str, Any]) -> Optional[WatchlistSymbol]:
        """Parse quote data from Schwab API response"""
        try:
            if symbol not in quote_data:
                logger.warning(f"No data found for symbol {symbol}")
                return None
            
            symbol_data = quote_data[symbol]
            quote = symbol_data.get('quote', {})
            fundamental = symbol_data.get('fundamental', {})
            
            # Extract price information
            current_price = quote.get('lastPrice', 0.0)
            previous_close = quote.get('closePrice', current_price)
            
            # Calculate price change
            price_change = current_price - previous_close
            price_change_percent = (price_change / previous_close * 100) if previous_close > 0 else 0.0
            
            # Extract volume and market cap
            volume = quote.get('totalVolume', 0)
            market_cap = fundamental.get('marketCap')
            
            return WatchlistSymbol(
                symbol=symbol,
                current_price=current_price,
                price_change=price_change,
                price_change_percent=price_change_percent,
                volume=volume,
                market_cap=market_cap,
                last_updated=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error parsing quote data for {symbol}: {e}")
            return None
    
    async def update_symbol_prices(self, symbols: Optional[List[str]] = None) -> Dict[str, WatchlistSymbol]:
        """Update prices for specified symbols or all watchlist symbols"""
        if symbols is None:
            symbols = self.symbols_to_monitor
        
        if not symbols:
            logger.info("No symbols to update")
            return {}
        
        logger.info(f"Updating prices for {len(symbols)} symbols: {symbols}")
        updated_data = {}
        
        # Process symbols in batches to avoid API rate limits
        batch_size = 10
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            
            # Create tasks for concurrent processing
            tasks = []
            for symbol in batch:
                task = self.fetch_and_update_symbol(symbol)
                tasks.append(task)
            
            # Execute batch concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for symbol, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error updating {symbol}: {result}")
                elif result:
                    updated_data[symbol] = result
                    self.watchlist_data[symbol] = result
            
            # Small delay between batches to respect rate limits
            if i + batch_size < len(symbols):
                await asyncio.sleep(0.5)
        
        # Save updated data
        if updated_data:
            self.save_watchlist()
            logger.info(f"Successfully updated {len(updated_data)} symbols")
        
        return updated_data
    
    async def fetch_and_update_symbol(self, symbol: str) -> Optional[WatchlistSymbol]:
        """Fetch and update data for a single symbol"""
        try:
            quote_data = self.fetch_quote_data(symbol)
            if quote_data:
                return self.parse_quote_data(symbol, quote_data)
            return None
        except Exception as e:
            logger.error(f"Error fetching and updating {symbol}: {e}")
            return None
    
    async def refresh_all_prices(self) -> Dict[str, Any]:
        """Refresh prices for all watchlist symbols and return summary"""
        start_time = datetime.now()
        
        updated_data = await self.update_symbol_prices()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        summary = {
            'symbols_updated': len(updated_data),
            'total_symbols': len(self.symbols_to_monitor),
            'update_duration': duration,
            'last_update': end_time.isoformat(),
            'watchlist_data': self.get_watchlist_data()
        }
        
        logger.info(f"Refreshed {len(updated_data)}/{len(self.symbols_to_monitor)} symbols in {duration:.2f}s")
        return summary
    
    def get_symbols_to_monitor(self) -> List[str]:
        """Get list of all symbols being monitored (for integration with other systems)"""
        return self.symbols_to_monitor.copy()
    
    def get_market_status(self) -> str:
        """Get current market status (simplified)"""
        now = datetime.now()
        current_hour = now.hour
        
        # Simple market hours check (9:30 AM - 4:00 PM ET)
        # This is a simplified version - in production you'd want more sophisticated logic
        if 9 <= current_hour < 16:
            return "Open"
        elif 16 <= current_hour < 20:
            return "After Hours"
        else:
            return "Closed"

    def insert_watchlist_to_db(self) -> bool:
        """Insert watchlist data into PostgreSQL database with truncation"""
        try:
            # Database connection parameters
            conn_params = {
                'host': 'localhost',
                'database': 'volflow_options',
                'user': 'isaac',
                'password': None  # Will use peer authentication
            }
            
            # Connect to database
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Truncate the table first
            print("🗑️  Truncating integrated_watchlist table...")
            # Use DELETE instead of TRUNCATE to avoid relation locks in parallel execution
            cur.execute("DELETE FROM integrated_watchlist;")
            
            # Insert watchlist data
            insert_count = 0
            for symbol, symbol_obj in self.watchlist_data.items():
                try:
                    # Prepare insert statement
                    insert_sql = """
                    INSERT INTO integrated_watchlist (
                        timestamp, symbol, current_price, price_change, price_change_percent,
                        volume, market_cap, last_updated, high_52_week, low_52_week,
                        avg_volume, pe_ratio, dividend_yield, market_status, sector, industry
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    # Prepare values
                    values = (
                        datetime.now().isoformat(),
                        symbol,
                        symbol_obj.current_price,
                        symbol_obj.price_change,
                        symbol_obj.price_change_percent,
                        symbol_obj.volume,
                        symbol_obj.market_cap,
                        symbol_obj.last_updated,
                        0.0,  # high_52_week - would need additional data
                        0.0,  # low_52_week - would need additional data
                        0,    # avg_volume - would need additional data
                        0.0,  # pe_ratio - would need additional data
                        0.0,  # dividend_yield - would need additional data
                        self.get_market_status(),
                        'Unknown',  # sector - would need additional data
                        'Unknown'   # industry - would need additional data
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    print(f"❌ Error inserting {symbol}: {e}")
                    continue
            
            # Commit the transaction
            conn.commit()
            
            # Close connections
            cur.close()
            conn.close()
            
            print(f"✅ Successfully inserted {insert_count} watchlist symbols into database")
            return True
            
        except Exception as e:
            print(f"❌ Error inserting watchlist to database: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def create_integrated_watchlist_json(self) -> bool:
        """Create integrated_watchlist.json file combining api_watchlist.json and current_positions.json"""
        try:
            # Reload the latest data from both sources
            api_symbols = self._load_api_watchlist_symbols()
            position_symbols = self._load_position_symbols()
            
            # Get position details from current_positions.json
            position_details = {}
            try:
                with open('current_positions.json', 'r') as f:
                    positions_data = json.load(f)
                    position_details = positions_data.get('positions', {})
            except Exception as e:
                logger.warning(f"Could not load position details: {e}")
            
            # Combine all symbols
            all_symbols = list(set(api_symbols + position_symbols))
            
            # Create integrated data structure
            integrated_data = {
                'strategy_name': 'Integrated_Watchlist',
                'last_updated': datetime.now().isoformat(),
                'total_symbols': len(all_symbols),
                'watchlist_data': {},
                'metadata': {
                    'data_sources': ['api_watchlist.json', 'current_positions.json'],
                    'api_watchlist_symbols': len(api_symbols),
                    'position_symbols': len(position_symbols),
                    'total_integrated_symbols': len(all_symbols),
                    'update_source': 'symbols_monitor_handler',
                    'creation_date': datetime.now().isoformat()
                }
            }
            
            # Add watchlist data for each symbol
            for symbol in all_symbols:
                symbol_data = {
                    'symbol': symbol,
                    'current_price': 0.0,
                    'price_change': 0.0,
                    'price_change_percent': 0.0,
                    'volume': 0,
                    'market_cap': None,
                    'last_updated': datetime.now().isoformat(),
                    'source': []
                }
                
                # Mark source
                if symbol in api_symbols:
                    symbol_data['source'].append('api_watchlist')
                if symbol in position_symbols:
                    symbol_data['source'].append('current_positions')
                    
                    # Add position details if available
                    for pos_key, pos_data in position_details.items():
                        if pos_data.get('symbol') == symbol:
                            symbol_data['position_details'] = {
                                'quantity': pos_data.get('quantity', 0),
                                'market_value': pos_data.get('market_value', 0),
                                'cost_basis': pos_data.get('cost_basis', 0),
                                'unrealized_pl': pos_data.get('unrealized_pl', 0),
                                'unrealized_pl_percent': pos_data.get('unrealized_pl_percent', 0),
                                'account': pos_data.get('account', ''),
                                'position_type': pos_data.get('position_type', 'LONG'),
                                'instrument_type': pos_data.get('instrument_type', 'EQUITY')
                            }
                            break
                
                # Use current watchlist data if available
                if symbol in self.watchlist_data:
                    watchlist_symbol = self.watchlist_data[symbol]
                    symbol_data.update({
                        'current_price': watchlist_symbol.current_price,
                        'price_change': watchlist_symbol.price_change,
                        'price_change_percent': watchlist_symbol.price_change_percent,
                        'volume': watchlist_symbol.volume,
                        'market_cap': watchlist_symbol.market_cap,
                        'last_updated': watchlist_symbol.last_updated
                    })
                
                integrated_data['watchlist_data'][symbol] = symbol_data
            
            # Write to integrated_watchlist.json
            with open('integrated_watchlist.json', 'w') as f:
                json.dump(integrated_data, f, indent=2)
            
            logger.info(f"✅ Created integrated_watchlist.json with {len(all_symbols)} symbols")
            logger.info(f"📊 Sources: API({len(api_symbols)}) + Positions({len(position_symbols)}) = Total({len(all_symbols)})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating integrated watchlist JSON: {e}")
            return False

    async def refresh_and_store_watchlist(self) -> Dict[str, Any]:
        """Refresh watchlist prices and create integrated JSON"""
        try:
            # Refresh all prices
            summary = await self.refresh_all_prices()
            
            # Create integrated watchlist JSON
            json_created = self.create_integrated_watchlist_json()
            summary['integrated_json_created'] = json_created
            
            return summary
            
        except Exception as e:
            print(f"❌ Error refreshing watchlist: {e}")
            return {}


def cli_interface():
    """Command-line interface for managing watchlist symbols"""
    import sys
    
    handler = SymbolsMonitorHandler()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 symbols_monitor_handler.py add <SYMBOL>     - Add symbol to watchlist")
        print("  python3 symbols_monitor_handler.py remove <SYMBOL>  - Remove symbol from watchlist")
        print("  python3 symbols_monitor_handler.py list             - List all symbols")
        print("  python3 symbols_monitor_handler.py refresh          - Refresh all prices")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'add' and len(sys.argv) > 2:
        symbol = sys.argv[2].upper()
        success = handler.add_symbol(symbol)
        print(f"{'✅' if success else '❌'} {'Added' if success else 'Failed to add'} {symbol}")
    
    elif command == 'remove' and len(sys.argv) > 2:
        symbol = sys.argv[2].upper()
        success = handler.remove_symbol(symbol)
        print(f"{'✅' if success else '❌'} {'Removed' if success else 'Failed to remove'} {symbol}")
    
    elif command == 'list':
        symbols = handler.get_watchlist_symbols()
        print(f"📊 Watchlist symbols ({len(symbols)}):")
        for symbol in symbols:
            print(f"  - {symbol}")
    
    elif command == 'refresh':
        async def refresh():
            summary = await handler.refresh_all_prices()
            print(f"🔄 Refreshed {summary['symbols_updated']}/{summary['total_symbols']} symbols")
            print(f"⏱️  Duration: {summary['update_duration']:.2f}s")
        
        asyncio.run(refresh())
    
    else:
        print("❌ Invalid command or missing arguments")
        cli_interface()


def main():
    """Main function - automatically refresh watchlist and store in database"""
    print("Symbols Monitor Handler - Watchlist Analysis")
    print("=" * 50)
    
    # Initialize handler
    handler = SymbolsMonitorHandler()
    
    # Get current symbols
    symbols = handler.get_watchlist_symbols()
    print(f"Processing {len(symbols)} watchlist symbols: {symbols}")
    
    # Run async refresh and store
    async def run_refresh_and_store():
        summary = await handler.refresh_and_store_watchlist()
        print(f"✅ Watchlist analysis completed")
        print(f"📊 Updated {summary.get('symbols_updated', 0)}/{summary.get('total_symbols', 0)} symbols")
        print(f"⏱️  Duration: {summary.get('update_duration', 0):.2f}s")
        return summary
    
    # Execute the async function
    import asyncio
    asyncio.run(run_refresh_and_store())


if __name__ == "__main__":
    main()
