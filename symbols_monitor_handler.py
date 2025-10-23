#!/usr/bin/env python3
"""
Simplified Symbols Monitor Handler for VolFlow Options Breakout
Loads symbols from api_watchlist.json and current_positions.json,
fetches market data using Schwab quotes API, and updates integrated_watchlist.json
"""

import json
import logging
import asyncio
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from config_loader import ConfigLoader
from historical_data_handler import HistoricalDataHandler

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
    """Simplified handler for monitoring watchlist symbols and updating integrated_watchlist.json"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the symbols monitor handler"""
        self.config_loader = ConfigLoader(config_path)
        self.symbols_to_monitor: List[str] = []
        self.watchlist_data: Dict[str, WatchlistSymbol] = {}
        
        # Initialize historical data handler for quotes
        self.historical_handler = HistoricalDataHandler()
        
        logger.info("🔧 Simplified Symbols Monitor Handler initialized with centralized quotes API")
    
    def load_symbols_from_sources(self) -> List[str]:
        """Load and combine symbols from current_positions.json and trading_config_live.json strategies"""
        try:
            logger.info("📋 === LOADING SYMBOLS FROM SOURCES ===")
            
            # Load from current_positions.json
            position_symbols = self._load_position_symbols()
            logger.info(f"💼 POSITION SYMBOLS ({len(position_symbols)}): {position_symbols}")
            
            # Load from trading_config_live.json strategies
            strategy_symbols = self._load_strategy_symbols()
            logger.info(f"🎯 STRATEGY SYMBOLS ({len(strategy_symbols)}): {strategy_symbols}")
            
            # Combine both sources
            all_symbols = list(set(position_symbols + strategy_symbols))
            logger.info(f"🎯 COMBINED SYMBOLS ({len(all_symbols)}): {sorted(all_symbols)}")
            logger.info(f"🔍 Position-only symbols: {[s for s in position_symbols if s not in strategy_symbols]}")
            logger.info(f"🔍 Strategy-only symbols: {[s for s in strategy_symbols if s not in position_symbols]}")
            logger.info(f"🔍 Overlapping symbols: {[s for s in all_symbols if s in position_symbols and s in strategy_symbols]}")
            
            self.symbols_to_monitor = all_symbols
            print(f"Total unique symbols to monitor: {len(all_symbols)}")
            return all_symbols
            
        except Exception as e:
            logger.error(f"❌ Error loading symbols from sources: {e}")
            return []

    def _load_api_watchlist_symbols(self) -> List[str]:
        """Load symbols from api_watchlist.json"""
        try:
            with open('api_watchlist.json', 'r') as f:
                data = json.load(f)
                symbols = data.get('watchlist', {}).get('symbols', [])
                logger.info(f"📋 Loaded {len(symbols)} symbols from api_watchlist.json")
                return symbols
        except FileNotFoundError:
            logger.info("📋 api_watchlist.json not found")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading api_watchlist.json: {e}")
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
            logger.info("💼 current_positions.json not found")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading current_positions.json: {e}")
            return []

    def _load_strategy_symbols(self) -> List[str]:
        """Load symbols from trading_config_live.json strategy watchlists"""
        try:
            with open('trading_config_live.json', 'r') as f:
                data = json.load(f)
                strategies = data.get('strategies', {})
                
                all_strategy_symbols = []
                strategy_details = {}
                
                for strategy_name, strategy_config in strategies.items():
                    strategy_symbols = []
                    
                    # Check for different watchlist naming patterns
                    watchlist_keys = [
                        f"{strategy_name}strategy_watchlist",  # pmlstrategy_watchlist
                        f"{strategy_name}_strategy_watchlist",  # divergence_strategy_watchlist, iron_condor_strategy_watchlist
                        f"{strategy_name}_watchlist",
                        "watchlist"
                    ]
                    
                    for key in watchlist_keys:
                        if key in strategy_config:
                            strategy_symbols = strategy_config[key]
                            break
                    
                    if strategy_symbols:
                        all_strategy_symbols.extend(strategy_symbols)
                        strategy_details[strategy_name] = {
                            'symbols': strategy_symbols,
                            'count': len(strategy_symbols),
                            'running': strategy_config.get('running_state', {}).get('is_running', False)
                        }
                        logger.info(f"🎯 {strategy_name.upper()} strategy: {len(strategy_symbols)} symbols - {strategy_symbols} (Running: {strategy_details[strategy_name]['running']})")
                    else:
                        logger.info(f"🎯 {strategy_name.upper()} strategy: No symbols found")
                
                # Remove duplicates while preserving order
                unique_symbols = list(dict.fromkeys(all_strategy_symbols))
                
                logger.info(f"🎯 Total strategy symbols loaded: {len(unique_symbols)} unique symbols from {len(strategy_details)} strategies")
                logger.info(f"🎯 Strategy symbol details: {strategy_details}")
                
                return unique_symbols
                
        except FileNotFoundError:
            logger.info("🎯 trading_config_live.json not found")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading strategy symbols from trading_config_live.json: {e}")
            return []
    
    def fetch_quotes_data(self, symbols: List[str]) -> Optional[Dict[str, Any]]:
        """Fetch current quote data for multiple symbols using centralized HistoricalDataHandler"""
        if not symbols:
            logger.warning("No symbols provided for quote fetch")
            return None
            
        try:
            logger.info(f"📊 Fetching quotes for {len(symbols)} symbols using centralized handler: {symbols}")
            
            # Use the centralized historical data handler for quotes
            quotes_data = self.historical_handler.get_quotes(symbols, fields="quote,reference")
            
            if quotes_data:
                logger.info(f"✅ Successfully fetched quotes for {len(quotes_data)} symbols via centralized handler")
                return quotes_data
            else:
                logger.warning("⚠️ No quotes data returned from centralized handler")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching quotes data via centralized handler: {e}")
            return None
    
    def parse_quotes_data(self, quotes_data: Dict[str, Any]) -> Dict[str, WatchlistSymbol]:
        """Parse quotes data from Schwab API response"""
        parsed_symbols = {}
        
        try:
            for symbol, symbol_data in quotes_data.items():
                try:
                    quote = symbol_data.get('quote', {})
                    reference = symbol_data.get('reference', {})
                    
                    # Extract price information
                    current_price = quote.get('lastPrice', 0.0)
                    close_price = quote.get('closePrice', current_price)
                    
                    # Calculate price change
                    price_change = quote.get('netChange', 0.0)
                    price_change_percent = quote.get('netPercentChange', 0.0)
                    
                    # Extract volume
                    volume = quote.get('totalVolume', 0)
                    
                    # Create WatchlistSymbol object
                    watchlist_symbol = WatchlistSymbol(
                        symbol=symbol,
                        current_price=current_price,
                        price_change=price_change,
                        price_change_percent=price_change_percent,
                        volume=volume,
                        market_cap=None,  # Not available in quotes API
                        last_updated=datetime.now().isoformat()
                    )
                    
                    parsed_symbols[symbol] = watchlist_symbol
                    logger.info(f"✅ Parsed {symbol}: ${current_price:.2f} ({price_change:+.2f}, {price_change_percent:+.2f}%)")
                    
                except Exception as e:
                    logger.error(f"❌ Error parsing quote data for {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error parsing quotes data: {e}")
            
        return parsed_symbols
    
    async def fetch_market_data_for_symbols(self, symbols: List[str]) -> Dict[str, WatchlistSymbol]:
        """Fetch market data for all symbols using batch quotes API"""
        if not symbols:
            logger.info("ℹ️ No symbols to fetch data for")
            return {}
        
        logger.info(f"📊 Fetching market data for {len(symbols)} symbols using batch quotes API")
        
        try:
            # Process symbols in batches to avoid URL length limits
            batch_size = 50  # Schwab API can handle many symbols at once
            all_market_data = {}
            
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                logger.info(f"📊 Processing batch {i//batch_size + 1}: {len(batch)} symbols")
                
                # Fetch quotes for this batch
                quotes_data = self.fetch_quotes_data(batch)
                
                if quotes_data:
                    # Parse the quotes data
                    batch_market_data = self.parse_quotes_data(quotes_data)
                    all_market_data.update(batch_market_data)
                    
                    logger.info(f"✅ Batch {i//batch_size + 1}: Got data for {len(batch_market_data)} symbols")
                else:
                    logger.warning(f"⚠️ Batch {i//batch_size + 1}: No data returned")
                
                # Small delay between batches to respect rate limits
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.5)
            
            logger.info(f"✅ Successfully fetched market data for {len(all_market_data)}/{len(symbols)} symbols")
            return all_market_data
            
        except Exception as e:
            logger.error(f"❌ Error fetching market data for symbols: {e}")
            return {}

    def create_integrated_watchlist_json(self, market_data: Dict[str, WatchlistSymbol]) -> bool:
        """Create integrated_watchlist.json file with combined symbols and market data"""
        try:
            logger.info("🔧 === CREATING INTEGRATED WATCHLIST JSON ===")
            
            # Reload the latest data from the two specified sources
            position_symbols = self._load_position_symbols()
            strategy_symbols = self._load_strategy_symbols()
            
            # Get position details from current_positions.json
            position_details = {}
            try:
                with open('current_positions.json', 'r') as f:
                    positions_data = json.load(f)
                    position_details = positions_data.get('positions', {})
                logger.info(f"📊 Position details loaded for {len(position_details)} positions")
            except Exception as e:
                logger.warning(f"⚠️ Could not load position details: {e}")
            
            # Combine symbols from the two sources
            all_symbols = list(set(position_symbols + strategy_symbols))
            
            # Create integrated data structure
            integrated_data = {
                'strategy_name': 'Integrated_Watchlist',
                'last_updated': datetime.now().isoformat(),
                'total_symbols': len(all_symbols),
                'watchlist_data': {},
                'metadata': {
                    'data_sources': ['current_positions.json', 'trading_config_live.json'],
                    'position_symbols': len(position_symbols),
                    'strategy_symbols': len(strategy_symbols),
                    'total_integrated_symbols': len(all_symbols),
                    'update_source': 'symbols_monitor_handler',
                    'market_data_source': 'schwab_quotes_api',
                    'creation_date': datetime.now().isoformat()
                }
            }
            
            # Add watchlist data for each symbol
            logger.info(f"🔧 === PROCESSING {len(all_symbols)} SYMBOLS FOR INTEGRATED WATCHLIST ===")
            
            for symbol in all_symbols:
                logger.info(f"📊 Processing symbol: {symbol}")
                
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
                
                # Mark source and log it
                sources_found = []
                if symbol in position_symbols:
                    symbol_data['source'].append('current_positions')
                    sources_found.append('CURRENT_POSITIONS')
                    
                    # Add position details if available
                    for pos_key, pos_data in position_details.items():
                        if pos_data.get('symbol') == symbol:
                            position_info = {
                                'quantity': pos_data.get('quantity', 0),
                                'market_value': pos_data.get('market_value', 0),
                                'cost_basis': pos_data.get('cost_basis', 0),
                                'unrealized_pl': pos_data.get('unrealized_pl', 0),
                                'unrealized_pl_percent': pos_data.get('unrealized_pl_percent', 0),
                                'account': pos_data.get('account', ''),
                                'position_type': pos_data.get('position_type', 'LONG'),
                                'instrument_type': pos_data.get('instrument_type', 'EQUITY')
                            }
                            symbol_data['position_details'] = position_info
                            logger.info(f"   💼 Position details: {pos_data.get('quantity', 0)} shares, "
                                      f"Market Value: ${pos_data.get('market_value', 0):.2f}, "
                                      f"P&L: ${pos_data.get('unrealized_pl', 0):.2f} "
                                      f"({pos_data.get('unrealized_pl_percent', 0):.2f}%)")
                            break
                if symbol in strategy_symbols:
                    symbol_data['source'].append('trading_strategies')
                    sources_found.append('TRADING_STRATEGIES')
                
                # Use market data if available
                market_data_found = False
                if symbol in market_data:
                    market_symbol = market_data[symbol]
                    symbol_data.update({
                        'current_price': market_symbol.current_price,
                        'price_change': market_symbol.price_change,
                        'price_change_percent': market_symbol.price_change_percent,
                        'volume': market_symbol.volume,
                        'market_cap': market_symbol.market_cap,
                        'last_updated': market_symbol.last_updated
                    })
                    market_data_found = True
                    logger.info(f"   📈 Market data: ${market_symbol.current_price:.2f} "
                              f"({market_symbol.price_change:+.2f}, {market_symbol.price_change_percent:+.2f}%), "
                              f"Volume: {market_symbol.volume:,}")
                else:
                    logger.info(f"   ⚠️ No market data available for {symbol}")
                
                # Log the complete symbol entry
                logger.info(f"   ✅ {symbol} added - Sources: {sources_found}, "
                          f"Market Data: {'YES' if market_data_found else 'NO'}, "
                          f"Position: {'YES' if 'position_details' in symbol_data else 'NO'}")
                
                integrated_data['watchlist_data'][symbol] = symbol_data
            
            # Write to integrated_watchlist.json
            with open('integrated_watchlist.json', 'w') as f:
                json.dump(integrated_data, f, indent=2)
            
            logger.info(f"✅ Created integrated_watchlist.json with {len(all_symbols)} symbols")
            logger.info(f"📊 Sources: Positions({len(position_symbols)}) + Strategies({len(strategy_symbols)}) = Total({len(all_symbols)})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating integrated watchlist JSON: {e}")
            return False

    async def run_full_update(self) -> Dict[str, Any]:
        """Run the complete update process: load symbols, fetch data, update JSON"""
        try:
            start_time = datetime.now()
            logger.info("🚀 === STARTING FULL WATCHLIST UPDATE ===")
            
            # Step 1: Load symbols from both sources
            symbols = self.load_symbols_from_sources()
            if not symbols:
                logger.warning("⚠️ No symbols found to monitor")
                return {
                    'success': False,
                    'error': 'No symbols found',
                    'symbols_processed': 0,
                    'duration': 0
                }
            
            # Step 2: Fetch market data for all symbols using batch quotes API
            market_data = await self.fetch_market_data_for_symbols(symbols)
            
            # Step 3: Create integrated watchlist JSON
            json_created = self.create_integrated_watchlist_json(market_data)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            summary = {
                'success': json_created,
                'symbols_processed': len(symbols),
                'market_data_fetched': len(market_data),
                'duration': duration,
                'last_update': end_time.isoformat(),
                'symbols': symbols
            }
            
            logger.info(f"🎉 === FULL UPDATE COMPLETED ===")
            logger.info(f"✅ Processed {len(symbols)} symbols in {duration:.2f}s")
            logger.info(f"📊 Market data fetched for {len(market_data)} symbols")
            logger.info(f"📄 Integrated JSON created: {json_created}")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error in full update: {e}")
            return {
                'success': False,
                'error': str(e),
                'symbols_processed': 0,
                'duration': 0
            }

    def _update_integrated_watchlist_json(self, market_data: Dict[str, WatchlistSymbol] = None, fetch_market_data: bool = False) -> bool:
        """
        Internal method to update integrated_watchlist.json file automatically.
        Called every time get_watchlist_symbols() or get_watchlist_data() is executed.
        Similar to account_data_handler.py pattern.
        
        Args:
            market_data (Dict[str, WatchlistSymbol], optional): Market data to include
            fetch_market_data (bool): Whether to fetch market data if none provided
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # If no market data provided and fetch_market_data is True, fetch it
            if market_data is None and fetch_market_data:
                logger.info("🔄 Auto-update: Fetching market data for integrated watchlist")
                symbols = self.load_symbols_from_sources()
                if symbols:
                    try:
                        # Use synchronous approach for auto-updates
                        quotes_data = self.fetch_quotes_data(symbols)
                        if quotes_data:
                            market_data = self.parse_quotes_data(quotes_data)
                            logger.info(f"✅ Auto-update: Fetched market data for {len(market_data)} symbols")
                        else:
                            market_data = {}
                            logger.warning("⚠️ Auto-update: No market data returned")
                    except Exception as e:
                        logger.warning(f"⚠️ Auto-update: Failed to fetch market data: {e}")
                        market_data = {}
                else:
                    market_data = {}
            elif market_data is None:
                market_data = {}
                logger.debug("Auto-update: Skipping market data fetch for performance")
            
            # Create/update the integrated watchlist JSON
            success = self.create_integrated_watchlist_json(market_data)
            
            if success:
                logger.debug("✅ Auto-updated integrated_watchlist.json")
            else:
                logger.warning("⚠️ Failed to auto-update integrated_watchlist.json")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error auto-updating integrated_watchlist.json: {e}")
            return False

    # Enhanced methods that auto-update integrated_watchlist.json on every call
    def get_watchlist_symbols(self) -> List[str]:
        """
        Get list of symbols being monitored and automatically update integrated_watchlist.json.
        Similar to account_data_handler.py pattern - updates JSON file on every call.
        """
        try:
            # Load symbols from only the two specified sources
            position_symbols = self._load_position_symbols()
            strategy_symbols = self._load_strategy_symbols()
            all_symbols = list(set(position_symbols + strategy_symbols))
            
            # Automatically update integrated_watchlist.json every time this method is called
            try:
                self._update_integrated_watchlist_json()
            except Exception as e:
                logger.warning(f"Warning: Failed to auto-update integrated_watchlist.json: {e}")
            
            logger.debug(f"📋 Retrieved {len(all_symbols)} symbols and auto-updated JSON")
            return all_symbols
            
        except Exception as e:
            logger.warning(f"Could not load symbols: {e}")
            return []
    
    def get_watchlist_data(self, include_market_data: bool = True) -> Dict[str, Any]:
        """
        Get watchlist data as dictionary and automatically update integrated_watchlist.json.
        Similar to account_data_handler.py pattern - updates JSON file on every call.
        
        Args:
            include_market_data (bool): Whether to fetch market data (default: True for realtime_monitor)
        """
        try:
            # First, ensure the JSON is updated with latest data including market data
            try:
                self._update_integrated_watchlist_json(fetch_market_data=include_market_data)
            except Exception as e:
                logger.warning(f"Warning: Failed to auto-update integrated_watchlist.json: {e}")
            
            # Then read and return the data
            with open('integrated_watchlist.json', 'r') as f:
                data = json.load(f)
                watchlist_data = data.get('watchlist_data', {})
                logger.debug(f"📋 Retrieved watchlist data for {len(watchlist_data)} symbols and auto-updated JSON (market_data={include_market_data})")
                return watchlist_data
                
        except Exception as e:
            logger.warning(f"Could not load integrated watchlist data: {e}")
            return {}

    # Legacy method names for backward compatibility (also auto-update)
    def get_symbols(self) -> List[str]:
        """Legacy method name - calls get_watchlist_symbols()"""
        return self.get_watchlist_symbols()
    
    def get_data(self) -> Dict[str, Any]:
        """Legacy method name - calls get_watchlist_data()"""
        return self.get_watchlist_data()

    # Additional methods for compatibility with realtime_monitor.py
    def add_symbol(self, symbol: str) -> bool:
        """Add a symbol to monitoring (for compatibility)"""
        logger.info(f"📋 Add symbol request: {symbol} (handled by api_watchlist.json)")
        return True
    
    def remove_symbol(self, symbol: str) -> bool:
        """Remove a symbol from monitoring (for compatibility)"""
        logger.info(f"📋 Remove symbol request: {symbol} (handled by api_watchlist.json)")
        return True


def main():
    """Main function - run the simplified symbols monitor handler"""
    print("Simplified Symbols Monitor Handler with Schwab Quotes API")
    print("=" * 60)
    
    # Initialize handler
    handler = SymbolsMonitorHandler()
    
    # Run the full update process
    async def run_update():
        summary = await handler.run_full_update()
        
        if summary['success']:
            print(f"✅ Update completed successfully")
            print(f"📊 Symbols processed: {summary['symbols_processed']}")
            print(f"📈 Market data fetched: {summary['market_data_fetched']}")
            print(f"⏱️  Duration: {summary['duration']:.2f}s")
            print(f"🎯 Symbols: {summary['symbols']}")
        else:
            print(f"❌ Update failed: {summary.get('error', 'Unknown error')}")
    
    # Execute the async function
    asyncio.run(run_update())


if __name__ == "__main__":
    main()
