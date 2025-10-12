#!/usr/bin/env python3
"""
Simplified Real-time P&L and Technical Indicators Monitor

This script continuously monitors all data concurrently:
1. P&L data from Schwab account positions
2. Technical indicators for traded symbols
3. Real-time price updates
4. Account data
5. Recent transactions

Usage: python3 realtime_monitor.py
"""

import json
import time
import asyncio
import threading
import os
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our handlers
from schwab_transaction_handler import SchwabTransactionHandler
from pnl_data_handler import PnLDataHandler
from technical_indicators import UnifiedTechnicalIndicators
from historical_data_handler import HistoricalDataHandler
from options_data_handler import OptionsDataHandler
from account_data_handler import AccountDataHandler
from connection_manager import get_all_positions
from iron_condor_strategy import IronCondorStrategy
from pml_strategy import PMLStrategy
from divergence_strategy import DivergenceStrategy
from symbols_monitor_handler import SymbolsMonitorHandler
from db_inserter import DatabaseInserter

class RealTimeMonitor:
    """Simplified real-time monitoring system with concurrent data collection."""
    
    def __init__(self, update_interval: float = 1.0):
        """Initialize the real-time monitor."""
        self.update_interval = update_interval
        self.running = False
        self.data_lock = threading.Lock()
        
        # Rate limiting
        self.api_call_delay = 0.2  # 200ms delay between API calls
        self.last_api_call = {}  # Track last API call time per endpoint
        self.max_concurrent_requests = 10 # Limit concurrent requests
        
        # Initialize handlers
        self.transaction_handler = SchwabTransactionHandler()
        self.pnl_handler = PnLDataHandler()
        self.technical_handler = UnifiedTechnicalIndicators()
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        self.account_handler = AccountDataHandler()
        self.iron_condor_strategy = IronCondorStrategy()
        self.pml_strategy = PMLStrategy()
        self.divergence_strategy = DivergenceStrategy()
        self.symbols_monitor = SymbolsMonitorHandler()
        
        # Initialize centralized database inserter
        self.db_inserter = DatabaseInserter()
        self.db_inserter_thread = None
        self.db_inserter_running = False
        
        # Options data update tracking
        self.last_options_update = 0
        self.options_update_interval = 10  # Update options data every 10 seconds
        
        # Setup logging first
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Database insertion functionality removed - now handled by individual strategy files
        self.db_thread = None
        self.db_thread_running = False
        
        # Initialize background processing threads
        self.technical_thread = None
        self.technical_thread_running = False
        self.technical_data_cache = {}
        self.strategy_data_cache = {}
        
        # Data storage
        self.current_data = {}
        self.symbols_to_monitor = set()
        self.watchlist_symbols = set()
        
        # Load initial watchlist symbols
        self._load_initial_watchlist()



        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("RealTimeMonitor initialized for continuous operation")
        self.logger.info("Centralized database inserter will run on 30-second intervals")
    
    def _load_initial_watchlist(self):
        """Load initial watchlist symbols from API watchlist only."""
        try:
            # No hardcoded symbols - only load from API watchlist and positions
            self.logger.info("Initial watchlist will be populated from positions and API watchlist only")
            #print initial watchlist state
            self.logger.info(f"Initial watchlist symbols: {self.watchlist_symbols}")
            self.logger.info(f"Initial symbols to monitor (positions): {self.symbols_to_monitor}")
            self._sync_api_watchlist_to_live_monitor()
            self.logger.info(f"Final watchlist symbols after sync: {self.watchlist_symbols}")
            self.logger.info(f"Final symbols to monitor (positions): {self.symbols_to_monitor})")

            
        except Exception as e:
            self.logger.error(f"Error loading initial watchlist: {e}")
    
    def _rate_limit_api_call(self, endpoint: str):
        """Apply rate limiting to API calls."""
        current_time = time.time()
        if endpoint in self.last_api_call:
            time_since_last = current_time - self.last_api_call[endpoint]
            if time_since_last < self.api_call_delay:
                sleep_time = self.api_call_delay - time_since_last
                time.sleep(sleep_time)
        
        self.last_api_call[endpoint] = time.time()

    def _safe_api_call(self, func, *args, **kwargs):
        """Make API call with error handling and retries."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Add small delay between retries to prevent rate limiting
                if attempt > 0:
                    time.sleep(0.3)  # Fixed 300ms delay
                
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if "403" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"403 error on attempt {attempt + 1}, retrying...")
                    time.sleep(1.5)  # Fixed 1.5 second delay for 403 errors
                    continue
                elif attempt == max_retries - 1:
                    self.logger.error(f"API call failed after {max_retries} attempts: {e}")
                    return None
        return None
    
    def add_symbol_to_watchlist(self, symbol: str) -> bool:
        """Add a symbol to the watchlist and start monitoring it immediately."""
        try:
            symbol = symbol.upper().strip()
            
            if not symbol:
                self.logger.warning("Cannot add empty symbol")
                return False
            
            if symbol in self.watchlist_symbols:
                self.logger.info(f"Symbol {symbol} already in watchlist")
                return False
            
            # Add to watchlist
            self.watchlist_symbols.add(symbol)
            self.symbols_to_monitor.add(symbol)
            
            # Add to symbols monitor handler
            success = self.symbols_monitor.add_symbol(symbol)
            
            if success:
                self.logger.info(f"Added {symbol} to real-time monitoring")
                return True
            else:
                # Remove from our sets if symbols monitor failed
                self.watchlist_symbols.discard(symbol)
                self.symbols_to_monitor.discard(symbol)
                return False
                
        except Exception as e:
            self.logger.error(f"Error adding symbol {symbol} to watchlist: {e}")
            return False
    
    def remove_symbol_from_watchlist(self, symbol: str) -> bool:
        """Remove a symbol from the watchlist."""
        try:
            symbol = symbol.upper().strip()
            
            if symbol not in self.watchlist_symbols:
                self.logger.warning(f"Symbol {symbol} not in watchlist")
                return False
            
            # Remove from watchlist
            self.watchlist_symbols.discard(symbol)
            
            # Remove from symbols monitor handler
            success = self.symbols_monitor.remove_symbol(symbol)
            
            if success:
                self.logger.info(f"Removed {symbol} from real-time monitoring")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error removing symbol {symbol} from watchlist: {e}")
            return False
    
    def get_watchlist_symbols(self) -> List[str]:
        """Get current watchlist symbols."""
        return list(self.watchlist_symbols)
    
    def _sync_api_watchlist_to_live_monitor(self):
        """Read api_watchlist.json and sync symbols to live_monitor.json"""
        try:
            # Read symbols from API watchlist
            api_symbols = set()
            try:
                with open('api_watchlist.json', 'r') as f:
                    api_data = json.load(f)
                    
                if 'watchlist' in api_data and 'symbols' in api_data['watchlist']:
                    api_symbols = set(api_data['watchlist']['symbols'])
                    self.logger.debug(f"📋 Found {len(api_symbols)} symbols in API watchlist: {api_symbols}")
                    
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                self.logger.debug(f"No API watchlist found: {e}")
                api_symbols = set()
            
            # Get position symbols (these should always be included)
            position_symbols = set(self.symbols_to_monitor)
            
            # Combine API symbols with position symbols - this is our definitive list
            all_symbols = api_symbols.union(position_symbols)
            
            # Update our internal tracking to match
            self.watchlist_symbols = all_symbols
            
            self.logger.debug(f"🔄 Final symbol list: {sorted(all_symbols)} (API: {len(api_symbols)}, Positions: {len(position_symbols)})")
            
        except Exception as e:
            self.logger.error(f"Error syncing API watchlist to live monitor: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        sys.exit(0)

    def _get_positions_data(self) -> Dict[str, Any]:
        """Get current positions from Schwab account."""
        try:
            positions_data = get_all_positions()
            if not positions_data:
                return {}
            
            formatted_positions = {}
            total_market_value = 0.0
            total_unrealized_pl = 0.0
            
            for account_number, positions in positions_data.items():
                for position in positions:
                    symbol = position.get('symbol', '')
                    if symbol:
                        self.symbols_to_monitor.add(symbol)
                        
                        formatted_positions[symbol] = {
                            'symbol': symbol,
                            'quantity': position.get('quantity', 0),
                            'market_value': position.get('market_value', 0),
                            'cost_basis': position.get('cost_basis', 0),
                            'unrealized_pl': position.get('unrealized_pl', 0),
                            'unrealized_pl_percent': position.get('unrealized_pl_percent', 0),
                            'account': account_number
                        }
                        
                        total_market_value += position.get('market_value', 0)
                        total_unrealized_pl += position.get('unrealized_pl', 0)
            
            return {
                'positions': formatted_positions,
                'summary': {
                    'total_positions': len(formatted_positions),
                    'total_market_value': total_market_value,
                    'total_unrealized_pl': total_unrealized_pl,
                    'symbols_count': len(self.symbols_to_monitor)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            return {}

    def _get_transactions_data(self) -> Dict[str, Any]:
        """Get recent transaction data and P&L analysis."""
        try:
            df = self.transaction_handler.get_all_transactions(days=30, csv_output=False)
            
            if df.empty:
                return {
                    'transactions': [], 
                    'pnl_stats': {},
                    'trading_stats': {
                        'overall': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                        'long': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                        'short': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0}
                    }
                }
            
            # Get P&L stats from existing handler
            pnl_stats = self.pnl_handler.calculate_win_loss_stats(df)
            
            # Get comprehensive trading statistics
            trading_stats = self.transaction_handler.calculate_win_loss_stats(df)
            
            recent_transactions = df.head(10).to_dict('records')
            
            # Convert timestamps to strings for JSON serialization
            for txn in recent_transactions:
                for key, value in txn.items():
                    if hasattr(value, 'isoformat'):
                        txn[key] = value.isoformat()
            
            return {
                'transactions': recent_transactions,
                'pnl_stats': pnl_stats,
                'trading_stats': trading_stats,
                'transaction_count': len(df)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting transactions: {e}")
            return {
                'transactions': [], 
                'pnl_stats': {},
                'trading_stats': {
                    'overall': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                    'long': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                    'short': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0}
                }
            }

    def _run_technical_indicators_script(self, symbols: List[str]):
        """Run the technical_indicators.py script to generate fresh technical_indicators.json in separate thread"""
        def run_indicators_thread():
            try:
                self.logger.info("🧵 [Thread] Running technical_indicators.py to generate fresh indicators...")
                import subprocess
                result = subprocess.run([
                    'python3', 'technical_indicators.py'
                ], capture_output=True, text=True, timeout=120)  # Increased timeout for parallel processing
                
                if result.returncode == 0:
                    self.logger.info("✅ [Thread] Technical indicators script completed successfully")
                else:
                    self.logger.error(f"❌ [Thread] Technical indicators script failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                self.logger.error("❌ [Thread] Technical indicators script timed out after 2 minutes")
            except Exception as e:
                self.logger.error(f"❌ [Thread] Error running technical indicators script: {e}")
        
        # Run in separate thread for non-blocking execution
        indicators_thread = threading.Thread(target=run_indicators_thread, daemon=True)
        indicators_thread.start()
        self.logger.info("🚀 Technical indicators script started in separate thread")

    def _run_strategy_scripts(self, symbols: List[str]):
        """Run all strategy scripts to generate fresh JSON files in parallel threads"""
        def run_strategy_thread(script_name: str, strategy_name: str):
            try:
                self.logger.info(f"🧵 [Thread] Running {script_name} to generate fresh {strategy_name} signals...")
                import subprocess
                result = subprocess.run([
                    'python3', script_name
                ], capture_output=True, text=True, timeout=90)  # Increased timeout for parallel processing
                
                if result.returncode == 0:
                    self.logger.info(f"✅ [Thread] {strategy_name} strategy script completed successfully")
                else:
                    self.logger.error(f"❌ [Thread] {strategy_name} strategy script failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                self.logger.error(f"❌ [Thread] {strategy_name} strategy script timed out after 90 seconds")
            except Exception as e:
                self.logger.error(f"❌ [Thread] Error running {strategy_name} strategy script: {e}")
        
        try:
            scripts = [
                ('iron_condor_strategy.py', 'Iron Condor'),
                ('pml_strategy.py', 'PML'),
                ('divergence_strategy.py', 'Divergence'),
                ('current_positions_handler.py', 'Current Positions')
            ]
            
            # Run all strategy scripts in parallel threads
            strategy_threads = []
            for script_name, strategy_name in scripts:
                thread = threading.Thread(
                    target=run_strategy_thread, 
                    args=(script_name, strategy_name), 
                    daemon=True
                )
                strategy_threads.append(thread)
                thread.start()
                self.logger.info(f"🚀 {strategy_name} script started in separate thread")
            
            self.logger.info(f"🔥 All {len(scripts)} scripts running in parallel threads")
                    
        except Exception as e:
            self.logger.error(f"❌ Error running strategy scripts in parallel: {e}")

    def _get_technical_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Read technical indicators from fresh technical_indicators.json file."""
        try:
            self.logger.info(f"Reading fresh technical indicators for {len(symbols)} symbols from JSON")
            
            # Read technical indicators from the fresh JSON file
            indicators_data = {}
            json_data = self._load_technical_indicators_from_json(symbols)
            
            for symbol in symbols:
                if symbol in json_data:
                    # Get current quote data
                    try:
                        quote = self.options_handler.get_quote(symbol)
                        current_price = 0
                        if quote:
                            if 'quote' in quote and 'lastPrice' in quote['quote']:
                                current_price = quote['quote']['lastPrice']
                            elif 'lastPrice' in quote:
                                current_price = quote['lastPrice']
                        
                        indicators_data[symbol] = {
                            'symbol': symbol,
                            'current_price': current_price,
                            'quote_data': {
                                'bid': quote.get('bidPrice', 0) if quote else 0,
                                'ask': quote.get('askPrice', 0) if quote else 0,
                                'volume': quote.get('totalVolume', 0) if quote else 0,
                                'change': quote.get('netChange', 0) if quote else 0,
                                'change_percent': quote.get('netPercentChangeInDouble', 0) if quote else 0,
                            },
                            'technical_indicators': json_data[symbol],
                            'timestamp': datetime.now().isoformat(),
                            'source': 'fresh_json_file'
                        }
                    except Exception as e:
                        self.logger.error(f"Error getting quote for {symbol}: {e}")
                        indicators_data[symbol] = {
                            'symbol': symbol,
                            'technical_indicators': json_data[symbol],
                            'error': f'Quote error: {str(e)}',
                            'timestamp': datetime.now().isoformat(),
                            'source': 'fresh_json_file'
                        }
                else:
                    self.logger.warning(f"No technical indicators found for {symbol}")
            
            self.logger.info(f"Loaded fresh technical indicators for {len(indicators_data)} symbols")
            return indicators_data
            
        except Exception as e:
            self.logger.error(f"Error reading fresh technical indicators: {e}")
            return {}

    def _load_technical_indicators_from_json(self, symbols: List[str]) -> Dict[str, Any]:
        """Load technical indicators from JSON file as fallback."""
        try:
            import json
            import os
            
            technical_indicators_path = os.path.join(os.path.dirname(__file__), 'technical_indicators.json')
            
            if not os.path.exists(technical_indicators_path):
                self.logger.warning("technical_indicators.json not found")
                return {}
            
            with open(technical_indicators_path, 'r') as f:
                technical_data = json.load(f)
            
            indicators = technical_data.get('indicators', {})
            
            # Filter to only requested symbols
            filtered_indicators = {}
            for symbol in symbols:
                if symbol in indicators:
                    filtered_indicators[symbol] = indicators[symbol]
            
            self.logger.info(f"Loaded {len(filtered_indicators)} technical indicators from JSON file")
            return filtered_indicators
            
        except Exception as e:
            self.logger.error(f"Error loading technical indicators from JSON: {e}")
            return {}

    def _get_account_data(self) -> Dict[str, Any]:
        """Get comprehensive account data."""
        try:
            summaries = self.account_handler.get_all_account_summaries()
            
            if not summaries:
                return {}
            
            primary_account = summaries[0] if summaries else {}
            
            return {
                'account_summary': primary_account,
                'total_accounts': len(summaries),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting account data: {e}")
            return {}

    def _get_market_status(self) -> Dict[str, Any]:
        """Get current market status."""
        now = datetime.now()
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        is_market_hours = market_open <= now <= market_close
        is_weekday = now.weekday() < 5
        
        return {
            'current_time': now.isoformat(),
            'market_open_time': market_open.isoformat(),
            'market_close_time': market_close.isoformat(),
            'is_market_hours': is_market_hours and is_weekday,
            'is_weekday': is_weekday,
            'session_status': 'OPEN' if (is_market_hours and is_weekday) else 'CLOSED',
            'minutes_to_open': max(0, (market_open - now).total_seconds() / 60) if not is_market_hours else 0,
            'minutes_to_close': max(0, (market_close - now).total_seconds() / 60) if is_market_hours else 0
        }

    def _get_iron_condor_signals(self, symbols: List[str]) -> Dict[str, Any]:
        """Read Iron Condor trading signals from fresh iron_condor_signals.json file."""
        try:
            self.logger.info(f"Reading fresh Iron Condor signals for {len(symbols)} symbols from JSON")
            
            # Read Iron Condor signals from the fresh JSON file
            iron_condor_signals_path = os.path.join(os.path.dirname(__file__), 'iron_condor_signals.json')
            
            if not os.path.exists(iron_condor_signals_path):
                self.logger.warning("iron_condor_signals.json not found")
                return {}
            
            with open(iron_condor_signals_path, 'r') as f:
                iron_condor_data = json.load(f)
            
            # Extract just the signals for live_monitor.json
            signals = iron_condor_data.get('signals', {})
            print("iron_condor_signals:", signals)
            
            self.logger.info(f"Loaded fresh Iron Condor signals for {len(signals)} symbols")
            return signals
            
        except Exception as e:
            self.logger.error(f"Error reading fresh Iron Condor signals: {e}")
            return {}

    def _get_pml_signals(self, symbols: List[str]) -> Dict[str, Any]:
        """Read PML trading signals from fresh pml_signals.json file."""
        try:
            self.logger.info(f"Reading fresh PML signals for {len(symbols)} symbols from JSON")
            
            # Read PML signals from the fresh JSON file
            pml_signals_path = os.path.join(os.path.dirname(__file__), 'pml_signals.json')
            
            if not os.path.exists(pml_signals_path):
                self.logger.warning("pml_signals.json not found")
                return {}
            
            with open(pml_signals_path, 'r') as f:
                pml_data = json.load(f)
            
            # Extract just the signals for live_monitor.json
            signals = pml_data.get('signals', {})
            print("pml_signals:", signals)
            
            self.logger.info(f"Loaded fresh PML signals for {len(signals)} symbols")
            return signals
            
        except Exception as e:
            self.logger.error(f"Error reading fresh PML signals: {e}")
            return {}

    def _get_divergence_signals(self, symbols: List[str]) -> Dict[str, Any]:
        """Read Divergence trading signals from fresh divergence_signals.json file."""
        try:
            self.logger.info(f"Reading fresh Divergence signals for {len(symbols)} symbols from JSON")
            
            # Read Divergence signals from the fresh JSON file
            divergence_signals_path = os.path.join(os.path.dirname(__file__), 'divergence_signals.json')
            
            if not os.path.exists(divergence_signals_path):
                self.logger.warning("divergence_signals.json not found")
                return {}
            
            with open(divergence_signals_path, 'r') as f:
                divergence_data = json.load(f)
            
            # Extract just the signals for live_monitor.json
            signals = divergence_data.get('signals', {})
            print("divergence_signals:", signals)   
            
            self.logger.info(f"Loaded fresh Divergence signals for {len(signals)} symbols")
            return signals
            
        except Exception as e:
            self.logger.error(f"Error reading fresh Divergence signals: {e}")
            return {}

    def _get_current_positions_data(self) -> Dict[str, Any]:
        """Read current positions data from current_positions.json file and join with watchlist."""
        try:
            self.logger.info("Reading current positions data from current_positions.json")
            
            # Read current positions from the JSON file
            current_positions_path = os.path.join(os.path.dirname(__file__), 'current_positions.json')
            
            if not os.path.exists(current_positions_path):
                self.logger.warning("current_positions.json not found - returning empty positions")
                return {
                    'positions': {},
                    'summary': {
                        'total_positions': 0,
                        'total_market_value': 0.0,
                        'total_unrealized_pl': 0.0,
                        'total_day_pl': 0.0,
                        'total_cost_basis': 0.0,
                        'accounts_count': 0,
                        'symbols_count': 0,
                        'winning_positions': 0,
                        'losing_positions': 0,
                        'break_even_positions': 0
                    },
                    'accounts': {},
                    'symbols': [],
                    'last_updated': datetime.now().isoformat(),
                    'fetch_success': False,
                    'data_source': 'current_positions_handler'
                }
            
            with open(current_positions_path, 'r') as f:
                positions_data = json.load(f)
            
            # Extract positions data for live_monitor.json
            positions_info = {
                'positions': positions_data.get('positions', {}),
                'summary': positions_data.get('summary', {}),
                'accounts': positions_data.get('accounts', {}),
                'symbols': positions_data.get('symbols', []),
                'last_updated': positions_data.get('last_updated'),
                'fetch_success': positions_data.get('fetch_success', False),
                'data_source': 'current_positions_handler'
            }
            
            # Update symbols_to_monitor with position symbols for watchlist integration
            if positions_data.get('symbols'):
                for symbol in positions_data['symbols']:
                    self.symbols_to_monitor.add(symbol)
                    # Also add to watchlist for integrated monitoring
                    self.watchlist_symbols.add(symbol)
                    
                self.logger.info(f"Added {len(positions_data['symbols'])} position symbols to watchlist: {positions_data['symbols']}")
            
            self.logger.info(f"Loaded current positions data: {len(positions_info.get('positions', {}))} positions")
            return positions_info
            
        except Exception as e:
            self.logger.error(f"Error reading current positions data: {e}")
            return {
                'positions': {},
                'summary': {
                    'total_positions': 0,
                    'total_market_value': 0.0,
                    'total_unrealized_pl': 0.0,
                    'total_day_pl': 0.0,
                    'total_cost_basis': 0.0,
                    'accounts_count': 0,
                    'symbols_count': 0,
                    'winning_positions': 0,
                    'losing_positions': 0,
                    'break_even_positions': 0
                },
                'accounts': {},
                'symbols': [],
                'last_updated': datetime.now().isoformat(),
                'fetch_success': False,
                'data_source': 'current_positions_handler',
                'error': str(e)
            }

    def _get_integrated_watchlist_data(self) -> Dict[str, Any]:
        """Get integrated watchlist data from symbols monitor handler."""
        try:
            # Use the symbols monitor handler to get integrated watchlist data
            # This now handles both api_watchlist.json and current_positions.json
            symbols = self.symbols_monitor.get_watchlist_symbols()
            watchlist_data = self.symbols_monitor.get_watchlist_data()
            
            if not symbols:
                return {}
            
            # Create integrated data structure
            integrated_data = {
                'symbols': symbols,
                'watchlist_data': watchlist_data,
                'metadata': {
                    'total_symbols': len(symbols),
                    'last_updated': datetime.now().isoformat(),
                    'update_source': 'symbols_monitor_handler'
                }
            }
            
            self.logger.info(f"Retrieved integrated watchlist data for {len(symbols)} symbols from symbols monitor handler")
            return integrated_data
            
        except Exception as e:
            self.logger.error(f"Error getting integrated watchlist data: {e}")
            return {}

    def _update_options_data_if_needed(self, symbols: List[str]) -> Dict[str, Any]:
        """Update options data if enough time has passed since last update."""
        try:
            current_time = time.time()
            
            # Check if it's time to update options data
            if current_time - self.last_options_update >= self.options_update_interval:
                self.logger.info(f"🔍 Updating options data for {len(symbols)} symbols (every {self.options_update_interval}s)")
                
                # Run options data update
                options_data = self.options_handler.run_options_data_update(
                    symbols=symbols,
                    output_json=True
                )
                
                # Update last update time
                self.last_options_update = current_time
                
                if options_data:
                    total_contracts = sum(
                        symbol_data.get('total_contracts', 0) 
                        for symbol_data in options_data.get('symbols', {}).values()
                    )
                    self.logger.info(f"✅ Options data updated: {total_contracts} contracts processed")
                    
                    return {
                        'last_updated': datetime.now().isoformat(),
                        'symbols_processed': len(options_data.get('symbols', {})),
                        'total_contracts': total_contracts,
                        'update_interval_seconds': self.options_update_interval,
                        'status': 'updated'
                    }
                else:
                    self.logger.warning("❌ Options data update failed")
                    return {
                        'last_updated': datetime.now().isoformat(),
                        'status': 'failed',
                        'error': 'No data returned from options handler'
                    }
            else:
                # Not time to update yet
                time_until_next = self.options_update_interval - (current_time - self.last_options_update)
                return {
                    'status': 'skipped',
                    'time_until_next_update': round(time_until_next, 1),
                    'last_updated': datetime.fromtimestamp(self.last_options_update).isoformat() if self.last_options_update > 0 else 'never'
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error updating options data: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'last_updated': datetime.now().isoformat()
            }

    def collect_all_data_concurrently(self) -> Dict[str, Any]:
        """Collect all data concurrently using ThreadPoolExecutor."""
        try:
            # Use ThreadPoolExecutor for concurrent data collection
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all data collection tasks
                futures = {
                    'positions': executor.submit(self._get_current_positions_data),
                    'transactions': executor.submit(self._get_transactions_data),
                    'account': executor.submit(self._get_account_data),
                    'market_status': executor.submit(self._get_market_status),
                    'watchlist': executor.submit(self._get_integrated_watchlist_data)
                }
                
                # Collect results as they complete
                results = {}
                for name, future in futures.items():
                    try:
                        results[name] = future.result(timeout=10)  # 10 second timeout
                    except Exception as e:
                        self.logger.error(f"Error collecting {name} data: {e}")
                        results[name] = {}
                
                # Get symbols from positions and integrated watchlist for technical analysis
                all_symbols = self.symbols_to_monitor.union(self.watchlist_symbols)
                symbols = list(all_symbols) if all_symbols else []
                
                # Log the symbol integration
                self.logger.info(f"📊 Integrated symbols: Positions({len(self.symbols_to_monitor)}) + Watchlist({len(self.watchlist_symbols)}) = Total({len(symbols)})")
                if symbols:
                    self.logger.info(f"🎯 Monitoring symbols: {sorted(symbols)}")
                    
                    # First, run the scripts to generate fresh JSON files
                    self._run_technical_indicators_script(symbols)
                    self._run_strategy_scripts(symbols)
                    
                    # Then read the fresh JSON files
                    results['technical'] = self._get_technical_data(symbols[:3])  # Limit to 3 symbols to avoid rate limits
                    results['iron_condor_signals'] = self._get_iron_condor_signals(symbols)
                    results['pml_signals'] = self._get_pml_signals(symbols)
                    results['divergence_signals'] = self._get_divergence_signals(symbols)
                    
                    # Get options data (every 10 seconds to avoid rate limits)
                    results['options_data'] = self._update_options_data_if_needed(symbols[:2])  # Limit to 2 symbols for options
                else:
                    self.logger.warning("⚠️ No symbols found - pausing strategy scripts and technical analysis")
                    # Return empty results for strategies when no symbols are available
                    results['technical'] = {}
                    results['iron_condor_signals'] = {}
                    results['pml_signals'] = {}
                    results['divergence_signals'] = {}
                    results['options_data'] = {
                        'status': 'paused',
                        'reason': 'no_symbols_available',
                        'last_updated': datetime.now().isoformat()
                    }
            
            # Compile comprehensive data
            comprehensive_data = {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'update_interval_seconds': self.update_interval,
                    'symbols_monitored': list(symbols),
                    'data_sources': [
                        'schwab_positions',
                        'schwab_transactions', 
                        'schwab_account_data',
                        'technical_indicators',
                        'iron_condor_signals',
                        'pml_signals',
                        'divergence_signals',
                        'watchlist_data',
                        'market_quotes',
                        'options_data'
                    ]
                },
                'market_status': results.get('market_status', {}),
                'account_data': results.get('account', {}),
                'positions': results.get('positions', {}),
                'transactions': results.get('transactions', {}),
                'technical_indicators': results.get('technical', {}),
                'iron_condor_signals': results.get('iron_condor_signals', {}),
                'pml_signals': results.get('pml_signals', {}),
                'divergence_signals': results.get('divergence_signals', {}),
                'integrated_watchlist': results.get('watchlist', {}),
                'options_data': results.get('options_data', {}),
                'system_status': {
                    'monitor_running': self.running,
                    'symbols_count': len(symbols),
                    'positions_count': results.get('positions', {}).get('summary', {}).get('total_positions', 0)
                }
            }
            
            return comprehensive_data
            
        except Exception as e:
            self.logger.error(f"Error collecting comprehensive data: {e}")
            return {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                },
                'error': True
            }

    def update_data(self):
        """Update all data concurrently in a thread-safe manner."""
        try:
            new_data = self.collect_all_data_concurrently()
            
            with self.data_lock:
                self.current_data = new_data
                
        except Exception as e:
            self.logger.error(f"Error updating data: {e}")

    def get_current_data(self) -> Dict[str, Any]:
        """Get current data in a thread-safe manner."""
        with self.data_lock:
            return self.current_data.copy()
    
    def _start_database_inserter_thread(self):
        """Start the centralized database inserter in a separate thread."""
        def db_inserter_worker():
            """Database inserter worker thread that runs continuously."""
            self.logger.info("🗄️ Database inserter thread started")
            
            while self.db_inserter_running:
                try:
                    # Use the throttling method to check if it's time to insert
                    results = self.db_inserter.run_with_throttling()
                    
                    if results:  # Only log if insertions actually happened
                        successful = sum(1 for success in results.values() if success)
                        total = len(results)
                        self.logger.info(f"🗄️ Database insertion completed: {successful}/{total} successful")
                        
                        # Log any failures
                        for file_type, success in results.items():
                            if not success:
                                self.logger.warning(f"🗄️ Database insertion failed for: {file_type}")
                    
                    # Sleep for 1 second before checking again
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"🗄️ Error in database inserter thread: {e}")
                    time.sleep(5)  # Wait longer on error
            
            self.logger.info("🗄️ Database inserter thread stopped")
        
        # Start the database inserter thread
        self.db_inserter_running = True
        self.db_inserter_thread = threading.Thread(target=db_inserter_worker, daemon=True)
        self.db_inserter_thread.start()
        self.logger.info("🗄️ Started centralized database inserter thread (30-second intervals)")

    def _stop_database_inserter_thread(self):
        """Stop the database inserter thread."""
        if self.db_inserter_running:
            self.logger.info("🗄️ Stopping database inserter thread...")
            self.db_inserter_running = False
            if self.db_inserter_thread and self.db_inserter_thread.is_alive():
                self.db_inserter_thread.join(timeout=5)
            self.logger.info("🗄️ Database inserter thread stopped")

    def start_continuous_monitoring(self):
        """Start continuous monitoring with concurrent data collection."""
        self.logger.info(f"Starting continuous monitor with {self.update_interval}s interval")
        self.logger.info("All data will be collected concurrently")
        self.running = True
        
        # Start the centralized database inserter thread
        self._start_database_inserter_thread()
        
        # Initial data collection
        self.update_data()
        
        try:
            while self.running:
                start_time = time.time()
                
                # Update all data concurrently
                self.update_data()
                
                # Get current data and output
                data = self.get_current_data()
                
                # Write JSON to live monitor file
                try:
                    with open('live_monitor.json', 'w') as f:
                        json.dump(data, f, indent=2, default=str)
                        
                except Exception as e:
                    self.logger.error(f"Error writing JSON: {e}")
                
                # Database insertion functionality removed - now handled by individual strategy files
                
                
                # Maintain update interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.running = False
            # Stop the database inserter thread
            self._stop_database_inserter_thread()
            self.logger.info("Monitor stopped")


def main():
    """Main function - simplified for continuous operation only."""
    print("Real-time P&L and Technical Indicators Monitor")
    print("Continuous mode with concurrent data collection")
    print("Press Ctrl+C to stop")
    print("="*50)
    
    # Create and start monitor with shorter interval
    monitor = RealTimeMonitor(update_interval=0.5)
    monitor.start_continuous_monitoring()


if __name__ == "__main__":
    main()
