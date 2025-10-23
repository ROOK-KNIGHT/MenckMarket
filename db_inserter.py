#!/usr/bin/env python3
"""
Centralized Database Inserter

This handler manages all database insertions from JSON files:
1. Reads data from all strategy JSON files
2. Handles database connections and insertions
3. Uses DELETE instead of TRUNCATE for parallel execution safety
4. Provides centralized error handling and logging
5. Supports throttling and batch operations

JSON Files Processed:
- iron_condor_signals.json
- pml_signals.json  
- divergence_signals.json
- current_positions.json
- options_data.json
- technical_indicators.json
"""

import json
import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

class DatabaseInserter:
    """
    Centralized database inserter for all JSON data files
    """
    
    def __init__(self):
        """Initialize the database inserter."""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Database connection parameters
        self.db_config = {
            'host': 'localhost',
            'database': 'volflow_options',
            'user': 'isaac',
            'password': None  # Will use peer authentication
        }
        
        # JSON file paths
        self.json_files = {
            'iron_condor_signals': 'iron_condor_signals.json',
            'pml_signals': 'pml_signals.json',
            'divergence_signals': 'divergence_signals.json',
            'current_positions': 'current_positions.json',
            'technical_indicators': 'technical_indicators.json',
            'pnl_statistics': 'pnl_statistics.json',
            'transactions': 'transactions.json',
            'account_data': 'account_data.json'
        }
        
        self.logger.info("DatabaseInserter initialized")
        self.logger.info(f"Monitoring {len(self.json_files)} JSON files")

    def load_json_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Load data from a JSON file with retry logic to handle file access conflicts."""
        import time
        
        max_retries = 3
        retry_delay = 0.1  # 100ms delay between retries
        
        for attempt in range(max_retries):
            try:
                if not os.path.exists(file_path):
                    self.logger.warning(f"JSON file not found: {file_path}")
                    return None
                
                # Use a shorter timeout for file operations to prevent blocking
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if attempt > 0:
                    self.logger.info(f"Successfully loaded JSON file on attempt {attempt + 1}: {file_path}")
                else:
                    self.logger.debug(f"Loaded JSON file: {file_path}")
                return data
                
            except (IOError, OSError, PermissionError) as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"File access conflict for {file_path} (attempt {attempt + 1}), retrying in {retry_delay}s: {e}")
                    time.sleep(retry_delay)
                    continue
                else:
                    self.logger.error(f"Failed to load JSON file {file_path} after {max_retries} attempts: {e}")
                    return None
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error in {file_path}: {e}")
                return None
            except Exception as e:
                self.logger.error(f"Unexpected error loading JSON file {file_path}: {e}")
                return None
        
        return None

    def insert_iron_condor_signals(self, data: Dict[str, Any]) -> bool:
        """Insert iron condor signals into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Use DELETE instead of TRUNCATE for parallel execution safety
            cur.execute("DELETE FROM iron_condor_signals;")
            
            signals = data.get('signals', {})
            insert_count = 0
            
            for symbol, signal_data in signals.items():
                try:
                    # Simplified insert matching actual JSON structure
                    insert_sql = """
                    INSERT INTO iron_condor_signals (
                        timestamp, symbol, signal_type, confidence, entry_reason,
                        position_size, stop_loss, profit_target, market_condition, volatility_environment,
                        auto_approve
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        signal_data.get('timestamp', datetime.now().isoformat()),
                        symbol,
                        signal_data.get('signal_type', 'NO_SIGNAL'),
                        signal_data.get('confidence', 0.0),
                        signal_data.get('entry_reason', ''),
                        signal_data.get('position_size', 0.0),
                        signal_data.get('stop_loss', 0.0),
                        signal_data.get('profit_target', 0.0),
                        signal_data.get('market_condition', 'UNCERTAIN'),
                        signal_data.get('volatility_environment', 'Unknown'),
                        signal_data.get('auto_approve', True)
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting iron condor signal {symbol}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} iron condor signals")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting iron condor signals: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_pml_signals(self, data: Dict[str, Any]) -> bool:
        """Insert PML signals into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM pml_signals;")
            
            signals = data.get('signals', {})
            insert_count = 0
            
            for symbol, signal_data in signals.items():
                try:
                    # Simplified insert matching actual JSON structure
                    insert_sql = """
                    INSERT INTO pml_signals (
                        timestamp, symbol, signal_type, confidence, entry_reason,
                        position_size, stop_loss, profit_target, market_condition, volatility_environment,
                        auto_approve
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        signal_data.get('timestamp', datetime.now().isoformat()),
                        symbol,
                        signal_data.get('signal_type', 'NO_SIGNAL'),
                        signal_data.get('confidence', 0.0),
                        signal_data.get('entry_reason', ''),
                        signal_data.get('position_size', 0.0),
                        signal_data.get('stop_loss', 0.0),
                        signal_data.get('profit_target', 0.0),
                        signal_data.get('market_condition', 'UNCERTAIN'),
                        signal_data.get('volatility_environment', 'Unknown'),
                        signal_data.get('auto_approve', True)
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting PML signal {symbol}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} PML signals")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting PML signals: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_divergence_signals(self, data: Dict[str, Any]) -> bool:
        """Insert divergence signals into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM divergence_signals;")
            
            signals = data.get('signals', {})
            insert_count = 0
            
            for symbol, signal_data in signals.items():
                try:
                    # Simplified insert matching actual JSON structure
                    insert_sql = """
                    INSERT INTO divergence_signals (
                        timestamp, symbol, signal_type, confidence, entry_reason,
                        position_size, stop_loss, profit_target, market_condition, volatility_environment,
                        auto_approve
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        signal_data.get('timestamp', datetime.now().isoformat()),
                        symbol,
                        signal_data.get('signal_type', 'NO_SIGNAL'),
                        signal_data.get('confidence', 0.0),
                        signal_data.get('entry_reason', ''),
                        signal_data.get('position_size', 0.0),
                        signal_data.get('stop_loss', 0.0),
                        signal_data.get('profit_target', 0.0),
                        signal_data.get('market_condition', 'UNCERTAIN'),
                        signal_data.get('volatility_environment', 'Unknown'),
                        signal_data.get('auto_approve', True)
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting divergence signal {symbol}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} divergence signals")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting divergence signals: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_current_positions(self, data: Dict[str, Any]) -> bool:
        """Insert current positions into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM positions;")
            
            positions = data.get('positions', {})
            insert_count = 0
            
            for position_key, position in positions.items():
                try:
                    insert_sql = """
                    INSERT INTO positions (
                        timestamp, symbol, quantity, market_value, cost_basis,
                        unrealized_pl, unrealized_pl_percent, account
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        datetime.now(),
                        position.get('symbol'),
                        position.get('quantity', 0),
                        position.get('market_value', 0),
                        position.get('cost_basis', 0),
                        position.get('unrealized_pl', 0),
                        position.get('unrealized_pl_percent', 0),
                        position.get('account')
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting position {position_key}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} positions")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting positions: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_technical_indicators(self, data: Dict[str, Any]) -> bool:
        """Insert technical indicators into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM technical_indicators;")
            
            indicators = data.get('indicators', {})
            insert_count = 0
            
            for symbol, indicator_data in indicators.items():
                try:
                    if 'error' in indicator_data:
                        continue
                    
                    base_indicators = indicator_data.get('base_indicators', {})
                    pml_indicators = indicator_data.get('pml_indicators', {})
                    iron_condor_indicators = indicator_data.get('iron_condor_indicators', {})
                    divergence_indicators = indicator_data.get('divergence_indicators', {})
                    
                    insert_sql = """
                    INSERT INTO technical_indicators (
                        timestamp, symbol, current_price, sma_20, sma_50, ema_9, ema_21, ema_50,
                        rsi, macd, macd_signal, macd_histogram, atr, realized_volatility,
                        volume, avg_volume, relative_volume_pct, trend_direction, trend_strength,
                        pml_price, ceiling_price, floor_price, green_line_price,
                        call_delta, put_delta, net_delta, pml_cross_bullish, delta_confirmation,
                        iv_rank, range_pct, recent_high, recent_low, is_range_bound,
                        is_low_volatility, is_suitable_for_ic, bullish_divergence_detected,
                        bearish_divergence_detected, divergence_strength, support_levels,
                        resistance_levels, swing_highs, swing_lows
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    """
                    
                    def convert_numpy_types(obj):
                        """Convert numpy types to native Python types for JSON serialization"""
                        import numpy as np
                        if isinstance(obj, np.integer):
                            return int(obj)
                        elif isinstance(obj, np.floating):
                            return float(obj)
                        elif isinstance(obj, np.bool_):
                            return bool(obj)
                        elif isinstance(obj, np.ndarray):
                            return obj.tolist()
                        elif isinstance(obj, dict):
                            return {key: convert_numpy_types(value) for key, value in obj.items()}
                        elif isinstance(obj, list):
                            return [convert_numpy_types(item) for item in obj]
                        else:
                            return obj
                    
                    values = (
                        datetime.now().isoformat(),
                        symbol,
                        float(base_indicators.get('current_price', 0.0)),
                        float(base_indicators.get('sma_20', 0.0)),
                        float(base_indicators.get('sma_50', 0.0)),
                        float(base_indicators.get('ema_9', 0.0)),
                        float(base_indicators.get('ema_21', 0.0)),
                        float(base_indicators.get('ema_50', 0.0)),
                        float(base_indicators.get('rsi', 50.0)),
                        float(base_indicators.get('macd', 0.0)),
                        float(base_indicators.get('macd_signal', 0.0)),
                        float(base_indicators.get('macd_histogram', 0.0)),
                        float(base_indicators.get('atr', 0.0)),
                        float(base_indicators.get('realized_volatility', 20.0)),
                        int(base_indicators.get('volume', 0)),
                        float(base_indicators.get('avg_volume', 0.0)),
                        float(base_indicators.get('relative_volume_pct', 100.0)),
                        str(base_indicators.get('trend_direction', 'neutral')),
                        float(base_indicators.get('trend_strength', 0.0)),
                        float(pml_indicators.get('pml_price', 0.0)),
                        float(pml_indicators.get('ceiling_price', 0.0)),
                        float(pml_indicators.get('floor_price', 0.0)),
                        float(pml_indicators.get('green_line_price', 0.0)),
                        float(pml_indicators.get('call_delta', 0.0)),
                        float(pml_indicators.get('put_delta', 0.0)),
                        float(pml_indicators.get('net_delta', 0.0)),
                        bool(pml_indicators.get('pml_cross_bullish', False)),
                        bool(pml_indicators.get('delta_confirmation', False)),
                        float(iron_condor_indicators.get('iv_rank', 50.0)),
                        float(iron_condor_indicators.get('range_pct', 0.1)),
                        float(iron_condor_indicators.get('recent_high', 0.0)),
                        float(iron_condor_indicators.get('recent_low', 0.0)),
                        bool(iron_condor_indicators.get('is_range_bound', False)),
                        bool(iron_condor_indicators.get('is_low_volatility', False)),
                        bool(iron_condor_indicators.get('is_suitable_for_ic', False)),
                        bool(divergence_indicators.get('bullish_divergence_detected', False)),
                        bool(divergence_indicators.get('bearish_divergence_detected', False)),
                        str(divergence_indicators.get('divergence_strength', 'medium')),
                        json.dumps(convert_numpy_types(divergence_indicators.get('support_levels', []))),
                        json.dumps(convert_numpy_types(divergence_indicators.get('resistance_levels', []))),
                        json.dumps(convert_numpy_types(divergence_indicators.get('swing_highs', []))),
                        json.dumps(convert_numpy_types(divergence_indicators.get('swing_lows', [])))
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting technical indicators {symbol}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} technical indicators")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting technical indicators: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_integrated_watchlist(self, data: Dict[str, Any]) -> bool:
        """Insert integrated watchlist into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM integrated_watchlist;")
            
            # Handle the new integrated watchlist format from symbols_monitor_handler
            watchlist_data = data.get('watchlist_data', {})
            
            if not watchlist_data:
                self.logger.warning("No watchlist_data found in integrated_watchlist.json")
                return False
            
            insert_count = 0
            
            for symbol, symbol_data in watchlist_data.items():
                try:
                    # Determine market status based on current time
                    now = datetime.now()
                    current_hour = now.hour
                    if 9 <= current_hour < 16:
                        market_status = "Open"
                    elif 16 <= current_hour < 20:
                        market_status = "After Hours"
                    else:
                        market_status = "Closed"
                    
                    insert_sql = """
                    INSERT INTO integrated_watchlist (
                        timestamp, symbol, current_price, price_change, price_change_percent,
                        volume, market_cap, last_updated, high_52_week, low_52_week,
                        avg_volume, pe_ratio, dividend_yield, market_status, sector, industry
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        datetime.now().isoformat(),
                        symbol,
                        float(symbol_data.get('current_price', 0.0)),
                        float(symbol_data.get('price_change', 0.0)),
                        float(symbol_data.get('price_change_percent', 0.0)),
                        int(symbol_data.get('volume', 0)),
                        symbol_data.get('market_cap'),
                        symbol_data.get('last_updated', datetime.now().isoformat()),
                        0.0,  # high_52_week - would need additional data
                        0.0,  # low_52_week - would need additional data
                        0,    # avg_volume - would need additional data
                        0.0,  # pe_ratio - would need additional data
                        0.0,  # dividend_yield - would need additional data
                        market_status,
                        'Unknown',  # sector - would need additional data
                        'Unknown'   # industry - would need additional data
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                    # Log additional info for symbols with position details
                    if 'position_details' in symbol_data:
                        pos_details = symbol_data['position_details']
                        self.logger.debug(f"Symbol {symbol}: Position {pos_details.get('quantity', 0)} shares, "
                                        f"P&L: ${pos_details.get('unrealized_pl', 0):.2f}")
                    
                except Exception as e:
                    self.logger.error(f"Error inserting watchlist symbol {symbol}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} integrated watchlist symbols")
            
            # Log summary of data sources
            metadata = data.get('metadata', {})
            api_symbols = metadata.get('api_watchlist_symbols', 0)
            position_symbols = metadata.get('position_symbols', 0)
            self.logger.info(f"📊 Data sources: API watchlist ({api_symbols}) + Positions ({position_symbols}) = Total ({insert_count})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting integrated watchlist: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def process_all_json_files(self) -> Dict[str, bool]:
        """Process all JSON files and insert into database."""
        results = {}
        self.logger.info("🔄 Starting centralized database insertion process...")
        
        # Iron Condor Signals
        if os.path.exists(self.json_files['iron_condor_signals']):
            data = self.load_json_file(self.json_files['iron_condor_signals'])
            if data:
                results['iron_condor_signals'] = self.insert_iron_condor_signals(data)
            else:
                results['iron_condor_signals'] = False
        else:
            self.logger.warning(f"Iron Condor signals file not found: {self.json_files['iron_condor_signals']}")
            results['iron_condor_signals'] = False
        
        # PML Signals
        if os.path.exists(self.json_files['pml_signals']):
            data = self.load_json_file(self.json_files['pml_signals'])
            if data:
                results['pml_signals'] = self.insert_pml_signals(data)
            else:
                results['pml_signals'] = False
        else:
            self.logger.warning(f"PML signals file not found: {self.json_files['pml_signals']}")
            results['pml_signals'] = False
        
        # Divergence Signals
        if os.path.exists(self.json_files['divergence_signals']):
            data = self.load_json_file(self.json_files['divergence_signals'])
            if data:
                results['divergence_signals'] = self.insert_divergence_signals(data)
            else:
                results['divergence_signals'] = False
        else:
            self.logger.warning(f"Divergence signals file not found: {self.json_files['divergence_signals']}")
            results['divergence_signals'] = False
        
        # Current Positions
        if os.path.exists(self.json_files['current_positions']):
            data = self.load_json_file(self.json_files['current_positions'])
            if data:
                results['current_positions'] = self.insert_current_positions(data)
            else:
                results['current_positions'] = False
        else:
            self.logger.warning(f"Current positions file not found: {self.json_files['current_positions']}")
            results['current_positions'] = False
        
        # Technical Indicators
        if os.path.exists(self.json_files['technical_indicators']):
            data = self.load_json_file(self.json_files['technical_indicators'])
            if data:
                results['technical_indicators'] = self.insert_technical_indicators(data)
            else:
                results['technical_indicators'] = False
        else:
            self.logger.warning(f"Technical indicators file not found: {self.json_files['technical_indicators']}")
            results['technical_indicators'] = False
        
        # PnL Statistics
        if os.path.exists(self.json_files['pnl_statistics']):
            data = self.load_json_file(self.json_files['pnl_statistics'])
            if data:
                results['pnl_statistics'] = self.insert_pnl_statistics(data)
            else:
                results['pnl_statistics'] = False
        else:
            self.logger.warning(f"PnL statistics file not found: {self.json_files['pnl_statistics']}")
            results['pnl_statistics'] = False
        
        # Transactions
        if os.path.exists(self.json_files['transactions']):
            data = self.load_json_file(self.json_files['transactions'])
            if data:
                results['transactions'] = self.insert_transactions(data)
            else:
                results['transactions'] = False
        else:
            self.logger.warning(f"Transactions file not found: {self.json_files['transactions']}")
            results['transactions'] = False
        
        # Account Data
        if os.path.exists(self.json_files['account_data']):
            data = self.load_json_file(self.json_files['account_data'])
            if data:
                results['account_data'] = self.insert_account_data(data)
            else:
                results['account_data'] = False
        else:
            self.logger.warning(f"Account data file not found: {self.json_files['account_data']}")
            results['account_data'] = False
        
        # Integrated Watchlist (from symbols monitor handler)
        integrated_watchlist_file = 'integrated_watchlist.json'
        if os.path.exists(integrated_watchlist_file):
            data = self.load_json_file(integrated_watchlist_file)
            if data:
                results['integrated_watchlist'] = self.insert_integrated_watchlist(data)
            else:
                results['integrated_watchlist'] = False
        else:
            self.logger.warning(f"Integrated watchlist file not found: {integrated_watchlist_file}")
            results['integrated_watchlist'] = False
        
        # Summary
        successful_insertions = sum(1 for success in results.values() if success)
        total_files = len(results)
        
        self.logger.info(f"✅ Database insertion completed: {successful_insertions}/{total_files} successful")
        
        return results

    def insert_pnl_statistics(self, data: Dict[str, Any]) -> bool:
        """Insert PnL statistics into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM pnl_statistics;")
            
            statistics = data.get('statistics', {})
            insert_count = 0
            
            # Get overall performance data (main record)
            overall_data = statistics.get('overall_performance', {})
            long_data = statistics.get('long_performance', {})
            short_data = statistics.get('short_performance', {})
            
            if overall_data:
                try:
                    insert_sql = """
                    INSERT INTO pnl_statistics (
                        timestamp, overall_wins, overall_losses, overall_profit_loss, overall_win_rate,
                        overall_avg_win, overall_avg_loss, overall_win_loss_ratio,
                        long_wins, long_losses, long_profit_loss, long_win_rate,
                        long_avg_win, long_avg_loss,
                        short_wins, short_losses, short_profit_loss, short_win_rate,
                        short_avg_win, short_avg_loss
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        datetime.now(),
                        overall_data.get('winning_trades', 0),
                        overall_data.get('losing_trades', 0),
                        overall_data.get('total_pnl', 0.0),
                        overall_data.get('win_rate', 0.0),
                        overall_data.get('avg_win', 0.0),
                        overall_data.get('avg_loss', 0.0),
                        overall_data.get('profit_factor', 0.0),
                        long_data.get('winning_trades', 0),
                        long_data.get('losing_trades', 0),
                        long_data.get('total_pnl', 0.0),
                        long_data.get('win_rate', 0.0),
                        long_data.get('avg_win', 0.0),
                        long_data.get('avg_loss', 0.0),
                        short_data.get('winning_trades', 0),
                        short_data.get('losing_trades', 0),
                        short_data.get('total_pnl', 0.0),
                        short_data.get('win_rate', 0.0),
                        short_data.get('avg_win', 0.0),
                        short_data.get('avg_loss', 0.0)
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting PnL statistics: {e}")
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} PnL statistics")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting PnL statistics: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_transactions(self, data: Dict[str, Any]) -> bool:
        """Insert transactions into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM transactions;")
            
            transactions = data.get('transactions', [])
            insert_count = 0
            
            for transaction in transactions:
                try:
                    insert_sql = """
                    INSERT INTO transactions (
                        timestamp, transaction_id, symbol, transaction_type, quantity,
                        price, amount, fees, account
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        transaction.get('timestamp', datetime.now()),
                        transaction.get('transaction_id', ''),
                        transaction.get('symbol', ''),
                        transaction.get('transaction_type', 'UNKNOWN'),
                        transaction.get('quantity', 0),
                        transaction.get('price', 0.0),
                        transaction.get('amount', 0.0),
                        transaction.get('fees', 0.0),
                        transaction.get('account', 'Unknown')
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting transaction: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} transactions")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting transactions: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def insert_account_data(self, data: Dict[str, Any]) -> bool:
        """Insert account data into database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("DELETE FROM account_data;")
            
            account_data = data.get('account_data', {})
            insert_count = 0
            
            for account_number, account_record in account_data.items():
                try:
                    insert_sql = """
                    INSERT INTO account_data (
                        timestamp, account_number, total_count, total_market_value, total_unrealized_pl,
                        total_day_pl, equity, buying_power, available_funds, day_trading_buying_power,
                        stock_buying_power, option_buying_power, is_day_trader, is_closing_only_restricted,
                        round_trips, pfcb_flag, maintenance_requirement, equity_percentage,
                        margin_balance, is_in_call
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        datetime.now(),
                        account_record.get('account_number', account_number),
                        account_record.get('total_count', 0),
                        account_record.get('total_market_value', 0.0),
                        account_record.get('total_unrealized_pl', 0.0),
                        account_record.get('total_day_pl', 0.0),
                        account_record.get('equity', 0.0),
                        account_record.get('buying_power', 0.0),
                        account_record.get('available_funds', 0.0),
                        account_record.get('day_trading_buying_power', 0.0),
                        account_record.get('stock_buying_power', 0.0),
                        account_record.get('option_buying_power', 0.0),
                        account_record.get('is_day_trader', False),
                        account_record.get('is_closing_only_restricted', False),
                        account_record.get('round_trips', 0),
                        account_record.get('pfcb_flag', False),
                        account_record.get('maintenance_requirement', 0.0),
                        account_record.get('equity_percentage', 0.0),
                        account_record.get('margin_balance', 0.0),
                        account_record.get('is_in_call', 0)
                    )
                    
                    cur.execute(insert_sql, values)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error inserting account data {account_number}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.logger.info(f"✅ Inserted {insert_count} account data records")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting account data: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    # Removed run_with_throttling method - timing now controlled by realtime_monitor.py


def main():
    """Main function to run centralized database insertion"""
    print("Centralized Database Inserter")
    print("=" * 40)
    
    # Initialize database inserter
    inserter = DatabaseInserter()
    
    # Process all JSON files and insert into database
    results = inserter.process_all_json_files()
    
    # Display results
    print(f"\n📊 Database Insertion Results:")
    for file_type, success in results.items():
        status = "✅ Success" if success else "❌ Failed"
        print(f"   {file_type}: {status}")
    
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    print(f"\n🎯 Overall: {successful}/{total} successful insertions")


if __name__ == "__main__":
    main()
