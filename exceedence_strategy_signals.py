#!/usr/bin/env python3
"""
Simplified Exceedence Strategy Signal Generator

This handler provides exceedence-based trading signals for LONG positions only by consuming
exceedance indicators from exceedance_data/exceedance_indicators_5min.json:

1. Only 5-minute timeframe
2. Long positions only (no short/reversal logic)
3. No ATR, confidence levels, stop loss, or risk-reward logic
4. Uses PML trading config for risk management
5. Position sizing similar to divergence strategy
6. Trading logic follows exceedence_strategy.py approach

Simplified Strategy Overview:
- Load exceedance indicators from 5min JSON file
- Generate BUY signals when price is at lower band (position_in_range <= 5%) or upper band (>= 95%)
- Use PML strategy risk management settings
- Ultra-parallel processing for time-sensitive signals
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
import json
import os
import time 
import pytz
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum

# Import trading engine for execution
from exceedance_trading_engine import ExceedanceTradingEngine
import hashlib

class ExceedenceStrategy:
    """
    Simplified Exceedence Strategy Signal Generator for long positions only
    """
    
    def __init__(self):
        """Initialize the simplified Exceedence strategy signal generator."""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Load PML trading configuration
        self.trading_config = self._load_trading_config()
        self.pml_config = self.trading_config.get('strategies', {}).get('pml', {})
        
        # Get risk management settings from PML config
        self.risk_mgmt = self.pml_config.get('risk_management', {})
        self.strategy_allocation_pct = self.risk_mgmt.get('strategy_allocation', 20.0) / 100.0
        self.position_size_pct = self.risk_mgmt.get('position_size', 15.0) / 100.0
        self.max_shares = self.risk_mgmt.get('max_contracts', 1000)
        self.auto_approve = self.pml_config.get('auto_approve', False)
        
        self.logger.info("ExceedenceStrategy initialized:")
        self.logger.info(f"  Strategy allocation: {self.strategy_allocation_pct*100:.1f}%")
        self.logger.info(f"  Position size: {self.position_size_pct*100:.1f}%")
        self.logger.info(f"  Max shares: {self.max_shares}")
        self.logger.info(f"  Auto approve: {self.auto_approve}")
        
        # Debug: Print the actual PML config to verify auto_approve loading
        print(f"🔧 DEBUG: PML auto_approve from config: {self.pml_config.get('auto_approve', 'NOT_FOUND')}")

    def _load_trading_config(self) -> Dict[str, Any]:
        """Load trading configuration from trading_config_live.json"""
        try:
            if not os.path.exists('trading_config_live.json'):
                self.logger.warning("trading_config_live.json not found")
                return {}
            
            with open('trading_config_live.json', 'r') as f:
                data = json.load(f)
            
            self.logger.info("Loaded trading configuration")
            return data
            
        except Exception as e:
            self.logger.error(f"Error loading trading configuration: {e}")
            return {}

    def load_exceedance_indicators_5min(self) -> Dict[str, Any]:
        """Load exceedance indicators from 5min JSON file"""
        try:
            filepath = 'exceedance_data/exceedance_indicators_5min.json'
            
            if not os.path.exists(filepath):
                self.logger.warning(f"Exceedance indicators file not found: {filepath}")
                return {}
            
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            indicators = data.get('indicators', {})
            self.logger.info(f"Loaded 5min exceedance indicators for {len(indicators)} symbols")
            return indicators
            
        except Exception as e:
            self.logger.error(f"Error loading 5min exceedance indicators: {e}")
            return {}

    def load_watchlist_from_pml_strategy(self) -> List[str]:
        """Load watchlist symbols from PML strategy in trading_config_live.json"""
        try:
            pml_config = self.trading_config.get('strategies', {}).get('pml', {})
            watchlist_symbols = pml_config.get('pmlstrategy_watchlist', [])
            
            if watchlist_symbols:
                self.logger.info(f"Loaded {len(watchlist_symbols)} symbols from PML strategy watchlist")
                return watchlist_symbols
            else:
                self.logger.warning("PML strategy watchlist is empty - no symbols to process")
                return []
            
        except Exception as e:
            self.logger.error(f"Error loading watchlist from trading config: {e}")
            return []

    def calculate_position_size(self, current_price: float) -> int:
        """Calculate position size in shares based on PML risk management settings"""
        try:
            # Load account data to get equity
            account_data = self.load_account_data()
            if not account_data:
                self.logger.warning("No account data available, using minimum position size")
                return 1
            
            equity = account_data.get('equity', 0.0)
            if equity <= 0 or current_price <= 0:
                self.logger.warning(f"Invalid equity (${equity:.2f}) or price (${current_price:.2f})")
                return 1
            
            # Calculate position value based on strategy allocation and position size percentage
            strategy_allocation = equity * self.strategy_allocation_pct
            position_value = strategy_allocation * self.position_size_pct
            
            # Calculate shares
            shares = int(position_value / current_price)
            
            # Apply max shares limit
            shares = min(shares, self.max_shares)
            
            # Ensure at least 1 share
            shares = max(1, shares)
            
            self.logger.info(f"Position sizing: Equity=${equity:.2f}, Allocation=${strategy_allocation:.2f}, "
                           f"Position=${position_value:.2f}, Shares={shares}")
            
            return shares
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {e}")
            return 1

    def load_account_data(self) -> Dict[str, Any]:
        """Load account data from account_data.json"""
        try:
            if not os.path.exists('account_data.json'):
                return {}
            
            with open('account_data.json', 'r') as f:
                data = json.load(f)
            
            # Extract first account data
            account_data = data.get('account_data', {})
            if account_data:
                return list(account_data.values())[0]
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error loading account data: {e}")
            return {}

    def load_current_positions(self) -> Dict[str, Dict[str, Any]]:
        """Load current positions from current_positions.json (same source as trading engine)"""
        try:
            positions_file = 'current_positions.json'
            if not os.path.exists(positions_file):
                self.logger.debug(f"📄 {positions_file} not found")
                return {}
            
            with open(positions_file, 'r') as f:
                data = json.load(f)
            
            positions = {}
            
            # Extract positions from current_positions.json structure
            positions_data = data.get('positions', {})
            for position_key, position in positions_data.items():
                symbol = position.get('symbol', '')
                quantity = position.get('quantity', 0)
                avg_price = position.get('average_price', 0.0)
                
                if symbol and quantity != 0:
                    positions[symbol] = {
                        'quantity': quantity,
                        'average_price': avg_price,
                        'market_value': position.get('market_value', 0.0),
                        'position_type': 'LONG' if quantity > 0 else 'SHORT'
                    }
            
            self.logger.debug(f"✅ Loaded {len(positions)} current positions from {positions_file}")
            return positions
            
        except Exception as e:
            self.logger.error(f"Error loading current positions: {e}")
            return {}

    def calculate_scale_in_quantity(self, symbol: str, current_price: float, current_position: Dict[str, Any]) -> int:
        """Calculate scale-in quantity to bring average price within 0.02% of current price"""
        try:
            current_quantity = abs(current_position.get('quantity', 0))
            entry_price = current_position.get('average_price', 0.0)
            
            if current_quantity == 0 or entry_price <= 0 or current_price <= 0:
                # No existing position or invalid prices, use normal position sizing
                return self.calculate_position_size(current_price)
            
            # Calculate price difference and target difference (0.02% of current price)
            price_diff = abs(current_price - entry_price)
            target_diff = current_price * 0.0002  # 0.02% target
            
            self.logger.info(f"Scale-in calculation for {symbol}:")
            self.logger.info(f"  Current Price: ${current_price:.2f}")
            self.logger.info(f"  Entry Price: ${entry_price:.2f}")
            self.logger.info(f"  Price Difference: ${price_diff:.2f}")
            self.logger.info(f"  Target Difference: ${target_diff:.2f}")
            self.logger.info(f"  Current Quantity: {current_quantity}")
            
            if price_diff <= target_diff:
                # Already within target range, use normal position sizing
                self.logger.info("  Already within target range, using normal position sizing")
                return self.calculate_position_size(current_price)
            
            # Calculate shares needed to get average price within target_diff of current price
            try:
                # For LONG positions
                if current_price < entry_price:
                    # Averaging down - calculate shares to get average within target of current price
                    target_avg = current_price + target_diff
                    denominator = target_avg - current_price
                    
                    if abs(denominator) < 0.001:
                        raise ValueError("Target average too close to current price")
                    
                    # Formula: add_qty = current_qty * (entry_price - target_avg) / (target_avg - current_price)
                    calc_quantity = current_quantity * (entry_price - target_avg) / denominator
                    
                elif current_price > entry_price:
                    # Price is above entry - can't average up to current price, use normal sizing
                    self.logger.info("  Current price above entry - using normal position sizing")
                    return self.calculate_position_size(current_price)
                else:
                    # Prices are equal, use normal sizing
                    return self.calculate_position_size(current_price)
                
                # Validate calculated quantity
                if calc_quantity <= 0:
                    self.logger.info(f"  Calculated quantity ({calc_quantity:.2f}) invalid, using normal sizing")
                    return self.calculate_position_size(current_price)
                
                additional_quantity = max(1, int(round(calc_quantity)))
                self.logger.info(f"  Calculated scale-in quantity: {additional_quantity} shares")
                return additional_quantity
                
            except Exception as e:
                self.logger.error(f"  Error in scale-in calculation: {e}")
                return self.calculate_position_size(current_price)
                
        except Exception as e:
            self.logger.error(f"Error calculating scale-in quantity for {symbol}: {e}")
            return self.calculate_position_size(current_price)

    def generate_trading_signal(self, symbol: str, exceedance_indicators: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate simplified trading signal for long positions only.
        
        Simple logic:
        1. Check if price is in exceedance condition (≤5% or ≥95% of range)
        2. If yes → BUY signal (scale-in if position exists, new if not)
        3. If no → NO_SIGNAL
        
        Args:
            symbol: Symbol to analyze
            exceedance_indicators: Exceedance indicators for the symbol
            
        Returns:
            Trading signal dictionary
        """
        try:
            # Load current positions to check for existing position
            current_positions = self.load_current_positions()
            current_position = current_positions.get(symbol, {})
            has_position = bool(current_position.get('quantity', 0))
            
            # Get essential data
            current_price = exceedance_indicators.get('current_price', 0.0)
            position_in_range = exceedance_indicators.get('position_in_range', 50.0)
            high_exceedance = exceedance_indicators.get('high_exceedance', 0.0)
            low_exceedance = exceedance_indicators.get('low_exceedance', 0.0)
            
            # Initialize signal
            signal = {
                'symbol': symbol,
                'signal_type': 'NO_SIGNAL',
                'timestamp': datetime.now().isoformat(),
                'entry_reason': f"Price within normal range: {position_in_range:.1f}% - no exceedance signal",
                'position_size': 0,
                'auto_approve': self.auto_approve,
                'current_price': current_price,
                'position_in_range': position_in_range,
                'high_exceedance': high_exceedance,
                'low_exceedance': low_exceedance,
                'market_condition': 'WITHIN_BANDS',
                'has_trade_signal': False,
                'is_scale_in': has_position,
                'existing_position': current_position
            }
            
            if current_price <= 0:
                signal['entry_reason'] = 'Invalid current price'
                return signal
            
            # CORRECTED: Check for actual exceedance conditions (price beyond bands)
            is_exceedance = high_exceedance > 0.0 or low_exceedance > 0.0
            
            if is_exceedance:
                # Generate BUY signal
                signal['signal_type'] = 'BUY'
                signal['has_trade_signal'] = True
                
                # Determine market condition and entry reason based on actual exceedance values
                if low_exceedance > 0.0:
                    signal['market_condition'] = 'LOW_EXCEEDANCE'
                    exceedance_type = f"below lower band (${low_exceedance:.2f})"
                    exceedance_value = low_exceedance
                elif high_exceedance > 0.0:
                    signal['market_condition'] = 'HIGH_EXCEEDANCE'
                    exceedance_type = f"above upper band (+${high_exceedance:.2f})"
                    exceedance_value = high_exceedance
                else:
                    # Fallback (shouldn't happen with corrected logic)
                    signal['market_condition'] = 'EXCEEDANCE_DETECTED'
                    exceedance_type = "exceedance condition"
                    exceedance_value = 0.0
                
                # Calculate position size and set entry reason
                if has_position:
                    # Scale-in to existing position
                    signal['position_size'] = self.calculate_scale_in_quantity(symbol, current_price, current_position)
                    signal['entry_reason'] = f"SCALE-IN: Price {exceedance_type}"
                else:
                    # New position
                    signal['position_size'] = self.calculate_position_size(current_price)
                    signal['entry_reason'] = f"NEW: Price {exceedance_type}"
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating trading signal for {symbol}: {e}")
            return {
                'symbol': symbol,
                'signal_type': 'NO_SIGNAL',
                'timestamp': datetime.now().isoformat(),
                'entry_reason': f'Error in analysis: {str(e)}',
                'position_size': 0,
                'auto_approve': self.auto_approve,
                'current_price': 0.0,
                'position_in_range': 50.0,
                'market_condition': 'UNCERTAIN',
                'has_trade_signal': False,
                'is_scale_in': False,
                'existing_position': {}
            }

def analyze_symbol_exceedence_ultra_parallel(symbol: str, exceedance_indicators: Dict[str, Any], 
                                           strategy: ExceedenceStrategy) -> Tuple[str, Dict[str, Any]]:
    """Analyze a single symbol for exceedance signals (ultra-parallel processing)"""
    try:
        # Generate trading signal
        signal = strategy.generate_trading_signal(symbol, exceedance_indicators)
        
        # Print summary with scale-in information
        current_price = exceedance_indicators.get('current_price', 0.0)
        position_in_range = exceedance_indicators.get('position_in_range', 50.0)
        low_exceedance = exceedance_indicators.get('low_exceedance', 0.0)
        high_exceedance = exceedance_indicators.get('high_exceedance', 0.0)
        
        exceedance_info = ""
        if high_exceedance > 0:
            exceedance_info = f" | High Exc: +${high_exceedance:.2f}"
        elif low_exceedance > 0:
            exceedance_info = f" | Low Exc: -${low_exceedance:.2f}"
        else:
            exceedance_info = f" | Range: {position_in_range:.1f}%"
        
        # Add scale-in indicator to display
        signal_display = signal['signal_type']
        if signal['signal_type'] == 'BUY' and signal.get('is_scale_in', False):
            existing_pos = signal.get('existing_position', {})
            existing_qty = existing_pos.get('quantity', 0)
            existing_avg = existing_pos.get('average_price', 0.0)
            signal_display = f"BUY (SCALE-IN: {existing_qty}@${existing_avg:.2f})"
        elif signal['signal_type'] == 'BUY':
            signal_display = "BUY (NEW)"
        
        print(f"  {symbol}: ${current_price:.2f}{exceedance_info} | {signal_display}")
        
        return symbol, signal
        
    except Exception as e:
        print(f"  Error analyzing {symbol}: {e}")
        # Return error signal
        error_signal = {
            'symbol': symbol,
            'signal_type': 'NO_SIGNAL',
            'timestamp': datetime.now().isoformat(),
            'entry_reason': f'Analysis error: {str(e)}',
            'position_size': 0,
            'auto_approve': False,
            'current_price': 0.0,
            'position_in_range': 50.0,
            'market_condition': 'UNCERTAIN',
            'has_trade_signal': False
        }
        return symbol, error_signal

def execute_immediate_trade(trading_engine: ExceedanceTradingEngine, signal: Dict[str, Any]) -> bool:
    """
    Execute immediate trade using new atomic methods from refactored trading engine
    
    Uses the new modular approach:
    - execute_new_position_trade() for new positions
    - execute_scale_in_trade() for scale-in orders
    
    Args:
        trading_engine: Trading engine instance
        signal: Trading signal data
        
    Returns:
        True if successful, False otherwise
    """
    try:
        symbol = signal.get('symbol', '')
        quantity = signal.get('position_size', 0)
        current_price = signal.get('current_price', 0.0)
        is_scale_in = signal.get('is_scale_in', False)
        
        # Validate trade parameters using atomic method
        validation = trading_engine.validate_trade_params(symbol, quantity, current_price)
        if not validation['valid']:
            print(f"❌ Invalid trade parameters: {', '.join(validation['errors'])}")
            return False
        
        # Calculate profit target using atomic method
        profit_calc = trading_engine.calculate_profit_target_atomic(current_price, trading_engine.profit_target_pct)
        if not profit_calc['success']:
            print(f"❌ Profit target calculation failed: {profit_calc['error']}")
            return False
        
        profit_target = profit_calc['profit_target']
        signal['profit_target'] = profit_target
        
        trade_type = "SCALE-IN" if is_scale_in else "NEW"
        print(f"🎯 Executing {trade_type} ATOMIC trade: {symbol} {quantity} shares")
        print(f"   Entry: MARKET (est. ${current_price:.2f})")
        print(f"   Auto Take Profit: ${profit_target:.2f} ({trading_engine.profit_target_pct*100:.3f}%)")
        
        # Use appropriate atomic method based on trade type
        if is_scale_in:
            print(f"   🔄 Using atomic scale-in trade execution")
            result = trading_engine.execute_scale_in_trade(symbol, quantity, current_price)
        else:
            print(f"   🎯 Using atomic new position trade execution")
            result = trading_engine.execute_new_position_trade(symbol, quantity, current_price)
        
        if result.get('success', False):
            market_order_id = result.get('market_order_id', 'N/A')
            profit_order_id = result.get('profit_order_id', 'N/A')
            print(f"✅ {trade_type} ATOMIC trade completed successfully")
            print(f"   Market Order ID: {market_order_id}")
            print(f"   Profit Order ID: {profit_order_id}")
            
            if result.get('warning'):
                print(f"⚠️ Warning: {result['warning']}")
            
            return True
        else:
            error_msg = result.get('error', 'Unknown error')
            step = result.get('step', 'unknown')
            print(f"❌ {trade_type} ATOMIC trade failed at step '{step}': {error_msg}")
            return False
            
    except Exception as e:
        print(f"❌ Error executing immediate trade: {e}")
        return False

def generate_fresh_exceedance_indicators() -> bool:
    """Generate fresh exceedance indicators using the calculator"""
    try:
        print("🔄 Generating fresh exceedance indicators...")
        
        # Run the exceedance indicators calculator
        result = subprocess.run(
            [sys.executable, 'exceedance_indicators_calculator.py'],
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Successfully generated fresh exceedance indicators")
            # Log key output lines for confirmation
            if result.stdout:
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if any(keyword in line for keyword in ['Analysis completed successfully', 'Created 5 JSON files', 'ULTRA-PARALLEL analysis completed']):
                        print(f"   {line.strip()}")
            return True
        else:
            print(f"❌ Exceedance calculator failed with return code {result.returncode}")
            if result.stderr:
                print(f"   Error: {result.stderr.strip()}")
            if result.stdout:
                print(f"   Output: {result.stdout[-200:].strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Exceedance calculator timed out after 2 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running exceedance calculator: {e}")
        return False

def run_exceedence_analysis_ultra_parallel(reload_data: bool = False, execute_trades: bool = False, trading_engine=None) -> Dict[str, Any]:
    """Run exceedance analysis with ultra-parallel processing for time-sensitive signals"""
    
    # ALWAYS generate fresh exceedance indicators first for most accurate data
    print("📊 Generating fresh exceedance indicators for most accurate data...")
    if not generate_fresh_exceedance_indicators():
        print("⚠️ Failed to generate fresh indicators, proceeding with existing data")
    
    strategy = ExceedenceStrategy()
    
    # Initialize trading engine if trade execution is enabled and not provided
    if execute_trades and trading_engine is None:
        try:
            trading_engine = ExceedanceTradingEngine()
            print("🎯 Trading engine initialized for trade execution")
        except Exception as e:
            print(f"❌ Failed to initialize trading engine: {e}")
            print("⚠️ Continuing with signal generation only")
            execute_trades = False
            trading_engine = None
    
    # Reload trading config if requested (for continuous monitoring)
    if reload_data:
        print("🔄 Reloading trading configuration...")
        
        # Reload trading config
        strategy.trading_config = strategy._load_trading_config()
        strategy.pml_config = strategy.trading_config.get('strategies', {}).get('pml', {})
        strategy.risk_mgmt = strategy.pml_config.get('risk_management', {})
        strategy.auto_approve = strategy.pml_config.get('auto_approve', False)
        
        # Update strategy allocation and position size percentages
        strategy.strategy_allocation_pct = strategy.risk_mgmt.get('strategy_allocation', 20.0) / 100.0
        strategy.position_size_pct = strategy.risk_mgmt.get('position_size', 15.0) / 100.0
        strategy.max_shares = strategy.risk_mgmt.get('max_shares', 1000)
    
    # Load exceedance indicators (always fresh data)
    exceedance_indicators = strategy.load_exceedance_indicators_5min()
    if not exceedance_indicators:
        print("❌ No exceedance indicators available")
        return {}
    
    # Load watchlist (always fresh data)
    watchlist_symbols = strategy.load_watchlist_from_pml_strategy()
    if not watchlist_symbols:
        print("❌ No symbols in PML watchlist")
        return {}
    
    # Filter to only symbols that have exceedance data
    available_symbols = [symbol for symbol in watchlist_symbols if symbol in exceedance_indicators]
    
    print("🚀 ULTRA-PARALLEL Exceedence Strategy Analysis")
    print("=" * 60)
    print(f"Processing {len(available_symbols)} symbols simultaneously...")
    print("⚡ Each symbol runs in its own parallel process for maximum speed")
    if reload_data:
        print("🔄 Using fresh watchlist and indicators data")
    print()
    
    start_time = time.time()
    exceedence_signals = {}
    
    # Use maximum parallelization - one thread per symbol
    max_workers = min(20, len(available_symbols))  # Cap at 20 to avoid overwhelming
    completed_operations = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks
        future_to_symbol = {
            executor.submit(analyze_symbol_exceedence_ultra_parallel, symbol, 
                          exceedance_indicators[symbol], strategy): symbol 
            for symbol in available_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                symbol_result, signal_data = future.result(timeout=30)  # 30 second timeout
                exceedence_signals[symbol_result] = signal_data
                completed_operations += 1
                
                # Progress updates every 5 completions
                if completed_operations % 5 == 0 or completed_operations == len(available_symbols):
                    elapsed = time.time() - start_time
                    rate = completed_operations / elapsed if elapsed > 0 else 0
                    print(f"  ⚡ Completed {completed_operations}/{len(available_symbols)} ({rate:.1f}/sec)")
                    
            except Exception as e:
                print(f"  ❌ Error processing {symbol}: {e}")
                exceedence_signals[symbol] = {
                    'symbol': symbol,
                    'signal_type': 'NO_SIGNAL',
                    'timestamp': datetime.now().isoformat(),
                    'entry_reason': f'Processing error: {str(e)}',
                    'position_size': 0,
                    'auto_approve': False,
                    'current_price': 0.0,
                    'position_in_range': 50.0,
                    'market_condition': 'UNCERTAIN',
                    'has_trade_signal': False
                }
                completed_operations += 1
    
    elapsed_time = time.time() - start_time
    print(f"\n✅ Ultra-parallel analysis completed in {elapsed_time:.2f} seconds")
    print(f"📊 Performance: ~{len(available_symbols) / elapsed_time:.1f} symbols per second")
    
    return exceedence_signals

def calculate_bar_number() -> int:
    """Calculate current bar number starting from 6:30 AM EST (9:30 PST)"""
    try:
        # Get current time in Pacific timezone
        pst = pytz.timezone('America/Los_Angeles')
        current_time = datetime.now(pst)
        
        # Market start time: 6:30 AM EST = 9:30 AM PST (adjusted for pre-market)
        market_start = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
        
        # If current time is before market start, use previous day
        if current_time < market_start:
            market_start = market_start.replace(day=market_start.day - 1)
        
        # Calculate minutes since market start
        time_diff = current_time - market_start
        minutes_elapsed = int(time_diff.total_seconds() / 60)
        
        # Each 5-minute bar, starting from bar 1
        bar_number = max(1, (minutes_elapsed // 5) + 1)
        
        return bar_number
        
    except Exception as e:
        print(f"❌ Error calculating bar number: {e}")
        return 1

def generate_signal_id(signal: Dict[str, Any]) -> str:
    """Generate simplified signal ID: symbol_price_bar#_scale_in"""
    try:
        symbol = signal.get('symbol', 'UNK')
        current_price = signal.get('current_price', 0.0)
        bar_number = calculate_bar_number()
        is_scale_in = signal.get('is_scale_in', False)
        
        # Create simple fingerprint: SYMBOL_PRICE_BAR#_SCALEIN
        price_str = f"{current_price:.2f}".replace('.', '')  # Remove decimal point
        scale_str = "SI" if is_scale_in else "NEW"
        
        signal_id = f"{symbol}_{price_str}_{bar_number}_{scale_str}"
        
        return signal_id
        
    except Exception as e:
        print(f"❌ Error generating signal ID: {e}")
        return f"{signal.get('symbol', 'UNK')}_{int(time.time())}"

def load_executed_signals() -> Dict[str, Dict[str, Any]]:
    """Load previously executed signals to prevent duplicates"""
    try:
        filepath = 'executed_exceedance_signals.json'
        if not os.path.exists(filepath):
            return {}
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return data.get('executed_signals', {})
        
    except Exception as e:
        print(f"❌ Error loading executed signals: {e}")
        return {}

def save_executed_signal(signal_id: str, signal: Dict[str, Any], execution_result: Dict[str, Any]) -> bool:
    """Save executed signal to prevent duplicate trades"""
    try:
        filepath = 'executed_exceedance_signals.json'
        
        # Load existing executed signals
        executed_signals = load_executed_signals()
        
        # Add new executed signal
        executed_signals[signal_id] = {
            'signal': signal,
            'execution_result': execution_result,
            'executed_at': datetime.now().isoformat(),
            'signal_id': signal_id
        }
        
        # Create complete data structure
        executed_data = {
            'strategy_name': 'Exceedance_Strategy_Executed_Signals',
            'last_updated': datetime.now().isoformat(),
            'total_executed_signals': len(executed_signals),
            'executed_signals': executed_signals,
            'metadata': {
                'purpose': 'prevent_duplicate_trades_on_same_bar',
                'signal_id_method': 'symbol_price_bar_number_scale_in_flag',
                'signal_id_format': 'SYMBOL_PRICE_BAR#_SCALETYPE',
                'bar_calculation': '5min_bars_from_630am_est_930am_pst',
                'retention_policy': 'daily_reset_via_bar_number_progression',
                'duplicate_prevention': 'same_symbol_same_bar_same_price_same_type',
                'version': '1.0.0'
            }
        }
        
        # Save to file
        with open(filepath, 'w') as f:
            json.dump(executed_data, f, indent=2)
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving executed signal: {e}")
        return False

def is_signal_already_executed(signal: Dict[str, Any]) -> Tuple[bool, str]:
    """Check if signal has already been executed today"""
    try:
        signal_id = generate_signal_id(signal)
        executed_signals = load_executed_signals()
        
        if signal_id in executed_signals:
            executed_at = executed_signals[signal_id].get('executed_at', 'unknown')
            return True, executed_at
        
        return False, ''
        
    except Exception as e:
        print(f"❌ Error checking signal execution status: {e}")
        return False, ''

def save_exceedence_signals_to_file(exceedence_signals: Dict[str, Any]) -> bool:
    """Save exceedance signals to dedicated exceedence_signals.json file"""
    try:
        # Add signal IDs to all signals for tracking
        for symbol, signal in exceedence_signals.items():
            signal['signal_id'] = generate_signal_id(signal)
        
        # Create comprehensive exceedance signals data
        exceedence_data = {
            'strategy_name': 'Simplified_Exceedence_Strategy',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(exceedence_signals),
            'signals_generated': len([s for s in exceedence_signals.values() if s['signal_type'] == 'BUY']),
            'analysis_summary': {
                'buy': len([s for s in exceedence_signals.values() if s['signal_type'] == 'BUY']),
                'no_signal': len([s for s in exceedence_signals.values() if s['signal_type'] == 'NO_SIGNAL'])
            },
            'signals': exceedence_signals,
            'metadata': {
                'strategy_type': 'simplified_exceedance_long_only',
                'timeframe': '5min',
                'signal_types': ['BUY', 'NO_SIGNAL'],
                'analysis_method': 'exceedance_detection_long_only',
                'update_frequency': 'on_demand',
                'data_source': 'exceedance_data/exceedance_indicators_5min.json',
                'risk_management_source': 'pml_strategy_config',
                'position_direction': 'long_only',
                'ultra_parallel_processing': True,
                'signal_serialization': True,
                'duplicate_prevention': True
            }
        }
        
        # Write to dedicated exceedance signals file
        filepath = 'exceedence_signals.json'
        with open(filepath, 'w') as f:
            json.dump(exceedence_data, f, indent=2)
        
        print(f"✅ Saved exceedance signals to {filepath}")
        print(f"📊 Analysis: {exceedence_data['analysis_summary']['buy']} BUY signals generated")
        return True
        
    except Exception as e:
        print(f"❌ Error saving exceedance signals to file: {e}")
        return False

def get_strategy_timeframe() -> int:
    """Get the timeframe in minutes from strategy configuration or data source"""
    try:
        # Since this strategy uses exceedance_indicators_5min.json, it's 5-minute timeframe
        # But let's make it configurable by checking what data files exist
        
        timeframe_files = {
            1: 'exceedance_data/exceedance_indicators_1min.json',
            5: 'exceedance_data/exceedance_indicators_5min.json', 
            15: 'exceedance_data/exceedance_indicators_15min.json',
            30: 'exceedance_data/exceedance_indicators_30min.json'
        }
        
        # Check which timeframe file this strategy is configured to use
        # For exceedance strategy, it's hardcoded to use 5min in load_exceedance_indicators_5min()
        return 5
        
        # Alternative: Could check trading config for timeframe setting
        # try:
        #     with open('trading_config_live.json', 'r') as f:
        #         config = json.load(f)
        #     exceedance_config = config.get('strategies', {}).get('exceedance', {})
        #     return exceedance_config.get('timeframe_minutes', 5)
        # except:
        #     return 5
            
    except Exception as e:
        print(f"❌ Error getting strategy timeframe: {e}")
        return 5  # Default to 5-minute

def should_run_analysis() -> bool:
    """Check if we should run analysis at the end of the data cycle (auto-detect timeframe)"""
    # Auto-detect timeframe from strategy configuration
    timeframe_minutes = get_strategy_timeframe()
    
    current_time = datetime.now(pytz.timezone('America/Los_Angeles'))
    minutes_since_hour = current_time.minute
    seconds = current_time.second
    
    # Run analysis in the last 7 seconds of each timeframe bar
    if timeframe_minutes == 1:
        # 1-minute: run in last 7 seconds of each minute
        return seconds >= 53 and seconds <= 59
    elif timeframe_minutes == 5:
        # 5-minute: run in last 7 seconds of each 5-minute bar
        # 5-minute bars: 0-4, 5-9, 10-14, 15-19, etc.
        current_5min_bar = minutes_since_hour // 5
        end_of_5min_bar = (current_5min_bar + 1) * 5 - 1  # 4, 9, 14, 19, etc.
        return minutes_since_hour == end_of_5min_bar and seconds >= 53 and seconds <= 59
    elif timeframe_minutes == 15:
        # 15-minute: run in last 7 seconds of each 15-minute bar
        # 15-minute bars: 0-14, 15-29, 30-44, 45-59
        current_15min_bar = minutes_since_hour // 15
        end_of_15min_bar = (current_15min_bar + 1) * 15 - 1  # 14, 29, 44, 59
        return minutes_since_hour == end_of_15min_bar and seconds >= 53 and seconds <= 59
    elif timeframe_minutes == 30:
        # 30-minute: run in last 7 seconds of each 30-minute bar
        # 30-minute bars: 0-29, 30-59
        current_30min_bar = minutes_since_hour // 30
        end_of_30min_bar = (current_30min_bar + 1) * 30 - 1  # 29, 59
        return minutes_since_hour == end_of_30min_bar and seconds >= 53 and seconds <= 59
    else:
        # Default to 1-minute behavior for unsupported timeframes
        return seconds >= 53 and seconds <= 59

def wait_for_analysis_window():
    """Wait until we're in the analysis window (auto-detect timeframe)"""
    timeframe_minutes = get_strategy_timeframe()
    
    # Get current time info for debugging
    current_time = datetime.now(pytz.timezone('America/Los_Angeles'))
    minutes_since_hour = current_time.minute
    seconds = current_time.second
    
    # Calculate next analysis window
    if timeframe_minutes == 5:
        current_5min_bar = minutes_since_hour // 5
        end_of_5min_bar = (current_5min_bar + 1) * 5 - 1  # 4, 9, 14, 19, etc.
        next_analysis_minute = end_of_5min_bar
        
        print(f"⏰ Current time: {current_time.strftime('%H:%M:%S')} (minute {minutes_since_hour}, second {seconds})")
        print(f"📊 Next analysis window: minute {next_analysis_minute} at 53-59 seconds")
        
        # If we're already past the current bar's analysis window, wait for next bar
        if minutes_since_hour > end_of_5min_bar or (minutes_since_hour == end_of_5min_bar and seconds > 59):
            next_analysis_minute = ((current_5min_bar + 2) * 5) - 1
            if next_analysis_minute >= 60:
                next_analysis_minute = next_analysis_minute - 60
            print(f"📊 Waiting for next bar analysis window: minute {next_analysis_minute} at 53-59 seconds")
    
    # Add timeout to prevent infinite waiting
    max_wait_time = 300  # 5 minutes max wait
    start_wait = time.time()
    
    while not should_run_analysis():
        # Check for timeout
        if time.time() - start_wait > max_wait_time:
            print(f"⚠️ Timeout reached ({max_wait_time}s), proceeding with analysis")
            break
        time.sleep(0.5)

def run_continuous_exceedance_monitoring(execute_trades: bool = False):
    """Run continuous exceedance monitoring with time constraints"""
    timeframe_minutes = get_strategy_timeframe()
    
    print("🚀 Starting Continuous Exceedance Strategy Monitoring")
    print("=" * 60)
    print(f"📋 Long positions only | {timeframe_minutes}min timeframe | PML risk management")
    print(f"⏰ Analysis runs at end of each {timeframe_minutes}-minute bar (53-59 seconds)")
    if execute_trades:
        print("🎯 Trade execution enabled for auto-approved signals")
    print("🔄 Press Ctrl+C to stop")
    print()
    
    cycle_count = 0
    
    # Initialize trading engine if trade execution is enabled
    trading_engine = None
    if execute_trades:
        try:
            trading_engine = ExceedanceTradingEngine()
            print("🎯 Trading engine initialized for continuous monitoring")
        except Exception as e:
            print(f"❌ Failed to initialize trading engine: {e}")
            print("⚠️ Continuing with signal generation only")
            execute_trades = False
    
    try:
        while True:
            # Wait for analysis window (53-59 seconds)
            wait_for_analysis_window()
            
            cycle_count += 1
            current_time = datetime.now(pytz.timezone('America/Los_Angeles'))
            
            print(f"\n🔄 Analysis Cycle #{cycle_count} - {current_time.strftime('%H:%M:%S')} PT")
            print("-" * 50)
            
            # Run ultra-parallel analysis with fresh data reload and pass trading engine
            exceedence_signals = run_exceedence_analysis_ultra_parallel(reload_data=True, execute_trades=execute_trades, trading_engine=trading_engine)
            
            if exceedence_signals:
                # Save signals to file
                save_exceedence_signals_to_file(exceedence_signals)
                
                # Check for BUY signals and execute trades if auto-approved
                buy_signals = [s for s in exceedence_signals.values() if s['signal_type'] == 'BUY']
                executed_trades = 0
                
                if buy_signals:
                    # Initialize trading engine for this pass
                    try:
                        trading_engine = ExceedanceTradingEngine()
                        print("🎯 Trading engine initialized for this analysis pass")
                    except Exception as e:
                        print(f"❌ Failed to initialize trading engine: {e}")
                        trading_engine = None
                    
                    for signal in buy_signals:
                        # Check if signal has already been executed to prevent duplicates
                        already_executed, executed_at = is_signal_already_executed(signal)
                        if already_executed:
                            print(f"⚠️ DUPLICATE PREVENTED: {signal['symbol']} already executed at {executed_at}")
                            continue
                        
                        # Execute trade if auto-approved and trading engine is available
                        if signal.get('auto_approve', False) and trading_engine:
                            try:
                                print(f"🎯 EXECUTING TRADE: {signal['symbol']} (Signal ID: {signal.get('signal_id', 'N/A')})")
                                
                                # Use new OTA market order for immediate execution
                                success = execute_immediate_trade(trading_engine, signal)
                                
                                if success:
                                    print(f"✅ OTA Trade executed: {signal['symbol']} {signal['position_size']} shares at MARKET → ${signal.get('profit_target', 'N/A'):.2f}")
                                    executed_trades += 1
                                    
                                    # Save executed signal to prevent duplicates
                                    signal_id = signal.get('signal_id', generate_signal_id(signal))
                                    execution_result = {
                                        'success': True,
                                        'symbol': signal['symbol'],
                                        'quantity': signal['position_size'],
                                        'profit_target': signal.get('profit_target', 0),
                                        'executed_at': datetime.now().isoformat()
                                    }
                                    save_executed_signal(signal_id, signal, execution_result)
                                    print(f"💾 Saved executed signal: {signal_id}")
                                    
                                else:
                                    print(f"❌ OTA Trade failed: {signal['symbol']} - execution error")
                            except Exception as e:
                                print(f"❌ OTA Trade failed: {signal['symbol']} - {e}")
                        elif signal.get('auto_approve', False):
                            print(f"🔧 Debug: Auto-approved signal but trading engine failed to initialize: {signal['symbol']}")
                        else:
                            print(f"🔧 Debug: Signal not auto-approved: {signal['symbol']} auto_approve={signal.get('auto_approve')}")
                
                # Only show summary if trades were executed
                if executed_trades > 0:
                    print(f"📊 Executed {executed_trades} of {len(buy_signals)} signals")
                elif len(buy_signals) > 0:
                    print(f"📊 Generated {len(buy_signals)} signals (no auto-approved trades)")
                else:
                    print(f"📊 No BUY signals from {len(exceedence_signals)} symbols")
            else:
                print("❌ No analysis data available")
            
            # Wait until next minute starts (avoid running multiple times in same minute)
            while should_run_analysis():
                time.sleep(0.5)
            
            # Brief pause before next cycle
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping continuous monitoring after {cycle_count} cycles")
        print("👋 Exceedance Strategy Monitor stopped")

def main():
    """Main function with options for single run or continuous monitoring"""
    import sys
    
    # Check command line arguments
    execute_trades = False
    if len(sys.argv) > 1:
        if sys.argv[1] == '--continuous':
            # Check for trade execution flag
            if len(sys.argv) > 2 and sys.argv[2] == '--execute-trades':
                execute_trades = True
            run_continuous_exceedance_monitoring(execute_trades=execute_trades)
            return
        elif sys.argv[1] == '--single':
            # Check for trade execution flag
            if len(sys.argv) > 2 and sys.argv[2] == '--execute-trades':
                execute_trades = True
        elif sys.argv[1] == '--execute-trades':
            execute_trades = True
        else:
            print("Usage: python3 exceedence_strategy_signals.py [--continuous|--single] [--execute-trades]")
            print("  --continuous: Run continuous monitoring with time constraints")
            print("  --single: Run single analysis cycle")
            print("  --execute-trades: Enable automatic trade execution for auto-approved signals")
            return
    
    # Single analysis mode
    print("🔄 Starting Single Exceedance Strategy Analysis...")
    print("📋 Long positions only | 5min timeframe | PML risk management")
    if execute_trades:
        print("🎯 Trade execution enabled for auto-approved signals")
    print()
    
    # Run ultra-parallel analysis
    exceedence_signals = run_exceedence_analysis_ultra_parallel(execute_trades=execute_trades)
    
    if exceedence_signals:
        # Save signals to file
        save_exceedence_signals_to_file(exceedence_signals)
        
        # Check for BUY signals and execute trades if auto-approved
        buy_signals = [s for s in exceedence_signals.values() if s['signal_type'] == 'BUY']
        executed_trades = 0
        
        if buy_signals:
            # Initialize trading engine for this pass
            try:
                trading_engine = ExceedanceTradingEngine()
                print("🎯 Trading engine initialized for single analysis")
            except Exception as e:
                print(f"❌ Failed to initialize trading engine: {e}")
                trading_engine = None
            
            for signal in buy_signals:
                # Check if signal has already been executed to prevent duplicates
                already_executed, executed_at = is_signal_already_executed(signal)
                if already_executed:
                    print(f"⚠️ DUPLICATE PREVENTED: {signal['symbol']} already executed at {executed_at}")
                    continue
                
                # Execute trade if auto-approved and trading engine is available
                if signal.get('auto_approve', False) and trading_engine:
                    try:
                        print(f"🎯 EXECUTING TRADE: {signal['symbol']} (Signal ID: {signal.get('signal_id', 'N/A')})")
                        
                        # Use new OTA market order for immediate execution
                        success = execute_immediate_trade(trading_engine, signal)
                        
                        if success:
                            print(f"✅ OTA Trade executed: {signal['symbol']} {signal['position_size']} shares at MARKET → ${signal.get('profit_target', 'N/A'):.2f}")
                            executed_trades += 1
                            
                            # Save executed signal to prevent duplicates
                            signal_id = signal.get('signal_id', generate_signal_id(signal))
                            execution_result = {
                                'success': True,
                                'symbol': signal['symbol'],
                                'quantity': signal['position_size'],
                                'profit_target': signal.get('profit_target', 0),
                                'executed_at': datetime.now().isoformat()
                            }
                            save_executed_signal(signal_id, signal, execution_result)
                            print(f"💾 Saved executed signal: {signal_id}")
                            
                        else:
                            print(f"❌ OTA Trade failed: {signal['symbol']} - execution error")
                    except Exception as e:
                        print(f"❌ OTA Trade failed: {signal['symbol']} - {e}")
                elif signal.get('auto_approve', False):
                    print(f"🔧 Debug: Auto-approved signal but trading engine failed to initialize: {signal['symbol']}")
                else:
                    print(f"🔧 Debug: Signal not auto-approved: {signal['symbol']} auto_approve={signal.get('auto_approve')}")
        
        # Print summary with scale-in details
        if buy_signals:
            new_signals = [s for s in buy_signals if not s.get('is_scale_in', False)]
            scale_in_signals = [s for s in buy_signals if s.get('is_scale_in', False)]
            
            print(f"\n🎯 Generated {len(buy_signals)} BUY signals:")
            if new_signals:
                print(f"\n📈 NEW POSITIONS ({len(new_signals)}):")
                for signal in new_signals:
                    print(f"   {signal['symbol']}: {signal['entry_reason']}")
                    print(f"      Price: ${signal['current_price']:.2f} | Size: {signal['position_size']} shares")
            
            if scale_in_signals:
                print(f"\n🔄 SCALE-IN POSITIONS ({len(scale_in_signals)}):")
                for signal in scale_in_signals:
                    existing_pos = signal.get('existing_position', {})
                    existing_qty = existing_pos.get('quantity', 0)
                    existing_avg = existing_pos.get('average_price', 0.0)
                    print(f"   {signal['symbol']}: {signal['entry_reason']}")
                    print(f"      Current Price: ${signal['current_price']:.2f} | Add: {signal['position_size']} shares")
                    print(f"      Existing: {existing_qty} shares @ ${existing_avg:.2f}")
        else:
            print(f"\n📊 No BUY signals generated from {len(exceedence_signals)} symbols analyzed")
    else:
        print("❌ No signals generated")

if __name__ == "__main__":
    main()
