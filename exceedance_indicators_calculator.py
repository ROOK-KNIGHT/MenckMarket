#!/usr/bin/env python3
"""
Exceedance Indicators Calculator

This script creates simplified exceedance indicators for multiple timeframes:
- exceedance_indicators_1min.json
- exceedance_indicators_5min.json  
- exceedance_indicators_15min.json
- exceedance_indicators_30min.json
- exceedance_indicators_daily.json

Each JSON file contains only the essential data needed for exceedance strategy:
- Simple volatility bands (moving average + standard deviation)
- Price position within bands (0-100%)
- Exceedance detection (price beyond bands)
- Band stability metrics
- Basic price data (current price, volume)

The strategy script handles all signal generation logic. This calculator only
provides the raw exceedance data needed for analysis.

Usage: python3 exceedance_indicators_calculator.py [--single|--continuous]
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
import json
import os
import time as time_module
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# Import TA-Lib for technical analysis
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available. Using simplified calculations.")

# Import our existing data handlers
from historical_data_handler import HistoricalDataHandler

class ExceedanceTimeframeConfig:
    """Configuration for different timeframes - focused on exceedance needs"""
    
    # Directory for storing exceedance indicators
    OUTPUT_DIR = "exceedance_data"
    
    TIMEFRAMES = {
        "1min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 1,
            "min_periods": 100,
            "filename": "exceedance_indicators_1min.json"
        },
        "5min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 5,
            "min_periods": 100,
            "filename": "exceedance_indicators_5min.json"
        },
        "15min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 15,
            "min_periods": 80,
            "filename": "exceedance_indicators_15min.json"
        },
        "30min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 30,
            "min_periods": 60,
            "filename": "exceedance_indicators_30min.json"
        },
        "daily": {
            "period_type": "month", 
            "period": 6, 
            "frequency_type": "daily", 
            "frequency": 1,
            "min_periods": 50,
            "filename": "exceedance_indicators_daily.json"
        }
    }

class ExceedanceIndicatorsCalculator:
    """
    Focused calculator for exceedance indicators only
    """
    
    def __init__(self, timeframe: str):
        """Initialize the exceedance indicators calculator for a specific timeframe."""
        self.timeframe = timeframe
        self.historical_handler = HistoricalDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"ExceedanceIndicatorsCalculator initialized for {timeframe}")

    def get_historical_data_for_timeframe(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get historical data for the configured timeframe."""
        try:
            if self.timeframe not in ExceedanceTimeframeConfig.TIMEFRAMES:
                return None
            
            params = ExceedanceTimeframeConfig.TIMEFRAMES[self.timeframe]
            historical_data = self.historical_handler.get_historical_data(
                symbol=symbol,
                periodType=params["period_type"],
                period=params["period"],
                frequencyType=params["frequency_type"],
                freq=params["frequency"]
            )
            
            if historical_data and 'candles' in historical_data:
                df = pd.DataFrame(historical_data['candles'])
                if not df.empty:
                    # Convert datetime column - handle both timestamp and datetime formats
                    try:
                        # Try converting from milliseconds first
                        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                    except (ValueError, TypeError):
                        # If that fails, try direct datetime conversion
                        df['datetime'] = pd.to_datetime(df['datetime'])
                    return df
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol} {self.timeframe}: {e}")
            return None

    def calculate_volatility_bands(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate volatility bands using the same method as exceedence_strategy.py"""

        if len(df) < 20:  # Need minimum data for meaningful analysis
                return None
            
            # Use previous bar's close for calculations (same as original strategy)
        prev_bar = df.iloc[-2]  # Previous bar      
        lookback = min(2000, len(df)-2)  # Use all available data up to 2000 bars
            
            # Calculate rolling means and standard deviations using highside/lowside volatility
        highside_vol = df['high'] - df['close']
        lowside_vol = df['low'] - df['close']
            
            # Calculate rolling statistics up to previous bar
        mean_highside = highside_vol.iloc[:-1].rolling(window=lookback).mean().iloc[-1]
        mean_lowside = lowside_vol.iloc[:-1].rolling(window=lookback).mean().iloc[-1]
        std_highside = highside_vol.iloc[:-1].rolling(window=lookback).std().iloc[-1]
        std_lowside = lowside_vol.iloc[:-1].rolling(window=lookback).std().iloc[-1]
        
            # Calculate volatility bands based on close price
        upper_band = df['close'] + (std_highside + mean_highside)
        lower_band = df['close'] - (std_lowside - mean_lowside)
        middle_band = (upper_band + lower_band) / 2
            
        return upper_band, middle_band, lower_band
            


    def calculate_position_in_range(self, price: float, upper_band: float, lower_band: float) -> float:
        """Calculate position within volatility bands as percentage (0-100%)."""
        try:
            if upper_band <= lower_band:
                return 50.0  # Default to middle if bands are invalid
            
            band_range = upper_band - lower_band
            if band_range == 0:
                return 50.0
            
            position = ((price - lower_band) / band_range) * 100
            return max(0.0, min(100.0, position))  # Clamp between 0-100%
            
        except Exception as e:
            self.logger.error(f"Error calculating position in range: {e}")
            return 50.0

    def detect_exceedances(self, df: pd.DataFrame, upper_band: pd.Series, lower_band: pd.Series) -> Dict[str, float]:
        """Detect price exceedances beyond volatility bands."""
        try:
            current_price = float(df['close'].iloc[-1])
            current_high = float(df['high'].iloc[-1])
            current_low = float(df['low'].iloc[-1])
            current_upper = float(upper_band.iloc[-1])
            current_lower = float(lower_band.iloc[-1])
            
            # Calculate exceedances
            high_exceedance = max(0.0, current_high - current_upper)
            low_exceedance = max(0.0, current_lower - current_low)
            
            # Calculate position in range
            position_in_range = self.calculate_position_in_range(current_price, current_upper, current_lower)
            
            return {
                'high_exceedance': high_exceedance,
                'low_exceedance': low_exceedance,
                'position_in_range': position_in_range,
                'upper_band': current_upper,
                'lower_band': current_lower,
                'band_range': current_upper - current_lower
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting exceedances: {e}")
            return {
                'high_exceedance': 0.0,
                'low_exceedance': 0.0,
                'position_in_range': 50.0,
                'upper_band': 0.0,
                'lower_band': 0.0,
                'band_range': 0.0
            }

    def calculate_band_stability(self, upper_band: pd.Series, lower_band: pd.Series, lookback: int = 10) -> bool:
        """Calculate band stability over lookback period."""
        try:
            if len(upper_band) < lookback or len(lower_band) < lookback:
                return False
            
            # Get recent band ranges
            recent_ranges = []
            for i in range(lookback):
                idx = -(i + 1)
                if abs(idx) <= len(upper_band):
                    band_range = upper_band.iloc[idx] - lower_band.iloc[idx]
                    recent_ranges.append(band_range)
            
            if len(recent_ranges) < 2:
                return False
            
            # Calculate coefficient of variation
            mean_range = np.mean(recent_ranges)
            std_range = np.std(recent_ranges)
            
            if mean_range == 0:
                return False
            
            cv = std_range / mean_range
            
            # Bands are stable if coefficient of variation is low
            return cv < 0.15  # 15% threshold for stability
            
        except Exception as e:
            self.logger.error(f"Error calculating band stability: {e}")
            return False


    def calculate_exceedance_indicators(self, symbol: str) -> Dict[str, Any]:
        """Calculate focused exceedance indicators for a symbol."""
        try:
            # Get data for this specific timeframe
            df = self.get_historical_data_for_timeframe(symbol)
            if df is None or len(df) < 50:
                return self._create_empty_exceedance_indicators(symbol)
            
            # Ensure numeric types
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate simple volatility bands
            upper_band, middle_band, lower_band = self.calculate_volatility_bands(df)
            
            # Detect exceedances
            exceedance_data = self.detect_exceedances(df, upper_band, lower_band)
            
            # Calculate band stability
            band_stability = self.calculate_band_stability(upper_band, lower_band)
            
            # Extract current values
            current_price = float(df['close'].iloc[-1])
            current_volume = int(df['volume'].iloc[-1])
            
            # Determine market condition based on exceedances only
            if exceedance_data['high_exceedance'] > 0:
                market_condition = "HIGH_EXCEEDANCE"
            elif exceedance_data['low_exceedance'] > 0:
                market_condition = "LOW_EXCEEDANCE"
            else:
                market_condition = "WITHIN_BANDS"
            
            return {
                'symbol': symbol,
                'timeframe': self.timeframe,
                'timestamp': datetime.now().isoformat(),
                'current_price': current_price,
                'current_volume': current_volume,
                
                # Exceedance analysis (essential data only)
                'high_exceedance': exceedance_data['high_exceedance'],
                'low_exceedance': exceedance_data['low_exceedance'],
                'position_in_range': exceedance_data['position_in_range'],
                'upper_band': exceedance_data['upper_band'],
                'lower_band': exceedance_data['lower_band'],
                'band_range': exceedance_data['band_range'],
                'band_stability': band_stability,
                'market_condition': market_condition
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating exceedance indicators for {symbol}: {e}")
            return self._create_empty_exceedance_indicators(symbol)

    def _create_empty_exceedance_indicators(self, symbol: str) -> Dict[str, Any]:
        """Create empty exceedance indicators for error cases."""
        return {
            'symbol': symbol,
            'timeframe': self.timeframe,
            'timestamp': datetime.now().isoformat(),
            'current_price': 0.0,
            'current_volume': 0,
            
            # Exceedance analysis (essential data only)
            'high_exceedance': 0.0,
            'low_exceedance': 0.0,
            'position_in_range': 50.0,
            'upper_band': 0.0,
            'lower_band': 0.0,
            'band_range': 0.0,
            'band_stability': False,
            'market_condition': 'UNCERTAIN'
        }

def load_watchlist_from_pml_strategy() -> List[str]:
    """Load watchlist symbols from PML strategy in trading_config_live.json"""
    try:
        if not os.path.exists('trading_config_live.json'):
            print("❌ trading_config_live.json not found")
            return []
        
        with open('trading_config_live.json', 'r') as f:
            data = json.load(f)
        
        # Extract symbols from PML strategy watchlist
        pml_config = data.get('strategies', {}).get('pml', {})
        watchlist_symbols = pml_config.get('pmlstrategy_watchlist', [])
        
        if watchlist_symbols:
            print(f"📋 Loaded {len(watchlist_symbols)} symbols from PML strategy watchlist")
            return watchlist_symbols
        else:
            print("❌ PML strategy watchlist is empty - no symbols to process")
            return []
        
    except Exception as e:
        print(f"❌ Error loading watchlist from trading config: {e}")
        return []

def analyze_symbol_for_timeframe(symbol: str, timeframe: str) -> Tuple[str, str, Dict[str, Any]]:
    """Analyze a single symbol for a specific timeframe"""
    try:
        print(f"💻 Calculating exceedance indicators for {symbol} ({timeframe})")
        
        # Create timeframe-specific calculator
        calculator = ExceedanceIndicatorsCalculator(timeframe)
        
        # Calculate exceedance indicators for this timeframe
        indicators = calculator.calculate_exceedance_indicators(symbol)
        
        # Print summary
        position = indicators.get('position_in_range', 50.0)
        high_exc = indicators.get('high_exceedance', 0.0)
        low_exc = indicators.get('low_exceedance', 0.0)
        signal = indicators.get('trading_signal', 'NO_SIGNAL')
        
        exceedance_info = ""
        if high_exc > 0:
            exceedance_info = f" | High Exc: +${high_exc:.2f}"
        elif low_exc > 0:
            exceedance_info = f" | Low Exc: -${low_exc:.2f}"
        else:
            exceedance_info = f" | Pos: {position:.1f}%"
        
        print(f"  {symbol} ({timeframe}): ${indicators.get('current_price', 0):.2f}{exceedance_info} | Signal: {signal}")
        
        return symbol, timeframe, indicators
        
    except Exception as e:
        print(f"  Error analyzing {symbol} for {timeframe}: {e}")
        # Return error indicators
        error_indicators = {
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'current_price': 0.0,
            'trading_signal': 'NO_SIGNAL'
        }
        return symbol, timeframe, error_indicators

def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization"""
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

def save_exceedance_indicators_for_timeframe(timeframe: str, indicators: Dict[str, Any]) -> bool:
    """Save exceedance indicators to timeframe-specific JSON file in organized directory"""
    try:
        # Create output directory if it doesn't exist
        output_dir = ExceedanceTimeframeConfig.OUTPUT_DIR
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 Created directory: {output_dir}")
        
        # Get filename and create full path
        filename = ExceedanceTimeframeConfig.TIMEFRAMES[timeframe]["filename"]
        filepath = os.path.join(output_dir, filename)
        
        # Convert numpy types to native Python types
        clean_indicators = convert_numpy_types(indicators)
        
        # Create comprehensive exceedance indicators data
        exceedance_data = {
            'strategy_name': f'Exceedance_Indicators_{timeframe.upper()}',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(clean_indicators),
            'analysis_summary': {
                'symbols_with_data': len([s for s in clean_indicators.values() if 'error' not in s]),
                'symbols_with_errors': len([s for s in clean_indicators.values() if 'error' in s]),
                'high_exceedances': len([s for s in clean_indicators.values() if s.get('high_exceedance', 0) > 0]),
                'low_exceedances': len([s for s in clean_indicators.values() if s.get('low_exceedance', 0) > 0])
            },
            'indicators': clean_indicators,
            'metadata': {
                'analysis_type': 'exceedance_indicators',
                'timeframe': timeframe,
                'indicator_focus': ['volatility_bands', 'exceedance_detection', 'band_stability'],                'update_frequency': 'minute_intervals',
                'data_source': 'schwab_historical_data',
                'output_directory': output_dir,
                'watchlist_source': 'pml_strategy'
            }
        }
        
        # Write to timeframe-specific file in organized directory
        with open(filepath, 'w') as f:
            json.dump(exceedance_data, f, indent=2)
        
        print(f"✅ Saved {timeframe} exceedance indicators to {filepath}")
        print(f"📊 Analysis: {exceedance_data['analysis_summary']['symbols_with_data']} symbols, {exceedance_data['analysis_summary']['high_exceedances']} high exc, {exceedance_data['analysis_summary']['low_exceedances']} low exc")
        return True
        
    except Exception as e:
        print(f"❌ Error saving {timeframe} exceedance indicators to file: {e}")
        return False

def run_single_analysis_cycle():
    """Run a single analysis cycle for all timeframes and symbols with ultra-parallel processing"""
    watchlist_symbols = load_watchlist_from_pml_strategy()
    
    if not watchlist_symbols:
        print("❌ No symbols loaded from PML strategy. Cannot proceed.")
        return False
    
    total_operations = len(watchlist_symbols) * len(ExceedanceTimeframeConfig.TIMEFRAMES)
    print(f"🚀 ULTRA-PARALLEL Exceedance Analysis")
    print("=" * 60)
    print(f"Processing {len(watchlist_symbols)} symbols × {len(ExceedanceTimeframeConfig.TIMEFRAMES)} timeframes = {total_operations} operations")
    print("⚡ Each symbol-timeframe combination runs in its own parallel process")
    print()
    
    overall_start_time = time_module.time()
    
    # Step 1: Process ALL symbol-timeframe combinations in parallel
    all_results = process_all_combinations_ultra_parallel(watchlist_symbols)
    
    # Step 2: Organize results by timeframe and save
    print("📁 Organizing results and saving to JSON files...")
    for timeframe in ExceedanceTimeframeConfig.TIMEFRAMES.keys():
        timeframe_indicators = {}
        
        # Collect all results for this timeframe
        for symbol in watchlist_symbols:
            combination_key = f"{symbol}_{timeframe}"
            if combination_key in all_results:
                timeframe_indicators[symbol] = all_results[combination_key]
            else:
                # Create error entry if missing
                timeframe_indicators[symbol] = {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': datetime.now().isoformat(),
                    'error': 'Processing failed or timed out',
                    'trading_signal': 'NO_SIGNAL'
                }
        
        # Save indicators for this timeframe
        save_exceedance_indicators_for_timeframe(timeframe, timeframe_indicators)
    
    overall_elapsed_time = time_module.time() - overall_start_time
    print(f"\n🎉 ULTRA-PARALLEL analysis completed in {overall_elapsed_time:.2f} seconds")
    print(f"⚡ Performance: ~{total_operations / overall_elapsed_time:.1f} operations per second")
    
    return True

def process_all_combinations_ultra_parallel(watchlist_symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Process all symbol-timeframe combinations in ultra-parallel mode"""
    print("⚡ Launching ultra-parallel processing...")
    process_start = time_module.time()
    
    # Create all symbol-timeframe combinations
    all_combinations = []
    for symbol in watchlist_symbols:
        for timeframe in ExceedanceTimeframeConfig.TIMEFRAMES.keys():
            all_combinations.append((symbol, timeframe))
    
    total_combinations = len(all_combinations)
    print(f"🔥 Processing {total_combinations} symbol-timeframe combinations simultaneously...")
    
    # Results storage with thread safety
    results = {}
    results_lock = threading.Lock()
    
    # Use maximum parallelization - one thread per combination (up to reasonable limit)
    max_workers = min(20, total_combinations)  # Cap at 20 to avoid overwhelming the system
    completed_operations = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all combinations for processing
        future_to_combination = {
            executor.submit(process_single_combination, symbol, timeframe): (symbol, timeframe)
            for symbol, timeframe in all_combinations
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_combination):
            symbol, timeframe = future_to_combination[future]
            combination_key = f"{symbol}_{timeframe}"
            
            try:
                _, _, indicators = future.result(timeout=45)  # 45 second timeout per combination
                
                with results_lock:
                    results[combination_key] = indicators
                    completed_operations += 1
                    
                    # Progress updates every 5 completions
                    if completed_operations % 5 == 0 or completed_operations == total_combinations:
                        elapsed = time_module.time() - process_start
                        rate = completed_operations / elapsed if elapsed > 0 else 0
                        print(f"  ⚡ Completed {completed_operations}/{total_combinations} ({rate:.1f}/sec)")
                        
            except Exception as e:
                print(f"  ❌ Error processing {symbol} {timeframe}: {e}")
                with results_lock:
                    results[combination_key] = {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'timestamp': datetime.now().isoformat(),
                        'error': str(e),
                        'trading_signal': 'NO_SIGNAL'
                    }
                    completed_operations += 1
    
    process_elapsed = time_module.time() - process_start
    print(f"✅ Ultra-parallel processing completed in {process_elapsed:.2f}s")
    print(f"📊 Average rate: {total_combinations / process_elapsed:.1f} combinations per second")
    
    return results

def process_single_combination(symbol: str, timeframe: str) -> Tuple[str, str, Dict[str, Any]]:
    """Process a single symbol-timeframe combination (optimized for parallel execution)"""
    try:
        # Create timeframe-specific calculator (each thread gets its own instance)
        calculator = ExceedanceIndicatorsCalculator(timeframe)
        
        # Calculate exceedance indicators for this specific combination
        indicators = calculator.calculate_exceedance_indicators(symbol)
        
        return symbol, timeframe, indicators
        
    except Exception as e:
        # Return error indicators
        error_indicators = {
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'current_price': 0.0,
            'trading_signal': 'NO_SIGNAL'
        }
        return symbol, timeframe, error_indicators

def run_continuous_analysis():
    """Run continuous exceedance analysis on minute intervals"""
    print("🚀 Starting Continuous Exceedance Indicators Calculator")
    print("=" * 60)
    print("📊 Monitoring PML strategy watchlist")
    print("⏰ Running on minute intervals")
    print("🔄 Press Ctrl+C to stop")
    print()
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            start_time = time_module.time()
            
            print(f"\n🔄 Analysis Cycle #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 50)
            
            # Run analysis cycle
            success = run_single_analysis_cycle()
            
            elapsed_time = time_module.time() - start_time
            print(f"\n✅ Cycle #{cycle_count} completed in {elapsed_time:.2f} seconds")
            
            if success:
                print(f"📁 Updated all timeframe files in {ExceedanceTimeframeConfig.OUTPUT_DIR}/ directory")
            else:
                print("⚠️ Cycle completed with errors")
            
            # Wait for next minute interval
            print(f"⏰ Waiting for next minute interval...")
            time_module.sleep(60)  # Wait 60 seconds for next cycle
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping continuous analysis after {cycle_count} cycles")
        print("👋 Exceedance Indicators Calculator stopped")

def run_single_analysis():
    """Run a single analysis cycle (for testing)"""
    print("🚀 Single Exceedance Indicators Analysis")
    print("=" * 50)
    
    success = run_single_analysis_cycle()
    
    if success:
        print(f"\n🎉 Analysis completed successfully!")
        print(f"📁 Created {len(ExceedanceTimeframeConfig.TIMEFRAMES)} JSON files in {ExceedanceTimeframeConfig.OUTPUT_DIR}/ directory:")
        for timeframe, config in ExceedanceTimeframeConfig.TIMEFRAMES.items():
            filepath = os.path.join(ExceedanceTimeframeConfig.OUTPUT_DIR, config['filename'])
            print(f"   - {filepath}")
    else:
        print("❌ Analysis completed with errors")

def main():
    """Main function to run exceedance indicators calculation"""
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--continuous':
            run_continuous_analysis()
        elif sys.argv[1] == '--single':
            run_single_analysis()
        else:
            print("Usage: python3 exceedance_indicators_calculator.py [--continuous|--single]")
            print("  --continuous: Run continuous analysis on minute intervals")
            print("  --single: Run single analysis cycle")
    else:
        # Default to single analysis
        run_single_analysis()

if __name__ == "__main__":
    main()
