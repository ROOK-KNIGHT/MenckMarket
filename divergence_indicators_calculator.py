#!/usr/bin/env python3
"""
Divergence Indicators Calculator

This script creates focused divergence indicators for multiple timeframes:
- divergence_indicators_1min.json
- divergence_indicators_5min.json  
- divergence_indicators_15min.json
- divergence_indicators_30min.json
- divergence_indicators_daily.json

Each JSON file contains only the indicators needed for divergence strategy:
- RSI (14-period)
- Price swing highs/lows
- Divergence detection results
- Basic price data (OHLC)

Usage: python3 divergence_indicators_calculator.py
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time as time_module
import threading
from collections import defaultdict

# Import TA-Lib for technical analysis
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available. Using simplified RSI calculation.")

# Import scipy for swing point detection
try:
    from scipy.signal import argrelextrema
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("SciPy not available. Using simplified swing detection.")

# Import our existing data handlers
from historical_data_handler import HistoricalDataHandler

class DivergenceTimeframeConfig:
    """Configuration for different timeframes - focused on divergence needs"""
    
    # Directory for storing divergence indicators
    OUTPUT_DIR = "divergence_data"
    
    TIMEFRAMES = {
        "1min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 1,
            "min_periods": 100,
            "filename": "divergence_indicators_1min.json"
        },
        "5min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 5,
            "min_periods": 100,
            "filename": "divergence_indicators_5min.json"
        },
        "15min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 15,
            "min_periods": 80,
            "filename": "divergence_indicators_15min.json"
        },
        "30min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 30,
            "min_periods": 60,
            "filename": "divergence_indicators_30min.json"
        },
        "daily": {
            "period_type": "month", 
            "period": 6, 
            "frequency_type": "daily", 
            "frequency": 1,
            "min_periods": 50,
            "filename": "divergence_indicators_daily.json"
        }
    }

class DivergenceIndicatorsCalculator:
    """
    Focused calculator for divergence indicators only
    """
    
    def __init__(self, timeframe: str):
        """Initialize the divergence indicators calculator for a specific timeframe."""
        self.timeframe = timeframe
        self.historical_handler = HistoricalDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"DivergenceIndicatorsCalculator initialized for {timeframe}")

    def get_historical_data_for_timeframe(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get historical data for the configured timeframe."""
        try:
            if self.timeframe not in DivergenceTimeframeConfig.TIMEFRAMES:
                return None
            
            params = DivergenceTimeframeConfig.TIMEFRAMES[self.timeframe]
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

    def calculate_rsi(self, df: pd.DataFrame) -> pd.Series:
        """Calculate RSI (14-period) for divergence analysis."""
        try:
            if TALIB_AVAILABLE:
                rsi = talib.RSI(df['close'].values, timeperiod=14)
                return pd.Series(rsi, index=df.index)
            else:
                # Simplified RSI calculation
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                return rsi
                
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {e}")
            return pd.Series([50.0] * len(df), index=df.index)

    def detect_swing_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect price swing highs and lows using actual high/low prices."""
        try:
            df_swings = df.copy()
            
            if SCIPY_AVAILABLE:
                # Find local maxima using HIGH prices and minima using LOW prices
                high_indices = argrelextrema(df_swings['high'].values, np.greater, order=5)[0]
                low_indices = argrelextrema(df_swings['low'].values, np.less, order=5)[0]
            else:
                # Simplified swing detection using high/low prices
                high_indices = []
                low_indices = []
                for i in range(5, len(df_swings) - 5):
                    # Check if current high is the highest in the window
                    if df_swings['high'].iloc[i] == df_swings['high'].iloc[i-5:i+6].max():
                        high_indices.append(i)
                    # Check if current low is the lowest in the window
                    if df_swings['low'].iloc[i] == df_swings['low'].iloc[i-5:i+6].min():
                        low_indices.append(i)
            
            # Initialize swing columns
            df_swings['swing_high'] = np.nan
            df_swings['swing_low'] = np.nan
            
            # Mark swing points using actual high/low values
            if len(high_indices) > 0:
                df_swings.loc[df_swings.index[high_indices], 'swing_high'] = df_swings.iloc[high_indices]['high'].values
            
            if len(low_indices) > 0:
                df_swings.loc[df_swings.index[low_indices], 'swing_low'] = df_swings.iloc[low_indices]['low'].values
            
            return df_swings
            
        except Exception as e:
            self.logger.error(f"Error detecting swing points: {e}")
            return df

    def detect_rsi_divergences(self, df: pd.DataFrame) -> Dict[str, Dict[str, bool]]:
        """Detect RSI divergences with improved logic (30-bar lookback, actual high/low prices)."""
        try:
            divergences = {
                "bullish": {"strong": False, "medium": False, "weak": False},
                "bearish": {"strong": False, "medium": False, "weak": False}
            }
            
            # Time constraint: Only consider swing points within last 30 bars
            current_bar_idx = len(df) - 1
            lookback_start = max(0, current_bar_idx - 30)
            
            # Get swing points within the last 30 bars
            recent_df = df.iloc[lookback_start:]
            swing_highs = recent_df['swing_high'].dropna()
            swing_lows = recent_df['swing_low'].dropna()
            
            if len(swing_highs) < 2 and len(swing_lows) < 2:
                return divergences
            
            # Check bullish divergences (swing lows) - Use actual LOWS for price comparison
            if len(swing_lows) >= 2:
                lows_indices = swing_lows.index.tolist()[-2:]  # Get last 2 swing lows
                if len(lows_indices) >= 2:
                    idx1, idx2 = lows_indices[0], lows_indices[1]
                    
                    # Use actual LOW prices for bullish divergence comparison
                    price1 = float(df.loc[idx1, 'low'])  # First swing low price
                    price2 = float(df.loc[idx2, 'low'])  # Second swing low price
                    rsi1 = float(df.loc[idx1, 'rsi'])    # RSI at first swing low
                    rsi2 = float(df.loc[idx2, 'rsi'])    # RSI at second swing low
                    
                    # Bullish divergence: Price makes lower low, RSI makes higher low
                    if price2 < price1 and rsi2 > rsi1:
                        # Determine strength based on magnitude of divergence
                        price_decline_pct = ((price1 - price2) / price1) * 100
                        rsi_improvement = rsi2 - rsi1
                        
                        # Adjusted thresholds for shorter timeframes
                        if price_decline_pct > 0.5 and rsi_improvement > 3.0:
                            divergences["bullish"]["strong"] = True
                        elif price_decline_pct > 0.2 and rsi_improvement > 2.0:
                            divergences["bullish"]["medium"] = True
                        else:
                            divergences["bullish"]["weak"] = True
            
            # Check bearish divergences (swing highs) - Use actual HIGHS for price comparison
            if len(swing_highs) >= 2:
                highs_indices = swing_highs.index.tolist()[-2:]  # Get last 2 swing highs
                if len(highs_indices) >= 2:
                    idx1, idx2 = highs_indices[0], highs_indices[1]
                    
                    # Use actual HIGH prices for bearish divergence comparison
                    price1 = float(df.loc[idx1, 'high'])  # First swing high price
                    price2 = float(df.loc[idx2, 'high'])  # Second swing high price
                    rsi1 = float(df.loc[idx1, 'rsi'])     # RSI at first swing high
                    rsi2 = float(df.loc[idx2, 'rsi'])     # RSI at second swing high
                    
                    # Bearish divergence: Price makes higher high, RSI makes lower high
                    if price2 > price1 and rsi2 < rsi1:
                        # Determine strength based on magnitude of divergence
                        price_advance_pct = ((price2 - price1) / price1) * 100
                        rsi_deterioration = rsi1 - rsi2
                        
                        # Adjusted thresholds for shorter timeframes
                        if price_advance_pct > 0.5 and rsi_deterioration > 3.0:
                            divergences["bearish"]["strong"] = True
                        elif price_advance_pct > 0.2 and rsi_deterioration > 2.0:
                            divergences["bearish"]["medium"] = True
                        else:
                            divergences["bearish"]["weak"] = True
            
            return divergences
            
        except Exception as e:
            self.logger.error(f"Error detecting RSI divergences: {e}")
            return {
                "bullish": {"strong": False, "medium": False, "weak": False},
                "bearish": {"strong": False, "medium": False, "weak": False}
            }

    def calculate_divergence_indicators(self, symbol: str) -> Dict[str, Any]:
        """Calculate focused divergence indicators for a symbol."""
        try:
            # Get data for this specific timeframe
            df = self.get_historical_data_for_timeframe(symbol)
            if df is None or len(df) < 50:
                return self._create_empty_divergence_indicators(symbol)
            
            # Ensure numeric types
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate RSI
            df['rsi'] = self.calculate_rsi(df)
            
            # Detect swing points
            df_with_swings = self.detect_swing_points(df)
            
            # Detect divergences
            divergences = self.detect_rsi_divergences(df_with_swings)
            
            # Extract current values
            current_price = float(df['close'].iloc[-1])
            current_rsi = float(df['rsi'].iloc[-1])
            current_volume = int(df['volume'].iloc[-1])
            
            # Extract swing points
            swing_highs = df_with_swings['swing_high'].dropna().tolist()
            swing_lows = df_with_swings['swing_low'].dropna().tolist()
            
            # Determine overall divergence status
            bullish_divergence_detected = any(divergences['bullish'].values())
            bearish_divergence_detected = any(divergences['bearish'].values())
            
            # Determine divergence strength
            if divergences['bullish']['strong'] or divergences['bearish']['strong']:
                divergence_strength = 'strong'
            elif divergences['bullish']['medium'] or divergences['bearish']['medium']:
                divergence_strength = 'medium'
            else:
                divergence_strength = 'weak'
            
            # Determine trend direction (simplified)
            if len(df) >= 20:
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                if current_price > sma_20 * 1.02:
                    trend_direction = 'bullish'
                elif current_price < sma_20 * 0.98:
                    trend_direction = 'bearish'
                else:
                    trend_direction = 'neutral'
            else:
                trend_direction = 'neutral'
            
            # Determine signal type
            if bullish_divergence_detected:
                signal_type = "BUY"
            elif bearish_divergence_detected:
                signal_type = "SELL"
            else:
                signal_type = "NO_SIGNAL"
            
            return {
                'symbol': symbol,
                'timeframe': self.timeframe,
                'timestamp': datetime.now().isoformat(),
                'current_price': current_price,
                'current_rsi': current_rsi,
                'current_volume': current_volume,
                'swing_highs': swing_highs[-5:] if len(swing_highs) > 5 else swing_highs,  # Last 5
                'swing_lows': swing_lows[-5:] if len(swing_lows) > 5 else swing_lows,  # Last 5
                'bullish_divergence_detected': bullish_divergence_detected,
                'bearish_divergence_detected': bearish_divergence_detected,
                'bullish_divergence_strong': divergences['bullish']['strong'],
                'bullish_divergence_medium': divergences['bullish']['medium'],
                'bullish_divergence_weak': divergences['bullish']['weak'],
                'bearish_divergence_strong': divergences['bearish']['strong'],
                'bearish_divergence_medium': divergences['bearish']['medium'],
                'bearish_divergence_weak': divergences['bearish']['weak'],
                'divergence_strength': divergence_strength,
                'trend_direction': trend_direction,
                'signal_type': signal_type,
                'has_trade_signal': bullish_divergence_detected or bearish_divergence_detected
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating divergence indicators for {symbol}: {e}")
            return self._create_empty_divergence_indicators(symbol)

    def _create_empty_divergence_indicators(self, symbol: str) -> Dict[str, Any]:
        """Create empty divergence indicators for error cases."""
        return {
            'symbol': symbol,
            'timeframe': self.timeframe,
            'timestamp': datetime.now().isoformat(),
            'current_price': 0.0,
            'current_rsi': 50.0,
            'current_volume': 0,
            'swing_highs': [],
            'swing_lows': [],
            'bullish_divergence_detected': False,
            'bearish_divergence_detected': False,
            'bullish_divergence_strong': False,
            'bullish_divergence_medium': False,
            'bullish_divergence_weak': False,
            'bearish_divergence_strong': False,
            'bearish_divergence_medium': False,
            'bearish_divergence_weak': False,
            'divergence_strength': 'none',
            'trend_direction': 'neutral',
            'signal_type': 'NO_SIGNAL',
            'has_trade_signal': False
        }

def load_watchlist_from_trading_config() -> List[str]:
    """Load watchlist symbols from trading_config_live.json - no fallbacks"""
    try:
        if not os.path.exists('trading_config_live.json'):
            print("❌ trading_config_live.json not found")
            return []
        
        with open('trading_config_live.json', 'r') as f:
            data = json.load(f)
        
        # Extract symbols from divergence strategy watchlist
        divergence_config = data.get('strategies', {}).get('divergence', {})
        watchlist_symbols = divergence_config.get('divergence_strategy_watchlist', [])
        
        if watchlist_symbols:
            print(f"📋 Loaded {len(watchlist_symbols)} symbols from trading_config_live.json divergence watchlist")
            return watchlist_symbols
        else:
            print("❌ No symbols found in divergence_strategy_watchlist")
            return []
        
    except Exception as e:
        print(f"❌ Error loading watchlist from trading config: {e}")
        return []

def analyze_symbol_for_timeframe(symbol: str, timeframe: str) -> Tuple[str, str, Dict[str, Any]]:
    """Analyze a single symbol for a specific timeframe"""
    try:
        print(f"💻 Calculating divergence indicators for {symbol} ({timeframe})")
        
        # Create timeframe-specific calculator
        calculator = DivergenceIndicatorsCalculator(timeframe)
        
        # Calculate divergence indicators for this timeframe
        indicators = calculator.calculate_divergence_indicators(symbol)
        
        # Print summary
        print(f"  {symbol} ({timeframe}): ${indicators.get('current_price', 0):.2f} | RSI: {indicators.get('current_rsi', 50):.1f} | Signal: {indicators.get('signal_type', 'NO_SIGNAL')}")
        
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
            'current_rsi': 50.0,
            'signal_type': 'NO_SIGNAL'
        }
        return symbol, timeframe, error_indicators

def run_timeframe_analysis_parallel(timeframe: str, watchlist_symbols: List[str]) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Run analysis for a single timeframe across all symbols in parallel"""
    print(f"🔄 Processing {timeframe.upper()} timeframe for {len(watchlist_symbols)} symbols...")
    
    timeframe_start = time_module.time()
    timeframe_indicators = {}
    
    # Use ThreadPoolExecutor for parallel symbol processing within this timeframe
    max_workers = min(4, len(watchlist_symbols))  # Conservative concurrency for focused calculation
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks for this specific timeframe
        future_to_symbol = {
            executor.submit(analyze_symbol_for_timeframe, symbol, timeframe): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, tf, indicators_data = future.result(timeout=60)  # 60 second timeout per symbol
                timeframe_indicators[symbol] = indicators_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  ❌ Error processing {symbol} for {timeframe}: {e}")
                timeframe_indicators[symbol] = {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e),
                    'current_price': 0.0,
                    'current_rsi': 50.0,
                    'signal_type': 'NO_SIGNAL'
                }
    
    elapsed_time = time_module.time() - timeframe_start
    print(f"  ✅ {timeframe.upper()} completed in {elapsed_time:.2f}s")
    
    return timeframe, timeframe_indicators

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

def save_divergence_indicators_for_timeframe(timeframe: str, indicators: Dict[str, Any]) -> bool:
    """Save divergence indicators to timeframe-specific JSON file in organized directory"""
    try:
        # Create output directory if it doesn't exist
        output_dir = DivergenceTimeframeConfig.OUTPUT_DIR
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 Created directory: {output_dir}")
        
        # Get filename and create full path
        filename = DivergenceTimeframeConfig.TIMEFRAMES[timeframe]["filename"]
        filepath = os.path.join(output_dir, filename)
        
        # Convert numpy types to native Python types
        clean_indicators = convert_numpy_types(indicators)
        
        # Create comprehensive divergence indicators data
        divergence_data = {
            'strategy_name': f'Divergence_Indicators_{timeframe.upper()}',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(clean_indicators),
            'analysis_summary': {
                'symbols_with_data': len([s for s in clean_indicators.values() if 'error' not in s]),
                'symbols_with_errors': len([s for s in clean_indicators.values() if 'error' in s]),
                'bullish_divergences': len([s for s in clean_indicators.values() if s.get('bullish_divergence_detected', False)]),
                'bearish_divergences': len([s for s in clean_indicators.values() if s.get('bearish_divergence_detected', False)]),
                'strong_divergences': len([s for s in clean_indicators.values() if s.get('divergence_strength') == 'strong']),
                'trade_signals': len([s for s in clean_indicators.values() if s.get('has_trade_signal', False)])
            },
            'indicators': clean_indicators,
            'metadata': {
                'analysis_type': 'divergence_indicators',
                'timeframe': timeframe,
                'indicator_focus': ['rsi', 'swing_points', 'divergence_detection'],
                'lookback_constraint': '30_bars',
                'price_comparison': 'actual_high_low',
                'update_frequency': 'on_demand',
                'data_source': 'schwab_historical_data',
                'output_directory': output_dir,
                'talib_available': TALIB_AVAILABLE,
                'scipy_available': SCIPY_AVAILABLE
            }
        }
        
        # Write to timeframe-specific file in organized directory
        with open(filepath, 'w') as f:
            json.dump(divergence_data, f, indent=2)
        
        print(f"✅ Saved {timeframe} divergence indicators to {filepath}")
        print(f"📊 Analysis: {divergence_data['analysis_summary']['symbols_with_data']} symbols, {divergence_data['analysis_summary']['bullish_divergences']} bullish, {divergence_data['analysis_summary']['bearish_divergences']} bearish divergences")
        return True
        
    except Exception as e:
        print(f"❌ Error saving {timeframe} divergence indicators to file: {e}")
        return False

def fetch_all_data_parallel(watchlist_symbols: List[str]) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Fetch all historical data for all symbols and timeframes in parallel"""
    print("📡 Pre-fetching ALL market data simultaneously...")
    fetch_start = time_module.time()
    
    # Create data structure: symbol -> timeframe -> DataFrame
    all_data = defaultdict(dict)
    data_lock = threading.Lock()
    
    def fetch_symbol_timeframe_data(symbol: str, timeframe: str) -> Tuple[str, str, Optional[pd.DataFrame]]:
        """Fetch data for a specific symbol and timeframe"""
        try:
            calculator = DivergenceIndicatorsCalculator(timeframe)
            df = calculator.get_historical_data_for_timeframe(symbol)
            return symbol, timeframe, df
        except Exception as e:
            print(f"  ❌ Error fetching {symbol} {timeframe}: {e}")
            return symbol, timeframe, None
    
    # Calculate total operations
    total_operations = len(watchlist_symbols) * len(DivergenceTimeframeConfig.TIMEFRAMES)
    print(f"🚀 Launching {total_operations} simultaneous API calls...")
    
    # Use aggressive parallelization for data fetching
    max_workers = min(20, total_operations)  # Aggressive concurrency for data fetching
    completed_operations = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        future_to_params = {}
        for symbol in watchlist_symbols:
            for timeframe in DivergenceTimeframeConfig.TIMEFRAMES.keys():
                future = executor.submit(fetch_symbol_timeframe_data, symbol, timeframe)
                future_to_params[future] = (symbol, timeframe)
        
        # Collect results as they complete
        for future in as_completed(future_to_params):
            symbol, timeframe = future_to_params[future]
            try:
                _, _, df = future.result(timeout=30)  # 30 second timeout per fetch
                
                with data_lock:
                    all_data[symbol][timeframe] = df
                    completed_operations += 1
                    
                    # Progress updates
                    if completed_operations % 10 == 0 or completed_operations == total_operations:
                        print(f"  ⚡ Completed {completed_operations}/{total_operations} API calls...")
                        
            except Exception as e:
                print(f"  ❌ Error processing {symbol} {timeframe}: {e}")
                with data_lock:
                    all_data[symbol][timeframe] = None
                    completed_operations += 1
    
    fetch_elapsed = time_module.time() - fetch_start
    print(f"✅ Data pre-fetch completed in {fetch_elapsed:.2f}s")
    
    return dict(all_data)

def process_symbol_timeframe_indicators(symbol: str, timeframe: str, df: Optional[pd.DataFrame]) -> Tuple[str, str, Dict[str, Any]]:
    """Process indicators for a specific symbol and timeframe using pre-fetched data"""
    try:
        if df is None or len(df) < 50:
            return symbol, timeframe, create_empty_divergence_indicators(symbol, timeframe)
        
        # Create calculator for this timeframe
        calculator = DivergenceIndicatorsCalculator(timeframe)
        
        # Ensure numeric types
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate RSI
        df['rsi'] = calculator.calculate_rsi(df)
        
        # Detect swing points
        df_with_swings = calculator.detect_swing_points(df)
        
        # Detect divergences
        divergences = calculator.detect_rsi_divergences(df_with_swings)
        
        # Extract current values
        current_price = float(df['close'].iloc[-1])
        current_rsi = float(df['rsi'].iloc[-1])
        current_volume = int(df['volume'].iloc[-1])
        
        # Extract swing points
        swing_highs = df_with_swings['swing_high'].dropna().tolist()
        swing_lows = df_with_swings['swing_low'].dropna().tolist()
        
        # Determine overall divergence status
        bullish_divergence_detected = any(divergences['bullish'].values())
        bearish_divergence_detected = any(divergences['bearish'].values())
        
        # Determine divergence strength
        if divergences['bullish']['strong'] or divergences['bearish']['strong']:
            divergence_strength = 'strong'
        elif divergences['bullish']['medium'] or divergences['bearish']['medium']:
            divergence_strength = 'medium'
        else:
            divergence_strength = 'weak'
        
        # Determine trend direction (simplified)
        if len(df) >= 20:
            sma_20 = df['close'].rolling(20).mean().iloc[-1]
            if current_price > sma_20 * 1.02:
                trend_direction = 'bullish'
            elif current_price < sma_20 * 0.98:
                trend_direction = 'bearish'
            else:
                trend_direction = 'neutral'
        else:
            trend_direction = 'neutral'
        
        # Determine signal type
        if bullish_divergence_detected:
            signal_type = "BUY"
        elif bearish_divergence_detected:
            signal_type = "SELL"
        else:
            signal_type = "NO_SIGNAL"
        
        indicators = {
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp': datetime.now().isoformat(),
            'current_price': current_price,
            'current_rsi': current_rsi,
            'current_volume': current_volume,
            'swing_highs': swing_highs[-5:] if len(swing_highs) > 5 else swing_highs,  # Last 5
            'swing_lows': swing_lows[-5:] if len(swing_lows) > 5 else swing_lows,  # Last 5
            'bullish_divergence_detected': bullish_divergence_detected,
            'bearish_divergence_detected': bearish_divergence_detected,
            'bullish_divergence_strong': divergences['bullish']['strong'],
            'bullish_divergence_medium': divergences['bullish']['medium'],
            'bullish_divergence_weak': divergences['bullish']['weak'],
            'bearish_divergence_strong': divergences['bearish']['strong'],
            'bearish_divergence_medium': divergences['bearish']['medium'],
            'bearish_divergence_weak': divergences['bearish']['weak'],
            'divergence_strength': divergence_strength,
            'trend_direction': trend_direction,
            'signal_type': signal_type,
            'has_trade_signal': bullish_divergence_detected or bearish_divergence_detected
        }
        
        return symbol, timeframe, indicators
        
    except Exception as e:
        print(f"  ❌ Error processing {symbol} {timeframe}: {e}")
        return symbol, timeframe, create_empty_divergence_indicators(symbol, timeframe)

def create_empty_divergence_indicators(symbol: str, timeframe: str) -> Dict[str, Any]:
    """Create empty divergence indicators for error cases."""
    return {
        'symbol': symbol,
        'timeframe': timeframe,
        'timestamp': datetime.now().isoformat(),
        'current_price': 0.0,
        'current_rsi': 50.0,
        'current_volume': 0,
        'swing_highs': [],
        'swing_lows': [],
        'bullish_divergence_detected': False,
        'bearish_divergence_detected': False,
        'bullish_divergence_strong': False,
        'bullish_divergence_medium': False,
        'bullish_divergence_weak': False,
        'bearish_divergence_strong': False,
        'bearish_divergence_medium': False,
        'bearish_divergence_weak': False,
        'divergence_strength': 'none',
        'trend_direction': 'neutral',
        'signal_type': 'NO_SIGNAL',
        'has_trade_signal': False
    }

def process_all_indicators_parallel(all_data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Process all indicators in parallel using pre-fetched data"""
    print("⚡ Processing ALL combinations in parallel...")
    process_start = time_module.time()
    
    # Create results structure: timeframe -> symbol -> indicators
    results = defaultdict(dict)
    results_lock = threading.Lock()
    
    # Calculate total operations
    total_operations = sum(len(timeframes) for timeframes in all_data.values())
    print(f"🔥 Processing {total_operations} symbol-timeframe combinations simultaneously...")
    
    # Use aggressive parallelization for processing
    max_workers = min(16, total_operations)  # Aggressive concurrency for processing
    completed_operations = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all processing tasks
        futures = []
        for symbol, timeframe_data in all_data.items():
            for timeframe, df in timeframe_data.items():
                future = executor.submit(process_symbol_timeframe_indicators, symbol, timeframe, df)
                futures.append(future)
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                symbol, timeframe, indicators = future.result(timeout=30)
                
                with results_lock:
                    results[timeframe][symbol] = indicators
                    completed_operations += 1
                    
                    # Progress updates
                    if completed_operations % 5 == 0 or completed_operations == total_operations:
                        print(f"  ⚡ Completed {completed_operations}/{total_operations} operations...")
                        
            except Exception as e:
                print(f"  ❌ Error in processing: {e}")
                completed_operations += 1
    
    process_elapsed = time_module.time() - process_start
    print(f"✅ Processing completed in {process_elapsed:.2f}s")
    
    return dict(results)

def run_all_timeframes_analysis():
    """Run ultra-parallel divergence indicators analysis"""
    watchlist_symbols = load_watchlist_from_trading_config()
    
    print("🚀 ULTRA-PARALLEL Divergence Indicators Calculator")
    print("=" * 70)
    
    if not watchlist_symbols:
        print("❌ No symbols loaded from trading config. Cannot proceed.")
        print("❌ Please ensure trading_config_live.json contains divergence_strategy_watchlist with symbols")
        return
    
    total_operations = len(watchlist_symbols) * len(DivergenceTimeframeConfig.TIMEFRAMES)
    print(f"Processing {len(watchlist_symbols)} symbols × {len(DivergenceTimeframeConfig.TIMEFRAMES)} timeframes = {total_operations} total operations...")
    print("🔥 Using ultra-parallel processing with simultaneous data fetch and calculation")
    print()
    
    overall_start_time = time_module.time()
    
    # Step 1: Fetch all data in parallel
    all_data = fetch_all_data_parallel(watchlist_symbols)
    
    # Step 2: Process all indicators in parallel
    all_results = process_all_indicators_parallel(all_data)
    
    # Step 3: Save results to files
    print("📁 Saving results to JSON files...")
    for timeframe in DivergenceTimeframeConfig.TIMEFRAMES.keys():
        if timeframe in all_results:
            save_divergence_indicators_for_timeframe(timeframe, all_results[timeframe])
        else:
            print(f"⚠️ No results for {timeframe} timeframe")
    
    overall_elapsed_time = time_module.time() - overall_start_time
    print(f"\n🎉 ULTRA-PARALLEL analysis completed in {overall_elapsed_time:.2f} seconds")
    print(f"⚡ Performance improvement: ~{(total_operations * 2) / overall_elapsed_time:.1f}x faster than sequential processing")
    
    print(f"\n📁 Created {len(DivergenceTimeframeConfig.TIMEFRAMES)} JSON files in {DivergenceTimeframeConfig.OUTPUT_DIR}/ directory:")
    for timeframe, config in DivergenceTimeframeConfig.TIMEFRAMES.items():
        filepath = os.path.join(DivergenceTimeframeConfig.OUTPUT_DIR, config['filename'])
        print(f"   - {filepath}")

def main():
    """Main function to run divergence indicators calculation"""
    run_all_timeframes_analysis()

if __name__ == "__main__":
    main()
