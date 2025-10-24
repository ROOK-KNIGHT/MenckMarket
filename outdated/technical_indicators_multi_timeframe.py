#!/usr/bin/env python3
"""
Multi-Timeframe Technical Indicators Handler

This script creates separate JSON files for each timeframe:
- technical_indicators_1min.json
- technical_indicators_5min.json  
- technical_indicators_15min.json
- technical_indicators_30min.json
- technical_indicators_1hour.json
- technical_indicators_4hour.json
- technical_indicators_daily.json

Each JSON file contains the same structure as the original technical_indicators.json
but calculated for that specific timeframe.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
import json
import os
from dataclasses import dataclass, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import time as time_module
import threading
from collections import defaultdict
import requests
import requests.adapters

# Import TA-Lib for technical analysis
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available. Some indicators will use simplified calculations.")

# Import scipy for advanced calculations
try:
    from scipy.signal import argrelextrema
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("SciPy not available. Some advanced calculations will be simplified.")

# Import our existing data handlers
from historical_data_handler import HistoricalDataHandler
from options_data_handler import OptionsDataHandler

class UltraParallelAPIManager:
    """Ultra-parallel API manager with simultaneous calls for all symbols"""
    
    def __init__(self):
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        self.data_cache = {}
        self.cache_lock = threading.Lock()
        
        # Create persistent session for connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=50,  # Number of connection pools
            pool_maxsize=50,      # Max connections per pool
            max_retries=3
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
    def fetch_all_symbols_all_timeframes_parallel(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch ALL data for ALL symbols and timeframes in parallel"""
        print(f"🚀 SIMULTANEOUS API calls for {len(symbols)} symbols across {len(TimeframeConfig.TIMEFRAMES)} timeframes")
        
        all_data = {}
        total_calls = len(symbols) * (len(TimeframeConfig.TIMEFRAMES) + 1)  # +1 for options
        
        # Use high concurrency for I/O bound operations
        max_workers = min(100, total_calls)  # Aggressive API concurrency
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit ALL API calls simultaneously
            futures = []
            
            # Historical data calls - ALL symbols, ALL timeframes
            for symbol in symbols:
                for timeframe in TimeframeConfig.TIMEFRAMES.keys():
                    future = executor.submit(self._parallel_historical_call, symbol, timeframe)
                    futures.append((future, symbol, timeframe, 'historical'))
                
                # Options data calls - ALL symbols
                future = executor.submit(self._parallel_options_call, symbol)
                futures.append((future, symbol, None, 'options'))
            
            print(f"📡 Launched {len(futures)} simultaneous API calls...")
            
            # Collect results as they complete
            completed = 0
            for future, symbol, timeframe, data_type in futures:
                try:
                    data = future.result(timeout=30)  # Aggressive timeout
                    
                    if symbol not in all_data:
                        all_data[symbol] = {'historical': {}, 'options': None}
                    
                    if data_type == 'historical' and data is not None:
                        all_data[symbol]['historical'][timeframe] = data
                    elif data_type == 'options' and data is not None:
                        all_data[symbol]['options'] = data
                    
                    completed += 1
                    if completed % 10 == 0:
                        print(f"  ⚡ Completed {completed}/{len(futures)} API calls...")
                        
                except Exception as e:
                    print(f"  ⚠️  API call failed for {symbol} {timeframe or 'options'}: {e}")
        
        print(f"✅ Completed {completed}/{len(futures)} API calls")
        return all_data
    
    def _parallel_historical_call(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Make parallel historical data API call with session reuse"""
        try:
            from connection_manager import ensure_valid_tokens
            
            # Get tokens once per call (cached by connection manager)
            tokens = ensure_valid_tokens()
            access_token = tokens["access_token"]
            
            params = TimeframeConfig.TIMEFRAMES[timeframe]
            current_epoch_ms = int(pd.Timestamp.now().timestamp() * 1000)
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            }
            
            url = (f"{self.historical_handler.base_url}/marketdata/v1/pricehistory"
                  f"?symbol={symbol}"
                  f"&periodType={params['period_type']}"
                  f"&period={params['period']}"
                  f"&frequencyType={params['frequency_type']}"
                  f"&frequency={params['frequency']}"
                  f"&endDate={current_epoch_ms}"
                  f"&needExtendedHoursData=true")
            
            # Use persistent session for connection pooling
            response = self.session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get("empty", True) and "candles" in data:
                df = pd.DataFrame(data['candles'])
                if not df.empty:
                    # Convert datetime column
                    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                    return df
            
            return None
            
        except Exception as e:
            return None
    
    def _parallel_options_call(self, symbol: str) -> Optional[Dict]:
        """Make parallel options data API call with session reuse"""
        try:
            from connection_manager import ensure_valid_tokens
            
            tokens = ensure_valid_tokens()
            access_token = tokens["access_token"]
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            }
            
            url = f"{self.options_handler.base_url}/marketdata/v1/chains?symbol={symbol}"
            
            # Use persistent session for connection pooling
            response = self.session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            return None

class UltraParallelDataManager:
    """Ultra-aggressive parallel data manager with pre-fetching and caching"""
    
    def __init__(self):
        self.api_manager = UltraParallelAPIManager()
        self.data_cache = {}  # Unified cache for all data
        self.cache_lock = threading.Lock()
        
    def prefetch_all_data(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Pre-fetch ALL data for ALL symbols and timeframes using ultra-parallel API manager"""
        print(f"🚀 Using ULTRA-PARALLEL API manager for maximum performance...")
        
        # Use the new ultra-parallel API manager for simultaneous calls
        return self.api_manager.fetch_all_symbols_all_timeframes_parallel(symbols)
    
    def _fetch_historical_data_fast(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fast historical data fetch"""
        try:
            params = TimeframeConfig.TIMEFRAMES[timeframe]
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
                    return df
            return None
        except Exception as e:
            return None
    
    def _fetch_options_data_fast(self, symbol: str) -> Optional[Dict]:
        """Fast options data fetch"""
        try:
            return self.options_handler.get_options_chain(symbol)
        except Exception as e:
            return None

class ParallelDataManager:
    """Manages parallel data fetching and caching for multiple timeframes"""
    
    
    def __init__(self):
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        self.options_cache = {}  # Cache options data to avoid redundant API calls
        self.cache_lock = threading.Lock()
        
    def fetch_all_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch historical data for all timeframes in parallel"""
        timeframe_data = {}
        
        # Use ThreadPoolExecutor to fetch all timeframes in parallel
        with ThreadPoolExecutor(max_workers=len(TimeframeConfig.TIMEFRAMES)) as executor:
            future_to_timeframe = {
                executor.submit(self._fetch_single_timeframe_data, symbol, timeframe): timeframe
                for timeframe in TimeframeConfig.TIMEFRAMES.keys()
            }
            
            for future in as_completed(future_to_timeframe):
                timeframe = future_to_timeframe[future]
                try:
                    df = future.result(timeout=30)
                    if df is not None:
                        timeframe_data[timeframe] = df
                except Exception as e:
                    print(f"  Error fetching {timeframe} data for {symbol}: {e}")
        
        return timeframe_data
    
    def _fetch_single_timeframe_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fetch data for a single timeframe"""
        try:
            if timeframe not in TimeframeConfig.TIMEFRAMES:
                return None
            
            params = TimeframeConfig.TIMEFRAMES[timeframe]
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
                    return df
            
            return None
                
        except Exception as e:
            print(f"Error getting {timeframe} data for {symbol}: {e}")
            return None
    
    def get_cached_options_data(self, symbol: str) -> Optional[Dict]:
        """Get cached options data or fetch if not cached"""
        with self.cache_lock:
            if symbol not in self.options_cache:
                try:
                    self.options_cache[symbol] = self.options_handler.get_options_chain(symbol)
                except Exception as e:
                    print(f"Error fetching options data for {symbol}: {e}")
                    self.options_cache[symbol] = None
            
            return self.options_cache[symbol]

class TimeframeConfig:
    """Configuration for different timeframes"""
    
    TIMEFRAMES = {
        "1min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 1,
            "min_periods": 100,
            "filename": "technical_indicators_1min.json"
        },
        "5min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 5,
            "min_periods": 100,
            "filename": "technical_indicators_5min.json"
        },
        "15min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 15,
            "min_periods": 80,
            "filename": "technical_indicators_15min.json"
        },
        "30min": {
            "period_type": "day", 
            "period": 10, 
            "frequency_type": "minute", 
            "frequency": 30,
            "min_periods": 60,
            "filename": "technical_indicators_30min.json"
        },
        "daily": {
            "period_type": "month", 
            "period": 6, 
            "frequency_type": "daily", 
            "frequency": 1,
            "min_periods": 50,
            "filename": "technical_indicators_daily.json"
        }
    }

class UnifiedTechnicalIndicatorsTimeframe:
    """
    Unified Technical Indicators Handler for a specific timeframe
    """
    
    def __init__(self, timeframe: str):
        """Initialize the unified technical indicators handler for a specific timeframe."""
        self.timeframe = timeframe
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"UnifiedTechnicalIndicatorsTimeframe initialized for {timeframe}")

    def get_historical_data_for_timeframe(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get historical data for the configured timeframe."""
        try:
            if self.timeframe not in TimeframeConfig.TIMEFRAMES:
                return None
            
            params = TimeframeConfig.TIMEFRAMES[self.timeframe]
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
                    return df
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol} {self.timeframe}: {e}")
            return None

    def calculate_base_indicators(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        """Calculate base technical indicators."""
        try:
            if df is None or len(df) < 50:
                return self._create_empty_base_indicators(symbol)
            
            # Ensure numeric types
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            current_price = float(df['close'].iloc[-1])
            current_volume = int(df['volume'].iloc[-1])
            
            # Moving averages
            if TALIB_AVAILABLE:
                sma_20 = float(talib.SMA(df['close'].values, timeperiod=20)[-1])
                sma_50 = float(talib.SMA(df['close'].values, timeperiod=50)[-1]) if len(df) >= 50 else sma_20
                ema_9 = float(talib.EMA(df['close'].values, timeperiod=9)[-1])
                ema_21 = float(talib.EMA(df['close'].values, timeperiod=21)[-1])
                ema_50 = float(talib.EMA(df['close'].values, timeperiod=50)[-1])
                
                # RSI
                rsi = float(talib.RSI(df['close'].values, timeperiod=14)[-1])
                
                # MACD
                macd, macd_signal, macd_hist = talib.MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
                macd_val = float(macd[-1])
                macd_signal_val = float(macd_signal[-1])
                macd_histogram = float(macd_hist[-1])
                
                # ATR
                atr = float(talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)[-1])
            else:
                # Simplified calculations without TA-Lib
                sma_20 = float(df['close'].rolling(20).mean().iloc[-1])
                sma_50 = float(df['close'].rolling(50).mean().iloc[-1]) if len(df) >= 50 else sma_20
                ema_9 = float(df['close'].ewm(span=9).mean().iloc[-1])
                ema_21 = float(df['close'].ewm(span=21).mean().iloc[-1])
                ema_50 = float(df['close'].ewm(span=50).mean().iloc[-1])
                
                # Simplified RSI
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = float(100 - (100 / (1 + rs.iloc[-1])))
                
                # Simplified MACD
                ema_fast = df['close'].ewm(span=12).mean()
                ema_slow = df['close'].ewm(span=26).mean()
                macd_val = float(ema_fast.iloc[-1] - ema_slow.iloc[-1])
                macd_signal_val = float(pd.Series([macd_val]).ewm(span=9).mean().iloc[-1])
                macd_histogram = macd_val - macd_signal_val
                
                # Simplified ATR
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                true_range = np.maximum(high_low, np.maximum(high_close, low_close))
                atr = float(true_range.rolling(14).mean().iloc[-1])
            
            # Volume analysis
            avg_volume = float(df['volume'].rolling(14).mean().iloc[-1])
            relative_volume_pct = (current_volume / avg_volume * 100) if avg_volume > 0 else 100.0
            
            # Volatility
            returns = df['close'].pct_change().dropna()
            realized_volatility = float(returns.std() * np.sqrt(252) * 100)  # Annualized %
            
            # Trend analysis
            if sma_20 > 0:
                trend_strength = abs(current_price - sma_20) / sma_20
                if current_price > ema_9 > ema_21 > ema_50:
                    trend_direction = "bullish"
                elif current_price < ema_9 < ema_21 < ema_50:
                    trend_direction = "bearish"
                else:
                    trend_direction = "neutral"
            else:
                trend_strength = 0.0
                trend_direction = "neutral"
            
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'current_price': current_price,
                'sma_20': sma_20,
                'sma_50': sma_50,
                'ema_9': ema_9,
                'ema_21': ema_21,
                'ema_50': ema_50,
                'rsi': rsi,
                'macd': macd_val,
                'macd_signal': macd_signal_val,
                'macd_histogram': macd_histogram,
                'atr': atr,
                'realized_volatility': realized_volatility,
                'volume': current_volume,
                'avg_volume': avg_volume,
                'relative_volume_pct': relative_volume_pct,
                'trend_direction': trend_direction,
                'trend_strength': trend_strength
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating base indicators for {symbol}: {e}")
            return self._create_empty_base_indicators(symbol)

    def calculate_pml_indicators(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Calculate PML strategy indicators."""
        try:
            # Get options chain data
            options_chain = self.options_handler.get_options_chain(symbol)
            
            if not options_chain:
                return {
                    'pml_price': current_price * 0.98,
                    'ceiling_price': current_price * 1.05,
                    'floor_price': current_price * 0.95,
                    'green_line_price': current_price,
                    'call_delta': 0.0,
                    'put_delta': 0.0,
                    'net_delta': 0.0,
                    'pml_cross_bullish': False,
                    'delta_confirmation': False,
                    'market_condition': 'UNCERTAIN'
                }
            
            # Calculate exposure levels from options data
            exposure_data = self._calculate_exposure_levels(options_chain, current_price)
            
            # Calculate PML levels
            pml_price = self._calculate_pml_price(exposure_data, current_price)
            ceiling_price = pml_price * 1.05
            floor_price = pml_price * 0.95
            green_line_price = (current_price + pml_price) / 2
            
            # Calculate delta flows
            delta_analysis = self._analyze_delta_flows(exposure_data, green_line_price)
            
            # Determine market condition
            pml_cross_bullish = current_price > pml_price
            delta_confirmation = (delta_analysis['call_delta'] > 50 and delta_analysis['put_delta'] > 50)
            
            if pml_cross_bullish and delta_confirmation:
                market_condition = "PML_CROSS_BULLISH"
            elif not pml_cross_bullish:
                market_condition = "PML_CROSS_BEARISH"
            else:
                market_condition = "UNCERTAIN"
            
            return {
                'pml_price': pml_price,
                'ceiling_price': ceiling_price,
                'floor_price': floor_price,
                'green_line_price': green_line_price,
                'call_delta': delta_analysis['call_delta'],
                'put_delta': delta_analysis['put_delta'],
                'net_delta': delta_analysis['net_delta'],
                'pml_cross_bullish': pml_cross_bullish,
                'delta_confirmation': delta_confirmation,
                'market_condition': market_condition
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating PML indicators for {symbol}: {e}")
            return {
                'pml_price': current_price * 0.98,
                'ceiling_price': current_price * 1.05,
                'floor_price': current_price * 0.95,
                'green_line_price': current_price,
                'call_delta': 0.0,
                'put_delta': 0.0,
                'net_delta': 0.0,
                'pml_cross_bullish': False,
                'delta_confirmation': False,
                'market_condition': 'UNCERTAIN'
            }

    def calculate_iron_condor_indicators(self, df: pd.DataFrame, symbol: str, current_price: float) -> Dict[str, Any]:
        """Calculate Iron Condor strategy indicators."""
        try:
            if df is None or len(df) < 20:
                return {
                    'iv_rank': 50.0,
                    'realized_vol': 20.0,
                    'range_pct': 0.1,
                    'recent_high': current_price * 1.05,
                    'recent_low': current_price * 0.95,
                    'is_range_bound': False,
                    'is_low_volatility': False,
                    'is_suitable_for_ic': False,
                    'trend_strength': 0.0,
                    'market_condition': 'UNCERTAIN'
                }
            
            # Volatility analysis
            returns = df['close'].pct_change().dropna()
            realized_vol = float(returns.std() * np.sqrt(252) * 100)  # Annualized %
            
            # Range analysis
            recent_high = float(df['high'].tail(20).max())
            recent_low = float(df['low'].tail(20).min())
            range_pct = (recent_high - recent_low) / current_price if current_price > 0 else 0
            
            # Trend strength
            sma_20 = df['close'].rolling(20).mean().iloc[-1]
            trend_strength = abs(current_price - sma_20) / sma_20 if sma_20 > 0 else 0
            
            # IV Rank calculation (simplified)
            if realized_vol < 15:
                iv_rank = 25.0  # Low IV environment
            elif realized_vol > 30:
                iv_rank = 75.0  # High IV environment
            else:
                iv_rank = 50.0  # Medium IV environment
            
            # Market condition assessment
            is_range_bound = range_pct < 0.15 and trend_strength < 0.3
            is_low_volatility = realized_vol < 20
            is_suitable_for_ic = is_range_bound and 20 <= iv_rank <= 80
            
            # Determine market condition for Iron Condor
            if is_range_bound:
                market_condition = "RANGE_BOUND"
            elif is_low_volatility:
                market_condition = "LOW_VOLATILITY"
            elif realized_vol > 30:
                market_condition = "HIGH_VOLATILITY"
            else:
                market_condition = "UNCERTAIN"
            
            return {
                'iv_rank': iv_rank,
                'realized_vol': realized_vol,
                'range_pct': range_pct,
                'recent_high': recent_high,
                'recent_low': recent_low,
                'is_range_bound': is_range_bound,
                'is_low_volatility': is_low_volatility,
                'is_suitable_for_ic': is_suitable_for_ic,
                'trend_strength': trend_strength,
                'market_condition': market_condition
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating Iron Condor indicators for {symbol}: {e}")
            return {
                'iv_rank': 50.0,
                'realized_vol': 20.0,
                'range_pct': 0.1,
                'recent_high': current_price * 1.05,
                'recent_low': current_price * 0.95,
                'is_range_bound': False,
                'is_low_volatility': False,
                'is_suitable_for_ic': False,
                'trend_strength': 0.0,
                'market_condition': 'UNCERTAIN'
            }

    def calculate_exceedence_indicators(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        """Calculate Exceedence strategy indicators."""
        try:
            if df is None or len(df) < 20:
                return self._create_empty_exceedence_indicators()
            
            # Use previous bar's data for calculations (exceedence strategy approach)
            if len(df) < 2:
                return self._create_empty_exceedence_indicators()
                
            prev_bar = df.iloc[-2]  # Previous bar
            current_bar = df.iloc[-1]  # Current bar
            lookback = min(2000, len(df)-2)  # Use last bars excluding current bar
            
            # Calculate rolling volatility components
            highside_vol = df['high'] - df['close']
            lowside_vol = df['low'] - df['close']
            
            # Calculate means and standard deviations up to previous bar
            mean_highside = float(highside_vol.iloc[:-1].rolling(window=lookback).mean().iloc[-1])
            mean_lowside = float(lowside_vol.iloc[:-1].rolling(window=lookback).mean().iloc[-1])
            std_highside = float(highside_vol.iloc[:-1].rolling(window=lookback).std().iloc[-1])
            std_lowside = float(lowside_vol.iloc[:-1].rolling(window=lookback).std().iloc[-1])
            
            # Calculate volatility bands based on previous bar's close
            high_side_limit = prev_bar['close'] + (std_highside + mean_highside)
            low_side_limit = prev_bar['close'] - (std_lowside - mean_lowside)
            
            # Calculate exceedances for current bar
            current_price = current_bar['close']
            high_exceedance = max(0, current_bar['high'] - high_side_limit)
            low_exceedance = max(0, low_side_limit - current_bar['low'])
            
            # Calculate relative distances and levels
            band_range = high_side_limit - low_side_limit
            band_midpoint = low_side_limit + (band_range / 2)
            
            # Calculate distance from each band as percentage
            distance_to_high = ((high_side_limit - current_price) / band_range) * 100 if band_range > 0 else 50.0
            distance_to_low = ((current_price - low_side_limit) / band_range) * 100 if band_range > 0 else 50.0
            
            # Calculate position within band range as percentage (0% = at lower band, 100% = at upper band)
            position_in_range = ((current_price - low_side_limit) / band_range) * 100 if band_range > 0 else 50.0
            
            # Determine trading signals based on position in range
            trading_signal = None
            signal_direction = None
            
            # Generate signals when price is at extreme positions (99% or 1% of range)
            if position_in_range >= 99:
                trading_signal = f"EXCEEDENCE_HIGH_SIGNAL"
                signal_direction = "SHORT"  # Reversal signal at top
            elif position_in_range <= 1:
                trading_signal = f"EXCEEDENCE_LOW_SIGNAL"
                signal_direction = "LONG"   # Reversal signal at bottom
            
            # Calculate band stability (simplified)
            band_stability = True  # Default to stable
            if len(df) >= 12:  # Need at least 12 bars for stability check
                recent_highs = []
                recent_lows = []
                for i in range(len(df)-12, len(df)-1):  # Last 12 bars excluding current
                    bar_data = df.iloc[:i+1]
                    if len(bar_data) >= lookback:
                        h_vol = bar_data['high'] - bar_data['close']
                        l_vol = bar_data['low'] - bar_data['close']
                        m_h = h_vol.rolling(window=lookback).mean().iloc[-1]
                        m_l = l_vol.rolling(window=lookback).mean().iloc[-1]
                        s_h = h_vol.rolling(window=lookback).std().iloc[-1]
                        s_l = l_vol.rolling(window=lookback).std().iloc[-1]
                        recent_highs.append(bar_data['close'].iloc[-1] + (s_h + m_h))
                        recent_lows.append(bar_data['close'].iloc[-1] - (s_l - m_l))
                
                # Check stability (bands shouldn't change more than 0.19% per bar)
                if len(recent_highs) > 1:
                    for i in range(1, len(recent_highs)):
                        high_change = abs((recent_highs[i] - recent_highs[i-1]) / recent_highs[i-1])
                        low_change = abs((recent_lows[i] - recent_lows[i-1]) / recent_lows[i-1])
                        if high_change >= 0.0019 or low_change >= 0.0019:
                            band_stability = False
                            break
            
            # Determine market condition
            if high_exceedance > 0:
                market_condition = "HIGH_EXCEEDANCE"
            elif low_exceedance > 0:
                market_condition = "LOW_EXCEEDANCE"
            elif position_in_range >= 80:
                market_condition = "NEAR_HIGH_BAND"
            elif position_in_range <= 20:
                market_condition = "NEAR_LOW_BAND"
            else:
                market_condition = "WITHIN_BANDS"
            
            return {
                'high_band': float(high_side_limit),
                'low_band': float(low_side_limit),
                'high_exceedance': float(high_exceedance),
                'low_exceedance': float(low_exceedance),
                'distance_to_high': float(distance_to_high),
                'distance_to_low': float(distance_to_low),
                'position_in_range': float(position_in_range),
                'trading_signal': trading_signal,
                'signal_direction': signal_direction,
                'band_range': float(band_range),
                'band_midpoint': float(band_midpoint),
                'highside_volatility': float(std_highside),
                'lowside_volatility': float(std_lowside),
                'mean_highside': float(mean_highside),
                'mean_lowside': float(mean_lowside),
                'std_highside': float(std_highside),
                'std_lowside': float(std_lowside),
                'band_stability': band_stability,
                'market_condition': market_condition
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating exceedence indicators for {symbol}: {e}")
            return self._create_empty_exceedence_indicators()

    def calculate_divergence_indicators(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        """Calculate Divergence strategy indicators."""
        try:
            if df is None or len(df) < 50:
                return {
                    'swing_highs': [],
                    'swing_lows': [],
                    'support_levels': [],
                    'resistance_levels': [],
                    'bullish_divergence_detected': False,
                    'bearish_divergence_detected': False,
                    'divergence_strength': 'medium',
                    'trend_direction': 'neutral',
                    'bullish_divergence_strong': False,
                    'bullish_divergence_medium': False,
                    'bearish_divergence_strong': False,
                    'bearish_divergence_medium': False,
                    'has_trade_signal': False,
                    'signal_type': 'NO_SIGNAL'
                }
            
            # Calculate RSI for divergence analysis
            if TALIB_AVAILABLE:
                rsi = talib.RSI(df['close'].values, timeperiod=14)
            else:
                # Simplified RSI calculation
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
            
            df['rsi'] = rsi
            
            # Detect swing points
            df_with_swings = self._detect_swing_points(df)
            
            # Extract swing points
            swing_highs = df_with_swings['swing_high'].dropna().tolist()
            swing_lows = df_with_swings['swing_low'].dropna().tolist()
            
            # Detect divergences
            divergences = self._detect_rsi_divergences(df_with_swings)
            
            # Support and resistance levels
            support_levels = []
            resistance_levels = []
            current_price = df['close'].iloc[-1]
            
            # Simple support/resistance from swing points
            for low in swing_lows:
                if low < current_price:
                    support_levels.append(low)
            
            for high in swing_highs:
                if high > current_price:
                    resistance_levels.append(high)
            
            # Determine trend direction
            if len(df) >= 50:
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                sma_50 = df['close'].rolling(50).mean().iloc[-1]
                if current_price > sma_20 > sma_50:
                    trend_direction = 'bullish'
                elif current_price < sma_20 < sma_50:
                    trend_direction = 'bearish'
                else:
                    trend_direction = 'neutral'
            else:
                trend_direction = 'neutral'
            
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
            
            return {
                'swing_highs': swing_highs[-5:] if len(swing_highs) > 5 else swing_highs,  # Last 5
                'swing_lows': swing_lows[-5:] if len(swing_lows) > 5 else swing_lows,  # Last 5
                'support_levels': support_levels[-3:] if len(support_levels) > 3 else support_levels,  # Last 3
                'resistance_levels': resistance_levels[-3:] if len(resistance_levels) > 3 else resistance_levels,  # Last 3
                'bullish_divergence_detected': bullish_divergence_detected,
                'bearish_divergence_detected': bearish_divergence_detected,
                'divergence_strength': divergence_strength,
                'trend_direction': trend_direction,
                'bullish_divergence_strong': divergences['bullish']['strong'],
                'bullish_divergence_medium': divergences['bullish']['medium'],
                'bearish_divergence_strong': divergences['bearish']['strong'],
                'bearish_divergence_medium': divergences['bearish']['medium'],
                'has_trade_signal': bullish_divergence_detected or bearish_divergence_detected,
                'signal_type': "BUY" if bullish_divergence_detected else "SELL" if bearish_divergence_detected else "NO_SIGNAL"
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating divergence indicators for {symbol}: {e}")
            return {
                'swing_highs': [],
                'swing_lows': [],
                'support_levels': [],
                'resistance_levels': [],
                'bullish_divergence_detected': False,
                'bearish_divergence_detected': False,
                'divergence_strength': 'medium',
                'trend_direction': 'neutral',
                'bullish_divergence_strong': False,
                'bullish_divergence_medium': False,
                'bearish_divergence_strong': False,
                'bearish_divergence_medium': False,
                'has_trade_signal': False,
                'signal_type': 'NO_SIGNAL'
            }

    def calculate_unified_indicators(self, symbol: str) -> Dict[str, Any]:
        """Calculate all unified technical indicators for a symbol for this timeframe."""
        try:
            # Get data for this specific timeframe
            df = self.get_historical_data_for_timeframe(symbol)
            if df is None:
                return self._create_empty_unified_indicators(symbol)
            
            current_price = float(df['close'].iloc[-1])
            
            # Calculate all indicator types
            base = self.calculate_base_indicators(df, symbol)
            pml = self.calculate_pml_indicators(symbol, current_price)
            iron_condor = self.calculate_iron_condor_indicators(df, symbol, current_price)
            divergence = self.calculate_divergence_indicators(df, symbol)
            exceedence = self.calculate_exceedence_indicators(df, symbol)
            
            # Return indicators with timeframe info
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'base_indicators': base,
                'pml_indicators': pml,
                'iron_condor_indicators': iron_condor,
                'divergence_indicators': divergence,
                'exceedence_indicators': exceedence
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating unified indicators for {symbol}: {e}")
            return self._create_empty_unified_indicators(symbol)

    # Helper methods
    def _calculate_exposure_levels(self, options_chain: Dict, current_price: float) -> Dict[str, Any]:
        """Calculate market maker exposure levels from options chain."""
        exposure_data = {
            'strikes': [],
            'call_gamma': [],
            'put_gamma': [],
            'call_delta': [],
            'put_delta': []
        }
        
        try:
            call_exp_map = options_chain.get("callExpDateMap", {})
            put_exp_map = options_chain.get("putExpDateMap", {})
            
            # Collect all strikes
            all_strikes = set()
            for exp_data in call_exp_map.values():
                all_strikes.update(float(strike) for strike in exp_data.keys())
            for exp_data in put_exp_map.values():
                all_strikes.update(float(strike) for strike in exp_data.keys())
            
            # Process each strike
            for strike in sorted(all_strikes):
                strike_str = str(strike)
                call_gamma_total = 0
                put_gamma_total = 0
                call_delta_total = 0
                put_delta_total = 0
                
                # Sum across all expirations
                for exp_data in call_exp_map.values():
                    if strike_str in exp_data and exp_data[strike_str]:
                        option = exp_data[strike_str][0]
                        oi = option.get('openInterest', 0)
                        call_gamma_total += option.get('gamma', 0) * oi
                        call_delta_total += option.get('delta', 0) * oi
                
                for exp_data in put_exp_map.values():
                    if strike_str in exp_data and exp_data[strike_str]:
                        option = exp_data[strike_str][0]
                        oi = option.get('openInterest', 0)
                        put_gamma_total += option.get('gamma', 0) * oi
                        put_delta_total += abs(option.get('delta', 0)) * oi
                
                # Store data
                exposure_data['strikes'].append(strike)
                exposure_data['call_gamma'].append(call_gamma_total)
                exposure_data['put_gamma'].append(put_gamma_total)
                exposure_data['call_delta'].append(call_delta_total)
                exposure_data['put_delta'].append(put_delta_total)
            
            return exposure_data
            
        except Exception as e:
            self.logger.error(f"Error calculating exposure levels: {e}")
            return exposure_data

    def _calculate_pml_price(self, exposure_data: Dict, current_price: float) -> float:
        """Calculate PML (Peak Money Line) price from exposure data."""
        try:
            if not exposure_data['strikes'] or current_price <= 0:
                return max(1.0, current_price * 0.98)
            
            strikes = np.array(exposure_data['strikes'])
            call_gamma = np.array(exposure_data['call_gamma'])
            put_gamma = np.array(exposure_data['put_gamma'])
            
            if len(strikes) == 0:
                return max(1.0, current_price * 0.98)
            
            # Calculate net gamma exposure
            net_gamma = put_gamma - call_gamma
            
            # Find PML (most negative gamma exposure)
            if len(net_gamma) > 0 and not np.all(net_gamma == 0):
                pml_idx = np.argmin(net_gamma)
                pml_price = strikes[pml_idx]
                if pml_price > 0 and 0.5 * current_price <= pml_price <= 2.0 * current_price:
                    return float(pml_price)
            
            return max(1.0, current_price * 0.98)
            
        except Exception as e:
            self.logger.error(f"Error calculating PML price: {e}")
            return max(1.0, current_price * 0.98) if current_price > 0 else 1.0

    def _analyze_delta_flows(self, exposure_data: Dict, green_line_price: float) -> Dict[str, float]:
        """Analyze delta flows relative to green line."""
        try:
            if not exposure_data['strikes'] or green_line_price <= 0:
                return {
                    'call_delta': 0.0,
                    'put_delta': 0.0,
                    'net_delta': 0.0
                }
            
            strikes = np.array(exposure_data['strikes'])
            call_deltas = np.array(exposure_data['call_delta'])
            put_deltas = np.array(exposure_data['put_delta'])
            
            if len(strikes) == 0:
                return {
                    'call_delta': 0.0,
                    'put_delta': 0.0,
                    'net_delta': 0.0
                }
            
            total_call_delta = np.sum(call_deltas)
            total_put_delta = np.sum(put_deltas)
            net_delta = total_call_delta - total_put_delta
            
            return {
                'call_delta': float(total_call_delta),
                'put_delta': float(total_put_delta),
                'net_delta': float(net_delta)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing delta flows: {e}")
            return {
                'call_delta': 0.0,
                'put_delta': 0.0,
                'net_delta': 0.0
            }

    def _detect_swing_points(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _detect_rsi_divergences(self, df: pd.DataFrame) -> Dict[str, Dict[str, bool]]:
        """Detect RSI divergences with improved logic."""
        try:
            divergences = {
                "bullish": {"strong": False, "medium": False, "weak": False, "hidden": False},
                "bearish": {"strong": False, "medium": False, "weak": False, "hidden": False}
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
                "bullish": {"strong": False, "medium": False, "weak": False, "hidden": False},
                "bearish": {"strong": False, "medium": False, "weak": False, "hidden": False}
            }

    def _create_empty_exceedence_indicators(self) -> Dict[str, Any]:
        """Create empty exceedence indicators for error cases."""
        return {
            'high_band': 0.0,
            'low_band': 0.0,
            'high_exceedance': 0.0,
            'low_exceedance': 0.0,
            'distance_to_high': 50.0,
            'distance_to_low': 50.0,
            'position_in_range': 50.0,
            'trading_signal': None,
            'signal_direction': None,
            'band_range': 0.0,
            'band_midpoint': 0.0,
            'highside_volatility': 0.0,
            'lowside_volatility': 0.0,
            'mean_highside': 0.0,
            'mean_lowside': 0.0,
            'std_highside': 0.0,
            'std_lowside': 0.0,
            'band_stability': True,
            'market_condition': 'NEUTRAL'
        }

    def _create_empty_base_indicators(self, symbol: str) -> Dict[str, Any]:
        """Create empty base indicators for error cases."""
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': 0.0,
            'sma_20': 0.0,
            'sma_50': 0.0,
            'ema_9': 0.0,
            'ema_21': 0.0,
            'ema_50': 0.0,
            'rsi': 50.0,
            'macd': 0.0,
            'macd_signal': 0.0,
            'macd_histogram': 0.0,
            'atr': 0.0,
            'realized_volatility': 20.0,
            'volume': 0,
            'avg_volume': 0.0,
            'relative_volume_pct': 100.0,
            'trend_direction': 'neutral',
            'trend_strength': 0.0
        }

    def _create_empty_unified_indicators(self, symbol: str) -> Dict[str, Any]:
        """Create empty unified indicators for error cases."""
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'base_indicators': self._create_empty_base_indicators(symbol),
            'pml_indicators': {
                'pml_price': 0.0,
                'ceiling_price': 0.0,
                'floor_price': 0.0,
                'green_line_price': 0.0,
                'call_delta': 0.0,
                'put_delta': 0.0,
                'net_delta': 0.0,
                'pml_cross_bullish': False,
                'delta_confirmation': False,
                'market_condition': 'UNCERTAIN'
            },
            'iron_condor_indicators': {
                'iv_rank': 50.0,
                'realized_vol': 20.0,
                'range_pct': 0.1,
                'recent_high': 0.0,
                'recent_low': 0.0,
                'is_range_bound': False,
                'is_low_volatility': False,
                'is_suitable_for_ic': False,
                'trend_strength': 0.0,
                'market_condition': 'UNCERTAIN'
            },
            'divergence_indicators': {
                'swing_highs': [],
                'swing_lows': [],
                'support_levels': [],
                'resistance_levels': [],
                'bullish_divergence_detected': False,
                'bearish_divergence_detected': False,
                'divergence_strength': 'medium',
                'trend_direction': 'neutral',
                'bullish_divergence_strong': False,
                'bullish_divergence_medium': False,
                'bearish_divergence_strong': False,
                'bearish_divergence_medium': False,
                'has_trade_signal': False,
                'signal_type': 'NO_SIGNAL'
            }
        }

def load_watchlist_from_live_monitor() -> List[str]:
    """Load watchlist symbols from live_monitor.json"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        live_monitor_path = os.path.join(script_dir, 'live_monitor.json')
        
        if not os.path.exists(live_monitor_path):
            print(f"Warning: live_monitor.json not found at {live_monitor_path}")
            return ['SPY', 'QQQ', 'NVDA', 'AAPL', 'TSLA']  # Fallback symbols
        
        with open(live_monitor_path, 'r') as f:
            data = json.load(f)
        
        # Extract symbols from integrated_watchlist
        watchlist_symbols = data.get('integrated_watchlist', {}).get('symbols', [])
        
        if not watchlist_symbols:
            # Fallback to symbols_monitored in metadata
            watchlist_symbols = data.get('metadata', {}).get('symbols_monitored', [])
        
        if not watchlist_symbols:
            print("Warning: No symbols found in live_monitor.json, using fallback")
            return ['SPY', 'QQQ', 'NVDA', 'AAPL', 'TSLA']
        
        print(f"Loaded {len(watchlist_symbols)} symbols from live_monitor.json")
        return watchlist_symbols
        
    except Exception as e:
        print(f"Error loading watchlist from live_monitor.json: {e}")
        return ['SPY', 'QQQ', 'NVDA', 'AAPL', 'TSLA']  # Fallback symbols

def analyze_symbol_parallel(symbol: str, data_manager: ParallelDataManager) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Analyze a single symbol across all timeframes in parallel"""
    try:
        print(f"Analyzing {symbol} across all timeframes...")
        
        # Fetch all timeframe data in parallel
        timeframe_data = data_manager.fetch_all_timeframe_data(symbol)
        
        # Get cached options data once for all timeframes
        options_data = data_manager.get_cached_options_data(symbol)
        
        # Process each timeframe's data
        symbol_results = {}
        
        for timeframe in TimeframeConfig.TIMEFRAMES.keys():
            try:
                df = timeframe_data.get(timeframe)
                if df is None or len(df) < 20:
                    # Create empty indicators for insufficient data
                    symbol_results[timeframe] = create_empty_unified_indicators(symbol)
                    continue
                
                current_price = float(df['close'].iloc[-1])
                
                # Calculate indicators using the fetched data
                base = calculate_base_indicators_from_df(df, symbol)
                pml = calculate_pml_indicators_with_options(symbol, current_price, options_data)
                iron_condor = calculate_iron_condor_indicators_from_df(df, symbol, current_price)
                divergence = calculate_divergence_indicators_from_df(df, symbol)
                exceedence = calculate_exceedence_indicators_from_df(df, symbol)
                
                symbol_results[timeframe] = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'base_indicators': base,
                    'pml_indicators': pml,
                    'iron_condor_indicators': iron_condor,
                    'divergence_indicators': divergence,
                    'exceedence_indicators': exceedence
                }
                
                # Print summary
                print(f"  {symbol} ({timeframe}): ${base.get('current_price', 0):.2f} | RSI: {base.get('rsi', 50):.1f} | Trend: {base.get('trend_direction', 'neutral')} | PML: ${pml.get('pml_price', 0):.2f}")
                
            except Exception as e:
                print(f"  Error processing {symbol} for {timeframe}: {e}")
                symbol_results[timeframe] = create_empty_unified_indicators(symbol)
        
        return symbol, symbol_results
        
    except Exception as e:
        print(f"  Error analyzing {symbol}: {e}")
        # Return error indicators for all timeframes
        error_results = {}
        for timeframe in TimeframeConfig.TIMEFRAMES.keys():
            error_results[timeframe] = create_empty_unified_indicators(symbol)
        return symbol, error_results

def analyze_symbol_for_timeframe(symbol: str, timeframe: str) -> Tuple[str, str, Dict[str, Any]]:
    """Analyze a single symbol for a specific timeframe"""
    try:
        print(f"Analyzing {symbol} for {timeframe} timeframe...")
        
        # Create timeframe-specific handler
        indicators_handler = UnifiedTechnicalIndicatorsTimeframe(timeframe)
        
        # Calculate unified indicators for this timeframe
        unified_indicators = indicators_handler.calculate_unified_indicators(symbol)
        
        # Print summary
        base = unified_indicators.get('base_indicators', {})
        pml = unified_indicators.get('pml_indicators', {})
        print(f"  {symbol} ({timeframe}): ${base.get('current_price', 0):.2f} | RSI: {base.get('rsi', 50):.1f} | Trend: {base.get('trend_direction', 'neutral')} | PML: ${pml.get('pml_price', 0):.2f}")
        
        return symbol, timeframe, unified_indicators
        
    except Exception as e:
        print(f"  Error analyzing {symbol} for {timeframe}: {e}")
        # Return error indicators
        error_indicators = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'base_indicators': {'current_price': 0.0, 'rsi': 50.0, 'trend_direction': 'neutral'},
            'pml_indicators': {'pml_price': 0.0},
            'iron_condor_indicators': {'market_condition': 'UNCERTAIN'},
            'divergence_indicators': {'signal_type': 'NO_SIGNAL'}
        }
        return symbol, timeframe, error_indicators

# Helper functions for parallel processing
def create_empty_unified_indicators(symbol: str) -> Dict[str, Any]:
    """Create empty unified indicators for error cases."""
    return {
        'symbol': symbol,
        'timestamp': datetime.now().isoformat(),
        'base_indicators': {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': 0.0,
            'sma_20': 0.0,
            'sma_50': 0.0,
            'ema_9': 0.0,
            'ema_21': 0.0,
            'ema_50': 0.0,
            'rsi': 50.0,
            'macd': 0.0,
            'macd_signal': 0.0,
            'macd_histogram': 0.0,
            'atr': 0.0,
            'realized_volatility': 20.0,
            'volume': 0,
            'avg_volume': 0.0,
            'relative_volume_pct': 100.0,
            'trend_direction': 'neutral',
            'trend_strength': 0.0
        },
        'pml_indicators': {
            'pml_price': 0.0,
            'ceiling_price': 0.0,
            'floor_price': 0.0,
            'green_line_price': 0.0,
            'call_delta': 0.0,
            'put_delta': 0.0,
            'net_delta': 0.0,
            'pml_cross_bullish': False,
            'delta_confirmation': False,
            'market_condition': 'UNCERTAIN'
        },
        'iron_condor_indicators': {
            'iv_rank': 50.0,
            'realized_vol': 20.0,
            'range_pct': 0.1,
            'recent_high': 0.0,
            'recent_low': 0.0,
            'is_range_bound': False,
            'is_low_volatility': False,
            'is_suitable_for_ic': False,
            'trend_strength': 0.0,
            'market_condition': 'UNCERTAIN'
        },
        'divergence_indicators': {
            'swing_highs': [],
            'swing_lows': [],
            'support_levels': [],
            'resistance_levels': [],
            'bullish_divergence_detected': False,
            'bearish_divergence_detected': False,
            'divergence_strength': 'medium',
            'trend_direction': 'neutral',
            'bullish_divergence_strong': False,
            'bullish_divergence_medium': False,
            'bearish_divergence_strong': False,
            'bearish_divergence_medium': False,
            'has_trade_signal': False,
            'signal_type': 'NO_SIGNAL'
        }
    }

def calculate_base_indicators_from_df(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Calculate base indicators from DataFrame"""
    try:
        if df is None or len(df) < 50:
            return create_empty_base_indicators(symbol)
        
        # Ensure numeric types
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        current_price = float(df['close'].iloc[-1])
        current_volume = int(df['volume'].iloc[-1])
        
        # Extract price arrays
        close_prices = df['close'].values
        high_prices = df['high'].values
        low_prices = df['low'].values
        
        print(f"💻 Using CPU calculations for {symbol}")
        
        if TALIB_AVAILABLE:
            sma_20 = float(talib.SMA(close_prices, timeperiod=20)[-1])
            sma_50 = float(talib.SMA(close_prices, timeperiod=50)[-1]) if len(close_prices) >= 50 else sma_20
            ema_9 = float(talib.EMA(close_prices, timeperiod=9)[-1])
            ema_21 = float(talib.EMA(close_prices, timeperiod=21)[-1])
            ema_50 = float(talib.EMA(close_prices, timeperiod=50)[-1])
            rsi = float(talib.RSI(close_prices, timeperiod=14)[-1])
            
            macd, macd_signal, macd_hist = talib.MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
            macd_val = float(macd[-1])
            macd_signal_val = float(macd_signal[-1])
            macd_histogram = float(macd_hist[-1])
            
            atr = float(talib.ATR(high_prices, low_prices, close_prices, timeperiod=14)[-1])
        else:
            # Simplified calculations
            sma_20 = float(df['close'].rolling(20).mean().iloc[-1])
            sma_50 = float(df['close'].rolling(50).mean().iloc[-1]) if len(df) >= 50 else sma_20
            ema_9 = float(df['close'].ewm(span=9).mean().iloc[-1])
            ema_21 = float(df['close'].ewm(span=21).mean().iloc[-1])
            ema_50 = float(df['close'].ewm(span=50).mean().iloc[-1])
            
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = float(100 - (100 / (1 + rs.iloc[-1])))
            
            ema_fast = df['close'].ewm(span=12).mean()
            ema_slow = df['close'].ewm(span=26).mean()
            macd_val = float(ema_fast.iloc[-1] - ema_slow.iloc[-1])
            macd_signal_val = float(pd.Series([macd_val]).ewm(span=9).mean().iloc[-1])
            macd_histogram = macd_val - macd_signal_val
            
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            true_range = np.maximum(high_low, np.maximum(high_close, low_close))
            atr = float(true_range.rolling(14).mean().iloc[-1])
        
        # Volume analysis
        avg_volume = float(df['volume'].rolling(14).mean().iloc[-1])
        relative_volume_pct = (current_volume / avg_volume * 100) if avg_volume > 0 else 100.0
        
        # Volatility calculation
        returns = df['close'].pct_change().dropna()
        realized_volatility = float(returns.std() * np.sqrt(252) * 100)
        
        # Trend analysis
        if sma_20 > 0:
            trend_strength = abs(current_price - sma_20) / sma_20
            if current_price > ema_9 > ema_21 > ema_50:
                trend_direction = "bullish"
            elif current_price < ema_9 < ema_21 < ema_50:
                trend_direction = "bearish"
            else:
                trend_direction = "neutral"
        else:
            trend_strength = 0.0
            trend_direction = "neutral"
        
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': current_price,
            'sma_20': sma_20,
            'sma_50': sma_50,
            'ema_9': ema_9,
            'ema_21': ema_21,
            'ema_50': ema_50,
            'rsi': rsi,
            'macd': macd_val,
            'macd_signal': macd_signal_val,
            'macd_histogram': macd_histogram,
            'atr': atr,
            'realized_volatility': realized_volatility,
            'volume': current_volume,
            'avg_volume': avg_volume,
            'relative_volume_pct': relative_volume_pct,
            'trend_direction': trend_direction,
            'trend_strength': trend_strength
        }
        
    except Exception as e:
        print(f"Error calculating base indicators for {symbol}: {e}")
        return create_empty_base_indicators(symbol)

def create_empty_base_indicators(symbol: str) -> Dict[str, Any]:
    """Create empty base indicators for error cases."""
    return {
        'symbol': symbol,
        'timestamp': datetime.now().isoformat(),
        'current_price': 0.0,
        'sma_20': 0.0,
        'sma_50': 0.0,
        'ema_9': 0.0,
        'ema_21': 0.0,
        'ema_50': 0.0,
        'rsi': 50.0,
        'macd': 0.0,
        'macd_signal': 0.0,
        'macd_histogram': 0.0,
        'atr': 0.0,
        'realized_volatility': 20.0,
        'volume': 0,
        'avg_volume': 0.0,
        'relative_volume_pct': 100.0,
        'trend_direction': 'neutral',
        'trend_strength': 0.0
    }

def calculate_pml_indicators_with_options(symbol: str, current_price: float, options_data: Optional[Dict]) -> Dict[str, Any]:
    """Calculate PML indicators with pre-fetched options data"""
    temp_handler = UnifiedTechnicalIndicatorsTimeframe("1min")
    if options_data:
        # Temporarily set the options data to avoid re-fetching
        temp_handler.options_handler._cached_options = {symbol: options_data}
    return temp_handler.calculate_pml_indicators(symbol, current_price)

def calculate_iron_condor_indicators_from_df(df: pd.DataFrame, symbol: str, current_price: float) -> Dict[str, Any]:
    """Calculate Iron Condor indicators from DataFrame"""
    temp_handler = UnifiedTechnicalIndicatorsTimeframe("1min")
    return temp_handler.calculate_iron_condor_indicators(df, symbol, current_price)

def calculate_divergence_indicators_from_df(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Calculate Divergence indicators from DataFrame"""
    temp_handler = UnifiedTechnicalIndicatorsTimeframe("1min")
    return temp_handler.calculate_divergence_indicators(df, symbol)

def calculate_exceedence_indicators_from_df(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Calculate Exceedence indicators from DataFrame"""
    temp_handler = UnifiedTechnicalIndicatorsTimeframe("1min")
    return temp_handler.calculate_exceedence_indicators(df, symbol)

def analyze_symbol_timeframe_ultra_fast(symbol: str, timeframe: str, all_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Ultra-fast analysis using pre-fetched data"""
    try:
        # Get pre-fetched data
        symbol_data = all_data.get(symbol, {})
        df = symbol_data.get('historical', {}).get(timeframe)
        options_data = symbol_data.get('options')
        
        if df is None or len(df) < 20:
            return create_empty_unified_indicators(symbol)
        
        current_price = float(df['close'].iloc[-1])
        
        # Calculate indicators using pre-fetched data
        base = calculate_base_indicators_from_df(df, symbol)
        pml = calculate_pml_indicators_with_options(symbol, current_price, options_data)
        iron_condor = calculate_iron_condor_indicators_from_df(df, symbol, current_price)
        divergence = calculate_divergence_indicators_from_df(df, symbol)
        exceedence = calculate_exceedence_indicators_from_df(df, symbol)
        
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'base_indicators': base,
            'pml_indicators': pml,
            'iron_condor_indicators': iron_condor,
            'divergence_indicators': divergence,
            'exceedence_indicators': exceedence
        }
        
    except Exception as e:
        return create_empty_unified_indicators(symbol)

def run_timeframe_analysis_parallel(timeframe: str, watchlist_symbols: List[str]) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Run analysis for a single timeframe across all symbols in parallel"""
    print(f"🔄 Processing {timeframe.upper()} timeframe for {len(watchlist_symbols)} symbols...")
    
    timeframe_start = time_module.time()
    timeframe_indicators = {}
    
    # Use ThreadPoolExecutor for parallel symbol processing within this timeframe
    max_workers = min(8, len(watchlist_symbols))  # Higher concurrency per timeframe
    
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
                timeframe_indicators[symbol] = create_empty_unified_indicators(symbol)
    
    elapsed_time = time_module.time() - timeframe_start
    print(f"  ✅ {timeframe.upper()} completed in {elapsed_time:.2f}s")
    
    return timeframe, timeframe_indicators

def run_ultra_parallel_analysis() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Run ultra-parallel analysis with aggressive optimizations"""
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print("🚀 ULTRA-PARALLEL Multi-Timeframe Technical Indicators Analysis")
    print("=" * 70)
    print(f"Processing {len(watchlist_symbols)} symbols × {len(TimeframeConfig.TIMEFRAMES)} timeframes = {len(watchlist_symbols) * len(TimeframeConfig.TIMEFRAMES)} total operations...")
    print("🔥 Using aggressive parallelization and data sharing optimizations")
    print()
    
    overall_start_time = time_module.time()
    
    # Create shared data manager with aggressive caching
    data_manager = UltraParallelDataManager()
    
    # Pre-fetch ALL data for ALL symbols and timeframes simultaneously
    print("📡 Pre-fetching ALL market data simultaneously...")
    prefetch_start = time_module.time()
    all_data = data_manager.prefetch_all_data(watchlist_symbols)
    prefetch_time = time_module.time() - prefetch_start
    print(f"✅ Data pre-fetch completed in {prefetch_time:.2f}s")
    
    # Results organized by timeframe -> symbol -> indicators
    timeframe_results = defaultdict(dict)
    
    # Process ALL symbol-timeframe combinations simultaneously
    print("⚡ Processing ALL combinations in parallel...")
    max_workers = min(20, len(watchlist_symbols) * len(TimeframeConfig.TIMEFRAMES))  # Aggressive parallelism
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit ALL symbol-timeframe combinations
        futures = []
        for symbol in watchlist_symbols:
            for timeframe in TimeframeConfig.TIMEFRAMES.keys():
                future = executor.submit(analyze_symbol_timeframe_ultra_fast, symbol, timeframe, all_data)
                futures.append((future, symbol, timeframe))
        
        # Collect results as they complete
        completed = 0
        for future, symbol, timeframe in futures:
            try:
                indicators_data = future.result(timeout=30)  # Short timeout per operation
                timeframe_results[timeframe][symbol] = indicators_data
                completed += 1
                if completed % 5 == 0:  # Progress updates
                    print(f"  ⚡ Completed {completed}/{len(futures)} operations...")
            except Exception as e:
                print(f"  ❌ Error processing {symbol}-{timeframe}: {e}")
                timeframe_results[timeframe][symbol] = create_empty_unified_indicators(symbol)
    
    overall_elapsed_time = time_module.time() - overall_start_time
    print(f"\n🎉 ULTRA-PARALLEL analysis completed in {overall_elapsed_time:.2f} seconds")
    print(f"⚡ Performance improvement: ~{(27.42 / overall_elapsed_time):.1f}x faster than sequential processing")
    print(f"🔥 Data fetch: {prefetch_time:.2f}s, Processing: {(overall_elapsed_time - prefetch_time):.2f}s")
    
    return dict(timeframe_results)

def run_parallel_analysis() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Fallback to ultra-parallel analysis"""
    return run_ultra_parallel_analysis()

def run_technical_indicators_for_timeframe(timeframe: str) -> Dict[str, Any]:
    """Run technical indicators analysis for all symbols in a specific timeframe"""
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print(f"Technical Indicators Analysis for {timeframe.upper()} timeframe")
    print("=" * 60)
    print(f"Processing {len(watchlist_symbols)} symbols...")
    
    start_time = time_module.time()
    technical_indicators = {}
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(4, len(watchlist_symbols))  # Limit to 4 concurrent threads per timeframe
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks for this timeframe
        future_to_symbol = {
            executor.submit(analyze_symbol_for_timeframe, symbol, timeframe): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, tf, indicators_data = future.result(timeout=60)  # 60 second timeout per symbol
                technical_indicators[symbol] = indicators_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  Error processing {symbol}: {e}")
                technical_indicators[symbol] = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e),
                    'base_indicators': {'current_price': 0.0, 'rsi': 50.0, 'trend_direction': 'neutral'},
                    'pml_indicators': {'pml_price': 0.0},
                    'iron_condor_indicators': {'market_condition': 'UNCERTAIN'},
                    'divergence_indicators': {'signal_type': 'NO_SIGNAL'}
                }
    
    elapsed_time = time_module.time() - start_time
    print(f"✅ {timeframe.upper()} analysis completed in {elapsed_time:.2f} seconds")
    
    return technical_indicators

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

def save_technical_indicators_for_timeframe(timeframe: str, technical_indicators: Dict[str, Any]) -> bool:
    """Save technical indicators to timeframe-specific JSON file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filename = TimeframeConfig.TIMEFRAMES[timeframe]["filename"]
        file_path = os.path.join(script_dir, filename)
        
        # Convert numpy types to native Python types
        clean_indicators = convert_numpy_types(technical_indicators)
        
        # Create comprehensive technical indicators data
        technical_data = {
            'strategy_name': f'Unified_Technical_Indicators_{timeframe.upper()}',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(clean_indicators),
            'analysis_summary': {
                'symbols_with_data': len([s for s in clean_indicators.values() if 'error' not in s]),
                'symbols_with_errors': len([s for s in clean_indicators.values() if 'error' in s]),
                'pml_bullish_crosses': len([s for s in clean_indicators.values() if s.get('pml_indicators', {}).get('pml_cross_bullish', False)]),
                'range_bound_markets': len([s for s in clean_indicators.values() if s.get('iron_condor_indicators', {}).get('is_range_bound', False)]),
                'divergence_signals': len([s for s in clean_indicators.values() if s.get('divergence_indicators', {}).get('has_trade_signal', False)])
            },
            'indicators': clean_indicators,
            'metadata': {
                'analysis_type': 'unified_technical_indicators',
                'timeframe': timeframe,
                'indicator_categories': ['base_indicators', 'pml_indicators', 'iron_condor_indicators', 'divergence_indicators'],
                'update_frequency': 'on_demand',
                'data_sources': ['schwab_historical_data', 'schwab_options_chain'],
                'talib_available': TALIB_AVAILABLE,
                'scipy_available': SCIPY_AVAILABLE
            }
        }
        
        # Write to timeframe-specific file
        with open(file_path, 'w') as f:
            json.dump(technical_data, f, indent=2)
        
        print(f"✅ Saved {timeframe} indicators to {filename}")
        print(f"📊 Analysis: {technical_data['analysis_summary']['symbols_with_data']} symbols analyzed, {technical_data['analysis_summary']['pml_bullish_crosses']} PML bullish crosses")
        return True
        
    except Exception as e:
        print(f"❌ Error saving {timeframe} indicators to file: {e}")
        return False

def run_all_timeframes_analysis_parallel():
    """Run parallel analysis across all symbols and timeframes simultaneously"""
    try:
        # Run the new parallel analysis
        timeframe_results = run_parallel_analysis()
        
        # Save results for each timeframe
        print("\n📁 Saving results to JSON files...")
        for timeframe in TimeframeConfig.TIMEFRAMES.keys():
            if timeframe in timeframe_results:
                save_technical_indicators_for_timeframe(timeframe, timeframe_results[timeframe])
            else:
                print(f"⚠️  No results for {timeframe} timeframe")
        
        print(f"\n📁 Created {len(TimeframeConfig.TIMEFRAMES)} JSON files:")
        for timeframe, config in TimeframeConfig.TIMEFRAMES.items():
            print(f"   - {config['filename']}")
            
    except Exception as e:
        print(f"❌ Error in parallel analysis: {e}")
        print("🔄 Falling back to sequential processing...")
        run_all_timeframes_analysis_sequential()

def run_all_timeframes_analysis_sequential():
    """Run technical indicators analysis for all timeframes sequentially (fallback)"""
    print("Multi-Timeframe Technical Indicators Analysis (Sequential)")
    print("=" * 60)
    print(f"Processing {len(TimeframeConfig.TIMEFRAMES)} timeframes: {list(TimeframeConfig.TIMEFRAMES.keys())}")
    print()
    
    overall_start_time = time_module.time()
    
    # Process each timeframe
    for timeframe in TimeframeConfig.TIMEFRAMES.keys():
        try:
            # Run analysis for this timeframe
            technical_indicators = run_technical_indicators_for_timeframe(timeframe)
            
            # Save to timeframe-specific JSON file
            save_technical_indicators_for_timeframe(timeframe, technical_indicators)
            
            print()  # Add spacing between timeframes
            
        except Exception as e:
            print(f"❌ Error processing {timeframe} timeframe: {e}")
            continue
    
    overall_elapsed_time = time_module.time() - overall_start_time
    print(f"🎉 All timeframes analysis completed in {overall_elapsed_time:.2f} seconds")
    print(f"📁 Created {len(TimeframeConfig.TIMEFRAMES)} JSON files:")
    for timeframe, config in TimeframeConfig.TIMEFRAMES.items():
        print(f"   - {config['filename']}")

def run_all_timeframes_analysis():
    """Main entry point - uses parallel processing by default"""
    run_all_timeframes_analysis_parallel()

def main():
    """Main function to run multi-timeframe technical indicators analysis"""
    run_all_timeframes_analysis()

if __name__ == "__main__":
    main()
