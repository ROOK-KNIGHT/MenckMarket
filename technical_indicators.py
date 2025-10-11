#!/usr/bin/env python3
"""
Unified Technical Indicators Handler

This handler provides comprehensive technical analysis indicators for all trading strategies:
1. PML Strategy Indicators: Options flow analysis, gamma/delta exposure, PML levels
2. Iron Condor Strategy Indicators: Volatility analysis, range detection, IV rank
3. Divergence Strategy Indicators: RSI divergences, swing points, multi-timeframe analysis
4. Base Indicators: ATR, Volume, Moving averages, Support/Resistance
5. JSON Output: Creates technical_indicators.json with all analysis

All indicators are configurable and can be used independently or together.
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

class UnifiedTechnicalIndicators:
    """
    Unified Technical Indicators Handler combining all strategy indicators
    """
    
    def __init__(self):
        """Initialize the unified technical indicators handler."""
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        
        self.logger.info("UnifiedTechnicalIndicators initialized")
        self.logger.info(f"  TA-Lib available: {TALIB_AVAILABLE}")
        self.logger.info(f"  SciPy available: {SCIPY_AVAILABLE}")

    def get_historical_data_for_timeframe(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Get historical data for specified timeframe."""
        try:
            timeframe_map = {
                "1min": {"period_type": "day", "period": 10, "frequency_type": "minute", "frequency": 1},
                "5min": {"period_type": "day", "period": 10, "frequency_type": "minute", "frequency": 5},
                "15min": {"period_type": "day", "period": 10, "frequency_type": "minute", "frequency": 15},
                "1hour": {"period_type": "day", "period": 20, "frequency_type": "minute", "frequency": 60},
                "daily": {"period_type": "month", "period": 6, "frequency_type": "daily", "frequency": 1}
            }
            
            if timeframe not in timeframe_map:
                return None
            
            params = timeframe_map[timeframe]
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
            self.logger.error(f"Error getting historical data for {symbol} {timeframe}: {e}")
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
                    'trend_strength': 0.0
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
                'trend_strength': 0.0
            }

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
        """Calculate all unified technical indicators for a symbol (NO TRADING SIGNALS)."""
        try:
            # Get daily data for base analysis
            df_daily = self.get_historical_data_for_timeframe(symbol, "daily")
            if df_daily is None:
                return self._create_empty_unified_indicators(symbol)
            
            current_price = float(df_daily['close'].iloc[-1])
            
            # Calculate all indicator types (PURE INDICATORS ONLY)
            base = self.calculate_base_indicators(df_daily, symbol)
            pml = self.calculate_pml_indicators(symbol, current_price)
            iron_condor = self.calculate_iron_condor_indicators(df_daily, symbol, current_price)
            divergence = self.calculate_divergence_indicators(df_daily, symbol)
            
            # Return ONLY technical indicators - NO trading signals or decisions
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'base_indicators': base,
                'pml_indicators': pml,
                'iron_condor_indicators': iron_condor,
                'divergence_indicators': divergence
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
        """Detect price swing highs and lows."""
        try:
            df_swings = df.copy()
            
            if SCIPY_AVAILABLE:
                # Find local maxima and minima
                high_indices = argrelextrema(df_swings['close'].values, np.greater, order=5)[0]
                low_indices = argrelextrema(df_swings['close'].values, np.less, order=5)[0]
            else:
                # Simplified swing detection
                high_indices = []
                low_indices = []
                for i in range(5, len(df_swings) - 5):
                    if df_swings['close'].iloc[i] == df_swings['close'].iloc[i-5:i+6].max():
                        high_indices.append(i)
                    if df_swings['close'].iloc[i] == df_swings['close'].iloc[i-5:i+6].min():
                        low_indices.append(i)
            
            # Initialize swing columns
            df_swings['swing_high'] = np.nan
            df_swings['swing_low'] = np.nan
            
            # Mark swing points
            if len(high_indices) > 0:
                df_swings.loc[df_swings.index[high_indices], 'swing_high'] = df_swings.iloc[high_indices]['close'].values
            
            if len(low_indices) > 0:
                df_swings.loc[df_swings.index[low_indices], 'swing_low'] = df_swings.iloc[low_indices]['close'].values
            
            return df_swings
            
        except Exception as e:
            self.logger.error(f"Error detecting swing points: {e}")
            return df

    def _detect_rsi_divergences(self, df: pd.DataFrame) -> Dict[str, Dict[str, bool]]:
        """Detect RSI divergences."""
        try:
            divergences = {
                "bullish": {"strong": False, "medium": False, "weak": False, "hidden": False},
                "bearish": {"strong": False, "medium": False, "weak": False, "hidden": False}
            }
            
            # Get swing points
            swing_highs = df['swing_high'].dropna()
            swing_lows = df['swing_low'].dropna()
            
            if len(swing_highs) < 2 and len(swing_lows) < 2:
                return divergences
            
            # Check bullish divergences (swing lows)
            if len(swing_lows) >= 2:
                lows_indices = swing_lows.index.tolist()[-2:]
                if len(lows_indices) >= 2:
                    idx1, idx2 = lows_indices[0], lows_indices[1]
                    price1, price2 = float(df.loc[idx1, 'close']), float(df.loc[idx2, 'close'])
                    rsi1, rsi2 = float(df.loc[idx1, 'rsi']), float(df.loc[idx2, 'rsi'])
                    
                    if price2 < price1 and rsi2 > rsi1:  # Bullish divergence
                        divergences["bullish"]["strong"] = True
            
            # Check bearish divergences (swing highs)
            if len(swing_highs) >= 2:
                highs_indices = swing_highs.index.tolist()[-2:]
                if len(highs_indices) >= 2:
                    idx1, idx2 = highs_indices[0], highs_indices[1]
                    price1, price2 = float(df.loc[idx1, 'close']), float(df.loc[idx2, 'close'])
                    rsi1, rsi2 = float(df.loc[idx1, 'rsi']), float(df.loc[idx2, 'rsi'])
                    
                    if price2 > price1 and rsi2 < rsi1:  # Bearish divergence
                        divergences["bearish"]["strong"] = True
            
            return divergences
            
        except Exception as e:
            self.logger.error(f"Error detecting RSI divergences: {e}")
            return {
                "bullish": {"strong": False, "medium": False, "weak": False, "hidden": False},
                "bearish": {"strong": False, "medium": False, "weak": False, "hidden": False}
            }

    # Removed trading signal methods - this is now a pure indicators engine
    # Strategies will read from technical_indicators.json to make trading decisions

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
                'trend_strength': 0.0
            },
            'divergence_indicators': {
                'swing_highs': [],
                'swing_lows': [],
                'support_levels': [],
                'resistance_levels': [],
                'bullish_divergence_strong': False,
                'bullish_divergence_medium': False,
                'bearish_divergence_strong': False,
                'bearish_divergence_medium': False,
                'has_trade_signal': False,
                'signal_type': 'NO_SIGNAL'
            },
            'overall_signal': 'NO_SIGNAL',
            'overall_confidence': 0.0,
            'market_condition': 'UNCERTAIN',
            'volatility_environment': 'MEDIUM_VOLATILITY',
            'position_size_recommendation': 1.0,
            'stop_loss_level': 0.0,
            'profit_target_level': 0.0
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


def analyze_symbol_technical_indicators(symbol: str, indicators_handler: UnifiedTechnicalIndicators) -> Tuple[str, Dict[str, Any]]:
    """Analyze a single symbol for all technical indicators (for parallel processing)"""
    try:
        print(f"Analyzing technical indicators for {symbol}...")
        
        # Calculate unified indicators
        unified_indicators = indicators_handler.calculate_unified_indicators(symbol)
        
        # Print summary
        base = unified_indicators.get('base_indicators', {})
        pml = unified_indicators.get('pml_indicators', {})
        print(f"  {symbol}: ${base.get('current_price', 0):.2f} | RSI: {base.get('rsi', 50):.1f} | Trend: {base.get('trend_direction', 'neutral')} | PML: ${pml.get('pml_price', 0):.2f}")
        
        return symbol, unified_indicators
        
    except Exception as e:
        print(f"  Error analyzing {symbol}: {e}")
        # Return error indicators
        error_indicators = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'overall_signal': 'NO_SIGNAL',
            'overall_confidence': 0.0
        }
        return symbol, error_indicators


def run_technical_indicators_analysis_for_watchlist() -> Dict[str, Any]:
    """Run technical indicators analysis for all symbols in the watchlist using parallel processing"""
    indicators_handler = UnifiedTechnicalIndicators()
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print("Unified Technical Indicators Analysis (Parallel Processing)")
    print("=" * 60)
    print(f"Processing {len(watchlist_symbols)} symbols concurrently...")
    
    start_time = time_module.time()
    technical_indicators = {}
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(6, len(watchlist_symbols))  # Limit to 6 concurrent threads
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks
        future_to_symbol = {
            executor.submit(analyze_symbol_technical_indicators, symbol, indicators_handler): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, indicators_data = future.result(timeout=60)  # 60 second timeout per symbol
                technical_indicators[symbol] = indicators_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  Error processing {symbol}: {e}")
                technical_indicators[symbol] = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e),
                    'overall_signal': 'NO_SIGNAL',
                    'overall_confidence': 0.0
                }
    
    elapsed_time = time_module.time() - start_time
    print(f"\n✅ Technical Indicators Analysis completed in {elapsed_time:.2f} seconds")
    print(f"📊 Processed {len(technical_indicators)} symbols with {max_workers} concurrent threads")
    
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

def insert_technical_indicators_to_db(technical_indicators: Dict[str, Any]) -> bool:
    """Insert technical indicators into PostgreSQL database with truncation"""
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
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
        print("🗑️  Truncating technical_indicators table...")
        # Use DELETE instead of TRUNCATE to avoid relation locks in parallel execution
        cur.execute("DELETE FROM technical_indicators;")
        
        # Insert new data
        insert_count = 0
        for symbol, indicator_data in technical_indicators.items():
            try:
                # Skip symbols with errors
                if 'error' in indicator_data:
                    continue
                
                # Extract indicator categories
                base_indicators = indicator_data.get('base_indicators', {})
                pml_indicators = indicator_data.get('pml_indicators', {})
                iron_condor_indicators = indicator_data.get('iron_condor_indicators', {})
                divergence_indicators = indicator_data.get('divergence_indicators', {})
                
                # Prepare insert statement
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
                
                # Prepare values with proper type conversion
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
                print(f"❌ Error inserting {symbol}: {e}")
                continue
        
        # Commit the transaction
        conn.commit()
        
        # Close connections
        cur.close()
        conn.close()
        
        print(f"✅ Successfully inserted {insert_count} technical indicators into database")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting technical indicators to database: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def save_technical_indicators_to_file(technical_indicators: Dict[str, Any]) -> bool:
    """Save technical indicators to dedicated technical_indicators.json file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        technical_indicators_path = os.path.join(script_dir, 'technical_indicators.json')
        
        # Convert numpy types to native Python types
        clean_indicators = convert_numpy_types(technical_indicators)
        
        # Create comprehensive technical indicators data
        technical_data = {
            'strategy_name': 'Unified_Technical_Indicators',
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
                'indicator_categories': ['base_indicators', 'pml_indicators', 'iron_condor_indicators', 'divergence_indicators'],
                'update_frequency': 'on_demand',
                'data_sources': ['schwab_historical_data', 'schwab_options_chain'],
                'talib_available': TALIB_AVAILABLE,
                'scipy_available': SCIPY_AVAILABLE
            }
        }
        
        # Write to dedicated technical indicators file
        with open(technical_indicators_path, 'w') as f:
            json.dump(technical_data, f, indent=2)
        
        print(f"✅ Saved technical indicators to {technical_indicators_path}")
        print(f"📊 Analysis: {technical_data['analysis_summary']['symbols_with_data']} symbols analyzed, {technical_data['analysis_summary']['pml_bullish_crosses']} PML bullish crosses")
        return True
        
    except Exception as e:
        print(f"❌ Error saving technical indicators to file: {e}")
        return False


# Global throttling variables for database insertion
_last_db_insertion = 0
_db_insertion_interval = 30  # seconds

def main():
    """Main function to run unified technical indicators analysis"""
    # Run analysis for all watchlist symbols
    technical_indicators = run_technical_indicators_analysis_for_watchlist()
    
    # Save indicators to dedicated file
    save_technical_indicators_to_file(technical_indicators)


if __name__ == "__main__":
    main()
