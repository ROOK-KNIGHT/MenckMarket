#!/usr/bin/env python3
"""
PML (Peak Money Line) Options Strategy Handler

This handler provides PML options strategy analysis and trading signals:
1. PML price calculation from options flow data
2. Market maker exposure analysis
3. Delta cross signal generation
4. Entry and exit signal generation
5. Risk management calculations
6. Position sizing recommendations

PML Strategy Overview:
- Monitor spot price vs PML (Peak Money Line) crossover
- Analyze call/put delta flows for confirmation
- Enter ITM call positions when conditions align
- Target ceiling price levels for profit taking
- Best in trending markets with positive delta flow
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Import our existing handlers
from historical_data_handler import HistoricalDataHandler
from options_data_handler import OptionsDataHandler

class SignalType(Enum):
    """Trading signal types"""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"
    NO_SIGNAL = "NO_SIGNAL"

class MarketCondition(Enum):
    """Market condition types"""
    PML_CROSS_BULLISH = "PML_CROSS_BULLISH"
    PML_CROSS_BEARISH = "PML_CROSS_BEARISH"
    DELTA_POSITIVE = "DELTA_POSITIVE"
    DELTA_NEGATIVE = "DELTA_NEGATIVE"
    RANGE_BOUND = "RANGE_BOUND"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    UNCERTAIN = "UNCERTAIN"

@dataclass
class PMLConfig:
    """Configuration for PML strategy"""
    # Time to expiration preferences
    min_dte: int = 7   # Minimum days to expiration
    max_dte: int = 45  # Maximum days to expiration
    optimal_dte: int = 21  # Optimal days to expiration
    
    # PML calculation parameters
    gamma_weight: float = 1.0  # Weight for gamma exposure
    delta_weight: float = 0.8  # Weight for delta exposure
    volume_weight: float = 0.6  # Weight for volume
    oi_weight: float = 0.4     # Weight for open interest
    
    # Entry criteria
    min_pml_cross_pct: float = 0.0001  # Min % above PML for signal (0.01% - more sensitive)
    min_delta_threshold: float = 100   # Min absolute delta for confirmation (reduced)
    min_volume_ratio: float = 0.5      # Min volume vs avg volume (reduced)
    
    # Delta flow criteria
    delta_confirmation_period: int = 3  # Periods to confirm delta flow
    min_call_delta_positive: float = 50    # Min positive call delta (reduced)
    min_put_delta_positive: float = 50     # Min positive put delta (reduced)
    
    # Risk management
    stop_loss_pct: float = 0.10        # Stop loss percentage
    take_profit_pct: float = 0.25      # Take profit percentage
    max_loss_pct: float = 0.02         # Max loss as % of account
    
    # Position management
    max_positions_per_symbol: int = 1
    position_size_pct: float = 0.05    # Position size as % of account
    
    # Market condition filters
    min_liquidity_volume: int = 1000   # Min daily volume
    max_spread_pct: float = 0.05       # Max bid-ask spread %

@dataclass
class PMLSetup:
    """PML trading setup details"""
    symbol: str
    expiration_date: datetime
    dte: int
    
    # Strike and pricing
    strike: float
    option_type: str  # 'CALL' or 'PUT'
    current_price: float
    bid: float
    ask: float
    mid_price: float
    
    # PML analysis
    pml_price: float
    ceiling_price: float
    floor_price: float
    green_line_price: float
    
    # Delta analysis
    call_delta: float
    put_delta: float
    net_delta: float
    
    # Greeks and metrics
    delta: float
    gamma: float
    theta: float
    vega: float
    implied_vol: float
    
    # Risk metrics
    intrinsic_value: float
    time_value: float
    potential_profit: float
    max_loss: float
    
    # Market context
    spot_price: float
    volume: int
    open_interest: int
    market_condition: MarketCondition

@dataclass
class TradingSignal:
    """Trading signal with details"""
    signal_type: SignalType
    symbol: str
    timestamp: datetime
    confidence: float  # 0-1 confidence score
    
    # Signal details
    setup: Optional[PMLSetup] = None
    entry_reason: str = ""
    exit_reason: str = ""
    
    # Risk management
    position_size: float = 0.0
    stop_loss: float = 0.0
    profit_target: float = 0.0
    
    # Market context
    market_condition: MarketCondition = MarketCondition.UNCERTAIN
    volatility_environment: str = ""

class PMLStrategy:
    """
    PML (Peak Money Line) Strategy Handler for options trading signals
    """
    
    def __init__(self, config: PMLConfig = None):
        """
        Initialize the PML strategy handler.
        
        Args:
            config: Configuration for strategy parameters
        """
        self.config = config or PMLConfig()
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        
        self.logger.info("PMLStrategy initialized with configuration:")
        self.logger.info(f"  Target DTE: {self.config.min_dte}-{self.config.max_dte} days")
        self.logger.info(f"  Min PML cross: {self.config.min_pml_cross_pct*100:.2f}%")
        self.logger.info(f"  Delta threshold: {self.config.min_delta_threshold}")
        self.logger.info(f"  Stop loss: {self.config.stop_loss_pct*100:.1f}%")

    def calculate_pml_levels(self, symbol: str) -> Dict[str, Any]:
        """
        Calculate PML (Peak Money Line) and related levels from options data.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary with PML analysis
        """
        try:
            # Get options chain data
            options_chain = self.options_handler.get_options_chain(symbol)
            
            if not options_chain:
                self.logger.warning(f"No options chain data for {symbol}")
                return self._create_empty_pml_analysis(symbol)
            
            # Get current stock price
            quote = self.options_handler.get_quote(symbol)
            current_price = 0
            if quote:
                # Try different price fields in order of preference
                if 'quote' in quote and 'lastPrice' in quote['quote']:
                    current_price = quote['quote']['lastPrice']
                elif 'regular' in quote and 'regularMarketLastPrice' in quote['regular']:
                    current_price = quote['regular']['regularMarketLastPrice']
                elif 'lastPrice' in quote:
                    current_price = quote['lastPrice']
                else:
                    print(f"Warning: Could not find price data for {symbol} in quote response")
            
            if not current_price:
                return self._create_empty_pml_analysis(symbol)
            
            # Calculate exposure levels from options data
            exposure_data = self._calculate_exposure_levels(options_chain, current_price)
            
            # Calculate PML, ceiling, floor, and green line
            pml_price = self._calculate_pml_price(exposure_data, current_price)
            ceiling_price = self._calculate_ceiling_price(pml_price, current_price, exposure_data)
            floor_price = self._calculate_floor_price(pml_price, current_price, exposure_data)
            green_line_price = self._calculate_green_line_price(pml_price, current_price)
            
            # Calculate delta flows
            delta_analysis = self._analyze_delta_flows(exposure_data, green_line_price)
            
            return {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'spot_price': current_price,
                'pml_price': pml_price,
                'ceiling_price': ceiling_price,
                'floor_price': floor_price,
                'green_line_price': green_line_price,
                'call_delta': delta_analysis['call_delta'],
                'put_delta': delta_analysis['put_delta'],
                'net_delta': delta_analysis['net_delta'],
                'calls_above_green': delta_analysis['calls_above_green'],
                'puts_above_green': delta_analysis['puts_above_green'],
                'calls_below_green': delta_analysis['calls_below_green'],
                'puts_below_green': delta_analysis['puts_below_green'],
                'pml_cross_bullish': current_price > pml_price,
                'delta_confirmation': self._check_delta_confirmation(delta_analysis),
                'market_condition': self._determine_pml_market_condition(
                    current_price, pml_price, delta_analysis
                )
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating PML levels for {symbol}: {e}")
            return self._create_empty_pml_analysis(symbol)

    def _calculate_exposure_levels(self, options_chain: Dict, current_price: float) -> Dict[str, Any]:
        """Calculate market maker exposure levels from options chain."""
        exposure_data = {
            'strikes': [],
            'call_gamma': [],
            'put_gamma': [],
            'call_delta': [],
            'put_delta': [],
            'call_volume': [],
            'put_volume': [],
            'call_oi': [],
            'put_oi': []
        }
        
        try:
            call_exp_map = options_chain.get("callExpDateMap", {})
            put_exp_map = options_chain.get("putExpDateMap", {})
            
            # Process all strikes across all expirations
            all_strikes = set()
            
            # Collect all strikes
            for exp_data in call_exp_map.values():
                all_strikes.update(float(strike) for strike in exp_data.keys())
            for exp_data in put_exp_map.values():
                all_strikes.update(float(strike) for strike in exp_data.keys())
            
            # Sort strikes
            sorted_strikes = sorted(all_strikes)
            
            # Aggregate data for each strike
            for strike in sorted_strikes:
                strike_str = str(strike)
                
                # Initialize accumulators
                call_gamma_total = 0
                put_gamma_total = 0
                call_delta_total = 0
                put_delta_total = 0
                call_volume_total = 0
                put_volume_total = 0
                call_oi_total = 0
                put_oi_total = 0
                
                # Sum across all expirations for this strike
                for exp_data in call_exp_map.values():
                    if strike_str in exp_data and exp_data[strike_str]:
                        option = exp_data[strike_str][0]
                        oi = option.get('openInterest', 0)
                        call_gamma_total += option.get('gamma', 0) * oi
                        call_delta_total += option.get('delta', 0) * oi
                        call_volume_total += option.get('totalVolume', 0)
                        call_oi_total += oi
                
                for exp_data in put_exp_map.values():
                    if strike_str in exp_data and exp_data[strike_str]:
                        option = exp_data[strike_str][0]
                        oi = option.get('openInterest', 0)
                        put_gamma_total += option.get('gamma', 0) * oi
                        put_delta_total += abs(option.get('delta', 0)) * oi  # Put delta is negative
                        put_volume_total += option.get('totalVolume', 0)
                        put_oi_total += oi
                
                # Store aggregated data
                exposure_data['strikes'].append(strike)
                exposure_data['call_gamma'].append(call_gamma_total)
                exposure_data['put_gamma'].append(put_gamma_total)
                exposure_data['call_delta'].append(call_delta_total)
                exposure_data['put_delta'].append(put_delta_total)
                exposure_data['call_volume'].append(call_volume_total)
                exposure_data['put_volume'].append(put_volume_total)
                exposure_data['call_oi'].append(call_oi_total)
                exposure_data['put_oi'].append(put_oi_total)
            
            return exposure_data
            
        except Exception as e:
            self.logger.error(f"Error calculating exposure levels: {e}")
            return exposure_data

    def _calculate_pml_price(self, exposure_data: Dict, current_price: float) -> float:
        """Calculate PML (Peak Money Line) price from exposure data."""
        try:
            if not exposure_data['strikes'] or current_price <= 0:
                return max(1.0, current_price * 0.98)  # Fallback with minimum value
            
            strikes = np.array(exposure_data['strikes'])
            call_gamma = np.array(exposure_data['call_gamma'])
            put_gamma = np.array(exposure_data['put_gamma'])
            
            # Ensure arrays have data and no division by zero
            if len(strikes) == 0 or len(call_gamma) == 0 or len(put_gamma) == 0:
                return max(1.0, current_price * 0.98)
            
            # Calculate net gamma exposure (put gamma - call gamma for MM perspective)
            net_gamma = put_gamma - call_gamma
            
            # Find the strike with maximum negative gamma exposure (PML)
            if len(net_gamma) > 0 and not np.all(net_gamma == 0):
                pml_idx = np.argmin(net_gamma)  # Most negative
                pml_price = strikes[pml_idx]
                # Ensure reasonable PML price
                if pml_price > 0 and 0.5 * current_price <= pml_price <= 2.0 * current_price:
                    return float(pml_price)
            
            # Fallback to slightly below current price
            return max(1.0, current_price * 0.98)
            
        except Exception as e:
            self.logger.error(f"Error calculating PML price: {e}")
            return max(1.0, current_price * 0.98) if current_price > 0 else 1.0

    def _calculate_ceiling_price(self, pml_price: float, current_price: float, 
                               exposure_data: Dict) -> float:
        """Calculate ceiling price level."""
        try:
            # Ceiling is typically above current price where call gamma becomes significant
            volatility_factor = abs(current_price - pml_price) * 1.5
            ceiling = max(current_price, pml_price) + volatility_factor
            return float(ceiling)
        except Exception:
            return current_price * 1.05

    def _calculate_floor_price(self, pml_price: float, current_price: float, 
                             exposure_data: Dict) -> float:
        """Calculate floor price level."""
        try:
            # Floor is typically below current price where put gamma becomes significant
            volatility_factor = abs(current_price - pml_price) * 1.5
            floor = min(current_price, pml_price) - volatility_factor
            return float(max(0, floor))
        except Exception:
            return current_price * 0.95

    def _calculate_green_line_price(self, pml_price: float, current_price: float) -> float:
        """Calculate green line price (typically between current and PML)."""
        return (current_price + pml_price) / 2

    def _analyze_delta_flows(self, exposure_data: Dict, green_line_price: float) -> Dict[str, float]:
        """Analyze delta flows relative to green line."""
        try:
            if not exposure_data['strikes'] or green_line_price <= 0:
                return {
                    'call_delta': 0.0, 'put_delta': 0.0, 'net_delta': 0.0,
                    'calls_above_green': 0.0, 'puts_above_green': 0.0,
                    'calls_below_green': 0.0, 'puts_below_green': 0.0
                }
            
            strikes = np.array(exposure_data['strikes'])
            call_deltas = np.array(exposure_data['call_delta'])
            put_deltas = np.array(exposure_data['put_delta'])
            
            # Ensure arrays have data
            if len(strikes) == 0 or len(call_deltas) == 0 or len(put_deltas) == 0:
                return {
                    'call_delta': 0.0, 'put_delta': 0.0, 'net_delta': 0.0,
                    'calls_above_green': 0.0, 'puts_above_green': 0.0,
                    'calls_below_green': 0.0, 'puts_below_green': 0.0
                }
            
            # Separate above and below green line
            above_green_mask = strikes >= green_line_price
            below_green_mask = strikes < green_line_price
            
            calls_above_green = np.sum(call_deltas[above_green_mask]) if np.any(above_green_mask) else 0.0
            puts_above_green = np.sum(put_deltas[above_green_mask]) if np.any(above_green_mask) else 0.0
            calls_below_green = np.sum(call_deltas[below_green_mask]) if np.any(below_green_mask) else 0.0
            puts_below_green = np.sum(put_deltas[below_green_mask]) if np.any(below_green_mask) else 0.0
            
            total_call_delta = np.sum(call_deltas)
            total_put_delta = np.sum(put_deltas)
            net_delta = total_call_delta - total_put_delta
            
            return {
                'call_delta': float(total_call_delta),
                'put_delta': float(total_put_delta),
                'net_delta': float(net_delta),
                'calls_above_green': float(calls_above_green),
                'puts_above_green': float(puts_above_green),
                'calls_below_green': float(calls_below_green),
                'puts_below_green': float(puts_below_green)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing delta flows: {e}")
            return {
                'call_delta': 0.0, 'put_delta': 0.0, 'net_delta': 0.0,
                'calls_above_green': 0.0, 'puts_above_green': 0.0,
                'calls_below_green': 0.0, 'puts_below_green': 0.0
            }

    def _check_delta_confirmation(self, delta_analysis: Dict) -> bool:
        """Check if delta flows confirm bullish signal."""
        return (
            delta_analysis['call_delta'] > self.config.min_call_delta_positive and
            delta_analysis['put_delta'] > self.config.min_put_delta_positive
        )

    def _determine_pml_market_condition(self, current_price: float, pml_price: float, 
                                      delta_analysis: Dict) -> MarketCondition:
        """Determine market condition based on PML analysis."""
        # Prevent division by zero
        if pml_price <= 0 or current_price <= 0:
            return MarketCondition.UNCERTAIN
            
        pml_cross_pct = (current_price - pml_price) / pml_price
        
        if pml_cross_pct > self.config.min_pml_cross_pct:
            if self._check_delta_confirmation(delta_analysis):
                return MarketCondition.PML_CROSS_BULLISH
            else:
                return MarketCondition.DELTA_NEGATIVE
        elif pml_cross_pct < -self.config.min_pml_cross_pct:
            return MarketCondition.PML_CROSS_BEARISH
        else:
            return MarketCondition.RANGE_BOUND

    def find_pml_setups(self, symbol: str) -> List[PMLSetup]:
        """
        Find potential PML trading setups for a symbol using comprehensive options data.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            List of potential PML setups with actual contract details
        """
        try:
            # Get PML analysis
            pml_analysis = self.calculate_pml_levels(symbol)
            
            if pml_analysis['market_condition'] not in [
                MarketCondition.PML_CROSS_BULLISH, MarketCondition.DELTA_POSITIVE
            ]:
                self.logger.info(f"Market conditions not suitable for PML strategy on {symbol}")
                return []
            
            # Get comprehensive options data from our enhanced handler
            options_data = self.options_handler.get_all_options_data([symbol])
            
            if not options_data or symbol not in options_data.get('symbols', {}):
                self.logger.warning(f"No comprehensive options data for {symbol}")
                return []
            
            symbol_data = options_data['symbols'][symbol]
            current_price = symbol_data['underlying_price']
            
            setups = []
            
            # Process each expiration
            for expiration in symbol_data.get('expirations', []):
                dte = expiration['days_to_expiration']
                
                # Check if expiration is suitable for PML
                if not (self.config.min_dte <= dte <= self.config.max_dte):
                    continue
                
                # Look for ITM call setups using comprehensive data
                call_setups = self._find_itm_call_setups_from_comprehensive_data(
                    symbol, expiration, current_price, pml_analysis
                )
                setups.extend(call_setups)
            
            # Sort by potential profit (highest first)
            setups.sort(key=lambda x: x.potential_profit, reverse=True)
            
            return setups[:5]  # Return top 5 setups
            
        except Exception as e:
            self.logger.error(f"Error finding PML setups for {symbol}: {e}")
            return []

    def _find_suitable_expirations(self, options_chain: Dict) -> List[datetime]:
        """Find option expirations suitable for PML strategy."""
        suitable_exps = []
        
        if 'callExpDateMap' not in options_chain:
            return suitable_exps
        
        for exp_str in options_chain['callExpDateMap'].keys():
            try:
                # Parse expiration date from string
                exp_date = datetime.strptime(exp_str.split(':')[0], '%Y-%m-%d')
                dte = (exp_date - datetime.now()).days
                
                if self.config.min_dte <= dte <= self.config.max_dte:
                    suitable_exps.append(exp_date)
            except Exception as e:
                self.logger.warning(f"Error parsing expiration date {exp_str}: {e}")
                continue
        
        return suitable_exps

    def _find_itm_call_setups(self, symbol: str, exp_date: datetime, current_price: float,
                            ceiling_price: float, options_chain: Dict, 
                            pml_analysis: Dict) -> List[PMLSetup]:
        """Find ITM call setups for PML strategy."""
        setups = []
        
        try:
            dte = (exp_date - datetime.now()).days
            exp_str = exp_date.strftime('%Y-%m-%d') + ':0'
            
            if exp_str not in options_chain['callExpDateMap']:
                return setups
            
            calls = options_chain['callExpDateMap'][exp_str]
            
            # Look for ITM calls (strike < current price)
            for strike_str, call_data_list in calls.items():
                try:
                    strike = float(strike_str)
                    
                    # Only consider ITM calls
                    if strike >= current_price:
                        continue
                    
                    if not call_data_list:
                        continue
                    
                    call_data = call_data_list[0]
                    
                    # Check liquidity requirements
                    volume = call_data.get('totalVolume', 0)
                    open_interest = call_data.get('openInterest', 0)
                    
                    if volume < self.config.min_liquidity_volume / 10:  # Adjusted for options
                        continue
                    
                    # Calculate pricing
                    bid = call_data.get('bid', 0)
                    ask = call_data.get('ask', 0)
                    
                    if ask <= bid or ask == 0:
                        continue
                    
                    mid_price = (bid + ask) / 2
                    spread_pct = (ask - bid) / mid_price
                    
                    if spread_pct > self.config.max_spread_pct:
                        continue
                    
                    # Calculate potential profit at ceiling
                    intrinsic_at_ceiling = max(0, ceiling_price - strike)
                    potential_profit = intrinsic_at_ceiling - mid_price
                    
                    if potential_profit <= 0:
                        continue
                    
                    # Create setup
                    setup = PMLSetup(
                        symbol=symbol,
                        expiration_date=exp_date,
                        dte=dte,
                        strike=strike,
                        option_type='CALL',
                        current_price=mid_price,
                        bid=bid,
                        ask=ask,
                        mid_price=mid_price,
                        pml_price=pml_analysis['pml_price'],
                        ceiling_price=ceiling_price,
                        floor_price=pml_analysis['floor_price'],
                        green_line_price=pml_analysis['green_line_price'],
                        call_delta=pml_analysis['call_delta'],
                        put_delta=pml_analysis['put_delta'],
                        net_delta=pml_analysis['net_delta'],
                        delta=call_data.get('delta', 0),
                        gamma=call_data.get('gamma', 0),
                        theta=call_data.get('theta', 0),
                        vega=call_data.get('vega', 0),
                        implied_vol=call_data.get('volatility', 0),
                        intrinsic_value=max(0, current_price - strike),
                        time_value=mid_price - max(0, current_price - strike),
                        potential_profit=potential_profit,
                        max_loss=mid_price,
                        spot_price=current_price,
                        volume=volume,
                        open_interest=open_interest,
                        market_condition=pml_analysis['market_condition']
                    )
                    
                    setups.append(setup)
                    
                except (ValueError, KeyError, TypeError) as e:
                    continue
            
            return setups
            
        except Exception as e:
            self.logger.error(f"Error finding ITM call setups: {e}")
            return setups

    def generate_trading_signal(self, symbol: str, technical_indicators: Dict[str, Any] = None) -> TradingSignal:
        """
        Generate trading signal for PML strategy using technical indicators.
        
        Args:
            symbol: Stock symbol to analyze
            technical_indicators: Pre-calculated technical indicators (optional)
            
        Returns:
            Trading signal with recommendation
        """
        try:
            # Load technical indicators if not provided
            if technical_indicators is None:
                all_indicators = load_technical_indicators()
                if symbol not in all_indicators:
                    return TradingSignal(
                        signal_type=SignalType.NO_SIGNAL,
                        symbol=symbol,
                        timestamp=datetime.now(),
                        confidence=0.0,
                        entry_reason=f"No technical indicators available for {symbol}"
                    )
                technical_indicators = all_indicators[symbol]
            
            # Extract PML indicators from technical indicators
            pml_indicators = technical_indicators.get('pml_indicators', {})
            base_indicators = technical_indicators.get('base_indicators', {})
            
            if not pml_indicators:
                return TradingSignal(
                    signal_type=SignalType.NO_SIGNAL,
                    symbol=symbol,
                    timestamp=datetime.now(),
                    confidence=0.0,
                    entry_reason="No PML indicators available"
                )
            
            # Create PML analysis from technical indicators
            pml_analysis = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'spot_price': base_indicators.get('current_price', 0),
                'pml_price': pml_indicators.get('pml_price', 0),
                'ceiling_price': pml_indicators.get('ceiling_price', 0),
                'floor_price': pml_indicators.get('floor_price', 0),
                'green_line_price': pml_indicators.get('green_line_price', 0),
                'call_delta': pml_indicators.get('call_delta', 0),
                'put_delta': pml_indicators.get('put_delta', 0),
                'net_delta': pml_indicators.get('net_delta', 0),
                'pml_cross_bullish': pml_indicators.get('pml_cross_bullish', False),
                'delta_confirmation': pml_indicators.get('delta_confirmation', False),
                'market_condition': self._parse_market_condition(pml_indicators.get('market_condition', 'UNCERTAIN'))
            }
            
            # Determine signal based on analysis
            signal_type, confidence, reason = self._determine_pml_signal_from_indicators(
                pml_analysis, base_indicators
            )
            
            # Create trading signal
            signal = TradingSignal(
                signal_type=signal_type,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=confidence,
                entry_reason=reason,
                market_condition=pml_analysis['market_condition'],
                volatility_environment=self._get_volatility_environment_from_indicators(base_indicators)
            )
            
            # Add dynamic position sizing and risk management for buy signals
            if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
                # Load dynamic risk configuration
                risk_config = load_risk_config()
                
                # Calculate position size using dynamic config
                signal.position_size = self._calculate_dynamic_position_size(base_indicators, risk_config)
                
                current_price = base_indicators.get('current_price', 0)
                if current_price > 0:
                    # Use dynamic stop loss and take profit from risk config
                    stop_loss_pct, take_profit_pct = self._get_dynamic_risk_settings(risk_config, 'pml')
                    signal.stop_loss = current_price * (1 - stop_loss_pct)
                    signal.profit_target = current_price * (1 + take_profit_pct)
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating PML trading signal for {symbol}: {e}")
            return TradingSignal(
                signal_type=SignalType.NO_SIGNAL,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=0.0,
                entry_reason=f"Error in analysis: {str(e)}"
            )

    def _determine_pml_signal(self, pml_analysis: Dict, setups: List[PMLSetup]) -> Tuple[SignalType, float, str]:
        """Determine trading signal based on PML analysis."""
        # Simplified signal generation based on PML cross
        if pml_analysis['market_condition'] == MarketCondition.PML_CROSS_BEARISH:
            return SignalType.NO_SIGNAL, 0.0, "Price below PML - bearish signal"
        
        if not pml_analysis['pml_cross_bullish']:
            return SignalType.NO_SIGNAL, 0.0, "Price has not crossed above PML"
        
        # Generate signals even without complex options setups
        confidence_factors = []
        
        # PML cross factor (main signal)
        if pml_analysis['spot_price'] > 0 and pml_analysis['pml_price'] > 0:
            pml_cross_strength = (pml_analysis['spot_price'] - pml_analysis['pml_price']) / pml_analysis['pml_price']
            confidence_factors.append(min(0.4, max(0.1, pml_cross_strength * 20)))  # 0.1 to 0.4
        
        # Delta confirmation factor
        if pml_analysis['delta_confirmation']:
            confidence_factors.append(0.3)
        else:
            confidence_factors.append(0.1)  # Still give some credit
        
        # Market condition factor
        if pml_analysis['market_condition'] == MarketCondition.PML_CROSS_BULLISH:
            confidence_factors.append(0.2)
        
        # Base confidence for any bullish cross
        confidence_factors.append(0.1)
        
        total_confidence = sum(confidence_factors)
        
        # Generate signals based on simplified criteria
        if total_confidence >= 0.7:
            reason = "Strong PML bullish cross"
            if setups:
                reason += f" with {len(setups)} option setups available"
            return SignalType.STRONG_BUY, total_confidence, reason
        elif total_confidence >= 0.5:
            reason = "Good PML bullish cross"
            if setups:
                reason += f" with {len(setups)} option setups available"
            return SignalType.BUY, total_confidence, reason
        elif total_confidence >= 0.3:
            reason = "Moderate PML bullish signal"
            if setups:
                reason += f" with {len(setups)} option setups available"
            return SignalType.HOLD, total_confidence, reason
        else:
            return SignalType.NO_SIGNAL, total_confidence, "Weak PML signal strength"

    def _get_pml_volatility_environment(self, pml_analysis: Dict) -> str:
        """Get volatility environment description for PML."""
        try:
            spot_price = pml_analysis.get('spot_price', 0)
            pml_price = pml_analysis.get('pml_price', 0)
            
            # Prevent division by zero
            if spot_price <= 0:
                return "Unknown Volatility"
                
            pml_distance = abs(spot_price - pml_price)
            price_pct = pml_distance / spot_price
            
            if price_pct > 0.05:
                return "High Volatility"
            elif price_pct < 0.01:
                return "Low Volatility"
            else:
                return "Medium Volatility"
        except Exception as e:
            return "Unknown Volatility"

    def _calculate_position_size(self, setup: PMLSetup) -> float:
        """Calculate appropriate position size for PML setup."""
        # Simplified position sizing based on max loss
        max_risk_per_trade = 1000  # Would be based on account size
        contracts = max_risk_per_trade / (setup.max_loss * 100)  # 100 shares per contract
        return max(1, min(10, int(contracts)))  # Between 1-10 contracts

    def _parse_market_condition(self, condition_str: str) -> MarketCondition:
        """Parse market condition string to enum."""
        try:
            return MarketCondition(condition_str)
        except ValueError:
            return MarketCondition.UNCERTAIN

    def _determine_pml_signal_from_indicators(self, pml_analysis: Dict, base_indicators: Dict) -> Tuple[SignalType, float, str]:
        """Determine PML signal from technical indicators."""
        try:
            # Check if PML cross is bullish
            if not pml_analysis.get('pml_cross_bullish', False):
                return SignalType.NO_SIGNAL, 0.0, "Price has not crossed above PML"
            
            # Calculate confidence factors
            confidence_factors = []
            
            # PML cross strength
            spot_price = pml_analysis.get('spot_price', 0)
            pml_price = pml_analysis.get('pml_price', 0)
            
            if spot_price > 0 and pml_price > 0:
                pml_cross_strength = (spot_price - pml_price) / pml_price
                confidence_factors.append(min(0.4, max(0.1, pml_cross_strength * 20)))
            
            # Delta confirmation
            if pml_analysis.get('delta_confirmation', False):
                confidence_factors.append(0.3)
            else:
                confidence_factors.append(0.1)
            
            # Market condition
            market_condition = pml_analysis.get('market_condition', MarketCondition.UNCERTAIN)
            if market_condition == MarketCondition.PML_CROSS_BULLISH:
                confidence_factors.append(0.2)
            
            # RSI support
            rsi = base_indicators.get('rsi', 50)
            if rsi < 70:  # Not overbought
                confidence_factors.append(0.1)
            
            total_confidence = sum(confidence_factors)
            
            # Generate signal based on confidence
            if total_confidence >= 0.7:
                return SignalType.STRONG_BUY, total_confidence, "Strong PML bullish cross with confirmation"
            elif total_confidence >= 0.5:
                return SignalType.BUY, total_confidence, "Good PML bullish cross"
            elif total_confidence >= 0.3:
                return SignalType.HOLD, total_confidence, "Moderate PML signal"
            else:
                return SignalType.NO_SIGNAL, total_confidence, "Weak PML signal"
                
        except Exception as e:
            return SignalType.NO_SIGNAL, 0.0, f"Error in signal determination: {str(e)}"

    def _get_volatility_environment_from_indicators(self, base_indicators: Dict) -> str:
        """Get volatility environment from base indicators."""
        try:
            realized_vol = base_indicators.get('realized_volatility', 20)
            
            if realized_vol > 30:
                return "High Volatility"
            elif realized_vol < 15:
                return "Low Volatility"
            else:
                return "Medium Volatility"
        except Exception:
            return "Unknown Volatility"

    def _calculate_basic_position_size(self, base_indicators: Dict) -> float:
        """Calculate basic position size from indicators."""
        try:
            # Simple position sizing based on volatility
            atr = base_indicators.get('atr', 0)
            current_price = base_indicators.get('current_price', 100)
            
            if current_price > 0 and atr > 0:
                volatility_pct = (atr / current_price) * 100
                if volatility_pct > 3:
                    return 1.0  # Small position for high volatility
                elif volatility_pct < 1:
                    return 3.0  # Larger position for low volatility
                else:
                    return 2.0  # Medium position
            else:
                return 1.0  # Default small position
        except Exception:
            return 1.0

    def _calculate_dynamic_position_size(self, base_indicators: Dict, risk_config: Dict) -> float:
        """Calculate position size using dynamic risk configuration."""
        try:
            # Get PML-specific risk settings
            pml_config = risk_config.get('pml_strategy', {})
            
            # Check if dynamic position sizing is enabled
            if not pml_config.get('use_dynamic_position_sizing', {}).get('enabled', False):
                return self._calculate_basic_position_size(base_indicators)
            
            # Get position sizing parameters
            sizing_params = pml_config.get('position_sizing', {})
            base_size = sizing_params.get('base_position_size', 1.0)
            max_size = sizing_params.get('max_position_size', 5.0)
            min_size = sizing_params.get('min_position_size', 0.5)
            
            # Volatility-based adjustment
            atr = base_indicators.get('atr', 0)
            current_price = base_indicators.get('current_price', 100)
            
            if current_price > 0 and atr > 0:
                volatility_pct = (atr / current_price) * 100
                
                # Adjust position size based on volatility
                if volatility_pct > 4:  # High volatility
                    adjusted_size = base_size * 0.5
                elif volatility_pct < 1:  # Low volatility
                    adjusted_size = base_size * 1.5
                else:  # Medium volatility
                    adjusted_size = base_size
                
                # Apply min/max constraints
                final_size = max(min_size, min(max_size, adjusted_size))
                
                print(f"📊 Dynamic position sizing: volatility={volatility_pct:.1f}%, size={final_size:.1f}")
                return final_size
            else:
                return base_size
                
        except Exception as e:
            print(f"❌ Error in dynamic position sizing: {e}")
            return self._calculate_basic_position_size(base_indicators)

    def _get_dynamic_risk_settings(self, risk_config: Dict, strategy_name: str) -> Tuple[float, float]:
        """Get dynamic stop loss and take profit settings from risk config."""
        try:
            # Get strategy-specific config
            strategy_config = risk_config.get(f'{strategy_name}_strategy', {})
            
            # Default values
            default_stop_loss = 0.10  # 10%
            default_take_profit = 0.25  # 25%
            
            # Check if dynamic risk management is enabled
            if not strategy_config.get('use_dynamic_risk_management', {}).get('enabled', False):
                return default_stop_loss, default_take_profit
            
            # Get stop loss settings
            stop_loss_config = strategy_config.get('stop_loss', {})
            if stop_loss_config.get('enabled', False):
                stop_loss_pct = stop_loss_config.get('percentage', default_stop_loss)
            else:
                stop_loss_pct = default_stop_loss
            
            # Get take profit settings
            take_profit_config = strategy_config.get('take_profit', {})
            if take_profit_config.get('enabled', False):
                take_profit_ratio = take_profit_config.get('ratio', 2.5)  # Default 2.5:1 ratio
                take_profit_pct = stop_loss_pct * take_profit_ratio
            else:
                take_profit_pct = default_take_profit
            
            print(f"📊 Dynamic risk settings for {strategy_name}: SL={stop_loss_pct*100:.1f}%, TP={take_profit_pct*100:.1f}%")
            return stop_loss_pct, take_profit_pct
            
        except Exception as e:
            print(f"❌ Error getting dynamic risk settings: {e}")
            return default_stop_loss, default_take_profit

    def _find_itm_call_setups_from_comprehensive_data(self, symbol: str, expiration: Dict, 
                                                    current_price: float, pml_analysis: Dict) -> List[PMLSetup]:
        """Find ITM call setups from comprehensive options data."""
        setups = []
        
        try:
            exp_date_str = expiration['expiration_date']
            dte = expiration['days_to_expiration']
            
            # Parse expiration date
            exp_date = datetime.strptime(exp_date_str.split(':')[0], '%Y-%m-%d')
            
            contracts = expiration.get('contracts', [])
            if not contracts:
                return setups
            
            # Look for ITM call options (strike < current price)
            for contract in contracts:
                call_data = contract.get('call')
                if not call_data:
                    continue
                
                strike = contract['strike']
                
                # Only consider ITM calls
                if strike >= current_price:
                    continue
                
                # Check liquidity requirements
                volume = call_data.get('volume', 0)
                open_interest = call_data.get('open_interest', 0)
                
                if volume < self.config.min_liquidity_volume / 10:  # Adjusted for options
                    continue
                
                # Calculate pricing
                bid = call_data.get('bid', 0)
                ask = call_data.get('ask', 0)
                
                if ask <= bid or ask == 0:
                    continue
                
                mid_price = (bid + ask) / 2
                spread_pct = (ask - bid) / mid_price if mid_price > 0 else float('inf')
                
                if spread_pct > self.config.max_spread_pct:
                    continue
                
                # Calculate potential profit at ceiling
                ceiling_price = pml_analysis['ceiling_price']
                intrinsic_at_ceiling = max(0, ceiling_price - strike)
                potential_profit = intrinsic_at_ceiling - mid_price
                
                if potential_profit <= 0:
                    continue
                
                # Create setup
                setup = PMLSetup(
                    symbol=symbol,
                    expiration_date=exp_date,
                    dte=dte,
                    strike=strike,
                    option_type='CALL',
                    current_price=mid_price,
                    bid=bid,
                    ask=ask,
                    mid_price=mid_price,
                    pml_price=pml_analysis['pml_price'],
                    ceiling_price=ceiling_price,
                    floor_price=pml_analysis['floor_price'],
                    green_line_price=pml_analysis['green_line_price'],
                    call_delta=pml_analysis['call_delta'],
                    put_delta=pml_analysis['put_delta'],
                    net_delta=pml_analysis['net_delta'],
                    delta=call_data.get('delta', 0),
                    gamma=call_data.get('gamma', 0),
                    theta=call_data.get('theta', 0),
                    vega=call_data.get('vega', 0),
                    implied_vol=call_data.get('implied_volatility', 0),
                    intrinsic_value=max(0, current_price - strike),
                    time_value=mid_price - max(0, current_price - strike),
                    potential_profit=potential_profit,
                    max_loss=mid_price,
                    spot_price=current_price,
                    volume=volume,
                    open_interest=open_interest,
                    market_condition=pml_analysis['market_condition']
                )
                
                setups.append(setup)
                
        except Exception as e:
            self.logger.error(f"Error finding ITM call setups from comprehensive data: {e}")
        
        return setups

    def _create_empty_pml_analysis(self, symbol: str) -> Dict[str, Any]:
        """Create empty PML analysis for error cases."""
        return {
            'symbol': symbol,
            'timestamp': datetime.now(),
            'spot_price': 0.0,
            'pml_price': 0.0,
            'ceiling_price': 0.0,
            'floor_price': 0.0,
            'green_line_price': 0.0,
            'call_delta': 0.0,
            'put_delta': 0.0,
            'net_delta': 0.0,
            'calls_above_green': 0.0,
            'puts_above_green': 0.0,
            'calls_below_green': 0.0,
            'puts_below_green': 0.0,
            'pml_cross_bullish': False,
            'delta_confirmation': False,
            'market_condition': MarketCondition.UNCERTAIN
        }

    def get_strategy_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get comprehensive strategy summary for a symbol.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary with complete strategy analysis
        """
        pml_analysis = self.calculate_pml_levels(symbol)
        setups = self.find_pml_setups(symbol)
        signal = self.generate_trading_signal(symbol)
        
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'pml_analysis': pml_analysis,
            'potential_setups': [
                {
                    'expiration': setup.expiration_date.isoformat(),
                    'dte': setup.dte,
                    'strike': setup.strike,
                    'option_type': setup.option_type,
                    'current_price': setup.current_price,
                    'potential_profit': setup.potential_profit,
                    'max_loss': setup.max_loss,
                    'pml_price': setup.pml_price,
                    'ceiling_price': setup.ceiling_price
                }
                for setup in setups
            ],
            'trading_signal': {
                'signal': signal.signal_type.value,
                'confidence': signal.confidence,
                'reason': signal.entry_reason,
                'position_size': signal.position_size,
                'market_condition': signal.market_condition.value,
                'volatility_environment': signal.volatility_environment
            }
        }


def load_risk_config() -> Dict[str, Any]:
    """Load risk configuration from risk_config_live.json"""
    try:
        import os
        import json
        script_dir = os.path.dirname(os.path.abspath(__file__))
        risk_config_path = os.path.join(script_dir, 'risk_config_live.json')
        
        if not os.path.exists(risk_config_path):
            print(f"Warning: risk_config_live.json not found at {risk_config_path}")
            return {}
        
        with open(risk_config_path, 'r') as f:
            data = json.load(f)
        
        print(f"Loaded risk configuration with {len(data)} sections")
        return data
        
    except Exception as e:
        print(f"Error loading risk configuration: {e}")
        return {}

def load_technical_indicators() -> Dict[str, Any]:
    """Load technical indicators from technical_indicators.json"""
    try:
        import os
        import json
        script_dir = os.path.dirname(os.path.abspath(__file__))
        technical_indicators_path = os.path.join(script_dir, 'technical_indicators.json')
        
        if not os.path.exists(technical_indicators_path):
            print(f"Warning: technical_indicators.json not found at {technical_indicators_path}")
            return {}
        
        with open(technical_indicators_path, 'r') as f:
            data = json.load(f)
        
        indicators = data.get('indicators', {})
        print(f"Loaded technical indicators for {len(indicators)} symbols")
        return indicators
        
    except Exception as e:
        print(f"Error loading technical indicators: {e}")
        return {}

def load_watchlist_from_live_monitor() -> List[str]:
    """Load watchlist symbols from live_monitor.json"""
    try:
        import json
        import os
        
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        live_monitor_path = os.path.join(script_dir, 'live_monitor.json')
        
        if not os.path.exists(live_monitor_path):
            print(f"Warning: live_monitor.json not found at {live_monitor_path}")
            return ['SPY', 'QQQ', 'NVDA']  # Fallback symbols
        
        with open(live_monitor_path, 'r') as f:
            data = json.load(f)
        
        # Extract symbols from integrated_watchlist
        watchlist_symbols = data.get('integrated_watchlist', {}).get('symbols', [])
        
        if not watchlist_symbols:
            # Fallback to symbols_monitored in metadata
            watchlist_symbols = data.get('metadata', {}).get('symbols_monitored', [])
        
        if not watchlist_symbols:
            print("Warning: No symbols found in live_monitor.json, using fallback")
            return ['SPY', 'QQQ', 'NVDA']
        
        print(f"Loaded {len(watchlist_symbols)} symbols from live_monitor.json")
        return watchlist_symbols
        
    except Exception as e:
        print(f"Error loading watchlist from live_monitor.json: {e}")
        return ['SPY', 'QQQ', 'NVDA']  # Fallback symbols

def analyze_symbol_pml(symbol: str, strategy: PMLStrategy) -> Tuple[str, Dict[str, Any]]:
    """Analyze a single symbol for PML signals (for parallel processing)"""
    try:
        print(f"Analyzing {symbol}...")
        
        # Generate trading signal using ONLY technical indicators
        signal = strategy.generate_trading_signal(symbol)
        
        # Read auto_approve status from centralized config
        auto_approve = True  # Default
        try:
            import os
            config_file = 'auto_approve_config.json'
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    auto_config = json.load(f)
                strategy_controls = auto_config.get('strategy_controls', {})
                pml_controls = strategy_controls.get('pml', {})
                auto_approve = pml_controls.get('auto_approve', True)
        except Exception as e:
            print(f"Warning: Could not read auto_approve config: {e}")
        
        # Store signal data for live_monitor.json
        signal_data = {
            'signal_type': signal.signal_type.value,
            'confidence': signal.confidence,
            'entry_reason': signal.entry_reason,
            'timestamp': signal.timestamp.isoformat(),
            'market_condition': signal.market_condition.value,
            'volatility_environment': signal.volatility_environment,
            'position_size': signal.position_size,
            'stop_loss': signal.stop_loss,
            'profit_target': signal.profit_target,
            'auto_approve': auto_approve  # Read from centralized config
        }
        
        # Add setup details if available
        if signal.setup:
            signal_data['setup'] = {
                'strike': signal.setup.strike,
                'option_type': signal.setup.option_type,
                'expiration_date': signal.setup.expiration_date.isoformat(),
                'dte': signal.setup.dte,
                'current_price': signal.setup.current_price,
                'pml_price': signal.setup.pml_price,
                'ceiling_price': signal.setup.ceiling_price,
                'potential_profit': signal.setup.potential_profit,
                'max_loss': signal.setup.max_loss
            }
        
        # Print summary using data from technical indicators
        all_indicators = load_technical_indicators()
        if symbol in all_indicators:
            base_indicators = all_indicators[symbol].get('base_indicators', {})
            pml_indicators = all_indicators[symbol].get('pml_indicators', {})
            current_price = base_indicators.get('current_price', 0)
            pml_price = pml_indicators.get('pml_price', 0)
            market_condition = pml_indicators.get('market_condition', 'UNCERTAIN')
            print(f"  {symbol}: ${current_price:.2f} | PML: ${pml_price:.2f} | {market_condition} | {signal.signal_type.value}")
        else:
            print(f"  {symbol}: No technical indicators available | {signal.signal_type.value}")
        
        return symbol, signal_data
        
    except Exception as e:
        print(f"  Error analyzing {symbol}: {e}")
        # Return error signal
        error_signal = {
            'signal_type': 'NO_SIGNAL',
            'confidence': 0.0,
            'entry_reason': f'Analysis error: {str(e)}',
            'timestamp': datetime.now().isoformat(),
            'market_condition': 'UNCERTAIN',
            'volatility_environment': 'Unknown',
            'position_size': 0,
            'stop_loss': 0,
            'profit_target': 0
        }
        return symbol, error_signal

def run_pml_analysis_for_watchlist() -> Dict[str, Any]:
    """Run PML analysis for all symbols in the watchlist using parallel processing"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    strategy = PMLStrategy()
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print("PML Strategy Analysis (Parallel Processing)")
    print("=" * 50)
    print(f"Processing {len(watchlist_symbols)} symbols concurrently...")
    
    start_time = time.time()
    pml_signals = {}
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(6, len(watchlist_symbols))  # Limit to 6 concurrent threads to avoid API rate limits
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks
        future_to_symbol = {
            executor.submit(analyze_symbol_pml, symbol, strategy): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, signal_data = future.result(timeout=30)  # 30 second timeout per symbol
                pml_signals[symbol] = signal_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  Error processing {symbol}: {e}")
                pml_signals[symbol] = {
                    'signal_type': 'NO_SIGNAL',
                    'confidence': 0.0,
                    'entry_reason': f'Processing error: {str(e)}',
                    'timestamp': datetime.now().isoformat(),
                    'market_condition': 'UNCERTAIN',
                    'volatility_environment': 'Unknown',
                    'position_size': 0,
                    'stop_loss': 0,
                    'profit_target': 0
                }
    
    elapsed_time = time.time() - start_time
    print(f"\n✅ PML Analysis completed in {elapsed_time:.2f} seconds")
    print(f"📊 Processed {len(pml_signals)} symbols with {max_workers} concurrent threads")
    
    return pml_signals

def save_pml_signals_to_file(pml_signals: Dict[str, Any]) -> bool:
    """Save PML signals to dedicated pml_signals.json file"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pml_signals_path = os.path.join(script_dir, 'pml_signals.json')
        
        # Create comprehensive PML signals data
        pml_data = {
            'strategy_name': 'PML_Strategy',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(pml_signals),
            'signals_generated': len([s for s in pml_signals.values() if s['signal_type'] in ['BUY', 'STRONG_BUY', 'HOLD']]),
            'analysis_summary': {
                'strong_buy': len([s for s in pml_signals.values() if s['signal_type'] == 'STRONG_BUY']),
                'buy': len([s for s in pml_signals.values() if s['signal_type'] == 'BUY']),
                'hold': len([s for s in pml_signals.values() if s['signal_type'] == 'HOLD']),
                'no_signal': len([s for s in pml_signals.values() if s['signal_type'] == 'NO_SIGNAL'])
            },
            'signals': pml_signals,
            'metadata': {
                'strategy_type': 'options_flow_analysis',
                'signal_types': ['STRONG_BUY', 'BUY', 'HOLD', 'NO_SIGNAL'],
                'analysis_method': 'pml_cross_with_delta_confirmation',
                'update_frequency': 'on_demand',
                'data_source': 'schwab_options_chain'
            }
        }
        
        # Write to dedicated PML signals file
        with open(pml_signals_path, 'w') as f:
            json.dump(pml_data, f, indent=2)
        
        print(f"✅ Saved PML signals to {pml_signals_path}")
        print(f"📊 Analysis: {pml_data['analysis_summary']['strong_buy']} STRONG_BUY, {pml_data['analysis_summary']['buy']} BUY, {pml_data['analysis_summary']['hold']} HOLD")
        return True
        
    except Exception as e:
        print(f"❌ Error saving PML signals to file: {e}")
        return False

def update_existing_pml_signals_auto_approve():
    """Update existing PML signals with current auto_approve status from centralized config"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pml_signals_path = os.path.join(script_dir, 'pml_signals.json')
        
        # Check if PML signals file exists
        if not os.path.exists(pml_signals_path):
            print(f"PML signals file not found at {pml_signals_path}")
            return False
        
        # Read current auto_approve status from centralized config
        auto_approve = True  # Default
        try:
            config_file = os.path.join(script_dir, 'auto_approve_config.json')
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    auto_config = json.load(f)
                strategy_controls = auto_config.get('strategy_controls', {})
                pml_controls = strategy_controls.get('pml', {})
                auto_approve = pml_controls.get('auto_approve', True)
                print(f"📖 Read auto_approve status for PML: {auto_approve}")
        except Exception as e:
            print(f"Warning: Could not read auto_approve config: {e}")
        
        # Load existing PML signals
        with open(pml_signals_path, 'r') as f:
            pml_data = json.load(f)
        
        # Update auto_approve for all signals
        signals_updated = 0
        if 'signals' in pml_data:
            for symbol, signal_data in pml_data['signals'].items():
                if isinstance(signal_data, dict):
                    old_auto_approve = signal_data.get('auto_approve', None)
                    signal_data['auto_approve'] = auto_approve
                    if old_auto_approve != auto_approve:
                        signals_updated += 1
        
        # Update metadata
        if 'metadata' not in pml_data:
            pml_data['metadata'] = {}
        pml_data['metadata']['auto_approve_last_updated'] = datetime.now().isoformat()
        pml_data['metadata']['auto_approve_status'] = auto_approve
        
        # Save updated PML signals
        with open(pml_signals_path, 'w') as f:
            json.dump(pml_data, f, indent=2)
        
        print(f"✅ Updated {signals_updated} PML signals with auto_approve: {auto_approve}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating PML signals auto_approve: {e}")
        return False

def monitor_auto_approve_config():
    """Monitor auto_approve_config.json for changes and update signals in real-time"""
    import time
    import os
    
    config_file = 'auto_approve_config.json'
    last_modified = 0
    
    print("🔍 Starting real-time auto_approve config monitoring for PML strategy...")
    
    while True:
        try:
            if os.path.exists(config_file):
                current_modified = os.path.getmtime(config_file)
                
                # Check if file has been modified
                if current_modified > last_modified:
                    last_modified = current_modified
                    print(f"📋 Config file updated, refreshing PML signals auto_approve status...")
                    
                    # Update existing signals with new auto_approve status
                    update_existing_pml_signals_auto_approve()
                    
                    print(f"✅ PML signals updated at {datetime.now().strftime('%H:%M:%S')}")
            
            # Check every 2 seconds for config changes
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n🛑 Stopping PML auto_approve config monitoring...")
            break
        except Exception as e:
            print(f"❌ Error in PML config monitoring: {e}")
            time.sleep(5)  # Wait longer on error

def insert_pml_signals_to_db(pml_signals: Dict[str, Any]) -> bool:
    """Insert PML signals into PostgreSQL database with truncation"""
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
        print("🗑️  Truncating pml_signals table...")
        # Use DELETE instead of TRUNCATE to avoid relation locks in parallel execution
        cur.execute("DELETE FROM pml_signals;")
        
        # Insert new data
        insert_count = 0
        for symbol, signal_data in pml_signals.items():
            try:
                # Extract setup data if available
                setup_data = signal_data.get('setup', {})
                
                # Prepare insert statement
                insert_sql = """
                INSERT INTO pml_signals (
                    timestamp, symbol, signal_type, confidence, entry_reason, exit_reason,
                    position_size, stop_loss, profit_target, market_condition, volatility_environment,
                    expiration_date, dte, strike, option_type, current_price, bid, ask,
                    pml_price, ceiling_price, floor_price, green_line_price,
                    call_delta, put_delta, net_delta, delta, gamma, theta, vega,
                    implied_vol, intrinsic_value, time_value, potential_profit, max_loss,
                    spot_price, volume, open_interest, auto_approve
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                # Prepare values
                values = (
                    signal_data.get('timestamp', datetime.now().isoformat()),
                    symbol,
                    signal_data.get('signal_type', 'NO_SIGNAL'),
                    signal_data.get('confidence', 0.0),
                    signal_data.get('entry_reason', ''),
                    signal_data.get('exit_reason', ''),
                    signal_data.get('position_size', 0.0),
                    signal_data.get('stop_loss', 0.0),
                    signal_data.get('profit_target', 0.0),
                    signal_data.get('market_condition', 'UNCERTAIN'),
                    signal_data.get('volatility_environment', 'Unknown'),
                    setup_data.get('expiration_date'),
                    setup_data.get('dte', 0),
                    setup_data.get('strike', 0.0),
                    setup_data.get('option_type', 'CALL'),
                    setup_data.get('current_price', 0.0),
                    setup_data.get('bid', 0.0),
                    setup_data.get('ask', 0.0),
                    setup_data.get('pml_price', 0.0),
                    setup_data.get('ceiling_price', 0.0),
                    setup_data.get('floor_price', 0.0),
                    setup_data.get('green_line_price', 0.0),
                    setup_data.get('call_delta', 0.0),
                    setup_data.get('put_delta', 0.0),
                    setup_data.get('net_delta', 0.0),
                    setup_data.get('delta', 0.0),
                    setup_data.get('gamma', 0.0),
                    setup_data.get('theta', 0.0),
                    setup_data.get('vega', 0.0),
                    setup_data.get('implied_vol', 0.0),
                    setup_data.get('intrinsic_value', 0.0),
                    setup_data.get('time_value', 0.0),
                    setup_data.get('potential_profit', 0.0),
                    setup_data.get('max_loss', 0.0),
                    setup_data.get('spot_price', 0.0),
                    setup_data.get('volume', 0),
                    setup_data.get('open_interest', 0),
                    signal_data.get('auto_approve', True)
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
        
        print(f"✅ Successfully inserted {insert_count} PML signals into database")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting PML signals to database: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

# Global throttling variables for database insertion
_last_db_insertion = 0
_db_insertion_interval = 30  # seconds

def main():
    """Main function to run PML strategy analysis"""
    import sys
    
    # Check if we should run in monitoring mode
    if len(sys.argv) > 1 and sys.argv[1] == '--monitor':
        monitor_auto_approve_config()
        return
    
    print("🔄 Starting PML Strategy Analysis...")
    
    # First, update existing signals with current auto_approve status
    print("📋 Updating existing PML signals with current auto_approve status...")
    update_existing_pml_signals_auto_approve()
    
    # Run analysis for all watchlist symbols
    pml_signals = run_pml_analysis_for_watchlist()
    
    # Save signals to dedicated PML file
    save_pml_signals_to_file(pml_signals)
    
    # Update signals again after saving to ensure consistency
    print("🔄 Final update of PML signals with auto_approve status...")
    update_existing_pml_signals_auto_approve()


if __name__ == "__main__":
    main()
