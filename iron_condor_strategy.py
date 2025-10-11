#!/usr/bin/env python3
"""
Iron Condor Strategy Handler

This handler provides Iron Condor options strategy analysis and trading signals:
1. Market condition analysis (volatility, trend, range-bound detection)
2. Iron Condor setup identification
3. Entry and exit signal generation
4. Risk management calculations
5. Position sizing recommendations

Iron Condor Strategy Overview:
- Sell OTM Call and Put (collect premium)
- Buy further OTM Call and Put (limit risk)
- Profit from time decay in range-bound markets
- Best in low volatility, sideways markets
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
    RANGE_BOUND = "RANGE_BOUND"
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    UNCERTAIN = "UNCERTAIN"

@dataclass
class IronCondorConfig:
    """Configuration for Iron Condor strategy"""
    # Time to expiration preferences
    min_dte: int = 30  # Minimum days to expiration
    max_dte: int = 45  # Maximum days to expiration
    optimal_dte: int = 35  # Optimal days to expiration
    
    # Strike selection
    short_call_delta: float = 0.20  # Target delta for short call
    short_put_delta: float = -0.20  # Target delta for short put
    wing_width: float = 5.0  # Width between short and long strikes
    
    # Entry criteria
    min_credit: float = 1.00  # Minimum credit to collect
    max_credit_to_width_ratio: float = 0.40  # Max credit as % of wing width
    min_prob_profit: float = 0.60  # Minimum probability of profit
    
    # Volatility criteria
    min_iv_rank: float = 20.0  # Minimum IV rank for entry
    max_iv_rank: float = 80.0  # Maximum IV rank for entry
    optimal_iv_rank: float = 50.0  # Optimal IV rank
    
    # Market condition filters
    max_trend_strength: float = 0.3  # Max trend strength (0-1)
    min_range_days: int = 10  # Min days in range for range-bound detection
    volatility_lookback: int = 20  # Days for volatility calculation
    
    # Risk management
    profit_target_pct: float = 0.50  # Take profit at 50% of max profit
    stop_loss_pct: float = 2.00  # Stop loss at 200% of credit received
    max_loss_pct: float = 0.02  # Max loss as % of account
    
    # Position management
    max_positions_per_symbol: int = 1
    max_total_positions: int = 5
    position_size_pct: float = 0.05  # Position size as % of account

@dataclass
class IronCondorSetup:
    """Iron Condor setup details"""
    symbol: str
    expiration_date: datetime
    dte: int
    
    # Strike prices
    long_put_strike: float
    short_put_strike: float
    short_call_strike: float
    long_call_strike: float
    
    # Option details
    long_put_price: float
    short_put_price: float
    short_call_price: float
    long_call_price: float
    
    # Greeks and metrics
    net_credit: float
    max_profit: float
    max_loss: float
    breakeven_lower: float
    breakeven_upper: float
    prob_profit: float
    
    # Risk metrics
    delta: float
    gamma: float
    theta: float
    vega: float
    
    # Market analysis
    current_price: float
    iv_rank: float
    market_condition: MarketCondition

@dataclass
class TradingSignal:
    """Trading signal with details"""
    signal_type: SignalType
    symbol: str
    timestamp: datetime
    confidence: float  # 0-1 confidence score
    
    # Signal details
    setup: Optional[IronCondorSetup] = None
    entry_reason: str = ""
    exit_reason: str = ""
    
    # Risk management
    position_size: float = 0.0
    stop_loss: float = 0.0
    profit_target: float = 0.0
    
    # Market context
    market_condition: MarketCondition = MarketCondition.UNCERTAIN
    volatility_environment: str = ""

class IronCondorStrategy:
    """
    Iron Condor Strategy Handler for options trading signals
    """
    
    def __init__(self, config: IronCondorConfig = None):
        """
        Initialize the Iron Condor strategy handler.
        
        Args:
            config: Configuration for strategy parameters
        """
        self.config = config or IronCondorConfig()
        self.historical_handler = HistoricalDataHandler()
        self.options_handler = OptionsDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        
        self.logger.info("IronCondorStrategy initialized with configuration:")
        self.logger.info(f"  Target DTE: {self.config.min_dte}-{self.config.max_dte} days")
        self.logger.info(f"  Short strikes delta: ±{self.config.short_call_delta}")
        self.logger.info(f"  Wing width: ${self.config.wing_width}")
        self.logger.info(f"  Min credit: ${self.config.min_credit}")

    def analyze_market_condition(self, symbol: str, lookback_days: int = None) -> Dict[str, Any]:
        """
        Analyze current market condition for Iron Condor suitability.
        
        Args:
            symbol: Stock symbol to analyze
            lookback_days: Days to look back for analysis
            
        Returns:
            Dictionary with market condition analysis
        """
        if lookback_days is None:
            lookback_days = self.config.volatility_lookback
            
        try:
            # Get historical data
            historical_data = self.historical_handler.get_historical_data(
                symbol=symbol,
                periodType='month',
                period=2,
                frequencyType='daily',
                freq=1
            )
            
            if not historical_data or 'candles' not in historical_data:
                return self._create_empty_market_analysis(symbol)
            
            df = pd.DataFrame(historical_data['candles'])
            if len(df) < lookback_days:
                return self._create_empty_market_analysis(symbol)
            
            # Calculate various metrics
            current_price = df['close'].iloc[-1]
            
            # Volatility analysis
            returns = df['close'].pct_change().dropna()
            realized_vol = returns.std() * np.sqrt(252) * 100  # Annualized %
            
            # Trend analysis
            sma_20 = df['close'].rolling(20).mean().iloc[-1]
            sma_50 = df['close'].rolling(50).mean().iloc[-1] if len(df) >= 50 else sma_20
            
            # Prevent division by zero
            if sma_20 > 0:
                trend_strength = abs(current_price - sma_20) / sma_20
            else:
                trend_strength = 0.0
            trend_direction = 1 if current_price > sma_20 else -1
            
            # Range analysis
            recent_high = df['high'].tail(lookback_days).max()
            recent_low = df['low'].tail(lookback_days).min()
            range_pct = (recent_high - recent_low) / current_price
            
            # Determine market condition
            market_condition = self._determine_market_condition(
                trend_strength, realized_vol, range_pct
            )
            
            # IV Rank calculation (simplified - would need options data)
            iv_rank = self._calculate_iv_rank(symbol, realized_vol)
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'market_condition': market_condition,
                'trend_strength': trend_strength,
                'trend_direction': trend_direction,
                'realized_volatility': realized_vol,
                'iv_rank': iv_rank,
                'range_pct': range_pct,
                'recent_high': recent_high,
                'recent_low': recent_low,
                'sma_20': sma_20,
                'sma_50': sma_50,
                'is_iron_condor_suitable': self._is_suitable_for_iron_condor(
                    market_condition, trend_strength, iv_rank
                )
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing market condition for {symbol}: {e}")
            return self._create_empty_market_analysis(symbol)

    def _determine_market_condition(self, trend_strength: float, volatility: float, 
                                  range_pct: float) -> MarketCondition:
        """Determine the current market condition."""
        if trend_strength > self.config.max_trend_strength:
            return MarketCondition.TRENDING_UP if trend_strength > 0 else MarketCondition.TRENDING_DOWN
        elif volatility > 25:  # High volatility threshold
            return MarketCondition.HIGH_VOLATILITY
        elif volatility < 15:  # Low volatility threshold
            return MarketCondition.LOW_VOLATILITY
        elif range_pct < 0.15:  # Range-bound threshold
            return MarketCondition.RANGE_BOUND
        else:
            return MarketCondition.UNCERTAIN

    def _calculate_iv_rank(self, symbol: str, realized_vol: float) -> float:
        """Calculate IV rank (simplified version)."""
        # In a real implementation, this would compare current IV to historical IV
        # For now, using realized volatility as a proxy
        if realized_vol < 15:
            return 25.0  # Low IV environment
        elif realized_vol > 30:
            return 75.0  # High IV environment
        else:
            return 50.0  # Medium IV environment

    def _is_suitable_for_iron_condor(self, market_condition: MarketCondition, 
                                   trend_strength: float, iv_rank: float) -> bool:
        """Check if conditions are suitable for Iron Condor."""
        return (
            market_condition in [MarketCondition.RANGE_BOUND, MarketCondition.LOW_VOLATILITY] and
            trend_strength <= self.config.max_trend_strength and
            self.config.min_iv_rank <= iv_rank <= self.config.max_iv_rank
        )

    def find_iron_condor_setups(self, symbol: str) -> List[IronCondorSetup]:
        """
        Find potential Iron Condor setups for a symbol using comprehensive options data.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            List of potential Iron Condor setups with actual contract details
        """
        try:
            # Get market analysis
            market_analysis = self.analyze_market_condition(symbol)
            
            if not market_analysis['is_iron_condor_suitable']:
                self.logger.info(f"Market conditions not suitable for Iron Condor on {symbol}")
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
                
                # Check if expiration is suitable for Iron Condor
                if not (self.config.min_dte <= dte <= self.config.max_dte):
                    continue
                
                setup = self._create_iron_condor_setup_from_comprehensive_data(
                    symbol, expiration, current_price, market_analysis
                )
                
                if setup and self._validate_setup(setup):
                    setups.append(setup)
            
            # Sort by attractiveness (highest credit/risk ratio)
            setups.sort(key=lambda x: x.net_credit / x.max_loss if x.max_loss > 0 else 0, reverse=True)
            
            return setups[:3]  # Return top 3 setups
            
        except Exception as e:
            self.logger.error(f"Error finding Iron Condor setups for {symbol}: {e}")
            return []

    def _find_suitable_expirations(self, options_chain: Dict) -> List[datetime]:
        """Find option expirations suitable for Iron Condor."""
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

    def _create_iron_condor_setup(self, symbol: str, exp_date: datetime, 
                                current_price: float, options_chain: Dict,
                                market_analysis: Dict) -> Optional[IronCondorSetup]:
        """Create an Iron Condor setup for given parameters."""
        try:
            dte = (exp_date - datetime.now()).days
            exp_str = exp_date.strftime('%Y-%m-%d') + ':0'
            
            if exp_str not in options_chain['callExpDateMap']:
                return None
            
            calls = options_chain['callExpDateMap'][exp_str]
            puts = options_chain['putExpDateMap'][exp_str]
            
            # Find strikes based on delta targets
            short_call_strike = self._find_strike_by_delta(calls, self.config.short_call_delta, 'call')
            short_put_strike = self._find_strike_by_delta(puts, self.config.short_put_delta, 'put')
            
            if not short_call_strike or not short_put_strike:
                return None
            
            # Calculate wing strikes
            long_call_strike = short_call_strike + self.config.wing_width
            long_put_strike = short_put_strike - self.config.wing_width
            
            # Get option prices
            option_prices = self._get_option_prices(
                calls, puts, short_call_strike, short_put_strike,
                long_call_strike, long_put_strike
            )
            
            if not option_prices:
                return None
            
            # Calculate setup metrics
            net_credit = (option_prices['short_call'] + option_prices['short_put'] - 
                         option_prices['long_call'] - option_prices['long_put'])
            
            max_profit = net_credit
            max_loss = self.config.wing_width - net_credit
            
            breakeven_lower = short_put_strike - net_credit
            breakeven_upper = short_call_strike + net_credit
            
            # Calculate probability of profit (simplified)
            prob_profit = self._calculate_prob_profit(
                current_price, breakeven_lower, breakeven_upper, market_analysis['realized_volatility'], dte
            )
            
            return IronCondorSetup(
                symbol=symbol,
                expiration_date=exp_date,
                dte=dte,
                long_put_strike=long_put_strike,
                short_put_strike=short_put_strike,
                short_call_strike=short_call_strike,
                long_call_strike=long_call_strike,
                long_put_price=option_prices['long_put'],
                short_put_price=option_prices['short_put'],
                short_call_price=option_prices['short_call'],
                long_call_price=option_prices['long_call'],
                net_credit=net_credit,
                max_profit=max_profit,
                max_loss=max_loss,
                breakeven_lower=breakeven_lower,
                breakeven_upper=breakeven_upper,
                prob_profit=prob_profit,
                delta=0.0,  # Would calculate from individual option deltas
                gamma=0.0,
                theta=0.0,
                vega=0.0,
                current_price=current_price,
                iv_rank=market_analysis['iv_rank'],
                market_condition=market_analysis['market_condition']
            )
            
        except Exception as e:
            self.logger.error(f"Error creating Iron Condor setup: {e}")
            return None

    def _find_strike_by_delta(self, options: Dict, target_delta: float, option_type: str) -> Optional[float]:
        """Find strike price closest to target delta."""
        best_strike = None
        best_delta_diff = float('inf')
        
        for strike_str, strike_data in options.items():
            try:
                strike = float(strike_str)
                if not strike_data:
                    continue
                
                option_data = strike_data[0]  # First option in the list
                delta = option_data.get('delta', 0)
                
                if option_type == 'put':
                    delta = -abs(delta)  # Puts have negative delta
                
                delta_diff = abs(delta - target_delta)
                if delta_diff < best_delta_diff:
                    best_delta_diff = delta_diff
                    best_strike = strike
                    
            except (ValueError, KeyError, IndexError):
                continue
        
        return best_strike

    def _get_option_prices(self, calls: Dict, puts: Dict, short_call_strike: float,
                          short_put_strike: float, long_call_strike: float,
                          long_put_strike: float) -> Optional[Dict[str, float]]:
        """Get option prices for all legs."""
        try:
            prices = {}
            
            # Get prices for each leg
            strikes_and_types = [
                (str(short_call_strike), calls, 'short_call'),
                (str(short_put_strike), puts, 'short_put'),
                (str(long_call_strike), calls, 'long_call'),
                (str(long_put_strike), puts, 'long_put')
            ]
            
            for strike_str, options_dict, leg_name in strikes_and_types:
                if strike_str in options_dict and options_dict[strike_str]:
                    option_data = options_dict[strike_str][0]
                    # Use mid price (bid + ask) / 2 with None checks
                    bid = option_data.get('bid') or 0
                    ask = option_data.get('ask') or 0
                    prices[leg_name] = (bid + ask) / 2 if ask and bid and ask > bid else bid
                else:
                    return None
            
            return prices
            
        except Exception as e:
            self.logger.error(f"Error getting option prices: {e}")
            return None

    def _calculate_prob_profit(self, current_price: float, lower_be: float, 
                             upper_be: float, volatility: float, dte: int) -> float:
        """Calculate probability of profit (simplified Black-Scholes approach)."""
        try:
            # Simplified calculation - in practice would use more sophisticated models
            time_to_exp = dte / 365.0
            vol_decimal = volatility / 100.0
            
            # Calculate probability that stock stays between breakevens
            # Using normal distribution approximation
            std_dev = vol_decimal * np.sqrt(time_to_exp)
            
            # Z-scores for breakeven levels
            z_lower = (np.log(lower_be / current_price)) / std_dev
            z_upper = (np.log(upper_be / current_price)) / std_dev
            
            from scipy.stats import norm
            prob_profit = norm.cdf(z_upper) - norm.cdf(z_lower)
            
            return max(0.0, min(1.0, prob_profit))
            
        except Exception:
            # Fallback to simple estimate
            range_width = upper_be - lower_be
            price_range = current_price * 0.2  # Assume 20% typical range
            return min(0.8, range_width / price_range)

    def _validate_setup(self, setup: IronCondorSetup) -> bool:
        """Validate if setup meets all criteria."""
        # Prevent division by zero
        if self.config.wing_width <= 0:
            return False
            
        return (
            setup.net_credit >= self.config.min_credit and
            setup.net_credit / self.config.wing_width <= self.config.max_credit_to_width_ratio and
            setup.prob_profit >= self.config.min_prob_profit and
            setup.max_loss > 0
        )

    def generate_trading_signal(self, symbol: str, technical_indicators: Dict[str, Any] = None) -> TradingSignal:
        """
        Generate trading signal for Iron Condor strategy using technical indicators.
        
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
            
            # Extract indicators from technical indicators
            base_indicators = technical_indicators.get('base_indicators', {})
            iron_condor_indicators = technical_indicators.get('iron_condor_indicators', {})
            
            if not iron_condor_indicators:
                return TradingSignal(
                    signal_type=SignalType.NO_SIGNAL,
                    symbol=symbol,
                    timestamp=datetime.now(),
                    confidence=0.0,
                    entry_reason="No Iron Condor indicators available"
                )
            
            # Create market analysis from technical indicators
            market_analysis = {
                'symbol': symbol,
                'current_price': base_indicators.get('current_price', 0),
                'market_condition': self._parse_market_condition_from_indicators(iron_condor_indicators),
                'trend_strength': iron_condor_indicators.get('trend_strength', 0),
                'realized_volatility': iron_condor_indicators.get('realized_vol', 20),
                'iv_rank': iron_condor_indicators.get('iv_rank', 50),
                'range_pct': iron_condor_indicators.get('range_pct', 0.1),
                'recent_high': iron_condor_indicators.get('recent_high', 0),
                'recent_low': iron_condor_indicators.get('recent_low', 0),
                'is_iron_condor_suitable': iron_condor_indicators.get('is_suitable_for_ic', False)
            }
            
            # Determine signal based on technical indicators
            signal_type, confidence, reason = self._determine_signal_from_indicators(
                market_analysis, iron_condor_indicators
            )
            
            # Get actual Iron Condor setups if signal is positive
            best_setup = None
            if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
                setups = self.find_iron_condor_setups(symbol)
                if setups:
                    best_setup = setups[0]  # Use the best setup
                    reason += f" - Found {len(setups)} Iron Condor setups available"
            
            # Create trading signal
            signal = TradingSignal(
                signal_type=signal_type,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=confidence,
                entry_reason=reason,
                setup=best_setup,  # Include actual setup details
                market_condition=market_analysis['market_condition'],
                volatility_environment=self._get_volatility_environment(market_analysis['iv_rank'])
            )
            
            # Add dynamic position sizing and risk management based on setup
            if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
                # Load dynamic risk configuration
                risk_config = load_risk_config()
                
                if best_setup:
                    # Use dynamic position sizing and setup-based risk management
                    signal.position_size = self._calculate_dynamic_position_size(base_indicators, risk_config)
                    
                    # Use dynamic risk settings for Iron Condor
                    stop_loss_multiplier, take_profit_multiplier = self._get_dynamic_risk_settings(risk_config, 'iron_condor')
                    signal.stop_loss = best_setup.net_credit * stop_loss_multiplier
                    signal.profit_target = best_setup.net_credit * take_profit_multiplier
                else:
                    # Fallback to basic position sizing with dynamic risk settings
                    signal.position_size = self._calculate_dynamic_position_size(base_indicators, risk_config)
                    current_price = base_indicators.get('current_price', 0)
                    if current_price > 0:
                        # Use dynamic percentage-based risk settings as fallback
                        stop_loss_pct, take_profit_pct = self._get_dynamic_risk_settings(risk_config, 'iron_condor')
                        # Convert multipliers to percentages for non-setup scenarios
                        signal.stop_loss = current_price * (1 - min(0.1, stop_loss_pct * 0.05))  # Cap at 10%
                        signal.profit_target = current_price * (1 + min(0.05, take_profit_pct * 0.025))  # Cap at 5%
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating Iron Condor trading signal for {symbol}: {e}")
            return TradingSignal(
                signal_type=SignalType.NO_SIGNAL,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=0.0,
                entry_reason=f"Error in analysis: {str(e)}"
            )

    def _determine_signal(self, market_analysis: Dict, setups: List[IronCondorSetup]) -> Tuple[SignalType, float, str]:
        """Determine trading signal based on analysis."""
        if not market_analysis['is_iron_condor_suitable']:
            return SignalType.NO_SIGNAL, 0.0, "Market conditions not suitable for Iron Condor"
        
        if not setups:
            return SignalType.NO_SIGNAL, 0.0, "No suitable Iron Condor setups found"
        
        best_setup = setups[0]
        
        # Calculate confidence based on multiple factors
        confidence_factors = []
        
        # Market condition factor
        if market_analysis['market_condition'] == MarketCondition.RANGE_BOUND:
            confidence_factors.append(0.3)
        elif market_analysis['market_condition'] == MarketCondition.LOW_VOLATILITY:
            confidence_factors.append(0.2)
        
        # IV rank factor
        iv_rank = market_analysis['iv_rank']
        if self.config.min_iv_rank <= iv_rank <= self.config.max_iv_rank:
            iv_factor = 1 - abs(iv_rank - self.config.optimal_iv_rank) / 50
            confidence_factors.append(iv_factor * 0.25)
        
        # Setup quality factor
        credit_ratio = best_setup.net_credit / self.config.wing_width
        prob_factor = best_setup.prob_profit
        confidence_factors.append(credit_ratio * 0.15)
        confidence_factors.append(prob_factor * 0.1)
        
        total_confidence = sum(confidence_factors)
        
        # Determine signal strength
        if total_confidence >= 0.8:
            return SignalType.STRONG_BUY, total_confidence, "Excellent Iron Condor opportunity"
        elif total_confidence >= 0.6:
            return SignalType.BUY, total_confidence, "Good Iron Condor setup"
        elif total_confidence >= 0.4:
            return SignalType.HOLD, total_confidence, "Marginal Iron Condor opportunity"
        else:
            return SignalType.NO_SIGNAL, total_confidence, "Poor Iron Condor conditions"

    def _get_volatility_environment(self, iv_rank: float) -> str:
        """Get volatility environment description."""
        if iv_rank < 25:
            return "Low Volatility"
        elif iv_rank > 75:
            return "High Volatility"
        else:
            return "Medium Volatility"

    def _calculate_position_size(self, setup: IronCondorSetup) -> float:
        """Calculate appropriate position size."""
        # Simplified position sizing based on max loss
        max_risk_per_trade = 1000  # Would be based on account size
        contracts = max_risk_per_trade / (setup.max_loss * 100)  # 100 shares per contract
        return max(1, min(10, int(contracts)))  # Between 1-10 contracts

    def _parse_market_condition_from_indicators(self, iron_condor_indicators: Dict) -> MarketCondition:
        """Parse market condition from iron condor indicators."""
        try:
            if iron_condor_indicators.get('is_range_bound', False):
                return MarketCondition.RANGE_BOUND
            elif iron_condor_indicators.get('is_low_volatility', False):
                return MarketCondition.LOW_VOLATILITY
            elif iron_condor_indicators.get('realized_vol', 20) > 30:
                return MarketCondition.HIGH_VOLATILITY
            else:
                return MarketCondition.UNCERTAIN
        except Exception:
            return MarketCondition.UNCERTAIN

    def _determine_signal_from_indicators(self, market_analysis: Dict, iron_condor_indicators: Dict) -> Tuple[SignalType, float, str]:
        """Determine Iron Condor signal from technical indicators."""
        try:
            if not market_analysis.get('is_iron_condor_suitable', False):
                return SignalType.NO_SIGNAL, 0.0, "Market conditions not suitable for Iron Condor"
            
            # Calculate confidence factors
            confidence_factors = []
            
            # Range-bound market factor
            if iron_condor_indicators.get('is_range_bound', False):
                confidence_factors.append(0.4)
            
            # Low volatility factor
            if iron_condor_indicators.get('is_low_volatility', False):
                confidence_factors.append(0.3)
            
            # IV rank factor
            iv_rank = iron_condor_indicators.get('iv_rank', 50)
            if self.config.min_iv_rank <= iv_rank <= self.config.max_iv_rank:
                iv_factor = 1 - abs(iv_rank - self.config.optimal_iv_rank) / 50
                confidence_factors.append(iv_factor * 0.2)
            
            # Trend strength factor (lower is better for Iron Condor)
            trend_strength = iron_condor_indicators.get('trend_strength', 0.5)
            if trend_strength <= self.config.max_trend_strength:
                trend_factor = (self.config.max_trend_strength - trend_strength) / self.config.max_trend_strength
                confidence_factors.append(trend_factor * 0.1)
            
            total_confidence = sum(confidence_factors)
            
            # Generate signal based on confidence
            if total_confidence >= 0.7:
                return SignalType.STRONG_BUY, total_confidence, "Excellent Iron Condor conditions - range-bound low volatility market"
            elif total_confidence >= 0.5:
                return SignalType.BUY, total_confidence, "Good Iron Condor setup - suitable market conditions"
            elif total_confidence >= 0.3:
                return SignalType.HOLD, total_confidence, "Marginal Iron Condor opportunity"
            else:
                return SignalType.NO_SIGNAL, total_confidence, "Poor Iron Condor conditions"
                
        except Exception as e:
            return SignalType.NO_SIGNAL, 0.0, f"Error in signal determination: {str(e)}"

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
            # Get Iron Condor-specific risk settings
            iron_condor_config = risk_config.get('iron_condor_strategy', {})
            
            # Check if dynamic position sizing is enabled
            if not iron_condor_config.get('use_dynamic_position_sizing', {}).get('enabled', False):
                return self._calculate_basic_position_size(base_indicators)
            
            # Get position sizing parameters
            sizing_params = iron_condor_config.get('position_sizing', {})
            base_size = sizing_params.get('base_position_size', 1.0)
            max_size = sizing_params.get('max_position_size', 3.0)
            min_size = sizing_params.get('min_position_size', 0.5)
            
            # Volatility-based adjustment (Iron Condor prefers lower volatility)
            atr = base_indicators.get('atr', 0)
            current_price = base_indicators.get('current_price', 100)
            
            if current_price > 0 and atr > 0:
                volatility_pct = (atr / current_price) * 100
                
                # Adjust position size based on volatility (opposite of PML - lower vol = larger size)
                if volatility_pct > 4:  # High volatility - smaller position
                    adjusted_size = base_size * 0.3
                elif volatility_pct < 1.5:  # Low volatility - larger position
                    adjusted_size = base_size * 1.5
                else:  # Medium volatility
                    adjusted_size = base_size
                
                # Apply min/max constraints
                final_size = max(min_size, min(max_size, adjusted_size))
                
                print(f"📊 Dynamic Iron Condor position sizing: volatility={volatility_pct:.1f}%, size={final_size:.1f}")
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
            
            # Default values for Iron Condor (different from other strategies)
            default_stop_loss = 2.0  # 200% of credit (typical for Iron Condor)
            default_take_profit = 0.5  # 50% of credit (typical for Iron Condor)
            
            # Check if dynamic risk management is enabled
            if not strategy_config.get('use_dynamic_risk_management', {}).get('enabled', False):
                return default_stop_loss, default_take_profit
            
            # Get stop loss settings
            stop_loss_config = strategy_config.get('stop_loss', {})
            if stop_loss_config.get('enabled', False):
                stop_loss_multiplier = stop_loss_config.get('multiplier', default_stop_loss)
            else:
                stop_loss_multiplier = default_stop_loss
            
            # Get take profit settings
            take_profit_config = strategy_config.get('take_profit', {})
            if take_profit_config.get('enabled', False):
                take_profit_multiplier = take_profit_config.get('multiplier', default_take_profit)
            else:
                take_profit_multiplier = default_take_profit
            
            print(f"📊 Dynamic risk settings for {strategy_name}: SL={stop_loss_multiplier*100:.0f}% of credit, TP={take_profit_multiplier*100:.0f}% of credit")
            return stop_loss_multiplier, take_profit_multiplier
            
        except Exception as e:
            print(f"❌ Error getting dynamic risk settings: {e}")
            return default_stop_loss, default_take_profit

    def _create_iron_condor_setup_from_comprehensive_data(self, symbol: str, expiration: Dict, 
                                                        current_price: float, market_analysis: Dict) -> Optional[IronCondorSetup]:
        """Create Iron Condor setup from comprehensive options data."""
        try:
            exp_date_str = expiration['expiration_date']
            dte = expiration['days_to_expiration']
            
            # Parse expiration date
            exp_date = datetime.strptime(exp_date_str.split(':')[0], '%Y-%m-%d')
            
            # Find suitable strikes for Iron Condor
            contracts = expiration.get('contracts', [])
            if not contracts:
                return None
            
            # Separate calls and puts, find strikes near target deltas
            call_contracts = []
            put_contracts = []
            
            for contract in contracts:
                call_data = contract.get('call')
                put_data = contract.get('put')
                strike = contract['strike']
                
                if call_data and call_data.get('delta'):
                    call_contracts.append({
                        'strike': strike,
                        'delta': call_data['delta'],
                        'bid': call_data.get('bid', 0),
                        'ask': call_data.get('ask', 0),
                        'mark': call_data.get('mark', 0),
                        'volume': call_data.get('volume', 0),
                        'open_interest': call_data.get('open_interest', 0)
                    })
                
                if put_data and put_data.get('delta'):
                    put_contracts.append({
                        'strike': strike,
                        'delta': abs(put_data['delta']),  # Make positive for comparison
                        'bid': put_data.get('bid', 0),
                        'ask': put_data.get('ask', 0),
                        'mark': put_data.get('mark', 0),
                        'volume': put_data.get('volume', 0),
                        'open_interest': put_data.get('open_interest', 0)
                    })
            
            # Find short strikes based on delta targets
            short_call = self._find_contract_by_delta(call_contracts, self.config.short_call_delta)
            short_put = self._find_contract_by_delta(put_contracts, abs(self.config.short_put_delta))
            
            if not short_call or not short_put:
                return None
            
            # Find long strikes (wing protection)
            long_call_strike = short_call['strike'] + self.config.wing_width
            long_put_strike = short_put['strike'] - self.config.wing_width
            
            # Find long contracts
            long_call = self._find_contract_by_strike(call_contracts, long_call_strike)
            long_put = self._find_contract_by_strike(put_contracts, long_put_strike)
            
            if not long_call or not long_put:
                return None
            
            # Calculate prices (use mid prices) with None checks
            short_call_bid = short_call.get('bid') or 0
            short_call_ask = short_call.get('ask') or 0
            short_call_price = (short_call_bid + short_call_ask) / 2 if short_call_ask and short_call_bid and short_call_ask > short_call_bid else short_call_bid
            
            short_put_bid = short_put.get('bid') or 0
            short_put_ask = short_put.get('ask') or 0
            short_put_price = (short_put_bid + short_put_ask) / 2 if short_put_ask and short_put_bid and short_put_ask > short_put_bid else short_put_bid
            
            long_call_bid = long_call.get('bid') or 0
            long_call_ask = long_call.get('ask') or 0
            long_call_price = (long_call_bid + long_call_ask) / 2 if long_call_ask and long_call_bid and long_call_ask > long_call_bid else long_call_bid
            
            long_put_bid = long_put.get('bid') or 0
            long_put_ask = long_put.get('ask') or 0
            long_put_price = (long_put_bid + long_put_ask) / 2 if long_put_ask and long_put_bid and long_put_ask > long_put_bid else long_put_bid
            
            # Calculate Iron Condor metrics
            net_credit = short_call_price + short_put_price - long_call_price - long_put_price
            max_profit = net_credit
            max_loss = self.config.wing_width - net_credit
            
            breakeven_lower = short_put['strike'] - net_credit
            breakeven_upper = short_call['strike'] + net_credit
            
            # Calculate probability of profit
            prob_profit = self._calculate_prob_profit(
                current_price, breakeven_lower, breakeven_upper, 
                market_analysis['realized_volatility'], dte
            )
            
            return IronCondorSetup(
                symbol=symbol,
                expiration_date=exp_date,
                dte=dte,
                long_put_strike=long_put['strike'],
                short_put_strike=short_put['strike'],
                short_call_strike=short_call['strike'],
                long_call_strike=long_call['strike'],
                long_put_price=long_put_price,
                short_put_price=short_put_price,
                short_call_price=short_call_price,
                long_call_price=long_call_price,
                net_credit=net_credit,
                max_profit=max_profit,
                max_loss=max_loss,
                breakeven_lower=breakeven_lower,
                breakeven_upper=breakeven_upper,
                prob_profit=prob_profit,
                delta=0.0,  # Would calculate net delta
                gamma=0.0,
                theta=0.0,
                vega=0.0,
                current_price=current_price,
                iv_rank=market_analysis['iv_rank'],
                market_condition=market_analysis['market_condition']
            )
            
        except Exception as e:
            self.logger.error(f"Error creating Iron Condor setup from comprehensive data: {e}")
            return None

    def _find_contract_by_delta(self, contracts: List[Dict], target_delta: float) -> Optional[Dict]:
        """Find contract closest to target delta."""
        if not contracts:
            return None
        
        best_contract = None
        best_delta_diff = float('inf')
        
        for contract in contracts:
            delta = contract.get('delta', 0)
            delta_diff = abs(delta - target_delta)
            
            if delta_diff < best_delta_diff:
                best_delta_diff = delta_diff
                best_contract = contract
        
        return best_contract

    def _find_contract_by_strike(self, contracts: List[Dict], target_strike: float) -> Optional[Dict]:
        """Find contract closest to target strike."""
        if not contracts:
            return None
        
        best_contract = None
        best_strike_diff = float('inf')
        
        for contract in contracts:
            strike = contract.get('strike', 0)
            strike_diff = abs(strike - target_strike)
            
            if strike_diff < best_strike_diff:
                best_strike_diff = strike_diff
                best_contract = contract
        
        return best_contract

    def _create_empty_market_analysis(self, symbol: str) -> Dict[str, Any]:
        """Create empty market analysis for error cases."""
        return {
            'symbol': symbol,
            'current_price': 0.0,
            'market_condition': MarketCondition.UNCERTAIN,
            'trend_strength': 0.0,
            'trend_direction': 0,
            'realized_volatility': 0.0,
            'iv_rank': 0.0,
            'range_pct': 0.0,
            'recent_high': 0.0,
            'recent_low': 0.0,
            'sma_20': 0.0,
            'sma_50': 0.0,
            'is_iron_condor_suitable': False
        }

    def get_strategy_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get comprehensive strategy summary for a symbol.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Dictionary with complete strategy analysis
        """
        market_analysis = self.analyze_market_condition(symbol)
        setups = self.find_iron_condor_setups(symbol)
        signal = self.generate_trading_signal(symbol)
        
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'market_analysis': market_analysis,
            'potential_setups': [
                {
                    'expiration': setup.expiration_date.isoformat(),
                    'dte': setup.dte,
                    'strikes': f"{setup.long_put_strike}/{setup.short_put_strike}/{setup.short_call_strike}/{setup.long_call_strike}",
                    'net_credit': setup.net_credit,
                    'max_profit': setup.max_profit,
                    'max_loss': setup.max_loss,
                    'prob_profit': setup.prob_profit,
                    'breakevens': f"{setup.breakeven_lower:.2f} - {setup.breakeven_upper:.2f}"
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
            return ['SPY', 'QQQ', 'IWM']  # Fallback symbols
        
        with open(live_monitor_path, 'r') as f:
            data = json.load(f)
        
        # Extract symbols from integrated_watchlist
        watchlist_symbols = data.get('integrated_watchlist', {}).get('symbols', [])
        
        if not watchlist_symbols:
            # Fallback to symbols_monitored in metadata
            watchlist_symbols = data.get('metadata', {}).get('symbols_monitored', [])
        
        if not watchlist_symbols:
            print("Warning: No symbols found in live_monitor.json, using fallback")
            return ['SPY', 'QQQ', 'IWM']
        
        print(f"Loaded {len(watchlist_symbols)} symbols from live_monitor.json")
        return watchlist_symbols
        
    except Exception as e:
        print(f"Error loading watchlist from live_monitor.json: {e}")
        return ['SPY', 'QQQ', 'IWM']  # Fallback symbols

def analyze_symbol_iron_condor(symbol: str, strategy: IronCondorStrategy) -> Tuple[str, Dict[str, Any]]:
    """Analyze a single symbol for Iron Condor signals (for parallel processing)"""
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
                iron_condor_controls = strategy_controls.get('iron_condor', {})
                auto_approve = iron_condor_controls.get('auto_approve', True)
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
                'expiration_date': signal.setup.expiration_date.isoformat(),
                'dte': signal.setup.dte,
                'strikes': {
                    'long_put': signal.setup.long_put_strike,
                    'short_put': signal.setup.short_put_strike,
                    'short_call': signal.setup.short_call_strike,
                    'long_call': signal.setup.long_call_strike
                },
                'net_credit': signal.setup.net_credit,
                'max_profit': signal.setup.max_profit,
                'max_loss': signal.setup.max_loss,
                'breakeven_lower': signal.setup.breakeven_lower,
                'breakeven_upper': signal.setup.breakeven_upper,
                'prob_profit': signal.setup.prob_profit,
                'iv_rank': signal.setup.iv_rank
            }
        
        # Print summary using data from technical indicators
        all_indicators = load_technical_indicators()
        if symbol in all_indicators:
            base_indicators = all_indicators[symbol].get('base_indicators', {})
            iron_condor_indicators = all_indicators[symbol].get('iron_condor_indicators', {})
            current_price = base_indicators.get('current_price', 0)
            market_condition = iron_condor_indicators.get('market_condition', 'UNCERTAIN')
            iv_rank = iron_condor_indicators.get('iv_rank', 50)
            print(f"  {symbol}: ${current_price:.2f} | {market_condition} | IV: {iv_rank:.1f} | {signal.signal_type.value}")
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

def run_iron_condor_analysis_for_watchlist() -> Dict[str, Any]:
    """Run iron condor analysis for all symbols in the watchlist using parallel processing"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    strategy = IronCondorStrategy()
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print("Iron Condor Strategy Analysis (Parallel Processing)")
    print("=" * 50)
    print(f"Processing {len(watchlist_symbols)} symbols concurrently...")
    
    start_time = time.time()
    iron_condor_signals = {}
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(6, len(watchlist_symbols))  # Limit to 6 concurrent threads to avoid API rate limits
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks
        future_to_symbol = {
            executor.submit(analyze_symbol_iron_condor, symbol, strategy): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, signal_data = future.result(timeout=30)  # 30 second timeout per symbol
                iron_condor_signals[symbol] = signal_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  Error processing {symbol}: {e}")
                iron_condor_signals[symbol] = {
                    'signal_type': 'NO_SIGNAL',
                    'confidence': 0.0,
                    'entry_reason': f'Processing error: {str(e)}',
                    'timestamp': datetime.now().isoformat(),
                    'market_condition': 'UNCERTAIN',
                    'volatility_environment': 'Unknown',
                    'position_size': 0,
                    'stop_loss': 0,
                    'profit_target': 0,
                    'auto_approve': True  # Default to auto-approve
                }
    
    elapsed_time = time.time() - start_time
    print(f"\n✅ Iron Condor Analysis completed in {elapsed_time:.2f} seconds")
    print(f"📊 Processed {len(iron_condor_signals)} symbols with {max_workers} concurrent threads")
    
    return iron_condor_signals

def save_iron_condor_signals_to_file(iron_condor_signals: Dict[str, Any]) -> bool:
    """Save Iron Condor signals to dedicated iron_condor_signals.json file"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        iron_condor_signals_path = os.path.join(script_dir, 'iron_condor_signals.json')
        
        # Create comprehensive Iron Condor signals data
        iron_condor_data = {
            'strategy_name': 'Iron_Condor_Strategy',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(iron_condor_signals),
            'signals_generated': len([s for s in iron_condor_signals.values() if s['signal_type'] in ['BUY', 'STRONG_BUY', 'HOLD']]),
            'analysis_summary': {
                'strong_buy': len([s for s in iron_condor_signals.values() if s['signal_type'] == 'STRONG_BUY']),
                'buy': len([s for s in iron_condor_signals.values() if s['signal_type'] == 'BUY']),
                'hold': len([s for s in iron_condor_signals.values() if s['signal_type'] == 'HOLD']),
                'no_signal': len([s for s in iron_condor_signals.values() if s['signal_type'] == 'NO_SIGNAL'])
            },
            'signals': iron_condor_signals,
            'metadata': {
                'strategy_type': 'options_income_strategy',
                'signal_types': ['STRONG_BUY', 'BUY', 'HOLD', 'NO_SIGNAL'],
                'analysis_method': 'range_bound_low_volatility_detection',
                'update_frequency': 'on_demand',
                'data_source': 'schwab_options_chain',
                'optimal_conditions': 'range_bound_markets_with_medium_iv'
            }
        }
        
        # Write to dedicated Iron Condor signals file
        with open(iron_condor_signals_path, 'w') as f:
            json.dump(iron_condor_data, f, indent=2)
        
        print(f"✅ Saved Iron Condor signals to {iron_condor_signals_path}")
        print(f"📊 Analysis: {iron_condor_data['analysis_summary']['strong_buy']} STRONG_BUY, {iron_condor_data['analysis_summary']['buy']} BUY, {iron_condor_data['analysis_summary']['hold']} HOLD")
        return True
        
    except Exception as e:
        print(f"❌ Error saving Iron Condor signals to file: {e}")
        return False

def update_existing_iron_condor_signals_auto_approve():
    """Update existing Iron Condor signals with current auto_approve status from centralized config"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        iron_condor_signals_path = os.path.join(script_dir, 'iron_condor_signals.json')
        
        # Check if Iron Condor signals file exists
        if not os.path.exists(iron_condor_signals_path):
            print(f"Iron Condor signals file not found at {iron_condor_signals_path}")
            return False
        
        # Read current auto_approve status from centralized config
        auto_approve = True  # Default
        try:
            config_file = os.path.join(script_dir, 'auto_approve_config.json')
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    auto_config = json.load(f)
                strategy_controls = auto_config.get('strategy_controls', {})
                iron_condor_controls = strategy_controls.get('iron_condor', {})
                auto_approve = iron_condor_controls.get('auto_approve', True)
                print(f"📖 Read auto_approve status for Iron Condor: {auto_approve}")
        except Exception as e:
            print(f"Warning: Could not read auto_approve config: {e}")
        
        # Load existing Iron Condor signals
        with open(iron_condor_signals_path, 'r') as f:
            iron_condor_data = json.load(f)
        
        # Update auto_approve for all signals
        signals_updated = 0
        if 'signals' in iron_condor_data:
            for symbol, signal_data in iron_condor_data['signals'].items():
                if isinstance(signal_data, dict):
                    old_auto_approve = signal_data.get('auto_approve', None)
                    signal_data['auto_approve'] = auto_approve
                    if old_auto_approve != auto_approve:
                        signals_updated += 1
        
        # Update metadata
        if 'metadata' not in iron_condor_data:
            iron_condor_data['metadata'] = {}
        iron_condor_data['metadata']['auto_approve_last_updated'] = datetime.now().isoformat()
        iron_condor_data['metadata']['auto_approve_status'] = auto_approve
        
        # Save updated Iron Condor signals
        with open(iron_condor_signals_path, 'w') as f:
            json.dump(iron_condor_data, f, indent=2)
        
        print(f"✅ Updated {signals_updated} Iron Condor signals with auto_approve: {auto_approve}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating Iron Condor signals auto_approve: {e}")
        return False

def monitor_auto_approve_config():
    """Monitor auto_approve_config.json for changes and update signals in real-time"""
    import time
    import os
    
    config_file = 'auto_approve_config.json'
    last_modified = 0
    
    print("🔍 Starting real-time auto_approve config monitoring for Iron Condor strategy...")
    
    while True:
        try:
            if os.path.exists(config_file):
                current_modified = os.path.getmtime(config_file)
                
                # Check if file has been modified
                if current_modified > last_modified:
                    last_modified = current_modified
                    print(f"📋 Config file updated, refreshing Iron Condor signals auto_approve status...")
                    
                    # Update existing signals with new auto_approve status
                    update_existing_iron_condor_signals_auto_approve()
                    
                    print(f"✅ Iron Condor signals updated at {datetime.now().strftime('%H:%M:%S')}")
            
            # Check every 2 seconds for config changes
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n🛑 Stopping Iron Condor auto_approve config monitoring...")
            break
        except Exception as e:
            print(f"❌ Error in Iron Condor config monitoring: {e}")
            time.sleep(5)  # Wait longer on error


def main():
    """Main function to run iron condor strategy analysis"""
    import sys
    
    # Check if we should run in monitoring mode
    if len(sys.argv) > 1 and sys.argv[1] == '--monitor':
        monitor_auto_approve_config()
        return
    
    print("🔄 Starting Iron Condor Strategy Analysis...")
    
    # First, update existing signals with current auto_approve status
    print("📋 Updating existing Iron Condor signals with current auto_approve status...")
    update_existing_iron_condor_signals_auto_approve()
    
    # Run analysis for all watchlist symbols
    iron_condor_signals = run_iron_condor_analysis_for_watchlist()
    
    # Save signals to dedicated Iron Condor file
    save_iron_condor_signals_to_file(iron_condor_signals)
    
    # Update signals again after saving to ensure consistency
    print("🔄 Final update of Iron Condor signals with auto_approve status...")
    update_existing_iron_condor_signals_auto_approve()


if __name__ == "__main__":
    main()
