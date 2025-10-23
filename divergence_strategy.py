#!/usr/bin/env python3
"""
Divergence Strategy Signal Generator

This handler provides divergence-based trading signals for stocks and futures by consuming
pre-calculated technical indicators from technical_indicators.json:

1. RSI divergence signal generation (bullish/bearish)
2. Swing point analysis from technical indicators
3. Entry and exit signal generation with risk management
4. Position sizing recommendations

Divergence Strategy Overview:
- Consume divergence indicators from technical_indicators.json
- Generate buy/sell signals based on detected divergences
- Provide risk management levels (stop loss, take profit)
- Best for trending markets with clear swing patterns
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import json

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
    BULLISH_DIVERGENCE = "BULLISH_DIVERGENCE"
    BEARISH_DIVERGENCE = "BEARISH_DIVERGENCE"
    HIDDEN_BULLISH = "HIDDEN_BULLISH"
    HIDDEN_BEARISH = "HIDDEN_BEARISH"
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGE_BOUND = "RANGE_BOUND"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    UNCERTAIN = "UNCERTAIN"

class DivergenceType(Enum):
    """Divergence strength types"""
    STRONG = "STRONG"
    MEDIUM = "MEDIUM"
    WEAK = "WEAK"
    HIDDEN = "HIDDEN"

@dataclass
class DivergenceConfig:
    """Configuration for Divergence strategy"""
    # Timeframe settings
    primary_timeframe: str = "15min"  # Primary timeframe for signals
    confirmation_timeframe: str = "1min"  # Confirmation timeframe
    
    # Swing detection parameters
    swing_lookback: int = 5  # Lookback period for swing detection
    min_swing_percent: float = 0.1  # Minimum swing percentage
    divergence_threshold: float = 0.03  # Divergence detection threshold
    
    # Technical indicator parameters
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    ema_short: int = 9
    ema_medium: int = 21
    ema_long: int = 50
    atr_period: int = 14
    
    # Entry criteria
    min_confidence: float = 0.6  # Minimum confidence for signal
    require_timeframe_confirmation: bool = True  # Require multi-timeframe confirmation
    max_bars_since_signal: int = 5  # Max bars since divergence for valid signal
    
    # Risk management
    stop_loss_atr_mult: float = 1.5  # Stop loss multiplier based on ATR
    risk_reward_ratio: float = 2.0  # Minimum reward:risk ratio
    max_risk_percent: float = 1.0  # Maximum risk percentage per trade
    
    # Position management
    max_positions_per_symbol: int = 1
    position_size_pct: float = 0.02  # Position size as % of account (2% for stocks/futures, will be loaded from risk config)
    
    def __post_init__(self):
        """Load position size from risk configuration after initialization"""
        try:
            risk_config = load_risk_config()
            if risk_config:
                position_sizing = risk_config.get('risk_management', {}).get('position_sizing', {})
                max_position_size = position_sizing.get('max_position_size', 5)  # Default 5%
                self.position_size_pct = max_position_size / 100.0  # Convert to decimal
                print(f"📊 Divergence: Loaded position_size_pct = {self.position_size_pct:.3f} ({max_position_size}%) from risk config")
            else:
                print(f"⚠️ Divergence: Using default position_size_pct = {self.position_size_pct:.3f}")
        except Exception as e:
            print(f"❌ Divergence: Error loading position size from risk config: {e}")
            print(f"📊 Divergence: Using default position_size_pct = {self.position_size_pct:.3f}")

@dataclass
class SwingPoint:
    """Swing point data"""
    timestamp: datetime
    price: float
    rsi: float
    swing_type: str  # 'high' or 'low'
    index: int

@dataclass
class DivergenceSetup:
    """Divergence trading setup details"""
    symbol: str
    timeframe: str
    divergence_type: DivergenceType
    direction: str  # 'bullish' or 'bearish'
    
    # Swing points
    first_swing: SwingPoint
    second_swing: SwingPoint
    
    # Market data
    current_price: float
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Risk metrics
    risk_amount: float
    reward_amount: float
    reward_risk_ratio: float
    
    # Technical context
    rsi_value: float
    macd_value: float
    trend_direction: str
    atr_value: float
    
    # Support/Resistance levels
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    
    # Confirmation data
    timeframe_confirmed: bool = False
    confirmation_timeframes: List[str] = None

@dataclass
class TradingSignal:
    """Trading signal with details"""
    signal_type: SignalType
    symbol: str
    timestamp: datetime
    confidence: float  # 0-1 confidence score
    
    # Signal details
    setup: Optional[DivergenceSetup] = None
    entry_reason: str = ""
    exit_reason: str = ""
    
    # Risk management
    position_size: float = 0.0
    stop_loss: float = 0.0
    profit_target: float = 0.0
    
    # Market context
    market_condition: MarketCondition = MarketCondition.UNCERTAIN
    volatility_environment: str = ""

class DivergenceStrategy:
    """
    Divergence Strategy Signal Generator for stocks and futures trading signals
    """
    
    def __init__(self, config: DivergenceConfig = None):
        """
        Initialize the Divergence strategy signal generator.
        
        Args:
            config: Configuration for strategy parameters
        """
        self.config = config or DivergenceConfig()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        
        self.logger.info("DivergenceStrategy Signal Generator initialized:")
        self.logger.info(f"  Risk/Reward ratio: {self.config.risk_reward_ratio}")
        self.logger.info(f"  Stop loss ATR multiplier: {self.config.stop_loss_atr_mult}")
        self.logger.info("  Data source: technical_indicators.json")

    # All data fetching and calculation methods removed - now consuming from technical_indicators.json

    def generate_trading_signal(self, symbol: str) -> TradingSignal:
        """
        Generate trading signal for Divergence strategy using technical indicators.
        
        Args:
            symbol: Stock/futures symbol to analyze
            
        Returns:
            Trading signal with recommendation
        """
        try:
            # Load technical indicators
            all_indicators = load_technical_indicators()
            if symbol not in all_indicators:
                return self._create_no_signal(symbol, f"No technical indicators available for {symbol}")
            
            technical_indicators = all_indicators[symbol]
            base_indicators = technical_indicators.get('base_indicators', {})
            divergence_indicators = technical_indicators.get('divergence_indicators', {})
            
            if not divergence_indicators:
                return self._create_no_signal(symbol, "No divergence indicators available")
            
            # Determine signal based on divergence indicators
            signal_type, confidence, reason = self._determine_divergence_signal_from_indicators(
                divergence_indicators, base_indicators
            )
            
            # Create trading signal
            signal = TradingSignal(
                signal_type=signal_type,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=confidence,
                entry_reason=reason,
                market_condition=self._determine_market_condition_from_indicators(divergence_indicators),
                volatility_environment=self._get_volatility_environment_from_indicators(base_indicators)
            )
            
            # Add dynamic position sizing and risk management for actionable signals
            if signal_type in [SignalType.BUY, SignalType.STRONG_BUY, SignalType.SELL, SignalType.STRONG_SELL]:
                # Load dynamic risk configuration
                risk_config = load_risk_config()
                
                # Calculate position size using dynamic config
                signal.position_size = self._calculate_dynamic_position_size(base_indicators, risk_config)
                
                current_price = base_indicators.get('current_price', 0)
                atr = base_indicators.get('atr', current_price * 0.01)
                
                if current_price > 0:
                    # Use dynamic risk settings for Divergence
                    stop_loss_mult, take_profit_mult = self._get_dynamic_risk_settings(risk_config, 'divergence')
                    
                    if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
                        signal.stop_loss = current_price - (atr * stop_loss_mult)
                        signal.profit_target = current_price + (atr * take_profit_mult)
                    else:  # SELL signals
                        signal.stop_loss = current_price + (atr * stop_loss_mult)
                        signal.profit_target = current_price - (atr * take_profit_mult)
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating divergence trading signal for {symbol}: {e}")
            return self._create_no_signal(symbol, f"Error in analysis: {str(e)}")

    def _determine_divergence_signal(self, symbol: str, primary_divergences: Dict, 
                                   confirmed_signals: Dict, indicators: Dict) -> Tuple[SignalType, float, str, Optional[DivergenceSetup]]:
        """Determine trading signal based on divergence analysis."""
        try:
            # Check for confirmed multi-timeframe signals first (highest priority)
            if confirmed_signals:
                for signal_key, signal_data in confirmed_signals.items():
                    if "bullish" in signal_key and "strong" in signal_key:
                        return SignalType.STRONG_BUY, 0.9, "Strong bullish divergence confirmed across multiple timeframes", None
                    elif "bearish" in signal_key and "strong" in signal_key:
                        return SignalType.STRONG_SELL, 0.9, "Strong bearish divergence confirmed across multiple timeframes", None
                    elif "bullish" in signal_key:
                        return SignalType.BUY, 0.8, "Bullish divergence confirmed across multiple timeframes", None
                    elif "bearish" in signal_key:
                        return SignalType.SELL, 0.8, "Bearish divergence confirmed across multiple timeframes", None
            
            # Check for single timeframe signals
            trade_signals = primary_divergences.get("trade_signals", {})
            if trade_signals:
                for signal_key, signal_data in trade_signals.items():
                    if "strong" in signal_key:
                        if "bullish" in signal_key:
                            return SignalType.BUY, 0.7, "Strong bullish RSI divergence detected", None
                        elif "bearish" in signal_key:
                            return SignalType.SELL, 0.7, "Strong bearish RSI divergence detected", None
            
            # Check for any divergences without trade signals
            rsi_bull = primary_divergences.get("rsi_divergences", {}).get("bullish", {})
            rsi_bear = primary_divergences.get("rsi_divergences", {}).get("bearish", {})
            
            if rsi_bull.get("medium", False) or rsi_bull.get("weak", False):
                return SignalType.HOLD, 0.5, "Weak bullish divergence detected - monitor for confirmation", None
            elif rsi_bear.get("medium", False) or rsi_bear.get("weak", False):
                return SignalType.HOLD, 0.5, "Weak bearish divergence detected - monitor for confirmation", None
            
            return SignalType.NO_SIGNAL, 0.0, "No significant divergences detected"
            
        except Exception as e:
            self.logger.error(f"Error determining divergence signal: {e}")
            return SignalType.NO_SIGNAL, 0.0, f"Error in signal determination: {str(e)}", None

    # Unused methods removed - strategy now only consumes from technical_indicators.json

    def _create_no_signal(self, symbol: str, reason: str) -> TradingSignal:
        """Create a NO_SIGNAL trading signal."""
        return TradingSignal(
            signal_type=SignalType.NO_SIGNAL,
            symbol=symbol,
            timestamp=datetime.now(),
            confidence=0.0,
            entry_reason=reason
        )

    def _determine_divergence_signal_from_indicators(self, divergence_indicators: Dict, base_indicators: Dict) -> Tuple[SignalType, float, str]:
        """Determine divergence signal from technical indicators."""
        try:
            # Check for strong divergence signals
            if divergence_indicators.get('bullish_divergence_detected', False):
                confidence = 0.7
                if divergence_indicators.get('divergence_strength', 'medium') == 'strong':
                    return SignalType.STRONG_BUY, 0.9, "Strong bullish divergence detected in technical indicators"
                else:
                    return SignalType.BUY, confidence, "Bullish divergence detected in technical indicators"
            
            if divergence_indicators.get('bearish_divergence_detected', False):
                confidence = 0.7
                if divergence_indicators.get('divergence_strength', 'medium') == 'strong':
                    return SignalType.STRONG_SELL, 0.9, "Strong bearish divergence detected in technical indicators"
                else:
                    return SignalType.SELL, confidence, "Bearish divergence detected in technical indicators"
            
            # Check RSI levels for potential setups
            rsi = base_indicators.get('rsi', 50)
            if rsi < 30:
                return SignalType.HOLD, 0.4, "RSI oversold - potential bullish divergence setup"
            elif rsi > 70:
                return SignalType.HOLD, 0.4, "RSI overbought - potential bearish divergence setup"
            
            return SignalType.NO_SIGNAL, 0.0, "No divergence signals detected"
            
        except Exception as e:
            return SignalType.NO_SIGNAL, 0.0, f"Error in divergence signal determination: {str(e)}"

    def _determine_market_condition_from_indicators(self, divergence_indicators: Dict) -> MarketCondition:
        """Determine market condition from divergence indicators."""
        try:
            if divergence_indicators.get('bullish_divergence_detected', False):
                if divergence_indicators.get('divergence_strength', 'medium') == 'strong':
                    return MarketCondition.BULLISH_DIVERGENCE
                else:
                    return MarketCondition.HIDDEN_BULLISH
            elif divergence_indicators.get('bearish_divergence_detected', False):
                if divergence_indicators.get('divergence_strength', 'medium') == 'strong':
                    return MarketCondition.BEARISH_DIVERGENCE
                else:
                    return MarketCondition.HIDDEN_BEARISH
            
            # Check trend direction
            trend = divergence_indicators.get('trend_direction', 'neutral')
            if trend == 'bullish':
                return MarketCondition.TRENDING_UP
            elif trend == 'bearish':
                return MarketCondition.TRENDING_DOWN
            else:
                return MarketCondition.RANGE_BOUND
                
        except Exception:
            return MarketCondition.UNCERTAIN

    def _get_volatility_environment_from_indicators(self, base_indicators: Dict) -> str:
        """Get volatility environment from base indicators."""
        try:
            atr = base_indicators.get('atr', 0)
            current_price = base_indicators.get('current_price', 100)
            
            if current_price > 0 and atr > 0:
                atr_pct = (atr / current_price) * 100
                if atr_pct > 3.0:
                    return "High Volatility"
                elif atr_pct < 1.0:
                    return "Low Volatility"
                else:
                    return "Medium Volatility"
            else:
                return "Unknown Volatility"
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
            # Get Divergence-specific risk settings
            divergence_config = risk_config.get('divergence_strategy', {})
            
            # Check if dynamic position sizing is enabled
            if not divergence_config.get('use_dynamic_position_sizing', {}).get('enabled', False):
                return self._calculate_basic_position_size(base_indicators)
            
            # Get position sizing parameters
            sizing_params = divergence_config.get('position_sizing', {})
            base_size = sizing_params.get('base_position_size', 2.0)
            max_size = sizing_params.get('max_position_size', 5.0)
            min_size = sizing_params.get('min_position_size', 0.5)
            
            # Volatility-based adjustment (Divergence works well in various volatility environments)
            atr = base_indicators.get('atr', 0)
            current_price = base_indicators.get('current_price', 100)
            
            if current_price > 0 and atr > 0:
                volatility_pct = (atr / current_price) * 100
                
                # Adjust position size based on volatility
                if volatility_pct > 4:  # High volatility - smaller position
                    adjusted_size = base_size * 0.6
                elif volatility_pct < 1:  # Low volatility - larger position
                    adjusted_size = base_size * 1.3
                else:  # Medium volatility
                    adjusted_size = base_size
                
                # Apply min/max constraints
                final_size = max(min_size, min(max_size, adjusted_size))
                
                print(f"📊 Dynamic Divergence position sizing: volatility={volatility_pct:.1f}%, size={final_size:.1f}")
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
            
            # Default values for Divergence (ATR-based)
            default_stop_loss_mult = 1.5  # 1.5x ATR for stop loss
            default_take_profit_mult = 3.0  # 3.0x ATR for take profit (2:1 reward:risk)
            
            # Check if dynamic risk management is enabled
            if not strategy_config.get('use_dynamic_risk_management', {}).get('enabled', False):
                return default_stop_loss_mult, default_take_profit_mult
            
            # Get stop loss settings
            stop_loss_config = strategy_config.get('stop_loss', {})
            if stop_loss_config.get('enabled', False):
                stop_loss_mult = stop_loss_config.get('atr_multiplier', default_stop_loss_mult)
            else:
                stop_loss_mult = default_stop_loss_mult
            
            # Get take profit settings
            take_profit_config = strategy_config.get('take_profit', {})
            if take_profit_config.get('enabled', False):
                reward_ratio = take_profit_config.get('reward_ratio', 2.0)  # Default 2:1 reward:risk
                take_profit_mult = stop_loss_mult * reward_ratio
            else:
                take_profit_mult = default_take_profit_mult
            
            print(f"📊 Dynamic risk settings for {strategy_name}: SL={stop_loss_mult:.1f}x ATR, TP={take_profit_mult:.1f}x ATR")
            return stop_loss_mult, take_profit_mult
            
        except Exception as e:
            print(f"❌ Error getting dynamic risk settings: {e}")
            return default_stop_loss_mult, default_take_profit_mult

    def get_strategy_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get comprehensive strategy summary for a symbol using technical indicators.
        
        Args:
            symbol: Stock/futures symbol to analyze
            
        Returns:
            Dictionary with complete strategy analysis
        """
        try:
            # Load technical indicators
            all_indicators = load_technical_indicators()
            if symbol not in all_indicators:
                return {"error": "No technical indicators available"}
            
            technical_indicators = all_indicators[symbol]
            base_indicators = technical_indicators.get('base_indicators', {})
            divergence_indicators = technical_indicators.get('divergence_indicators', {})
            
            # Generate trading signal
            signal = self.generate_trading_signal(symbol)
            
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'timeframe': self.config.primary_timeframe,
                'current_price': base_indicators.get('current_price', 0),
                'rsi': base_indicators.get('rsi', 50),
                'trend': divergence_indicators.get('trend_direction', 'neutral'),
                'divergence_analysis': {
                    'bullish_divergence_detected': divergence_indicators.get('bullish_divergence_detected', False),
                    'bearish_divergence_detected': divergence_indicators.get('bearish_divergence_detected', False),
                    'divergence_strength': divergence_indicators.get('divergence_strength', 'none')
                },
                'trading_signal': {
                    'signal': signal.signal_type.value,
                    'confidence': signal.confidence,
                    'reason': signal.entry_reason,
                    'position_size': signal.position_size,
                    'market_condition': signal.market_condition.value,
                    'volatility_environment': signal.volatility_environment
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting strategy summary for {symbol}: {e}")
            return {"error": str(e)}


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
            return ['SPY', 'QQQ', '/ES', '/NQ']  # Fallback symbols
        
        with open(live_monitor_path, 'r') as f:
            data = json.load(f)
        
        # Extract symbols from integrated_watchlist
        watchlist_symbols = data.get('integrated_watchlist', {}).get('symbols', [])
        
        print(f"Loaded {len(watchlist_symbols)} symbols from live_monitor.json")
        return watchlist_symbols
        
    except Exception as e:
        print(f"Error loading watchlist from live_monitor.json: {e}")
        return ['SPY', 'QQQ', '/ES', '/NQ']  # Fallback symbols

def analyze_symbol_divergence(symbol: str, strategy: DivergenceStrategy) -> Tuple[str, Dict[str, Any]]:
    """Analyze a single symbol for Divergence signals (for parallel processing)"""
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
                divergence_controls = strategy_controls.get('divergence', {})
                auto_approve = divergence_controls.get('auto_approve', True)
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
                'timeframe': signal.setup.timeframe,
                'divergence_type': signal.setup.divergence_type.value,
                'direction': signal.setup.direction,
                'entry_price': signal.setup.entry_price,
                'stop_loss': signal.setup.stop_loss,
                'take_profit': signal.setup.take_profit,
                'reward_risk_ratio': signal.setup.reward_risk_ratio,
                'rsi_value': signal.setup.rsi_value,
                'trend_direction': signal.setup.trend_direction,
                'timeframe_confirmed': signal.setup.timeframe_confirmed
            }
        
        # Print summary using data from technical indicators
        all_indicators = load_technical_indicators()
        if symbol in all_indicators:
            base_indicators = all_indicators[symbol].get('base_indicators', {})
            divergence_indicators = all_indicators[symbol].get('divergence_indicators', {})
            current_price = base_indicators.get('current_price', 0)
            rsi = base_indicators.get('rsi', 50)
            trend = divergence_indicators.get('trend_direction', 'neutral')
            
            # Get divergence info from indicators
            bullish_divs = []
            bearish_divs = []
            if divergence_indicators.get('bullish_divergence_detected', False):
                bullish_divs.append('detected')
            if divergence_indicators.get('bearish_divergence_detected', False):
                bearish_divs.append('detected')
            
            divergence_info = ""
            if bullish_divs:
                divergence_info += f" Bullish: {', '.join(bullish_divs)}"
            if bearish_divs:
                divergence_info += f" Bearish: {', '.join(bearish_divs)}"
            
            print(f"  {symbol}: ${current_price:.2f} | RSI: {rsi:.1f} | {trend} | {signal.signal_type.value}")
            if divergence_info:
                print(f"  {divergence_info}")
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

def run_divergence_analysis_for_watchlist() -> Dict[str, Any]:
    """Run divergence analysis for all symbols in the watchlist using parallel processing"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    strategy = DivergenceStrategy()
    watchlist_symbols = load_watchlist_from_live_monitor()
    
    print("Divergence Strategy Analysis (Parallel Processing)")
    print("=" * 50)
    print(f"Processing {len(watchlist_symbols)} symbols concurrently...")
    
    start_time = time.time()
    divergence_signals = {}
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(6, len(watchlist_symbols))  # Limit to 6 concurrent threads to avoid API rate limits
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all symbol analysis tasks
        future_to_symbol = {
            executor.submit(analyze_symbol_divergence, symbol, strategy): symbol 
            for symbol in watchlist_symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            try:
                symbol, signal_data = future.result(timeout=45)  # 45 second timeout per symbol (divergence needs more time)
                divergence_signals[symbol] = signal_data
            except Exception as e:
                symbol = future_to_symbol[future]
                print(f"  Error processing {symbol}: {e}")
                divergence_signals[symbol] = {
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
    print(f"\n✅ Divergence Analysis completed in {elapsed_time:.2f} seconds")
    print(f"📊 Processed {len(divergence_signals)} symbols with {max_workers} concurrent threads")
    
    return divergence_signals

def save_divergence_signals_to_file(divergence_signals: Dict[str, Any]) -> bool:
    """Save Divergence signals to dedicated divergence_signals.json file"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        divergence_signals_path = os.path.join(script_dir, 'divergence_signals.json')
        
        # Create comprehensive Divergence signals data
        divergence_data = {
            'strategy_name': 'Divergence_Strategy',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(divergence_signals),
            'signals_generated': len([s for s in divergence_signals.values() if s['signal_type'] in ['BUY', 'STRONG_BUY', 'HOLD']]),
            'analysis_summary': {
                'strong_buy': len([s for s in divergence_signals.values() if s['signal_type'] == 'STRONG_BUY']),
                'buy': len([s for s in divergence_signals.values() if s['signal_type'] == 'BUY']),
                'hold': len([s for s in divergence_signals.values() if s['signal_type'] == 'HOLD']),
                'no_signal': len([s for s in divergence_signals.values() if s['signal_type'] == 'NO_SIGNAL'])
            },
            'signals': divergence_signals,
            'metadata': {
                'strategy_type': 'technical_analysis_momentum',
                'signal_types': ['STRONG_BUY', 'BUY', 'HOLD', 'NO_SIGNAL'],
                'analysis_method': 'rsi_price_divergence_detection',
                'update_frequency': 'on_demand',
                'data_source': 'schwab_historical_data',
                'optimal_conditions': 'oversold_bullish_divergence_or_overbought_bearish_divergence'
            }
        }
        
        # Write to dedicated Divergence signals file
        with open(divergence_signals_path, 'w') as f:
            json.dump(divergence_data, f, indent=2)
        
        print(f"✅ Saved Divergence signals to {divergence_signals_path}")
        print(f"📊 Analysis: {divergence_data['analysis_summary']['strong_buy']} STRONG_BUY, {divergence_data['analysis_summary']['buy']} BUY, {divergence_data['analysis_summary']['hold']} HOLD")
        return True
        
    except Exception as e:
        print(f"❌ Error saving Divergence signals to file: {e}")
        return False

def update_existing_divergence_signals_auto_approve():
    """Update existing Divergence signals with current auto_approve status from centralized config"""
    try:
        import json
        import os
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        divergence_signals_path = os.path.join(script_dir, 'divergence_signals.json')
        
        # Check if Divergence signals file exists
        if not os.path.exists(divergence_signals_path):
            print(f"Divergence signals file not found at {divergence_signals_path}")
            return False
        
        # Read current auto_approve status from centralized config
        auto_approve = True  # Default
        try:
            config_file = os.path.join(script_dir, 'auto_approve_config.json')
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    auto_config = json.load(f)
                strategy_controls = auto_config.get('strategy_controls', {})
                divergence_controls = strategy_controls.get('divergence', {})
                auto_approve = divergence_controls.get('auto_approve', True)
                print(f"📖 Read auto_approve status for Divergence: {auto_approve}")
        except Exception as e:
            print(f"Warning: Could not read auto_approve config: {e}")
        
        # Load existing Divergence signals
        with open(divergence_signals_path, 'r') as f:
            divergence_data = json.load(f)
        
        # Update auto_approve for all signals
        signals_updated = 0
        if 'signals' in divergence_data:
            for symbol, signal_data in divergence_data['signals'].items():
                if isinstance(signal_data, dict):
                    old_auto_approve = signal_data.get('auto_approve', None)
                    signal_data['auto_approve'] = auto_approve
                    if old_auto_approve != auto_approve:
                        signals_updated += 1
        
        # Update metadata
        if 'metadata' not in divergence_data:
            divergence_data['metadata'] = {}
        divergence_data['metadata']['auto_approve_last_updated'] = datetime.now().isoformat()
        divergence_data['metadata']['auto_approve_status'] = auto_approve
        
        # Save updated Divergence signals
        with open(divergence_signals_path, 'w') as f:
            json.dump(divergence_data, f, indent=2)
        
        print(f"✅ Updated {signals_updated} Divergence signals with auto_approve: {auto_approve}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating Divergence signals auto_approve: {e}")
        return False

def monitor_auto_approve_config():
    """Monitor auto_approve_config.json for changes and update signals in real-time"""
    import time
    import os
    
    config_file = 'auto_approve_config.json'
    last_modified = 0
    
    print("🔍 Starting real-time auto_approve config monitoring for Divergence strategy...")
    
    while True:
        try:
            if os.path.exists(config_file):
                current_modified = os.path.getmtime(config_file)
                
                # Check if file has been modified
                if current_modified > last_modified:
                    last_modified = current_modified
                    print(f"📋 Config file updated, refreshing Divergence signals auto_approve status...")
                    
                    # Update existing signals with new auto_approve status
                    update_existing_divergence_signals_auto_approve()
                    
                    print(f"✅ Divergence signals updated at {datetime.now().strftime('%H:%M:%S')}")
            
            # Check every 2 seconds for config changes
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n🛑 Stopping Divergence auto_approve config monitoring...")
            break
        except Exception as e:
            print(f"❌ Error in Divergence config monitoring: {e}")
            time.sleep(5)  # Wait longer on error

def insert_divergence_signals_to_db(divergence_signals: Dict[str, Any]) -> bool:
    """Insert divergence signals into PostgreSQL database with truncation"""
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
        print("🗑️  Truncating divergence_signals table...")
        # Use DELETE instead of TRUNCATE to avoid relation locks in parallel execution
        cur.execute("DELETE FROM divergence_signals;")
        
        # Insert new data
        insert_count = 0
        for symbol, signal_data in divergence_signals.items():
            try:
                # Extract setup data if available
                setup_data = signal_data.get('setup', {})
                
                # Prepare insert statement
                insert_sql = """
                INSERT INTO divergence_signals (
                    timestamp, symbol, signal_type, confidence, entry_reason, exit_reason,
                    position_size, stop_loss, profit_target, market_condition, volatility_environment,
                    timeframe, divergence_type, direction, current_price, entry_price, take_profit,
                    first_swing_timestamp, first_swing_price, first_swing_rsi, first_swing_type,
                    second_swing_timestamp, second_swing_price, second_swing_rsi, second_swing_type,
                    risk_amount, reward_amount, reward_risk_ratio, rsi_value, macd_value,
                    trend_direction, atr_value, support_level, resistance_level,
                    timeframe_confirmed, confirmation_timeframes, auto_approve
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    setup_data.get('timeframe', '15min'),
                    setup_data.get('divergence_type', 'MEDIUM'),
                    setup_data.get('direction', 'neutral'),
                    setup_data.get('current_price', 0.0),
                    setup_data.get('entry_price', 0.0),
                    setup_data.get('take_profit', 0.0),
                    setup_data.get('first_swing', {}).get('timestamp') if setup_data.get('first_swing') else None,
                    setup_data.get('first_swing', {}).get('price', 0.0) if setup_data.get('first_swing') else 0.0,
                    setup_data.get('first_swing', {}).get('rsi', 0.0) if setup_data.get('first_swing') else 0.0,
                    setup_data.get('first_swing', {}).get('swing_type', 'unknown') if setup_data.get('first_swing') else 'unknown',
                    setup_data.get('second_swing', {}).get('timestamp') if setup_data.get('second_swing') else None,
                    setup_data.get('second_swing', {}).get('price', 0.0) if setup_data.get('second_swing') else 0.0,
                    setup_data.get('second_swing', {}).get('rsi', 0.0) if setup_data.get('second_swing') else 0.0,
                    setup_data.get('second_swing', {}).get('swing_type', 'unknown') if setup_data.get('second_swing') else 'unknown',
                    setup_data.get('risk_amount', 0.0),
                    setup_data.get('reward_amount', 0.0),
                    setup_data.get('reward_risk_ratio', 0.0),
                    setup_data.get('rsi_value', 50.0),
                    setup_data.get('macd_value', 0.0),
                    setup_data.get('trend_direction', 'neutral'),
                    setup_data.get('atr_value', 0.0),
                    setup_data.get('support_level'),
                    setup_data.get('resistance_level'),
                    setup_data.get('timeframe_confirmed', False),
                    setup_data.get('confirmation_timeframes', []) or None,
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
        
        print(f"✅ Successfully inserted {insert_count} divergence signals into database")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting divergence signals to database: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

# Global throttling variables for database insertion
_last_db_insertion = 0
_db_insertion_interval = 30  # seconds

def main():
    """Main function to run divergence strategy analysis"""
    import sys
    
    # Check if we should run in monitoring mode
    if len(sys.argv) > 1 and sys.argv[1] == '--monitor':
        monitor_auto_approve_config()
        return
    
    print("🔄 Starting Divergence Strategy Analysis...")
    
    # First, update existing signals with current auto_approve status
    print("📋 Updating existing Divergence signals with current auto_approve status...")
    update_existing_divergence_signals_auto_approve()
    
    # Run analysis for all watchlist symbols
    divergence_signals = run_divergence_analysis_for_watchlist()
    
    # Save signals to dedicated Divergence file
    save_divergence_signals_to_file(divergence_signals)
    
    # Update signals again after saving to ensure consistency
    print("🔄 Final update of Divergence signals with auto_approve status...")
    update_existing_divergence_signals_auto_approve()


if __name__ == "__main__":
    main()
