#!/usr/bin/env python3
"""
Divergence Trading Engine

Specialized trading engine for divergence strategy signals that:
1. Monitors divergence_data/divergence_signals_multi_timeframe.json
2. Executes trades automatically when divergence signals meet criteria
3. Integrates with trading_config_live.json for risk management
4. Provides enhanced divergence-specific features

Key Features:
- Multi-timeframe divergence signal monitoring
- Automatic fresh data generation via divergence_indicators_calculator.py
- Enhanced risk management for divergence trades
- Position sizing based on divergence strength
- Specialized logging and notifications for divergence trades

Usage: python3 divergence_trading_engine.py
"""

import json
import os
import time
import threading
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass
from enum import Enum

# Import existing handlers
from order_handler import OrderHandler

class DivergenceSignalType(Enum):
    """Divergence signal types"""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    STRONG_SELL = "STRONG_SELL"
    SELL = "SELL"
    NO_SIGNAL = "NO_SIGNAL"

class OrderStatus(Enum):
    """Order status types"""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"

@dataclass
class DivergenceSignal:
    """Divergence signal data structure"""
    symbol: str
    signal_type: str
    confidence: float
    entry_reason: str
    timestamp: str
    market_condition: str
    volatility_environment: str
    position_size: float
    stop_loss: float
    profit_target: float
    auto_approve: bool
    max_shares: int
    strategy_allocation: float
    confirmation_strength: str
    confirmed_timeframes: List[str]
    timeframe_analysis: Dict[str, Any]

@dataclass
class DivergenceTradeOrder:
    """Divergence trade order data structure"""
    order_id: str
    symbol: str
    signal: DivergenceSignal
    quantity: int
    entry_price: float
    stop_loss: float
    profit_target: float
    status: OrderStatus
    created_at: datetime
    filled_at: Optional[datetime] = None
    filled_price: Optional[float] = None
    notes: str = ""
    divergence_strength: str = "weak"
    confirmed_timeframes: List[str] = None

class DivergenceTradingEngine:
    """
    Specialized trading engine for divergence strategy signals
    """
    
    def __init__(self):
        """Initialize the divergence trading engine"""
        self.running = False
        self.monitor_thread = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('divergence_trading_engine.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize handlers
        self.order_handler = OrderHandler()
        
        # Trading state
        self.active_orders: Dict[str, DivergenceTradeOrder] = {}
        self.executed_trades: List[DivergenceTradeOrder] = []
        self.last_signal_check = {}
        
        # Signal state persistence
        self.processed_signals_file = 'divergence_data/processed_divergence_signals.json'
        self.processed_signals = self._load_processed_signals()
        
        # Configuration
        self.config = self.load_trading_config()
        
        # Divergence-specific settings
        self.signals_file = 'divergence_data/divergence_signals_multi_timeframe.json'
        self.auto_generate_data = True  # Auto-generate fresh divergence data
        self.data_generation_interval = 300  # 5 minutes
        self.last_data_generation = datetime.min
        
        self.logger.info("🎯 Divergence Trading Engine initialized")
        self.logger.info(f"📊 Signals file: {self.signals_file}")
        self.logger.info(f"🔄 Auto-generate data: {self.auto_generate_data}")
        self.logger.info(f"⏰ Monitor interval: {self.config.get('monitor_interval', 30)} seconds")
        self.logger.info(f"📋 Loaded {len(self.processed_signals)} previously processed signals")

    def load_trading_config(self) -> Dict[str, Any]:
        """Load divergence-specific trading configuration"""
        try:
            if not os.path.exists('trading_config_live.json'):
                self.logger.error("❌ trading_config_live.json not found")
                return self._get_default_config()
            
            with open('trading_config_live.json', 'r') as f:
                trading_config = json.load(f)
            
            # Extract divergence strategy configuration
            divergence_config = trading_config.get('strategies', {}).get('divergence', {})
            if not divergence_config:
                self.logger.error("❌ No divergence strategy configuration found")
                return self._get_default_config()
            
            # Extract risk management settings
            risk_mgmt = divergence_config.get('risk_management', {})
            if not risk_mgmt:
                self.logger.error("❌ No risk management configuration found")
                return self._get_default_config()
            
            # Build divergence-specific config
            config = {
                "monitor_interval": 30,  # Check every 30 seconds for divergence signals
                "enable_auto_trading": True,
                "enable_risk_management": True,
                "min_confidence_threshold": 0.65,  # Higher threshold for divergence
                
                # Divergence strategy settings from trading config
                "strategy_allocation": risk_mgmt.get('strategy_allocation', 20),
                "position_size": risk_mgmt.get('position_size', 15),
                "max_shares": risk_mgmt.get('max_shares', 1000),
                "auto_approve": divergence_config.get('auto_approve', False),
                "watchlist_symbols": divergence_config.get('divergence_strategy_watchlist', []),
                
                # Risk limits
                "risk_limits": {
                    "max_daily_loss_pct": 3.0,  # Conservative for divergence
                    "max_position_size_pct": 2.0,  # Smaller positions for divergence
                    "max_account_risk_pct": 15.0,  # Conservative account risk
                    "equity_buffer": 5000.0
                },
                
                # Divergence-specific parameters
                "divergence_settings": {
                    "require_multi_timeframe_confirmation": True,
                    "min_confirmation_timeframes": 2,
                    "strong_signal_multiplier": 1.5,  # Increase position size for strong signals
                    "weak_signal_multiplier": 0.7,   # Decrease position size for weak signals
                    "max_positions": 5,  # Max divergence positions
                    "stop_loss_atr_multiplier": 1.5,
                    "profit_target_ratio": 2.0
                }
            }
            
            self.logger.info("✅ Loaded divergence trading configuration")
            self.logger.info(f"   Strategy allocation: {config['strategy_allocation']}%")
            self.logger.info(f"   Position size: {config['position_size']}%")
            self.logger.info(f"   Max shares: {config['max_shares']}")
            self.logger.info(f"   Auto approve: {config['auto_approve']}")
            self.logger.info(f"   Watchlist symbols: {len(config['watchlist_symbols'])}")
            
            return config
            
        except Exception as e:
            self.logger.error(f"❌ Error loading trading config: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration when config loading fails"""
        return {
            "monitor_interval": 60,
            "enable_auto_trading": False,
            "enable_risk_management": True,
            "min_confidence_threshold": 0.75,
            "strategy_allocation": 10,
            "position_size": 5,
            "max_shares": 100,
            "auto_approve": False,
            "watchlist_symbols": [],
            "risk_limits": {"max_daily_loss_pct": 1.0, "max_position_size_pct": 1.0},
            "divergence_settings": {"max_positions": 2}
        }

    def _load_processed_signals(self) -> Dict[str, Dict[str, Any]]:
        """Load previously processed divergence signals"""
        try:
            # Ensure divergence_data directory exists
            os.makedirs('divergence_data', exist_ok=True)
            
            if os.path.exists(self.processed_signals_file):
                with open(self.processed_signals_file, 'r') as f:
                    data = json.load(f)
                    
                # Clean up old signals (older than 3 days for divergence)
                cutoff_time = datetime.now() - timedelta(days=3)
                cleaned_signals = {}
                
                for signal_key, signal_info in data.items():
                    try:
                        processed_time = datetime.fromisoformat(signal_info.get('processed_at', ''))
                        if processed_time > cutoff_time:
                            cleaned_signals[signal_key] = signal_info
                    except Exception:
                        continue
                
                if len(cleaned_signals) < len(data):
                    self._save_processed_signals(cleaned_signals)
                    self.logger.info(f"🧹 Cleaned up {len(data) - len(cleaned_signals)} old processed signals")
                
                return cleaned_signals
            else:
                self.logger.info("📝 No processed signals file found - starting fresh")
                return {}
                
        except Exception as e:
            self.logger.error(f"❌ Error loading processed signals: {e}")
            return {}

    def _save_processed_signals(self, signals_dict: Dict[str, Dict[str, Any]] = None):
        """Save processed signals to persistent storage"""
        try:
            if signals_dict is None:
                signals_dict = self.processed_signals
                
            os.makedirs('divergence_data', exist_ok=True)
            with open(self.processed_signals_file, 'w') as f:
                json.dump(signals_dict, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"❌ Error saving processed signals: {e}")

    def start_monitoring(self):
        """Start the divergence signal monitoring"""
        if self.running:
            self.logger.warning("⚠️ Divergence trading engine is already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_divergence_signals, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info("🚀 Divergence Trading Engine started - monitoring for divergence signals")

    def stop_monitoring(self):
        """Stop the divergence signal monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        # Save processed signals on shutdown
        try:
            self._save_processed_signals()
            self.logger.info(f"💾 Saved {len(self.processed_signals)} processed signals on shutdown")
        except Exception as e:
            self.logger.error(f"❌ Error saving processed signals on shutdown: {e}")
        
        self.logger.info("🛑 Divergence Trading Engine stopped")

    def _monitor_divergence_signals(self):
        """Main monitoring loop for divergence signals"""
        self.logger.info("📡 Divergence signal monitoring loop started")
        
        while self.running:
            try:
                # Auto-generate fresh divergence data if needed
                if self.auto_generate_data and self._should_generate_fresh_data():
                    self._generate_fresh_divergence_data()
                
                # Check for divergence signals
                self.logger.info("🔍 Checking divergence signals...")
                self._check_divergence_signals()
                
                # Manage existing orders
                active_orders_count = len(self.active_orders)
                if active_orders_count > 0:
                    self.logger.info(f"📋 Managing {active_orders_count} active divergence orders")
                    self._manage_existing_orders()
                
                # Sleep until next check
                monitor_interval = self.config.get('monitor_interval', 30)
                self.logger.info(f"⏰ Next divergence check in {monitor_interval} seconds...")
                time.sleep(monitor_interval)
                
            except KeyboardInterrupt:
                self.logger.info("⚠️ Received interrupt signal")
                break
            except Exception as e:
                self.logger.error(f"❌ Error in divergence monitoring loop: {e}")
                time.sleep(60)  # Wait longer on error

    def _should_generate_fresh_data(self) -> bool:
        """Check if we should generate fresh divergence data"""
        try:
            time_since_last = datetime.now() - self.last_data_generation
            return time_since_last.total_seconds() > self.data_generation_interval
        except Exception:
            return True

    def _generate_fresh_divergence_data(self) -> bool:
        """Generate fresh divergence indicators using the calculator"""
        try:
            self.logger.info("📊 Auto-generating fresh divergence indicators...")
            
            result = subprocess.run(
                [sys.executable, 'divergence_indicators_calculator.py'],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Successfully generated fresh divergence indicators")
                self.last_data_generation = datetime.now()
                
                # Log key output lines
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if any(keyword in line for keyword in ['ULTRA-PARALLEL', 'completed in', 'Created']):
                        self.logger.info(f"   {line.strip()}")
                return True
            else:
                self.logger.error(f"❌ Divergence calculator failed with return code {result.returncode}")
                if result.stderr:
                    self.logger.error(f"   Error: {result.stderr.strip()}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Divergence calculator timed out after 5 minutes")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error generating fresh divergence data: {e}")
            return False

    def _check_divergence_signals(self):
        """Check for actionable divergence signals"""
        try:
            # Load divergence signals
            signals_data = self._load_divergence_signals()
            if not signals_data:
                return
            
            signals = signals_data.get('signals', {})
            if not signals:
                self.logger.debug("📊 No divergence signals found")
                return
            
            # Check each symbol for actionable signals
            actionable_signals = 0
            for symbol, signal_data in signals.items():
                try:
                    if self._should_execute_divergence_trade(symbol, signal_data):
                        self._execute_divergence_trade(symbol, signal_data)
                        actionable_signals += 1
                except Exception as e:
                    self.logger.error(f"❌ Error processing divergence signal for {symbol}: {e}")
            
            if actionable_signals > 0:
                self.logger.info(f"🎯 Processed {actionable_signals} actionable divergence signals")
            else:
                self.logger.debug("📊 No actionable divergence signals found")
                    
        except Exception as e:
            self.logger.error(f"❌ Error checking divergence signals: {e}")

    def _load_divergence_signals(self) -> Optional[Dict[str, Any]]:
        """Load divergence signals from JSON file"""
        try:
            if not os.path.exists(self.signals_file):
                self.logger.debug(f"📄 Divergence signals file not found: {self.signals_file}")
                return None
            
            with open(self.signals_file, 'r') as f:
                data = json.load(f)
            
            self.logger.debug(f"📊 Loaded divergence signals from {self.signals_file}")
            return data
                
        except Exception as e:
            self.logger.error(f"❌ Error loading divergence signals: {e}")
            return None

    def _should_execute_divergence_trade(self, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Determine if a divergence trade should be executed"""
        try:
            # Check if auto_approve is enabled
            if not signal_data.get('auto_approve', False):
                self.logger.debug(f"🔒 Auto-approve disabled for divergence {symbol}, skipping trade")
                return False
            
            # Check signal type
            signal_type = signal_data.get('signal_type', 'NO_SIGNAL')
            if signal_type not in ['STRONG_BUY', 'BUY', 'STRONG_SELL', 'SELL']:
                self.logger.debug(f"📊 No actionable signal for {symbol}: {signal_type}")
                return False
            
            # Check confidence threshold
            confidence = signal_data.get('confidence', 0.0)
            min_confidence = self.config.get('min_confidence_threshold', 0.65)
            if confidence < min_confidence:
                self.logger.debug(f"📉 {symbol}: Confidence {confidence:.2f} below threshold {min_confidence}")
                return False
            
            # Check multi-timeframe confirmation
            if self.config.get('divergence_settings', {}).get('require_multi_timeframe_confirmation', True):
                if not signal_data.get('multi_timeframe_confirmation', False):
                    self.logger.debug(f"📊 {symbol}: No multi-timeframe confirmation")
                    return False
                
                confirmed_timeframes = signal_data.get('confirmed_timeframes', [])
                min_confirmations = self.config.get('divergence_settings', {}).get('min_confirmation_timeframes', 2)
                if len(confirmed_timeframes) < min_confirmations:
                    self.logger.debug(f"📊 {symbol}: Only {len(confirmed_timeframes)} timeframe confirmations (need {min_confirmations})")
                    return False
            
            # Check if signal is recent
            signal_timestamp = signal_data.get('timestamp', '')
            if not self._is_signal_recent(signal_timestamp, max_age_minutes=30):  # 30 min for divergence
                self.logger.debug(f"⏰ {symbol}: Signal too old ({signal_timestamp})")
                return False
            
            # Check if already processed
            if self._is_divergence_signal_processed(symbol, signal_timestamp):
                self.logger.debug(f"✅ {symbol}: Signal already processed ({signal_timestamp})")
                return False
            
            # Check position limits
            if not self._check_divergence_position_limits(symbol, signal_type):
                return False
            
            # Check risk management
            if not self._check_divergence_risk_limits(symbol, signal_data):
                return False
            
            # Check market status
            if not self._is_market_open():
                self.logger.debug(f"🏪 Market closed, skipping divergence trade: {symbol}")
                return False

            self.logger.info(f"✅ Divergence signal approved for execution: {symbol} {signal_type} (confidence: {confidence:.2f})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking if should execute divergence trade for {symbol}: {e}")
            return False

    def _is_signal_recent(self, timestamp_str: str, max_age_minutes: int = 30) -> bool:
        """Check if divergence signal is recent enough"""
        try:
            if not timestamp_str:
                return False
            
            signal_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            current_time = datetime.now()
            
            # Handle timezone-naive datetime
            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=None)
                current_time = current_time.replace(tzinfo=None)
            
            age_minutes = (current_time - signal_time).total_seconds() / 60
            return age_minutes <= max_age_minutes
            
        except Exception as e:
            self.logger.error(f"❌ Error checking signal age: {e}")
            return False

    def _is_divergence_signal_processed(self, symbol: str, signal_timestamp: str) -> bool:
        """Check if divergence signal has already been processed"""
        try:
            signal_key = f"divergence_{symbol}_{signal_timestamp}"
            
            if signal_key in self.last_signal_check:
                return True
                
            if signal_key in self.processed_signals:
                self.last_signal_check[signal_key] = datetime.now()
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error checking if divergence signal processed: {e}")
            return False

    def _check_divergence_position_limits(self, symbol: str, signal_type: str) -> bool:
        """Check divergence-specific position limits"""
        try:
            # Count current divergence positions
            divergence_positions = len([order for order in self.active_orders.values() 
                                      if order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED]])
            
            max_divergence_positions = self.config.get('divergence_settings', {}).get('max_positions', 5)
            if divergence_positions >= max_divergence_positions:
                self.logger.debug(f"📊 At divergence position limit ({divergence_positions}/{max_divergence_positions})")
                return False
            
            # Check for existing positions in this symbol
            symbol_orders = [order for order in self.active_orders.values() 
                           if order.symbol == symbol and order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED]]
            
            if symbol_orders:
                self.logger.debug(f"📊 Already have divergence position for {symbol}")
                return False
            
            # Check watchlist restriction
            watchlist_symbols = self.config.get('watchlist_symbols', [])
            if watchlist_symbols and symbol not in watchlist_symbols:
                self.logger.debug(f"📋 {symbol} not in divergence watchlist")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking divergence position limits: {e}")
            return False

    def _check_divergence_risk_limits(self, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Check divergence-specific risk limits"""
        try:
            # Get current price and calculate position details
            current_price = self._get_current_price(symbol)
            if current_price <= 0:
                self.logger.warning(f"❌ {symbol}: Could not get valid current price")
                return False
            
            # Calculate position size based on divergence strength
            base_position_size = signal_data.get('position_size', self.config.get('position_size', 15))
            max_shares = signal_data.get('max_shares', self.config.get('max_shares', 1000))
            
            # Adjust position size based on divergence strength
            confirmation_strength = signal_data.get('confirmation_strength', 'weak')
            divergence_settings = self.config.get('divergence_settings', {})
            
            if confirmation_strength == 'strong':
                multiplier = divergence_settings.get('strong_signal_multiplier', 1.5)
            elif confirmation_strength == 'weak':
                multiplier = divergence_settings.get('weak_signal_multiplier', 0.7)
            else:  # medium
                multiplier = 1.0
            
            adjusted_position_size = min(max_shares, int(base_position_size * multiplier))
            
            # Calculate risk
            stop_loss = signal_data.get('stop_loss', 0)
            if stop_loss <= 0:
                # Calculate stop loss using ATR multiplier
                atr_multiplier = divergence_settings.get('stop_loss_atr_multiplier', 1.5)
                estimated_atr = current_price * 0.015  # 1.5% as ATR estimate
                stop_loss = current_price - (estimated_atr * atr_multiplier)
            
            risk_per_share = abs(current_price - stop_loss)
            total_risk = risk_per_share * adjusted_position_size
            total_position_value = current_price * adjusted_position_size
            
            # Check position size limit
            risk_limits = self.config.get('risk_limits', {})
            max_position_size_pct = risk_limits.get('max_position_size_pct', 2.0)
            equity_buffer = risk_limits.get('equity_buffer', 5000.0)
            
            estimated_account_value = equity_buffer * 20  # Conservative estimate
            max_position_value = estimated_account_value * (max_position_size_pct / 100)
            
            if total_position_value > max_position_value:
                self.logger.warning(f"💰 {symbol}: Position value ${total_position_value:.2f} exceeds {max_position_size_pct}% limit")
                return False
            
            # Check daily loss limit
            max_daily_loss_pct = risk_limits.get('max_daily_loss_pct', 3.0)
            max_daily_loss_dollars = estimated_account_value * (max_daily_loss_pct / 100)
            
            current_daily_risk = sum(abs(order.entry_price - order.stop_loss) * order.quantity 
                                   for order in self.active_orders.values() 
                                   if order.created_at.date() == datetime.now().date())
            
            if current_daily_risk + total_risk > max_daily_loss_dollars:
                self.logger.warning(f"📉 Daily risk limit would be exceeded: ${current_daily_risk + total_risk:.2f} > ${max_daily_loss_dollars:.2f}")
                return False
            
            self.logger.debug(f"✅ {symbol}: Risk check passed - Position: ${total_position_value:.2f}, Risk: ${total_risk:.2f}, Strength: {confirmation_strength}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking divergence risk limits: {e}")
            return False

    def _is_market_open(self) -> bool:
        """Check if market is open for trading"""
        try:
            if os.path.exists('live_monitor.json'):
                with open('live_monitor.json', 'r') as f:
                    data = json.load(f)
                return data.get('market_status', {}).get('is_market_hours', False)
            else:
                # Fallback: assume market is open during business hours
                now = datetime.now()
                return 9 <= now.hour <= 16 and now.weekday() < 5
        except Exception:
            return False

    def _execute_divergence_trade(self, symbol: str, signal_data: Dict[str, Any]):
        """Execute a divergence trade"""
        try:
            self.logger.info(f"🎯 Executing divergence trade for {symbol}")
            
            # Create divergence signal object
            signal = DivergenceSignal(
                symbol=symbol,
                signal_type=signal_data.get('signal_type', ''),
                confidence=signal_data.get('confidence', 0.0),
                entry_reason=signal_data.get('entry_reason', ''),
                timestamp=signal_data.get('timestamp', ''),
                market_condition=signal_data.get('market_condition', ''),
                volatility_environment=signal_data.get('volatility_environment', ''),
                position_size=signal_data.get('position_size', 0),
                stop_loss=signal_data.get('stop_loss', 0),
                profit_target=signal_data.get('profit_target', 0),
                auto_approve=signal_data.get('auto_approve', False),
                max_shares=signal_data.get('max_shares', 1000),
                strategy_allocation=signal_data.get('strategy_allocation', 20),
                confirmation_strength=signal_data.get('confirmation_strength', 'weak'),
                confirmed_timeframes=signal_data.get('confirmed_timeframes', []),
                timeframe_analysis=signal_data.get('timeframe_analysis', {})
            )
            
            # Calculate trade parameters
            quantity, entry_price = self._calculate_divergence_trade_parameters(symbol, signal)
            
            if quantity <= 0 or entry_price <= 0:
                self.logger.error(f"❌ Invalid divergence trade parameters for {symbol}: quantity={quantity}, price={entry_price}")
                return
            
            # Create order
            order_id = f"divergence_{symbol}_{int(datetime.now().timestamp())}"
            
            trade_order = DivergenceTradeOrder(
                order_id=order_id,
                symbol=symbol,
                signal=signal,
                quantity=quantity,
                entry_price=entry_price,
                stop_loss=signal.stop_loss,
                profit_target=signal.profit_target,
                status=OrderStatus.PENDING,
                created_at=datetime.now(),
                notes=f"Divergence trade: {signal.entry_reason}",
                divergence_strength=signal.confirmation_strength,
                confirmed_timeframes=signal.confirmed_timeframes
            )
            
            # Submit order
            success = self._submit_divergence_order(trade_order)
            
            if success:
                self.active_orders[order_id] = trade_order
                self.logger.info(f"✅ Successfully submitted divergence order for {symbol}: {quantity} @ ${entry_price:.2f}")
                
                # Mark signal as processed
                self._mark_divergence_signal_processed(symbol, signal.timestamp, signal_data)
                
                # Log trade details
                self._log_divergence_trade(trade_order)
                
            else:
                self.logger.error(f"❌ Failed to submit divergence order for {symbol}")
                
        except Exception as e:
            self.logger.error(f"❌ Error executing divergence trade for {symbol}: {e}")

    def _calculate_divergence_trade_parameters(self, symbol: str, signal: DivergenceSignal) -> Tuple[int, float]:
        """Calculate divergence trade parameters with strength-based sizing"""
        try:
            # Get current market price
            entry_price = self._get_current_price(symbol)
            if entry_price <= 0:
                return 0, 0
            
            # Calculate position size based on divergence strength
            base_position_size = signal.position_size
            max_shares = signal.max_shares
            
            # Adjust position size based on divergence strength
            divergence_settings = self.config.get('divergence_settings', {})
            
            if signal.confirmation_strength == 'strong':
                multiplier = divergence_settings.get('strong_signal_multiplier', 1.5)
            elif signal.confirmation_strength == 'weak':
                multiplier = divergence_settings.get('weak_signal_multiplier', 0.7)
            else:  # medium
                multiplier = 1.0
            
            # Calculate final quantity
            adjusted_position_size = min(max_shares, int(base_position_size * multiplier))
            quantity = max(1, adjusted_position_size)  # Minimum 1 share
            
            self.logger.info(f"📊 {symbol}: Position sizing - Base: {base_position_size}, Strength: {signal.confirmation_strength}, Multiplier: {multiplier:.1f}, Final: {quantity}")
            
            return quantity, entry_price
            
        except Exception as e:
            self.logger.error(f"❌ Error calculating divergence trade parameters: {e}")
            return 0, 0

    def _get_current_price(self, symbol: str) -> float:
        """Get current market price for symbol"""
        try:
            # Try to get price from live_monitor.json first
            if os.path.exists('live_monitor.json'):
                with open('live_monitor.json', 'r') as f:
                    live_data = json.load(f)
                
                # Check watchlist data
                watchlist_data = live_data.get('integrated_watchlist', {}).get('watchlist_data', {})
                if symbol in watchlist_data:
                    current_price = watchlist_data[symbol].get('current_price', 0)
                    if current_price > 0:
                        return float(current_price)
            
            # Fallback to divergence indicators data
            for timeframe in ['1min', '5min', '15min']:
                filepath = f'divergence_data/divergence_indicators_{timeframe}.json'
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    indicators = data.get('indicators', {})
                    if symbol in indicators:
                        current_price = indicators[symbol].get('current_price', 0)
                        if current_price > 0:
                            return float(current_price)
            
            self.logger.warning(f"⚠️ Could not get current price for {symbol}")
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ Error getting current price for {symbol}: {e}")
            return 0.0

    def _submit_divergence_order(self, trade_order: DivergenceTradeOrder) -> bool:
        """Submit divergence order through order handler"""
        try:
            self.logger.info(f"📤 Submitting divergence order: {trade_order.order_id}")
            self.logger.info(f"   Symbol: {trade_order.symbol}")
            self.logger.info(f"   Quantity: {trade_order.quantity}")
            self.logger.info(f"   Entry Price: ${trade_order.entry_price:.2f}")
            self.logger.info(f"   Stop Loss: ${trade_order.stop_loss:.2f}")
            self.logger.info(f"   Profit Target: ${trade_order.profit_target:.2f}")
            self.logger.info(f"   Divergence Strength: {trade_order.divergence_strength}")
            
            # Determine order instruction
            if trade_order.signal.signal_type in ['BUY', 'STRONG_BUY']:
                instruction = 'BUY'
            elif trade_order.signal.signal_type in ['SELL', 'STRONG_SELL']:
                instruction = 'SELL_SHORT'
            else:
                self.logger.error(f"❌ Invalid signal type: {trade_order.signal.signal_type}")
                trade_order.status = OrderStatus.REJECTED
                return False
            
            # Use OCO order with targets for better risk management
            has_profit_target = trade_order.profit_target > 0
            has_stop_loss = trade_order.stop_loss > 0
            
            if has_profit_target and has_stop_loss:
                # Use OCO order with automatic targets
                result = self.order_handler.place_stock_oco_order_with_targets(
                    action_type=instruction,
                    symbol=trade_order.symbol,
                    shares=trade_order.quantity,
                    entry_price=round(trade_order.entry_price, 2),
                    profit_target=round(trade_order.profit_target, 2),
                    stop_loss=round(trade_order.stop_loss, 2),
                    timestamp=trade_order.created_at
                )
                
                self.logger.info(f"✅ Using OCO order with targets for divergence trade")
                
            else:
                # Use regular market order
                result = self.order_handler.place_market_order(
                    action_type=instruction,
                    symbol=trade_order.symbol,
                    shares=trade_order.quantity,
                    current_price=round(trade_order.entry_price, 2),
                    timestamp=trade_order.created_at
                )
                
                self.logger.info(f"📊 Using regular market order for divergence trade")
            
            # Process result
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = result.get('fill_price', trade_order.entry_price)
                
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ Divergence order submitted successfully: {result.get('order_id', 'N/A')}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | Rejection: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ Divergence order rejected: {result.get('reason', 'Unknown')}")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Error submitting divergence order: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | Error: {str(e)}"
            return False

    def _mark_divergence_signal_processed(self, symbol: str, signal_timestamp: str, signal_data: Dict[str, Any]):
        """Mark divergence signal as processed"""
        try:
            signal_key = f"divergence_{symbol}_{signal_timestamp}"
            
            signal_info = {
                'symbol': symbol,
                'signal_timestamp': signal_timestamp,
                'signal_type': signal_data.get('signal_type', ''),
                'confidence': signal_data.get('confidence', 0.0),
                'confirmation_strength': signal_data.get('confirmation_strength', 'weak'),
                'confirmed_timeframes': signal_data.get('confirmed_timeframes', []),
                'processed_at': datetime.now().isoformat(),
                'entry_reason': signal_data.get('entry_reason', ''),
                'market_condition': signal_data.get('market_condition', ''),
                'auto_approve': signal_data.get('auto_approve', False)
            }
            
            self.processed_signals[signal_key] = signal_info
            self.last_signal_check[signal_key] = datetime.now()
            
            # Save every 5 processed signals
            if len(self.processed_signals) % 5 == 0:
                self._save_processed_signals()
                
            self.logger.debug(f"✅ Marked divergence signal as processed: {signal_key}")
            
        except Exception as e:
            self.logger.error(f"❌ Error marking divergence signal as processed: {e}")

    def _manage_existing_orders(self):
        """Manage existing divergence orders"""
        try:
            if not self.active_orders:
                return
            
            # Check status of each active order
            for order_id, order in list(self.active_orders.items()):
                try:
                    self._check_divergence_order_status(order)
                    
                    # Remove completed orders
                    if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED]:
                        self.executed_trades.append(order)
                        del self.active_orders[order_id]
                        
                        if order.status == OrderStatus.FILLED:
                            self.logger.info(f"✅ Divergence order filled: {order.symbol} @ ${order.filled_price:.2f}")
                        else:
                            self.logger.info(f"❌ Divergence order {order.status.value}: {order.symbol}")
                            
                except Exception as e:
                    self.logger.error(f"❌ Error managing divergence order {order_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ Error managing existing divergence orders: {e}")

    def _check_divergence_order_status(self, order: DivergenceTradeOrder):
        """Check status of a divergence order"""
        try:
            if order.status != OrderStatus.SUBMITTED:
                return
            
            # Extract Schwab order ID if available
            schwab_order_id = None
            if "Schwab Order ID:" in order.notes:
                try:
                    schwab_order_id = order.notes.split("Schwab Order ID: ")[1].split(" |")[0].strip()
                except Exception:
                    pass
            
            if schwab_order_id:
                # Check status using order handler
                status_result = self.order_handler.get_order_status(schwab_order_id)
                
                if 'error' not in status_result:
                    schwab_status = status_result.get('status', '').upper()
                    
                    # Map Schwab status to our OrderStatus
                    if schwab_status in ['FILLED']:
                        order.status = OrderStatus.FILLED
                        order.filled_at = datetime.now()
                        order.filled_price = order.entry_price  # Simplified
                    elif schwab_status in ['CANCELED', 'CANCELLED']:
                        order.status = OrderStatus.CANCELLED
                    elif schwab_status in ['REJECTED']:
                        order.status = OrderStatus.REJECTED
                    elif schwab_status in ['EXPIRED']:
                        order.status = OrderStatus.CANCELLED
                    # WORKING, QUEUED, etc. remain as SUBMITTED
                    
                else:
                    self.logger.warning(f"⚠️ Error checking divergence order status: {status_result.get('error')}")
            else:
                # Simulate order progression for testing
                time_since_submission = datetime.now() - order.created_at
                if time_since_submission.total_seconds() > 120:  # 2 minutes
                    order.status = OrderStatus.FILLED
                    order.filled_at = datetime.now()
                    order.filled_price = order.entry_price
                    
        except Exception as e:
            self.logger.error(f"❌ Error checking divergence order status: {e}")

    def _log_divergence_trade(self, trade_order: DivergenceTradeOrder):
        """Log divergence trade execution details"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'order_id': trade_order.order_id,
                'symbol': trade_order.symbol,
                'strategy': 'divergence',
                'signal_type': trade_order.signal.signal_type,
                'confidence': trade_order.signal.confidence,
                'confirmation_strength': trade_order.divergence_strength,
                'confirmed_timeframes': trade_order.confirmed_timeframes,
                'quantity': trade_order.quantity,
                'entry_price': trade_order.entry_price,
                'stop_loss': trade_order.stop_loss,
                'profit_target': trade_order.profit_target,
                'risk_amount': abs(trade_order.entry_price - trade_order.stop_loss) * trade_order.quantity,
                'entry_reason': trade_order.signal.entry_reason,
                'market_condition': trade_order.signal.market_condition,
                'volatility_environment': trade_order.signal.volatility_environment
            }
            
            # Append to divergence trades log
            log_file = 'divergence_data/divergence_trades.json'
            trades_log = []
            
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    trades_log = json.load(f)
            
            trades_log.append(log_entry)
            
            os.makedirs('divergence_data', exist_ok=True)
            with open(log_file, 'w') as f:
                json.dump(trades_log, f, indent=2)
                
            self.logger.info(f"📝 Logged divergence trade to {log_file}")
                
        except Exception as e:
            self.logger.error(f"❌ Error logging divergence trade: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get current divergence trading engine status"""
        try:
            return {
                'running': self.running,
                'active_orders': len(self.active_orders),
                'executed_trades': len(self.executed_trades),
                'last_check': datetime.now().isoformat(),
                'signals_file': self.signals_file,
                'auto_generate_data': self.auto_generate_data,
                'last_data_generation': self.last_data_generation.isoformat() if self.last_data_generation != datetime.min else None,
                'config': {
                    'auto_trading_enabled': self.config.get('enable_auto_trading', False),
                    'monitor_interval': self.config.get('monitor_interval', 30),
                    'max_positions': self.config.get('divergence_settings', {}).get('max_positions', 5),
                    'min_confidence': self.config.get('min_confidence_threshold', 0.65),
                    'strategy_allocation': self.config.get('strategy_allocation', 20),
                    'position_size': self.config.get('position_size', 15)
                },
                'orders': [
                    {
                        'order_id': order.order_id,
                        'symbol': order.symbol,
                        'signal_type': order.signal.signal_type,
                        'confidence': order.signal.confidence,
                        'divergence_strength': order.divergence_strength,
                        'confirmed_timeframes': order.confirmed_timeframes,
                        'status': order.status.value,
                        'created_at': order.created_at.isoformat()
                    }
                    for order in self.active_orders.values()
                ]
            }
        except Exception as e:
            self.logger.error(f"❌ Error getting divergence engine status: {e}")
            return {'error': str(e)}

def main():
    """Main function to run divergence trading engine"""
    import signal
    import sys
    
    def signal_handler(signum, frame):
        print("\n🛑 Shutting down divergence trading engine...")
        engine.stop_monitoring()
        sys.exit(0)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start divergence trading engine
    engine = DivergenceTradingEngine()
    
    try:
        engine.start_monitoring()
        
        # Keep main thread alive
        while engine.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Received interrupt signal")
    finally:
        engine.stop_monitoring()
        print("👋 Divergence trading engine stopped")

if __name__ == "__main__":
    main()
