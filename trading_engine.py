#!/usr/bin/env python3
"""
Trading Engine

This engine monitors strategy signals and executes trades automatically when:
1. Signal is present (BUY, STRONG_BUY, SELL, STRONG_SELL)
2. auto_approve is True for that strategy
3. Risk management criteria are met
4. Position limits are not exceeded

Features:
- Real-time signal monitoring
- Automatic trade execution
- Risk management integration
- Position tracking
- Order management
- Logging and notifications
"""

import json
import os
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass
from enum import Enum

# Import existing handlers
from order_handler import OrderHandler
# Note: Removed problematic imports that don't exist in the modules

class TradeType(Enum):
    """Trade types"""
    STOCK = "STOCK"
    OPTION = "OPTION"
    FUTURE = "FUTURE"

class OrderStatus(Enum):
    """Order status types"""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"

@dataclass
class TradingSignal:
    """Trading signal data structure"""
    symbol: str
    strategy: str
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
    setup: Optional[Dict] = None

@dataclass
class TradeOrder:
    """Trade order data structure"""
    order_id: str
    symbol: str
    strategy: str
    signal: TradingSignal
    trade_type: TradeType
    quantity: int
    entry_price: float
    stop_loss: float
    profit_target: float
    status: OrderStatus
    created_at: datetime
    filled_at: Optional[datetime] = None
    filled_price: Optional[float] = None
    notes: str = ""

class TradingEngine:
    """
    Main trading engine that monitors signals and executes trades
    """
    
    def __init__(self):
        """Initialize the trading engine"""
        self.running = False
        self.monitor_thread = None
        
        # Setup logging first
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('trading_engine.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize handlers
        self.order_handler = OrderHandler()
        # Note: Only initializing order_handler as other classes don't exist
        
        # Trading state
        self.active_orders: Dict[str, TradeOrder] = {}
        self.executed_trades: List[TradeOrder] = []
        self.last_signal_check = {}
        
        # Signal state persistence - prevents re-trading signals across engine restarts
        self.processed_signals_file = 'processed_signals.json'
        self.processed_signals = self._load_processed_signals()
        
        # Configuration
        self.config = self.load_trading_config()
        
        self.logger.info("Trading Engine initialized")
        self.logger.info(f"Monitor interval: {self.config.get('monitor_interval', 10)} seconds")
        self.logger.info(f"Max positions per strategy: {self.config.get('max_positions_per_strategy', 3)}")
        self.logger.info(f"Loaded {len(self.processed_signals)} previously processed signals")

    def load_trading_config(self) -> Dict[str, Any]:
        """Load trading engine configuration from risk_config_live.json"""
        try:
            risk_config_file = 'risk_config_live.json'
            if os.path.exists(risk_config_file):
                with open(risk_config_file, 'r') as f:
                    risk_config = json.load(f)
                
                # Extract risk management settings
                risk_mgmt = risk_config.get('risk_management', {})
                account_limits = risk_mgmt.get('account_limits', {})
                position_sizing = risk_mgmt.get('position_sizing', {})
                stop_loss_settings = risk_mgmt.get('stop_loss_settings', {})
                parameter_states = risk_mgmt.get('parameter_states', {})
                
                # Convert risk config to trading engine format
                trading_config = {
                    "monitor_interval": 10,  # seconds
                    "enable_auto_trading": True,
                    "enable_risk_management": parameter_states.get('enable_max_account_risk', True) or parameter_states.get('enable_daily_loss_limit', True),
                    "enable_position_sizing": parameter_states.get('enable_max_position_size', True),
                    "min_confidence_threshold": 0.6,
                    
                    # Extract limits from risk config
                    "max_total_positions": int(position_sizing.get('max_positions', 15)),
                    "max_positions_per_strategy": max(1, int(position_sizing.get('max_positions', 15)) // 3),  # Divide by 3 strategies
                    
                    "risk_limits": {
                        "max_daily_loss_pct": float(account_limits.get('daily_loss_limit', 5.0)),
                        "max_position_size_pct": float(position_sizing.get('max_position_size', 5.0)),
                        "max_account_risk_pct": float(account_limits.get('max_account_risk', 25.0)),
                        "equity_buffer": float(account_limits.get('equity_buffer', 10000.0))
                    },
                    
                    "stop_loss": {
                        "method": stop_loss_settings.get('method', 'atr'),
                        "value": float(stop_loss_settings.get('value', 2.0)),
                        "take_profit_ratio": float(stop_loss_settings.get('take_profit_ratio', 2.0)),
                        "enabled": parameter_states.get('enable_stop_loss', True)
                    },
                    
                    "parameter_states": parameter_states,
                    
                    "strategies": {
                        "pml": {
                            "enabled": True,
                            "max_positions": max(1, int(position_sizing.get('max_positions', 15)) // 3),
                            "position_size_multiplier": 1.0,
                            "min_confidence": 0.7
                        },
                        "iron_condor": {
                            "enabled": True,
                            "max_positions": max(1, int(position_sizing.get('max_positions', 15)) // 3),
                            "position_size_multiplier": 0.8,
                            "min_confidence": 0.6
                        },
                        "divergence": {
                            "enabled": True,
                            "max_positions": max(1, int(position_sizing.get('max_positions', 15)) // 3),
                            "position_size_multiplier": 1.2,
                            "min_confidence": 0.65
                        }
                    }
                }
                
                self.logger.info(f"Loaded trading config from {risk_config_file}")
                self.logger.info(f"Risk management enabled: {trading_config['enable_risk_management']}")
                self.logger.info(f"Max total positions: {trading_config['max_total_positions']}")
                self.logger.info(f"Max daily loss: {trading_config['risk_limits']['max_daily_loss_pct']}%")
                self.logger.info(f"Max position size: {trading_config['risk_limits']['max_position_size_pct']}%")
                
                return trading_config
            else:
                self.logger.error(f"Risk config file not found: {risk_config_file}")
                # Return minimal safe config
                return {
                    "monitor_interval": 10,
                    "enable_auto_trading": False,
                    "enable_risk_management": True,
                    "max_total_positions": 5,
                    "risk_limits": {"max_daily_loss_pct": 2.0, "max_position_size_pct": 1.0}
                }
                
        except Exception as e:
            self.logger.error(f"Error loading risk config: {e}")
            return {"monitor_interval": 10, "enable_auto_trading": False}

    def _load_processed_signals(self) -> Dict[str, Dict[str, Any]]:
        """Load previously processed signals from persistent storage"""
        try:
            if os.path.exists(self.processed_signals_file):
                with open(self.processed_signals_file, 'r') as f:
                    data = json.load(f)
                    
                # Clean up old signals (older than 7 days to prevent file from growing too large)
                cutoff_time = datetime.now() - timedelta(days=7)
                cleaned_signals = {}
                
                for signal_key, signal_info in data.items():
                    try:
                        processed_time = datetime.fromisoformat(signal_info.get('processed_at', ''))
                        if processed_time > cutoff_time:
                            cleaned_signals[signal_key] = signal_info
                    except Exception:
                        # Skip invalid entries
                        continue
                
                # Save cleaned data back if we removed old entries
                if len(cleaned_signals) < len(data):
                    self._save_processed_signals(cleaned_signals)
                    self.logger.info(f"Cleaned up {len(data) - len(cleaned_signals)} old processed signals")
                
                return cleaned_signals
            else:
                self.logger.info("No processed signals file found - starting fresh")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error loading processed signals: {e}")
            return {}

    def _save_processed_signals(self, signals_dict: Dict[str, Dict[str, Any]] = None):
        """Save processed signals to persistent storage"""
        try:
            if signals_dict is None:
                signals_dict = self.processed_signals
                
            with open(self.processed_signals_file, 'w') as f:
                json.dump(signals_dict, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving processed signals: {e}")

    def _mark_signal_as_processed(self, strategy: str, symbol: str, signal_timestamp: str, signal_data: Dict[str, Any]):
        """Mark a signal as processed to prevent re-trading across engine restarts"""
        try:
            signal_serial_key = f"{strategy}_{symbol}_{signal_timestamp}"
            
            signal_info = {
                'strategy': strategy,
                'symbol': symbol,
                'signal_timestamp': signal_timestamp,
                'signal_type': signal_data.get('signal_type', ''),
                'confidence': signal_data.get('confidence', 0.0),
                'processed_at': datetime.now().isoformat(),
                'entry_reason': signal_data.get('entry_reason', ''),
                'market_condition': signal_data.get('market_condition', ''),
                'auto_approve': signal_data.get('auto_approve', False)
            }
            
            # Add to both in-memory and persistent storage
            self.processed_signals[signal_serial_key] = signal_info
            self.last_signal_check[signal_serial_key] = datetime.now()
            
            # Save to file every 10 processed signals to avoid excessive I/O
            if len(self.processed_signals) % 10 == 0:
                self._save_processed_signals()
                
            self.logger.debug(f"Marked signal as processed: {signal_serial_key}")
            
        except Exception as e:
            self.logger.error(f"Error marking signal as processed: {e}")

    def _is_signal_already_processed(self, strategy: str, symbol: str, signal_timestamp: str) -> bool:
        """Check if a signal has already been processed (prevents re-trading across restarts)"""
        try:
            signal_serial_key = f"{strategy}_{symbol}_{signal_timestamp}"
            
            # Check both in-memory cache and persistent storage
            if signal_serial_key in self.last_signal_check:
                self.logger.debug(f"Signal found in memory cache: {signal_serial_key}")
                return True
                
            if signal_serial_key in self.processed_signals:
                # Move to memory cache for faster future lookups
                self.last_signal_check[signal_serial_key] = datetime.now()
                self.logger.debug(f"Signal found in persistent storage: {signal_serial_key}")
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking if signal already processed: {e}")
            return False  # Default to allowing the signal if we can't check

    def start_monitoring(self):
        """Start the signal monitoring thread"""
        if self.running:
            self.logger.warning("Trading engine is already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_signals, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info("🚀 Trading Engine started - monitoring signals for auto-execution")

    def stop_monitoring(self):
        """Stop the signal monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        # Save processed signals on shutdown to ensure persistence
        try:
            self._save_processed_signals()
            self.logger.info(f"💾 Saved {len(self.processed_signals)} processed signals on shutdown")
        except Exception as e:
            self.logger.error(f"Error saving processed signals on shutdown: {e}")
        
        self.logger.info("🛑 Trading Engine stopped")

    def _monitor_signals(self):
        """Main monitoring loop"""
        self.logger.info("📡 Signal monitoring loop started")
        
        while self.running:
            try:
                # Monitor each strategy (auto-approve checked at individual signal level)
                self.logger.info(f"🔍 Checking signals... (Auto-approve checked per signal)")
                
                # Monitor each strategy
                strategies = ['pml', 'iron_condor', 'divergence']
                self.logger.info(f"📊 Monitoring {len(strategies)} strategies: {strategies}")
                
                for strategy in strategies:
                    # Check if strategy has any auto-approved signals
                    strategy_has_auto_approve = self._strategy_has_auto_approve_signals(strategy)
                    self.logger.info(f"🎯 Checking {strategy} strategy (Auto-approve signals: {'YES' if strategy_has_auto_approve else 'NO'})")
                    
                    # Always check signals regardless - individual signal auto_approve will be checked in _should_execute_trade
                    try:
                        self._check_strategy_signals(strategy)
                    except Exception as e:
                        self.logger.error(f"Error checking {strategy} signals: {e}")
                
                # Check and manage existing orders
                active_orders_count = len(self.active_orders)
                self.logger.info(f"📋 Managing {active_orders_count} active orders")
                self._manage_existing_orders()
                
                # Sleep until next check
                monitor_interval = self.config.get('monitor_interval', 10)
                self.logger.info(f"⏰ Next check in {monitor_interval} seconds...")
                time.sleep(monitor_interval)
                
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal")
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(30)  # Wait longer on error

    def _check_strategy_signals(self, strategy: str):
        """Check signals for a specific strategy"""
        try:
            # Load strategy signals
            signals = self._load_strategy_signals(strategy)
            if not signals:
                return
            
            strategy_data = signals.get('signals', {})
            if not strategy_data:
                return
            
            # Check each symbol for actionable signals
            for symbol, signal_data in strategy_data.items():
                try:
                    if self._should_execute_trade(strategy, symbol, signal_data):
                        self._execute_trade(strategy, symbol, signal_data)
                except Exception as e:
                    self.logger.error(f"Error processing {strategy} signal for {symbol}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error checking {strategy} signals: {e}")

    def _strategy_has_auto_approve_signals(self, strategy: str) -> bool:
        """Check if strategy has any signals with auto_approve enabled"""
        try:
            signals = self._load_strategy_signals(strategy)
            if not signals:
                return False
            
            strategy_data = signals.get('signals', {})
            if not strategy_data:
                return False
            
            # Check if any individual signal has auto_approve enabled
            for symbol, signal_data in strategy_data.items():
                if signal_data.get('auto_approve', False):
                    return True
            
            # Also check global strategy auto_approve_status in metadata
            metadata = signals.get('metadata', {})
            global_auto_approve = metadata.get('auto_approve_status', False)
            
            return global_auto_approve
            
        except Exception as e:
            self.logger.error(f"Error checking {strategy} auto-approve signals: {e}")
            return False

    def _load_strategy_signals(self, strategy: str) -> Optional[Dict[str, Any]]:
        """Load signals from strategy JSON file"""
        try:
            signal_files = {
                'pml': 'pml_signals.json',
                'iron_condor': 'iron_condor_signals.json',
                'divergence': 'divergence_signals.json'
            }
            
            if strategy not in signal_files:
                return None
            
            signal_file = signal_files[strategy]
            if not os.path.exists(signal_file):
                return None
            
            with open(signal_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            self.logger.error(f"Error loading {strategy} signals: {e}")
            return None

    def _should_execute_trade(self, strategy: str, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Determine if a trade should be executed"""
        try:
            # Check if auto_approve is enabled for this specific signal
            if not signal_data.get('auto_approve', False):
                self.logger.debug(f"Auto-approve disabled for {strategy} {symbol}, skipping trade")
                return False
            
            # Check signal type
            signal_type = signal_data.get('signal_type', 'NO_SIGNAL')
            if signal_type not in ['BUY', 'STRONG_BUY', 'SELL', 'STRONG_SELL']:
                return False
            
            # Check confidence threshold
            confidence = signal_data.get('confidence', 0.0)
            min_confidence = self.config.get('min_confidence_threshold', 0.6)
            if confidence < min_confidence:
                self.logger.debug(f"{strategy} {symbol}: Confidence {confidence:.2f} below threshold {min_confidence}")
                return False
            
            # Check if signal is recent enough
            signal_timestamp = signal_data.get('timestamp', '')
            if not self._is_signal_recent(signal_timestamp):
                return False
            
            # Check if this signal has already been processed (prevents re-trading across restarts)
            if self._is_signal_already_processed(strategy, symbol, signal_timestamp):
                self.logger.debug(f"Signal already processed (timestamp: {signal_timestamp}): {strategy}_{symbol}_{signal_timestamp}")
                return False
            
            # Check position limits and boxed position prevention
            if not self._check_position_limits(strategy, symbol, signal_type):
                return False
            
            # Check risk management
            if not self._check_risk_limits(strategy, symbol, signal_data):
                return False
            
            self.logger.info(f"✅ Signal approved for execution: {strategy} {symbol} {signal_type} (confidence: {confidence:.2f}, timestamp: {signal_timestamp})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking if should execute trade for {strategy} {symbol}: {e}")
            return False

    def _is_signal_recent(self, timestamp_str: str, max_age_minutes: int = 60) -> bool:
        """Check if signal is recent enough to act on"""
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
            self.logger.error(f"Error checking signal age: {e}")
            return False

    def _check_position_limits(self, strategy: str, symbol: str, signal_type: str) -> bool:
        """Check if position limits allow new trade and prevent boxed positions"""
        try:
            # Count current positions for this strategy
            strategy_positions = sum(1 for order in self.active_orders.values() 
                                   if order.strategy == strategy and order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED])
            
            max_strategy_positions = self.config.get('strategies', {}).get(strategy, {}).get('max_positions', 3)
            if strategy_positions >= max_strategy_positions:
                self.logger.debug(f"{strategy}: At position limit ({strategy_positions}/{max_strategy_positions})")
                return False
            
            # Check total positions
            total_positions = len([order for order in self.active_orders.values() 
                                 if order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED]])
            
            max_total_positions = self.config.get('max_total_positions', 10)
            if total_positions >= max_total_positions:
                self.logger.debug(f"At total position limit ({total_positions}/{max_total_positions})")
                return False
            
            # Check current positions from live_monitor.json to prevent boxed positions
            current_positions = self._get_current_positions()
            if current_positions and symbol in current_positions:
                current_position = current_positions[symbol]
                current_quantity = current_position.get('quantity', 0)
                
                # Determine if this would create a boxed position
                if signal_type in ['BUY', 'STRONG_BUY']:
                    # We want to buy, check if we have short positions
                    if current_quantity < 0:
                        self.logger.warning(f"🚫 Cannot BUY {symbol}: Would create boxed position (current short: {current_quantity})")
                        return False
                elif signal_type in ['SELL', 'STRONG_SELL']:
                    # We want to sell, check if we have long positions
                    if current_quantity > 0:
                        self.logger.warning(f"🚫 Cannot SELL {symbol}: Would create boxed position (current long: {current_quantity})")
                        return False
            
            # Check if we already have pending orders for this symbol
            symbol_orders = [order for order in self.active_orders.values() 
                           if order.symbol == symbol and order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED]]
            
            if symbol_orders:
                self.logger.debug(f"Already have pending/filled orders for {symbol}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking position limits: {e}")
            return False

    def _check_risk_limits(self, strategy: str, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Check risk management limits using risk_config_live.json parameters"""
        try:
            parameter_states = self.config.get('parameter_states', {})
            risk_limits = self.config.get('risk_limits', {})
            
            # Get current market price and position details
            entry_price = self._get_current_price(symbol)
            position_size = signal_data.get('position_size', 1.0)
            stop_loss = signal_data.get('stop_loss', 0.0)
            
            if entry_price <= 0:
                self.logger.warning(f"{strategy} {symbol}: Could not get valid entry price")
                return False
            
            # Use stop loss from signal or calculate using ATR method
            if stop_loss <= 0:
                stop_loss_config = self.config.get('stop_loss', {})
                if stop_loss_config.get('method') == 'atr':
                    atr_multiplier = stop_loss_config.get('value', 2.0)
                    # Simplified ATR calculation - would use real ATR in production
                    estimated_atr = entry_price * 0.02  # 2% as rough ATR estimate
                    stop_loss = entry_price - (estimated_atr * atr_multiplier)
            
            if stop_loss <= 0:
                self.logger.warning(f"{strategy} {symbol}: Invalid stop loss: {stop_loss}")
                return False
            
            # Calculate position risk
            risk_per_unit = abs(entry_price - stop_loss)
            total_position_value = entry_price * position_size
            total_risk = risk_per_unit * position_size
            
            # Check maximum position size as percentage of account
            if parameter_states.get('enable_max_position_size', True):
                max_position_size_pct = risk_limits.get('max_position_size_pct', 5.0)
                equity_buffer = risk_limits.get('equity_buffer', 10000.0)
                
                # Assume account value (would get from account data in production)
                estimated_account_value = equity_buffer * 10  # Rough estimate
                max_position_value = estimated_account_value * (max_position_size_pct / 100)
                
                if total_position_value > max_position_value:
                    self.logger.warning(f"{strategy} {symbol}: Position value ${total_position_value:.2f} exceeds {max_position_size_pct}% limit (${max_position_value:.2f})")
                    return False
            
            # Check daily loss limit as percentage
            if parameter_states.get('enable_daily_loss_limit', True):
                max_daily_loss_pct = risk_limits.get('max_daily_loss_pct', 5.0)
                equity_buffer = risk_limits.get('equity_buffer', 10000.0)
                
                # Calculate max daily loss in dollars
                estimated_account_value = equity_buffer * 10  # Rough estimate
                max_daily_loss_dollars = estimated_account_value * (max_daily_loss_pct / 100)
                
                # Calculate current daily risk
                current_daily_risk = sum(abs(order.entry_price - order.stop_loss) * order.quantity 
                                       for order in self.active_orders.values() 
                                       if order.created_at.date() == datetime.now().date())
                
                if current_daily_risk + total_risk > max_daily_loss_dollars:
                    self.logger.warning(f"Daily risk limit would be exceeded: ${current_daily_risk + total_risk:.2f} > {max_daily_loss_pct}% (${max_daily_loss_dollars:.2f})")
                    return False
            
            # Check account risk limit
            if parameter_states.get('enable_max_account_risk', True):
                max_account_risk_pct = risk_limits.get('max_account_risk_pct', 25.0)
                equity_buffer = risk_limits.get('equity_buffer', 10000.0)
                
                # Calculate total portfolio risk
                total_portfolio_risk = sum(abs(order.entry_price - order.stop_loss) * order.quantity 
                                         for order in self.active_orders.values() 
                                         if order.status in [OrderStatus.SUBMITTED, OrderStatus.FILLED])
                
                estimated_account_value = equity_buffer * 10  # Rough estimate
                max_account_risk_dollars = estimated_account_value * (max_account_risk_pct / 100)
                
                if total_portfolio_risk + total_risk > max_account_risk_dollars:
                    self.logger.warning(f"Account risk limit would be exceeded: ${total_portfolio_risk + total_risk:.2f} > {max_account_risk_pct}% (${max_account_risk_dollars:.2f})")
                    return False
            
            # Check equity buffer
            if parameter_states.get('enable_equity_buffer', True):
                equity_buffer = risk_limits.get('equity_buffer', 10000.0)
                if total_risk > equity_buffer * 0.1:  # Don't risk more than 10% of equity buffer per trade
                    self.logger.warning(f"{strategy} {symbol}: Position risk ${total_risk:.2f} exceeds 10% of equity buffer (${equity_buffer * 0.1:.2f})")
                    return False
            
            self.logger.debug(f"{strategy} {symbol}: Risk check passed - Position: ${total_position_value:.2f}, Risk: ${total_risk:.2f}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking risk limits: {e}")
            return False

    def _execute_trade(self, strategy: str, symbol: str, signal_data: Dict[str, Any]):
        """Execute a trade based on signal"""
        try:
            self.logger.info(f"🎯 Executing {strategy} trade for {symbol}")
            
            # Create trading signal object
            signal = TradingSignal(
                symbol=symbol,
                strategy=strategy,
                signal_type=signal_data.get('signal_type', ''),
                confidence=signal_data.get('confidence', 0.0),
                entry_reason=signal_data.get('entry_reason', ''),
                timestamp=signal_data.get('timestamp', ''),
                market_condition=signal_data.get('market_condition', ''),
                volatility_environment=signal_data.get('volatility_environment', ''),
                position_size=signal_data.get('position_size', 1.0),
                stop_loss=signal_data.get('stop_loss', 0.0),
                profit_target=signal_data.get('profit_target', 0.0),
                auto_approve=signal_data.get('auto_approve', False),
                setup=signal_data.get('setup')
            )
            
            # Determine trade type and parameters
            trade_type, quantity, entry_price = self._determine_trade_parameters(strategy, symbol, signal)
            
            if quantity <= 0 or entry_price <= 0:
                self.logger.error(f"Invalid trade parameters for {symbol}: quantity={quantity}, price={entry_price}")
                return
            
            # Create order
            order_id = f"{strategy}_{symbol}_{int(datetime.now().timestamp())}"
            
            trade_order = TradeOrder(
                order_id=order_id,
                symbol=symbol,
                strategy=strategy,
                signal=signal,
                trade_type=trade_type,
                quantity=quantity,
                entry_price=entry_price,
                stop_loss=signal.stop_loss,
                profit_target=signal.profit_target,
                status=OrderStatus.PENDING,
                created_at=datetime.now(),
                notes=f"Auto-trade from {strategy} strategy: {signal.entry_reason}"
            )
            
            # Submit order through order handler
            success = self._submit_order(trade_order)
            
            if success:
                self.active_orders[order_id] = trade_order
                self.logger.info(f"✅ Successfully submitted {strategy} order for {symbol}: {quantity} @ ${entry_price:.2f}")
                
                # Mark signal as processed to prevent re-trading across engine restarts
                self._mark_signal_as_processed(strategy, symbol, signal.timestamp, signal_data)
                
                # Log trade details
                self._log_trade_execution(trade_order)
                
                # Save order to file
                self._save_order_to_file(trade_order)
                
            else:
                self.logger.error(f"❌ Failed to submit {strategy} order for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error executing trade for {strategy} {symbol}: {e}")

    def _determine_trade_parameters(self, strategy: str, symbol: str, signal: TradingSignal) -> Tuple[TradeType, int, float]:
        """Determine trade type, quantity, and entry price with elegant options/stocks handling"""
        try:
            # Get current market price
            entry_price = self._get_current_price(symbol)
            if entry_price <= 0:
                return TradeType.STOCK, 0, 0
            
            # Determine trade type based on strategy and signal setup
            trade_type = self._determine_asset_type(strategy, symbol, signal)
            
            # Calculate position size based on trade type
            if trade_type == TradeType.OPTION:
                # For options, position_size represents number of contracts
                base_quantity = int(signal.position_size)
                # Options are typically traded in smaller quantities
                strategy_multiplier = self.config.get('strategies', {}).get(strategy, {}).get('position_size_multiplier', 0.5)
                quantity = max(1, int(base_quantity * strategy_multiplier))
                
                self.logger.info(f"{strategy} {symbol}: Option trade - {quantity} contracts")
                
            else:  # STOCK/ETF
                # For stocks, position_size represents number of shares
                base_quantity = int(signal.position_size)
                strategy_multiplier = self.config.get('strategies', {}).get(strategy, {}).get('position_size_multiplier', 1.0)
                quantity = max(1, int(base_quantity * strategy_multiplier))
                
                self.logger.info(f"{strategy} {symbol}: Stock trade - {quantity} shares")
            
            return trade_type, quantity, entry_price
            
        except Exception as e:
            self.logger.error(f"Error determining trade parameters: {e}")
            return TradeType.STOCK, 0, 0

    def _determine_asset_type(self, strategy: str, symbol: str, signal: TradingSignal) -> TradeType:
        """Determine whether to trade the underlying stock or options based on strategy and setup"""
        try:
            # Check if signal has option-specific setup information
            setup = signal.setup or {}
            
            # Iron Condor strategy should always use options when setup is present
            if strategy == 'iron_condor':
                # Iron Condor is inherently an options strategy
                if setup:  # If we have setup details, use options
                    self.logger.info(f"Iron Condor {symbol}: Using options (setup detected)")
                    return TradeType.OPTION
                else:
                    # Fallback to stock if no setup available
                    self.logger.info(f"Iron Condor {symbol}: No setup available, using stock fallback")
                    return TradeType.STOCK
            
            # PML strategy uses options when setup contains option details
            elif strategy == 'pml':
                # Check if setup contains option-specific fields
                if setup and ('strike' in setup or 'option_type' in setup):
                    self.logger.info(f"PML {symbol}: Using options (option setup detected)")
                    return TradeType.OPTION
                else:
                    # Default to stock for PML when no option setup
                    self.logger.info(f"PML {symbol}: Using stock (no option setup)")
                    return TradeType.STOCK
            
            # Divergence strategy typically uses stocks but can use options
            elif strategy == 'divergence':
                # Check if setup specifies options usage
                if setup and setup.get('use_options', False):
                    return TradeType.OPTION
                else:
                    # Default to stock for divergence
                    return TradeType.STOCK
            
            # Default to stock for unknown strategies
            else:
                self.logger.warning(f"Unknown strategy {strategy}, defaulting to STOCK")
                return TradeType.STOCK
                
        except Exception as e:
            self.logger.error(f"Error determining asset type: {e}")
            return TradeType.STOCK

    def _get_current_positions(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Load current positions from live_monitor.json to prevent boxed positions"""
        try:
            if os.path.exists('live_monitor.json'):
                with open('live_monitor.json', 'r') as f:
                    live_data = json.load(f)
                
                # Extract positions from the nested structure
                positions_data = live_data.get('positions', {}).get('positions', {})
                if positions_data:
                    self.logger.debug(f"Loaded {len(positions_data)} current positions from live_monitor.json")
                    return positions_data
                else:
                    self.logger.debug("No positions found in live_monitor.json")
                    return {}
            else:
                self.logger.warning("live_monitor.json not found - cannot check current positions")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error loading current positions from live_monitor.json: {e}")
            return {}

    def _get_current_price(self, symbol: str) -> float:
        """Get current market price for symbol"""
        try:
            # First try to get price from live_monitor.json (most current)
            if os.path.exists('live_monitor.json'):
                with open('live_monitor.json', 'r') as f:
                    live_data = json.load(f)
                
                # Check watchlist data first
                watchlist_data = live_data.get('integrated_watchlist', {}).get('watchlist_data', {})
                if symbol in watchlist_data:
                    current_price = watchlist_data[symbol].get('current_price', 0)
                    if current_price > 0:
                        return float(current_price)
                
                # Check technical indicators in live_monitor
                tech_indicators = live_data.get('technical_indicators', {})
                if symbol in tech_indicators:
                    current_price = tech_indicators[symbol].get('current_price', 0)
                    if current_price > 0:
                        return float(current_price)
            
            # Fallback to technical_indicators.json
            if os.path.exists('technical_indicators.json'):
                with open('technical_indicators.json', 'r') as f:
                    indicators = json.load(f)
                
                if symbol in indicators.get('indicators', {}):
                    base_indicators = indicators['indicators'][symbol].get('base_indicators', {})
                    current_price = base_indicators.get('current_price', 0)
                    if current_price > 0:
                        return float(current_price)
            
            # Fallback - would need real market data integration
            self.logger.warning(f"Could not get current price for {symbol}, using placeholder")
            return 100.0  # Placeholder
            
        except Exception as e:
            self.logger.error(f"Error getting current price for {symbol}: {e}")
            return 0.0

    def _submit_order(self, trade_order: TradeOrder) -> bool:
        """Submit order through existing order handler"""
        try:
            self.logger.info(f"Submitting order: {trade_order.order_id}")
            self.logger.info(f"  Symbol: {trade_order.symbol}")
            self.logger.info(f"  Strategy: {trade_order.strategy}")
            self.logger.info(f"  Type: {trade_order.trade_type.value}")
            self.logger.info(f"  Quantity: {trade_order.quantity}")
            self.logger.info(f"  Entry Price: ${trade_order.entry_price:.2f}")
            
            # Handle Iron Condor as special 4-leg spread order
            if trade_order.strategy == 'iron_condor' and trade_order.signal.setup:
                return self._submit_iron_condor_spread_order(trade_order)
            
            # Determine order instruction based on signal type
            if trade_order.signal.signal_type in ['BUY', 'STRONG_BUY']:
                instruction = 'BUY'  # Buy to open long position
            elif trade_order.signal.signal_type in ['SELL', 'STRONG_SELL']:
                instruction = 'SELL_SHORT'  # Sell short to open short position
            else:
                self.logger.error(f"Invalid signal type for order: {trade_order.signal.signal_type}")
                trade_order.status = OrderStatus.REJECTED
                return False
            
            # Submit order based on trade type
            if trade_order.trade_type == TradeType.STOCK:
                # ==================== STOCK ORDER LOGIC WITH OCO SUPPORT ====================
                # 
                # This section implements intelligent order routing for stock trades:
                # 1. If the signal includes both profit_target and stop_loss, AND
                # 2. The instruction is BUY or SELL_SHORT (new position entry), THEN
                # 3. Use OCO (One Cancels Other) order for automatic risk management
                # 4. Otherwise, use regular market order
                #
                # OCO Order Benefits:
                # - Entry order fills → Automatic profit target and stop loss placement
                # - One exit fills → Other exit automatically cancelled
                # - No manual intervention required
                # - Guaranteed risk management on every trade
                
                # Check if we have valid profit target and stop loss for OCO order
                has_profit_target = trade_order.profit_target > 0
                has_stop_loss = trade_order.stop_loss > 0
                supports_oco = instruction in ['BUY', 'SELL_SHORT']  # Only new position entries support OCO
                
                if (has_profit_target and has_stop_loss and supports_oco):
                    # ========== USE OCO ORDER WITH AUTOMATIC TARGETS ==========
                    # This creates a complex order structure:
                    # 1. Entry order (BUY/SELL_SHORT) at entry_price
                    # 2. Once filled, automatically places TWO exit orders:
                    #    - Profit target LIMIT order at profit_target price
                    #    - Stop loss STOP order at stop_loss price
                    # 3. If either exit fills, the other is cancelled (OCO logic)
                    
                    result = self.order_handler.place_stock_oco_order_with_targets(
                        action_type=instruction,
                        symbol=trade_order.symbol,
                        shares=trade_order.quantity,
                        entry_price=round(trade_order.entry_price, 2),
                        profit_target=round(trade_order.profit_target, 2),
                        stop_loss=round(trade_order.stop_loss, 2),
                        timestamp=trade_order.created_at
                    )
                    
                    self.logger.info(f"✅ Using OCO order with targets for {instruction} {trade_order.symbol}")
                    self.logger.info(f"   Entry: ${trade_order.entry_price:.2f}")
                    self.logger.info(f"   Profit Target: ${trade_order.profit_target:.2f}")
                    self.logger.info(f"   Stop Loss: ${trade_order.stop_loss:.2f}")
                    
                else:
                    # ========== USE REGULAR MARKET ORDER ==========
                    # Fallback to regular market order when:
                    # - No profit target specified (profit_target <= 0)
                    # - No stop loss specified (stop_loss <= 0)
                    # - Instruction is SELL or BUY_TO_COVER (closing existing positions)
                    
                    result = self.order_handler.place_market_order(
                        action_type=instruction,
                        symbol=trade_order.symbol,
                        shares=trade_order.quantity,
                        current_price=round(trade_order.entry_price, 2),
                        timestamp=trade_order.created_at
                    )
                    
                    # Log why we're using regular order instead of OCO
                    if not has_profit_target:
                        self.logger.info(f"📊 Using regular market order for {instruction} {trade_order.symbol} (no profit target)")
                    elif not has_stop_loss:
                        self.logger.info(f"📊 Using regular market order for {instruction} {trade_order.symbol} (no stop loss)")
                    elif not supports_oco:
                        self.logger.info(f"📊 Using regular market order for {instruction} {trade_order.symbol} (closing position)")
                    else:
                        self.logger.info(f"📊 Using regular market order for {instruction} {trade_order.symbol}")
                
            elif trade_order.trade_type == TradeType.OPTION:
                # Handle PML single-leg option orders
                if trade_order.strategy == 'pml':
                    return self._submit_pml_option_order(trade_order, instruction)
                else:
                    # Generic single-leg option order
                    return self._submit_generic_option_order(trade_order, instruction)
                
            else:
                self.logger.error(f"Unsupported trade type: {trade_order.trade_type}")
                result = {'status': 'rejected', 'reason': 'Unsupported trade type'}
            
            # Process result from order handler (for stock orders)
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = result.get('fill_price', trade_order.entry_price)
                
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ Stock order submitted successfully: {result.get('order_id', 'N/A')}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | Rejection reason: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ Stock order rejected: {result.get('reason', 'Unknown')}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error submitting order: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | Error: {str(e)}"
            return False

    def _submit_iron_condor_spread_order(self, trade_order: TradeOrder) -> bool:
        """Submit Iron Condor 4-leg spread order using Schwab API format"""
        try:
            setup = trade_order.signal.setup
            strikes = setup.get('strikes', {})
            expiration_date_str = setup.get('expiration_date', '')
            net_credit = setup.get('net_credit', 0)
            
            # Parse expiration date for option symbol construction
            try:
                if 'T' in expiration_date_str:
                    expiration_date = datetime.fromisoformat(expiration_date_str.replace('Z', '+00:00'))
                else:
                    expiration_date = datetime.strptime(expiration_date_str, '%Y-%m-%d')
            except Exception as e:
                self.logger.error(f"Error parsing Iron Condor expiration date: {e}")
                return False
            
            # Format expiration for option symbol (YYMMDD)
            exp_formatted = expiration_date.strftime('%y%m%d')
            
            # Construct all 4 option symbols using Schwab format
            # Format: SYMBOL YYMMDD[C/P]SSSSSPPP (6 chars symbol + 6 chars date + 1 char type + 8 chars strike)
            symbol_padded = f"{trade_order.symbol:<6}"  # Left-align and pad to 6 characters
            
            # Helper function to format strike price (5 digits + 3 decimals = 8 total)
            def format_strike(strike):
                return f"{int(strike * 1000):08d}"
            
            long_put_symbol = f"{symbol_padded}{exp_formatted}P{format_strike(strikes['long_put'])}"
            short_put_symbol = f"{symbol_padded}{exp_formatted}P{format_strike(strikes['short_put'])}"
            short_call_symbol = f"{symbol_padded}{exp_formatted}C{format_strike(strikes['short_call'])}"
            long_call_symbol = f"{symbol_padded}{exp_formatted}C{format_strike(strikes['long_call'])}"
            
            self.logger.info(f"Iron Condor option symbols:")
            self.logger.info(f"  Long Put:   {long_put_symbol}")
            self.logger.info(f"  Short Put:  {short_put_symbol}")
            self.logger.info(f"  Short Call: {short_call_symbol}")
            self.logger.info(f"  Long Call:  {long_call_symbol}")
            
            # Create 4-leg spread order using Schwab API format
            spread_order = {
                "orderType": "NET_CREDIT",  # Iron Condor collects credit
                "session": "NORMAL",
                "price": str(net_credit),  # Net credit we want to collect
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": "BUY_TO_OPEN",
                        "quantity": trade_order.quantity,
                        "instrument": {
                            "symbol": long_put_symbol,
                            "assetType": "OPTION"
                        }
                    },
                    {
                        "instruction": "SELL_TO_OPEN",
                        "quantity": trade_order.quantity,
                        "instrument": {
                            "symbol": short_put_symbol,
                            "assetType": "OPTION"
                        }
                    },
                    {
                        "instruction": "SELL_TO_OPEN",
                        "quantity": trade_order.quantity,
                        "instrument": {
                            "symbol": short_call_symbol,
                            "assetType": "OPTION"
                        }
                    },
                    {
                        "instruction": "BUY_TO_OPEN",
                        "quantity": trade_order.quantity,
                        "instrument": {
                            "symbol": long_call_symbol,
                            "assetType": "OPTION"
                        }
                    }
                ]
            }
            
            # Submit the 4-leg spread order
            result = self.order_handler.place_complex_option_order(
                order_data=spread_order,
                timestamp=trade_order.created_at
            )
            
            # Process result
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = net_credit  # Use net credit as "price"
                
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ Iron Condor spread order submitted successfully")
                self.logger.info(f"  Spread: {strikes['long_put']}/{strikes['short_put']}/{strikes['short_call']}/{strikes['long_call']}")
                self.logger.info(f"  Net Credit: ${net_credit:.2f}")
                self.logger.info(f"  Contracts: {trade_order.quantity}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | Iron Condor rejection: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ Iron Condor spread order rejected: {result.get('reason', 'Unknown')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error submitting Iron Condor spread order: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | Iron Condor error: {str(e)}"
            return False

    def _submit_pml_option_order(self, trade_order: TradeOrder, instruction: str) -> bool:
        """Submit PML single-leg option order using setup details"""
        try:
            setup = trade_order.signal.setup
            strike_price = setup.get('strike', 0)
            option_type = setup.get('option_type', 'CALL')
            expiration_date_str = setup.get('expiration_date', '')
            current_price = setup.get('current_price', 0)
            
            if not strike_price or not expiration_date_str:
                self.logger.error(f"PML option order missing required setup details")
                return False
            
            # Parse expiration date
            try:
                if 'T' in expiration_date_str:
                    expiration_date = datetime.fromisoformat(expiration_date_str.replace('Z', '+00:00'))
                else:
                    expiration_date = datetime.strptime(expiration_date_str, '%Y-%m-%d')
            except Exception as e:
                self.logger.error(f"Error parsing PML expiration date: {e}")
                return False
            
            # Format expiration for option symbol (YYMMDD)
            exp_formatted = expiration_date.strftime('%y%m%d')
            
            # Construct option symbol using Schwab format
            symbol_padded = f"{trade_order.symbol:<6}"
            option_type_char = 'C' if option_type.upper() in ['CALL', 'C'] else 'P'
            strike_formatted = f"{int(strike_price * 1000):08d}"
            
            option_symbol = f"{symbol_padded}{exp_formatted}{option_type_char}{strike_formatted}"
            
            # Map instruction to option-specific actions
            if instruction == 'BUY':
                option_instruction = 'BUY_TO_OPEN'
            elif instruction == 'SELL':
                option_instruction = 'SELL_TO_CLOSE'
            else:
                self.logger.error(f"Invalid PML option instruction: {instruction}")
                return False
            
            # Use current price from setup or calculate limit price
            limit_price = current_price if current_price > 0 else self._calculate_option_price(trade_order)
            
            result = self.order_handler.place_option_limit_order(
                action_type=option_instruction,
                option_symbol=option_symbol,
                contracts=trade_order.quantity,
                limit_price=limit_price,
                timestamp=trade_order.created_at
            )
            
            # Process result
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = limit_price
                
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ PML option order submitted successfully")
                self.logger.info(f"  Option: {option_symbol}")
                self.logger.info(f"  Action: {option_instruction}")
                self.logger.info(f"  Limit Price: ${limit_price:.2f}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | PML option rejection: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ PML option order rejected: {result.get('reason', 'Unknown')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error submitting PML option order: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | PML option error: {str(e)}"
            return False

    def _submit_generic_option_order(self, trade_order: TradeOrder, instruction: str) -> bool:
        """Submit generic single-leg option order"""
        try:
            # Construct option symbol using generic method
            option_symbol = self._construct_option_symbol(trade_order)
            if not option_symbol:
                self.logger.error(f"Failed to construct option symbol for {trade_order.symbol}")
                return False
            
            # Map instruction to option-specific actions
            if instruction == 'BUY':
                option_instruction = 'BUY_TO_OPEN'
            elif instruction == 'SELL':
                option_instruction = 'SELL_TO_CLOSE'
            else:
                self.logger.error(f"Invalid option instruction: {instruction}")
                return False
            
            # Calculate option limit price
            option_limit_price = self._calculate_option_price(trade_order)
            
            result = self.order_handler.place_option_limit_order(
                action_type=option_instruction,
                option_symbol=option_symbol,
                contracts=trade_order.quantity,
                limit_price=option_limit_price,
                timestamp=trade_order.created_at
            )
            
            # Process result
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = option_limit_price
                
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ Generic option order submitted: {option_instruction} {trade_order.quantity} contracts of {option_symbol} @ ${option_limit_price:.2f}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | Generic option rejection: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ Generic option order rejected: {result.get('reason', 'Unknown')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error submitting generic option order: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | Generic option error: {str(e)}"
            return False

    def _manage_existing_orders(self):
        """Check and manage existing orders using get_all_orders to avoid dangerous assumptions"""
        try:
            if not self.active_orders:
                return
                
            # Use get_all_orders to safely check all orders at once
            # Get orders from the last 24 hours to capture recent orders
            from_time = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            to_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            try:
                all_orders_result = self.order_handler.get_all_orders(
                    from_entered_time=from_time,
                    to_entered_time=to_time,
                    max_results=1000
                )
                
                if 'error' in all_orders_result:
                    error_msg = all_orders_result['error']
                    if '401' in error_msg or 'Unauthorized' in error_msg:
                        self.logger.debug("Authorization error getting orders - tokens may need refresh")
                        # Don't make assumptions about order status during auth errors
                        return
                    else:
                        self.logger.warning(f"Error getting all orders: {error_msg}")
                        return
                
                # Process each active order
                for order_id, order in list(self.active_orders.items()):
                    try:
                        self._update_order_from_all_orders(order, all_orders_result)
                        
                        # Remove completed orders from active list
                        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED]:
                            self.executed_trades.append(order)
                            del self.active_orders[order_id]
                            
                            if order.status == OrderStatus.FILLED:
                                self.logger.info(f"✅ Order filled: {order.symbol} @ ${order.filled_price:.2f}")
                            else:
                                self.logger.info(f"❌ Order {order.status.value}: {order.symbol}")
                                
                    except Exception as e:
                        self.logger.error(f"Error managing order {order_id}: {e}")
                        
            except Exception as e:
                self.logger.error(f"Error calling get_all_orders: {e}")
                # Don't make assumptions about order status on API errors
                return
                    
        except Exception as e:
            self.logger.error(f"Error managing existing orders: {e}")

    def _update_order_from_all_orders(self, order: TradeOrder, all_orders_result: Dict[str, Any]):
        """Update order status from get_all_orders result - safer than individual status calls"""
        try:
            # Extract Schwab order ID from notes if available
            schwab_order_id = None
            if "Schwab Order ID:" in order.notes:
                try:
                    schwab_order_id = order.notes.split("Schwab Order ID: ")[1].split(" |")[0].strip()
                except Exception:
                    pass
            
            if not schwab_order_id:
                # No Schwab order ID available - cannot update status safely
                return
            
            # Search for this order in the all_orders_result
            if isinstance(all_orders_result, list):
                orders_list = all_orders_result
            else:
                orders_list = all_orders_result.get('orders', [])
            
            for schwab_order in orders_list:
                if str(schwab_order.get('orderId', '')) == str(schwab_order_id):
                    # Found our order - update status
                    schwab_status = schwab_order.get('status', '').upper()
                    
                    # Map Schwab status to our OrderStatus with comprehensive status handling
                    if schwab_status in ['FILLED']:
                        order.status = OrderStatus.FILLED
                        order.filled_at = datetime.now()
                        
                        # Try to get fill price from order activities
                        if 'orderActivityCollection' in schwab_order:
                            activities = schwab_order['orderActivityCollection']
                            if activities and len(activities) > 0:
                                execution_legs = activities[0].get('executionLegs', [])
                                if execution_legs and len(execution_legs) > 0:
                                    order.filled_price = float(execution_legs[0].get('price', order.entry_price))
                                else:
                                    order.filled_price = order.entry_price
                        else:
                            order.filled_price = order.entry_price
                            
                    elif schwab_status in ['CANCELED', 'CANCELLED']:
                        order.status = OrderStatus.CANCELLED
                    elif schwab_status in ['REJECTED']:
                        order.status = OrderStatus.REJECTED
                    elif schwab_status in ['EXPIRED']:
                        order.status = OrderStatus.CANCELLED
                    elif schwab_status in ['WORKING']:
                        # Order is active and working in the market
                        order.status = OrderStatus.SUBMITTED
                        self.logger.debug(f"Order {schwab_order_id} is WORKING in the market")
                    elif schwab_status in ['PENDING_ACTIVATION', 'AWAITING_PARENT_ORDER', 'AWAITING_CONDITION', 
                                         'AWAITING_STOP_CONDITION', 'AWAITING_MANUAL_REVIEW', 'ACCEPTED', 
                                         'AWAITING_UR_OUT', 'QUEUED', 'AWAITING_RELEASE_TIME', 
                                         'PENDING_ACKNOWLEDGEMENT', 'PENDING_RECALL']:
                        # Order is submitted but not yet active (e.g., day orders after 3pm CT)
                        order.status = OrderStatus.SUBMITTED
                        self.logger.debug(f"Order {schwab_order_id} status: {schwab_status} (pending activation)")
                    elif schwab_status in ['PENDING_CANCEL']:
                        # Order cancellation is pending
                        order.status = OrderStatus.SUBMITTED  # Keep as submitted until actually cancelled
                        self.logger.debug(f"Order {schwab_order_id} cancellation pending")
                    elif schwab_status in ['PENDING_REPLACE', 'REPLACED']:
                        # Order is being replaced or has been replaced
                        order.status = OrderStatus.SUBMITTED  # Keep tracking the replacement
                        self.logger.debug(f"Order {schwab_order_id} status: {schwab_status}")
                    elif schwab_status in ['NEW', 'UNKNOWN']:
                        # New or unknown status - keep as submitted for safety
                        order.status = OrderStatus.SUBMITTED
                        self.logger.debug(f"Order {schwab_order_id} has status: {schwab_status}")
                    else:
                        # Unknown status - log it but don't change order status
                        self.logger.warning(f"Unknown order status for {schwab_order_id}: {schwab_status}")
                    
                    self.logger.debug(f"Updated order {schwab_order_id} status to {order.status.value}")
                    return
            
            # Order not found in results - this could mean it's very old or there was an error
            # Don't make assumptions about status
            self.logger.debug(f"Order {schwab_order_id} not found in get_all_orders result")
            
        except Exception as e:
            self.logger.error(f"Error updating order from all_orders result: {e}")

    def _check_order_status(self, order: TradeOrder):
        """Check the status of an order using Schwab API"""
        try:
            if order.status != OrderStatus.SUBMITTED:
                return
            
            # Extract Schwab order ID from notes if available
            schwab_order_id = None
            if "Schwab Order ID:" in order.notes:
                try:
                    schwab_order_id = order.notes.split("Schwab Order ID: ")[1].split(" |")[0].strip()
                except Exception:
                    pass
            
            if schwab_order_id:
                # Use existing order handler to check status
                status_result = self.order_handler.get_order_status(schwab_order_id)
                
                if 'error' not in status_result:
                    schwab_status = status_result.get('status', '').upper()
                    
                    # Map Schwab status to our OrderStatus
                    if schwab_status in ['FILLED']:
                        order.status = OrderStatus.FILLED
                        order.filled_at = datetime.now()
                        # Try to get fill price from result
                        if 'orderActivityCollection' in status_result:
                            activities = status_result['orderActivityCollection']
                            if activities and len(activities) > 0:
                                execution_legs = activities[0].get('executionLegs', [])
                                if execution_legs and len(execution_legs) > 0:
                                    order.filled_price = float(execution_legs[0].get('price', order.entry_price))
                                else:
                                    order.filled_price = order.entry_price
                        else:
                            order.filled_price = order.entry_price
                            
                    elif schwab_status in ['CANCELED', 'CANCELLED']:
                        order.status = OrderStatus.CANCELLED
                    elif schwab_status in ['REJECTED']:
                        order.status = OrderStatus.REJECTED
                    elif schwab_status in ['EXPIRED']:
                        order.status = OrderStatus.CANCELLED
                    # WORKING, QUEUED, ACCEPTED, etc. remain as SUBMITTED
                    
                else:
                    self.logger.warning(f"Error checking order status for {schwab_order_id}: {status_result.get('error')}")
            else:
                # Fallback: simulate order progression for orders without Schwab ID
                time_since_submission = datetime.now() - order.created_at
                if time_since_submission.total_seconds() > 60:  # 1 minute timeout
                    order.status = OrderStatus.FILLED
                    order.filled_at = datetime.now()
                    order.filled_price = order.entry_price
                    
        except Exception as e:
            self.logger.error(f"Error checking order status: {e}")

    def _log_trade_execution(self, trade_order: TradeOrder):
        """Log trade execution details"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'order_id': trade_order.order_id,
                'symbol': trade_order.symbol,
                'strategy': trade_order.strategy,
                'signal_type': trade_order.signal.signal_type,
                'confidence': trade_order.signal.confidence,
                'quantity': trade_order.quantity,
                'entry_price': trade_order.entry_price,
                'stop_loss': trade_order.stop_loss,
                'profit_target': trade_order.profit_target,
                'risk_amount': abs(trade_order.entry_price - trade_order.stop_loss) * trade_order.quantity,
                'entry_reason': trade_order.signal.entry_reason
            }
            
            # Append to trade log file
            log_file = 'auto_trades.json'
            trades_log = []
            
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    trades_log = json.load(f)
            
            trades_log.append(log_entry)
            
            with open(log_file, 'w') as f:
                json.dump(trades_log, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error logging trade execution: {e}")

    def _save_order_to_file(self, trade_order: TradeOrder):
        """Save order to active orders file"""
        try:
            orders_file = 'orders/active_orders.json'
            
            # Ensure orders directory exists
            os.makedirs('orders', exist_ok=True)
            
            # Load existing orders
            active_orders_data = {}
            if os.path.exists(orders_file):
                with open(orders_file, 'r') as f:
                    active_orders_data = json.load(f)
            
            # Add new order - ensure we're working with a dictionary
            if not isinstance(active_orders_data, dict):
                active_orders_data = {}
            
            order_data = {
                'order_id': trade_order.order_id,
                'symbol': trade_order.symbol,
                'strategy': trade_order.strategy,
                'trade_type': trade_order.trade_type.value,
                'quantity': trade_order.quantity,
                'entry_price': trade_order.entry_price,
                'stop_loss': trade_order.stop_loss,
                'profit_target': trade_order.profit_target,
                'status': trade_order.status.value,
                'created_at': trade_order.created_at.isoformat(),
                'signal': {
                    'signal_type': trade_order.signal.signal_type,
                    'confidence': trade_order.signal.confidence,
                    'entry_reason': trade_order.signal.entry_reason
                }
            }
            
            active_orders_data[trade_order.order_id] = order_data
            
            # Save updated orders
            with open(orders_file, 'w') as f:
                json.dump(active_orders_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving order to file: {e}")

    def _construct_option_symbol(self, trade_order: TradeOrder) -> Optional[str]:
        """Construct option symbol for the trade order"""
        try:
            # Get option parameters from signal setup or use defaults
            setup = trade_order.signal.setup or {}
            
            # Default option parameters (would be more sophisticated in production)
            expiration_days = setup.get('expiration_days', 30)  # 30 days from now
            option_type = setup.get('option_type', 'C')  # Default to Call
            strike_offset = setup.get('strike_offset', 0.05)  # 5% OTM by default
            
            # Calculate expiration date
            expiration_date = datetime.now() + timedelta(days=expiration_days)
            # Round to next Friday (typical option expiration)
            days_ahead = 4 - expiration_date.weekday()  # Friday is 4
            if days_ahead <= 0:
                days_ahead += 7
            expiration_date += timedelta(days=days_ahead)
            
            # Calculate strike price based on current price and offset
            current_price = trade_order.entry_price
            if trade_order.signal.signal_type in ['BUY', 'STRONG_BUY']:
                # For buy signals, use slightly OTM calls or ITM puts
                if option_type == 'C':
                    strike_price = current_price * (1 + strike_offset)
                else:  # Put
                    strike_price = current_price * (1 - strike_offset)
            else:  # SELL signals
                # For sell signals, use slightly OTM puts or ITM calls
                if option_type == 'P':
                    strike_price = current_price * (1 + strike_offset)
                else:  # Call
                    strike_price = current_price * (1 - strike_offset)
            
            # Round strike price to nearest $0.50 or $1.00
            if strike_price < 50:
                strike_price = round(strike_price * 2) / 2  # Round to nearest $0.50
            else:
                strike_price = round(strike_price)  # Round to nearest $1.00
            
            # Use order handler's option symbol construction method
            option_symbol = self.order_handler.create_option_symbol(
                underlying=trade_order.symbol,
                expiration_date=expiration_date.strftime('%Y-%m-%d'),
                option_type=option_type,
                strike_price=strike_price
            )
            
            if option_symbol:
                self.logger.info(f"Constructed option symbol: {option_symbol} (Strike: ${strike_price}, Exp: {expiration_date.strftime('%Y-%m-%d')})")
                return option_symbol
            else:
                self.logger.error(f"Failed to construct option symbol for {trade_order.symbol}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error constructing option symbol: {e}")
            return None

    def _calculate_option_price(self, trade_order: TradeOrder) -> float:
        """Calculate option limit price (simplified Black-Scholes approximation)"""
        try:
            # This is a simplified option pricing model
            # In production, you'd use real option pricing data or a proper Black-Scholes model
            
            setup = trade_order.signal.setup or {}
            current_price = trade_order.entry_price
            
            # Get option parameters
            strike_offset = setup.get('strike_offset', 0.05)
            expiration_days = setup.get('expiration_days', 30)
            option_type = setup.get('option_type', 'C')
            
            # Calculate strike price (same logic as symbol construction)
            if trade_order.signal.signal_type in ['BUY', 'STRONG_BUY']:
                if option_type == 'C':
                    strike_price = current_price * (1 + strike_offset)
                else:
                    strike_price = current_price * (1 - strike_offset)
            else:
                if option_type == 'P':
                    strike_price = current_price * (1 + strike_offset)
                else:
                    strike_price = current_price * (1 - strike_offset)
            
            # Round strike price
            if strike_price < 50:
                strike_price = round(strike_price * 2) / 2
            else:
                strike_price = round(strike_price)
            
            # Simplified option pricing based on intrinsic + time value
            if option_type == 'C':  # Call option
                intrinsic_value = max(0, current_price - strike_price)
            else:  # Put option
                intrinsic_value = max(0, strike_price - current_price)
            
            # Time value approximation (very simplified)
            time_value = (expiration_days / 365) * current_price * 0.02  # 2% annualized
            
            # Add some volatility premium
            volatility_premium = current_price * 0.01  # 1% of stock price
            
            option_price = intrinsic_value + time_value + volatility_premium
            
            # Ensure minimum option price
            option_price = max(0.05, option_price)  # Minimum $0.05
            
            # Round to nearest $0.05
            option_price = round(option_price * 20) / 20
            
            self.logger.info(f"Calculated option price: ${option_price:.2f} (Intrinsic: ${intrinsic_value:.2f}, Time: ${time_value:.2f})")
            
            return option_price
            
        except Exception as e:
            self.logger.error(f"Error calculating option price: {e}")
            # Fallback to simple percentage of stock price
            return max(0.50, trade_order.entry_price * 0.02)  # 2% of stock price, minimum $0.50

    def get_status(self) -> Dict[str, Any]:
        """Get current trading engine status"""
        try:
            return {
                'running': self.running,
                'active_orders': len(self.active_orders),
                'executed_trades': len(self.executed_trades),
                'last_check': datetime.now().isoformat(),
                'config': {
                    'auto_trading_enabled': self.config.get('enable_auto_trading', False),
                    'monitor_interval': self.config.get('monitor_interval', 10),
                    'max_positions': self.config.get('max_total_positions', 10)
                },
                'orders': [
                    {
                        'order_id': order.order_id,
                        'symbol': order.symbol,
                        'strategy': order.strategy,
                        'status': order.status.value,
                        'created_at': order.created_at.isoformat()
                    }
                    for order in self.active_orders.values()
                ]
            }
        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {'error': str(e)}

def main():
    """Main function to run trading engine"""
    import signal
    import sys
    
    def signal_handler(signum, frame):
        print("\n🛑 Shutting down trading engine...")
        engine.stop_monitoring()
        sys.exit(0)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start trading engine
    engine = TradingEngine()
    
    try:
        engine.start_monitoring()
        
        # Keep main thread alive
        while engine.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Received interrupt signal")
    finally:
        engine.stop_monitoring()
        print("👋 Trading engine stopped")

if __name__ == "__main__":
    main()
