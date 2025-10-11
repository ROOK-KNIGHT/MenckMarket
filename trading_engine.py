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
        
        # Configuration
        self.config = self.load_trading_config()
        
        self.logger.info("Trading Engine initialized")
        self.logger.info(f"Monitor interval: {self.config.get('monitor_interval', 10)} seconds")
        self.logger.info(f"Max positions per strategy: {self.config.get('max_positions_per_strategy', 3)}")

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
        
        self.logger.info("🛑 Trading Engine stopped")

    def _monitor_signals(self):
        """Main monitoring loop"""
        self.logger.info("📡 Signal monitoring loop started")
        
        while self.running:
            try:
                # Check if auto-trading is enabled
                if not self.config.get('enable_auto_trading', False):
                    time.sleep(self.config.get('monitor_interval', 10))
                    continue
                
                # Monitor each strategy
                strategies = ['pml', 'iron_condor', 'divergence']
                
                for strategy in strategies:
                    if not self.config.get('strategies', {}).get(strategy, {}).get('enabled', True):
                        continue
                    
                    try:
                        self._check_strategy_signals(strategy)
                    except Exception as e:
                        self.logger.error(f"Error checking {strategy} signals: {e}")
                
                # Check and manage existing orders
                self._manage_existing_orders()
                
                # Sleep until next check
                time.sleep(self.config.get('monitor_interval', 10))
                
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
            # Check if auto_approve is enabled
            if not signal_data.get('auto_approve', False):
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
            
            # Check if we already processed this signal
            signal_key = f"{strategy}_{symbol}_{signal_timestamp}"
            if signal_key in self.last_signal_check:
                return False
            
            # Check position limits and boxed position prevention
            if not self._check_position_limits(strategy, symbol, signal_type):
                return False
            
            # Check risk management
            if not self._check_risk_limits(strategy, symbol, signal_data):
                return False
            
            # Mark signal as checked
            self.last_signal_check[signal_key] = datetime.now()
            
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
                
                # Log trade details
                self._log_trade_execution(trade_order)
                
                # Save order to file
                self._save_order_to_file(trade_order)
                
            else:
                self.logger.error(f"❌ Failed to submit {strategy} order for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error executing trade for {strategy} {symbol}: {e}")

    def _determine_trade_parameters(self, strategy: str, symbol: str, signal: TradingSignal) -> Tuple[TradeType, int, float]:
        """Determine trade type, quantity, and entry price"""
        try:
            # Force all trades to be STOCK type for testing
            trade_type = TradeType.STOCK
            
            # Get current market price (simplified - would use real market data)
            entry_price = self._get_current_price(symbol)
            if entry_price <= 0:
                return trade_type, 0, 0
            
            # Calculate position size
            base_quantity = int(signal.position_size)
            
            # Apply strategy multiplier
            strategy_multiplier = self.config.get('strategies', {}).get(strategy, {}).get('position_size_multiplier', 1.0)
            quantity = max(1, int(base_quantity * strategy_multiplier))
            
            # Force all trades to be STOCK type - no options for now
            # This ensures all signals will be executed as stock orders
            self.logger.info(f"Forcing {strategy} {symbol} to STOCK type for testing")
            
            return trade_type, quantity, entry_price
            
        except Exception as e:
            self.logger.error(f"Error determining trade parameters: {e}")
            return TradeType.STOCK, 0, 0

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
            self.logger.info(f"  Stop Loss: ${trade_order.stop_loss:.2f}")
            self.logger.info(f"  Profit Target: ${trade_order.profit_target:.2f}")
            
            # Determine order instruction based on signal type
            if trade_order.signal.signal_type in ['BUY', 'STRONG_BUY']:
                instruction = 'BUY'
            elif trade_order.signal.signal_type in ['SELL', 'STRONG_SELL']:
                instruction = 'SELL'
            else:
                self.logger.error(f"Invalid signal type for order: {trade_order.signal.signal_type}")
                trade_order.status = OrderStatus.REJECTED
                return False
            
            # Submit order based on trade type
            if trade_order.trade_type == TradeType.STOCK:
                # Submit stock order using existing order handler with proper parameter name
                result = self.order_handler.place_market_order(
                    action_type=instruction,  # Use correct parameter name from order handler
                    symbol=trade_order.symbol,
                    shares=trade_order.quantity,
                    current_price=trade_order.entry_price,
                    timestamp=trade_order.created_at
                )
                
            elif trade_order.trade_type == TradeType.OPTION:
                # For options, we'd need to construct the option symbol and use option methods
                # This is simplified - would need proper option symbol construction
                self.logger.warning(f"Option orders not fully implemented yet for {trade_order.symbol}")
                result = {'status': 'rejected', 'reason': 'Option orders not implemented'}
                
            else:
                self.logger.error(f"Unsupported trade type: {trade_order.trade_type}")
                result = {'status': 'rejected', 'reason': 'Unsupported trade type'}
            
            # Process result from order handler
            if result.get('status') == 'submitted':
                trade_order.status = OrderStatus.SUBMITTED
                trade_order.filled_price = result.get('fill_price', trade_order.entry_price)
                
                # Store Schwab order ID if available
                if 'order_id' in result:
                    trade_order.notes += f" | Schwab Order ID: {result['order_id']}"
                
                self.logger.info(f"✅ Order submitted successfully to Schwab: {result.get('order_id', 'N/A')}")
                return True
                
            else:
                trade_order.status = OrderStatus.REJECTED
                trade_order.notes += f" | Rejection reason: {result.get('reason', 'Unknown')}"
                self.logger.error(f"❌ Order rejected by Schwab: {result.get('reason', 'Unknown')}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error submitting order to Schwab: {e}")
            trade_order.status = OrderStatus.FAILED
            trade_order.notes += f" | Error: {str(e)}"
            return False

    def _manage_existing_orders(self):
        """Check and manage existing orders"""
        try:
            for order_id, order in list(self.active_orders.items()):
                try:
                    # Check order status (would integrate with broker API)
                    self._check_order_status(order)
                    
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
            self.logger.error(f"Error managing existing orders: {e}")

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
