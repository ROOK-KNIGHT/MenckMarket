#!/usr/bin/env python3
"""
VolFlow Alert Manager

This module handles alert generation and notification dispatch for trading events:
1. Monitors trading signals and position changes
2. Evaluates alert conditions based on user preferences
3. Sends notifications through configured channels (Email, Slack, Telegram)
4. Manages alert frequency and quiet hours
5. Integrates with risk management system

Alert Types:
- Strategy Signal Alerts (new buy/sell signals)
- Position Alerts (large gains/losses, position size limits)
- Risk Management Alerts (stop loss hits, daily loss limits)
- Volatility Alerts (VIX spikes, market condition changes)
- System Alerts (connection issues, data problems)
"""

import json
import logging
import asyncio
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlertType(Enum):
    """Types of alerts that can be generated"""
    STRATEGY_SIGNAL = "strategy_signal"
    POSITION_ALERT = "position_alert"
    RISK_MANAGEMENT = "risk_management"
    VOLATILITY_SPIKE = "volatility_spike"
    SYSTEM_ALERT = "system_alert"
    MARKET_CONDITION = "market_condition"

class AlertPriority(Enum):
    """Alert priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

class AlertCategory(Enum):
    """Alert categories for organization"""
    TRADING = "trading"
    RISK = "risk"
    SYSTEM = "system"
    MARKET = "market"

@dataclass
class AlertCondition:
    """Defines conditions for triggering alerts"""
    alert_type: AlertType
    condition_name: str
    enabled: bool = True
    threshold_value: Optional[float] = None
    comparison_operator: str = ">"  # >, <, >=, <=, ==, !=
    lookback_period: int = 1  # minutes
    cooldown_period: int = 5  # minutes to wait before sending same alert again
    
@dataclass
class AlertPreferences:
    """User preferences for alerts and notifications"""
    # Alert Types
    max_loss_alert: bool = True
    max_loss_threshold: float = 1000.0
    volatility_alert: bool = True
    vix_threshold: float = 30.0
    position_size_alert: bool = True
    signal_alert: bool = True
    
    # Notification Channels
    email_enabled: bool = False
    email_address: str = ""
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#trading-alerts"
    
    # Preferences
    alert_frequency: str = "5min"  # immediate, 5min, 15min, 30min, 1hour
    quiet_hours_enabled: bool = False
    quiet_hours_start: str = "22:00"
    quiet_hours_end: str = "08:00"
    
    # Risk thresholds
    daily_loss_limit_pct: float = 5.0
    position_size_limit_pct: float = 10.0
    account_risk_limit_pct: float = 25.0

@dataclass
class Alert:
    """Represents a generated alert"""
    alert_id: str
    alert_type: AlertType
    priority: AlertPriority
    category: AlertCategory
    title: str
    message: str
    symbol: Optional[str] = None
    strategy: Optional[str] = None
    current_value: Optional[float] = None
    threshold_value: Optional[float] = None
    timestamp: datetime = None
    data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.data is None:
            self.data = {}
        if self.alert_id is None:
            self.alert_id = f"{self.alert_type.value}_{int(self.timestamp.timestamp())}"

class AlertManager:
    """Main alert management system"""
    
    def __init__(self, config_file: str = None):
        """Initialize the Alert Manager"""
        self.config_file = config_file or "alert_config.json"
        self.preferences = self.load_preferences()
        self.alert_conditions = self.setup_default_conditions()
        self.alert_history = []
        self.last_alert_times = {}  # Track cooldown periods
        self.notification_server_url = "http://localhost:5002"
        
        # Load existing alert history
        self.load_alert_history()
        
        logger.info("AlertManager initialized")
        logger.info(f"Email notifications: {'enabled' if self.preferences.email_enabled else 'disabled'}")
        logger.info(f"Telegram notifications: {'enabled' if self.preferences.telegram_enabled else 'disabled'}")
        logger.info(f"Slack notifications: {'enabled' if self.preferences.slack_enabled else 'disabled'}")
    
    def load_preferences(self) -> AlertPreferences:
        """Load alert preferences from configuration file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                
                # Extract preferences from nested structure (matching frontend format)
                alerts = config_data.get('alerts', {})
                notifications = config_data.get('notifications', {})
                preferences_data = config_data.get('preferences', {})
                
                return AlertPreferences(
                    # Alert types
                    max_loss_alert=alerts.get('maxLossAlert', True),
                    max_loss_threshold=alerts.get('maxLossThreshold', 1000.0),
                    volatility_alert=alerts.get('volatilityAlert', True),
                    vix_threshold=alerts.get('vixThreshold', 30.0),
                    position_size_alert=alerts.get('positionSizeAlert', True),
                    signal_alert=alerts.get('signalAlert', True),
                    
                    # Email notifications
                    email_enabled=notifications.get('email', {}).get('enabled', False),
                    email_address=notifications.get('email', {}).get('address', ''),
                    
                    # Telegram notifications
                    telegram_enabled=notifications.get('telegram', {}).get('enabled', False),
                    telegram_bot_token=notifications.get('telegram', {}).get('botToken', ''),
                    telegram_chat_id=notifications.get('telegram', {}).get('chatId', ''),
                    
                    # Slack notifications
                    slack_enabled=notifications.get('slack', {}).get('enabled', False),
                    slack_webhook_url=notifications.get('slack', {}).get('webhookUrl', ''),
                    slack_channel=notifications.get('slack', {}).get('channel', '#trading-alerts'),
                    
                    # Preferences
                    alert_frequency=preferences_data.get('alertFrequency', '5min'),
                    quiet_hours_enabled=preferences_data.get('quietHours', {}).get('enabled', False),
                    quiet_hours_start=preferences_data.get('quietHours', {}).get('start', '22:00'),
                    quiet_hours_end=preferences_data.get('quietHours', {}).get('end', '08:00')
                )
            else:
                logger.info(f"Config file {self.config_file} not found, using defaults")
                return AlertPreferences()
                
        except Exception as e:
            logger.error(f"Error loading preferences: {e}")
            return AlertPreferences()
    
    def save_preferences(self):
        """Save current preferences to configuration file"""
        try:
            config_data = {
                'alerts': {
                    'maxLossAlert': self.preferences.max_loss_alert,
                    'maxLossThreshold': self.preferences.max_loss_threshold,
                    'volatilityAlert': self.preferences.volatility_alert,
                    'vixThreshold': self.preferences.vix_threshold,
                    'positionSizeAlert': self.preferences.position_size_alert,
                    'signalAlert': self.preferences.signal_alert
                },
                'notifications': {
                    'email': {
                        'enabled': self.preferences.email_enabled,
                        'address': self.preferences.email_address
                    },
                    'telegram': {
                        'enabled': self.preferences.telegram_enabled,
                        'botToken': self.preferences.telegram_bot_token,
                        'chatId': self.preferences.telegram_chat_id
                    },
                    'slack': {
                        'enabled': self.preferences.slack_enabled,
                        'webhookUrl': self.preferences.slack_webhook_url,
                        'channel': self.preferences.slack_channel
                    }
                },
                'preferences': {
                    'alertFrequency': self.preferences.alert_frequency,
                    'quietHours': {
                        'enabled': self.preferences.quiet_hours_enabled,
                        'start': self.preferences.quiet_hours_start,
                        'end': self.preferences.quiet_hours_end
                    }
                },
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
                
            logger.info("Alert preferences saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving preferences: {e}")
    
    def setup_default_conditions(self) -> List[AlertCondition]:
        """Setup default alert conditions"""
        conditions = [
            # Strategy signal alerts
            AlertCondition(
                alert_type=AlertType.STRATEGY_SIGNAL,
                condition_name="new_buy_signal",
                enabled=self.preferences.signal_alert,
                cooldown_period=1  # 1 minute cooldown for signals
            ),
            AlertCondition(
                alert_type=AlertType.STRATEGY_SIGNAL,
                condition_name="new_sell_signal",
                enabled=self.preferences.signal_alert,
                cooldown_period=1
            ),
            
            # Position alerts
            AlertCondition(
                alert_type=AlertType.POSITION_ALERT,
                condition_name="max_loss_exceeded",
                enabled=self.preferences.max_loss_alert,
                threshold_value=self.preferences.max_loss_threshold,
                comparison_operator="<",  # Loss is negative
                cooldown_period=10  # 10 minute cooldown for loss alerts
            ),
            AlertCondition(
                alert_type=AlertType.POSITION_ALERT,
                condition_name="position_size_exceeded",
                enabled=self.preferences.position_size_alert,
                threshold_value=self.preferences.position_size_limit_pct,
                comparison_operator=">",
                cooldown_period=15
            ),
            
            # Risk management alerts
            AlertCondition(
                alert_type=AlertType.RISK_MANAGEMENT,
                condition_name="daily_loss_limit",
                enabled=True,
                threshold_value=self.preferences.daily_loss_limit_pct,
                comparison_operator="<",
                cooldown_period=30  # 30 minute cooldown for risk alerts
            ),
            AlertCondition(
                alert_type=AlertType.RISK_MANAGEMENT,
                condition_name="account_risk_limit",
                enabled=True,
                threshold_value=self.preferences.account_risk_limit_pct,
                comparison_operator=">",
                cooldown_period=30
            ),
            
            # Volatility alerts
            AlertCondition(
                alert_type=AlertType.VOLATILITY_SPIKE,
                condition_name="vix_spike",
                enabled=self.preferences.volatility_alert,
                threshold_value=self.preferences.vix_threshold,
                comparison_operator=">",
                cooldown_period=60  # 1 hour cooldown for volatility alerts
            )
        ]
        
        return conditions
    
    def load_alert_history(self):
        """Load alert history from file"""
        try:
            history_file = "alert_history.json"
            if os.path.exists(history_file):
                with open(history_file, 'r') as f:
                    history_data = json.load(f)
                
                # Load recent alerts (last 24 hours)
                cutoff_time = datetime.now() - timedelta(hours=24)
                for alert_data in history_data.get('alerts', []):
                    alert_time = datetime.fromisoformat(alert_data['timestamp'])
                    if alert_time > cutoff_time:
                        alert = Alert(
                            alert_id=alert_data['alert_id'],
                            alert_type=AlertType(alert_data['alert_type']),
                            priority=AlertPriority(alert_data['priority']),
                            category=AlertCategory(alert_data['category']),
                            title=alert_data['title'],
                            message=alert_data['message'],
                            symbol=alert_data.get('symbol'),
                            strategy=alert_data.get('strategy'),
                            current_value=alert_data.get('current_value'),
                            threshold_value=alert_data.get('threshold_value'),
                            timestamp=alert_time,
                            data=alert_data.get('data', {})
                        )
                        self.alert_history.append(alert)
                
                logger.info(f"Loaded {len(self.alert_history)} recent alerts from history")
                
        except Exception as e:
            logger.error(f"Error loading alert history: {e}")
    
    def save_alert_history(self):
        """Save alert history to file"""
        try:
            history_file = "alert_history.json"
            
            # Keep only last 7 days of alerts
            cutoff_time = datetime.now() - timedelta(days=7)
            recent_alerts = [alert for alert in self.alert_history if alert.timestamp > cutoff_time]
            
            history_data = {
                'alerts': [asdict(alert) for alert in recent_alerts],
                'last_updated': datetime.now().isoformat()
            }
            
            with open(history_file, 'w') as f:
                json.dump(history_data, f, indent=2, default=str)
                
            logger.info(f"Saved {len(recent_alerts)} alerts to history")
            
        except Exception as e:
            logger.error(f"Error saving alert history: {e}")
    
    def is_quiet_hours(self) -> bool:
        """Check if current time is within quiet hours"""
        if not self.preferences.quiet_hours_enabled:
            return False
        
        try:
            now = datetime.now().time()
            start_time = datetime.strptime(self.preferences.quiet_hours_start, '%H:%M').time()
            end_time = datetime.strptime(self.preferences.quiet_hours_end, '%H:%M').time()
            
            # Handle overnight quiet hours (e.g., 22:00 to 08:00)
            if start_time > end_time:
                return now >= start_time or now <= end_time
            else:
                return start_time <= now <= end_time
                
        except Exception as e:
            logger.error(f"Error checking quiet hours: {e}")
            return False
    
    def is_cooldown_active(self, alert_key: str, cooldown_minutes: int) -> bool:
        """Check if alert is in cooldown period"""
        if alert_key not in self.last_alert_times:
            return False
        
        last_alert_time = self.last_alert_times[alert_key]
        cooldown_period = timedelta(minutes=cooldown_minutes)
        
        return datetime.now() - last_alert_time < cooldown_period
    
    def should_send_alert(self, alert: Alert, condition: AlertCondition) -> bool:
        """Determine if alert should be sent based on preferences and conditions"""
        # Check if alert type is enabled
        if not condition.enabled:
            return False
        
        # Check quiet hours
        if self.is_quiet_hours() and alert.priority != AlertPriority.CRITICAL:
            logger.info(f"Suppressing alert {alert.alert_id} due to quiet hours")
            return False
        
        # Check cooldown period
        alert_key = f"{alert.alert_type.value}_{alert.symbol or 'global'}"
        if self.is_cooldown_active(alert_key, condition.cooldown_period):
            logger.info(f"Suppressing alert {alert.alert_id} due to cooldown")
            return False
        
        return True
    
    async def send_notification(self, alert: Alert) -> bool:
        """Send notification through configured channels"""
        if self.is_quiet_hours() and alert.priority != AlertPriority.CRITICAL:
            logger.info(f"Skipping notification for alert {alert.alert_id} - quiet hours")
            return False
        
        try:
            # Prepare notification data
            notification_data = {
                'title': alert.title,
                'message': alert.message,
                'priority': alert.priority.value,
                'category': alert.category.value,
                'data': {
                    'symbol': alert.symbol,
                    'strategy': alert.strategy,
                    'current_value': alert.current_value,
                    'threshold_value': alert.threshold_value,
                    'alert_type': alert.alert_type.value,
                    'timestamp': alert.timestamp.isoformat(),
                    **alert.data
                }
            }
            
            # Send to notification server
            response = requests.post(
                f"{self.notification_server_url}/notify",
                json=notification_data,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Notification sent successfully for alert {alert.alert_id}")
                logger.info(f"Notification results: {result.get('results', {})}")
                return True
            else:
                logger.error(f"Notification server error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending notification for alert {alert.alert_id}: {e}")
            return False
    
    def create_strategy_signal_alert(self, signal_data: Dict[str, Any]) -> Optional[Alert]:
        """Create alert for new strategy signals"""
        try:
            symbol = signal_data.get('symbol', 'Unknown')
            signal_type = signal_data.get('signal_type', 'NO_SIGNAL')
            strategy = signal_data.get('strategy', 'Unknown')
            confidence = signal_data.get('confidence', 0.0)
            entry_reason = signal_data.get('entry_reason', '')
            
            # Only alert on actionable signals
            if signal_type in ['NO_SIGNAL', 'HOLD']:
                return None
            
            # Determine priority based on signal strength
            if signal_type in ['STRONG_BUY', 'STRONG_SELL']:
                priority = AlertPriority.HIGH
            elif confidence > 0.8:
                priority = AlertPriority.HIGH
            elif confidence > 0.6:
                priority = AlertPriority.NORMAL
            else:
                priority = AlertPriority.LOW
            
            # Create alert
            alert = Alert(
                alert_id=f"signal_{strategy}_{symbol}_{int(datetime.now().timestamp())}",
                alert_type=AlertType.STRATEGY_SIGNAL,
                priority=priority,
                category=AlertCategory.TRADING,
                title=f"🎯 {strategy.upper()} Signal: {signal_type}",
                message=f"New {signal_type} signal for {symbol} from {strategy} strategy.\n\nReason: {entry_reason}\nConfidence: {confidence*100:.1f}%",
                symbol=symbol,
                strategy=strategy,
                current_value=confidence,
                data={
                    'signal_type': signal_type,
                    'entry_reason': entry_reason,
                    'position_size': signal_data.get('position_size', 0),
                    'stop_loss': signal_data.get('stop_loss', 0),
                    'profit_target': signal_data.get('profit_target', 0)
                }
            )
            
            return alert
            
        except Exception as e:
            logger.error(f"Error creating strategy signal alert: {e}")
            return None
    
    def create_position_alert(self, position_data: Dict[str, Any], alert_type: str) -> Optional[Alert]:
        """Create alert for position-related events"""
        try:
            symbol = position_data.get('symbol', 'Unknown')
            current_pl = position_data.get('unrealized_pl', 0)
            market_value = position_data.get('market_value', 0)
            
            if alert_type == "max_loss_exceeded":
                alert = Alert(
                    alert_id=f"loss_{symbol}_{int(datetime.now().timestamp())}",
                    alert_type=AlertType.POSITION_ALERT,
                    priority=AlertPriority.HIGH,
                    category=AlertCategory.RISK,
                    title=f"⚠️ Maximum Loss Alert: {symbol}",
                    message=f"Position in {symbol} has exceeded maximum loss threshold.\n\nCurrent P/L: ${current_pl:,.2f}\nThreshold: ${self.preferences.max_loss_threshold:,.2f}",
                    symbol=symbol,
                    current_value=current_pl,
                    threshold_value=self.preferences.max_loss_threshold,
                    data={
                        'market_value': market_value,
                        'position_type': position_data.get('position_type', 'Unknown')
                    }
                )
                return alert
            
            elif alert_type == "position_size_exceeded":
                # Calculate position size as percentage of account
                account_equity = self.get_current_account_equity()
                position_pct = (abs(market_value) / account_equity * 100) if account_equity > 0 else 0
                
                alert = Alert(
                    alert_id=f"size_{symbol}_{int(datetime.now().timestamp())}",
                    alert_type=AlertType.POSITION_ALERT,
                    priority=AlertPriority.NORMAL,
                    category=AlertCategory.RISK,
                    title=f"📏 Position Size Alert: {symbol}",
                    message=f"Position in {symbol} exceeds size limit.\n\nPosition Size: {position_pct:.1f}% of account\nLimit: {self.preferences.position_size_limit_pct:.1f}%",
                    symbol=symbol,
                    current_value=position_pct,
                    threshold_value=self.preferences.position_size_limit_pct,
                    data={
                        'market_value': market_value,
                        'account_equity': account_equity
                    }
                )
                return alert
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating position alert: {e}")
            return None
    
    def create_risk_management_alert(self, risk_data: Dict[str, Any], alert_type: str) -> Optional[Alert]:
        """Create alert for risk management events"""
        try:
            if alert_type == "daily_loss_limit":
                daily_pl = risk_data.get('daily_pl', 0)
                account_equity = risk_data.get('account_equity', 0)
                daily_pl_pct = (daily_pl / account_equity * 100) if account_equity > 0 else 0
                
                alert = Alert(
                    alert_id=f"daily_loss_{int(datetime.now().timestamp())}",
                    alert_type=AlertType.RISK_MANAGEMENT,
                    priority=AlertPriority.CRITICAL,
                    category=AlertCategory.RISK,
                    title="🚨 Daily Loss Limit Exceeded",
                    message=f"Daily loss limit has been exceeded!\n\nDaily P/L: ${daily_pl:,.2f} ({daily_pl_pct:.1f}%)\nLimit: {self.preferences.daily_loss_limit_pct:.1f}%\n\nConsider stopping trading for today.",
                    current_value=daily_pl_pct,
                    threshold_value=self.preferences.daily_loss_limit_pct,
                    data={
                        'daily_pl': daily_pl,
                        'account_equity': account_equity,
                        'recommendation': 'STOP_TRADING'
                    }
                )
                return alert
            
            elif alert_type == "account_risk_limit":
                risk_utilization = risk_data.get('risk_utilization', 0)
                
                alert = Alert(
                    alert_id=f"account_risk_{int(datetime.now().timestamp())}",
                    alert_type=AlertType.RISK_MANAGEMENT,
                    priority=AlertPriority.HIGH,
                    category=AlertCategory.RISK,
                    title="⚠️ Account Risk Limit Exceeded",
                    message=f"Account risk utilization has exceeded the limit!\n\nCurrent Risk: {risk_utilization:.1f}%\nLimit: {self.preferences.account_risk_limit_pct:.1f}%\n\nConsider reducing position sizes.",
                    current_value=risk_utilization,
                    threshold_value=self.preferences.account_risk_limit_pct,
                    data={
                        'recommendation': 'REDUCE_POSITIONS'
                    }
                )
                return alert
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating risk management alert: {e}")
            return None
    
    def create_volatility_alert(self, market_data: Dict[str, Any]) -> Optional[Alert]:
        """Create alert for volatility spikes"""
        try:
            vix_value = market_data.get('vix', 0)
            
            if vix_value > self.preferences.vix_threshold:
                alert = Alert(
                    alert_id=f"vix_spike_{int(datetime.now().timestamp())}",
                    alert_type=AlertType.VOLATILITY_SPIKE,
                    priority=AlertPriority.HIGH,
                    category=AlertCategory.MARKET,
                    title=f"📈 Volatility Spike Alert",
                    message=f"VIX has spiked above threshold!\n\nCurrent VIX: {vix_value:.1f}\nThreshold: {self.preferences.vix_threshold:.1f}\n\nMarket volatility is elevated - exercise caution.",
                    current_value=vix_value,
                    threshold_value=self.preferences.vix_threshold,
                    data={
                        'market_condition': market_data.get('market_condition', 'Unknown'),
                        'recommendation': 'EXERCISE_CAUTION'
                    }
                )
                return alert
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating volatility alert: {e}")
            return None
    
    def get_current_account_equity(self) -> float:
        """Get current account equity (placeholder - would integrate with account data)"""
        try:
            # This would integrate with your account data system
            # For now, return a default value
            return 125000.0  # Placeholder
        except Exception as e:
            logger.error(f"Error getting account equity: {e}")
            return 125000.0
    
    async def process_alert(self, alert: Alert) -> bool:
        """Process and potentially send an alert"""
        try:
            # Find matching condition
            condition = None
            for cond in self.alert_conditions:
                if cond.alert_type == alert.alert_type:
                    condition = cond
                    break
            
            if not condition:
                logger.warning(f"No condition found for alert type {alert.alert_type}")
                return False
            
            # Check if alert should be sent
            if not self.should_send_alert(alert, condition):
                return False
            
            # Add to history
            self.alert_history.append(alert)
            
            # Update last alert time for cooldown
            alert_key = f"{alert.alert_type.value}_{alert.symbol or 'global'}"
            self.last_alert_times[alert_key] = datetime.now()
            
            # Send notification
            success = await self.send_notification(alert)
            
            if success:
                logger.info(f"Alert {alert.alert_id} processed and sent successfully")
            else:
                logger.error(f"Failed to send alert {alert.alert_id}")
            
            # Save history periodically
            if len(self.alert_history) % 10 == 0:
                self.save_alert_history()
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing alert {alert.alert_id}: {e}")
            return False
    
    def update_preferences(self, new_preferences: Dict[str, Any]):
        """Update alert preferences"""
        try:
            # Update preferences object
            for key, value in new_preferences.items():
                if hasattr(self.preferences, key):
                    setattr(self.preferences, key, value)
            
            # Update alert conditions based on new preferences
            self.alert_conditions = self.setup_default_conditions()
            
            # Save preferences
            self.save_preferences()
            
            logger.info("Alert preferences updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating preferences: {e}")
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of recent alerts"""
        try:
            # Get alerts from last 24 hours
            cutoff_time = datetime.now() - timedelta(hours=24)
            recent_alerts = [alert for alert in self.alert_history if alert.timestamp > cutoff_time]
            
            # Count by type
            type_counts = {}
            priority_counts = {}
            
            for alert in recent_alerts:
                alert_type = alert.alert_type.value
                priority = alert.priority.value
                
                type_counts[alert_type] = type_counts.get(alert_type, 0) + 1
                priority_counts[priority] = priority_counts.get(priority, 0) + 1
            
            return {
                'total_alerts_24h': len(recent_alerts),
                'alerts_by_type': type_counts,
                'alerts_by_priority': priority_counts,
                'last_alert_time': recent_alerts[-1].timestamp.isoformat() if recent_alerts else None,
                'preferences': asdict(self.preferences),
                'conditions_enabled': len([c for c in self.alert_conditions if c.enabled])
            }
            
        except Exception as e:
            logger.error(f"Error getting alert summary: {e}")
            return {}

# Integration functions for strategy files
async def send_strategy_signal_alert(signal_data: Dict[str, Any], strategy_name: str) -> bool:
    """Send alert for strategy signal - integration function for strategy files"""
    try:
        alert_manager = AlertManager()
        
        # Add strategy name to signal data
        signal_data['strategy'] = strategy_name
        
        # Create alert
        alert = alert_manager.create_strategy_signal_alert(signal_data)
        
        if alert:
            # Process and send alert
            success = await alert_manager.process_alert(alert)
            return success
        
        return False
        
    except Exception as e:
        logger.error(f"Error sending strategy signal alert: {e}")
        return False

async def send_position_alert(position_data: Dict[str, Any], alert_type: str) -> bool:
    """Send alert for position events - integration function"""
    try:
        alert_manager = AlertManager()
        
        # Create alert
        alert = alert_manager.create_position_alert(position_data, alert_type)
        
        if alert:
            # Process and send alert
            success = await alert_manager.process_alert(alert)
            return success
        
        return False
        
    except Exception as e:
        logger.error(f"Error sending position alert: {e}")
        return False

async def send_risk_management_alert(risk_data: Dict[str, Any], alert_type: str) -> bool:
    """Send alert for risk management events - integration function"""
    try:
        alert_manager = AlertManager()
        
        # Create alert
        alert = alert_manager.create_risk_management_alert(risk_data, alert_type)
        
        if alert:
            # Process and send alert
            success = await alert_manager.process_alert(alert)
            return success
        
        return False
        
    except Exception as e:
        logger.error(f"Error sending risk management alert: {e}")
        return False

def main():
    """Main function for testing alert manager"""
    import asyncio
    
    async def test_alerts():
        alert_manager = AlertManager()
        
        # Test strategy signal alert
        test_signal = {
            'symbol': 'NVDA',
            'signal_type': 'STRONG_BUY',
            'strategy': 'PML',
            'confidence': 0.85,
            'entry_reason': 'Strong PML cross with delta confirmation',
            'position_size': 2.5,
            'stop_loss': 145.50,
            'profit_target': 165.75
        }
        
        alert = alert_manager.create_strategy_signal_alert(test_signal)
        if alert:
            await alert_manager.process_alert(alert)
        
        # Print alert summary
        summary = alert_manager.get_alert_summary()
        print("Alert Summary:", json.dumps(summary, indent=2))
    
    asyncio.run(test_alerts())

if __name__ == "__main__":
    main()
