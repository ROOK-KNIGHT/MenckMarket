#!/usr/bin/env python3
"""
Alert Monitor
Periodically checks trading conditions and triggers email notifications when alerts are met
"""

import json
import os
import logging
import time
from datetime import datetime
from email_notification_engine import EmailNotificationEngine
from db_query_handler import DatabaseQueryHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/alert_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AlertMonitor:
    """Monitor trading conditions and trigger email alerts"""
    
    def __init__(self, config_file='alerts_config.json'):
        """Initialize the alert monitor"""
        self.config_file = config_file
        self.alerts_config = None
        self.trading_config = None
        self.email_engine = None
        self.db_query_handler = None
        self.last_alert_times = {}  # Track when alerts were last sent
        
        self.load_alerts_config()
        self.load_trading_config()
        self.initialize_components()
    
    def load_alerts_config(self):
        """Load alerts configuration from JSON file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.alerts_config = json.load(f)
                logger.info(f"✅ Loaded alerts configuration from {self.config_file}")
            else:
                logger.error(f"❌ Alerts config file not found: {self.config_file}")
                self.alerts_config = None
        except Exception as e:
            logger.error(f"❌ Error loading alerts config: {e}")
            self.alerts_config = None
    
    def load_trading_config(self):
        """Load trading configuration from JSON file"""
        try:
            trading_config_file = 'trading_config_live.json'
            if os.path.exists(trading_config_file):
                with open(trading_config_file, 'r') as f:
                    self.trading_config = json.load(f)
                logger.info(f"✅ Loaded trading configuration from {trading_config_file}")
            else:
                logger.warning(f"⚠️ Trading config file not found: {trading_config_file}")
                self.trading_config = None
        except Exception as e:
            logger.error(f"❌ Error loading trading config: {e}")
            self.trading_config = None
    
    def initialize_components(self):
        """Initialize email engine and database handler"""
        try:
            # Initialize email engine
            self.email_engine = EmailNotificationEngine(self.config_file)
            
            # Initialize database query handler
            self.db_query_handler = DatabaseQueryHandler()
            
            logger.info("✅ Alert monitor components initialized")
        except Exception as e:
            logger.error(f"❌ Error initializing components: {e}")
    
    def is_alert_enabled(self, alert_type):
        """Check if a specific alert type is enabled"""
        if not self.alerts_config:
            return False
        
        try:
            alert_types = self.alerts_config.get('alerts_notifications', {}).get('alert_types', {})
            return alert_types.get(alert_type, {}).get('enabled', False)
        except Exception:
            return False
    
    def is_email_enabled(self):
        """Check if email notifications are enabled"""
        if not self.alerts_config:
            return False
        
        try:
            email_config = self.alerts_config.get('alerts_notifications', {}).get('notification_channels', {}).get('email_notifications', {})
            return email_config.get('enabled', False)
        except Exception:
            return False
    
    def is_quiet_hours(self):
        """Check if currently in quiet hours"""
        if not self.alerts_config:
            return False
        
        try:
            prefs = self.alerts_config.get('alerts_notifications', {}).get('notification_preferences', {})
            quiet_hours = prefs.get('quiet_hours', {})
            
            if not quiet_hours.get('enabled', False):
                return False
            
            from datetime import time
            current_time = datetime.now().time()
            start_time = time.fromisoformat(quiet_hours.get('start_time', '22:00'))
            end_time = time.fromisoformat(quiet_hours.get('end_time', '08:00'))
            
            if start_time <= end_time:
                return start_time <= current_time <= end_time
            else:
                return current_time >= start_time or current_time <= end_time
                
        except Exception as e:
            logger.error(f"❌ Error checking quiet hours: {e}")
            return False
    
    def should_send_alert(self, alert_type):
        """Check if we should send an alert based on frequency settings"""
        if not self.alerts_config:
            return True
        
        try:
            # Get alert frequency setting
            prefs = self.alerts_config.get('alerts_notifications', {}).get('notification_preferences', {})
            frequency = prefs.get('alert_frequency', {}).get('value', '5min')
            
            # Convert frequency to seconds
            frequency_seconds = {
                'immediate': 0,
                '5min': 300,
                '15min': 900,
                '30min': 1800,
                '1hour': 3600
            }.get(frequency, 300)
            
            # Check if enough time has passed since last alert
            last_alert_time = self.last_alert_times.get(alert_type, 0)
            current_time = time.time()
            
            if current_time - last_alert_time >= frequency_seconds:
                self.last_alert_times[alert_type] = current_time
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking alert frequency: {e}")
            return True
    
    def get_account_data(self):
        """Get current account data"""
        try:
            # Load account data from JSON file
            account_file = 'account_data.json'
            if os.path.exists(account_file):
                with open(account_file, 'r') as f:
                    account_data = json.load(f)
                return account_data
            else:
                logger.warning(f"⚠️ Account data file not found: {account_file}")
                return None
        except Exception as e:
            logger.error(f"❌ Error loading account data: {e}")
            return None
    
    def get_current_positions(self):
        """Get current positions data"""
        try:
            # Load positions from JSON file
            positions_file = 'current_positions.json'
            if os.path.exists(positions_file):
                with open(positions_file, 'r') as f:
                    positions_data = json.load(f)
                return positions_data
            else:
                logger.warning(f"⚠️ Positions data file not found: {positions_file}")
                return None
        except Exception as e:
            logger.error(f"❌ Error loading positions data: {e}")
            return None
    
    def get_vix_data(self):
        """Get current VIX data from vix_data.json file"""
        try:
            # Load VIX data from JSON file created by vix_data_handler.py
            vix_file = 'vix_data.json'
            if os.path.exists(vix_file):
                with open(vix_file, 'r') as f:
                    vix_data = json.load(f)
                
                # Extract VIX information from the data structure
                vix_info = vix_data.get('vix_data', {})
                
                return {
                    'vix': vix_info.get('current_vix', 0),
                    'vix_level': vix_info.get('vix_level', 'UNKNOWN'),
                    'market_fear': vix_info.get('market_fear', 'UNKNOWN'),
                    'daily_change_pct': vix_info.get('daily_change_pct', 0),
                    'daily_high': vix_info.get('daily_high', 0),
                    'daily_low': vix_info.get('daily_low', 0),
                    'timestamp': vix_info.get('timestamp', datetime.now().isoformat()),
                    'last_updated': vix_info.get('last_updated', datetime.now().isoformat())
                }
            else:
                logger.warning(f"⚠️ VIX data file not found: {vix_file}")
                return None
        except Exception as e:
            logger.error(f"❌ Error getting VIX data: {e}")
            return None
    
    def check_maximum_loss_alert(self):
        """Check if maximum loss threshold has been exceeded"""
        if not self.is_alert_enabled('maximum_loss_alert'):
            return False
        
        try:
            # Get loss threshold from config
            alert_config = self.alerts_config.get('alerts_notifications', {}).get('alert_types', {}).get('maximum_loss_alert', {})
            threshold = alert_config.get('loss_threshold', {}).get('value', 1000)
            
            # Get account data
            account_data = self.get_account_data()
            if not account_data:
                return False
            
            # Calculate current loss (example logic - adjust based on your data structure)
            day_pnl = account_data.get('day_pnl', 0)
            open_pnl = account_data.get('open_pnl', 0)
            total_loss = abs(min(day_pnl, open_pnl, 0))  # Only consider losses
            
            if total_loss > threshold:
                logger.warning(f"🚨 Maximum loss alert triggered: ${total_loss:.2f} > ${threshold:.2f}")
                
                # Send alert email
                alert_data = {
                    'loss_amount': total_loss,
                    'threshold': threshold,
                    'day_pnl': day_pnl,
                    'open_pnl': open_pnl,
                    'timestamp': datetime.now().isoformat()
                }
                
                return self.send_alert('maximum_loss_alert', alert_data)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking maximum loss alert: {e}")
            return False
    
    def check_volatility_spike_alert(self):
        """Check if VIX has exceeded threshold"""
        if not self.is_alert_enabled('volatility_spike_alert'):
            return False
        
        try:
            # Get VIX threshold from config
            alert_config = self.alerts_config.get('alerts_notifications', {}).get('alert_types', {}).get('volatility_spike_alert', {})
            threshold = alert_config.get('vix_threshold', {}).get('value', 30)
            
            # Get VIX data
            vix_data = self.get_vix_data()
            if not vix_data:
                return False
            
            current_vix = vix_data.get('vix', 0)
            
            if current_vix > threshold:
                logger.warning(f"🚨 Volatility spike alert triggered: VIX {current_vix:.2f} > {threshold}")
                
                # Send alert email with comprehensive VIX data
                alert_data = {
                    'current_vix': current_vix,
                    'threshold': threshold,
                    'vix_level': vix_data.get('vix_level', 'UNKNOWN'),
                    'market_fear': vix_data.get('market_fear', 'UNKNOWN'),
                    'daily_change_pct': vix_data.get('daily_change_pct', 0),
                    'daily_high': vix_data.get('daily_high', 0),
                    'daily_low': vix_data.get('daily_low', 0),
                    'vix_last_updated': vix_data.get('last_updated', 'Unknown'),
                    'timestamp': datetime.now().isoformat()
                }
                
                return self.send_alert('volatility_spike_alert', alert_data)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking volatility spike alert: {e}")
            return False
    
    def check_position_size_alert(self):
        """Check if any position size exceeds limits"""
        if not self.is_alert_enabled('position_size_alert'):
            return False
        
        try:
            # Get positions data
            positions_data = self.get_current_positions()
            if not positions_data:
                return False
            
            # Get account data for total equity
            account_data = self.get_account_data()
            if not account_data:
                return False
            
            total_equity = account_data.get('total_equity', 0)
            if total_equity <= 0:
                return False
            
            # Get max position size from trading config
            max_position_pct = 0.1  # Default 10%
            if self.trading_config:
                # Look for max position size in trading config
                risk_management = self.trading_config.get('risk_management', {})
                max_position_pct = risk_management.get('max_position_size_pct', 0.1)
                
                # Convert from percentage if needed (e.g., 10 -> 0.1)
                if max_position_pct > 1:
                    max_position_pct = max_position_pct / 100
            
            # Check each position
            positions = positions_data.get('positions', [])
            
            for position in positions:
                market_value = abs(position.get('market_value', 0))
                position_pct = market_value / total_equity
                
                if position_pct > max_position_pct:
                    symbol = position.get('symbol', 'Unknown')
                    logger.warning(f"🚨 Position size alert triggered: {symbol} is {position_pct:.1%} of portfolio (max: {max_position_pct:.1%})")
                    
                    # Send alert email
                    alert_data = {
                        'symbol': symbol,
                        'position_value': market_value,
                        'position_percentage': position_pct * 100,
                        'max_percentage': max_position_pct * 100,
                        'total_equity': total_equity,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    return self.send_alert('position_size_alert', alert_data)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking position size alert: {e}")
            return False
    
    def check_strategy_signal_alert(self):
        """Check if new strategy signals have been generated"""
        if not self.is_alert_enabled('strategy_signal_alert'):
            return False
        
        try:
            # Get recent signals from database
            if not self.db_query_handler:
                return False
            
            # Check for recent PML signals
            pml_signals = self.db_query_handler.get_pml_signals()
            recent_signals = []
            
            # Check if any signals are from the last 5 minutes
            current_time = datetime.now()
            for signal in pml_signals:
                signal_time = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00'))
                time_diff = (current_time - signal_time.replace(tzinfo=None)).total_seconds()
                
                if time_diff <= 300:  # 5 minutes
                    recent_signals.append(signal)
            
            if recent_signals:
                logger.info(f"🚨 Strategy signal alert triggered: {len(recent_signals)} new signals")
                
                # Send alert email
                alert_data = {
                    'signal_count': len(recent_signals),
                    'signals': recent_signals[:3],  # Include first 3 signals
                    'timestamp': datetime.now().isoformat()
                }
                
                return self.send_alert('strategy_signal_alert', alert_data)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking strategy signal alert: {e}")
            return False
    
    def send_alert(self, alert_type, alert_data):
        """Send an alert email"""
        try:
            # Check if we should send this alert (frequency control)
            if not self.should_send_alert(alert_type):
                logger.info(f"⏰ Skipping {alert_type} alert due to frequency limit")
                return False
            
            # Check quiet hours
            if self.is_quiet_hours():
                logger.info(f"🔇 Skipping {alert_type} alert due to quiet hours")
                return False
            
            # Check if email is enabled
            if not self.is_email_enabled():
                logger.info(f"📧 Email notifications disabled, skipping {alert_type} alert")
                return False
            
            # Send email using email engine
            if self.email_engine:
                success = self.email_engine.send_alert_email(alert_type, alert_data)
                if success:
                    logger.info(f"✅ {alert_type} alert email sent successfully")
                else:
                    logger.error(f"❌ Failed to send {alert_type} alert email")
                return success
            else:
                logger.error("❌ Email engine not initialized")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending {alert_type} alert: {e}")
            return False
    
    def run_alert_checks(self):
        """Run all alert checks"""
        logger.info("🔍 Running alert checks...")
        
        try:
            # Reload config in case it changed
            self.load_alerts_config()
            
            alerts_triggered = 0
            
            # Check maximum loss alert
            if self.check_maximum_loss_alert():
                alerts_triggered += 1
            
            # Check volatility spike alert
            if self.check_volatility_spike_alert():
                alerts_triggered += 1
            
            # Check position size alert
            if self.check_position_size_alert():
                alerts_triggered += 1
            
            # Check strategy signal alert
            if self.check_strategy_signal_alert():
                alerts_triggered += 1
            
            if alerts_triggered > 0:
                logger.info(f"🚨 {alerts_triggered} alerts triggered")
            else:
                logger.info("✅ No alerts triggered")
            
            return alerts_triggered
            
        except Exception as e:
            logger.error(f"❌ Error running alert checks: {e}")
            return 0
    
    def monitor_continuously(self, check_interval=60):
        """Monitor alerts continuously"""
        logger.info(f"🔍 Starting continuous alert monitoring (checking every {check_interval} seconds)...")
        
        while True:
            try:
                self.run_alert_checks()
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Alert monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in continuous monitoring: {e}")
                time.sleep(check_interval)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='VolFlow Alert Monitor')
    parser.add_argument('--check', action='store_true', help='Run alert checks once')
    parser.add_argument('--monitor', action='store_true', help='Start continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Check interval in seconds (default: 60)')
    parser.add_argument('--config', default='alerts_config.json', help='Path to alerts config file')
    
    args = parser.parse_args()
    
    # Create alert monitor
    alert_monitor = AlertMonitor(args.config)
    
    if args.check:
        # Run checks once
        alerts_triggered = alert_monitor.run_alert_checks()
        print(f"Alert check completed: {alerts_triggered} alerts triggered")
    
    elif args.monitor:
        # Start continuous monitoring
        alert_monitor.monitor_continuously(args.interval)
    
    else:
        print("VolFlow Alert Monitor")
        print("Usage:")
        print("  --check     Run alert checks once")
        print("  --monitor   Start continuous monitoring")
        print("  --interval  Set check interval in seconds")
        print("  --config    Specify config file path")

if __name__ == "__main__":
    main()
