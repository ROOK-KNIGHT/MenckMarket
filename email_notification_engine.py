#!/usr/bin/env python3
"""
Email Notification Engine
Reads alerts_config.json and sends email notifications based on configuration
"""

import json
import smtplib
import os
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import time
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/email_notifications.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EmailNotificationEngine:
    """Email notification engine for VolFlow trading alerts"""
    
    def __init__(self, config_file='alerts_config.json'):
        """Initialize the email notification engine"""
        self.config_file = config_file
        self.alerts_config = None
        self.smtp_config = {
            'smtp_server': 'smtp.gmail.com',  # Default to Gmail
            'smtp_port': 587,
            'use_tls': True
        }
        self.load_alerts_config()
        self.load_smtp_config()
    
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
    
    def load_smtp_config(self):
        """Load SMTP configuration from AWS Secrets Manager"""
        try:
            # Get email from alerts config first
            email_address = self.get_email_address()
            if not email_address:
                logger.warning("⚠️ No email address found in alerts config")
                return
            
            # Try to load SMTP credentials from AWS Secrets Manager
            smtp_credentials = self._load_smtp_from_aws(email_address)
            
            if smtp_credentials:
                self.smtp_config.update({
                    'smtp_server': smtp_credentials.get('smtp_server', 'smtp.gmail.com'),
                    'smtp_port': int(smtp_credentials.get('smtp_port', '587')),
                    'username': smtp_credentials.get('username'),
                    'password': smtp_credentials.get('password')
                })
                logger.info(f"✅ SMTP configuration loaded from AWS for {email_address}")
            else:
                # If not found in AWS, create and store default config
                logger.info("📧 SMTP credentials not found in AWS, creating default configuration")
                self._create_default_smtp_config(email_address)
                
        except Exception as e:
            logger.error(f"❌ Error loading SMTP config: {e}")
    
    def _load_smtp_from_aws(self, email_address):
        """Load SMTP credentials from AWS Secrets Manager"""
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            
            # Initialize AWS Secrets Manager client
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            secret_name = f"production/email-notifications/smtp-{email_address.replace('@', '-at-').replace('.', '-')}"
            
            # Try to get the secret
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response['SecretString'])
            
            logger.info(f"✅ SMTP credentials loaded from AWS: {secret_name}")
            return secret_data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"📧 SMTP secret not found in AWS: {secret_name}")
                return None
            else:
                logger.error(f"❌ AWS error loading SMTP credentials: {e}")
                return None
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found for SMTP config")
            return None
        except ImportError:
            logger.error("❌ boto3 not available for AWS integration")
            return None
        except Exception as e:
            logger.error(f"❌ Error loading SMTP from AWS: {e}")
            return None
    
    def _create_default_smtp_config(self, email_address):
        """Create and store default SMTP configuration in AWS"""
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            
            # Initialize AWS Secrets Manager client
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            secret_name = f"production/email-notifications/smtp-{email_address.replace('@', '-at-').replace('.', '-')}"
            
            # Create default SMTP configuration
            default_config = {
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': '587',
                'username': email_address,
                'password': 'YOUR_APP_PASSWORD_HERE',  # User needs to update this
                'description': f'SMTP configuration for {email_address}',
                'created_by': 'email_notification_engine',
                'created_at': datetime.now().isoformat(),
                'instructions': 'Update the password field with your Gmail app password'
            }
            
            # Store in AWS Secrets Manager
            secrets_client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(default_config),
                Description=f"SMTP configuration for email notifications to {email_address}"
            )
            
            logger.info(f"✅ Created default SMTP config in AWS: {secret_name}")
            logger.warning("⚠️ Please update the SMTP password in AWS Secrets Manager")
            logger.info(f"💡 Secret name: {secret_name}")
            
            # Update local config with default values (password will be invalid until updated)
            self.smtp_config.update({
                'smtp_server': default_config['smtp_server'],
                'smtp_port': int(default_config['smtp_port']),
                'username': default_config['username'],
                'password': default_config['password']
            })
            
        except Exception as e:
            logger.error(f"❌ Error creating default SMTP config: {e}")
    
    def is_email_enabled(self):
        """Check if email notifications are enabled in config"""
        if not self.alerts_config:
            return False
        
        try:
            email_config = self.alerts_config.get('alerts_notifications', {}).get('notification_channels', {}).get('email_notifications', {})
            return email_config.get('enabled', False)
        except Exception as e:
            logger.error(f"❌ Error checking email config: {e}")
            return False
    
    def get_email_address(self):
        """Get the configured email address for notifications"""
        if not self.alerts_config:
            return None
        
        try:
            email_config = self.alerts_config.get('alerts_notifications', {}).get('notification_channels', {}).get('email_notifications', {})
            email_address = email_config.get('email_address', {}).get('value', '')
            return email_address if email_address else None
        except Exception as e:
            logger.error(f"❌ Error getting email address: {e}")
            return None
    
    def create_test_email(self, to_email):
        """Create a test email message"""
        msg = MIMEMultipart()
        msg['From'] = self.smtp_config.get('username', 'volflow@trading.com')
        msg['To'] = to_email
        msg['Subject'] = "VolFlow Trading System - Test Email Notification"
        
        # Create HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px 10px 0 0; text-align: center; }}
                .content {{ padding: 30px; }}
                .alert-box {{ background-color: #e8f5e8; border-left: 4px solid #4caf50; padding: 15px; margin: 20px 0; border-radius: 4px; }}
                .footer {{ background-color: #f8f9fa; padding: 20px; border-radius: 0 0 10px 10px; text-align: center; color: #666; font-size: 12px; }}
                .status-badge {{ display: inline-block; padding: 5px 10px; background-color: #4caf50; color: white; border-radius: 15px; font-size: 12px; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 VolFlow Trading System</h1>
                    <p>Email Notification Test</p>
                </div>
                <div class="content">
                    <h2>🧪 Test Email Notification</h2>
                    <p>This is a test email from your VolFlow Trading System to verify that email notifications are working correctly.</p>
                    
                    <div class="alert-box">
                        <strong>✅ Email System Status:</strong> <span class="status-badge">OPERATIONAL</span>
                    </div>
                    
                    <h3>📊 System Information:</h3>
                    <ul>
                        <li><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}</li>
                        <li><strong>Notification Type:</strong> Test Email</li>
                        <li><strong>System Status:</strong> Active</li>
                        <li><strong>Email Engine:</strong> Operational</li>
                    </ul>
                    
                    <h3>🔔 Configured Alerts:</h3>
                    <p>Your email notifications are now configured and ready to send alerts for:</p>
                    <ul>
                        <li>Maximum Loss Alerts</li>
                        <li>Volatility Spike Alerts</li>
                        <li>Position Size Alerts</li>
                        <li>Strategy Signal Alerts</li>
                    </ul>
                    
                    <p><strong>If you received this email, your notification system is working correctly!</strong></p>
                </div>
                <div class="footer">
                    <p>VolFlow Options Trading System | Automated Email Notification</p>
                    <p>This email was sent automatically by your trading system.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML body
        msg.attach(MIMEText(html_body, 'html'))
        
        return msg
    
    def create_alert_email(self, alert_type, alert_data, to_email):
        """Create an alert email message"""
        msg = MIMEMultipart()
        msg['From'] = self.smtp_config.get('username', 'volflow@trading.com')
        msg['To'] = to_email
        
        # Set subject based on alert type
        alert_subjects = {
            'maximum_loss_alert': '🚨 TRADING ALERT: Maximum Loss Threshold Exceeded',
            'volatility_spike_alert': '📈 TRADING ALERT: Market Volatility Spike Detected',
            'position_size_alert': '⚠️ TRADING ALERT: Position Size Limit Exceeded',
            'strategy_signal_alert': '🎯 TRADING ALERT: New Strategy Signal Generated'
        }
        
        msg['Subject'] = alert_subjects.get(alert_type, '🔔 TRADING ALERT: System Notification')
        
        # Create HTML email body for alerts
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); color: white; padding: 20px; border-radius: 10px 10px 0 0; text-align: center; }}
                .content {{ padding: 30px; }}
                .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0; border-radius: 4px; }}
                .critical-alert {{ background-color: #f8d7da; border-left: 4px solid #dc3545; }}
                .footer {{ background-color: #f8f9fa; padding: 20px; border-radius: 0 0 10px 10px; text-align: center; color: #666; font-size: 12px; }}
                .data-table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
                .data-table th, .data-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .data-table th {{ background-color: #f2f2f2; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚨 VolFlow Trading Alert</h1>
                    <p>{alert_subjects.get(alert_type, 'System Notification')}</p>
                </div>
                <div class="content">
                    <div class="alert-box {'critical-alert' if 'loss' in alert_type else ''}">
                        <h3>⚠️ Alert Details</h3>
                        <p><strong>Alert Type:</strong> {alert_type.replace('_', ' ').title()}</p>
                        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
                        <p><strong>Status:</strong> ACTIVE</p>
                    </div>
                    
                    <h3>📊 Alert Data:</h3>
                    <table class="data-table">
        """
        
        # Add alert data to table
        for key, value in alert_data.items():
            html_body += f"<tr><td><strong>{key.replace('_', ' ').title()}</strong></td><td>{value}</td></tr>"
        
        html_body += f"""
                    </table>
                    
                    <h3>🎯 Recommended Actions:</h3>
                    <ul>
                        <li>Review your current positions immediately</li>
                        <li>Check market conditions and volatility</li>
                        <li>Consider risk management measures</li>
                        <li>Monitor the situation closely</li>
                    </ul>
                    
                    <p><strong>⚠️ This is an automated alert from your VolFlow Trading System.</strong></p>
                </div>
                <div class="footer">
                    <p>VolFlow Options Trading System | Automated Alert Notification</p>
                    <p>Alert generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML body
        msg.attach(MIMEText(html_body, 'html'))
        
        return msg
    
    def send_email(self, msg):
        """Send email using SMTP"""
        try:
            if not self.smtp_config.get('username') or not self.smtp_config.get('password'):
                logger.error("❌ SMTP credentials not configured")
                return False
            
            # Create SMTP session
            server = smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config['smtp_port'])
            
            if self.smtp_config.get('use_tls', True):
                server.starttls()  # Enable TLS encryption
            
            # Login with credentials
            server.login(self.smtp_config['username'], self.smtp_config['password'])
            
            # Send email
            text = msg.as_string()
            server.sendmail(msg['From'], msg['To'], text)
            server.quit()
            
            logger.info(f"✅ Email sent successfully to {msg['To']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False
    
    def send_test_email(self):
        """Send a test email notification"""
        logger.info("📧 Sending test email notification...")
        
        # Check if email is enabled
        if not self.is_email_enabled():
            logger.warning("⚠️ Email notifications are disabled in configuration")
            return False
        
        # Get email address
        to_email = self.get_email_address()
        if not to_email:
            logger.error("❌ No email address configured")
            return False
        
        # Create and send test email
        try:
            msg = self.create_test_email(to_email)
            success = self.send_email(msg)
            
            if success:
                logger.info(f"✅ Test email sent successfully to {to_email}")
            else:
                logger.error(f"❌ Failed to send test email to {to_email}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error sending test email: {e}")
            return False
    
    def send_alert_email(self, alert_type, alert_data):
        """Send an alert email notification"""
        logger.info(f"🚨 Sending {alert_type} alert email...")
        
        # Check if email is enabled
        if not self.is_email_enabled():
            logger.warning("⚠️ Email notifications are disabled in configuration")
            return False
        
        # Get email address
        to_email = self.get_email_address()
        if not to_email:
            logger.error("❌ No email address configured")
            return False
        
        # Create and send alert email
        try:
            msg = self.create_alert_email(alert_type, alert_data, to_email)
            success = self.send_email(msg)
            
            if success:
                logger.info(f"✅ Alert email sent successfully to {to_email}")
            else:
                logger.error(f"❌ Failed to send alert email to {to_email}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error sending alert email: {e}")
            return False
    
    def monitor_and_send_alerts(self, check_interval=60):
        """Monitor for alert conditions and send emails (placeholder for future implementation)"""
        logger.info(f"🔍 Starting alert monitoring (checking every {check_interval} seconds)...")
        
        while True:
            try:
                # Reload config in case it changed
                self.load_alerts_config()
                
                # Here you would implement logic to check for alert conditions
                # For now, this is a placeholder
                logger.info("🔍 Checking for alert conditions...")
                
                # Example: Check for maximum loss alert
                # if self.check_maximum_loss_condition():
                #     self.send_alert_email('maximum_loss_alert', {'loss_amount': 1500, 'threshold': 1000})
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Alert monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in alert monitoring: {e}")
                time.sleep(check_interval)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='VolFlow Email Notification Engine')
    parser.add_argument('--test', action='store_true', help='Send a test email')
    parser.add_argument('--monitor', action='store_true', help='Start alert monitoring')
    parser.add_argument('--config', default='alerts_config.json', help='Path to alerts config file')
    
    args = parser.parse_args()
    
    # Create email engine
    email_engine = EmailNotificationEngine(args.config)
    
    if args.test:
        # Send test email
        success = email_engine.send_test_email()
        if success:
            print("✅ Test email sent successfully!")
        else:
            print("❌ Failed to send test email. Check logs for details.")
    
    elif args.monitor:
        # Start monitoring for alerts
        email_engine.monitor_and_send_alerts()
    
    else:
        print("VolFlow Email Notification Engine")
        print("Usage:")
        print("  --test     Send a test email")
        print("  --monitor  Start alert monitoring")
        print("  --config   Specify config file path")

if __name__ == "__main__":
    main()
