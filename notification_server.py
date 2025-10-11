#!/usr/bin/env python3
"""
VolFlow Notification Server
Handles Email, Slack, and Telegram notifications for trading alerts and system events
"""

import os
import json
import smtplib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from jinja2 import Template

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class NotificationConfig:
    """Configuration for notification channels"""
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_from: str = ""
    email_to: List[str] = None
    
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#volflow-alerts"
    slack_username: str = "VolFlow Bot"
    
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

@dataclass
class NotificationMessage:
    """Structure for notification messages"""
    title: str
    message: str
    priority: str = "normal"  # low, normal, high, critical
    category: str = "general"  # general, trading, risk, system
    data: Dict[str, Any] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.data is None:
            self.data = {}

class NotificationServer:
    """Main notification server class"""
    
    def __init__(self, config_file: str = None):
        self.config = self.load_config(config_file)
        self.app = Flask(__name__)
        CORS(self.app)
        self.setup_routes()
        
        # Message templates
        self.templates = self.load_templates()
        
        logger.info("VolFlow Notification Server initialized")
        logger.info(f"Email enabled: {self.config.email_enabled}")
        logger.info(f"Slack enabled: {self.config.slack_enabled}")
        logger.info(f"Telegram enabled: {self.config.telegram_enabled}")
    
    def load_config(self, config_file: str = None) -> NotificationConfig:
        """Load configuration from file or environment variables"""
        config_data = {}
        
        # Try to load from file first
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config_data = json.load(f)
                logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.warning(f"Failed to load config file {config_file}: {e}")
        
        # Override with environment variables
        env_config = {
            'email_enabled': os.getenv('EMAIL_ENABLED', 'false').lower() == 'true',
            'email_smtp_server': os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com'),
            'email_smtp_port': int(os.getenv('EMAIL_SMTP_PORT', '587')),
            'email_username': os.getenv('EMAIL_USERNAME', ''),
            'email_password': os.getenv('EMAIL_PASSWORD', ''),
            'email_from': os.getenv('EMAIL_FROM', ''),
            'email_to': os.getenv('EMAIL_TO', '').split(',') if os.getenv('EMAIL_TO') else [],
            
            'slack_enabled': os.getenv('SLACK_ENABLED', 'false').lower() == 'true',
            'slack_webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
            'slack_channel': os.getenv('SLACK_CHANNEL', '#volflow-alerts'),
            'slack_username': os.getenv('SLACK_USERNAME', 'VolFlow Bot'),
            
            'telegram_enabled': os.getenv('TELEGRAM_ENABLED', 'false').lower() == 'true',
            'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),
            'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID', ''),
        }
        
        # Merge configurations
        config_data.update({k: v for k, v in env_config.items() if v})
        
        return NotificationConfig(**config_data)
    
    def load_templates(self) -> Dict[str, Template]:
        """Load message templates"""
        templates = {
            'email_html': Template("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{{ title }}</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }
        .header h1 { margin: 0; font-size: 24px; font-weight: 600; }
        .priority-{{ priority }} .header { background: {% if priority == 'critical' %}linear-gradient(135deg, #e53e3e, #c53030){% elif priority == 'high' %}linear-gradient(135deg, #d69e2e, #b7791f){% else %}linear-gradient(135deg, #667eea, #764ba2){% endif %}; }
        .content { padding: 30px; }
        .message { font-size: 16px; line-height: 1.6; color: #2d3748; margin-bottom: 20px; }
        .data-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        .data-table th, .data-table td { padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }
        .data-table th { background-color: #f7fafc; font-weight: 600; color: #4a5568; }
        .footer { background-color: #f7fafc; padding: 20px; text-align: center; color: #718096; font-size: 14px; }
        .timestamp { color: #a0aec0; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container priority-{{ priority }}">
        <div class="header">
            <h1>🚀 {{ title }}</h1>
        </div>
        <div class="content">
            <div class="message">{{ message }}</div>
            {% if data %}
            <table class="data-table">
                {% for key, value in data.items() %}
                <tr>
                    <th>{{ key.replace('_', ' ').title() }}</th>
                    <td>{{ value }}</td>
                </tr>
                {% endfor %}
            </table>
            {% endif %}
        </div>
        <div class="footer">
            <div>VolFlow Options Breakout System</div>
            <div class="timestamp">{{ timestamp }}</div>
        </div>
    </div>
</body>
</html>
            """),
            
            'slack_message': Template("""
{
    "channel": "{{ channel }}",
    "username": "{{ username }}",
    "icon_emoji": ":chart_with_upwards_trend:",
    "attachments": [
        {
            "color": "{% if priority == 'critical' %}danger{% elif priority == 'high' %}warning{% else %}good{% endif %}",
            "title": "{{ title }}",
            "text": "{{ message }}",
            "fields": [
                {% for key, value in data.items() %}
                {
                    "title": "{{ key.replace('_', ' ').title() }}",
                    "value": "{{ value }}",
                    "short": true
                }{% if not loop.last %},{% endif %}
                {% endfor %}
            ],
            "footer": "VolFlow System",
            "ts": {{ timestamp_unix }}
        }
    ]
}
            """),
            
            'telegram_message': Template("""
🚀 *{{ title }}*

{{ message }}

{% if data %}
📊 *Details:*
{% for key, value in data.items() %}
• *{{ key.replace('_', ' ').title() }}:* {{ value }}
{% endfor %}
{% endif %}

🕐 {{ timestamp }}
            """)
        }
        
        return templates
    
    def setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'channels': {
                    'email': self.config.email_enabled,
                    'slack': self.config.slack_enabled,
                    'telegram': self.config.telegram_enabled
                }
            })
        
        @self.app.route('/notify', methods=['POST'])
        def send_notification():
            try:
                data = request.get_json()
                
                # Create notification message
                notification = NotificationMessage(
                    title=data.get('title', 'VolFlow Notification'),
                    message=data.get('message', ''),
                    priority=data.get('priority', 'normal'),
                    category=data.get('category', 'general'),
                    data=data.get('data', {}),
                    timestamp=data.get('timestamp')
                )
                
                # Send to all enabled channels
                results = self.send_notification_sync(notification)
                
                return jsonify({
                    'success': True,
                    'results': results,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error sending notification: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/test', methods=['POST'])
        def test_notifications():
            """Test all notification channels"""
            try:
                # Create test notification
                test_notification = NotificationMessage(
                    title="🧪 VolFlow Test Notification",
                    message="This is a test message from your VolFlow notification system. All channels are working correctly!",
                    priority="normal",
                    category="system",
                    data={
                        'test_type': 'System Test',
                        'server_status': 'Online',
                        'channels_tested': 'Email, Slack, Telegram'
                    }
                )
                
                results = self.send_notification_sync(test_notification)
                
                return jsonify({
                    'success': True,
                    'message': 'Test notifications sent',
                    'results': results,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error sending test notifications: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/config', methods=['GET', 'POST'])
        def manage_config():
            """Get or update configuration"""
            if request.method == 'GET':
                # Return current config (without sensitive data)
                safe_config = asdict(self.config)
                safe_config.pop('email_password', None)
                safe_config.pop('telegram_bot_token', None)
                return jsonify(safe_config)
            
            elif request.method == 'POST':
                try:
                    new_config = request.get_json()
                    
                    # Update configuration
                    for key, value in new_config.items():
                        if hasattr(self.config, key):
                            setattr(self.config, key, value)
                    
                    logger.info("Configuration updated")
                    return jsonify({
                        'success': True,
                        'message': 'Configuration updated',
                        'timestamp': datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    logger.error(f"Error updating configuration: {e}")
                    return jsonify({
                        'success': False,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
    
    def send_notification_sync(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send notification to all enabled channels (synchronous)"""
        results = {}
        
        if self.config.email_enabled:
            try:
                results['email'] = self.send_email(notification)
            except Exception as e:
                logger.error(f"Email notification failed: {e}")
                results['email'] = {'success': False, 'error': str(e)}
        
        if self.config.slack_enabled:
            try:
                results['slack'] = self.send_slack(notification)
            except Exception as e:
                logger.error(f"Slack notification failed: {e}")
                results['slack'] = {'success': False, 'error': str(e)}
        
        if self.config.telegram_enabled:
            try:
                results['telegram'] = self.send_telegram(notification)
            except Exception as e:
                logger.error(f"Telegram notification failed: {e}")
                results['telegram'] = {'success': False, 'error': str(e)}
        
        return results
    
    def send_email(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send email notification"""
        if not self.config.email_enabled or not self.config.email_to:
            return {'success': False, 'error': 'Email not configured'}
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[VolFlow] {notification.title}"
            msg['From'] = self.config.email_from
            msg['To'] = ', '.join(self.config.email_to)
            
            # Create HTML content
            html_content = self.templates['email_html'].render(
                title=notification.title,
                message=notification.message,
                priority=notification.priority,
                data=notification.data,
                timestamp=notification.timestamp
            )
            
            # Create plain text version
            text_content = f"""
{notification.title}

{notification.message}

Details:
{chr(10).join([f"• {k.replace('_', ' ').title()}: {v}" for k, v in notification.data.items()])}

Timestamp: {notification.timestamp}
VolFlow Options Breakout System
            """
            
            # Attach parts
            part1 = MIMEText(text_content, 'plain')
            part2 = MIMEText(html_content, 'html')
            
            msg.attach(part1)
            msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.config.email_smtp_server, self.config.email_smtp_port) as server:
                server.starttls()
                server.login(self.config.email_username, self.config.email_password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {len(self.config.email_to)} recipients")
            return {'success': True, 'recipients': len(self.config.email_to)}
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return {'success': False, 'error': str(e)}
    
    def send_slack(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send Slack notification"""
        if not self.config.slack_enabled or not self.config.slack_webhook_url:
            return {'success': False, 'error': 'Slack not configured'}
        
        try:
            # Prepare timestamp
            timestamp_unix = int(datetime.fromisoformat(notification.timestamp.replace('Z', '+00:00')).timestamp())
            
            # Create Slack message
            slack_payload = json.loads(self.templates['slack_message'].render(
                channel=self.config.slack_channel,
                username=self.config.slack_username,
                title=notification.title,
                message=notification.message,
                priority=notification.priority,
                data=notification.data,
                timestamp_unix=timestamp_unix
            ))
            
            # Send to Slack
            response = requests.post(
                self.config.slack_webhook_url,
                json=slack_payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
                return {'success': True, 'channel': self.config.slack_channel}
            else:
                logger.error(f"Slack API error: {response.status_code} - {response.text}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return {'success': False, 'error': str(e)}
    
    def send_telegram(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send Telegram notification"""
        if not self.config.telegram_enabled or not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            return {'success': False, 'error': 'Telegram not configured'}
        
        try:
            # Create Telegram message
            message_text = self.templates['telegram_message'].render(
                title=notification.title,
                message=notification.message,
                data=notification.data,
                timestamp=notification.timestamp
            )
            
            # Send to Telegram
            url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.config.telegram_chat_id,
                'text': message_text,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info("Telegram notification sent successfully")
                return {'success': True, 'chat_id': self.config.telegram_chat_id}
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")
            return {'success': False, 'error': str(e)}
    
    def run(self, host='0.0.0.0', port=5002, debug=False):
        """Run the notification server"""
        logger.info(f"Starting VolFlow Notification Server on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='VolFlow Notification Server')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5002, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Create and run server
    server = NotificationServer(config_file=args.config)
    server.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == '__main__':
    main()
