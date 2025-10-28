#!/usr/bin/env python3
"""
VolFlow Google SSO Authentication Server
Provides secure single-user Google OAuth authentication
"""

from flask import Flask, request, redirect, session, jsonify, render_template_string
from google.auth.transport import requests
from google.oauth2 import id_token
import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import secrets
import logging
import asyncio
import threading
from websocket_server_modular import ModularWebSocketServer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_google_credentials_from_secrets():
    """
    Retrieve Google OAuth credentials from AWS Secrets Manager
    """
    secret_name = os.getenv('GOOGLE_OAUTH_SECRET_NAME', 'google-oauth-credentials')
    region_name = os.getenv('AWS_REGION', 'us-east-1')
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        # Parse the secret
        secret = json.loads(get_secret_value_response['SecretString'])
        
        logger.info(f"Successfully retrieved Google OAuth credentials from AWS Secrets Manager")
        return secret
        
    except ClientError as e:
        logger.error(f"Error retrieving secret from AWS Secrets Manager: {e}")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing secret JSON: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error retrieving Google credentials: {e}")
        raise e

def load_google_credentials():
    """
    Load Google OAuth credentials from AWS Secrets Manager
    """
    try:
        # Get credentials from AWS Secrets Manager
        google_credentials = get_google_credentials_from_secrets()
        client_id = google_credentials.get('client_id')
        authorized_email = google_credentials.get('authorized_email', 'your-email@gmail.com')
        
        if not client_id:
            raise ValueError("client_id not found in AWS Secrets Manager")
            
        logger.info("Google OAuth credentials successfully loaded from AWS Secrets Manager")
        return client_id, authorized_email
        
    except Exception as e:
        logger.error(f"Failed to load Google OAuth credentials from AWS Secrets Manager: {e}")
        raise ValueError("Missing Google OAuth credentials - AWS Secrets Manager is required")

# Load Google credentials
GOOGLE_CLIENT_ID, AUTHORIZED_EMAIL = load_google_credentials()
logger.info(f"Google SSO configured for authorized user: {AUTHORIZED_EMAIL}")

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))

class AuthManager:
    def __init__(self):
        self.authorized_users = [AUTHORIZED_EMAIL.lower()]
        self.active_sessions = {}
        logger.info(f"AuthManager initialized with {len(self.authorized_users)} authorized user(s)")
    
    def is_authorized_user(self, email):
        """Check if user email is in authorized list"""
        is_auth = email.lower() in self.authorized_users
        logger.info(f"Authorization check for {email}: {'ALLOWED' if is_auth else 'DENIED'}")
        return is_auth
    
    def create_session(self, user_info):
        """Create secure session for authorized user"""
        if not self.is_authorized_user(user_info['email']):
            logger.warning(f"Unauthorized session creation attempt: {user_info['email']}")
            return None
        
        session_token = secrets.token_urlsafe(32)
        self.active_sessions[session_token] = {
            'user': user_info,
            'created_at': datetime.now(),
            'expires_at': datetime.now() + timedelta(hours=24),
            'last_activity': datetime.now()
        }
        
        logger.info(f"Session created for {user_info['email']}: {session_token[:8]}...")
        return session_token
    
    def validate_session(self, token):
        """Validate session token"""
        if not token or token not in self.active_sessions:
            return None
        
        session_data = self.active_sessions[token]
        if datetime.now() > session_data['expires_at']:
            logger.info(f"Session expired: {token[:8]}...")
            del self.active_sessions[token]
            return None
        
        # Update last activity
        session_data['last_activity'] = datetime.now()
        return session_data['user']
    
    def revoke_session(self, token):
        """Revoke a session"""
        if token in self.active_sessions:
            user_email = self.active_sessions[token]['user']['email']
            del self.active_sessions[token]
            logger.info(f"Session revoked for {user_email}")
            return True
        return False

# Initialize auth manager
auth_manager = AuthManager()

# Login page HTML template
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VolFlow - Secure Login</title>
    <script src="https://accounts.google.com/gsi/client" async defer></script>
    <style>
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0;
        }
        
        .login-container {
            background: white;
            border-radius: 20px;
            padding: 3rem;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            text-align: center;
            max-width: 400px;
            width: 90%;
        }
        
        .logo {
            font-size: 2.5rem;
            margin-bottom: 1rem;
        }
        
        .title {
            font-size: 1.8rem;
            font-weight: 600;
            color: #2d3748;
            margin-bottom: 0.5rem;
        }
        
        .subtitle {
            color: #718096;
            margin-bottom: 2rem;
            font-size: 0.9rem;
        }
        
        .google-signin {
            margin: 2rem 0;
        }
        
        .security-notice {
            background: #f7fafc;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 1rem;
            margin-top: 2rem;
            font-size: 0.8rem;
            color: #4a5568;
        }
        
        .error-message {
            background: #fed7d7;
            border: 1px solid #feb2b2;
            color: #742a2a;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
            display: none;
        }
        
        .loading {
            display: none;
            color: #667eea;
            margin: 1rem 0;
        }
        
        .spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f3f3;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="logo">📈</div>
        <h1 class="title">VolFlow Options</h1>
        <p class="subtitle">Secure access to your trading dashboard</p>
        
        <div class="error-message" id="error-message"></div>
        <div class="loading" id="loading">
            <div class="spinner"></div>
            Verifying access...
        </div>
        
        <div class="google-signin">
            <div id="g_id_onload"
                 data-client_id="{{ client_id }}"
                 data-callback="handleCredentialResponse"
                 data-auto_prompt="false">
            </div>
            <div class="g_id_signin"
                 data-type="standard"
                 data-size="large"
                 data-theme="outline"
                 data-text="sign_in_with"
                 data-shape="rectangular"
                 data-logo_alignment="left">
            </div>
        </div>
        
        <div class="security-notice">
            <strong>🔒 Authorized Access Only</strong><br>
            This application is restricted to authorized users only. 
            Unauthorized access attempts are logged and monitored.
        </div>
    </div>

    <script>
        function handleCredentialResponse(response) {
            const loadingEl = document.getElementById('loading');
            const errorEl = document.getElementById('error-message');
            
            // Show loading state
            loadingEl.style.display = 'block';
            errorEl.style.display = 'none';
            
            // Send token to backend for verification
            fetch('/auth/google', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    credential: response.credential
                })
            })
            .then(response => response.json())
            .then(data => {
                loadingEl.style.display = 'none';
                
                if (data.success) {
                    // Store session token
                    localStorage.setItem('volflow_session', data.session_token);
                    localStorage.setItem('volflow_user', JSON.stringify(data.user));
                    
                    // Redirect to dashboard
                    window.location.href = data.redirect_url;
                } else {
                    // Show error message
                    errorEl.textContent = data.error;
                    errorEl.style.display = 'block';
                    
                    // Log unauthorized access attempt
                    console.warn('Access denied:', data);
                }
            })
            .catch(error => {
                loadingEl.style.display = 'none';
                errorEl.textContent = 'Login failed. Please try again.';
                errorEl.style.display = 'block';
                console.error('Login error:', error);
            });
        }
        
        // Check if user is already logged in
        window.onload = function() {
            const sessionToken = localStorage.getItem('volflow_session');
            if (sessionToken) {
                // Validate existing session
                fetch('/auth/validate', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        session_token: sessionToken
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.valid) {
                        // Redirect to dashboard if session is valid
                        window.location.href = '/dashboard';
                    }
                })
                .catch(error => {
                    console.log('Session validation failed:', error);
                });
            }
        };
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Main login page"""
    return render_template_string(LOGIN_TEMPLATE, client_id=GOOGLE_CLIENT_ID)

@app.route('/auth/google', methods=['POST'])
def google_auth():
    """Handle Google OAuth token verification"""
    try:
        # Get the token from the request
        token = request.json.get('credential')
        
        if not token:
            return jsonify({
                'success': False,
                'error': 'No credential provided'
            }), 400
        
        # Verify the token with Google
        idinfo = id_token.verify_oauth2_token(
            token, requests.Request(), GOOGLE_CLIENT_ID
        )
        
        # Extract user information
        user_email = idinfo['email']
        user_name = idinfo['name']
        user_picture = idinfo.get('picture', '')
        
        logger.info(f"Login attempt from: {user_email}")
        
        # Check if user is authorized
        if not auth_manager.is_authorized_user(user_email):
            logger.warning(f"UNAUTHORIZED ACCESS ATTEMPT: {user_email}")
            return jsonify({
                'success': False,
                'error': 'Access denied. You are not authorized to access this application.',
                'code': 'UNAUTHORIZED_USER'
            }), 403
        
        # Create session for authorized user
        user_info = {
            'email': user_email,
            'name': user_name,
            'picture': user_picture,
            'login_time': datetime.now().isoformat()
        }
        
        session_token = auth_manager.create_session(user_info)
        
        if session_token:
            logger.info(f"✅ AUTHORIZED LOGIN: {user_email}")
            return jsonify({
                'success': True,
                'user': user_info,
                'session_token': session_token,
                'redirect_url': '/dashboard'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to create session'
            }), 500
            
    except ValueError as e:
        logger.error(f"Token verification failed: {e}")
        return jsonify({
            'success': False,
            'error': 'Invalid token'
        }), 400
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return jsonify({
            'success': False,
            'error': 'Authentication failed'
        }), 500

@app.route('/auth/validate', methods=['POST'])
def validate_session():
    """Validate session token"""
    try:
        token = request.json.get('session_token')
        user = auth_manager.validate_session(token)
        
        if user:
            return jsonify({
                'valid': True,
                'user': user
            })
        else:
            return jsonify({
                'valid': False
            }), 401
    except Exception as e:
        logger.error(f"Session validation error: {e}")
        return jsonify({
            'valid': False
        }), 401

@app.route('/auth/logout', methods=['POST'])
def logout():
    """Logout user and invalidate session"""
    try:
        token = request.json.get('session_token')
        if auth_manager.revoke_session(token):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Session not found'})
    except Exception as e:
        logger.error(f"Logout error: {e}")
        return jsonify({'success': False, 'error': 'Logout failed'})

@app.route('/dashboard')
def dashboard():
    """Protected dashboard - serve main app"""
    try:
        with open('index.html', 'r') as f:
            return f.read()
    except FileNotFoundError:
        return jsonify({
            'error': 'Dashboard not found',
            'message': 'Please ensure index.html exists in the project directory'
        }), 404

@app.route('/index.html')
def serve_index():
    """Serve the main dashboard"""
    try:
        with open('index.html', 'r') as f:
            return f.read()
    except FileNotFoundError:
        return jsonify({
            'error': 'Dashboard not found',
            'message': 'Please ensure index.html exists in the project directory'
        }), 404

@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files (CSS, JS, etc.)"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
        
        # Set appropriate content type
        if filename.endswith('.css'):
            return content, 200, {'Content-Type': 'text/css'}
        elif filename.endswith('.js'):
            return content, 200, {'Content-Type': 'application/javascript'}
        elif filename.endswith('.json'):
            return content, 200, {'Content-Type': 'application/json'}
        else:
            return content
    except FileNotFoundError:
        return jsonify({'error': f'File {filename} not found'}), 404

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'volflow-auth',
        'timestamp': datetime.now().isoformat(),
        'active_sessions': len(auth_manager.active_sessions)
    })

def create_auth_app():
    """
    Create and configure the Flask authentication app
    This allows the auth module to be imported and integrated into other applications
    """
    auth_app = Flask(__name__)
    auth_app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))
    
    # Register all the routes with the app
    auth_app.add_url_rule('/', 'index', index)
    auth_app.add_url_rule('/auth/google', 'google_auth', google_auth, methods=['POST'])
    auth_app.add_url_rule('/auth/validate', 'validate_session', validate_session, methods=['POST'])
    auth_app.add_url_rule('/auth/logout', 'logout', logout, methods=['POST'])
    auth_app.add_url_rule('/dashboard', 'dashboard', dashboard)
    auth_app.add_url_rule('/index.html', 'serve_index', serve_index)
    auth_app.add_url_rule('/<path:filename>', 'serve_static', serve_static)
    auth_app.add_url_rule('/health', 'health', health)
    
    return auth_app

def get_auth_manager():
    """
    Get the global auth manager instance
    This allows other modules to access the authentication state
    """
    return auth_manager

def register_auth_routes(flask_app):
    """
    Register authentication routes with an existing Flask app
    This allows integration with existing applications
    """
    flask_app.add_url_rule('/auth', 'auth_index', index)
    flask_app.add_url_rule('/auth/google', 'google_auth', google_auth, methods=['POST'])
    flask_app.add_url_rule('/auth/validate', 'validate_session', validate_session, methods=['POST'])
    flask_app.add_url_rule('/auth/logout', 'logout', logout, methods=['POST'])
    flask_app.add_url_rule('/auth/health', 'auth_health', health)

def get_google_client_id():
    """
    Get the Google Client ID for use in other modules
    """
    return GOOGLE_CLIENT_ID

def get_authorized_email():
    """
    Get the authorized email for use in other modules
    """
    return AUTHORIZED_EMAIL

def start_websocket_server(port=8765):
    """
    Start the websocket server in a separate thread
    """
    def run_websocket_server():
        try:
            logger.info(f"🚀 Starting WebSocket server on port {port}")
            websocket_server = ModularWebSocketServer(port)
            asyncio.run(websocket_server.start_server())
        except Exception as e:
            logger.error(f"❌ Error starting WebSocket server: {e}")
    
    # Start websocket server in a separate thread
    websocket_thread = threading.Thread(target=run_websocket_server, daemon=True)
    websocket_thread.start()
    logger.info(f"✅ WebSocket server thread started on port {port}")
    return websocket_thread

def start_combined_server(auth_port=5001, websocket_port=8765):
    """
    Start both the authentication server and websocket server
    """
    logger.info("🚀 Starting VolFlow Combined Server (Auth + WebSocket)...")
    logger.info(f"Google Client ID: {GOOGLE_CLIENT_ID[:20]}...")
    logger.info(f"Authorized Email: {AUTHORIZED_EMAIL}")
    
    # Start websocket server in background
    websocket_thread = start_websocket_server(websocket_port)
    
    # Start Flask authentication server
    logger.info(f"🌐 Starting Flask authentication server on port {auth_port}")
    app.run(
        host='0.0.0.0', 
        port=auth_port, 
        debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    )

if __name__ == '__main__':
    # Get ports from environment variables
    auth_port = int(os.environ.get('AUTH_PORT', 5001))
    websocket_port = int(os.environ.get('WEBSOCKET_PORT', 8765))
    
    # Check if we should start combined server or just auth server
    start_websocket = os.environ.get('START_WEBSOCKET', 'true').lower() == 'true'
    
    if start_websocket:
        # Start combined server (auth + websocket)
        start_combined_server(auth_port, websocket_port)
    else:
        # Start only authentication server
        logger.info("Starting VolFlow Authentication Server (Auth Only)...")
        logger.info(f"Google Client ID: {GOOGLE_CLIENT_ID[:20]}...")
        logger.info(f"Authorized Email: {AUTHORIZED_EMAIL}")
        
        app.run(
            host='0.0.0.0', 
            port=auth_port, 
            debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
        )
