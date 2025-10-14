#!/usr/bin/env python3
"""
Authentication API Server
Provides REST API endpoints for managing Schwab API authentication
"""

import json
import os
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import connection_manager

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests

@app.route('/api/auth/status', methods=['GET'])
def get_auth_status():
    """Get current authentication status"""
    try:
        # Try to load and validate tokens
        tokens = connection_manager.load_tokens()
        
        if not tokens:
            return jsonify({
                'authenticated': False,
                'error': 'No tokens found',
                'expires_at': None,
                'account_info': None
            })
        
        # Check if tokens are valid and not expired
        expires_at = tokens.get('expires_at')
        if expires_at:
            try:
                expires_datetime = datetime.fromisoformat(expires_at)
                current_time = datetime.now()
                
                if current_time >= expires_datetime:
                    return jsonify({
                        'authenticated': False,
                        'error': 'Tokens expired',
                        'expires_at': expires_at,
                        'account_info': None
                    })
                
                # Tokens are valid
                return jsonify({
                    'authenticated': True,
                    'expires_at': expires_at,
                    'account_info': {
                        'account_number': 'Connected',  # Would get real account info in production
                        'status': 'Active'
                    }
                })
                
            except ValueError:
                return jsonify({
                    'authenticated': False,
                    'error': 'Invalid token expiration format',
                    'expires_at': None,
                    'account_info': None
                })
        else:
            return jsonify({
                'authenticated': False,
                'error': 'No expiration time in tokens',
                'expires_at': None,
                'account_info': None
            })
            
    except Exception as e:
        return jsonify({
            'authenticated': False,
            'error': f'Error checking auth status: {str(e)}',
            'expires_at': None,
            'account_info': None
        }), 500

@app.route('/api/auth/refresh', methods=['POST'])
def refresh_tokens():
    """Refresh authentication tokens"""
    try:
        # Load current tokens
        tokens = connection_manager.load_tokens()
        
        if not tokens:
            return jsonify({
                'success': False,
                'error': 'No tokens found to refresh'
            }), 400
        
        refresh_token = tokens.get('refresh_token')
        if not refresh_token:
            return jsonify({
                'success': False,
                'error': 'No refresh token available'
            }), 400
        
        # Attempt to refresh tokens
        new_tokens = connection_manager.refresh_tokens(refresh_token)
        
        if new_tokens:
            return jsonify({
                'success': True,
                'message': 'Tokens refreshed successfully',
                'expires_at': new_tokens.get('expires_at')
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to refresh tokens - may need re-authentication'
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error refreshing tokens: {str(e)}'
        }), 500

@app.route('/api/auth/reauth', methods=['POST'])
def initiate_reauth():
    """Initiate re-authentication process"""
    try:
        # Generate the authorization URL with the correct callback URL
        callback_url = "https://127.0.0.1"
        auth_url = f"{connection_manager.BASE_URL}/v1/oauth/authorize?response_type=code&client_id={connection_manager.APP_KEY}&redirect_uri={callback_url}&scope=readonly"
        
        return jsonify({
            'success': True,
            'auth_url': auth_url,
            'message': 'Authentication URL generated. Complete authentication in the new window.'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error generating auth URL: {str(e)}'
        }), 500

@app.route('/api/auth/test', methods=['GET'])
def test_api_connection():
    """Test API connection and permissions"""
    try:
        # Ensure we have valid tokens
        tokens = connection_manager.ensure_valid_tokens()
        
        if not tokens:
            return jsonify({
                'success': False,
                'error': 'No valid tokens available'
            }), 401
        
        access_token = tokens.get('access_token')
        if not access_token:
            return jsonify({
                'success': False,
                'error': 'No access token available'
            }), 401
        
        # Test API connection by getting account numbers
        try:
            account_numbers = connection_manager.get_account_numbers(access_token)
            
            if account_numbers:
                return jsonify({
                    'success': True,
                    'message': 'API connection test successful',
                    'account_numbers': len(account_numbers) if isinstance(account_numbers, list) else 1,
                    'test_time': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to retrieve account information'
                }), 400
                
        except Exception as api_error:
            return jsonify({
                'success': False,
                'error': f'API connection failed: {str(api_error)}'
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error testing API connection: {str(e)}'
        }), 500

@app.route('/api/auth/callback', methods=['GET'])
def auth_callback():
    """Handle OAuth callback - directly use connection_manager"""
    try:
        code = request.args.get('code')
        
        if not code:
            return jsonify({
                'success': False,
                'error': 'No authorization code provided'
            }), 400
        
        print(f"🔄 Processing authorization code: {code[:20]}...")
        
        # Use connection_manager directly - it handles all the token exchange logic
        tokens = connection_manager.get_tokens(code)
        
        if tokens:
            print("✅ Tokens exchanged and saved successfully")
            return jsonify({
                'success': True,
                'message': 'Authentication completed successfully',
                'expires_at': tokens.get('expires_at')
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to exchange code for tokens'
            }), 400
            
    except Exception as e:
        error_msg = f'Error processing callback: {str(e)}'
        print(f"❌ {error_msg}")
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'auth-api-server',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("🔐 Starting Authentication API Server...")
    print("📡 Available endpoints:")
    print("   GET  /api/auth/status   - Check authentication status")
    print("   POST /api/auth/refresh  - Refresh tokens")
    print("   POST /api/auth/reauth   - Initiate re-authentication")
    print("   GET  /api/auth/test     - Test API connection")
    print("   GET  /api/auth/callback - OAuth callback handler")
    print("   GET  /health            - Health check")
    print()
    
    # Run the server
    app.run(
        host='0.0.0.0',
        port=5001,  # Different port from main app
        debug=True,
        threaded=True
    )
