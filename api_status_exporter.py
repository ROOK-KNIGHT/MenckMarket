#!/usr/bin/env python3
"""
API Status Exporter
Extracts token data and API connection status, outputting to JSON file
that matches the frontend security panel field structure.
"""

import json
import os
import sys
from datetime import datetime, timezone
import requests
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import connection_manager
    print("✅ Connection manager loaded - AWS Secrets Manager integration available")
except ImportError:
    print("Warning: connection_manager module not found - falling back to local file only")
    connection_manager = None


class APIStatusExporter:
    def __init__(self):
        self.status_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "authentication": {
                "status": "unknown",
                "authenticated": False,
                "token_expiry": None,
                "expires_at": None,
                "last_check": None,
                "account_info": {
                    "account_number": None,
                    "available": False
                }
            },
            "connections": {
                "schwab": {
                    "status": "disconnected",
                    "connected": False,
                    "last_test": None,
                    "error": None,
                    "token_expires": None,
                    "details": {}
                }
            },
            "frontend_fields": {
                "auth-status": "Unknown",
                "auth-indicator": "unknown",
                "schwab-status": "Checking...",
                "schwab-indicator": "disconnected",
                "connection-last-updated": "Never",
                "token-expiry": "Unknown",
                "last-auth-check": "--:--:--",
                "auth-account-info": "Not Available"
            },
            "last_updated": datetime.now().isoformat()
        }
    
    def check_schwab_token_status(self):
        """Check Schwab API token status"""
        try:
            # Try to load tokens using connection_manager (which handles AWS Secrets Manager)
            token_data = None
            
            if connection_manager:
                try:
                    print("🔍 Attempting to load tokens from AWS Secrets Manager...")
                    token_data = connection_manager.load_tokens()
                    if token_data:
                        print("✅ Tokens loaded from AWS Secrets Manager")
                    else:
                        print("⚠️ No tokens found in AWS Secrets Manager")
                except Exception as e:
                    print(f"Warning: Failed to load tokens via connection_manager: {e}")
            
            # Fallback to local file if connection_manager fails
            if not token_data:
                token_file = Path("schwab_tokens.json")
                if not token_file.exists():
                    self.status_data["connections"]["schwab"]["status"] = "no_token"
                    self.status_data["connections"]["schwab"]["error"] = "Token not found in AWS or local file"
                    self.status_data["frontend_fields"]["schwab-status"] = "No Token"
                    self.status_data["frontend_fields"]["schwab-indicator"] = "error"
                    return
                
                # Load token data from local file
                with open(token_file, 'r') as f:
                    token_data = json.load(f)
            
            if not token_data:
                self.status_data["connections"]["schwab"]["status"] = "no_token"
                self.status_data["connections"]["schwab"]["error"] = "No token data available"
                self.status_data["frontend_fields"]["schwab-status"] = "No Token"
                self.status_data["frontend_fields"]["schwab-indicator"] = "error"
                return
            
            # Check token expiration
            if 'expires_at' in token_data:
                expires_at_str = token_data['expires_at']
                print(f"🔍 Token expires_at: {expires_at_str}")
                
                # Handle different datetime formats
                try:
                    if expires_at_str.endswith('Z'):
                        expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                    elif '+' in expires_at_str or expires_at_str.endswith('00:00'):
                        expires_at = datetime.fromisoformat(expires_at_str)
                    else:
                        # Assume local time, convert to UTC-aware
                        expires_at = datetime.fromisoformat(expires_at_str)
                        if expires_at.tzinfo is None:
                            expires_at = expires_at.replace(tzinfo=timezone.utc)
                    
                    now = datetime.now(expires_at.tzinfo if expires_at.tzinfo else timezone.utc)
                    print(f"🔍 Parsed expiration: {expires_at}")
                    print(f"🔍 Current time: {now}")
                except Exception as e:
                    print(f"❌ Error parsing expires_at: {e}")
                    expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                
                if expires_at <= now:
                    self.status_data["connections"]["schwab"]["status"] = "expired"
                    self.status_data["authentication"]["status"] = "expired"
                    self.status_data["frontend_fields"]["schwab-status"] = "Token Expired"
                    self.status_data["frontend_fields"]["schwab-indicator"] = "warning"
                    self.status_data["frontend_fields"]["auth-status"] = "Expired"
                    self.status_data["frontend_fields"]["auth-indicator"] = "expired"
                    return
                
                # Calculate time until expiry
                time_until_expiry = expires_at - now
                hours = int(time_until_expiry.total_seconds() // 3600)
                minutes = int((time_until_expiry.total_seconds() % 3600) // 60)
                
                self.status_data["authentication"]["expires_at"] = expires_at.isoformat()
                self.status_data["authentication"]["token_expiry"] = f"{hours}h {minutes}m"
                self.status_data["connections"]["schwab"]["token_expires"] = expires_at.isoformat()
                self.status_data["frontend_fields"]["token-expiry"] = f"{hours}h {minutes}m"
                
                # Check if expiring soon (within 30 minutes)
                if time_until_expiry.total_seconds() <= 1800:  # 30 minutes
                    self.status_data["authentication"]["status"] = "expiring_soon"
                    self.status_data["frontend_fields"]["auth-status"] = "Expiring Soon"
                    self.status_data["frontend_fields"]["auth-indicator"] = "expiring"
            
            # Test API connection using connection_manager
            self.test_schwab_api_connection()
                
        except Exception as e:
            self.status_data["connections"]["schwab"]["status"] = "error"
            self.status_data["connections"]["schwab"]["error"] = str(e)
            self.status_data["frontend_fields"]["schwab-status"] = "Error"
            self.status_data["frontend_fields"]["schwab-indicator"] = "error"
    
    def test_schwab_api_connection(self, access_token=None):
        """Test Schwab API connection using connection_manager"""
        try:
            print("🔍 Testing API connection using connection_manager...")
            
            if not connection_manager:
                raise Exception("Connection manager not available")
            
            # Use connection_manager's authenticated request method
            url = 'https://api.schwabapi.com/trader/v1/accounts/accountNumbers'
            print(f"🔍 Making authenticated API call to: {url}")
            
            success, account_data = connection_manager.make_authenticated_request(
                url, 
                "test API connection for status exporter"
            )
            
            print(f"🔍 API call success: {success}")
            
            if success and account_data:
                print(f"🔍 Account data received: {len(account_data) if isinstance(account_data, list) else 'non-list'} accounts")
                
                self.status_data["connections"]["schwab"]["status"] = "connected"
                self.status_data["connections"]["schwab"]["connected"] = True
                self.status_data["authentication"]["authenticated"] = True
                self.status_data["authentication"]["status"] = "authenticated"
                
                # Update frontend fields
                self.status_data["frontend_fields"]["schwab-status"] = "Connected"
                self.status_data["frontend_fields"]["schwab-indicator"] = "connected"
                self.status_data["frontend_fields"]["auth-status"] = "Authenticated"
                self.status_data["frontend_fields"]["auth-indicator"] = "authenticated"
                
                # Extract account info if available
                if isinstance(account_data, list) and len(account_data) > 0:
                    first_account = account_data[0]
                    account_number = first_account.get('accountNumber', 'Unknown')
                    self.status_data["authentication"]["account_info"]["account_number"] = account_number
                    self.status_data["authentication"]["account_info"]["available"] = True
                    self.status_data["frontend_fields"]["auth-account-info"] = f"Account: {account_number}"
                    print(f"✅ Account info extracted: {account_number}")
                
            else:
                # API call failed
                self.status_data["connections"]["schwab"]["status"] = "error"
                self.status_data["connections"]["schwab"]["error"] = "API call failed via connection_manager"
                self.status_data["frontend_fields"]["schwab-status"] = "Error"
                self.status_data["frontend_fields"]["schwab-indicator"] = "error"
                print("❌ API call failed via connection_manager")
                
            self.status_data["connections"]["schwab"]["last_test"] = datetime.now().isoformat()
            
        except Exception as e:
            self.status_data["connections"]["schwab"]["status"] = "error"
            self.status_data["connections"]["schwab"]["error"] = f"Error using connection_manager: {str(e)}"
            self.status_data["frontend_fields"]["schwab-status"] = "Error"
            self.status_data["frontend_fields"]["schwab-indicator"] = "error"
            print(f"❌ Error using connection_manager: {e}")
    
    
    def update_timestamps(self):
        """Update timestamp fields"""
        now = datetime.now()
        self.status_data["authentication"]["last_check"] = now.isoformat()
        self.status_data["frontend_fields"]["last-auth-check"] = now.strftime("%H:%M:%S")
        self.status_data["frontend_fields"]["connection-last-updated"] = f"Last updated: {now.strftime('%H:%M:%S')}"
        self.status_data["last_updated"] = now.isoformat()
    
    def export_status(self):
        """Export complete API status"""
        print("🔍 Checking Schwab API token status...")
        self.check_schwab_token_status()
        
        print("⏰ Updating timestamps...")
        self.update_timestamps()
        
        return self.status_data
    
    def save_to_file(self, filename="api_status.json"):
        """Save status data to JSON file"""
        try:
            status = self.export_status()
            
            with open(filename, 'w') as f:
                json.dump(status, f, indent=2, default=str)
            
            print(f"✅ API status exported to {filename}")
            
            # Print summary
            print("\n📊 Status Summary:")
            print(f"   Authentication: {status['frontend_fields']['auth-status']}")
            print(f"   Schwab API: {status['frontend_fields']['schwab-status']}")
            print(f"   Token Expiry: {status['frontend_fields']['token-expiry']}")
            print(f"   Last Check: {status['frontend_fields']['last-auth-check']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving status to file: {e}")
            return False


def main():
    """Main function to run the API status exporter"""
    print("🚀 API Status Exporter")
    print("=" * 50)
    
    exporter = APIStatusExporter()
    
    # Export to default file
    success = exporter.save_to_file()
    
    if success:
        print("\n✅ Export completed successfully!")
        print("📄 Check api_status.json for the complete status data")
    else:
        print("\n❌ Export failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
