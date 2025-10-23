#!/usr/bin/env python3
"""
Token Structure Reader
Reads tokens from AWS Secrets Manager to understand the structure
"""

import json
import sys
import os
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import connection_manager
    print("✅ Connection manager loaded - AWS Secrets Manager integration available")
except ImportError:
    print("❌ Connection manager module not found")
    sys.exit(1)

def main():
    print("🔍 Reading token structure from AWS Secrets Manager...")
    print("=" * 60)
    
    try:
        # Load tokens using connection_manager
        token_data = connection_manager.load_tokens()
        
        if not token_data:
            print("❌ No token data found")
            return 1
        
        print("✅ Token data loaded successfully!")
        print("\n📊 Token Structure Analysis:")
        print("=" * 40)
        
        # Analyze the structure
        print(f"📋 Token data type: {type(token_data)}")
        print(f"📋 Number of keys: {len(token_data) if isinstance(token_data, dict) else 'N/A'}")
        
        if isinstance(token_data, dict):
            print("\n🔑 Available keys:")
            for key in token_data.keys():
                value = token_data[key]
                value_type = type(value).__name__
                
                # Show truncated value for sensitive data
                if key in ['access_token', 'refresh_token']:
                    display_value = f"{str(value)[:20]}..." if value else "None"
                else:
                    display_value = str(value)
                
                print(f"   - {key}: {value_type} = {display_value}")
        
        # Pretty print the full structure (with sensitive data masked)
        print("\n📄 Full Token Structure (sensitive data masked):")
        print("=" * 50)
        
        masked_data = token_data.copy() if isinstance(token_data, dict) else token_data
        if isinstance(masked_data, dict):
            for key in ['access_token', 'refresh_token']:
                if key in masked_data and masked_data[key]:
                    masked_data[key] = f"{masked_data[key][:20]}...{masked_data[key][-10:]}"
        
        print(json.dumps(masked_data, indent=2, default=str))
        
        # Check for expiration
        if isinstance(token_data, dict) and 'expires_at' in token_data:
            expires_at_str = token_data['expires_at']
            print(f"\n⏰ Token Expiration Analysis:")
            print(f"   Raw expires_at: {expires_at_str}")
            
            try:
                expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                now = datetime.now(expires_at.tzinfo)
                time_remaining = expires_at - now
                
                print(f"   Parsed expiration: {expires_at}")
                print(f"   Current time: {now}")
                print(f"   Time remaining: {time_remaining}")
                print(f"   Is expired: {expires_at <= now}")
                
            except Exception as e:
                print(f"   ❌ Error parsing expiration: {e}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error reading token structure: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
