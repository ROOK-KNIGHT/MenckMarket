#connection_manager.py

import base64
import requests
import webbrowser
import json
import urllib.parse
import os
from datetime import timedelta, datetime
import time
from dotenv import load_dotenv
from config_loader import get_config
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Load environment variables from .env file
load_dotenv()

# Load configuration
config = get_config()
api_config = config.get_api_config()

# AWS Secrets Manager configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECRET_NAME = os.getenv("SCHWAB_SECRET_NAME", "production/schwab-api/tokens")
CREDENTIALS_SECRET_NAME = os.getenv("SCHWAB_CREDENTIALS_SECRET_NAME", "production/schwab-api/credentials")
USE_AWS_SECRETS = os.getenv("USE_AWS_SECRETS", "true").lower() == "true"
TOKEN_FILE = os.getenv("SCHWAB_TOKEN_FILE")  # Keep as fallback for local development

# Initialize Schwab API credentials from AWS
APP_KEY = None
APP_SECRET = None
REDIRECT_URI = None

def load_schwab_credentials_from_aws():
    """Load Schwab API credentials from AWS Secrets Manager"""
    global APP_KEY, APP_SECRET, REDIRECT_URI
    
    try:
        if not secrets_client:
            return False
            
        # Try to get Schwab API credentials from AWS
        response = secrets_client.get_secret_value(SecretId=CREDENTIALS_SECRET_NAME)
        credentials = json.loads(response['SecretString'])
        
        # Map AWS secret keys to connection manager variables
        APP_KEY = credentials.get('schwab_client_id')
        APP_SECRET = credentials.get('schwab_client_secret')
        REDIRECT_URI = credentials.get('schwab_callback_url', 'https://127.0.0.1')
        
        if APP_KEY and APP_SECRET:
            print(f"✅ Schwab API credentials loaded from AWS: {CREDENTIALS_SECRET_NAME}")
            return True
        else:
            print("❌ Invalid Schwab API credentials in AWS secret")
            return False
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"❌ Schwab API credentials not found in AWS: {CREDENTIALS_SECRET_NAME}")
        else:
            print(f"❌ AWS error loading Schwab credentials: {e}")
        return False
    except Exception as e:
        print(f"❌ Error loading Schwab credentials from AWS: {e}")
        return False

# Initialize AWS Secrets Manager client if enabled
secrets_client = None
if USE_AWS_SECRETS:
    try:
        secrets_client = boto3.client('secretsmanager', region_name=AWS_REGION)
        print(f"✅ AWS Secrets Manager initialized for region: {AWS_REGION}")
        
        # Load Schwab API credentials from AWS
        if load_schwab_credentials_from_aws():
            print("✅ Connection manager ready with AWS credentials")
        else:
            print("❌ Failed to load Schwab credentials from AWS")
            USE_AWS_SECRETS = False
            
    except NoCredentialsError:
        print("❌ AWS credentials not found. Falling back to local token storage.")
        USE_AWS_SECRETS = False
    except Exception as e:
        print(f"❌ Failed to initialize AWS Secrets Manager: {e}")
        USE_AWS_SECRETS = False

# Use configured base URL - Updated for current Schwab API
BASE_URL = api_config.get('base_url', 'https://api.schwabapi.com')

# Initialize URLs after loading credentials
def initialize_urls():
    """Initialize AUTH_URL and TOKEN_URL after credentials are loaded"""
    global AUTH_URL, TOKEN_URL
    if APP_KEY and REDIRECT_URI:
        AUTH_URL = f"{BASE_URL}/v1/oauth/authorize?response_type=code&client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"
        TOKEN_URL = f"{BASE_URL}/v1/oauth/token"
    else:
        AUTH_URL = None
        TOKEN_URL = None

# Initialize URLs
initialize_urls()

# Get retry and timeout settings from configuration
MAX_RETRIES = api_config.get('max_retries', 5)
RETRY_DELAY = api_config.get('retry_delay', 2)
REQUEST_TIMEOUT = api_config.get('request_timeout', 10)
RATE_LIMIT_DELAY = api_config.get('rate_limit_delay', 60)

def save_tokens_to_aws(tokens):
    """Save tokens to AWS Secrets Manager"""
    try:
        # Calculate and save the expiration time as a string
        expires_at = datetime.now() + timedelta(seconds=int(tokens['expires_in']))
        tokens['expires_at'] = expires_at.isoformat()
        
        # Convert tokens to JSON string for AWS Secrets Manager
        secret_value = json.dumps(tokens)
        
        # Try to update existing secret first
        try:
            response = secrets_client.update_secret(
                SecretId=SECRET_NAME,
                SecretString=secret_value
            )
            print(f"✅ Tokens updated in AWS Secrets Manager: {SECRET_NAME}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Secret doesn't exist, create it
                response = secrets_client.create_secret(
                    Name=SECRET_NAME,
                    SecretString=secret_value,
                    Description="Schwab API tokens for trading application"
                )
                print(f"✅ New secret created in AWS Secrets Manager: {SECRET_NAME}")
                return True
            else:
                raise e
                
    except Exception as e:
        print(f"❌ Failed to save tokens to AWS Secrets Manager: {e}")
        return False

def get_comprehensive_account_data():
    """
    Get comprehensive account data including all balances, positions, and account information.
    This function returns the full Schwab API response structure.
    
    Returns:
        Dictionary containing full account data structure as returned by Schwab API
    """
    try:
        print("📊 Fetching comprehensive account data (full API response)...")
        
        url = "https://api.schwabapi.com/trader/v1/accounts?fields=positions"
        success, accounts_data = make_authenticated_request(url, "get comprehensive account data")
        
        if not success or not accounts_data:
            print("❌ Failed to get comprehensive account data")
            return None
        
        # Return the full response structure with additional metadata
        comprehensive_data = {
            'accounts': accounts_data,
            'metadata': {
                'fetch_timestamp': datetime.now().isoformat(),
                'total_accounts': len(accounts_data),
                'api_endpoint': url,
                'fields_requested': 'positions',
                'response_structure': 'full_schwab_api_format'
            }
        }
        
        # Log summary information
        total_positions = 0
        total_accounts = len(accounts_data)
        
        for account in accounts_data:
            securities_account = account.get('securitiesAccount', {})
            positions = securities_account.get('positions', [])
            total_positions += len(positions)
            
            account_number = securities_account.get('accountNumber', 'Unknown')
            account_type = securities_account.get('type', 'Unknown')
            
            print(f"✅ Account {account_number} ({account_type}): {len(positions)} positions")
            
            # Log balance summary
            current_balances = securities_account.get('currentBalances', {})
            if current_balances:
                equity = current_balances.get('equity', 0)
                buying_power = current_balances.get('buyingPower', 0)
                print(f"   Equity: ${equity:,.2f}, Buying Power: ${buying_power:,.2f}")
        
        print(f"✅ Retrieved comprehensive data: {total_accounts} accounts, {total_positions} total positions")
        return comprehensive_data
        
    except Exception as e:
        print(f"❌ Error getting comprehensive account data: {str(e)}")
        return None

def extract_account_balances(comprehensive_data):
    """
    Extract and format account balance information from comprehensive data.
    
    Args:
        comprehensive_data: Full account data from get_comprehensive_account_data()
        
    Returns:
        Dictionary with formatted balance information for each account
    """
    try:
        if not comprehensive_data or 'accounts' not in comprehensive_data:
            return {}
        
        balances_summary = {}
        
        for account in comprehensive_data['accounts']:
            securities_account = account.get('securitiesAccount', {})
            account_number = securities_account.get('accountNumber')
            
            if not account_number:
                continue
            
            # Extract all balance types
            initial_balances = securities_account.get('initialBalances', {})
            current_balances = securities_account.get('currentBalances', {})
            projected_balances = securities_account.get('projectedBalances', {})
            aggregated_balance = account.get('aggregatedBalance', {})
            
            balances_summary[account_number] = {
                'account_info': {
                    'account_number': account_number,
                    'account_type': securities_account.get('type'),
                    'is_day_trader': securities_account.get('isDayTrader', False),
                    'round_trips': securities_account.get('roundTrips', 0)
                },
                'initial_balances': initial_balances,
                'current_balances': current_balances,
                'projected_balances': projected_balances,
                'aggregated_balance': aggregated_balance,
                'balance_summary': {
                    'current_equity': current_balances.get('equity', 0),
                    'buying_power': current_balances.get('buyingPower', 0),
                    'cash_balance': current_balances.get('cashBalance', 0),
                    'day_trading_buying_power': current_balances.get('dayTradingBuyingPower', 0),
                    'long_market_value': current_balances.get('longMarketValue', 0),
                    'short_market_value': current_balances.get('shortMarketValue', 0),
                    'maintenance_requirement': current_balances.get('maintenanceRequirement', 0),
                    'available_funds': current_balances.get('availableFunds', 0)
                }
            }
        
        print(f"✅ Extracted balance data for {len(balances_summary)} accounts")
        return balances_summary
        
    except Exception as e:
        print(f"❌ Error extracting account balances: {str(e)}")
        return {}

def extract_detailed_positions(comprehensive_data):
    """
    Extract detailed position information from comprehensive data.
    
    Args:
        comprehensive_data: Full account data from get_comprehensive_account_data()
        
    Returns:
        Dictionary with detailed position information
    """
    try:
        if not comprehensive_data or 'accounts' not in comprehensive_data:
            return {}
        
        detailed_positions = {}
        
        for account in comprehensive_data['accounts']:
            securities_account = account.get('securitiesAccount', {})
            account_number = securities_account.get('accountNumber')
            positions = securities_account.get('positions', [])
            
            if not account_number:
                continue
            
            account_positions = []
            
            for pos in positions:
                instrument = pos.get('instrument', {})
                
                # Create detailed position entry
                position_detail = {
                    # Instrument details
                    'symbol': instrument.get('symbol'),
                    'cusip': instrument.get('cusip'),
                    'asset_type': instrument.get('assetType'),
                    'description': instrument.get('description', ''),
                    
                    # Position quantities
                    'short_quantity': pos.get('shortQuantity', 0),
                    'long_quantity': pos.get('longQuantity', 0),
                    'settled_long_quantity': pos.get('settledLongQuantity', 0),
                    'settled_short_quantity': pos.get('settledShortQuantity', 0),
                    'previous_session_long_quantity': pos.get('previousSessionLongQuantity', 0),
                    
                    # Pricing information
                    'average_price': pos.get('averagePrice', 0),
                    'average_long_price': pos.get('averageLongPrice', 0),
                    'tax_lot_average_long_price': pos.get('taxLotAverageLongPrice', 0),
                    'market_value': pos.get('marketValue', 0),
                    'maintenance_requirement': pos.get('maintenanceRequirement', 0),
                    
                    # P&L information
                    'current_day_profit_loss': pos.get('currentDayProfitLoss', 0),
                    'current_day_profit_loss_percentage': pos.get('currentDayProfitLossPercentage', 0),
                    'long_open_profit_loss': pos.get('longOpenProfitLoss', 0),
                    'current_day_cost': pos.get('currentDayCost', 0),
                    
                    # Market data
                    'net_change': instrument.get('netChange', 0),
                    
                    # Options-specific fields (if applicable)
                    'underlying_symbol': instrument.get('underlyingSymbol'),
                    'put_call': instrument.get('putCall'),
                    'strike_price': instrument.get('strikePrice'),
                    'expiration_date': instrument.get('expirationDate'),
                    'option_multiplier': instrument.get('optionMultiplier'),
                    'option_deliverables': instrument.get('optionDeliverables', []),
                    
                    # Calculated fields
                    'net_quantity': pos.get('longQuantity', 0) - pos.get('shortQuantity', 0),
                    'position_type': 'LONG' if pos.get('longQuantity', 0) > 0 else 'SHORT' if pos.get('shortQuantity', 0) > 0 else 'FLAT'
                }
                
                account_positions.append(position_detail)
            
            detailed_positions[account_number] = {
                'account_number': account_number,
                'account_type': securities_account.get('type'),
                'positions_count': len(account_positions),
                'positions': account_positions
            }
        
        print(f"✅ Extracted detailed positions for {len(detailed_positions)} accounts")
        return detailed_positions
        
    except Exception as e:
        print(f"❌ Error extracting detailed positions: {str(e)}")
        return {}

def load_tokens_from_aws():
    """Load tokens from AWS Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = response['SecretString']
        tokens = json.loads(secret_string)
        print(f"✅ Tokens loaded from AWS Secrets Manager: {SECRET_NAME}")
        return tokens
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"⚠️ Secret not found in AWS Secrets Manager: {SECRET_NAME}")
        else:
            print(f"❌ Failed to load tokens from AWS Secrets Manager: {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading tokens from AWS Secrets Manager: {e}")
        return None

def save_tokens_to_file(tokens):
    """Save tokens to local file (fallback method)"""
    try:
        # Calculate and save the expiration time as a string
        expires_at = datetime.now() + timedelta(seconds=int(tokens['expires_in']))
        tokens['expires_at'] = expires_at.isoformat()
        
        with open(TOKEN_FILE, 'w') as f:
            json.dump(tokens, f)
        print(f"✅ Tokens saved to local file: {TOKEN_FILE}")
        return True
    except Exception as e:
        print(f"❌ Failed to save tokens to file: {e}")
        return False

def load_tokens_from_file():
    """Load tokens from local file (fallback method)"""
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r') as f:
                tokens = json.load(f)
            print(f"✅ Tokens loaded from local file: {TOKEN_FILE}")
            return tokens
    except Exception as e:
        print(f"❌ Failed to load tokens from file: {e}")
    return None

def save_tokens(tokens):
    """Save tokens using the configured method (AWS or local file)"""
    if USE_AWS_SECRETS and secrets_client:
        success = save_tokens_to_aws(tokens)
        if success:
            return
        else:
            print("⚠️ AWS save failed, falling back to local file storage")
    
    # Fallback to local file storage
    if TOKEN_FILE:
        save_tokens_to_file(tokens)
    else:
        print("❌ No token storage method available")

def load_tokens():
    """Load tokens using the configured method (AWS or local file)"""
    if USE_AWS_SECRETS and secrets_client:
        tokens = load_tokens_from_aws()
        if tokens:
            return tokens
        else:
            print("⚠️ AWS load failed, trying local file storage")
    
    # Fallback to local file storage
    if TOKEN_FILE:
        return load_tokens_from_file()
    
    print("❌ No token storage method available")
    return None

def get_authorization_code():
    print("Manual authentication required. Go to the following URL to authenticate:")
    print(AUTH_URL)
    webbrowser.open(AUTH_URL)
    
    returned_url = input("Paste the full returned URL here as soon as you get it: ")
    
    # Extract the authorization code from the returned URL
    parsed_url = urllib.parse.urlparse(returned_url)
    code = urllib.parse.parse_qs(parsed_url.query).get('code', [None])[0]
    
    if not code:
        raise ValueError("Failed to extract code from the returned URL")
    
    # URL decode the code (important for Schwab API)
    code = urllib.parse.unquote(code)
    print(f"Extracted and decoded authorization code: {code}")
    
    return code


def get_tokens(code):
    print("Exchanging authorization code for tokens...")
    credentials = f"{APP_KEY}:{APP_SECRET}"
    base64_credentials = base64.b64encode(credentials.encode()).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI
    }
    
    token_response = requests.post(TOKEN_URL, headers=headers, data=payload)
    if token_response.status_code == 200:
        tokens = token_response.json()
        save_tokens(tokens)
        return tokens
    else:
        print("Failed to get tokens")
        print("Status Code:", token_response.status_code)
        print("Response:", token_response.text)
        return None

def refresh_tokens(refresh_token):
    print("Refreshing access token...")
    credentials = f"{APP_KEY}:{APP_SECRET}"
    base64_credentials = base64.b64encode(credentials.encode()).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    refresh_response = requests.post(TOKEN_URL, headers=headers, data=payload)
    
    if refresh_response.status_code == 200:
        new_tokens = refresh_response.json()
        save_tokens(new_tokens)
        return new_tokens
    else:
        print("Failed to refresh tokens")
        print("Status Code:", refresh_response.status_code)
        print("Response:", refresh_response.text)
        return None


# Modify ensure_valid_tokens to check expiration time
def ensure_valid_tokens(refresh=True):
    tokens = load_tokens()
    if tokens:
        expires_at = tokens.get('expires_at')
        
        # Check if 'expires_at' exists and is a valid string
        if expires_at:
            try:
                expires_at = datetime.fromisoformat(expires_at)
            except ValueError:
                print("Invalid 'expires_at' format in tokens. Re-authentication required.")
                tokens = None  # Force re-authentication
        else:
            print("'expires_at' missing from tokens. Re-authentication required.")
            tokens = None  # Force re-authentication

        if tokens:
            refresh_token = tokens.get("refresh_token")
            # Check if access token is expired or about to expire (within a buffer, e.g., 2 minutes)
            if datetime.now() >= expires_at - timedelta(minutes=2):
                print("Access token is about to expire or has expired, attempting to refresh...")
                new_tokens = refresh_tokens(refresh_token)
                if new_tokens:
                    return new_tokens  # Token successfully refreshed
                else:
                    print("Failed to refresh tokens. Please re-authenticate.")
            else:
                return tokens  # Access token is still valid

    # If no tokens or refreshing failed, require manual re-authentication
    print("Manual re-authentication required.")
    code = get_authorization_code()
    return get_tokens(code)


def get_account_numbers(access_token):
    url = f"{BASE_URL}/trader/v1/accounts/accountNumbers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    retries = MAX_RETRIES
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()  # Raise error for bad status codes
            return response.json()
        except requests.exceptions.ReadTimeout:
            print(f"Request timed out on attempt {attempt + 1}/{retries}. Retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}, attempt {attempt + 1}/{retries}")
        time.sleep(RETRY_DELAY ** attempt)  # Exponential backoff

    raise Exception(f"Failed to fetch account numbers after {retries} attempts")

def get_account_details(access_token, account_number, field):
    #print(f"DEBUG : Fetching account details for account {account_number}...")
    url = f"{BASE_URL}/trader/v1/accounts/{account_number}?fields={field}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        retry_after = response.headers.get("Retry-After", RATE_LIMIT_DELAY)
        print(f"Rate limit exceeded. Retry after {retry_after} seconds.")
        return None
    else:
        print(f"Failed to get account details\nStatus Code: {response.status_code}\nResponse: {response.text}")
        return None

def get_positions(access_token, account_number):
    """Get current positions for the specified account"""
    url = f"{BASE_URL}/trader/v1/accounts/{account_number}/positions"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    retries = MAX_RETRIES
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                positions = response.json()
                # Format the positions data
                formatted_positions = []
                for pos in positions.get('positions', []):
                    formatted_positions.append({
                        'symbol': pos.get('symbol'),
                        'quantity': pos.get('quantity'),
                        'cost_basis': pos.get('costBasis'),
                        'market_value': pos.get('marketValue'),
                        'unrealized_pl': pos.get('unrealizedPL'),
                        'unrealized_pl_percent': pos.get('unrealizedPLPercent')
                    })
                return formatted_positions
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RATE_LIMIT_DELAY))
                print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                print(f"Failed to get positions. Status Code: {response.status_code}")
                print(f"Response: {response.text}")
                time.sleep(RETRY_DELAY ** attempt)  # Exponential backoff
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(RETRY_DELAY ** attempt)  # Exponential backoff
    
    return None

def handle_api_response(response, operation_name="API call", retry_on_403=True):
    """
    Handle API response with comprehensive error handling including 403 authorization errors
    
    Args:
        response: requests.Response object
        operation_name: Name of the operation for logging
        retry_on_403: Whether to retry on 403 errors with token refresh
        
    Returns:
        tuple: (success: bool, data: dict or None, should_retry: bool)
    """
    try:
        if response.status_code == 200:
            return True, response.json(), False
        elif response.status_code == 401:
            print(f"🔐 401 Unauthorized for {operation_name} - token may be invalid")
            return False, None, True
        elif response.status_code == 403:
            print(f"🚫 403 Forbidden for {operation_name} - authorization error")
            if retry_on_403:
                print("🔄 Attempting token refresh for 403 error...")
                return False, None, True
            else:
                return False, None, False
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", RATE_LIMIT_DELAY))
            print(f"⏳ 429 Rate limit exceeded for {operation_name}. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return False, None, True
        else:
            print(f"❌ {operation_name} failed. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            return False, None, False
            
    except Exception as e:
        print(f"❌ Error handling API response for {operation_name}: {e}")
        return False, None, False

# Global flag to pause operations when authentication fails
_authentication_paused = False
_last_auth_failure_time = 0

def pause_operations_for_reauth():
    """Pause all operations and trigger re-authentication flow"""
    global _authentication_paused, _last_auth_failure_time
    _authentication_paused = True
    _last_auth_failure_time = time.time()
    
    print("🚨 AUTHENTICATION FAILURE - TRIGGERING RE-AUTHENTICATION")
    print("=" * 60)
    print("🔐 Multiple 403 Forbidden errors detected")
    print("🔄 Initiating re-authentication flow...")
    print("=" * 60)
    
    # Trigger immediate re-authentication
    try:
        print("🌐 Opening browser for re-authentication...")
        code = get_authorization_code()
        new_tokens = get_tokens(code)
        
        if new_tokens:
            print("✅ Re-authentication successful!")
            print("🔄 Resuming operations...")
            resume_operations()
            return True
        else:
            print("❌ Re-authentication failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during re-authentication: {e}")
        print("📋 Manual steps required:")
        print("   1. Check your Schwab API credentials")
        print("   2. Verify your app permissions")
        print("   3. Restart the application")
        return False

def is_authentication_paused():
    """Check if operations are paused due to authentication failure"""
    return _authentication_paused

def resume_operations():
    """Resume operations after successful re-authentication"""
    global _authentication_paused
    _authentication_paused = False
    print("✅ Operations resumed after successful re-authentication")

def make_authenticated_request(url, operation_name="API call", max_retries=3, **kwargs):
    """
    Make an authenticated request with automatic token refresh and operation pausing on persistent 403 errors
    
    Args:
        url: API endpoint URL
        operation_name: Name of the operation for logging
        max_retries: Maximum number of retries
        **kwargs: Additional arguments for requests.get()
        
    Returns:
        tuple: (success: bool, data: dict or None)
    """
    # Check if operations are paused due to authentication failure
    if is_authentication_paused():
        print(f"🛑 Operations paused - skipping {operation_name}")
        return False, None
    
    consecutive_403_errors = 0
    
    for attempt in range(max_retries + 1):
        try:
            # Check again if operations are paused (could have been paused during retry loop)
            if is_authentication_paused():
                print(f"🛑 Operations paused during retry - stopping {operation_name}")
                return False, None
            
            # Ensure we have valid tokens
            tokens = ensure_valid_tokens()
            if not tokens:
                print(f"❌ Failed to get valid tokens for {operation_name} - PAUSING OPERATIONS")
                pause_operations_for_reauth()
                return False, None
                
            access_token = tokens['access_token']
            
            # Set up headers
            headers = kwargs.get('headers', {})
            headers.update({
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            })
            kwargs['headers'] = headers
            
            # Set timeout if not provided
            if 'timeout' not in kwargs:
                kwargs['timeout'] = REQUEST_TIMEOUT
            
            # Make the request
            response = requests.get(url, **kwargs)
            
            # Handle the response
            success, data, should_retry = handle_api_response(response, operation_name)
            
            if success:
                # Reset consecutive error counter on success
                consecutive_403_errors = 0
                return True, data
            elif should_retry and attempt < max_retries:
                if response.status_code == 403:
                    consecutive_403_errors += 1
                    print(f"🚫 403 Forbidden for {operation_name} - authorization error (consecutive: {consecutive_403_errors})")
                    
                    # If we get ANY 403 error, immediately pause operations (more aggressive)
                    print(f"🚨 403 error detected - IMMEDIATELY PAUSING OPERATIONS")
                    pause_operations_for_reauth()
                    return False, None
                        
                elif response.status_code == 401:
                    print(f"🚨 401 Unauthorized for {operation_name} - IMMEDIATELY PAUSING OPERATIONS")
                    pause_operations_for_reauth()
                    return False, None
                
                # Wait before retry (exponential backoff)
                wait_time = RETRY_DELAY ** attempt
                print(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                # No more retries or shouldn't retry
                if response.status_code in [401, 403]:
                    print(f"🚨 Persistent {response.status_code} errors for {operation_name} - PAUSING OPERATIONS")
                    pause_operations_for_reauth()
                return False, data
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request exception for {operation_name} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries:
                wait_time = RETRY_DELAY ** attempt
                time.sleep(wait_time)
            else:
                return False, None
        except Exception as e:
            print(f"❌ Unexpected error for {operation_name}: {e}")
            return False, None
    
    # If we exhausted all retries, pause operations
    print(f"🚨 Exhausted all retries for {operation_name} - PAUSING OPERATIONS")
    pause_operations_for_reauth()
    
    return False, None

def get_all_positions():
    """Get comprehensive account data including positions, balances, and account info"""
    try:
        print("📊 Fetching comprehensive account data with positions...")
        
        # Use the new authenticated request method
        url = "https://api.schwabapi.com/trader/v1/accounts?fields=positions"
        success, accounts_data = make_authenticated_request(url, "get all positions")
        
        if not success or not accounts_data:
            print("❌ Failed to get accounts data")
            return None
        
        # Process the full response structure
        processed_accounts = {}
        
        for account in accounts_data:
            securities_account = account.get('securitiesAccount', {})
            account_number = securities_account.get('accountNumber')
            
            if not account_number:
                continue
            
            # Extract comprehensive account information
            account_info = {
                'account_number': account_number,
                'account_type': securities_account.get('type'),
                'round_trips': securities_account.get('roundTrips', 0),
                'is_day_trader': securities_account.get('isDayTrader', False),
                'is_closing_only_restricted': securities_account.get('isClosingOnlyRestricted', False),
                'pfcb_flag': securities_account.get('pfcbFlag', False),
                
                # Process positions
                'positions': [],
                
                # Process balances
                'initial_balances': securities_account.get('initialBalances', {}),
                'current_balances': securities_account.get('currentBalances', {}),
                'projected_balances': securities_account.get('projectedBalances', {}),
                
                # Aggregated balance from parent level
                'aggregated_balance': account.get('aggregatedBalance', {})
            }
            
            # Process positions with full detail
            positions = securities_account.get('positions', [])
            for pos in positions:
                instrument = pos.get('instrument', {})
                
                # Create comprehensive position data
                position_data = {
                    # Basic position info
                    'symbol': instrument.get('symbol'),
                    'cusip': instrument.get('cusip'),
                    'asset_type': instrument.get('assetType'),
                    'net_change': instrument.get('netChange', 0),
                    
                    # Quantities
                    'short_quantity': pos.get('shortQuantity', 0),
                    'long_quantity': pos.get('longQuantity', 0),
                    'settled_long_quantity': pos.get('settledLongQuantity', 0),
                    'settled_short_quantity': pos.get('settledShortQuantity', 0),
                    'previous_session_long_quantity': pos.get('previousSessionLongQuantity', 0),
                    
                    # Pricing and values
                    'average_price': pos.get('averagePrice', 0),
                    'average_long_price': pos.get('averageLongPrice', 0),
                    'tax_lot_average_long_price': pos.get('taxLotAverageLongPrice', 0),
                    'market_value': pos.get('marketValue', 0),
                    'maintenance_requirement': pos.get('maintenanceRequirement', 0),
                    
                    # P&L calculations
                    'current_day_profit_loss': pos.get('currentDayProfitLoss', 0),
                    'current_day_profit_loss_percentage': pos.get('currentDayProfitLossPercentage', 0),
                    'long_open_profit_loss': pos.get('longOpenProfitLoss', 0),
                    'current_day_cost': pos.get('currentDayCost', 0),
                    
                    # Additional fields for options
                    'underlying_symbol': instrument.get('underlyingSymbol'),
                    'option_deliverables': instrument.get('optionDeliverables', []),
                    'option_multiplier': instrument.get('optionMultiplier'),
                    'put_call': instrument.get('putCall'),
                    'strike_price': instrument.get('strikePrice'),
                    'expiration_date': instrument.get('expirationDate'),
                    
                    # Calculated fields for compatibility
                    'quantity': pos.get('longQuantity', 0) - pos.get('shortQuantity', 0),
                    'cost_basis': pos.get('averagePrice', 0),
                    'unrealized_pl': pos.get('longOpenProfitLoss', 0),
                    'unrealized_pl_percent': pos.get('currentDayProfitLossPercentage', 0),
                    'day_pl': pos.get('currentDayProfitLoss', 0),
                    'current_price': 0,  # Will be calculated from market_value / quantity if needed
                    'instrument_type': instrument.get('assetType', 'EQUITY')
                }
                
                # Calculate current price if possible
                total_quantity = abs(position_data['quantity'])
                if total_quantity > 0 and position_data['market_value'] != 0:
                    position_data['current_price'] = abs(position_data['market_value'] / total_quantity)
                
                account_info['positions'].append(position_data)
            
            processed_accounts[account_number] = account_info
            print(f"✅ Retrieved {len(positions)} positions for account {account_number}")
            print(f"   Account Type: {account_info['account_type']}, Day Trader: {account_info['is_day_trader']}")
            
            # Log balance information
            current_balances = account_info['current_balances']
            if current_balances:
                print(f"   Equity: ${current_balances.get('equity', 0):,.2f}, Buying Power: ${current_balances.get('buyingPower', 0):,.2f}")
        
        print(f"✅ Successfully retrieved comprehensive data for {len(processed_accounts)} accounts")
        return processed_accounts
        
    except Exception as e:
        print(f"❌ Error getting comprehensive account data: {str(e)}")
        return None

def get_account_numbers_with_retry(access_token=None, max_retries=3):
    """Get account numbers with improved error handling and retry logic"""
    if not access_token:
        tokens = ensure_valid_tokens()
        if not tokens:
            print("❌ Failed to get valid tokens for account numbers")
            return None
        access_token = tokens['access_token']
    
    url = f"{BASE_URL}/trader/v1/accounts/accountNumbers"
    success, data = make_authenticated_request(url, "get account numbers", max_retries)
    
    if success:
        print(f"✅ Retrieved {len(data) if data else 0} account numbers")
        return data
    else:
        print("❌ Failed to get account numbers after retries")
        return None

def get_account_details_with_retry(account_number, field="positions", max_retries=3):
    """Get account details with improved error handling"""
    url = f"{BASE_URL}/trader/v1/accounts/{account_number}?fields={field}"
    success, data = make_authenticated_request(url, f"get account details for {account_number}", max_retries)
    
    if success:
        print(f"✅ Retrieved account details for {account_number}")
        return data
    else:
        print(f"❌ Failed to get account details for {account_number}")
        return None

def startup_authentication_check():
    """
    Perform startup authentication check - blocks until valid tokens are obtained.
    This must be called before starting any processes or operations.
    
    Returns:
        bool: True if authentication successful, False otherwise
    """
    print("🔐 STARTUP AUTHENTICATION CHECK")
    print("=" * 50)
    print("🔍 Checking for valid tokens before starting any processes...")
    
    try:
        # Try to get valid tokens - this will handle refresh or re-authentication
        tokens = ensure_valid_tokens()
        
        if not tokens:
            print("❌ Failed to obtain valid tokens during startup")
            return False
        
        # Test the tokens with a simple API call
        print("🧪 Testing tokens with account numbers API call...")
        url = f"{BASE_URL}/trader/v1/accounts/accountNumbers"
        headers = {
            "Authorization": f"Bearer {tokens['access_token']}",
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            account_data = response.json()
            print(f"✅ Authentication successful! Found {len(account_data)} accounts")
            print("🚀 All systems ready - proceeding with startup...")
            return True
        elif response.status_code in [401, 403]:
            print(f"❌ Token test failed with {response.status_code} - authentication required")
            print("🔄 Attempting re-authentication...")
            
            # Force re-authentication
            code = get_authorization_code()
            new_tokens = get_tokens(code)
            
            if new_tokens:
                print("✅ Re-authentication successful!")
                return True
            else:
                print("❌ Re-authentication failed")
                return False
        else:
            print(f"❌ Unexpected response during token test: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error during startup authentication check: {e}")
        return False

def verify_authentication_before_start():
    """
    Wrapper function that ensures authentication is valid before allowing any operations.
    This is the main gate that should be called before starting the realtime monitor.
    
    Returns:
        bool: True if ready to proceed, False if authentication failed
    """
    print("\n" + "=" * 60)
    print("🛡️  SCHWAB API AUTHENTICATION GATE")
    print("=" * 60)
    print("⚠️  NO PROCESSES WILL START UNTIL AUTHENTICATION IS VERIFIED")
    print("=" * 60)
    
    max_attempts = 3
    for attempt in range(max_attempts):
        print(f"\n🔄 Authentication attempt {attempt + 1}/{max_attempts}")
        
        if startup_authentication_check():
            print("\n" + "=" * 60)
            print("✅ AUTHENTICATION GATE PASSED")
            print("🚀 READY TO START ALL PROCESSES")
            print("=" * 60)
            return True
        else:
            if attempt < max_attempts - 1:
                print(f"❌ Authentication attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
            else:
                print("\n" + "=" * 60)
                print("❌ AUTHENTICATION GATE FAILED")
                print("🛑 CANNOT START PROCESSES WITHOUT VALID AUTHENTICATION")
                print("=" * 60)
                print("\n📋 Manual steps required:")
                print("   1. Check your Schwab API credentials in .env file")
                print("   2. Verify your app is approved in Schwab Developer Portal")
                print("   3. Ensure redirect URI matches exactly")
                print("   4. Check if your refresh token has expired (7 days)")
                print("   5. Restart the application")
                return False
    
    return False
