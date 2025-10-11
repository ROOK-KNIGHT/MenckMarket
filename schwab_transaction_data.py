import base64
import requests
import json
import urllib.parse
import os
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz
import argparse
import sys
from typing import Dict, Any, List, Optional
from urllib.parse import quote

# Schwab API credentials
APP_KEY = "UXvDmuMdEsgAyXAWGMSOblaaLbnR8MhW"
APP_SECRET = "Hl8zGamcb7Valfee"
REDIRECT_URI = "https://127.0.0.1"
TOKEN_FILE = "/Users/isaac/Desktop/CS_TOKENS/cs_tokens.json"

class SchwabTransactionFetcher:
    def __init__(self):
        """Initialize the transaction fetcher with valid tokens"""
        self.tokens = self.ensure_valid_tokens()
        self.accounts = self.get_account_numbers()
        self.timezone = pytz.timezone('America/Los_Angeles')

    def save_tokens(self, tokens):
        """Save tokens to file"""
        with open(TOKEN_FILE, 'w') as f:
            json.dump(tokens, f)
        print("Tokens saved successfully")

    def load_tokens(self):
        """Load tokens from file if they exist"""
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r') as f:
                return json.load(f)
        return None

    def get_authorization_code(self):
        """Get authorization code through browser flow"""
        auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?response_type=code&client_id={APP_KEY}&redirect_uri={REDIRECT_URI}&scope=readonly"
        print("Opening browser for authentication...")
        print(f"If browser doesn't open, visit: {auth_url}")
        import webbrowser
        webbrowser.open(auth_url)
        returned_url = input("Paste the full returned URL here: ")
        
        parsed_url = urllib.parse.urlparse(returned_url)
        code = urllib.parse.parse_qs(parsed_url.query).get('code', [None])[0]
        if not code:
            raise ValueError("Failed to extract authorization code")
        return code

    def get_initial_tokens(self, code):
        """Exchange authorization code for tokens"""
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
        
        response = requests.post("https://api.schwabapi.com/v1/oauth/token", headers=headers, data=payload)
        if response.status_code != 200:
            raise Exception(f"Failed to get tokens: {response.text}")
        tokens = response.json()
        self.save_tokens(tokens)
        return tokens

    def refresh_tokens(self, refresh_token):
        """Refresh expired tokens"""
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
        
        response = requests.post("https://api.schwabapi.com/v1/oauth/token", headers=headers, data=payload)
        if response.status_code != 200:
            raise Exception(f"Failed to refresh tokens: {response.text}")
        tokens = response.json()
        self.save_tokens(tokens)
        return tokens

    def ensure_valid_tokens(self):
        """Ensure we have valid tokens, refreshing or getting new ones if needed"""
        tokens = self.load_tokens()
        if tokens:
            test_url = "https://api.schwabapi.com/trader/v1/accounts"
            headers = {"Authorization": f"Bearer {tokens['access_token']}"}
            try:
                response = requests.get(test_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    print("Access token validated successfully")
                    return tokens
                elif response.status_code == 401:
                    print("Access token expired, refreshing...")
                    return self.refresh_tokens(tokens["refresh_token"])
            except requests.RequestException as e:
                print(f"Token validation failed: {e}")
        
        code = self.get_authorization_code()
        return self.get_initial_tokens(code)

    def get_account_numbers(self) -> List[Dict]:
        """Get all available account numbers and details"""
        url = "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Accept": "application/json"
        }
        
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                accounts = response.json()
                
                if isinstance(accounts, list) and len(accounts) > 0:
                    print(f"Found {len(accounts)} accounts")
                    return accounts
                else:
                    raise ValueError(f"No accounts found in response: {accounts}")
                    
            except requests.exceptions.ReadTimeout:
                print(f"Request timed out on attempt {attempt + 1}/{retries}. Retrying...")
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}, attempt {attempt + 1}/{retries}")
            
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
        print("Failed to fetch account numbers after all retries")
        return []

    def list_accounts(self):
        """Print available accounts in a user-friendly format"""
        if not self.accounts:
            print("No accounts available")
            return
        
        print("\nAvailable Accounts:")
        print("-" * 80)
        for i, account in enumerate(self.accounts):
            display_name = account.get('displayName', 'Unknown')
            nickname = account.get('nickname', 'No nickname')
            account_type = account.get('type', 'Unknown type')
            
            print(f"{i+1}. {display_name} ({nickname}) - Type: {account_type}")
            print(f"   Hash: {account.get('hashValue', 'N/A')}")
            print()
            
        print("-" * 80)

    def get_transactions(self, account_hash: str, 
                       from_date: Optional[str] = None, 
                       to_date: Optional[str] = None,
                       transaction_type: Optional[str] = None,
                       symbol: Optional[str] = None,
                       max_results: int = 100) -> List[Dict]:
        """
        Get transaction history for a specified account
        
        Parameters:
        - account_hash: Account hash ID
        - from_date: Start date in YYYY-MM-DD format (default: 30 days ago)
        - to_date: End date in YYYY-MM-DD format (default: today)
        - transaction_type: Filter by transaction type (e.g., "TRADE", "DIVIDEND", etc.)
        - symbol: Filter by security symbol
        - max_results: Maximum number of results to return (default: 100)
        
        Returns:
        - List of transaction dictionaries
        """
        # Set default date range if not specified (last 30 days)
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        # Format dates for API with milliseconds as required by Schwab API
        from_date_iso = f"{from_date}T00:00:00.000Z"
        to_date_iso = f"{to_date}T23:59:59.000Z"
        
        # Build the URL with query parameters
        url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/transactions"
        params = {
            "startDate": from_date_iso,
            "endDate": to_date_iso
        }
        
        # Add optional filters if provided
        if transaction_type:
            params["types"] = transaction_type  # Schwab API expects "types" not "type"
        if symbol:
            params["symbol"] = symbol
            
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Accept": "application/json"
        }
        
        # Make the request with retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Fetching transactions (attempt {attempt+1}/{max_retries})...")
                response = requests.get(url, headers=headers, params=params, timeout=20)
                
                # Handle expired token
                if response.status_code == 401:
                    print("Token expired, refreshing...")
                    self.tokens = self.refresh_tokens(self.tokens["refresh_token"])
                    headers["Authorization"] = f"Bearer {self.tokens['access_token']}"
                    continue
                    
                # Raise exception for other errors
                response.raise_for_status()
                
                # Parse and return the results
                transactions = response.json()
                return transactions
            
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print("Failed to fetch transactions after all retries")
                    return []
    
    def get_transaction_details(self, account_hash: str, transaction_id: str) -> Dict:
        """
        Get detailed information about a specific transaction
        
        Parameters:
        - account_hash: Account hash ID
        - transaction_id: Transaction ID to fetch details for
        
        Returns:
        - Transaction details dictionary
        """
        url = f"https://api.schwabapi.com/trader/v1/accounts/{account_hash}/transactions/{transaction_id}"
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Accept": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            # Handle expired token
            if response.status_code == 401:
                print("Token expired, refreshing...")
                self.tokens = self.refresh_tokens(self.tokens["refresh_token"])
                return self.get_transaction_details(account_hash, transaction_id)
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to get transaction details: {e}")
            return {}
            
    def process_transactions(self, transactions: List[Dict]) -> pd.DataFrame:
        """
        Process raw transaction data into a structured DataFrame
        
        Parameters:
        - transactions: List of transaction dictionaries from API
        
        Returns:
        - Pandas DataFrame with standardized transaction data
        """
        if not transactions:
            print("No transactions to process")
            return pd.DataFrame()
            
        # Extract relevant fields from each transaction
        processed_data = []
        
        for txn in transactions:
            # Common fields - check various field names that might contain date information
            processed_txn = {
                'transaction_id': txn.get('activityId', txn.get('transactionId', '')),
                'date': txn.get('time', txn.get('tradeDate', txn.get('transactionDate', ''))),
                'settlement_date': txn.get('settlementDate', ''),
                'type': txn.get('type', ''),
                'description': txn.get('description', ''),
                'amount': txn.get('netAmount', 0.0),
                'fees': txn.get('fees', 0.0),
                'status': txn.get('status', txn.get('transactionStatus', ''))
            }
            
            # Add security-specific information if available
            if 'transactionItem' in txn:
                item = txn['transactionItem']
                if 'instrument' in item:
                    instrument = item['instrument']
                    processed_txn.update({
                        'symbol': instrument.get('symbol', ''),
                        'asset_type': instrument.get('assetType', ''),
                        'quantity': item.get('amount', 0),
                        'price': item.get('price', 0.0)
                    })
            
            # Handle the 'transferItems' structure from Schwab API
            if 'transferItems' in txn and isinstance(txn['transferItems'], list):
                for item in txn['transferItems']:
                    if item.get('instrument', {}).get('assetType') == 'EQUITY':
                        # Extract equity information
                        processed_txn.update({
                            'symbol': item['instrument'].get('symbol', ''),
                            'asset_type': item['instrument'].get('assetType', ''),
                            'quantity': item.get('amount', 0),
                            'price': item.get('price', 0.0),
                            'position_effect': item.get('positionEffect', '')
                        })
                        break  # Take the first equity item
                    
                # Calculate total fees
                total_fees = 0
                for item in txn['transferItems']:
                    if 'feeType' in item:
                        total_fees += abs(item.get('cost', 0))
                
                if total_fees > 0:
                    processed_txn['fees'] = total_fees
                
            processed_data.append(processed_txn)
            
        # Create DataFrame and do basic processing
        df = pd.DataFrame(processed_data)
        
        # Convert date columns to datetime if they exist
        for date_col in ['date', 'settlement_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col])
                
        # Sort by date, newest first
        if 'date' in df.columns:
            df = df.sort_values('date', ascending=False)
            
        return df
        
    def save_to_csv(self, df: pd.DataFrame, filename: str = None):
        """Save transaction data to CSV file"""
        if df.empty:
            print("No data to save")
            return
            
        if not filename:

            filename = f"schwab_transactions.csv"
        
        # Ensure the transaction_data directory exists
        transaction_dir = "transaction_data"
        os.makedirs(transaction_dir, exist_ok=True)
        
        # Create full path with transaction_data directory
        full_path = os.path.join(transaction_dir, filename)
        
        df.to_csv(full_path, index=False)
        print(f"Saved {len(df)} transactions to {full_path}")
    
    def get_account_by_index(self, index: int) -> Dict:
        """Get account by its index in the accounts list"""
        if not self.accounts or index < 0 or index >= len(self.accounts):
            print(f"Invalid account index: {index}")
            return {}
        return self.accounts[index]
            
    def get_all_transactions(self, days: int = None, csv_output: bool = True):
        """
        Get transactions for all accounts over the specified period
        
        Parameters:
        - days: Number of days to look back (default: None, uses today only)
        - csv_output: Whether to save results to CSV files (default: True)
        """
        # If days is None, just get today's transactions
        if days is None:
            from_date = datetime.now().strftime("%Y-%m-%d")
        else:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        all_data = []
        
        for i, account in enumerate(self.accounts):
            account_hash = account.get('hashValue')
            display_name = account.get('displayName', f'Account {i+1}')
            
            print(f"\nFetching transactions for {display_name} ({account_hash})")
            print(f"Date range: {from_date} to {to_date}")
            
            txns = self.get_transactions(
                account_hash=account_hash,
                from_date=from_date,
                to_date=to_date,
                transaction_type="TRADE",  # Add default filter for trade transactions
                max_results=500
            )
            
            if txns:
                df = self.process_transactions(txns)
                print(f"Found {len(df)} transactions")
                
                # Add account info to dataframe
                df['account_name'] = display_name
                df['account_hash'] = account_hash
                
                all_data.append(df)
                
                # Save individual account data if requested
                if csv_output:
                    account_filename = f"schwab_transactions_{display_name.replace(' ', '_')}_{from_date}_{to_date}.csv"
                    self.save_to_csv(df, account_filename)
            else:
                print("No transactions found for this account")
                
        # Combine all account data if we have multiple accounts
        if len(all_data) > 1:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"\nCombined data: {len(combined_df)} transactions across {len(all_data)} accounts")
            
            if csv_output:
                combined_filename = f"schwab_transactions_all_accounts_{from_date}_{to_date}.csv"
                self.save_to_csv(combined_df, combined_filename)
                
            return combined_df
        elif len(all_data) == 1:
            return all_data[0]
        else:
            print("\nNo transaction data found for any accounts")
            return pd.DataFrame()

def calculate_win_loss_stats(df):
    """Calculate and display win/loss statistics for trades, including short vs long breakdown"""
    # First, ensure we have the necessary columns
    if 'symbol' not in df.columns or 'amount' not in df.columns or 'quantity' not in df.columns:
        print("Win/Loss calculation requires symbol, amount, and quantity columns")
        return
        
    # Create a copy to avoid modifying the original dataframe
    trade_df = df.copy()
    
    # Identify buy and sell transactions
    trade_df['trade_type'] = 'Unknown'
    trade_df.loc[trade_df['quantity'] > 0, 'trade_type'] = 'Buy'
    trade_df.loc[trade_df['quantity'] < 0, 'trade_type'] = 'Sell'
    
    # Group by symbol for analysis
    symbols = trade_df['symbol'].unique()
    
    # Initialize win/loss counters
    total_wins = 0
    total_losses = 0
    long_wins = 0
    long_losses = 0
    short_wins = 0
    short_losses = 0
    symbol_stats = {}
    profit_loss_total = 0
    long_pl_total = 0
    short_pl_total = 0
    
    # Initialize collections for average win/loss calculations
    all_win_amounts = []
    all_loss_amounts = []
    all_long_win_amounts = []
    all_long_loss_amounts = []
    all_short_win_amounts = []
    all_short_loss_amounts = []
    
    # Initialize streak tracking
    all_streaks = []  # Positive for wins, negative for losses
    long_streaks = []
    short_streaks = []
    max_win_streak = 0
    max_loss_streak = 0
    long_max_win_streak = 0
    long_max_loss_streak = 0
    short_max_win_streak = 0
    short_max_loss_streak = 0
    
    print("\nWin/Loss Statistics:")
    print("-" * 70)
    
    for symbol in symbols:
        # Filter for this symbol
        symbol_trades = trade_df[trade_df['symbol'] == symbol].sort_values('date')
        
        # Skip symbols with less than 2 trades (need at least a buy and sell)
        if len(symbol_trades) < 2:
            continue
            
        # Track positions for this symbol
        long_position = 0  # Positive values = long position
        short_position = 0  # Positive values = short position
        long_cost_basis = 0
        short_cost_basis = 0
        
        wins = 0
        losses = 0
        long_symbol_wins = 0
        long_symbol_losses = 0
        short_symbol_wins = 0
        short_symbol_losses = 0
        profit_loss = 0
        long_profit_loss = 0
        short_profit_loss = 0
        
        # Process each trade chronologically
        for _, trade in symbol_trades.iterrows():
            if trade['trade_type'] == 'Buy':
                bought_shares = abs(trade['quantity'])
                bought_amount = abs(trade['amount'])
                
                # Case 1: Covering a short position
                if short_position > 0:
                    # Calculate how many shares are used to cover the short
                    covering_shares = min(bought_shares, short_position)
                    
                    # Calculate P&L for the covered portion
                    covering_cost = (bought_amount / bought_shares) * covering_shares
                    short_sale_proceeds = short_cost_basis * covering_shares
                    
                    # For shorts: profit = sold high, bought low (short_sale_proceeds - covering_cost)
                    trade_pl = short_sale_proceeds - covering_cost
                    
                    if trade_pl >= 0:
                        short_symbol_wins += 1
                        short_wins += 1
                        total_wins += 1
                        wins += 1
                    else:
                        short_symbol_losses += 1
                        short_losses += 1
                        total_losses += 1
                        losses += 1
                    
                    short_profit_loss += trade_pl
                    profit_loss += trade_pl
                    
                    # Reduce short position
                    short_position -= covering_shares
                    
                    # Remaining shares go to long position
                    remaining_shares = bought_shares - covering_shares
                    if remaining_shares > 0:
                        remaining_cost = (bought_amount / bought_shares) * remaining_shares
                        
                        # Update long cost basis
                        if long_position + remaining_shares > 0:
                            long_cost_basis = ((long_position * long_cost_basis) + remaining_cost) / (long_position + remaining_shares)
                        
                        long_position += remaining_shares
                
                # Case 2: Adding to a long position
                else:
                    # Update long cost basis
                    if long_position + bought_shares > 0:
                        long_cost_basis = ((long_position * long_cost_basis) + bought_amount) / (long_position + bought_shares)
                    
                    long_position += bought_shares
            
            elif trade['trade_type'] == 'Sell':
                sold_shares = abs(trade['quantity'])
                sold_amount = abs(trade['amount'])
                
                # Case 1: Closing a long position
                if long_position > 0:
                    # Calculate how many shares are used to close the long
                    closing_shares = min(sold_shares, long_position)
                    
                    # Calculate P&L for the closed portion
                    closing_proceeds = (sold_amount / sold_shares) * closing_shares
                    long_cost = long_cost_basis * closing_shares
                    
                    # For longs: profit = sold high, bought low (closing_proceeds - long_cost)
                    trade_pl = closing_proceeds - long_cost
                    
                    if trade_pl >= 0:
                        long_symbol_wins += 1
                        long_wins += 1
                        total_wins += 1
                        wins += 1
                    else:
                        long_symbol_losses += 1
                        long_losses += 1
                        total_losses += 1
                        losses += 1
                    
                    long_profit_loss += trade_pl
                    profit_loss += trade_pl
                    
                    # Reduce long position
                    long_position -= closing_shares
                    
                    # Remaining shares go to short position
                    remaining_shares = sold_shares - closing_shares
                    if remaining_shares > 0:
                        remaining_proceeds = (sold_amount / sold_shares) * remaining_shares
                        
                        # Update short cost basis
                        if short_position + remaining_shares > 0:
                            short_cost_basis = ((short_position * short_cost_basis) + remaining_proceeds) / (short_position + remaining_shares)
                        
                        short_position += remaining_shares
                
                # Case 2: Opening a short position
                else:
                    # Update short cost basis
                    if short_position + sold_shares > 0:
                        short_cost_basis = ((short_position * short_cost_basis) + sold_amount) / (short_position + sold_shares)
                    
                    short_position += sold_shares
        
        # Track win and loss amounts for average calculations
        win_amounts = []
        loss_amounts = []
        long_win_amounts = []
        long_loss_amounts = []
        short_win_amounts = []
        short_loss_amounts = []
        
        # Replay the trades to capture individual P&L values
        long_position = 0
        short_position = 0
        long_cost_basis = 0
        short_cost_basis = 0
        
        # Track streaks for this symbol
        symbol_streaks = []
        symbol_long_streaks = []
        symbol_short_streaks = []
        current_streak = 0  # Positive for wins, negative for losses
        current_long_streak = 0
        current_short_streak = 0
        
        # Second pass to calculate individual trade P&Ls and track streaks
        for _, trade in symbol_trades.iterrows():
            if trade['trade_type'] == 'Buy':
                bought_shares = abs(trade['quantity'])
                bought_amount = abs(trade['amount'])
                
                # Case 1: Covering a short position
                if short_position > 0:
                    covering_shares = min(bought_shares, short_position)
                    covering_cost = (bought_amount / bought_shares) * covering_shares
                    short_sale_proceeds = short_cost_basis * covering_shares
                    trade_pl = short_sale_proceeds - covering_cost
                    
                    # Track win/loss and streak for short positions
                    if trade_pl >= 0:
                        short_win_amounts.append(trade_pl)
                        win_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_short_streak <= 0:
                            # End of a losing streak or start of tracking
                            if current_short_streak < 0:
                                symbol_short_streaks.append(current_short_streak)
                                short_streaks.append(current_short_streak)
                            current_short_streak = 1
                        else:
                            # Continue win streak
                            current_short_streak += 1
                            
                        if current_streak <= 0:
                            if current_streak < 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = 1
                        else:
                            current_streak += 1
                    else:
                        short_loss_amounts.append(trade_pl)
                        loss_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_short_streak >= 0:
                            # End of a winning streak or start of tracking
                            if current_short_streak > 0:
                                symbol_short_streaks.append(current_short_streak)
                                short_streaks.append(current_short_streak)
                            current_short_streak = -1
                        else:
                            # Continue loss streak
                            current_short_streak -= 1
                            
                        if current_streak >= 0:
                            if current_streak > 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = -1
                        else:
                            current_streak -= 1
                    
                    short_position -= covering_shares
                    
                    remaining_shares = bought_shares - covering_shares
                    if remaining_shares > 0:
                        remaining_cost = (bought_amount / bought_shares) * remaining_shares
                        if long_position + remaining_shares > 0:
                            long_cost_basis = ((long_position * long_cost_basis) + remaining_cost) / (long_position + remaining_shares)
                        long_position += remaining_shares
                else:
                    # Just adding to long position
                    if long_position + bought_shares > 0:
                        long_cost_basis = ((long_position * long_cost_basis) + bought_amount) / (long_position + bought_shares)
                    long_position += bought_shares
            
            elif trade['trade_type'] == 'Sell':
                sold_shares = abs(trade['quantity'])
                sold_amount = abs(trade['amount'])
                
                # Case 1: Closing a long position
                if long_position > 0:
                    closing_shares = min(sold_shares, long_position)
                    closing_proceeds = (sold_amount / sold_shares) * closing_shares
                    long_cost = long_cost_basis * closing_shares
                    trade_pl = closing_proceeds - long_cost
                    
                    # Track win/loss and streak for long positions
                    if trade_pl > 0:
                        long_win_amounts.append(trade_pl)
                        win_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_long_streak <= 0:
                            # End of a losing streak or start of tracking
                            if current_long_streak < 0:
                                symbol_long_streaks.append(current_long_streak)
                                long_streaks.append(current_long_streak)
                            current_long_streak = 1
                        else:
                            # Continue win streak
                            current_long_streak += 1
                            
                        if current_streak <= 0:
                            if current_streak < 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = 1
                        else:
                            current_streak += 1
                    else:
                        long_loss_amounts.append(trade_pl)
                        loss_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_long_streak >= 0:
                            # End of a winning streak or start of tracking
                            if current_long_streak > 0:
                                symbol_long_streaks.append(current_long_streak)
                                long_streaks.append(current_long_streak)
                            current_long_streak = -1
                        else:
                            # Continue loss streak
                            current_long_streak -= 1
                            
                        if current_streak >= 0:
                            if current_streak > 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = -1
                        else:
                            current_streak -= 1
                    
                    long_position -= closing_shares
                    
                    remaining_shares = sold_shares - closing_shares
                    if remaining_shares > 0:
                        remaining_proceeds = (sold_amount / sold_shares) * remaining_shares
                        if short_position + remaining_shares > 0:
                            short_cost_basis = ((short_position * short_cost_basis) + remaining_proceeds) / (short_position + remaining_shares)
                        short_position += remaining_shares
                else:
                    # Just opening a short position
                    if short_position + sold_shares > 0:
                        short_cost_basis = ((short_position * short_cost_basis) + sold_amount) / (short_position + sold_shares)
                    short_position += sold_shares
        
        # Store symbol stats if there were completed trades
        if wins + losses > 0:
            win_rate = (wins / (wins + losses)) * 100
            
            # Calculate long and short win rates
            long_win_rate = 0 if long_symbol_wins + long_symbol_losses == 0 else (long_symbol_wins / (long_symbol_wins + long_symbol_losses)) * 100
            short_win_rate = 0 if short_symbol_wins + short_symbol_losses == 0 else (short_symbol_wins / (short_symbol_wins + short_symbol_losses)) * 100
            
            # Calculate average win and loss sizes
            avg_win = sum(win_amounts) / len(win_amounts) if win_amounts else 0
            avg_loss = sum(abs(x) for x in loss_amounts) / len(loss_amounts) if loss_amounts else 0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
            
            # Calculate long strategy averages
            long_avg_win = sum(long_win_amounts) / len(long_win_amounts) if long_win_amounts else 0
            long_avg_loss = sum(abs(x) for x in long_loss_amounts) / len(long_loss_amounts) if long_loss_amounts else 0
            long_win_loss_ratio = long_avg_win / long_avg_loss if long_avg_loss > 0 else 0
            
            # Calculate short strategy averages
            short_avg_win = sum(short_win_amounts) / len(short_win_amounts) if short_win_amounts else 0
            short_avg_loss = sum(abs(x) for x in short_loss_amounts) / len(short_loss_amounts) if short_loss_amounts else 0
            short_win_loss_ratio = short_avg_win / short_avg_loss if short_avg_loss > 0 else 0
            
            symbol_stats[symbol] = {
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'long_wins': long_symbol_wins,
                'long_losses': long_symbol_losses,
                'long_win_rate': long_win_rate,
                'short_wins': short_symbol_wins,
                'short_losses': short_symbol_losses,
                'short_win_rate': short_win_rate,
                'profit_loss': profit_loss,
                'long_profit_loss': long_profit_loss,
                'short_profit_loss': short_profit_loss,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'win_loss_ratio': win_loss_ratio,
                'long_avg_win': long_avg_win,
                'long_avg_loss': long_avg_loss,
                'long_win_loss_ratio': long_win_loss_ratio,
                'short_avg_win': short_avg_win,
                'short_avg_loss': short_avg_loss,
                'short_win_loss_ratio': short_win_loss_ratio
            }
            
            profit_loss_total += profit_loss
            long_pl_total += long_profit_loss
            short_pl_total += short_profit_loss
            
            # Collect all averages for overall calculation
            all_win_amounts.extend(win_amounts)
            all_loss_amounts.extend(loss_amounts)
            all_long_win_amounts.extend(long_win_amounts)
            all_long_loss_amounts.extend(long_loss_amounts)
            all_short_win_amounts.extend(short_win_amounts)
            all_short_loss_amounts.extend(short_loss_amounts)
            
            # Print symbol statistics with long/short breakdown
            long_trades = long_symbol_wins + long_symbol_losses
            short_trades = short_symbol_wins + short_symbol_losses
            
            print(f"{symbol}: {wins} wins, {losses} losses ({win_rate:.1f}% win rate), P&L: ${profit_loss:.2f}")
            print(f"  Avg Win: ${avg_win:.2f}, Avg Loss: ${avg_loss:.2f}, Ratio: {win_loss_ratio:.2f}")
            
            if long_trades > 0:
                print(f"  Long: {long_symbol_wins} wins, {long_symbol_losses} losses ({long_win_rate:.1f}% win rate), P&L: ${long_profit_loss:.2f}")
                print(f"    Avg Win: ${long_avg_win:.2f}, Avg Loss: ${long_avg_loss:.2f}, Ratio: {long_win_loss_ratio:.2f}")
            
            if short_trades > 0:
                print(f"  Short: {short_symbol_wins} wins, {short_symbol_losses} losses ({short_win_rate:.1f}% win rate), P&L: ${short_profit_loss:.2f}")
                print(f"    Avg Win: ${short_avg_win:.2f}, Avg Loss: ${short_avg_loss:.2f}, Ratio: {short_win_loss_ratio:.2f}")
    
    # Calculate overall statistics
    if total_wins + total_losses > 0:
        overall_win_rate = (total_wins / (total_wins + total_losses)) * 100
        long_overall_win_rate = 0 if long_wins + long_losses == 0 else (long_wins / (long_wins + long_losses)) * 100
        short_overall_win_rate = 0 if short_wins + short_losses == 0 else (short_wins / (short_wins + short_losses)) * 100
        
        # Calculate overall average win and loss
        overall_avg_win = sum(all_win_amounts) / len(all_win_amounts) if all_win_amounts else 0
        overall_avg_loss = sum(abs(x) for x in all_loss_amounts) / len(all_loss_amounts) if all_loss_amounts else 0
        overall_win_loss_ratio = overall_avg_win / overall_avg_loss if overall_avg_loss > 0 else 0
        
        # Calculate long strategy averages
        long_avg_win = sum(all_long_win_amounts) / len(all_long_win_amounts) if all_long_win_amounts else 0
        long_avg_loss = sum(abs(x) for x in all_long_loss_amounts) / len(all_long_loss_amounts) if all_long_loss_amounts else 0
        long_win_loss_ratio = long_avg_win / long_avg_loss if long_avg_loss > 0 else 0
        
        # Calculate short strategy averages
        short_avg_win = sum(all_short_win_amounts) / len(all_short_win_amounts) if all_short_win_amounts else 0
        short_avg_loss = sum(abs(x) for x in all_short_loss_amounts) / len(all_short_loss_amounts) if all_short_loss_amounts else 0
        short_win_loss_ratio = short_avg_win / short_avg_loss if short_avg_loss > 0 else 0
        
        # Calculate streak statistics
        # For all streaks
        win_streaks = [s for s in all_streaks if s > 0]
        loss_streaks = [abs(s) for s in all_streaks if s < 0]
        max_win_streak = max(win_streaks) if win_streaks else 0
        max_loss_streak = max(loss_streaks) if loss_streaks else 0
        avg_win_streak = sum(win_streaks) / len(win_streaks) if win_streaks else 0
        avg_loss_streak = sum(loss_streaks) / len(loss_streaks) if loss_streaks else 0
        
        # For long strategy
        long_win_streaks = [s for s in long_streaks if s > 0]
        long_loss_streaks = [abs(s) for s in long_streaks if s < 0]
        long_max_win_streak = max(long_win_streaks) if long_win_streaks else 0
        long_max_loss_streak = max(long_loss_streaks) if long_loss_streaks else 0
        long_avg_win_streak = sum(long_win_streaks) / len(long_win_streaks) if long_win_streaks else 0
        long_avg_loss_streak = sum(long_loss_streaks) / len(long_loss_streaks) if long_loss_streaks else 0
        
        # For short strategy
        short_win_streaks = [s for s in short_streaks if s > 0]
        short_loss_streaks = [abs(s) for s in short_streaks if s < 0]
        short_max_win_streak = max(short_win_streaks) if short_win_streaks else 0
        short_max_loss_streak = max(short_loss_streaks) if short_loss_streaks else 0
        short_avg_win_streak = sum(short_win_streaks) / len(short_win_streaks) if short_win_streaks else 0
        short_avg_loss_streak = sum(short_loss_streaks) / len(short_loss_streaks) if short_loss_streaks else 0
        
        print("-" * 70)
        print(f"Overall: {total_wins} wins, {total_losses} losses ({overall_win_rate:.1f}% win rate)")
        print(f"Total P&L: ${profit_loss_total:.2f}")
        print(f"Avg Win: ${overall_avg_win:.2f}, Avg Loss: ${overall_avg_loss:.2f}, Ratio: {overall_win_loss_ratio:.2f}")
        
        # Display streak information
        print("\nStreak Analysis:")
        print(f"Max Win Streak: {max_win_streak} trades, Max Loss Streak: {max_loss_streak} trades")
        print(f"Avg Win Streak: {avg_win_streak:.1f} trades, Avg Loss Streak: {avg_loss_streak:.1f} trades")
        
        # Print long/short breakdowns
        long_trades = long_wins + long_losses
        short_trades = short_wins + short_losses
        
        if long_trades > 0:
            print(f"\nLong Strategy: {long_wins} wins, {long_losses} losses ({long_overall_win_rate:.1f}% win rate), P&L: ${long_pl_total:.2f}")
            print(f"  Avg Win: ${long_avg_win:.2f}, Avg Loss: ${long_avg_loss:.2f}, Ratio: {long_win_loss_ratio:.2f}")
            print(f"  Max Win Streak: {long_max_win_streak} trades, Max Loss Streak: {long_max_loss_streak} trades")
            print(f"  Avg Win Streak: {long_avg_win_streak:.1f} trades, Avg Loss Streak: {long_avg_loss_streak:.1f} trades")
        
        if short_trades > 0:
            print(f"\nShort Strategy: {short_wins} wins, {short_losses} losses ({short_overall_win_rate:.1f}% win rate), P&L: ${short_pl_total:.2f}")
            print(f"  Avg Win: ${short_avg_win:.2f}, Avg Loss: ${short_avg_loss:.2f}, Ratio: {short_win_loss_ratio:.2f}")
            print(f"  Max Win Streak: {short_max_win_streak} trades, Max Loss Streak: {short_max_loss_streak} trades")
            print(f"  Avg Win Streak: {short_avg_win_streak:.1f} trades, Avg Loss Streak: {short_avg_loss_streak:.1f} trades")
    else:
        print("No completed trades found for win/loss analysis")

def display_transactions(df, max_rows=None):
    """Display transactions in a formatted table"""
    if df.empty:
        print("No transactions to display")
        return
        
    if max_rows and len(df) > max_rows:
        print(f"Displaying first {max_rows} of {len(df)} transactions:")
        display_df = df.head(max_rows)
    else:
        print(f"Displaying all {len(df)} transactions:")
        display_df = df
        
    # Select and format columns for display
    display_cols = ['date', 'type', 'symbol', 'description', 'quantity', 'price', 'amount']
    display_cols = [col for col in display_cols if col in df.columns]
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 50)
    
    print("\n", "-" * 100)
    print(display_df[display_cols].to_string(index=False))
    print("-" * 100)

def export_to_json(df, symbol_stats=None, analysis_results=None, filename=None, include_transactions=False):
    """
    Export transaction analysis to JSON format with improved trade formatting
    
    The JSON output for trades will be organized to clearly show round-trip trades with:
    - Entry details (date, price, shares, total amount)
    - Exit details (date, price, shares, total amount)
    - Overall trade results (P&L amount and percentage)
    """
    """
    Export transaction analysis to JSON format
    
    Parameters:
    - df: DataFrame containing transaction data
    - symbol_stats: Dictionary containing symbol-level trading statistics
    - analysis_results: Dictionary containing overall analysis results
    - filename: Output filename (default: auto-generated)
    - include_transactions: Whether to include all transaction data in the JSON (default: False)
    """
    if df.empty:
        print("No transactions to export to JSON")
        return

    # Create the JSON structure with metadata
    output = {
        "metadata": {
            "total_transactions": len(df),
            "date_generated": datetime.now().isoformat(),
        }
    }
    
    # Add transaction data if requested
    if include_transactions:
        # Create a clean copy of the dataframe
        clean_df = df.copy()
        
        # Replace NaN values with None (null in JSON)
        clean_df = clean_df.where(pd.notnull(clean_df), None)
        
        # Convert DataFrame to dictionaries
        transactions = clean_df.to_dict(orient='records')
        
        # Handle special types for JSON serialization
        for txn in transactions:
            for key, value in list(txn.items()):
                # Handle pd.Timestamp objects
                if isinstance(value, pd.Timestamp):
                    txn[key] = value.isoformat()
                # Handle NaT (Not a Time) values
                elif pd.isna(value):
                    txn[key] = None
        
        output["transactions"] = transactions
    
    # Add symbol stats if available
    if symbol_stats:
        output["symbol_stats"] = symbol_stats
    
    # Add analysis results if available
    if analysis_results:
        output["analysis"] = analysis_results
    
    # Determine output file
    if not filename:
        filename = f"schwab_transactions.json"
    
    # Ensure the transaction_data directory exists
    transaction_dir = "transaction_data"
    os.makedirs(transaction_dir, exist_ok=True)
    
    # Create full path with transaction_data directory
    full_path = os.path.join(transaction_dir, filename)
    
    # Write to file with improved formatting for trades
    with open(full_path, 'w') as f:
        # If we have trades in the output, format them for better readability
        if 'trades' in output:
            # Sort trades by profit/loss for wins/losses
            output['trades'].sort(key=lambda x: abs(x['profit_loss']), reverse=True)
            
            # Add summary statistics for the trades
            total_pl = sum(t['profit_loss'] for t in output['trades'])
            avg_pl = total_pl / len(output['trades']) if output['trades'] else 0
            avg_pl_percent = sum(t['profit_loss_percent'] for t in output['trades']) / len(output['trades']) if output['trades'] else 0
            
            output['trade_summary'] = {
                'count': len(output['trades']),
                'total_profit_loss': total_pl,
                'average_profit_loss': avg_pl,
                'average_profit_loss_percent': avg_pl_percent
            }
        
        json.dump(output, f, indent=2)
    
    print(f"Saved JSON output to {full_path}")
    
    # Print trade summary to console if we have trades
    if 'trades' in output and output['trades']:
        print("\nTrade Summary:")
        print(f"Number of trades: {len(output['trades'])}")
        print(f"Total P&L: ${output['trade_summary']['total_profit_loss']:.2f}")
        print(f"Average P&L: ${output['trade_summary']['average_profit_loss']:.2f}")
        print(f"Average P&L %: {output['trade_summary']['average_profit_loss_percent']:.2f}%")
        
        print("\nTop 5 Trades:")
        for i, trade in enumerate(output['trades'][:5], 1):
            print(f"\n{i}. {trade['symbol']} ({trade['type'].upper()}):")
            print(f"   Entry: {trade['entry']['shares']} shares @ ${trade['entry']['price']:.2f} = ${trade['entry']['amount']:.2f}")
            print(f"   Exit:  {trade['exit']['shares']} shares @ ${trade['exit']['price']:.2f} = ${trade['exit']['amount']:.2f}")
            print(f"   P&L: ${trade['profit_loss']:.2f} ({trade['profit_loss_percent']:.2f}%)")
            print(f"   Exit Date: {trade['exit']['date']}")
    
    return output

def analyze_transactions_with_json(df):
    """Perform analysis and return results in a format suitable for JSON export"""
    if df.empty:
        return None
    
    analysis_results = {}
    
    # Transaction type breakdown
    type_counts = df['type'].value_counts().to_dict()
    type_percentages = {}
    for txn_type, count in type_counts.items():
        type_percentages[txn_type] = (count / len(df)) * 100
    
    analysis_results["transaction_types"] = {
        "counts": type_counts,
        "percentages": type_percentages
    }
    
    # Time span
    if 'date' in df.columns:
        min_date = df['date'].min()
        max_date = df['date'].max()
        date_range = (max_date - min_date).days + 1
        
        analysis_results["date_range"] = {
            "start_date": min_date.isoformat(),
            "end_date": max_date.isoformat(),
            "days": date_range
        }
    
    # Financial summary
    if 'amount' in df.columns:
        total_inflow = float(df[df['amount'] > 0]['amount'].sum())
        total_outflow = float(df[df['amount'] < 0]['amount'].sum())
        
        analysis_results["financial_summary"] = {
            "total_inflows": total_inflow,
            "total_outflows": total_outflow,
            "net_flow": total_inflow + total_outflow
        }
    
    # Trading activity
    if 'symbol' in df.columns and not df['symbol'].isna().all():
        trade_df = df[df['symbol'].notna()]
        
        if not trade_df.empty:
            symbol_counts = trade_df['symbol'].value_counts().head(10).to_dict()
            
            analysis_results["trading_activity"] = {
                "top_symbols": symbol_counts
            }
            
            # Fees
            if 'fees' in df.columns:
                total_fees = float(df['fees'].sum())
                analysis_results["trading_activity"]["total_fees"] = total_fees
    
    return analysis_results

def calculate_win_loss_stats_json(df, export_trades=None):
    """
    Calculate win/loss statistics and return as structured data for JSON export
    
    Parameters:
    - df: DataFrame containing transaction data
    - export_trades: Optional string ('wins', 'losses', 'breakeven') to export specific trades
    """
    if 'symbol' not in df.columns or 'amount' not in df.columns or 'quantity' not in df.columns:
        return None
    
    # Create a copy to avoid modifying the original dataframe
    trade_df = df.copy()
    
    # Identify buy and sell transactions
    trade_df['trade_type'] = 'Unknown'
    trade_df.loc[trade_df['quantity'] > 0, 'trade_type'] = 'Buy'
    trade_df.loc[trade_df['quantity'] < 0, 'trade_type'] = 'Sell'
    
    # Group by symbol for analysis
    symbols = trade_df['symbol'].unique()
    
    # Initialize win/loss counters
    total_wins = 0
    total_losses = 0
    long_wins = 0
    long_losses = 0
    short_wins = 0
    short_losses = 0
    symbol_stats = {}
    profit_loss_total = 0
    long_pl_total = 0
    short_pl_total = 0
    
    # Initialize collections for average win/loss calculations
    all_win_amounts = []
    all_loss_amounts = []
    all_long_win_amounts = []
    all_long_loss_amounts = []
    all_short_win_amounts = []
    all_short_loss_amounts = []
    
    # Initialize collections for trade tracking
    winning_trades = []
    losing_trades = []
    breakeven_trades = []
    
    # Initialize streak tracking
    all_streaks = []  # Positive for wins, negative for losses
    long_streaks = []
    short_streaks = []
    
    for symbol in symbols:
        # Filter for this symbol
        symbol_trades = trade_df[trade_df['symbol'] == symbol].sort_values('date')
        
        # Skip symbols with less than 2 trades (need at least a buy and sell)
        if len(symbol_trades) < 2:
            continue
            
        # Track positions for this symbol
        long_position = 0  # Positive values = long position
        short_position = 0  # Positive values = short position
        long_cost_basis = 0
        short_cost_basis = 0
        
        wins = 0
        losses = 0
        long_symbol_wins = 0
        long_symbol_losses = 0
        short_symbol_wins = 0
        short_symbol_losses = 0
        profit_loss = 0
        long_profit_loss = 0
        short_profit_loss = 0
        
        # Process each trade chronologically
        for _, trade in symbol_trades.iterrows():
            if trade['trade_type'] == 'Buy':
                bought_shares = abs(trade['quantity'])
                bought_amount = abs(trade['amount'])
                
                # Case 1: Covering a short position
                if short_position > 0:
                    # Calculate how many shares are used to cover the short
                    covering_shares = min(bought_shares, short_position)
                    
                    # Calculate P&L for the covered portion
                    covering_cost = (bought_amount / bought_shares) * covering_shares
                    short_sale_proceeds = short_cost_basis * covering_shares
                    
                    # For shorts: profit = sold high, bought low (short_sale_proceeds - covering_cost)
                    trade_pl = short_sale_proceeds - covering_cost
                    
                    # Store trade details with round trip information
                    trade_details = {
                        'symbol': symbol,
                        'type': 'short',
                        'entry': {
                            'date': None,  # Track this in future enhancement
                            'price': short_cost_basis,
                            'shares': covering_shares,
                            'amount': short_cost_basis * covering_shares
                        },
                        'exit': {
                            'date': trade['date'].isoformat() if isinstance(trade['date'], pd.Timestamp) else trade['date'],
                            'price': covering_cost / covering_shares,
                            'shares': covering_shares,
                            'amount': covering_cost
                        },
                        'profit_loss': trade_pl,
                        'profit_loss_percent': (trade_pl / (short_cost_basis * covering_shares)) * 100
                    }
                    
                    if abs(trade_pl) < 0.01:  # Consider trades with < 1 cent P&L as breakeven
                        breakeven_trades.append(trade_details)
                    elif trade_pl > 0:
                        winning_trades.append(trade_details)
                        short_symbol_wins += 1
                        short_wins += 1
                        total_wins += 1
                        wins += 1
                    else:
                        losing_trades.append(trade_details)
                        short_symbol_losses += 1
                        short_losses += 1
                        total_losses += 1
                        losses += 1
                    
                    short_profit_loss += trade_pl
                    profit_loss += trade_pl
                    
                    # Reduce short position
                    short_position -= covering_shares
                    
                    # Remaining shares go to long position
                    remaining_shares = bought_shares - covering_shares
                    if remaining_shares > 0:
                        remaining_cost = (bought_amount / bought_shares) * remaining_shares
                        
                        # Update long cost basis
                        if long_position + remaining_shares > 0:
                            long_cost_basis = ((long_position * long_cost_basis) + remaining_cost) / (long_position + remaining_shares)
                        
                        long_position += remaining_shares
                
                # Case 2: Adding to a long position
                else:
                    # Update long cost basis
                    if long_position + bought_shares > 0:
                        long_cost_basis = ((long_position * long_cost_basis) + bought_amount) / (long_position + bought_shares)
                    
                    long_position += bought_shares
            
            elif trade['trade_type'] == 'Sell':
                sold_shares = abs(trade['quantity'])
                sold_amount = abs(trade['amount'])
                
                # Case 1: Closing a long position
                if long_position > 0:
                    # Calculate how many shares are used to close the long
                    closing_shares = min(sold_shares, long_position)
                    
                    # Calculate P&L for the closed portion
                    closing_proceeds = (sold_amount / sold_shares) * closing_shares
                    long_cost = long_cost_basis * closing_shares
                    
                    # For longs: profit = sold high, bought low (closing_proceeds - long_cost)
                    trade_pl = closing_proceeds - long_cost
                    
                    # Store trade details with round trip information
                    trade_details = {
                        'symbol': symbol,
                        'type': 'long',
                        'entry': {
                            'date': None,  # Track this in future enhancement
                            'price': long_cost_basis,
                            'shares': closing_shares,
                            'amount': long_cost_basis * closing_shares
                        },
                        'exit': {
                            'date': trade['date'].isoformat() if isinstance(trade['date'], pd.Timestamp) else trade['date'],
                            'price': closing_proceeds / closing_shares,
                            'shares': closing_shares,
                            'amount': closing_proceeds
                        },
                        'profit_loss': trade_pl,
                        'profit_loss_percent': (trade_pl / (long_cost_basis * closing_shares)) * 100
                    }
                    
                    if abs(trade_pl) < 0.01:  # Consider trades with < 1 cent P&L as breakeven
                        breakeven_trades.append(trade_details)
                    elif trade_pl > 0:
                        winning_trades.append(trade_details)
                        long_symbol_wins += 1
                        long_wins += 1
                        total_wins += 1
                        wins += 1
                    else:
                        losing_trades.append(trade_details)
                        long_symbol_losses += 1
                        long_losses += 1
                        total_losses += 1
                        losses += 1
                    
                    long_profit_loss += trade_pl
                    profit_loss += trade_pl
                    
                    # Reduce long position
                    long_position -= closing_shares
                    
                    # Remaining shares go to short position
                    remaining_shares = sold_shares - closing_shares
                    if remaining_shares > 0:
                        remaining_proceeds = (sold_amount / sold_shares) * remaining_shares
                        
                        # Update short cost basis
                        if short_position + remaining_shares > 0:
                            short_cost_basis = ((short_position * short_cost_basis) + remaining_proceeds) / (short_position + remaining_shares)
                        
                        short_position += remaining_shares
                
                # Case 2: Opening a short position
                else:
                    # Update short cost basis
                    if short_position + sold_shares > 0:
                        short_cost_basis = ((short_position * short_cost_basis) + sold_amount) / (short_position + sold_shares)
                    
                    short_position += sold_shares
                    
        # Track win and loss amounts for average calculations
        win_amounts = []
        loss_amounts = []
        long_win_amounts = []
        long_loss_amounts = []
        short_win_amounts = []
        short_loss_amounts = []
        
        # Replay the trades to capture individual P&L values
        long_position = 0
        short_position = 0
        long_cost_basis = 0
        short_cost_basis = 0
        
        # Track streaks for this symbol
        symbol_streaks = []
        symbol_long_streaks = []
        symbol_short_streaks = []
        current_streak = 0  # Positive for wins, negative for losses
        current_long_streak = 0
        current_short_streak = 0
        
        # Second pass to calculate individual trade P&Ls and track streaks
        for _, trade in symbol_trades.iterrows():
            if trade['trade_type'] == 'Buy':
                bought_shares = abs(trade['quantity'])
                bought_amount = abs(trade['amount'])
                
                # Case 1: Covering a short position
                if short_position > 0:
                    covering_shares = min(bought_shares, short_position)
                    covering_cost = (bought_amount / bought_shares) * covering_shares
                    short_sale_proceeds = short_cost_basis * covering_shares
                    trade_pl = short_sale_proceeds - covering_cost
                    
                    # Track win/loss and streak for short positions
                    if trade_pl > 0:
                        short_win_amounts.append(trade_pl)
                        win_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_short_streak <= 0:
                            # End of a losing streak or start of tracking
                            if current_short_streak < 0:
                                symbol_short_streaks.append(current_short_streak)
                                short_streaks.append(current_short_streak)
                            current_short_streak = 1
                        else:
                            # Continue win streak
                            current_short_streak += 1
                            
                        if current_streak <= 0:
                            if current_streak < 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = 1
                        else:
                            current_streak += 1
                    else:
                        short_loss_amounts.append(trade_pl)
                        loss_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_short_streak >= 0:
                            # End of a winning streak or start of tracking
                            if current_short_streak > 0:
                                symbol_short_streaks.append(current_short_streak)
                                short_streaks.append(current_short_streak)
                            current_short_streak = -1
                        else:
                            # Continue loss streak
                            current_short_streak -= 1
                            
                        if current_streak >= 0:
                            if current_streak > 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = -1
                        else:
                            current_streak -= 1
                    
                    short_position -= covering_shares
                    
                    remaining_shares = bought_shares - covering_shares
                    if remaining_shares > 0:
                        remaining_cost = (bought_amount / bought_shares) * remaining_shares
                        if long_position + remaining_shares > 0:
                            long_cost_basis = ((long_position * long_cost_basis) + remaining_cost) / (long_position + remaining_shares)
                        long_position += remaining_shares
                else:
                    # Just adding to long position
                    if long_position + bought_shares > 0:
                        long_cost_basis = ((long_position * long_cost_basis) + bought_amount) / (long_position + bought_shares)
                    long_position += bought_shares
            
            elif trade['trade_type'] == 'Sell':
                sold_shares = abs(trade['quantity'])
                sold_amount = abs(trade['amount'])
                
                # Case 1: Closing a long position
                if long_position > 0:
                    closing_shares = min(sold_shares, long_position)
                    closing_proceeds = (sold_amount / sold_shares) * closing_shares
                    long_cost = long_cost_basis * closing_shares
                    trade_pl = closing_proceeds - long_cost
                    
                    # Track win/loss and streak for long positions
                    if trade_pl > 0:
                        long_win_amounts.append(trade_pl)
                        win_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_long_streak <= 0:
                            # End of a losing streak or start of tracking
                            if current_long_streak < 0:
                                symbol_long_streaks.append(current_long_streak)
                                long_streaks.append(current_long_streak)
                            current_long_streak = 1
                        else:
                            # Continue win streak
                            current_long_streak += 1
                            
                        if current_streak <= 0:
                            if current_streak < 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = 1
                        else:
                            current_streak += 1
                    else:
                        long_loss_amounts.append(trade_pl)
                        loss_amounts.append(trade_pl)
                        
                        # Update streak counters
                        if current_long_streak >= 0:
                            # End of a winning streak or start of tracking
                            if current_long_streak > 0:
                                symbol_long_streaks.append(current_long_streak)
                                long_streaks.append(current_long_streak)
                            current_long_streak = -1
                        else:
                            # Continue loss streak
                            current_long_streak -= 1
                            
                        if current_streak >= 0:
                            if current_streak > 0:
                                symbol_streaks.append(current_streak)
                                all_streaks.append(current_streak)
                            current_streak = -1
                        else:
                            current_streak -= 1
                    
                    long_position -= closing_shares
                    
                    remaining_shares = sold_shares - closing_shares
                    if remaining_shares > 0:
                        remaining_proceeds = (sold_amount / sold_shares) * remaining_shares
                        if short_position + remaining_shares > 0:
                            short_cost_basis = ((short_position * short_cost_basis) + remaining_proceeds) / (short_position + remaining_shares)
                        short_position += remaining_shares
                else:
                    # Just opening a short position
                    if short_position + sold_shares > 0:
                        short_cost_basis = ((short_position * short_cost_basis) + sold_amount) / (short_position + sold_shares)
                    short_position += sold_shares
        
        # Store symbol stats if there were completed trades
        if wins + losses > 0:
            win_rate = (wins / (wins + losses)) * 100
            
            # Calculate long and short win rates
            long_win_rate = 0 if long_symbol_wins + long_symbol_losses == 0 else (long_symbol_wins / (long_symbol_wins + long_symbol_losses)) * 100
            short_win_rate = 0 if short_symbol_wins + short_symbol_losses == 0 else (short_symbol_wins / (short_symbol_wins + short_symbol_losses)) * 100
            
            # Calculate average win and loss sizes
            avg_win = sum(win_amounts) / len(win_amounts) if win_amounts else 0
            avg_loss = sum(abs(x) for x in loss_amounts) / len(loss_amounts) if loss_amounts else 0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
            
            # Calculate long strategy averages
            long_avg_win = sum(long_win_amounts) / len(long_win_amounts) if long_win_amounts else 0
            long_avg_loss = sum(abs(x) for x in long_loss_amounts) / len(long_loss_amounts) if long_loss_amounts else 0
            long_win_loss_ratio = long_avg_win / long_avg_loss if long_avg_loss > 0 else 0
            
            # Calculate short strategy averages
            short_avg_win = sum(short_win_amounts) / len(short_win_amounts) if short_win_amounts else 0
            short_avg_loss = sum(abs(x) for x in short_loss_amounts) / len(short_loss_amounts) if short_loss_amounts else 0
            short_win_loss_ratio = short_avg_win / short_avg_loss if short_avg_loss > 0 else 0
            
            symbol_stats[symbol] = {
                'symbol': symbol,
                'total': {
                    'wins': wins,
                    'losses': losses,
                    'win_rate': float(win_rate),
                    'profit_loss': float(profit_loss),
                    'avg_win': float(avg_win),
                    'avg_loss': float(avg_loss),
                    'win_loss_ratio': float(win_loss_ratio)
                },
                'long': {
                    'wins': long_symbol_wins,
                    'losses': long_symbol_losses,
                    'win_rate': float(long_win_rate),
                    'profit_loss': float(long_profit_loss),
                    'avg_win': float(long_avg_win),
                    'avg_loss': float(long_avg_loss),
                    'win_loss_ratio': float(long_win_loss_ratio)
                },
                'short': {
                    'wins': short_symbol_wins,
                    'losses': short_symbol_losses,
                    'win_rate': float(short_win_rate),
                    'profit_loss': float(short_profit_loss),
                    'avg_win': float(short_avg_win),
                    'avg_loss': float(short_avg_loss),
                    'win_loss_ratio': float(short_win_loss_ratio)
                }
            }
            
            profit_loss_total += profit_loss
            long_pl_total += long_profit_loss
            short_pl_total += short_profit_loss
            
            # Collect all averages for overall calculation
            all_win_amounts.extend(win_amounts)
            all_loss_amounts.extend(loss_amounts)
            all_long_win_amounts.extend(long_win_amounts)
            all_long_loss_amounts.extend(long_loss_amounts)
            all_short_win_amounts.extend(short_win_amounts)
            all_short_loss_amounts.extend(short_loss_amounts)
    
    # Calculate overall statistics
    overall_stats = {}
    
    if total_wins + total_losses > 0:
        overall_win_rate = (total_wins / (total_wins + total_losses)) * 100
        long_overall_win_rate = 0 if long_wins + long_losses == 0 else (long_wins / (long_wins + long_losses)) * 100
        short_overall_win_rate = 0 if short_wins + short_losses == 0 else (short_wins / (short_wins + short_losses)) * 100
        
        # Calculate overall average win and loss
        overall_avg_win = sum(all_win_amounts) / len(all_win_amounts) if all_win_amounts else 0
        overall_avg_loss = sum(abs(x) for x in all_loss_amounts) / len(all_loss_amounts) if all_loss_amounts else 0
        overall_win_loss_ratio = overall_avg_win / overall_avg_loss if overall_avg_loss > 0 else 0
        
        # Calculate long strategy averages
        long_avg_win = sum(all_long_win_amounts) / len(all_long_win_amounts) if all_long_win_amounts else 0
        long_avg_loss = sum(abs(x) for x in all_long_loss_amounts) / len(all_long_loss_amounts) if all_long_loss_amounts else 0
        long_win_loss_ratio = long_avg_win / long_avg_loss if long_avg_loss > 0 else 0
        
        # Calculate short strategy averages
        short_avg_win = sum(all_short_win_amounts) / len(all_short_win_amounts) if all_short_win_amounts else 0
        short_avg_loss = sum(abs(x) for x in all_short_loss_amounts) / len(all_short_loss_amounts) if all_short_loss_amounts else 0
        short_win_loss_ratio = short_avg_win / short_avg_loss if short_avg_loss > 0 else 0
        
        # Calculate streak statistics
        # For all streaks
        win_streaks = [s for s in all_streaks if s > 0]
        loss_streaks = [abs(s) for s in all_streaks if s < 0]
        max_win_streak = max(win_streaks) if win_streaks else 0
        max_loss_streak = max(loss_streaks) if loss_streaks else 0
        avg_win_streak = sum(win_streaks) / len(win_streaks) if win_streaks else 0
        avg_loss_streak = sum(loss_streaks) / len(loss_streaks) if loss_streaks else 0
        
        # For long strategy
        long_win_streaks = [s for s in long_streaks if s > 0]
        long_loss_streaks = [abs(s) for s in long_streaks if s < 0]
        long_max_win_streak = max(long_win_streaks) if long_win_streaks else 0
        long_max_loss_streak = max(long_loss_streaks) if long_loss_streaks else 0
        long_avg_win_streak = sum(long_win_streaks) / len(long_win_streaks) if long_win_streaks else 0
        long_avg_loss_streak = sum(long_loss_streaks) / len(long_loss_streaks) if long_loss_streaks else 0
        
        # For short strategy
        short_win_streaks = [s for s in short_streaks if s > 0]
        short_loss_streaks = [abs(s) for s in short_streaks if s < 0]
        short_max_win_streak = max(short_win_streaks) if short_win_streaks else 0
        short_max_loss_streak = max(short_loss_streaks) if short_loss_streaks else 0
        short_avg_win_streak = sum(short_win_streaks) / len(short_win_streaks) if short_win_streaks else 0
        short_avg_loss_streak = sum(short_loss_streaks) / len(short_loss_streaks) if short_loss_streaks else 0
        
        overall_stats = {
            'total': {
                'wins': total_wins,
                'losses': total_losses,
                'win_rate': float(overall_win_rate),
                'profit_loss': float(profit_loss_total),
                'avg_win': float(overall_avg_win),
                'avg_loss': float(overall_avg_loss),
                'win_loss_ratio': float(overall_win_loss_ratio),
                'streaks': {
                    'max_win_streak': int(max_win_streak),
                    'max_loss_streak': int(max_loss_streak),
                    'avg_win_streak': float(avg_win_streak),
                    'avg_loss_streak': float(avg_loss_streak)
                }
            },
            'long': {
                'wins': long_wins,
                'losses': long_losses,
                'win_rate': float(long_overall_win_rate),
                'profit_loss': float(long_pl_total),
                'avg_win': float(long_avg_win),
                'avg_loss': float(long_avg_loss),
                'win_loss_ratio': float(long_win_loss_ratio),
                'streaks': {
                    'max_win_streak': int(long_max_win_streak),
                    'max_loss_streak': int(long_max_loss_streak),
                    'avg_win_streak': float(long_avg_win_streak),
                    'avg_loss_streak': float(long_avg_loss_streak)
                }
            },
            'short': {
                'wins': short_wins,
                'losses': short_losses,
                'win_rate': float(short_overall_win_rate),
                'profit_loss': float(short_pl_total),
                'avg_win': float(short_avg_win),
                'avg_loss': float(short_avg_loss),
                'win_loss_ratio': float(short_win_loss_ratio),
                'streaks': {
                    'max_win_streak': int(short_max_win_streak),
                    'max_loss_streak': int(short_max_loss_streak),
                    'avg_win_streak': float(short_avg_win_streak),
                    'avg_loss_streak': float(short_avg_loss_streak)
                }
            }
        }
    
    # Organize symbol stats into a list for easier JSON handling
    symbol_stats_list = [stats for sym, stats in symbol_stats.items()]
    
    result = {
        "overall": overall_stats,
        "symbols": symbol_stats_list
    }
    
    # Add specific trades if requested
    if export_trades == 'wins':
        result['trades'] = winning_trades
    elif export_trades == 'losses':
        result['trades'] = losing_trades
    elif export_trades == 'breakeven':
        result['trades'] = breakeven_trades
    
    return result

def analyze_transactions(df):
    """Perform basic analysis on transaction data"""
    if df.empty:
        print("No transactions to analyze")
        return
        
    print("\nTransaction Analysis:")
    print("-" * 80)
    
    # Transaction type breakdown
    print("\nTransaction Type Breakdown:")
    type_counts = df['type'].value_counts()
    for txn_type, count in type_counts.items():
        percent = (count / len(df)) * 100
        print(f"{txn_type}: {count} ({percent:.1f}%)")
    
    # Time span
    if 'date' in df.columns:
        min_date = df['date'].min()
        max_date = df['date'].max()
        date_range = (max_date - min_date).days + 1
        print(f"\nDate Range: {min_date.date()} to {max_date.date()} ({date_range} days)")
    
    # Financial summary
    if 'amount' in df.columns:
        total_inflow = df[df['amount'] > 0]['amount'].sum()
        total_outflow = df[df['amount'] < 0]['amount'].sum()
        print(f"\nFinancial Summary:")
        print(f"Total Inflows: ${total_inflow:,.2f}")
        print(f"Total Outflows: ${abs(total_outflow):,.2f}")
        print(f"Net Flow: ${(total_inflow + total_outflow):,.2f}")
    
    # Trading activity (if applicable)
    if 'symbol' in df.columns and not df['symbol'].isna().all():
        trade_df = df[df['symbol'].notna()]
        
        if not trade_df.empty:
            print("\nTrading Activity:")
            symbol_counts = trade_df['symbol'].value_counts().head(10)
            print("\nMost Traded Symbols:")
            for symbol, count in symbol_counts.items():
                print(f"{symbol}: {count} transactions")
            
            # Calculate fees if available
            if 'fees' in df.columns:
                total_fees = df['fees'].sum()
                print(f"\nTotal Fees Paid: ${abs(total_fees):,.2f}")
            
            # Calculate win/loss statistics
            calculate_win_loss_stats(trade_df)
    
    print("-" * 80)

def main():
    """Main function to handle command line arguments and workflow"""
    parser = argparse.ArgumentParser(description='Schwab Transaction Data Retriever')
    
    parser.add_argument('--days', type=int, default=None,
                        help='Number of days to look back for transactions (defaults to today if not specified)')
    parser.add_argument('--account', type=int, default=None,
                        help='Account index to retrieve (default: all accounts)')
    parser.add_argument('--symbol', type=str, default=None,
                        help='Filter by symbol (e.g., AAPL, MSFT)')
    parser.add_argument('--type', type=str, default=None,
                        help='Filter by transaction type (e.g., TRADE, DIVIDEND)')
    parser.add_argument('--no-csv', action='store_true',
                        help='Disable CSV output')
    parser.add_argument('--analyze', action='store_true',
                        help='Perform basic analysis on transaction data')
    parser.add_argument('--max-display', type=int, default=50,
                        help='Maximum number of transactions to display (default: 50)')
    parser.add_argument('--json', action='store_true',
                        help='Output data in JSON format')
    parser.add_argument('--json-file', type=str, default=None,
                        help='Filename for JSON output (default: auto-generated)')
    parser.add_argument('--json-only', action='store_true',
                        help='Output only JSON format (suppress console output)')
    parser.add_argument('--stats-only', action='store_true',
                        help='Output only trading stats in JSON format (shorthand for --json --analyze --json-only)')
    parser.add_argument('--trade-outcome', type=str, choices=['wins', 'losses', 'breakeven'],
                        help='Export trades with specific outcome to JSON file')
    
    args = parser.parse_args()
    
    # Handle stats-only shortcut
    if args.stats_only:
        args.json = True
        args.analyze = True
        args.json_only = True
    
    # Initialize the transaction fetcher
    fetcher = SchwabTransactionFetcher()
    
    # List available accounts (unless JSON-only mode)
    if not args.json_only:
        fetcher.list_accounts()
    
    # Process based on arguments
    if args.account is not None:
        # Process single account
        account = fetcher.get_account_by_index(args.account - 1)  # Convert to 0-based index
        if not account:
            print("Account not found")
            return
            
        account_hash = account.get('hashValue')
        display_name = account.get('displayName', f'Account {args.account}')
        
        if not args.json_only:
            print(f"\nProcessing transactions for {display_name}")
        
        # If days is None, just get today's transactions
        if args.days is None:
            from_date = datetime.now().strftime("%Y-%m-%d")
        else:
            from_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Default to TRADE type if none specified
        transaction_type = args.type if args.type else "TRADE"
        
        txns = fetcher.get_transactions(
            account_hash=account_hash,
            from_date=from_date,
            to_date=to_date,
            symbol=args.symbol,
            transaction_type=transaction_type,
            max_results=1000
        )
        
        df = fetcher.process_transactions(txns)
        
        # Save to CSV unless disabled
        if not args.no_csv:
            account_filename = f"schwab_transactions_{display_name.replace(' ', '_')}_{from_date}_{to_date}.csv"
            fetcher.save_to_csv(df, account_filename)
    else:
        # Process all accounts
        df = fetcher.get_all_transactions(
            days=args.days,
            csv_output=not args.no_csv
        )
    
    # Handle JSON output if requested
    if args.json and not df.empty:
        # Generate analysis data for JSON
        analysis_results = analyze_transactions_with_json(df) if args.analyze else None
        stats_results = calculate_win_loss_stats_json(df, args.trade_outcome) if args.analyze else None
        
        # Generate output filename if not specified
        json_filename = args.json_file
        if not json_filename:
            # If days is None, just use today's date for from_date
            if args.days is None:
                from_date = datetime.now().strftime("%Y-%m-%d")
            else:
                from_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
            to_date = datetime.now().strftime("%Y-%m-%d")
            outcome_suffix = f"_{args.trade_outcome}" if args.trade_outcome else ""
            if args.account is not None:
                display_name = account.get('displayName', f'Account_{args.account}').replace(' ', '_')
                json_filename = f"schwab_transactions_{display_name}_{from_date}_{to_date}{outcome_suffix}.json"
            else:
                json_filename = f"schwab_transactions_all_accounts_{from_date}_{to_date}{outcome_suffix}.json"
        
        # Export to JSON (without transaction data by default, as requested)
        export_to_json(df, stats_results, analysis_results, json_filename, include_transactions=False)
    
    # Skip console output in JSON-only mode
    if args.json_only:
        return
    
    # Display transactions
    display_transactions(df, args.max_display)
    
    # Analyze if requested
    if args.analyze and not df.empty:
        analyze_transactions(df)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation canceled by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
