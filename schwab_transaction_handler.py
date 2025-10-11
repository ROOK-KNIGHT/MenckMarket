import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from connection_manager import ensure_valid_tokens
from config_loader import get_config

class SchwabTransactionHandler:
    """
    Handles Schwab API transaction data retrieval.
    Focused on fetching and processing raw transaction data from Schwab API.
    """
    
    def __init__(self):
        """
        Initialize the SchwabTransactionHandler.
        """
        # Load configuration
        self.config = get_config()
        self.api_config = self.config.get_api_config()
        
        # Get API settings from configuration with proper defaults
        if self.api_config:
            self.max_retries = self.api_config.get('max_retries', 5)
            self.retry_delay = self.api_config.get('retry_delay', 2)
            self.rate_limit_delay = self.api_config.get('rate_limit_delay', 60)
            self.base_url = self.api_config.get('base_url', 'https://api.schwabapi.com')
            self.request_timeout = self.api_config.get('request_timeout', 10)
        else:
            # Fallback defaults if config is not available
            self.max_retries = 5
            self.retry_delay = 2
            self.rate_limit_delay = 60
            self.base_url = 'https://api.schwabapi.com'
            self.request_timeout = 10
        

    def get_account_numbers(self) -> List[Dict]:
        """
        Get all available account numbers and details.
        
        Returns:
            List[Dict]: List of account dictionaries with account details
        """
        retry_delay = self.retry_delay
        
        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]
                
                url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
                
                response = requests.get(url, headers=headers, timeout=self.request_timeout)
                response.raise_for_status()
                
                accounts = response.json()
                
                if isinstance(accounts, list) and len(accounts) > 0:
                    return accounts
                else:
                    print(f"No accounts found in response: {accounts}")
                    return []
                    
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 401:
                    print("Token expired, refreshing tokens...")
                    ensure_valid_tokens(refresh=True)
                    # Continue to retry with new token
                elif response.status_code == 429:
                    print("Rate limit exceeded, waiting before retry...")
                    time.sleep(self.rate_limit_delay)
                    # Continue to retry after rate limit delay
                else:
                    print(f"HTTP error occurred: {http_err}")
                    if attempt < self.max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print("Maximum retry attempts reached")
                        return []
                        
            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return []
                    
        return []

    def get_transactions(self, account_hash: str, 
                        from_date: Optional[str] = None, 
                        to_date: Optional[str] = None,
                        transaction_type: Optional[str] = None,
                        symbol: Optional[str] = None,
                        max_results: int = 100) -> List[Dict]:
        """
        Get transaction history for a specified account.
        
        Args:
            account_hash (str): Account hash ID
            from_date (str, optional): Start date in YYYY-MM-DD format (default: 30 days ago)
            to_date (str, optional): End date in YYYY-MM-DD format (default: today)
            transaction_type (str, optional): Filter by transaction type (e.g., "TRADE", "DIVIDEND", etc.)
            symbol (str, optional): Filter by security symbol
            max_results (int): Maximum number of results to return (default: 100)
        
        Returns:
            List[Dict]: List of transaction dictionaries
        """
        # Set default date range if not specified (last 30 days)
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        # Format dates for API with milliseconds as required by Schwab API
        from_date_iso = f"{from_date}T00:00:00.000Z"
        to_date_iso = f"{to_date}T23:59:59.000Z"
        
        retry_delay = self.retry_delay
        
        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]
                
                # Build the URL with query parameters
                url = f"{self.base_url}/trader/v1/accounts/{account_hash}/transactions"
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
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=20)
                response.raise_for_status()
                
                # Parse and return the results
                transactions = response.json()
                return transactions
            
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 401:
                    print("Token expired, refreshing tokens...")
                    ensure_valid_tokens(refresh=True)
                    # Continue to retry with new token
                elif response.status_code == 429:
                    print("Rate limit exceeded, waiting before retry...")
                    time.sleep(self.rate_limit_delay)
                    # Continue to retry after rate limit delay
                else:
                    print(f"HTTP error occurred: {http_err}")
                    if attempt < self.max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print("Maximum retry attempts reached")
                        return []
                        
            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return []
    
    def get_transaction_details(self, account_hash: str, transaction_id: str) -> Dict:
        """
        Get detailed information about a specific transaction.
        
        Args:
            account_hash (str): Account hash ID
            transaction_id (str): Transaction ID to fetch details for
        
        Returns:
            Dict: Transaction details dictionary
        """
        retry_delay = self.retry_delay
        
        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]
                
                url = f"{self.base_url}/trader/v1/accounts/{account_hash}/transactions/{transaction_id}"
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
                
                response = requests.get(url, headers=headers, timeout=self.request_timeout)
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 401:
                    print("Token expired, refreshing tokens...")
                    ensure_valid_tokens(refresh=True)
                    # Continue to retry with new token
                elif response.status_code == 429:
                    print("Rate limit exceeded, waiting before retry...")
                    time.sleep(self.rate_limit_delay)
                    # Continue to retry after rate limit delay
                else:
                    print(f"HTTP error occurred: {http_err}")
                    if attempt < self.max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print("Maximum retry attempts reached")
                        return {}
                        
            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return {}
                    
        return {}
            
    def process_transactions(self, transactions: List[Dict]) -> pd.DataFrame:
        """
        Process raw transaction data into a structured DataFrame.
        
        Args:
            transactions (List[Dict]): List of transaction dictionaries from API
        
        Returns:
            pd.DataFrame: Pandas DataFrame with standardized transaction data
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
        """
        Save transaction data to CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame containing transaction data
            filename (str, optional): Output filename (default: auto-generated)
        """
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

    def create_transactions_json(self, df: pd.DataFrame) -> bool:
        """Create transactions.json file for database insertion matching the transactions table schema"""
        try:
            import json
            
            if df.empty:
                print("❌ No transaction data available")
                return False
            
            # Create the JSON structure for db_inserter
            transactions_data = {
                'strategy_name': 'Transaction_Data',
                'last_updated': datetime.now().isoformat(),
                'total_transactions': len(df),
                'transactions': [],
                'metadata': {
                    'data_source': 'schwab_api',
                    'handler_type': 'schwab_transaction_handler',
                    'update_frequency': 'on_demand',
                    'database_integration': True,
                    'creation_date': datetime.now().isoformat()
                }
            }
            
            # Process each transaction to match database schema
            for _, row in df.iterrows():
                # Create transaction record matching the database schema
                transaction_record = {
                    'transaction_id': str(row.get('transaction_id', '')),
                    'symbol': str(row.get('symbol', '')),
                    'transaction_type': str(row.get('type', 'UNKNOWN')),
                    'quantity': float(row.get('quantity', 0.0)),
                    'price': float(row.get('price', 0.0)),
                    'amount': float(row.get('amount', 0.0)),
                    'fees': float(row.get('fees', 0.0)),
                    'account': str(row.get('account_name', row.get('account_hash', 'Unknown'))),
                    'timestamp': row.get('date', datetime.now()).isoformat() if pd.notna(row.get('date')) else datetime.now().isoformat()
                }
                
                transactions_data['transactions'].append(transaction_record)
            
            # Write to transactions.json in root directory
            with open('transactions.json', 'w') as f:
                json.dump(transactions_data, f, indent=2)
            
            print(f"✅ Created transactions.json with {len(df)} transactions")
            print(f"📊 Transactions processed from {len(df['account_name'].unique()) if 'account_name' in df.columns else 1} accounts")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating transactions.json: {e}")
            return False

    def refresh_and_store_transaction_data(self, days: int = 30) -> Dict[str, Any]:
        """Refresh transaction data and create JSON for database insertion"""
        try:
            # Get fresh transaction data
            df = self.get_all_transactions(days=days, csv_output=True)
            
            if df.empty:
                return {
                    'success': False,
                    'error': 'No transaction data available',
                    'transactions_processed': 0
                }
            
            # Create transactions.json
            json_created = self.create_transactions_json(df)
            
            return {
                'success': True,
                'transactions_processed': len(df),
                'json_created': json_created,
                'csv_created': True,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Error refreshing transaction data: {e}")
            return {
                'success': False,
                'error': str(e),
                'transactions_processed': 0
            }
    
    def get_all_transactions(self, days: int = None, csv_output: bool = True) -> pd.DataFrame:
        """
        Get transactions for all accounts over the specified period.
        
        Args:
            days (int, optional): Number of days to look back (default: None, uses today only)
            csv_output (bool): Whether to save results to CSV files (default: True)
            
        Returns:
            pd.DataFrame: Combined transaction data from all accounts
        """
        # If days is None, just get today's transactions
        if days is None:
            from_date = datetime.now().strftime("%Y-%m-%d")
        else:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Get account numbers
        accounts = self.get_account_numbers()
        if not accounts:
            print("No accounts found")
            return pd.DataFrame()
        
        all_data = []
        
        for i, account in enumerate(accounts):
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

    def calculate_win_loss_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate comprehensive win/loss statistics for trades.
        
        Args:
            df (pd.DataFrame): DataFrame containing transaction data
            
        Returns:
            Dict[str, Any]: Dictionary containing win/loss statistics
        """
        if df.empty or 'symbol' not in df.columns or 'amount' not in df.columns or 'quantity' not in df.columns:
            return {
                'overall': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                'long': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                'short': {'wins': 0, 'losses': 0, 'win_rate': 0, 'total_pl': 0, 'avg_win': 0, 'avg_loss': 0},
                'symbols': []
            }
        
        # Create a copy to avoid modifying the original dataframe
        trade_df = df.copy()
        
        # Identify buy and sell transactions
        trade_df['trade_type'] = 'Unknown'
        trade_df.loc[trade_df['quantity'] > 0, 'trade_type'] = 'Buy'
        trade_df.loc[trade_df['quantity'] < 0, 'trade_type'] = 'Sell'
        
        # Initialize win/loss counters
        total_wins = 0
        total_losses = 0
        long_wins = 0
        long_losses = 0
        short_wins = 0
        short_losses = 0
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
        
        # Simple win/loss calculation based on transaction amounts
        for _, trade in trade_df.iterrows():
            if trade['amount'] != 0:  # Skip zero-amount transactions
                if trade['amount'] > 0:
                    total_wins += 1
                    all_win_amounts.append(trade['amount'])
                    if trade['quantity'] > 0:  # Long position
                        long_wins += 1
                        all_long_win_amounts.append(trade['amount'])
                    else:  # Short position
                        short_wins += 1
                        all_short_win_amounts.append(trade['amount'])
                else:
                    total_losses += 1
                    all_loss_amounts.append(abs(trade['amount']))
                    if trade['quantity'] > 0:  # Long position
                        long_losses += 1
                        all_long_loss_amounts.append(abs(trade['amount']))
                    else:  # Short position
                        short_losses += 1
                        all_short_loss_amounts.append(abs(trade['amount']))
                
                profit_loss_total += trade['amount']
                if trade['quantity'] > 0:
                    long_pl_total += trade['amount']
                else:
                    short_pl_total += trade['amount']
        
        # Calculate overall statistics
        overall_stats = {}
        long_stats = {}
        short_stats = {}
        
        if total_wins + total_losses > 0:
            overall_win_rate = (total_wins / (total_wins + total_losses)) * 100
            overall_avg_win = sum(all_win_amounts) / len(all_win_amounts) if all_win_amounts else 0
            overall_avg_loss = sum(all_loss_amounts) / len(all_loss_amounts) if all_loss_amounts else 0
            
            overall_stats = {
                'wins': total_wins,
                'losses': total_losses,
                'win_rate': float(overall_win_rate),
                'total_pl': float(profit_loss_total),
                'avg_win': float(overall_avg_win),
                'avg_loss': float(overall_avg_loss),
                'win_loss_ratio': float(overall_avg_win / overall_avg_loss) if overall_avg_loss > 0 else 0
            }
        
        if long_wins + long_losses > 0:
            long_win_rate = (long_wins / (long_wins + long_losses)) * 100
            long_avg_win = sum(all_long_win_amounts) / len(all_long_win_amounts) if all_long_win_amounts else 0
            long_avg_loss = sum(all_long_loss_amounts) / len(all_long_loss_amounts) if all_long_loss_amounts else 0
            
            long_stats = {
                'wins': long_wins,
                'losses': long_losses,
                'win_rate': float(long_win_rate),
                'total_pl': float(long_pl_total),
                'avg_win': float(long_avg_win),
                'avg_loss': float(long_avg_loss),
                'win_loss_ratio': float(long_avg_win / long_avg_loss) if long_avg_loss > 0 else 0
            }
        
        if short_wins + short_losses > 0:
            short_win_rate = (short_wins / (short_wins + short_losses)) * 100
            short_avg_win = sum(all_short_win_amounts) / len(all_short_win_amounts) if all_short_win_amounts else 0
            short_avg_loss = sum(all_short_loss_amounts) / len(all_short_loss_amounts) if all_short_loss_amounts else 0
            
            short_stats = {
                'wins': short_wins,
                'losses': short_losses,
                'win_rate': float(short_win_rate),
                'total_pl': float(short_pl_total),
                'avg_win': float(short_avg_win),
                'avg_loss': float(short_avg_loss),
                'win_loss_ratio': float(short_avg_win / short_avg_loss) if short_avg_loss > 0 else 0
            }
        
        return {
            'overall': overall_stats,
            'long': long_stats,
            'short': short_stats,
            'symbols': []
        }



def main():
    """Main function - automatically fetch transactions and store in database"""
    print("Schwab Transaction Handler - Transaction Analysis")
    print("=" * 50)
    
    # Initialize handler
    handler = SchwabTransactionHandler()
    
    # Get transactions for the last 30 days and create JSON
    result = handler.refresh_and_store_transaction_data(days=30)
    
    if result['success']:
        print(f"✅ Transaction analysis completed")
        print(f"📊 Processed {result['transactions_processed']} transactions")
        print(f"✅ Created transactions.json for database insertion")
        
        # Get the dataframe for statistics display
        df = handler.get_all_transactions(days=30, csv_output=False)
        if not df.empty:
            # Calculate and display win/loss statistics
            stats = handler.calculate_win_loss_stats(df)
            overall = stats.get('overall', {})
            print(f"📈 Overall Stats: {overall.get('wins', 0)} wins, {overall.get('losses', 0)} losses")
            print(f"📊 Win Rate: {overall.get('win_rate', 0):.1f}%")
            print(f"💵 Total P&L: ${overall.get('total_pl', 0):.2f}")
    else:
        print(f"❌ Transaction analysis failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
