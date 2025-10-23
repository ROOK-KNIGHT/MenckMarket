#!/usr/bin/env python3
"""
Account Data Handler

This handler provides comprehensive account information retrieval from Schwab API:
1. Account balances (initial, current, projected)
2. Position details with P&L calculations
3. Account metadata and trading restrictions
4. Buying power and margin information
5. Real-time account status updates

All data follows Schwab API structure and integrates with existing handlers.
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from connection_manager import ensure_valid_tokens, is_authentication_paused, make_authenticated_request
from config_loader import get_config

class AccountDataHandler:
    """
    Handles Schwab API account data retrieval and processing.
    Focused on fetching comprehensive account information including balances,
    positions, and trading restrictions.
    """
    
    def __init__(self):
        """
        Initialize the AccountDataHandler.
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

    def get_all_accounts(self, include_positions: bool = True) -> List[Dict[str, Any]]:
        """
        Get all linked account information for the user with authentication pause support.
        
        Args:
            include_positions (bool): Whether to include position details (default: True)
            
        Returns:
            List[Dict[str, Any]]: List of account dictionaries with full details
        """
        # Check if authentication is paused
        if is_authentication_paused():
            print("🛑 Authentication paused - skipping get_all_accounts")
            return []
        
        try:
            # Build URL with optional fields parameter
            url = f"{self.base_url}/trader/v1/accounts"
            params = {}
            if include_positions:
                params['fields'] = 'positions'
            
            # Use the improved authenticated request method
            success, accounts = make_authenticated_request(
                url, 
                "get all accounts", 
                max_retries=self.max_retries,
                params=params,
                timeout=self.request_timeout
            )
            
            if success and accounts:
                if isinstance(accounts, list) and len(accounts) > 0:
                    return accounts
                else:
                    print(f"No accounts found in response: {accounts}")
                    return []
            else:
                print("❌ Failed to get accounts data")
                return []
                
        except Exception as e:
            print(f"❌ Unexpected error getting all accounts: {e}")
            return []

    def get_account_details(self, account_number: str, include_positions: bool = True) -> Dict[str, Any]:
        """
        Get detailed information for a specific account.
        
        Args:
            account_number (str): Account number to retrieve
            include_positions (bool): Whether to include position details (default: True)
            
        Returns:
            Dict[str, Any]: Account details dictionary
        """
        retry_delay = self.retry_delay
        
        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]
                
                # Build URL with optional fields parameter
                url = f"{self.base_url}/trader/v1/accounts/{account_number}"
                params = {}
                if include_positions:
                    params['fields'] = 'positions'
                
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=self.request_timeout)
                response.raise_for_status()
                
                account_data = response.json()
                return account_data
                
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

    def extract_positions(self, account_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract and standardize position data from account response.
        
        Args:
            account_data (Dict[str, Any]): Raw account data from API
            
        Returns:
            List[Dict[str, Any]]: Standardized position data
        """
        positions = []
        
        # Navigate the account structure
        securities_account = account_data.get('securitiesAccount', {})
        raw_positions = securities_account.get('positions', [])
        
        for position in raw_positions:
            # Extract instrument information
            instrument = position.get('instrument', {})
            
            # Calculate total quantity (long - short)
            long_qty = position.get('longQuantity', 0)
            short_qty = position.get('shortQuantity', 0)
            total_qty = long_qty - short_qty
            
            # Calculate P&L
            long_pl = position.get('longOpenProfitLoss', 0)
            short_pl = position.get('shortOpenProfitLoss', 0)
            total_pl = long_pl + short_pl
            
            # Calculate day P&L
            day_pl = position.get('currentDayProfitLoss', 0)
            day_pl_pct = position.get('currentDayProfitLossPercentage', 0)
            
            standardized_position = {
                'symbol': instrument.get('symbol', ''),
                'description': instrument.get('description', ''),
                'cusip': instrument.get('cusip', ''),
                'instrument_id': instrument.get('instrumentId', 0),
                'instrument_type': instrument.get('type', ''),
                'net_change': instrument.get('netChange', 0),
                
                # Quantities
                'quantity': total_qty,
                'long_quantity': long_qty,
                'short_quantity': short_qty,
                'settled_long_quantity': position.get('settledLongQuantity', 0),
                'settled_short_quantity': position.get('settledShortQuantity', 0),
                'aged_quantity': position.get('agedQuantity', 0),
                'previous_session_long_quantity': position.get('previousSessionLongQuantity', 0),
                'previous_session_short_quantity': position.get('previousSessionShortQuantity', 0),
                
                # Prices
                'average_price': position.get('averagePrice', 0),
                'average_long_price': position.get('averageLongPrice', 0),
                'average_short_price': position.get('averageShortPrice', 0),
                'tax_lot_average_long_price': position.get('taxLotAverageLongPrice', 0),
                'tax_lot_average_short_price': position.get('taxLotAverageShortPrice', 0),
                
                # Values and P&L
                'market_value': position.get('marketValue', 0),
                'unrealized_pl': total_pl,
                'long_open_pl': long_pl,
                'short_open_pl': short_pl,
                'current_day_pl': day_pl,
                'current_day_pl_percentage': day_pl_pct,
                'current_day_cost': position.get('currentDayCost', 0),
                
                # Risk
                'maintenance_requirement': position.get('maintenanceRequirement', 0),
                
                # Metadata
                'account_number': securities_account.get('accountNumber', ''),
                'timestamp': datetime.now().isoformat()
            }
            
            positions.append(standardized_position)
            
        return positions

    def extract_balances(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and standardize balance data from account response.
        
        Args:
            account_data (Dict[str, Any]): Raw account data from API
            
        Returns:
            Dict[str, Any]: Standardized balance data
        """
        securities_account = account_data.get('securitiesAccount', {})
        
        # Extract all balance types
        initial_balances = securities_account.get('initialBalances', {})
        current_balances = securities_account.get('currentBalances', {})
        projected_balances = securities_account.get('projectedBalances', {})
        
        return {
            'account_number': securities_account.get('accountNumber', ''),
            'initial_balances': initial_balances,
            'current_balances': current_balances,
            'projected_balances': projected_balances,
            'timestamp': datetime.now().isoformat()
        }

    def extract_account_metadata(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract account metadata and trading restrictions.
        
        Args:
            account_data (Dict[str, Any]): Raw account data from API
            
        Returns:
            Dict[str, Any]: Account metadata
        """
        securities_account = account_data.get('securitiesAccount', {})
        
        return {
            'account_number': securities_account.get('accountNumber', ''),
            'round_trips': securities_account.get('roundTrips', 0),
            'is_day_trader': securities_account.get('isDayTrader', False),
            'is_closing_only_restricted': securities_account.get('isClosingOnlyRestricted', False),
            'pfcb_flag': securities_account.get('pfcbFlag', False),
            'timestamp': datetime.now().isoformat()
        }

    def get_account_summary(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a comprehensive account summary.
        
        Args:
            account_data (Dict[str, Any]): Raw account data from API
            
        Returns:
            Dict[str, Any]: Account summary with key metrics
        """
        positions = self.extract_positions(account_data)
        balances = self.extract_balances(account_data)
        metadata = self.extract_account_metadata(account_data)
        
        # Calculate position summaries
        total_positions = len(positions)
        total_market_value = sum(pos['market_value'] for pos in positions)
        total_unrealized_pl = sum(pos['unrealized_pl'] for pos in positions)
        total_day_pl = sum(pos['current_day_pl'] for pos in positions)
        
        # Get key balance metrics
        current_balances = balances.get('current_balances', {})
        equity = current_balances.get('equity', 0)
        buying_power = current_balances.get('buyingPower', 0)
        available_funds = current_balances.get('availableFunds', 0)
        
        return {
            'account_number': metadata['account_number'],
            'timestamp': datetime.now().isoformat(),
            
            # Position Summary
            'positions': {
                'total_count': total_positions,
                'total_market_value': total_market_value,
                'total_unrealized_pl': total_unrealized_pl,
                'total_day_pl': total_day_pl,
                'symbols': [pos['symbol'] for pos in positions if pos['symbol']]
            },
            
            # Key Balances
            'balances': {
                'equity': equity,
                'buying_power': buying_power,
                'available_funds': available_funds,
                'day_trading_buying_power': current_balances.get('dayTradingBuyingPower', 0),
                'stock_buying_power': current_balances.get('stockBuyingPower', 0),
                'option_buying_power': current_balances.get('optionBuyingPower', 0)
            },
            
            # Account Status
            'status': {
                'is_day_trader': metadata['is_day_trader'],
                'is_closing_only_restricted': metadata['is_closing_only_restricted'],
                'round_trips': metadata['round_trips'],
                'pfcb_flag': metadata['pfcb_flag']
            },
            
            # Risk Metrics
            'risk': {
                'maintenance_requirement': sum(pos['maintenance_requirement'] for pos in positions),
                'equity_percentage': current_balances.get('equityPercentage', 0),
                'margin_balance': current_balances.get('marginBalance', 0),
                'is_in_call': current_balances.get('isInCall', 0)
            }
        }

    def get_all_account_summaries(self) -> List[Dict[str, Any]]:
        """
        Get summaries for all linked accounts and automatically update account_data.json.
        
        Returns:
            List[Dict[str, Any]]: List of account summaries
        """
        accounts = self.get_all_accounts(include_positions=True)
        summaries = []
        
        for account in accounts:
            try:
                summary = self.get_account_summary(account)
                summaries.append(summary)
            except Exception as e:
                print(f"Error processing account summary: {e}")
                continue
        
        # Automatically update account_data.json every time this method is called
        if summaries:
            try:
                self._update_account_data_json(summaries)
            except Exception as e:
                print(f"Warning: Failed to update account_data.json: {e}")
                
        return summaries

    def _update_account_data_json(self, summaries: List[Dict[str, Any]]) -> bool:
        """
        Internal method to update account_data.json file with current account summaries.
        Called automatically every time get_all_account_summaries() is executed.
        
        Args:
            summaries (List[Dict[str, Any]]): List of account summaries
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import json
            
            # Create the JSON structure for db_inserter
            account_data = {
                'strategy_name': 'Account_Data',
                'last_updated': datetime.now().isoformat(),
                'total_accounts': len(summaries),
                'account_data': {},
                'metadata': {
                    'data_source': 'schwab_api',
                    'handler_type': 'account_data_handler',
                    'update_frequency': 'realtime_monitor_calls',
                    'database_integration': True,
                    'auto_updated': True,
                    'creation_date': datetime.now().isoformat()
                }
            }
            
            # Process each account summary to match database schema
            for summary in summaries:
                account_number = summary['account_number']
                positions = summary.get('positions', {})
                balances = summary.get('balances', {})
                status = summary.get('status', {})
                risk = summary.get('risk', {})
                
                # Create account data matching the database schema
                account_record = {
                    'account_number': account_number,
                    'total_count': positions.get('total_count', 0),
                    'total_market_value': float(positions.get('total_market_value', 0.0)),
                    'total_unrealized_pl': float(positions.get('total_unrealized_pl', 0.0)),
                    'total_day_pl': float(positions.get('total_day_pl', 0.0)),
                    'equity': float(balances.get('equity', 0.0)),
                    'buying_power': float(balances.get('buying_power', 0.0)),
                    'available_funds': float(balances.get('available_funds', 0.0)),
                    'day_trading_buying_power': float(balances.get('day_trading_buying_power', 0.0)),
                    'stock_buying_power': float(balances.get('stock_buying_power', 0.0)),
                    'option_buying_power': float(balances.get('option_buying_power', 0.0)),
                    'is_day_trader': bool(status.get('is_day_trader', False)),
                    'is_closing_only_restricted': bool(status.get('is_closing_only_restricted', False)),
                    'round_trips': int(status.get('round_trips', 0)),
                    'pfcb_flag': bool(status.get('pfcb_flag', False)),
                    'maintenance_requirement': float(risk.get('maintenance_requirement', 0.0)),
                    'equity_percentage': float(risk.get('equity_percentage', 0.0)),
                    'margin_balance': float(risk.get('margin_balance', 0.0)),
                    'is_in_call': int(risk.get('is_in_call', 0)),
                    'timestamp': datetime.now().isoformat()
                }
                
                account_data['account_data'][account_number] = account_record
            
            # Write to account_data.json in root directory
            with open('account_data.json', 'w') as f:
                json.dump(account_data, f, indent=2)
            
            return True
            
        except Exception as e:
            print(f"❌ Error updating account_data.json: {e}")
            return False

    def export_account_data(self, filename: str = None, include_positions: bool = True) -> Dict[str, Any]:
        """
        Export comprehensive account data to JSON file.
        
        Args:
            filename (str, optional): Output filename (default: auto-generated)
            include_positions (bool): Whether to include detailed position data
            
        Returns:
            Dict[str, Any]: Exported account data
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"account_data_{timestamp}.json"
        
        # Get all account data
        accounts = self.get_all_accounts(include_positions=include_positions)
        
        # Process each account
        processed_accounts = []
        for account in accounts:
            processed_account = {
                'metadata': self.extract_account_metadata(account),
                'balances': self.extract_balances(account),
                'summary': self.get_account_summary(account)
            }
            
            if include_positions:
                processed_account['positions'] = self.extract_positions(account)
                
            processed_accounts.append(processed_account)
        
        # Create export structure
        export_data = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_accounts': len(processed_accounts),
                'includes_positions': include_positions
            },
            'accounts': processed_accounts
        }
        
        # Save to file
        import json
        import os
        
        # Ensure the account_data directory exists
        account_dir = "account_data"
        os.makedirs(account_dir, exist_ok=True)
        
        # Create full path
        full_path = os.path.join(account_dir, filename)
        
        with open(full_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        print(f"Saved account data to {full_path}")
        return export_data

    def create_account_data_json(self) -> bool:
        """Create account_data.json file for database insertion matching the account_data table schema"""
        try:
            import json
            
            # Get all account summaries
            summaries = self.get_all_account_summaries()
            
            if not summaries:
                print("❌ No account data available")
                return False
            
            # Create the JSON structure for db_inserter
            account_data = {
                'strategy_name': 'Account_Data',
                'last_updated': datetime.now().isoformat(),
                'total_accounts': len(summaries),
                'account_data': {},
                'metadata': {
                    'data_source': 'schwab_api',
                    'handler_type': 'account_data_handler',
                    'update_frequency': 'on_demand',
                    'database_integration': True,
                    'creation_date': datetime.now().isoformat()
                }
            }
            
            # Process each account summary to match database schema
            for summary in summaries:
                account_number = summary['account_number']
                positions = summary.get('positions', {})
                balances = summary.get('balances', {})
                status = summary.get('status', {})
                risk = summary.get('risk', {})
                
                # Create account data matching the database schema
                account_record = {
                    'account_number': account_number,
                    'total_count': positions.get('total_count', 0),
                    'total_market_value': float(positions.get('total_market_value', 0.0)),
                    'total_unrealized_pl': float(positions.get('total_unrealized_pl', 0.0)),
                    'total_day_pl': float(positions.get('total_day_pl', 0.0)),
                    'equity': float(balances.get('equity', 0.0)),
                    'buying_power': float(balances.get('buying_power', 0.0)),
                    'available_funds': float(balances.get('available_funds', 0.0)),
                    'day_trading_buying_power': float(balances.get('day_trading_buying_power', 0.0)),
                    'stock_buying_power': float(balances.get('stock_buying_power', 0.0)),
                    'option_buying_power': float(balances.get('option_buying_power', 0.0)),
                    'is_day_trader': bool(status.get('is_day_trader', False)),
                    'is_closing_only_restricted': bool(status.get('is_closing_only_restricted', False)),
                    'round_trips': int(status.get('round_trips', 0)),
                    'pfcb_flag': bool(status.get('pfcb_flag', False)),
                    'maintenance_requirement': float(risk.get('maintenance_requirement', 0.0)),
                    'equity_percentage': float(risk.get('equity_percentage', 0.0)),
                    'margin_balance': float(risk.get('margin_balance', 0.0)),
                    'is_in_call': int(risk.get('is_in_call', 0)),
                    'timestamp': datetime.now().isoformat()
                }
                
                account_data['account_data'][account_number] = account_record
            
            # Write to account_data.json in root directory
            with open('account_data.json', 'w') as f:
                json.dump(account_data, f, indent=2)
            
            print(f"✅ Created account_data.json with {len(summaries)} accounts")
            print(f"📊 Accounts processed: {list(account_data['account_data'].keys())}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating account_data.json: {e}")
            return False

    def refresh_and_store_account_data(self) -> Dict[str, Any]:
        """Refresh account data and create JSON for database insertion"""
        try:
            # Get fresh account summaries
            summaries = self.get_all_account_summaries()
            
            if not summaries:
                return {
                    'success': False,
                    'error': 'No account data available',
                    'accounts_processed': 0
                }
            
            # Create account_data.json
            json_created = self.create_account_data_json()
            
            # Also export detailed data to account_data directory
            export_data = self.export_account_data(include_positions=True)
            
            return {
                'success': True,
                'accounts_processed': len(summaries),
                'json_created': json_created,
                'export_created': bool(export_data),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Error refreshing account data: {e}")
            return {
                'success': False,
                'error': str(e),
                'accounts_processed': 0
            }

    def format_currency(self, amount: float) -> str:
        """
        Format currency amount with appropriate sign.
        
        Args:
            amount (float): Amount to format
            
        Returns:
            str: Formatted currency string
        """
        sign = '+' if amount >= 0 else ''
        return f"{sign}${amount:,.2f}"

    def format_percentage(self, percentage: float) -> str:
        """
        Format percentage with appropriate sign.
        
        Args:
            percentage (float): Percentage to format
            
        Returns:
            str: Formatted percentage string
        """
        sign = '+' if percentage >= 0 else ''
        return f"{sign}{percentage:.2f}%"

    def display_account_summary(self, summary: Dict[str, Any]):
        """
        Display account summary in a formatted way.
        
        Args:
            summary (Dict[str, Any]): Account summary data
        """
        print(f"\nAccount Summary: {summary['account_number']}")
        print("=" * 60)
        
        # Positions
        positions = summary['positions']
        print(f"Positions: {positions['total_count']}")
        print(f"Market Value: {self.format_currency(positions['total_market_value'])}")
        print(f"Unrealized P&L: {self.format_currency(positions['total_unrealized_pl'])}")
        print(f"Day P&L: {self.format_currency(positions['total_day_pl'])}")
        
        # Balances
        balances = summary['balances']
        print(f"\nEquity: {self.format_currency(balances['equity'])}")
        print(f"Buying Power: {self.format_currency(balances['buying_power'])}")
        print(f"Available Funds: {self.format_currency(balances['available_funds'])}")
        
        # Status
        status = summary['status']
        print(f"\nDay Trader: {'Yes' if status['is_day_trader'] else 'No'}")
        print(f"Round Trips: {status['round_trips']}")
        print(f"Restricted: {'Yes' if status['is_closing_only_restricted'] else 'No'}")


def main():
    """Example usage of AccountDataHandler"""
    handler = AccountDataHandler()
    
    print("Account Data Handler Test")
    print("=" * 50)
    
    # Get all account summaries
    summaries = handler.get_all_account_summaries()
    
    for summary in summaries:
        handler.display_account_summary(summary)
    
    # Export account data (detailed export)
    export_data = handler.export_account_data(include_positions=True)
    print(f"\nExported data for {len(export_data['accounts'])} accounts")
    
    # Create account_data.json for database insertion
    result = handler.refresh_and_store_account_data()
    if result['success']:
        print(f"✅ Created account_data.json for database insertion")
        print(f"📊 Processed {result['accounts_processed']} accounts")
    else:
        print(f"❌ Failed to create account_data.json: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
