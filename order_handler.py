#!/usr/bin/env python3
"""
Charles Schwab Order Handler for Trading Operations
Supports market orders, limit orders, short and long positions with proper action types.
Makes actual API requests to Charles Schwab using connection_manager.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime
import logging
import requests
import json
import sys
import os
sys.path.append(os.path.dirname(__file__))
import connection_manager

class OrderHandler:
    """
    Charles Schwab order handler for managing different types of trading orders.
    Supports proper action types for EQUITY and OPTIONS:
    
    EQUITY Instructions: BUY, SELL, SELL_SHORT, BUY_TO_COVER
    OPTION Instructions: BUY_TO_OPEN, BUY_TO_CLOSE, SELL_TO_OPEN, SELL_TO_CLOSE
    
    Makes actual API requests to Charles Schwab.
    """
    
    def __init__(self):
        """
        Initialize the order handler with Schwab API integration.
        """
        self.order_history = []
        
        # Get valid tokens and account info using connection manager
        self.tokens = connection_manager.ensure_valid_tokens()
        if not self.tokens:
            raise ValueError("Failed to get valid Schwab API tokens")
        
        self.account_numbers = connection_manager.get_account_numbers(self.tokens['access_token'])
        if not self.account_numbers or len(self.account_numbers) == 0:
            raise ValueError("No account numbers found")
        
        # Use the first account
        self.account_number = self.account_numbers[0]['hashValue']
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"OrderHandler initialized with account: {self.account_number}")

    def _get_auth_headers(self):
        """Get authorization headers for API requests."""
        return {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Accept": "application/json"
        }

    def get_account(self) -> Dict[str, Any]:
        """
        Get account information and balances
        
        Returns:
            Dict containing account information
        """
        tokens = connection_manager.ensure_valid_tokens()
        access_token = tokens["access_token"]
        
        if not self.account_number:
            # Get accounts linked to the user
            accounts_url = "https://api.schwabapi.com/trader/v1/accounts"
            headers = self._get_auth_headers()
            
            response = requests.get(accounts_url, headers=headers)
            
            if response.status_code == 200:
                accounts = response.json()
                if accounts and len(accounts) > 0:
                    # Use the first account's hashValue
                    self.account_number = accounts[0]['hashValue']
                else:
                    print("No accounts found")
                    return {}
            else:
                print(f"Failed to retrieve accounts: {response.status_code}, {response.text}")
                return {}
        
        # Get account details
        account_url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}"
        headers = self._get_auth_headers()
        
        response = requests.get(account_url, headers=headers, 
                              params={"fields": "positions"})
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve account: {response.status_code}, {response.text}")
            return {}
    

    
    def place_market_order(self, action_type: str, symbol: str, shares: int, 
                          current_price: float = None, timestamp: datetime = None) -> Dict:
        """
        Place a market order with proper action types using Schwab API.
        
        Args:
            action_type: Order action ("BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER")
            symbol: Stock symbol
            shares: Number of shares
            current_price: Current market price (optional for market orders)
            timestamp: Order timestamp
            
        Returns:
            Order execution result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type
        valid_actions = ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} market order for {shares} shares of {symbol}")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Create order payload for Schwab API - aligned with API documentation
            order_payload = {
                "orderType": "MARKET",
                "session": "NORMAL",
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "symbol": symbol,
                            "assetType": "EQUITY"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Calculate dollar amount if price provided
                if current_price is not None:
                    dollar_amount = shares * current_price
                    price_info = f" at ${current_price:.2f}"
                else:
                    dollar_amount = None
                    price_info = " at market price"
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'market',
                    'shares': shares,
                    'price': current_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} market order submitted: {shares} shares of {symbol}{price_info}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'shares': shares,
                    'fill_price': current_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error executing {action_type} market order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }
    
    def place_limit_order(self, action_type: str, symbol: str, shares: int,
                         limit_price: float, timestamp: datetime = None) -> Dict:
        """
        Place a limit order with proper action types using Schwab API.
        
        Args:
            action_type: Order action ("BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER")
            symbol: Stock symbol
            shares: Number of shares
            limit_price: Limit price for the order
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type
        valid_actions = ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} limit order for {shares} shares of {symbol} @ ${limit_price:.2f}")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Create order payload for Schwab API - aligned with API documentation
            order_payload = {
                "orderType": "LIMIT",
                "session": "SEAMLESS",  # SEAMLESS for after hours
                "price": str(limit_price),  # Price must be a string in API
                "duration": "GOOD_TILL_CANCEL",  # GTC keeps the order active until filled or cancelled
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "symbol": symbol,
                            "assetType": "EQUITY"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Calculate dollar amount
                dollar_amount = shares * limit_price
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'limit',
                    'shares': shares,
                    'limit_price': limit_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} limit order submitted: {shares} shares of {symbol} at ${limit_price:.2f}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'limit',
                    'shares': shares,
                    'limit_price': limit_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place limit order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing {action_type} limit order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }
    
    def buy_market(self, symbol: str, shares: float = None,
                   timestamp: datetime = None) -> Dict:
        """Convenience method for BUY market orders."""
        return self.place_market_order("BUY", symbol, shares, timestamp)
    
    def sell_market(self, symbol: str, shares: float = None,
                    timestamp: datetime = None) -> Dict:
        """Convenience method for SELL market orders."""
        return self.place_market_order("SELL", symbol, shares,timestamp)
    
    def sell_short_market(self, symbol: str, shares:float = None,
                         timestamp: datetime = None) -> Dict:
        """Convenience method for SELL_SHORT market orders."""
        return self.place_market_order("SELL_SHORT", symbol, shares, timestamp)
    
    def buy_to_cover_market(self, symbol: str, shares: float = None,
                           timestamp: datetime = None) -> Dict:
        """Convenience method for BUY_TO_COVER market orders."""
        return self.place_market_order("BUY_TO_COVER", symbol, shares, timestamp)
    
    def buy_limit(self, symbol: str, shares: int, limit_price: float,
                  timestamp: datetime = None) -> Dict:
        """Convenience method for BUY limit orders."""
        return self.place_limit_order("BUY", symbol, shares, limit_price, timestamp)
    
    def sell_limit(self, symbol: str, shares: int, limit_price: float,
                   timestamp: datetime = None) -> Dict:
        """Convenience method for SELL limit orders."""
        return self.place_limit_order("SELL", symbol, shares, limit_price, timestamp)
    
    def sell_short_limit(self, symbol: str, shares: int, limit_price: float,
                        timestamp: datetime = None) -> Dict:
        """Convenience method for SELL_SHORT limit orders."""
        return self.place_limit_order("SELL_SHORT", symbol, shares, limit_price, timestamp)
    
    def buy_to_cover_limit(self, symbol: str, shares: int, limit_price: float,
                          timestamp: datetime = None) -> Dict:
        """Convenience method for BUY_TO_COVER limit orders."""
        return self.place_limit_order("BUY_TO_COVER", symbol, shares, limit_price, timestamp)
    
    def place_stop_order(self, action_type: str, symbol: str, shares: int,
                        stop_price: float, timestamp: datetime = None) -> Dict:
        """
        Place a stop order using Schwab API.
        
        Args:
            action_type: Order action ("BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER")
            symbol: Stock symbol
            shares: Number of shares
            stop_price: Stop price for the order
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type
        valid_actions = ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} stop order for {shares} shares of {symbol} @ ${stop_price:.2f}")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Create order payload for Schwab API - aligned with API documentation
            order_payload = {
                "orderType": "STOP",
                "session": "NORMAL",
                "stopPrice": str(stop_price),  # Stop price must be a string in API
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "symbol": symbol,
                            "assetType": "EQUITY"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stop',
                    'shares': shares,
                    'stop_price': stop_price,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} stop order submitted: {shares} shares of {symbol} at ${stop_price:.2f}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stop',
                    'shares': shares,
                    'stop_price': stop_price,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place stop order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing {action_type} stop order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }
    
    def place_stop_limit_order(self, action_type: str, symbol: str, shares: int,
                              stop_price: float, limit_price: float, timestamp: datetime = None) -> Dict:
        """
        Place a stop-limit order using Schwab API.
        
        Args:
            action_type: Order action ("BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER")
            symbol: Stock symbol
            shares: Number of shares
            stop_price: Stop price for the order
            limit_price: Limit price for the order
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type
        valid_actions = ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} stop-limit order for {shares} shares of {symbol} stop @ ${stop_price:.2f}, limit @ ${limit_price:.2f}")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Create order payload for Schwab API - aligned with API documentation
            order_payload = {
                "orderType": "STOP_LIMIT",
                "session": "NORMAL",
                "price": str(limit_price),  # Limit price must be a string in API
                "stopPrice": str(stop_price),  # Stop price must be a string in API
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "symbol": symbol,
                            "assetType": "EQUITY"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stop_limit',
                    'shares': shares,
                    'stop_price': stop_price,
                    'limit_price': limit_price,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} stop-limit order submitted: {shares} shares of {symbol} stop @ ${stop_price:.2f}, limit @ ${limit_price:.2f}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stop_limit',
                    'shares': shares,
                    'stop_price': stop_price,
                    'limit_price': limit_price,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place stop-limit order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing {action_type} stop-limit order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }
    
    def place_trailing_stop_order(self, action_type: str, symbol: str, shares: int,
                                 stop_price_offset: float, timestamp: datetime = None) -> Dict:
        """
        Place a trailing stop order using Schwab API.
        
        Args:
            action_type: Order action ("BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER")
            symbol: Stock symbol
            shares: Number of shares
            stop_price_offset: Dollar amount for trailing stop offset
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type
        valid_actions = ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} trailing stop order for {shares} shares of {symbol} with ${stop_price_offset:.2f} offset")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Create order payload for Schwab API - aligned with API documentation
            order_payload = {
                "orderType": "TRAILING_STOP",
                "session": "NORMAL",
                "stopPriceLinkBasis": "BID",
                "stopPriceLinkType": "VALUE",
                "stopPriceOffset": stop_price_offset,
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "symbol": symbol,
                            "assetType": "EQUITY"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'trailing_stop',
                    'shares': shares,
                    'stop_price_offset': stop_price_offset,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} trailing stop order submitted: {shares} shares of {symbol} with ${stop_price_offset:.2f} offset, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'trailing_stop',
                    'shares': shares,
                    'stop_price_offset': stop_price_offset,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place trailing stop order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing {action_type} trailing stop order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }
    
    def get_order_history_df(self) -> pd.DataFrame:
        """
        Get order history as a pandas DataFrame.
        
        Returns:
            DataFrame with order history
        """
        if not self.order_history:
            return pd.DataFrame()
        
        return pd.DataFrame(self.order_history)
    
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Get the status of a specific order
        
        Parameters:
            order_id: The ID of the order to check
            
        Returns:
            Dictionary containing order status information
        """
        if not self.account_number:
            return {"error": "No account number available"}
        
        url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders/{order_id}"
        headers = self._get_auth_headers()
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                error_message = f"Failed to get order status: {response.status_code}, {response.text}"
                self.logger.error(error_message)
                return {"error": error_message}
        except Exception as e:
            error_message = f"Error getting order status: {str(e)}"
            self.logger.error(error_message)
            return {"error": error_message}
    
    def get_all_orders(self, from_entered_time: str = None, to_entered_time: str = None, 
                      max_results: int = 3000, status: str = None) -> Dict[str, Any]:
        """
        Get all orders for the account with optional filtering
        
        Parameters:
            from_entered_time: Start date in ISO format (e.g., "2024-01-01T00:00:00.000Z")
            to_entered_time: End date in ISO format (e.g., "2024-12-31T23:59:59.999Z")
            max_results: Maximum number of orders to return (default 3000)
            status: Filter by order status (AWAITING_PARENT_ORDER, AWAITING_CONDITION, 
                   AWAITING_STOP_CONDITION, AWAITING_MANUAL_REVIEW, ACCEPTED, AWAITING_UR_OUT, 
                   PENDING_ACTIVATION, QUEUED, WORKING, REJECTED, PENDING_CANCEL, CANCELED, 
                   PENDING_REPLACE, REPLACED, FILLED, EXPIRED, NEW, AWAITING_RELEASE_TIME, 
                   AWAITING_ACCOUNT_OPENING, AWAITING_FIRST_FILL)
            
        Returns:
            Dictionary containing orders information
        """
        if not self.account_number:
            return {"error": "No account number available"}
        
        url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
        headers = self._get_auth_headers()
        
        params = {"maxResults": max_results}
        if from_entered_time:
            params["fromEnteredTime"] = from_entered_time
        if to_entered_time:
            params["toEnteredTime"] = to_entered_time
        if status:
            params["status"] = status
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                error_message = f"Failed to get orders: {response.status_code}, {response.text}"
                self.logger.error(error_message)
                return {"error": error_message}
        except Exception as e:
            error_message = f"Error getting orders: {str(e)}"
            self.logger.error(error_message)
            return {"error": error_message}
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order
        
        Parameters:
            order_id: The ID of the order to cancel
            
        Returns:
            Dictionary containing result of cancellation
        """
        if not self.account_number:
            return {"error": "No account number available"}
        
        url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders/{order_id}"
        headers = self._get_auth_headers()
        
        try:
            response = requests.delete(url, headers=headers)
            
            if response.status_code == 200:
                return {"status": "SUCCESS", "message": "Order cancelled successfully"}
            else:
                error_message = f"Failed to cancel order: {response.status_code}, {response.text}"
                self.logger.error(error_message)
                return {"error": error_message}
        except Exception as e:
            error_message = f"Error cancelling order: {str(e)}"
            self.logger.error(error_message)
            return {"error": error_message}
    
    def replace_order(self, order_id: str, new_order_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace an existing order with a new order
        
        Parameters:
            order_id: The ID of the order to replace
            new_order_payload: The new order payload following Schwab API format
            
        Returns:
            Dictionary containing result of order replacement
        """
        if not self.account_number:
            return {"error": "No account number available"}
        
        url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders/{order_id}"
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            response = requests.put(url, json=new_order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                return {"status": "SUCCESS", "message": "Order replaced successfully"}
            else:
                error_message = f"Failed to replace order: {response.status_code}, {response.text}"
                self.logger.error(error_message)
                return {"error": error_message}
        except Exception as e:
            error_message = f"Error replacing order: {str(e)}"
            self.logger.error(error_message)
            return {"error": error_message}


    # ==================== OPTION ORDER METHODS ====================
    
    def place_option_limit_order(self, action_type: str, option_symbol: str, contracts: int,
                                limit_price: float, timestamp: datetime = None) -> Dict:
        """
        Place a limit order for options using Schwab API.
        
        Args:
            action_type: Option action ("BUY_TO_OPEN", "SELL_TO_CLOSE", "SELL_TO_OPEN", "BUY_TO_CLOSE")
            option_symbol: Full option symbol (e.g., "AAPL  251017C00250000")
            contracts: Number of option contracts
            limit_price: Limit price per contract
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type for options
        valid_actions = ["BUY_TO_OPEN", "SELL_TO_CLOSE", "SELL_TO_OPEN", "BUY_TO_CLOSE"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid option action type: {action_type}. Must be one of {valid_actions}',
                'timestamp': timestamp
            }
        
        self.logger.info(f"Attempting to place {action_type} option limit order for {contracts} contracts of {option_symbol} @ ${limit_price:.2f}")
        
        try:
            if contracts <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid contract quantity',
                    'timestamp': timestamp
                }
            
            # Create option order payload for Schwab API - following the exact format from examples
            order_payload = {
                "complexOrderStrategyType": "NONE",
                "orderType": "LIMIT",
                "session": "NORMAL",
                "price": str(limit_price),  # Price must be a string in API
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": contracts,
                        "instrument": {
                            "symbol": option_symbol,
                            "assetType": "OPTION"
                        }
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Calculate dollar amount (options are per contract * 100)
                dollar_amount = contracts * limit_price * 100
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': option_symbol,
                    'action_type': action_type,
                    'order_type': 'option_limit',
                    'contracts': contracts,
                    'limit_price': limit_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"{action_type} option limit order submitted: {contracts} contracts of {option_symbol} at ${limit_price:.2f}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': option_symbol,
                    'action_type': action_type,
                    'order_type': 'option_limit',
                    'contracts': contracts,
                    'limit_price': limit_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place option limit order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing {action_type} option limit order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }

    def place_option_spread_order(self, legs: List[Dict], net_price: float, order_type: str = "NET_DEBIT", 
                                 timestamp: datetime = None) -> Dict:
        """
        Place a multi-leg option spread order using Schwab API.
        
        Args:
            legs: List of option legs, each containing:
                  - action_type: "BUY_TO_OPEN", "SELL_TO_CLOSE", "SELL_TO_OPEN", "BUY_TO_CLOSE"
                  - option_symbol: Full option symbol
                  - contracts: Number of contracts
            net_price: Net price for the spread (debit/credit)
            order_type: "NET_DEBIT" or "NET_CREDIT"
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        self.logger.info(f"Attempting to place option spread order with {len(legs)} legs at net ${net_price:.2f}")
        
        try:
            if not legs or len(legs) < 2:
                return {
                    'status': 'rejected',
                    'reason': 'Spread orders require at least 2 legs',
                    'timestamp': timestamp
                }
            
            # Validate all legs
            valid_actions = ["BUY_TO_OPEN", "SELL_TO_CLOSE", "SELL_TO_OPEN", "BUY_TO_CLOSE"]
            order_leg_collection = []
            
            for leg in legs:
                if leg['action_type'] not in valid_actions:
                    return {
                        'status': 'rejected',
                        'reason': f'Invalid option action type: {leg["action_type"]}. Must be one of {valid_actions}',
                        'timestamp': timestamp
                    }
                
                order_leg_collection.append({
                    "instruction": leg['action_type'],
                    "quantity": leg['contracts'],
                    "instrument": {
                        "symbol": leg['option_symbol'],
                        "assetType": "OPTION"
                    }
                })
            
            # Create spread order payload for Schwab API - following the vertical spread example
            order_payload = {
                "orderType": order_type,
                "session": "NORMAL",
                "price": str(net_price),  # Price must be a string in API
                "duration": "DAY",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": order_leg_collection
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Calculate total dollar amount
                total_contracts = sum(leg['contracts'] for leg in legs)
                dollar_amount = total_contracts * net_price * 100
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': f"SPREAD_{len(legs)}_LEGS",
                    'action_type': 'SPREAD',
                    'order_type': 'option_spread',
                    'legs': legs,
                    'net_price': net_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"Option spread order submitted: {len(legs)} legs at net ${net_price:.2f}, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': f"SPREAD_{len(legs)}_LEGS",
                    'action_type': 'SPREAD',
                    'order_type': 'option_spread',
                    'legs': legs,
                    'net_price': net_price,
                    'dollar_amount': dollar_amount,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place option spread order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing option spread order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }

    # Convenience methods for option orders
    def buy_to_open_option_limit(self, option_symbol: str, contracts: int, limit_price: float,
                                timestamp: datetime = None) -> Dict:
        """Convenience method for BUY_TO_OPEN option limit orders."""
        return self.place_option_limit_order("BUY_TO_OPEN", option_symbol, contracts, limit_price, timestamp)
    
    def sell_to_close_option_limit(self, option_symbol: str, contracts: int, limit_price: float,
                                  timestamp: datetime = None) -> Dict:
        """Convenience method for SELL_TO_CLOSE option limit orders."""
        return self.place_option_limit_order("SELL_TO_CLOSE", option_symbol, contracts, limit_price, timestamp)
    
    def sell_to_open_option_limit(self, option_symbol: str, contracts: int, limit_price: float,
                                 timestamp: datetime = None) -> Dict:
        """Convenience method for SELL_TO_OPEN option limit orders."""
        return self.place_option_limit_order("SELL_TO_OPEN", option_symbol, contracts, limit_price, timestamp)
    
    def buy_to_close_option_limit(self, option_symbol: str, contracts: int, limit_price: float,
                                 timestamp: datetime = None) -> Dict:
        """Convenience method for BUY_TO_CLOSE option limit orders."""
        return self.place_option_limit_order("BUY_TO_CLOSE", option_symbol, contracts, limit_price, timestamp)

    def create_option_symbol(self, underlying: str, expiration_date: str, option_type: str, strike_price: float) -> str:
        """
        Create a properly formatted option symbol for Schwab API.
        
        Args:
            underlying: Underlying stock symbol (e.g., "AAPL")
            expiration_date: Expiration date in YYYY-MM-DD format (e.g., "2025-10-17")
            option_type: "C" for Call or "P" for Put
            strike_price: Strike price (e.g., 250.0)
            
        Returns:
            Formatted option symbol (e.g., "AAPL  251017C00250000")
        """
        try:
            # Parse the expiration date
            exp_date = datetime.strptime(expiration_date, '%Y-%m-%d')
            
            # Format date as YYMMDD
            date_str = exp_date.strftime('%y%m%d')
            
            # Format strike price as 8-digit string with 3 decimal places
            # Strike price is multiplied by 1000 and zero-padded to 8 digits
            strike_str = f"{int(strike_price * 1000):08d}"
            
            # Create the option symbol: UNDERLYING  YYMMDD[C/P]XXXXXXXX
            # Note: Schwab uses 2 spaces between underlying and date for symbols under 6 chars
            spaces = "  " if len(underlying) <= 4 else " "
            option_symbol = f"{underlying}{spaces}{date_str}{option_type.upper()}{strike_str}"
            
            return option_symbol
            
        except Exception as e:
            self.logger.error(f"Error creating option symbol: {str(e)}")
            return None

    def place_complex_option_order(self, order_data: Dict[str, Any], timestamp: datetime = None) -> Dict:
        """
        Place a complex option order (spreads, Iron Condors, etc.) using Schwab API.
        
        Args:
            order_data: Complete order payload in Schwab API format
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        self.logger.info(f"Attempting to place complex option order with {len(order_data.get('orderLegCollection', []))} legs")
        
        try:
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_data, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': 'COMPLEX_OPTION',
                    'action_type': 'COMPLEX',
                    'order_type': 'complex_option',
                    'order_data': order_data,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"Complex option order submitted successfully, Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'order_type': 'complex_option',
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place complex option order: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing complex option order: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }

    # ==================== STOCK OCO ORDER METHODS ====================
    
    def place_stock_oco_order_with_targets(self, action_type: str, symbol: str, shares: int, 
                                         entry_price: float, profit_target: float, stop_loss: float,
                                         timestamp: datetime = None) -> Dict:
        """
        Place a "One Triggers A One Cancels Another" order for STOCKS with automatic profit targets and stop losses.
        
        This is the core method for implementing automated risk management on stock trades.
        It creates a complex order structure that:
        1. Places an entry order (BUY or SELL_SHORT) at the specified limit price
        2. Once the entry order fills, automatically submits TWO exit orders:
           - A profit target LIMIT order (to capture gains)
           - A stop loss STOP order (to limit losses)
        3. If either exit order fills, the other is automatically cancelled (OCO = One Cancels Other)
        
        This eliminates the need for manual order management and ensures every trade has
        both profit protection and loss protection from the moment of entry.
        
        Args:
            action_type: Entry action ("BUY" for long positions, "SELL_SHORT" for short positions)
            symbol: Stock symbol (e.g., "AAPL", "TSLA")
            shares: Number of shares to trade
            entry_price: Entry limit price - the price at which we want to enter the position
            profit_target: Profit target limit price - where we want to take profits
            stop_loss: Stop loss price - where we want to cut losses
            timestamp: Order timestamp (defaults to current time)
            
        Returns:
            Dict containing order placement result with status, order_id, and trade details
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Validate action type - only BUY and SELL_SHORT supported for OCO with targets
        valid_actions = ["BUY", "SELL_SHORT"]
        if action_type not in valid_actions:
            return {
                'status': 'rejected',
                'reason': f'Invalid action type for stock OCO order: {action_type}. Must be BUY or SELL_SHORT',
                'timestamp': timestamp
            }
        
        # Determine exit actions based on entry action
        if action_type == "BUY":
            exit_action = "SELL"
        else:  # SELL_SHORT
            exit_action = "BUY_TO_COVER"
        
        self.logger.info(f"Attempting to place STOCK OCO order: {action_type} {shares} shares of {symbol} @ ${entry_price:.2f}")
        self.logger.info(f"  Profit target: {exit_action} @ ${profit_target:.2f}")
        self.logger.info(f"  Stop loss: {exit_action} @ ${stop_loss:.2f}")
        
        try:
            if shares <= 0:
                return {
                    'status': 'rejected',
                    'reason': 'Invalid share quantity',
                    'timestamp': timestamp
                }
            
            # Validate price relationships
            if action_type == "BUY":
                # For BUY orders: profit_target > entry_price > stop_loss
                if not (profit_target > entry_price > stop_loss):
                    return {
                        'status': 'rejected',
                        'reason': f'Invalid price relationship for BUY: profit_target ({profit_target}) > entry_price ({entry_price}) > stop_loss ({stop_loss})',
                        'timestamp': timestamp
                    }
            else:  # SELL_SHORT
                # For SELL_SHORT orders: stop_loss > entry_price > profit_target
                if not (stop_loss > entry_price > profit_target):
                    return {
                        'status': 'rejected',
                        'reason': f'Invalid price relationship for SELL_SHORT: stop_loss ({stop_loss}) > entry_price ({entry_price}) > profit_target ({profit_target})',
                        'timestamp': timestamp
                    }
            
            # Create the 1st Trigger OCO order payload using Schwab API format for STOCKS
            order_payload = {
                "orderStrategyType": "TRIGGER",
                "session": "NORMAL",
                "duration": "DAY",
                "orderType": "LIMIT",
                "price": entry_price,
                "orderLegCollection": [
                    {
                        "instruction": action_type,
                        "quantity": shares,
                        "instrument": {
                            "assetType": "EQUITY",
                            "symbol": symbol
                        }
                    }
                ],
                "childOrderStrategies": [
                    {
                        "orderStrategyType": "OCO",
                        "childOrderStrategies": [
                            {
                                "orderStrategyType": "SINGLE",
                                "session": "NORMAL",
                                "duration": "GOOD_TILL_CANCEL",
                                "orderType": "LIMIT",
                                "price": profit_target,
                                "orderLegCollection": [
                                    {
                                        "instruction": exit_action,
                                        "quantity": shares,
                                        "instrument": {
                                            "assetType": "EQUITY",
                                            "symbol": symbol
                                        }
                                    }
                                ]
                            },
                            {
                                "orderStrategyType": "SINGLE",
                                "session": "NORMAL",
                                "duration": "GOOD_TILL_CANCEL",
                                "orderType": "STOP",
                                "stopPrice": stop_loss,
                                "orderLegCollection": [
                                    {
                                        "instruction": exit_action,
                                        "quantity": shares,
                                        "instrument": {
                                            "assetType": "EQUITY",
                                            "symbol": symbol
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            
            # Make API request to Schwab
            url = f"https://api.schwabapi.com/trader/v1/accounts/{self.account_number}/orders"
            headers = {
                "Authorization": f"Bearer {self.tokens['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, json=order_payload, headers=headers)
            
            if response.status_code in [200, 201]:
                order_id = response.headers.get('Location', '').split('/')[-1]
                
                # Calculate potential profit and risk
                if action_type == "BUY":
                    potential_profit = (profit_target - entry_price) * shares
                    potential_loss = (entry_price - stop_loss) * shares
                else:  # SELL_SHORT
                    potential_profit = (entry_price - profit_target) * shares
                    potential_loss = (stop_loss - entry_price) * shares
                
                # Record successful order
                order_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stock_oco_with_targets',
                    'shares': shares,
                    'entry_price': entry_price,
                    'profit_target': profit_target,
                    'stop_loss': stop_loss,
                    'potential_profit': potential_profit,
                    'potential_loss': potential_loss,
                    'order_id': order_id,
                    'status': 'submitted'
                }
                
                self.order_history.append(order_record)
                
                self.logger.info(f"STOCK OCO order with targets submitted successfully:")
                self.logger.info(f"  Entry: {action_type} {shares} shares of {symbol} @ ${entry_price:.2f}")
                self.logger.info(f"  Profit Target: {exit_action} @ ${profit_target:.2f} (${potential_profit:.2f} profit)")
                self.logger.info(f"  Stop Loss: {exit_action} @ ${stop_loss:.2f} (${potential_loss:.2f} loss)")
                self.logger.info(f"  Order ID: {order_id}")
                
                return {
                    'status': 'submitted',
                    'symbol': symbol,
                    'action_type': action_type,
                    'order_type': 'stock_oco_with_targets',
                    'shares': shares,
                    'entry_price': entry_price,
                    'profit_target': profit_target,
                    'stop_loss': stop_loss,
                    'exit_action': exit_action,
                    'potential_profit': potential_profit,
                    'potential_loss': potential_loss,
                    'risk_reward_ratio': potential_profit / potential_loss if potential_loss > 0 else 0,
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                error_msg = f"Failed to place STOCK OCO order with targets: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return {
                    'status': 'rejected',
                    'reason': error_msg,
                    'timestamp': timestamp
                }
            
        except Exception as e:
            self.logger.error(f"Error placing STOCK OCO order with targets: {str(e)}")
            return {
                'status': 'error',
                'reason': str(e),
                'timestamp': timestamp
            }

    # Convenience methods for STOCK OCO orders with targets
    def buy_stock_with_targets(self, symbol: str, shares: int, entry_price: float, 
                              profit_target: float, stop_loss: float, timestamp: datetime = None) -> Dict:
        """
        Convenience method for BUY STOCK orders with automatic profit targets and stop losses.
        
        Args:
            symbol: Stock symbol
            shares: Number of shares
            entry_price: Entry limit price
            profit_target: Profit target price (must be > entry_price)
            stop_loss: Stop loss price (must be < entry_price)
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        return self.place_stock_oco_order_with_targets("BUY", symbol, shares, entry_price, 
                                                     profit_target, stop_loss, timestamp)

    def sell_short_stock_with_targets(self, symbol: str, shares: int, entry_price: float, 
                                     profit_target: float, stop_loss: float, timestamp: datetime = None) -> Dict:
        """
        Convenience method for SELL_SHORT STOCK orders with automatic profit targets and stop losses.
        
        Args:
            symbol: Stock symbol
            shares: Number of shares
            entry_price: Entry limit price
            profit_target: Profit target price (must be < entry_price)
            stop_loss: Stop loss price (must be > entry_price)
            timestamp: Order timestamp
            
        Returns:
            Order placement result
        """
        return self.place_stock_oco_order_with_targets("SELL_SHORT", symbol, shares, entry_price, 
                                                     profit_target, stop_loss, timestamp)


def main():
    """Command-line interface for OrderHandler."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Execute trading orders using OrderHandler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # BUY market order for 100 shares of AAPL at current price $150
  python3 handlers/order_handler.py AAPL BUY market --shares 100 
  
  # SELL_SHORT market order for 50 shares of TSLA at current price $200
  python3 handlers/order_handler.py TSLA SELL_SHORT market --shares 50 
  
  # uSELL limit order for 25 shares at limit price $155
  python3 handlers/order_handler.py AAPL SELL limit --shares 25 --price 155.0
  
  # BUY_TO_COVER market order for 75 shares at current price $195
  python3 handlers/order_handler.py TSLA BUY_TO_COVER market --shares 75 
        """
    )
    
    parser.add_argument('symbol', help='Stock symbol (e.g., AAPL, TSLA)')
    parser.add_argument('action', choices=['BUY', 'SELL', 'SELL_SHORT', 'BUY_TO_COVER'], 
                       help='Order action type')
    parser.add_argument('order_type', choices=['market', 'limit'], help='Order type')
    parser.add_argument('--shares', type=int, required=True, help='Number of shares')
    parser.add_argument('--price', type=float, help='Market price or limit price (required for limit orders)')
    args = parser.parse_args()
    
    # Validate price requirement for limit orders
    if args.order_type == 'limit' and args.price is None:
        print("Error: --price is required for limit orders")
        return
    
    # Create order handler
    handler = OrderHandler()
    
    print("OrderHandler initialized")
    
    # Execute order based on parameters
    if args.order_type == 'market':
        result = handler.place_market_order(
            args.action, args.symbol, args.shares, args.price
        )
    else:  # limit
        result = handler.place_limit_order(
            args.action, args.symbol, args.shares, args.price
        )
    print(f"Order Result:")
    for key, value in result.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
