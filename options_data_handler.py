import os
import requests
import time
import json
import psycopg2
import psycopg2.extras
from connection_manager import ensure_valid_tokens
from datetime import datetime, timedelta
from config_loader import get_config

class OptionsDataHandler:
    def __init__(self):
        """
        Initialize the OptionsDataHandler.
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
        else:
            # Fallback defaults if config is not available
            self.max_retries = 5
            self.retry_delay = 2
            self.rate_limit_delay = 60
            self.base_url = 'https://api.schwabapi.com'
        

    def get_options_chain(self, symbol, contractType=None, strikeCount=None, includeQuotes=None, 
                         strategy=None, interval=None, strike=None, range_param=None, 
                         fromDate=None, toDate=None, volatility=None, underlyingPrice=None, 
                         interestRate=None, daysToExpiration=None, expMonth=None, optionType=None):
        """
        Retrieve options chain data from the Schwab API with automatic retry logic.

        Args:
            symbol (str): The underlying symbol for the options chain (e.g., 'AAPL', 'MSFT')
            contractType (str, optional): Type of contracts. Valid values: 'CALL', 'PUT', 'ALL' (default: 'ALL')
            strikeCount (int, optional): Number of strikes to return above and below the at-the-money price
            includeQuotes (bool, optional): Include quotes for options in the option chain
            strategy (str, optional): Passing a value returns a Strategy Chain. Valid values: 
                'SINGLE', 'ANALYTICAL', 'COVERED', 'VERTICAL', 'CALENDAR', 'STRANGLE', 
                'STRADDLE', 'BUTTERFLY', 'CONDOR', 'DIAGONAL', 'COLLAR', 'ROLL'
            interval (float, optional): Strike interval for spread strategy chains
            strike (float, optional): Provide a strike price to return options only at that strike price
            range_param (str, optional): Returns options for the given range. Valid values:
                'ITM' (In-the-money), 'NTM' (Near-the-money), 'OTM' (Out-of-the-money), 
                'SAK' (Strikes Above Market), 'SBK' (Strikes Below Market), 'SNK' (Strikes Near Market), 'ALL' (All Strikes)
            fromDate (str, optional): Only return expirations after this date (ISO-8601 format: YYYY-MM-DD)
            toDate (str, optional): Only return expirations before this date (ISO-8601 format: YYYY-MM-DD)
            volatility (float, optional): Volatility to use in calculations
            underlyingPrice (float, optional): Underlying price to use in calculations
            interestRate (float, optional): Interest rate to use in calculations
            daysToExpiration (int, optional): Days to expiration to use in calculations
            expMonth (str, optional): Return only options expiring in the specified month (ALL, JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC)
            optionType (str, optional): Type of contracts. Valid values: 'S' (Standard contracts), 'NS' (Non-standard contracts), 'ALL' (All contracts)

        Returns:
            dict: Options chain data with calls, puts, and underlying information
            None: If no data is available or request fails
        """
        retry_delay = self.retry_delay
        max_retries = self.max_retries  # Store in local variable to avoid any scoping issues

        for attempt in range(max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]

                # Prepare headers
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }

                # Build URL with parameters
                url = f"{self.base_url}/marketdata/v1/chains?symbol={symbol}"
                
                # Add optional parameters
                params = {}
                if contractType is not None:
                    params['contractType'] = contractType
                if strikeCount is not None:
                    params['strikeCount'] = strikeCount
                if includeQuotes is not None:
                    params['includeQuotes'] = str(includeQuotes).lower()
                if strategy is not None:
                    params['strategy'] = strategy
                if interval is not None:
                    params['interval'] = interval
                if strike is not None:
                    params['strike'] = strike
                if range_param is not None:
                    params['range'] = range_param
                if fromDate is not None:
                    params['fromDate'] = fromDate
                if toDate is not None:
                    params['toDate'] = toDate
                if volatility is not None:
                    params['volatility'] = volatility
                if underlyingPrice is not None:
                    params['underlyingPrice'] = underlyingPrice
                if interestRate is not None:
                    params['interestRate'] = interestRate
                if daysToExpiration is not None:
                    params['daysToExpiration'] = daysToExpiration
                if expMonth is not None:
                    params['expMonth'] = expMonth
                if optionType is not None:
                    params['optionType'] = optionType

                # Make API request
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()

                data = response.json()

                # Return data if available
                if data and ('callExpDateMap' in data or 'putExpDateMap' in data):
                    return data
                else:
                    print(f"No options data available for {symbol}")
                    return None

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
                        return None

            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return None

            except Exception as e:
                print(f"Unexpected error fetching options data: {e}")
                return None

        return None

    def get_quote(self, symbol):
        """
        Get current quote data for a symbol.

        Args:
            symbol (str): The symbol to get quote for (e.g., 'AAPL', 'MSFT')

        Returns:
            dict: Quote data with current price and other market information
            None: If no data is available or request fails
        """
        retry_delay = self.retry_delay

        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]

                # Prepare headers
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }

                # Build URL
                url = f"{self.base_url}/marketdata/v1/{symbol}/quotes"

                # Make API request
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                data = response.json()

                # Return quote data if available
                if data and symbol in data:
                    return data[symbol]
                else:
                    print(f"No quote data available for {symbol}")
                    return None

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
                        return None

            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return None

            except Exception as e:
                print(f"Unexpected error fetching quote data: {e}")
                return None

        return None

    def get_option_expirations(self, symbol):
        """
        Get option expiration dates for a symbol without individual contract details.

        Args:
            symbol (str): The underlying symbol (e.g., 'AAPL', 'MSFT')

        Returns:
            dict: Expiration information with expirationList containing dates and metadata
            None: If no data is available or request fails
        """
        retry_delay = self.retry_delay
        max_retries = self.max_retries

        for attempt in range(max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]

                # Prepare headers
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }

                # Build URL for expiration endpoint
                url = f"{self.base_url}/marketdata/v1/expirationchain?symbol={symbol}"

                # Make API request
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                data = response.json()

                # Return data if available
                if data and 'expirationList' in data:
                    return data
                else:
                    print(f"No expiration data available for {symbol}")
                    return None

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
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print("Maximum retry attempts reached")
                        return None

            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Maximum retry attempts reached")
                    return None

            except Exception as e:
                print(f"Unexpected error fetching expiration data: {e}")
                return None

        return None

    def format_options_chain(self, options_data, max_strikes=10):
        """
        Format options chain data for easy reading.

        Args:
            options_data (dict): Raw options chain data from API
            max_strikes (int): Maximum number of strikes to display per expiration

        Returns:
            dict: Formatted options data with readable structure
        """
        if not options_data:
            return None

        formatted_data = {
            'symbol': options_data.get('symbol', 'UNKNOWN'),
            'underlying_price': options_data.get('underlyingPrice'),
            'underlying': options_data.get('underlying', {}),
            'expirations': []
        }

        call_exp_map = options_data.get('callExpDateMap', {})
        put_exp_map = options_data.get('putExpDateMap', {})

        # Get all unique expiration dates
        all_expirations = set()
        for exp_date in call_exp_map.keys():
            all_expirations.add(exp_date)
        
        for exp_date in put_exp_map.keys():
            all_expirations.add(exp_date)

        # Process each expiration
        for exp_date in sorted(all_expirations):
            exp_data = {
                'expiration_date': exp_date,
                'days_to_expiration': self._calculate_days_to_expiration(exp_date),
                'strikes': []
            }

            # Collect strikes for this expiration
            call_strikes = {}
            put_strikes = {}

            # Get calls for this expiration
            if exp_date in call_exp_map:
                for strike_key, call_data in call_exp_map[exp_date].items():
                    strike = float(strike_key)
                    call_strikes[strike] = call_data

            # Get puts for this expiration
            if exp_date in put_exp_map:
                for strike_key, put_data in put_exp_map[exp_date].items():
                    strike = float(strike_key)
                    put_strikes[strike] = put_data

            # Get sorted strikes and limit to max_strikes
            all_strikes = sorted(set(list(call_strikes.keys()) + list(put_strikes.keys())))
            
            # If we have an underlying price, focus on strikes near it
            underlying_price = formatted_data['underlying_price']
            if underlying_price and len(all_strikes) > max_strikes:
                closest_strikes = sorted(all_strikes, key=lambda x: abs(x - underlying_price))[:max_strikes]
                all_strikes = sorted(closest_strikes)

            # Format strike data
            for strike in all_strikes:
                call_data = call_strikes.get(strike, {})
                put_data = put_strikes.get(strike, {})

                strike_info = {
                    'strike': strike,
                    'call': {
                        'bid': call_data.get('bidPrice'),
                        'ask': call_data.get('askPrice'),
                        'last': call_data.get('lastPrice'),
                        'mark': call_data.get('markPrice'),
                        'volume': call_data.get('totalVolume'),
                        'open_interest': call_data.get('openInterest'),
                        'implied_volatility': call_data.get('volatility'),
                        'delta': call_data.get('delta'),
                        'gamma': call_data.get('gamma'),
                        'theta': call_data.get('theta'),
                        'vega': call_data.get('vega'),
                        'rho': call_data.get('rho'),
                        'in_the_money': call_data.get('isInTheMoney'),
                        'strike_price': call_data.get('strikePrice'),
                        'expiration_date': call_data.get('expirationDate'),
                        'days_to_expiration': call_data.get('daysToExpiration')
                    } if call_data else None,
                    'put': {
                        'bid': put_data.get('bidPrice'),
                        'ask': put_data.get('askPrice'),
                        'last': put_data.get('lastPrice'),
                        'mark': put_data.get('markPrice'),
                        'volume': put_data.get('totalVolume'),
                        'open_interest': put_data.get('openInterest'),
                        'implied_volatility': put_data.get('volatility'),
                        'delta': put_data.get('delta'),
                        'gamma': put_data.get('gamma'),
                        'theta': put_data.get('theta'),
                        'vega': put_data.get('vega'),
                        'rho': put_data.get('rho'),
                        'in_the_money': put_data.get('isInTheMoney'),
                        'strike_price': put_data.get('strikePrice'),
                        'expiration_date': put_data.get('expirationDate'),
                        'days_to_expiration': put_data.get('daysToExpiration')
                    } if put_data else None
                }

                exp_data['strikes'].append(strike_info)

            formatted_data['expirations'].append(exp_data)

        return formatted_data

    def _calculate_days_to_expiration(self, exp_date_str):
        """Calculate days to expiration from date string."""
        try:
            # Handle format like '2025-10-10:1' by splitting on ':'
            date_part = exp_date_str.split(':')[0]
            exp_date = datetime.strptime(date_part, '%Y-%m-%d')
            today = datetime.now()
            return (exp_date - today).days
        except (ValueError, IndexError):
            return None

    def get_all_options_data(self, symbols, max_dte=60, max_strikes_per_exp=20):
        """
        Get comprehensive options data for multiple symbols.
        
        Args:
            symbols (list): List of symbols to fetch options for
            max_dte (int): Maximum days to expiration to include
            max_strikes_per_exp (int): Maximum strikes per expiration
            
        Returns:
            dict: Complete options data for all symbols
        """
        print(f"🔍 Fetching options data for {len(symbols)} symbols: {symbols}")
        
        all_options_data = {
            'timestamp': datetime.now().isoformat(),
            'symbols_analyzed': len(symbols),
            'symbols': {},
            'metadata': {
                'max_dte': max_dte,
                'max_strikes_per_exp': max_strikes_per_exp,
                'data_source': 'schwab_api',
                'update_frequency': '10_seconds'
            }
        }
        
        for symbol in symbols:
            print(f"📊 Processing {symbol}...")
            symbol_data = self._get_symbol_options_data(symbol, max_dte, max_strikes_per_exp)
            if symbol_data:
                all_options_data['symbols'][symbol] = symbol_data
                print(f"✅ {symbol}: {len(symbol_data.get('expirations', []))} expirations")
            else:
                print(f"❌ Failed to get data for {symbol}")
                
        return all_options_data

    def _get_symbol_options_data(self, symbol, max_dte, max_strikes_per_exp):
        """Get comprehensive options data for a single symbol."""
        # Get options chain first (more reliable than quote after hours)
        options_data = self.get_options_chain(
            symbol=symbol,
            contractType='ALL',
            strikeCount=50,
            includeQuotes=True
        )
        
        if not options_data:
            return None
            
        # Get underlying price from options chain (more reliable)
        underlying_price = options_data.get('underlyingPrice')
        if not underlying_price:
            return None
            
        # Get current quote (may be None after hours)
        quote_data = self.get_quote(symbol)
        if not quote_data:
            # Create fallback quote data using options chain info
            quote_data = {
                'lastPrice': underlying_price,
                'mark': underlying_price,
                'bidPrice': None,
                'askPrice': None,
                'totalVolume': 0,
                'netChange': 0,
                'netPercentChangeInDouble': 0
            }
            
        # Process and structure the data
        symbol_options = {
            'symbol': symbol,
            'underlying_price': underlying_price,
            'underlying_data': {
                'last_price': quote_data.get('lastPrice'),
                'bid': quote_data.get('bidPrice'),
                'ask': quote_data.get('askPrice'),
                'mark': quote_data.get('mark'),
                'volume': quote_data.get('totalVolume'),
                'change': quote_data.get('netChange'),
                'change_percent': quote_data.get('netPercentChangeInDouble'),
                '52_week_high': quote_data.get('52WkHigh'),
                '52_week_low': quote_data.get('52WkLow'),
                'market_cap': quote_data.get('marketCap'),
                'pe_ratio': quote_data.get('peRatio'),
                'dividend_yield': quote_data.get('divYield')
            },
            'expirations': [],
            'total_contracts': 0,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        call_exp_map = options_data.get('callExpDateMap', {})
        put_exp_map = options_data.get('putExpDateMap', {})
        
        # Get all unique expiration dates
        all_expirations = set()
        for exp_date in call_exp_map.keys():
            all_expirations.add(exp_date)
        for exp_date in put_exp_map.keys():
            all_expirations.add(exp_date)
            
        # Process each expiration
        for exp_date in sorted(all_expirations):
            dte = self._calculate_days_to_expiration(exp_date)
            if dte is None or dte > max_dte or dte < 1:
                continue
                
            exp_data = {
                'expiration_date': exp_date,
                'days_to_expiration': dte,
                'contracts': [],
                'total_call_volume': 0,
                'total_put_volume': 0,
                'total_call_oi': 0,
                'total_put_oi': 0
            }
            
            # Get strikes for this expiration
            call_strikes = call_exp_map.get(exp_date, {})
            put_strikes = put_exp_map.get(exp_date, {})
            
            # Get all strikes and sort them
            all_strikes = set()
            for strike in call_strikes.keys():
                all_strikes.add(float(strike))
            for strike in put_strikes.keys():
                all_strikes.add(float(strike))
                
            sorted_strikes = sorted(all_strikes)
            
            # Limit strikes around the money if too many
            if len(sorted_strikes) > max_strikes_per_exp:
                # Focus on strikes near underlying price
                closest_strikes = sorted(sorted_strikes, 
                                       key=lambda x: abs(x - underlying_price))[:max_strikes_per_exp]
                sorted_strikes = sorted(closest_strikes)
                
            # Process each strike
            for strike in sorted_strikes:
                strike_str = str(strike)
                
                call_data = None
                put_data = None
                
                # Get call data
                if strike_str in call_strikes:
                    call_list = call_strikes[strike_str]
                    if call_list and len(call_list) > 0:
                        call_info = call_list[0]
                        call_data = {
                            'contract_symbol': call_info.get('symbol'),
                            'strike_price': call_info.get('strikePrice'),
                            'bid': call_info.get('bidPrice'),
                            'ask': call_info.get('askPrice'),
                            'last': call_info.get('lastPrice'),
                            'mark': call_info.get('markPrice'),
                            'volume': call_info.get('totalVolume', 0),
                            'open_interest': call_info.get('openInterest', 0),
                            'implied_volatility': call_info.get('volatility'),
                            'delta': call_info.get('delta'),
                            'gamma': call_info.get('gamma'),
                            'theta': call_info.get('theta'),
                            'vega': call_info.get('vega'),
                            'rho': call_info.get('rho'),
                            'time_value': call_info.get('timeValue'),
                            'intrinsic_value': call_info.get('intrinsicValue'),
                            'in_the_money': call_info.get('inTheMoney'),
                            'expiration_type': call_info.get('expirationType'),
                            'last_trading_day': call_info.get('lastTradingDay'),
                            'multiplier': call_info.get('multiplier'),
                            'settlement_type': call_info.get('settlementType'),
                            'deliverable_note': call_info.get('deliverableNote')
                        }
                        exp_data['total_call_volume'] += call_data['volume']
                        exp_data['total_call_oi'] += call_data['open_interest']
                        
                # Get put data
                if strike_str in put_strikes:
                    put_list = put_strikes[strike_str]
                    if put_list and len(put_list) > 0:
                        put_info = put_list[0]
                        put_data = {
                            'contract_symbol': put_info.get('symbol'),
                            'strike_price': put_info.get('strikePrice'),
                            'bid': put_info.get('bidPrice'),
                            'ask': put_info.get('askPrice'),
                            'last': put_info.get('lastPrice'),
                            'mark': put_info.get('markPrice'),
                            'volume': put_info.get('totalVolume', 0),
                            'open_interest': put_info.get('openInterest', 0),
                            'implied_volatility': put_info.get('volatility'),
                            'delta': put_info.get('delta'),
                            'gamma': put_info.get('gamma'),
                            'theta': put_info.get('theta'),
                            'vega': put_info.get('vega'),
                            'rho': put_info.get('rho'),
                            'time_value': put_info.get('timeValue'),
                            'intrinsic_value': put_info.get('intrinsicValue'),
                            'in_the_money': put_info.get('inTheMoney'),
                            'expiration_type': put_info.get('expirationType'),
                            'last_trading_day': put_info.get('lastTradingDay'),
                            'multiplier': put_info.get('multiplier'),
                            'settlement_type': put_info.get('settlementType'),
                            'deliverable_note': put_info.get('deliverableNote')
                        }
                        exp_data['total_put_volume'] += put_data['volume']
                        exp_data['total_put_oi'] += put_data['open_interest']
                        
                # Add contract data if we have either call or put
                if call_data or put_data:
                    contract_data = {
                        'strike': strike,
                        'call': call_data,
                        'put': put_data
                    }
                    exp_data['contracts'].append(contract_data)
                    symbol_options['total_contracts'] += 1
                    
            if exp_data['contracts']:
                symbol_options['expirations'].append(exp_data)
                
        return symbol_options

    def save_options_data_to_json(self, options_data, filename='options_data.json'):
        """Save options data to JSON file."""
        try:
            with open(filename, 'w') as f:
                json.dump(options_data, f, indent=2, default=str)
            print(f"✅ Options data saved to {filename}")
            return True
        except Exception as e:
            print(f"❌ Error saving options data to JSON: {e}")
            return False

    def insert_options_data_to_database(self, options_data):
        """Insert options data into PostgreSQL database."""
        try:
            # Database connection
            conn = psycopg2.connect(
                host='localhost',
                database='volflow_options',
                user='isaac',
                port=5432
            )
            
            with conn.cursor() as cur:
                # Truncate existing options data
                print("🗑️  Truncating options tables...")
                cur.execute("TRUNCATE TABLE options_contracts;")
                
                # Insert new options data
                contracts_inserted = 0
                
                for symbol, symbol_data in options_data.get('symbols', {}).items():
                    underlying_price = symbol_data.get('underlying_price')
                    underlying_data = symbol_data.get('underlying_data', {})
                    
                    for expiration in symbol_data.get('expirations', []):
                        exp_date = expiration['expiration_date']
                        dte = expiration['days_to_expiration']
                        
                        for contract in expiration.get('contracts', []):
                            strike = contract['strike']
                            call_data = contract.get('call')
                            put_data = contract.get('put')
                            
                            # Clean expiration date for database (remove :X suffix)
                            clean_exp_date = exp_date.split(':')[0]
                            
                            # Insert call contract
                            if call_data:
                                cur.execute("""
                                    INSERT INTO options_contracts (
                                        symbol, underlying_price, contract_type, strike_price,
                                        expiration_date, days_to_expiration, contract_symbol,
                                        bid, ask, last_price, mark, volume, open_interest,
                                        implied_volatility, delta, gamma, theta, vega, rho,
                                        time_value, intrinsic_value, in_the_money,
                                        timestamp
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """, (
                                    symbol, underlying_price, 'CALL', strike,
                                    clean_exp_date, dte, call_data.get('contract_symbol'),
                                    call_data.get('bid'), call_data.get('ask'),
                                    call_data.get('last'), call_data.get('mark'),
                                    call_data.get('volume'), call_data.get('open_interest'),
                                    call_data.get('implied_volatility'), call_data.get('delta'),
                                    call_data.get('gamma'), call_data.get('theta'),
                                    call_data.get('vega'), call_data.get('rho'),
                                    call_data.get('time_value'), call_data.get('intrinsic_value'),
                                    call_data.get('in_the_money'), datetime.now()
                                ))
                                contracts_inserted += 1
                                
                            # Insert put contract
                            if put_data:
                                cur.execute("""
                                    INSERT INTO options_contracts (
                                        symbol, underlying_price, contract_type, strike_price,
                                        expiration_date, days_to_expiration, contract_symbol,
                                        bid, ask, last_price, mark, volume, open_interest,
                                        implied_volatility, delta, gamma, theta, vega, rho,
                                        time_value, intrinsic_value, in_the_money,
                                        timestamp
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """, (
                                    symbol, underlying_price, 'PUT', strike,
                                    clean_exp_date, dte, put_data.get('contract_symbol'),
                                    put_data.get('bid'), put_data.get('ask'),
                                    put_data.get('last'), put_data.get('mark'),
                                    put_data.get('volume'), put_data.get('open_interest'),
                                    put_data.get('implied_volatility'), put_data.get('delta'),
                                    put_data.get('gamma'), put_data.get('theta'),
                                    put_data.get('vega'), put_data.get('rho'),
                                    put_data.get('time_value'), put_data.get('intrinsic_value'),
                                    put_data.get('in_the_money'), datetime.now()
                                ))
                                contracts_inserted += 1
                
                conn.commit()
                print(f"✅ Inserted {contracts_inserted} options contracts into database")
                return True
                
        except Exception as e:
            print(f"❌ Error inserting options data to database: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def run_options_data_update(self, symbols, output_json=True):
        """
        Main method to fetch options data and update JSON file.
        This method will be called by the realtime monitor.
        
        Args:
            symbols (list): List of symbols to fetch options for
            output_json (bool): Whether to save to JSON file
            
        Returns:
            dict: The options data that was processed
        """
        print(f"🚀 Starting options data update for {len(symbols)} symbols")
        
        # Get all options data
        options_data = self.get_all_options_data(symbols)
        
        if not options_data or not options_data.get('symbols'):
            print("❌ No options data retrieved")
            return None
            
        # Save to JSON if requested
        if output_json:
            self.save_options_data_to_json(options_data, 'options_data.json')
            
        print(f"✅ Options data update completed")
        return options_data


def main():
    """Main function to run options data handler directly."""
    print("🔍 Options Data Handler - Direct Test")
    print("=" * 50)
    
    # Initialize handler
    handler = OptionsDataHandler()
    
    # Test with NVDA
    test_symbols = ['NVDA']
    
    print(f"📊 Testing options data for: {test_symbols}")
    
    # Run the main update method
    result = handler.run_options_data_update(
        symbols=test_symbols,
        output_json=True
    )
    
    if result:
        print("\n✅ SUCCESS! Options data handler working correctly")
        total_contracts = sum(
            symbol_data.get('total_contracts', 0) 
            for symbol_data in result.get('symbols', {}).values()
        )
        print(f"📈 Processed {len(result.get('symbols', {}))} symbols")
        print(f"📊 Total contracts: {total_contracts}")
        
        # Show summary for each symbol
        for symbol, data in result.get('symbols', {}).items():
            print(f"   {symbol}: {data.get('total_contracts', 0)} contracts, {len(data.get('expirations', []))} expirations")
            print(f"   Current price: ${data.get('underlying_price', 0):.2f}")
    else:
        print("\n❌ FAILED! No options data retrieved")
        print("Check API tokens, market hours, or symbol validity")


if __name__ == "__main__":
    main()
