import os
import requests
import pandas as pd
import time
from connection_manager import ensure_valid_tokens
from datetime import datetime
from config_loader import get_config

class HistoricalDataHandler:
    def __init__(self):
        """
        Initialize the HistoricalDataHandler.
        """
        # Load configuration
        self.config = get_config()
        self.api_config = self.config.get_api_config()
        
        # Get API settings from configuration
        self.max_retries = self.api_config.get('max_retries', 5)
        self.retry_delay = self.api_config.get('retry_delay', 2)
        self.rate_limit_delay = self.api_config.get('rate_limit_delay', 60)
        self.base_url = self.api_config.get('base_url', 'https://api.schwabapi.com')

    def get_historical_data(self, symbol, periodType, period, frequencyType, freq, startDate=None, endDate=None, needExtendedHoursData=True):
        """
        Retrieve historical price data from the Schwab API with automatic retry logic.

        Args:
            symbol (str): The stock symbol (e.g., 'AAPL', 'MSFT')
            periodType (str): The chart period type. Valid values: 'day', 'month', 'year', 'ytd'
            period (int): The number of chart period types. Valid values depend on periodType:
                - day: 1, 2, 3, 4, 5, 10 (default: 10)
                - month: 1, 2, 3, 6 (default: 1)
                - year: 1, 2, 3, 5, 10, 15, 20 (default: 1)
                - ytd: 1 (default: 1)
            frequencyType (str): The time frequency type. Valid values depend on periodType:
                - day: 'minute' (default)
                - month: 'daily', 'weekly' (default: 'weekly')
                - year: 'daily', 'weekly', 'monthly' (default: 'monthly')
                - ytd: 'daily', 'weekly' (default: 'weekly')
            freq (int): The time frequency duration. Valid values depend on frequencyType:
                - minute: 1, 5, 10, 15, 30 (default: 1)
                - daily: 1 (default: 1)
                - weekly: 1 (default: 1)
                - monthly: 1 (default: 1)
            startDate (int, optional): Start date in milliseconds since UNIX epoch
            endDate (int, optional): End date in milliseconds since UNIX epoch
            needExtendedHoursData (bool): Whether to include extended hours data

        Returns:
            dict: Historical data with candles, symbol, previousClose, and previousCloseDate
            None: If no data is available or request fails
        """
        retry_delay = self.retry_delay

        for attempt in range(self.max_retries):
            try:
                # Get valid tokens
                tokens = ensure_valid_tokens()
                access_token = tokens["access_token"]
                current_epoch_ms = int(pd.Timestamp.now().timestamp() * 1000)
                endDate = current_epoch_ms

                # Prepare headers
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }

                # Build URL with parameters
                url = (f"{self.base_url}/marketdata/v1/pricehistory"
                      f"?symbol={symbol}"
                      f"&periodType={periodType}"
                      f"&period={period}"
                      f"&frequencyType={frequencyType}"
                      f"&frequency={freq}"
                      f"&endDate={endDate}"
                      f"&needExtendedHoursData={str(needExtendedHoursData).lower()}")

                if startDate:
                    url += f"&startDate={startDate}"

                # Make API request
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                data = response.json()

                # Process and return data if available
                if not data.get("empty", True) and "candles" in data:
                    candles = [
                        {
                            "datetime": self._convert_timestamp(bar["datetime"]),
                            "open": bar.get("open"),
                            "high": bar.get("high"),
                            "low": bar.get("low"),
                            "close": bar.get("close"),
                            "volume": bar.get("volume")
                        }
                        for bar in data["candles"]
                    ]
                    
                    return {
                        "symbol": symbol,
                        "candles": candles,
                        "previousClose": data.get("previousClose"),
                        "previousCloseDate": self._convert_timestamp(data.get("previousCloseDate"))
                    }
                else:
                    print(f"No historical data available for {symbol}")
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
                print(f"Unexpected error fetching historical data: {e}")
                return None

        return None

    def _convert_timestamp(self, timestamp):
        """
        Convert a timestamp to a formatted datetime string.

        Args:
            timestamp (int): The timestamp to convert (in milliseconds)

        Returns:
            str: The formatted datetime string or None if timestamp is invalid
        """
        if timestamp is not None:
            try:
                return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, OSError):
                return None
        return None
