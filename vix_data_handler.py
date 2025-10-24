#!/usr/bin/env python3
"""
VIX Data Handler

This script fetches current VIX (Volatility Index) data and creates a JSON file
with the current VIX value and related volatility metrics.

The script uses the same historical data handler pattern as the exceedance
indicators calculator for consistency.

Usage: python3 vix_data_handler.py [--single|--continuous]
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
import json
import os
import time as time_module

# Import our existing data handlers
from historical_data_handler import HistoricalDataHandler

class VIXDataHandler:
    """
    Simple VIX data handler to fetch and process VIX data
    """
    
    def __init__(self):
        """Initialize the VIX data handler."""
        self.historical_handler = HistoricalDataHandler()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # VIX symbol
        self.vix_symbol = "$VIX"  # Standard VIX symbol for Schwab
        
        # Output file
        self.output_file = "vix_data.json"
        
        self.logger.info("VIXDataHandler initialized")

    def get_current_vix_data(self) -> Optional[pd.DataFrame]:
        """Get current VIX data using intraday 1-minute data."""
        try:
            # Get recent intraday data for VIX
            historical_data = self.historical_handler.get_historical_data(
                symbol=self.vix_symbol,
                periodType="day",
                period=1,  # Last 1 day
                frequencyType="minute",
                freq=1  # 1-minute intervals
            )
            
            if historical_data and 'candles' in historical_data:
                df = pd.DataFrame(historical_data['candles'])
                if not df.empty:
                    # Convert datetime column
                    try:
                        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                    except (ValueError, TypeError):
                        df['datetime'] = pd.to_datetime(df['datetime'])
                    
                    # Ensure numeric types
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    return df
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error getting VIX data: {e}")
            return None

    def calculate_vix_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate VIX metrics from the data."""
        try:
            if df is None or len(df) == 0:
                return self._create_empty_vix_data()
            
            # Get current values (most recent bar)
            current_vix = float(df['close'].iloc[-1])
            current_high = float(df['high'].iloc[-1])
            current_low = float(df['low'].iloc[-1])
            current_volume = int(df['volume'].iloc[-1]) if 'volume' in df.columns else 0
            
            # Calculate daily statistics
            daily_high = float(df['high'].max())
            daily_low = float(df['low'].min())
            daily_range = daily_high - daily_low
            
            # Calculate moving averages if we have enough data
            if len(df) >= 20:
                vix_20_avg = float(df['close'].tail(20).mean())
            else:
                vix_20_avg = current_vix
            
            if len(df) >= 50:
                vix_50_avg = float(df['close'].tail(50).mean())
            else:
                vix_50_avg = current_vix
            
            # Calculate volatility of VIX itself (volatility of volatility)
            if len(df) >= 10:
                vix_volatility = float(df['close'].tail(10).std())
            else:
                vix_volatility = 0.0
            
            # Determine VIX level category
            if current_vix < 15:
                vix_level = "LOW"
                market_fear = "COMPLACENT"
            elif current_vix < 20:
                vix_level = "NORMAL"
                market_fear = "CALM"
            elif current_vix < 30:
                vix_level = "ELEVATED"
                market_fear = "CONCERNED"
            elif current_vix < 40:
                vix_level = "HIGH"
                market_fear = "FEARFUL"
            else:
                vix_level = "EXTREME"
                market_fear = "PANIC"
            
            # Calculate position relative to daily range
            if daily_range > 0:
                position_in_daily_range = ((current_vix - daily_low) / daily_range) * 100
            else:
                position_in_daily_range = 50.0
            
            return {
                'symbol': self.vix_symbol,
                'timestamp': datetime.now().isoformat(),
                'last_updated': df['datetime'].iloc[-1].isoformat(),
                
                # Current VIX values
                'current_vix': current_vix,
                'current_high': current_high,
                'current_low': current_low,
                'current_volume': current_volume,
                
                # Daily statistics
                'daily_high': daily_high,
                'daily_low': daily_low,
                'daily_range': daily_range,
                'position_in_daily_range': position_in_daily_range,
                
                # Moving averages
                'vix_20_avg': vix_20_avg,
                'vix_50_avg': vix_50_avg,
                
                # VIX volatility (vol of vol)
                'vix_volatility': vix_volatility,
                
                # Market interpretation
                'vix_level': vix_level,
                'market_fear': market_fear,
                
                # Relative positioning
                'above_20_avg': current_vix > vix_20_avg,
                'above_50_avg': current_vix > vix_50_avg,
                'daily_change_pct': ((current_vix - df['close'].iloc[0]) / df['close'].iloc[0] * 100) if len(df) > 1 else 0.0
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating VIX metrics: {e}")
            return self._create_empty_vix_data()

    def _create_empty_vix_data(self) -> Dict[str, Any]:
        """Create empty VIX data for error cases."""
        return {
            'symbol': self.vix_symbol,
            'timestamp': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'error': 'Unable to fetch VIX data',
            
            # Current VIX values
            'current_vix': 0.0,
            'current_high': 0.0,
            'current_low': 0.0,
            'current_volume': 0,
            
            # Daily statistics
            'daily_high': 0.0,
            'daily_low': 0.0,
            'daily_range': 0.0,
            'position_in_daily_range': 50.0,
            
            # Moving averages
            'vix_20_avg': 0.0,
            'vix_50_avg': 0.0,
            
            # VIX volatility
            'vix_volatility': 0.0,
            
            # Market interpretation
            'vix_level': 'UNKNOWN',
            'market_fear': 'UNKNOWN',
            
            # Relative positioning
            'above_20_avg': False,
            'above_50_avg': False,
            'daily_change_pct': 0.0
        }

    def fetch_and_process_vix(self) -> Dict[str, Any]:
        """Main method to fetch and process VIX data."""
        try:
            print(f"📊 Fetching VIX data...")
            
            # Get VIX data
            df = self.get_current_vix_data()
            
            if df is None or len(df) == 0:
                print("❌ No VIX data available")
                return self._create_empty_vix_data()
            
            # Calculate metrics
            vix_metrics = self.calculate_vix_metrics(df)
            
            # Print summary
            current_vix = vix_metrics.get('current_vix', 0.0)
            vix_level = vix_metrics.get('vix_level', 'UNKNOWN')
            market_fear = vix_metrics.get('market_fear', 'UNKNOWN')
            daily_change = vix_metrics.get('daily_change_pct', 0.0)
            
            print(f"✅ VIX: {current_vix:.2f} | Level: {vix_level} | Fear: {market_fear} | Daily: {daily_change:+.2f}%")
            
            return vix_metrics
            
        except Exception as e:
            self.logger.error(f"Error in fetch_and_process_vix: {e}")
            return self._create_empty_vix_data()

    def save_vix_data(self, vix_data: Dict[str, Any]) -> bool:
        """Save VIX data to JSON file."""
        try:
            # Create comprehensive VIX data structure
            output_data = {
                'strategy_name': 'VIX_Data_Handler',
                'last_updated': datetime.now().isoformat(),
                'data_source': 'schwab_historical_data',
                'update_frequency': 'minute_intervals',
                
                # VIX data
                'vix_data': vix_data,
                
                # Metadata
                'metadata': {
                    'symbol': self.vix_symbol,
                    'data_type': 'volatility_index',
                    'calculation_method': 'intraday_1min',
                    'interpretation': {
                        'LOW': 'VIX < 15 - Market complacency',
                        'NORMAL': 'VIX 15-20 - Normal volatility',
                        'ELEVATED': 'VIX 20-30 - Increased concern',
                        'HIGH': 'VIX 30-40 - High fear',
                        'EXTREME': 'VIX > 40 - Extreme fear/panic'
                    }
                }
            }
            
            # Write to file
            with open(self.output_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            print(f"✅ Saved VIX data to {self.output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving VIX data to file: {e}")
            return False

def run_single_vix_analysis():
    """Run a single VIX analysis cycle."""
    print("🚀 Single VIX Data Analysis")
    print("=" * 40)
    
    # Create VIX handler
    vix_handler = VIXDataHandler()
    
    # Fetch and process VIX data
    vix_data = vix_handler.fetch_and_process_vix()
    
    # Save to file
    success = vix_handler.save_vix_data(vix_data)
    
    if success:
        print(f"\n🎉 VIX analysis completed successfully!")
        print(f"📁 Created {vix_handler.output_file}")
    else:
        print("❌ VIX analysis completed with errors")
    
    return success

def run_continuous_vix_analysis():
    """Run continuous VIX analysis on minute intervals."""
    print("🚀 Starting Continuous VIX Data Handler")
    print("=" * 50)
    print("📊 Monitoring VIX volatility index")
    print("⏰ Running on minute intervals")
    print("🔄 Press Ctrl+C to stop")
    print()
    
    cycle_count = 0
    vix_handler = VIXDataHandler()
    
    try:
        while True:
            cycle_count += 1
            start_time = time_module.time()
            
            print(f"\n🔄 VIX Analysis Cycle #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 40)
            
            # Fetch and process VIX data
            vix_data = vix_handler.fetch_and_process_vix()
            
            # Save to file
            success = vix_handler.save_vix_data(vix_data)
            
            elapsed_time = time_module.time() - start_time
            print(f"✅ Cycle #{cycle_count} completed in {elapsed_time:.2f} seconds")
            
            if success:
                print(f"📁 Updated {vix_handler.output_file}")
            else:
                print("⚠️ Cycle completed with errors")
            
            # Wait for next minute interval
            print(f"⏰ Waiting for next minute interval...")
            time_module.sleep(60)  # Wait 60 seconds for next cycle
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping continuous VIX analysis after {cycle_count} cycles")
        print("👋 VIX Data Handler stopped")

def main():
    """Main function to run VIX data handler."""
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--continuous':
            run_continuous_vix_analysis()
        elif sys.argv[1] == '--single':
            run_single_vix_analysis()
        else:
            print("Usage: python3 vix_data_handler.py [--continuous|--single]")
            print("  --continuous: Run continuous VIX analysis on minute intervals")
            print("  --single: Run single VIX analysis cycle")
    else:
        # Default to single analysis
        run_single_vix_analysis()

if __name__ == "__main__":
    main()
