"""
Stock Screener Handler

Handles concurrent screening of stocks based on specific criteria:
1. Stock price ≥ $5 (excludes penny stocks)
2. Relative Volume in first 5 minutes ≥ 100% (twice average)
3. Average trading volume (14 days) ≥ 1,000,000 shares/day
4. Average True Range (14 days) > $0.50

Uses concurrent processing for efficient batch screening.
"""

import asyncio
import csv
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd

from technical_indicators import TechnicalIndicators, IndicatorConfig


@dataclass
class ScreeningCriteria:
    """Screening criteria configuration"""
    min_price: float = 5.0
    min_relative_volume_pct: float = 100.0
    min_avg_volume: int = 1_000_000
    min_atr: float = 0.50


@dataclass
class ScreeningResult:
    """Result of screening a single stock"""
    symbol: str
    meets_criteria: bool
    current_price: float
    relative_volume_pct: float
    avg_volume_14d: int
    atr_14d: float
    error: Optional[str] = None


class ScreenerHandler:
    """Handles concurrent stock screening operations"""
    
    def __init__(self, max_workers: int = 10):
        """
        Initialize screener handler
        
        Args:
            max_workers: Maximum number of concurrent workers
        """
        # Create indicator config that matches our screening criteria
        indicator_config = IndicatorConfig(
            min_price=5.0,
            min_relative_volume_pct=100.0,
            min_avg_volume=1_000_000,
            min_atr_dollars=0.50
        )
        
        self.technical_indicators = TechnicalIndicators(indicator_config)
        self.max_workers = max_workers
        self.criteria = ScreeningCriteria()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def load_symbols_from_csv(self, csv_path: str, limit: Optional[int] = None) -> List[str]:
        """
        Load stock symbols from SEC CSV file
        
        Args:
            csv_path: Path to SEC symbols CSV file
            limit: Optional limit on number of symbols to load
            
        Returns:
            List of stock symbols
        """
        symbols = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        break
                    symbol = row['symbol'].strip()
                    if symbol and symbol != 'symbol':  # Skip header if present
                        symbols.append(symbol)
            
            self.logger.info(f"Loaded {len(symbols)} symbols from {csv_path}")
            return symbols
            
        except Exception as e:
            self.logger.error(f"Error loading symbols from CSV: {e}")
            return []
    
    def screen_single_stock(self, symbol: str) -> ScreeningResult:
        """
        Screen a single stock against all criteria
        
        Args:
            symbol: Stock symbol to screen
            
        Returns:
            ScreeningResult with analysis
        """
        try:
            # Get stock analysis from technical indicators
            analysis = self.technical_indicators.get_indicator_summary(symbol)
            
            if not analysis:
                return ScreeningResult(
                    symbol=symbol,
                    meets_criteria=False,
                    current_price=0.0,
                    relative_volume_pct=0.0,
                    avg_volume_14d=0,
                    atr_14d=0.0,
                    error='No analysis data'
                )
            
            # Extract values
            current_price = analysis.get('current_price', 0.0)
            indicators = analysis.get('indicators', {})
            relative_volume_pct = indicators.get('relative_volume_pct', 0.0)
            avg_volume_14d = indicators.get('avg_volume_14d', 0)
            atr_14d = indicators.get('atr_14d', 0.0)
            
            # Check all criteria
            meets_price = current_price >= self.criteria.min_price
            meets_rel_volume = relative_volume_pct >= self.criteria.min_relative_volume_pct
            meets_avg_volume = avg_volume_14d >= self.criteria.min_avg_volume
            meets_atr = atr_14d > self.criteria.min_atr
            
            meets_criteria = all([meets_price, meets_rel_volume, meets_avg_volume, meets_atr])
            
            result = ScreeningResult(
                symbol=symbol,
                meets_criteria=meets_criteria,
                current_price=current_price,
                relative_volume_pct=relative_volume_pct,
                avg_volume_14d=int(avg_volume_14d),
                atr_14d=atr_14d
            )
            
            if meets_criteria:
                self.logger.info(f"✅ {symbol} meets all criteria - Price: ${current_price:.2f}, RelVol: {relative_volume_pct:.1f}%, AvgVol: {avg_volume_14d:,}, ATR: ${atr_14d:.2f}")
            else:
                self.logger.debug(f"❌ {symbol} failed criteria - Price: ${current_price:.2f}, RelVol: {relative_volume_pct:.1f}%, AvgVol: {avg_volume_14d:,}, ATR: ${atr_14d:.2f}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error screening {symbol}: {e}")
            return ScreeningResult(
                symbol=symbol,
                meets_criteria=False,
                current_price=0.0,
                relative_volume_pct=0.0,
                avg_volume_14d=0,
                atr_14d=0.0,
                error=str(e)
            )
    
    def screen_stocks_concurrent(self, symbols: List[str], batch_size: int = 50) -> List[ScreeningResult]:
        """
        Screen multiple stocks concurrently
        
        Args:
            symbols: List of stock symbols to screen
            batch_size: Number of stocks to process in each batch
            
        Returns:
            List of ScreeningResult objects
        """
        all_results = []
        total_symbols = len(symbols)
        
        self.logger.info(f"🔍 Starting concurrent screening of {total_symbols} stocks...")
        self.logger.info(f"📊 Screening Criteria:")
        self.logger.info(f"   • Min Price: ${self.criteria.min_price}")
        self.logger.info(f"   • Min Relative Volume: {self.criteria.min_relative_volume_pct}%")
        self.logger.info(f"   • Min Avg Volume (14d): {self.criteria.min_avg_volume:,}")
        self.logger.info(f"   • Min ATR (14d): ${self.criteria.min_atr}")
        
        start_time = time.time()
        
        # Process in batches to manage API rate limits
        for batch_start in range(0, total_symbols, batch_size):
            batch_end = min(batch_start + batch_size, total_symbols)
            batch_symbols = symbols[batch_start:batch_end]
            
            self.logger.info(f"📦 Processing batch {batch_start//batch_size + 1}/{(total_symbols-1)//batch_size + 1} ({len(batch_symbols)} symbols)")
            
            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(self.screen_single_stock, symbol): symbol 
                    for symbol in batch_symbols
                }
                
                # Collect results as they complete
                batch_results = []
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result(timeout=30)  # 30 second timeout per stock
                        batch_results.append(result)
                    except Exception as e:
                        self.logger.error(f"Error processing {symbol}: {e}")
                        batch_results.append(ScreeningResult(
                            symbol=symbol,
                            meets_criteria=False,
                            current_price=0.0,
                            relative_volume_pct=0.0,
                            avg_volume_14d=0,
                            atr_14d=0.0,
                            error=str(e)
                        ))
                
                all_results.extend(batch_results)
            
            # Brief pause between batches to respect API limits
            if batch_end < total_symbols:
                time.sleep(1)
        
        elapsed_time = time.time() - start_time
        
        # Filter successful results
        successful_results = [r for r in all_results if r.error is None]
        passing_results = [r for r in successful_results if r.meets_criteria]
        
        self.logger.info(f"🎯 Screening Complete!")
        self.logger.info(f"   • Total Processed: {len(all_results)}")
        self.logger.info(f"   • Successful: {len(successful_results)}")
        self.logger.info(f"   • Meeting Criteria: {len(passing_results)}")
        self.logger.info(f"   • Processing Time: {elapsed_time:.1f} seconds")
        self.logger.info(f"   • Average Time per Stock: {elapsed_time/len(all_results):.2f} seconds")
        
        return all_results
    
    def get_top_stocks(self, results: List[ScreeningResult], limit: int = 20) -> List[ScreeningResult]:
        """
        Get top stocks that meet criteria, sorted by relative volume
        
        Args:
            results: List of screening results
            limit: Maximum number of stocks to return
            
        Returns:
            List of top ScreeningResult objects
        """
        # Filter stocks that meet criteria and have no errors
        qualifying_stocks = [
            r for r in results 
            if r.meets_criteria and r.error is None
        ]
        
        # Sort by relative volume (descending) as primary criteria
        # Then by ATR (descending) as secondary criteria
        qualifying_stocks.sort(
            key=lambda x: (x.relative_volume_pct, x.atr_14d), 
            reverse=True
        )
        
        return qualifying_stocks[:limit]
    
    def print_screening_results(self, results: List[ScreeningResult], show_all: bool = False):
        """
        Print formatted screening results
        
        Args:
            results: List of screening results
            show_all: Whether to show all results or just those meeting criteria
        """
        if show_all:
            display_results = results
            title = "All Screening Results"
        else:
            display_results = [r for r in results if r.meets_criteria and r.error is None]
            title = "Stocks Meeting All Criteria"
        
        print(f"\n{'='*80}")
        print(f"📊 {title}")
        print(f"{'='*80}")
        
        if not display_results:
            print("No stocks found meeting the criteria.")
            return
        
        # Header
        print(f"{'Rank':<4} {'Symbol':<8} {'Price':<10} {'RelVol%':<8} {'ATR':<8} {'AvgVol':<12} {'Status'}")
        print("-" * 80)
        
        # Results
        for i, result in enumerate(display_results, 1):
            status = "✅ PASS" if result.meets_criteria else "❌ FAIL"
            if result.error:
                status = f"⚠️  ERROR: {result.error[:20]}..."
            
            print(f"{i:<4} {result.symbol:<8} ${result.current_price:<9.2f} "
                  f"{result.relative_volume_pct:<7.1f}% ${result.atr_14d:<7.2f} "
                  f"{result.avg_volume_14d:<12,} {status}")
    
    def export_results_to_csv(self, results: List[ScreeningResult], filename: str):
        """
        Export screening results to CSV file
        
        Args:
            results: List of screening results
            filename: Output CSV filename
        """
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'symbol', 'meets_criteria', 'current_price', 'relative_volume_pct',
                    'avg_volume_14d', 'atr_14d', 'error'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for result in results:
                    writer.writerow({
                        'symbol': result.symbol,
                        'meets_criteria': result.meets_criteria,
                        'current_price': result.current_price,
                        'relative_volume_pct': result.relative_volume_pct,
                        'avg_volume_14d': result.avg_volume_14d,
                        'atr_14d': result.atr_14d,
                        'error': result.error or ''
                    })
            
            self.logger.info(f"Results exported to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error exporting results to CSV: {e}")
    
    def export_results_to_json(self, results: List[ScreeningResult], filename: str):
        """
        Export screening results to JSON file
        
        Args:
            results: List of screening results
            filename: Output JSON filename
        """
        try:
            # Convert dataclass objects to dictionaries
            json_data = {
                'screening_metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'total_stocks_screened': len(results),
                    'stocks_meeting_criteria': len([r for r in results if r.meets_criteria and r.error is None]),
                    'screening_criteria': {
                        'min_price': self.criteria.min_price,
                        'min_relative_volume_pct': self.criteria.min_relative_volume_pct,
                        'min_avg_volume': self.criteria.min_avg_volume,
                        'min_atr': self.criteria.min_atr
                    }
                },
                'results': [asdict(result) for result in results]
            }
            
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(json_data, jsonfile, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Results exported to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error exporting results to JSON: {e}")
    
    def get_results_as_json(self, results: List[ScreeningResult]) -> Dict:
        """
        Convert screening results to JSON-serializable dictionary
        
        Args:
            results: List of screening results
            
        Returns:
            Dictionary containing screening results and metadata
        """
        return {
            'screening_metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_stocks_screened': len(results),
                'stocks_meeting_criteria': len([r for r in results if r.meets_criteria and r.error is None]),
                'screening_criteria': {
                    'min_price': self.criteria.min_price,
                    'min_relative_volume_pct': self.criteria.min_relative_volume_pct,
                    'min_avg_volume': self.criteria.min_avg_volume,
                    'min_atr': self.criteria.min_atr
                }
            },
            'results': [asdict(result) for result in results]
        }
    
    def update_criteria(self, **kwargs):
        """
        Update screening criteria
        
        Args:
            **kwargs: Criteria parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self.criteria, key):
                setattr(self.criteria, key, value)
                self.logger.info(f"Updated {key} to {value}")
            else:
                self.logger.warning(f"Unknown criteria parameter: {key}")


if __name__ == "__main__":
    # Example usage
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize screener
    screener = ScreenerHandler(max_workers=5)
    
    # Load symbols from CSV (first 10 for testing)
    symbols = screener.load_symbols_from_csv('SEC_CIKs_Symbols.csv', limit=10)
    
    if symbols:
        # Run screening
        results = screener.screen_stocks_concurrent(symbols)
        
        # Display results
        screener.print_screening_results(results)
        
        # Get top stocks
        top_stocks = screener.get_top_stocks(results, limit=5)
        if top_stocks:
            print(f"\n🏆 Top 5 Stocks:")
            screener.print_screening_results(top_stocks)
        
        # Export results
        screener.export_results_to_csv(results, 'screening_results.csv')
        screener.export_results_to_json(results, 'screening_results.json')
        
        # Get results as JSON dict
        json_results = screener.get_results_as_json(results)
        print(f"\n📄 JSON Results Summary:")
        print(f"   • Total Stocks: {json_results['screening_metadata']['total_stocks_screened']}")
        print(f"   • Meeting Criteria: {json_results['screening_metadata']['stocks_meeting_criteria']}")
