import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from config_loader import get_config
import psycopg2
from psycopg2.extras import RealDictCursor

class PnLDataHandler:
    """
    Handles P&L analysis and calculations for transaction data.
    Focused on analyzing transaction data to calculate win/loss statistics,
    trade performance, and generate reports.
    """
    
    def __init__(self):
        """
        Initialize the PnLDataHandler.
        """
        # Load configuration
        self.config = get_config()
        self.api_config = self.config.get_api_config()
        
        # Initialize collections for tracking
        self.symbol_stats = {}
        self.trade_history = []
        self.winning_trades = []
        self.losing_trades = []
        self.breakeven_trades = []
        

    def calculate_win_loss_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate comprehensive win/loss statistics for trades, including short vs long breakdown.
        
        Args:
            df (pd.DataFrame): DataFrame containing transaction data
            
        Returns:
            Dict[str, Any]: Comprehensive statistics including overall, long, and short performance
        """
        # First, ensure we have the necessary columns
        if 'symbol' not in df.columns or 'amount' not in df.columns or 'quantity' not in df.columns:
            print("Win/Loss calculation requires symbol, amount, and quantity columns")
            return {}
            
        # Create a copy to avoid modifying the original dataframe
        trade_df = df.copy()
        
        # Identify buy and sell transactions
        trade_df['trade_type'] = 'Unknown'
        trade_df.loc[trade_df['quantity'] > 0, 'trade_type'] = 'Buy'
        trade_df.loc[trade_df['quantity'] < 0, 'trade_type'] = 'Sell'
        
        # Group by symbol for analysis
        symbols = trade_df['symbol'].unique()
        
        # Initialize win/loss counters
        stats = {
            'overall': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'long': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'short': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'symbols': {},
            'trades': {'winning': [], 'losing': [], 'breakeven': []},
            'streaks': {'all': [], 'long': [], 'short': []}
        }
        
        # Initialize collections for average calculations
        all_win_amounts = []
        all_loss_amounts = []
        long_win_amounts = []
        long_loss_amounts = []
        short_win_amounts = []
        short_loss_amounts = []
        
        for symbol in symbols:
            # Filter for this symbol
            symbol_trades = trade_df[trade_df['symbol'] == symbol].sort_values('date')
            
            # Skip symbols with less than 2 trades (need at least a buy and sell)
            if len(symbol_trades) < 2:
                continue
                
            symbol_result = self._analyze_symbol_trades(symbol, symbol_trades)
            
            if symbol_result:
                # Update overall stats
                stats['overall']['wins'] += symbol_result['total']['wins']
                stats['overall']['losses'] += symbol_result['total']['losses']
                stats['overall']['profit_loss'] += symbol_result['total']['profit_loss']
                
                stats['long']['wins'] += symbol_result['long']['wins']
                stats['long']['losses'] += symbol_result['long']['losses']
                stats['long']['profit_loss'] += symbol_result['long']['profit_loss']
                
                stats['short']['wins'] += symbol_result['short']['wins']
                stats['short']['losses'] += symbol_result['short']['losses']
                stats['short']['profit_loss'] += symbol_result['short']['profit_loss']
                
                # Store symbol stats
                stats['symbols'][symbol] = symbol_result
                
                # Collect win/loss amounts
                all_win_amounts.extend(symbol_result.get('win_amounts', []))
                all_loss_amounts.extend(symbol_result.get('loss_amounts', []))
                long_win_amounts.extend(symbol_result.get('long_win_amounts', []))
                long_loss_amounts.extend(symbol_result.get('long_loss_amounts', []))
                short_win_amounts.extend(symbol_result.get('short_win_amounts', []))
                short_loss_amounts.extend(symbol_result.get('short_loss_amounts', []))
                
                # Collect trades
                stats['trades']['winning'].extend(symbol_result.get('winning_trades', []))
                stats['trades']['losing'].extend(symbol_result.get('losing_trades', []))
                stats['trades']['breakeven'].extend(symbol_result.get('breakeven_trades', []))
        
        # Calculate final statistics
        stats = self._calculate_final_stats(stats, all_win_amounts, all_loss_amounts,
                                          long_win_amounts, long_loss_amounts,
                                          short_win_amounts, short_loss_amounts)
        
        return stats

    def _analyze_symbol_trades(self, symbol: str, symbol_trades: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze trades for a specific symbol.
        
        Args:
            symbol (str): Symbol to analyze
            symbol_trades (pd.DataFrame): Trades for this symbol
            
        Returns:
            Dict[str, Any]: Analysis results for the symbol
        """
        # Track positions for this symbol
        long_position = 0  # Positive values = long position
        short_position = 0  # Positive values = short position
        long_cost_basis = 0
        short_cost_basis = 0
        
        # Initialize counters
        result = {
            'symbol': symbol,
            'total': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'long': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'short': {'wins': 0, 'losses': 0, 'profit_loss': 0.0},
            'win_amounts': [],
            'loss_amounts': [],
            'long_win_amounts': [],
            'long_loss_amounts': [],
            'short_win_amounts': [],
            'short_loss_amounts': [],
            'winning_trades': [],
            'losing_trades': [],
            'breakeven_trades': []
        }
        
        # Process each trade chronologically
        for _, trade in symbol_trades.iterrows():
            if trade['trade_type'] == 'Buy':
                bought_shares = abs(trade['quantity'])
                bought_amount = abs(trade['amount'])
                
                # Case 1: Covering a short position
                if short_position > 0:
                    covering_shares = min(bought_shares, short_position)
                    covering_cost = (bought_amount / bought_shares) * covering_shares
                    short_sale_proceeds = short_cost_basis * covering_shares
                    
                    # For shorts: profit = sold high, bought low
                    trade_pl = short_sale_proceeds - covering_cost
                    
                    # Create trade record
                    trade_record = self._create_trade_record(
                        symbol, 'short', covering_shares, short_cost_basis,
                        covering_cost / covering_shares, trade_pl, trade['date']
                    )
                    
                    # Categorize trade
                    if abs(trade_pl) < 0.01:  # Breakeven
                        result['breakeven_trades'].append(trade_record)
                    elif trade_pl > 0:
                        result['winning_trades'].append(trade_record)
                        result['short']['wins'] += 1
                        result['total']['wins'] += 1
                        result['short_win_amounts'].append(trade_pl)
                        result['win_amounts'].append(trade_pl)
                    else:
                        result['losing_trades'].append(trade_record)
                        result['short']['losses'] += 1
                        result['total']['losses'] += 1
                        result['short_loss_amounts'].append(trade_pl)
                        result['loss_amounts'].append(trade_pl)
                    
                    result['short']['profit_loss'] += trade_pl
                    result['total']['profit_loss'] += trade_pl
                    short_position -= covering_shares
                    
                    # Handle remaining shares
                    remaining_shares = bought_shares - covering_shares
                    if remaining_shares > 0:
                        remaining_cost = (bought_amount / bought_shares) * remaining_shares
                        if long_position + remaining_shares > 0:
                            long_cost_basis = ((long_position * long_cost_basis) + remaining_cost) / (long_position + remaining_shares)
                        long_position += remaining_shares
                
                # Case 2: Adding to a long position
                else:
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
                    
                    # For longs: profit = sold high, bought low
                    trade_pl = closing_proceeds - long_cost
                    
                    # Create trade record
                    trade_record = self._create_trade_record(
                        symbol, 'long', closing_shares, long_cost_basis,
                        closing_proceeds / closing_shares, trade_pl, trade['date']
                    )
                    
                    # Categorize trade
                    if abs(trade_pl) < 0.01:  # Breakeven
                        result['breakeven_trades'].append(trade_record)
                    elif trade_pl > 0:
                        result['winning_trades'].append(trade_record)
                        result['long']['wins'] += 1
                        result['total']['wins'] += 1
                        result['long_win_amounts'].append(trade_pl)
                        result['win_amounts'].append(trade_pl)
                    else:
                        result['losing_trades'].append(trade_record)
                        result['long']['losses'] += 1
                        result['total']['losses'] += 1
                        result['long_loss_amounts'].append(trade_pl)
                        result['loss_amounts'].append(trade_pl)
                    
                    result['long']['profit_loss'] += trade_pl
                    result['total']['profit_loss'] += trade_pl
                    long_position -= closing_shares
                    
                    # Handle remaining shares
                    remaining_shares = sold_shares - closing_shares
                    if remaining_shares > 0:
                        remaining_proceeds = (sold_amount / sold_shares) * remaining_shares
                        if short_position + remaining_shares > 0:
                            short_cost_basis = ((short_position * short_cost_basis) + remaining_proceeds) / (short_position + remaining_shares)
                        short_position += remaining_shares
                
                # Case 2: Opening a short position
                else:
                    if short_position + sold_shares > 0:
                        short_cost_basis = ((short_position * short_cost_basis) + sold_amount) / (short_position + sold_shares)
                    short_position += sold_shares
        
        return result if result['total']['wins'] + result['total']['losses'] > 0 else None

    def _create_trade_record(self, symbol: str, trade_type: str, shares: float, 
                           entry_price: float, exit_price: float, pl: float, 
                           exit_date: datetime) -> Dict[str, Any]:
        """
        Create a standardized trade record.
        
        Args:
            symbol (str): Trading symbol
            trade_type (str): 'long' or 'short'
            shares (float): Number of shares
            entry_price (float): Entry price per share
            exit_price (float): Exit price per share
            pl (float): Profit/loss amount
            exit_date (datetime): Exit date
            
        Returns:
            Dict[str, Any]: Standardized trade record
        """
        return {
            'symbol': symbol,
            'type': trade_type,
            'entry': {
                'price': entry_price,
                'shares': shares,
                'amount': entry_price * shares
            },
            'exit': {
                'date': exit_date.isoformat() if isinstance(exit_date, pd.Timestamp) else str(exit_date),
                'price': exit_price,
                'shares': shares,
                'amount': exit_price * shares
            },
            'profit_loss': pl,
            'profit_loss_percent': (pl / (entry_price * shares)) * 100 if entry_price * shares != 0 else 0
        }

    def _calculate_final_stats(self, stats: Dict[str, Any], all_win_amounts: List[float],
                             all_loss_amounts: List[float], long_win_amounts: List[float],
                             long_loss_amounts: List[float], short_win_amounts: List[float],
                             short_loss_amounts: List[float]) -> Dict[str, Any]:
        """
        Calculate final statistics including win rates, averages, and ratios.
        
        Args:
            stats (Dict[str, Any]): Current statistics
            *_amounts (List[float]): Various win/loss amount lists
            
        Returns:
            Dict[str, Any]: Complete statistics with calculated metrics
        """
        # Calculate overall statistics
        total_trades = stats['overall']['wins'] + stats['overall']['losses']
        if total_trades > 0:
            stats['overall']['win_rate'] = (stats['overall']['wins'] / total_trades) * 100
            stats['overall']['avg_win'] = sum(all_win_amounts) / len(all_win_amounts) if all_win_amounts else 0
            stats['overall']['avg_loss'] = sum(abs(x) for x in all_loss_amounts) / len(all_loss_amounts) if all_loss_amounts else 0
            stats['overall']['win_loss_ratio'] = stats['overall']['avg_win'] / stats['overall']['avg_loss'] if stats['overall']['avg_loss'] > 0 else 0
        
        # Calculate long statistics
        long_trades = stats['long']['wins'] + stats['long']['losses']
        if long_trades > 0:
            stats['long']['win_rate'] = (stats['long']['wins'] / long_trades) * 100
            stats['long']['avg_win'] = sum(long_win_amounts) / len(long_win_amounts) if long_win_amounts else 0
            stats['long']['avg_loss'] = sum(abs(x) for x in long_loss_amounts) / len(long_loss_amounts) if long_loss_amounts else 0
            stats['long']['win_loss_ratio'] = stats['long']['avg_win'] / stats['long']['avg_loss'] if stats['long']['avg_loss'] > 0 else 0
        
        # Calculate short statistics
        short_trades = stats['short']['wins'] + stats['short']['losses']
        if short_trades > 0:
            stats['short']['win_rate'] = (stats['short']['wins'] / short_trades) * 100
            stats['short']['avg_win'] = sum(short_win_amounts) / len(short_win_amounts) if short_win_amounts else 0
            stats['short']['avg_loss'] = sum(abs(x) for x in short_loss_amounts) / len(short_loss_amounts) if short_loss_amounts else 0
            stats['short']['win_loss_ratio'] = stats['short']['avg_win'] / stats['short']['avg_loss'] if stats['short']['avg_loss'] > 0 else 0
        
        return stats

    def display_win_loss_stats(self, stats: Dict[str, Any]):
        """
        Display win/loss statistics in a formatted table.
        
        Args:
            stats (Dict[str, Any]): Statistics from calculate_win_loss_stats
        """
        print("\nWin/Loss Statistics:")
        print("-" * 70)
        
        # Display symbol-level stats
        for symbol, symbol_stats in stats.get('symbols', {}).items():
            total = symbol_stats['total']
            long = symbol_stats['long']
            short = symbol_stats['short']
            
            print(f"{symbol}: {total['wins']} wins, {total['losses']} losses "
                  f"({total.get('win_rate', 0):.1f}% win rate), P&L: ${total['profit_loss']:.2f}")
            
            if long['wins'] + long['losses'] > 0:
                print(f"  Long: {long['wins']} wins, {long['losses']} losses "
                      f"({long.get('win_rate', 0):.1f}% win rate), P&L: ${long['profit_loss']:.2f}")
            
            if short['wins'] + short['losses'] > 0:
                print(f"  Short: {short['wins']} wins, {short['losses']} losses "
                      f"({short.get('win_rate', 0):.1f}% win rate), P&L: ${short['profit_loss']:.2f}")
        
        # Display overall stats
        overall = stats['overall']
        long = stats['long']
        short = stats['short']
        
        print("-" * 70)
        print(f"Overall: {overall['wins']} wins, {overall['losses']} losses "
              f"({overall.get('win_rate', 0):.1f}% win rate)")
        print(f"Total P&L: ${overall['profit_loss']:.2f}")
        print(f"Avg Win: ${overall.get('avg_win', 0):.2f}, Avg Loss: ${overall.get('avg_loss', 0):.2f}, "
              f"Ratio: {overall.get('win_loss_ratio', 0):.2f}")
        
        if long['wins'] + long['losses'] > 0:
            print(f"\nLong Strategy: {long['wins']} wins, {long['losses']} losses "
                  f"({long.get('win_rate', 0):.1f}% win rate), P&L: ${long['profit_loss']:.2f}")
            print(f"  Avg Win: ${long.get('avg_win', 0):.2f}, Avg Loss: ${long.get('avg_loss', 0):.2f}, "
                  f"Ratio: {long.get('win_loss_ratio', 0):.2f}")
        
        if short['wins'] + short['losses'] > 0:
            print(f"\nShort Strategy: {short['wins']} wins, {short['losses']} losses "
                  f"({short.get('win_rate', 0):.1f}% win rate), P&L: ${short['profit_loss']:.2f}")
            print(f"  Avg Win: ${short.get('avg_win', 0):.2f}, Avg Loss: ${short.get('avg_loss', 0):.2f}, "
                  f"Ratio: {short.get('win_loss_ratio', 0):.2f}")

    def export_to_json(self, df: pd.DataFrame, stats: Dict[str, Any] = None, 
                      filename: str = None, include_transactions: bool = False) -> Dict[str, Any]:
        """
        Export transaction analysis to JSON format.
        
        Args:
            df (pd.DataFrame): Transaction data
            stats (Dict[str, Any], optional): Pre-calculated statistics
            filename (str, optional): Output filename (default: auto-generated)
            include_transactions (bool): Whether to include all transaction data
            
        Returns:
            Dict[str, Any]: Exported data structure
        """
        if df.empty:
            print("No transactions to export to JSON")
            return {}

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
        
        # Add statistics if available
        if stats:
            output["win_loss_analysis"] = stats
        
        # Determine output file
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pnl_analysis_{timestamp}.json"
        
        # Ensure the transaction_data directory exists
        transaction_dir = "transaction_data"
        os.makedirs(transaction_dir, exist_ok=True)
        
        # Create full path with transaction_data directory
        full_path = os.path.join(transaction_dir, filename)
        
        # Write to file
        with open(full_path, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"Saved P&L analysis to {full_path}")
        return output

    def analyze_transactions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive analysis on transaction data.
        
        Args:
            df (pd.DataFrame): Transaction data
            
        Returns:
            Dict[str, Any]: Analysis results
        """
        if df.empty:
            print("No transactions to analyze")
            return {}
            
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

    def insert_pnl_statistics_to_db(self, stats: Dict[str, Any]) -> bool:
        """Insert P&L statistics into PostgreSQL database with truncation"""
        try:
            # Database connection parameters
            conn_params = {
                'host': 'localhost',
                'database': 'volflow_options',
                'user': 'isaac',
                'password': None  # Will use peer authentication
            }
            
            # Connect to database
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Truncate the table first
            print("🗑️  Truncating pnl_statistics table...")
            # Use DELETE instead of TRUNCATE to avoid relation locks in parallel execution
            cur.execute("DELETE FROM pnl_statistics;")
            
            # Insert single row with all statistics
            insert_count = 0
            
            # Prepare insert statement for the existing schema
            insert_sql = """
            INSERT INTO pnl_statistics (
                timestamp, overall_wins, overall_losses, overall_profit_loss, overall_win_rate,
                overall_avg_win, overall_avg_loss, overall_win_loss_ratio,
                long_wins, long_losses, long_profit_loss, long_win_rate,
                long_avg_win, long_avg_loss,
                short_wins, short_losses, short_profit_loss, short_win_rate,
                short_avg_win, short_avg_loss
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Extract statistics
            overall = stats.get('overall', {})
            long_stats = stats.get('long', {})
            short_stats = stats.get('short', {})
            
            values = (
                datetime.now(),
                overall.get('wins', 0),
                overall.get('losses', 0),
                overall.get('profit_loss', 0.0),
                overall.get('win_rate', 0.0),
                overall.get('avg_win', 0.0),
                overall.get('avg_loss', 0.0),
                overall.get('win_loss_ratio', 0.0),
                long_stats.get('wins', 0),
                long_stats.get('losses', 0),
                long_stats.get('profit_loss', 0.0),
                long_stats.get('win_rate', 0.0),
                long_stats.get('avg_win', 0.0),
                long_stats.get('avg_loss', 0.0),
                short_stats.get('wins', 0),
                short_stats.get('losses', 0),
                short_stats.get('profit_loss', 0.0),
                short_stats.get('win_rate', 0.0),
                short_stats.get('avg_win', 0.0),
                short_stats.get('avg_loss', 0.0)
            )
            
            cur.execute(insert_sql, values)
            insert_count += 1
            
            # Commit the transaction
            conn.commit()
            
            # Close connections
            cur.close()
            conn.close()
            
            print(f"✅ Successfully inserted {insert_count} P&L statistics into database")
            return True
            
        except Exception as e:
            print(f"❌ Error inserting P&L statistics to database: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def analyze_and_store_transactions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze transactions and store results to JSON file"""
        try:
            # Calculate win/loss statistics
            stats = self.calculate_win_loss_stats(df)
            
            # Export to JSON file (for historical records)
            self.export_to_json(df, stats)
            
            # Create pnl_statistics.json for db_inserter
            self.create_pnl_statistics_json(stats)
            
            return stats
            
        except Exception as e:
            print(f"❌ Error analyzing and storing transactions: {e}")
            return {}

    def create_pnl_statistics_json(self, stats: Dict[str, Any]) -> bool:
        """Create pnl_statistics.json file for database insertion"""
        try:
            # Create the JSON structure that db_inserter expects
            pnl_data = {
                'strategy_name': 'PnL_Statistics',
                'last_updated': datetime.now().isoformat(),
                'total_statistics': 1,
                'statistics': {
                    'overall_performance': {
                        'account': 'All_Accounts',
                        'total_pnl': stats.get('overall', {}).get('profit_loss', 0.0),
                        'realized_pnl': stats.get('overall', {}).get('profit_loss', 0.0),  # Assuming all P&L is realized
                        'unrealized_pnl': 0.0,  # Would need position data for this
                        'total_trades': stats.get('overall', {}).get('wins', 0) + stats.get('overall', {}).get('losses', 0),
                        'winning_trades': stats.get('overall', {}).get('wins', 0),
                        'losing_trades': stats.get('overall', {}).get('losses', 0),
                        'win_rate': stats.get('overall', {}).get('win_rate', 0.0),
                        'avg_win': stats.get('overall', {}).get('avg_win', 0.0),
                        'avg_loss': stats.get('overall', {}).get('avg_loss', 0.0),
                        'profit_factor': stats.get('overall', {}).get('win_loss_ratio', 0.0),
                        'max_drawdown': 0.0,  # Would need to calculate from trade sequence
                        'sharpe_ratio': 0.0,  # Would need returns data
                        'sortino_ratio': 0.0,  # Would need returns data
                        'calmar_ratio': 0.0   # Would need returns data
                    },
                    'long_performance': {
                        'account': 'Long_Strategy',
                        'total_pnl': stats.get('long', {}).get('profit_loss', 0.0),
                        'realized_pnl': stats.get('long', {}).get('profit_loss', 0.0),
                        'unrealized_pnl': 0.0,
                        'total_trades': stats.get('long', {}).get('wins', 0) + stats.get('long', {}).get('losses', 0),
                        'winning_trades': stats.get('long', {}).get('wins', 0),
                        'losing_trades': stats.get('long', {}).get('losses', 0),
                        'win_rate': stats.get('long', {}).get('win_rate', 0.0),
                        'avg_win': stats.get('long', {}).get('avg_win', 0.0),
                        'avg_loss': stats.get('long', {}).get('avg_loss', 0.0),
                        'profit_factor': stats.get('long', {}).get('win_loss_ratio', 0.0),
                        'max_drawdown': 0.0,
                        'sharpe_ratio': 0.0,
                        'sortino_ratio': 0.0,
                        'calmar_ratio': 0.0
                    },
                    'short_performance': {
                        'account': 'Short_Strategy',
                        'total_pnl': stats.get('short', {}).get('profit_loss', 0.0),
                        'realized_pnl': stats.get('short', {}).get('profit_loss', 0.0),
                        'unrealized_pnl': 0.0,
                        'total_trades': stats.get('short', {}).get('wins', 0) + stats.get('short', {}).get('losses', 0),
                        'winning_trades': stats.get('short', {}).get('wins', 0),
                        'losing_trades': stats.get('short', {}).get('losses', 0),
                        'win_rate': stats.get('short', {}).get('win_rate', 0.0),
                        'avg_win': stats.get('short', {}).get('avg_win', 0.0),
                        'avg_loss': stats.get('short', {}).get('avg_loss', 0.0),
                        'profit_factor': stats.get('short', {}).get('win_loss_ratio', 0.0),
                        'max_drawdown': 0.0,
                        'sharpe_ratio': 0.0,
                        'sortino_ratio': 0.0,
                        'calmar_ratio': 0.0
                    }
                },
                'metadata': {
                    'strategy_type': 'pnl_analysis',
                    'analysis_method': 'transaction_based_win_loss_calculation',
                    'update_frequency': 'on_demand',
                    'data_source': 'schwab_transactions',
                    'calculation_date': datetime.now().isoformat()
                }
            }
            
            # Write to pnl_statistics.json in root directory
            with open('pnl_statistics.json', 'w') as f:
                json.dump(pnl_data, f, indent=2)
            
            print(f"✅ Created pnl_statistics.json for database insertion")
            print(f"📊 Statistics: {pnl_data['statistics']['overall_performance']['total_trades']} trades, "
                  f"${pnl_data['statistics']['overall_performance']['total_pnl']:.2f} P&L, "
                  f"{pnl_data['statistics']['overall_performance']['win_rate']:.1f}% win rate")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating pnl_statistics.json: {e}")
            return False


def main():
    """Main function - automatically analyze P&L and store in database"""
    print("P&L Data Handler - Transaction Analysis")
    print("=" * 50)
    
    # Initialize handler
    handler = PnLDataHandler()
    
    # First, we need transaction data to analyze
    # Import the transaction handler to get fresh data
    from schwab_transaction_handler import SchwabTransactionHandler
    
    print("Fetching transaction data for P&L analysis...")
    transaction_handler = SchwabTransactionHandler()
    
    # Get transactions for the last 30 days
    df = transaction_handler.get_all_transactions(days=30, csv_output=False)
    
    if not df.empty:
        print(f"Analyzing {len(df)} transactions for P&L statistics...")
        
        # Analyze and store P&L statistics
        stats = handler.analyze_and_store_transactions(df)
        
        if stats:
            print(f"✅ P&L analysis completed")
            
            # Display summary statistics
            overall = stats.get('overall', {})
            long_stats = stats.get('long', {})
            short_stats = stats.get('short', {})
            
            print(f"📊 Overall Performance:")
            print(f"   Total Trades: {overall.get('wins', 0) + overall.get('losses', 0)}")
            print(f"   Win Rate: {overall.get('win_rate', 0):.1f}%")
            print(f"   Total P&L: ${overall.get('profit_loss', 0):.2f}")
            print(f"   Avg Win: ${overall.get('avg_win', 0):.2f}")
            print(f"   Avg Loss: ${overall.get('avg_loss', 0):.2f}")
            
            if long_stats.get('wins', 0) + long_stats.get('losses', 0) > 0:
                print(f"📈 Long Strategy:")
                print(f"   Trades: {long_stats.get('wins', 0) + long_stats.get('losses', 0)}")
                print(f"   Win Rate: {long_stats.get('win_rate', 0):.1f}%")
                print(f"   P&L: ${long_stats.get('profit_loss', 0):.2f}")
            
            if short_stats.get('wins', 0) + short_stats.get('losses', 0) > 0:
                print(f"📉 Short Strategy:")
                print(f"   Trades: {short_stats.get('wins', 0) + short_stats.get('losses', 0)}")
                print(f"   Win Rate: {short_stats.get('win_rate', 0):.1f}%")
                print(f"   P&L: ${short_stats.get('profit_loss', 0):.2f}")
        else:
            print("❌ P&L analysis failed")
    else:
        print("No transaction data available for P&L analysis")


if __name__ == "__main__":
    main()
