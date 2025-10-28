#!/usr/bin/env python3
"""
Database Query Handler for Dashboard Data

This handler provides all database queries needed for the dashboard:
1. Trading statistics and P&L data
2. Current positions
3. Strategy signals (Iron Condor, PML, Divergence)
4. Account data
5. Watchlist data
6. Market status and real-time data
7. Recent transactions
8.

All data for the dashboard comes from PostgreSQL instead of JSON files.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import json

class DatabaseQueryHandler:
    """
    Centralized database query handler for dashboard data
    """
    
    def __init__(self):
        """Initialize the database query handler."""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Database connection parameters
        self.db_config = {
            'host': 'localhost',
            'database': 'volflow_options',
            'user': 'isaac',
            'password': None  # Will use peer authentication
        }
        
        self.logger.info("DatabaseQueryHandler initialized")

    def get_connection(self):
        """Get database connection."""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            self.logger.error(f"❌ Error connecting to database: {e}")
            return None

    def get_trading_statistics(self) -> Dict[str, Any]:
        """Get comprehensive trading statistics from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get latest P&L statistics
            cur.execute("""
                SELECT * FROM pnl_statistics 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            
            pnl_stats = cur.fetchone()
            
            if not pnl_stats:
                cur.close()
                conn.close()
                return self.get_empty_trading_statistics()
            
            # Convert to dictionary - all calculations are already done in database
            stats = dict(pnl_stats)
            
            # Use pre-calculated values directly from database
            trading_stats = {
                'total_trades': int(stats['overall_wins']) + int(stats['overall_losses']),
                'total_wins': int(stats['overall_wins']),
                'total_losses': int(stats['overall_losses']),
                'win_rate': float(stats['overall_win_rate'] or 0) / 100.0,  # Convert percentage to decimal
                'avg_win': float(stats['overall_avg_win'] or 0),
                'avg_loss': float(stats['overall_avg_loss'] or 0),
                'total_pl': float(stats['overall_profit_loss'] or 0),
                'win_loss_ratio': float(stats['overall_win_loss_ratio'] or 0),
                'profit_factor': 0.0,  # Not stored in database, would need separate calculation
                'long_performance': {
                    'wins': int(stats['long_wins'] or 0),
                    'losses': int(stats['long_losses'] or 0),
                    'win_rate': float(stats['long_win_rate'] or 0) / 100.0,  # Convert percentage to decimal
                    'avg_win': float(stats['long_avg_win'] or 0),
                    'avg_loss': float(stats['long_avg_loss'] or 0),
                    'total_pl': float(stats['long_profit_loss'] or 0)
                },
                'short_performance': {
                    'wins': int(stats['short_wins'] or 0),
                    'losses': int(stats['short_losses'] or 0),
                    'win_rate': float(stats['short_win_rate'] or 0) / 100.0,  # Convert percentage to decimal
                    'avg_win': float(stats['short_avg_win'] or 0),
                    'avg_loss': float(stats['short_avg_loss'] or 0),
                    'total_pl': float(stats['short_profit_loss'] or 0)
                },
                'last_updated': stats['timestamp'].isoformat() if stats['timestamp'] else datetime.now().isoformat()
            }
            
            cur.close()
            conn.close()
            
            self.logger.info(f"📊 Retrieved trading statistics: {trading_stats['total_trades']} trades, {trading_stats['win_rate']:.1%} win rate")
            return trading_stats
            
        except Exception as e:
            self.logger.error(f"❌ Error getting trading statistics: {e}")
            return self.get_empty_trading_statistics()

    def get_empty_trading_statistics(self) -> Dict[str, Any]:
        """Return empty trading statistics structure."""
        return {
            'total_trades': 0,
            'total_wins': 0,
            'total_losses': 0,
            'win_rate': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'total_pl': 0.0,
            'win_loss_ratio': 0.0,
            'profit_factor': 0.0,
            'long_performance': {
                'wins': 0, 'losses': 0, 'win_rate': 0.0,
                'avg_win': 0.0, 'avg_loss': 0.0, 'total_pl': 0.0
            },
            'short_performance': {
                'wins': 0, 'losses': 0, 'win_rate': 0.0,
                'avg_win': 0.0, 'avg_loss': 0.0, 'total_pl': 0.0
            },
            'last_updated': datetime.now().isoformat()
        }

    def get_current_positions(self) -> Dict[str, Any]:
        """Get current positions from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get all current positions
            cur.execute("""
                SELECT * FROM positions 
                ORDER BY timestamp DESC
            """)
            
            positions_data = cur.fetchall()
            
            positions = {}
            total_market_value = 0.0
            total_unrealized_pl = 0.0
            total_cost_basis = 0.0
            
            for pos in positions_data:
                symbol = pos['symbol']
                positions[symbol] = {
                    'symbol': symbol,
                    'quantity': pos['quantity'],
                    'market_value': pos['market_value'],
                    'cost_basis': pos['cost_basis'],
                    'unrealized_pl': pos['unrealized_pl'],
                    'unrealized_pl_percent': pos['unrealized_pl_percent'],
                    'account': pos['account'],
                    'timestamp': pos['timestamp'].isoformat() if pos['timestamp'] else None
                }
                
                total_market_value += float(pos['market_value'] or 0)
                total_unrealized_pl += float(pos['unrealized_pl'] or 0)
                total_cost_basis += float(pos['cost_basis'] or 0)
            
            summary = {
                'total_positions': len(positions),
                'total_market_value': total_market_value,
                'total_unrealized_pl': total_unrealized_pl,
                'total_cost_basis': total_cost_basis,
                'symbols_count': len(positions)
            }
            
            cur.close()
            conn.close()
            
            self.logger.info(f"📈 Retrieved {len(positions)} positions from database")
            return {
                'positions': positions,
                'summary': summary,
                'last_updated': datetime.now().isoformat(),
                'data_source': 'postgresql'
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error getting positions: {e}")
            return {}

    def get_iron_condor_signals(self) -> List[Dict[str, Any]]:
        """Get Iron Condor signals from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM iron_condor_signals 
                ORDER BY timestamp DESC
            """)
            
            signals_data = cur.fetchall()
            
            signals = []
            for signal in signals_data:
                signals.append({
                    'symbol': signal['symbol'],
                    'signal_type': signal['signal_type'],
                    'confidence': signal['confidence'],
                    'entry_reason': signal['entry_reason'],
                    'position_size': signal['position_size'],
                    'stop_loss': signal['stop_loss'],
                    'profit_target': signal['profit_target'],
                    'expiration_date': signal['expiration_date'],
                    'dte': signal['dte'],
                    'net_credit': signal['net_credit'],
                    'max_profit': signal['max_profit'],
                    'max_loss': signal['max_loss'],
                    'prob_profit': signal['prob_profit'],
                    'auto_approve': signal['auto_approve'],
                    'timestamp': signal['timestamp'].isoformat() if signal['timestamp'] else None
                })
            
            cur.close()
            conn.close()
            
            self.logger.info(f"🎯 Retrieved {len(signals)} Iron Condor signals from database")
            return signals
            
        except Exception as e:
            self.logger.error(f"❌ Error getting Iron Condor signals: {e}")
            return []

    def get_pml_signals(self) -> List[Dict[str, Any]]:
        """Get PML signals from database using exceedence structure."""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM pml_signals 
                ORDER BY timestamp DESC
            """)
            
            signals_data = cur.fetchall()
            
            signals = []
            for signal in signals_data:
                # Parse existing_position JSON if it exists
                existing_position = {}
                if signal['existing_position']:
                    try:
                        existing_position = json.loads(signal['existing_position'])
                    except (json.JSONDecodeError, TypeError):
                        existing_position = {}
                
                signals.append({
                    'symbol': signal['symbol'],
                    'signal_type': signal['signal_type'],
                    'entry_reason': signal['entry_reason'],
                    'position_size': signal['position_size'],
                    'auto_approve': signal['auto_approve'],
                    'current_price': float(signal['current_price']) if signal['current_price'] else 0.0,
                    'position_in_range': float(signal['position_in_range']) if signal['position_in_range'] else 0.0,
                    'high_exceedance': float(signal['high_exceedance']) if signal['high_exceedance'] else 0.0,
                    'low_exceedance': float(signal['low_exceedance']) if signal['low_exceedance'] else 0.0,
                    'market_condition': signal['market_condition'],
                    'has_trade_signal': signal['has_trade_signal'],
                    'is_scale_in': signal['is_scale_in'],
                    'existing_position': existing_position,
                    'signal_id': signal['signal_id'],
                    'strategy_name': signal['strategy_name'],
                    'timestamp': signal['timestamp'].isoformat() if signal['timestamp'] else None
                })
            
            cur.close()
            conn.close()
            
            self.logger.info(f"📊 Retrieved {len(signals)} PML signals from database")
            return signals
            
        except Exception as e:
            self.logger.error(f"❌ Error getting PML signals: {e}")
            return []

    def get_divergence_signals(self) -> List[Dict[str, Any]]:
        """Get Divergence signals from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM divergence_signals 
                ORDER BY timestamp DESC
            """)
            
            signals_data = cur.fetchall()
            
            signals = []
            for signal in signals_data:
                signals.append({
                    'symbol': signal['symbol'],
                    'signal_type': signal['signal_type'],
                    'confidence': signal['confidence'],
                    'entry_reason': signal['entry_reason'],
                    'stop_loss': signal['stop_loss'],
                    'profit_target': signal['profit_target'],
                    'divergence_type': signal['divergence_type'],
                    'direction': signal['direction'],
                    'current_price': signal['current_price'],
                    'entry_price': signal['entry_price'],
                    'take_profit': signal['take_profit'],
                    'reward_risk_ratio': signal['reward_risk_ratio'],
                    'auto_approve': signal['auto_approve'],
                    'timestamp': signal['timestamp'].isoformat() if signal['timestamp'] else None
                })
            
            cur.close()
            conn.close()
            
            self.logger.info(f"📈 Retrieved {len(signals)} Divergence signals from database")
            return signals
            
        except Exception as e:
            self.logger.error(f"❌ Error getting Divergence signals: {e}")
            return []


    def get_account_data(self) -> Dict[str, Any]:
        """Get account data from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM account_data 
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            
            accounts_data = cur.fetchall()
            
            accounts = {}
            for account in accounts_data:
                account_number = account['account_number']
                accounts[account_number] = {
                    'account_number': account_number,
                    'equity': account['equity'],
                    'buying_power': account['buying_power'],
                    'available_funds': account['available_funds'],
                    'total_market_value': account['total_market_value'],
                    'total_unrealized_pl': account['total_unrealized_pl'],
                    'total_day_pl': account['total_day_pl'],
                    'is_day_trader': account['is_day_trader'],
                    'timestamp': account['timestamp'].isoformat() if account['timestamp'] else None
                }
            
            cur.close()
            conn.close()
            
            self.logger.info(f"💰 Retrieved account data for {len(accounts)} accounts from database")
            return {
                'accounts': accounts,
                'last_updated': datetime.now().isoformat(),
                'data_source': 'postgresql'
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error getting account data: {e}")
            return {}

    def get_watchlist_data(self) -> List[Dict[str, Any]]:
        """Get watchlist data from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM integrated_watchlist 
                ORDER BY timestamp DESC
            """)
            
            watchlist_data = cur.fetchall()
            
            watchlist = []
            for item in watchlist_data:
                watchlist.append({
                    'symbol': item['symbol'],
                    'current_price': item['current_price'],
                    'price_change': item['price_change'],
                    'price_change_percent': item['price_change_percent'],
                    'volume': item['volume'],
                    'market_status': item['market_status'],
                    'timestamp': item['timestamp'].isoformat() if item['timestamp'] else None
                })
            
            cur.close()
            conn.close()
            
            self.logger.info(f"📋 Retrieved watchlist data for {len(watchlist)} symbols from database")
            return watchlist
            
        except Exception as e:
            self.logger.error(f"❌ Error getting watchlist data: {e}")
            return []

    def get_recent_transactions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent transactions from database."""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute(f"""
                SELECT * FROM transactions 
                ORDER BY timestamp DESC
                LIMIT {limit}
            """)
            
            transactions_data = cur.fetchall()
            
            transactions = []
            for txn in transactions_data:
                transactions.append({
                    'transaction_id': txn['transaction_id'],
                    'symbol': txn['symbol'],
                    'transaction_type': txn['transaction_type'],
                    'quantity': txn['quantity'],
                    'price': txn['price'],
                    'amount': txn['amount'],
                    'fees': txn['fees'],
                    'account': txn['account'],
                    'timestamp': txn['timestamp'].isoformat() if txn['timestamp'] else None
                })
            
            cur.close()
            conn.close()
            
            self.logger.info(f"💳 Retrieved {len(transactions)} recent transactions from database")
            return transactions
            
        except Exception as e:
            self.logger.error(f"❌ Error getting recent transactions: {e}")
            return []

    def get_market_status(self) -> Dict[str, Any]:
        """Get current market status (calculated, not from database)."""
        now = datetime.now()
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        is_market_hours = market_open <= now <= market_close
        is_weekday = now.weekday() < 5
        
        return {
            'current_time': now.isoformat(),
            'market_open_time': market_open.isoformat(),
            'market_close_time': market_close.isoformat(),
            'is_market_hours': is_market_hours and is_weekday,
            'is_weekday': is_weekday,
            'session_status': 'OPEN' if (is_market_hours and is_weekday) else 'CLOSED',
            'minutes_to_open': max(0, (market_open - now).total_seconds() / 60) if not is_market_hours else 0,
            'minutes_to_close': max(0, (market_close - now).total_seconds() / 60) if is_market_hours else 0
        }

    def get_comprehensive_dashboard_data(self) -> Dict[str, Any]:
        """Get all dashboard data from PostgreSQL database."""
        try:
            self.logger.info("🔄 Fetching comprehensive dashboard data from PostgreSQL...")
            
            # Get all data from database
            trading_stats = self.get_trading_statistics()
            positions_data = self.get_current_positions()
            iron_condor_signals = self.get_iron_condor_signals()
            pml_signals = self.get_pml_signals()
            divergence_signals = self.get_divergence_signals()
            account_data = self.get_account_data()
            watchlist_data = self.get_watchlist_data()
            recent_transactions = self.get_recent_transactions()
            market_status = self.get_market_status()
            
            # Compile comprehensive data
            comprehensive_data = {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'postgresql',
                    'query_handler': 'DatabaseQueryHandler',
                    'total_positions': len(positions_data.get('positions', {})),
                    'total_iron_condor_signals': len(iron_condor_signals),
                    'total_pml_signals': len(pml_signals),
                    'total_divergence_signals': len(divergence_signals),
                    'total_watchlist_symbols': len(watchlist_data),
                    'total_recent_transactions': len(recent_transactions)
                },
                'trading_statistics': trading_stats,
                'positions': positions_data,
                'iron_condor_signals': iron_condor_signals,
                'pml_signals': pml_signals,
                'divergence_signals': divergence_signals,
                'account_data': account_data,
                'watchlist_data': watchlist_data,
                'recent_transactions': recent_transactions,
                'market_status': market_status,
                'system_status': {
                    'database_connected': True,
                    'last_updated': datetime.now().isoformat()
                }
            }
            
            self.logger.info("✅ Successfully fetched all dashboard data from PostgreSQL")
            return comprehensive_data
            
        except Exception as e:
            self.logger.error(f"❌ Error getting comprehensive dashboard data: {e}")
            return {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'postgresql',
                    'error': str(e)
                },
                'error': True,
                'system_status': {
                    'database_connected': False,
                    'last_updated': datetime.now().isoformat()
                }
            }


def main():
    """Main function to test database queries"""
    print("Database Query Handler Test")
    print("=" * 40)
    
    # Initialize query handler
    query_handler = DatabaseQueryHandler()
    
    # Test comprehensive data retrieval
    data = query_handler.get_comprehensive_dashboard_data()
    
    # Display results
    print(f"\n📊 Dashboard Data Summary:")
    print(f"   Trading Statistics: {data.get('trading_statistics', {}).get('total_trades', 0)} trades")
    print(f"   Current Positions: {len(data.get('positions', {}).get('positions', {}))}")
    print(f"   Iron Condor Signals: {len(data.get('iron_condor_signals', []))}")
    print(f"   PML Signals: {len(data.get('pml_signals', []))}")
    print(f"   Divergence Signals: {len(data.get('divergence_signals', []))}")
    print(f"   Watchlist Symbols: {len(data.get('watchlist_data', []))}")
    print(f"   Recent Transactions: {len(data.get('recent_transactions', []))}")
    
    print(f"\n🎯 Data Source: {data.get('metadata', {}).get('data_source', 'unknown')}")


if __name__ == "__main__":
    main()
