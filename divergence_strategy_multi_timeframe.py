#!/usr/bin/env python3
"""
Multi-Timeframe Divergence Strategy Signal Generator

This strategy identifies bullish and bearish divergences between price and momentum indicators
across multiple timeframes (1min, 5min, 15min) to generate high-confidence trading signals. 
It requires divergence confirmation on both 5-minute and 15-minute timeframes before 
generating signals based on 1-minute execution timing.

Key Features:
1. Multi-timeframe divergence detection (1min, 5min, 15min)
2. Divergence confirmation requirement across timeframes
3. 1-minute precision for trade execution timing
4. Enhanced signal confidence through timeframe alignment
5. Risk/reward calculations with multi-timeframe context

Usage: python3 divergence_strategy_multi_timeframe.py
"""

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import subprocess
import sys
import time
import signal as signal_module
import threading
from dataclasses import dataclass
from enum import Enum

# Import trading components
try:
    from order_handler import OrderHandler
    TRADING_ENABLED = True
except ImportError:
    TRADING_ENABLED = False
    OrderHandler = None

# Risk configuration will be loaded from risk_config_live.json

class MultiTimeframeDivergenceStrategy:
    """
    Multi-timeframe divergence strategy that requires confirmation across timeframes
    """
    
    def __init__(self):
        """Initialize the Multi-Timeframe Divergence Strategy."""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize config loaded flag
        self.config_loaded = False
        
        # Initialize shutdown flag for continuous operation
        self.shutdown_requested = False
        
        # Load trading configuration from trading_config_live.json
        try:
            trading_config = self._load_trading_config()
            if trading_config and 'strategies' in trading_config and 'divergence' in trading_config['strategies']:
                divergence_config = trading_config['strategies']['divergence']
                risk_mgmt = divergence_config.get('risk_management', {})
                
                # Validate required risk management settings exist
                if not risk_mgmt:
                    self.logger.error("❌ No risk_management configuration found in divergence strategy")
                    raise ValueError("Missing risk_management configuration")
                
                # Load risk management settings (no defaults)
                if 'strategy_allocation' not in risk_mgmt:
                    self.logger.error("❌ Missing strategy_allocation in risk_management")
                    raise ValueError("Missing strategy_allocation")
                if 'position_size' not in risk_mgmt:
                    self.logger.error("❌ Missing position_size in risk_management")
                    raise ValueError("Missing position_size")
                if 'max_shares' not in risk_mgmt:
                    self.logger.error("❌ Missing max_shares in risk_management")
                    raise ValueError("Missing max_shares")
                
                strategy_allocation = risk_mgmt['strategy_allocation']
                position_size = risk_mgmt['position_size']
                self.max_shares = risk_mgmt['max_shares']
                self.auto_approve = divergence_config.get('auto_approve', False)
                
                # Convert percentages to decimals
                self.strategy_allocation_pct = strategy_allocation / 100.0
                self.position_size_pct = position_size / 100.0
                
                # Fixed risk parameters
                self.stop_loss_atr_multiplier = 1.5
                self.risk_reward_ratio = 2.0
                
                # Load watchlist symbols
                self.watchlist_symbols = divergence_config.get('divergence_strategy_watchlist', [])
                
                # Mark config as successfully loaded
                self.config_loaded = True
                
                self.logger.info(f"✅ Multi-Timeframe Divergence: Successfully loaded from trading_config_live.json:")
                self.logger.info(f"   Strategy allocation: {strategy_allocation}%")
                self.logger.info(f"   Position size: {position_size}%")
                self.logger.info(f"   Max shares: {self.max_shares}")
                self.logger.info(f"   Auto approve: {self.auto_approve}")
                self.logger.info(f"   Watchlist symbols: {len(self.watchlist_symbols)} symbols")
            else:
                self.logger.error("❌ Multi-Timeframe Divergence: No divergence strategy configuration found in trading_config_live.json")
                raise ValueError("Missing divergence strategy configuration")
        except Exception as e:
            self.logger.error(f"❌ Multi-Timeframe Divergence: Error loading trading config: {e}")
            self.logger.error("❌ Strategy will not run without proper configuration")
            self.config_loaded = False
            # Don't set any default values - strategy should not run
            return
        
        # Strategy parameters
        self.timeframes = ['1min', '5min', '15min']
        self.confirmation_timeframes = ['5min', '15min']  # Require confirmation on these
        self.execution_timeframe = '1min'  # Use 1min for precise execution timing
        
        self.logger.info("Multi-Timeframe Divergence Strategy initialized:")
        self.logger.info(f"  Timeframes: {', '.join(self.timeframes)}")
        self.logger.info(f"  Confirmation required on: {', '.join(self.confirmation_timeframes)}")
        self.logger.info(f"  Execution timeframe: {self.execution_timeframe}")
        self.logger.info(f"  Risk/Reward ratio: {self.risk_reward_ratio}")
        self.logger.info(f"  Stop loss ATR multiplier: {self.stop_loss_atr_multiplier}")


    def _generate_fresh_divergence_data(self) -> bool:
        """Generate fresh divergence indicators using the ultra-parallel calculator."""
        try:
            self.logger.info("🔄 Calling divergence indicators calculator...")
            
            # Run the divergence indicators calculator
            result = subprocess.run(
                [sys.executable, 'divergence_indicators_calculator.py'],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Successfully generated fresh divergence indicators")
                # Log key output lines
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if any(keyword in line for keyword in ['ULTRA-PARALLEL', 'completed in', 'Created', 'symbols']):
                        self.logger.info(f"   {line.strip()}")
                return True
            else:
                self.logger.error(f"❌ Divergence calculator failed with return code {result.returncode}")
                if result.stderr:
                    self.logger.error(f"   Error: {result.stderr.strip()}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Divergence calculator timed out after 5 minutes")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error running divergence calculator: {e}")
            return False

    def _load_trading_config(self) -> Dict[str, Any]:
        """Load trading configuration from trading_config_live.json"""
        try:
            trading_config_path = 'trading_config_live.json'
            
            if not os.path.exists(trading_config_path):
                self.logger.warning(f"Warning: trading_config_live.json not found at {trading_config_path}")
                return {}
            
            with open(trading_config_path, 'r') as f:
                data = json.load(f)
            
            self.logger.info(f"Loaded trading configuration from {trading_config_path}")
            return data
            
        except Exception as e:
            self.logger.error(f"Error loading trading configuration: {e}")
            return {}

    def _reload_trading_config(self) -> None:
        """Reload trading configuration to check for changes, especially auto_approve"""
        try:
            trading_config = self._load_trading_config()
            if trading_config and 'strategies' in trading_config and 'divergence' in trading_config['strategies']:
                divergence_config = trading_config['strategies']['divergence']
                
                # Update auto_approve setting dynamically
                old_auto_approve = self.auto_approve
                self.auto_approve = divergence_config.get('auto_approve', False)
                
                # Log if auto_approve changed
                if old_auto_approve != self.auto_approve:
                    self.logger.info(f"🔄 Auto approve setting changed: {old_auto_approve} → {self.auto_approve}")
                
                # Update watchlist if changed
                old_watchlist = self.watchlist_symbols
                self.watchlist_symbols = divergence_config.get('divergence_strategy_watchlist', [])
                
                if old_watchlist != self.watchlist_symbols:
                    self.logger.info(f"🔄 Watchlist updated: {len(old_watchlist)} → {len(self.watchlist_symbols)} symbols")
                    
        except Exception as e:
            self.logger.error(f"❌ Error reloading trading config: {e}")

    def load_multi_timeframe_data(self) -> Dict[str, Dict[str, Any]]:
        """Load divergence indicators from divergence_data directory."""
        timeframe_data = {}
        divergence_data_dir = 'divergence_data'
        
        for timeframe in self.timeframes:
            filename = f'divergence_indicators_{timeframe}.json'
            filepath = os.path.join(divergence_data_dir, filename)
            
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Extract indicators from the divergence data structure
                    timeframe_data[timeframe] = data.get('indicators', {})
                    self.logger.info(f"✅ Loaded {len(timeframe_data[timeframe])} symbols from {filepath}")
                else:
                    self.logger.warning(f"❌ File not found: {filepath}")
                    timeframe_data[timeframe] = {}
                    
            except Exception as e:
                self.logger.error(f"❌ Error loading {filepath}: {e}")
                timeframe_data[timeframe] = {}
        
        return timeframe_data

    def check_divergence_confirmation(self, symbol: str, timeframe_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Check for divergence confirmation across multiple timeframes.
        
        Args:
            symbol: Symbol to analyze
            timeframe_data: Data from all timeframes
            
        Returns:
            Dictionary with confirmation results
        """
        confirmation_result = {
            'symbol': symbol,
            'has_confirmation': False,
            'bullish_confirmed': False,
            'bearish_confirmed': False,
            'timeframe_signals': {},
            'confirmation_strength': 'none',
            'execution_data': {}
        }
        
        try:
            # Check each timeframe for divergence signals
            for timeframe in self.timeframes:
                if timeframe not in timeframe_data or symbol not in timeframe_data[timeframe]:
                    confirmation_result['timeframe_signals'][timeframe] = {
                        'available': False,
                        'bullish_divergence': False,
                        'bearish_divergence': False,
                        'strength': 'none'
                    }
                    continue
                
                # Data is now directly from divergence indicators (no nested structure)
                symbol_data = timeframe_data[timeframe][symbol]
                
                # Extract divergence information directly from symbol data
                bullish_div = symbol_data.get('bullish_divergence_detected', False)
                bearish_div = symbol_data.get('bearish_divergence_detected', False)
                strength = symbol_data.get('divergence_strength', 'none')
                
                confirmation_result['timeframe_signals'][timeframe] = {
                    'available': True,
                    'bullish_divergence': bullish_div,
                    'bearish_divergence': bearish_div,
                    'strength': strength,
                    'signal_type': symbol_data.get('signal_type', 'NO_SIGNAL'),
                    'trend_direction': symbol_data.get('trend_direction', 'neutral')
                }
            
            # Check for confirmation across required timeframes
            bullish_confirmations = []
            bearish_confirmations = []
            
            for timeframe in self.confirmation_timeframes:
                if timeframe in confirmation_result['timeframe_signals']:
                    tf_signal = confirmation_result['timeframe_signals'][timeframe]
                    if tf_signal['available']:
                        if tf_signal['bullish_divergence']:
                            bullish_confirmations.append(timeframe)
                        if tf_signal['bearish_divergence']:
                            bearish_confirmations.append(timeframe)
            
            # Determine confirmation status
            required_confirmations = len(self.confirmation_timeframes)
            
            if len(bullish_confirmations) >= required_confirmations:
                confirmation_result['bullish_confirmed'] = True
                confirmation_result['has_confirmation'] = True
                confirmation_result['confirmation_strength'] = self._calculate_confirmation_strength(bullish_confirmations, timeframe_data, symbol, 'bullish')
            
            if len(bearish_confirmations) >= required_confirmations:
                confirmation_result['bearish_confirmed'] = True
                confirmation_result['has_confirmation'] = True
                confirmation_result['confirmation_strength'] = self._calculate_confirmation_strength(bearish_confirmations, timeframe_data, symbol, 'bearish')
            
            # Get execution data from 1-minute timeframe
            if self.execution_timeframe in timeframe_data and symbol in timeframe_data[self.execution_timeframe]:
                exec_data = timeframe_data[self.execution_timeframe][symbol]
                confirmation_result['execution_data'] = {
                    'current_price': exec_data.get('current_price', 0),
                    'atr': 0,  # ATR not available in divergence data, will calculate from price
                    'rsi': exec_data.get('current_rsi', 50),
                    'trend': exec_data.get('trend_direction', 'neutral'),
                    'volume': exec_data.get('current_volume', 0)
                }
            
            return confirmation_result
            
        except Exception as e:
            self.logger.error(f"Error checking divergence confirmation for {symbol}: {e}")
            return confirmation_result

    def _calculate_confirmation_strength(self, confirmations: List[str], timeframe_data: Dict[str, Dict[str, Any]], 
                                       symbol: str, direction: str) -> str:
        """Calculate the strength of multi-timeframe confirmation."""
        try:
            strength_scores = []
            
            for timeframe in confirmations:
                if timeframe in timeframe_data and symbol in timeframe_data[timeframe]:
                    symbol_data = timeframe_data[timeframe][symbol]
                    strength = symbol_data.get('divergence_strength', 'weak')
                    
                    # Convert strength to numeric score
                    if strength == 'strong':
                        strength_scores.append(3)
                    elif strength == 'medium':
                        strength_scores.append(2)
                    else:  # weak
                        strength_scores.append(1)
            
            if not strength_scores:
                return 'weak'
            
            avg_strength = sum(strength_scores) / len(strength_scores)
            
            if avg_strength >= 2.5:
                return 'strong'
            elif avg_strength >= 1.5:
                return 'medium'
            else:
                return 'weak'
                
        except Exception:
            return 'weak'

    def load_account_data(self) -> Dict[str, Any]:
        """Load current account data from account_data.json"""
        with open('account_data.json', 'r') as f:
            data = json.load(f)
        
        # Extract first account data
        account_data = data.get('account_data', {})
        return list(account_data.values())[0]
    
    def load_current_positions(self) -> Dict[str, Any]:
        """Load current positions data from current_positions.json"""
        with open('current_positions.json', 'r') as f:
            return json.load(f)
    
    def calculate_comprehensive_position_size(self, symbol: str, current_price: float, 
                                            signal_strength: str, stop_loss_price: float) -> Dict[str, Any]:
        """
        Calculate comprehensive position size considering all risk factors
        
        Args:
            symbol: Symbol to trade
            current_price: Current market price
            signal_strength: Signal strength ('weak', 'medium', 'strong')
            stop_loss_price: Stop loss price for risk calculation
            
        Returns:
            Dictionary with position sizing details and risk analysis
        """
        # Load all required data
        account_data = self.load_account_data()
        positions_data = self.load_current_positions()
        trading_config = self._load_trading_config()
        
        # Extract key values
        equity = account_data.get('equity', 0.0)
        available_funds = account_data.get('available_funds', 0.0)
        total_day_pl = account_data.get('total_day_pl', 0.0)
        
        # Get risk limits from config and check if they're enabled
        risk_management = trading_config.get('risk_management', {})
        account_limits = risk_management.get('account_limits', {})
        parameter_states = risk_management.get('parameter_states', {})
        
        # Only use limits if they're enabled
        daily_loss_limit_pct = account_limits.get('daily_loss_limit', 3.0) if parameter_states.get('enable_daily_loss_limit', False) else None
        max_account_risk_pct = account_limits.get('max_account_risk', 6.0) if parameter_states.get('enable_max_account_risk', False) else None
        max_positions = account_limits.get('max_positions', 10) if parameter_states.get('enable_max_positions', False) else None
        equity_buffer = account_limits.get('equity_buffer', 1000) if parameter_states.get('enable_equity_buffer', False) else None
        
        # Get strategy settings
        strategy_config = trading_config.get('strategies', {}).get('divergence', {})
        risk_mgmt = strategy_config.get('risk_management', {})
        strategy_allocation_pct = risk_mgmt.get('strategy_allocation', 20.0)
        base_position_size_pct = risk_mgmt.get('position_size', 15.0)
        max_shares = risk_mgmt.get('max_shares', 1000)
        
        # Calculate daily loss status (only if enabled)
        if daily_loss_limit_pct is not None:
            daily_loss_limit_dollars = equity * (daily_loss_limit_pct / 100.0)
            current_daily_loss = abs(min(0, total_day_pl))
            remaining_daily_loss = max(0, daily_loss_limit_dollars - current_daily_loss)
            daily_loss_check = current_daily_loss < daily_loss_limit_dollars
        else:
            daily_loss_limit_dollars = None
            current_daily_loss = 0.0
            remaining_daily_loss = float('inf')
            daily_loss_check = True
        
        # Calculate account risk status (only if enabled)
        total_unrealized_pl = positions_data.get('summary', {}).get('total_unrealized_pl', 0.0)
        if max_account_risk_pct is not None:
            max_account_risk_dollars = equity * (max_account_risk_pct / 100.0)
            current_risk_exposure = abs(min(0, total_unrealized_pl))
            remaining_risk_capacity = max(0, max_account_risk_dollars - current_risk_exposure)
            account_risk_check = current_risk_exposure < max_account_risk_dollars
        else:
            max_account_risk_dollars = None
            current_risk_exposure = 0.0
            remaining_risk_capacity = float('inf')
            account_risk_check = True
        
        # Calculate strategy allocation status
        max_strategy_allocation = equity * (strategy_allocation_pct / 100.0)
        
        # Calculate current divergence strategy exposure by matching positions with strategy watchlist
        current_strategy_exposure = 0.0
        divergence_watchlist = set(self.watchlist_symbols)  # Convert to set for faster lookup
        
        positions = positions_data.get('positions', {})
        for position_key, position_data in positions.items():
            # Extract symbol from position key (format: "SYMBOL_ACCOUNT" e.g., "AAPL_74680244")
            symbol = position_data.get('symbol', position_key.split('_')[0])
            
            # Only count positions that are in the divergence strategy watchlist
            if symbol in divergence_watchlist:
                # Get the market value for this position (use absolute value for exposure calculation)
                market_value = abs(position_data.get('market_value', 0.0))
                current_strategy_exposure += market_value
                self.logger.info(f"   🎯 Found divergence position: {symbol} = ${market_value:,.2f}")
        
        remaining_strategy_allocation = max(0, max_strategy_allocation - current_strategy_exposure)
        strategy_allocation_check = current_strategy_exposure < max_strategy_allocation
        
        # Calculate position limits (only if enabled)
        current_positions = positions_data.get('total_positions', 0)
        if max_positions is not None:
            remaining_positions = max(0, max_positions - current_positions)
            position_limit_check = current_positions < max_positions
        else:
            remaining_positions = float('inf')
            position_limit_check = True
        
        # Check if trading is allowed (only check enabled limits)
        can_trade = (
            daily_loss_check and
            account_risk_check and
            strategy_allocation_check and
            position_limit_check
        )
        
        if not can_trade:
            return {
                'can_trade': False,
                'position_size': 0,
                'position_value': 0.0,
                'risk_amount': 0.0,
                'reason': 'Risk limits exceeded',
                'risk_analysis': {
                    'daily_loss_status': {
                        'limit_pct': daily_loss_limit_pct,
                        'limit_dollars': daily_loss_limit_dollars,
                        'current_loss': current_daily_loss,
                        'remaining': remaining_daily_loss,
                        'can_trade': current_daily_loss < daily_loss_limit_dollars
                    },
                    'account_risk_status': {
                        'limit_pct': max_account_risk_pct,
                        'limit_dollars': max_account_risk_dollars,
                        'current_exposure': current_risk_exposure,
                        'remaining_capacity': remaining_risk_capacity,
                        'can_add_risk': current_risk_exposure < max_account_risk_dollars
                    },
                    'strategy_allocation_status': {
                        'allocation_pct': strategy_allocation_pct,
                        'max_allocation': max_strategy_allocation,
                        'current_exposure': current_strategy_exposure,
                        'remaining_allocation': remaining_strategy_allocation,
                        'can_add_position': current_strategy_exposure < max_strategy_allocation
                    },
                    'position_limits_status': {
                        'max_positions': max_positions,
                        'current_positions': current_positions,
                        'remaining_positions': remaining_positions,
                        'can_add_position': current_positions < max_positions
                    }
                }
            }
        
        # Calculate base position size as percentage of strategy allocation (not total equity)
        base_position_value = max_strategy_allocation * (base_position_size_pct / 100.0)
        
        # Calculate position size in shares
        base_shares = int(base_position_value / current_price) if current_price > 0 else 0

        # Apply various limits
        limits = []
        
        # Strategy allocation limit
        max_shares_by_strategy = int(remaining_strategy_allocation / current_price) if current_price > 0 else 0
        limits.append(('strategy_allocation', max_shares_by_strategy))
        
        # Daily loss limit (if stop loss is provided and limit is enabled)
        if stop_loss_price > 0 and daily_loss_limit_pct is not None:
            risk_per_share = abs(current_price - stop_loss_price)
            max_shares_by_daily_loss = int(remaining_daily_loss / risk_per_share) if risk_per_share > 0 else 0
            limits.append(('daily_loss_limit', max_shares_by_daily_loss))
        
        # Account risk limit (if stop loss is provided and limit is enabled)
        if stop_loss_price > 0 and max_account_risk_pct is not None:
            risk_per_share = abs(current_price - stop_loss_price)
            max_shares_by_account_risk = int(remaining_risk_capacity / risk_per_share) if risk_per_share > 0 else 0
            limits.append(('account_risk_limit', max_shares_by_account_risk))
        
        # Max shares from strategy config
        limits.append(('max_shares_config', max_shares))
        
        # Available funds limit
        max_shares_by_funds = int(available_funds / current_price) if current_price > 0 else 0
        limits.append(('available_funds', max_shares_by_funds))
        
        # Find the most restrictive limit
        final_shares = base_shares
        limiting_factor = 'base_calculation'
        
        for limit_name, limit_value in limits:
            if limit_value < final_shares:
                final_shares = limit_value
                limiting_factor = limit_name
        
        # Ensure minimum of 1 share if any trading is allowed
        final_shares = max(1, final_shares) if can_trade and final_shares > 0 else 0
        
        # Calculate final values
        final_position_value = final_shares * current_price
        final_risk_amount = final_shares * abs(current_price - stop_loss_price) if stop_loss_price > 0 else 0
        
        return {
            'can_trade': can_trade and final_shares > 0,
            'position_size': final_shares,
            'position_value': final_position_value,
            'risk_amount': final_risk_amount,
            'limiting_factor': limiting_factor,
            'signal_strength': signal_strength,
            'base_position_value': base_position_value,
            'limits_analysis': dict(limits),
            'risk_analysis': {
                'daily_loss_status': {
                    'limit_pct': daily_loss_limit_pct,
                    'limit_dollars': daily_loss_limit_dollars,
                    'current_loss': current_daily_loss,
                    'remaining': remaining_daily_loss,
                    'can_trade': current_daily_loss < daily_loss_limit_dollars
                },
                'account_risk_status': {
                    'limit_pct': max_account_risk_pct,
                    'limit_dollars': max_account_risk_dollars,
                    'current_exposure': current_risk_exposure,
                    'remaining_capacity': remaining_risk_capacity,
                    'can_add_risk': current_risk_exposure < max_account_risk_dollars
                },
                'strategy_allocation_status': {
                    'allocation_pct': strategy_allocation_pct,
                    'max_allocation': max_strategy_allocation,
                    'current_exposure': current_strategy_exposure,
                    'remaining_allocation': remaining_strategy_allocation,
                    'can_add_position': current_strategy_exposure < max_strategy_allocation
                },
                'position_limits_status': {
                    'max_positions': max_positions,
                    'current_positions': current_positions,
                    'remaining_positions': remaining_positions,
                    'can_add_position': current_positions < max_positions
                }
            },
            'account_data': {
                'equity': equity,
                'available_funds': available_funds,
                'buying_power': account_data.get('buying_power', 0)
            }
        }

    def generate_multi_timeframe_signal(self, symbol: str, confirmation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Generate trading signal based on multi-timeframe confirmation."""
        # Initialize signal with defaults
        signal = {
            'symbol': symbol,
            'signal_type': 'NO_SIGNAL',
            'confidence': 0.0,
            'entry_reason': 'No multi-timeframe confirmation',
            'timestamp': datetime.now().isoformat(),
            'market_condition': 'NEUTRAL',
            'volatility_environment': 'Unknown',
            'position_size': 0.0,
            'stop_loss': 0.0,
            'profit_target': 0.0,
            'auto_approve': self.auto_approve,
            'multi_timeframe_confirmation': False,
            'confirmed_timeframes': [],
            'timeframe_analysis': confirmation_result.get('timeframe_signals', {}),
            'confirmation_strength': confirmation_result.get('confirmation_strength', 'none')
        }
        
        try:
            # Check if we have multi-timeframe confirmation
            if not confirmation_result.get('has_confirmation', False):
                return signal
            
            # Get execution data
            exec_data = confirmation_result.get('execution_data', {})
            current_price = exec_data.get('current_price', 0)
            
            if current_price <= 0:
                signal['entry_reason'] = 'Invalid current price'
                return signal
            
            # Estimate ATR (simplified calculation)
            atr = current_price * 0.015  # Assume 1.5% ATR
            
            # Generate signal based on confirmation type
            if confirmation_result.get('bullish_confirmed', False):
                strength = confirmation_result['confirmation_strength']
                if strength == 'strong':
                    signal['signal_type'] = 'STRONG_BUY'
                    signal['confidence'] = 0.9
                    signal['entry_reason'] = f"Strong bullish divergence confirmed on {', '.join(self.confirmation_timeframes)}"
                else:
                    signal['signal_type'] = 'BUY'
                    signal['confidence'] = 0.75
                    signal['entry_reason'] = f"Bullish divergence confirmed on {', '.join(self.confirmation_timeframes)}"
                
                # Calculate risk management for long position
                signal['stop_loss'] = current_price - (atr * self.stop_loss_atr_multiplier)
                signal['profit_target'] = current_price + (atr * self.stop_loss_atr_multiplier * self.risk_reward_ratio)
                signal['market_condition'] = 'BULLISH_DIVERGENCE'
                
            elif confirmation_result.get('bearish_confirmed', False):
                strength = confirmation_result['confirmation_strength']
                if strength == 'strong':
                    signal['signal_type'] = 'STRONG_SELL'
                    signal['confidence'] = 0.9
                    signal['entry_reason'] = f"Strong bearish divergence confirmed on {', '.join(self.confirmation_timeframes)}"
                else:
                    signal['signal_type'] = 'SELL'
                    signal['confidence'] = 0.75
                    signal['entry_reason'] = f"Bearish divergence confirmed on {', '.join(self.confirmation_timeframes)}"
                
                # Calculate risk management for short position
                signal['stop_loss'] = current_price + (atr * self.stop_loss_atr_multiplier)
                signal['profit_target'] = current_price - (atr * self.stop_loss_atr_multiplier * self.risk_reward_ratio)
                signal['market_condition'] = 'BEARISH_DIVERGENCE'
            
            # Calculate comprehensive position sizing
            position_sizing = self.calculate_comprehensive_position_size(
                symbol, current_price, strength, signal['stop_loss']
            )
            
            # Update signal with position sizing data
            signal.update({
                'multi_timeframe_confirmation': True,
                'confirmed_timeframes': self.confirmation_timeframes.copy(),
                'position_size': position_sizing.get('position_size', 0),
                'position_value': position_sizing.get('position_value', 0.0),
                'risk_amount': position_sizing.get('risk_amount', 0.0),
                'can_trade': position_sizing.get('can_trade', False),
                'limiting_factor': position_sizing.get('limiting_factor', 'unknown'),
                'risk_analysis': position_sizing.get('risk_analysis', {}),
                'account_data': position_sizing.get('account_data', {}),
                'max_shares': self.max_shares,
                'strategy_allocation': self.strategy_allocation_pct * 100
            })
            
            # Determine volatility environment
            if current_price > 0 and atr > 0:
                atr_pct = (atr / current_price) * 100
                if atr_pct > 3.0:
                    signal['volatility_environment'] = 'High Volatility'
                elif atr_pct < 1.0:
                    signal['volatility_environment'] = 'Low Volatility'
                else:
                    signal['volatility_environment'] = 'Medium Volatility'
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating multi-timeframe signal for {symbol}: {e}")
            signal['entry_reason'] = f'Error in signal generation: {str(e)}'
            return signal

    def analyze_symbol(self, symbol: str, timeframe_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze a single symbol for multi-timeframe divergence signals."""
        try:
            # Check for multi-timeframe confirmation
            confirmation_result = self.check_divergence_confirmation(symbol, timeframe_data)
            
            # Generate trading signal
            signal = self.generate_multi_timeframe_signal(symbol, confirmation_result)
            
            # Log analysis result
            if signal['signal_type'] != 'NO_SIGNAL':
                self.logger.info(f"🎯 {symbol}: {signal['signal_type']} (Confidence: {signal['confidence']:.1%})")
                self.logger.info(f"   Confirmed on: {', '.join(signal['confirmed_timeframes'])}")
                exec_data = confirmation_result.get('execution_data', {})
                current_price = exec_data.get('current_price', 0)
                self.logger.info(f"   Entry: ${current_price:.2f} | SL: ${signal['stop_loss']:.2f} | TP: ${signal['profit_target']:.2f}")
            else:
                self.logger.debug(f"   {symbol}: No multi-timeframe confirmation")
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error analyzing {symbol}: {e}")
            return {
                'signal_type': 'NO_SIGNAL',
                'confidence': 0.0,
                'entry_reason': f'Analysis error: {str(e)}',
                'timestamp': datetime.now().isoformat(),
                'market_condition': 'UNCERTAIN',
                'volatility_environment': 'Unknown',
                'position_size': 0.0,
                'stop_loss': 0.0,
                'profit_target': 0.0,
                'auto_approve': True
            }

    def print_position_sizing_analysis(self, sample_price: float = 150.0) -> None:
        """Print comprehensive position sizing analysis for current account state."""
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE POSITION SIZING ANALYSIS")
        print("="*80)
        
        try:
            # Load current data
            account_data = self.load_account_data()
            positions_data = self.load_current_positions()
            trading_config = self._load_trading_config()
            
            # Display account information
            print(f"\n💰 ACCOUNT STATUS:")
            print(f"   Equity: ${account_data.get('equity', 0):,.2f}")
            print(f"   Available Funds: ${account_data.get('available_funds', 0):,.2f}")
            print(f"   Buying Power: ${account_data.get('buying_power', 0):,.2f}")
            print(f"   Day P&L: ${account_data.get('total_day_pl', 0):,.2f}")
            
            # Display current positions
            print(f"\n📈 CURRENT POSITIONS:")
            print(f"   Total Positions: {positions_data.get('total_positions', 0)}")
            print(f"   Total Market Value: ${positions_data.get('total_market_value', 0):,.2f}")
            print(f"   Unrealized P&L: ${positions_data.get('summary', {}).get('total_unrealized_pl', 0):,.2f}")
            
            # Get risk settings and their enable states
            risk_management = trading_config.get('risk_management', {})
            account_limits = risk_management.get('account_limits', {})
            parameter_states = risk_management.get('parameter_states', {})
            
            print(f"\n⚙️ RISK MANAGEMENT SETTINGS:")
            print(f"   Daily Loss Limit: {account_limits.get('daily_loss_limit', 'N/A')}% " +
                  f"({'ENABLED' if parameter_states.get('enable_daily_loss_limit', False) else 'DISABLED'})")
            print(f"   Max Account Risk: {account_limits.get('max_account_risk', 'N/A')}% " +
                  f"({'ENABLED' if parameter_states.get('enable_max_account_risk', False) else 'DISABLED'})")
            print(f"   Max Positions: {account_limits.get('max_positions', 'N/A')} " +
                  f"({'ENABLED' if parameter_states.get('enable_max_positions', False) else 'DISABLED'})")
            print(f"   Equity Buffer: ${account_limits.get('equity_buffer', 'N/A')} " +
                  f"({'ENABLED' if parameter_states.get('enable_equity_buffer', False) else 'DISABLED'})")
            
            # Strategy settings
            strategy_config = trading_config.get('strategies', {}).get('divergence', {})
            risk_mgmt = strategy_config.get('risk_management', {})
            
            print(f"\n🎯 DIVERGENCE STRATEGY SETTINGS:")
            print(f"   Strategy Allocation: {risk_mgmt.get('strategy_allocation', 'N/A')}%")
            print(f"   Base Position Size: {risk_mgmt.get('position_size', 'N/A')}%")
            print(f"   Max Shares per Position: {risk_mgmt.get('max_shares', 'N/A')}")
            
            # Calculate position sizing for sample scenarios
            print(f"\n🧮 POSITION SIZING ANALYSIS (Sample Price: ${sample_price:.2f}):")
            print("-" * 60)
            
            scenarios = [
                ("Strong Signal", "strong", sample_price * 0.98),  # 2% stop loss
                ("Medium Signal", "medium", sample_price * 0.97), # 3% stop loss
                ("Weak Signal", "weak", sample_price * 0.95)      # 5% stop loss
            ]
            
            for scenario_name, strength, stop_loss in scenarios:
                print(f"\n📋 {scenario_name.upper()} SCENARIO:")
                position_sizing = self.calculate_comprehensive_position_size(
                    "SAMPLE", sample_price, strength, stop_loss
                )
                
                print(f"   Can Trade: {'✅ YES' if position_sizing.get('can_trade', False) else '❌ NO'}")
                print(f"   Position Size: {position_sizing.get('position_size', 0)} shares")
                print(f"   Position Value: ${position_sizing.get('position_value', 0):,.2f}")
                print(f"   Risk Amount: ${position_sizing.get('risk_amount', 0):,.2f}")
                print(f"   Limiting Factor: {position_sizing.get('limiting_factor', 'unknown')}")
                
                # Show limits analysis
                limits = position_sizing.get('limits_analysis', {})
                if limits:
                    print(f"   📊 Limits Analysis:")
                    for limit_name, limit_value in limits.items():
                        print(f"      {limit_name}: {limit_value} shares")
                
                # Show risk analysis
                risk_analysis = position_sizing.get('risk_analysis', {})
                if risk_analysis:
                    print(f"   🔍 Risk Analysis:")
                    
                    # Daily loss status
                    daily_status = risk_analysis.get('daily_loss_status', {})
                    if daily_status.get('limit_pct') is not None:
                        print(f"      Daily Loss: ${daily_status.get('current_loss', 0):,.2f} / ${daily_status.get('limit_dollars', 0):,.2f} " +
                              f"({daily_status.get('current_loss', 0) / daily_status.get('limit_dollars', 1) * 100:.1f}%)")
                    else:
                        print(f"      Daily Loss: DISABLED")
                    
                    # Account risk status
                    account_status = risk_analysis.get('account_risk_status', {})
                    if account_status.get('limit_pct') is not None:
                        print(f"      Account Risk: ${account_status.get('current_exposure', 0):,.2f} / ${account_status.get('limit_dollars', 0):,.2f} " +
                              f"({account_status.get('current_exposure', 0) / account_status.get('limit_dollars', 1) * 100:.1f}%)")
                    else:
                        print(f"      Account Risk: DISABLED")
                    
                    # Strategy allocation status
                    strategy_status = risk_analysis.get('strategy_allocation_status', {})
                    print(f"      Strategy Allocation: ${strategy_status.get('current_exposure', 0):,.2f} / ${strategy_status.get('max_allocation', 0):,.2f} " +
                          f"({strategy_status.get('current_exposure', 0) / strategy_status.get('max_allocation', 1) * 100:.1f}%)")
                    
                    # Position limits status
                    position_status = risk_analysis.get('position_limits_status', {})
                    if position_status.get('max_positions') is not None:
                        print(f"      Position Count: {position_status.get('current_positions', 0)} / {position_status.get('max_positions', 0)}")
                    else:
                        print(f"      Position Count: DISABLED")
            
            print("\n" + "="*80)
            
        except Exception as e:
            print(f"❌ Error generating position sizing analysis: {e}")

    def _execute_divergence_trades_directly(self) -> bool:
        """Execute divergence trades directly without running the continuous trading engine"""
        try:
            self.logger.info("🎯 Processing divergence signals for immediate trade execution...")
            
            # Import the trading engine class directly
            try:
                from divergence_trading_engine import DivergenceTradingEngine
            except ImportError:
                self.logger.error("❌ Could not import DivergenceTradingEngine")
                return False
            
            # Create a trading engine instance (but don't start monitoring)
            trading_engine = DivergenceTradingEngine()
            
            # Process signals once without continuous monitoring
            self.logger.info("📊 Checking for actionable divergence signals...")
            trading_engine._check_divergence_signals()
            
            # Get status to see if any trades were executed
            status = trading_engine.get_status()
            active_orders = status.get('active_orders', 0)
            
            if active_orders > 0:
                self.logger.info(f"✅ Successfully submitted {active_orders} divergence trade orders")
                return True
            else:
                self.logger.info("📊 No actionable divergence trades executed (signals may not meet trading criteria)")
                return True  # Still successful, just no trades
                
        except Exception as e:
            self.logger.error(f"❌ Error executing divergence trades directly: {e}")
            return False

    def run_analysis(self, auto_generate_data: bool = True) -> Dict[str, Any]:
        """Run multi-timeframe divergence analysis for configured symbols."""
        # Check if configuration was loaded successfully
        if not self.config_loaded:
            self.logger.error("❌ Cannot run analysis: Trading configuration not loaded properly")
            self.logger.error("❌ Please ensure trading_config_live.json exists and contains proper divergence strategy configuration")
            return {}
        
        # Reload trading config to check for changes (especially auto_approve)
        self._reload_trading_config()
        
        # Print comprehensive position sizing analysis first
        self.print_position_sizing_analysis()
        
        self.logger.info("🚀 Starting Multi-Timeframe Divergence Analysis...")
        
        # Auto-generate fresh divergence indicators if requested
        if auto_generate_data:
            self.logger.info("📊 Auto-generating fresh divergence indicators...")
            success = self._generate_fresh_divergence_data()
            if not success:
                self.logger.warning("⚠️ Failed to generate fresh data, proceeding with existing data")
        
        # Load data from all timeframes
        timeframe_data = self.load_multi_timeframe_data()
        
        # Determine symbols to analyze
        if self.watchlist_symbols:
            # Use symbols from trading config watchlist
            symbols_to_analyze = self.watchlist_symbols
            self.logger.info(f"📋 Using {len(symbols_to_analyze)} symbols from divergence strategy watchlist")
        else:
            # Fallback to all symbols in timeframe data
            all_symbols = set()
            for tf_data in timeframe_data.values():
                all_symbols.update(tf_data.keys())
            symbols_to_analyze = list(all_symbols)
            self.logger.info(f"📊 No watchlist configured, analyzing all {len(symbols_to_analyze)} available symbols")
        
        if not symbols_to_analyze:
            self.logger.warning("❌ No symbols found for analysis")
            return {}
        
        self.logger.info(f"📊 Analyzing {len(symbols_to_analyze)} symbols across {len(self.timeframes)} timeframes")
        
        # Analyze each symbol
        signals = {}
        confirmed_signals = 0
        
        for symbol in sorted(symbols_to_analyze):
            # Only analyze if symbol has data in timeframes
            if any(symbol in tf_data for tf_data in timeframe_data.values()):
                signal = self.analyze_symbol(symbol, timeframe_data)
                signals[symbol] = signal
                
                if signal['signal_type'] in ['STRONG_BUY', 'BUY', 'STRONG_SELL', 'SELL']:
                    confirmed_signals += 1
            else:
                self.logger.warning(f"⚠️ No data available for symbol {symbol}")
        
        self.logger.info(f"✅ Analysis complete: {confirmed_signals} confirmed signals out of {len(symbols_to_analyze)} symbols")
        
        # Debug logging for trading engine decision
        self.logger.info(f"🔍 Trading Engine Decision Check:")
        self.logger.info(f"   Confirmed Signals: {confirmed_signals}")
        self.logger.info(f"   Auto Approve: {self.auto_approve}")
        self.logger.info(f"   Will Call Trading Engine: {confirmed_signals > 0 and self.auto_approve}")
        
        # Execute divergence trades directly if we have confirmed signals and auto_approve is enabled
        if confirmed_signals > 0 and self.auto_approve:
            self.logger.info(f"🎯 Found {confirmed_signals} confirmed signals with auto_approve enabled - executing trades directly")
            trading_success = self._execute_divergence_trades_directly()
            if trading_success:
                self.logger.info("✅ Divergence trades processed successfully")
            else:
                self.logger.warning("⚠️ Divergence trade execution failed")
        elif confirmed_signals > 0:
            self.logger.info(f"📊 Found {confirmed_signals} confirmed signals but auto_approve is disabled")
        else:
            self.logger.info(f"📊 No confirmed signals found - trading engine will not be called")
        
        return signals

def save_signals_to_file(signals: Dict[str, Any]) -> bool:
    """Save multi-timeframe divergence signals to JSON file in divergence_data directory."""
    try:
        # Create output directory if it doesn't exist
        output_dir = 'divergence_data'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 Created directory: {output_dir}")
        
        # Create comprehensive signals data
        signals_data = {
            'strategy_name': 'Multi_Timeframe_Divergence_Strategy',
            'last_updated': datetime.now().isoformat(),
            'total_symbols_analyzed': len(signals),
            'confirmed_signals': len([s for s in signals.values() if s.get('multi_timeframe_confirmation', False)]),
            'analysis_summary': {
                'strong_buy': len([s for s in signals.values() if s['signal_type'] == 'STRONG_BUY']),
                'buy': len([s for s in signals.values() if s['signal_type'] == 'BUY']),
                'strong_sell': len([s for s in signals.values() if s['signal_type'] == 'STRONG_SELL']),
                'sell': len([s for s in signals.values() if s['signal_type'] == 'SELL']),
                'no_signal': len([s for s in signals.values() if s['signal_type'] == 'NO_SIGNAL'])
            },
            'signals': signals,
            'metadata': {
                'strategy_type': 'multi_timeframe_divergence',
                'timeframes_used': ['1min', '5min', '15min'],
                'confirmation_timeframes': ['5min', '15min'],
                'execution_timeframe': '1min',
                'signal_types': ['STRONG_BUY', 'BUY', 'STRONG_SELL', 'SELL', 'NO_SIGNAL'],
                'analysis_method': 'multi_timeframe_divergence_confirmation',
                'update_frequency': 'on_demand',
                'data_source': 'divergence_data_directory',
                'output_directory': output_dir
            }
        }
        
        # Write to file in organized directory
        filename = 'divergence_signals_multi_timeframe.json'
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(signals_data, f, indent=2)
        
        print(f"✅ Saved multi-timeframe divergence signals to {filepath}")
        print(f"📊 Summary: {signals_data['analysis_summary']['strong_buy']} STRONG_BUY, {signals_data['analysis_summary']['buy']} BUY, {signals_data['analysis_summary']['strong_sell']} STRONG_SELL, {signals_data['analysis_summary']['sell']} SELL")
        return True
        
    except Exception as e:
        print(f"❌ Error saving signals to file: {e}")
        return False

# Global strategy instance for signal handling
strategy_instance = None

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    global strategy_instance
    print(f"\n🛑 Received signal {sig}, initiating graceful shutdown...")
    if strategy_instance:
        strategy_instance.shutdown_requested = True
    else:
        print("🛑 Forcing immediate shutdown...")
        sys.exit(0)

def main():
    """Main function to run multi-timeframe divergence analysis continuously."""
    global strategy_instance
    
    print("🔄 Multi-Timeframe Divergence Strategy - Continuous Mode")
    print("=" * 60)
    print("⏰ Running every 60 seconds. Press Ctrl+C to stop gracefully.")
    print("=" * 60)
    
    # Set up signal handlers for graceful shutdown
    signal_module.signal(signal_module.SIGINT, signal_handler)   # Ctrl+C
    signal_module.signal(signal_module.SIGTERM, signal_handler)  # Termination signal
    
    # Initialize strategy
    strategy_instance = MultiTimeframeDivergenceStrategy()
    strategy = strategy_instance
    
    if not strategy.config_loaded:
        print("❌ Strategy configuration not loaded. Exiting.")
        return
    
    cycle_count = 0
    start_time = datetime.now()
    
    print(f"🚀 Starting continuous divergence analysis at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        while not strategy.shutdown_requested:
            cycle_count += 1
            cycle_start = datetime.now()
            
            print(f"\n{'='*60}")
            print(f"🔄 ANALYSIS CYCLE #{cycle_count}")
            print(f"⏰ Started at: {cycle_start.strftime('%H:%M:%S')}")
            print(f"{'='*60}")
            
            try:
                # Run analysis
                signals = strategy.run_analysis()
                
                if signals:
                    # Save signals to file
                    save_signals_to_file(signals)
                    
                    # Print summary
                    confirmed_signals = [s for s in signals.values() if s.get('multi_timeframe_confirmation', False)]
                    if confirmed_signals:
                        print(f"\n🎯 Multi-Timeframe Confirmed Signals (Cycle #{cycle_count}):")
                        for signal in confirmed_signals:
                            print(f"   {signal['symbol']}: {signal['signal_type']} (Confidence: {signal['confidence']:.1%})")
                            print(f"      Reason: {signal['entry_reason']}")
                    else:
                        print(f"\n📊 No multi-timeframe confirmed signals found in cycle #{cycle_count}")
                else:
                    print(f"❌ No signals generated in cycle #{cycle_count}")
                
                cycle_end = datetime.now()
                cycle_duration = (cycle_end - cycle_start).total_seconds()
                
                print(f"\n✅ Cycle #{cycle_count} completed in {cycle_duration:.1f} seconds")
                print(f"⏰ Next analysis in 60 seconds...")
                
                # Wait for 60 seconds or until shutdown is requested
                for i in range(60):
                    if strategy.shutdown_requested:
                        break
                    time.sleep(1)
                
            except Exception as e:
                print(f"❌ Error in analysis cycle #{cycle_count}: {e}")
                print("⏰ Waiting 60 seconds before retry...")
                
                # Wait for 60 seconds or until shutdown is requested
                for i in range(60):
                    if strategy.shutdown_requested:
                        break
                    time.sleep(1)
    
    except KeyboardInterrupt:
        print(f"\n🛑 Keyboard interrupt received")
        strategy.shutdown_requested = True
    
    except Exception as e:
        print(f"\n❌ Unexpected error in main loop: {e}")
        strategy.shutdown_requested = True
    
    finally:
        end_time = datetime.now()
        total_runtime = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*60}")
        print(f"🛑 SHUTDOWN SUMMARY")
        print(f"{'='*60}")
        print(f"⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Ended: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Total Runtime: {total_runtime/60:.1f} minutes ({total_runtime:.0f} seconds)")
        print(f"🔄 Total Cycles: {cycle_count}")
        if cycle_count > 0:
            print(f"📊 Average Cycle Time: {total_runtime/cycle_count:.1f} seconds")
        print(f"✅ Multi-Timeframe Divergence Strategy shutdown complete")
        print(f"{'='*60}")

if __name__ == "__main__":
    main()
