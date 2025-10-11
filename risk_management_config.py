#!/usr/bin/env python3
"""
Risk Management Configuration and Engine
Handles risk parameters, position sizing, and risk monitoring for VolFlow Options Breakout
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RiskStatus(Enum):
    SAFE = "safe"
    WARNING = "warning"
    DANGER = "danger"
    EMERGENCY = "emergency"

class StopLossMethod(Enum):
    PERCENTAGE = "percentage"
    ATR = "atr"
    TECHNICAL = "technical"
    VOLATILITY = "volatility"

class TradingHours(Enum):
    MARKET = "market"
    EXTENDED = "extended"
    CRYPTO_24H = "24h"

class VolatilityFilter(Enum):
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

class MarketRegime(Enum):
    ALL = "all"
    BULL = "bull"
    BEAR = "bear"
    SIDEWAYS = "sideways"

@dataclass
class AccountRiskLimits:
    """Account-level risk limits"""
    max_account_risk_percent: float = 25.0  # Maximum percentage of account equity at risk
    daily_loss_limit_percent: float = 5.0   # Stop trading if daily loss exceeds this
    equity_buffer_dollars: float = 10000.0  # Minimum cash buffer to maintain
    max_drawdown_percent: float = 15.0      # Maximum portfolio drawdown allowed

@dataclass
class PositionSizing:
    """Position sizing parameters"""
    max_position_size_percent: float = 5.0  # Maximum equity per single position
    max_positions: int = 15                  # Maximum number of concurrent positions
    max_correlated_positions: int = 3       # Maximum positions in same sector/asset class
    position_concentration_limit: float = 20.0  # Max percentage in any single asset class

@dataclass
class StopLossConfig:
    """Stop loss and take profit configuration"""
    method: StopLossMethod = StopLossMethod.ATR
    value: float = 2.0                       # Stop loss distance (units depend on method)
    take_profit_ratio: float = 2.0          # Risk/reward ratio (profit target as multiple of risk)
    trailing_stop_enabled: bool = False     # Enable trailing stop loss
    trailing_stop_distance: float = 1.5     # Trailing stop distance in ATR multiples

@dataclass
class TimeBasedRisk:
    """Time-based risk management"""
    max_hold_time_days: int = 30            # Automatically close positions after this period
    close_before_expiry_days: int = 7       # Close options positions before expiration
    trading_hours: TradingHours = TradingHours.EXTENDED
    avoid_earnings: bool = True             # Avoid holding through earnings announcements
    avoid_fomc: bool = True                 # Avoid holding through FOMC meetings

@dataclass
class MarketConditions:
    """Market condition filters"""
    volatility_filter: VolatilityFilter = VolatilityFilter.MEDIUM
    market_regime: MarketRegime = MarketRegime.ALL
    vix_threshold: float = 30.0             # Reduce position sizes when VIX exceeds this
    correlation_threshold: float = 0.7      # Maximum correlation between positions
    liquidity_min_volume: int = 100000      # Minimum daily volume for position entry

@dataclass
class EmergencyControls:
    """Emergency risk controls"""
    emergency_stop_active: bool = False     # Emergency stop all trading
    auto_close_on_limit: bool = True        # Automatically close positions when limits hit
    risk_override_password: str = ""        # Password for risk limit overrides
    max_daily_trades: int = 50              # Maximum trades per day
    cooling_off_period_hours: int = 24      # Cooling off period after emergency stop

@dataclass
class RiskConfiguration:
    """Complete risk management configuration"""
    account_limits: AccountRiskLimits
    position_sizing: PositionSizing
    stop_loss: StopLossConfig
    time_based: TimeBasedRisk
    market_conditions: MarketConditions
    emergency: EmergencyControls
    created_timestamp: str
    last_updated: str
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RiskConfiguration':
        """Create from dictionary"""
        return cls(
            account_limits=AccountRiskLimits(**data.get('account_limits', {})),
            position_sizing=PositionSizing(**data.get('position_sizing', {})),
            stop_loss=StopLossConfig(
                method=StopLossMethod(data.get('stop_loss', {}).get('method', 'atr')),
                **{k: v for k, v in data.get('stop_loss', {}).items() if k != 'method'}
            ),
            time_based=TimeBasedRisk(
                trading_hours=TradingHours(data.get('time_based', {}).get('trading_hours', 'extended')),
                **{k: v for k, v in data.get('time_based', {}).items() if k != 'trading_hours'}
            ),
            market_conditions=MarketConditions(
                volatility_filter=VolatilityFilter(data.get('market_conditions', {}).get('volatility_filter', 'medium')),
                market_regime=MarketRegime(data.get('market_conditions', {}).get('market_regime', 'all')),
                **{k: v for k, v in data.get('market_conditions', {}).items() if k not in ['volatility_filter', 'market_regime']}
            ),
            emergency=EmergencyControls(**data.get('emergency', {})),
            created_timestamp=data.get('created_timestamp', datetime.now().isoformat()),
            last_updated=data.get('last_updated', datetime.now().isoformat()),
            version=data.get('version', '1.0')
        )

class RiskManager:
    """Risk management engine"""
    
    def __init__(self, config_file: str = "risk_config.json"):
        self.config_file = config_file
        self.config = self.load_configuration()
        self.current_positions = {}
        self.daily_stats = {
            'trades_today': 0,
            'daily_pnl': 0.0,
            'max_drawdown_today': 0.0,
            'risk_violations': []
        }
        
    def load_configuration(self) -> RiskConfiguration:
        """Load risk configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    data = json.load(f)
                    config = RiskConfiguration.from_dict(data)
                    logger.info(f"✅ Risk configuration loaded from {self.config_file}")
                    return config
            else:
                # Create default configuration
                config = self.create_default_configuration()
                self.save_configuration(config)
                logger.info(f"📝 Created default risk configuration: {self.config_file}")
                return config
                
        except Exception as e:
            logger.error(f"❌ Error loading risk configuration: {e}")
            return self.create_default_configuration()
    
    def create_default_configuration(self) -> RiskConfiguration:
        """Create default risk configuration"""
        now = datetime.now().isoformat()
        
        return RiskConfiguration(
            account_limits=AccountRiskLimits(),
            position_sizing=PositionSizing(),
            stop_loss=StopLossConfig(),
            time_based=TimeBasedRisk(),
            market_conditions=MarketConditions(),
            emergency=EmergencyControls(),
            created_timestamp=now,
            last_updated=now
        )
    
    def save_configuration(self, config: Optional[RiskConfiguration] = None) -> bool:
        """Save risk configuration to file"""
        try:
            if config is None:
                config = self.config
            
            config.last_updated = datetime.now().isoformat()
            
            with open(self.config_file, 'w') as f:
                json.dump(config.to_dict(), f, indent=2, default=str)
            
            logger.info(f"💾 Risk configuration saved to {self.config_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving risk configuration: {e}")
            return False
    
    def update_configuration(self, updates: Dict[str, Any]) -> bool:
        """Update risk configuration with new values"""
        try:
            # Update configuration
            config_dict = self.config.to_dict()
            
            # Apply updates recursively
            self._deep_update(config_dict, updates)
            
            # Create new configuration
            self.config = RiskConfiguration.from_dict(config_dict)
            
            # Save updated configuration
            return self.save_configuration()
            
        except Exception as e:
            logger.error(f"❌ Error updating risk configuration: {e}")
            return False
    
    def _deep_update(self, base_dict: Dict, update_dict: Dict) -> None:
        """Recursively update nested dictionary"""
        for key, value in update_dict.items():
            if isinstance(value, dict) and key in base_dict and isinstance(base_dict[key], dict):
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value
    
    def calculate_position_size(self, symbol: str, entry_price: float, 
                              stop_loss_price: float, account_equity: float) -> Tuple[int, float, str]:
        """
        Calculate appropriate position size based on risk parameters
        
        Returns:
            Tuple of (shares, dollar_amount, reason)
        """
        try:
            # Calculate risk per share
            risk_per_share = abs(entry_price - stop_loss_price)
            
            if risk_per_share <= 0:
                return 0, 0.0, "Invalid stop loss price"
            
            # Calculate maximum position size based on account risk
            max_risk_dollars = account_equity * (self.config.account_limits.max_account_risk_percent / 100)
            max_position_dollars = account_equity * (self.config.position_sizing.max_position_size_percent / 100)
            
            # Calculate shares based on risk
            max_shares_by_risk = int(max_risk_dollars / risk_per_share)
            max_shares_by_position_size = int(max_position_dollars / entry_price)
            
            # Use the smaller of the two limits
            shares = min(max_shares_by_risk, max_shares_by_position_size)
            dollar_amount = shares * entry_price
            
            # Check if we have enough equity buffer
            if dollar_amount > (account_equity - self.config.account_limits.equity_buffer_dollars):
                shares = int((account_equity - self.config.account_limits.equity_buffer_dollars) / entry_price)
                dollar_amount = shares * entry_price
                reason = f"Limited by equity buffer requirement"
            else:
                reason = f"Position sized for {self.config.position_sizing.max_position_size_percent}% max position size"
            
            return max(0, shares), max(0.0, dollar_amount), reason
            
        except Exception as e:
            logger.error(f"❌ Error calculating position size for {symbol}: {e}")
            return 0, 0.0, f"Error: {str(e)}"
    
    def check_risk_limits(self, account_equity: float, daily_pnl: float, 
                         positions: Dict[str, Any]) -> Tuple[RiskStatus, List[str]]:
        """
        Check current risk status against limits
        
        Returns:
            Tuple of (risk_status, violations_list)
        """
        violations = []
        risk_status = RiskStatus.SAFE
        
        try:
            # Check daily loss limit
            daily_loss_percent = (daily_pnl / account_equity) * 100
            if abs(daily_loss_percent) > self.config.account_limits.daily_loss_limit_percent:
                violations.append(f"Daily loss limit exceeded: {daily_loss_percent:.2f}%")
                risk_status = RiskStatus.DANGER
            elif abs(daily_loss_percent) > self.config.account_limits.daily_loss_limit_percent * 0.8:
                violations.append(f"Approaching daily loss limit: {daily_loss_percent:.2f}%")
                risk_status = max(risk_status, RiskStatus.WARNING)
            
            # Check position count
            active_positions = len([p for p in positions.values() if p.get('quantity', 0) != 0])
            if active_positions > self.config.position_sizing.max_positions:
                violations.append(f"Too many positions: {active_positions}/{self.config.position_sizing.max_positions}")
                risk_status = max(risk_status, RiskStatus.WARNING)
            
            # Check account risk utilization
            total_position_value = sum(abs(p.get('market_value', 0)) for p in positions.values())
            risk_utilization = (total_position_value / account_equity) * 100
            
            if risk_utilization > self.config.account_limits.max_account_risk_percent:
                violations.append(f"Account risk exceeded: {risk_utilization:.1f}%")
                risk_status = RiskStatus.DANGER
            elif risk_utilization > self.config.account_limits.max_account_risk_percent * 0.8:
                violations.append(f"High account risk: {risk_utilization:.1f}%")
                risk_status = max(risk_status, RiskStatus.WARNING)
            
            # Check equity buffer
            available_cash = account_equity - total_position_value
            if available_cash < self.config.account_limits.equity_buffer_dollars:
                violations.append(f"Equity buffer violated: ${available_cash:,.2f}")
                risk_status = max(risk_status, RiskStatus.WARNING)
            
            # Check emergency stop
            if self.config.emergency.emergency_stop_active:
                violations.append("Emergency stop is active")
                risk_status = RiskStatus.EMERGENCY
            
            return risk_status, violations
            
        except Exception as e:
            logger.error(f"❌ Error checking risk limits: {e}")
            return RiskStatus.DANGER, [f"Error checking risk: {str(e)}"]
    
    def should_close_position(self, symbol: str, position_data: Dict[str, Any], 
                            current_price: float, market_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Determine if a position should be closed based on risk rules
        
        Returns:
            Tuple of (should_close, reason)
        """
        try:
            entry_time = datetime.fromisoformat(position_data.get('entry_time', datetime.now().isoformat()))
            hold_time = datetime.now() - entry_time
            
            # Check maximum hold time
            if hold_time.days > self.config.time_based.max_hold_time_days:
                return True, f"Maximum hold time exceeded: {hold_time.days} days"
            
            # Check stop loss
            entry_price = position_data.get('entry_price', current_price)
            stop_loss_price = position_data.get('stop_loss_price')
            
            if stop_loss_price:
                quantity = position_data.get('quantity', 0)
                if quantity > 0 and current_price <= stop_loss_price:  # Long position
                    return True, f"Stop loss hit: ${current_price:.2f} <= ${stop_loss_price:.2f}"
                elif quantity < 0 and current_price >= stop_loss_price:  # Short position
                    return True, f"Stop loss hit: ${current_price:.2f} >= ${stop_loss_price:.2f}"
            
            # Check take profit
            take_profit_price = position_data.get('take_profit_price')
            if take_profit_price:
                quantity = position_data.get('quantity', 0)
                if quantity > 0 and current_price >= take_profit_price:  # Long position
                    return True, f"Take profit hit: ${current_price:.2f} >= ${take_profit_price:.2f}"
                elif quantity < 0 and current_price <= take_profit_price:  # Short position
                    return True, f"Take profit hit: ${current_price:.2f} <= ${take_profit_price:.2f}"
            
            # Check options expiration
            expiry_date = position_data.get('expiry_date')
            if expiry_date:
                expiry = datetime.fromisoformat(expiry_date)
                days_to_expiry = (expiry - datetime.now()).days
                if days_to_expiry <= self.config.time_based.close_before_expiry_days:
                    return True, f"Close before expiry: {days_to_expiry} days remaining"
            
            # Check VIX threshold
            vix_level = market_data.get('vix', 0)
            if vix_level > self.config.market_conditions.vix_threshold:
                unrealized_pnl_percent = position_data.get('unrealized_pl_percent', 0)
                if unrealized_pnl_percent < -5:  # Close losing positions in high VIX
                    return True, f"High VIX environment: {vix_level:.1f}, position losing {unrealized_pnl_percent:.1f}%"
            
            return False, "Position within risk parameters"
            
        except Exception as e:
            logger.error(f"❌ Error checking position close criteria for {symbol}: {e}")
            return False, f"Error: {str(e)}"
    
    def get_risk_metrics(self, account_equity: float, positions: Dict[str, Any], 
                        daily_pnl: float) -> Dict[str, Any]:
        """Get comprehensive risk metrics"""
        try:
            total_position_value = sum(abs(p.get('market_value', 0)) for p in positions.values())
            active_positions = len([p for p in positions.values() if p.get('quantity', 0) != 0])
            
            risk_utilization = (total_position_value / account_equity) * 100 if account_equity > 0 else 0
            daily_pnl_percent = (daily_pnl / account_equity) * 100 if account_equity > 0 else 0
            
            available_cash = account_equity - total_position_value
            equity_buffer_used = max(0, self.config.account_limits.equity_buffer_dollars - available_cash)
            
            risk_status, violations = self.check_risk_limits(account_equity, daily_pnl, positions)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'risk_status': risk_status.value,
                'account_equity': account_equity,
                'total_position_value': total_position_value,
                'available_cash': available_cash,
                'risk_utilization_percent': risk_utilization,
                'daily_pnl': daily_pnl,
                'daily_pnl_percent': daily_pnl_percent,
                'active_positions': active_positions,
                'max_positions': self.config.position_sizing.max_positions,
                'equity_buffer_used': equity_buffer_used,
                'equity_buffer_required': self.config.account_limits.equity_buffer_dollars,
                'violations': violations,
                'limits': {
                    'max_account_risk_percent': self.config.account_limits.max_account_risk_percent,
                    'daily_loss_limit_percent': self.config.account_limits.daily_loss_limit_percent,
                    'max_position_size_percent': self.config.position_sizing.max_position_size_percent,
                    'max_positions': self.config.position_sizing.max_positions
                },
                'emergency_stop_active': self.config.emergency.emergency_stop_active
            }
            
        except Exception as e:
            logger.error(f"❌ Error calculating risk metrics: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'risk_status': RiskStatus.DANGER.value
            }
    
    def activate_emergency_stop(self, reason: str = "Manual activation") -> bool:
        """Activate emergency stop"""
        try:
            self.config.emergency.emergency_stop_active = True
            self.save_configuration()
            
            logger.warning(f"🚨 EMERGENCY STOP ACTIVATED: {reason}")
            
            # Log emergency stop event
            self.daily_stats['risk_violations'].append({
                'timestamp': datetime.now().isoformat(),
                'type': 'emergency_stop',
                'reason': reason
            })
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error activating emergency stop: {e}")
            return False
    
    def deactivate_emergency_stop(self, reason: str = "Manual deactivation") -> bool:
        """Deactivate emergency stop"""
        try:
            self.config.emergency.emergency_stop_active = False
            self.save_configuration()
            
            logger.info(f"✅ Emergency stop deactivated: {reason}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error deactivating emergency stop: {e}")
            return False
    
    def export_risk_report(self, account_equity: float, positions: Dict[str, Any], 
                          daily_pnl: float) -> Dict[str, Any]:
        """Export comprehensive risk report"""
        try:
            risk_metrics = self.get_risk_metrics(account_equity, positions, daily_pnl)
            
            report = {
                'report_timestamp': datetime.now().isoformat(),
                'report_type': 'risk_management_report',
                'version': self.config.version,
                'configuration': self.config.to_dict(),
                'current_metrics': risk_metrics,
                'daily_statistics': self.daily_stats,
                'position_analysis': self._analyze_positions(positions),
                'recommendations': self._generate_recommendations(risk_metrics, positions)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"❌ Error generating risk report: {e}")
            return {
                'report_timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _analyze_positions(self, positions: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze current positions for risk report"""
        try:
            if not positions:
                return {'total_positions': 0, 'analysis': 'No positions to analyze'}
            
            position_analysis = {
                'total_positions': len(positions),
                'long_positions': len([p for p in positions.values() if p.get('quantity', 0) > 0]),
                'short_positions': len([p for p in positions.values() if p.get('quantity', 0) < 0]),
                'largest_position': max(positions.values(), key=lambda x: abs(x.get('market_value', 0))),
                'total_unrealized_pnl': sum(p.get('unrealized_pl', 0) for p in positions.values()),
                'positions_at_risk': []
            }
            
            # Identify positions at risk
            for symbol, position in positions.items():
                unrealized_pnl_percent = position.get('unrealized_pl_percent', 0)
                if unrealized_pnl_percent < -10:  # Positions losing more than 10%
                    position_analysis['positions_at_risk'].append({
                        'symbol': symbol,
                        'unrealized_pnl_percent': unrealized_pnl_percent,
                        'market_value': position.get('market_value', 0)
                    })
            
            return position_analysis
            
        except Exception as e:
            logger.error(f"❌ Error analyzing positions: {e}")
            return {'error': str(e)}
    
    def _generate_recommendations(self, risk_metrics: Dict[str, Any], 
                                positions: Dict[str, Any]) -> List[str]:
        """Generate risk management recommendations"""
        recommendations = []
        
        try:
            risk_status = risk_metrics.get('risk_status', 'safe')
            risk_utilization = risk_metrics.get('risk_utilization_percent', 0)
            daily_pnl_percent = risk_metrics.get('daily_pnl_percent', 0)
            active_positions = risk_metrics.get('active_positions', 0)
            
            # Risk utilization recommendations
            if risk_utilization > 80:
                recommendations.append("🔴 URGENT: Risk utilization very high. Consider reducing position sizes.")
            elif risk_utilization > 60:
                recommendations.append("🟡 WARNING: Risk utilization elevated. Monitor closely.")
            
            # Daily P&L recommendations
            if daily_pnl_percent < -3:
                recommendations.append("🔴 URGENT: Significant daily loss. Consider stopping new trades.")
            elif daily_pnl_percent < -1:
                recommendations.append("🟡 WARNING: Daily loss accumulating. Review position management.")
            
            # Position count recommendations
            if active_positions > self.config.position_sizing.max_positions * 0.9:
                recommendations.append("🟡 WARNING: Approaching maximum position limit.")
            
            # Emergency recommendations
            if risk_status == 'danger':
                recommendations.append("🚨 DANGER: Multiple risk limits breached. Consider emergency measures.")
            elif risk_status == 'emergency':
                recommendations.append("🚨 EMERGENCY: Emergency stop is active. Review all positions.")
            
            # Position-specific recommendations
            positions_at_risk = [p for p in positions.values() if p.get('unrealized_pl_percent', 0) < -15]
            if positions_at_risk:
                recommendations.append(f"📉 {len(positions_at_risk)} positions losing >15%. Review stop losses.")
            
            if not recommendations:
                recommendations.append("✅ All risk metrics within acceptable ranges.")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"❌ Error generating recommendations: {e}")
            return [f"Error generating recommendations: {str(e)}"]

def main():
    """Test the risk management system"""
    print("🛡️ VolFlow Risk Management System Test")
    print("=" * 50)
    
    # Create risk manager
    risk_manager = RiskManager("test_risk_config.json")
    
    # Test configuration
    print(f"✅ Risk configuration loaded")
    print(f"📊 Max account risk: {risk_manager.config.account_limits.max_account_risk_percent}%")
    print(f"📊 Max position size: {risk_manager.config.position_sizing.max_position_size_percent}%")
    print(f"📊 Daily loss limit: {risk_manager.config.account_limits.daily_loss_limit_percent}%")
    
    # Test position sizing
    account_equity = 100000.0
    entry_price = 150.0
    stop_loss_price = 145.0
    
    shares, dollar_amount, reason = risk_manager.calculate_position_size(
        "AAPL", entry_price, stop_loss_price, account_equity
    )
    
    print(f"\n📈 Position Sizing Test:")
    print(f"   Symbol: AAPL")
    print(f"   Entry: ${entry_price}")
    print(f"   Stop Loss: ${stop_loss_price}")
    print(f"   Account Equity: ${account_equity:,.2f}")
    print(f"   Recommended Shares: {shares}")
    print(f"   Dollar Amount: ${dollar_amount:,.2f}")
    print(f"   Reason: {reason}")
    
    # Test risk metrics
    mock_positions = {
        'AAPL': {'quantity': 100, 'market_value': 15000, 'unrealized_pl': 500, 'unrealized_pl_percent': 3.33},
        'TSLA': {'quantity': 50, 'market_value': 12000, 'unrealized_pl': -800, 'unrealized_pl_percent': -6.25}
    }
    
    daily_pnl = -300
    risk_metrics = risk_manager.get_risk_metrics(account_equity, mock_positions, daily_pnl)
    
    print(f"\n🛡️ Risk Metrics:")
    print(f"   Risk Status: {risk_metrics['risk_status'].upper()}")
    print(f"   Risk Utilization: {risk_metrics['risk_utilization_percent']:.1f}%")
    print(f"   Daily P&L: ${risk_metrics['daily_pnl']:,.2f} ({risk_metrics['daily_pnl_percent']:.2f}%)")
    print(f"   Active Positions: {risk_metrics['active_positions']}")
    print(f"   Available Cash: ${risk_metrics['available_cash']:,.2f}")
    
    if risk_metrics['violations']:
        print(f"   ⚠️ Violations: {', '.join(risk_metrics['violations'])}")
    
    # Test risk report
    report = risk_manager.export_risk_report(account_equity, mock_positions, daily_pnl)
    print(f"\n📋 Risk Report Generated:")
    print(f"   Timestamp: {report['report_timestamp']}")
    print(f"   Total Positions: {report['position_analysis']['total_positions']}")
    print(f"   Recommendations: {len(report['recommendations'])}")
    
    for i, rec in enumerate(report['recommendations'][:3], 1):
        print(f"   {i}. {rec}")
    
    print(f"\n✅ Risk Management System Test Complete")

if __name__ == "__main__":
    main()
