-- Create database for VolFlow Options data
CREATE DATABASE volflow_options;

-- Connect to the new database
\c volflow_options;

-- Create table for metadata
CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    update_interval_seconds DECIMAL,
    symbols_monitored TEXT[],
    data_sources TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for market status
CREATE TABLE IF NOT EXISTS market_status (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    current_time_value TIMESTAMPTZ,
    market_open_time TIMESTAMPTZ,
    market_close_time TIMESTAMPTZ,
    is_market_hours BOOLEAN,
    is_weekday BOOLEAN,
    session_status VARCHAR(20),
    minutes_to_open DECIMAL,
    minutes_to_close DECIMAL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for account data
CREATE TABLE IF NOT EXISTS account_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    account_number VARCHAR(50),
    total_count INTEGER,
    total_market_value DECIMAL,
    total_unrealized_pl DECIMAL,
    total_day_pl DECIMAL,
    equity DECIMAL,
    buying_power DECIMAL,
    available_funds DECIMAL,
    day_trading_buying_power DECIMAL,
    stock_buying_power DECIMAL,
    option_buying_power DECIMAL,
    is_day_trader BOOLEAN,
    is_closing_only_restricted BOOLEAN,
    round_trips INTEGER,
    pfcb_flag BOOLEAN,
    maintenance_requirement DECIMAL,
    equity_percentage DECIMAL,
    margin_balance DECIMAL,
    is_in_call INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for positions
CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    quantity DECIMAL,
    market_value DECIMAL,
    cost_basis DECIMAL,
    unrealized_pl DECIMAL,
    unrealized_pl_percent DECIMAL,
    account VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Create table for transactions
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    transaction_id VARCHAR(100),
    symbol VARCHAR(10),
    transaction_type VARCHAR(50),
    quantity DECIMAL,
    price DECIMAL,
    amount DECIMAL,
    fees DECIMAL,
    account VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for technical indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    current_price DECIMAL,
    sma_20 DECIMAL,
    sma_50 DECIMAL,
    ema_9 DECIMAL,
    ema_21 DECIMAL,
    ema_50 DECIMAL,
    rsi DECIMAL,
    macd DECIMAL,
    macd_signal DECIMAL,
    macd_histogram DECIMAL,
    atr DECIMAL,
    realized_volatility DECIMAL,
    volume BIGINT,
    avg_volume DECIMAL,
    relative_volume_pct DECIMAL,
    trend_direction VARCHAR(20),
    trend_strength DECIMAL,
    pml_price DECIMAL,
    ceiling_price DECIMAL,
    floor_price DECIMAL,
    green_line_price DECIMAL,
    call_delta DECIMAL,
    put_delta DECIMAL,
    net_delta DECIMAL,
    pml_cross_bullish BOOLEAN,
    delta_confirmation BOOLEAN,
    iv_rank DECIMAL,
    range_pct DECIMAL,
    recent_high DECIMAL,
    recent_low DECIMAL,
    is_range_bound BOOLEAN,
    is_low_volatility BOOLEAN,
    is_suitable_for_ic BOOLEAN,
    bullish_divergence_detected BOOLEAN,
    bearish_divergence_detected BOOLEAN,
    divergence_strength VARCHAR(20),
    support_levels JSONB,
    resistance_levels JSONB,
    swing_highs JSONB,
    swing_lows JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for iron condor signals
CREATE TABLE IF NOT EXISTS iron_condor_signals (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(20),
    confidence DECIMAL,
    entry_reason TEXT,
    exit_reason TEXT,
    position_size DECIMAL,
    stop_loss DECIMAL,
    profit_target DECIMAL,
    market_condition VARCHAR(20),
    volatility_environment VARCHAR(20),
    expiration_date DATE,
    dte INTEGER,
    long_put_strike DECIMAL,
    short_put_strike DECIMAL,
    short_call_strike DECIMAL,
    long_call_strike DECIMAL,
    long_put_price DECIMAL,
    short_put_price DECIMAL,
    short_call_price DECIMAL,
    long_call_price DECIMAL,
    net_credit DECIMAL,
    max_profit DECIMAL,
    max_loss DECIMAL,
    breakeven_lower DECIMAL,
    breakeven_upper DECIMAL,
    prob_profit DECIMAL,
    delta DECIMAL,
    gamma DECIMAL,
    theta DECIMAL,
    vega DECIMAL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    implied_vol DECIMAL,
    rho DECIMAL,
    current_price DECIMAL,
    auto_approve BOOLEAN,
    iv_rank DECIMAL
);

-- Create table for PML signals
CREATE TABLE IF NOT EXISTS pml_signals (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(20),
    confidence DECIMAL,
    entry_reason TEXT,
    exit_reason TEXT,
    position_size DECIMAL,
    stop_loss DECIMAL,
    profit_target DECIMAL,
    market_condition VARCHAR(20),
    volatility_environment VARCHAR(20),
    expiration_date DATE,
    dte INTEGER,
    strike DECIMAL,
    option_type VARCHAR(10),
    current_price DECIMAL,
    bid DECIMAL,
    ask DECIMAL,
    pml_price DECIMAL,
    ceiling_price DECIMAL,
    floor_price DECIMAL,
    green_line_price DECIMAL,
    call_delta DECIMAL,
    put_delta DECIMAL,
    net_delta DECIMAL,
    delta DECIMAL,
    gamma DECIMAL,
    theta DECIMAL,
    vega DECIMAL,
    potential_profit DECIMAL,
    max_loss DECIMAL,
    intrinsic_value DECIMAL,
    time_value DECIMAL,
    spot_price DECIMAL,
    volume BIGINT,
    open_interest BIGINT,
    implied_vol DECIMAL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    auto_approve BOOLEAN
);

-- Create table for divergence signals
CREATE TABLE IF NOT EXISTS divergence_signals (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(20),
    confidence DECIMAL,
    entry_reason TEXT,
    exit_reason TEXT,
    position_size DECIMAL,
    stop_loss DECIMAL,
    profit_target DECIMAL,
    market_condition VARCHAR(20),
    volatility_environment VARCHAR(20),
    timeframe VARCHAR(10),
    divergence_type VARCHAR(20),
    direction VARCHAR(10),
    current_price DECIMAL,
    entry_price DECIMAL,
    take_profit DECIMAL,
    first_swing_timestamp TIMESTAMPTZ,
    first_swing_price DECIMAL,
    first_swing_rsi DECIMAL,
    first_swing_type VARCHAR(10),
    second_swing_timestamp TIMESTAMPTZ,
    second_swing_price DECIMAL,
    second_swing_rsi DECIMAL,
    second_swing_type VARCHAR(10),
    risk_amount DECIMAL,
    reward_amount DECIMAL,
    reward_risk_ratio DECIMAL,
    rsi_value DECIMAL,
    macd_value DECIMAL,
    trend_direction VARCHAR(10),
    atr_value DECIMAL,
    support_level DECIMAL,
    resistance_level DECIMAL,
    timeframe_confirmed BOOLEAN,
    confirmation_timeframes TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    auto_approve BOOLEAN
);

-- Create table for integrated watchlist
CREATE TABLE IF NOT EXISTS integrated_watchlist (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    current_price DECIMAL,
    price_change DECIMAL,
    price_change_percent DECIMAL,
    volume BIGINT,
    market_cap BIGINT,
    last_updated TIMESTAMPTZ,
    high_52_week DECIMAL,
    low_52_week DECIMAL,
    avg_volume BIGINT,
    pe_ratio DECIMAL,
    dividend_yield DECIMAL,
    market_status VARCHAR(20),
    sector VARCHAR(50),
    industry VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create table for options contracts
CREATE TABLE IF NOT EXISTS options_contracts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    underlying_price DECIMAL(10,2),
    contract_type VARCHAR(4) NOT NULL CHECK (contract_type IN ('CALL', 'PUT')),
    strike_price DECIMAL(10,2) NOT NULL,
    expiration_date DATE NOT NULL,
    days_to_expiration INTEGER,
    contract_symbol VARCHAR(50),
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    last_price DECIMAL(10,4),
    mark DECIMAL(10,4),
    volume INTEGER DEFAULT 0,
    open_interest INTEGER DEFAULT 0,
    implied_volatility DECIMAL(8,6),
    delta DECIMAL(8,6),
    gamma DECIMAL(8,6),
    theta DECIMAL(8,6),
    vega DECIMAL(8,6),
    rho DECIMAL(8,6),
    time_value DECIMAL(10,4),
    intrinsic_value DECIMAL(10,4),
    in_the_money BOOLEAN,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for PnL statistics
CREATE TABLE IF NOT EXISTS pnl_statistics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    overall_wins INTEGER DEFAULT 0,
    overall_losses INTEGER DEFAULT 0,
    overall_profit_loss DECIMAL(15,2) DEFAULT 0.0,
    overall_win_rate DECIMAL(5,2) DEFAULT 0.0,
    overall_avg_win DECIMAL(15,2) DEFAULT 0.0,
    overall_avg_loss DECIMAL(15,2) DEFAULT 0.0,
    overall_win_loss_ratio DECIMAL(10,4) DEFAULT 0.0,
    long_wins INTEGER DEFAULT 0,
    long_losses INTEGER DEFAULT 0,
    long_profit_loss DECIMAL(15,2) DEFAULT 0.0,
    long_win_rate DECIMAL(5,2) DEFAULT 0.0,
    long_avg_win DECIMAL(15,2) DEFAULT 0.0,
    long_avg_loss DECIMAL(15,2) DEFAULT 0.0,
    short_wins INTEGER DEFAULT 0,
    short_losses INTEGER DEFAULT 0,
    short_profit_loss DECIMAL(15,2) DEFAULT 0.0,
    short_win_rate DECIMAL(5,2) DEFAULT 0.0,
    short_avg_win DECIMAL(15,2) DEFAULT 0.0,
    short_avg_loss DECIMAL(15,2) DEFAULT 0.0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create table for system status
CREATE TABLE IF NOT EXISTS system_status (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    monitor_running BOOLEAN,
    symbols_count INTEGER,
    positions_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_metadata_timestamp ON metadata(timestamp);
CREATE INDEX IF NOT EXISTS idx_market_status_timestamp ON market_status(timestamp);
CREATE INDEX IF NOT EXISTS idx_account_data_timestamp ON account_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions(timestamp);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_symbol ON transactions(symbol);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_timestamp ON technical_indicators(timestamp);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol ON technical_indicators(symbol);
CREATE INDEX IF NOT EXISTS idx_iron_condor_signals_timestamp ON iron_condor_signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_iron_condor_signals_symbol ON iron_condor_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_pml_signals_timestamp ON pml_signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_pml_signals_symbol ON pml_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_divergence_signals_timestamp ON divergence_signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_divergence_signals_symbol ON divergence_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_integrated_watchlist_timestamp ON integrated_watchlist(timestamp);
CREATE INDEX IF NOT EXISTS idx_integrated_watchlist_symbol ON integrated_watchlist(symbol);
CREATE INDEX IF NOT EXISTS idx_integrated_watchlist_created_at ON integrated_watchlist(created_at);
CREATE INDEX IF NOT EXISTS idx_options_contracts_timestamp ON options_contracts(timestamp);
CREATE INDEX IF NOT EXISTS idx_options_contracts_symbol ON options_contracts(symbol);
CREATE INDEX IF NOT EXISTS idx_options_contracts_expiration ON options_contracts(expiration_date);
CREATE INDEX IF NOT EXISTS idx_options_contracts_strike ON options_contracts(strike_price);
CREATE INDEX IF NOT EXISTS idx_options_contracts_type ON options_contracts(contract_type);
CREATE INDEX IF NOT EXISTS idx_pnl_statistics_timestamp ON pnl_statistics(timestamp);
CREATE INDEX IF NOT EXISTS idx_system_status_timestamp ON system_status(timestamp);

-- Display created tables
\dt
