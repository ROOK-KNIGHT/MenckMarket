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
    current_time TIMESTAMPTZ,
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
    created_at TIMESTAMPTZ DEFAULT NOW()
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
    bid DECIMAL,
    ask DECIMAL,
    volume BIGINT,
    change_amount DECIMAL,
    change_percent DECIMAL,
    relative_volume_pct DECIMAL,
    avg_volume_14d BIGINT,
    atr_14d DECIMAL,
    first_5min_high DECIMAL,
    first_5min_low DECIMAL,
    first_5min_close DECIMAL,
    first_5min_volume BIGINT,
    meets_criteria BOOLEAN,
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
    created_at TIMESTAMPTZ DEFAULT NOW()
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
    created_at TIMESTAMPTZ DEFAULT NOW()
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
    created_at TIMESTAMPTZ DEFAULT NOW()
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
    added_via VARCHAR(20),
    status VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
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
CREATE INDEX IF NOT EXISTS idx_system_status_timestamp ON system_status(timestamp);

-- Display created tables
\dt
