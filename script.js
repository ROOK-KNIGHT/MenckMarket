// VolFlow Options Breakout Screener - JavaScript
// Handles UI interactions and future backend integration

// Global variables for trading statistics
let winLossChart;

// WebSocket connection for real-time data streaming
class WebSocketManager {
    constructor() {
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000;
        this.isConnected = false;
        this.callbacks = {
            onData: [],
            onConnect: [],
            onDisconnect: [],
            onError: []
        };
        
        this.connect();
    }
    
    connect() {
        try {
            console.log('🔄 Connecting to WebSocket server...');
            // Connect directly to WebSocket server on port 8765
            const wsUrl = 'ws://localhost:8765';
            console.log(`📡 WebSocket URL: ${wsUrl}`);
            this.ws = new WebSocket(wsUrl);
            
            this.ws.onopen = () => {
                console.log('✅ WebSocket connected');
                this.isConnected = true;
                this.reconnectAttempts = 0;
                this.updateConnectionStatus('connected');
                this.callbacks.onConnect.forEach(callback => callback());
            };
            
            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    console.log('📡 Received WebSocket data:', data.timestamp);
                    this.callbacks.onData.forEach(callback => callback(data));
                } catch (error) {
                    console.error('❌ Error parsing WebSocket data:', error);
                }
            };
            
            this.ws.onclose = () => {
                console.log('🔌 WebSocket disconnected');
                this.isConnected = false;
                this.updateConnectionStatus('disconnected');
                this.callbacks.onDisconnect.forEach(callback => callback());
                this.attemptReconnect();
            };
            
            this.ws.onerror = (error) => {
                console.error('❌ WebSocket error:', error);
                this.callbacks.onError.forEach(callback => callback(error));
            };
            
        } catch (error) {
            console.error('❌ Failed to create WebSocket connection:', error);
            this.attemptReconnect();
        }
    }
    
    attemptReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            console.log(`🔄 Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts})...`);
            
            setTimeout(() => {
                this.connect();
            }, this.reconnectDelay * this.reconnectAttempts);
        } else {
            console.error('❌ Max reconnection attempts reached');
            this.updateConnectionStatus('failed');
        }
    }
    
    updateConnectionStatus(status) {
        const statusElements = document.querySelectorAll('.connection-status');
        statusElements.forEach(element => {
            switch (status) {
                case 'connected':
                    element.textContent = '🟢 Connected';
                    element.className = 'connection-status connected';
                    break;
                case 'disconnected':
                    element.textContent = '🟡 Reconnecting...';
                    element.className = 'connection-status reconnecting';
                    break;
                case 'failed':
                    element.textContent = '🔴 Connection Failed';
                    element.className = 'connection-status failed';
                    break;
            }
        });
    }
    
    send(data) {
        if (this.isConnected && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(data));
        } else {
            console.warn('⚠️ WebSocket not connected, cannot send data');
        }
    }
    
    ping() {
        this.send({ type: 'ping' });
    }
    
    requestFreshData() {
        this.send({ type: 'request_data' });
    }
    
    subscribe(dataTypes) {
        this.send({ type: 'subscribe', data_types: dataTypes });
    }
    
    on(event, callback) {
        if (this.callbacks[event]) {
            this.callbacks[event].push(callback);
        }
    }
    
    off(event, callback) {
        if (this.callbacks[event]) {
            const index = this.callbacks[event].indexOf(callback);
            if (index > -1) {
                this.callbacks[event].splice(index, 1);
            }
        }
    }
    
    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }
}

class VolFlowScreener {
    constructor() {
        this.config = {
            dte: 30,
            closeTime: '15:50',
            relVolLength: 5,
            atrLength: 14,
            stopLossPct: 10,
            minPrice: 5.00
        };
        
        this.isScreening = false;
        this.screeningResults = [];
        this.startTime = null;
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.loadSavedConfig();
        this.updateStatusDisplay();
        this.updateAccountInfo();
        this.initializeToggleStates();
        
        // Initialize WebSocket connection for real-time data
        this.initializeWebSocket();
        
        // Start auto-refresh for live data (fallback)
        this.startAutoRefresh(30); // Refresh every 5 seconds as fallback
        
        // Initial load of live data
        this.refreshPositions();
    }
    
    initializeWebSocket() {
        console.log('🚀 Initializing WebSocket connection...');
        
        // Create WebSocket manager
        this.wsManager = new WebSocketManager();
        
        // Set up WebSocket event handlers
        this.wsManager.on('onConnect', () => {
            console.log('✅ WebSocket connected - subscribing to data streams');
            this.showToast('Real-time data connection established', 'success');
            
            // Subscribe to all data types
            this.wsManager.subscribe([
                'pml_signals', 'iron_condor_signals', 'divergence_signals',
                'watchlist_data', 'positions', 'account_data', 'market_status', 'pnl_statistics'
            ]);
            
            // Stop fallback auto-refresh since WebSocket is active
            this.stopAutoRefresh();
        });
        
        this.wsManager.on('onDisconnect', () => {
            console.log('🔌 WebSocket disconnected - falling back to polling');
            this.showToast('Real-time connection lost, using fallback mode', 'warning');
            
            // Restart auto-refresh as fallback
            this.startAutoRefresh(5);
        });
        
        this.wsManager.on('onData', (data) => {
            console.log('📡 Processing real-time data update');
            
            // Handle risk configuration messages first
            if (data.type && data.type.startsWith('risk_config')) {
                handleRiskConfigWebSocketMessage(data);
                return;
            }
            
            // Handle other WebSocket data
            this.handleWebSocketData(data);
        });
        
        this.wsManager.on('onError', (error) => {
            console.error('❌ WebSocket error:', error);
            this.showToast('Real-time data connection error', 'error');
        });
        
        // Set up periodic ping to keep connection alive
        setInterval(() => {
            if (this.wsManager.isConnected) {
                this.wsManager.ping();
            }
        }, 30000); // Ping every 30 seconds
    }
    
    handleWebSocketData(data) {
        try {
            console.log('📊 Processing WebSocket data with keys:', Object.keys(data));
            
            // Update last data timestamp
            this.lastWebSocketUpdate = new Date();
            
            // Process different data types
            if (data.positions && Array.isArray(data.positions)) {
                console.log('📈 Processing positions data:', data.positions.length, 'positions');
                this.updatePositionsFromWebSocket(data);
            }
            
            if (data.account_data) {
                console.log('💰 Processing account data');
                this.updateAccountFromWebSocket(data);
            }
            
            if (data.pml_signals || data.iron_condor_signals || data.divergence_signals) {
                console.log('🎯 Processing strategy signals');
                this.updateStrategiesFromWebSocket(data);
            }
            
            if (data.watchlist_data && Array.isArray(data.watchlist_data)) {
                console.log('📋 Processing watchlist data:', data.watchlist_data.length, 'symbols');
                this.updateWatchlistFromWebSocket(data);
            }
            
            if (data.market_status) {
                console.log('🕐 Processing market status');
                this.updateMarketStatusFromWebSocket(data);
            }
            
            if (data.pnl_statistics) {
                console.log('📊 Processing PnL statistics:', data.pnl_statistics);
                this.updatePnLStatisticsFromWebSocket(data);
            }
            
            // Update connection status indicator
            this.updateLastUpdateTime();
            
        } catch (error) {
            console.error('❌ Error processing WebSocket data:', error);
            console.error('❌ Error details:', error.message);
            console.error('❌ Data received:', data);
        }
    }
    
    updatePositionsFromWebSocket(data) {
        if (data.positions && Array.isArray(data.positions)) {
            // Convert WebSocket position data to display format
            const positions = data.positions.map(position => ({
                symbol: position.symbol,
                type: position.quantity > 0 ? 'Long' : 'Short',
                quantity: Math.abs(position.quantity),
                plOpen: position.unrealized_pl || 0,
                plDay: 0, // Not available in current structure
                marketValue: position.market_value || 0,
                costBasis: position.cost_basis || 0
            }));
            
            this.updatePositionsListFromLive(positions);
            
            // Calculate and update summary from positions data
            const summary = this.calculatePositionsSummary(positions);
            this.updatePositionsSummary(summary);
        }
    }
    
    calculatePositionsSummary(positions) {
        const summary = {
            total_positions: positions.length,
            total_unrealized_pl: 0,
            total_day_pl: 0,
            total_market_value: 0
        };
        
        positions.forEach(position => {
            summary.total_unrealized_pl += position.plOpen || 0;
            summary.total_day_pl += position.plDay || 0;
            summary.total_market_value += position.marketValue || 0;
        });
        
        return summary;
    }
    
    updatePositionsSummary(summary) {
        // Update summary display elements
        const plOpenElement = document.querySelector('.summary-item:nth-child(1) .summary-value');
        const plDayElement = document.querySelector('.summary-item:nth-child(2) .summary-value');
        const totalPositionsElement = document.querySelector('.summary-item:nth-child(3) .summary-value');
        
        if (plOpenElement) {
            plOpenElement.textContent = this.formatCurrency(summary.total_unrealized_pl || 0);
            plOpenElement.className = `summary-value ${(summary.total_unrealized_pl || 0) >= 0 ? 'positive' : 'negative'}`;
        }
        
        if (plDayElement) {
            plDayElement.textContent = this.formatCurrency(summary.total_day_pl || 0);
            plDayElement.className = `summary-value ${(summary.total_day_pl || 0) >= 0 ? 'positive' : 'negative'}`;
        }
        
        if (totalPositionsElement) {
            totalPositionsElement.textContent = (summary.total_positions || 0).toString();
        }
        
        // Update summary icons
        const plOpenIcon = document.querySelector('.summary-item:nth-child(1) .summary-icon');
        const plDayIcon = document.querySelector('.summary-item:nth-child(2) .summary-icon');
        
        if (plOpenIcon) {
            plOpenIcon.className = `summary-icon ${(summary.total_unrealized_pl || 0) >= 0 ? 'positive' : 'negative'}`;
        }
        if (plDayIcon) {
            plDayIcon.className = `summary-icon ${(summary.total_day_pl || 0) >= 0 ? 'positive' : 'negative'}`;
        }
    }
    
    updateAccountFromWebSocket(data) {
        if (data.account_data) {
            const accountData = data.account_data;
            
            // Update equity display
            if (accountData.equity) {
                const equityElement = document.querySelector('.equity-amount');
                if (equityElement) {
                    equityElement.textContent = `$${accountData.equity.toLocaleString('en-US', { 
                        minimumFractionDigits: 2, 
                        maximumFractionDigits: 2 
                    })}`;
                }
            }
            
            // Update P/L Day from account data (not positions)
            if (accountData.total_day_pl !== undefined) {
                const plDayElement = document.querySelector('.summary-item:nth-child(2) .summary-value');
                const plDayIcon = document.querySelector('.summary-item:nth-child(2) .summary-icon');
                
                if (plDayElement) {
                    plDayElement.textContent = this.formatCurrency(accountData.total_day_pl);
                    plDayElement.className = `summary-value ${accountData.total_day_pl >= 0 ? 'positive' : 'negative'}`;
                }
                
                if (plDayIcon) {
                    plDayIcon.className = `summary-icon ${accountData.total_day_pl >= 0 ? 'positive' : 'negative'}`;
                }
                
                console.log(`📊 Updated P/L Day from account data: ${this.formatCurrency(accountData.total_day_pl)}`);
            }
            
            // Update account number if available
            if (accountData.account_number) {
                const accountElement = document.querySelector('.account-number');
                if (accountElement) {
                    const maskedAccount = `****${accountData.account_number.slice(-4)}`;
                    accountElement.textContent = maskedAccount;
                }
            }
        }
        
        // Update Risk Management overview cards with real data
        this.updateRiskManagementOverview(data);
    }
    
    async updateRiskManagementOverview(data) {
        if (!data.account_data && !data.positions) return;
        
        const accountData = data.account_data || {};
        const positions = data.positions || [];
        const summary = data.summary || {};
        
        console.log('📊 Updating Risk Management overview with real data');
        
        // Calculate real metrics
        const currentEquity = accountData.equity || 0;
        const totalDayPL = accountData.total_day_pl || 0;
        const totalUnrealizedPL = accountData.total_unrealized_pl || 0;
        const totalMarketValue = accountData.total_market_value || 0;
        const activePositionsCount = summary.total_positions || positions.length || 0;
        
        // Load risk configuration from file
        const riskConfig = await this.loadRiskConfigFromFile();
        
        // 1. Update Account Equity
        this.updateRiskOverviewCard('current-equity', currentEquity, 'currency', {
            change: totalDayPL,
            changePercent: currentEquity > 0 ? (totalDayPL / currentEquity) * 100 : 0
        });
        
        // 2. Update Risk Utilization
        const riskUtilization = currentEquity > 0 ? (Math.abs(totalUnrealizedPL) / currentEquity) * 100 : 0;
        const maxRiskPercent = riskConfig.max_account_risk || 25;
        this.updateRiskOverviewCard('risk-utilization', riskUtilization, 'percentage', {
            max: maxRiskPercent,
            status: this.getRiskStatus(riskUtilization, maxRiskPercent)
        });
        
        // 3. Update Active Positions
        const maxPositions = riskConfig.max_positions || 15;
        this.updateRiskOverviewCard('active-positions', activePositionsCount, 'count', {
            max: maxPositions,
            status: this.getRiskStatus(activePositionsCount, maxPositions, 'count')
        });
        
        // 4. Update Daily Drawdown
        const dailyDrawdownPercent = currentEquity > 0 ? (totalDayPL / currentEquity) * 100 : 0;
        this.updateRiskOverviewCard('daily-drawdown', dailyDrawdownPercent, 'percentage', {
            isDrawdown: true,
            status: totalDayPL >= 0 ? 'positive' : 'negative'
        });
        
        console.log('✅ Risk Management overview updated with real data:', {
            equity: currentEquity,
            riskUtilization: riskUtilization.toFixed(2) + '%',
            activePositions: activePositionsCount,
            dailyDrawdown: dailyDrawdownPercent.toFixed(2) + '%',
            configSource: 'risk_config_live.json'
        });
    }
    
    async loadRiskConfigFromFile() {
        try {
            // Check if we have cached config
            if (window.riskConfigCache && window.riskConfigCache.timestamp && 
                (Date.now() - window.riskConfigCache.timestamp) < 30000) { // Cache for 30 seconds
                return window.riskConfigCache.config;
            }
            
            console.log('📥 Loading risk configuration from risk_config_live.json');
            const response = await fetch('./risk_config_live.json');
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const riskConfigData = await response.json();
            
            if (riskConfigData.risk_management) {
                const config = {
                    // Account limits
                    max_account_risk: riskConfigData.risk_management.account_limits?.max_account_risk || 25,
                    daily_loss_limit: riskConfigData.risk_management.account_limits?.daily_loss_limit || 5,
                    equity_buffer: riskConfigData.risk_management.account_limits?.equity_buffer || 10000,
                    
                    // Position sizing
                    max_position_size: riskConfigData.risk_management.position_sizing?.max_position_size || 5,
                    max_positions: riskConfigData.risk_management.position_sizing?.max_positions || 15,
                    
                    // Stop loss settings
                    stop_loss_method: riskConfigData.risk_management.stop_loss_settings?.method || 'atr',
                    stop_loss_value: riskConfigData.risk_management.stop_loss_settings?.value || 2,
                    take_profit_ratio: riskConfigData.risk_management.stop_loss_settings?.take_profit_ratio || 2,
                    
                    // Parameter states
                    parameter_states: riskConfigData.risk_management.parameter_states || {},
                    
                    // Metadata
                    last_updated: riskConfigData.risk_management.metadata?.last_updated,
                    updated_by: riskConfigData.risk_management.metadata?.updated_by
                };
                
                // Cache the config
                window.riskConfigCache = {
                    config: config,
                    timestamp: Date.now()
                };
                
                console.log('✅ Risk configuration loaded from file:', config);
                return config;
            } else {
                throw new Error('Invalid risk configuration structure');
            }
            
        } catch (error) {
            console.error('❌ Error loading risk configuration from file:', error);
            
            // Return default configuration as fallback
            return this.getDefaultRiskConfig();
        }
    }
    
    updateRiskOverviewCard(cardId, value, type, options = {}) {
        const cardElement = document.getElementById(cardId);
        if (!cardElement) return;
        
        let formattedValue = '';
        let displayText = '';
        
        switch (type) {
            case 'currency':
                formattedValue = `$${value.toLocaleString('en-US', { 
                    minimumFractionDigits: 2, 
                    maximumFractionDigits: 2 
                })}`;
                
                if (options.change !== undefined) {
                    const changePercent = options.changePercent || 0;
                    const changeClass = changePercent >= 0 ? 'positive' : 'negative';
                    const changeSign = changePercent >= 0 ? '+' : '';
                    displayText = `${formattedValue}\n${changeSign}${changePercent.toFixed(2)}%`;
                    
                    // Update card styling based on change
                    cardElement.className = `risk-overview-card ${changeClass}`;
                } else {
                    displayText = formattedValue;
                }
                break;
                
            case 'percentage':
                if (options.max) {
                    displayText = `${value.toFixed(1)}%\n/ ${options.max}% Max`;
                    
                    // Update card styling based on risk status
                    if (options.status) {
                        cardElement.className = `risk-overview-card ${options.status}`;
                    }
                } else {
                    const sign = options.isDrawdown ? (value >= 0 ? '+' : '') : '';
                    displayText = `${sign}${value.toFixed(2)}%`;
                    
                    if (options.status) {
                        cardElement.className = `risk-overview-card ${options.status}`;
                    }
                }
                break;
                
            case 'count':
                if (options.max) {
                    displayText = `${value}\n/ ${options.max} Max`;
                    
                    // Update card styling based on risk status
                    if (options.status) {
                        cardElement.className = `risk-overview-card ${options.status}`;
                    }
                } else {
                    displayText = value.toString();
                }
                break;
        }
        
        // Update the card content
        const valueElement = cardElement.querySelector('.risk-value');
        if (valueElement) {
            // Handle multi-line text
            const lines = displayText.split('\n');
            if (lines.length > 1) {
                valueElement.innerHTML = lines.map(line => `<div>${line}</div>`).join('');
            } else {
                valueElement.textContent = displayText;
            }
        }
    }
    
    getRiskStatus(current, max, type = 'percentage') {
        if (type === 'count') {
            if (current >= max) return 'danger';
            if (current >= max * 0.8) return 'warning';
            return 'safe';
        } else {
            if (current >= max * 0.9) return 'danger';
            if (current >= max * 0.7) return 'warning';
            return 'safe';
        }
    }
    
    getDefaultRiskConfig() {
        return {
            // Use the same property names as the file structure
            max_account_risk: 25.0,
            daily_loss_limit: 5.0,
            equity_buffer: 10000.0,
            max_position_size: 5.0,
            max_positions: 15,
            stop_loss_method: 'atr',
            stop_loss_value: 2.0,
            take_profit_ratio: 2.0,
            parameter_states: {
                enable_max_account_risk: true,
                enable_daily_loss_limit: true,
                enable_equity_buffer: true,
                enable_max_position_size: true,
                enable_max_positions: true,
                enable_stop_loss: true,
                enable_stop_loss_value: true,
                enable_risk_reward_ratio: true
            }
        };
    }
    
    updateStrategiesFromWebSocket(data) {
        // Update strategy cards with real-time signal data
        if (data.pml_signals) {
            this.updateStrategyCardFromWebSocket('pml', data.pml_signals);
        }
        
        if (data.iron_condor_signals) {
            this.updateStrategyCardFromWebSocket('iron-condor', data.iron_condor_signals);
        }
        
        if (data.divergence_signals) {
            this.updateStrategyCardFromWebSocket('divergence', data.divergence_signals);
        }
    }
    
    updateStrategyCardFromWebSocket(strategyType, signals) {
        const statusElement = document.getElementById(`${strategyType}-status`);
        const signalsElement = document.getElementById(`${strategyType}-signals`);
        
        if (!statusElement || !signalsElement || !Array.isArray(signals)) {
            console.log(`❌ Invalid parameters for ${strategyType}:`, {
                statusElement: !!statusElement,
                signalsElement: !!signalsElement,
                signalsIsArray: Array.isArray(signals),
                signalsLength: signals ? signals.length : 'null'
            });
            return;
        }
        
        console.log(`📊 Processing ${strategyType} signals from WebSocket:`, signals.length, 'signals');
        
        // Find active signals (signals that are not NO_SIGNAL)
        const activeSignals = signals.filter(s => s.signal_type && s.signal_type !== 'NO_SIGNAL');
        
        console.log(`🎯 Found ${activeSignals.length} active signals for ${strategyType}`);
        
        if (activeSignals.length === 0) {
            this.updateStrategyStatus(statusElement, 'NO_SIGNAL', 'no-signal');
            this.showStrategyPlaceholder(signalsElement, 'No active signals');
        } else {
            const primarySignal = this.getPrimarySignal(activeSignals);
            console.log(`🏆 Primary signal for ${strategyType}:`, primarySignal.signal_type, 'for', primarySignal.symbol);
            this.updateStrategyStatus(statusElement, primarySignal.signal_type, this.getSignalClass(primarySignal.signal_type));
            this.showStrategySignals(signalsElement, activeSignals.slice(0, 3));
        }
    }
    
    updateWatchlistFromWebSocket(data) {
        if (data.watchlist_data && Array.isArray(data.watchlist_data)) {
            console.log('📊 Updating watchlist from WebSocket:', data.watchlist_data.length, 'symbols');
            
            // Update the global watchlist data with real-time PostgreSQL data
            watchlistSymbols = [];
            watchlistData = {};
            
            data.watchlist_data.forEach(symbolData => {
                const symbol = symbolData.symbol;
                watchlistSymbols.push(symbol);
                watchlistData[symbol] = {
                    symbol: symbol,
                    current_price: parseFloat(symbolData.current_price || 0),
                    price_change: parseFloat(symbolData.price_change || 0),
                    price_change_percent: parseFloat(symbolData.price_change_percent || 0),
                    volume: parseInt(symbolData.volume || 0),
                    last_updated: symbolData.timestamp || new Date().toISOString()
                };
            });
            
            // Update watchlist display with real-time data
            updateWatchlistDisplay();
            updateWatchlistStatus();
            
            console.log('✅ Watchlist updated with', watchlistSymbols.length, 'symbols from PostgreSQL');
        }
    }
    
    updateMarketStatusFromWebSocket(data) {
        if (data.market_status) {
            const marketStatus = data.market_status;
            
            // Update market status indicators
            const statusItems = document.querySelectorAll('.status-item-horizontal');
            if (statusItems.length > 0) {
                const systemStatusIcon = statusItems[0]?.querySelector('.status-icon');
                const systemStatusValue = statusItems[0]?.querySelector('.status-value');
                
                if (systemStatusIcon && systemStatusValue) {
                    if (marketStatus.is_market_hours) {
                        systemStatusIcon.className = 'status-icon ready';
                        systemStatusIcon.innerHTML = '<i class="fas fa-check-circle"></i>';
                        systemStatusValue.textContent = 'Market Open';
                    } else {
                        systemStatusIcon.className = 'status-icon';
                        systemStatusIcon.style.background = '#fed7d7';
                        systemStatusIcon.style.color = '#e53e3e';
                        systemStatusIcon.innerHTML = '<i class="fas fa-moon"></i>';
                        systemStatusValue.textContent = 'Market Closed';
                    }
                }
            }
        }
    }
    
    updatePnLStatisticsFromWebSocket(data) {
        if (data.pnl_statistics && data.pnl_statistics.overall) {
            const overall = data.pnl_statistics.overall;
            
            console.log('📊 Updating PnL statistics from WebSocket:', overall);
            
            // Update trading metrics with smooth transitions
            this.smoothUpdateMetricElement('win-rate', `${(overall.win_rate || 0).toFixed(1)}%`);
            this.smoothUpdateMetricElement('total-trades', (overall.wins || 0) + (overall.losses || 0));
            this.smoothUpdateMetricElement('avg-win', `$${(overall.avg_win || 0).toFixed(2)}`, 'positive');
            this.smoothUpdateMetricElement('avg-loss', `$${Math.abs(overall.avg_loss || 0).toFixed(2)}`, 'negative');
            this.smoothUpdateMetricElement('total-pl', `$${(overall.profit_loss || 0).toFixed(2)}`, (overall.profit_loss || 0) >= 0 ? 'positive' : 'negative');
            this.smoothUpdateMetricElement('wl-ratio', (overall.win_loss_ratio || 0).toFixed(2));
            
            // Update win/loss chart with real-time data (only if data changed)
            this.updateWinLossChartFromWebSocket(overall);
        }
    }
    
    smoothUpdateMetricElement(elementId, value, className = null) {
        const element = document.getElementById(elementId);
        if (!element) return;
        
        // Only update if value actually changed
        if (element.textContent === value.toString()) return;
        
        // Remove existing color classes
        element.classList.remove('positive', 'negative');
        
        // Add new class if specified
        if (className) {
            element.classList.add(className);
        }
        
        // Add subtle highlight effect for changed values
        element.style.transition = 'background-color 0.3s ease, color 0.3s ease';
        element.style.backgroundColor = 'rgba(102, 126, 234, 0.1)';
        element.textContent = value;
        
        // Remove highlight after animation
        setTimeout(() => {
            element.style.backgroundColor = '';
        }, 300);
    }
    
    updateWinLossChartFromWebSocket(overall) {
        const wins = overall.wins || 0;
        const losses = overall.losses || 0;
        
        // Get canvas element
        const canvas = document.getElementById('winLossChart');
        if (!canvas) return;
        
        const ctx = canvas.getContext('2d');
        
        // Destroy existing chart if it exists
        if (winLossChart) {
            winLossChart.destroy();
        }
        
        // Create new chart only if we have data
        if (wins > 0 || losses > 0) {
            winLossChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Wins', 'Losses'],
                    datasets: [{
                        data: [wins, losses],
                        backgroundColor: [
                            '#38a169', // Green for wins
                            '#e53e3e'  // Red for losses
                        ],
                        borderWidth: 2,
                        borderColor: '#ffffff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 15,
                                font: {
                                    size: 12,
                                    family: 'Inter'
                                }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const label = context.label || '';
                                    const value = context.parsed;
                                    const total = wins + losses;
                                    const percentage = ((value / total) * 100).toFixed(1);
                                    return `${label}: ${value} (${percentage}%)`;
                                }
                            }
                        }
                    },
                    cutout: '60%',
                    animation: {
                        animateRotate: true,
                        duration: 1000
                    }
                }
            });
        } else {
            // Show empty state
            this.showEmptyChart(ctx);
        }
    }
    
    updateLastUpdateTime() {
        const timeElements = document.querySelectorAll('.last-update-time, #last-update-time');
        const now = new Date();
        const timeString = now.toLocaleTimeString([], { 
            hour: '2-digit', 
            minute: '2-digit',
            second: '2-digit'
        });
        
        timeElements.forEach(element => {
            if (element) {
                element.textContent = timeString;
            }
        });
    }
    
    bindEvents() {
        // Configuration form events
        const startScreeningBtn = document.getElementById('start-screening');
        const resetConfigBtn = document.getElementById('reset-config');
        const saveConfigBtn = document.getElementById('save-config');
        
        if (startScreeningBtn) startScreeningBtn.addEventListener('click', () => this.startScreening());
        if (resetConfigBtn) resetConfigBtn.addEventListener('click', () => this.resetConfig());
        if (saveConfigBtn) saveConfigBtn.addEventListener('click', () => this.saveConfig());
        
        // Export events
        const exportCsvBtn = document.getElementById('export-csv');
        const exportJsonBtn = document.getElementById('export-json');
        
        if (exportCsvBtn) exportCsvBtn.addEventListener('click', () => this.exportResults('csv'));
        if (exportJsonBtn) exportJsonBtn.addEventListener('click', () => this.exportResults('json'));
        
        // Modal events
        const cancelScreeningBtn = document.getElementById('cancel-screening');
        if (cancelScreeningBtn) cancelScreeningBtn.addEventListener('click', () => this.cancelScreening());
        
        // Positions events
        const refreshPositionsBtn = document.getElementById('refresh-positions');
        if (refreshPositionsBtn) refreshPositionsBtn.addEventListener('click', () => this.refreshPositions());
        
        // Strategy Dashboard events
        const refreshStrategiesBtn = document.getElementById('refresh-strategies');
        if (refreshStrategiesBtn) refreshStrategiesBtn.addEventListener('click', () => this.refreshStrategies());
        
        // Trading control events
        const envToggle = document.getElementById('env-toggle');
        const tradeToggle = document.getElementById('trade-toggle');
        
        // Environment toggle with comprehensive event listeners
        if (envToggle) {
            // Add change listener to checkbox
            envToggle.addEventListener('change', (e) => this.handleEnvironmentToggle(e.target.checked));
            
            // Add click listener to the toggle switch container
            const envToggleSwitch = envToggle.closest('.toggle-switch');
            if (envToggleSwitch) {
                envToggleSwitch.addEventListener('click', (e) => {
                    // Prevent double-triggering if clicking directly on the input
                    if (e.target === envToggle) return;
                    
                    console.log('Environment toggle switch clicked');
                    // Toggle the checkbox state
                    envToggle.checked = !envToggle.checked;
                    // Trigger the change event
                    envToggle.dispatchEvent(new Event('change'));
                });
                
                // Add click listener to the slider specifically
                const envSlider = envToggleSwitch.querySelector('.toggle-slider');
                if (envSlider) {
                    envSlider.addEventListener('click', (e) => {
                        e.stopPropagation();
                        console.log('Environment toggle slider clicked');
                        envToggle.checked = !envToggle.checked;
                        envToggle.dispatchEvent(new Event('change'));
                    });
                }
            }
        }
        
        // Trading toggle with comprehensive event listeners
        if (tradeToggle) {
            // Add change listener to checkbox
            tradeToggle.addEventListener('change', (e) => this.handleTradingToggle(e.target.checked));
            
            // Add click listener to the toggle switch container
            const tradeToggleSwitch = tradeToggle.closest('.toggle-switch');
            if (tradeToggleSwitch) {
                tradeToggleSwitch.addEventListener('click', (e) => {
                    // Prevent double-triggering if clicking directly on the input
                    if (e.target === tradeToggle) return;
                    
                    console.log('Trading toggle switch clicked');
                    // Toggle the checkbox state
                    tradeToggle.checked = !tradeToggle.checked;
                    // Trigger the change event
                    tradeToggle.dispatchEvent(new Event('change'));
                });
                
                // Add click listener to the slider specifically
                const tradeSlider = tradeToggleSwitch.querySelector('.toggle-slider');
                if (tradeSlider) {
                    tradeSlider.addEventListener('click', (e) => {
                        e.stopPropagation();
                        console.log('Trading toggle slider clicked');
                        tradeToggle.checked = !tradeToggle.checked;
                        tradeToggle.dispatchEvent(new Event('change'));
                    });
                }
            }
        }
        
        // Configuration input events
        const configInputs = document.querySelectorAll('.config-input, .config-select');
        configInputs.forEach(input => {
            if (input) {
                input.addEventListener('change', (e) => this.updateConfig(e.target.id, e.target.value));
                input.addEventListener('input', (e) => this.validateInput(e.target));
            }
        });
    }
    
    updateConfig(key, value) {
        // Convert key from kebab-case to camelCase
        const camelKey = key.replace(/-([a-z])/g, (g) => g[1].toUpperCase());
        
        // Parse numeric values
        if (['dte', 'relVolLength', 'atrLength', 'stopLossPct', 'minPrice'].includes(camelKey)) {
            value = parseFloat(value);
        }
        
        this.config[camelKey] = value;
        this.updateStatusDisplay();
    }
    
    validateInput(input) {
        const value = parseFloat(input.value);
        const min = parseFloat(input.min);
        const max = parseFloat(input.max);
        
        // Remove any existing validation classes
        input.classList.remove('error', 'warning');
        
        // Validate range
        if (value < min || value > max) {
            input.classList.add('error');
            this.showToast(`${input.previousElementSibling.textContent} must be between ${min} and ${max}`, 'error');
        } else if (input.id === 'stop-loss-pct' && value < 5) {
            input.classList.add('warning');
        }
    }
    
    resetConfig() {
        this.config = {
            dte: 30,
            closeTime: '15:50',
            relVolLength: 5,
            atrLength: 14,
            stopLossPct: 10,
            minPrice: 5.00
        };
        
        this.updateConfigDisplay();
        this.showToast('Configuration reset to defaults', 'success');
    }
    
    updateConfigDisplay() {
        document.getElementById('dte').value = this.config.dte;
        document.getElementById('close-time').value = this.config.closeTime;
        document.getElementById('rel-vol-length').value = this.config.relVolLength;
        document.getElementById('atr-length').value = this.config.atrLength;
        document.getElementById('stop-loss-pct').value = this.config.stopLossPct;
        document.getElementById('min-price').value = this.config.minPrice;
    }
    
    saveConfig() {
        try {
            localStorage.setItem('volflow-config', JSON.stringify(this.config));
            this.showToast('Configuration saved successfully', 'success');
        } catch (error) {
            this.showToast('Failed to save configuration', 'error');
        }
    }
    
    loadSavedConfig() {
        try {
            const saved = localStorage.getItem('volflow-config');
            if (saved) {
                this.config = { ...this.config, ...JSON.parse(saved) };
                this.updateConfigDisplay();
            }
        } catch (error) {
            console.warn('Failed to load saved configuration:', error);
        }
    }
    
    updateAccountInfo() {
        // Account info is now updated from live data only

    }
    
    showToast(message, type = 'info') {
        // Create toast notification
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <i class="fas fa-${this.getToastIcon(type)}"></i>
                <span>${message}</span>
            </div>
        `;
        
        // Add toast styles if not already present
        if (!document.querySelector('#toast-styles')) {
            const styles = document.createElement('style');
            styles.id = 'toast-styles';
            styles.textContent = `
                .toast {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
                    padding: 1rem 1.5rem;
                    z-index: 10000;
                    transform: translateX(400px);
                    transition: transform 0.3s ease;
                    border-left: 4px solid #667eea;
                }
                .toast-success { border-left-color: #38a169; }
                .toast-error { border-left-color: #e53e3e; }
                .toast-warning { border-left-color: #d69e2e; }
                .toast-content {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    font-weight: 500;
                    color: #2d3748;
                }
                .toast.show { transform: translateX(0); }
            `;
            document.head.appendChild(styles);
        }
        
        document.body.appendChild(toast);
        
        // Animate in
        setTimeout(() => toast.classList.add('show'), 100);
        
        // Remove after delay
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => document.body.removeChild(toast), 300);
        }, 4000);
    }
    
    getToastIcon(type) {
        const icons = {
            success: 'check-circle',
            error: 'exclamation-circle',
            warning: 'exclamation-triangle',
            info: 'info-circle'
        };
        return icons[type] || icons.info;
    }
    
    formatTime(milliseconds) {
        const seconds = Math.floor(milliseconds / 1000);
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
    }
    
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    async refreshPositions() {
        // Fetch real positions data from live monitor JSON
        const refreshBtn = document.getElementById('refresh-positions');
        const originalContent = refreshBtn.innerHTML;
        
        // Show loading state
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        
        try {
            // Fetch live monitor data
            const response = await fetch('./live_monitor.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const liveData = await response.json();
            
            // Update positions with live data
            this.updatePositionsDisplayFromLive(liveData);
            this.updateAccountInfoFromLive(liveData);
            this.showToast('Positions refreshed successfully', 'success');
            
        } catch (error) {
            console.error('Failed to refresh positions:', error);
            this.showToast('Failed to refresh positions', 'error');
            // No fallback - only show real data
        } finally {
            // Restore button state
            refreshBtn.disabled = false;
            refreshBtn.innerHTML = originalContent;
        }
    }

    async refreshStrategies() {
        // Fetch strategy signals from live monitor JSON
        const refreshBtn = document.getElementById('refresh-strategies');
        const originalContent = refreshBtn.innerHTML;
        
        // Show loading state
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        
        try {
            // Fetch live monitor data
            const response = await fetch('./live_monitor.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const liveData = await response.json();
            
            // Update strategy dashboard with live data
            this.updateStrategyDashboard(liveData);
            this.showToast('Strategy signals refreshed successfully', 'success');
            
        } catch (error) {
            console.error('Failed to refresh strategies:', error);
            this.showToast('Failed to refresh strategy signals', 'error');
        } finally {
            // Restore button state
            refreshBtn.disabled = false;
            refreshBtn.innerHTML = originalContent;
        }
    }

    updateStrategyDashboard(liveData) {
        // Update Iron Condor Strategy
        this.updateStrategyCard('iron-condor', liveData.iron_condor_signals || {});
        
        // Update PML Strategy
        this.updateStrategyCard('pml', liveData.pml_signals || {});
        
        // Update Divergence Strategy
        this.updateStrategyCard('divergence', liveData.divergence_signals || {});
    }

    updateStrategyCard(strategyType, signalsData) {
        const cardId = `${strategyType}-card`;
        const statusId = `${strategyType}-status`;
        const signalsId = `${strategyType}-signals`;
        
        const statusElement = document.getElementById(statusId);
        const signalsElement = document.getElementById(signalsId);
        
        if (!statusElement || !signalsElement) return;
        
        // Convert signals object to array with symbol included
        const signals = Object.entries(signalsData).map(([symbol, signal]) => ({
            ...signal,
            symbol: symbol
        }));
        
        if (signals.length === 0) {
            // No signals available
            this.updateStrategyStatus(statusElement, 'NO_SIGNAL', 'no-signal');
            this.showStrategyPlaceholder(signalsElement, 'No signals available');
            return;
        }
        
        // Find the most significant signal
        const activeSignals = signals.filter(s => s.signal_type !== 'NO_SIGNAL');
        
        if (activeSignals.length === 0) {
            // All signals are NO_SIGNAL
            this.updateStrategyStatus(statusElement, 'NO_SIGNAL', 'no-signal');
            this.showStrategyPlaceholder(signalsElement, 'No active signals');
        } else {
            // Show active signals
            const primarySignal = this.getPrimarySignal(activeSignals);
            this.updateStrategyStatus(statusElement, primarySignal.signal_type, this.getSignalClass(primarySignal.signal_type));
            this.showStrategySignals(signalsElement, activeSignals);
        }
    }

    updateStrategyStatus(statusElement, signalType, signalClass) {
        const indicator = statusElement.querySelector('.status-indicator');
        const text = statusElement.querySelector('.status-text');
        
        if (indicator) {
            indicator.className = `status-indicator ${signalClass}`;
        }
        
        if (text) {
            text.textContent = signalType;
        }
    }

    getSignalClass(signalType) {
        const signalClasses = {
            'STRONG_BUY': 'buy',
            'BUY': 'buy',
            'HOLD': 'hold',
            'SELL': 'sell',
            'STRONG_SELL': 'sell',
            'NO_SIGNAL': 'no-signal'
        };
        return signalClasses[signalType] || 'no-signal';
    }

    getPrimarySignal(signals) {
        // Priority order for signal types
        const priority = {
            'STRONG_BUY': 5,
            'STRONG_SELL': 5,
            'BUY': 4,
            'SELL': 4,
            'HOLD': 3,
            'NO_SIGNAL': 1
        };
        
        return signals.reduce((primary, signal) => {
            const currentPriority = priority[signal.signal_type] || 0;
            const primaryPriority = priority[primary.signal_type] || 0;
            
            if (currentPriority > primaryPriority) {
                return signal;
            } else if (currentPriority === primaryPriority && signal.confidence > primary.confidence) {
                return signal;
            }
            return primary;
        });
    }

    showStrategyPlaceholder(signalsElement, message) {
        signalsElement.innerHTML = `
            <div class="signal-placeholder">
                <i class="fas fa-clock"></i>
                <span>${message}</span>
            </div>
        `;
    }

    showStrategySignals(signalsElement, signals) {
        signalsElement.innerHTML = '';
        
        // Show up to 3 most significant signals
        const topSignals = signals
            .sort((a, b) => b.confidence - a.confidence)
            .slice(0, 3);
        
        topSignals.forEach(signal => {
            const signalElement = document.createElement('div');
            signalElement.className = 'signal-item';
            signalElement.innerHTML = this.createSignalHTML(signal);
            signalsElement.appendChild(signalElement);
        });
    }

    createSignalHTML(signal) {
        const signalClass = this.getSignalClass(signal.signal_type);
        const confidencePercent = Math.round(signal.confidence * 100);
        
        // Determine strategy type from signal context
        const strategyType = this.getStrategyTypeFromSignal(signal);
        
        // Check if manual approval is enabled for this strategy
        const manualApprovalEnabled = this.isManualApprovalEnabled(strategyType);
        
        // Create manual approval buttons if enabled
        const manualApprovalHTML = manualApprovalEnabled ? `
            <div class="signal-manual-approval">
                <div class="approval-label">Manual Approval Required:</div>
                <div class="approval-buttons">
                    <button class="btn-approve" onclick="approveSignal('${signal.symbol}', '${strategyType}')">
                        <i class="fas fa-check"></i>
                        Approve & Execute
                    </button>
                    <button class="btn-reject" onclick="rejectSignal('${signal.symbol}', '${strategyType}')">
                        <i class="fas fa-times"></i>
                        Reject
                    </button>
                </div>
            </div>
        ` : '';
        
        return `
            <div class="signal-header">
                <div class="signal-symbol">${signal.symbol}</div>
                <div class="signal-type ${signalClass}">${signal.signal_type}</div>
            </div>
            <div class="signal-details">
                <div class="signal-detail">
                    <div class="signal-detail-label">Entry Reason</div>
                    <div class="signal-detail-value">${this.truncateText(signal.entry_reason, 30)}</div>
                </div>
                <div class="signal-detail">
                    <div class="signal-detail-label">Position Size</div>
                    <div class="signal-detail-value">${signal.position_size || 'N/A'}</div>
                </div>
                <div class="signal-detail">
                    <div class="signal-detail-label">Stop Loss</div>
                    <div class="signal-detail-value">${signal.stop_loss && typeof signal.stop_loss === 'number' ? '$' + signal.stop_loss.toFixed(2) : 'N/A'}</div>
                </div>
                <div class="signal-detail">
                    <div class="signal-detail-label">Target</div>
                    <div class="signal-detail-value">${signal.profit_target && typeof signal.profit_target === 'number' ? '$' + signal.profit_target.toFixed(2) : 'N/A'}</div>
                </div>
            </div>
            <div class="signal-confidence">
                <div class="confidence-bar">
                    <div class="confidence-fill" style="width: ${confidencePercent}%"></div>
                </div>
                <div class="confidence-text">${confidencePercent}%</div>
            </div>
            ${manualApprovalHTML}
        `;
    }

    truncateText(text, maxLength) {
        if (!text || text.length <= maxLength) return text || 'N/A';
        return text.substring(0, maxLength) + '...';
    }

    getStrategyTypeFromSignal(signal) {
        // Determine strategy type based on signal context or card location
        // This could be enhanced to look at the parent element or signal metadata
        const signalElement = document.querySelector(`[data-symbol="${signal.symbol}"]`);
        if (signalElement) {
            const strategyCard = signalElement.closest('.strategy-card');
            if (strategyCard) {
                const cardId = strategyCard.id;
                if (cardId.includes('iron-condor')) return 'iron_condor';
                if (cardId.includes('pml')) return 'pml';
                if (cardId.includes('divergence')) return 'divergence';
            }
        }
        
        // Fallback: try to determine from current context
        // This is a simplified approach - in a real implementation, 
        // the signal data would include strategy type metadata
        return 'iron_condor'; // Default fallback
    }

    isManualApprovalEnabled(strategyType) {
        // Check if manual approval is enabled for the given strategy
        if (!window.strategyControlStates) {
            return false;
        }
        
        const manualApprovalKey = `${strategyType}_manual_approval`;
        return window.strategyControlStates[manualApprovalKey] || false;
    }
    
    // Removed mock data generation - only use real data from live monitor
    
    formatCurrency(amount) {
        const sign = amount >= 0 ? '+' : '';
        return `${sign}$${Math.abs(amount).toLocaleString('en-US', { 
            minimumFractionDigits: 2, 
            maximumFractionDigits: 2 
        })}`;
    }
    
    updatePositionsDisplayFromLive(liveData) {
        // Extract positions data from live monitor JSON
        const positionsData = liveData.positions || {};
        const positions = positionsData.positions || {};
        const summary = positionsData.summary || {};
        
        // Convert live positions to display format
        const livePositions = Object.values(positions).map(position => ({
            symbol: position.symbol,
            type: position.quantity > 0 ? 'Long' : 'Short', // Determine type from quantity
            quantity: Math.abs(position.quantity),
            plOpen: position.unrealized_pl || 0,
            plDay: 0, // Day P&L not available in current data structure
            marketValue: position.market_value || 0,
            costBasis: position.cost_basis || 0
        }));
        
        // Update summary values with live data
        const totalPLOpen = summary.total_unrealized_pl || 0;
        const totalPLDay = summary.total_day_pl || 0;
        const totalPositions = summary.total_positions || 0;
        
        // Update summary display
        const plOpenElement = document.querySelector('.summary-item:nth-child(1) .summary-value');
        const plDayElement = document.querySelector('.summary-item:nth-child(2) .summary-value');
        const totalPositionsElement = document.querySelector('.summary-item:nth-child(3) .summary-value');
        
        if (plOpenElement) {
            plOpenElement.textContent = this.formatCurrency(totalPLOpen);
            plOpenElement.className = `summary-value ${totalPLOpen >= 0 ? 'positive' : 'negative'}`;
        }
        
        if (plDayElement) {
            plDayElement.textContent = this.formatCurrency(totalPLDay);
            plDayElement.className = `summary-value ${totalPLDay >= 0 ? 'positive' : 'negative'}`;
        }
        
        if (totalPositionsElement) {
            totalPositionsElement.textContent = totalPositions.toString();
        }
        
        // Update summary icons
        const plOpenIcon = document.querySelector('.summary-item:nth-child(1) .summary-icon');
        const plDayIcon = document.querySelector('.summary-item:nth-child(2) .summary-icon');
        
        if (plOpenIcon) {
            plOpenIcon.className = `summary-icon ${totalPLOpen >= 0 ? 'positive' : 'negative'}`;
        }
        if (plDayIcon) {
            plDayIcon.className = `summary-icon ${totalPLDay >= 0 ? 'positive' : 'negative'}`;
        }
        
        // Update individual positions list
        this.updatePositionsListFromLive(livePositions);
    }
    
    updatePositionsListFromLive(positions) {
        const positionsList = document.querySelector('.positions-list');
        if (!positionsList) return;
        
        // Get existing position elements
        const existingPositions = Array.from(positionsList.querySelectorAll('.position-item'));
        const existingSymbols = existingPositions.map(el => el.querySelector('.symbol')?.textContent);
        
        // Create a map of current positions by symbol
        const positionsMap = {};
        positions.forEach(position => {
            positionsMap[position.symbol] = position;
        });
        
        // Update existing positions or remove if no longer present
        existingPositions.forEach(positionElement => {
            const symbolElement = positionElement.querySelector('.symbol');
            if (!symbolElement) return;
            
            const symbol = symbolElement.textContent;
            const currentPosition = positionsMap[symbol];
            
            if (currentPosition) {
                // Update existing position without recreating DOM
                this.updatePositionElement(positionElement, currentPosition);
                delete positionsMap[symbol]; // Mark as processed
            } else {
                // Position no longer exists, remove with fade out
                positionElement.style.opacity = '0';
                positionElement.style.transform = 'scale(0.95)';
                setTimeout(() => {
                    if (positionElement.parentElement) {
                        positionElement.remove();
                    }
                }, 300);
            }
        });
        
        // Add new positions that weren't in the existing list
        Object.values(positionsMap).forEach(position => {
            const positionElement = document.createElement('div');
            positionElement.className = 'position-item';
            positionElement.style.opacity = '0';
            positionElement.innerHTML = `
                <div class="position-symbol">
                    <span class="symbol">${position.symbol}</span>
                    <span class="position-type">${position.type}</span>
                </div>
                <div class="position-details">
                    <div class="detail-row">
                        <span class="detail-label">Quantity:</span>
                        <span class="detail-value" data-field="quantity">${position.quantity > 0 ? '+' : ''}${position.quantity}</span>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">Market Value:</span>
                        <span class="detail-value" data-field="marketValue">${this.formatCurrency(position.marketValue)}</span>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">P/L Open:</span>
                        <span class="detail-value ${position.plOpen >= 0 ? 'positive' : 'negative'}" data-field="plOpen">
                            ${this.formatCurrency(position.plOpen)}
                        </span>
                    </div>
                </div>
            `;
            positionsList.appendChild(positionElement);
            
            // Fade in new position
            setTimeout(() => {
                positionElement.style.opacity = '1';
                positionElement.style.transition = 'opacity 0.3s ease';
            }, 50);
        });
        
        // Show/hide empty state
        const emptyState = document.querySelector('.positions-empty');
        if (positions.length === 0) {
            positionsList.style.display = 'none';
            if (emptyState) emptyState.style.display = 'block';
        } else {
            positionsList.style.display = 'flex';
            if (emptyState) emptyState.style.display = 'none';
        }
    }
    
    updatePositionElement(positionElement, position) {
        // Update quantity
        const quantityElement = positionElement.querySelector('[data-field="quantity"]');
        if (quantityElement) {
            const newQuantity = `${position.quantity > 0 ? '+' : ''}${position.quantity}`;
            if (quantityElement.textContent !== newQuantity) {
                this.smoothUpdateText(quantityElement, newQuantity);
            }
        }
        
        // Update market value
        const marketValueElement = positionElement.querySelector('[data-field="marketValue"]');
        if (marketValueElement) {
            const newMarketValue = this.formatCurrency(position.marketValue);
            if (marketValueElement.textContent !== newMarketValue) {
                this.smoothUpdateText(marketValueElement, newMarketValue);
            }
        }
        
        // Update P/L Open
        const plOpenElement = positionElement.querySelector('[data-field="plOpen"]');
        if (plOpenElement) {
            const newPlOpen = this.formatCurrency(position.plOpen);
            const newClass = `detail-value ${position.plOpen >= 0 ? 'positive' : 'negative'}`;
            
            if (plOpenElement.textContent !== newPlOpen) {
                this.smoothUpdateText(plOpenElement, newPlOpen);
            }
            
            if (plOpenElement.className !== newClass) {
                plOpenElement.className = newClass;
            }
        }
    }
    
    smoothUpdateText(element, newText) {
        // Only update if text actually changed
        if (element.textContent === newText) return;
        
        // Add subtle highlight effect for changed values
        element.style.transition = 'background-color 0.3s ease';
        element.style.backgroundColor = 'rgba(102, 126, 234, 0.1)';
        element.textContent = newText;
        
        // Remove highlight after animation
        setTimeout(() => {
            element.style.backgroundColor = '';
        }, 300);
    }
    
    updateAccountInfoFromLive(liveData) {
        // Update account info with live data
        const summary = liveData.positions?.summary || {};
        const marketStatus = liveData.market_status || {};
        const accountData = liveData.account_data?.account_summary || {};
        
        // Update total equity from real account data
        const equity = accountData.balances?.equity || 0;
        const equityElement = document.querySelector('.equity-amount');
        if (equityElement && equity > 0) {
            equityElement.textContent = `$${equity.toLocaleString('en-US', { 
                minimumFractionDigits: 2, 
                maximumFractionDigits: 2 
            })}`;
        }
        
        // Update account number (masked)
        const accountNumber = accountData.account_number || '';
        const accountElement = document.querySelector('.account-number');
        if (accountElement && accountNumber) {
            // Mask account number: show ****XXXX format
            const maskedAccount = `****${accountNumber.slice(-4)}`;
            accountElement.textContent = maskedAccount;
        }
        
        // Update market status in the status bar
        const statusItems = document.querySelectorAll('.status-item-horizontal');
        if (statusItems.length > 0) {
            // Update system status based on market status
            const systemStatusIcon = statusItems[0]?.querySelector('.status-icon');
            const systemStatusValue = statusItems[0]?.querySelector('.status-value');
            
            if (systemStatusIcon && systemStatusValue) {
                const isMarketOpen = marketStatus.is_market_hours;
                systemStatusIcon.className = 'status-icon';
                
                if (isMarketOpen) {
                    systemStatusIcon.classList.add('ready');
                    systemStatusIcon.innerHTML = '<i class="fas fa-check-circle"></i>';
                    systemStatusValue.textContent = 'Market Open';
                } else {
                    systemStatusIcon.style.background = '#fed7d7';
                    systemStatusIcon.style.color = '#e53e3e';
                    systemStatusIcon.innerHTML = '<i class="fas fa-moon"></i>';
                    systemStatusValue.textContent = 'Market Closed';
                }
            }
            
            // Update symbols count
            const symbolsValue = statusItems[1]?.querySelector('.status-value');
            if (symbolsValue) {
                const symbolsCount = liveData.metadata?.symbols_monitored?.length || 0;
                symbolsValue.textContent = symbolsCount.toLocaleString();
            }
            
            // Update last update time
            const timeValue = statusItems[2]?.querySelector('.status-value');
            if (timeValue && liveData.metadata?.timestamp) {
                const updateTime = new Date(liveData.metadata.timestamp);
                timeValue.textContent = updateTime.toLocaleTimeString([], { 
                    hour: '2-digit', 
                    minute: '2-digit' 
                });
            }
        }
    }
    
    // Add auto-refresh functionality
    startAutoRefresh(intervalSeconds = 5) {
        // Clear any existing interval
        if (this.autoRefreshInterval) {
            clearInterval(this.autoRefreshInterval);
        }
        
        // Set up new interval
        this.autoRefreshInterval = setInterval(async () => {
            try {
                const response = await fetch('./live_monitor.json');
                if (response.ok) {
                    const liveData = await response.json();
                    this.updatePositionsDisplayFromLive(liveData);
                    this.updateAccountInfoFromLive(liveData);
                    this.updateStrategyDashboard(liveData);
                    this.updateTradingStatistics(liveData);
                }
            } catch (error) {
                console.warn('Auto-refresh failed:', error);
            }
        }, intervalSeconds * 1000);
        
        console.log(`Auto-refresh started with ${intervalSeconds}s interval`);
    }
    
    // Trading Statistics Functions
    updateTradingStatistics(liveData) {
        // Extract PnL statistics from live data
        const pnlStats = liveData.transactions?.pnl_stats || {};
        
        if (pnlStats.overall) {
            this.updateTradingMetrics(pnlStats);
            this.updateWinLossChart(pnlStats);
        }
    }
    
    updateTradingMetrics(pnlStats) {
        const overall = pnlStats.overall || {};
        
        // Update overall metrics with real PnL data
        this.updateMetricElement('win-rate', `${(overall.win_rate || 0).toFixed(1)}%`);
        this.updateMetricElement('total-trades', (overall.wins || 0) + (overall.losses || 0));
        this.updateMetricElement('avg-win', `$${(overall.avg_win || 0).toFixed(2)}`, 'positive');
        this.updateMetricElement('avg-loss', `$${Math.abs(overall.avg_loss || 0).toFixed(2)}`, 'negative');
        this.updateMetricElement('total-pl', `$${(overall.profit_loss || 0).toFixed(2)}`, (overall.profit_loss || 0) >= 0 ? 'positive' : 'negative');
        this.updateMetricElement('wl-ratio', (overall.win_loss_ratio || 0).toFixed(2));
    }
    
    updateMetricElement(elementId, value, className = null) {
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = value;
            
            // Remove existing color classes
            element.classList.remove('positive', 'negative');
            
            // Add new class if specified
            if (className) {
                element.classList.add(className);
            }
        }
    }
    
    updateWinLossChart(tradingStats) {
        const overall = tradingStats.overall || {};
        const wins = overall.wins || 0;
        const losses = overall.losses || 0;
        
        // Get canvas element
        const canvas = document.getElementById('winLossChart');
        if (!canvas) return;
        
        const ctx = canvas.getContext('2d');
        
        // Destroy existing chart if it exists
        if (winLossChart) {
            winLossChart.destroy();
        }
        
        // Create new chart only if we have data
        if (wins > 0 || losses > 0) {
            winLossChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Wins', 'Losses'],
                    datasets: [{
                        data: [wins, losses],
                        backgroundColor: [
                            '#38a169', // Green for wins
                            '#e53e3e'  // Red for losses
                        ],
                        borderWidth: 2,
                        borderColor: '#ffffff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 15,
                                font: {
                                    size: 12,
                                    family: 'Inter'
                                }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const label = context.label || '';
                                    const value = context.parsed;
                                    const total = wins + losses;
                                    const percentage = ((value / total) * 100).toFixed(1);
                                    return `${label}: ${value} (${percentage}%)`;
                                }
                            }
                        }
                    },
                    cutout: '60%',
                    animation: {
                        animateRotate: true,
                        duration: 1000
                    }
                }
            });
        } else {
            // Show empty state
            this.showEmptyChart(ctx);
        }
    }
    
    showEmptyChart(ctx) {
        // Clear canvas
        ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
        
        // Draw empty state
        const centerX = ctx.canvas.width / 2;
        const centerY = ctx.canvas.height / 2;
        
        // Draw circle outline
        ctx.beginPath();
        ctx.arc(centerX, centerY, 60, 0, 2 * Math.PI);
        ctx.strokeStyle = '#e2e8f0';
        ctx.lineWidth = 2;
        ctx.stroke();
        
        // Draw text
        ctx.fillStyle = '#718096';
        ctx.font = '14px Inter';
        ctx.textAlign = 'center';
        ctx.fillText('No Data', centerX, centerY);
    }
    
    stopAutoRefresh() {
        if (this.autoRefreshInterval) {
            clearInterval(this.autoRefreshInterval);
            this.autoRefreshInterval = null;
            console.log('Auto-refresh stopped');
        }
    }
    
    formatVolume(volume) {
        if (volume >= 1000000) {
            return `${(volume / 1000000).toFixed(1)}M`;
        } else if (volume >= 1000) {
            return `${(volume / 1000).toFixed(1)}K`;
        }
        return volume.toLocaleString();
    }
    
    handleEnvironmentToggle(isLive) {
        const environment = isLive ? 'Live' : 'Sandbox';
        this.showToast(`Switched to ${environment} environment`, 'info');
        
        // Update status text
        document.getElementById('env-status').textContent = environment;
        
        // Update any environment-specific UI elements
        const accountCard = document.querySelector('.account-card');
        const envToggle = document.getElementById('env-toggle');
        const toggleSlider = envToggle?.closest('.toggle-switch')?.querySelector('.toggle-slider');
        
        if (isLive) {
            accountCard.style.background = 'linear-gradient(135deg, #48cc6c, #38a169)';
            if (toggleSlider) {
                toggleSlider.style.background = 'linear-gradient(135deg, #48cc6c, #38a169)';
            }
        } else {
            accountCard.style.background = 'linear-gradient(135deg, #f6e05e, #d69e2e)';
            if (toggleSlider) {
                toggleSlider.style.background = 'linear-gradient(135deg, #f6e05e, #d69e2e)';
            }
        }
    }
    
    handleTradingToggle(isActive) {
        const status = isActive ? 'Active' : 'Stopped';
        this.showToast(`Trading ${status.toLowerCase()}`, isActive ? 'success' : 'warning');
        
        // Update status text
        document.getElementById('trade-status').textContent = status;
        
        // Update trading status in the interface
        const startBtn = document.getElementById('start-screening');
        if (isActive) {
            startBtn.classList.add('btn-success');
            startBtn.classList.remove('btn-primary');
        } else {
            startBtn.classList.add('btn-primary');
            startBtn.classList.remove('btn-success');
        }
    }
    
    updateStatusDisplay() {
        const now = new Date();
        
        // Update horizontal status items
        const statusItems = document.querySelectorAll('.status-item-horizontal');
        if (statusItems.length > 0) {
            // Update last update time
            if (statusItems[2]) {
                const timeValue = statusItems[2].querySelector('.status-value');
                if (timeValue) {
                    timeValue.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                }
            }
            
            // Update qualifying stocks count
            if (statusItems[3] && this.screeningResults.length > 0) {
                const qualifyingCount = this.screeningResults.filter(r => r.meets_criteria).length;
                const countValue = statusItems[3].querySelector('.status-value');
                if (countValue) {
                    countValue.textContent = qualifyingCount.toLocaleString();
                }
            }
        }
    }
    
    updateStatusIcon(status) {
        const statusItems = document.querySelectorAll('.status-item-horizontal');
        if (statusItems.length > 0) {
            const statusIcon = statusItems[0].querySelector('.status-icon');
            const statusValue = statusItems[0].querySelector('.status-value');
            
            if (statusIcon && statusValue) {
                statusIcon.className = 'status-icon';
                
                switch (status) {
                    case 'ready':
                        statusIcon.classList.add('ready');
                        statusIcon.innerHTML = '<i class="fas fa-check-circle"></i>';
                        statusValue.textContent = 'Ready';
                        break;
                    case 'screening':
                        statusIcon.classList.add('pulse');
                        statusIcon.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
                        statusValue.textContent = 'Screening...';
                        break;
                    case 'error':
                        statusIcon.style.background = '#fed7d7';
                        statusIcon.style.color = '#e53e3e';
                        statusIcon.innerHTML = '<i class="fas fa-exclamation-triangle"></i>';
                        statusValue.textContent = 'Error';
                        break;
                }
            }
        }
    }
    
    initializeToggleStates() {
        // Initialize environment toggle (checked = Live, unchecked = Sandbox)
        const envToggle = document.getElementById('env-toggle');
        const envStatus = document.getElementById('env-status');
        
        if (envToggle && envStatus) {
            // Set initial state based on checkbox
            envStatus.textContent = envToggle.checked ? 'Live' : 'Sandbox';
            
            // Set account card and toggle colors based on initial state
            const accountCard = document.querySelector('.account-card');
            const toggleSlider = envToggle.closest('.toggle-switch')?.querySelector('.toggle-slider');
            
            if (envToggle.checked) {
                accountCard.style.background = 'linear-gradient(135deg, #48cc6c, #38a169)';
                if (toggleSlider) {
                    toggleSlider.style.background = 'linear-gradient(135deg, #48cc6c, #38a169)';
                }
            } else {
                accountCard.style.background = 'linear-gradient(135deg, #f6e05e, #d69e2e)';
                if (toggleSlider) {
                    toggleSlider.style.background = 'linear-gradient(135deg, #f6e05e, #d69e2e)';
                }
            }
        }
        
        // Initialize trading toggle (checked = Active, unchecked = Stopped)
        const tradeToggle = document.getElementById('trade-toggle');
        const tradeStatus = document.getElementById('trade-status');
        
        if (tradeToggle && tradeStatus) {
            // Set initial state based on checkbox
            tradeStatus.textContent = tradeToggle.checked ? 'Active' : 'Stopped';
        }
        
        // Add debugging
        console.log('Toggle states initialized:', {
            envToggle: envToggle?.checked,
            tradeToggle: tradeToggle?.checked,
            envStatus: envStatus?.textContent,
            tradeStatus: tradeStatus?.textContent
        });
    }
}

// Watchlist functionality
let watchlistSymbols = [];
let watchlistData = {};

// Watchlist Functions
function initializeWatchlist() {
    // Add event listeners for watchlist functionality
    const addSymbolBtn = document.getElementById('add-symbol');
    const refreshWatchlistBtn = document.getElementById('refresh-watchlist');
    const confirmAddBtn = document.getElementById('confirm-add-symbol');
    const cancelAddBtn = document.getElementById('cancel-add-symbol');
    const symbolInput = document.getElementById('symbol-input');
    
    console.log('Initializing watchlist...', {
        addSymbolBtn: !!addSymbolBtn,
        refreshWatchlistBtn: !!refreshWatchlistBtn,
        confirmAddBtn: !!confirmAddBtn,
        cancelAddBtn: !!cancelAddBtn,
        symbolInput: !!symbolInput
    });
    
    if (addSymbolBtn) {
        addSymbolBtn.addEventListener('click', showAddSymbolForm);
        console.log('Add symbol button listener added');
    } else {
        console.warn('Add symbol button not found');
    }
    
    if (refreshWatchlistBtn) {
        refreshWatchlistBtn.addEventListener('click', refreshWatchlist);
        console.log('Refresh watchlist button listener added');
    } else {
        console.warn('Refresh watchlist button not found');
    }
    
    if (confirmAddBtn) {
        confirmAddBtn.addEventListener('click', confirmAddSymbol);
        console.log('Confirm add button listener added');
    } else {
        console.warn('Confirm add button not found');
    }
    
    if (cancelAddBtn) {
        cancelAddBtn.addEventListener('click', hideAddSymbolForm);
        console.log('Cancel add button listener added');
    } else {
        console.warn('Cancel add button not found');
    }
    
    if (symbolInput) {
        symbolInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                confirmAddSymbol();
            }
        });
        console.log('Symbol input listener added');
    } else {
        console.warn('Symbol input not found');
    }
    
    // Load existing watchlist
    loadWatchlist();
    console.log('Watchlist initialization complete');
}

function showAddSymbolForm() {
    const form = document.getElementById('add-symbol-form');
    const input = document.getElementById('symbol-input');
    
    if (form) {
        form.style.display = 'block';
        if (input) {
            input.focus();
            input.value = '';
        }
    }
}

function hideAddSymbolForm() {
    const form = document.getElementById('add-symbol-form');
    const input = document.getElementById('symbol-input');
    
    if (form) {
        form.style.display = 'none';
        if (input) {
            input.value = '';
        }
    }
}

async function confirmAddSymbol() {
    const input = document.getElementById('symbol-input');
    if (!input) return;
    
    const symbol = input.value.trim().toUpperCase();
    
    if (!symbol) {
        showToast('Please enter a valid symbol', 'warning');
        return;
    }
    
    if (watchlistSymbols.includes(symbol)) {
        showToast(`${symbol} is already in your watchlist`, 'warning');
        return;
    }
    
    // Show loading state
    const confirmBtn = document.getElementById('confirm-add-symbol');
    const originalContent = confirmBtn.innerHTML;
    confirmBtn.disabled = true;
    confirmBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Adding...';
    
    try {
        // Add symbol to watchlist
        watchlistSymbols.push(symbol);
        
        // Initialize with placeholder data
        watchlistData[symbol] = {
            symbol: symbol,
            current_price: 0.0,
            price_change: 0.0,
            price_change_percent: 0.0,
            volume: 0,
            last_updated: new Date().toISOString()
        };
        
        // Save to localStorage
        saveWatchlist();
        
        // Update display
        updateWatchlistDisplay();
        updateWatchlistStatus();
        
        // Hide form
        hideAddSymbolForm();
        
        // Integrate with live monitor system
        await integrateWithLiveMonitor(symbol);
        
        // Fetch initial price data
        await fetchSymbolPrice(symbol);
        
        // Show success message
        showToast(`Added ${symbol} to watchlist and live monitor`, 'success');
        
    } catch (error) {
        console.error('Error adding symbol:', error);
        showToast(`Failed to add ${symbol} to watchlist`, 'error');
        
        // Remove from watchlist if integration failed
        const index = watchlistSymbols.indexOf(symbol);
        if (index > -1) {
            watchlistSymbols.splice(index, 1);
            delete watchlistData[symbol];
            saveWatchlist();
            updateWatchlistDisplay();
            updateWatchlistStatus();
        }
    } finally {
        // Restore button state
        confirmBtn.disabled = false;
        confirmBtn.innerHTML = originalContent;
    }
}

async function removeSymbol(symbol) {
    const index = watchlistSymbols.indexOf(symbol);
    if (index > -1) {
        try {
            // Remove from API server first
            const apiResult = await removeSymbolFromLiveMonitor(symbol);
            
            if (apiResult.success) {
                console.log(`Successfully removed ${symbol} from API server`);
            } else {
                console.warn(`API removal failed for ${symbol}:`, apiResult.error);
            }
            
            // Remove from local arrays regardless of API result
            watchlistSymbols.splice(index, 1);
            delete watchlistData[symbol];
            
            saveWatchlist();
            updateWatchlistDisplay();
            updateWatchlistStatus();
            
            showToast(`Removed ${symbol} from watchlist`, 'info');
            
        } catch (error) {
            console.error(`Error removing ${symbol}:`, error);
            
            // Still remove locally even if API call fails
            watchlistSymbols.splice(index, 1);
            delete watchlistData[symbol];
            
            saveWatchlist();
            updateWatchlistDisplay();
            updateWatchlistStatus();
            
            showToast(`Removed ${symbol} from watchlist (API error)`, 'warning');
        }
    }
}

async function refreshWatchlist() {
    const refreshBtn = document.getElementById('refresh-watchlist');
    if (!refreshBtn) return;
    
    const originalContent = refreshBtn.innerHTML;
    
    // Show loading state
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
    
    try {
        // Fetch prices for all symbols
        const promises = watchlistSymbols.map(symbol => fetchSymbolPrice(symbol));
        await Promise.all(promises);
        
        // Update display
        updateWatchlistDisplay();
        updateWatchlistStatus();
        
        showToast('Watchlist refreshed successfully', 'success');
        
    } catch (error) {
        console.error('Failed to refresh watchlist:', error);
        showToast('Failed to refresh watchlist', 'error');
    } finally {
        // Restore button state
        refreshBtn.disabled = false;
        refreshBtn.innerHTML = originalContent;
    }
}

async function fetchSymbolPrice(symbol) {
    try {
        // Fetch real data from live monitor JSON
        const response = await fetch('./live_monitor.json');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const liveData = await response.json();
        
        // Check if symbol exists in integrated_watchlist
        if (liveData.integrated_watchlist && liveData.integrated_watchlist.watchlist_data && liveData.integrated_watchlist.watchlist_data[symbol]) {
            const symbolData = liveData.integrated_watchlist.watchlist_data[symbol];
            
            watchlistData[symbol] = {
                symbol: symbol,
                current_price: symbolData.current_price || 0,
                price_change: symbolData.price_change || 0,
                price_change_percent: symbolData.price_change_percent || 0,
                volume: symbolData.volume || 0,
                market_cap: symbolData.market_cap || null,
                last_updated: symbolData.last_updated || new Date().toISOString()
            };
            
            return watchlistData[symbol];
        }
        
        // If not in integrated_watchlist, check technical_indicators for price data
        if (liveData.technical_indicators && liveData.technical_indicators[symbol]) {
            const techData = liveData.technical_indicators[symbol];
            const quoteData = techData.quote_data || {};
            
            watchlistData[symbol] = {
                symbol: symbol,
                current_price: techData.current_price || 0,
                price_change: quoteData.change || 0,
                price_change_percent: quoteData.change_percent || 0,
                volume: quoteData.volume || 0,
                market_cap: null,
                last_updated: techData.timestamp || new Date().toISOString()
            };
            
            return watchlistData[symbol];
        }
        
        // If symbol not found in live data, return null
        console.warn(`Symbol ${symbol} not found in live data`);
        return null;
        
    } catch (error) {
        console.error(`Failed to fetch price for ${symbol}:`, error);
        return null;
    }
}

function updateWatchlistDisplay() {
    const watchlistList = document.getElementById('watchlist-list');
    const emptyState = document.getElementById('watchlist-empty');
    
    if (!watchlistList || !emptyState) return;
    
    if (watchlistSymbols.length === 0) {
        watchlistList.style.display = 'none';
        emptyState.style.display = 'block';
        return;
    }
    
    // Show watchlist and hide empty state
    watchlistList.style.display = 'flex';
    emptyState.style.display = 'none';
    
    // Clear existing items
    watchlistList.innerHTML = '';
    
    // Add each symbol
    watchlistSymbols.forEach(symbol => {
        const symbolData = watchlistData[symbol];
        if (!symbolData) return;
        
        const item = document.createElement('div');
        item.className = 'watchlist-item fade-in';
        item.innerHTML = createWatchlistItemHTML(symbolData);
        watchlistList.appendChild(item);
    });
}

function createWatchlistItemHTML(symbolData) {
    const priceChangeClass = symbolData.price_change >= 0 ? 'positive' : 'negative';
    const priceChangeIcon = symbolData.price_change >= 0 ? 'fa-arrow-up' : 'fa-arrow-down';
    const symbolInitial = symbolData.symbol.charAt(0);
    
    return `
        <div class="watchlist-symbol">
            <div class="symbol-icon">${symbolInitial}</div>
            <div class="symbol-info">
                <div class="symbol-name">${symbolData.symbol}</div>
                <div class="symbol-type">Stock</div>
            </div>
        </div>
        <div class="watchlist-price">
            <div class="current-price">$${symbolData.current_price.toFixed(2)}</div>
            <div class="price-change ${priceChangeClass}">
                <i class="fas ${priceChangeIcon}"></i>
                ${symbolData.price_change >= 0 ? '+' : ''}${symbolData.price_change.toFixed(2)} 
                (${symbolData.price_change_percent >= 0 ? '+' : ''}${symbolData.price_change_percent.toFixed(2)}%)
            </div>
        </div>
        <div class="watchlist-actions">
            <button class="remove-symbol" onclick="removeSymbol('${symbolData.symbol}')" title="Remove from watchlist">
                <i class="fas fa-times"></i>
            </button>
        </div>
    `;
}

function updateWatchlistStatus() {
    // Update symbols count
    const symbolsCount = document.getElementById('symbols-count');
    if (symbolsCount) {
        symbolsCount.textContent = watchlistSymbols.length.toString();
    }
    
    // Update last update time
    const lastUpdateTime = document.getElementById('last-update-time');
    if (lastUpdateTime) {
        const now = new Date();
        lastUpdateTime.textContent = now.toLocaleTimeString([], { 
            hour: '2-digit', 
            minute: '2-digit' 
        });
    }
    
    // Update market status (simplified)
    const marketStatus = document.getElementById('market-status');
    if (marketStatus) {
        const now = new Date();
        const hour = now.getHours();
        
        if (hour >= 9 && hour < 16) {
            marketStatus.textContent = 'Open';
        } else if (hour >= 16 && hour < 20) {
            marketStatus.textContent = 'After Hours';
        } else {
            marketStatus.textContent = 'Closed';
        }
    }
}

function saveWatchlist() {
    try {
        const watchlistState = {
            symbols: watchlistSymbols,
            data: watchlistData,
            lastUpdated: new Date().toISOString()
        };
        localStorage.setItem('volflow-watchlist', JSON.stringify(watchlistState));
    } catch (error) {
        console.error('Failed to save watchlist:', error);
    }
}

async function loadWatchlist() {
    try {
        console.log('Loading watchlist from API server...');
        
        // First try to load from API server
        const apiWatchlist = await loadWatchlistFromAPI();
        
        if (apiWatchlist && apiWatchlist.success) {
            console.log('Loaded watchlist from API:', apiWatchlist.symbols);
            
            // Use API data as the source of truth
            watchlistSymbols = apiWatchlist.symbols || [];
            
            // Initialize watchlist data for each symbol
            watchlistData = {};
            watchlistSymbols.forEach(symbol => {
                watchlistData[symbol] = {
                    symbol: symbol,
                    current_price: 0.0,
                    price_change: 0.0,
                    price_change_percent: 0.0,
                    volume: 0,
                    last_updated: new Date().toISOString()
                };
            });
            
            // Fetch current prices for all symbols
            await Promise.all(watchlistSymbols.map(symbol => fetchSymbolPrice(symbol)));
            
            // Update display
            updateWatchlistDisplay();
            updateWatchlistStatus();
            
            // Save to localStorage as backup
            saveWatchlist();
            
            console.log(`✅ Loaded ${watchlistSymbols.length} symbols from API server`);
            
        } else {
            // Fallback to localStorage if API is not available
            console.log('API not available, falling back to localStorage...');
            await loadWatchlistFromLocalStorage();
        }
        
    } catch (error) {
        console.error('Failed to load watchlist from API:', error);
        // Fallback to localStorage
        await loadWatchlistFromLocalStorage();
    }
}

async function loadWatchlistFromAPI() {
    try {
        const response = await fetch('http://localhost:8080/api/watchlist/symbols');
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        return result;
        
    } catch (error) {
        console.error('Error loading watchlist from API:', error);
        return null;
    }
}

async function loadWatchlistFromLocalStorage() {
    try {
        const saved = localStorage.getItem('volflow-watchlist');
        if (saved) {
            const watchlistState = JSON.parse(saved);
            watchlistSymbols = watchlistState.symbols || [];
            watchlistData = watchlistState.data || {};
            
            // Update display
            updateWatchlistDisplay();
            updateWatchlistStatus();
            
            console.log(`Loaded ${watchlistSymbols.length} symbols from localStorage`);
        } else {
            watchlistSymbols = [];
            watchlistData = {};
            updateWatchlistDisplay();
            updateWatchlistStatus();
        }
    } catch (error) {
        console.error('Failed to load watchlist from localStorage:', error);
        watchlistSymbols = [];
        watchlistData = {};
        updateWatchlistDisplay();
        updateWatchlistStatus();
    }
}

function showToast(message, type = 'info') {
    // Create toast notification
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <div class="toast-content">
            <i class="fas fa-${getToastIcon(type)}"></i>
            <span>${message}</span>
        </div>
    `;
    
    // Add toast styles if not already present
    if (!document.querySelector('#toast-styles')) {
        const styles = document.createElement('style');
        styles.id = 'toast-styles';
        styles.textContent = `
            .toast {
                position: fixed;
                top: 20px;
                right: 20px;
                background: white;
                border-radius: 8px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
                padding: 1rem 1.5rem;
                z-index: 10000;
                transform: translateX(400px);
                transition: transform 0.3s ease;
                border-left: 4px solid #667eea;
            }
            .toast-success { border-left-color: #38a169; }
            .toast-error { border-left-color: #e53e3e; }
            .toast-warning { border-left-color: #d69e2e; }
            .toast-content {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                font-weight: 500;
                color: #2d3748;
            }
            .toast.show { transform: translateX(0); }
        `;
        document.head.appendChild(styles);
    }
    
    document.body.appendChild(toast);
    
    // Animate in
    setTimeout(() => toast.classList.add('show'), 100);
    
    // Remove after delay
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => document.body.removeChild(toast), 300);
    }, 4000);
}

function getToastIcon(type) {
    const icons = {
        success: 'check-circle',
        error: 'exclamation-circle',
        warning: 'exclamation-triangle',
        info: 'info-circle'
    };
    return icons[type] || icons.info;
}

// Live Monitor Integration Functions
async function integrateWithLiveMonitor(symbol) {
    try {
        console.log(`Integrating ${symbol} with live monitor system...`);
        
        // Call the Python symbols monitor handler to add symbol
        const response = await addSymbolToLiveMonitor(symbol);
        
        if (response.success) {
            console.log(`Successfully integrated ${symbol} with live monitor`);
            return true;
        } else {
            throw new Error(response.error || 'Failed to integrate with live monitor');
        }
        
    } catch (error) {
        console.error(`Failed to integrate ${symbol} with live monitor:`, error);
        throw error;
    }
}

async function addSymbolToLiveMonitor(symbol) {
    try {
        // Make HTTP POST request to the Python API server
        const response = await fetch('http://localhost:8080/api/watchlist/add', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                symbol: symbol,
                timestamp: new Date().toISOString(),
                source: 'web_interface'
            })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            console.log(`Successfully added ${symbol} to live monitor via API`);
            return result;
        } else {
            throw new Error(result.error || 'Failed to add symbol to live monitor');
        }
        
    } catch (error) {
        console.error(`Error adding ${symbol} to live monitor:`, error);
        
        // Fallback: try to add directly to watchlist file if API is not available
        console.log('API not available, using fallback method...');
        return await addSymbolFallback(symbol);
    }
}

async function addSymbolFallback(symbol) {
    try {
        // Fallback method: add to localStorage and update local files
        const watchlistData = await loadWatchlistFromFile() || { symbols: [], watchlist_data: {} };
        
        if (!watchlistData.symbols.includes(symbol)) {
            watchlistData.symbols.push(symbol);
            watchlistData.watchlist_data[symbol] = {
                symbol: symbol,
                current_price: 0.0,
                price_change: 0.0,
                price_change_percent: 0.0,
                volume: 0,
                market_cap: null,
                last_updated: new Date().toISOString()
            };
            watchlistData.last_updated = new Date().toISOString();
        }
        
        return {
            success: true,
            message: `${symbol} added to watchlist (fallback mode)`,
            timestamp: new Date().toISOString()
        };
        
    } catch (error) {
        console.error(`Fallback method failed for ${symbol}:`, error);
        return {
            success: false,
            error: error.message,
            timestamp: new Date().toISOString()
        };
    }
}

async function removeSymbolFromLiveMonitor(symbol) {
    try {
        // Make HTTP POST request to the Python API server
        const response = await fetch('http://localhost:8080/api/watchlist/remove', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                symbol: symbol,
                timestamp: new Date().toISOString(),
                source: 'web_interface'
            })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            console.log(`Successfully removed ${symbol} from live monitor via API`);
            return result;
        } else {
            throw new Error(result.error || 'Failed to remove symbol from live monitor');
        }
        
    } catch (error) {
        console.error(`Error removing ${symbol} from live monitor:`, error);
        return {
            success: false,
            error: error.message,
            timestamp: new Date().toISOString()
        };
    }
}

async function loadWatchlistFromFile() {
    try {
        // Load the watchlist data from the live monitor JSON file
        const response = await fetch('./live_monitor.json');
        if (response.ok) {
            const liveData = await response.json();
            // Return the integrated_watchlist section in the expected format
            const integratedWatchlist = liveData.integrated_watchlist || {};
            return {
                symbols: integratedWatchlist.symbols || [],
                watchlist_data: integratedWatchlist.watchlist_data || {},
                last_updated: integratedWatchlist.metadata?.last_updated || new Date().toISOString()
            };
        }
        return null;
    } catch (error) {
        console.error('Failed to load watchlist from live monitor:', error);
        return null;
    }
}

async function getWatchlistStatus() {
    try {
        // Get watchlist status from API server
        const response = await fetch('http://localhost:8080/api/watchlist/status');
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        return result;
        
    } catch (error) {
        console.error('Error getting watchlist status:', error);
        // Fallback to loading from file
        return await loadWatchlistFromFile();
    }
}

async function updateLiveMonitorJSON(symbol) {
    try {
        console.log(`Updating live monitor JSON with ${symbol}...`);
        
      
        
        // Fetch current live monitor data
        const response = await fetch('./live_monitor.json');
        let liveData = {};
        
        if (response.ok) {
            liveData = await response.json();
        }
        
        // Add symbol to metadata if not already present
        if (!liveData.metadata) {
            liveData.metadata = {};
        }
        
        if (!liveData.metadata.symbols_monitored) {
            liveData.metadata.symbols_monitored = [];
        }
        
        if (!liveData.metadata.symbols_monitored.includes(symbol)) {
            liveData.metadata.symbols_monitored.push(symbol);
        }
        
        // Add symbol to watchlist data structure
        if (!liveData.watchlist_data) {
            liveData.watchlist_data = {};
        }
        
        liveData.watchlist_data[symbol] = {
            symbol: symbol,
            current_price: 0.0,
            price_change: 0.0,
            price_change_percent: 0.0,
            volume: 0,
            market_cap: null,
            last_updated: new Date().toISOString(),
            historical_data_status: 'fetching',
            integration_timestamp: new Date().toISOString()
        };
        
        // Update timestamp
        liveData.metadata.timestamp = new Date().toISOString();
        
        console.log(`Live monitor JSON updated with ${symbol}`);
        return true;
        
    } catch (error) {
        console.error(`Failed to update live monitor JSON for ${symbol}:`, error);
        throw error;
    }
}



// Risk Management Functions
function initializeRiskManagement() {
    console.log('Initializing risk management...');
    
    // Add event listeners for risk management controls
    const applyRiskBtn = document.getElementById('apply-risk-settings');
    const resetRiskBtn = document.getElementById('reset-risk-config');
    const saveRiskBtn = document.getElementById('save-risk-config');
    const exportRiskBtn = document.getElementById('export-risk-report');
    
    // Emergency controls
    const emergencyStopBtn = document.getElementById('emergency-stop');
    const closeAllPositionsBtn = document.getElementById('close-all-positions');
    const pauseTradingBtn = document.getElementById('pause-trading');
    
    // Stop loss method change handler
    const stopLossMethod = document.getElementById('stop-loss-method');
    
    if (applyRiskBtn) applyRiskBtn.addEventListener('click', applyRiskSettings);
    if (resetRiskBtn) resetRiskBtn.addEventListener('click', resetRiskConfiguration);
    if (saveRiskBtn) saveRiskBtn.addEventListener('click', saveRiskConfiguration);
    if (exportRiskBtn) exportRiskBtn.addEventListener('click', exportRiskReport);
    
    if (emergencyStopBtn) emergencyStopBtn.addEventListener('click', handleEmergencyStop);
    if (closeAllPositionsBtn) closeAllPositionsBtn.addEventListener('click', handleCloseAllPositions);
    if (pauseTradingBtn) pauseTradingBtn.addEventListener('click', handlePauseTrading);
    
    if (stopLossMethod) stopLossMethod.addEventListener('change', updateStopLossUnit);
    
    // Initialize parameter toggles
    initializeParameterToggles();
    
    // Load saved risk configuration
    loadRiskConfiguration();
    
    // Initialize risk monitoring
    startRiskMonitoring();
    
    console.log('Risk management initialization complete');
}

function initializeParameterToggles() {
    console.log('Initializing parameter toggles...');
    
    // Get all parameter toggle switches
    const parameterToggles = [
        'enable_max_account_risk',
        'enable_daily_loss_limit',
        'enable_equity_buffer',
        'enable_max_position_size',
        'enable_max_positions',
        'enable_stop_loss',
        'enable_stop_loss_value',
        'enable_risk_reward_ratio'
    ];
    
    parameterToggles.forEach(toggleId => {
        const toggle = document.getElementById(toggleId);
        if (toggle) {
            console.log(`Found toggle: ${toggleId}`);
            
            // Add comprehensive event listeners for better UX
            setupToggleEventListeners(toggle, toggleId, (toggleElement, skipSave) => {
                handleParameterToggle(toggleElement, skipSave);
            });
            
            // Set initial state without triggering save
            handleParameterToggle(toggle, true); // Pass true to skip save during initialization
        } else {
            console.warn(`Toggle not found: ${toggleId}`);
        }
    });
    
    console.log('Parameter toggles initialized');
}

function handleParameterToggle(toggle, skipSave = false) {
    console.log(`handleParameterToggle called for ${toggle.id}, checked: ${toggle.checked}`);
    
    const isEnabled = toggle.checked;
    const parameterCard = toggle.closest('.risk-parameter-card');
    
    if (!parameterCard) {
        console.error(`No parameter card found for toggle ${toggle.id}`);
        return;
    }
    
    console.log(`Found parameter card for ${toggle.id}`);
    
    // Get the associated input/select elements
    const inputs = parameterCard.querySelectorAll('.config-input, .config-select');
    console.log(`Found ${inputs.length} inputs for ${toggle.id}`);
    
    if (isEnabled) {
        // Enable the parameter
        parameterCard.classList.remove('disabled');
        inputs.forEach(input => {
            input.disabled = false;
            input.style.cursor = 'text';
            input.style.opacity = '1';
        });
        
        console.log(`Parameter ${toggle.id} enabled - card classes:`, parameterCard.className);
    } else {
        // Disable the parameter
        parameterCard.classList.add('disabled');
        inputs.forEach(input => {
            input.disabled = true;
            input.style.cursor = 'not-allowed';
            input.style.opacity = '0.6';
        });
        
        console.log(`Parameter ${toggle.id} disabled - card classes:`, parameterCard.className);
    }
    
    // Update risk configuration
    updateRiskParameterState(toggle.id, isEnabled, skipSave);
    
    // Auto-save the risk configuration when toggles change (but not during initialization)
    if (!skipSave) {
        saveRiskConfigurationViaWebSocket(getRiskConfiguration());
    }
}

function updateRiskParameterState(toggleId, isEnabled, skipToast = false) {
    // Store the parameter state for use in risk calculations
    if (!window.riskParameterStates) {
        window.riskParameterStates = {};
    }
    
    window.riskParameterStates[toggleId] = isEnabled;
    
    // Show toast notification only if not skipping
    if (!skipToast) {
        const parameterName = getParameterDisplayName(toggleId);
        const status = isEnabled ? 'enabled' : 'disabled';
        showToast(`${parameterName} ${status}`, isEnabled ? 'success' : 'warning');
    }
}

function getParameterDisplayName(toggleId) {
    const displayNames = {
        'enable_max_account_risk': 'Maximum Account Risk',
        'enable_daily_loss_limit': 'Daily Loss Limit',
        'enable_equity_buffer': 'Equity Buffer',
        'enable_max_position_size': 'Max Position Size',
        'enable_max_positions': 'Maximum Positions',
        'enable_stop_loss': 'Stop Loss Method',
        'enable_stop_loss_value': 'Stop Loss Value',
        'enable_risk_reward_ratio': 'Risk/Reward Ratio'
    };
    
    return displayNames[toggleId] || 'Parameter';
}

function getEnabledRiskParameters() {
    // Return only the enabled risk parameters for trading decisions
    const enabledParams = {};
    const config = getRiskConfiguration();
    
    if (!window.riskParameterStates) {
        // If no states are set, assume all are enabled (default behavior)
        return config;
    }
    
    // Only include parameters that are enabled
    if (window.riskParameterStates['enable_max_account_risk']) {
        enabledParams.maxAccountRisk = config.maxAccountRisk;
    }

    if (window.riskParameterStates['enable_daily_loss_limit']) {
        enabledParams.dailyLossLimit = config.dailyLossLimit;
    }

    if (window.riskParameterStates['enable_equity_buffer']) {
        enabledParams.equityBuffer = config.equityBuffer;
    }

    if (window.riskParameterStates['enable_max_position_size']) {
        enabledParams.maxPositionSize = config.maxPositionSize;
    }

    if (window.riskParameterStates['enable_max_positions']) {
        enabledParams.maxPositions = config.maxPositions;
    }

    if (window.riskParameterStates['enable_stop_loss']) {
        enabledParams.stopLossMethod = config.stopLossMethod;
    }

    if (window.riskParameterStates['enable_stop_loss_value']) {
        enabledParams.stopLossValue = config.stopLossValue;
    }

    if (window.riskParameterStates['enable_risk_reward_ratio']) {
        enabledParams.takeProfitRatio = config.takeProfitRatio;
    }
    
    return enabledParams;
}

function applyRiskSettings() {
    console.log('Applying risk settings...');
    
    const riskConfig = getRiskConfiguration();
    
    // Validate risk settings
    if (!validateRiskSettings(riskConfig)) {
        return;
    }
    
    // Apply settings to risk management system
    try {
        // Update risk parameters
        updateRiskParameters(riskConfig);
        
        // Update risk status display
        updateRiskStatusDisplay(riskConfig);
        
        // Save to WebSocket server and JSON file
        saveRiskConfigurationViaWebSocket(riskConfig);
        
        showToast('Risk settings applied successfully', 'success');
        
    } catch (error) {
        console.error('Error applying risk settings:', error);
        showToast('Failed to apply risk settings', 'error');
    }
}

function getRiskConfiguration() {
    return {
        // Account Risk Limits
        maxAccountRisk: parseFloat(document.getElementById('max_account_risk').value),
        dailyLossLimit: parseFloat(document.getElementById('daily_loss_limit').value),
        equityBuffer: parseFloat(document.getElementById('equity_buffer').value),
        
        // Position Sizing
        maxPositionSize: parseFloat(document.getElementById('max_position_size').value),
        maxPositions: parseInt(document.getElementById('max_positions').value),

        
        // Stop Loss & Take Profit
        stopLossMethod: document.getElementById('stop_loss_method').value,
        stopLossValue: parseFloat(document.getElementById('stop_loss_value').value),
        takeProfitRatio: parseFloat(document.getElementById('take_profit_ratio').value),
        
    };
}

function validateRiskSettings(config) {
    const errors = [];
    
    // Validate account risk limits
    if (config.maxAccountRisk < 5 || config.maxAccountRisk > 50) {
        errors.push('Maximum account risk must be between 5% and 50%');
    }
    
    if (config.dailyLossLimit < 1 || config.dailyLossLimit > 20) {
        errors.push('Daily loss limit must be between 1% and 20%');
    }
    
    if (config.equityBuffer < 1000) {
        errors.push('Equity buffer must be at least $1,000');
    }
    
    // Validate position sizing
    if (config.maxPositionSize < 1 || config.maxPositionSize > 20) {
        errors.push('Maximum position size must be between 1% and 20%');
    }
    
    if (config.maxPositions < 5 || config.maxPositions > 50) {
        errors.push('Maximum positions must be between 5 and 50');
    }
    
    // Validate stop loss settings
    if (config.stopLossValue < 0.5 || config.stopLossValue > 10) {
        errors.push('Stop loss value must be between 0.5 and 10');
    }
    
    if (config.takeProfitRatio < 1.0 || config.takeProfitRatio > 5.0) {
        errors.push('Risk/reward ratio must be between 1.0 and 5.0');
    }
    
    if (errors.length > 0) {
        showToast(errors.join('. '), 'error');
        return false;
    }
    
    return true;
}

function updateRiskParameters(config) {
    // Store risk configuration globally
    window.riskConfig = config;
    
    // Update risk utilization display (placeholder)
    console.log('Risk utilization update - placeholder');
    
    // Update position limits (placeholder)
    console.log('Position limits update - placeholder');
    
    // Update stop loss calculations (placeholder)
    console.log('Stop loss calculations update - placeholder');
    
    console.log('Risk parameters updated:', config);
}

function updateRiskStatusDisplay(config) {
    // Update risk status indicator
    const riskStatus = calculateRiskStatus(config);
    updateRiskStatusIndicator(riskStatus);
    
    // Update risk overview cards
    updateRiskOverviewCards(config);
}

function calculateRiskStatus(config) {
    // Get current account data
    const currentEquity = getCurrentEquity();
    const activePositions = getActivePositionsCount();
    const dailyPL = getDailyPL();
    
    // Calculate risk utilization
    const riskUtilization = (getPositionsValue() / currentEquity) * 100;
    
    // Determine risk status
    if (riskUtilization > config.maxAccountRisk * 0.9 || Math.abs(dailyPL) > config.dailyLossLimit * 0.9) {
        return 'danger';
    } else if (riskUtilization > config.maxAccountRisk * 0.7 || Math.abs(dailyPL) > config.dailyLossLimit * 0.7) {
        return 'warning';
    } else {
        return 'safe';
    }
}

function updateRiskStatusIndicator(status) {
    const statusIndicator = document.getElementById('risk-status');
    if (!statusIndicator) return;
    
    const statusDot = statusIndicator.querySelector('.status-dot');
    const statusText = statusIndicator.querySelector('.status-text');
    
    if (statusDot && statusText) {
        statusDot.className = `status-dot ${status}`;
        statusText.textContent = status.charAt(0).toUpperCase() + status.slice(1);
        
        // Update indicator background
        statusIndicator.style.background = getStatusBackground(status);
        statusIndicator.style.borderColor = getStatusBorderColor(status);
        statusText.style.color = getStatusTextColor(status);
    }
}

function getStatusBackground(status) {
    const backgrounds = {
        safe: 'rgba(56, 161, 105, 0.1)',
        warning: 'rgba(214, 158, 46, 0.1)',
        danger: 'rgba(229, 62, 62, 0.1)'
    };
    return backgrounds[status] || backgrounds.safe;
}

function getStatusBorderColor(status) {
    const colors = {
        safe: 'rgba(56, 161, 105, 0.2)',
        warning: 'rgba(214, 158, 46, 0.2)',
        danger: 'rgba(229, 62, 62, 0.2)'
    };
    return colors[status] || colors.safe;
}

function getStatusTextColor(status) {
    const colors = {
        safe: '#38a169',
        warning: '#d69e2e',
        danger: '#e53e3e'
    };
    return colors[status] || colors.safe;
}

function updateRiskOverviewCards(config) {
    // Update current equity
    const currentEquity = getCurrentEquity();
    const equityElement = document.getElementById('current-equity');
    if (equityElement) {
        equityElement.textContent = `$${currentEquity.toLocaleString('en-US', { 
            minimumFractionDigits: 2, 
            maximumFractionDigits: 2 
        })}`;
    }
    
    // Update risk utilization
    const riskUtilization = (getPositionsValue() / currentEquity) * 100;
    const utilizationElement = document.getElementById('risk-utilization');
    if (utilizationElement) {
        utilizationElement.textContent = `${riskUtilization.toFixed(1)}%`;
    }
    
    // Update active positions count
    const activePositions = getActivePositionsCount();
    const positionsElement = document.getElementById('active-positions');
    if (positionsElement) {
        positionsElement.textContent = activePositions.toString();
    }
    
    // Update daily drawdown
    const dailyPL = getDailyPL();
    const drawdownElement = document.getElementById('daily-drawdown');
    if (drawdownElement) {
        const drawdownPercent = (dailyPL / currentEquity) * 100;
        drawdownElement.textContent = `${drawdownPercent.toFixed(2)}%`;
        drawdownElement.className = `risk-value ${dailyPL >= 0 ? 'positive' : 'negative'}`;
    }
}

function updateStopLossUnit() {
    const method = document.getElementById('stop-loss-method').value;
    const unitElement = document.getElementById('stop-loss-unit');
    
    if (unitElement) {
        const units = {
            percentage: '%',
            atr: 'x ATR',
            technical: 'levels',
            volatility: 'x Vol'
        };
        unitElement.textContent = units[method] || '%';
    }
}

function resetRiskConfiguration() {
    console.log('Resetting risk configuration to defaults...');
    
    // Reset all risk management inputs to default values
    document.getElementById('max_account_risk').value = '25';
    document.getElementById('daily_loss_limit').value = '5';
    document.getElementById('equity_buffer').value = '10000';
    
    document.getElementById('max_position_size').value = '5';
    document.getElementById('max_positions').value = '15';
    document.getElementById('position_correlation').value = '3';
    
    document.getElementById('stop_loss_method').value = 'atr';
    document.getElementById('stop_loss_value').value = '2.0';
    document.getElementById('take_profit_ratio').value = '2.0';
    
    document.getElementById('max_hold_time').value = '30';
    document.getElementById('close_before_expiry').value = '7';
    document.getElementById('trading_hours').value = 'extended';
    
    document.getElementById('volatility_filter').value = 'medium';
    document.getElementById('market_regime').value = 'all';
    document.getElementById('vix_threshold').value = '30';
    
    // Update stop loss unit
    updateStopLossUnit();
    
    showToast('Risk configuration reset to defaults', 'success');
}

function saveRiskConfiguration() {
    console.log('Saving risk configuration...');
    
    try {
        const config = getRiskConfiguration();
        config.timestamp = new Date().toISOString();
        
        // Save to localStorage
        localStorage.setItem('volflow-risk-config', JSON.stringify(config));
        
        showToast('Risk configuration saved successfully', 'success');
        
    } catch (error) {
        console.error('Error saving risk configuration:', error);
        showToast('Failed to save risk configuration', 'error');
    }
}

function loadRiskConfiguration() {
    console.log('Loading saved risk configuration...');
    
    try {
        const saved = localStorage.getItem('volflow-risk-config');
        if (saved) {
            const config = JSON.parse(saved);
            
            // Apply saved configuration to form inputs
            if (config.maxAccountRisk) document.getElementById('max_account_risk').value = config.maxAccountRisk;
            if (config.dailyLossLimit) document.getElementById('daily_loss_limit').value = config.dailyLossLimit;
            if (config.equityBuffer) document.getElementById('equity_buffer').value = config.equityBuffer;
            
            if (config.maxPositionSize) document.getElementById('max_position_size').value = config.maxPositionSize;
            if (config.maxPositions) document.getElementById('max_positions').value = config.maxPositions;

            
            if (config.stopLossMethod) document.getElementById('stop_loss_method').value = config.stopLossMethod;
            if (config.stopLossValue) document.getElementById('stop_loss_value').value = config.stopLossValue;
            if (config.takeProfitRatio) document.getElementById('take_profit_ratio').value = config.takeProfitRatio;

            // Update stop loss unit
            updateStopLossUnit();
            
            console.log('Risk configuration loaded:', config);
        }
    } catch (error) {
        console.error('Error loading risk configuration:', error);
    }
}

function exportRiskReport() {
    console.log('Exporting risk report...');
    
    try {
        const config = getRiskConfiguration();
        const riskStatus = calculateRiskStatus(config);
        
        const report = {
            timestamp: new Date().toISOString(),
            risk_status: riskStatus,
            configuration: config,
            current_metrics: {
                account_equity: getCurrentEquity(),
                risk_utilization: (getPositionsValue() / getCurrentEquity()) * 100,
                active_positions: getActivePositionsCount(),
                daily_pl: getDailyPL(),
                positions_value: getPositionsValue()
            },
            limits: {
                max_account_risk_dollar: getCurrentEquity() * (config.maxAccountRisk / 100),
                max_position_size_dollar: getCurrentEquity() * (config.maxPositionSize / 100),
                daily_loss_limit_dollar: getCurrentEquity() * (config.dailyLossLimit / 100)
            }
        };
        
        // Create and download JSON file
        const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `volflow-risk-report-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        showToast('Risk report exported successfully', 'success');
        
    } catch (error) {
        console.error('Error exporting risk report:', error);
        showToast('Failed to export risk report', 'error');
    }
}

// Emergency Controls
function handleEmergencyStop() {
    if (confirm('Are you sure you want to execute an EMERGENCY STOP? This will halt all trading activities immediately.')) {
        console.log('Emergency stop activated');
        
        // Stop all trading activities
        stopAllTrading();
        
        // Update UI to reflect emergency state
        updateEmergencyState(true);
        
        showToast('EMERGENCY STOP ACTIVATED - All trading halted', 'error');
    }
}

function handleCloseAllPositions() {
    if (confirm('Are you sure you want to close ALL open positions? This action cannot be undone.')) {
        console.log('Closing all positions');
        
        // Close all positions
        closeAllPositions();
        
        showToast('Closing all open positions...', 'warning');
    }
}

function handlePauseTrading() {
    console.log('Pausing trading');
    
    // Pause trading activities
    pauseTrading();
    
    showToast('Trading paused - existing positions maintained', 'info');
}

// Risk Monitoring
function startRiskMonitoring() {
    // Monitor risk metrics every 30 seconds
    setInterval(() => {
        if (window.riskConfig) {
            updateRiskStatusDisplay(window.riskConfig);
            checkRiskLimits(window.riskConfig);
        }
    }, 30000);
    
    console.log('Risk monitoring started');
}

function checkRiskLimits(config) {
    const currentEquity = getCurrentEquity();
    const riskUtilization = (getPositionsValue() / currentEquity) * 100;
    const dailyPL = getDailyPL();
    const dailyPLPercent = (dailyPL / currentEquity) * 100;
    
    // Check account risk limit
    if (riskUtilization > config.maxAccountRisk) {
        showRiskAlert('Account risk limit exceeded', 'danger');
        triggerRiskAction('account_risk_exceeded');
    }
    
    // Check daily loss limit
    if (Math.abs(dailyPLPercent) > config.dailyLossLimit) {
        showRiskAlert('Daily loss limit exceeded', 'danger');
        triggerRiskAction('daily_loss_exceeded');
    }
    
    // Check position count limit
    const activePositions = getActivePositionsCount();
    if (activePositions > config.maxPositions) {
        showRiskAlert('Maximum positions limit exceeded', 'warning');
    }
}

function showRiskAlert(message, type) {
    // Create risk alert element
    const alertsContainer = document.querySelector('.risk-config-grid');
    if (!alertsContainer) return;
    
    const alert = document.createElement('div');
    alert.className = `risk-alert ${type}`;
    alert.innerHTML = `
        <i class="fas fa-exclamation-triangle"></i>
        <span>${message}</span>
        <button onclick="this.parentElement.remove()" style="margin-left: auto; background: none; border: none; cursor: pointer;">
            <i class="fas fa-times"></i>
        </button>
    `;
    
    alertsContainer.insertBefore(alert, alertsContainer.firstChild);
    
    // Auto-remove after 10 seconds
    setTimeout(() => {
        if (alert.parentElement) {
            alert.remove();
        }
    }, 10000);
}

function triggerRiskAction(actionType) {
    console.log(`Risk action triggered: ${actionType}`);
    
    switch (actionType) {
        case 'account_risk_exceeded':
            // Automatically reduce position sizes or close positions
            handleAccountRiskExceeded();
            break;
        case 'daily_loss_exceeded':
            // Stop new trades for the day
            handleDailyLossExceeded();
            break;
    }
}

// Helper functions for risk calculations
function getCurrentEquity() {
    // Get current equity from account data
    const equityElement = document.querySelector('.equity-amount');
    if (equityElement) {
        const equityText = equityElement.textContent.replace(/[$,]/g, '');
        return parseFloat(equityText) || 
    }
    return 0;
}

function getActivePositionsCount() {
    const positionsElement = document.getElementById('total-positions');
    if (positionsElement) {
        return parseInt(positionsElement.textContent) || 0;
    }
    return 0;
}

function getDailyPL() {
    const plDayElement = document.getElementById('pl-day');
    if (plDayElement) {
        const plText = plDayElement.textContent.replace(/[$,+]/g, '');
        return parseFloat(plText) || 0;
    }
    return 0;
}

function getPositionsValue() {
    const plOpenElement = document.getElementById('pl-open');
    if (plOpenElement) {
        const plText = plOpenElement.textContent.replace(/[$,+]/g, '');
        return Math.abs(parseFloat(plText)) || 0;
    }
    return 0;
}

// Placeholder functions for trading operations
function stopAllTrading() {
    console.log('Stopping all trading activities');
    // Implementation would connect to trading system
}

function closeAllPositions() {
    console.log('Closing all positions');
    // Implementation would connect to trading system
}

function pauseTrading() {
    console.log('Pausing trading');
    // Implementation would connect to trading system
}

function updateEmergencyState(isEmergency) {
    const body = document.body;
    if (isEmergency) {
        body.classList.add('emergency-mode');
    } else {
        body.classList.remove('emergency-mode');
    }
}

function handleAccountRiskExceeded() {
    console.log('Handling account risk exceeded');
    // Implementation would reduce position sizes or close positions
}

function handleDailyLossExceeded() {
    console.log('Handling daily loss exceeded');
    // Implementation would stop new trades
}

// Navigation functionality
function initializeNavigation() {
    console.log('Initializing navigation...');
    
    const navItems = document.querySelectorAll('.nav-item');
    
    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            
            const targetSection = item.getAttribute('data-section');
            const targetElement = document.getElementById(targetSection);
            
            if (targetElement) {
                // Update active nav item
                navItems.forEach(nav => nav.classList.remove('active'));
                item.classList.add('active');
                
                // Calculate offset to stop slightly above the section
                const elementTop = targetElement.offsetTop;
                const offset = 220; // Fixed header (120px) + nav bar (60px) + buffer (40px)
                const scrollPosition = elementTop - offset;
                
                // Smooth scroll to position with offset
                window.scrollTo({
                    top: scrollPosition,
                    behavior: 'smooth'
                });
                
                // Add visual feedback
                targetElement.style.transform = 'scale(1.01)';
                setTimeout(() => {
                    targetElement.style.transform = 'scale(1)';
                }, 200);
            }
        });
    });
    
    console.log('Navigation initialization complete');
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM Content Loaded - Initializing application...');
    
    window.volflowScreener = new VolFlowScreener();
    initializeWatchlist();
    
    // Add a small delay to ensure all elements are rendered
    setTimeout(() => {
        console.log('Initializing risk management with delay...');
        initializeRiskManagement();
        initializeSettings();
        initializeStrategyControls();
    }, 100);
    
    initializeNavigation();
});

// Handle window resize for responsive behavior
window.addEventListener('resize', () => {
    // Add any resize-specific logic here if needed
});

// Settings functionality
function initializeSettings() {
    console.log('Initializing settings...');
    
    // Initialize settings toggles
    initializeSettingsToggles();
    
    // Load saved settings
    loadSettings();
    
    // Add event listeners for settings actions
    const saveAllBtn = document.getElementById('save-all-settings');
    const logoutBtn = document.getElementById('logout-btn');
    const logoutCurrentBtn = document.getElementById('logout-current-session');
    const testNotificationsBtn = document.getElementById('test-notifications');
    
    // Security panel event listeners
    const saveSecurityBtn = document.getElementById('save-security-settings');
    const logoutSecurityBtn = document.getElementById('logout-security-btn');
    
    // Authentication event listeners
    const changePasswordBtn = document.getElementById('change-password-btn');
    const savePasswordBtn = document.getElementById('save-password-btn');
    const cancelPasswordBtn = document.getElementById('cancel-password-btn');
    const enable2faToggle = document.getElementById('enable-2fa');
    const setup2faBtn = document.getElementById('setup-2fa-btn');
    const viewLoginHistoryBtn = document.getElementById('view-login-history');
    
    if (saveAllBtn) saveAllBtn.addEventListener('click', saveAllSettings);
    if (logoutBtn) logoutBtn.addEventListener('click', handleLogout);
    if (logoutCurrentBtn) logoutCurrentBtn.addEventListener('click', handleLogout);
    if (testNotificationsBtn) testNotificationsBtn.addEventListener('click', testNotifications);
    
    // Security panel event listeners
    if (saveSecurityBtn) saveSecurityBtn.addEventListener('click', saveAllSettings);
    if (logoutSecurityBtn) logoutSecurityBtn.addEventListener('click', handleLogout);
    
    // Authentication event listeners
    if (changePasswordBtn) changePasswordBtn.addEventListener('click', showPasswordChangeForm);
    if (savePasswordBtn) savePasswordBtn.addEventListener('click', handlePasswordChange);
    if (cancelPasswordBtn) cancelPasswordBtn.addEventListener('click', hidePasswordChangeForm);
    if (enable2faToggle) enable2faToggle.addEventListener('change', handle2FAToggle);
    if (setup2faBtn) setup2faBtn.addEventListener('click', setup2FA);
    if (viewLoginHistoryBtn) viewLoginHistoryBtn.addEventListener('click', viewLoginHistory);
    
    console.log('Settings initialization complete');
}

function initializeSettingsToggles() {
    // Get all settings toggle switches
    const settingsToggles = [
        'enable-max-loss-alert',
        'enable-volatility-alert',
        'enable-position-size-alert',
        'enable-signal-alert',
        'enable-email-notifications',
        'enable-telegram-notifications',
        'enable-slack-notifications',
        'enable-quiet-hours',
        'enable-auto-logout',
        'enable-2fa'
    ];
    
    settingsToggles.forEach(toggleId => {
        const toggle = document.getElementById(toggleId);
        if (toggle) {
            console.log(`Found settings toggle: ${toggleId}`);
            
            // Add change listener to checkbox
            toggle.addEventListener('change', (e) => {
                console.log(`Settings toggle changed: ${toggleId}, checked: ${e.target.checked}`);
                handleSettingsToggle(e.target);
            });
            
            // Add click listener to the toggle switch container
            const toggleSwitch = toggle.closest('.toggle-switch');
            if (toggleSwitch) {
                toggleSwitch.addEventListener('click', (e) => {
                    // Prevent double-triggering if clicking directly on the input
                    if (e.target === toggle) return;
                    
                    console.log(`Settings toggle switch clicked: ${toggleId}`);
                    // Toggle the checkbox state
                    toggle.checked = !toggle.checked;
                    // Trigger the change event
                    toggle.dispatchEvent(new Event('change'));
                });
                
                // Add click listener to the slider specifically
                const toggleSlider = toggleSwitch.querySelector('.toggle-slider');
                if (toggleSlider) {
                    toggleSlider.addEventListener('click', (e) => {
                        e.stopPropagation();
                        console.log(`Settings toggle slider clicked: ${toggleId}`);
                        toggle.checked = !toggle.checked;
                        toggle.dispatchEvent(new Event('change'));
                    });
                }
            }
            
            // Set initial state
            handleSettingsToggle(toggle);
        } else {
            console.warn(`Settings toggle not found: ${toggleId}`);
        }
    });
    
    // Add event listeners for dependent settings
    const alertFrequency = document.getElementById('alert-frequency');
    const autoLogoutTimeout = document.getElementById('auto-logout-timeout');
    
    if (alertFrequency) alertFrequency.addEventListener('change', saveSettings);
    if (autoLogoutTimeout) autoLogoutTimeout.addEventListener('change', saveSettings);
}

function handleSettingsToggle(toggle) {
    const isEnabled = toggle.checked;
    const toggleId = toggle.id;
    
    // Handle dependent settings visibility
    switch (toggleId) {
        case 'enable-max-loss-alert':
            toggleDependentSetting('max-loss-settings', isEnabled);
            break;
        case 'enable-volatility-alert':
            toggleDependentSetting('volatility-settings', isEnabled);
            break;
        case 'enable-email-notifications':
            toggleDependentSetting('email-settings', isEnabled);
            break;
        case 'enable-telegram-notifications':
            toggleDependentSetting('telegram-settings', isEnabled);
            toggleDependentSetting('telegram-chat-settings', isEnabled);
            break;
        case 'enable-slack-notifications':
            toggleDependentSetting('slack-settings', isEnabled);
            toggleDependentSetting('slack-channel-settings', isEnabled);
            break;
        case 'enable-quiet-hours':
            toggleDependentSetting('quiet-hours-settings', isEnabled);
            break;
        case 'enable-auto-logout':
            toggleDependentSetting('auto-logout-settings', isEnabled);
            break;
    }
    
    // Save settings when toggled
    saveSettings();
    
    // Show notification
    const settingName = getSettingDisplayName(toggleId);
    const status = isEnabled ? 'enabled' : 'disabled';
    showToast(`${settingName} ${status}`, isEnabled ? 'success' : 'info');
}

function toggleDependentSetting(settingId, show) {
    const settingElement = document.getElementById(settingId);
    if (settingElement) {
        settingElement.style.display = show ? 'flex' : 'none';
    }
}

function getSettingDisplayName(toggleId) {
    const displayNames = {
        'enable-max-loss-alert': 'Maximum Loss Alert',
        'enable-volatility-alert': 'Volatility Spike Alert',
        'enable-position-size-alert': 'Position Size Alert',
        'enable-signal-alert': 'Strategy Signal Alert',
        'enable-email-notifications': 'Email Notifications',
        'enable-telegram-notifications': 'Telegram Notifications',
        'enable-slack-notifications': 'Slack Notifications',
        'enable-quiet-hours': 'Quiet Hours',
        'enable-auto-logout': 'Auto Logout'
    };
    
    return displayNames[toggleId] || 'Setting';
}

function saveAllSettings() {
    console.log('Saving all settings...');
    
    try {
        const settings = gatherAllSettings();
        
        // Save to localStorage
        localStorage.setItem('volflow-settings', JSON.stringify(settings));
        
        // Apply settings to the application
        applySettings(settings);
        
        showToast('All settings saved successfully', 'success');
        
    } catch (error) {
        console.error('Error saving settings:', error);
        showToast('Failed to save settings', 'error');
    }
}

function saveSettings() {
    // Auto-save settings when changed
    try {
        const settings = gatherAllSettings();
        localStorage.setItem('volflow-settings', JSON.stringify(settings));
    } catch (error) {
        console.error('Error auto-saving settings:', error);
    }
}

function gatherAllSettings() {
    const settings = {
        alerts: {
            maxLossAlert: document.getElementById('enable-max-loss-alert')?.checked || false,
            maxLossThreshold: parseFloat(document.getElementById('max-loss-threshold')?.value) || 1000,
            volatilityAlert: document.getElementById('enable-volatility-alert')?.checked || false,
            vixThreshold: parseFloat(document.getElementById('vix-alert-threshold')?.value) || 30,
            positionSizeAlert: document.getElementById('enable-position-size-alert')?.checked || false,
            signalAlert: document.getElementById('enable-signal-alert')?.checked || false
        },
        notifications: {
            email: {
                enabled: document.getElementById('enable-email-notifications')?.checked || false,
                address: document.getElementById('notification-email')?.value || ''
            },
            telegram: {
                enabled: document.getElementById('enable-telegram-notifications')?.checked || false,
                botToken: document.getElementById('telegram-bot-token')?.value || '',
                chatId: document.getElementById('telegram-chat-id')?.value || ''
            },
            slack: {
                enabled: document.getElementById('enable-slack-notifications')?.checked || false,
                webhookUrl: document.getElementById('slack-webhook-url')?.value || '',
                channel: document.getElementById('slack-channel')?.value || ''
            }
        },
        preferences: {
            alertFrequency: document.getElementById('alert-frequency')?.value || '5min',
            quietHours: {
                enabled: document.getElementById('enable-quiet-hours')?.checked || false,
                start: document.getElementById('quiet-hours-start')?.value || '22:00',
                end: document.getElementById('quiet-hours-end')?.value || '08:00'
            }
        },
        security: {
            autoLogout: {
                enabled: document.getElementById('enable-auto-logout')?.checked || false,
                timeout: parseInt(document.getElementById('auto-logout-timeout')?.value) || 30
            }
        },
        timestamp: new Date().toISOString()
    };
    
    return settings;
}

function loadSettings() {
    console.log('Loading saved settings...');
    
    try {
        const saved = localStorage.getItem('volflow-settings');
        if (saved) {
            const settings = JSON.parse(saved);
            applySettingsToForm(settings);
            applySettings(settings);
            console.log('Settings loaded:', settings);
        }
    } catch (error) {
        console.error('Error loading settings:', error);
    }
}

function applySettingsToForm(settings) {
    // Apply alert settings
    if (settings.alerts) {
        const alerts = settings.alerts;
        if (document.getElementById('enable-max-loss-alert')) document.getElementById('enable-max-loss-alert').checked = alerts.maxLossAlert;
        if (document.getElementById('max-loss-threshold')) document.getElementById('max-loss-threshold').value = alerts.maxLossThreshold;
        if (document.getElementById('enable-volatility-alert')) document.getElementById('enable-volatility-alert').checked = alerts.volatilityAlert;
        if (document.getElementById('vix-alert-threshold')) document.getElementById('vix-alert-threshold').value = alerts.vixThreshold;
        if (document.getElementById('enable-position-size-alert')) document.getElementById('enable-position-size-alert').checked = alerts.positionSizeAlert;
        if (document.getElementById('enable-signal-alert')) document.getElementById('enable-signal-alert').checked = alerts.signalAlert;
    }
    
    // Apply notification settings
    if (settings.notifications) {
        const notifications = settings.notifications;
        
        // Email
        if (notifications.email) {
            if (document.getElementById('enable-email-notifications')) document.getElementById('enable-email-notifications').checked = notifications.email.enabled;
            if (document.getElementById('notification-email')) document.getElementById('notification-email').value = notifications.email.address;
        }
        
        // Telegram
        if (notifications.telegram) {
            if (document.getElementById('enable-telegram-notifications')) document.getElementById('enable-telegram-notifications').checked = notifications.telegram.enabled;
            if (document.getElementById('telegram-bot-token')) document.getElementById('telegram-bot-token').value = notifications.telegram.botToken;
            if (document.getElementById('telegram-chat-id')) document.getElementById('telegram-chat-id').value = notifications.telegram.chatId;
        }
        
        // Slack
        if (notifications.slack) {
            if (document.getElementById('enable-slack-notifications')) document.getElementById('enable-slack-notifications').checked = notifications.slack.enabled;
            if (document.getElementById('slack-webhook-url')) document.getElementById('slack-webhook-url').value = notifications.slack.webhookUrl;
            if (document.getElementById('slack-channel')) document.getElementById('slack-channel').value = notifications.slack.channel;
        }
    }
    
    // Apply preference settings
    if (settings.preferences) {
        const preferences = settings.preferences;
        if (document.getElementById('alert-frequency')) document.getElementById('alert-frequency').value = preferences.alertFrequency;
        
        if (preferences.quietHours) {
            if (document.getElementById('enable-quiet-hours')) document.getElementById('enable-quiet-hours').checked = preferences.quietHours.enabled;
            if (document.getElementById('quiet-hours-start')) document.getElementById('quiet-hours-start').value = preferences.quietHours.start;
            if (document.getElementById('quiet-hours-end')) document.getElementById('quiet-hours-end').value = preferences.quietHours.end;
        }
    }
    
    // Apply security settings
    if (settings.security) {
        const security = settings.security;
        if (security.autoLogout) {
            if (document.getElementById('enable-auto-logout')) document.getElementById('enable-auto-logout').checked = security.autoLogout.enabled;
            if (document.getElementById('auto-logout-timeout')) document.getElementById('auto-logout-timeout').value = security.autoLogout.timeout;
        }
    }
    
    // Trigger toggle handlers to show/hide dependent settings
    const toggles = document.querySelectorAll('.setting-control .toggle-switch input[type="checkbox"]');
    toggles.forEach(toggle => {
        if (toggle.id) {
            handleSettingsToggle(toggle);
        }
    });
}

function applySettings(settings) {
    // Apply settings to the application runtime
    window.appSettings = settings;
    
    // Set up auto-logout if enabled
    if (settings.security?.autoLogout?.enabled) {
        setupAutoLogout(settings.security.autoLogout.timeout);
    }
    
    // Initialize notification systems
    if (settings.notifications?.email?.enabled) {
        console.log('Email notifications enabled');
    }
    
    if (settings.notifications?.telegram?.enabled) {
        console.log('Telegram notifications enabled');
    }
    
    if (settings.notifications?.slack?.enabled) {
        console.log('Slack notifications enabled');
    }
}

function setupAutoLogout(timeoutMinutes) {
    // Clear existing timeout
    if (window.autoLogoutTimeout) {
        clearTimeout(window.autoLogoutTimeout);
    }
    
    // Set up new timeout
    const timeoutMs = timeoutMinutes * 60 * 1000;
    
    const resetTimeout = () => {
        if (window.autoLogoutTimeout) {
            clearTimeout(window.autoLogoutTimeout);
        }
        
        window.autoLogoutTimeout = setTimeout(() => {
            showToast('Session expired due to inactivity', 'warning');
            setTimeout(() => {
                handleLogout();
            }, 3000);
        }, timeoutMs);
    };
    
    // Reset timeout on user activity
    const events = ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart'];
    events.forEach(event => {
        document.addEventListener(event, resetTimeout, true);
    });
    
    // Initial timeout
    resetTimeout();
    
    console.log(`Auto-logout set for ${timeoutMinutes} minutes of inactivity`);
}

function testNotifications() {
    console.log('Testing notifications...');
    
    const testBtn = document.getElementById('test-notifications');
    const originalContent = testBtn.innerHTML;
    
    // Show loading state
    testBtn.disabled = true;
    testBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing...';
    
    try {
        const settings = gatherAllSettings();
        
        // Test enabled notification channels
        const promises = [];
        
        if (settings.notifications.email.enabled && settings.notifications.email.address) {
            promises.push(testEmailNotification(settings.notifications.email));
        }
        
        if (settings.notifications.telegram.enabled && settings.notifications.telegram.botToken) {
            promises.push(testTelegramNotification(settings.notifications.telegram));
        }
        
        if (settings.notifications.slack.enabled && settings.notifications.slack.webhookUrl) {
            promises.push(testSlackNotification(settings.notifications.slack));
        }
        
        if (promises.length === 0) {
            showToast('No notification channels configured', 'warning');
            return;
        }
        
        // Execute tests
        Promise.allSettled(promises).then(results => {
            const successful = results.filter(r => r.status === 'fulfilled').length;
            const failed = results.filter(r => r.status === 'rejected').length;
            
            if (successful > 0 && failed === 0) {
                showToast(`All ${successful} notification channels tested successfully`, 'success');
            } else if (successful > 0) {
                showToast(`${successful} channels successful, ${failed} failed`, 'warning');
            } else {
                showToast('All notification tests failed', 'error');
            }
        });
        
    } catch (error) {
        console.error('Error testing notifications:', error);
        showToast('Failed to test notifications', 'error');
    } finally {
        // Restore button state
        setTimeout(() => {
            testBtn.disabled = false;
            testBtn.innerHTML = originalContent;
        }, 2000);
    }
}

async function testEmailNotification(emailConfig) {
    // Test email notification via our notification server
    console.log('Testing email notification to:', emailConfig.address);
    
    try {
        const response = await fetch('http://localhost:5002/notify', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                title: '🧪 VolFlow Test Notification',
                message: 'This is a test email from your VolFlow Options Breakout system.\n\nIf you received this message, your email notifications are working correctly!',
                priority: 'normal',
                category: 'system',
                data: {
                    test: true,
                    timestamp: new Date().toISOString(),
                    source: 'frontend_test'
                }
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            console.log('Email test result:', result);
            return { success: true, channel: 'email', details: result };
        } else {
            throw new Error(`Notification server error: ${response.status}`);
        }
    } catch (error) {
        console.error('Email test failed:', error);
        throw error;
    }
}

async function testTelegramNotification(telegramConfig) {
    // Test Telegram notification via our notification server
    console.log('Testing Telegram notification');
    
    try {
        const response = await fetch('http://localhost:5002/notify', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                title: '🧪 VolFlow Test Notification',
                message: 'This is a test message from your VolFlow Options Breakout system.\n\nIf you received this message, your Telegram notifications are working correctly!',
                priority: 'normal',
                category: 'system',
                data: {
                    test: true,
                    timestamp: new Date().toISOString(),
                    source: 'frontend_test'
                }
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            console.log('Telegram test result:', result);
            return { success: true, channel: 'telegram', details: result };
        } else {
            throw new Error(`Notification server error: ${response.status}`);
        }
    } catch (error) {
        console.error('Telegram test failed:', error);
        throw error;
    }
}

async function testSlackNotification(slackConfig) {
    // Test Slack notification via our notification server
    console.log('Testing Slack notification');
    
    try {
        const response = await fetch('http://localhost:5002/notify', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                title: '🧪 VolFlow Test Notification',
                message: 'This is a test message from your VolFlow Options Breakout system.\n\nIf you received this message, your Slack notifications are working correctly!',
                priority: 'normal',
                category: 'system',
                data: {
                    test: true,
                    timestamp: new Date().toISOString(),
                    source: 'frontend_test'
                }
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            console.log('Slack test result:', result);
            return { success: true, channel: 'slack', details: result };
        } else {
            throw new Error(`Notification server error: ${response.status}`);
        }
    } catch (error) {
        console.error('Slack test failed:', error);
        throw error;
    }
}

async function handleLogout() {
    // Show custom logout confirmation modal
    showLogoutConfirmationModal();
}

function showLogoutConfirmationModal() {
    // Create modal overlay
    const modal = document.createElement('div');
    modal.className = 'logout-modal-overlay';
    modal.innerHTML = `
        <div class="logout-modal">
            <div class="logout-modal-header">
                <div class="logout-icon">
                    <i class="fas fa-sign-out-alt"></i>
                </div>
                <h3 class="logout-title">Confirm Logout</h3>
            </div>
            <div class="logout-modal-body">
                <p class="logout-message">Are you sure you want to logout?</p>
                <p class="logout-warning">Any unsaved changes will be lost.</p>
                <div class="logout-info">
                    <div class="logout-info-item">
                        <i class="fas fa-user-circle"></i>
                        <span>imart913@gmail.com</span>
                    </div>
                    <div class="logout-info-item">
                        <i class="fas fa-clock"></i>
                        <span>Session will be terminated</span>
                    </div>
                </div>
            </div>
            <div class="logout-modal-footer">
                <button class="logout-btn-cancel" onclick="closeLogoutModal()">
                    <i class="fas fa-times"></i>
                    Cancel
                </button>
                <button class="logout-btn-confirm" onclick="confirmLogout()">
                    <i class="fas fa-sign-out-alt"></i>
                    Logout
                </button>
            </div>
        </div>
    `;

    // Add modal styles
    if (!document.querySelector('#logout-modal-styles')) {
        const styles = document.createElement('style');
        styles.id = 'logout-modal-styles';
        styles.textContent = `
            .logout-modal-overlay {
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0, 0, 0, 0.6);
                backdrop-filter: blur(4px);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 10000;
                animation: fadeIn 0.3s ease;
            }

            .logout-modal {
                background: white;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                max-width: 420px;
                width: 90%;
                overflow: hidden;
                animation: slideIn 0.3s ease;
            }

            .logout-modal-header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 2rem;
                text-align: center;
            }

            .logout-icon {
                width: 60px;
                height: 60px;
                background: rgba(255, 255, 255, 0.2);
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto 1rem;
                font-size: 1.5rem;
            }

            .logout-title {
                margin: 0;
                font-size: 1.5rem;
                font-weight: 600;
            }

            .logout-modal-body {
                padding: 2rem;
                text-align: center;
            }

            .logout-message {
                font-size: 1.1rem;
                font-weight: 500;
                color: #2d3748;
                margin: 0 0 0.5rem 0;
            }

            .logout-warning {
                color: #e53e3e;
                font-size: 0.9rem;
                margin: 0 0 1.5rem 0;
                font-weight: 500;
            }

            .logout-info {
                background: #f7fafc;
                border-radius: 8px;
                padding: 1rem;
                margin-bottom: 1rem;
            }

            .logout-info-item {
                display: flex;
                align-items: center;
                gap: 0.75rem;
                margin-bottom: 0.5rem;
                font-size: 0.9rem;
                color: #4a5568;
            }

            .logout-info-item:last-child {
                margin-bottom: 0;
            }

            .logout-info-item i {
                width: 16px;
                color: #667eea;
            }

            .logout-modal-footer {
                padding: 1.5rem 2rem;
                display: flex;
                gap: 1rem;
                justify-content: flex-end;
                background: #f7fafc;
                border-top: 1px solid #e2e8f0;
            }

            .logout-btn-cancel,
            .logout-btn-confirm {
                padding: 0.75rem 1.5rem;
                border: none;
                border-radius: 8px;
                font-weight: 600;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 0.5rem;
                transition: all 0.2s ease;
                font-size: 0.9rem;
            }

            .logout-btn-cancel {
                background: #e2e8f0;
                color: #4a5568;
            }

            .logout-btn-cancel:hover {
                background: #cbd5e0;
                transform: translateY(-1px);
            }

            .logout-btn-confirm {
                background: linear-gradient(135deg, #e53e3e, #c53030);
                color: white;
                box-shadow: 0 4px 12px rgba(229, 62, 62, 0.3);
            }

            .logout-btn-confirm:hover {
                background: linear-gradient(135deg, #c53030, #9c2626);
                transform: translateY(-1px);
                box-shadow: 0 6px 16px rgba(229, 62, 62, 0.4);
            }

            .logout-btn-confirm:active {
                transform: translateY(0);
            }

            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }

            @keyframes slideIn {
                from { 
                    opacity: 0;
                    transform: translateY(-20px) scale(0.95);
                }
                to { 
                    opacity: 1;
                    transform: translateY(0) scale(1);
                }
            }

            .logout-modal-overlay.closing {
                animation: fadeOut 0.2s ease;
            }

            .logout-modal-overlay.closing .logout-modal {
                animation: slideOut 0.2s ease;
            }

            @keyframes fadeOut {
                from { opacity: 1; }
                to { opacity: 0; }
            }

            @keyframes slideOut {
                from { 
                    opacity: 1;
                    transform: translateY(0) scale(1);
                }
                to { 
                    opacity: 0;
                    transform: translateY(-20px) scale(0.95);
                }
            }
        `;
        document.head.appendChild(styles);
    }

    document.body.appendChild(modal);

    // Add click outside to close
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            closeLogoutModal();
        }
    });

    // Add escape key to close
    const handleEscape = (e) => {
        if (e.key === 'Escape') {
            closeLogoutModal();
            document.removeEventListener('keydown', handleEscape);
        }
    };
    document.addEventListener('keydown', handleEscape);
}

function closeLogoutModal() {
    const modal = document.querySelector('.logout-modal-overlay');
    if (modal) {
        modal.classList.add('closing');
        setTimeout(() => {
            if (modal.parentElement) {
                document.body.removeChild(modal);
            }
        }, 200);
    }
}

async function confirmLogout() {
    console.log('User logging out...');

    // Close modal immediately
    closeLogoutModal();

    // Clear auto-logout timeout
    if (window.autoLogoutTimeout) {
        clearTimeout(window.autoLogoutTimeout);
    }

    // Show logout message
    showToast('Logging out...', 'info');

    try {
        // Get session token
        const sessionToken = localStorage.getItem('volflow_session');
        
        if (sessionToken) {
            // Call logout endpoint to invalidate session on server
            await fetch('/auth/logout', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    session_token: sessionToken
                })
            });
        }
    } catch (error) {
        console.error('Error during server logout:', error);
        // Continue with client-side logout even if server call fails
    }

    // Clear all stored data
    try {
        localStorage.removeItem('volflow_session');
        localStorage.removeItem('volflow_user');
        localStorage.removeItem('volflow-config');
        localStorage.removeItem('volflow-watchlist');
        localStorage.removeItem('volflow-risk-config');
        localStorage.removeItem('volflow-settings');
    } catch (error) {
        console.error('Error clearing localStorage:', error);
    }

    // Redirect to login page
    setTimeout(() => {
        showToast('Logged out successfully', 'success');
        window.location.href = '/';
    }, 1000);
}

// Universal Toggle Setup Function
function setupToggleEventListeners(toggle, toggleId, handlerFunction) {
    console.log(`Setting up comprehensive event listeners for toggle: ${toggleId}`);
    
    // Add event listener to the checkbox input
    toggle.addEventListener('change', (e) => {
        console.log(`Toggle changed: ${toggleId}, checked: ${e.target.checked}`);
        handlerFunction(e.target, false); // Pass false to indicate this is not initialization
    });
    
    // Find the parent toggle switch container
    const toggleSwitch = toggle.closest('.toggle-switch');
    if (toggleSwitch) {
        // Add click event listener to the entire toggle switch area
        toggleSwitch.addEventListener('click', (e) => {
            // Prevent double-triggering if clicking directly on the input
            if (e.target === toggle) return;
            
            console.log(`Toggle switch clicked: ${toggleId}`);
            // Toggle the checkbox state
            toggle.checked = !toggle.checked;
            // Trigger the change event
            toggle.dispatchEvent(new Event('change'));
        });
        
        // Add click event listener to the toggle slider specifically
        const toggleSlider = toggleSwitch.querySelector('.toggle-slider');
        if (toggleSlider) {
            toggleSlider.addEventListener('click', (e) => {
                // Prevent event bubbling to avoid double-triggering
                e.stopPropagation();
                
                console.log(`Toggle slider clicked: ${toggleId}`);
                // Toggle the checkbox state
                toggle.checked = !toggle.checked;
                // Trigger the change event
                toggle.dispatchEvent(new Event('change'));
            });
        }
        
        console.log(`Added comprehensive click listeners for toggle switch: ${toggleId}`);
    } else {
        console.warn(`No toggle switch container found for: ${toggleId}`);
    }
    
    // Add keyboard support for accessibility
    toggle.addEventListener('keydown', (e) => {
        if (e.key === ' ' || e.key === 'Enter') {
            e.preventDefault();
            toggle.checked = !toggle.checked;
            toggle.dispatchEvent(new Event('change'));
        }
    });
}

// Authentication Functions
function showPasswordChangeForm() {
    console.log('Showing password change form...');
    
    // Show password change form elements
    const formElements = [
        'password-change-form',
        'new-password-form', 
        'confirm-password-form',
        'password-actions'
    ];
    
    formElements.forEach(elementId => {
        const element = document.getElementById(elementId);
        if (element) {
            element.style.display = 'flex';
        }
    });
    
    // Hide the change password button
    const changePasswordBtn = document.getElementById('change-password-btn');
    if (changePasswordBtn) {
        changePasswordBtn.style.display = 'none';
    }
    
    // Focus on current password field
    const currentPasswordField = document.getElementById('current-password');
    if (currentPasswordField) {
        currentPasswordField.focus();
    }
    
    showToast('Enter your current password to continue', 'info');
}

function hidePasswordChangeForm() {
    console.log('Hiding password change form...');
    
    // Hide password change form elements
    const formElements = [
        'password-change-form',
        'new-password-form',
        'confirm-password-form', 
        'password-actions'
    ];
    
    formElements.forEach(elementId => {
        const element = document.getElementById(elementId);
        if (element) {
            element.style.display = 'none';
        }
    });
    
    // Show the change password button
    const changePasswordBtn = document.getElementById('change-password-btn');
    if (changePasswordBtn) {
        changePasswordBtn.style.display = 'flex';
    }
    
    // Clear password fields
    const passwordFields = ['current-password', 'new-password', 'confirm-password'];
    passwordFields.forEach(fieldId => {
        const field = document.getElementById(fieldId);
        if (field) {
            field.value = '';
            field.classList.remove('error', 'success');
        }
    });
}

async function handlePasswordChange() {
    console.log('Handling password change...');
    
    const currentPassword = document.getElementById('current-password')?.value;
    const newPassword = document.getElementById('new-password')?.value;
    const confirmPassword = document.getElementById('confirm-password')?.value;
    
    // Validate inputs
    if (!currentPassword) {
        showToast('Please enter your current password', 'error');
        return;
    }
    
    if (!newPassword) {
        showToast('Please enter a new password', 'error');
        return;
    }
    
    if (newPassword.length < 8) {
        showToast('New password must be at least 8 characters long', 'error');
        return;
    }
    
    if (newPassword !== confirmPassword) {
        showToast('New passwords do not match', 'error');
        return;
    }
    
    // Show loading state
    const saveBtn = document.getElementById('save-password-btn');
    const originalContent = saveBtn.innerHTML;
    saveBtn.disabled = true;
    saveBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Changing...';
    
    try {
        // Simulate password change API call
        await simulatePasswordChange(currentPassword, newPassword);
        
        // Success
        showToast('Password changed successfully', 'success');
        hidePasswordChangeForm();
        
        // Log the user out after password change for security
        setTimeout(() => {
            showToast('Please log in again with your new password', 'info');
            setTimeout(() => {
                handleLogout();
            }, 2000);
        }, 1000);
        
    } catch (error) {
        console.error('Password change failed:', error);
        showToast(error.message || 'Failed to change password', 'error');
    } finally {
        // Restore button state
        saveBtn.disabled = false;
        saveBtn.innerHTML = originalContent;
    }
}

async function simulatePasswordChange(currentPassword, newPassword) {
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    // Simulate validation (in real app, this would be server-side)
    if (currentPassword === 'wrongpassword') {
        throw new Error('Current password is incorrect');
    }
    
    // Simulate successful password change
    console.log('Password changed successfully (simulated)');
    return { success: true };
}

function handle2FAToggle(event) {
    const isEnabled = event.target.checked;
    const setup2faElement = document.getElementById('2fa-setup');
    
    if (setup2faElement) {
        setup2faElement.style.display = isEnabled ? 'flex' : 'none';
    }
    
    if (isEnabled) {
        showToast('Two-Factor Authentication enabled - please complete setup', 'success');
    } else {
        showToast('Two-Factor Authentication disabled', 'warning');
    }
    
    // Save 2FA preference
    saveSettings();
}

async function setup2FA() {
    console.log('Setting up 2FA...');
    
    const setupBtn = document.getElementById('setup-2fa-btn');
    const originalContent = setupBtn.innerHTML;
    
    // Show loading state
    setupBtn.disabled = true;
    setupBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';
    
    try {
        // Simulate 2FA setup
        await simulate2FASetup();
        
        // Show QR code modal (simulated)
        show2FASetupModal();
        
    } catch (error) {
        console.error('2FA setup failed:', error);
        showToast('Failed to setup 2FA', 'error');
    } finally {
        // Restore button state
        setupBtn.disabled = false;
        setupBtn.innerHTML = originalContent;
    }
}

async function simulate2FASetup() {
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Generate mock secret key
    const secretKey = 'JBSWY3DPEHPK3PXP';
    const qrCodeUrl = `https://chart.googleapis.com/chart?chs=200x200&chld=M|0&cht=qr&chl=otpauth://totp/VolFlow%3Auser%40example.com%3Fsecret%3D${secretKey}%26issuer%3DVolFlow`;
    
    return { secretKey, qrCodeUrl };
}

function show2FASetupModal() {
    // Create modal for 2FA setup
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal">
            <div class="modal-header">
                <h3 class="modal-title">Setup Two-Factor Authentication</h3>
            </div>
            <div class="modal-body" style="text-align: center; padding: 2rem;">
                <p style="margin-bottom: 1rem;">Scan this QR code with your authenticator app:</p>
                <div style="background: white; padding: 1rem; border-radius: 8px; display: inline-block; margin-bottom: 1rem;">
                    <div style="width: 200px; height: 200px; background: #f0f0f0; display: flex; align-items: center; justify-content: center; border-radius: 4px;">
                        <i class="fas fa-qrcode" style="font-size: 3rem; color: #666;"></i>
                    </div>
                </div>
                <p style="font-size: 0.9rem; color: #666; margin-bottom: 1rem;">
                    Or enter this secret key manually:<br>
                    <code style="background: #f0f0f0; padding: 0.25rem 0.5rem; border-radius: 4px;">JBSWY3DPEHPK3PXP</code>
                </p>
                <p style="font-size: 0.8rem; color: #888;">
                    Recommended apps: Google Authenticator, Authy, Microsoft Authenticator
                </p>
            </div>
            <div class="modal-footer">
                <button class="btn btn-primary" onclick="complete2FASetup()">
                    <i class="fas fa-check"></i>
                    Complete Setup
                </button>
                <button class="btn btn-secondary" onclick="cancel2FASetup()">
                    <i class="fas fa-times"></i>
                    Cancel
                </button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

function complete2FASetup() {
    // Remove modal
    const modal = document.querySelector('.modal-overlay');
    if (modal) {
        document.body.removeChild(modal);
    }
    
    showToast('Two-Factor Authentication setup completed successfully', 'success');
    
    // Update button text
    const setupBtn = document.getElementById('setup-2fa-btn');
    if (setupBtn) {
        setupBtn.innerHTML = '<i class="fas fa-check-circle"></i> 2FA Enabled';
        setupBtn.disabled = true;
        setupBtn.classList.add('btn-success');
        setupBtn.classList.remove('btn-outline');
    }
}

function cancel2FASetup() {
    // Remove modal
    const modal = document.querySelector('.modal-overlay');
    if (modal) {
        document.body.removeChild(modal);
    }
    
    // Disable 2FA toggle
    const enable2faToggle = document.getElementById('enable-2fa');
    if (enable2faToggle) {
        enable2faToggle.checked = false;
        handle2FAToggle({ target: enable2faToggle });
    }
    
    showToast('2FA setup cancelled', 'info');
}

async function viewLoginHistory() {
    console.log('Viewing login history...');
    
    const viewBtn = document.getElementById('view-login-history');
    const originalContent = viewBtn.innerHTML;
    
    // Show loading state
    viewBtn.disabled = true;
    viewBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Loading...';
    
    try {
        // Simulate loading login history
        const loginHistory = await getLoginHistory();
        
        // Show login history modal
        showLoginHistoryModal(loginHistory);
        
    } catch (error) {
        console.error('Failed to load login history:', error);
        showToast('Failed to load login history', 'error');
    } finally {
        // Restore button state
        viewBtn.disabled = false;
        viewBtn.innerHTML = originalContent;
    }
}

async function getLoginHistory() {
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 800));
    
    // Generate mock login history
    const now = new Date();
    const history = [];
    
    for (let i = 0; i < 10; i++) {
        const date = new Date(now.getTime() - (i * 24 * 60 * 60 * 1000) - (Math.random() * 12 * 60 * 60 * 1000));
        history.push({
            timestamp: date.toISOString(),
            ip: `192.168.1.${Math.floor(Math.random() * 255)}`,
            location: ['San Francisco, CA', 'New York, NY', 'Los Angeles, CA', 'Chicago, IL'][Math.floor(Math.random() * 4)],
            device: ['Chrome on macOS', 'Safari on iPhone', 'Chrome on Windows', 'Firefox on Linux'][Math.floor(Math.random() * 4)],
            success: Math.random() > 0.1 // 90% success rate
        });
    }
    
    return history;
}

function showLoginHistoryModal(history) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';
    
    const historyRows = history.map(entry => {
        const date = new Date(entry.timestamp);
        const statusClass = entry.success ? 'success' : 'error';
        const statusIcon = entry.success ? 'check-circle' : 'times-circle';
        const statusText = entry.success ? 'Success' : 'Failed';
        
        return `
            <tr>
                <td>${date.toLocaleDateString()} ${date.toLocaleTimeString()}</td>
                <td>${entry.ip}</td>
                <td>${entry.location}</td>
                <td>${entry.device}</td>
                <td>
                    <span class="login-status ${statusClass}">
                        <i class="fas fa-${statusIcon}"></i>
                        ${statusText}
                    </span>
                </td>
            </tr>
        `;
    }).join('');
    
    modal.innerHTML = `
        <div class="modal" style="max-width: 800px;">
            <div class="modal-header">
                <h3 class="modal-title">Login History</h3>
            </div>
            <div class="modal-body">
                <div style="max-height: 400px; overflow-y: auto;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background: #f7fafc; border-bottom: 2px solid #e2e8f0;">
                                <th style="padding: 0.75rem; text-align: left; font-weight: 600;">Date & Time</th>
                                <th style="padding: 0.75rem; text-align: left; font-weight: 600;">IP Address</th>
                                <th style="padding: 0.75rem; text-align: left; font-weight: 600;">Location</th>
                                <th style="padding: 0.75rem; text-align: left; font-weight: 600;">Device</th>
                                <th style="padding: 0.75rem; text-align: left; font-weight: 600;">Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${historyRows}
                        </tbody>
                    </table>
                </div>
                <style>
                    .login-status {
                        display: inline-flex;
                        align-items: center;
                        gap: 0.25rem;
                        padding: 0.25rem 0.5rem;
                        border-radius: 4px;
                        font-size: 0.8rem;
                        font-weight: 600;
                    }
                    .login-status.success {
                        background: #c6f6d5;
                        color: #38a169;
                    }
                    .login-status.error {
                        background: #fed7d7;
                        color: #e53e3e;
                    }
                    tbody tr {
                        border-bottom: 1px solid #e2e8f0;
                    }
                    tbody tr:hover {
                        background: #f7fafc;
                    }
                    tbody td {
                        padding: 0.75rem;
                        font-size: 0.9rem;
                    }
                </style>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeLoginHistoryModal()">
                    <i class="fas fa-times"></i>
                    Close
                </button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

function closeLoginHistoryModal() {
    const modal = document.querySelector('.modal-overlay');
    if (modal) {
        document.body.removeChild(modal);
    }
}

// WebSocket Risk Configuration Functions
function saveRiskConfigurationViaWebSocket(riskConfig) {
    console.log('💾 Saving risk configuration via WebSocket...');
    
    if (!window.volflowScreener?.wsManager?.isConnected) {
        console.warn('⚠️ WebSocket not connected, cannot save risk configuration');
        showToast('WebSocket not connected - risk config not saved to server', 'warning');
        return;
    }
    
    try {
        // Prepare configuration data for WebSocket
        const configData = {
            account_limits: {
                max_account_risk: riskConfig.maxAccountRisk,
                daily_loss_limit: riskConfig.dailyLossLimit,
                equity_buffer: riskConfig.equityBuffer
            },
            position_sizing: {
                max_position_size: riskConfig.maxPositionSize,
                max_positions: riskConfig.maxPositions
            },
            stop_loss_settings: {
                method: riskConfig.stopLossMethod,
                value: riskConfig.stopLossValue,
                take_profit_ratio: riskConfig.takeProfitRatio
            },
            parameter_states: window.riskParameterStates || {
                enable_max_account_risk: true,
                enable_daily_loss_limit: true,
                enable_equity_buffer: true,
                enable_max_position_size: true,
                enable_max_positions: true,
                enable_stop_loss: true,
                enable_stop_loss_value: true,
                enable_risk_reward_ratio: true
            },
            updated_by: 'web_interface'
        };
        
        // Send via WebSocket
        const message = {
            type: 'save_risk_config',
            config: configData,
            timestamp: new Date().toISOString()
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Risk configuration sent via WebSocket');
        
    } catch (error) {
        console.error('❌ Error sending risk configuration via WebSocket:', error);
        showToast('Failed to send risk config via WebSocket', 'error');
    }
}

function loadRiskConfigurationViaWebSocket() {
    console.log('📥 Loading risk configuration via WebSocket...');
    
    if (!window.volflowScreener?.wsManager?.isConnected) {
        console.warn('⚠️ WebSocket not connected, cannot load risk configuration');
        return;
    }
    
    try {
        const message = {
            type: 'get_risk_config',
            timestamp: new Date().toISOString()
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Risk configuration request sent via WebSocket');
        
    } catch (error) {
        console.error('❌ Error requesting risk configuration via WebSocket:', error);
        showToast('Failed to request risk config via WebSocket', 'error');
    }
}

// Add WebSocket message handler for risk configuration responses
function handleRiskConfigWebSocketMessage(data) {
    console.log('📡 Handling risk config WebSocket message:', data.type);
    
    switch (data.type) {
        case 'risk_config_saved':
            if (data.success) {
                console.log('✅ Risk configuration saved successfully via WebSocket');
                showToast('Risk configuration saved to server', 'success');
                
                // Update local display with server response
                if (data.config && data.config.risk_management) {
                    updateRiskConfigurationFromServer(data.config.risk_management);
                }
            } else {
                console.error('❌ Risk configuration save failed:', data.error);
                showToast(`Failed to save risk config: ${data.error}`, 'error');
            }
            break;
            
        case 'risk_config_data':
            if (data.success && data.config && data.config.risk_management) {
                console.log('📥 Risk configuration loaded from server');
                updateRiskConfigurationFromServer(data.config.risk_management);
                showToast('Risk configuration loaded from server', 'success');
            } else {
                console.error('❌ Risk configuration load failed:', data.error);
                showToast(`Failed to load risk config: ${data.error}`, 'error');
            }
            break;
            
        case 'risk_config_updated':
            if (data.config && data.config.risk_management) {
                console.log('🔄 Risk configuration updated by another client');
                updateRiskConfigurationFromServer(data.config.risk_management);
                showToast('Risk configuration updated by another user', 'info');
            }
            break;
            
        case 'risk_config_error':
            console.error('❌ Risk configuration error:', data.error);
            showToast(`Risk config error: ${data.error}`, 'error');
            break;
    }
}

function updateRiskConfigurationFromServer(serverConfig) {
    console.log('🔄 Updating risk configuration from server data');
    
    try {
        // Update form inputs with server data
        if (serverConfig.account_limits) {
            const limits = serverConfig.account_limits;
            if (limits.max_account_risk !== undefined) {
                document.getElementById('max_account_risk').value = limits.max_account_risk;
            }
            if (limits.daily_loss_limit !== undefined) {
                document.getElementById('daily_loss_limit').value = limits.daily_loss_limit;
            }
            if (limits.equity_buffer !== undefined) {
                document.getElementById('equity_buffer').value = limits.equity_buffer;
            }
        }
        
        if (serverConfig.position_sizing) {
            const sizing = serverConfig.position_sizing;
            if (sizing.max_position_size !== undefined) {
                document.getElementById('max_position_size').value = sizing.max_position_size;
            }
            if (sizing.max_positions !== undefined) {
                document.getElementById('max_positions').value = sizing.max_positions;
            }
        }
        
        if (serverConfig.stop_loss_settings) {
            const stopLoss = serverConfig.stop_loss_settings;
            if (stopLoss.method !== undefined) {
                document.getElementById('stop_loss_method').value = stopLoss.method;
            }
            if (stopLoss.value !== undefined) {
                document.getElementById('stop_loss_value').value = stopLoss.value;
            }
            if (stopLoss.take_profit_ratio !== undefined) {
                document.getElementById('take_profit_ratio').value = stopLoss.take_profit_ratio;
            }
        }
        
        // Update parameter states (toggles)
        if (serverConfig.parameter_states) {
            const states = serverConfig.parameter_states;
            Object.keys(states).forEach(toggleId => {
                const toggle = document.getElementById(toggleId.replace(/_/g, '-'));
                if (toggle) {
                    toggle.checked = states[toggleId];
                    handleParameterToggle(toggle, true); // Pass true to skip save during server update
                }
            });
            
            // Update global parameter states
            window.riskParameterStates = states;
        }
        
        // Update stop loss unit display
        updateStopLossUnit();
        
        // Update risk status display
        const config = getRiskConfiguration();
        updateRiskStatusDisplay(config);
        
        console.log('✅ Risk configuration updated from server');
        
    } catch (error) {
        console.error('❌ Error updating risk configuration from server:', error);
        showToast('Error updating risk config from server', 'error');
    }
}

// Extend WebSocket data handler to include risk config messages
function handleWebSocketDataExtended(data) {
    // Handle risk configuration messages
    if (data.type && data.type.startsWith('risk_config')) {
        handleRiskConfigWebSocketMessage(data);
        return;
    }
    
    // Handle other WebSocket data (existing functionality)
    if (window.volflowScreener && typeof window.volflowScreener.handleWebSocketData === 'function') {
        window.volflowScreener.handleWebSocketData(data);
    }
}

// Auto-load risk configuration when WebSocket connects
document.addEventListener('DOMContentLoaded', () => {
    // Wait for WebSocket connection and then load risk config
    setTimeout(() => {
        if (window.volflowScreener?.wsManager?.isConnected) {
            loadRiskConfigurationViaWebSocket();
        } else {
            // Set up listener for when WebSocket connects
            if (window.volflowScreener?.wsManager) {
                window.volflowScreener.wsManager.on('onConnect', () => {
                    setTimeout(() => {
                        loadRiskConfigurationViaWebSocket();
                    }, 1000); // Small delay to ensure connection is stable
                });
            }
        }
    }, 2000); // Wait 2 seconds for initialization
});

// Strategy Controls Functions
function initializeStrategyControls() {
    console.log('Initializing strategy controls...');
    
    // Get all strategy control toggles
    const strategyToggles = [
        'iron_condor_auto_execute',
        'iron_condor_manual_approval',
        'pml_auto_execute',
        'pml_manual_approval',
        'divergence_auto_execute',
        'divergence_manual_approval'
    ];
    
    strategyToggles.forEach(toggleId => {
        const toggle = document.getElementById(toggleId);
        if (toggle) {
            console.log(`Found strategy toggle: ${toggleId}`);
            
            // Add comprehensive event listeners for better UX
            setupToggleEventListeners(toggle, toggleId, (toggleElement, skipSave) => {
                handleStrategyToggle(toggleElement, skipSave);
            });
            
            // Set initial state without triggering save
            handleStrategyToggle(toggle, true); // Pass true to skip save during initialization
        } else {
            console.warn(`Strategy toggle not found: ${toggleId}`);
        }
    });
    
    // Load saved strategy configuration
    loadStrategyConfiguration();
    
    console.log('Strategy controls initialization complete');
}

function handleStrategyToggle(toggle, skipSave = false) {
    console.log(`handleStrategyToggle called for ${toggle.id}, checked: ${toggle.checked}`);
    
    const isEnabled = toggle.checked;
    const toggleId = toggle.id;
    
    // Store the strategy control state
    if (!window.strategyControlStates) {
        window.strategyControlStates = {};
    }
    
    window.strategyControlStates[toggleId] = isEnabled;
    
    // Handle mutual exclusivity for manual approval and auto execute
    if (toggleId.includes('manual_approval') && isEnabled) {
        // If manual approval is enabled, disable auto execute for the same strategy
        const strategyName = toggleId.replace('_manual_approval', '');
        const autoExecuteToggle = document.getElementById(`${strategyName}_auto_execute`);
        if (autoExecuteToggle && autoExecuteToggle.checked) {
            autoExecuteToggle.checked = false;
            window.strategyControlStates[`${strategyName}_auto_execute`] = false;
            console.log(`Disabled auto execute for ${strategyName} due to manual approval`);
        }
    } else if (toggleId.includes('auto_execute') && isEnabled) {
        // If auto execute is enabled, disable manual approval for the same strategy
        const strategyName = toggleId.replace('_auto_execute', '');
        const manualApprovalToggle = document.getElementById(`${strategyName}_manual_approval`);
        if (manualApprovalToggle && manualApprovalToggle.checked) {
            manualApprovalToggle.checked = false;
            window.strategyControlStates[`${strategyName}_manual_approval`] = false;
            console.log(`Disabled manual approval for ${strategyName} due to auto execute`);
        }
    }
    
    // Show toast notification only if not skipping
    if (!skipSave) {
        const strategyName = getStrategyDisplayName(toggleId);
        const controlType = getControlTypeDisplayName(toggleId);
        const status = isEnabled ? 'enabled' : 'disabled';
        showToast(`${strategyName} ${controlType} ${status}`, isEnabled ? 'success' : 'warning');
        
        // Auto-save the strategy configuration when toggles change
        saveStrategyConfigurationViaWebSocket(getStrategyConfiguration());
    }
}

function getStrategyDisplayName(toggleId) {
    if (toggleId.includes('iron_condor')) return 'Iron Condor';
    if (toggleId.includes('pml')) return 'PML Strategy';
    if (toggleId.includes('divergence')) return 'Divergence Strategy';
    return 'Strategy';
}

function getControlTypeDisplayName(toggleId) {
    if (toggleId.includes('auto_execute')) return 'Auto Execute';
    if (toggleId.includes('manual_approval')) return 'Manual Approval';
    return 'Control';
}

function getStrategyConfiguration() {
    return {
        iron_condor: {
            auto_execute: window.strategyControlStates?.iron_condor_auto_execute || false,
            manual_approval: window.strategyControlStates?.iron_condor_manual_approval || false
        },
        pml: {
            auto_execute: window.strategyControlStates?.pml_auto_execute || false,
            manual_approval: window.strategyControlStates?.pml_manual_approval || false
        },
        divergence: {
            auto_execute: window.strategyControlStates?.divergence_auto_execute || false,
            manual_approval: window.strategyControlStates?.divergence_manual_approval || false
        },
        timestamp: new Date().toISOString()
    };
}

function saveStrategyConfiguration() {
    console.log('Saving strategy configuration...');
    
    try {
        const config = getStrategyConfiguration();
        
        // Save to localStorage
        localStorage.setItem('volflow-strategy-config', JSON.stringify(config));
        
        showToast('Strategy configuration saved successfully', 'success');
        
    } catch (error) {
        console.error('Error saving strategy configuration:', error);
        showToast('Failed to save strategy configuration', 'error');
    }
}

function loadStrategyConfiguration() {
    console.log('Loading saved strategy configuration...');
    
    try {
        const saved = localStorage.getItem('volflow-strategy-config');
        if (saved) {
            const config = JSON.parse(saved);
            
            // Apply saved configuration to toggles
            if (config.iron_condor) {
                if (document.getElementById('iron_condor_auto_execute')) {
                    document.getElementById('iron_condor_auto_execute').checked = config.iron_condor.auto_execute;
                }
                if (document.getElementById('iron_condor_manual_approval')) {
                    document.getElementById('iron_condor_manual_approval').checked = config.iron_condor.manual_approval;
                }
            }
            
            if (config.pml) {
                if (document.getElementById('pml_auto_execute')) {
                    document.getElementById('pml_auto_execute').checked = config.pml.auto_execute;
                }
                if (document.getElementById('pml_manual_approval')) {
                    document.getElementById('pml_manual_approval').checked = config.pml.manual_approval;
                }
            }
            
            if (config.divergence) {
                if (document.getElementById('divergence_auto_execute')) {
                    document.getElementById('divergence_auto_execute').checked = config.divergence.auto_execute;
                }
                if (document.getElementById('divergence_manual_approval')) {
                    document.getElementById('divergence_manual_approval').checked = config.divergence.manual_approval;
                }
            }
            
            // Update strategy control states
            window.strategyControlStates = {
                iron_condor_auto_execute: config.iron_condor?.auto_execute || false,
                iron_condor_manual_approval: config.iron_condor?.manual_approval || false,
                pml_auto_execute: config.pml?.auto_execute || false,
                pml_manual_approval: config.pml?.manual_approval || false,
                divergence_auto_execute: config.divergence?.auto_execute || false,
                divergence_manual_approval: config.divergence?.manual_approval || false
            };
            
            console.log('Strategy configuration loaded:', config);
        }
    } catch (error) {
        console.error('Error loading strategy configuration:', error);
    }
}

// WebSocket Strategy Configuration Functions
function saveStrategyConfigurationViaWebSocket(strategyConfig) {
    console.log('💾 Saving strategy configuration via WebSocket...');
    
    if (!window.volflowScreener?.wsManager?.isConnected) {
        console.warn('⚠️ WebSocket not connected, cannot save strategy configuration');
        showToast('WebSocket not connected - strategy config not saved to server', 'warning');
        return;
    }
    
    try {
        // Enhance config with auto_approve status for each strategy
        const enhancedConfig = {};
        
        Object.keys(strategyConfig).forEach(strategyName => {
            if (strategyName !== 'timestamp') {
                const strategy = strategyConfig[strategyName];
                enhancedConfig[strategyName] = {
                    ...strategy,
                    auto_approve: strategy.auto_execute || false // auto_approve is true when auto_execute is true
                };
            }
        });
        
        // Send via WebSocket
        const message = {
            type: 'save_strategy_config',
            config: enhancedConfig,
            timestamp: new Date().toISOString()
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Strategy configuration with auto_approve sent via WebSocket');
        
    } catch (error) {
        console.error('❌ Error sending strategy configuration via WebSocket:', error);
        showToast('Failed to send strategy config via WebSocket', 'error');
    }
}

function saveSpecificStrategyConfigurationViaWebSocket(toggleId) {
    console.log(`💾 Saving specific strategy configuration for toggle: ${toggleId}`);
    
    if (!window.volflowScreener?.wsManager?.isConnected) {
        console.warn('⚠️ WebSocket not connected, cannot save strategy configuration');
        showToast('WebSocket not connected - strategy config not saved to server', 'warning');
        return;
    }
    
    try {
        // Extract strategy name from toggle ID
        let strategyName;
        console.log(`🔍 Analyzing toggle ID: ${toggleId}`);
        
        if (toggleId.startsWith('iron_condor_')) {
            strategyName = 'iron_condor';
        } else if (toggleId.startsWith('pml_')) {
            strategyName = 'pml';
        } else if (toggleId.startsWith('divergence_')) {
            strategyName = 'divergence';
        } else {
            console.error(`❌ Unknown strategy for toggle: ${toggleId}`);
            console.error(`Toggle ID does not match expected patterns: iron_condor_*, pml_*, divergence_*`);
            return;
        }
        
        console.log(`📊 Identified strategy: ${strategyName} for toggle: ${toggleId}`);
        
        // Get the current state for this specific strategy
        const strategyConfig = {
            auto_execute: window.strategyControlStates?.[`${strategyName}_auto_execute`] || false,
            manual_approval: window.strategyControlStates?.[`${strategyName}_manual_approval`] || false
        };
        
        // Add auto_approve status
        strategyConfig.auto_approve = strategyConfig.auto_execute || false;
        
        console.log(`📝 Strategy config for ${strategyName}:`, strategyConfig);
        
        // Create config object with only this strategy
        const config = {
            [strategyName]: strategyConfig
        };
        
        // Send via WebSocket
        const message = {
            type: 'save_strategy_config',
            config: config,
            timestamp: new Date().toISOString(),
            source: 'individual_toggle'
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log(`📡 Individual strategy configuration sent for ${strategyName}`);
        
    } catch (error) {
        console.error('❌ Error sending individual strategy config via WebSocket:', error);
        showToast('Failed to send strategy config via WebSocket', 'error');
    }
}

function loadStrategyConfigurationViaWebSocket() {
    console.log('📥 Loading strategy configuration via WebSocket...');
    
    if (!window.volflowScreener?.wsManager?.isConnected) {
        console.warn('⚠️ WebSocket not connected, cannot load strategy configuration');
        return;
    }
    
    try {
        const message = {
            type: 'get_strategy_config',
            timestamp: new Date().toISOString()
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Strategy configuration request sent via WebSocket');
        
    } catch (error) {
        console.error('❌ Error requesting strategy configuration via WebSocket:', error);
        showToast('Failed to request strategy config via WebSocket', 'error');
    }
}

// Global Manual Approval Functions
function approveSignal(symbol, strategyType) {
    console.log(`Approving signal for ${symbol} in ${strategyType} strategy`);
    
    // Show confirmation dialog
    if (!confirm(`Are you sure you want to APPROVE and EXECUTE the ${strategyType.replace('_', ' ')} signal for ${symbol}?`)) {
        return;
    }
    
    // Send approval via WebSocket
    if (window.volflowScreener?.wsManager?.isConnected) {
        const message = {
            type: 'approve_signal',
            symbol: symbol,
            strategy: strategyType,
            action: 'approve_and_execute',
            timestamp: new Date().toISOString(),
            user: 'web_interface'
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Signal approval sent via WebSocket');
    }
    
    // Update UI to show approval pending
    updateSignalApprovalStatus(symbol, strategyType, 'approved');
    
    // Show success message
    showToast(`${symbol} signal approved and queued for execution`, 'success');
}

function rejectSignal(symbol, strategyType) {
    console.log(`Rejecting signal for ${symbol} in ${strategyType} strategy`);
    
    // Show confirmation dialog
    if (!confirm(`Are you sure you want to REJECT the ${strategyType.replace('_', ' ')} signal for ${symbol}?`)) {
        return;
    }
    
    // Send rejection via WebSocket
    if (window.volflowScreener?.wsManager?.isConnected) {
        const message = {
            type: 'reject_signal',
            symbol: symbol,
            strategy: strategyType,
            action: 'reject',
            timestamp: new Date().toISOString(),
            user: 'web_interface'
        };
        
        window.volflowScreener.wsManager.send(message);
        console.log('📡 Signal rejection sent via WebSocket');
    }
    
    // Update UI to show rejection
    updateSignalApprovalStatus(symbol, strategyType, 'rejected');
    
    // Show info message
    showToast(`${symbol} signal rejected`, 'info');
}

function updateSignalApprovalStatus(symbol, strategyType, status) {
    // Find the signal card for this symbol and strategy
    const strategyCard = document.getElementById(`${strategyType.replace('_', '-')}-card`);
    if (!strategyCard) return;
    
    const signalsContainer = strategyCard.querySelector('.strategy-signals');
    if (!signalsContainer) return;
    
    // Find the specific signal item
    const signalItems = signalsContainer.querySelectorAll('.signal-item');
    signalItems.forEach(item => {
        const symbolElement = item.querySelector('.signal-symbol');
        if (symbolElement && symbolElement.textContent === symbol) {
            const approvalSection = item.querySelector('.signal-manual-approval');
            if (approvalSection) {
                // Update the approval section based on status
                switch (status) {
                    case 'approved':
                        approvalSection.innerHTML = `
                            <div class="approval-status approved">
                                <i class="fas fa-check-circle"></i>
                                <span>Signal Approved - Executing Trade...</span>
                            </div>
                        `;
                        approvalSection.style.background = '#d4edda';
                        approvalSection.style.borderColor = '#38a169';
                        break;
                        
                    case 'rejected':
                        approvalSection.innerHTML = `
                            <div class="approval-status rejected">
                                <i class="fas fa-times-circle"></i>
                                <span>Signal Rejected</span>
                            </div>
                        `;
                        approvalSection.style.background = '#f8d7da';
                        approvalSection.style.borderColor = '#e53e3e';
                        
                        // Remove the signal after 3 seconds
                        setTimeout(() => {
                            item.style.opacity = '0.5';
                            item.style.transform = 'scale(0.95)';
                            setTimeout(() => {
                                if (item.parentElement) {
                                    item.remove();
                                }
                            }, 500);
                        }, 3000);
                        break;
                        
                    case 'executed':
                        approvalSection.innerHTML = `
                            <div class="approval-status executed">
                                <i class="fas fa-check-double"></i>
                                <span>Trade Executed Successfully</span>
                            </div>
                        `;
                        approvalSection.style.background = '#d1ecf1';
                        approvalSection.style.borderColor = '#17a2b8';
                        
                        // Remove the signal after 5 seconds
                        setTimeout(() => {
                            item.style.opacity = '0.5';
                            item.style.transform = 'scale(0.95)';
                            setTimeout(() => {
                                if (item.parentElement) {
                                    item.remove();
                                }
                            }, 500);
                        }, 5000);
                        break;
                }
            }
        }
    });
}

// Add CSS styles for approval status
if (!document.querySelector('#approval-status-styles')) {
    const styles = document.createElement('style');
    styles.id = 'approval-status-styles';
    styles.textContent = `
        .approval-status {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.75rem;
            border-radius: 6px;
            font-size: 0.9rem;
            font-weight: 600;
            text-align: center;
            justify-content: center;
        }
        
        .approval-status.approved {
            color: #155724;
            background: #d4edda;
            border: 1px solid #c3e6cb;
        }
        
        .approval-status.rejected {
            color: #721c24;
            background: #f8d7da;
            border: 1px solid #f5c6cb;
        }
        
        .approval-status.executed {
            color: #0c5460;
            background: #d1ecf1;
            border: 1px solid #bee5eb;
        }
        
        .approval-status i {
            font-size: 1rem;
        }
    `;
    document.head.appendChild(styles);
}

// Handle visibility change to pause/resume operations
document.addEventListener('visibilitychange', () => {
    if (document.hidden && window.volflowScreener) {
        // Optionally pause operations when tab is not visible
    }
});
