// Risk Management Module
class RiskManager {
    constructor() {
        this.riskConfig = {};
        this.riskMetrics = {};
        this.init();
    }
    
    init() {
        console.log('Initializing risk manager...');
        this.bindEvents();
        this.loadRiskConfiguration();
        this.updateRiskDisplay();
    }
    
    bindEvents() {
        // Risk management action buttons
        const applyRiskBtn = document.getElementById('apply-risk-settings');
        const resetRiskBtn = document.getElementById('reset-risk-config');
        const saveRiskBtn = document.getElementById('save-risk-config');
        const exportRiskBtn = document.getElementById('export-risk-report');
        
        if (applyRiskBtn) applyRiskBtn.addEventListener('click', () => this.applyRiskSettings());
        if (resetRiskBtn) resetRiskBtn.addEventListener('click', () => this.resetRiskConfiguration());
        if (saveRiskBtn) saveRiskBtn.addEventListener('click', () => this.saveRiskConfiguration());
        if (exportRiskBtn) exportRiskBtn.addEventListener('click', () => this.exportRiskReport());
        
        // Risk parameter toggles and inputs
        this.bindRiskParameterEvents();
    }
    
    bindRiskParameterEvents() {
        // Risk parameter toggles
        const riskToggles = [
            'enable_max_account_risk',
            'enable_daily_loss_limit',
            'enable_equity_buffer',
            'enable_max_position_size',
            'enable_max_positions',
            'enable_stop_loss',
            'enable_stop_loss_value',
            'enable_risk_reward_ratio'
        ];
        
        riskToggles.forEach(toggleId => {
            const toggle = document.getElementById(toggleId);
            if (toggle) {
                this.setupToggleEventListeners(toggle, toggleId, (toggleElement) => {
                    this.handleRiskToggle(toggleElement);
                });
            }
        });
        
        // Risk parameter inputs
        const riskInputs = [
            'max_account_risk',
            'daily_loss_limit',
            'equity_buffer',
            'max_position_size',
            'max_positions',
            'stop_loss_value',
            'take_profit_ratio'
        ];
        
        riskInputs.forEach(inputId => {
            const input = document.getElementById(inputId);
            if (input) {
                input.addEventListener('change', (e) => this.handleRiskInputChange(e.target));
                input.addEventListener('input', (e) => this.validateRiskInput(e.target));
            }
        });
        
        // Stop loss method selector
        const stopLossMethod = document.getElementById('stop_loss_method');
        if (stopLossMethod) {
            stopLossMethod.addEventListener('change', (e) => this.handleStopLossMethodChange(e.target.value));
        }
    }
    
    setupToggleEventListeners(toggle, toggleId, handlerFunction) {
        // Add event listener to the checkbox input
        toggle.addEventListener('change', (e) => {
            handlerFunction(e.target);
        });
        
        // Find the parent toggle switch container
        const toggleSwitch = toggle.closest('.toggle-switch');
        if (toggleSwitch) {
            // Add click event listener to the entire toggle switch area
            toggleSwitch.addEventListener('click', (e) => {
                if (e.target === toggle) return;
                toggle.checked = !toggle.checked;
                toggle.dispatchEvent(new Event('change'));
            });
            
            // Add click event listener to the toggle slider specifically
            const toggleSlider = toggleSwitch.querySelector('.toggle-slider');
            if (toggleSlider) {
                toggleSlider.addEventListener('click', (e) => {
                    e.stopPropagation();
                    toggle.checked = !toggle.checked;
                    toggle.dispatchEvent(new Event('change'));
                });
            }
        }
    }
    
    handleRiskToggle(toggle) {
        const isEnabled = toggle.checked;
        const toggleId = toggle.id;
        
        // Store the risk parameter state
        this.riskConfig[toggleId] = isEnabled;
        
        // Show/hide related input controls
        this.updateParameterControlVisibility(toggleId, isEnabled);
        
        // Auto-save configuration
        this.saveRiskConfiguration();
        
        // Show toast notification
        const parameterName = this.getRiskParameterDisplayName(toggleId);
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`${parameterName} ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    updateParameterControlVisibility(toggleId, isEnabled) {
        // Map toggle IDs to their corresponding input controls
        const controlMappings = {
            'enable_max_account_risk': 'max_account_risk',
            'enable_daily_loss_limit': 'daily_loss_limit',
            'enable_equity_buffer': 'equity_buffer',
            'enable_max_position_size': 'max_position_size',
            'enable_max_positions': 'max_positions',
            'enable_stop_loss_value': 'stop_loss_value',
            'enable_risk_reward_ratio': 'take_profit_ratio'
        };
        
        const inputId = controlMappings[toggleId];
        if (inputId) {
            const inputElement = document.getElementById(inputId);
            const inputGroup = inputElement?.closest('.input-group');
            
            if (inputGroup) {
                inputGroup.style.opacity = isEnabled ? '1' : '0.5';
                if (inputElement) {
                    inputElement.disabled = !isEnabled;
                }
            }
        }
    }
    
    handleRiskInputChange(input) {
        const inputId = input.id;
        const value = parseFloat(input.value) || 0;
        
        // Store the risk parameter value
        this.riskConfig[inputId] = value;
        
        // Validate the input
        this.validateRiskInput(input);
        
        // Update risk calculations
        this.updateRiskCalculations();
        
        // Auto-save configuration
        this.saveRiskConfiguration();
    }
    
    validateRiskInput(input) {
        const value = parseFloat(input.value);
        const min = parseFloat(input.min);
        const max = parseFloat(input.max);
        
        // Remove existing validation classes
        input.classList.remove('error', 'warning');
        
        // Validate range
        if (value < min || value > max) {
            input.classList.add('error');
            this.showToast(`${this.getRiskParameterDisplayName(input.id)} must be between ${min} and ${max}`, 'error');
        } else if (input.id === 'daily_loss_limit' && value > 10) {
            input.classList.add('warning');
        } else if (input.id === 'max_account_risk' && value > 30) {
            input.classList.add('warning');
        }
    }
    
    handleStopLossMethodChange(method) {
        this.riskConfig.stop_loss_method = method;
        
        // Update the stop loss unit display
        const stopLossUnit = document.getElementById('stop-loss-unit');
        if (stopLossUnit) {
            const units = {
                'percentage': '%',
                'atr': 'x ATR',
                'technical': 'levels',
                'volatility': 'x Vol'
            };
            stopLossUnit.textContent = units[method] || 'x ATR';
        }
        
        // Auto-save configuration
        this.saveRiskConfiguration();
        
        this.showToast(`Stop loss method changed to ${method}`, 'info');
    }
    
    getRiskParameterDisplayName(parameterId) {
        const displayNames = {
            'enable_max_account_risk': 'Maximum Account Risk',
            'max_account_risk': 'Maximum Account Risk',
            'enable_daily_loss_limit': 'Daily Loss Limit',
            'daily_loss_limit': 'Daily Loss Limit',
            'enable_equity_buffer': 'Equity Buffer',
            'equity_buffer': 'Equity Buffer',
            'enable_max_position_size': 'Maximum Position Size',
            'max_position_size': 'Maximum Position Size',
            'enable_max_positions': 'Maximum Positions',
            'max_positions': 'Maximum Positions',
            'enable_stop_loss': 'Stop Loss',
            'enable_stop_loss_value': 'Stop Loss Value',
            'stop_loss_value': 'Stop Loss Value',
            'enable_risk_reward_ratio': 'Risk/Reward Ratio',
            'take_profit_ratio': 'Risk/Reward Ratio'
        };
        return displayNames[parameterId] || parameterId;
    }
    
    updateRiskDisplay() {
        // Update risk overview cards with current data
        this.updateRiskOverviewCards();
        
        // Update risk status indicator
        this.updateRiskStatusIndicator();
    }
    
    updateRiskOverviewCards() {
        // Get current equity from positions manager if available
        const currentEquity = window.positionsManager?.getCurrentEquity() || 125847.32;
        const activePositions = window.positionsManager?.getActivePositionsCount() || 0;
        const dailyPL = window.positionsManager?.getDailyPL() || 0;
        const positionsValue = window.positionsManager?.getPositionsValue() || 0;
        
        // Update account equity
        const equityElement = document.getElementById('current-equity');
        const equityChangeElement = document.getElementById('equity-change');
        if (equityElement) {
            equityElement.textContent = `$${currentEquity.toLocaleString('en-US', { 
                minimumFractionDigits: 2, 
                maximumFractionDigits: 2 
            })}`;
        }
        
        // Calculate equity change percentage (simplified)
        const equityChangePercent = ((dailyPL / currentEquity) * 100);
        if (equityChangeElement) {
            const changeClass = equityChangePercent >= 0 ? 'positive' : 'negative';
            const changeSign = equityChangePercent >= 0 ? '+' : '';
            equityChangeElement.textContent = `${changeSign}${equityChangePercent.toFixed(2)}%`;
            equityChangeElement.className = `risk-change ${changeClass}`;
        }
        
        // Update risk utilization
        const riskUtilizationElement = document.getElementById('risk-utilization');
        if (riskUtilizationElement) {
            const maxAccountRisk = this.riskConfig.max_account_risk || 25;
            const currentRiskUtilization = (positionsValue / currentEquity) * 100;
            riskUtilizationElement.textContent = `${currentRiskUtilization.toFixed(1)}%`;
        }
        
        // Update active positions
        const activePositionsElement = document.getElementById('active-positions');
        if (activePositionsElement) {
            activePositionsElement.textContent = activePositions.toString();
        }
        
        // Update daily drawdown
        const dailyDrawdownElement = document.getElementById('daily-drawdown');
        if (dailyDrawdownElement) {
            const drawdownPercent = (dailyPL / currentEquity) * 100;
            const drawdownClass = drawdownPercent >= 0 ? 'positive' : 'negative';
            const drawdownSign = drawdownPercent >= 0 ? '+' : '';
            dailyDrawdownElement.textContent = `${drawdownSign}${drawdownPercent.toFixed(2)}%`;
            dailyDrawdownElement.className = `risk-value ${drawdownClass}`;
        }
    }
    
    updateRiskStatusIndicator() {
        const riskStatusElement = document.getElementById('risk-status');
        if (!riskStatusElement) return;
        
        const statusDot = riskStatusElement.querySelector('.status-dot');
        const statusText = riskStatusElement.querySelector('.status-text');
        
        // Calculate overall risk level
        const riskLevel = this.calculateOverallRiskLevel();
        
        if (statusDot && statusText) {
            switch (riskLevel) {
                case 'safe':
                    statusDot.className = 'status-dot safe';
                    statusText.textContent = 'Safe';
                    break;
                case 'moderate':
                    statusDot.className = 'status-dot moderate';
                    statusText.textContent = 'Moderate';
                    break;
                case 'high':
                    statusDot.className = 'status-dot high';
                    statusText.textContent = 'High Risk';
                    break;
                case 'critical':
                    statusDot.className = 'status-dot critical';
                    statusText.textContent = 'Critical';
                    break;
            }
        }
    }
    
    calculateOverallRiskLevel() {
        const currentEquity = window.positionsManager?.getCurrentEquity() || 125847.32;
        const activePositions = window.positionsManager?.getActivePositionsCount() || 0;
        const dailyPL = window.positionsManager?.getDailyPL() || 0;
        const positionsValue = window.positionsManager?.getPositionsValue() || 0;
        
        // Calculate risk factors
        const riskUtilization = (positionsValue / currentEquity) * 100;
        const dailyDrawdown = Math.abs((dailyPL / currentEquity) * 100);
        const positionCount = activePositions;
        
        const maxAccountRisk = this.riskConfig.max_account_risk || 25;
        const maxDailyLoss = this.riskConfig.daily_loss_limit || 5;
        const maxPositions = this.riskConfig.max_positions || 15;
        
        // Determine risk level based on thresholds
        if (riskUtilization > maxAccountRisk * 0.8 || 
            dailyDrawdown > maxDailyLoss * 0.8 || 
            positionCount > maxPositions * 0.8) {
            return 'high';
        } else if (riskUtilization > maxAccountRisk * 0.6 || 
                   dailyDrawdown > maxDailyLoss * 0.6 || 
                   positionCount > maxPositions * 0.6) {
            return 'moderate';
        } else if (riskUtilization > maxAccountRisk * 0.9 || 
                   dailyDrawdown > maxDailyLoss * 0.9) {
            return 'critical';
        } else {
            return 'safe';
        }
    }
    
    updateRiskCalculations() {
        // Recalculate risk metrics based on current configuration
        this.updateRiskDisplay();
    }
    
    applyRiskSettings() {
        console.log('Applying risk settings...');
        
        // Validate all risk parameters
        const isValid = this.validateAllRiskParameters();
        
        if (!isValid) {
            this.showToast('Please fix validation errors before applying settings', 'error');
            return;
        }
        
        // Send risk configuration to backend via WebSocket
        if (window.volflowApp?.modules?.websocket?.isConnected) {
            const message = {
                type: 'update_risk_config',
                config: this.getRiskConfiguration(),
                timestamp: new Date().toISOString(),
                user: 'web_interface'
            };
            
            window.volflowApp.modules.websocket.send(message);
            console.log('📡 Risk configuration sent via WebSocket');
        }
        
        // Save configuration locally
        this.saveRiskConfiguration();
        
        // Update risk calculations
        this.updateRiskCalculations();
        
        this.showToast('Risk settings applied successfully', 'success');
    }
    
    validateAllRiskParameters() {
        const riskInputs = document.querySelectorAll('.risk-parameter-card input[type="number"]');
        let isValid = true;
        
        riskInputs.forEach(input => {
            this.validateRiskInput(input);
            if (input.classList.contains('error')) {
                isValid = false;
            }
        });
        
        return isValid;
    }
    
    resetRiskConfiguration() {
        console.log('Resetting risk configuration to defaults...');
        
        // Show confirmation dialog
        if (!confirm('Are you sure you want to reset all risk settings to defaults? This action cannot be undone.')) {
            return;
        }
        
        // Default risk configuration
        const defaultConfig = {
            enable_max_account_risk: true,
            max_account_risk: 25,
            enable_daily_loss_limit: true,
            daily_loss_limit: 5,
            enable_equity_buffer: true,
            equity_buffer: 10000,
            enable_max_position_size: true,
            max_position_size: 5,
            enable_max_positions: true,
            max_positions: 15,
            enable_stop_loss: true,
            stop_loss_method: 'atr',
            enable_stop_loss_value: true,
            stop_loss_value: 2.0,
            enable_risk_reward_ratio: true,
            take_profit_ratio: 2.0
        };
        
        // Apply default configuration to UI
        this.applyConfigurationToUI(defaultConfig);
        
        // Store configuration
        this.riskConfig = defaultConfig;
        
        // Save to localStorage
        this.saveRiskConfiguration();
        
        // Update risk calculations
        this.updateRiskCalculations();
        
        this.showToast('Risk configuration reset to defaults', 'success');
    }
    
    applyConfigurationToUI(config) {
        // Apply toggle states
        Object.keys(config).forEach(key => {
            if (key.startsWith('enable_')) {
                const toggle = document.getElementById(key);
                if (toggle) {
                    toggle.checked = config[key];
                    this.updateParameterControlVisibility(key, config[key]);
                }
            } else {
                const input = document.getElementById(key);
                if (input) {
                    input.value = config[key];
                }
            }
        });
        
        // Update stop loss method selector
        const stopLossMethod = document.getElementById('stop_loss_method');
        if (stopLossMethod) {
            stopLossMethod.value = config.stop_loss_method || 'atr';
            this.handleStopLossMethodChange(stopLossMethod.value);
        }
    }
    
    getRiskConfiguration() {
        return {
            max_account_risk: {
                enabled: this.riskConfig.enable_max_account_risk || false,
                value: this.riskConfig.max_account_risk || 25
            },
            daily_loss_limit: {
                enabled: this.riskConfig.enable_daily_loss_limit || false,
                value: this.riskConfig.daily_loss_limit || 5
            },
            equity_buffer: {
                enabled: this.riskConfig.enable_equity_buffer || false,
                value: this.riskConfig.equity_buffer || 10000
            },
            max_position_size: {
                enabled: this.riskConfig.enable_max_position_size || false,
                value: this.riskConfig.max_position_size || 5
            },
            max_positions: {
                enabled: this.riskConfig.enable_max_positions || false,
                value: this.riskConfig.max_positions || 15
            },
            stop_loss: {
                enabled: this.riskConfig.enable_stop_loss || false,
                method: this.riskConfig.stop_loss_method || 'atr',
                value: this.riskConfig.stop_loss_value || 2.0
            },
            risk_reward_ratio: {
                enabled: this.riskConfig.enable_risk_reward_ratio || false,
                value: this.riskConfig.take_profit_ratio || 2.0
            },
            timestamp: new Date().toISOString()
        };
    }
    
    saveRiskConfiguration() {
        console.log('Saving risk configuration...');
        
        try {
            const config = this.getRiskConfiguration();
            
            // Save to localStorage
            localStorage.setItem('volflow-risk-config', JSON.stringify(config));
            
            console.log('Risk configuration saved successfully');
            
        } catch (error) {
            console.error('Error saving risk configuration:', error);
            this.showToast('Failed to save risk configuration', 'error');
        }
    }
    
    loadRiskConfiguration() {
        console.log('Loading saved risk configuration...');
        
        try {
            const saved = localStorage.getItem('volflow-risk-config');
            if (saved) {
                const config = JSON.parse(saved);
                
                // Convert saved config to internal format
                const internalConfig = {
                    enable_max_account_risk: config.max_account_risk?.enabled || false,
                    max_account_risk: config.max_account_risk?.value || 25,
                    enable_daily_loss_limit: config.daily_loss_limit?.enabled || false,
                    daily_loss_limit: config.daily_loss_limit?.value || 5,
                    enable_equity_buffer: config.equity_buffer?.enabled || false,
                    equity_buffer: config.equity_buffer?.value || 10000,
                    enable_max_position_size: config.max_position_size?.enabled || false,
                    max_position_size: config.max_position_size?.value || 5,
                    enable_max_positions: config.max_positions?.enabled || false,
                    max_positions: config.max_positions?.value || 15,
                    enable_stop_loss: config.stop_loss?.enabled || false,
                    stop_loss_method: config.stop_loss?.method || 'atr',
                    enable_stop_loss_value: config.stop_loss?.enabled || false,
                    stop_loss_value: config.stop_loss?.value || 2.0,
                    enable_risk_reward_ratio: config.risk_reward_ratio?.enabled || false,
                    take_profit_ratio: config.risk_reward_ratio?.value || 2.0
                };
                
                // Apply configuration to UI
                this.applyConfigurationToUI(internalConfig);
                
                // Store configuration
                this.riskConfig = internalConfig;
                
                console.log('Risk configuration loaded:', config);
            }
        } catch (error) {
            console.error('Error loading risk configuration:', error);
        }
    }
    
    exportRiskReport() {
        console.log('Exporting risk report...');
        
        const currentEquity = window.positionsManager?.getCurrentEquity() || 125847.32;
        const activePositions = window.positionsManager?.getActivePositionsCount() || 0;
        const dailyPL = window.positionsManager?.getDailyPL() || 0;
        const positionsValue = window.positionsManager?.getPositionsValue() || 0;
        
        const riskReport = {
            timestamp: new Date().toISOString(),
            account_summary: {
                current_equity: currentEquity,
                daily_pl: dailyPL,
                active_positions: activePositions,
                positions_value: positionsValue
            },
            risk_metrics: {
                risk_utilization_percent: (positionsValue / currentEquity) * 100,
                daily_drawdown_percent: (dailyPL / currentEquity) * 100,
                overall_risk_level: this.calculateOverallRiskLevel()
            },
            risk_configuration: this.getRiskConfiguration(),
            compliance_status: {
                within_account_risk_limit: (positionsValue / currentEquity) * 100 <= (this.riskConfig.max_account_risk || 25),
                within_daily_loss_limit: Math.abs((dailyPL / currentEquity) * 100) <= (this.riskConfig.daily_loss_limit || 5),
                within_position_limit: activePositions <= (this.riskConfig.max_positions || 15)
            }
        };
        
        // Create and download the report
        const blob = new Blob([JSON.stringify(riskReport, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `volflow-risk-report-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        this.showToast('Risk report exported successfully', 'success');
    }
    
    // Update from WebSocket data
    updateFromWebSocket(data) {
        if (data.risk_metrics) {
            console.log('📊 Processing risk metrics from WebSocket');
            this.updateRiskMetricsFromWebSocket(data.risk_metrics);
        }
        
        if (data.account_data) {
            console.log('💰 Processing account data for risk calculations');
            // Risk calculations will be updated when positions manager processes this data
            setTimeout(() => this.updateRiskDisplay(), 100);
        }
    }
    
    updateRiskMetricsFromWebSocket(riskMetrics) {
        // Store received risk metrics
        this.riskMetrics = riskMetrics;
        
        // Update risk display with real-time data
        this.updateRiskDisplay();
    }
    
    showToast(message, type = 'info') {
        // Use the global toast function if available
        if (window.showToast) {
            window.showToast(message, type);
        } else {
            console.log(`Toast: ${message} (${type})`);
        }
    }
}
