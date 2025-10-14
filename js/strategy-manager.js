// Strategy Management Module - Simplified Display Only
class StrategyManager {
    constructor() {
        this.strategies = {};
        this.signals = {};
        this.controlStates = {};
        this.init();
    }
    
    init() {
        console.log('Initializing strategy manager...');
        this.bindEvents();
        this.initializeStrategyControls();
        this.loadStrategyConfiguration();
    }
    
    bindEvents() {
        // Handle refresh button - simple refresh from WebSocket data
        const refreshStrategiesBtn = document.getElementById('refresh-strategies');
        
        if (refreshStrategiesBtn) {
            refreshStrategiesBtn.addEventListener('click', () => {
                console.log('Strategy refresh requested - data comes from WebSocket');
                this.showToast('Strategy data refreshes automatically via WebSocket', 'info');
            });
        }
        
        console.log('Strategy manager events bound');
    }
    
    // Strategy Controls Functions - Keep for UI state management
    initializeStrategyControls() {
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
                this.setupToggleEventListeners(toggle, toggleId);
                this.handleStrategyToggle(toggle, true); // Initialize without saving
            }
        });
        
        console.log('Strategy controls initialization complete');
    }
    
    setupToggleEventListeners(toggle, toggleId) {
        console.log(`Setting up toggle listeners for: ${toggleId}`);
        
        // Add event listener to the checkbox input
        toggle.addEventListener('change', (e) => {
            console.log(`Toggle changed: ${toggleId}, checked: ${e.target.checked}`);
            this.handleStrategyToggle(e.target, false);
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
    
    handleStrategyToggle(toggle, skipSave = false) {
        console.log(`handleStrategyToggle called for ${toggle.id}, checked: ${toggle.checked}`);
        
        const isEnabled = toggle.checked;
        const toggleId = toggle.id;
        
        // Store the strategy control state
        if (!this.controlStates) {
            this.controlStates = {};
        }
        
        this.controlStates[toggleId] = isEnabled;
        
        // Handle mutual exclusivity for manual approval and auto execute
        if (toggleId.includes('manual_approval') && isEnabled) {
            const strategyName = toggleId.replace('_manual_approval', '');
            const autoExecuteToggle = document.getElementById(`${strategyName}_auto_execute`);
            if (autoExecuteToggle && autoExecuteToggle.checked) {
                autoExecuteToggle.checked = false;
                this.controlStates[`${strategyName}_auto_execute`] = false;
            }
        } else if (toggleId.includes('auto_execute') && isEnabled) {
            const strategyName = toggleId.replace('_auto_execute', '');
            const manualApprovalToggle = document.getElementById(`${strategyName}_manual_approval`);
            if (manualApprovalToggle && manualApprovalToggle.checked) {
                manualApprovalToggle.checked = false;
                this.controlStates[`${strategyName}_manual_approval`] = false;
            }
        }
        
        if (!skipSave) {
            this.saveStrategyConfiguration();
        }
    }
    
    saveStrategyConfiguration() {
        console.log('Saving strategy configuration...');
        
        try {
            const config = this.getStrategyConfiguration();
            
            // Save to localStorage for immediate UI state
            localStorage.setItem('volflow-strategy-config', JSON.stringify(config));
            
            // Send to websocket server to update auto_approve_config.json
            this.sendStrategyConfigToWebSocket(config);
            
        } catch (error) {
            console.error('Error saving strategy configuration:', error);
        }
    }
    
    sendStrategyConfigToWebSocket(config) {
        console.log('📡 Sending strategy configuration to WebSocket server...');
        console.log('🔧 Config to send:', config);
        
        try {
            // Check WebSocket connection - try multiple possible references (same pattern as watchlist)
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager) {
                console.error('❌ WebSocket manager not found');
                this.showToast('WebSocket connection not available', 'error');
                return;
            }
            
            if (!wsManager.ws) {
                console.error('❌ WebSocket not created');
                this.showToast('WebSocket not connected', 'error');
                return;
            }
            
            if (wsManager.ws.readyState !== WebSocket.OPEN) {
                console.error('❌ WebSocket not open, state:', wsManager.ws.readyState);
                this.showToast('WebSocket not connected', 'error');
                return;
            }
            
            const message = {
                type: 'save_strategy_config',
                config: config,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending WebSocket message:', message);
            
            // Use direct WebSocket send (same pattern as watchlist manager)
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Strategy config WebSocket message sent successfully');
            
            // Show user feedback
            this.showToast('Strategy configuration saved', 'success');
            
        } catch (error) {
            console.error('❌ Error sending strategy config to WebSocket:', error);
            this.showToast('Error saving configuration to server', 'error');
        }
    }
    
    loadStrategyConfiguration() {
        console.log('Loading saved strategy configuration...');
        
        // First try to load from WebSocket server (authoritative source)
        this.loadStrategyConfigFromWebSocket();
        
        // Fallback to localStorage if WebSocket is not available
        try {
            const saved = localStorage.getItem('volflow-strategy-config');
            if (saved) {
                const config = JSON.parse(saved);
                console.log('📋 Loaded strategy config from localStorage as fallback:', config);
                this.applyStrategyConfiguration(config);
            }
        } catch (error) {
            console.error('Error loading strategy configuration from localStorage:', error);
        }
    }
    
    loadStrategyConfigFromWebSocket() {
        console.log('📡 Requesting strategy configuration from WebSocket server...');
        
        try {
            // Check WebSocket connection - try multiple possible references (same pattern as watchlist)
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager) {
                console.warn('⚠️ WebSocket manager not found, using localStorage fallback');
                return;
            }
            
            if (!wsManager.ws) {
                console.warn('⚠️ WebSocket not created, using localStorage fallback');
                return;
            }
            
            if (wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not open, using localStorage fallback');
                return;
            }
            
            const message = {
                type: 'get_strategy_config',
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending WebSocket config request:', message);
            
            // Use direct WebSocket send (same pattern as watchlist manager)
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Strategy config request WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error requesting strategy config from WebSocket:', error);
        }
    }
    
    applyStrategyConfiguration(config) {
        console.log('🔧 Applying strategy configuration:', config);
        
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
        this.controlStates = {
            iron_condor_auto_execute: config.iron_condor?.auto_execute || false,
            iron_condor_manual_approval: config.iron_condor?.manual_approval || false,
            pml_auto_execute: config.pml?.auto_execute || false,
            pml_manual_approval: config.pml?.manual_approval || false,
            divergence_auto_execute: config.divergence?.auto_execute || false,
            divergence_manual_approval: config.divergence?.manual_approval || false
        };
        
        console.log('✅ Strategy configuration applied successfully');
    }
    
    // Handle WebSocket responses for strategy configuration
    handleWebSocketMessage(data) {
        if (data.type === 'strategy_config_data') {
            console.log('📡 Received strategy config from WebSocket server:', data.config);
            this.applyStrategyConfiguration(data.config);
            
            // Also save to localStorage for offline use
            localStorage.setItem('volflow-strategy-config', JSON.stringify(data.config));
        } else if (data.type === 'strategy_config_saved') {
            console.log('✅ Strategy config saved successfully on server');
        } else if (data.type === 'strategy_config_updated') {
            console.log('📡 Strategy config updated from another client:', data.config);
            this.applyStrategyConfiguration(data.config);
        } else if (data.type === 'strategy_config_error') {
            console.error('❌ Strategy config error from server:', data.error);
            this.showToast('Error loading strategy configuration', 'error');
        }
    }
    
    getStrategyConfiguration() {
        return {
            iron_condor: {
                auto_execute: this.controlStates?.iron_condor_auto_execute || false,
                manual_approval: this.controlStates?.iron_condor_manual_approval || false
            },
            pml: {
                auto_execute: this.controlStates?.pml_auto_execute || false,
                manual_approval: this.controlStates?.pml_manual_approval || false
            },
            divergence: {
                auto_execute: this.controlStates?.divergence_auto_execute || false,
                manual_approval: this.controlStates?.divergence_manual_approval || false
            },
            timestamp: new Date().toISOString()
        };
    }
    
    // Update from WebSocket data - Simplified display only
    updateFromWebSocket(data) {
        if (data.pml_signals) {
            console.log('📊 Processing PML signals from PostgreSQL WebSocket');
            this.updateStrategyCardFromWebSocket('pml', data.pml_signals);
        }
        
        if (data.iron_condor_signals) {
            console.log('🎯 Processing Iron Condor signals from PostgreSQL WebSocket');
            this.updateStrategyCardFromWebSocket('iron-condor', data.iron_condor_signals);
        }
        
        if (data.divergence_signals) {
            console.log('📈 Processing Divergence signals from PostgreSQL WebSocket');
            this.updateStrategyCardFromWebSocket('divergence', data.divergence_signals);
        }
    }
    
    updateStrategyCardFromWebSocket(strategyType, signals) {
        const statusElement = document.getElementById(`${strategyType}-status`);
        const signalsElement = document.getElementById(`${strategyType}-signals`);
        
        if (!statusElement || !signalsElement || !Array.isArray(signals)) {
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
        
        // Simple display - no manual approval buttons or complex logic
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
                    <div class="signal-detail-value">${signal.stop_loss && signal.stop_loss > 0 ? '$' + parseFloat(signal.stop_loss).toFixed(2) : 'N/A'}</div>
                </div>
                <div class="signal-detail">
                    <div class="signal-detail-label">Target</div>
                    <div class="signal-detail-value">${signal.profit_target && signal.profit_target > 0 ? '$' + parseFloat(signal.profit_target).toFixed(2) : 'N/A'}</div>
                </div>
            </div>
            <div class="signal-confidence">
                <div class="confidence-bar">
                    <div class="confidence-fill" style="width: ${confidencePercent}%"></div>
                </div>
                <div class="confidence-text">${confidencePercent}%</div>
            </div>
        `;
    }
    
    truncateText(text, maxLength) {
        if (!text || text.length <= maxLength) return text || 'N/A';
        return text.substring(0, maxLength) + '...';
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
