// Strategy Management Module
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
        const refreshStrategiesBtn = document.getElementById('refresh-strategies');
        
        if (refreshStrategiesBtn) {
            refreshStrategiesBtn.addEventListener('click', () => this.refreshStrategies());
        }
    }
    
    async refreshStrategies() {
        // Fetch strategy signals from live monitor JSON
        const refreshBtn = document.getElementById('refresh-strategies');
        const originalContent = refreshBtn ? refreshBtn.innerHTML : '';
        
        // Show loading state
        if (refreshBtn) {
            refreshBtn.disabled = true;
            refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        }
        
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
            if (refreshBtn) {
                refreshBtn.disabled = false;
                refreshBtn.innerHTML = originalContent;
            }
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
        return 'iron_condor'; // Default fallback
    }
    
    isManualApprovalEnabled(strategyType) {
        // Check if manual approval is enabled for the given strategy
        if (!this.controlStates) {
            return false;
        }
        
        const manualApprovalKey = `${strategyType}_manual_approval`;
        return this.controlStates[manualApprovalKey] || false;
    }
    
    // Strategy Controls Functions
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
                
                // Add comprehensive event listeners for better UX
                this.setupToggleEventListeners(toggle, toggleId, (toggleElement, skipSave) => {
                    this.handleStrategyToggle(toggleElement, skipSave);
                });
                
                // Set initial state without triggering save
                this.handleStrategyToggle(toggle, true); // Pass true to skip save during initialization
            } else {
                console.warn(`Strategy toggle not found: ${toggleId}`);
            }
        });
        
        // Load saved strategy configuration
        this.loadStrategyConfiguration();
        
        console.log('Strategy controls initialization complete');
    }
    
    setupToggleEventListeners(toggle, toggleId, handlerFunction) {
        console.log(`Setting up toggle listeners for: ${toggleId}`);
        
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
            // If manual approval is enabled, disable auto execute for the same strategy
            const strategyName = toggleId.replace('_manual_approval', '');
            const autoExecuteToggle = document.getElementById(`${strategyName}_auto_execute`);
            if (autoExecuteToggle && autoExecuteToggle.checked) {
                autoExecuteToggle.checked = false;
                this.controlStates[`${strategyName}_auto_execute`] = false;
                console.log(`Disabled auto execute for ${strategyName} due to manual approval`);
            }
        } else if (toggleId.includes('auto_execute') && isEnabled) {
            // If auto execute is enabled, disable manual approval for the same strategy
            const strategyName = toggleId.replace('_auto_execute', '');
            const manualApprovalToggle = document.getElementById(`${strategyName}_manual_approval`);
            if (manualApprovalToggle && manualApprovalToggle.checked) {
                manualApprovalToggle.checked = false;
                this.controlStates[`${strategyName}_manual_approval`] = false;
                console.log(`Disabled manual approval for ${strategyName} due to auto execute`);
            }
        }
        
        // Show toast notification only if not skipping
        if (!skipSave) {
            const strategyName = this.getStrategyDisplayName(toggleId);
            const controlType = this.getControlTypeDisplayName(toggleId);
            const status = isEnabled ? 'enabled' : 'disabled';
            this.showToast(`${strategyName} ${controlType} ${status}`, isEnabled ? 'success' : 'warning');
            
            // Auto-save the strategy configuration when toggles change
            this.saveStrategyConfiguration();
        }
    }
    
    getStrategyDisplayName(toggleId) {
        if (toggleId.includes('iron_condor')) return 'Iron Condor';
        if (toggleId.includes('pml')) return 'PML Strategy';
        if (toggleId.includes('divergence')) return 'Divergence Strategy';
        return 'Strategy';
    }
    
    getControlTypeDisplayName(toggleId) {
        if (toggleId.includes('auto_execute')) return 'Auto Execute';
        if (toggleId.includes('manual_approval')) return 'Manual Approval';
        return 'Control';
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
    
    saveStrategyConfiguration() {
        console.log('Saving strategy configuration...');
        
        try {
            const config = this.getStrategyConfiguration();
            
            // Save to localStorage
            localStorage.setItem('volflow-strategy-config', JSON.stringify(config));
            
            this.showToast('Strategy configuration saved successfully', 'success');
            
        } catch (error) {
            console.error('Error saving strategy configuration:', error);
            this.showToast('Failed to save strategy configuration', 'error');
        }
    }
    
    loadStrategyConfiguration() {
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
                this.controlStates = {
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
    
    // Update from WebSocket data
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
    
    showToast(message, type = 'info') {
        // Use the global toast function if available
        if (window.showToast) {
            window.showToast(message, type);
        } else {
            console.log(`Toast: ${message} (${type})`);
        }
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
    if (window.volflowApp?.modules?.websocket?.isConnected) {
        const message = {
            type: 'approve_signal',
            symbol: symbol,
            strategy: strategyType,
            action: 'approve_and_execute',
            timestamp: new Date().toISOString(),
            user: 'web_interface'
        };
        
        window.volflowApp.modules.websocket.send(message);
        console.log('📡 Signal approval sent via WebSocket');
    }
    
    // Update UI to show approval pending
    updateSignalApprovalStatus(symbol, strategyType, 'approved');
    
    // Show success message
    if (window.showToast) {
        window.showToast(`${symbol} signal approved and queued for execution`, 'success');
    }
}

function rejectSignal(symbol, strategyType) {
    console.log(`Rejecting signal for ${symbol} in ${strategyType} strategy`);
    
    // Show confirmation dialog
    if (!confirm(`Are you sure you want to REJECT the ${strategyType.replace('_', ' ')} signal for ${symbol}?`)) {
        return;
    }
    
    // Send rejection via WebSocket
    if (window.volflowApp?.modules?.websocket?.isConnected) {
        const message = {
            type: 'reject_signal',
            symbol: symbol,
            strategy: strategyType,
            action: 'reject',
            timestamp: new Date().toISOString(),
            user: 'web_interface'
        };
        
        window.volflowApp.modules.websocket.send(message);
        console.log('📡 Signal rejection sent via WebSocket');
    }
    
    // Update UI to show rejection
    updateSignalApprovalStatus(symbol, strategyType, 'rejected');
    
    // Show info message
    if (window.showToast) {
        window.showToast(`${symbol} signal rejected`, 'info');
    }
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
