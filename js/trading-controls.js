/**
 * Trading Controls Manager
 * Handles start/stop trading functionality and emergency controls
 * Connects HTML interface to trading engine backend
 */

class TradingControlsManager {
    constructor() {
        this.isTrading = false;
        this.tradingEngineStatus = 'stopped';
        this.lastStatusCheck = null;
        this.statusCheckInterval = null;
        
        this.init();
    }

    init() {
        console.log('🎛️ Initializing Trading Controls Manager...');
        
        // Bind event listeners
        this.bindEventListeners();
        
        // Start status monitoring
        this.startStatusMonitoring();
        
        // Initial status check
        this.checkTradingStatus();
        
        console.log('✅ Trading Controls Manager initialized');
    }

    bindEventListeners() {
        console.log('🔗 Binding event listeners...');
        
        // Trading toggle
        const tradeToggle = document.getElementById('trade-toggle');
        if (tradeToggle) {
            console.log('✅ Found trade-toggle element');
            tradeToggle.addEventListener('change', (e) => {
                console.log('Toggle slider clicked: trade-', e.target.checked ? 'START' : 'STOP');
                this.handleTradingToggle(e.target.checked);
            });
        } else {
            console.warn('⚠️ trade-toggle element not found');
        }

        // Emergency controls
        const emergencyStop = document.getElementById('emergency-stop');
        if (emergencyStop) {
            console.log('✅ Found emergency-stop element');
            emergencyStop.addEventListener('click', () => {
                this.handleEmergencyStop();
            });
        } else {
            console.warn('⚠️ emergency-stop element not found');
        }

        const closeAllPositions = document.getElementById('close-all-positions');
        if (closeAllPositions) {
            console.log('✅ Found close-all-positions element');
            closeAllPositions.addEventListener('click', () => {
                this.handleCloseAllPositions();
            });
        } else {
            console.warn('⚠️ close-all-positions element not found');
        }

        const pauseTrading = document.getElementById('pause-trading');
        if (pauseTrading) {
            console.log('✅ Found pause-trading element');
            pauseTrading.addEventListener('click', () => {
                this.handlePauseTrading();
            });
        } else {
            console.warn('⚠️ pause-trading element not found');
        }

        // Environment toggle
        const envToggle = document.getElementById('env-toggle');
        if (envToggle) {
            console.log('✅ Found env-toggle element');
            envToggle.addEventListener('change', (e) => {
                this.handleEnvironmentToggle(e.target.checked);
            });
        } else {
            console.warn('⚠️ env-toggle element not found');
        }
        
        console.log('🔗 Event listeners binding complete');
    }

    async handleTradingToggle(isEnabled) {
        console.log(`🎛️ Trading toggle: ${isEnabled ? 'START' : 'STOP'}`);
        
        try {
            // Update UI immediately for responsiveness
            this.updateTradingStatus(isEnabled ? 'starting' : 'stopping');
            
            if (isEnabled) {
                await this.startTrading();
            } else {
                await this.stopTrading();
            }
        } catch (error) {
            console.error('❌ Error handling trading toggle:', error);
            this.showNotification('Error controlling trading engine', 'error');
            
            // Revert toggle state on error
            const tradeToggle = document.getElementById('trade-toggle');
            if (tradeToggle) {
                tradeToggle.checked = !isEnabled;
            }
            this.updateTradingStatus('error');
        }
    }

    async startTrading() {
        console.log('🚀 Starting trading engine via WebSocket...');
        
        try {
            // Check if WebSocket manager is available
            console.log('🔍 DEBUG: Checking WebSocket manager availability...');
            console.log('🔍 DEBUG: window.webSocketManager exists:', !!window.webSocketManager);
            
            if (window.webSocketManager) {
                console.log('🔍 DEBUG: WebSocket manager found, checking connection...');
                const isConnected = window.webSocketManager.isConnected();
                console.log('🔍 DEBUG: WebSocket isConnected():', isConnected);
                
                if (isConnected) {
                    console.log('🔍 DEBUG: Sending start_trading message...');
                    // Send start trading request via WebSocket
                    window.webSocketManager.send({
                        type: 'start_trading',
                        timestamp: new Date().toISOString()
                    });
                    
                    console.log('🔍 DEBUG: Message sent, setting up listener...');
                    // Set up one-time listener for start trading response
                    this.setupStartTradingListener();
                    
                } else {
                    throw new Error('WebSocket is not connected');
                }
            } else {
                throw new Error('WebSocket manager not found');
            }
        } catch (error) {
            console.error('❌ Failed to start trading:', error);
            console.error('❌ Error details:', error.message);
            this.updateTradingStatus('error');
            throw error;
        }
    }

    async stopTrading() {
        console.log('🛑 Stopping trading engine via WebSocket...');
        
        try {
            // Check if WebSocket manager is available
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                // Send stop trading request via WebSocket
                window.webSocketManager.send({
                    type: 'stop_trading',
                    timestamp: new Date().toISOString()
                });
                
                // Set up one-time listener for stop trading response
                this.setupStopTradingListener();
                
            } else {
                throw new Error('WebSocket connection not available');
            }
        } catch (error) {
            console.error('❌ Failed to stop trading:', error);
            this.updateTradingStatus('error');
            throw error;
        }
    }

    async handleEmergencyStop() {
        console.log('🚨 EMERGENCY STOP triggered!');
        
        if (!confirm('⚠️ EMERGENCY STOP\n\nThis will immediately stop all trading and cancel pending orders.\n\nAre you sure you want to proceed?')) {
            return;
        }

        try {
            this.showNotification('Emergency stop initiated...', 'warning');
            
            // Check if WebSocket manager is available
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                // Send emergency stop request via WebSocket
                window.webSocketManager.send({
                    type: 'emergency_stop',
                    timestamp: new Date().toISOString(),
                    reason: 'User initiated emergency stop'
                });
                
                // Set up one-time listener for emergency stop response
                this.setupEmergencyStopListener();
                
            } else {
                throw new Error('WebSocket connection not available for emergency stop');
            }
        } catch (error) {
            console.error('❌ Emergency stop failed:', error);
            this.showNotification('Emergency stop failed: ' + error.message, 'error');
        }
    }

    async handleCloseAllPositions() {
        console.log('🔄 Close all positions triggered');
        
        if (!confirm('⚠️ CLOSE ALL POSITIONS\n\nThis will attempt to close all open positions at market prices.\n\nAre you sure you want to proceed?')) {
            return;
        }

        try {
            this.showNotification('Closing all positions...', 'warning');
            
            const response = await fetch('http://localhost:5001/api/trading/close-all-positions', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    timestamp: new Date().toISOString(),
                    reason: 'User requested close all positions'
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            if (result.success) {
                this.showNotification(`✅ Initiated closing ${result.positions_count || 0} positions`, 'success');
                console.log('✅ Close all positions initiated');
            } else {
                throw new Error(result.error || 'Failed to close positions');
            }
        } catch (error) {
            console.error('❌ Close all positions failed:', error);
            this.showNotification('Failed to close positions: ' + error.message, 'error');
        }
    }

    async handlePauseTrading() {
        console.log('⏸️ Pause trading triggered');
        
        try {
            const response = await fetch('http://localhost:5001/api/trading/pause', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    timestamp: new Date().toISOString()
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            if (result.success) {
                this.tradingEngineStatus = 'paused';
                this.updateTradingStatus('paused');
                this.showNotification('Trading paused - no new positions will be opened', 'warning');
                console.log('⏸️ Trading paused');
            } else {
                throw new Error(result.error || 'Failed to pause trading');
            }
        } catch (error) {
            console.error('❌ Pause trading failed:', error);
            this.showNotification('Failed to pause trading: ' + error.message, 'error');
        }
    }

    handleEnvironmentToggle(isLive) {
        console.log(`🌍 Environment toggle: ${isLive ? 'LIVE' : 'PAPER'}`);
        
        const envStatus = document.getElementById('env-status');
        if (envStatus) {
            envStatus.textContent = isLive ? 'Live' : 'Paper';
            envStatus.className = isLive ? 'toggle-option live' : 'toggle-option paper';
        }

        // Show warning when switching to live
        if (isLive) {
            this.showNotification('⚠️ Switched to LIVE trading environment', 'warning');
        } else {
            this.showNotification('📝 Switched to PAPER trading environment', 'info');
        }
    }

    async checkTradingStatus() {
        try {
            console.log('🔍 Checking trading engine status via WebSocket...');
            
            // Check if WebSocket manager is available
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                // Send trading status request via WebSocket
                window.webSocketManager.send({
                    type: 'get_trading_status',
                    timestamp: new Date().toISOString()
                });
                
                // Set up one-time listener for trading status response
                this.setupTradingStatusListener();
                
            } else {
                console.warn('⚠️ WebSocket connection not available - trading engine may be offline');
                this.updateTradingStatus('engine_offline');
            }
            
        } catch (error) {
            console.error('❌ Failed to check trading status:', error);
            this.updateTradingStatus('connection_error');
        }
    }

    updateTradingStatus(status) {
        const tradeStatus = document.getElementById('trade-status');
        if (!tradeStatus) {
            console.warn('⚠️ trade-status element not found');
            return;
        }

        try {
            // Update status text and styling with more specific states
            switch (status) {
                case 'running':
                    tradeStatus.textContent = 'Running';
                    tradeStatus.className = 'toggle-option running';
                    break;
                case 'stopped':
                    tradeStatus.textContent = 'Stopped';
                    tradeStatus.className = 'toggle-option stopped';
                    break;
                case 'starting':
                    tradeStatus.textContent = 'Starting...';
                    tradeStatus.className = 'toggle-option starting';
                    break;
                case 'stopping':
                    tradeStatus.textContent = 'Stopping...';
                    tradeStatus.className = 'toggle-option stopping';
                    break;
                case 'paused':
                    tradeStatus.textContent = 'Paused';
                    tradeStatus.className = 'toggle-option paused';
                    break;
                case 'emergency_stopped':
                    tradeStatus.textContent = 'Emergency Stop';
                    tradeStatus.className = 'toggle-option emergency';
                    break;
                case 'error':
                    tradeStatus.textContent = 'Error';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'connection_error':
                    tradeStatus.textContent = 'Connection Error';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'engine_not_running':
                    tradeStatus.textContent = 'Engine Not Running';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'engine_offline':
                    tradeStatus.textContent = 'Engine Offline';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'engine_timeout':
                    tradeStatus.textContent = 'Engine Timeout';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'server_error':
                    tradeStatus.textContent = 'Server Error';
                    tradeStatus.className = 'toggle-option error';
                    break;
                case 'active':
                case 'started':
                    tradeStatus.textContent = 'Active';
                    tradeStatus.className = 'toggle-option running';
                    break;
                case 'inactive':
                case 'idle':
                    tradeStatus.textContent = 'Idle';
                    tradeStatus.className = 'toggle-option stopped';
                    break;
                case 'unknown':
                default:
                    tradeStatus.textContent = 'Status Unknown';
                    tradeStatus.className = 'toggle-option unknown';
                    break;
            }
            
            console.log(`📊 Updated trading status to: ${status} (${tradeStatus.textContent})`);
        } catch (error) {
            console.error('❌ Error updating trading status:', error);
        }
    }

    updateTradingToggle(isRunning) {
        const tradeToggle = document.getElementById('trade-toggle');
        if (tradeToggle && tradeToggle.checked !== isRunning) {
            tradeToggle.checked = isRunning;
        }
    }

    updateActiveOrdersCount(count) {
        // Update any UI elements that show active order count
        const activeOrdersElements = document.querySelectorAll('[data-active-orders]');
        activeOrdersElements.forEach(element => {
            element.textContent = count;
        });
    }

    startStatusMonitoring() {
        // Check status every 10 seconds
        this.statusCheckInterval = setInterval(() => {
            this.checkTradingStatus();
        }, 10000);
        
        console.log('📡 Started trading status monitoring (10s interval)');
    }

    stopStatusMonitoring() {
        if (this.statusCheckInterval) {
            clearInterval(this.statusCheckInterval);
            this.statusCheckInterval = null;
            console.log('📡 Stopped trading status monitoring');
        }
    }

    setupStartTradingListener() {
        // Set up listener for start trading response
        const handleStartResponse = (data) => {
            if (data.type === 'trading_start_response') {
                console.log('📊 Start trading response via WebSocket:', data);
                
                if (data.success) {
                    this.isTrading = true;
                    this.tradingEngineStatus = 'running';
                    this.updateTradingStatus('running');
                    this.showNotification('Trading engine started successfully', 'success');
                    console.log('✅ Trading engine started via WebSocket');
                } else {
                    this.isTrading = false;
                    this.tradingEngineStatus = 'error';
                    this.updateTradingStatus('error');
                    this.showNotification(`❌ Failed to start trading: ${data.error}`, 'error');
                    console.error('Failed to start trading via WebSocket:', data.error);
                }
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleStartResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleStartResponse);
        }
    }

    setupStopTradingListener() {
        // Set up listener for stop trading response
        const handleStopResponse = (data) => {
            if (data.type === 'trading_stop_response') {
                console.log('📊 Stop trading response via WebSocket:', data);
                
                if (data.success) {
                    this.isTrading = false;
                    this.tradingEngineStatus = 'stopped';
                    this.updateTradingStatus('stopped');
                    this.showNotification('Trading engine stopped successfully', 'success');
                    console.log('✅ Trading engine stopped via WebSocket');
                } else {
                    this.updateTradingStatus('error');
                    this.showNotification(`❌ Failed to stop trading: ${data.error}`, 'error');
                    console.error('Failed to stop trading via WebSocket:', data.error);
                }
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleStopResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleStopResponse);
        }
    }

    setupEmergencyStopListener() {
        // Set up listener for emergency stop response
        const handleEmergencyResponse = (data) => {
            if (data.type === 'emergency_stop_response') {
                console.log('📊 Emergency stop response via WebSocket:', data);
                
                if (data.success) {
                    this.isTrading = false;
                    this.tradingEngineStatus = 'emergency_stopped';
                    this.updateTradingStatus('emergency_stopped');
                    this.showNotification('🚨 EMERGENCY STOP EXECUTED - All trading halted', 'error');
                    
                    // Force toggle off
                    const tradeToggle = document.getElementById('trade-toggle');
                    if (tradeToggle) {
                        tradeToggle.checked = false;
                    }
                    
                    console.log('🚨 Emergency stop completed via WebSocket');
                } else {
                    this.showNotification(`❌ Emergency stop failed: ${data.error}`, 'error');
                    console.error('Emergency stop failed via WebSocket:', data.error);
                }
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleEmergencyResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleEmergencyResponse);
        }
    }

    setupTradingStatusListener() {
        // Set up listener for trading status response
        const handleStatusResponse = (data) => {
            if (data.type === 'trading_status_response') {
                console.log('📊 Trading status response via WebSocket:', data);
                
                if (data.success) {
                    // Process status similar to HTTP response
                    let engineStatus = 'unknown';
                    let isRunning = false;
                    
                    if (data.running === true) {
                        engineStatus = 'running';
                        isRunning = true;
                    } else if (data.running === false) {
                        engineStatus = data.status === 'paused' ? 'paused' : 'stopped';
                        isRunning = false;
                    } else if (data.status) {
                        engineStatus = data.status;
                        isRunning = ['running', 'active', 'started'].includes(data.status.toLowerCase());
                    }
                    
                    // Update internal state
                    this.isTrading = isRunning;
                    this.tradingEngineStatus = engineStatus;
                    this.lastStatusCheck = new Date();
                    
                    // Update UI
                    this.updateTradingStatus(engineStatus);
                    this.updateTradingToggle(isRunning);
                    
                    // Update additional status info if available
                    if (data.active_orders_count !== undefined) {
                        this.updateActiveOrdersCount(data.active_orders_count);
                    }
                    
                    console.log(`✅ Trading status via WebSocket: ${engineStatus} (running: ${isRunning})`);
                    console.log(`📊 Processed signals: ${data.processed_signals_count}, Active orders: ${data.active_orders_count}`);
                    
                } else {
                    console.error('❌ Trading status error via WebSocket:', data.error);
                    this.updateTradingStatus('error');
                }
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleStatusResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleStatusResponse);
        }
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <span class="notification-message">${message}</span>
                <button class="notification-close">&times;</button>
            </div>
        `;

        // Add to page
        document.body.appendChild(notification);

        // Handle close button
        const closeBtn = notification.querySelector('.notification-close');
        closeBtn.addEventListener('click', () => {
            notification.remove();
        });

        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);

        // Animate in
        setTimeout(() => {
            notification.classList.add('notification-show');
        }, 100);
    }

    // Public methods for external access
    getTradingStatus() {
        return {
            isTrading: this.isTrading,
            status: this.tradingEngineStatus,
            lastCheck: this.lastStatusCheck
        };
    }

    async refreshStatus() {
        await this.checkTradingStatus();
    }

    destroy() {
        this.stopStatusMonitoring();
        console.log('🎛️ Trading Controls Manager destroyed');
    }
}

// Global instance
let tradingControlsManager = null;

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    tradingControlsManager = new TradingControlsManager();
});

// Export for external access
window.TradingControlsManager = TradingControlsManager;
window.tradingControlsManager = tradingControlsManager;
