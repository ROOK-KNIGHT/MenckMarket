// Main Application Controller - MenckMarket Options Trading Platform
class VolFlowApp {
    constructor() {
        this.modules = {};
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
    
    async init() {
        console.log('🚀 Initializing VolFlow Options Trading Platform...');
        
        try {
            // Initialize core modules
            await this.initializeModules();
            
            // Set up global event handlers
            this.bindGlobalEvents();
            
            // Load saved configurations
            this.loadSavedConfig();
            
            // Initialize UI components
            this.initializeUI();
            
            // Set up WebSocket data flow between modules
            this.setupModuleIntegration();
            
            console.log('✅ VolFlow application initialized successfully');
            this.showToast('Application initialized successfully', 'success');
            
        } catch (error) {
            console.error('❌ Failed to initialize application:', error);
            this.showToast('Failed to initialize application', 'error');
        }
    }
    
    async initializeModules() {
        console.log('📦 Initializing application modules...');
        
        // Initialize WebSocket Manager first (other modules depend on it)
        this.modules.websocket = new WebSocketManager();
        console.log('✅ WebSocket Manager initialized');
        
        // Initialize Watchlist Manager
        this.modules.watchlist = new WatchlistManager();
        console.log('✅ Watchlist Manager initialized');
        
        // Initialize Positions Manager
        this.modules.positions = new PositionsManager();
        console.log('✅ Positions Manager initialized');
        
        // Initialize Strategy Manager
        this.modules.strategy = new StrategyManager();
        console.log('✅ Strategy Manager initialized');
        
        // Initialize Risk Manager
        this.modules.risk = new RiskManager();
        console.log('✅ Risk Manager initialized');
        
        // Initialize Settings Manager
        this.modules.settings = new SettingsManager();
        console.log('✅ Settings Manager initialized');
        
        // Initialize Analytics Manager
        this.modules.analytics = new AnalyticsManager();
        console.log('✅ Analytics Manager initialized');
        
        // Store references globally for backward compatibility
        window.wsManager = this.modules.websocket;
        window.watchlistManager = this.modules.watchlist;
        window.positionsManager = this.modules.positions;
        window.strategyManager = this.modules.strategy;
        window.riskManager = this.modules.risk;
        window.settingsManager = this.modules.settings;
        window.analyticsManager = this.modules.analytics;
        
        console.log('📦 All modules initialized successfully');
    }
    
    setupModuleIntegration() {
        console.log('🔗 Setting up module integration...');
        
        // Set up WebSocket data flow to modules
        this.modules.websocket.on('onConnect', () => {
            console.log('✅ WebSocket connected - setting up data subscriptions');
            this.showToast('Real-time data connection established', 'success');
            
            // Subscribe to all data types
            this.modules.websocket.subscribe([
                'pml_signals', 'iron_condor_signals', 'divergence_signals',
                'watchlist_data', 'positions', 'account_data', 'market_status', 'trading_statistics'
            ]);
            
            // Stop fallback auto-refresh since WebSocket is active
            this.modules.positions.stopAutoRefresh();
        });
        
        this.modules.websocket.on('onDisconnect', () => {
            console.log('🔌 WebSocket disconnected - enabling fallback mode');
            this.showToast('Real-time connection lost, using fallback mode', 'warning');
            
            // Start auto-refresh as fallback
            this.modules.positions.startAutoRefresh(5);
        });
        
        this.modules.websocket.on('onData', (data) => {
            console.log('📡 Processing real-time data update');
            
            // Route data to appropriate modules
            this.routeWebSocketData(data);
        });
        
        this.modules.websocket.on('onError', (error) => {
            console.error('❌ WebSocket error:', error);
            this.showToast('Real-time data connection error', 'error');
        });
        
        // Set up periodic ping to keep connection alive
        setInterval(() => {
            if (this.modules.websocket.isConnected) {
                this.modules.websocket.ping();
            }
        }, 30000); // Ping every 30 seconds
        
        console.log('🔗 Module integration setup complete');
    }
    
    routeWebSocketData(data) {
        try {
            console.log('📊 Routing WebSocket data with keys:', Object.keys(data));
            
            // Update last data timestamp
            this.lastWebSocketUpdate = new Date();
            
            // Route positions data
            if (data.positions && Array.isArray(data.positions)) {
                console.log('📈 Routing positions data to positions manager');
                this.modules.positions.updateFromWebSocket(data);
            }
            
            // Route account data
            if (data.account_data) {
                console.log('💰 Routing account data to positions manager');
                this.modules.positions.updateFromWebSocket(data);
                
                // Also route to risk manager for risk calculations
                this.modules.risk.updateFromWebSocket(data);
            }
            
            // Route watchlist data
            if (data.watchlist_data && Array.isArray(data.watchlist_data)) {
                console.log('📋 Routing watchlist data to watchlist manager');
                this.modules.watchlist.updateFromWebSocket(data);
            }
            
            // Route strategy signals
            if (data.pml_signals || data.iron_condor_signals || data.divergence_signals) {
                console.log('🎯 Routing strategy signals to strategy manager');
                this.modules.strategy.updateFromWebSocket(data);
            }
            
            // Route market status
            if (data.market_status) {
                console.log('🕐 Market status received');
                // Update global market status indicators
                this.updateMarketStatus(data.market_status);
            }
            
            // Route trading statistics
            if (data.trading_statistics) {
                console.log('📊 Routing trading statistics to analytics manager');
                this.modules.analytics.updateFromWebSocket(data);
            }
            
            // Route risk metrics
            if (data.risk_metrics) {
                console.log('⚠️ Routing risk metrics to risk manager');
                this.modules.risk.updateFromWebSocket(data);
            }
            
            // Route settings updates
            if (data.settings_update || data.login_history || data.notification_test_result) {
                console.log('⚙️ Routing settings data to settings manager');
                this.modules.settings.updateFromWebSocket(data);
            }
            
            // Route trading metrics
            if (data.trading_metrics) {
                console.log('📈 Routing trading metrics to analytics manager');
                this.modules.analytics.updateFromWebSocket(data);
            }
            
            // Update connection status indicator
            this.updateLastUpdateTime();
            
        } catch (error) {
            console.error('❌ Error routing WebSocket data:', error);
            console.error('❌ Error details:', error.message);
            console.error('❌ Data received:', data);
        }
    }
    
    updateMarketStatus(marketStatus) {
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
    
    bindGlobalEvents() {
        console.log('🎯 Binding global event handlers...');
        
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
        
        // Trading control events
        const envToggle = document.getElementById('env-toggle');
        const tradeToggle = document.getElementById('trade-toggle');
        
        // Environment toggle with comprehensive event listeners
        if (envToggle) {
            this.setupToggleEventListeners(envToggle, 'env-toggle', (toggle) => {
                this.handleEnvironmentToggle(toggle.checked);
            });
        }
        
        // Trading toggle with comprehensive event listeners
        if (tradeToggle) {
            this.setupToggleEventListeners(tradeToggle, 'trade-toggle', (toggle) => {
                this.handleTradingToggle(toggle.checked);
            });
        }
        
        // Configuration input events
        const configInputs = document.querySelectorAll('.config-input, .config-select');
        configInputs.forEach(input => {
            if (input) {
                input.addEventListener('change', (e) => this.updateConfig(e.target.id, e.target.value));
                input.addEventListener('input', (e) => this.validateInput(e.target));
            }
        });
        
        console.log('🎯 Global event handlers bound successfully');
    }
    
    setupToggleEventListeners(toggle, toggleId, handlerFunction) {
        console.log(`Setting up toggle listeners for: ${toggleId}`);
        
        // Add event listener to the checkbox input
        toggle.addEventListener('change', (e) => {
            console.log(`Toggle changed: ${toggleId}, checked: ${e.target.checked}`);
            handlerFunction(e.target);
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
    
    initializeUI() {
        console.log('🎨 Initializing UI components...');
        
        // Initialize toggle states
        this.initializeToggleStates();
        
        // Update status display
        this.updateStatusDisplay();
        
        // Initialize navigation
        this.initializeNavigation();
        
        console.log('🎨 UI components initialized');
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
        
        console.log('Toggle states initialized:', {
            envToggle: envToggle?.checked,
            tradeToggle: tradeToggle?.checked,
            envStatus: envStatus?.textContent,
            tradeStatus: tradeStatus?.textContent
        });
    }
    
    initializeNavigation() {
        console.log('🧭 Initializing navigation...');
        
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
        
        console.log('🧭 Navigation initialized');
    }
    
    // Configuration Management
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
    
    // Trading Controls
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
    
    // Placeholder methods for future implementation
    startScreening() {
        console.log('Starting screening process...');
        this.showToast('Screening functionality will be implemented in strategy manager', 'info');
    }
    
    cancelScreening() {
        console.log('Cancelling screening process...');
        this.showToast('Screening cancelled', 'info');
    }
    
    exportResults(format) {
        console.log(`Exporting results in ${format} format...`);
        this.showToast(`Export functionality will be implemented`, 'info');
    }
    
    // Utility Methods
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
    
    // Module access methods
    getModule(name) {
        return this.modules[name];
    }
    
    getAllModules() {
        return this.modules;
    }
}

// Global toast function for backward compatibility
function showToast(message, type = 'info') {
    if (window.volflowApp) {
        window.volflowApp.showToast(message, type);
    } else {
        console.log(`Toast: ${message} (${type})`);
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('🌟 DOM Content Loaded - Starting VolFlow Application...');
    
    // Create global app instance
    window.volflowApp = new VolFlowApp();
    
    // Store global reference for backward compatibility
    window.volflowScreener = window.volflowApp;
    
    // Make showToast globally available
    window.showToast = showToast;
});

// Handle window resize for responsive behavior
window.addEventListener('resize', () => {
    // Add any resize-specific logic here if needed
});

// Handle visibility change to pause/resume operations
document.addEventListener('visibilitychange', () => {
    if (document.hidden && window.volflowApp) {
        // Optionally pause operations when tab is not visible
    }
});
