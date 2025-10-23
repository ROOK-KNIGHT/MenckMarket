// Test if JavaScript is loading
console.log('🔥 APP.JS FILE IS LOADING!');

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
        
        // Initialize modules that exist, skip ones that don't
        const moduleClasses = [
            { name: 'websocket', class: 'WebSocketManager' },
            { name: 'watchlist', class: 'WatchlistManager' },
            { name: 'positions', class: 'PositionsManager' },
            { name: 'strategy', class: 'StrategyManager' },
            { name: 'risk', class: 'RiskManager' },
            { name: 'settings', class: 'SettingsManager' },
            { name: 'analytics', class: 'AnalyticsManager' },
            { name: 'api', class: 'APIManager' },
            { name: 'tradingControls', class: 'TradingControlsManager' }
        ];
        
        moduleClasses.forEach(module => {
            try {
                if (window[module.class]) {
                    this.modules[module.name] = new window[module.class]();
                    console.log(`✅ ${module.class} initialized`);
                    
                    // Store global reference for backward compatibility
                    window[module.name + 'Manager'] = this.modules[module.name];
                } else {
                    console.warn(`⚠️ ${module.class} not found, skipping...`);
                }
            } catch (error) {
                console.error(`❌ Failed to initialize ${module.class}:`, error);
            }
        });
        
        // Store additional global references for backward compatibility
        if (this.modules.websocket) {
            window.wsManager = this.modules.websocket;
            window.websocketManager = this.modules.websocket;
            window.webSocketManager = this.modules.websocket;
        }
        
        // Store strategy manager global reference
        if (this.modules.strategy) {
            window.strategyManager = this.modules.strategy;
        }
        
        // Store risk manager global reference
        if (this.modules.risk) {
            window.riskManager = this.modules.risk;
        }
        
        console.log('📦 Module initialization completed');
    }
    
    setupModuleIntegration() {
        console.log('🔗 Setting up module integration...');
        
        // Only set up WebSocket integration if WebSocket module exists
        if (!this.modules.websocket) {
            console.log('🔗 WebSocket module not available, skipping integration setup');
            return;
        }
        
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
            if (this.modules.websocket && typeof this.modules.websocket.isConnected === 'function' && this.modules.websocket.isConnected()) {
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
            
            // Handle specific watchlist response messages first
            if (data.type === 'watchlist_symbol_added' || 
                data.type === 'watchlist_symbol_removed' || 
                data.type === 'watchlist_refreshed_with_cleanup' ||
                data.type === 'watchlist_error' ||
                data.type === 'watchlist_data' ||
                data.type === 'watchlist_updated') {
                console.log('📋 Routing watchlist response message to watchlist manager:', data.type);
                this.modules.watchlist.updateFromWebSocket(data);
            }
            
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
            
            // Route watchlist data (for regular data stream)
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
        
        // Panel collapse/expand functionality
        this.initializePanelCollapse();
        
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
        
        // Load saved panel states
        this.loadPanelStates();
        
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
                    
                    // Find the section anchor element
                    const sectionAnchor = targetElement.querySelector('.section-anchor');
                    const sectionHeader = targetElement.querySelector('.panel-header') || 
                                        targetElement.querySelector('.panel-title') || 
                                        targetElement;
                    
                    if (sectionAnchor) {
                        // Scroll to the anchor element which is positioned above the header
                        sectionAnchor.scrollIntoView({ 
                            behavior: 'smooth', 
                            block: 'start' 
                        });
                        console.log(`🧭 Scrolling to ${targetSection} using section anchor`);
                    } else {
                        // Fallback to header with scroll margin
                        sectionHeader.style.scrollMarginTop = '100x';
                        sectionHeader.scrollIntoView({ 
                            behavior: 'smooth', 
                            block: 'start' 
                        });
                        console.log(`🧭 Scrolling to ${targetSection} using fallback method`);
                    }
                    
                    // Add visual feedback to the section header
                    sectionHeader.style.transform = 'scale(1.01)';
                    sectionHeader.style.background = 'rgba(102, 126, 234, 0.1)';
                    setTimeout(() => {
                        sectionHeader.style.transform = 'scale(1)';
                        sectionHeader.style.background = '';
                    }, 300);
                }
            });
        });
        
        // Add scroll spy functionality to update active nav item based on scroll position
        this.initializeScrollSpy();
        
        console.log('🧭 Navigation initialized');
    }
    
    initializeScrollSpy() {
        console.log('👁️ Initializing scroll spy...');
        
        const navItems = document.querySelectorAll('.nav-item');
        const sections = document.querySelectorAll('section[id]');
        
        if (sections.length === 0) return;
        
        const observerOptions = {
            root: null,
            rootMargin: '-20% 0px -70% 0px', // Trigger when section is 20% from top
            threshold: 0
        };
        
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const sectionId = entry.target.id;
                    
                    // Update active nav item
                    navItems.forEach(nav => {
                        nav.classList.remove('active');
                        if (nav.getAttribute('data-section') === sectionId) {
                            nav.classList.add('active');
                        }
                    });
                }
            });
        }, observerOptions);
        
        // Observe all sections
        sections.forEach(section => {
            observer.observe(section);
        });
        
        console.log('👁️ Scroll spy initialized for', sections.length, 'sections');
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
        const tradeStatus = document.getElementById('trade-status');
        if (tradeStatus) {
            tradeStatus.textContent = status;
        }
        
        // Update trading status in the interface
        const startBtn = document.getElementById('start-screening');
        if (startBtn) {
            if (isActive) {
                startBtn.classList.add('btn-success');
                startBtn.classList.remove('btn-primary');
            } else {
                startBtn.classList.add('btn-primary');
                startBtn.classList.remove('btn-success');
            }
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
    
    // Panel Collapse/Expand Functionality
    initializePanelCollapse() {
        console.log('📋 Initializing panel collapse functionality...');
        
        // Find all collapse toggle buttons
        const collapseToggles = document.querySelectorAll('.collapse-toggle');
        console.log('📋 Found collapse toggles:', collapseToggles.length);
        
        collapseToggles.forEach((toggle, index) => {
            console.log(`📋 Setting up toggle ${index}:`, toggle);
            toggle.addEventListener('click', (e) => {
                console.log('📋 Collapse toggle clicked!', e.target);
                e.preventDefault();
                e.stopPropagation();
                
                const targetId = toggle.getAttribute('data-target');
                console.log('📋 Target ID:', targetId);
                const panelContent = document.getElementById(targetId);
                console.log('📋 Panel content element:', panelContent);
                
                if (panelContent) {
                    console.log('📋 Calling togglePanel...');
                    this.togglePanel(panelContent, toggle);
                } else {
                    console.error('📋 Panel content not found for ID:', targetId);
                }
            });
        });
        
        // Also allow clicking on panel headers to toggle
        const panelHeaders = document.querySelectorAll('.panel-header[data-panel]');
        
        panelHeaders.forEach(header => {
            header.addEventListener('click', (e) => {
                // Don't trigger if clicking on buttons or other interactive elements
                if (e.target.closest('button') || e.target.closest('.btn')) {
                    return;
                }
                
                const panelName = header.getAttribute('data-panel');
                const panelContent = document.getElementById(`${panelName}-content`);
                const collapseToggle = header.querySelector('.collapse-toggle');
                
                if (panelContent && collapseToggle) {
                    this.togglePanel(panelContent, collapseToggle);
                }
            });
        });
        
        console.log(`📋 Panel collapse initialized for ${collapseToggles.length} panels`);
    }
    
    togglePanel(panelContent, toggleButton) {
        const isExpanded = panelContent.classList.contains('expanded');
        const panel = panelContent.closest('.panel');
        const chevronIcon = toggleButton.querySelector('i');
        
        console.log(`📋 Toggling panel: ${panelContent.id}, currently expanded: ${isExpanded}`);
        
        if (isExpanded) {
            // Collapse the panel
            panelContent.classList.remove('expanded');
            panelContent.classList.add('collapsed');
            toggleButton.classList.add('collapsed');
            
            if (panel) {
                panel.classList.add('collapsed');
            }
            
            // Rotate chevron icon
            if (chevronIcon) {
                chevronIcon.style.transform = 'rotate(-90deg)';
            }
            
            // Add collapsing animation class
            panelContent.classList.add('collapsing');
            setTimeout(() => {
                panelContent.classList.remove('collapsing');
            }, 300);
            
            console.log(`📋 Panel ${panelContent.id} collapsed`);
            
        } else {
            // Expand the panel
            panelContent.classList.remove('collapsed');
            panelContent.classList.add('expanded');
            toggleButton.classList.remove('collapsed');
            
            if (panel) {
                panel.classList.remove('collapsed');
            }
            
            // Reset chevron icon rotation
            if (chevronIcon) {
                chevronIcon.style.transform = 'rotate(0deg)';
            }
            
            // Add expanding animation class
            panelContent.classList.add('expanding');
            setTimeout(() => {
                panelContent.classList.remove('expanding');
            }, 300);
            
            console.log(`📋 Panel ${panelContent.id} expanded`);
        }
        
        // Save panel state to localStorage
        this.savePanelState(panelContent.id, !isExpanded);
    }
    
    savePanelState(panelId, isExpanded) {
        try {
            const panelStates = JSON.parse(localStorage.getItem('volflow-panel-states') || '{}');
            panelStates[panelId] = isExpanded;
            localStorage.setItem('volflow-panel-states', JSON.stringify(panelStates));
        } catch (error) {
            console.warn('Failed to save panel state:', error);
        }
    }
    
    loadPanelStates() {
        try {
            const panelStates = JSON.parse(localStorage.getItem('volflow-panel-states') || '{}');
            
            Object.entries(panelStates).forEach(([panelId, isExpanded]) => {
                const panelContent = document.getElementById(panelId);
                const panel = panelContent?.closest('.panel');
                const toggleButton = panel?.querySelector('.collapse-toggle');
                const chevronIcon = toggleButton?.querySelector('i');
                
                if (panelContent && toggleButton) {
                    if (isExpanded) {
                        panelContent.classList.remove('collapsed');
                        panelContent.classList.add('expanded');
                        toggleButton.classList.remove('collapsed');
                        if (panel) panel.classList.remove('collapsed');
                        if (chevronIcon) chevronIcon.style.transform = 'rotate(0deg)';
                    } else {
                        panelContent.classList.remove('expanded');
                        panelContent.classList.add('collapsed');
                        toggleButton.classList.add('collapsed');
                        if (panel) panel.classList.add('collapsed');
                        if (chevronIcon) chevronIcon.style.transform = 'rotate(-90deg)';
                    }
                }
            });
            
            console.log('📋 Panel states loaded from localStorage');
        } catch (error) {
            console.warn('Failed to load panel states:', error);
        }
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

// Add debugging to see what's happening
console.log('🔍 Setting up DOM event listener...');

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('🌟 DOM Content Loaded - Waiting for templates...');
    
    // Check if templates are loaded, if not wait a bit
    const checkTemplatesLoaded = () => {
        console.log('🔍 Checking if templates are loaded...');
        const positionsPanel = document.getElementById('positions');
        console.log('🔍 Positions panel found:', !!positionsPanel);
        
        if (positionsPanel) {
            console.log('🌟 Templates loaded - Starting VolFlow Application...');
            
            try {
                // Create global app instance
                window.volflowApp = new VolFlowApp();
                console.log('✅ VolFlowApp instance created successfully');
                
                // Store global reference for backward compatibility
                window.volflowScreener = window.volflowApp;
                
                // Make showToast globally available
                window.showToast = showToast;
                
            } catch (error) {
                console.error('❌ Failed to create VolFlowApp instance:', error);
            }
        } else {
            console.log('🌟 Templates not ready yet, waiting...');
            setTimeout(checkTemplatesLoaded, 100);
        }
    };
    
    checkTemplatesLoaded();
});

// Also check if DOM is already loaded
console.log('🔍 Document ready state:', document.readyState);
if (document.readyState === 'loading') {
    console.log('🔍 Document still loading, waiting for DOMContentLoaded...');
} else {
    console.log('🔍 Document already loaded, initializing immediately...');
    // DOM is already loaded, initialize immediately
    setTimeout(() => {
        console.log('🌟 Manual initialization - DOM was already loaded');
        
        const positionsPanel = document.getElementById('positions');
        console.log('🔍 Positions panel found:', !!positionsPanel);
        
        if (positionsPanel) {
            try {
                window.volflowApp = new VolFlowApp();
                console.log('✅ VolFlowApp instance created successfully (manual init)');
                window.volflowScreener = window.volflowApp;
                window.showToast = showToast;
            } catch (error) {
                console.error('❌ Failed to create VolFlowApp instance (manual init):', error);
            }
        }
    }, 100);
}

// Also provide a global initialization function that can be called after templates load
window.initializeVolFlowApp = () => {
    console.log('🌟 Manual initialization - Starting VolFlow Application...');
    
    // Create global app instance
    window.volflowApp = new VolFlowApp();
    
    // Store global reference for backward compatibility
    window.volflowScreener = window.volflowApp;
    
    // Make showToast globally available
    window.showToast = showToast;
};

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
