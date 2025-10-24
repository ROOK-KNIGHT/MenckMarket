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
        this.initializeRiskControls();
        this.loadStrategyConfiguration();
        
        // Add state restoration after a short delay to ensure WebSocket connection
        setTimeout(() => {
            this.restoreStrategyStates();
        }, 2000);
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
        
        // Handle run strategy buttons
        const runStrategyButtons = document.querySelectorAll('.run-strategy-btn');
        runStrategyButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                const strategyId = button.getAttribute('data-strategy');
                this.runStrategy(strategyId, button);
            });
        });
        
        console.log('Strategy manager events bound');
    }
    
    // Run Strategy Functionality
    runStrategy(strategyId, buttonElement) {
        console.log(`🚀 Strategy button clicked: ${strategyId}`);
        
        // Check current state and toggle
        if (buttonElement.classList.contains('running')) {
            // Stop the strategy
            console.log(`🛑 Stopping strategy: ${strategyId}`);
            this.stopStrategy(strategyId, buttonElement);
        } else if (buttonElement.classList.contains('stopped')) {
            // Start the strategy (from stopped state)
            console.log(`▶️ Restarting strategy: ${strategyId}`);
            this.startStrategy(strategyId, buttonElement);
        } else {
            // Start the strategy (from idle state)
            console.log(`▶️ Starting strategy: ${strategyId}`);
            this.startStrategy(strategyId, buttonElement);
        }
    }
    
    startStrategy(strategyId, buttonElement) {
        console.log(`🚀 Starting strategy: ${strategyId}`);
        
        // Update button state to running
        buttonElement.classList.remove('stopped');
        buttonElement.classList.add('running');
        buttonElement.disabled = false; // Keep clickable to allow stopping
        
        // Save running state to localStorage
        this.saveStrategyRunningState(strategyId, true, false, {
            id: `${strategyId}_${Date.now()}`,
            startTime: new Date().toISOString()
        });
        
        // Show toast notification
        this.showToast(`Starting ${strategyId} strategy...`, 'info');
        
        // Send start strategy command via WebSocket
        this.sendRunStrategyCommand(strategyId, buttonElement, 'start');
    }
    
    stopStrategy(strategyId, buttonElement) {
        console.log(`🛑 Stopping strategy: ${strategyId}`);
        
        // Update button state to stopped
        buttonElement.classList.remove('running');
        buttonElement.classList.add('stopped');
        buttonElement.disabled = false; // Keep clickable to allow restarting
        
        // Save stopped state to localStorage
        this.saveStrategyRunningState(strategyId, false, true);
        
        // Show toast notification
        this.showToast(`Stopping ${strategyId} strategy...`, 'warning');
        
        // Send stop strategy command via WebSocket
        this.sendRunStrategyCommand(strategyId, buttonElement, 'stop');
    }
    
    sendRunStrategyCommand(strategyId, buttonElement, action = 'start') {
        console.log(`📡 Sending ${action} strategy command for: ${strategyId}`);
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, running strategy locally');
                this.runStrategyLocally(strategyId, buttonElement, action);
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: action === 'stop' ? 'stop_strategy' : 'run_strategy',
                strategy_id: backendStrategyId,
                action: action,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log(`📨 Sending ${action} strategy WebSocket message:`, message);
            wsManager.ws.send(JSON.stringify(message));
            console.log(`✅ ${action} strategy WebSocket message sent successfully`);
            
            // Set timeout to reset button if no response
            setTimeout(() => {
                if (action === 'start' && buttonElement.classList.contains('running')) {
                    this.resetRunButton(buttonElement, strategyId);
                    this.showToast(`${strategyId} strategy execution timeout`, 'warning');
                } else if (action === 'stop' && buttonElement.classList.contains('stopped')) {
                    // Reset from stopped state if no response
                    buttonElement.classList.remove('stopped');
                    this.showToast(`${strategyId} strategy stop timeout`, 'warning');
                }
            }, 30000); // 30 second timeout
            
        } catch (error) {
            console.error(`❌ Error sending ${action} strategy command:`, error);
            this.runStrategyLocally(strategyId, buttonElement, action);
        }
    }
    
    runStrategyLocally(strategyId, buttonElement, action = 'start') {
        console.log(`💻 ${action === 'stop' ? 'Stopping' : 'Running'} strategy locally: ${strategyId}`);
        
        if (action === 'stop') {
            this.stopStrategyProcess(strategyId, buttonElement);
            return;
        }
        
        // Map strategy ID to Python script
        const strategyScripts = {
            'iron-condor': 'iron_condor_strategy.py',
            'divergence': 'divergence_strategy_multi_timeframe.py'
        };
        
        const scriptName = strategyScripts[strategyId];
        
        if (!scriptName) {
            console.error(`❌ No script found for strategy: ${strategyId}`);
            this.showToast(`No script configured for ${strategyId}`, 'error');
            this.resetRunButton(buttonElement, strategyId);
            return;
        }
        
        // Execute the actual Python script
        this.executeStrategyScript(strategyId, scriptName, buttonElement);
    }
    
    executeStrategyScript(strategyId, scriptName, buttonElement) {
        console.log(`🐍 Executing Python script: ${scriptName}`);
        this.showToast(`Executing ${scriptName}...`, 'info');
        
        // For divergence strategy, we'll execute it directly
        if (strategyId === 'divergence') {
            this.executeDivergenceStrategy(buttonElement);
        } else if (strategyId === 'pml') {
            // PML button now controls exceedance strategy
            this.executeExceedanceStrategy(buttonElement);
        } else {
            // For other strategies, show not implemented message
            this.showToast(`${strategyId} strategy execution not yet implemented`, 'warning');
            setTimeout(() => {
                this.resetRunButton(buttonElement, strategyId);
            }, 2000);
        }
    }
    
    executeDivergenceStrategy(buttonElement) {
        console.log(`🎯 Starting divergence strategy execution...`);
        
        // Store the process reference for this strategy
        if (!this.runningProcesses) {
            this.runningProcesses = {};
        }
        
        // Create a unique execution ID
        const executionId = `divergence_${Date.now()}`;
        this.runningProcesses['divergence'] = {
            id: executionId,
            startTime: new Date(),
            button: buttonElement
        };
        
        // Send execution request to backend via WebSocket
        this.sendDivergenceExecutionRequest(executionId, buttonElement);
    }
    
    sendDivergenceExecutionRequest(executionId, buttonElement) {
        console.log(`📡 Sending divergence execution request: ${executionId}`);
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, cannot execute strategy');
                this.showToast('WebSocket connection required for strategy execution', 'error');
                this.resetRunButton(buttonElement, 'divergence');
                return;
            }
            
            const message = {
                type: 'execute_python_script',
                script_name: 'divergence_strategy_multi_timeframe.py',
                strategy_id: 'divergence',
                execution_id: executionId,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending script execution WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Script execution request sent successfully');
            
            // Set timeout for execution
            setTimeout(() => {
                if (this.runningProcesses && this.runningProcesses['divergence'] && 
                    this.runningProcesses['divergence'].id === executionId) {
                    console.log('⏰ Divergence strategy execution timeout');
                    this.resetRunButton(buttonElement, 'divergence');
                    this.showToast('Divergence strategy execution timeout', 'warning');
                    delete this.runningProcesses['divergence'];
                }
            }, 120000); // 2 minute timeout for strategy execution
            
        } catch (error) {
            console.error('❌ Error sending divergence execution request:', error);
            this.showToast('Error starting divergence strategy', 'error');
            this.resetRunButton(buttonElement, 'divergence');
        }
    }
    
    executeExceedanceStrategy(buttonElement) {
        console.log(`🎯 Starting exceedance strategy execution...`);
        
        // Store the process reference for this strategy
        if (!this.runningProcesses) {
            this.runningProcesses = {};
        }
        
        // Create a unique execution ID
        const executionId = `exceedance_${Date.now()}`;
        this.runningProcesses['pml'] = {
            id: executionId,
            startTime: new Date(),
            button: buttonElement
        };
        
        // Send execution request to backend via WebSocket
        this.sendExceedanceExecutionRequest(executionId, buttonElement);
    }
    
    sendExceedanceExecutionRequest(executionId, buttonElement) {
        console.log(`📡 Sending exceedance execution request: ${executionId}`);
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, cannot execute strategy');
                this.showToast('WebSocket connection required for strategy execution', 'error');
                this.resetRunButton(buttonElement, 'pml');
                return;
            }
            
            const message = {
                type: 'execute_python_script',
                script_name: 'exceedance_trading_engine.py',
                strategy_id: 'pml',
                execution_id: executionId,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending exceedance script execution WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Exceedance script execution request sent successfully');
            
            // Set timeout for execution
            setTimeout(() => {
                if (this.runningProcesses && this.runningProcesses['pml'] && 
                    this.runningProcesses['pml'].id === executionId) {
                    console.log('⏰ Exceedance strategy execution timeout');
                    this.resetRunButton(buttonElement, 'pml');
                    this.showToast('Exceedance strategy execution timeout', 'warning');
                    delete this.runningProcesses['pml'];
                }
            }, 120000); // 2 minute timeout for strategy execution
            
        } catch (error) {
            console.error('❌ Error sending exceedance execution request:', error);
            this.showToast('Error starting exceedance strategy', 'error');
            this.resetRunButton(buttonElement, 'pml');
        }
    }
    
    stopStrategyProcess(strategyId, buttonElement) {
        console.log(`🛑 Stopping strategy process: ${strategyId}`);
        
        if (!this.runningProcesses || !this.runningProcesses[strategyId]) {
            console.log(`⚠️ No running process found for ${strategyId}`);
            buttonElement.classList.remove('stopped');
            this.showToast(`No running ${strategyId} process to stop`, 'warning');
            return;
        }
        
        const processInfo = this.runningProcesses[strategyId];
        this.showToast(`Stopping ${strategyId} strategy...`, 'warning');
        
        // Send stop request via WebSocket
        try {
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (wsManager && wsManager.ws && wsManager.ws.readyState === WebSocket.OPEN) {
                const message = {
                    type: 'stop_python_script',
                    strategy_id: strategyId,
                    execution_id: processInfo.id,
                    timestamp: new Date().toISOString(),
                    source: 'strategy_manager'
                };
                
                console.log('📨 Sending stop script WebSocket message:', message);
                wsManager.ws.send(JSON.stringify(message));
                console.log('✅ Stop script request sent successfully');
            }
        } catch (error) {
            console.error('❌ Error sending stop request:', error);
        }
        
        // Clean up process tracking
        setTimeout(() => {
            if (this.runningProcesses && this.runningProcesses[strategyId]) {
                delete this.runningProcesses[strategyId];
            }
            buttonElement.classList.remove('stopped');
            this.showToast(`${strategyId} strategy stopped`, 'success');
        }, 2000);
    }
    
    resetRunButton(buttonElement, strategyId) {
        console.log(`🔄 Resetting run button for: ${strategyId}`);
        
        buttonElement.classList.remove('running');
        buttonElement.disabled = false;
    }
    
    // Handle WebSocket responses for run strategy
    handleRunStrategyResponse(data) {
        if (data.type === 'strategy_run_started') {
            console.log(`✅ Strategy ${data.strategy_id} started successfully`);
            this.showToast(`${data.strategy_id} strategy started`, 'success');
        } else if (data.type === 'strategy_run_completed') {
            console.log(`✅ Strategy ${data.strategy_id} completed successfully`);
            const frontendStrategyId = this.mapBackendStrategyIdToFrontend(data.strategy_id);
            const button = document.querySelector(`#run-${frontendStrategyId}-btn`);
            if (button) {
                this.resetRunButton(button, frontendStrategyId);
            }
            this.showToast(`${data.strategy_id} strategy completed`, 'success');
        } else if (data.type === 'strategy_run_error') {
            console.error(`❌ Strategy ${data.strategy_id} error:`, data.error);
            const frontendStrategyId = this.mapBackendStrategyIdToFrontend(data.strategy_id);
            const button = document.querySelector(`#run-${frontendStrategyId}-btn`);
            if (button) {
                this.resetRunButton(button, frontendStrategyId);
            }
            this.showToast(`${data.strategy_id} strategy error: ${data.error}`, 'error');
        }
    }
    
    // Strategy Controls Functions - Keep for UI state management
    initializeStrategyControls() {
        console.log('Initializing strategy controls...');
        
        // Get all strategy control toggles
        const strategyToggles = [
            'iron_condor_auto_execute',
            'pml_auto_execute',
            'divergence_auto_execute',
            'iron_condor_auto_timer',
            'pml_auto_timer',
            'divergence_auto_timer'
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
        
        // Find the parent toggle switch container and make it clickable
        const toggleSwitch = toggle.closest('.toggle-switch');
        if (toggleSwitch) {
            console.log(`✅ Found toggle switch container for: ${toggleId}`);
            
            // Remove any existing event listeners to prevent duplicates
            const existingHandler = toggleSwitch._clickHandler;
            if (existingHandler) {
                toggleSwitch.removeEventListener('click', existingHandler);
            }
            
            // Create new click handler
            const clickHandler = (e) => {
                // Prevent the event if it's already on the checkbox input
                if (e.target === toggle) {
                    console.log(`Direct checkbox click for: ${toggleId}`);
                    return;
                }
                
                e.preventDefault();
                e.stopPropagation();
                console.log(`🔥 Toggle switch clicked for: ${toggleId}, current state: ${toggle.checked}`);
                
                // Toggle the checkbox state
                toggle.checked = !toggle.checked;
                
                // Trigger the change event
                const changeEvent = new Event('change', { bubbles: true });
                toggle.dispatchEvent(changeEvent);
            };
            
            // Store reference to handler for cleanup
            toggleSwitch._clickHandler = clickHandler;
            
            // Add click handler to the entire toggle switch container
            toggleSwitch.addEventListener('click', clickHandler);
            
            // Also add click handler to the slider for better coverage
            const slider = toggleSwitch.querySelector('.toggle-slider');
            if (slider) {
                slider.addEventListener('click', clickHandler);
                console.log(`✅ Added click handler to slider for: ${toggleId}`);
            }
            
            console.log(`✅ Toggle switch click handler attached for: ${toggleId}`);
        } else {
            console.warn(`⚠️ Toggle switch container not found for: ${toggleId}`);
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
        
        // No mutual exclusivity needed since manual approval toggles are removed
        
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
                type: 'save_trading_config',
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
                type: 'get_trading_config',
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
                document.getElementById('iron_condor_auto_execute').checked = config.iron_condor.auto_approve;
            }
            if (document.getElementById('iron_condor_auto_timer')) {
                document.getElementById('iron_condor_auto_timer').checked = config.iron_condor.auto_timer;
            }
            
            // Apply running state if available
            if (config.iron_condor.running_state) {
                this.applyRunningStateFromConfig('iron-condor', config.iron_condor.running_state);
            }
        }
        
        if (config.pml) {
            if (document.getElementById('pml_auto_execute')) {
                document.getElementById('pml_auto_execute').checked = config.pml.auto_approve;
            }
            if (document.getElementById('pml_auto_timer')) {
                document.getElementById('pml_auto_timer').checked = config.pml.auto_timer;
            }
            
            // Apply running state if available
            if (config.pml.running_state) {
                this.applyRunningStateFromConfig('pml', config.pml.running_state);
            }
        }
        
        if (config.divergence) {
            if (document.getElementById('divergence_auto_execute')) {
                document.getElementById('divergence_auto_execute').checked = config.divergence.auto_approve;
            }
            if (document.getElementById('divergence_auto_timer')) {
                document.getElementById('divergence_auto_timer').checked = config.divergence.auto_timer;
            }
            
            // Apply running state if available
            if (config.divergence.running_state) {
                this.applyRunningStateFromConfig('divergence', config.divergence.running_state);
            }
        }
        
        // Update strategy control states
        this.controlStates = {
            iron_condor_auto_execute: config.iron_condor?.auto_approve || false,
            iron_condor_auto_timer: config.iron_condor?.auto_timer || false,
            pml_auto_execute: config.pml?.auto_approve || false,
            pml_auto_timer: config.pml?.auto_timer || false,
            divergence_auto_execute: config.divergence?.auto_approve || false,
            divergence_auto_timer: config.divergence?.auto_timer || false
        };
        
        console.log('✅ Strategy configuration applied successfully');
        console.log('🔧 Updated control states:', this.controlStates);
    }
    
    // Apply running state from WebSocket configuration (authoritative source)
    applyRunningStateFromConfig(strategyId, runningState) {
        console.log(`🔧 Applying running state from config for ${strategyId}:`, runningState);
        
        const button = document.querySelector(`#run-${strategyId}-btn`);
        if (!button) {
            console.warn(`⚠️ Run button not found for strategy: ${strategyId}`);
            return;
        }
        
        // Clear existing states
        button.classList.remove('running', 'stopped');
        
        // Apply the authoritative state from backend
        if (runningState.is_running === true) {
            button.classList.add('running');
            console.log(`✅ Applied running state for ${strategyId}: RUNNING`);
            
            // Save to localStorage to maintain consistency
            this.saveStrategyRunningState(strategyId, true, false, {
                id: `${strategyId}_config_restore`,
                startTime: new Date().toISOString()
            });
            
        } else if (runningState.is_running === false) {
            // Strategy is not running - could be idle or stopped
            // We'll treat false as idle state unless we add more granular states later
            console.log(`✅ Applied running state for ${strategyId}: IDLE`);
            
            // Clear any saved running state
            this.clearStrategyRunningState(strategyId);
        }
        
        button.disabled = false; // Ensure button remains clickable
    }
    
    // Handle WebSocket responses for strategy configuration
    handleWebSocketMessage(data) {
        if (data.type === 'trading_config_data') {
            console.log('📡 Received trading config from WebSocket server:', data.config);
            // Extract strategies section from the full config
            if (data.config && data.config.strategies) {
                this.applyStrategyConfiguration(data.config.strategies);
                // Also save to localStorage for offline use
                localStorage.setItem('volflow-strategy-config', JSON.stringify(data.config.strategies));
                
                // Load strategy watchlists from the config
                this.loadStrategyWatchlistsFromConfig(data.config.strategies);
            }
        } else if (data.type === 'trading_config_saved') {
            console.log('✅ Trading config saved successfully on server');
            this.showToast('Strategy configuration saved', 'success');
        } else if (data.type === 'trading_config_updated') {
            console.log('📡 Trading config updated from another client:', data.config);
            // Extract strategies section from the full config
            if (data.config && data.config.strategies) {
                this.applyStrategyConfiguration(data.config.strategies);
                this.loadStrategyWatchlistsFromConfig(data.config.strategies);
            }
        } else if (data.type === 'trading_config_error') {
            console.error('❌ Trading config error from server:', data.error);
            this.showToast('Error loading strategy configuration', 'error');
        } else if (data.type === 'risk_settings_saved') {
            console.log('✅ Risk settings saved successfully on server');
            this.showToast('Risk settings saved successfully', 'success');
        } else if (data.type === 'risk_settings_error') {
            console.error('❌ Risk settings error from server:', data.error);
            this.showToast('Error saving risk settings', 'error');
        } else if (data.type === 'strategy_watchlist_saved') {
            console.log('✅ Strategy watchlist saved successfully on server');
            this.showToast(`Watchlist saved for ${data.strategy_id}`, 'success');
        } else if (data.type === 'strategy_watchlist_symbol_added') {
            console.log('✅ Symbol added to strategy watchlist successfully');
            this.showToast(`Added ${data.symbol} to ${data.strategy_id} watchlist`, 'success');
            // Map backend strategy ID to frontend strategy ID for display update
            const frontendStrategyId = this.mapBackendStrategyIdToFrontend(data.strategy_id);
            this.updateStrategyWatchlistDisplay(frontendStrategyId, data.symbols);
        } else if (data.type === 'strategy_watchlist_symbol_removed') {
            console.log('✅ Symbol removed from strategy watchlist successfully');
            this.showToast(`Removed ${data.symbol} from ${data.strategy_id} watchlist`, 'success');
            // Map backend strategy ID to frontend strategy ID for display update
            const frontendStrategyId = this.mapBackendStrategyIdToFrontend(data.strategy_id);
            this.updateStrategyWatchlistDisplay(frontendStrategyId, data.symbols);
        } else if (data.type === 'strategy_watchlist_error') {
            console.error('❌ Strategy watchlist error from server:', data.error);
            this.showToast('Error with strategy watchlist operation', 'error');
        } else if (data.type === 'python_script_started') {
            console.log('✅ Python script started successfully:', data.script_name);
            this.showToast(`${data.strategy_id} strategy started successfully`, 'success');
        } else if (data.type === 'python_script_completed') {
            console.log('✅ Python script completed successfully:', data.script_name);
            this.handleScriptCompletion(data);
        } else if (data.type === 'python_script_error') {
            console.error('❌ Python script error:', data.error);
            this.handleScriptError(data);
        } else if (data.type === 'python_script_stopped') {
            console.log('✅ Python script stopped successfully:', data.script_name);
            this.handleScriptStopped(data);
        } else if (data.type === 'timing_settings_saved') {
            console.log('✅ Timing settings saved successfully on server');
            this.showToast(`Timing settings saved for ${data.strategy_id}`, 'success');
        } else if (data.type === 'timing_settings_data') {
            console.log('📡 Received timing settings from WebSocket server:', data.settings);
            this.applyTimingSettings({[data.strategy_id]: data.settings});
        } else if (data.type === 'timing_settings_error') {
            console.error('❌ Timing settings error from server:', data.error);
            this.showToast('Error with timing settings operation', 'error');
        }
    }
    
    // Handle script execution completion
    handleScriptCompletion(data) {
        console.log('✅ Script execution completed:', data);
        
        const strategyId = data.strategy_id;
        const executionId = data.execution_id;
        
        // Clean up process tracking
        if (this.runningProcesses && this.runningProcesses[strategyId] && 
            this.runningProcesses[strategyId].id === executionId) {
            
            const processInfo = this.runningProcesses[strategyId];
            const button = processInfo.button;
            
            // Reset button state
            this.resetRunButton(button, strategyId);
            
            // Clean up process reference
            delete this.runningProcesses[strategyId];
            
            // Show success message
            this.showToast(`${strategyId} strategy completed successfully`, 'success');
            
            // Log execution time
            const executionTime = new Date() - processInfo.startTime;
            console.log(`⏱️ ${strategyId} strategy execution time: ${executionTime}ms`);
        }
    }
    
    // Handle script execution error
    handleScriptError(data) {
        console.error('❌ Script execution error:', data);
        
        const strategyId = data.strategy_id;
        const executionId = data.execution_id;
        const error = data.error || 'Unknown error';
        
        // Clean up process tracking
        if (this.runningProcesses && this.runningProcesses[strategyId] && 
            this.runningProcesses[strategyId].id === executionId) {
            
            const processInfo = this.runningProcesses[strategyId];
            const button = processInfo.button;
            
            // Reset button state
            this.resetRunButton(button, strategyId);
            
            // Clean up process reference
            delete this.runningProcesses[strategyId];
        }
        
        // Show error message
        this.showToast(`${strategyId} strategy error: ${error}`, 'error');
    }
    
    // Handle script stopped
    handleScriptStopped(data) {
        console.log('✅ Script stopped successfully:', data);
        
        const strategyId = data.strategy_id;
        const executionId = data.execution_id;
        
        // Clean up process tracking
        if (this.runningProcesses && this.runningProcesses[strategyId] && 
            this.runningProcesses[strategyId].id === executionId) {
            
            const processInfo = this.runningProcesses[strategyId];
            const button = processInfo.button;
            
            // Remove stopped state and reset to idle
            button.classList.remove('stopped');
            button.classList.remove('running');
            button.disabled = false;
            
            // Clean up process reference
            delete this.runningProcesses[strategyId];
        }
        
        // Show success message
        this.showToast(`${strategyId} strategy stopped successfully`, 'success');
    }
    
    getStrategyConfiguration() {
        return {
            strategies: {
                iron_condor: {
                    auto_approve: this.controlStates?.iron_condor_auto_execute || false,
                    auto_timer: this.controlStates?.iron_condor_auto_timer || false
                },
                pml: {
                    auto_approve: this.controlStates?.pml_auto_execute || false,
                    auto_timer: this.controlStates?.pml_auto_timer || false
                },
                divergence: {
                    auto_approve: this.controlStates?.divergence_auto_execute || false,
                    auto_timer: this.controlStates?.divergence_auto_timer || false
                }
            }
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
            this.updateStrategyStatus(statusElement, 'NO_SIGNAL');
            this.showStrategyPlaceholder(signalsElement, 'No active signals');
        } else {
            const primarySignal = this.getPrimarySignal(activeSignals);
            console.log(`🏆 Primary signal for ${strategyType}:`, primarySignal.signal_type, 'for', primarySignal.symbol);
            this.updateStrategyStatus(statusElement, primarySignal.signal_type);
            this.showStrategySignals(signalsElement, activeSignals.slice(0, 3));
        }
    }
    
    updateStrategyStatus(statusElement, signalType) {
        const indicator = statusElement.querySelector('.status-indicator');
        const text = statusElement.querySelector('.status-text');
        
        // Remove the signal type text to clean up the display
        if (text) {
            text.textContent = '';
        }
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
        // Check if this is an exceedence signal (PML) or traditional signal
        const isExceedenceSignal = signal.hasOwnProperty('current_price') && signal.hasOwnProperty('position_in_range');
        
        if (isExceedenceSignal) {
            // Handle exceedence signal structure (PML)
            return `
                <div class="signal-header">
                    <div class="signal-symbol">${signal.symbol}</div>
                    <div class="signal-type">${signal.signal_type}</div>
                </div>
                <div class="signal-details">
                    <div class="signal-detail">
                        <div class="signal-detail-label">Entry Reason</div>
                        <div class="signal-detail-value">${this.truncateText(signal.entry_reason, 30)}</div>
                    </div>
                    <div class="signal-detail">
                        <div class="signal-detail-label">Current Price</div>
                        <div class="signal-detail-value">$${parseFloat(signal.current_price || 0).toFixed(2)}</div>
                    </div>
                    <div class="signal-detail">
                        <div class="signal-detail-label">Position in Range</div>
                        <div class="signal-detail-value">${parseFloat(signal.position_in_range || 0).toFixed(1)}%</div>
                    </div>
                    <div class="signal-detail">
                        <div class="signal-detail-label">Low Exceedance</div>
                        <div class="signal-detail-value">${parseFloat(signal.low_exceedance || 0).toFixed(3)}</div>
                    </div>
                    <div class="signal-detail">
                        <div class="signal-detail-label">Market Condition</div>
                        <div class="signal-detail-value">${signal.market_condition || 'N/A'}</div>
                    </div>
                    <div class="signal-detail">
                        <div class="signal-detail-label">Position Size</div>
                        <div class="signal-detail-value">${signal.position_size || 'N/A'}</div>
                    </div>
                </div>
                <div class="signal-status">
                    <div class="signal-status-item ${signal.has_trade_signal ? 'active' : 'inactive'}">
                        <i class="fas fa-chart-line"></i>
                        <span>Trade Signal</span>
                    </div>
                    <div class="signal-status-item ${signal.auto_approve ? 'active' : 'inactive'}">
                        <i class="fas fa-check-circle"></i>
                        <span>Auto Approve</span>
                    </div>
                </div>
            `;
        } else {
            // Handle traditional signal structure (Iron Condor, Divergence)
            const confidencePercent = Math.round((signal.confidence || 0) * 100);
            
            return `
                <div class="signal-header">
                    <div class="signal-symbol">${signal.symbol}</div>
                    <div class="signal-type">${signal.signal_type}</div>
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
    
    // Initialize risk controls
    initializeRiskControls() {
        console.log('Initializing risk controls...');
        
        // Initialize risk controls as collapsed by default
        const riskGrids = document.querySelectorAll('.risk-controls-grid');
        riskGrids.forEach(grid => {
            grid.style.display = 'none';
        });
        
        const toggleButtons = document.querySelectorAll('.risk-toggle-btn');
        toggleButtons.forEach(button => {
            button.classList.add('collapsed');
            // Add event listener instead of relying on onclick
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.toggleRiskControls(strategyId);
                }
            });
        });
        
        const riskContainers = document.querySelectorAll('.strategy-risk-controls');
        riskContainers.forEach(container => {
            container.classList.add('collapsed');
        });
        
        // Add event listeners for risk save buttons
        const riskSaveButtons = document.querySelectorAll('.risk-save-btn');
        riskSaveButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.saveRiskSettings(strategyId);
                }
            });
        });
        
        // Initialize watchlist controls as collapsed by default
        const watchlistContents = document.querySelectorAll('.watchlist-content');
        watchlistContents.forEach(content => {
            content.style.display = 'none';
        });
        
        const watchlistToggleButtons = document.querySelectorAll('.watchlist-toggle-btn');
        watchlistToggleButtons.forEach(button => {
            button.classList.add('collapsed');
            // Add event listener instead of relying on onclick
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.toggleStrategyWatchlist(strategyId);
                }
            });
        });
        
        const watchlistContainers = document.querySelectorAll('.strategy-watchlist');
        watchlistContainers.forEach(container => {
            container.classList.add('collapsed');
        });
        
        // Add event listeners for watchlist add buttons
        const watchlistAddButtons = document.querySelectorAll('.watchlist-add-btn');
        watchlistAddButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.addToStrategyWatchlist(strategyId);
                }
            });
        });
        
        // Add event listeners for remove symbol buttons
        const removeSymbolButtons = document.querySelectorAll('.remove-symbol-btn');
        removeSymbolButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const symbolText = button.closest('.watchlist-symbol-item')?.querySelector('.symbol-text')?.textContent;
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId && symbolText) {
                    this.removeFromStrategyWatchlist(strategyId, symbolText);
                }
            });
        });
        
        // Add event listeners for timing save buttons
        const timingSaveButtons = document.querySelectorAll('.timing-save-btn');
        timingSaveButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.saveTimingSettings(strategyId);
                }
            });
        });
        
        // Add event listeners for timing toggle buttons
        const timingToggleButtons = document.querySelectorAll('.timing-toggle-btn');
        timingToggleButtons.forEach(button => {
            button.classList.add('collapsed');
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const strategyCard = button.closest('.strategy-card');
                const strategyId = strategyCard ? strategyCard.id.replace('-card', '') : null;
                if (strategyId) {
                    this.toggleTimingControls(strategyId);
                }
            });
        });
        
        // Initialize timing controls as collapsed by default
        const timingContainers = document.querySelectorAll('.strategy-timing-controls');
        timingContainers.forEach(container => {
            container.classList.add('collapsed');
        });
        
        const timingGrids = document.querySelectorAll('.timing-controls-grid');
        timingGrids.forEach(grid => {
            grid.style.display = 'none';
        });
        
        console.log('Risk controls, watchlist controls, and timing controls initialized with event listeners');
    }
    
    // Risk Controls Toggle Functionality
    toggleRiskControls(strategyId) {
        // Prevent rapid double-clicks with debouncing
        const now = Date.now();
        const lastToggle = this.lastRiskToggle || {};
        
        if (lastToggle[strategyId] && (now - lastToggle[strategyId]) < 300) {
            console.log(`⚠️ Ignoring rapid toggle for ${strategyId} risk controls (debounced)`);
            return;
        }
        
        this.lastRiskToggle = this.lastRiskToggle || {};
        this.lastRiskToggle[strategyId] = now;
        
        console.log(`Toggling risk controls for strategy: ${strategyId}`);
        
        const button = document.querySelector(`#${strategyId}-card .risk-toggle-btn`);
        const grid = document.querySelector(`#${strategyId}-card .risk-controls-grid`);
        const container = document.querySelector(`#${strategyId}-card .strategy-risk-controls`);
        const chevronIcon = button?.querySelector('i');
        
        console.log(`🔍 Debug - Button found: ${!!button}`);
        console.log(`🔍 Debug - Grid found: ${!!grid}`);
        console.log(`🔍 Debug - Container found: ${!!container}`);
        
        if (grid && button && container) {
            // Use container class as the primary state indicator (more reliable)
            const isCurrentlyCollapsed = container.classList.contains('collapsed');
            
            console.log(`🔍 Debug - Current state collapsed: ${isCurrentlyCollapsed}`);
            console.log(`🔍 Debug - Grid display: ${grid.style.display}`);
            console.log(`🔍 Debug - Container classes: ${container.className}`);
            
            if (isCurrentlyCollapsed) {
                // Expand - Remove collapsed class only, let CSS handle display
                container.classList.remove('collapsed');
                button.classList.remove('collapsed');
                
                // Rotate chevron icon to point down
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(0deg)';
                }
                
                console.log(`✅ Expanded risk controls for ${strategyId}`);
            } else {
                // Collapse - Add collapsed class only, let CSS handle display
                container.classList.add('collapsed');
                button.classList.add('collapsed');
                
                // Rotate chevron icon to point right
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(-90deg)';
                }
                
                console.log(`✅ Collapsed risk controls for ${strategyId}`);
            }
        } else {
            console.error(`❌ Could not find risk control elements for strategy: ${strategyId}`);
            console.error(`❌ Button: ${!!button}, Grid: ${!!grid}, Container: ${!!container}`);
        }
    }
    
    // Risk Settings Save Functionality
    saveRiskSettings(strategyId) {
        console.log(`Saving risk settings for strategy: ${strategyId}`);
        
        try {
            // Get risk control values for the strategy
            const riskSettings = this.getRiskSettingsForStrategy(strategyId);
            
            if (!riskSettings) {
                console.error(`No risk settings found for strategy: ${strategyId}`);
                this.showToast('Error: Could not find risk settings', 'error');
                return;
            }
            
            // Save to localStorage
            const allRiskSettings = JSON.parse(localStorage.getItem('volflow-risk-settings') || '{}');
            allRiskSettings[strategyId] = riskSettings;
            localStorage.setItem('volflow-risk-settings', JSON.stringify(allRiskSettings));
            
            // Send to WebSocket server if available
            this.sendRiskSettingsToWebSocket(strategyId, riskSettings);
            
            // Show success feedback
            this.showToast(`Risk settings saved for ${strategyId}`, 'success');
            
            console.log(`Risk settings saved for ${strategyId}:`, riskSettings);
            
        } catch (error) {
            console.error(`Error saving risk settings for ${strategyId}:`, error);
            this.showToast('Error saving risk settings', 'error');
        }
    }
    
    // Get risk settings for a specific strategy
    getRiskSettingsForStrategy(strategyId) {
        const strategyCard = document.querySelector(`#${strategyId}-card`);
        if (!strategyCard) return null;
        
        const settings = {};
        
        // Get allocation percentage
        const allocationInput = strategyCard.querySelector(`input[id*="strategy_allocation"], input[id*="allocation"]`);
        if (allocationInput) {
            settings.allocation_percentage = parseFloat(allocationInput.value) || 0;
        }
        
        // Get position size percentage
        const positionInput = strategyCard.querySelector(`input[id*="position_size"]`);
        if (positionInput) {
            settings.position_size_percentage = parseFloat(positionInput.value) || 0;
        }
        
        // Get max contracts/shares
        const maxInput = strategyCard.querySelector(`input[id*="max_contracts"], input[id*="max_shares"]`);
        if (maxInput) {
            const isShares = maxInput.id.includes('shares');
            settings[isShares ? 'max_shares' : 'max_contracts'] = parseInt(maxInput.value) || 0;
        }
        
        settings.strategy_id = strategyId;
        settings.timestamp = new Date().toISOString();
        
        return settings;
    }
    
    // Send risk settings to WebSocket server
    sendRiskSettingsToWebSocket(strategyId, riskSettings) {
        console.log('📡 Sending risk settings to WebSocket server...');
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, settings saved locally only');
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: 'save_risk_settings',
                strategy_id: backendStrategyId,
                settings: riskSettings,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending risk settings WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Risk settings WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error sending risk settings to WebSocket:', error);
        }
    }
    
    // Map frontend strategy IDs to backend strategy IDs
    mapStrategyIdToBackend(frontendStrategyId) {
        const mapping = {
            'iron-condor': 'iron_condor',
            'pml': 'pml',
            'divergence': 'divergence'
        };
        return mapping[frontendStrategyId] || frontendStrategyId;
    }
    
    // Strategy Watchlist Toggle Functionality
    toggleStrategyWatchlist(strategyId) {
        // Prevent rapid double-clicks with debouncing
        const now = Date.now();
        const lastToggle = this.lastWatchlistToggle || {};
        
        if (lastToggle[strategyId] && (now - lastToggle[strategyId]) < 300) {
            console.log(`⚠️ Ignoring rapid toggle for ${strategyId} (debounced)`);
            return;
        }
        
        this.lastWatchlistToggle = this.lastWatchlistToggle || {};
        this.lastWatchlistToggle[strategyId] = now;
        
        console.log(`Toggling watchlist for strategy: ${strategyId}`);
        
        const button = document.querySelector(`#${strategyId}-card .watchlist-toggle-btn`);
        const content = document.querySelector(`#${strategyId}-card .watchlist-content`);
        const container = document.querySelector(`#${strategyId}-card .strategy-watchlist`);
        const chevronIcon = button?.querySelector('i');
        
        console.log(`🔍 Debug - Button found: ${!!button}`);
        console.log(`🔍 Debug - Content found: ${!!content}`);
        console.log(`🔍 Debug - Container found: ${!!container}`);
        
        if (content && button && container) {
            // Use container class as the primary state indicator (more reliable)
            const isCurrentlyCollapsed = container.classList.contains('collapsed');
            
            console.log(`🔍 Debug - Current state collapsed: ${isCurrentlyCollapsed}`);
            console.log(`🔍 Debug - Content display: ${content.style.display}`);
            console.log(`🔍 Debug - Container classes: ${container.className}`);
            
            if (isCurrentlyCollapsed) {
                // Expand - Remove collapsed class only, let CSS handle display
                container.classList.remove('collapsed');
                button.classList.remove('collapsed');
                
                // Rotate chevron icon to point down
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(0deg)';
                }
                
                console.log(`✅ Expanded watchlist for ${strategyId}`);
            } else {
                // Collapse - Add collapsed class only, let CSS handle display
                container.classList.add('collapsed');
                button.classList.add('collapsed');
                
                // Rotate chevron icon to point right
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(-90deg)';
                }
                
                console.log(`✅ Collapsed watchlist for ${strategyId}`);
            }
        } else {
            console.error(`❌ Could not find watchlist elements for strategy: ${strategyId}`);
            console.error(`❌ Button: ${!!button}, Content: ${!!content}, Container: ${!!container}`);
        }
    }
    
    // Add symbol to strategy watchlist
    addToStrategyWatchlist(strategyId) {
        console.log(`Adding symbol to watchlist for strategy: ${strategyId}`);
        
        // Simple prompt for symbol input
        const symbol = prompt(`Enter symbol to add to ${strategyId} watchlist:`);
        
        if (!symbol) {
            console.log('No symbol entered, cancelling add operation');
            return;
        }
        
        const cleanSymbol = symbol.trim().toUpperCase();
        
        if (!cleanSymbol) {
            this.showToast('Please enter a valid symbol', 'error');
            return;
        }
        
        // Basic symbol validation (letters only, 1-5 characters)
        if (!/^[A-Z]{1,5}$/.test(cleanSymbol)) {
            this.showToast('Symbol must be 1-5 letters only', 'error');
            return;
        }
        
        console.log(`Adding symbol ${cleanSymbol} to ${strategyId} watchlist`);
        
        // Check if symbol already exists in the display
        const existingSymbols = document.querySelectorAll(`#${strategyId}-card .watchlist-symbol-item .symbol-text`);
        const currentSymbols = Array.from(existingSymbols).map(el => el.textContent);
        
        if (currentSymbols.includes(cleanSymbol)) {
            this.showToast(`${cleanSymbol} is already in ${strategyId} watchlist`, 'error');
            return;
        }
        
        // Send to WebSocket server
        this.addSymbolToStrategyWatchlistViaWebSocket(strategyId, cleanSymbol);
    }
    
    // Add symbol to strategy watchlist via WebSocket
    addSymbolToStrategyWatchlistViaWebSocket(strategyId, symbol) {
        console.log(`📡 Adding symbol ${symbol} to ${strategyId} watchlist via WebSocket...`);
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, adding symbol locally only');
                this.addSymbolToWatchlistDisplay(strategyId, symbol);
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: 'add_strategy_watchlist_symbol',
                strategy_id: backendStrategyId,
                symbol: symbol,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending add symbol WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Add symbol WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error adding symbol via WebSocket:', error);
            // Fallback to local addition
            this.addSymbolToWatchlistDisplay(strategyId, symbol);
        }
    }
    
    // Add symbol to watchlist display (local fallback)
    addSymbolToWatchlistDisplay(strategyId, symbol) {
        console.log(`📋 Adding ${symbol} to ${strategyId} watchlist display`);
        
        const watchlistContainer = document.querySelector(`#${strategyId}-card .watchlist-symbols`);
        if (!watchlistContainer) {
            console.warn(`⚠️ Watchlist container not found for strategy: ${strategyId}`);
            return;
        }
        
        // Create new symbol element
        const symbolElement = document.createElement('div');
        symbolElement.className = 'watchlist-symbol-item';
        symbolElement.innerHTML = `
            <span class="symbol-text">${symbol}</span>
            <button class="remove-symbol-btn" onclick="removeFromStrategyWatchlist('${strategyId}', '${symbol}')">
                <i class="fas fa-times"></i>
            </button>
        `;
        
        // Add to container
        watchlistContainer.appendChild(symbolElement);
        
        // Save updated watchlist
        this.saveStrategyWatchlist(strategyId);
        
        this.showToast(`Added ${symbol} to ${strategyId} watchlist`, 'success');
        console.log(`✅ Added ${symbol} to ${strategyId} watchlist display`);
    }
    
    // Remove symbol from strategy watchlist
    removeFromStrategyWatchlist(strategyId, symbol) {
        console.log(`Removing ${symbol} from watchlist for strategy: ${strategyId}`);
        
        // Send to WebSocket server first
        this.removeSymbolFromStrategyWatchlistViaWebSocket(strategyId, symbol);
    }
    
    // Remove symbol from strategy watchlist via WebSocket
    removeSymbolFromStrategyWatchlistViaWebSocket(strategyId, symbol) {
        console.log(`📡 Removing symbol ${symbol} from ${strategyId} watchlist via WebSocket...`);
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, removing symbol locally only');
                this.removeSymbolFromWatchlistDisplay(strategyId, symbol);
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: 'remove_strategy_watchlist_symbol',
                strategy_id: backendStrategyId,
                symbol: symbol,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending remove symbol WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Remove symbol WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error removing symbol via WebSocket:', error);
            // Fallback to local removal
            this.removeSymbolFromWatchlistDisplay(strategyId, symbol);
        }
    }
    
    // Remove symbol from watchlist display (local fallback)
    removeSymbolFromWatchlistDisplay(strategyId, symbol) {
        console.log(`📋 Removing ${symbol} from ${strategyId} watchlist display`);
        
        try {
            // Find all symbol items for this strategy
            const symbolItems = document.querySelectorAll(`#${strategyId}-card .watchlist-symbol-item`);
            
            symbolItems.forEach(item => {
                const symbolText = item.querySelector('.symbol-text');
                if (symbolText && symbolText.textContent === symbol) {
                    item.remove();
                    console.log(`Removed ${symbol} from ${strategyId} watchlist display`);
                    
                    // Save updated watchlist
                    this.saveStrategyWatchlist(strategyId);
                    
                    this.showToast(`Removed ${symbol} from ${strategyId} watchlist`, 'success');
                    return;
                }
            });
            
            console.warn(`⚠️ Symbol ${symbol} not found in ${strategyId} watchlist display`);
            
        } catch (error) {
            console.error(`Error removing ${symbol} from ${strategyId} watchlist:`, error);
            this.showToast('Error removing symbol from watchlist', 'error');
        }
    }
    
    // Save strategy watchlist to storage
    saveStrategyWatchlist(strategyId) {
        console.log(`Saving watchlist for strategy: ${strategyId}`);
        
        try {
            // Get current symbols from the display
            const symbolElements = document.querySelectorAll(`#${strategyId}-card .watchlist-symbol-item .symbol-text`);
            const symbols = Array.from(symbolElements).map(el => el.textContent);
            
            // Save to localStorage
            const allWatchlists = JSON.parse(localStorage.getItem('volflow-strategy-watchlists') || '{}');
            allWatchlists[strategyId] = symbols;
            localStorage.setItem('volflow-strategy-watchlists', JSON.stringify(allWatchlists));
            
            // Send to WebSocket server if available
            this.sendWatchlistToWebSocket(strategyId, symbols);
            
            console.log(`Saved watchlist for ${strategyId}:`, symbols);
            
        } catch (error) {
            console.error(`Error saving watchlist for ${strategyId}:`, error);
        }
    }
    
    // Send watchlist to WebSocket server
    sendWatchlistToWebSocket(strategyId, symbols) {
        console.log('📡 Sending strategy watchlist to WebSocket server...');
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, watchlist saved locally only');
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: 'save_strategy_watchlist',
                strategy_id: backendStrategyId,
                symbols: symbols,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending strategy watchlist WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Strategy watchlist WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error sending strategy watchlist to WebSocket:', error);
        }
    }
    
    // Load strategy watchlists from config
    loadStrategyWatchlistsFromConfig(strategiesConfig) {
        console.log('📋 Loading strategy watchlists from config:', strategiesConfig);
        
        Object.keys(strategiesConfig).forEach(strategyKey => {
            const strategyConfig = strategiesConfig[strategyKey];
            
            // Find the watchlist key for this strategy
            let watchlistKey = null;
            let symbols = [];
            
            if (strategyKey === 'pml' && strategyConfig.pmlstrategy_watchlist) {
                watchlistKey = 'pmlstrategy_watchlist';
                symbols = strategyConfig.pmlstrategy_watchlist;
            } else if (strategyKey === 'iron_condor' && strategyConfig.iron_condor_strategy_watchlist) {
                watchlistKey = 'iron_condor_strategy_watchlist';
                symbols = strategyConfig.iron_condor_strategy_watchlist;
            } else if (strategyKey === 'divergence' && strategyConfig.divergence_strategy_watchlist) {
                watchlistKey = 'divergence_strategy_watchlist';
                symbols = strategyConfig.divergence_strategy_watchlist;
            }
            
            if (watchlistKey && symbols && symbols.length > 0) {
                // Map backend strategy ID to frontend strategy ID
                const frontendStrategyId = this.mapBackendStrategyIdToFrontend(strategyKey);
                console.log(`📋 Loading watchlist for ${strategyKey} -> ${frontendStrategyId}:`, symbols);
                this.updateStrategyWatchlistDisplay(frontendStrategyId, symbols);
            }
        });
    }
    
    // Map backend strategy IDs to frontend strategy IDs
    mapBackendStrategyIdToFrontend(backendStrategyId) {
        const mapping = {
            'iron_condor': 'iron-condor',
            'pml': 'pml',
            'divergence': 'divergence'
        };
        return mapping[backendStrategyId] || backendStrategyId;
    }
    
    // Update strategy watchlist display
    updateStrategyWatchlistDisplay(strategyId, symbols) {
        console.log(`📋 Updating watchlist display for ${strategyId}:`, symbols);
        
        const watchlistContainer = document.querySelector(`#${strategyId}-card .watchlist-symbols`);
        if (!watchlistContainer) {
            console.warn(`⚠️ Watchlist container not found for strategy: ${strategyId}`);
            return;
        }
        
        // Clear existing symbols
        watchlistContainer.innerHTML = '';
        
        // Add each symbol
        symbols.forEach(symbol => {
            const symbolElement = document.createElement('div');
            symbolElement.className = 'watchlist-symbol-item';
            symbolElement.innerHTML = `
                <span class="symbol-text">${symbol}</span>
                <button class="remove-symbol-btn" onclick="removeFromStrategyWatchlist('${strategyId}', '${symbol}')">
                    <i class="fas fa-times"></i>
                </button>
            `;
            watchlistContainer.appendChild(symbolElement);
        });
        
        console.log(`✅ Updated watchlist display for ${strategyId} with ${symbols.length} symbols`);
    }
    
    // Add comprehensive state restoration method
    restoreStrategyStates() {
        console.log('🔄 Restoring strategy states from backend...');
        
        // Request current strategy states from WebSocket server
        this.requestStrategyStatesFromWebSocket();
        
        // Also restore from localStorage as fallback
        this.restoreFromLocalStorage();
    }
    
    requestStrategyStatesFromWebSocket() {
        console.log('📡 Requesting strategy states from WebSocket server...');
        
        try {
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available for state restoration');
                return;
            }
            
            // Request current trading configuration (includes strategy states and watchlists)
            const message = {
                type: 'get_trading_config',
                timestamp: new Date().toISOString(),
                source: 'strategy_manager_state_restore'
            };
            
            console.log('📨 Sending state restoration request:', message);
            wsManager.ws.send(JSON.stringify(message));
            
            // Also request strategy running states
            const statusMessage = {
                type: 'get_strategy_status',
                timestamp: new Date().toISOString(),
                source: 'strategy_manager_state_restore'
            };
            
            console.log('📨 Sending strategy status request:', statusMessage);
            wsManager.ws.send(JSON.stringify(statusMessage));
            
        } catch (error) {
            console.error('❌ Error requesting strategy states:', error);
        }
    }
    
    restoreFromLocalStorage() {
        console.log('💾 Restoring strategy states from localStorage...');
        
        try {
            // Restore strategy configuration
            const savedConfig = localStorage.getItem('volflow-strategy-config');
            if (savedConfig) {
                const config = JSON.parse(savedConfig);
                console.log('📋 Restoring strategy config from localStorage:', config);
                this.applyStrategyConfiguration(config);
            }
            
            // Restore strategy watchlists
            const savedWatchlists = localStorage.getItem('volflow-strategy-watchlists');
            if (savedWatchlists) {
                const watchlists = JSON.parse(savedWatchlists);
                console.log('📋 Restoring watchlists from localStorage:', watchlists);
                
                Object.keys(watchlists).forEach(strategyId => {
                    const symbols = watchlists[strategyId];
                    if (symbols && symbols.length > 0) {
                        this.updateStrategyWatchlistDisplay(strategyId, symbols);
                    }
                });
            }
            
            // Restore risk settings
            const savedRiskSettings = localStorage.getItem('volflow-risk-settings');
            if (savedRiskSettings) {
                const riskSettings = JSON.parse(savedRiskSettings);
                console.log('📋 Restoring risk settings from localStorage:', riskSettings);
                this.applyRiskSettings(riskSettings);
            }
            
            // Restore strategy running states
            const savedRunningStates = localStorage.getItem('volflow-strategy-running-states');
            if (savedRunningStates) {
                const runningStates = JSON.parse(savedRunningStates);
                console.log('📋 Restoring running states from localStorage:', runningStates);
                this.applyStrategyRunningStates(runningStates);
            }
            
            // Restore timing settings
            const savedTimingSettings = localStorage.getItem('volflow-timing-settings');
            if (savedTimingSettings) {
                const timingSettings = JSON.parse(savedTimingSettings);
                console.log('📋 Restoring timing settings from localStorage:', timingSettings);
                this.applyTimingSettings(timingSettings);
            }
            
        } catch (error) {
            console.error('❌ Error restoring from localStorage:', error);
        }
    }
    
    applyRiskSettings(riskSettings) {
        console.log('🔧 Applying risk settings:', riskSettings);
        
        Object.keys(riskSettings).forEach(strategyId => {
            const settings = riskSettings[strategyId];
            const strategyCard = document.querySelector(`#${strategyId}-card`);
            
            if (!strategyCard) return;
            
            // Apply allocation percentage
            if (settings.allocation_percentage !== undefined) {
                const allocationInput = strategyCard.querySelector(`input[id*="strategy_allocation"], input[id*="allocation"]`);
                if (allocationInput) {
                    allocationInput.value = settings.allocation_percentage;
                }
            }
            
            // Apply position size percentage
            if (settings.position_size_percentage !== undefined) {
                const positionInput = strategyCard.querySelector(`input[id*="position_size"]`);
                if (positionInput) {
                    positionInput.value = settings.position_size_percentage;
                }
            }
            
            // Apply max contracts/shares
            if (settings.max_contracts !== undefined) {
                const maxInput = strategyCard.querySelector(`input[id*="max_contracts"]`);
                if (maxInput) {
                    maxInput.value = settings.max_contracts;
                }
            }
            
            if (settings.max_shares !== undefined) {
                const maxInput = strategyCard.querySelector(`input[id*="max_shares"]`);
                if (maxInput) {
                    maxInput.value = settings.max_shares;
                }
            }
        });
    }
    
    applyStrategyRunningStates(runningStates) {
        console.log('🔧 Applying strategy running states:', runningStates);
        
        Object.keys(runningStates).forEach(strategyId => {
            const state = runningStates[strategyId];
            const button = document.querySelector(`#run-${strategyId}-btn`);
            
            if (!button) return;
            
            // Apply the saved state
            button.classList.remove('running', 'stopped');
            
            if (state.isRunning) {
                button.classList.add('running');
                console.log(`✅ Restored ${strategyId} as running`);
            } else if (state.isStopped) {
                button.classList.add('stopped');
                console.log(`✅ Restored ${strategyId} as stopped`);
            }
            
            // Restore process tracking if needed
            if (state.isRunning && state.processInfo) {
                if (!this.runningProcesses) {
                    this.runningProcesses = {};
                }
                this.runningProcesses[strategyId] = {
                    id: state.processInfo.id,
                    startTime: new Date(state.processInfo.startTime),
                    button: button
                };
            }
        });
    }
    
    // Save strategy running state to localStorage
    saveStrategyRunningState(strategyId, isRunning, isStopped = false, processInfo = null) {
        try {
            const savedStates = JSON.parse(localStorage.getItem('volflow-strategy-running-states') || '{}');
            
            savedStates[strategyId] = {
                isRunning: isRunning,
                isStopped: isStopped,
                processInfo: processInfo,
                timestamp: new Date().toISOString()
            };
            
            localStorage.setItem('volflow-strategy-running-states', JSON.stringify(savedStates));
            console.log(`💾 Saved running state for ${strategyId}:`, savedStates[strategyId]);
            
        } catch (error) {
            console.error('❌ Error saving strategy running state:', error);
        }
    }
    
    // Clear strategy running state from localStorage
    clearStrategyRunningState(strategyId) {
        try {
            const savedStates = JSON.parse(localStorage.getItem('volflow-strategy-running-states') || '{}');
            delete savedStates[strategyId];
            localStorage.setItem('volflow-strategy-running-states', JSON.stringify(savedStates));
            console.log(`💾 Cleared running state for ${strategyId}`);
            
        } catch (error) {
            console.error('❌ Error clearing strategy running state:', error);
        }
    }
    
    // Timing Controls Toggle Functionality
    toggleTimingControls(strategyId) {
        // Prevent rapid double-clicks with debouncing
        const now = Date.now();
        const lastToggle = this.lastTimingToggle || {};
        
        if (lastToggle[strategyId] && (now - lastToggle[strategyId]) < 300) {
            console.log(`⚠️ Ignoring rapid toggle for ${strategyId} timing controls (debounced)`);
            return;
        }
        
        this.lastTimingToggle = this.lastTimingToggle || {};
        this.lastTimingToggle[strategyId] = now;
        
        console.log(`Toggling timing controls for strategy: ${strategyId}`);
        
        const button = document.querySelector(`#${strategyId}-card .timing-toggle-btn`);
        const grid = document.querySelector(`#${strategyId}-card .timing-controls-grid`);
        const container = document.querySelector(`#${strategyId}-card .strategy-timing-controls`);
        const chevronIcon = button?.querySelector('i');
        
        console.log(`🔍 Debug - Button found: ${!!button}`);
        console.log(`🔍 Debug - Grid found: ${!!grid}`);
        console.log(`🔍 Debug - Container found: ${!!container}`);
        
        if (grid && button && container) {
            // Use container class as the primary state indicator (more reliable)
            const isCurrentlyCollapsed = container.classList.contains('collapsed');
            
            console.log(`🔍 Debug - Current state collapsed: ${isCurrentlyCollapsed}`);
            console.log(`🔍 Debug - Grid display: ${grid.style.display}`);
            console.log(`🔍 Debug - Container classes: ${container.className}`);
            
            if (isCurrentlyCollapsed) {
                // Expand - Remove collapsed class only, let CSS handle display
                container.classList.remove('collapsed');
                button.classList.remove('collapsed');
                
                // Rotate chevron icon to point down
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(0deg)';
                }
                
                console.log(`✅ Expanded timing controls for ${strategyId}`);
            } else {
                // Collapse - Add collapsed class only, let CSS handle display
                container.classList.add('collapsed');
                button.classList.add('collapsed');
                
                // Rotate chevron icon to point right
                if (chevronIcon) {
                    chevronIcon.style.transform = 'rotate(-90deg)';
                }
                
                console.log(`✅ Collapsed timing controls for ${strategyId}`);
            }
        } else {
            console.error(`❌ Could not find timing control elements for strategy: ${strategyId}`);
            console.error(`❌ Button: ${!!button}, Grid: ${!!grid}, Container: ${!!container}`);
        }
    }
    
    // Timing Settings Save Functionality
    saveTimingSettings(strategyId) {
        console.log(`Saving timing settings for strategy: ${strategyId}`);
        
        try {
            // Get timing control values for the strategy
            const timingSettings = this.getTimingSettingsForStrategy(strategyId);
            
            if (!timingSettings) {
                console.error(`No timing settings found for strategy: ${strategyId}`);
                this.showToast('Error: Could not find timing settings', 'error');
                return;
            }
            
            // Save to localStorage
            const allTimingSettings = JSON.parse(localStorage.getItem('volflow-timing-settings') || '{}');
            allTimingSettings[strategyId] = timingSettings;
            localStorage.setItem('volflow-timing-settings', JSON.stringify(allTimingSettings));
            
            // Send to WebSocket server if available
            this.sendTimingSettingsToWebSocket(strategyId, timingSettings);
            
            // Show success feedback
            this.showToast(`Timing settings saved for ${strategyId}`, 'success');
            
            console.log(`Timing settings saved for ${strategyId}:`, timingSettings);
            
        } catch (error) {
            console.error(`Error saving timing settings for ${strategyId}:`, error);
            this.showToast('Error saving timing settings', 'error');
        }
    }
    
    // Get timing settings for a specific strategy
    getTimingSettingsForStrategy(strategyId) {
        const strategyCard = document.querySelector(`#${strategyId}-card`);
        if (!strategyCard) return null;
        
        const settings = {};
        
        // Get start time
        const startTimeInput = strategyCard.querySelector(`input[id*="start_time"]`);
        if (startTimeInput) {
            settings.start_time = startTimeInput.value || '09:30';
        }
        
        // Get stop time
        const stopTimeInput = strategyCard.querySelector(`input[id*="stop_time"]`);
        if (stopTimeInput) {
            settings.stop_time = stopTimeInput.value || '16:00';
        }
        
        // Get market hours only setting
        const marketHoursInput = strategyCard.querySelector(`input[id*="market_hours_only"]`);
        if (marketHoursInput) {
            settings.market_hours_only = marketHoursInput.checked;
        }
        
        settings.strategy_id = strategyId;
        settings.timestamp = new Date().toISOString();
        
        return settings;
    }
    
    // Send timing settings to WebSocket server
    sendTimingSettingsToWebSocket(strategyId, timingSettings) {
        console.log('📡 Sending timing settings to WebSocket server...');
        
        try {
            // Check WebSocket connection
            const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
            
            if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
                console.warn('⚠️ WebSocket not available, settings saved locally only');
                return;
            }
            
            // Map frontend strategy ID to backend strategy ID
            const backendStrategyId = this.mapStrategyIdToBackend(strategyId);
            
            const message = {
                type: 'save_timing_settings',
                strategy_id: backendStrategyId,
                settings: timingSettings,
                timestamp: new Date().toISOString(),
                source: 'strategy_manager'
            };
            
            console.log('📨 Sending timing settings WebSocket message:', message);
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Timing settings WebSocket message sent successfully');
            
        } catch (error) {
            console.error('❌ Error sending timing settings to WebSocket:', error);
        }
    }
    
    
    // Apply timing settings from configuration
    applyTimingSettings(timingSettings) {
        console.log('🔧 Applying timing settings:', timingSettings);
        
        Object.keys(timingSettings).forEach(strategyId => {
            const settings = timingSettings[strategyId];
            const strategyCard = document.querySelector(`#${strategyId}-card`);
            
            if (!strategyCard) return;
            
            // Apply start time
            if (settings.start_time !== undefined) {
                const startTimeInput = strategyCard.querySelector(`input[id*="start_time"]`);
                if (startTimeInput) {
                    startTimeInput.value = settings.start_time;
                }
            }
            
            // Apply stop time
            if (settings.stop_time !== undefined) {
                const stopTimeInput = strategyCard.querySelector(`input[id*="stop_time"]`);
                if (stopTimeInput) {
                    stopTimeInput.value = settings.stop_time;
                }
            }
            
            // Apply market hours only setting
            if (settings.market_hours_only !== undefined) {
                const marketHoursInput = strategyCard.querySelector(`input[id*="market_hours_only"]`);
                if (marketHoursInput) {
                    marketHoursInput.checked = settings.market_hours_only;
                }
            }
        });
    }
    
}

// Make toggleRiskControls available globally for onclick handlers
window.toggleRiskControls = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.toggleRiskControls(strategyId);
    }
};

// Make saveRiskSettings available globally for onclick handlers
window.saveRiskSettings = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.saveRiskSettings(strategyId);
    }
};

// Make toggleStrategyWatchlist available globally for onclick handlers
window.toggleStrategyWatchlist = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.toggleStrategyWatchlist(strategyId);
    }
};

// Make addToStrategyWatchlist available globally for onclick handlers
window.addToStrategyWatchlist = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.addToStrategyWatchlist(strategyId);
    }
};

// Make removeFromStrategyWatchlist available globally for onclick handlers
window.removeFromStrategyWatchlist = function(strategyId, symbol) {
    if (window.strategyManager) {
        window.strategyManager.removeFromStrategyWatchlist(strategyId, symbol);
    }
};

// Make toggleTimingControls available globally for onclick handlers
window.toggleTimingControls = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.toggleTimingControls(strategyId);
    }
};

// Make saveTimingSettings available globally for onclick handlers
window.saveTimingSettings = function(strategyId) {
    if (window.strategyManager) {
        window.strategyManager.saveTimingSettings(strategyId);
    }
};

// Make StrategyManager class available globally for app.js initialization
window.StrategyManager = StrategyManager;

// Auto-initialize if not being managed by main app
if (!window.volflowApp) {
    document.addEventListener('DOMContentLoaded', () => {
        // Wait a bit for templates to load
        setTimeout(() => {
            if (!window.strategyManager) {
                console.log('🎯 Auto-initializing StrategyManager...');
                window.strategyManager = new StrategyManager();
            }
        }, 1000);
    });
}
