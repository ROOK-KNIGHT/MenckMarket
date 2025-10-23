// WebSocket connection management for real-time data streaming
class WebSocketManager {
    constructor() {
        console.log('🔍 DEBUG: WebSocketManager constructor called');
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000;
        this.connected = false;
        this.callbacks = {
            onData: [],
            onConnect: [],
            onDisconnect: [],
            onError: []
        };
        
        console.log('🔍 DEBUG: WebSocketManager initialized, calling connect()');
        this.connect();
    }
    
    connect() {
        try {
            console.log('🔍 DEBUG: WebSocketManager.connect() called');
            console.log('🔄 Connecting to WebSocket server...');
            // Connect directly to WebSocket server on port 8765
            const wsUrl = 'ws://localhost:8765';
            console.log(`📡 WebSocket URL: ${wsUrl}`);
            console.log('🔍 DEBUG: Creating new WebSocket instance...');
            this.ws = new WebSocket(wsUrl);
            console.log('🔍 DEBUG: WebSocket instance created:', !!this.ws);
            
            this.ws.onopen = () => {
                console.log('🔍 DEBUG: WebSocket onopen event fired');
                console.log('✅ WebSocket connected');
                this.connected = true;
                this.reconnectAttempts = 0;
                this.updateConnectionStatus('connected');
                console.log('🔍 DEBUG: Calling onConnect callbacks:', this.callbacks.onConnect.length);
                this.callbacks.onConnect.forEach(callback => callback());
            };
            
            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    console.log('📡 Received WebSocket data:', data.timestamp || data.type);
                    
                    // Route strategy and trading configuration messages to strategy manager
                    if (data.type && (data.type.includes('strategy_config') || data.type.includes('trading_config'))) {
                        console.log('🎯 Routing config message to strategy manager:', data.type);
                        if (window.strategyManager && typeof window.strategyManager.handleWebSocketMessage === 'function') {
                            window.strategyManager.handleWebSocketMessage(data);
                        } else {
                            console.warn('⚠️ Strategy manager not available for config message routing');
                        }
                    }
                    
                    // Route risk settings messages to strategy manager
                    if (data.type && data.type.includes('risk_settings')) {
                        console.log('🎯 Routing risk settings message to strategy manager:', data.type);
                        if (window.strategyManager && typeof window.strategyManager.handleWebSocketMessage === 'function') {
                            window.strategyManager.handleWebSocketMessage(data);
                        } else {
                            console.warn('⚠️ Strategy manager not available for risk settings message routing');
                        }
                    }
                    
                    // Route strategy watchlist messages to strategy manager
                    if (data.type && data.type.includes('strategy_watchlist')) {
                        console.log('🎯 Routing strategy watchlist message to strategy manager:', data.type);
                        if (window.strategyManager && typeof window.strategyManager.handleWebSocketMessage === 'function') {
                            window.strategyManager.handleWebSocketMessage(data);
                        } else {
                            console.warn('⚠️ Strategy manager not available for strategy watchlist message routing');
                        }
                    }
                    
                    // Call all registered data callbacks
                    this.callbacks.onData.forEach(callback => callback(data));
                } catch (error) {
                    console.error('❌ Error parsing WebSocket data:', error);
                }
            };
            
            this.ws.onclose = () => {
                console.log('🔍 DEBUG: WebSocket onclose event fired');
                console.log('🔌 WebSocket disconnected');
                this.connected = false;
                this.updateConnectionStatus('disconnected');
                this.callbacks.onDisconnect.forEach(callback => callback());
                this.attemptReconnect();
            };
            
            this.ws.onerror = (error) => {
                console.log('🔍 DEBUG: WebSocket onerror event fired');
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
        if (this.isConnected() && this.ws.readyState === WebSocket.OPEN) {
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
    
    // Add message listener for specific message types (used by API manager)
    addMessageListener(callback) {
        this.callbacks.onData.push(callback);
    }
    
    // Remove message listener
    removeMessageListener(callback) {
        const index = this.callbacks.onData.indexOf(callback);
        if (index > -1) {
            this.callbacks.onData.splice(index, 1);
        }
    }
    
    isConnected() {
        return this.ws && this.ws.readyState === WebSocket.OPEN;
    }
    
    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }
}

// Export to global scope for app.js to access
window.WebSocketManager = WebSocketManager;
console.log('🔍 DEBUG: WebSocketManager exported to window object');
