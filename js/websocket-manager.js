// WebSocket connection management for real-time data streaming
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
