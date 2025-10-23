/**
 * API Manager
 * Handles authentication status, token management, and API connectivity
 * Provides frontend interface for managing Schwab API authentication
 */

class APIManager {
    constructor() {
        this.authStatus = 'unknown';
        this.lastAuthCheck = null;
        this.authCheckInterval = null;
        this.tokenExpiry = null;
        this.isRefreshing = false;
        this.connectionStatus = {
            schwab: 'disconnected'
        };
        
        this.init();
    }

    init() {
        console.log('🔐 Initializing API Manager...');
        
        // Bind event listeners
        this.bindEventListeners();
        
        // Setup WebSocket for API connection monitoring
        this.setupWebSocket();
        
        // Start authentication monitoring
        this.startAuthMonitoring();
        
        // Initial auth check
        this.checkAuthStatus();
        
        console.log('✅ API Manager initialized');
    }

    bindEventListeners() {
        console.log('🔗 Binding API Manager event listeners...');
        
        // Re-authenticate button
        const reauthBtn = document.getElementById('reauth-btn');
        if (reauthBtn) {
            console.log('✅ Re-authenticate button found, adding event listener');
            reauthBtn.addEventListener('click', (e) => {
                console.log('🔐 Re-authenticate button clicked!');
                e.preventDefault();
                this.handleReAuthenticate();
            });
        } else {
            console.error('❌ Re-authenticate button not found!');
        }


        // Exchange tokens button
        const exchangeTokensBtn = document.getElementById('exchange-tokens-btn');
        if (exchangeTokensBtn) {
            console.log('✅ Exchange tokens button found, adding event listener');
            exchangeTokensBtn.addEventListener('click', (e) => {
                console.log('🔄 Exchange tokens button clicked!');
                e.preventDefault();
                this.handleExchangeTokens();
            });
        } else {
            console.error('❌ Exchange tokens button not found!');
        }
        
        console.log('🔗 API Manager event listeners bound');
    }

    async checkAuthStatus() {
        console.log('🔍 Checking authentication status via WebSocket...');
        
        try {
            // Update UI to show checking status
            this.updateAuthStatus('checking');
            
            // Check if WebSocket manager is available
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                // Send auth status check via WebSocket
                window.webSocketManager.send({
                    type: 'check_auth_status',
                    timestamp: new Date().toISOString()
                });
                
                // Set up one-time listener for auth status response
                this.setupAuthStatusListener();
                
            } else {
                throw new Error('WebSocket connection not available');
            }
            
        } catch (error) {
            console.error('❌ Failed to check auth status:', error);
            this.updateAuthStatus('error');
            this.showNotification('Failed to check authentication status - WebSocket unavailable', 'error');
        }
    }

    setupAuthStatusListener() {
        // Set up listener for auth status response
        const handleAuthResponse = (data) => {
            if (data.type === 'auth_status_response') {
                console.log('📊 Auth status response via WebSocket:', data);
                this.processAuthStatus(data);
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleAuthResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleAuthResponse);
        }
    }

    setupTokenExchangeListener(callbackUrlInput) {
        // Set up listener for token exchange response
        const handleTokenResponse = (data) => {
            if (data.type === 'token_exchange_response') {
                console.log('📊 Token exchange response via WebSocket:', data);
                
                if (data.success) {
                    this.showNotification('✅ Tokens exchanged and stored successfully!', 'success');
                    console.log('✅ Token exchange successful via WebSocket');
                    
                    // Re-check auth status to update the UI
                    setTimeout(() => {
                        this.checkAuthStatus();
                    }, 1000);
                    
                } else {
                    // Token exchange failed, show error
                    this.showNotification(`❌ Token exchange failed: ${data.error}`, 'error');
                    console.error('Token exchange failed via WebSocket:', data.error);
                    
                    // Check if it's an expiration error
                    if (data.error && data.error.includes('expired')) {
                        setTimeout(() => {
                            this.showNotification('⚠️ Authorization code expired. Please get a fresh code (expires in 10-15 minutes)', 'warning');
                        }, 2000);
                    }
                }
                
                // Remove listener after handling
                if (window.webSocketManager) {
                    window.webSocketManager.removeMessageListener(handleTokenResponse);
                }
            }
        };
        
        if (window.webSocketManager) {
            window.webSocketManager.addMessageListener(handleTokenResponse);
        }
    }

    processAuthStatus(authData) {
        try {
            if (authData.authenticated === true) {
                this.authStatus = 'authenticated';
                this.tokenExpiry = authData.expires_at ? new Date(authData.expires_at) : null;
                
                // Check if token is expiring soon (within 30 minutes)
                if (this.tokenExpiry) {
                    const timeUntilExpiry = this.tokenExpiry.getTime() - Date.now();
                    const minutesUntilExpiry = Math.floor(timeUntilExpiry / (1000 * 60));
                    
                    if (minutesUntilExpiry <= 30 && minutesUntilExpiry > 0) {
                        this.authStatus = 'expiring_soon';
                        this.showNotification(`Token expires in ${minutesUntilExpiry} minutes`, 'warning');
                    } else if (minutesUntilExpiry <= 0) {
                        this.authStatus = 'expired';
                        this.showNotification('Authentication token has expired', 'error');
                    }
                }
                
            } else if (authData.authenticated === false) {
                this.authStatus = 'unauthenticated';
                this.tokenExpiry = null;
                
                if (authData.error) {
                    console.error('Auth error:', authData.error);
                    this.showNotification(`Authentication error: ${authData.error}`, 'error');
                }
                
            } else {
                this.authStatus = 'unknown';
                this.tokenExpiry = null;
            }
            
            this.lastAuthCheck = new Date();
            this.updateAuthStatus(this.authStatus);
            
            // Update additional auth info
            this.updateAuthDetails(authData);
            
        } catch (error) {
            console.error('❌ Error processing auth status:', error);
            this.updateAuthStatus('error');
        }
    }

    updateAuthStatus(status) {
        const authStatusElement = document.getElementById('auth-status');
        const authIndicator = document.getElementById('auth-indicator');
        
        if (!authStatusElement) {
            console.warn('⚠️ auth-status element not found');
            return;
        }

        try {
            // Update status text and styling
            switch (status) {
                case 'authenticated':
                    authStatusElement.textContent = 'Authenticated';
                    authStatusElement.className = 'auth-status authenticated';
                    if (authIndicator) authIndicator.className = 'auth-indicator authenticated';
                    this.enableAuthButtons(true);
                    break;
                    
                case 'expiring_soon':
                    authStatusElement.textContent = 'Expiring Soon';
                    authStatusElement.className = 'auth-status expiring';
                    if (authIndicator) authIndicator.className = 'auth-indicator expiring';
                    this.enableAuthButtons(true);
                    break;
                    
                case 'expired':
                    authStatusElement.textContent = 'Expired';
                    authStatusElement.className = 'auth-status expired';
                    if (authIndicator) authIndicator.className = 'auth-indicator expired';
                    this.enableAuthButtons(false);
                    break;
                    
                case 'unauthenticated':
                    authStatusElement.textContent = 'Not Authenticated';
                    authStatusElement.className = 'auth-status unauthenticated';
                    if (authIndicator) authIndicator.className = 'auth-indicator unauthenticated';
                    this.enableAuthButtons(false);
                    break;
                    
                case 'checking':
                    authStatusElement.textContent = 'Checking...';
                    authStatusElement.className = 'auth-status checking';
                    if (authIndicator) authIndicator.className = 'auth-indicator checking';
                    break;
                    
                case 'refreshing':
                    authStatusElement.textContent = 'Refreshing...';
                    authStatusElement.className = 'auth-status refreshing';
                    if (authIndicator) authIndicator.className = 'auth-indicator refreshing';
                    break;
                    
                case 'error':
                    authStatusElement.textContent = 'Error';
                    authStatusElement.className = 'auth-status error';
                    if (authIndicator) authIndicator.className = 'auth-indicator error';
                    this.enableAuthButtons(false);
                    break;
                    
                default:
                    authStatusElement.textContent = 'Unknown';
                    authStatusElement.className = 'auth-status unknown';
                    if (authIndicator) authIndicator.className = 'auth-indicator unknown';
                    this.enableAuthButtons(false);
            }
            
            console.log(`🔐 Updated auth status to: ${status}`);
        } catch (error) {
            console.error('❌ Error updating auth status:', error);
        }
    }

    updateAuthDetails(authData) {
        try {
            // Update token expiry display
            const expiryElement = document.getElementById('token-expiry');
            if (expiryElement && this.tokenExpiry) {
                const timeUntilExpiry = this.tokenExpiry.getTime() - Date.now();
                const hoursUntilExpiry = Math.floor(timeUntilExpiry / (1000 * 60 * 60));
                const minutesUntilExpiry = Math.floor((timeUntilExpiry % (1000 * 60 * 60)) / (1000 * 60));
                
                if (timeUntilExpiry > 0) {
                    expiryElement.textContent = `${hoursUntilExpiry}h ${minutesUntilExpiry}m`;
                    expiryElement.className = hoursUntilExpiry < 1 ? 'token-expiry warning' : 'token-expiry';
                } else {
                    expiryElement.textContent = 'Expired';
                    expiryElement.className = 'token-expiry expired';
                }
            } else if (expiryElement) {
                expiryElement.textContent = 'Unknown';
                expiryElement.className = 'token-expiry unknown';
            }
            
            // Update last check time
            const lastCheckElement = document.getElementById('last-auth-check');
            if (lastCheckElement && this.lastAuthCheck) {
                lastCheckElement.textContent = this.lastAuthCheck.toLocaleTimeString();
            }
            
            // Update account info if available
            const accountElement = document.getElementById('auth-account-info');
            if (accountElement && authData.account_info) {
                accountElement.textContent = `Account: ${authData.account_info.account_number || 'Unknown'}`;
            }
            
        } catch (error) {
            console.error('❌ Error updating auth details:', error);
        }
    }

    enableAuthButtons(authenticated) {
        const refreshBtn = document.getElementById('refresh-tokens-btn');
        const testApiBtn = document.getElementById('test-api-btn');
        const reauthBtn = document.getElementById('reauth-btn');
        
        if (refreshBtn) {
            refreshBtn.disabled = !authenticated;
        }
        
        if (testApiBtn) {
            testApiBtn.disabled = !authenticated;
        }
        
        if (reauthBtn) {
            reauthBtn.disabled = false; // Always allow re-authentication
        }
    }

    async handleRefreshTokens() {
        if (this.isRefreshing) {
            console.log('Token refresh already in progress');
            return;
        }
        
        console.log('🔄 Refreshing authentication tokens...');
        this.isRefreshing = true;
        
        try {
            this.updateAuthStatus('refreshing');
            this.showNotification('Refreshing authentication tokens...', 'info');
            
            const response = await fetch('http://localhost:5001/api/auth/refresh', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            if (result.success) {
                this.showNotification('Tokens refreshed successfully', 'success');
                console.log('✅ Tokens refreshed successfully');
                
                // Re-check auth status
                setTimeout(() => {
                    this.checkAuthStatus();
                }, 1000);
                
            } else {
                throw new Error(result.error || 'Token refresh failed');
            }
            
        } catch (error) {
            console.error('❌ Failed to refresh tokens:', error);
            this.updateAuthStatus('error');
            this.showNotification('Failed to refresh tokens: ' + error.message, 'error');
        } finally {
            this.isRefreshing = false;
        }
    }

    async handleReAuthenticate() {
        console.log('🔐 Initiating re-authentication...');
        
        // Show immediate feedback
        this.showNotification('Preparing authentication...', 'info');
        
        // Use setTimeout to make the UI responsive immediately
        setTimeout(async () => {
            try {
                // Generate auth URL directly without API call for better performance
                const callback_url = "https://127.0.0.1";
                const auth_url = `https://api.schwabapi.com/v1/oauth/authorize?response_type=code&client_id=UXvDmuMdEsgAyXAWGMSOblaaLbnR8MhW&redirect_uri=${callback_url}&scope=readonly`;
                
                // Open authentication URL immediately
                window.open(auth_url, '_blank', 'width=600,height=700');
                this.showNotification('Complete authentication in the new window', 'info');
                
                // Start polling for authentication completion
                this.pollForAuthCompletion();
                
            } catch (error) {
                console.error('❌ Failed to initiate re-authentication:', error);
                this.showNotification('Failed to start re-authentication: ' + error.message, 'error');
            }
        }, 50); // Small delay to ensure UI responsiveness
    }

    pollForAuthCompletion() {
        console.log('📡 Polling for authentication completion...');
        
        const pollInterval = setInterval(async () => {
            try {
                const response = await fetch('http://localhost:5001/api/auth/status');
                if (response.ok) {
                    const authData = await response.json();
                    if (authData.authenticated === true) {
                        clearInterval(pollInterval);
                        this.showNotification('Authentication completed successfully!', 'success');
                        this.checkAuthStatus();
                    }
                }
            } catch (error) {
                console.error('Error polling auth status:', error);
            }
        }, 3000); // Poll every 3 seconds
        
        // Stop polling after 5 minutes
        setTimeout(() => {
            clearInterval(pollInterval);
        }, 300000);
    }

    async handleExchangeTokens() {
        console.log('🔄 Exchanging authorization code for tokens...');
        
        try {
            // Get the callback URL from the input field
            const callbackUrlInput = document.getElementById('callback-url-input');
            if (!callbackUrlInput) {
                throw new Error('Callback URL input field not found');
            }
            
            const callbackUrl = callbackUrlInput.value.trim();
            if (!callbackUrl) {
                this.showNotification('Please paste the callback URL first', 'warning');
                return;
            }
            
            // Extract the authorization code from the URL
            const urlParams = new URLSearchParams(new URL(callbackUrl).search);
            const authCode = urlParams.get('code');
            
            if (!authCode) {
                this.showNotification('No authorization code found in URL', 'error');
                return;
            }
            
            console.log('📋 Authorization code extracted:', authCode.substring(0, 20) + '...');
            this.showNotification('Exchanging authorization code for tokens...', 'info');
            
            // Use WebSocket to exchange tokens via the websocket server
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                // Send token exchange request via WebSocket
                window.webSocketManager.send({
                    type: 'exchange_tokens',
                    code: authCode,
                    timestamp: new Date().toISOString()
                });
                
                // Set up one-time listener for token exchange response
                this.setupTokenExchangeListener(callbackUrlInput);
                
            } else {
                // WebSocket not available, provide manual command as fallback
                this.showNotification(`❌ WebSocket unavailable. Run this command in terminal: python3 -c "import connection_manager; connection_manager.get_tokens('${authCode}')"`, 'info');
                console.log('Manual token exchange command:', `python3 -c "import connection_manager; connection_manager.get_tokens('${authCode}')"`);
                
                // Clear the input field
                callbackUrlInput.value = '';
            }
            
            // Clear the input field
            callbackUrlInput.value = '';
            
        } catch (error) {
            console.error('❌ Failed to exchange tokens:', error);
            
            // Extract the auth code for manual processing
            try {
                const callbackUrlInput = document.getElementById('callback-url-input');
                const callbackUrl = callbackUrlInput.value.trim();
                const urlParams = new URLSearchParams(new URL(callbackUrl).search);
                const authCode = urlParams.get('code');
                
                this.showNotification(`Please run this command manually: python3 -c "import connection_manager; connection_manager.get_tokens('${authCode}')"`, 'info');
                console.log('Manual token exchange command:', `python3 -c "import connection_manager; connection_manager.get_tokens('${authCode}')"`);
                
                // Clear the input field
                callbackUrlInput.value = '';
                
            } catch (extractError) {
                this.showNotification('Failed to exchange tokens: ' + error.message, 'error');
            }
        }
    }

    async testAPIConnection() {
        console.log('🧪 Testing API connection...');
        
        try {
            this.showNotification('Testing API connection...', 'info');
            
            const response = await fetch('http://localhost:5001/api/auth/test', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            if (result.success) {
                this.showNotification('✅ API connection test successful', 'success');
                console.log('✅ API connection test passed');
                
                // Update with test results
                if (result.account_numbers) {
                    console.log('📊 Available accounts:', result.account_numbers);
                }
                
            } else {
                throw new Error(result.error || 'API test failed');
            }
            
        } catch (error) {
            console.error('❌ API connection test failed:', error);
            this.showNotification('❌ API connection test failed: ' + error.message, 'error');
        }
    }

    startAuthMonitoring() {
        // Check auth status every 5 minutes
        this.authCheckInterval = setInterval(() => {
            this.checkAuthStatus();
        }, 300000); // 5 minutes
        
        console.log('📡 Started authentication monitoring (5 minute interval)');
    }

    stopAuthMonitoring() {
        if (this.authCheckInterval) {
            clearInterval(this.authCheckInterval);
            this.authCheckInterval = null;
            console.log('📡 Stopped authentication monitoring');
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
    getAuthStatus() {
        return {
            status: this.authStatus,
            tokenExpiry: this.tokenExpiry,
            lastCheck: this.lastAuthCheck,
            isAuthenticated: this.authStatus === 'authenticated'
        };
    }

    async refreshStatus() {
        await this.checkAuthStatus();
    }

    setupWebSocket() {
        // Wait for WebSocket manager to be available and connected
        const waitForWebSocket = () => {
            if (window.webSocketManager && window.webSocketManager.isConnected()) {
                console.log('🔌 Setting up API connection WebSocket listener...');
                
                // Set up listener for API status updates
                const handleApiStatusUpdate = (data) => {
                    if (data.type === 'api_status_update') {
                        console.log('📊 API status update:', data);
                        this.processApiStatusUpdate(data.data);
                    } else if (data.type === 'api_connection_status') {
                        console.log('📊 API connection status update:', data);
                        this.updateConnectionStatus(data.data);
                    }
                };
                
                window.webSocketManager.addMessageListener(handleApiStatusUpdate);
                
                // Also register this client with the API connection handler
                // This ensures the handler starts monitoring when we connect
                setTimeout(() => {
                    window.webSocketManager.send({
                        type: 'get_status',
                        timestamp: new Date().toISOString()
                    });
                }, 1000); // Small delay to ensure handler is ready
                
                console.log('✅ API connection WebSocket listener set up successfully');
            } else {
                console.warn('⚠️ WebSocket manager not available, retrying in 2 seconds...');
                setTimeout(waitForWebSocket, 2000);
            }
        };
        
        // Start waiting for WebSocket
        waitForWebSocket();
        
        // Also set up initial display with default values
        this.updateConnectionDisplay();
    }

    processApiStatusUpdate(statusData) {
        console.log('🔄 Processing API status update:', statusData);
        
        try {
            // Update authentication status using frontend_fields
            if (statusData.frontend_fields) {
                const fields = statusData.frontend_fields;
                
                // Update auth status elements directly using the frontend_fields
                this.updateElementById('auth-status', fields['auth-status']);
                this.updateElementById('schwab-status', fields['schwab-status']);
                this.updateElementById('token-expiry', fields['token-expiry']);
                this.updateElementById('last-auth-check', fields['last-auth-check']);
                this.updateElementById('auth-account-info', fields['auth-account-info']);
                this.updateElementById('connection-last-updated', fields['connection-last-updated']);
                
                // Update indicators
                this.updateIndicatorById('auth-indicator', fields['auth-indicator']);
                this.updateIndicatorById('schwab-indicator', fields['schwab-indicator']);
                
                console.log('✅ Updated frontend elements from API status');
            }
            
            // Update internal state
            if (statusData.authentication) {
                this.authStatus = statusData.authentication.status || 'unknown';
                this.lastAuthCheck = statusData.authentication.last_check ? new Date(statusData.authentication.last_check) : new Date();
                
                if (statusData.authentication.expires_at) {
                    this.tokenExpiry = new Date(statusData.authentication.expires_at);
                }
            }
            
            // Update connection status
            if (statusData.connections && statusData.connections.schwab) {
                this.connectionStatus.schwab = statusData.connections.schwab.status;
            }
            
            // Dispatch custom event for other components
            window.dispatchEvent(new CustomEvent('apiStatusUpdate', {
                detail: { statusData, apiManager: this }
            }));
            
        } catch (error) {
            console.error('❌ Error processing API status update:', error);
        }
    }

    updateConnectionStatus(statusData) {
        // Update connection status from WebSocket data
        if (statusData.schwab) {
            this.connectionStatus.schwab = statusData.schwab.status;
        }
        
        this.updateConnectionDisplay();
        
        // Dispatch custom event for other components
        window.dispatchEvent(new CustomEvent('apiConnectionStatusUpdate', {
            detail: { statusData, connectionStatus: this.connectionStatus }
        }));
    }

    testConnection(apiName) {
        // Test specific API connection
        if (window.webSocketManager && window.webSocketManager.isConnected()) {
            window.webSocketManager.send({ 
                type: 'test_connection', 
                api: apiName,
                timestamp: new Date().toISOString()
            });
        } else {
            console.warn('WebSocket not available for connection test');
        }
    }

    refreshAllConnections() {
        console.log('🔄 Refreshing all connections...');
        // Refresh all connection statuses
        if (window.webSocketManager && window.webSocketManager.isConnected()) {
            window.webSocketManager.send({ 
                type: 'refresh_status',
                timestamp: new Date().toISOString()
            });
            console.log('📤 Sent refresh_status message');
        } else {
            console.warn('WebSocket not available for connection refresh');
            this.showNotification('WebSocket not connected - cannot refresh status', 'warning');
        }
        console.log('🔄 Updating connection display...', this.connectionStatus);
        
        // Update Schwab status
        const schwabStatus = document.getElementById('schwab-status');
        const schwabIndicator = document.getElementById('schwab-indicator');
        
        if (schwabStatus && schwabIndicator) {
            schwabStatus.textContent = this.getStatusText(this.connectionStatus.schwab);
            schwabIndicator.className = `status-indicator ${this.getStatusClass(this.connectionStatus.schwab)}`;
            console.log('✅ Updated Schwab status:', this.connectionStatus.schwab);
        } else {
            console.warn('⚠️ Schwab status elements not found');
        }


        // Update last updated time
        const lastUpdated = document.getElementById('connection-last-updated');
        if (lastUpdated) {
            lastUpdated.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
        } else {
            console.warn('⚠️ Last updated element not found');
        }
    }

    getStatusText(status) {
        switch (status) {
            case 'connected': return 'Connected';
            case 'disconnected': return 'Disconnected';
            case 'expired': return 'Token Expired';
            case 'error': return 'Error';
            case 'no_token': return 'No Token';
            case 'no_key': return 'No API Key';
            case 'invalid_token': return 'Invalid Token';
            case 'unauthorized': return 'Unauthorized';
            case 'connection_error': return 'Connection Error';
            case 'rate_limited': return 'Rate Limited';
            default: return 'Unknown';
        }
    }

    getStatusClass(status) {
        switch (status) {
            case 'connected': return 'connected';
            case 'disconnected': return 'disconnected';
            case 'expired': return 'warning';
            case 'error': return 'error';
            case 'no_token': return 'error';
            case 'no_key': return 'error';
            case 'invalid_token': return 'error';
            case 'unauthorized': return 'error';
            case 'connection_error': return 'warning';
            case 'rate_limited': return 'warning';
            default: return 'unknown';
        }
    }

    updateElementById(elementId, value) {
        const element = document.getElementById(elementId);
        if (element && value !== undefined && value !== null) {
            element.textContent = value;
        }
    }

    updateIndicatorById(elementId, className) {
        const element = document.getElementById(elementId);
        if (element && className) {
            element.className = `status-indicator ${className}`;
        }
    }

    updateConnectionDisplay() {
        // This method is called by the old connection status system
        // We'll keep it for backward compatibility but it may not be needed
        // with the new API status system
        console.log('🔄 Updating connection display (legacy method)');
    }

    destroy() {
        this.stopAuthMonitoring();
        console.log('🔐 API Manager destroyed');
    }
}

// Global instance
let apiManager = null;

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    apiManager = new APIManager();
});

// Export for external access
window.APIManager = APIManager;
window.apiManager = apiManager;
