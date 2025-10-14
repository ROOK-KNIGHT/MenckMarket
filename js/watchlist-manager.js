// Simplified Asset Watchlist Management Module - WebSocket Only
class WatchlistManager {
    constructor() {
        this.watchlistSymbols = [];
        this.watchlistData = {};
        this.init();
    }
    
    init() {
        console.log('🔍 DEBUG: Initializing simplified watchlist manager...');
        this.bindEvents();
        // Watchlist will auto-populate from PostgreSQL data stream
        this.initializeDisplay();
    }
    
    bindEvents() {
        console.log('🔍 DEBUG: Binding watchlist events...');
        
        const addSymbolBtn = document.getElementById('add-symbol');
        const confirmAddBtn = document.getElementById('confirm-add-symbol');
        const cancelAddBtn = document.getElementById('cancel-add-symbol');
        const symbolInput = document.getElementById('symbol-input');
        const refreshBtn = document.getElementById('refresh-watchlist');
        
        console.log('🔍 DEBUG: Element check:', {
            addSymbolBtn: !!addSymbolBtn,
            confirmAddBtn: !!confirmAddBtn,
            cancelAddBtn: !!cancelAddBtn,
            symbolInput: !!symbolInput,
            refreshBtn: !!refreshBtn
        });
        
        if (addSymbolBtn) {
            addSymbolBtn.addEventListener('click', () => {
                console.log('🔍 DEBUG: Add symbol button clicked');
                this.showAddSymbolForm();
            });
        }
        
        if (confirmAddBtn) {
            confirmAddBtn.addEventListener('click', () => {
                console.log('🔍 DEBUG: Confirm add button clicked');
                this.confirmAddSymbol();
            });
        }
        
        if (cancelAddBtn) {
            cancelAddBtn.addEventListener('click', () => {
                console.log('🔍 DEBUG: Cancel add button clicked');
                this.hideAddSymbolForm();
            });
        }
        
        if (symbolInput) {
            symbolInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    console.log('🔍 DEBUG: Enter key pressed in symbol input');
                    this.confirmAddSymbol();
                }
            });
        }
        
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => {
                console.log('🔍 DEBUG: Refresh button clicked');
                this.requestWatchlistUpdate();
            });
        }
        
        console.log('✅ Watchlist events bound successfully');
    }
    
    showAddSymbolForm() {
        console.log('🔍 DEBUG: Showing add symbol form');
        const form = document.getElementById('add-symbol-form');
        const input = document.getElementById('symbol-input');
        
        if (form) {
            form.style.display = 'block';
            if (input) {
                input.focus();
                input.value = '';
            }
        }
    }
    
    hideAddSymbolForm() {
        console.log('🔍 DEBUG: Hiding add symbol form');
        const form = document.getElementById('add-symbol-form');
        const input = document.getElementById('symbol-input');
        
        if (form) {
            form.style.display = 'none';
            if (input) {
                input.value = '';
            }
        }
    }
    
    confirmAddSymbol() {
        const input = document.getElementById('symbol-input');
        if (!input) {
            console.log('🔍 DEBUG: Symbol input not found');
            return;
        }
        
        const symbol = input.value.trim().toUpperCase();
        console.log('🔍 DEBUG: confirmAddSymbol called with symbol:', symbol);
        
        if (!symbol) {
            console.log('🔍 DEBUG: Empty symbol, showing warning');
            this.showToast('Please enter a valid symbol', 'warning');
            return;
        }
        
        if (this.watchlistSymbols.includes(symbol)) {
            console.log('🔍 DEBUG: Symbol already exists:', this.watchlistSymbols);
            this.showToast(`${symbol} is already in your watchlist`, 'warning');
            return;
        }
        
        // Send WebSocket message immediately
        console.log('🔍 DEBUG: Sending WebSocket add symbol message');
        this.sendAddSymbolMessage(symbol);
    }
    
    sendAddSymbolMessage(symbol) {
        console.log('🔍 DEBUG: === sendAddSymbolMessage START ===');
        console.log('🔍 DEBUG: Symbol to add:', symbol);
        
        // Check WebSocket connection - try multiple possible references
        const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
        
        if (!wsManager) {
            console.error('🔍 DEBUG: WebSocket manager not found');
            console.error('🔍 DEBUG: Available globals:', Object.keys(window).filter(k => k.includes('websocket') || k.includes('ws')));
            this.showToast('WebSocket connection not available', 'error');
            return;
        }
        
        console.log('🔍 DEBUG: Found WebSocket manager:', !!wsManager);
        
        if (!wsManager.ws) {
            console.error('🔍 DEBUG: WebSocket not created');
            this.showToast('WebSocket not connected', 'error');
            return;
        }
        
        if (wsManager.ws.readyState !== WebSocket.OPEN) {
            console.error('🔍 DEBUG: WebSocket not open, state:', wsManager.ws.readyState);
            this.showToast('WebSocket not connected', 'error');
            return;
        }
        
        // Show loading state
        const confirmBtn = document.getElementById('confirm-add-symbol');
        if (confirmBtn) {
            confirmBtn.disabled = true;
            confirmBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Adding...';
        }
        
        // Create and send message
        const message = {
            type: 'add_watchlist_symbol',
            symbol: symbol,
            timestamp: new Date().toISOString()
        };
        
        console.log('🔍 DEBUG: Sending WebSocket message:', message);
        
        try {
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ WebSocket message sent successfully');
            
            // Hide form and reset button after sending
            setTimeout(() => {
                this.hideAddSymbolForm();
                if (confirmBtn) {
                    confirmBtn.disabled = false;
                    confirmBtn.innerHTML = '<i class="fas fa-check"></i> Add';
                }
            }, 1000);
            
        } catch (error) {
            console.error('🔍 DEBUG: Error sending WebSocket message:', error);
            this.showToast('Failed to send add symbol request', 'error');
            
            // Reset button state
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.innerHTML = '<i class="fas fa-check"></i> Add';
            }
        }
    }
    
    sendRemoveSymbolMessage(symbol) {
        console.log('🔍 DEBUG: === sendRemoveSymbolMessage START ===');
        console.log('🔍 DEBUG: Symbol to remove:', symbol);
        
        // Check WebSocket connection - try multiple possible references
        const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
        
        if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
            console.error('🔍 DEBUG: WebSocket not available for remove');
            this.showToast('WebSocket not connected', 'error');
            return;
        }
        
        // Create and send message
        const message = {
            type: 'remove_watchlist_symbol',
            symbol: symbol,
            timestamp: new Date().toISOString()
        };
        
        console.log('🔍 DEBUG: Sending remove WebSocket message:', message);
        
        try {
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Remove WebSocket message sent successfully');
        } catch (error) {
            console.error('🔍 DEBUG: Error sending remove WebSocket message:', error);
            this.showToast('Failed to send remove symbol request', 'error');
        }
    }
    
    requestWatchlistUpdate() {
        console.log('🔍 DEBUG: Requesting watchlist refresh with database cleanup via WebSocket');
        
        // Check WebSocket connection - try multiple possible references
        const wsManager = window.websocketManager || window.wsManager || (window.volflowApp && window.volflowApp.modules.websocket);
        
        if (!wsManager || !wsManager.ws || wsManager.ws.readyState !== WebSocket.OPEN) {
            console.error('🔍 DEBUG: WebSocket not available for refresh');
            this.showToast('WebSocket not connected', 'error');
            return;
        }
        
        // Show loading state
        const refreshBtn = document.getElementById('refresh-watchlist');
        if (refreshBtn) {
            refreshBtn.disabled = true;
            refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        }
        
        // Create and send message for refresh with database cleanup
        const message = {
            type: 'refresh_watchlist_with_cleanup',
            timestamp: new Date().toISOString()
        };
        
        console.log('🔍 DEBUG: Sending refresh watchlist with cleanup WebSocket message:', message);
        
        try {
            wsManager.ws.send(JSON.stringify(message));
            console.log('✅ Refresh watchlist with cleanup WebSocket message sent successfully');
            
            // Reset button after a delay
            setTimeout(() => {
                if (refreshBtn) {
                    refreshBtn.disabled = false;
                    refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh';
                }
            }, 3000); // 3 second delay to allow for cleanup process
            
        } catch (error) {
            console.error('🔍 DEBUG: Error sending refresh watchlist WebSocket message:', error);
            this.showToast('Failed to refresh watchlist', 'error');
            
            // Reset button state on error
            if (refreshBtn) {
                refreshBtn.disabled = false;
                refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh';
            }
        }
    }
    
    // Handle WebSocket data updates (called by app.js)
    updateFromWebSocket(data) {
        console.log('📡 Watchlist manager received WebSocket data:', Object.keys(data));
        
        // Handle specific watchlist responses (add/remove operations)
        if (data.type === 'watchlist_symbol_added' && data.success) {
            console.log('✅ Symbol added successfully:', data.symbol);
            this.showToast(`Added ${data.symbol} to watchlist`, 'success');
            
            // Add symbol to local list if not already present
            if (!this.watchlistSymbols.includes(data.symbol)) {
                this.watchlistSymbols.push(data.symbol);
                this.updateDisplay();
            }
        }
        
        if (data.type === 'watchlist_symbol_removed' && data.success) {
            console.log('✅ Symbol removed successfully:', data.symbol);
            this.showToast(`Removed ${data.symbol} from watchlist`, 'info');
            
            // Remove symbol from local list
            const index = this.watchlistSymbols.indexOf(data.symbol);
            if (index > -1) {
                this.watchlistSymbols.splice(index, 1);
                this.updateDisplay();
            }
        }
        
        if (data.type === 'watchlist_refreshed_with_cleanup' && data.success) {
            console.log('✅ Watchlist refreshed with cleanup successfully');
            this.showToast('Watchlist refreshed with database cleanup completed', 'success');
            
            // Update local watchlist data from the response
            if (data.watchlist && data.watchlist.symbols) {
                this.watchlistSymbols = data.watchlist.symbols;
                this.updateDisplay();
            }
        }
        
        if (data.type === 'watchlist_error') {
            console.log('❌ Watchlist error:', data.error);
            this.showToast(data.error, 'error');
        }
        
        // Handle PostgreSQL data stream - extract watchlist symbols and market data
        if (data.data_source === 'postgresql' && data.watchlist_data) {
            console.log('📊 Processing PostgreSQL watchlist data');
            
            // Extract symbols from PostgreSQL watchlist_data
            const pgWatchlistSymbols = data.watchlist_data.map(item => item.symbol);
            console.log('📋 PostgreSQL watchlist symbols:', pgWatchlistSymbols);
            
            // Update our local symbols list from PostgreSQL (this represents api_watchlist.json)
            this.watchlistSymbols = pgWatchlistSymbols;
            
            // Update market data for watchlist symbols from PostgreSQL
            this.watchlistData = {};
            data.watchlist_data.forEach(item => {
                this.watchlistData[item.symbol] = {
                    symbol: item.symbol,
                    current_price: parseFloat(item.current_price) || 0.0,
                    price_change: parseFloat(item.price_change) || 0.0,
                    price_change_percent: parseFloat(item.price_change_percent) || 0.0,
                    volume: parseInt(item.volume) || 0,
                    market_status: item.market_status || 'Unknown',
                    last_updated: item.timestamp || new Date().toISOString()
                };
            });
            
            console.log('✅ Updated watchlist from PostgreSQL:', this.watchlistSymbols.length, 'symbols');
            this.updateDisplay();
        }
    }
    
    initializeDisplay() {
        console.log('🎨 Initializing watchlist display');
        this.updateDisplay();
    }
    
    updateDisplay() {
        console.log('🎨 Updating watchlist display with', this.watchlistSymbols.length, 'symbols');
        
        const watchlistList = document.getElementById('watchlist-list');
        const emptyState = document.getElementById('watchlist-empty');
        
        if (!watchlistList || !emptyState) {
            console.log('🔍 DEBUG: Watchlist display elements not found');
            return;
        }
        
        if (this.watchlistSymbols.length === 0) {
            watchlistList.style.display = 'none';
            emptyState.style.display = 'block';
            this.updateStatus();
            return;
        }
        
        // Show watchlist and hide empty state
        watchlistList.style.display = 'flex';
        emptyState.style.display = 'none';
        
        // Clear existing items
        watchlistList.innerHTML = '';
        
        // Add each symbol
        this.watchlistSymbols.forEach(symbol => {
            const symbolData = this.watchlistData[symbol];
            if (symbolData) {
                const item = document.createElement('div');
                item.className = 'watchlist-item fade-in';
                item.innerHTML = this.createWatchlistItemHTML(symbolData);
                watchlistList.appendChild(item);
            }
        });
        
        this.updateStatus();
    }
    
    createWatchlistItemHTML(symbolData) {
        const priceChangeClass = symbolData.price_change >= 0 ? 'positive' : 'negative';
        const priceChangeIcon = symbolData.price_change >= 0 ? 'fa-arrow-up' : 'fa-arrow-down';
        const symbolInitial = symbolData.symbol.charAt(0);
        
        return `
            <div class="watchlist-symbol">
                <div class="symbol-icon">${symbolInitial}</div>
                <div class="symbol-info">
                    <div class="symbol-name">${symbolData.symbol}</div>
                    <div class="symbol-type">Stock</div>
                </div>
            </div>
            <div class="watchlist-price">
                <div class="current-price">$${symbolData.current_price.toFixed(2)}</div>
                <div class="price-change ${priceChangeClass}">
                    <i class="fas ${priceChangeIcon}"></i>
                    ${symbolData.price_change >= 0 ? '+' : ''}${symbolData.price_change.toFixed(2)} 
                    (${symbolData.price_change_percent >= 0 ? '+' : ''}${symbolData.price_change_percent.toFixed(2)}%)
                </div>
            </div>
            <div class="watchlist-actions">
                <button class="remove-symbol" onclick="window.watchlistManager.removeSymbol('${symbolData.symbol}')" title="Remove from watchlist">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;
    }
    
    updateStatus() {
        // Update symbols count
        const symbolsCount = document.getElementById('symbols-count');
        if (symbolsCount) {
            symbolsCount.textContent = this.watchlistSymbols.length.toString();
        }
        
        // Update last update time
        const lastUpdateTime = document.getElementById('last-update-time');
        if (lastUpdateTime) {
            const now = new Date();
            lastUpdateTime.textContent = now.toLocaleTimeString([], { 
                hour: '2-digit', 
                minute: '2-digit' 
            });
        }
        
        // Update market status (simplified)
        const marketStatus = document.getElementById('market-status');
        if (marketStatus) {
            const now = new Date();
            const hour = now.getHours();
            
            if (hour >= 9 && hour < 16) {
                marketStatus.textContent = 'Open';
            } else if (hour >= 16 && hour < 20) {
                marketStatus.textContent = 'After Hours';
            } else {
                marketStatus.textContent = 'Closed';
            }
        }
    }
    
    // Public method to remove symbol (called from HTML onclick)
    removeSymbol(symbol) {
        console.log('🔍 DEBUG: removeSymbol called for:', symbol);
        this.sendRemoveSymbolMessage(symbol);
    }
    

    showToast(message, type = 'info') {
        console.log(`Toast: ${message} (${type})`);
        if (window.showToast) {
            window.showToast(message, type);
        }
    }
}

// Global function for removing symbols (called from HTML)
function removeSymbol(symbol) {
    if (window.watchlistManager) {
        window.watchlistManager.removeSymbol(symbol);
    }
}
