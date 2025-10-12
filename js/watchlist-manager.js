// Asset Watchlist Management Module
class WatchlistManager {
    constructor() {
        this.watchlistSymbols = [];
        this.watchlistData = {};
        this.init();
    }
    
    init() {
        console.log('Initializing watchlist manager...');
        this.bindEvents();
        this.loadWatchlist();
    }
    
    bindEvents() {
        const addSymbolBtn = document.getElementById('add-symbol');
        const refreshWatchlistBtn = document.getElementById('refresh-watchlist');
        const confirmAddBtn = document.getElementById('confirm-add-symbol');
        const cancelAddBtn = document.getElementById('cancel-add-symbol');
        const symbolInput = document.getElementById('symbol-input');
        
        console.log('Binding watchlist events...', {
            addSymbolBtn: !!addSymbolBtn,
            refreshWatchlistBtn: !!refreshWatchlistBtn,
            confirmAddBtn: !!confirmAddBtn,
            cancelAddBtn: !!cancelAddBtn,
            symbolInput: !!symbolInput
        });
        
        if (addSymbolBtn) {
            addSymbolBtn.addEventListener('click', () => this.showAddSymbolForm());
        }
        
        if (refreshWatchlistBtn) {
            refreshWatchlistBtn.addEventListener('click', () => this.refreshWatchlist());
        }
        
        if (confirmAddBtn) {
            confirmAddBtn.addEventListener('click', () => this.confirmAddSymbol());
        }
        
        if (cancelAddBtn) {
            cancelAddBtn.addEventListener('click', () => this.hideAddSymbolForm());
        }
        
        if (symbolInput) {
            symbolInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.confirmAddSymbol();
                }
            });
        }
    }
    
    showAddSymbolForm() {
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
        const form = document.getElementById('add-symbol-form');
        const input = document.getElementById('symbol-input');
        
        if (form) {
            form.style.display = 'none';
            if (input) {
                input.value = '';
            }
        }
    }
    
    async confirmAddSymbol() {
        const input = document.getElementById('symbol-input');
        if (!input) return;
        
        const symbol = input.value.trim().toUpperCase();
        
        if (!symbol) {
            this.showToast('Please enter a valid symbol', 'warning');
            return;
        }
        
        if (this.watchlistSymbols.includes(symbol)) {
            this.showToast(`${symbol} is already in your watchlist`, 'warning');
            return;
        }
        
        // Show loading state
        const confirmBtn = document.getElementById('confirm-add-symbol');
        const originalContent = confirmBtn.innerHTML;
        confirmBtn.disabled = true;
        confirmBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Adding...';
        
        try {
            // Add symbol to watchlist
            this.watchlistSymbols.push(symbol);
            
            // Initialize with placeholder data
            this.watchlistData[symbol] = {
                symbol: symbol,
                current_price: 0.0,
                price_change: 0.0,
                price_change_percent: 0.0,
                volume: 0,
                last_updated: new Date().toISOString()
            };
            
            // Save to localStorage
            this.saveWatchlist();
            
            // Update display
            this.updateWatchlistDisplay();
            this.updateWatchlistStatus();
            
            // Hide form
            this.hideAddSymbolForm();
            
            // Integrate with live monitor system
            await this.integrateWithLiveMonitor(symbol);
            
            // Fetch initial price data
            await this.fetchSymbolPrice(symbol);
            
            // Show success message
            this.showToast(`Added ${symbol} to watchlist and live monitor`, 'success');
            
        } catch (error) {
            console.error('Error adding symbol:', error);
            this.showToast(`Failed to add ${symbol} to watchlist`, 'error');
            
            // Remove from watchlist if integration failed
            const index = this.watchlistSymbols.indexOf(symbol);
            if (index > -1) {
                this.watchlistSymbols.splice(index, 1);
                delete this.watchlistData[symbol];
                this.saveWatchlist();
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
            }
        } finally {
            // Restore button state
            confirmBtn.disabled = false;
            confirmBtn.innerHTML = originalContent;
        }
    }
    
    async removeSymbol(symbol) {
        const index = this.watchlistSymbols.indexOf(symbol);
        if (index > -1) {
            try {
                // Remove from API server first
                const apiResult = await this.removeSymbolFromLiveMonitor(symbol);
                
                if (apiResult.success) {
                    console.log(`Successfully removed ${symbol} from API server`);
                } else {
                    console.warn(`API removal failed for ${symbol}:`, apiResult.error);
                }
                
                // Remove from local arrays regardless of API result
                this.watchlistSymbols.splice(index, 1);
                delete this.watchlistData[symbol];
                
                this.saveWatchlist();
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
                
                this.showToast(`Removed ${symbol} from watchlist`, 'info');
                
            } catch (error) {
                console.error(`Error removing ${symbol}:`, error);
                
                // Still remove locally even if API call fails
                this.watchlistSymbols.splice(index, 1);
                delete this.watchlistData[symbol];
                
                this.saveWatchlist();
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
                
                this.showToast(`Removed ${symbol} from watchlist (API error)`, 'warning');
            }
        }
    }
    
    async refreshWatchlist() {
        const refreshBtn = document.getElementById('refresh-watchlist');
        if (!refreshBtn) return;
        
        const originalContent = refreshBtn.innerHTML;
        
        // Show loading state
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        
        try {
            // Fetch prices for all symbols
            const promises = this.watchlistSymbols.map(symbol => this.fetchSymbolPrice(symbol));
            await Promise.all(promises);
            
            // Update display
            this.updateWatchlistDisplay();
            this.updateWatchlistStatus();
            
            this.showToast('Watchlist refreshed successfully', 'success');
            
        } catch (error) {
            console.error('Failed to refresh watchlist:', error);
            this.showToast('Failed to refresh watchlist', 'error');
        } finally {
            // Restore button state
            refreshBtn.disabled = false;
            refreshBtn.innerHTML = originalContent;
        }
    }
    
    async fetchSymbolPrice(symbol) {
        try {
            // Fetch real data from live monitor JSON
            const response = await fetch('./live_monitor.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const liveData = await response.json();
            
            // Check if symbol exists in integrated_watchlist
            if (liveData.integrated_watchlist && liveData.integrated_watchlist.watchlist_data && liveData.integrated_watchlist.watchlist_data[symbol]) {
                const symbolData = liveData.integrated_watchlist.watchlist_data[symbol];
                
                this.watchlistData[symbol] = {
                    symbol: symbol,
                    current_price: symbolData.current_price || 0,
                    price_change: symbolData.price_change || 0,
                    price_change_percent: symbolData.price_change_percent || 0,
                    volume: symbolData.volume || 0,
                    market_cap: symbolData.market_cap || null,
                    last_updated: symbolData.last_updated || new Date().toISOString()
                };
                
                return this.watchlistData[symbol];
            }
            
            // If not in integrated_watchlist, check technical_indicators for price data
            if (liveData.technical_indicators && liveData.technical_indicators[symbol]) {
                const techData = liveData.technical_indicators[symbol];
                const quoteData = techData.quote_data || {};
                
                this.watchlistData[symbol] = {
                    symbol: symbol,
                    current_price: techData.current_price || 0,
                    price_change: quoteData.change || 0,
                    price_change_percent: quoteData.change_percent || 0,
                    volume: quoteData.volume || 0,
                    market_cap: null,
                    last_updated: techData.timestamp || new Date().toISOString()
                };
                
                return this.watchlistData[symbol];
            }
            
            // If symbol not found in live data, return null
            console.warn(`Symbol ${symbol} not found in live data`);
            return null;
            
        } catch (error) {
            console.error(`Failed to fetch price for ${symbol}:`, error);
            return null;
        }
    }
    
    updateWatchlistDisplay() {
        const watchlistList = document.getElementById('watchlist-list');
        const emptyState = document.getElementById('watchlist-empty');
        
        if (!watchlistList || !emptyState) return;
        
        if (this.watchlistSymbols.length === 0) {
            watchlistList.style.display = 'none';
            emptyState.style.display = 'block';
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
            if (!symbolData) return;
            
            const item = document.createElement('div');
            item.className = 'watchlist-item fade-in';
            item.innerHTML = this.createWatchlistItemHTML(symbolData);
            watchlistList.appendChild(item);
        });
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
    
    updateWatchlistStatus() {
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
    
    saveWatchlist() {
        try {
            const watchlistState = {
                symbols: this.watchlistSymbols,
                data: this.watchlistData,
                lastUpdated: new Date().toISOString()
            };
            localStorage.setItem('volflow-watchlist', JSON.stringify(watchlistState));
        } catch (error) {
            console.error('Failed to save watchlist:', error);
        }
    }
    
    async loadWatchlist() {
        try {
            console.log('Loading watchlist from API server...');
            
            // First try to load from API server
            const apiWatchlist = await this.loadWatchlistFromAPI();
            
            if (apiWatchlist && apiWatchlist.success) {
                console.log('Loaded watchlist from API:', apiWatchlist.symbols);
                
                // Use API data as the source of truth
                this.watchlistSymbols = apiWatchlist.symbols || [];
                
                // Initialize watchlist data for each symbol
                this.watchlistData = {};
                this.watchlistSymbols.forEach(symbol => {
                    this.watchlistData[symbol] = {
                        symbol: symbol,
                        current_price: 0.0,
                        price_change: 0.0,
                        price_change_percent: 0.0,
                        volume: 0,
                        last_updated: new Date().toISOString()
                    };
                });
                
                // Fetch current prices for all symbols
                await Promise.all(this.watchlistSymbols.map(symbol => this.fetchSymbolPrice(symbol)));
                
                // Update display
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
                
                // Save to localStorage as backup
                this.saveWatchlist();
                
                console.log(`✅ Loaded ${this.watchlistSymbols.length} symbols from API server`);
                
            } else {
                // Fallback to localStorage if API is not available
                console.log('API not available, falling back to localStorage...');
                await this.loadWatchlistFromLocalStorage();
            }
            
        } catch (error) {
            console.error('Failed to load watchlist from API:', error);
            // Fallback to localStorage
            await this.loadWatchlistFromLocalStorage();
        }
    }
    
    async loadWatchlistFromAPI() {
        try {
            const response = await fetch('http://localhost:8080/api/watchlist/symbols');
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            return result;
            
        } catch (error) {
            console.error('Error loading watchlist from API:', error);
            return null;
        }
    }
    
    async loadWatchlistFromLocalStorage() {
        try {
            const saved = localStorage.getItem('volflow-watchlist');
            if (saved) {
                const watchlistState = JSON.parse(saved);
                this.watchlistSymbols = watchlistState.symbols || [];
                this.watchlistData = watchlistState.data || {};
                
                // Update display
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
                
                console.log(`Loaded ${this.watchlistSymbols.length} symbols from localStorage`);
            } else {
                this.watchlistSymbols = [];
                this.watchlistData = {};
                this.updateWatchlistDisplay();
                this.updateWatchlistStatus();
            }
        } catch (error) {
            console.error('Failed to load watchlist from localStorage:', error);
            this.watchlistSymbols = [];
            this.watchlistData = {};
            this.updateWatchlistDisplay();
            this.updateWatchlistStatus();
        }
    }
    
    // Live Monitor Integration Functions
    async integrateWithLiveMonitor(symbol) {
        try {
            console.log(`Integrating ${symbol} with live monitor system...`);
            
            // Call the Python symbols monitor handler to add symbol
            const response = await this.addSymbolToLiveMonitor(symbol);
            
            if (response.success) {
                console.log(`Successfully integrated ${symbol} with live monitor`);
                return true;
            } else {
                throw new Error(response.error || 'Failed to integrate with live monitor');
            }
            
        } catch (error) {
            console.error(`Failed to integrate ${symbol} with live monitor:`, error);
            throw error;
        }
    }
    
    async addSymbolToLiveMonitor(symbol) {
        try {
            // Make HTTP POST request to the Python API server
            const response = await fetch('http://localhost:8080/api/watchlist/add', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    symbol: symbol,
                    timestamp: new Date().toISOString(),
                    source: 'web_interface'
                })
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.success) {
                console.log(`Successfully added ${symbol} to live monitor via API`);
                return result;
            } else {
                throw new Error(result.error || 'Failed to add symbol to live monitor');
            }
            
        } catch (error) {
            console.error(`Error adding ${symbol} to live monitor:`, error);
            
            // Fallback: try to add directly to watchlist file if API is not available
            console.log('API not available, using fallback method...');
            return await this.addSymbolFallback(symbol);
        }
    }
    
    async addSymbolFallback(symbol) {
        try {
            // Fallback method: add to localStorage and update local files
            const watchlistData = await this.loadWatchlistFromFile() || { symbols: [], watchlist_data: {} };
            
            if (!watchlistData.symbols.includes(symbol)) {
                watchlistData.symbols.push(symbol);
                watchlistData.watchlist_data[symbol] = {
                    symbol: symbol,
                    current_price: 0.0,
                    price_change: 0.0,
                    price_change_percent: 0.0,
                    volume: 0,
                    market_cap: null,
                    last_updated: new Date().toISOString()
                };
                watchlistData.last_updated = new Date().toISOString();
            }
            
            return {
                success: true,
                message: `${symbol} added to watchlist (fallback mode)`,
                timestamp: new Date().toISOString()
            };
            
        } catch (error) {
            console.error(`Fallback method failed for ${symbol}:`, error);
            return {
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    }
    
    async removeSymbolFromLiveMonitor(symbol) {
        try {
            // Make HTTP POST request to the Python API server
            const response = await fetch('http://localhost:8080/api/watchlist/remove', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    symbol: symbol,
                    timestamp: new Date().toISOString(),
                    source: 'web_interface'
                })
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.success) {
                console.log(`Successfully removed ${symbol} from live monitor via API`);
                return result;
            } else {
                throw new Error(result.error || 'Failed to remove symbol from live monitor');
            }
            
        } catch (error) {
            console.error(`Error removing ${symbol} from live monitor:`, error);
            return {
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    }
    
    async loadWatchlistFromFile() {
        try {
            // Load the watchlist data from the live monitor JSON file
            const response = await fetch('./live_monitor.json');
            if (response.ok) {
                const liveData = await response.json();
                // Return the integrated_watchlist section in the expected format
                const integratedWatchlist = liveData.integrated_watchlist || {};
                return {
                    symbols: integratedWatchlist.symbols || [],
                    watchlist_data: integratedWatchlist.watchlist_data || {},
                    last_updated: integratedWatchlist.metadata?.last_updated || new Date().toISOString()
                };
            }
            return null;
        } catch (error) {
            console.error('Failed to load watchlist from live monitor:', error);
            return null;
        }
    }
    
    // Update from WebSocket data
    updateFromWebSocket(data) {
        if (data.watchlist_data && Array.isArray(data.watchlist_data)) {
            console.log('📊 Updating watchlist from PostgreSQL WebSocket:', data.watchlist_data.length, 'symbols');
            
            // Update the global watchlist data with real-time PostgreSQL data
            this.watchlistSymbols = [];
            this.watchlistData = {};
            
            data.watchlist_data.forEach(symbolData => {
                const symbol = symbolData.symbol;
                this.watchlistSymbols.push(symbol);
                this.watchlistData[symbol] = {
                    symbol: symbol,
                    current_price: parseFloat(symbolData.current_price || 0),
                    price_change: parseFloat(symbolData.price_change || 0),
                    price_change_percent: parseFloat(symbolData.price_change_percent || 0),
                    volume: parseInt(symbolData.volume || 0),
                    last_updated: symbolData.timestamp || new Date().toISOString()
                };
            });
            
            // Update watchlist display with real-time data
            this.updateWatchlistDisplay();
            this.updateWatchlistStatus();
            
            console.log('✅ Watchlist updated with', this.watchlistSymbols.length, 'symbols from PostgreSQL database');
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

// Global function for removing symbols (called from HTML)
function removeSymbol(symbol) {
    if (window.watchlistManager) {
        window.watchlistManager.removeSymbol(symbol);
    }
}
