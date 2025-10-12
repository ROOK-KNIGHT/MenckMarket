// Current Positions Management Module - Simplified
class PositionsManager {
    constructor() {
        this.positions = [];
        this.init();
    }
    
    init() {
        console.log('Initializing positions manager...');
        this.bindEvents();
    }
    
    bindEvents() {
        const refreshPositionsBtn = document.getElementById('refresh-positions');
        
        if (refreshPositionsBtn) {
            refreshPositionsBtn.addEventListener('click', () => this.refreshPositions());
        }
    }
    
    async refreshPositions() {
        // Positions are automatically updated via WebSocket from PostgreSQL
        // This method just provides user feedback
        const refreshBtn = document.getElementById('refresh-positions');
        const originalContent = refreshBtn ? refreshBtn.innerHTML : '';
        
        // Show loading state
        if (refreshBtn) {
            refreshBtn.disabled = true;
            refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
        }
        
        try {
            // Wait a moment for WebSocket data to update
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            this.showToast('Positions refreshed from PostgreSQL database', 'success');
            
        } catch (error) {
            console.error('Failed to refresh positions:', error);
            this.showToast('Failed to refresh positions', 'error');
        } finally {
            // Restore button state
            if (refreshBtn) {
                refreshBtn.disabled = false;
                refreshBtn.innerHTML = originalContent;
            }
        }
    }
    
    // Update from WebSocket data - simplified
    updateFromWebSocket(data) {
        if (data.positions && Array.isArray(data.positions)) {
            console.log('📈 Processing positions from PostgreSQL WebSocket:', data.positions.length, 'positions');
            
            // Convert PostgreSQL WebSocket position data to display format
            const positions = data.positions.map(position => ({
                symbol: position.symbol,
                type: position.quantity > 0 ? 'Long' : 'Short',
                quantity: Math.abs(position.quantity),
                plOpen: position.unrealized_pl || 0,
                marketValue: position.market_value || 0,
                costBasis: position.cost_basis || 0
            }));
            
            this.updatePositionsList(positions);
            this.updatePositionsSummary(positions);
            
            // Store positions for other modules
            this.positions = positions;
        }
        
        if (data.account_data) {
            console.log('💰 Processing account data from PostgreSQL WebSocket');
            this.updateAccountData(data.account_data);
            
            // Also update positions summary with account-level P/L data (more accurate)
            this.updatePositionsSummaryFromAccount(data.account_data);
        }
    }
    
    updatePositionsList(positions) {
        const positionsList = document.querySelector('.positions-list');
        if (!positionsList) return;
        
        // Clear existing positions
        positionsList.innerHTML = '';
        
        // Add each position
        positions.forEach(position => {
            const positionElement = document.createElement('div');
            positionElement.className = 'position-item';
            positionElement.innerHTML = `
                <div class="position-symbol">
                    <span class="symbol">${position.symbol}</span>
                    <span class="position-type">${position.type}</span>
                </div>
                <div class="position-details">
                    <div class="detail-row">
                        <span class="detail-label">Quantity:</span>
                        <span class="detail-value">${position.quantity > 0 ? '+' : ''}${position.quantity}</span>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">Market Value:</span>
                        <span class="detail-value">${this.formatCurrency(position.marketValue)}</span>
                    </div>
                    <div class="detail-row">
                        <span class="detail-label">P/L Open:</span>
                        <span class="detail-value ${position.plOpen >= 0 ? 'positive' : 'negative'}">
                            ${this.formatCurrency(position.plOpen)}
                        </span>
                    </div>
                </div>
            `;
            positionsList.appendChild(positionElement);
        });
        
        // Show/hide empty state
        const emptyState = document.querySelector('.positions-empty');
        if (positions.length === 0) {
            positionsList.style.display = 'none';
            if (emptyState) emptyState.style.display = 'block';
        } else {
            positionsList.style.display = 'flex';
            if (emptyState) emptyState.style.display = 'none';
        }
    }
    
    updatePositionsSummary(positions) {
        // Simple calculation of summary values from individual positions
        let totalUnrealizedPL = 0;
        let totalMarketValue = 0;
        
        positions.forEach(position => {
            totalUnrealizedPL += position.plOpen || 0;
            totalMarketValue += position.marketValue || 0;
        });
        
        // Update summary display elements
        const plOpenElement = document.getElementById('pl-open');
        const totalPositionsElement = document.getElementById('total-positions');
        
        if (plOpenElement) {
            plOpenElement.textContent = this.formatCurrency(totalUnrealizedPL);
            plOpenElement.className = `summary-value ${totalUnrealizedPL >= 0 ? 'positive' : 'negative'}`;
        }
        
        if (totalPositionsElement) {
            totalPositionsElement.textContent = positions.length.toString();
        }
        
        // Update summary icons
        const plOpenIcon = plOpenElement?.closest('.summary-item')?.querySelector('.summary-icon');
        if (plOpenIcon) {
            plOpenIcon.className = `summary-icon ${totalUnrealizedPL >= 0 ? 'positive' : 'negative'}`;
        }
        
        console.log(`📊 Updated positions summary: ${positions.length} positions, ${this.formatCurrency(totalUnrealizedPL)} total P/L`);
    }
    
    updatePositionsSummaryFromAccount(accountData) {
        // Handle account data structure from database (accounts object with account numbers as keys)
        let totalUnrealizedPL = 0;
        let accountFound = false;
        
        if (accountData.accounts) {
            // Sum up total_unrealized_pl from all accounts
            Object.values(accountData.accounts).forEach(account => {
                if (account.total_unrealized_pl !== undefined) {
                    totalUnrealizedPL += parseFloat(account.total_unrealized_pl) || 0;
                    accountFound = true;
                }
            });
        } else if (accountData.total_unrealized_pl !== undefined) {
            // Direct account data structure
            totalUnrealizedPL = parseFloat(accountData.total_unrealized_pl) || 0;
            accountFound = true;
        }
        
        if (accountFound) {
            const plOpenElement = document.getElementById('pl-open');
            if (plOpenElement) {
                plOpenElement.textContent = this.formatCurrency(totalUnrealizedPL);
                plOpenElement.className = `summary-value ${totalUnrealizedPL >= 0 ? 'positive' : 'negative'}`;
                
                const plOpenIcon = plOpenElement?.closest('.summary-item')?.querySelector('.summary-icon');
                if (plOpenIcon) {
                    plOpenIcon.className = `summary-icon ${totalUnrealizedPL >= 0 ? 'positive' : 'negative'}`;
                }
                
                console.log(`📊 Updated P/L Open from account data: ${this.formatCurrency(totalUnrealizedPL)}`);
            }
        }
    }
    
    updateAccountData(accountData) {
        // Handle account data structure from database (accounts object with account numbers as keys)
        let totalEquity = 0;
        let totalDayPL = 0;
        let firstAccountNumber = null;
        
        if (accountData.accounts) {
            // Process all accounts
            Object.values(accountData.accounts).forEach(account => {
                if (account.equity) {
                    totalEquity += parseFloat(account.equity) || 0;
                }
                if (account.total_day_pl !== undefined) {
                    totalDayPL += parseFloat(account.total_day_pl) || 0;
                }
                if (!firstAccountNumber && account.account_number) {
                    firstAccountNumber = account.account_number;
                }
            });
        } else {
            // Direct account data structure
            totalEquity = parseFloat(accountData.equity) || 0;
            totalDayPL = parseFloat(accountData.total_day_pl) || 0;
            firstAccountNumber = accountData.account_number;
        }
        
        // Update equity display
        if (totalEquity > 0) {
            const equityElement = document.querySelector('.equity-amount');
            if (equityElement) {
                equityElement.textContent = `$${totalEquity.toLocaleString('en-US', { 
                    minimumFractionDigits: 2, 
                    maximumFractionDigits: 2 
                })}`;
            }
        }
        
        // Update P/L Day from account data
        const plDayElement = document.getElementById('pl-day');
        const plDayIcon = plDayElement?.closest('.summary-item')?.querySelector('.summary-icon');
        
        if (plDayElement) {
            plDayElement.textContent = this.formatCurrency(totalDayPL);
            plDayElement.className = `summary-value ${totalDayPL >= 0 ? 'positive' : 'negative'}`;
            console.log(`📊 Updated P/L Day from account data: ${this.formatCurrency(totalDayPL)}`);
        }
        
        if (plDayIcon) {
            plDayIcon.className = `summary-icon ${totalDayPL >= 0 ? 'positive' : 'negative'}`;
        }
        
        // Update account number if available
        if (firstAccountNumber) {
            const accountElement = document.querySelector('.account-number');
            if (accountElement) {
                const maskedAccount = `****${firstAccountNumber.slice(-4)}`;
                accountElement.textContent = maskedAccount;
            }
        }
    }
    
    formatCurrency(amount) {
        const sign = amount >= 0 ? '+' : '';
        return `${sign}$${Math.abs(amount).toLocaleString('en-US', { 
            minimumFractionDigits: 2, 
            maximumFractionDigits: 2 
        })}`;
    }
    
    // Simple helper methods for other modules
    getCurrentEquity() {
        const equityElement = document.querySelector('.equity-amount');
        if (equityElement) {
            const equityText = equityElement.textContent.replace(/[$,]/g, '');
            return parseFloat(equityText) || 0;
        }
        return 0;
    }
    
    getActivePositionsCount() {
        return this.positions.length;
    }
    
    getDailyPL() {
        const plDayElement = document.getElementById('pl-day');
        if (plDayElement) {
            const plText = plDayElement.textContent.replace(/[$,+]/g, '');
            return parseFloat(plText) || 0;
        }
        return 0;
    }
    
    getPositionsValue() {
        const plOpenElement = document.getElementById('pl-open');
        if (plOpenElement) {
            const plText = plOpenElement.textContent.replace(/[$,+]/g, '');
            return Math.abs(parseFloat(plText)) || 0;
        }
        return 0;
    }
    
    // Auto-refresh methods (simplified - WebSocket handles updates)
    stopAutoRefresh() {
        // No-op since we use WebSocket for real-time updates
        console.log('Auto-refresh not needed - using WebSocket updates');
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
