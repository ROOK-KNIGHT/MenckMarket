// Analytics and Trading Statistics Management Module - Simplified
class AnalyticsManager {
    constructor() {
        this.tradingStats = {};
        this.winLossChart = null;
        this.currentTimePeriod = '7days';
        this.init();
    }
    
    init() {
        console.log('Initializing analytics manager...');
        this.bindEvents();
        this.loadDefaultStatistics();
        this.initializeChart();
    }
    
    bindEvents() {
        // Bind time period selector change event
        const timePeriodSelect = document.getElementById('performance-time-period');
        if (timePeriodSelect) {
            timePeriodSelect.addEventListener('change', (e) => {
                this.handleTimePeriodChange(e.target.value);
            });
        }
        console.log('Analytics manager events bound');
    }
    
    updateTradingMetrics(metrics) {
        console.log('Updating trading metrics:', metrics);
        
        // Update win rate
        const winRateElement = document.getElementById('win-rate');
        if (winRateElement && metrics.win_rate !== undefined) {
            winRateElement.textContent = `${(metrics.win_rate * 100).toFixed(1)}%`;
        }
        
        // Update total trades
        const totalTradesElement = document.getElementById('total-trades');
        if (totalTradesElement && metrics.total_trades !== undefined) {
            totalTradesElement.textContent = metrics.total_trades.toLocaleString();
        }
        
        // Update average win
        const avgWinElement = document.getElementById('avg-win');
        if (avgWinElement && metrics.avg_win !== undefined) {
            avgWinElement.textContent = this.formatCurrency(metrics.avg_win);
            avgWinElement.className = `metric-value ${metrics.avg_win >= 0 ? 'positive' : 'negative'}`;
        }
        
        // Update average loss
        const avgLossElement = document.getElementById('avg-loss');
        if (avgLossElement && metrics.avg_loss !== undefined) {
            avgLossElement.textContent = this.formatCurrency(Math.abs(metrics.avg_loss));
            avgLossElement.className = `metric-value ${metrics.avg_loss <= 0 ? 'negative' : 'positive'}`;
        }
        
        // Update total P/L
        const totalPlElement = document.getElementById('total-pl');
        if (totalPlElement && metrics.total_pl !== undefined) {
            totalPlElement.textContent = this.formatCurrency(metrics.total_pl);
            totalPlElement.className = `metric-value ${metrics.total_pl >= 0 ? 'positive' : 'negative'}`;
        }
        
        // Update W/L ratio
        const wlRatioElement = document.getElementById('wl-ratio');
        if (wlRatioElement && metrics.win_loss_ratio !== undefined) {
            wlRatioElement.textContent = metrics.win_loss_ratio.toFixed(2);
        }
        
        // Store metrics for other components
        this.tradingStats = metrics;
        
        // Update the pie chart with new data
        this.updateChart(metrics);
    }
    
    loadDefaultStatistics() {
        console.log('Loading default trading statistics...');
        
        // Default/placeholder statistics
        const defaultMetrics = {
            total_trades: 0,
            win_rate: 0,
            avg_win: 0,
            avg_loss: 0,
            total_pl: 0,
            win_loss_ratio: 0
        };
        
        this.updateTradingMetrics(defaultMetrics);
    }
    
    // Handle time period change
    handleTimePeriodChange(newPeriod) {
        console.log(`📊 Time period changed to: ${newPeriod}`);
        this.currentTimePeriod = newPeriod;
        
        // Show loading state
        this.showLoadingState();
        
        // Request new data for the selected time period
        this.requestTradingStatistics(newPeriod);
        
        // Show toast notification
        this.showToast(`Loading ${this.getTimePeriodLabel(newPeriod)} trading statistics...`, 'info');
    }
    
    // Get human-readable label for time period
    getTimePeriodLabel(period) {
        const labels = {
            'today': 'Today\'s',
            '7days': '7 Days',
            '1month': '1 Month',
            '3months': '3 Months',
            '6months': '6 Months',
            '1year': '1 Year',
            'all': 'All Time'
        };
        return labels[period] || period;
    }
    
    // Show loading state for statistics
    showLoadingState() {
        const elements = [
            'win-rate', 'total-trades', 'avg-win', 
            'avg-loss', 'total-pl', 'wl-ratio'
        ];
        
        elements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = '...';
                element.style.opacity = '0.6';
            }
        });
    }
    
    // Hide loading state for statistics
    hideLoadingState() {
        const elements = [
            'win-rate', 'total-trades', 'avg-win', 
            'avg-loss', 'total-pl', 'wl-ratio'
        ];
        
        elements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.style.opacity = '1';
            }
        });
    }
    
    // Request trading statistics for specific time period
    requestTradingStatistics(timePeriod) {
        console.log(`📊 Requesting trading statistics for period: ${timePeriod}`);
        
        // Send WebSocket message to request statistics for specific time period
        if (window.websocketManager && window.websocketManager.isConnected()) {
            const message = {
                type: 'request_trading_statistics',
                time_period: timePeriod,
                timestamp: new Date().toISOString()
            };
            
            window.websocketManager.send(message);
            console.log('📊 Trading statistics request sent via WebSocket');
        } else {
            console.warn('📊 WebSocket not available, using mock data');
            // Fallback to mock data for demonstration
            setTimeout(() => {
                this.loadMockStatistics(timePeriod);
            }, 1000);
        }
    }
    
    // Load mock statistics for demonstration (when WebSocket is not available)
    loadMockStatistics(timePeriod) {
        const mockData = {
            'today': {
                total_trades: 3,
                win_rate: 0.667,
                avg_win: 125.50,
                avg_loss: -85.25,
                total_pl: 155.75,
                win_loss_ratio: 1.47
            },
            '7days': {
                total_trades: 15,
                win_rate: 0.6,
                avg_win: 145.75,
                avg_loss: -95.50,
                total_pl: 502.25,
                win_loss_ratio: 1.53
            },
            '1month': {
                total_trades: 42,
                win_rate: 0.571,
                avg_win: 165.25,
                avg_loss: -110.75,
                total_pl: 1250.50,
                win_loss_ratio: 1.49
            },
            '3months': {
                total_trades: 128,
                win_rate: 0.555,
                avg_win: 175.50,
                avg_loss: -125.25,
                total_pl: 2875.75,
                win_loss_ratio: 1.40
            },
            '6months': {
                total_trades: 256,
                win_rate: 0.547,
                avg_win: 185.75,
                avg_loss: -135.50,
                total_pl: 4250.25,
                win_loss_ratio: 1.37
            },
            '1year': {
                total_trades: 485,
                win_rate: 0.542,
                avg_win: 195.25,
                avg_loss: -145.75,
                total_pl: 7825.50,
                win_loss_ratio: 1.34
            },
            'all': {
                total_trades: 1247,
                win_rate: 0.538,
                avg_win: 205.50,
                avg_loss: -155.25,
                total_pl: 15750.75,
                win_loss_ratio: 1.32
            }
        };
        
        const stats = mockData[timePeriod] || mockData['7days'];
        this.updateTradingMetrics(stats);
        this.hideLoadingState();
        
        console.log(`📊 Mock statistics loaded for ${timePeriod}:`, stats);
    }

    // Update from WebSocket data - simplified
    updateFromWebSocket(data) {
        if (data.trading_statistics) {
            console.log('📊 Processing trading statistics from PostgreSQL WebSocket');
            this.updateTradingStatisticsFromDatabase(data.trading_statistics);
        }
        
        // Handle trading statistics response
        if (data.type === 'trading_statistics_response') {
            console.log('📊 Received trading statistics response:', data);
            this.hideLoadingState();
            
            if (data.success) {
                this.showToast(`Updated lookback period to ${data.lookback_days} days for ${this.getTimePeriodLabel(data.time_period)}`, 'success');
            } else {
                this.showToast(`Failed to update lookback period: ${data.error}`, 'error');
            }
        }
        
        // Handle trading statistics error
        if (data.type === 'trading_statistics_error') {
            console.log('❌ Trading statistics error:', data);
            this.hideLoadingState();
            this.showToast(`Error updating lookback period: ${data.error}`, 'error');
        }
    }
    
    updateTradingStatisticsFromDatabase(tradingStats) {
        console.log('📊 Updating trading statistics from PostgreSQL database:', tradingStats);
        
        // Map PostgreSQL data structure to our metrics format - simple mapping
        const metrics = {
            total_trades: tradingStats.total_trades || 0,
            win_rate: tradingStats.win_rate || 0,
            avg_win: tradingStats.avg_win || 0,
            avg_loss: tradingStats.avg_loss || 0,
            total_pl: tradingStats.total_pl || 0,
            win_loss_ratio: tradingStats.win_loss_ratio || 0
        };
        
        // Update the UI with the new metrics
        this.updateTradingMetrics(metrics);
        
        console.log('✅ Trading statistics updated from PostgreSQL database');
    }
    
    formatCurrency(amount) {
        const sign = amount >= 0 ? '+' : '';
        return `${sign}$${Math.abs(amount).toLocaleString('en-US', { 
            minimumFractionDigits: 2, 
            maximumFractionDigits: 2 
        })}`;
    }
    
    // Simple helper methods for other modules
    getTradingStatistics() {
        return this.tradingStats;
    }
    
    getWinRate() {
        return this.tradingStats.win_rate || 0;
    }
    
    getTotalTrades() {
        return this.tradingStats.total_trades || 0;
    }
    
    getTotalPnl() {
        return this.tradingStats.total_pl || 0;
    }
    
    initializeChart() {
        const chartCanvas = document.getElementById('winLossChart');
        if (!chartCanvas) {
            console.log('Win/Loss chart canvas not found');
            return;
        }

        const ctx = chartCanvas.getContext('2d');
        
        // Create simple pie chart for wins vs losses
        this.winLossChart = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: ['Wins', 'Losses'],
                datasets: [{
                    data: [0, 0], // Will be updated with real data
                    backgroundColor: [
                        '#10b981', // Green for wins
                        '#ef4444'  // Red for losses
                    ],
                    borderColor: [
                        '#059669',
                        '#dc2626'
                    ],
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 15,
                            font: {
                                size: 12
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label || '';
                                const value = context.parsed || 0;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                return `${label}: ${value} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });

        console.log('📊 Win/Loss pie chart initialized');
    }

    updateChart(metrics) {
        if (!this.winLossChart) {
            console.log('Chart not initialized, skipping update');
            return;
        }

        // Calculate wins and losses from metrics
        const totalTrades = metrics.total_trades || 0;
        const winRate = metrics.win_rate || 0;
        const wins = Math.round(totalTrades * winRate);
        const losses = totalTrades - wins;

        // Update chart data
        this.winLossChart.data.datasets[0].data = [wins, losses];
        this.winLossChart.update('none'); // Update without animation for better performance

        console.log(`📊 Updated pie chart: ${wins} wins, ${losses} losses`);
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

// Export to global scope for app.js to access
window.AnalyticsManager = AnalyticsManager;
