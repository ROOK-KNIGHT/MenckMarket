// Analytics and Trading Statistics Management Module - Simplified
class AnalyticsManager {
    constructor() {
        this.tradingStats = {};
        this.winLossChart = null;
        this.init();
    }
    
    init() {
        console.log('Initializing analytics manager...');
        this.bindEvents();
        this.loadDefaultStatistics();
        this.initializeChart();
    }
    
    bindEvents() {
        // No specific button events for analytics - it's mostly data-driven
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
    
    // Update from WebSocket data - simplified
    updateFromWebSocket(data) {
        if (data.trading_statistics) {
            console.log('📊 Processing trading statistics from PostgreSQL WebSocket');
            this.updateTradingStatisticsFromDatabase(data.trading_statistics);
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
