# MenckMarket - Options Trading Platform

A comprehensive options trading platform with real-time data streaming, strategy automation, and risk management capabilities.

## Features

### 🎯 Trading Strategies
- **Iron Condor Strategy**: Automated iron condor options trading with risk management
- **PML Strategy**: Price momentum and liquidity-based trading signals
- **Divergence Strategy**: Technical divergence detection and trading automation

### 📊 Real-Time Data & Analytics
- Live options data streaming via WebSocket
- Technical indicators (RSI, MACD, Bollinger Bands, ATR)
- Real-time P&L tracking and analysis
- Market status monitoring

### 🛡️ Risk Management
- Configurable risk parameters (account risk, position sizing, stop losses)
- Real-time risk monitoring and alerts
- Automated position management
- Daily loss limits and equity buffers

### 🖥️ Web Dashboard
- Modern, responsive web interface
- Real-time position monitoring
- Strategy signal visualization
- Risk management controls
- Trading performance analytics

### 🔧 System Architecture
- **Backend**: Python-based trading engine with modular strategy system
- **Frontend**: HTML/CSS/JavaScript with real-time WebSocket updates
- **Data Storage**: JSON-based data persistence (database-free architecture)
- **Authentication**: Secure authentication system with session management
- **Notifications**: Multi-channel notification system (Email, Telegram, Slack)

## Project Structure

```
MenckMarket/
├── Core Trading Engine
│   ├── trading_engine.py          # Main trading orchestrator
│   ├── order_handler.py           # Order execution and management
│   └── risk_management_config.py  # Risk parameter configuration
│
├── Strategy Modules
│   ├── iron_condor_strategy.py    # Iron condor options strategy
│   ├── pml_strategy.py            # Price momentum liquidity strategy
│   └── divergence_strategy.py     # Technical divergence strategy
│
├── Data Handlers
│   ├── options_data_handler.py    # Options market data processing
│   ├── technical_indicators.py    # Technical analysis calculations
│   ├── pnl_data_handler.py        # P&L analysis and statistics
│   └── account_data_handler.py    # Account information management
│
├── Real-Time Systems
│   ├── websocket_server.py        # Real-time data streaming server
│   ├── realtime_monitor.py        # Live market monitoring
│   └── symbols_monitor_handler.py # Symbol watchlist management
│
├── Web Interface
│   ├── index.html                 # Main dashboard interface
│   ├── script.js                  # Frontend JavaScript logic
│   ├── styles.css                 # Dashboard styling
│   └── websocket_dashboard.html   # WebSocket testing interface
│
├── Authentication & Security
│   ├── auth_server.py             # Authentication server
│   ├── notification_server.py     # Multi-channel notifications
│   └── ssl/                       # SSL certificates (excluded)
│
└── Configuration & Utilities
    ├── config_loader.py           # Configuration management
    ├── connection_manager.py      # Connection pooling
    ├── alert_manager.py           # Alert system
    └── screener_handler.py        # Stock screening utilities
```

## Key Technologies

- **Python 3.8+**: Core trading engine and data processing
- **WebSockets**: Real-time data streaming
- **HTML5/CSS3/JavaScript**: Modern web interface
- **JSON**: Data persistence and configuration
- **SSL/TLS**: Secure communications
- **RESTful APIs**: External data integration

## Installation & Setup

### Prerequisites
- Python 3.8 or higher
- Modern web browser
- Internet connection for market data

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/MenckMarket.git
   cd MenckMarket
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements-auth.txt
   pip install -r requirements-notifications.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

4. **Start the trading engine**
   ```bash
   python trading_engine.py
   ```

5. **Start the WebSocket server**
   ```bash
   python websocket_server.py
   ```

6. **Open the web dashboard**
   - Open `index.html` in your web browser
   - Or serve via HTTP server for full functionality

## Configuration

### Risk Management
Configure risk parameters in `risk_config_live.json`:
- Maximum account risk percentage
- Daily loss limits
- Position sizing rules
- Stop loss settings

### Strategy Controls
Manage strategy automation in `auto_approve_config.json`:
- Enable/disable auto-execution
- Manual approval requirements
- Strategy-specific parameters

### Notifications
Set up alerts via multiple channels:
- Email notifications
- Telegram bot integration
- Slack webhook alerts

## Usage

### Web Dashboard
1. **Monitor Positions**: View real-time position data and P&L
2. **Strategy Signals**: Track trading signals from all strategies
3. **Risk Management**: Monitor and adjust risk parameters
4. **Performance Analytics**: Analyze trading performance metrics

### Trading Strategies
- **Automatic Mode**: Strategies execute trades automatically based on signals
- **Manual Approval**: Review and approve trades before execution
- **Risk Controls**: All trades subject to risk management rules

### Data Analysis
- Real-time technical indicators
- Historical performance tracking
- Risk metrics and compliance monitoring
- Market condition analysis

## Security Features

- SSL/TLS encryption for all communications
- Session-based authentication
- API key management
- Sensitive data exclusion via .gitignore
- Configurable security parameters

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is proprietary software. All rights reserved.

## Disclaimer

This software is for educational and research purposes. Trading involves substantial risk of loss. Users are responsible for their own trading decisions and should consult with financial advisors before making investment decisions.

## Support

For support and questions, please open an issue in the GitHub repository.

---

**MenckMarket** - Advanced Options Trading Platform
