/**
 * Trading Controls Manager
 * Handles emergency controls only:
 * - Emergency stop (adjusts other toggles)
 * - Close all positions (direct script call)
 * - Pause (toggles auto-execute)
 */

class TradingControlsManager {
    constructor() {
        this.emergencyStopActive = false;
        this.init();
    }

    init() {
        console.log('🎛️ Initializing Trading Controls Manager...');
        this.bindEventListeners();
        console.log('✅ Trading Controls Manager initialized');
    }

    bindEventListeners() {
        // Emergency stop
        const emergencyStop = document.getElementById('emergency-stop');
        if (emergencyStop) {
            emergencyStop.addEventListener('click', () => {
                this.handleEmergencyStop();
            });
        }

        // Close all positions
        const closeAllPositions = document.getElementById('close-all-positions');
        if (closeAllPositions) {
            closeAllPositions.addEventListener('click', () => {
                this.handleCloseAllPositions();
            });
        }

        // Pause trading
        const pauseTrading = document.getElementById('pause-trading');
        if (pauseTrading) {
            pauseTrading.addEventListener('click', () => {
                this.handlePauseTrading();
            });
        }
    }

    handleEmergencyStop() {
        console.log('🚨 Emergency stop triggered');
        
        this.emergencyStopActive = !this.emergencyStopActive;
        
        if (this.emergencyStopActive) {
            if (!confirm('⚠️ EMERGENCY STOP\n\nThis will turn off all auto-execute toggles.\n\nAre you sure?')) {
                this.emergencyStopActive = false;
                return;
            }
            this.activateEmergencyStop();
        } else {
            if (!confirm('🔄 DEACTIVATE EMERGENCY STOP\n\nThis will allow auto-execute to be turned back on.\n\nAre you sure?')) {
                this.emergencyStopActive = true;
                return;
            }
            this.deactivateEmergencyStop();
        }
    }
    
    activateEmergencyStop() {
        console.log('🚨 Activating emergency stop...');
        
        // Turn off all auto-execute toggles
        this.disableAllAutoExecuteToggles();
        
        // Update emergency stop button appearance
        this.updateEmergencyStopButton(true);
        
        console.log('🚨 Emergency stop activated');
    }
    
    deactivateEmergencyStop() {
        console.log('🔄 Deactivating emergency stop...');
        
        // Update emergency stop button appearance
        this.updateEmergencyStopButton(false);
        
        console.log('✅ Emergency stop deactivated');
    }
    
    disableAllAutoExecuteToggles() {
        const autoExecuteToggles = [
            'iron_condor_auto_execute',
            'pml_auto_execute', 
            'divergence_auto_execute'
        ];
        
        autoExecuteToggles.forEach(toggleId => {
            const toggle = document.getElementById(toggleId);
            if (toggle && toggle.checked) {
                console.log(`🔄 Turning off auto-execute for ${toggleId}`);
                toggle.checked = false;
                toggle.dispatchEvent(new Event('change', { bubbles: true }));
            }
        });
    }
    
    updateEmergencyStopButton(isActive) {
        const emergencyStopBtn = document.getElementById('emergency-stop');
        if (emergencyStopBtn) {
            if (isActive) {
                emergencyStopBtn.classList.add('emergency-active');
                emergencyStopBtn.innerHTML = '<i class="fas fa-stop-circle"></i> EMERGENCY STOP ACTIVE';
                emergencyStopBtn.style.backgroundColor = '#dc3545';
                emergencyStopBtn.style.color = 'white';
            } else {
                emergencyStopBtn.classList.remove('emergency-active');
                emergencyStopBtn.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Emergency Stop';
                emergencyStopBtn.style.backgroundColor = '';
                emergencyStopBtn.style.color = '';
            }
        }
    }

    handleCloseAllPositions() {
        console.log('🔄 Close all positions requested');
        
        if (!confirm('⚠️ CLOSE ALL POSITIONS\n\nThis will cancel ALL orders and close ALL positions via the close all positions script.\n\nThis cannot be undone!\n\nAre you sure?')) {
            return;
        }

        // Direct call to close all positions script via WebSocket
        if (window.webSocketManager && window.webSocketManager.isConnected()) {
            window.webSocketManager.send({
                type: 'close_all_positions',
                timestamp: new Date().toISOString(),
                reason: 'User requested close all positions'
            });
            console.log('📤 Close all positions request sent');
        } else {
            console.error('❌ WebSocket not available for close all positions');
        }
    }

    handlePauseTrading() {
        console.log('⏸️ Pause trading - toggling auto-execute toggles');
        
        // Toggle all auto-execute toggles off (pause)
        this.disableAllAutoExecuteToggles();
        
        console.log('⏸️ All auto-execute toggles paused');
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.tradingControlsManager = new TradingControlsManager();
});

// Export for external access
window.TradingControlsManager = TradingControlsManager;
