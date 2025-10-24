// Settings and Authentication Management Module
class SettingsManager {
    constructor() {
        this.settings = {};
        this.notificationSettings = {};
        this.securitySettings = {};
        this.alertsConfig = null;
        this.init();
    }
    
    init() {
        console.log('Initializing settings manager...');
        this.bindEvents();
        this.loadSettings();
        this.loadAlertsConfig();
        this.initializeNotificationToggles();
    }
    
    bindEvents() {
        // Main settings buttons
        const saveAllBtn = document.getElementById('save-all-settings');
        const logoutBtn = document.getElementById('logout-btn');
        const saveSecurityBtn = document.getElementById('save-security-settings');
        const logoutSecurityBtn = document.getElementById('logout-security-btn');
        
        if (saveAllBtn) saveAllBtn.addEventListener('click', () => this.saveAllSettings());
        if (logoutBtn) logoutBtn.addEventListener('click', () => this.logout());
        if (saveSecurityBtn) saveSecurityBtn.addEventListener('click', () => this.saveSecuritySettings());
        if (logoutSecurityBtn) logoutSecurityBtn.addEventListener('click', () => this.logout());
        
        // Password management
        this.bindPasswordManagementEvents();
        
        // Notification settings
        this.bindNotificationEvents();
        
        // Session management
        this.bindSessionManagementEvents();
        
        // Test notifications
        const testNotificationsBtn = document.getElementById('test-notifications');
        if (testNotificationsBtn) {
            testNotificationsBtn.addEventListener('click', () => this.testNotifications());
        }
    }
    
    bindPasswordManagementEvents() {
        const changePasswordBtn = document.getElementById('change-password-btn');
        const savePasswordBtn = document.getElementById('save-password-btn');
        const cancelPasswordBtn = document.getElementById('cancel-password-btn');
        
        if (changePasswordBtn) {
            changePasswordBtn.addEventListener('click', () => this.showPasswordChangeForm());
        }
        
        if (savePasswordBtn) {
            savePasswordBtn.addEventListener('click', () => this.savePassword());
        }
        
        if (cancelPasswordBtn) {
            cancelPasswordBtn.addEventListener('click', () => this.hidePasswordChangeForm());
        }
        
        // Password validation
        const newPasswordInput = document.getElementById('new-password');
        const confirmPasswordInput = document.getElementById('confirm-password');
        
        if (newPasswordInput) {
            newPasswordInput.addEventListener('input', () => this.validatePassword());
        }
        
        if (confirmPasswordInput) {
            confirmPasswordInput.addEventListener('input', () => this.validatePasswordConfirmation());
        }
    }
    
    bindNotificationEvents() {
        console.log('🔔 Binding notification events...');
        
        // Notification channel toggles
        const notificationToggles = [
            'enable-email-notifications',
            'enable-telegram-notifications',
            'enable-slack-notifications'
        ];
        
        notificationToggles.forEach(toggleId => {
            const toggle = document.getElementById(toggleId);
            console.log(`🔔 Setting up toggle for ${toggleId}:`, toggle);
            if (toggle) {
                this.setupToggleEventListeners(toggle, toggleId, (toggleElement) => {
                    console.log(`🔔 Notification toggle changed: ${toggleId}`, toggleElement.checked);
                    this.handleNotificationToggle(toggleElement);
                });
            } else {
                console.warn(`🔔 Toggle element not found: ${toggleId}`);
            }
        });
        
        // Alert type toggles
        const alertToggles = [
            'enable-max-loss-alert',
            'enable-volatility-alert',
            'enable-position-size-alert',
            'enable-signal-alert'
        ];
        
        alertToggles.forEach(toggleId => {
            const toggle = document.getElementById(toggleId);
            if (toggle) {
                this.setupToggleEventListeners(toggle, toggleId, (toggleElement) => {
                    this.handleAlertToggle(toggleElement);
                });
            }
        });
        
        // Notification preference controls
        const alertFrequency = document.getElementById('alert-frequency');
        const quietHoursToggle = document.getElementById('enable-quiet-hours');
        
        if (alertFrequency) {
            alertFrequency.addEventListener('change', (e) => this.handleAlertFrequencyChange(e.target.value));
        }
        
        if (quietHoursToggle) {
            this.setupToggleEventListeners(quietHoursToggle, 'enable-quiet-hours', (toggleElement) => {
                this.handleQuietHoursToggle(toggleElement);
            });
        }
        
        // Notification input fields
        const notificationInputs = [
            'notification-email',
            'telegram-bot-token',
            'telegram-chat-id',
            'slack-webhook-url',
            'slack-channel',
            'max-loss-threshold',
            'vix-alert-threshold',
            'quiet-hours-start',
            'quiet-hours-end'
        ];
        
        notificationInputs.forEach(inputId => {
            const input = document.getElementById(inputId);
            if (input) {
                input.addEventListener('change', (e) => this.handleNotificationInputChange(e.target));
            }
        });
    }
    
    bindSessionManagementEvents() {
        const enableAutoLogout = document.getElementById('enable-auto-logout');
        const autoLogoutTimeout = document.getElementById('auto-logout-timeout');
        const logoutCurrentSession = document.getElementById('logout-current-session');
        const viewLoginHistory = document.getElementById('view-login-history');
        const enable2FA = document.getElementById('enable-2fa');
        const setup2FABtn = document.getElementById('setup-2fa-btn');
        
        if (enableAutoLogout) {
            this.setupToggleEventListeners(enableAutoLogout, 'enable-auto-logout', (toggleElement) => {
                this.handleAutoLogoutToggle(toggleElement);
            });
        }
        
        if (autoLogoutTimeout) {
            autoLogoutTimeout.addEventListener('change', (e) => this.handleAutoLogoutTimeoutChange(e.target.value));
        }
        
        if (logoutCurrentSession) {
            logoutCurrentSession.addEventListener('click', () => this.logout());
        }
        
        if (viewLoginHistory) {
            viewLoginHistory.addEventListener('click', () => this.viewLoginHistory());
        }
        
        if (enable2FA) {
            this.setupToggleEventListeners(enable2FA, 'enable-2fa', (toggleElement) => {
                this.handle2FAToggle(toggleElement);
            });
        }
        
        if (setup2FABtn) {
            setup2FABtn.addEventListener('click', () => this.setup2FA());
        }
    }
    
    setupToggleEventListeners(toggle, toggleId, handlerFunction) {
        // Add event listener to the checkbox input
        toggle.addEventListener('change', (e) => {
            handlerFunction(e.target);
        });
        
        // Find the parent toggle switch container
        const toggleSwitch = toggle.closest('.toggle-switch');
        if (toggleSwitch) {
            // Add click event listener to the entire toggle switch area
            toggleSwitch.addEventListener('click', (e) => {
                if (e.target === toggle) return;
                toggle.checked = !toggle.checked;
                toggle.dispatchEvent(new Event('change'));
            });
            
            // Add click event listener to the toggle slider specifically
            const toggleSlider = toggleSwitch.querySelector('.toggle-slider');
            if (toggleSlider) {
                toggleSlider.addEventListener('click', (e) => {
                    e.stopPropagation();
                    toggle.checked = !toggle.checked;
                    toggle.dispatchEvent(new Event('change'));
                });
            }
        }
    }
    
    // Password Management
    showPasswordChangeForm() {
        const forms = [
            'password-change-form',
            'new-password-form',
            'confirm-password-form',
            'password-actions'
        ];
        
        forms.forEach(formId => {
            const form = document.getElementById(formId);
            if (form) {
                form.style.display = 'flex';
            }
        });
        
        // Focus on current password field
        const currentPasswordInput = document.getElementById('current-password');
        if (currentPasswordInput) {
            currentPasswordInput.focus();
        }
    }
    
    hidePasswordChangeForm() {
        const forms = [
            'password-change-form',
            'new-password-form',
            'confirm-password-form',
            'password-actions'
        ];
        
        forms.forEach(formId => {
            const form = document.getElementById(formId);
            if (form) {
                form.style.display = 'none';
            }
        });
        
        // Clear password fields
        const passwordInputs = [
            'current-password',
            'new-password',
            'confirm-password'
        ];
        
        passwordInputs.forEach(inputId => {
            const input = document.getElementById(inputId);
            if (input) {
                input.value = '';
                input.classList.remove('error', 'success');
            }
        });
    }
    
    validatePassword() {
        const newPasswordInput = document.getElementById('new-password');
        if (!newPasswordInput) return;
        
        const password = newPasswordInput.value;
        const minLength = 8;
        
        // Remove existing classes
        newPasswordInput.classList.remove('error', 'success');
        
        if (password.length < minLength) {
            newPasswordInput.classList.add('error');
            return false;
        } else {
            newPasswordInput.classList.add('success');
            return true;
        }
    }
    
    validatePasswordConfirmation() {
        const newPasswordInput = document.getElementById('new-password');
        const confirmPasswordInput = document.getElementById('confirm-password');
        
        if (!newPasswordInput || !confirmPasswordInput) return;
        
        const newPassword = newPasswordInput.value;
        const confirmPassword = confirmPasswordInput.value;
        
        // Remove existing classes
        confirmPasswordInput.classList.remove('error', 'success');
        
        if (confirmPassword && newPassword !== confirmPassword) {
            confirmPasswordInput.classList.add('error');
            return false;
        } else if (confirmPassword && newPassword === confirmPassword) {
            confirmPasswordInput.classList.add('success');
            return true;
        }
        
        return false;
    }
    
    async savePassword() {
        const currentPasswordInput = document.getElementById('current-password');
        const newPasswordInput = document.getElementById('new-password');
        const confirmPasswordInput = document.getElementById('confirm-password');
        
        if (!currentPasswordInput || !newPasswordInput || !confirmPasswordInput) return;
        
        const currentPassword = currentPasswordInput.value;
        const newPassword = newPasswordInput.value;
        const confirmPassword = confirmPasswordInput.value;
        
        // Validate inputs
        if (!currentPassword) {
            this.showToast('Please enter your current password', 'error');
            return;
        }
        
        if (!this.validatePassword()) {
            this.showToast('New password must be at least 8 characters long', 'error');
            return;
        }
        
        if (!this.validatePasswordConfirmation()) {
            this.showToast('Password confirmation does not match', 'error');
            return;
        }
        
        // Show loading state
        const saveBtn = document.getElementById('save-password-btn');
        const originalContent = saveBtn.innerHTML;
        saveBtn.disabled = true;
        saveBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving...';
        
        try {
            // Send password change request via WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'change_password',
                    current_password: currentPassword,
                    new_password: newPassword,
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 Password change request sent via WebSocket');
            }
            
            // Hide password change form
            this.hidePasswordChangeForm();
            
            this.showToast('Password changed successfully', 'success');
            
        } catch (error) {
            console.error('Error changing password:', error);
            this.showToast('Failed to change password', 'error');
        } finally {
            // Restore button state
            saveBtn.disabled = false;
            saveBtn.innerHTML = originalContent;
        }
    }
    
    // Notification Management
    handleNotificationToggle(toggle) {
        const isEnabled = toggle.checked;
        const toggleId = toggle.id;
        
        // Store notification setting
        this.notificationSettings[toggleId] = isEnabled;
        
        // Show/hide related settings
        this.updateNotificationSettingsVisibility(toggleId, isEnabled);
        
        // Auto-save settings
        this.saveNotificationSettings();
        
        // Auto-save to alerts config JSON file
        this.saveAlertsConfig();
        
        // Show toast notification
        const channelName = this.getNotificationChannelName(toggleId);
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`${channelName} notifications ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    updateNotificationSettingsVisibility(toggleId, isEnabled) {
        const settingsMappings = {
            'enable-email-notifications': 'email-settings',
            'enable-telegram-notifications': ['telegram-settings', 'telegram-chat-settings'],
            'enable-slack-notifications': ['slack-settings', 'slack-channel-settings']
        };
        
        const relatedSettings = settingsMappings[toggleId];
        if (relatedSettings) {
            const settingsIds = Array.isArray(relatedSettings) ? relatedSettings : [relatedSettings];
            
            settingsIds.forEach(settingId => {
                const settingElement = document.getElementById(settingId);
                if (settingElement) {
                    settingElement.style.display = isEnabled ? 'flex' : 'none';
                }
            });
        }
    }
    
    handleAlertToggle(toggle) {
        const isEnabled = toggle.checked;
        const toggleId = toggle.id;
        
        // Store alert setting
        this.notificationSettings[toggleId] = isEnabled;
        
        // Show/hide related settings
        if (toggleId === 'enable-max-loss-alert') {
            const maxLossSettings = document.getElementById('max-loss-settings');
            if (maxLossSettings) {
                maxLossSettings.style.display = isEnabled ? 'flex' : 'none';
            }
        } else if (toggleId === 'enable-volatility-alert') {
            const volatilitySettings = document.getElementById('volatility-settings');
            if (volatilitySettings) {
                volatilitySettings.style.display = isEnabled ? 'flex' : 'none';
            }
        }
        
        // Auto-save settings
        this.saveNotificationSettings();
        
        // Auto-save to alerts config JSON file
        this.saveAlertsConfig();
        
        // Show toast notification
        const alertName = this.getAlertTypeName(toggleId);
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`${alertName} ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    handleQuietHoursToggle(toggle) {
        const isEnabled = toggle.checked;
        
        // Store quiet hours setting
        this.notificationSettings['enable-quiet-hours'] = isEnabled;
        
        // Show/hide quiet hours settings
        const quietHoursSettings = document.getElementById('quiet-hours-settings');
        if (quietHoursSettings) {
            quietHoursSettings.style.display = isEnabled ? 'flex' : 'none';
        }
        
        // Auto-save settings
        this.saveNotificationSettings();
        
        // Auto-save to alerts config JSON file
        this.saveAlertsConfig();
        
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`Quiet hours ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    handleAlertFrequencyChange(frequency) {
        this.notificationSettings['alert-frequency'] = frequency;
        this.saveNotificationSettings();
        
        // Auto-save to alerts config JSON file
        this.saveAlertsConfig();
        
        this.showToast(`Alert frequency set to ${frequency}`, 'info');
    }
    
    handleNotificationInputChange(input) {
        const inputId = input.id;
        const value = input.value;
        
        // Store notification input value
        this.notificationSettings[inputId] = value;
        
        // Auto-save settings
        this.saveNotificationSettings();
        
        // Auto-save to alerts config JSON file
        this.saveAlertsConfig();
    }
    
    getNotificationChannelName(toggleId) {
        const channelNames = {
            'enable-email-notifications': 'Email',
            'enable-telegram-notifications': 'Telegram',
            'enable-slack-notifications': 'Slack'
        };
        return channelNames[toggleId] || 'Notification';
    }
    
    getAlertTypeName(toggleId) {
        const alertNames = {
            'enable-max-loss-alert': 'Maximum loss alerts',
            'enable-volatility-alert': 'Volatility spike alerts',
            'enable-position-size-alert': 'Position size alerts',
            'enable-signal-alert': 'Strategy signal alerts'
        };
        return alertNames[toggleId] || 'Alert';
    }
    
    async testNotifications() {
        console.log('Testing notifications...');
        
        const testBtn = document.getElementById('test-notifications');
        const originalContent = testBtn.innerHTML;
        testBtn.disabled = true;
        testBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing...';
        
        try {
            // Send test notification request via WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'test_notifications',
                    settings: this.getNotificationConfiguration(),
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 Test notification request sent via WebSocket');
            }
            
            this.showToast('Test notifications sent successfully', 'success');
            
        } catch (error) {
            console.error('Error sending test notifications:', error);
            this.showToast('Failed to send test notifications', 'error');
        } finally {
            // Restore button state
            testBtn.disabled = false;
            testBtn.innerHTML = originalContent;
        }
    }
    
    // Session Management
    handleAutoLogoutToggle(toggle) {
        const isEnabled = toggle.checked;
        
        // Store auto logout setting
        this.securitySettings['enable-auto-logout'] = isEnabled;
        
        // Show/hide auto logout settings
        const autoLogoutSettings = document.getElementById('auto-logout-settings');
        if (autoLogoutSettings) {
            autoLogoutSettings.style.display = isEnabled ? 'flex' : 'none';
        }
        
        // Auto-save settings
        this.saveSecuritySettings();
        
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`Auto logout ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    handleAutoLogoutTimeoutChange(timeout) {
        this.securitySettings['auto-logout-timeout'] = timeout;
        this.saveSecuritySettings();
        this.showToast(`Auto logout timeout set to ${timeout} minutes`, 'info');
    }
    
    handle2FAToggle(toggle) {
        const isEnabled = toggle.checked;
        
        // Store 2FA setting
        this.securitySettings['enable-2fa'] = isEnabled;
        
        // Show/hide 2FA setup
        const setup2FA = document.getElementById('2fa-setup');
        if (setup2FA) {
            setup2FA.style.display = isEnabled ? 'flex' : 'none';
        }
        
        // Auto-save settings
        this.saveSecuritySettings();
        
        const status = isEnabled ? 'enabled' : 'disabled';
        this.showToast(`Two-factor authentication ${status}`, isEnabled ? 'success' : 'warning');
    }
    
    async setup2FA() {
        console.log('Setting up 2FA...');
        
        // Send 2FA setup request via WebSocket
        if (window.volflowApp?.modules?.websocket?.isConnected) {
            const message = {
                type: 'setup_2fa',
                timestamp: new Date().toISOString(),
                user: 'web_interface'
            };
            
            window.volflowApp.modules.websocket.send(message);
            console.log('📡 2FA setup request sent via WebSocket');
        }
        
        this.showToast('2FA setup initiated - check your authenticator app', 'info');
    }
    
    async viewLoginHistory() {
        console.log('Viewing login history...');
        
        // Send login history request via WebSocket
        if (window.volflowApp?.modules?.websocket?.isConnected) {
            const message = {
                type: 'get_login_history',
                timestamp: new Date().toISOString(),
                user: 'web_interface'
            };
            
            window.volflowApp.modules.websocket.send(message);
            console.log('📡 Login history request sent via WebSocket');
        }
        
        this.showToast('Login history requested', 'info');
    }
    
    async logout() {
        console.log('Logging out...');
        
        // Show confirmation dialog
        if (!confirm('Are you sure you want to logout?')) {
            return;
        }
        
        try {
            // Send logout request via WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'logout',
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 Logout request sent via WebSocket');
            }
            
            // Clear local storage
            localStorage.clear();
            
            // Redirect to login page or show logout message
            this.showToast('Logged out successfully', 'success');
            
            // Simulate redirect (in a real app, this would redirect to login page)
            setTimeout(() => {
                window.location.reload();
            }, 2000);
            
        } catch (error) {
            console.error('Error during logout:', error);
            this.showToast('Error during logout', 'error');
        }
    }
    
    // Settings Management
    initializeNotificationToggles() {
        // Initialize notification channel visibility based on toggle states
        const notificationToggles = [
            { id: 'enable-email-notifications', settings: 'email-settings' },
            { id: 'enable-telegram-notifications', settings: ['telegram-settings', 'telegram-chat-settings'] },
            { id: 'enable-slack-notifications', settings: ['slack-settings', 'slack-channel-settings'] }
        ];
        
        notificationToggles.forEach(({ id, settings }) => {
            const toggle = document.getElementById(id);
            if (toggle) {
                this.updateNotificationSettingsVisibility(id, toggle.checked);
            }
        });
        
        // Initialize alert settings visibility
        const alertToggles = [
            { id: 'enable-max-loss-alert', settings: 'max-loss-settings' },
            { id: 'enable-volatility-alert', settings: 'volatility-settings' }
        ];
        
        alertToggles.forEach(({ id, settings }) => {
            const toggle = document.getElementById(id);
            const settingsElement = document.getElementById(settings);
            if (toggle && settingsElement) {
                settingsElement.style.display = toggle.checked ? 'flex' : 'none';
            }
        });
        
        // Initialize quiet hours settings
        const quietHoursToggle = document.getElementById('enable-quiet-hours');
        const quietHoursSettings = document.getElementById('quiet-hours-settings');
        if (quietHoursToggle && quietHoursSettings) {
            quietHoursSettings.style.display = quietHoursToggle.checked ? 'flex' : 'none';
        }
        
        // Initialize auto logout settings
        const autoLogoutToggle = document.getElementById('enable-auto-logout');
        const autoLogoutSettings = document.getElementById('auto-logout-settings');
        if (autoLogoutToggle && autoLogoutSettings) {
            autoLogoutSettings.style.display = autoLogoutToggle.checked ? 'flex' : 'none';
        }
        
        // Initialize 2FA setup
        const enable2FA = document.getElementById('enable-2fa');
        const setup2FA = document.getElementById('2fa-setup');
        if (enable2FA && setup2FA) {
            setup2FA.style.display = enable2FA.checked ? 'flex' : 'none';
        }
    }
    
    getNotificationConfiguration() {
        return {
            email: {
                enabled: this.notificationSettings['enable-email-notifications'] || false,
                address: this.notificationSettings['notification-email'] || ''
            },
            telegram: {
                enabled: this.notificationSettings['enable-telegram-notifications'] || false,
                bot_token: this.notificationSettings['telegram-bot-token'] || '',
                chat_id: this.notificationSettings['telegram-chat-id'] || ''
            },
            slack: {
                enabled: this.notificationSettings['enable-slack-notifications'] || false,
                webhook_url: this.notificationSettings['slack-webhook-url'] || '',
                channel: this.notificationSettings['slack-channel'] || ''
            },
            alerts: {
                max_loss: {
                    enabled: this.notificationSettings['enable-max-loss-alert'] || false,
                    threshold: parseFloat(this.notificationSettings['max-loss-threshold']) || 1000
                },
                volatility: {
                    enabled: this.notificationSettings['enable-volatility-alert'] || false,
                    threshold: parseFloat(this.notificationSettings['vix-alert-threshold']) || 30
                },
                position_size: {
                    enabled: this.notificationSettings['enable-position-size-alert'] || false
                },
                signals: {
                    enabled: this.notificationSettings['enable-signal-alert'] || false
                }
            },
            preferences: {
                frequency: this.notificationSettings['alert-frequency'] || '5min',
                quiet_hours: {
                    enabled: this.notificationSettings['enable-quiet-hours'] || false,
                    start: this.notificationSettings['quiet-hours-start'] || '22:00',
                    end: this.notificationSettings['quiet-hours-end'] || '08:00'
                }
            },
            timestamp: new Date().toISOString()
        };
    }
    
    getSecurityConfiguration() {
        return {
            auto_logout: {
                enabled: this.securitySettings['enable-auto-logout'] || false,
                timeout: parseInt(this.securitySettings['auto-logout-timeout']) || 30
            },
            two_factor_auth: {
                enabled: this.securitySettings['enable-2fa'] || false
            },
            timestamp: new Date().toISOString()
        };
    }
    
    saveAllSettings() {
        console.log('Saving all settings...');
        
        try {
            this.saveNotificationSettings();
            this.saveSecuritySettings();
            
            // Send all settings to backend via WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'update_all_settings',
                    notification_settings: this.getNotificationConfiguration(),
                    security_settings: this.getSecurityConfiguration(),
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 All settings sent via WebSocket');
            }
            
            this.showToast('All settings saved successfully', 'success');
            
        } catch (error) {
            console.error('Error saving all settings:', error);
            this.showToast('Failed to save all settings', 'error');
        }
    }
    
    saveNotificationSettings() {
        try {
            const config = this.getNotificationConfiguration();
            localStorage.setItem('volflow-notification-settings', JSON.stringify(config));
            console.log('Notification settings saved successfully');
        } catch (error) {
            console.error('Error saving notification settings:', error);
        }
    }
    
    saveSecuritySettings() {
        try {
            const config = this.getSecurityConfiguration();
            localStorage.setItem('volflow-security-settings', JSON.stringify(config));
            console.log('Security settings saved successfully');
        } catch (error) {
            console.error('Error saving security settings:', error);
        }
    }
    
    loadSettings() {
        console.log('Loading saved settings...');
        
        try {
            // Load notification settings
            const notificationSettings = localStorage.getItem('volflow-notification-settings');
            if (notificationSettings) {
                const config = JSON.parse(notificationSettings);
                this.applyNotificationSettings(config);
            }
            
            // Load security settings
            const securitySettings = localStorage.getItem('volflow-security-settings');
            if (securitySettings) {
                const config = JSON.parse(securitySettings);
                this.applySecuritySettings(config);
            }
            
        } catch (error) {
            console.error('Error loading settings:', error);
        }
    }
    
    applyNotificationSettings(config) {
        // Apply notification channel settings
        if (config.email) {
            const emailToggle = document.getElementById('enable-email-notifications');
            const emailInput = document.getElementById('notification-email');
            if (emailToggle) emailToggle.checked = config.email.enabled;
            if (emailInput) emailInput.value = config.email.address;
        }
        
        if (config.telegram) {
            const telegramToggle = document.getElementById('enable-telegram-notifications');
            const botTokenInput = document.getElementById('telegram-bot-token');
            const chatIdInput = document.getElementById('telegram-chat-id');
            if (telegramToggle) telegramToggle.checked = config.telegram.enabled;
            if (botTokenInput) botTokenInput.value = config.telegram.bot_token;
            if (chatIdInput) chatIdInput.value = config.telegram.chat_id;
        }
        
        if (config.slack) {
            const slackToggle = document.getElementById('enable-slack-notifications');
            const webhookInput = document.getElementById('slack-webhook-url');
            const channelInput = document.getElementById('slack-channel');
            if (slackToggle) slackToggle.checked = config.slack.enabled;
            if (webhookInput) webhookInput.value = config.slack.webhook_url;
            if (channelInput) channelInput.value = config.slack.channel;
        }
        
        // Apply alert settings
        if (config.alerts) {
            const alertToggles = [
                { key: 'max_loss', id: 'enable-max-loss-alert' },
                { key: 'volatility', id: 'enable-volatility-alert' },
                { key: 'position_size', id: 'enable-position-size-alert' },
                { key: 'signals', id: 'enable-signal-alert' }
            ];
            
            alertToggles.forEach(({ key, id }) => {
                const toggle = document.getElementById(id);
                if (toggle && config.alerts[key]) {
                    toggle.checked = config.alerts[key].enabled;
                }
            });
            
            // Apply threshold values
            const maxLossThreshold = document.getElementById('max-loss-threshold');
            const vixThreshold = document.getElementById('vix-alert-threshold');
            
            if (maxLossThreshold && config.alerts.max_loss) {
                maxLossThreshold.value = config.alerts.max_loss.threshold;
            }
            
            if (vixThreshold && config.alerts.volatility) {
                vixThreshold.value = config.alerts.volatility.threshold;
            }
        }
        
        // Apply preference settings
        if (config.preferences) {
            const alertFrequency = document.getElementById('alert-frequency');
            const quietHoursToggle = document.getElementById('enable-quiet-hours');
            const quietHoursStart = document.getElementById('quiet-hours-start');
            const quietHoursEnd = document.getElementById('quiet-hours-end');
            
            if (alertFrequency) alertFrequency.value = config.preferences.frequency;
            if (quietHoursToggle) quietHoursToggle.checked = config.preferences.quiet_hours.enabled;
            if (quietHoursStart) quietHoursStart.value = config.preferences.quiet_hours.start;
            if (quietHoursEnd) quietHoursEnd.value = config.preferences.quiet_hours.end;
        }
    }
    
    applySecuritySettings(config) {
        // Apply auto logout settings
        if (config.auto_logout) {
            const autoLogoutToggle = document.getElementById('enable-auto-logout');
            const autoLogoutTimeout = document.getElementById('auto-logout-timeout');
            
            if (autoLogoutToggle) autoLogoutToggle.checked = config.auto_logout.enabled;
            if (autoLogoutTimeout) autoLogoutTimeout.value = config.auto_logout.timeout;
        }
        
        // Apply 2FA settings
        if (config.two_factor_auth) {
            const enable2FA = document.getElementById('enable-2fa');
            if (enable2FA) enable2FA.checked = config.two_factor_auth.enabled;
        }
    }
    
    // Update from WebSocket data
    updateFromWebSocket(data) {
        if (data.settings_update) {
            console.log('⚙️ Processing settings update from WebSocket');
            // Handle settings updates from backend
        }
        
        if (data.login_history) {
            console.log('📋 Processing login history from WebSocket');
            this.displayLoginHistory(data.login_history);
        }
        
        if (data.notification_test_result) {
            console.log('📧 Processing notification test result');
            this.handleNotificationTestResult(data.notification_test_result);
        }
        
        if (data.alerts_config_data) {
            console.log('🔔 Processing alerts config data from WebSocket');
            this.applyAlertsConfiguration(data.config);
        }
        
        if (data.alerts_config_saved) {
            console.log('✅ Alerts configuration saved successfully');
            this.showToast('Alerts configuration saved successfully', 'success');
        }
        
        if (data.alerts_config_updated) {
            console.log('🔔 Processing alerts config update from WebSocket');
            this.applyAlertsConfiguration(data.config);
        }
        
        if (data.password_changed) {
            console.log('🔐 Password changed successfully');
            this.showToast('Password changed successfully', 'success');
        }
        
        if (data.logout_response) {
            console.log('👋 Logout successful');
            this.showToast('Logout successful', 'success');
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        }
    }
    
    displayLoginHistory(loginHistory) {
        // Display login history in a modal or dedicated section
        console.log('Login history received:', loginHistory);
        this.showToast('Login history loaded', 'info');
    }
    
    handleNotificationTestResult(testResult) {
        if (testResult.success) {
            this.showToast('Test notifications sent successfully', 'success');
        } else {
            this.showToast(`Test notification failed: ${testResult.error}`, 'error');
        }
    }
    
    // Alerts Configuration Management
    async loadAlertsConfig() {
        console.log('Loading alerts configuration...');
        
        try {
            // Request alerts config from WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'get_alerts_config',
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 Alerts config request sent via WebSocket');
            }
        } catch (error) {
            console.error('Error loading alerts config:', error);
        }
    }
    
    async saveAlertsConfig() {
        console.log('Saving alerts configuration...');
        
        try {
            const alertsConfig = this.getAlertsConfiguration();
            
            // Send alerts config to WebSocket
            if (window.volflowApp?.modules?.websocket?.isConnected) {
                const message = {
                    type: 'save_alerts_config',
                    config: alertsConfig,
                    timestamp: new Date().toISOString(),
                    user: 'web_interface'
                };
                
                window.volflowApp.modules.websocket.send(message);
                console.log('📡 Alerts config save request sent via WebSocket');
            }
            
            this.showToast('Alerts configuration saved successfully', 'success');
            
        } catch (error) {
            console.error('Error saving alerts config:', error);
            this.showToast('Failed to save alerts configuration', 'error');
        }
    }
    
    getAlertsConfiguration() {
        return {
            alert_types: {
                maximum_loss_alert: {
                    enabled: this.notificationSettings['enable-max-loss-alert'] || false,
                    loss_threshold: {
                        value: parseFloat(this.notificationSettings['max-loss-threshold']) || 1000
                    }
                },
                volatility_spike_alert: {
                    enabled: this.notificationSettings['enable-volatility-alert'] || false,
                    vix_threshold: {
                        value: parseFloat(this.notificationSettings['vix-alert-threshold']) || 30
                    }
                },
                position_size_alert: {
                    enabled: this.notificationSettings['enable-position-size-alert'] || false
                },
                strategy_signal_alert: {
                    enabled: this.notificationSettings['enable-signal-alert'] || false
                }
            },
            notification_channels: {
                email_notifications: {
                    enabled: this.notificationSettings['enable-email-notifications'] || false,
                    email_address: {
                        value: this.notificationSettings['notification-email'] || ''
                    }
                },
                telegram_notifications: {
                    enabled: this.notificationSettings['enable-telegram-notifications'] || false,
                    bot_token: {
                        value: this.notificationSettings['telegram-bot-token'] || ''
                    },
                    chat_id: {
                        value: this.notificationSettings['telegram-chat-id'] || ''
                    }
                },
                slack_notifications: {
                    enabled: this.notificationSettings['enable-slack-notifications'] || false,
                    webhook_url: {
                        value: this.notificationSettings['slack-webhook-url'] || ''
                    },
                    channel: {
                        value: this.notificationSettings['slack-channel'] || ''
                    }
                }
            },
            notification_preferences: {
                alert_frequency: {
                    value: this.notificationSettings['alert-frequency'] || '5min'
                },
                quiet_hours: {
                    enabled: this.notificationSettings['enable-quiet-hours'] || false,
                    start_time: this.notificationSettings['quiet-hours-start'] || '22:00',
                    end_time: this.notificationSettings['quiet-hours-end'] || '08:00'
                }
            },
            updated_by: 'web_interface'
        };
    }
    
    applyAlertsConfiguration(config) {
        console.log('Applying alerts configuration:', config);
        
        try {
            const alertsNotifications = config.alerts_notifications || {};
            
            // Apply alert types
            const alertTypes = alertsNotifications.alert_types || {};
            
            if (alertTypes.maximum_loss_alert) {
                const toggle = document.getElementById('enable-max-loss-alert');
                const threshold = document.getElementById('max-loss-threshold');
                if (toggle) toggle.checked = alertTypes.maximum_loss_alert.enabled || false;
                if (threshold && alertTypes.maximum_loss_alert.loss_threshold) {
                    threshold.value = alertTypes.maximum_loss_alert.loss_threshold.value || 1000;
                }
            }
            
            if (alertTypes.volatility_spike_alert) {
                const toggle = document.getElementById('enable-volatility-alert');
                const threshold = document.getElementById('vix-alert-threshold');
                if (toggle) toggle.checked = alertTypes.volatility_spike_alert.enabled || false;
                if (threshold && alertTypes.volatility_spike_alert.vix_threshold) {
                    threshold.value = alertTypes.volatility_spike_alert.vix_threshold.value || 30;
                }
            }
            
            if (alertTypes.position_size_alert) {
                const toggle = document.getElementById('enable-position-size-alert');
                if (toggle) toggle.checked = alertTypes.position_size_alert.enabled || false;
            }
            
            if (alertTypes.strategy_signal_alert) {
                const toggle = document.getElementById('enable-signal-alert');
                if (toggle) toggle.checked = alertTypes.strategy_signal_alert.enabled || false;
            }
            
            // Apply notification channels
            const channels = alertsNotifications.notification_channels || {};
            
            if (channels.email_notifications) {
                const toggle = document.getElementById('enable-email-notifications');
                const emailInput = document.getElementById('notification-email');
                if (toggle) toggle.checked = channels.email_notifications.enabled || false;
                if (emailInput && channels.email_notifications.email_address) {
                    emailInput.value = channels.email_notifications.email_address.value || '';
                }
            }
            
            if (channels.telegram_notifications) {
                const toggle = document.getElementById('enable-telegram-notifications');
                const botTokenInput = document.getElementById('telegram-bot-token');
                const chatIdInput = document.getElementById('telegram-chat-id');
                if (toggle) toggle.checked = channels.telegram_notifications.enabled || false;
                if (botTokenInput && channels.telegram_notifications.bot_token) {
                    botTokenInput.value = channels.telegram_notifications.bot_token.value || '';
                }
                if (chatIdInput && channels.telegram_notifications.chat_id) {
                    chatIdInput.value = channels.telegram_notifications.chat_id.value || '';
                }
            }
            
            if (channels.slack_notifications) {
                const toggle = document.getElementById('enable-slack-notifications');
                const webhookInput = document.getElementById('slack-webhook-url');
                const channelInput = document.getElementById('slack-channel');
                if (toggle) toggle.checked = channels.slack_notifications.enabled || false;
                if (webhookInput && channels.slack_notifications.webhook_url) {
                    webhookInput.value = channels.slack_notifications.webhook_url.value || '';
                }
                if (channelInput && channels.slack_notifications.channel) {
                    channelInput.value = channels.slack_notifications.channel.value || '';
                }
            }
            
            // Apply notification preferences
            const preferences = alertsNotifications.notification_preferences || {};
            
            if (preferences.alert_frequency) {
                const frequencySelect = document.getElementById('alert-frequency');
                if (frequencySelect) {
                    frequencySelect.value = preferences.alert_frequency.value || '5min';
                }
            }
            
            if (preferences.quiet_hours) {
                const toggle = document.getElementById('enable-quiet-hours');
                const startTime = document.getElementById('quiet-hours-start');
                const endTime = document.getElementById('quiet-hours-end');
                if (toggle) toggle.checked = preferences.quiet_hours.enabled || false;
                if (startTime) startTime.value = preferences.quiet_hours.start_time || '22:00';
                if (endTime) endTime.value = preferences.quiet_hours.end_time || '08:00';
            }
            
            // Store the config internally
            this.alertsConfig = config;
            
            console.log('✅ Alerts configuration applied successfully');
            
        } catch (error) {
            console.error('Error applying alerts configuration:', error);
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

// Export SettingsManager to global scope
window.SettingsManager = SettingsManager;
