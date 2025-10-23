#!/usr/bin/env python3
"""
Simplified Real-time Account and Position Monitor

This script continuously monitors essential account data with independent processes:
1. Account data from Schwab API
2. Current positions and P&L
3. Recent transactions
4. Database insertion

Focused on core account monitoring without technical indicators or strategy logic.

Usage: python3 realtime_monitor.py
"""

import json
import time
import threading
import os
import subprocess
import psutil
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging
import signal
import sys
import pytz

# Import essential handlers only
from schwab_transaction_handler import SchwabTransactionHandler
from pnl_data_handler import PnLDataHandler
from account_data_handler import AccountDataHandler
from connection_manager import get_all_positions, make_authenticated_request, handle_api_response, is_authentication_paused, resume_operations, verify_authentication_before_start
from db_inserter import DatabaseInserter
from api_status_exporter import APIStatusExporter

class RealTimeMonitor:
    """Simplified Real-time Monitor for essential account data only."""    
    def __init__(self, update_interval: float = 1.0):
        """Initialize the simplified real-time monitor."""
        self.update_interval = update_interval
        self.running = False
        
        # Set up EST timezone
        self.est_tz = pytz.timezone('US/Eastern')
        
        # Thread-safe data storage with locks for essential data types only
        self.data_locks = {
            'positions': threading.RLock(),
            'transactions': threading.RLock(),
            'account': threading.RLock(),
            'market_status': threading.RLock(),
            'api_status': threading.RLock()
        }
        
        # Independent data storage for essential processes only
        self.data_cache = {
            'positions': {},
            'transactions': {},
            'account': {},
            'market_status': {},
            'api_status': {}
        }
        
        # Process management
        self.process_threads = {}
        
        # Script thread tracking for monitoring
        self.script_threads = {}
        self.script_threads_lock = threading.Lock()
        
        # Initialize handlers - lazy loading to avoid blocking
        self._handlers = {}
        self._handler_lock = threading.Lock()
        
        self.update_intervals = {
            'positions': 5.0,           # Every 5 seconds
            'transactions': 60.0,       # Every 60 seconds
            'account': 30.0,            # Every 30 seconds
            'market_status': 10.0,      # Every 10 seconds
            'pnl_statistics': 120.0,    # Every 2 minutes (120 seconds)
            'database': 5.0,            # Every 5 seconds
            'exceedance_monitor': 2.0,  # Every 2 seconds - monitor exceedance strategy
            'auto_timer_monitor': 30.0, # Every 30 seconds - monitor auto-timer flags and market hours
            'api_status': 30.0          # Every 30 seconds - monitor API authentication status
        }
        
        # Last update tracking for each process
        self.last_updates = {key: 0 for key in self.update_intervals.keys()}
        
        # Database inserter thread management
        self.db_inserter_running = False
        self.db_inserter_thread = None
        
        # Exceedance strategy monitoring
        self.exceedance_process = None
        self.exceedance_pid = None
        self.exceedance_restart_attempts = 0
        self.exceedance_max_restart_attempts = 3
        self.exceedance_last_restart_time = 0
        self.exceedance_restart_cooldown = 30  # seconds
        self.exceedance_script = 'exceedence_strategy_signals.py'
        self.trading_config_file = 'trading_config_live.json'
        
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Simplified RealTimeMonitor initialized")
        self.logger.info("Focused on account data, positions, transactions, database insertion, and exceedance strategy monitoring")
    
    
    def _get_handler(self, handler_name: str):
        """Get handler instance with lazy loading for essential handlers only."""
        if handler_name not in self._handlers:
            with self._handler_lock:
                if handler_name not in self._handlers:
                    try:
                        if handler_name == 'transaction':
                            self._handlers[handler_name] = SchwabTransactionHandler()
                        elif handler_name == 'pnl':
                            self._handlers[handler_name] = PnLDataHandler()
                        elif handler_name == 'account':
                            self._handlers[handler_name] = AccountDataHandler()
                        elif handler_name == 'db_inserter':
                            self._handlers[handler_name] = DatabaseInserter()
                        else:
                            raise ValueError(f"Unknown handler: {handler_name}")
                    except Exception as e:
                        self.logger.error(f"Error initializing {handler_name} handler: {e}")
                        return None
        
        return self._handlers.get(handler_name)
    
    @property
    def transaction_handler(self):
        return self._get_handler('transaction')
    
    @property
    def pnl_handler(self):
        return self._get_handler('pnl')
    
    @property
    def account_handler(self):
        return self._get_handler('account')
    
    @property
    def db_inserter(self):
        return self._get_handler('db_inserter')
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        sys.exit(0)

    def _get_market_status(self) -> Dict[str, Any]:
        """Get current market status using EST timezone."""
        now_est = datetime.now(self.est_tz)
        market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
        is_market_hours = market_open <= now_est <= market_close
        is_weekday = now_est.weekday() < 5
        
        return {
            'current_time': now_est.isoformat(),
            'market_open_time': market_open.isoformat(),
            'market_close_time': market_close.isoformat(),
            'is_market_hours': is_market_hours and is_weekday,
            'is_weekday': is_weekday,
            'session_status': 'OPEN' if (is_market_hours and is_weekday) else 'CLOSED',
            'minutes_to_open': max(0, (market_open - now_est).total_seconds() / 60) if not is_market_hours else 0,
            'minutes_to_close': max(0, (market_close - now_est).total_seconds() / 60) if is_market_hours else 0
        }

    def _start_independent_process(self, process_name: str):
        """Start an independent process that runs on its own schedule."""
        def process_worker():
            self.logger.info(f"🚀 Started independent {process_name} process")
            
            while self.running:
                try:
                    current_time = time.time()
                    interval = self.update_intervals[process_name]
                    
                    # Check if it's time to update this process
                    if current_time - self.last_updates[process_name] >= interval:
                        self.logger.debug(f"⏰ {process_name} process updating...")
                        
                        # Check if authentication is paused - if so, wait for re-authentication
                        if is_authentication_paused():
                            self.logger.warning(f"🛑 Authentication paused - waiting for re-authentication to complete for {process_name}")
                            # Wait a bit and check again - don't skip, just wait
                            time.sleep(2)
                            continue
                        
                        # Proceed with normal operations - initialize data variable
                        data = {}
                        
                        # Run scripts that get data themselves (essential processes only)
                        if process_name == 'positions':
                            self._run_script('current_positions_handler.py')
                        elif process_name == 'transactions':
                            self._run_script('schwab_transaction_handler.py')
                        elif process_name == 'account':
                            self._run_script('account_data_handler.py')
                        elif process_name == 'market_status':
                            data = self._get_market_status()
                            # Update cache with thread-safe access
                            with self.data_locks[process_name]:
                                self.data_cache[process_name] = data
                        elif process_name == 'pnl_statistics':
                            self._run_script('pnl_data_handler.py')
                        elif process_name == 'database':
                            self._run_database_insertion()
                        elif process_name == 'exceedance_monitor':
                            self._monitor_exceedance_strategy()
                        elif process_name == 'auto_timer_monitor':
                            self._monitor_auto_timer_strategies()
                        elif process_name == 'api_status':
                            self._monitor_api_status()
                        
                        self.last_updates[process_name] = current_time
                        self.logger.debug(f"✅ {process_name} process updated")
                    
                    # Sleep for a short time to prevent busy waiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error in {process_name} process: {e}")
                    time.sleep(1)  # Wait longer on error
            
            self.logger.info(f"🛑 {process_name} process stopped")
        
        # Start the process thread
        thread = threading.Thread(target=process_worker, daemon=True)
        thread.start()
        self.process_threads[process_name] = thread
        return thread
        
    def _run_script(self, script_name: str):
        """Run a script to fetch and save data in a separate thread."""
        def run_script_thread():
            try:
                import subprocess
                self.logger.debug(f"🔄 Running {script_name} in separate thread...")
                
                result = subprocess.run([
                    'python3', script_name
                ], capture_output=True, text=True, timeout=60)  # Increased timeout for complex scripts
                
                if result.returncode == 0:
                    self.logger.debug(f"✅ {script_name} completed successfully")
                else:
                    self.logger.warning(f"⚠️ {script_name} failed with return code {result.returncode}")
                    if result.stderr:
                        self.logger.warning(f"   Error: {result.stderr.strip()}")
                        
            except subprocess.TimeoutExpired:
                self.logger.error(f"❌ {script_name} timed out after 60 seconds")
            except Exception as e:
                self.logger.error(f"❌ Error running {script_name}: {e}")
            finally:
                # Remove from tracking when thread completes
                with self.script_threads_lock:
                    if script_name in self.script_threads:
                        del self.script_threads[script_name]
        
        # Track script threads for monitoring
        with self.script_threads_lock:
            # Clean up any completed threads first
            completed_scripts = [name for name, thread in self.script_threads.items() if not thread.is_alive()]
            for name in completed_scripts:
                del self.script_threads[name]
            
            # Start new script thread
            script_thread = threading.Thread(target=run_script_thread, daemon=True)
            script_thread.start()
            self.script_threads[script_name] = script_thread
            
        self.logger.debug(f"🚀 Started {script_name} in separate thread (tracking {len(self.script_threads)} active script threads)")
    
    def _run_database_insertion(self):
        """Run database insertion using the db_inserter."""
        try:
            self.logger.debug("🗄️ Running database insertion...")
            
            # Get database inserter and run insertion
            db_inserter = self.db_inserter
            if db_inserter:
                results = db_inserter.process_all_json_files()
                
                if results:
                    successful = sum(1 for success in results.values() if success)
                    total = len(results)
                    self.logger.info(f"🗄️ Database insertion completed: {successful}/{total} successful")
                    
                    # Log any failures
                    for file_type, success in results.items():
                        if not success:
                            self.logger.warning(f"🗄️ Database insertion failed for: {file_type}")
                else:
                    self.logger.debug("🗄️ No data available for database insertion")
            else:
                self.logger.error("🗄️ Database inserter not available")
                
        except Exception as e:
            self.logger.error(f"🗄️ Error in database insertion: {e}")
    
    def _start_database_inserter_thread(self):
        """Start the database inserter controlled by realtime monitor timing."""
        def db_inserter_worker():
            """Database inserter worker thread controlled by realtime monitor."""
            self.logger.info("🗄️ Database inserter started - controlled by realtime monitor")
            self.logger.info("🗄️ Running every 5 seconds when called by realtime monitor")
            
            # Initialize database inserter (no internal timing)
            db_inserter = DatabaseInserter()
            
            while self.db_inserter_running:
                try:
                    # Simply sleep and wait for calls - no internal timing
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"🗄️ Error in database inserter thread: {e}")
                    time.sleep(2)  # Wait on error
            
            self.logger.info("🗄️ Database inserter thread stopped")
        
        # Start the database inserter thread (passive, waits for calls)
        self.db_inserter_running = True
        self.db_inserter_thread = threading.Thread(target=db_inserter_worker, daemon=True)
        self.db_inserter_thread.start()
        self.logger.info("🗄️ Started database inserter thread (controlled by realtime monitor timing)")

    def _stop_database_inserter_thread(self):
        """Stop the database inserter thread."""
        if self.db_inserter_running:
            self.logger.info("🗄️ Stopping database inserter thread...")
            self.db_inserter_running = False
            if self.db_inserter_thread and self.db_inserter_thread.is_alive():
                self.db_inserter_thread.join(timeout=5)
            self.logger.info("🗄️ Database inserter thread stopped")

    def start_continuous_monitoring(self):
        """Start simplified continuous monitoring with essential processes only."""
        self.logger.info("🚀 Starting simplified monitor with essential processes")
        self.logger.info("📊 Focused on account data, positions, transactions, and database insertion")
        self.running = True
        
        # Start only essential processes
        processes_to_start = [
            'positions',         # Every 5 seconds
            'transactions',      # Every 60 seconds  
            'account',           # Every 30 seconds
            'market_status',     # Every 10 seconds
            'pnl_statistics',    # Every 2 minutes (120 seconds)
            'database',          # Every 5 seconds
            'exceedance_monitor', # Every 2 seconds - monitor exceedance strategy
            'auto_timer_monitor', # Every 30 seconds - monitor auto-timer flags and market hours
            'api_status'         # Every 30 seconds - monitor API authentication status
        ]
        
        self.logger.info(f"🔥 Starting {len(processes_to_start)} essential processes...")
        for process_name in processes_to_start:
            thread = self._start_independent_process(process_name)
            self.logger.info(f"✅ {process_name} process started (interval: {self.update_intervals[process_name]}s)")
        
        self.logger.info("🎯 All essential processes started - focused on core account monitoring!")
        
        # Start the database inserter as a completely separate independent process
        self._start_database_inserter_thread()
        
        try:
            # Main thread just monitors and handles shutdown
            while self.running:
                # Check process health
                alive_processes = sum(1 for t in self.process_threads.values() if t.is_alive())
                total_processes = len(self.process_threads)
                
                if alive_processes < total_processes:
                    self.logger.warning(f"⚠️ Process health: {alive_processes}/{total_processes} processes alive")
                
                # Sleep for a reasonable interval - main thread doesn't do heavy work
                time.sleep(5)
                    
        except KeyboardInterrupt:
            self.logger.info("🛑 Received keyboard interrupt - shutting down all processes...")
        finally:
            self.running = False
            self._stop_all_processes()
            self.logger.info("✅ All processes stopped - monitor shutdown complete")
        
    def _stop_all_processes(self):
        """Stop all independent processes gracefully."""
        self.logger.info("🛑 Stopping all independent processes...")
        
        # Stop database inserter
        self._stop_database_inserter_thread()
        
        # Wait for all process threads to finish
        for process_name, thread in self.process_threads.items():
            if thread.is_alive():
                self.logger.info(f"⏳ Waiting for {process_name} process to stop...")
                thread.join(timeout=3)
                if thread.is_alive():
                    self.logger.warning(f"⚠️ {process_name} process did not stop gracefully")
        
        # Wait for active script threads to complete
        with self.script_threads_lock:
            active_scripts = list(self.script_threads.items())
        
        if active_scripts:
            self.logger.info(f"⏳ Waiting for {len(active_scripts)} active script threads to complete...")
            for script_name, thread in active_scripts:
                if thread.is_alive():
                    self.logger.info(f"   Waiting for {script_name}...")
                    thread.join(timeout=5)  # Give scripts time to complete
                    if thread.is_alive():
                        self.logger.warning(f"⚠️ Script {script_name} did not complete gracefully")
        
        self.logger.info("✅ All processes and script threads stopped")

    def _monitor_exceedance_strategy(self):
        """Monitor exceedance strategy based on trading config and manage process accordingly."""
        try:
            # Check if trading config file exists
            if not os.path.exists(self.trading_config_file):
                self.logger.debug(f"📋 Trading config file not found: {self.trading_config_file}")
                return
            
            # Read trading config
            with open(self.trading_config_file, 'r') as f:
                config = json.load(f)
            
            # Check PML strategy running state
            pml_config = config.get('strategies', {}).get('pml', {})
            running_state = pml_config.get('running_state', {})
            is_running_config = running_state.get('is_running', False)
            
            # Get current process status
            process_actually_running = self._is_exceedance_process_running()
            
            self.logger.debug(f"🎯 Exceedance monitor - Config: {is_running_config}, Process: {process_actually_running}")
            
            # Decision logic
            if is_running_config and not process_actually_running:
                # Should be running but isn't - start it
                self.logger.info("🚀 Config says PML running=true but exceedance process not found - starting exceedance strategy")
                self._start_exceedance_process()
                
            elif not is_running_config and process_actually_running:
                # Should not be running but is - stop it
                self.logger.info("🛑 Config says PML running=false but exceedance process found - stopping exceedance strategy")
                self._stop_exceedance_process()
                
            elif is_running_config and process_actually_running:
                # Should be running and is - check health
                self.logger.debug("✅ Config and process state match - exceedance strategy running")
                self._check_exceedance_process_health()
                
            else:
                # Should not be running and isn't - all good
                self.logger.debug("✅ Config and process state match - exceedance strategy stopped")
            
        except Exception as e:
            self.logger.error(f"❌ Error monitoring exceedance strategy: {e}")

    def _is_exceedance_process_running(self):
        """Check if exceedance strategy process is currently running."""
        try:
            # First check our tracked PID
            if self.exceedance_pid and psutil.pid_exists(self.exceedance_pid):
                try:
                    proc = psutil.Process(self.exceedance_pid)
                    cmdline = proc.cmdline()
                    if any(self.exceedance_script in arg for arg in cmdline):
                        return True
                except psutil.NoSuchProcess:
                    pass
            
            # Fallback: search all processes
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(self.exceedance_script in arg for arg in cmdline):
                        # Update our tracking if we found it
                        if not self.exceedance_pid:
                            self.exceedance_pid = proc.info['pid']
                            self.logger.info(f"🔍 Found existing exceedance process PID: {self.exceedance_pid}")
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Clear tracking if not found
            if self.exceedance_pid:
                self.logger.debug("🔍 Exceedance process not found - clearing tracking")
                self.exceedance_process = None
                self.exceedance_pid = None
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error checking if exceedance process is running: {e}")
            return False

    def _start_exceedance_process(self):
        """Start the exceedance strategy process."""
        try:
            # Check if already running
            if self._is_exceedance_process_running():
                self.logger.warning("⚠️ Exceedance process already running - not starting another")
                return
            
            self.logger.info(f"🚀 Starting exceedance strategy: {self.exceedance_script}")
            
            # Start the process with --continuous flag for persistent operation
            self.exceedance_process = subprocess.Popen([
                'python3', self.exceedance_script, '--continuous'
            ], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            cwd=os.getcwd()
            )
            
            self.exceedance_pid = self.exceedance_process.pid
            
            self.logger.info(f"✅ Started exceedance strategy process with PID: {self.exceedance_pid}")
            
            # Give process time to initialize
            time.sleep(2)
            
            # Check if it's still running
            if self.exceedance_process.poll() is None:
                self.logger.info("✅ Exceedance strategy process is running successfully")
                # Reset restart attempts on successful start
                self.exceedance_restart_attempts = 0
            else:
                # Process failed to start - get error details
                return_code = self.exceedance_process.returncode
                stderr_output = ""
                stdout_output = ""
                
                try:
                    stdout_output, stderr_output = self.exceedance_process.communicate(timeout=5)
                    if isinstance(stdout_output, bytes):
                        stdout_output = stdout_output.decode('utf-8')
                    if isinstance(stderr_output, bytes):
                        stderr_output = stderr_output.decode('utf-8')
                except Exception as e:
                    self.logger.warning(f"⚠️ Could not get process output: {e}")
                
                self.logger.error(f"❌ Exceedance strategy process failed to start (return code: {return_code})")
                if stderr_output:
                    self.logger.error(f"❌ Process stderr: {stderr_output.strip()}")
                if stdout_output:
                    self.logger.info(f"ℹ️ Process stdout: {stdout_output.strip()}")
                
                self.exceedance_process = None
                self.exceedance_pid = None
            
        except Exception as e:
            self.logger.error(f"❌ Error starting exceedance process: {e}")
            self.exceedance_process = None
            self.exceedance_pid = None

    def _stop_exceedance_process(self):
        """Stop the exceedance strategy process."""
        try:
            if not self.exceedance_pid:
                self.logger.info("ℹ️ No exceedance process to stop")
                return
            
            self.logger.info(f"🛑 Stopping exceedance strategy process PID: {self.exceedance_pid}")
            
            # Try to terminate gracefully first
            if psutil.pid_exists(self.exceedance_pid):
                try:
                    proc = psutil.Process(self.exceedance_pid)
                    proc.terminate()
                    
                    # Wait for graceful termination
                    try:
                        proc.wait(timeout=10)
                        self.logger.info("✅ Exceedance process terminated gracefully")
                    except psutil.TimeoutExpired:
                        self.logger.warning("⚠️ Process didn't terminate gracefully, killing...")
                        proc.kill()
                        proc.wait(timeout=5)
                        self.logger.info("✅ Exceedance process killed")
                        
                except psutil.NoSuchProcess:
                    self.logger.info("ℹ️ Process already terminated")
            
            # Clear tracking
            self.exceedance_process = None
            self.exceedance_pid = None
            
        except Exception as e:
            self.logger.error(f"❌ Error stopping exceedance process: {e}")
            # Clear tracking anyway
            self.exceedance_process = None
            self.exceedance_pid = None

    def _check_exceedance_process_health(self):
        """Check if the exceedance process is still healthy and restart if crashed."""
        try:
            if not self.exceedance_pid:
                return
            
            # Check if process still exists
            if not psutil.pid_exists(self.exceedance_pid):
                self.logger.warning(f"⚠️ Exceedance process PID {self.exceedance_pid} no longer exists")
                self._handle_exceedance_process_crash()
                return
            
            # Check if it's still our process
            try:
                proc = psutil.Process(self.exceedance_pid)
                cmdline = proc.cmdline()
                
                if not any(self.exceedance_script in arg for arg in cmdline):
                    self.logger.warning(f"⚠️ PID {self.exceedance_pid} is not our exceedance process anymore")
                    self._handle_exceedance_process_crash()
                    return
                
                # Process is healthy
                self.logger.debug(f"✅ Exceedance process PID {self.exceedance_pid} is healthy")
                
            except psutil.NoSuchProcess:
                self.logger.warning(f"⚠️ Exceedance process PID {self.exceedance_pid} disappeared")
                self._handle_exceedance_process_crash()
                
        except Exception as e:
            self.logger.error(f"❌ Error checking exceedance process health: {e}")

    def _handle_exceedance_process_crash(self):
        """Handle exceedance process crash and attempt restart if appropriate."""
        self.logger.error(f"💥 Exceedance process crashed or disappeared (PID: {self.exceedance_pid})")
        
        # Clear process tracking
        self.exceedance_process = None
        self.exceedance_pid = None
        
        # Check if we should restart based on config
        try:
            if not os.path.exists(self.trading_config_file):
                self.logger.warning("⚠️ Config file not found - cannot determine restart policy")
                return
            
            with open(self.trading_config_file, 'r') as f:
                config = json.load(f)
            
            # Check if config still says it should be running
            pml_config = config.get('strategies', {}).get('pml', {})
            running_state = pml_config.get('running_state', {})
            is_running_config = running_state.get('is_running', False)
            
            if is_running_config:
                # Check restart limits and cooldown
                current_time = time.time()
                
                if self.exceedance_restart_attempts >= self.exceedance_max_restart_attempts:
                    self.logger.error(f"❌ Max restart attempts ({self.exceedance_max_restart_attempts}) reached - not restarting")
                    return
                
                if current_time - self.exceedance_last_restart_time < self.exceedance_restart_cooldown:
                    self.logger.warning(f"⏳ Restart cooldown active - waiting {self.exceedance_restart_cooldown}s between restarts")
                    return
                
                # Attempt restart
                self.logger.info("🔄 Attempting to restart exceedance strategy after crash...")
                self.exceedance_restart_attempts += 1
                self.exceedance_last_restart_time = current_time
                self._start_exceedance_process()
                
            else:
                self.logger.info("ℹ️ Config says should not be running - not restarting after crash")
                
        except Exception as e:
            self.logger.error(f"❌ Error handling exceedance process crash: {e}")

    def _monitor_auto_timer_strategies(self):
        """Monitor auto-timer flags and automatically start/stop strategies based on market hours."""
        try:
            # Check if trading config file exists
            if not os.path.exists(self.trading_config_file):
                self.logger.debug(f"📋 Trading config file not found: {self.trading_config_file}")
                return
            
            # Read trading config
            with open(self.trading_config_file, 'r') as f:
                config = json.load(f)
            
            # Get current time and market status in EST
            now_est = datetime.now(self.est_tz)
            current_time_str = now_est.strftime("%H:%M")
            is_weekday = now_est.weekday() < 5  # Monday = 0, Sunday = 6
            
            self.logger.debug(f"⏰ Auto-timer monitor (EST) - Current time: {current_time_str}, Weekday: {is_weekday}")
            
            # Track if any changes were made
            config_changed = False
            
            # Check each strategy
            strategies = config.get('strategies', {})
            for strategy_name, strategy_config in strategies.items():
                try:
                    # Check if auto-timer is enabled for this strategy
                    auto_timer_enabled = strategy_config.get('auto_timer', False)
                    
                    if not auto_timer_enabled:
                        self.logger.debug(f"⏰ {strategy_name}: auto-timer disabled, skipping")
                        continue
                    
                    # Get market config for this strategy
                    market_config = strategy_config.get('market_config', {})
                    market_open = market_config.get('market_open', '09:30')
                    market_close = market_config.get('market_close', '16:00')
                    market_hours_only = market_config.get('market_hours_only', True)
                    
                    # Get current running state
                    running_state = strategy_config.get('running_state', {})
                    is_currently_running = running_state.get('is_running', False)
                    current_auto_approve = strategy_config.get('auto_approve', False)
                    
                    # Determine if we should be running based on time
                    should_be_running = self._should_strategy_be_running(
                        current_time_str, market_open, market_close, 
                        is_weekday, market_hours_only
                    )
                    
                    self.logger.debug(f"⏰ {strategy_name}: auto_timer={auto_timer_enabled}, "
                                    f"market_hours={market_open}-{market_close}, "
                                    f"should_run={should_be_running}, currently_running={is_currently_running}")
                    
                    # Decision logic: Update config if state should change
                    if should_be_running and not is_currently_running:
                        # Should start - set is_running=true and auto_approve=true
                        self.logger.info(f"🚀 Auto-timer: Starting {strategy_name} strategy (market hours active)")
                        
                        # Update running state
                        if 'running_state' not in strategy_config:
                            strategy_config['running_state'] = {}
                        strategy_config['running_state']['is_running'] = True
                        strategy_config['running_state']['last_updated'] = datetime.now().isoformat()
                        strategy_config['running_state']['updated_by'] = 'auto_timer_monitor'
                        
                        # Update auto_approve
                        strategy_config['auto_approve'] = True
                        
                        config_changed = True
                        
                    elif not should_be_running and is_currently_running:
                        # Should stop - set is_running=false and auto_approve=false
                        self.logger.info(f"🛑 Auto-timer: Stopping {strategy_name} strategy (outside market hours)")
                        
                        # Update running state
                        if 'running_state' not in strategy_config:
                            strategy_config['running_state'] = {}
                        strategy_config['running_state']['is_running'] = False
                        strategy_config['running_state']['last_updated'] = datetime.now().isoformat()
                        strategy_config['running_state']['updated_by'] = 'auto_timer_monitor'
                        
                        # Update auto_approve
                        strategy_config['auto_approve'] = False
                        
                        config_changed = True
                        
                    else:
                        # No change needed
                        self.logger.debug(f"✅ {strategy_name}: auto-timer state correct, no changes needed")
                
                except Exception as e:
                    self.logger.error(f"❌ Error processing auto-timer for {strategy_name}: {e}")
                    continue
            
            # Save config if changes were made
            if config_changed:
                self._save_trading_config(config)
                self.logger.info("💾 Auto-timer: Trading config updated and saved")
            else:
                self.logger.debug("✅ Auto-timer: No config changes needed")
                
        except Exception as e:
            self.logger.error(f"❌ Error in auto-timer monitor: {e}")

    def _should_strategy_be_running(self, current_time_str: str, market_open: str, 
                                  market_close: str, is_weekday: bool, market_hours_only: bool) -> bool:
        """Determine if a strategy should be running based on time and market hours settings."""
        try:
            # If not market hours only, always run (if weekday)
            if not market_hours_only:
                return is_weekday
            
            # If not a weekday, don't run
            if not is_weekday:
                return False
            
            # Parse times
            current_hour, current_minute = map(int, current_time_str.split(':'))
            
            open_hour, open_minute = map(int, market_open.split(':'))
            close_hour, close_minute = map(int, market_close.split(':'))
            
            # Convert to minutes for easier comparison
            current_minutes = current_hour * 60 + current_minute
            open_minutes = open_hour * 60 + open_minute
            close_minutes = close_hour * 60 + close_minute
            
            # Check if current time is within market hours
            is_within_hours = open_minutes <= current_minutes <= close_minutes
            print(f"⏰ ShouldRUN: Checking market hours (EST): now={current_time_str}, open={market_open}, close={market_close}, within_hours={is_within_hours}")
            
            return is_within_hours
            
        except Exception as e:
            self.logger.error(f"❌ Error determining if strategy should be running: {e}")
            return False

    def _monitor_api_status(self):
        """Monitor API authentication status and save to file for WebSocket streaming."""
        try:
            self.logger.debug("🔐 Checking API authentication status...")
            
            # Create API status exporter instance
            api_exporter = APIStatusExporter()
            
            # Export current status
            status_data = api_exporter.export_status()
            
            # Save to file for WebSocket handler to read
            api_exporter.save_to_file("api_status.json")
            
            # Update cache with thread-safe access
            with self.data_locks['api_status']:
                self.data_cache['api_status'] = status_data
            
            # Log summary
            auth_status = status_data.get('frontend_fields', {}).get('auth-status', 'Unknown')
            schwab_status = status_data.get('frontend_fields', {}).get('schwab-status', 'Unknown')
            token_expiry = status_data.get('frontend_fields', {}).get('token-expiry', 'Unknown')
            
            self.logger.debug(f"🔐 API Status - Auth: {auth_status}, Schwab: {schwab_status}, Token: {token_expiry}")
            
        except Exception as e:
            self.logger.error(f"❌ Error monitoring API status: {e}")
            
            # Create error status data
            error_status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "authentication": {"status": "error", "authenticated": False},
                "connections": {"schwab": {"status": "error", "connected": False}},
                "frontend_fields": {
                    "auth-status": "Error",
                    "auth-indicator": "error",
                    "schwab-status": "Error", 
                    "schwab-indicator": "error",
                    "token-expiry": "Unknown",
                    "last-auth-check": "--:--:--",
                    "auth-account-info": "Not Available"
                }
            }
            
            # Update cache with error status
            with self.data_locks['api_status']:
                self.data_cache['api_status'] = error_status

    def _save_trading_config(self, config: dict):
        """Save the updated trading config to file."""
        try:
            # Create backup first
            backup_file = f"{self.trading_config_file}.backup"
            if os.path.exists(self.trading_config_file):
                import shutil
                shutil.copy2(self.trading_config_file, backup_file)
            
            # Write updated config
            with open(self.trading_config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            self.logger.debug(f"💾 Trading config saved to {self.trading_config_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving trading config: {e}")
            # Try to restore backup if save failed
            backup_file = f"{self.trading_config_file}.backup"
            if os.path.exists(backup_file):
                try:
                    import shutil
                    shutil.copy2(backup_file, self.trading_config_file)
                    self.logger.info("🔄 Restored trading config from backup after save failure")
                except Exception as restore_error:
                    self.logger.error(f"❌ Failed to restore backup: {restore_error}")


def main():
    """Main function with authentication gate - no processes start until authentication is verified."""
    print("Real-time P&L and Technical Indicators Monitor")
    print("Continuous mode with concurrent data collection")
    print("Press Ctrl+C to stop")
    print("="*50)
    
    # AUTHENTICATION GATE - Nothing starts until this passes
    print("\n🛡️ CHECKING AUTHENTICATION BEFORE STARTING ANY PROCESSES...")
    
    if not verify_authentication_before_start():
        print("\n❌ AUTHENTICATION FAILED - EXITING")
        print("🛑 Cannot start realtime monitor without valid Schwab API authentication")
        print("\n📋 Please:")
        print("   1. Check your .env file for correct Schwab API credentials")
        print("   2. Verify your app is approved in Schwab Developer Portal")
        print("   3. Ensure redirect URI matches exactly")
        print("   4. Try running the authentication flow manually")
        return
    
    print("\n🎉 AUTHENTICATION SUCCESSFUL - STARTING ALL PROCESSES")
    print("="*60)
    
    # Create and start monitor with shorter interval - only after authentication passes
    monitor = RealTimeMonitor(update_interval=0.5)
    monitor.start_continuous_monitoring()


if __name__ == "__main__":
    main()
