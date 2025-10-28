#!/usr/bin/env python3
"""
Real-time Data WebSocket Handler
Handles start/stop control for the real-time data monitor script
"""

import asyncio
import json
import subprocess
import os
import psutil
import signal
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealtimeDataHandler:
    """WebSocket handler for real-time data control"""
    
    def __init__(self, broadcast_callback=None, auto_start=True):
        """Initialize the real-time data handler"""
        self.broadcast_callback = broadcast_callback
        self.monitor_process = None
        self.monitor_pid = None
        self.script_name = 'realtime_monitor.py'
        self.is_running = False
        self.auto_start = auto_start
        
        # Check if monitor is already running on startup
        self._check_existing_process()
        
        # Auto-start will be handled when the server starts
        if self.auto_start and not self.is_running:
            logger.info("🚀 Real-time data monitor will auto-start when server is ready (default mode: ON)")
        
        logger.info("📊 Real-time Data Handler initialized")
    
    def _check_existing_process(self):
        """Check if realtime monitor is already running"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(self.script_name in arg for arg in cmdline):
                        self.monitor_pid = proc.info['pid']
                        self.monitor_process = psutil.Process(self.monitor_pid)
                        self.is_running = True
                        logger.info(f"🔍 Found existing realtime monitor process PID: {self.monitor_pid}")
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            logger.info("🔍 No existing realtime monitor process found")
            
        except Exception as e:
            logger.error(f"❌ Error checking for existing process: {e}")
    
    async def handle_message(self, websocket, message, client_addr):
        """Handle websocket messages for real-time data control"""
        try:
            message_type = message.get('type')
            
            # Handle both old and new message types for compatibility
            if message_type in ['start_realtime_monitor', 'start_realtime_data']:
                logger.info(f"🚀 Start real-time data request from {client_addr}")
                await self._handle_start_monitor(websocket, message)
                return True
                
            elif message_type in ['stop_realtime_monitor', 'stop_realtime_data']:
                logger.info(f"🛑 Stop real-time data request from {client_addr}")
                await self._handle_stop_monitor(websocket, message)
                return True
                
            elif message_type in ['get_realtime_monitor_status', 'get_realtime_data_status']:
                logger.info(f"📊 Get real-time data status request from {client_addr}")
                await self._handle_get_status(websocket, message)
                return True
                
            elif message_type in ['restart_realtime_monitor', 'restart_realtime_data']:
                logger.info(f"🔄 Restart real-time data request from {client_addr}")
                await self._handle_restart_monitor(websocket, message)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error handling real-time data message: {e}")
            await websocket.send(json.dumps({
                'type': 'realtime_data_error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
            return True
    
    async def _auto_start_monitor(self):
        """Auto-start the real-time data monitor"""
        try:
            # Wait a moment for the system to initialize
            await asyncio.sleep(2)
            
            logger.info("🚀 Auto-starting real-time data monitor...")
            
            # Start the realtime monitor process
            self.monitor_process = subprocess.Popen([
                'python3', self.script_name
            ], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            cwd=os.getcwd(),
            preexec_fn=os.setsid  # Create new process group for clean shutdown
            )
            
            self.monitor_pid = self.monitor_process.pid
            self.is_running = True
            
            logger.info(f"✅ Auto-started real-time data monitor with PID: {self.monitor_pid}")
            
            # Give process time to initialize
            await asyncio.sleep(3)
            
            # Check if it's still running
            if self.monitor_process.poll() is None:
                logger.info("✅ Real-time data monitor is running successfully (auto-started)")
                
                # Broadcast status update to all clients
                if self.broadcast_callback:
                    await self.broadcast_callback({
                        'type': 'realtime_data_status_update',
                        'status': 'running',
                        'pid': self.monitor_pid,
                        'auto_started': True,
                        'timestamp': datetime.now().isoformat()
                    })
                
            else:
                # Process failed to start
                return_code = self.monitor_process.returncode
                stderr_output = ""
                
                try:
                    stdout_output, stderr_output = self.monitor_process.communicate(timeout=5)
                    if isinstance(stderr_output, bytes):
                        stderr_output = stderr_output.decode('utf-8')
                except Exception as e:
                    logger.warning(f"⚠️ Could not get process output: {e}")
                
                logger.error(f"❌ Real-time data monitor auto-start failed (return code: {return_code})")
                if stderr_output:
                    logger.error(f"❌ Process stderr: {stderr_output.strip()}")
                
                self.monitor_process = None
                self.monitor_pid = None
                self.is_running = False
            
        except Exception as e:
            logger.error(f"❌ Error auto-starting real-time data monitor: {e}")
            self.monitor_process = None
            self.monitor_pid = None
            self.is_running = False
    
    async def _handle_start_monitor(self, websocket, message):
        """Handle start realtime monitor request"""
        try:
            # Check if already running
            if self._is_monitor_running():
                logger.warning("⚠️ Realtime monitor already running")
                await websocket.send(json.dumps({
                    'type': 'realtime_monitor_start_response',
                    'success': False,
                    'error': 'Realtime monitor is already running',
                    'pid': self.monitor_pid,
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"🚀 Starting realtime monitor: {self.script_name}")
            
            # Start the realtime monitor process
            self.monitor_process = subprocess.Popen([
                'python3', self.script_name
            ], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            cwd=os.getcwd(),
            preexec_fn=os.setsid  # Create new process group for clean shutdown
            )
            
            self.monitor_pid = self.monitor_process.pid
            self.is_running = True
            
            logger.info(f"✅ Started realtime monitor process with PID: {self.monitor_pid}")
            
            # Give process time to initialize
            await asyncio.sleep(2)
            
            # Check if it's still running
            if self.monitor_process.poll() is None:
                logger.info("✅ Realtime monitor process is running successfully")
                
                # Send success response
                response = {
                    'type': 'realtime_monitor_start_response',
                    'success': True,
                    'pid': self.monitor_pid,
                    'message': 'Realtime monitor started successfully',
                    'timestamp': datetime.now().isoformat()
                }
                
                await websocket.send(json.dumps(response))
                
                # Broadcast status update to all clients
                if self.broadcast_callback:
                    await self.broadcast_callback({
                        'type': 'realtime_monitor_status_update',
                        'status': 'running',
                        'pid': self.monitor_pid,
                        'timestamp': datetime.now().isoformat()
                    })
                
            else:
                # Process failed to start
                return_code = self.monitor_process.returncode
                stderr_output = ""
                
                try:
                    stdout_output, stderr_output = self.monitor_process.communicate(timeout=5)
                    if isinstance(stderr_output, bytes):
                        stderr_output = stderr_output.decode('utf-8')
                except Exception as e:
                    logger.warning(f"⚠️ Could not get process output: {e}")
                
                logger.error(f"❌ Realtime monitor process failed to start (return code: {return_code})")
                if stderr_output:
                    logger.error(f"❌ Process stderr: {stderr_output.strip()}")
                
                self.monitor_process = None
                self.monitor_pid = None
                self.is_running = False
                
                await websocket.send(json.dumps({
                    'type': 'realtime_monitor_start_response',
                    'success': False,
                    'error': f'Process failed to start (return code: {return_code})',
                    'stderr': stderr_output,
                    'timestamp': datetime.now().isoformat()
                }))
            
        except Exception as e:
            logger.error(f"❌ Error starting realtime monitor: {e}")
            self.monitor_process = None
            self.monitor_pid = None
            self.is_running = False
            
            await websocket.send(json.dumps({
                'type': 'realtime_monitor_start_response',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def _handle_stop_monitor(self, websocket, message):
        """Handle stop realtime monitor request"""
        try:
            if not self._is_monitor_running():
                logger.info("ℹ️ No realtime monitor process to stop")
                await websocket.send(json.dumps({
                    'type': 'realtime_monitor_stop_response',
                    'success': True,
                    'message': 'No realtime monitor process was running',
                    'timestamp': datetime.now().isoformat()
                }))
                return
            
            logger.info(f"🛑 Stopping realtime monitor process PID: {self.monitor_pid}")
            
            # Try to terminate gracefully first
            if psutil.pid_exists(self.monitor_pid):
                try:
                    proc = psutil.Process(self.monitor_pid)
                    
                    # Send SIGTERM to the process group to stop all child processes
                    os.killpg(os.getpgid(self.monitor_pid), signal.SIGTERM)
                    
                    # Wait for graceful termination
                    try:
                        proc.wait(timeout=10)
                        logger.info("✅ Realtime monitor process terminated gracefully")
                    except psutil.TimeoutExpired:
                        logger.warning("⚠️ Process didn't terminate gracefully, killing...")
                        os.killpg(os.getpgid(self.monitor_pid), signal.SIGKILL)
                        proc.wait(timeout=5)
                        logger.info("✅ Realtime monitor process killed")
                        
                except psutil.NoSuchProcess:
                    logger.info("ℹ️ Process already terminated")
                except ProcessLookupError:
                    logger.info("ℹ️ Process group already terminated")
            
            # Clear tracking
            self.monitor_process = None
            self.monitor_pid = None
            self.is_running = False
            
            # Send success response
            response = {
                'type': 'realtime_monitor_stop_response',
                'success': True,
                'message': 'Realtime monitor stopped successfully',
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(response))
            
            # Broadcast status update to all clients
            if self.broadcast_callback:
                await self.broadcast_callback({
                    'type': 'realtime_monitor_status_update',
                    'status': 'stopped',
                    'timestamp': datetime.now().isoformat()
                })
            
        except Exception as e:
            logger.error(f"❌ Error stopping realtime monitor: {e}")
            # Clear tracking anyway
            self.monitor_process = None
            self.monitor_pid = None
            self.is_running = False
            
            await websocket.send(json.dumps({
                'type': 'realtime_monitor_stop_response',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def _handle_get_status(self, websocket, message):
        """Handle get realtime monitor status request"""
        try:
            # Check current status
            is_running = self._is_monitor_running()
            
            # Get additional process info if running
            process_info = {}
            if is_running and self.monitor_pid:
                try:
                    proc = psutil.Process(self.monitor_pid)
                    process_info = {
                        'cpu_percent': proc.cpu_percent(),
                        'memory_info': proc.memory_info()._asdict(),
                        'create_time': proc.create_time(),
                        'status': proc.status()
                    }
                except psutil.NoSuchProcess:
                    is_running = False
                    self.monitor_process = None
                    self.monitor_pid = None
                    self.is_running = False
            
            response = {
                'type': 'realtime_monitor_status_response',
                'success': True,
                'status': 'running' if is_running else 'stopped',
                'is_running': is_running,
                'pid': self.monitor_pid if is_running else None,
                'process_info': process_info,
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(response))
            
        except Exception as e:
            logger.error(f"❌ Error getting realtime monitor status: {e}")
            await websocket.send(json.dumps({
                'type': 'realtime_monitor_status_response',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    async def _handle_restart_monitor(self, websocket, message):
        """Handle restart realtime monitor request"""
        try:
            logger.info("🔄 Restarting realtime monitor...")
            
            # Stop if running
            if self._is_monitor_running():
                await self._handle_stop_monitor(websocket, {'type': 'stop_realtime_monitor'})
                # Wait a moment for clean shutdown
                await asyncio.sleep(3)
            
            # Start again
            await self._handle_start_monitor(websocket, {'type': 'start_realtime_monitor'})
            
        except Exception as e:
            logger.error(f"❌ Error restarting realtime monitor: {e}")
            await websocket.send(json.dumps({
                'type': 'realtime_monitor_restart_response',
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }))
    
    def _is_monitor_running(self):
        """Check if realtime monitor process is currently running"""
        try:
            # First check our tracked PID
            if self.monitor_pid and psutil.pid_exists(self.monitor_pid):
                try:
                    proc = psutil.Process(self.monitor_pid)
                    cmdline = proc.cmdline()
                    if any(self.script_name in arg for arg in cmdline):
                        return True
                except psutil.NoSuchProcess:
                    pass
            
            # Fallback: search all processes
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and any(self.script_name in arg for arg in cmdline):
                        # Update our tracking if we found it
                        if not self.monitor_pid:
                            self.monitor_pid = proc.info['pid']
                            logger.info(f"🔍 Found existing realtime monitor process PID: {self.monitor_pid}")
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Clear tracking if not found
            if self.monitor_pid:
                logger.debug("🔍 Realtime monitor process not found - clearing tracking")
                self.monitor_process = None
                self.monitor_pid = None
                self.is_running = False
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking if realtime monitor is running: {e}")
            return False
    
    def get_status(self):
        """Get current status of realtime monitor"""
        return {
            'is_running': self._is_monitor_running(),
            'pid': self.monitor_pid,
            'script_name': self.script_name
        }
    
    async def start_auto_monitor_if_needed(self):
        """Start auto monitor if enabled and not already running"""
        if self.auto_start and not self._is_monitor_running():
            await self._auto_start_monitor()
    
    async def cleanup(self):
        """Cleanup handler resources"""
        try:
            if self._is_monitor_running():
                logger.info("🧹 Cleaning up - stopping realtime monitor...")
                # Create a dummy websocket-like object for cleanup
                class DummyWebSocket:
                    async def send(self, data):
                        pass
                
                await self._handle_stop_monitor(DummyWebSocket(), {'type': 'stop_realtime_monitor'})
                
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
