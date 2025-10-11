#!/usr/bin/env python3
"""
Stuck Connections Handler

This handler monitors PostgreSQL connections and automatically kills stuck connections
to prevent database connection limit issues:

1. Monitors PostgreSQL processes for stuck connections
2. Identifies connections in "waiting" states (TRUNCATE TABLE, etc.)
3. Automatically kills connections that are stuck for too long
4. Runs as a background daemon to prevent connection limit issues
5. Provides logging and monitoring of connection health

Key Features:
- Background monitoring of PostgreSQL connections
- Automatic cleanup of stuck connections
- Configurable thresholds and timeouts
- Detailed logging and reporting
- Safe connection management
- Prevention of connection limit exhaustion
"""

import subprocess
import time
import logging
import threading
import signal
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Set
import re
import os

class StuckConnectionsHandler:
    """
    Stuck Connections Handler for monitoring and cleaning up PostgreSQL connections
    """
    
    def __init__(self, 
                 check_interval: int = 30,
                 stuck_threshold_minutes: int = 5,
                 max_waiting_connections: int = 20,
                 auto_kill_enabled: bool = True):
        """
        Initialize the Stuck Connections Handler.
        
        Args:
            check_interval: How often to check for stuck connections (seconds)
            stuck_threshold_minutes: How long before a connection is considered stuck (minutes)
            max_waiting_connections: Maximum number of waiting connections before cleanup
            auto_kill_enabled: Whether to automatically kill stuck connections
        """
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.check_interval = check_interval
        self.stuck_threshold = timedelta(minutes=stuck_threshold_minutes)
        self.max_waiting_connections = max_waiting_connections
        self.auto_kill_enabled = auto_kill_enabled
        
        # State tracking
        self.running = False
        self.monitor_thread = None
        self.connection_history = {}  # Track when connections started
        self.cleanup_stats = {
            'total_checks': 0,
            'connections_killed': 0,
            'last_cleanup': None,
            'max_waiting_seen': 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("StuckConnectionsHandler initialized")
        self.logger.info(f"Configuration: check_interval={check_interval}s, "
                        f"stuck_threshold={stuck_threshold_minutes}min, "
                        f"max_waiting={max_waiting_connections}, "
                        f"auto_kill={auto_kill_enabled}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop_monitoring()
        sys.exit(0)

    def get_postgres_processes(self) -> List[Dict]:
        """
        Get all PostgreSQL processes and parse their information.
        
        Returns:
            List of dictionaries containing process information
        """
        try:
            # Get PostgreSQL processes
            result = subprocess.run(
                ['ps', 'aux'], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            
            if result.returncode != 0:
                self.logger.error(f"Failed to get processes: {result.stderr}")
                return []
            
            processes = []
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if 'postgres:' in line and 'volflow_options' in line:
                    # Parse process information
                    parts = line.split()
                    if len(parts) >= 11:
                        try:
                            process_info = {
                                'user': parts[0],
                                'pid': int(parts[1]),
                                'cpu': float(parts[2]),
                                'mem': float(parts[3]),
                                'start_time': parts[8],
                                'command': ' '.join(parts[10:]),
                                'full_line': line
                            }
                            
                            # Extract connection state
                            if 'waiting' in process_info['command'].lower():
                                process_info['state'] = 'waiting'
                                process_info['waiting_on'] = self._extract_waiting_reason(process_info['command'])
                            elif 'idle' in process_info['command'].lower():
                                process_info['state'] = 'idle'
                            else:
                                process_info['state'] = 'active'
                            
                            processes.append(process_info)
                            
                        except (ValueError, IndexError) as e:
                            self.logger.debug(f"Failed to parse process line: {line}, error: {e}")
                            continue
            
            return processes
            
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout getting PostgreSQL processes")
            return []
        except Exception as e:
            self.logger.error(f"Error getting PostgreSQL processes: {e}")
            return []

    def _extract_waiting_reason(self, command: str) -> str:
        """Extract what the connection is waiting on."""
        if 'TRUNCATE TABLE waiting' in command:
            return 'TRUNCATE_TABLE'
        elif 'SELECT waiting' in command:
            return 'SELECT'
        elif 'INSERT waiting' in command:
            return 'INSERT'
        elif 'UPDATE waiting' in command:
            return 'UPDATE'
        elif 'DELETE waiting' in command:
            return 'DELETE'
        else:
            return 'UNKNOWN'

    def analyze_connections(self) -> Dict:
        """
        Analyze current PostgreSQL connections and identify issues.
        
        Returns:
            Dictionary with connection analysis
        """
        processes = self.get_postgres_processes()
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'total_connections': len(processes),
            'waiting_connections': 0,
            'idle_connections': 0,
            'active_connections': 0,
            'stuck_connections': [],
            'waiting_by_type': {},
            'oldest_waiting': None,
            'needs_cleanup': False
        }
        
        current_time = datetime.now()
        
        for process in processes:
            state = process.get('state', 'unknown')
            
            if state == 'waiting':
                analysis['waiting_connections'] += 1
                waiting_on = process.get('waiting_on', 'UNKNOWN')
                analysis['waiting_by_type'][waiting_on] = analysis['waiting_by_type'].get(waiting_on, 0) + 1
                
                # Track connection history
                pid = process['pid']
                if pid not in self.connection_history:
                    self.connection_history[pid] = current_time
                
                # Check if connection is stuck
                connection_age = current_time - self.connection_history[pid]
                if connection_age > self.stuck_threshold:
                    process['stuck_duration'] = connection_age
                    analysis['stuck_connections'].append(process)
                
                # Track oldest waiting connection
                if (analysis['oldest_waiting'] is None or 
                    connection_age > analysis['oldest_waiting'].get('stuck_duration', timedelta(0))):
                    analysis['oldest_waiting'] = process.copy()
                    analysis['oldest_waiting']['stuck_duration'] = connection_age
                    
            elif state == 'idle':
                analysis['idle_connections'] += 1
            else:
                analysis['active_connections'] += 1
        
        # Clean up history for processes that no longer exist
        current_pids = {p['pid'] for p in processes}
        old_pids = set(self.connection_history.keys()) - current_pids
        for pid in old_pids:
            del self.connection_history[pid]
        
        # Determine if cleanup is needed
        analysis['needs_cleanup'] = (
            analysis['waiting_connections'] > self.max_waiting_connections or
            len(analysis['stuck_connections']) > 0
        )
        
        # Update stats
        self.cleanup_stats['max_waiting_seen'] = max(
            self.cleanup_stats['max_waiting_seen'],
            analysis['waiting_connections']
        )
        
        return analysis

    def kill_stuck_connections(self, analysis: Dict) -> int:
        """
        Kill stuck connections based on analysis.
        
        Args:
            analysis: Connection analysis from analyze_connections()
            
        Returns:
            Number of connections killed
        """
        if not self.auto_kill_enabled:
            self.logger.info("Auto-kill disabled, skipping connection cleanup")
            return 0
        
        stuck_connections = analysis.get('stuck_connections', [])
        if not stuck_connections:
            return 0
        
        killed_count = 0
        
        for connection in stuck_connections:
            try:
                pid = connection['pid']
                waiting_on = connection.get('waiting_on', 'UNKNOWN')
                duration = connection.get('stuck_duration', timedelta(0))
                
                self.logger.warning(f"Killing stuck connection PID {pid} "
                                  f"(waiting on {waiting_on} for {duration})")
                
                # Kill the process
                result = subprocess.run(
                    ['kill', '-9', str(pid)],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                if result.returncode == 0:
                    killed_count += 1
                    self.logger.info(f"✅ Successfully killed stuck connection PID {pid}")
                else:
                    self.logger.error(f"❌ Failed to kill connection PID {pid}: {result.stderr}")
                    
            except Exception as e:
                self.logger.error(f"❌ Error killing connection PID {connection.get('pid', 'unknown')}: {e}")
        
        # Also kill connections if there are too many waiting
        if analysis['waiting_connections'] > self.max_waiting_connections:
            self.logger.warning(f"Too many waiting connections ({analysis['waiting_connections']}), "
                              f"killing all waiting connections")
            
            try:
                result = subprocess.run(
                    ['pkill', '-9', '-f', 'TRUNCATE TABLE waiting'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    additional_killed = analysis['waiting_connections'] - len(stuck_connections)
                    killed_count += additional_killed
                    self.logger.info(f"✅ Killed {additional_killed} additional waiting connections")
                else:
                    self.logger.warning(f"pkill returned {result.returncode}, trying alternative method")
                    # Try alternative method
                    result2 = subprocess.run(
                        ['pkill', '-9', '-f', 'waiting'],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result2.returncode == 0:
                        killed_count += analysis['waiting_connections']
                        self.logger.info(f"✅ Killed waiting connections using alternative method")
                    
            except Exception as e:
                self.logger.error(f"❌ Error killing waiting connections: {e}")
        
        if killed_count > 0:
            self.cleanup_stats['connections_killed'] += killed_count
            self.cleanup_stats['last_cleanup'] = datetime.now()
        
        return killed_count

    def monitor_connections(self):
        """Main monitoring loop that runs in background thread."""
        self.logger.info("🔄 Starting connection monitoring loop")
        
        while self.running:
            try:
                start_time = time.time()
                
                # Analyze connections
                analysis = self.analyze_connections()
                self.cleanup_stats['total_checks'] += 1
                
                # Log current status
                self.logger.info(f"📊 Connection Status: "
                               f"Total={analysis['total_connections']}, "
                               f"Waiting={analysis['waiting_connections']}, "
                               f"Stuck={len(analysis['stuck_connections'])}, "
                               f"Idle={analysis['idle_connections']}")
                
                # Kill stuck connections if needed
                if analysis['needs_cleanup']:
                    self.logger.warning("🚨 Connection cleanup needed!")
                    killed = self.kill_stuck_connections(analysis)
                    if killed > 0:
                        self.logger.info(f"🧹 Cleaned up {killed} stuck connections")
                
                # Log waiting connection details if any
                if analysis['waiting_connections'] > 0:
                    waiting_types = analysis['waiting_by_type']
                    type_summary = ', '.join([f"{k}:{v}" for k, v in waiting_types.items()])
                    self.logger.info(f"⏳ Waiting connections by type: {type_summary}")
                
                # Sleep for the remaining interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.check_interval - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"❌ Error in monitoring loop: {e}")
                time.sleep(self.check_interval)
        
        self.logger.info("🛑 Connection monitoring stopped")

    def start_monitoring(self):
        """Start the background monitoring thread."""
        if self.running:
            self.logger.warning("Monitoring already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self.monitor_connections, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info("🚀 Started background connection monitoring")

    def stop_monitoring(self):
        """Stop the background monitoring thread."""
        if not self.running:
            return
        
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        self.logger.info("🛑 Stopped background connection monitoring")

    def get_stats(self) -> Dict:
        """Get monitoring statistics."""
        return {
            'running': self.running,
            'configuration': {
                'check_interval': self.check_interval,
                'stuck_threshold_minutes': self.stuck_threshold.total_seconds() / 60,
                'max_waiting_connections': self.max_waiting_connections,
                'auto_kill_enabled': self.auto_kill_enabled
            },
            'stats': self.cleanup_stats.copy(),
            'current_connections': len(self.connection_history)
        }

    def run_single_check(self) -> Dict:
        """Run a single connection check and cleanup (for testing/manual use)."""
        self.logger.info("🔍 Running single connection check...")
        
        analysis = self.analyze_connections()
        self.cleanup_stats['total_checks'] += 1
        
        if analysis['needs_cleanup']:
            killed = self.kill_stuck_connections(analysis)
            analysis['connections_killed'] = killed
        else:
            analysis['connections_killed'] = 0
        
        return analysis


def main():
    """Main function to run the stuck connections handler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL Stuck Connections Handler')
    parser.add_argument('--check-interval', type=int, default=30,
                       help='Check interval in seconds (default: 30)')
    parser.add_argument('--stuck-threshold', type=int, default=5,
                       help='Stuck threshold in minutes (default: 5)')
    parser.add_argument('--max-waiting', type=int, default=20,
                       help='Maximum waiting connections before cleanup (default: 20)')
    parser.add_argument('--no-auto-kill', action='store_true',
                       help='Disable automatic killing of stuck connections')
    parser.add_argument('--single-check', action='store_true',
                       help='Run a single check and exit')
    
    args = parser.parse_args()
    
    # Create handler
    handler = StuckConnectionsHandler(
        check_interval=args.check_interval,
        stuck_threshold_minutes=args.stuck_threshold,
        max_waiting_connections=args.max_waiting,
        auto_kill_enabled=not args.no_auto_kill
    )
    
    if args.single_check:
        # Run single check
        print("🔍 Running single connection check...")
        result = handler.run_single_check()
        
        print(f"📊 Results:")
        print(f"   • Total connections: {result['total_connections']}")
        print(f"   • Waiting connections: {result['waiting_connections']}")
        print(f"   • Stuck connections: {len(result['stuck_connections'])}")
        print(f"   • Connections killed: {result.get('connections_killed', 0)}")
        
        if result['waiting_by_type']:
            print(f"   • Waiting by type: {result['waiting_by_type']}")
    else:
        # Run continuous monitoring
        print("🚀 Starting PostgreSQL Stuck Connections Handler...")
        print(f"   • Check interval: {args.check_interval} seconds")
        print(f"   • Stuck threshold: {args.stuck_threshold} minutes")
        print(f"   • Max waiting connections: {args.max_waiting}")
        print(f"   • Auto-kill enabled: {not args.no_auto_kill}")
        print("Press Ctrl+C to stop")
        print("="*50)
        
        try:
            handler.start_monitoring()
            
            # Keep main thread alive
            while handler.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
        finally:
            handler.stop_monitoring()
            
            # Print final stats
            stats = handler.get_stats()
            print(f"\n📊 Final Statistics:")
            print(f"   • Total checks: {stats['stats']['total_checks']}")
            print(f"   • Connections killed: {stats['stats']['connections_killed']}")
            print(f"   • Max waiting seen: {stats['stats']['max_waiting_seen']}")


if __name__ == "__main__":
    main()
