"""
Simple configuration loader for historical data handler.
"""

import yaml
import os

class ConfigLoader:
    """Simple configuration loader."""
    
    def __init__(self, config_file: str = "config.yaml"):
        """Initialize with config file."""
        self.config_file = config_file
        self.config = self._load_default_config()
        
        # Try to load from file, but don't fail if it doesn't exist
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as file:
                    file_config = yaml.safe_load(file) or {}
                    self.config.update(file_config)
            except Exception:
                pass  # Use defaults if file can't be loaded
    
    def _load_default_config(self):
        """Load default configuration."""
        return {
            'api': {
                'base_url': 'https://api.schwabapi.com',
                'max_retries': 5,
                'retry_delay': 2,
                'rate_limit_delay': 60
            }
        }
    
    def get_api_config(self):
        """Get API configuration."""
        return self.config.get('api', {})

# Global instance
_config_instance = None

def get_config(config_file: str = "config.yaml"):
    """Get global config instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader(config_file)
    return _config_instance
