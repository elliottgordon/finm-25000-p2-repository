# API Keys Configuration
# This file shows how to securely load API keys from environment variables

import os
from typing import Optional

def load_api_key(key_name: str, required: bool = True) -> Optional[str]:
    """
    Load an API key from environment variables with error handling
    
    Args:
        key_name: Name of the environment variable
        required: Whether this key is required (raises error if missing)
        
    Returns:
        API key string or None if not required and not found
    """
    key = os.getenv(key_name)
    
    if key is None or key.strip() == "":
        if required:
            raise ValueError(f"Required API key '{key_name}' not found in environment variables. "
                           f"Please set it in your .env file or environment.")
        return None
    
    return key.strip()

# Load API keys (will be None until you create .env file)
try:
    NEWSAPI_KEY = load_api_key("NEWSAPI_KEY", required=False)
    ALPHA_VANTAGE_KEY = load_api_key("ALPHA_VANTAGE_KEY", required=False)
    
    print("✅ API keys configuration loaded successfully")
    if NEWSAPI_KEY:
        print(f"   News API: Configured (key length: {len(NEWSAPI_KEY)})")
    else:
        print("   News API: Not configured")
        
    if ALPHA_VANTAGE_KEY:
        print(f"   Alpha Vantage: Configured (key length: {len(ALPHA_VANTAGE_KEY)})")
    else:
        print("   Alpha Vantage: Not configured")
        
except ValueError as e:
    print(f"❌ API Key Error: {e}")
    NEWSAPI_KEY = None
    ALPHA_VANTAGE_KEY = None
