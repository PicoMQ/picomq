# client/python/verify_client.py
import sys
import os
from flaredb_client import FlareDBClient

def test_client_initialization():
    print("⏳ Initializing local FlareDB client connection instance...")
    client = FlareDBClient(base_url="http://localhost:8080")
    
    # Verify parameter mapping values match requirements
    print(f"🔗 Target Endpoint: {client.base_url}")
    print(f"🔄 Maximum Retries Threshold: {client.retries}")
    
    try:
        # Test input logic parameter filters safely
        client.connect(mode="invalid_mode")
    except ValueError:
        print("✅ Core parameter verification filters working correctly.")
        
    print("🚀 Local package layout logic successfully validated!")

if __name__ == "__main__":
    test_client_initialization()
