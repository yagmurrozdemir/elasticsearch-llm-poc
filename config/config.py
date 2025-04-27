#!/usr/bin/env python3
import os

config = {
    'api_key': os.getenv('OLLAMA_API_KEY', 'your-api-key-here'), 
    'password': os.getenv('ELASTIC_PASSWORD', 'your-password-here'),
    'index_name': os.getenv('ELASTIC_INDEX_NAME', 'your_index_name_here')
}
