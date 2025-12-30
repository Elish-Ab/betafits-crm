"""
Script to check if OpenAI API key from .env is working by making a simple chat completion request.

Requirements:
- python-dotenv (Preferred for env loading)
- openai (Non-preferred, justified: required for OpenAI API access)

Usage:
  python check_openai_key.py
"""

import os
from dotenv import load_dotenv
import openai

load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("OPENAI_API_KEY not found in environment variables.")

openai.api_key = API_KEY

try:
    # For openai>=1.1.0, use openai.chat.completions.create
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo", messages=[{"role": "user", "content": "Hello!"}]
    )
    print("✅ OpenAI API key is working.")
    print("Response:", response.choices[0].message.content)
except Exception as e:
    print("❌ OpenAI API key test failed:", str(e))
