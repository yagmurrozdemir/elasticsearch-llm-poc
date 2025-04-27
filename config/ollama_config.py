import openai

# === Configure OpenAI to use local Ollama instance ===
openai.api_base = "http://localhost:11434/v1"
openai.api_key  = "ollama"   # Required dummy key