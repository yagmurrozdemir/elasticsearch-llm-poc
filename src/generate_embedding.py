#!/usr/bin/env python3
import sys
import requests

# Constants
EMBEDDING_API_URL = "http://127.0.0.1:8000/text-embedding/"


def get_embedding(text: str) -> list:
    response = requests.post(
        EMBEDDING_API_URL,
        json={"text": text},
        headers={"Content-Type": "application/json"},
    )

    if response.status_code != 200:
        raise Exception(f"Embedding server error: {response.status_code} {response.text}")

    return response.json()["embedding"][0]


def main(input_file_path: str, output_file_path: str):
    
    with open(input_file_path, 'r', encoding='utf-8') as f:
        result = f.read().strip()

    if "~" not in result:
        # No embedding required, write empty file
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write('')
        return

    query_template, text = result.split('~', 1)
    embedding = get_embedding(text)

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(str(embedding))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_embedding.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
