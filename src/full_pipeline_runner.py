#!/usr/bin/env python3

"""
Full end-to-end pipeline to generate, execute, and validate Elasticsearch queries
using an LLM, CLIP embedding model, and Elasticsearch.
"""

import subprocess
import sys
import json
from pathlib import Path

# Constants
DATA_DIR = Path("outputs/")
INPUT_FILE = Path("data/demo.jsonl")

# Output file paths
MAPPING_PATH = DATA_DIR / "mapping.txt"
QWEN_RESPONSE_PATH = DATA_DIR / "qwen_response.txt"
EMBEDDING_PATH = DATA_DIR / "embedding.txt"
QUERY_WITH_VECTOR_PATH = DATA_DIR / "query.txt"
FINAL_RESULTS_PATH = DATA_DIR / "final_results.txt"
CORRECT_RESULTS_PATH = DATA_DIR / "correct_results.txt"


def run(cmd_list: list, description: str = ""):
    if description:
        print(f"🔹 {description}")
    subprocess.run(cmd_list, check=True)


def compare_files(file1_path: Path, file2_path: Path) -> int:
    with open(file1_path, 'r', encoding='utf-8') as f1, open(file2_path, 'r', encoding='utf-8') as f2:
        return int(f1.read().strip() == f2.read().strip())


def main(index_name: str, nlq: str, json_data: str):

    # 1. Export index mapping
    run(["python3", "src/export_index_mapping.py", index_name, str(MAPPING_PATH)],
        description="Exporting index mapping")

    # 2. Generate DSL query using LLM
    run(["python3", "src/generate_query_from_nlq.py", nlq, str(MAPPING_PATH), str(QWEN_RESPONSE_PATH)],
        description="Generating DSL query from LLM")

    # 3. Generate embedding if needed
    run(["python3", "src/generate_embedding.py", str(QWEN_RESPONSE_PATH), str(EMBEDDING_PATH)],
        description="Generating embedding if needed")

    # 4. Inject embedding into query
    run(["python3", "src/inject_embedding_into_query.py", str(QWEN_RESPONSE_PATH), str(EMBEDDING_PATH), str(QUERY_WITH_VECTOR_PATH)],
        description="Injecting embedding into query")

    # 5. Execute generated query
    run(["python3", "src/execute_query.py", index_name, str(QUERY_WITH_VECTOR_PATH), str(FINAL_RESULTS_PATH)],
        description="Executing generated query")

    # 6. Execute correct expected query
    run(["python3", "src/run_correct_query.py", json_data, str(CORRECT_RESULTS_PATH)],
        description="Executing ground-truth query")


if __name__ == "__main__":
    correct_count = 0
    total = 0

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as infile:
            for line in infile:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                table_id = data["table_id"]
                index_name = f"table{table_id.replace('-', '_')[1:]}"
                nlq = data["question"]
                json_data = json.dumps(data)  # serialize back to JSON string

                try:
                    total += 1
                    main(index_name, nlq, json_data)
                    match = compare_files(CORRECT_RESULTS_PATH, FINAL_RESULTS_PATH)
                    correct_count += match
                    print(f" Match: {match}   ({correct_count}/{total})")
                except subprocess.CalledProcessError as e:
                    print(f"Step failed: {e.cmd}", file=sys.stderr)
                    sys.exit(1)

        print(f"\n Final Result: {correct_count} correct out of {total}")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
