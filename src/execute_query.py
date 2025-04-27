#!/usr/bin/env python3
import sys
import json
from elasticsearch import Elasticsearch, exceptions
from config import config


def create_es_connection() -> Elasticsearch:

    password = config['password']
    es = Elasticsearch(
        ["https://localhost:9200"],
        basic_auth=("elastic", password),
        verify_certs=False  # WARNING: Set to True if using a trusted SSL cert
    )
    return es


def execute_search(index_name: str, query_body: dict, output_file_path: str):
    
    es_client = create_es_connection()

    try:
        response = es_client.search(
            index=index_name,
            body=query_body
        )

        with open(output_file_path, 'w', encoding='utf-8') as f:
            for hit in response.get("hits", {}).get("hits", []):
                f.write("-----------------------------------------------------------\n")
                f.write(json.dumps(hit["_source"], indent=2, ensure_ascii=False))
                f.write("\n")
            f.write("-----------------------------------------------------------\n")

    except exceptions.BadRequestError as e:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write("BadRequestError detected: ")
            f.write(str(e))
    except Exception as e:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(f"Unexpected error: {str(e)}")


def main(index_name: str, input_file_path: str, output_file_path: str):

    with open(input_file_path, 'r', encoding='utf-8') as f:
        query_template_with_vector = f.read().strip()

    try:
        query_body = json.loads(query_template_with_vector)
    except json.JSONDecodeError:
        print("Error: The input file is not valid JSON.", file=sys.stderr)
        sys.exit(1)

    execute_search(index_name, query_body, output_file_path)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python execute_search_query.py <index_name> <input_query_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
