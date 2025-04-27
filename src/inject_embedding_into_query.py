#!/usr/bin/env python3

"""
Injects an embedding vector into a generated Elasticsearch query template.
"""

import sys


def clean_result_file(content: str) -> str:
    return content.replace("```json", "").replace("```", "").strip()


def inject_vector(query_template: str, embedding: str) -> str:
    query_with_vector = query_template.replace('"$vector$"', embedding)
    query_with_vector = query_with_vector.replace('$vector$', embedding)
    return query_with_vector


def main(input_file1_path: str, input_file2_path: str, output_file_path: str):

    with open(input_file1_path, 'r', encoding='utf-8') as f:
        result = f.read().strip()

    cleaned_result = clean_result_file(result)

    if "~" not in cleaned_result:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(cleaned_result)
        return

    query_template, _ = cleaned_result.split('~', 1)

    with open(input_file2_path, 'r', encoding='utf-8') as f:
        embedding_str = f.read().strip()

    final_query = inject_vector(query_template, embedding_str)

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(final_query)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python inject_embedding_into_query.py <input_query_file> <embedding_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
