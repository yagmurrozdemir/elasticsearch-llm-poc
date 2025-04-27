#!/usr/bin/env python3

"""
Convert a WikiSQL-style encoded query into an Elasticsearch DSL query,
execute it against an ES index, and save the results.
"""

import json
import sys
import pandas as pd
from elasticsearch import Elasticsearch, exceptions
from config import config
from pathlib import Path


TYPES_FILE = Path("data/types.csv")
MASTER_CSV = Path("data/headers.csv")


def create_es_connection() -> Elasticsearch:

    password = config['password']
    es = Elasticsearch(
        ["https://localhost:9200"],
        basic_auth=("elastic", password),
        verify_certs=False
    )
    return es


class Query:
    agg_ops = ['', 'max', 'min', 'value_count', 'sum', 'avg']
    cond_ops = ['=', '>', '<', 'OP']
    agg_ops_sql = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']


def load_table_metadata(master_csv: Path, types_file: Path, table_id: str):
    master_df = pd.read_csv(master_csv)
    types_df = pd.read_csv(types_file)

    table_row = master_df[master_df['Table ID'].str.strip() == table_id.strip()]
    type_row = types_df[types_df['Table ID'].str.strip() == table_id.strip()]

    if table_row.empty or type_row.empty:
        print(f"⏩ Skipping: Table ID '{table_id}' not found.")
        return None, None

    column_data = table_row.iloc[0]['Headers']
    type_data = type_row.iloc[0]['Types']

    return [col.strip() for col in column_data.split(';')], [typ.strip() for typ in type_data.split(';')]


def convert_to_elasticsearch_dsl(encoded_query: dict, master_csv: Path, types_file: Path):
    table_id = encoded_query['table_id']
    question = encoded_query['question']
    sql = encoded_query['sql']

    index_name = f"table{table_id.replace('-', '_')[1:]}"
    columns, types = load_table_metadata(master_csv, types_file, table_id)
    if columns is None or types is None:
        return None

    sel_col = columns[sql['sel']]
    sel_col_type = types[sql['sel']]
    agg_op = Query.agg_ops[sql['agg']]

    conditions = []
    for col_index, op_index, value in sql['conds']:
        col_name = columns[col_index]
        operator = Query.cond_ops[op_index]
        col_type = types[col_index]

        if col_name.endswith("."):
            col_name = col_name[:-1]

        if col_type == 'text':
            conditions.append({
                "term": {f"{col_name}.keyword": {"value": value, "case_insensitive": True}}
            })
        elif col_type == 'dense_vector':
            conditions.append({
                "knn": {"field": col_name, "query_vector": value, "k": 20, "similarity": 0.98}
            })
        else:
            if isinstance(value, str):
                value = value.replace(',', '.')
            if operator == '=':
                conditions.append({"term": {col_name: {"value": value}}})
            elif operator == '>':
                conditions.append({"range": {col_name: {"gt": value}}})
            elif operator == '<':
                conditions.append({"range": {col_name: {"lt": value}}})

    query_dsl = {
        "query": {
            "bool": {
                "must": conditions
            }
        }
    }

    # Handle aggregation
    if sel_col.endswith("."):
        sel_col = sel_col[:-1]

    if agg_op:
        field_name = f"{sel_col}.keyword" if sel_col_type == 'text' else sel_col
        query_dsl["aggs"] = {
            f"{agg_op}_{sel_col}": {agg_op: {"field": field_name}}
        }
        query_dsl["_source"] = [f"{agg_op}_{sel_col}"]
        agg_info = True
    else:
        query_dsl["_source"] = [sel_col]
        agg_info = False

    return agg_info, index_name, query_dsl, question


def execute_query(index_name: str, dsl_query: dict):
    es_client = create_es_connection()
    response = es_client.search(index=index_name, body=dsl_query)
    return response


def main(encoded_query_str: str, output_file_path: str):

    encoded_query = json.loads(encoded_query_str)

    result = convert_to_elasticsearch_dsl(encoded_query, MASTER_CSV, TYPES_FILE)
    if result is None:
        print("No query generated. Exiting.")
        return

    agg_info, index_name, dsl_query, question = result

    print(f"Index: {index_name}")
    print(f"Question: {question}")
    print("Generated Elasticsearch Query:")
    print(json.dumps(dsl_query, indent=2))

    search_result = execute_query(index_name, dsl_query)

    with open(output_file_path, "w", encoding='utf-8') as f:
        for hit in search_result["hits"]["hits"]:
            f.write("-----------------------------------------------------------\n")
            f.write(json.dumps(hit["_source"], indent=2, ensure_ascii=False))
            f.write("\n")
        f.write("-----------------------------------------------------------\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_query_pipeline.py <encoded_query_json> <output_file>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
