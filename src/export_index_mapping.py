import sys
from elasticsearch import Elasticsearch, exceptions
from config import config


def create_es_connection() -> Elasticsearch:
    password = config['password']
    es = Elasticsearch(
        ["https://localhost:9200"],
        basic_auth=("elastic", password),
        verify_certs=False  
    )
    return es

def main(index_name, output_file_path):
    # Get index mapping
    es_client = create_es_connection()
    mapping = es_client.indices.get_mapping(index=index_name)
    # Save the output to a file
    with open(output_file_path, 'w') as f:
        f.write(str(mapping))

if __name__ == "__main__":

    index_name = sys.argv[1]
    output_file_path = sys.argv[2]
    main(index_name, output_file_path)