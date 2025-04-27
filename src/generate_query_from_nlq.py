#!/usr/bin/env python3

import openai
import re
import sys
from pathlib import Path

import config.ollama_client  # Loads Ollama settings automatically

MODEL_NAME = "qwen2.5-coder:14b"
PROMPT_TEMPLATE_PATH = Path("prompts/query_generation_prompt.txt")


def load_prompt_template() -> str:
    with open(PROMPT_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        return f.read()


def get_llm_response(prompt_text: str) -> str:
    resp = openai.ChatCompletion.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt_text}],
        temperature=0.7,
        stream=False,
    )
    text = resp.choices[0].message.content
    return re.sub(r"<think>.*?</think>\s*", "", text, flags=re.DOTALL).strip()


def generate_query(index_mapping: str, nl_query: str) -> str:

    # Example data to fill in the template
    example_nlq = "How many schools are in a location characterized as: \"Bloomington is a city in Monroe County, Indiana, United States, and its county seat. The population was 79,168 at the 2020 census. It is the seventh‑most populous city in Indiana and the fourth‑most populous outside the Indianapolis metropolitan area. It is the home of Indiana University Bloomington, the flagship campus of the Indiana University system. Established in 1820, IU Bloomington enrolls over 45,000 students. The city was established in 1818 by a group of settlers from Kentucky, Tennessee, the Carolinas, and Virginia who were so impressed with \\\"a haven of blooms\\\" that they called it Bloomington. It is the principal city of the Bloomington metropolitan area in south‑central Indiana, which had 161,039 residents in 2020. Bloomington has been designated a Tree City USA since 1984. The city was also the location of the Academy Award–winning 1979 movie Breaking Away, featuring a reenactment of Indiana University's annual Little 500 bicycle race.\""
    example_output_query = """
    {
      "query": {
        "bool": {
          "must": [
            {
              "knn": {
                "field": "Location",
                "query_vector": $vector$,
                "k": 20,
                "similarity": 0.98
              }
            }
          ]
        }
      },
      "aggs": {
        "value_count_Founded": {
          "value_count": {
            "field": "Founded"
          }
        }
      },
      "_source": [
        "value_count_Founded"
      ]
    }
    """
    example_vector_content  = "\"Bloomington is a city in Monroe County, Indiana, United States, and its county seat. The population was 79,168 at the 2020 census. It is the seventh‑most populous city in Indiana and the fourth‑most populous outside the Indianapolis metropolitan area. It is the home of Indiana University Bloomington, the flagship campus of the Indiana University system. Established in 1820, IU Bloomington enrolls over 45,000 students. The city was established in 1818 by a group of settlers from Kentucky, Tennessee, the Carolinas, and Virginia who were so impressed with \\\"a haven of blooms\\\" that they called it Bloomington. It is the principal city of the Bloomington metropolitan area in south‑central Indiana, which had 161,039 residents in 2020. Bloomington has been designated a Tree City USA since 1984. The city was also the location of the Academy Award–winning 1979 movie Breaking Away, featuring a reenactment of Indiana University's annual Little 500 bicycle race.\""
    example_nlq_2 = "Tell me what the notes are for South Australia"
    example_output_query_2 = """
    {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "Current slogan.keyword": {
                  "value": "SOUTH AUSTRALIA",
                  "case_insensitive": true
                }
              }
            }
          ]
        }
      },
      "_source": [
        "Notes"
      ]
    }
    """

    template = load_prompt_template()
    prompt_text = template.format(
        example_nlq=example_nlq,
        example_output_query=example_output_query,
        example_nlq_2=example_nlq_2,
        example_output_query_2=example_output_query_2,
        example_vector_content=example_vector_content,
        index_mapping=index_mapping,
        nl_query=nl_query,
    )
    return get_llm_response(prompt_text)


def main(nlq, mapping_path: str, output_path: str):

    # Load the natural language query
   
    nl_query  = nlq
    # Load the index mapping
    with open(mapping_path, 'r', encoding='utf-8') as f:
        index_mapping = f.read().strip()

    # Generate query
    result = generate_query(index_mapping, nl_query)

    # Save output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(result)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_query_from_nlq.py <nlq.txt> <mapping.txt> <output.txt>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
