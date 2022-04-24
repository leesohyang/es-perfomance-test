#-*- coding:utf-8 -*-
import os
import json
from elasticsearch import Elasticsearch, helpers
import uuid

import numpy as np
from openpyxl import load_workbook

import io
import re


elastic = Elasticsearch(hosts="", timeout=30, max_retris=10, retry_on_timeout=True)


import time
from elasticsearch import Elasticsearch, helpers
import uuid
import pandas as pd


def get_data_from_file(file_name):
    if "/" in file_name or chr(92) in file_name:
        file = open(file_name, encoding="utf8", errors='ignore')
    else:
        # use the script_path() function to get path if none is passed
        file = open(file_name, encoding="utf8", errors='ignore')
    data = [line.strip() for line in file]
    file.close()
    return data


# -----


def bulk_json_data(json_file, _index, doc_type):
    json_list = get_data_from_file(json_file)
    for doc in json_list:
        # use a `yield` generator so that the data
        # isn't loaded into memory

        json_tmp = json.loads(doc, strict=False)

        if 'logDttm' not in doc:
            pass

        elif '{"index"' not in doc:
            yield {
                "_index": _index,
                # "_type": doc_type,
                "_id": uuid.uuid4(),
                "_source": json_tmp

            }


if __name__ == '__main__':

    file_dir = '/tmp/DATA/raw_total'
    mapping_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mapping'))

    columns = ["file_name", "edge_ngram", "not analyzed", "standard", "whitespace"]

    total_row = list()
    for i, file in enumerate(os.listdir(file_dir)):
        print('File : {}'.format(file))
        row = list()
        row.append(file)
        for mapping_file in os.listdir(mapping_dir):
            with open(os.path.join(mapping_dir, mapping_file), 'r', encoding='utf-8') as mf:
                mapping = json.load(mf)

            with open(os.path.join(file_dir, file), 'r', encoding='utf-8') as f:
                start = time.time()
                if elastic.indices.exists(index="{0}_{1}".format(file, mapping_file)):
                    elastic.indices.delete(index="{0}_{1}".format(file, mapping_file))

                elastic.indices.create(index="{0}_{1}".format(file, mapping_file), body=mapping)
                try:
                    response = helpers.bulk(elastic, bulk_json_data(os.path.join(file_dir, file),
                                                                    "{0}_{1}".format(file, mapping_file), "_doc"))
                except Exception as e:
                    print("\nERROR:", e)
                # put filename and time(single row) to df
                spend_time = time.time() - start
                row.append(spend_time)
                print("finish indexing {0}_{1}".format(file, mapping_file))

        total_row.append(row)
        df = pd.DataFrame(np.array(total_row), columns=columns)
        append_df_to_excel("es_tmp_result.xlsx", df)
