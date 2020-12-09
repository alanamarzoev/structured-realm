import json 
import pandas as pd 

import abc
from concurrent import futures
import time


from absl import logging
from language.realm import featurization
from language.realm import parallel
from language.realm import profile
from subprocess import check_call

import numpy as np
import tensorflow.compat.v1 as tf
import tensorflow_hub as hub


def get_dataframes():
    with open('tables_small.json', 'r') as j:
        lines = j.readlines()
        tbls = {}
        for line in lines: 
            contents = json.loads(line)
            table = {}
            col_order = []
            for i, col in enumerate(contents['tableHeaders'][0]): 
                col_name = col['text']
                table[col_name] = []
                col_order.append(col_name)
            for row_cell in range(len(contents['tableData'])):
                for col_cell in range(len(contents['tableData'][row_cell])):
                    col_name = col_order[col_cell]
                    data = contents['tableData'][row_cell][col_cell]['text']
                    if data == '': 
                        continue
                    table[col_name].append(data)    
            try: 
                tbl = pd.DataFrame.from_dict(table)
                caption = contents['tableCaption']
                title = contents['pgTitle']
                sec_title = contents['sectionTitle']
                table_info = {}
                table_info['data'] = tbl 
                table_info['sec_title'] = sec_title 
                table_info['title'] = title 
                tbls[caption] = table_info
            except Exception as e:
                print('SKIPPING') 
                continue 
        return tbls


def gzipFile(path):
    check_call('gzip ' + path)


def convert_dataframes(tbls): 
    json_list = []
    path = 'dataset.tfrecord'
    with open(path, 'a+') as f: 
        for capt, info in tbls.items(): 
            info['title'] = capt 
            info['body'] = info['data'].to_string()
            jsoned = info.to_json()
            f.write(jsoned)
    gzipFile(path)
      

def load_doc(path):  
    """Loads Documents from a GZIP-ed TFRecords file into a Python list."""
    gzip_option = tf.python_io.TFRecordOptions(
        tf.python_io.TFRecordCompressionType.GZIP)

    def get_bytes_feature(ex, name):
        return list(ex.features.feature[name].bytes_list.value)

    def get_ints_feature(ex, name):
        # 32-bit Numpy arrays are more memory-efficient than Python lists.
        return np.array(ex.features.feature[name].int64_list.value, dtype=np.int32)

    docs = []
    for val in tf.python_io.tf_record_iterator(path, gzip_option):
        try: 
            ex = tf.train.Example.FromString(val)
            title = get_bytes_feature(ex, 'title')[0]
            body = get_bytes_feature(ex, 'body')[0]    
            doc_uid = featurization.get_document_uid(title, body)
            title_token_ids = get_ints_feature(ex, 'title_token_ids')
            body_token_ids = get_ints_feature(ex, 'body_token_ids')

            doc = featurization.Document(
                uid=doc_uid,
                title_token_ids=title_token_ids,
                body_token_ids=body_token_ids)
            docs.append(doc)
        except Exception as e: 
            continue

    
    import ipdb; ipdb.set_trace()
    return docs


def main():
    # example_path = '/raid/lingo/marzoev/structured-realm/language/realm-data-small/pretrain_corpus_small/wikipedia_annotated_with_dates_public-00000-of-00020.tfrecord.gz'
    # load_doc(example_path)
    tbls = get_dataframes()
    convert_dataframes(tbls)
    load_doc('dataset.tfrecord.gz')
        

if __name__ == '__main__': 
    main()