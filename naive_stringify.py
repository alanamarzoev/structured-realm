import tensorflow.compat.v1 as tf
tf.enable_eager_execution()

import tensorflow_hub as hub
import json 
import pandas as pd 
from absl import app
from absl import flags

import abc
from concurrent import futures
import time
import os
import bert
from bert import tokenization


from absl import logging
from language.realm import featurization
from language.realm import parallel
from language.realm import profile
from language.realm.refresh_doc_embeds import load_featurizer
from subprocess import check_call

import numpy as np


def get_dataframes():
    with open('tables.jsonl', 'r') as j:
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
                table_info['body'] = tbl.to_json()
                table_info['sec_title'] = sec_title 
                table_info['pgtitle'] = title 
                table_info['id'] = contents['tableId']
                table_info['title'] = caption 
                tbls[caption] = table_info
            except Exception as e:
                print(e) 
                continue 
        with open('tables_preproc_large.jsonl', 'a+') as g: 
            for k, v in tbls.items(): 
                g.write(json.dumps(v) + '\n') 

        return tbls




# def load_doc(tbls):  
#     print(tf.executing_eagerly())
#     docs = []
#     # params_path = os.path.join('out', "estimator_params.json")

#     # with tf.gfile.GFile(params_path) as f:
#     #     params = json.load(f)

#     # tokenizer = featurization.Tokenizer(
#     #     vocab_path=params["vocab_path"], do_lower_case=params["do_lower_case"])
#     bert_layer = hub.KerasLayer("https://tfhub.dev/tensorflow/bert_en_uncased_L-12_H-768_A-12/1",
#                                 trainable=False)
#     vocab_file = bert_layer.resolved_object.vocab_file.asset_path.numpy()
#     # to_lower_case = bert_layer.resolved_object.do_lower_case.numpy()
#     tokenizer = tokenization.FullTokenizer(
#     vocab_file=vocab_file, do_lower_case=True)

#     for capt, tbl_info in tbls.items(): 
#         title = capt 
#         body = tbl_info['data'].to_string()
#         doc_uid = tbl_info['id']
#         title_token_ids = tokenizer.tokenize(title)
#         title_token_ids = tokenizer.convert_tokens_to_ids(title_token_ids)
#         body_token_ids = tokenizer.tokenize(body)
#         body_token_ids = tokenizer.convert_tokens_to_ids(body_token_ids)
#         doc = featurization.Document(
#             uid=doc_uid,
#             title_token_ids=title_token_ids,
#             body_token_ids=body_token_ids)
#         docs.append(doc)
    
#     return docs



def main(argv):
    # example_path = '/raid/lingo/marzoev/structured-realm/language/realm-data-small/pretrain_corpus_small/wikipedia_annotated_with_dates_public-00000-of-00020.tfrecord.gz'
    # load_doc(example_path)
    tbls = get_dataframes()
    # convert_dataframes(tbls)
    load_doc(tbls)
        

FLAGS = flags.FLAGS
# flags.DEFINE_boolean('preserve_unused_tokens', True, '')


if __name__ == '__main__':
   app.run(main)
    # main()
