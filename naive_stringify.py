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
                table_info['id'] = contents['tableId']
                tbls[caption] = table_info
            except Exception as e:
                print('SKIPPING') 
                continue 
        return tbls


# def convert_dataframes(tbls): 
#     json_list = []
#     path = 'dataset.tfrecord'
#     with open(path, 'a+') as f: 
#         for capt, info in tbls.items(): 
#             info['title'] = capt 
#             info['body'] = info['data'].to_string()
#             del info['data']
#             jsoned = json.dumps(info)
            # f.write(jsoned)
      

def load_doc(tbls):  
    docs = []
    # params_path = os.path.join('out', "estimator_params.json")

    # with tf.gfile.GFile(params_path) as f:
    #     params = json.load(f)

    # tokenizer = featurization.Tokenizer(
    #     vocab_path=params["vocab_path"], do_lower_case=params["do_lower_case"])
    bert_layer = hub.KerasLayer("https://tfhub.dev/tensorflow/bert_en_uncased_L-12_H-768_A-12/1",
                                trainable=False)
    vocabulary_file = bert_layer.resolved_object.vocab_file.asset_path
    # to_lower_case = bert_layer.resolved_object.do_lower_case.numpy()
    tokenizer = tokenization.FullTokenizer(
    vocab_file=vocab_file, do_lower_case=True)

    for capt, tbl_info in tbls.items(): 
        title = capt 
        body = tbl_info['data'].to_string()
        doc_uid = tbl_info['id']
        title_token_ids = tokenizer.tokenize(title)
        body_token_ids = tokenizer.tokenize(body)
        doc = featurization.Document(
            uid=doc_uid,
            title_token_ids=title_token_ids,
            body_token_ids=body_token_ids)
        docs.append(doc)
    
    import ipdb; ipdb.set_trace()
    return docs


def main():
    # example_path = '/raid/lingo/marzoev/structured-realm/language/realm-data-small/pretrain_corpus_small/wikipedia_annotated_with_dates_public-00000-of-00020.tfrecord.gz'
    # load_doc(example_path)
    tbls = get_dataframes()
    # convert_dataframes(tbls)
    load_doc(tbls)
        

# FLAGS = flags.FLAGS
# flags.DEFINE_boolean('preserve_unused_tokens', True, '')


if __name__ == '__main__':
#   app.run(main)
    main()
