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


def load_documents(path):
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
    ex = tf.train.Example.FromString(val)
    print(ex)
    title = get_bytes_feature(ex, 'title')[0]
    body = get_bytes_feature(ex, 'body')[0]

    doc_uid = featurization.get_document_uid(title, body)
    title_token_ids = get_ints_feature(ex, 'title_token_ids')
    body_token_ids = get_ints_feature(ex, 'body_token_ids')
    print('IDS: {}'.format(title_token_ids))
    doc = featurization.Document(
        uid=doc_uid,
        title_token_ids=title_token_ids,
        body_token_ids=body_token_ids)
    docs.append(doc)


def main(): 
    load_documents('language/realm/tables_preproc/None-00000-of-00001')



if __name__ == '__main__': 
    main()