#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:36 2019

@author: ngrasley
"""
import warnings
import copy
import json
from collections import OrderedDict
from ast import literal_eval
import numpy as np
from base import FeatureBase
from comparisons import JW, EuclideanDistance, GeoDistance, BiGram, TriGram, NGram,\
                        BooleanMatch,CommonalityWeight

"""TODO the whole pipeline should be redesigned. First, pandas dataframes should
be scrapped for some numpy array representation. Second, the comparison vector
should be separate from the records info so that record columns don't accidentally
end up in the comparison vector. This is also slower since the two record vectors
have to be wrangled into one dataframe with different names for each record vector.
"""
class FeatureEngineer(FeatureBase):
    """This class generates features from the merged census data. Implemented
       features are listed in self.features_avail. The values in self.features_avail
       are functors in comparisons.py. To add a comparison, use tuples, i.e.
       feature_engineer.add_comparison(("first_name",), ("jaro-winkler",)).
       I do this because a comparison could potentially work on multiple columns
       and/or have a pipeline of pre/post-processing of the comparison (e.g. weights).
       A cleaner way of doing this would be great. Also, most of the preprocessing
       steps should now be done on the record sets themselves.
    """
    def __init__(self):
        self.features_avail = {"jw": JW(),
                               "euclidean dist": EuclideanDistance(),
                               "geo dist": GeoDistance(),
                               "bigram": BiGram(),
                               "trigram": TriGram(),
                               "ngram": NGram(),
                               "exact match": BooleanMatch()}
        self.raw_features = OrderedDict() #FIXME add this for saving/loading
        self.rec_columns = set()
        self.ncompares = 0
        self.pipeline = OrderedDict()

    def add_comparison(self, record_col, compare_type, extra_args=None):
        if extra_args is None:
            extra_args = ({} for i in range(len(compare_type)))
        self.raw_features[record_col] = [compare_type, tuple(extra_args)]
        print(extra_args)
        self.rec_columns.add(record_col)
        self.ncompares += 1
        comp_func = copy.deepcopy(self.features_avail[compare_type])
        comp_func.__init__(**extra_args)
        if len(record_col) == 1:
            comp_func.col = record_col[0]
        else:
            comp_func.col = list(record_col)
        self.pipeline[record_col] = comp_func
        
    def rm_comparison(self, record_col): #FIXME this does not account for a feature col that has multiple comparisons.
        self.pipeline.pop(record_col)
        for i in record_col:
            self.rec_columns.remove(record_col)
        self.ncompares -= 1
        
    def check_pipeline(self, example_rec):
        """Build the pipeline"""
        unused_cols = set(example_rec.dtype.names).difference(self.rec_columns)
        if len(unused_cols) > 0:
            warnings.warn(f"The column(s) {unused_cols} do(es) not have a \
                              comparison function.", RuntimeWarning)
            
        extra_cols = self.rec_columns.difference(set(example_rec.dtype.names))
        if len(extra_cols) > 0:
            raise Exception(f"The column(s) {extra_cols} are not in the \
                                example record. Please remove the offending \
                                columns and run check_pipeline again.")

    def compare(self, rec1, rec2):
        comp_vec = np.ndarray(self.ncompares, dtype=np.float32)
        i = 0
        for col in self.pipeline:
            pipe_params = [rec1, rec2]
            comp_vec[i] = self.pipeline[col].transform(*pipe_params)
            i += 1
        return comp_vec

    def save(self, file_path):
            with open(file_path, 'w') as f:
                json.dump(OrderedDict((str(k),v) for k,v in self.raw_features.items()), f)

    def load(self, file_path):
        self.__init__()
        with open(file_path, 'r') as f:
            self.raw_features = json.load(f, object_pairs_hook=OrderedDict)
        self.raw_features = {literal_eval(k):v for k, v in self.raw_features.items()}
        for key in self.raw_features:
            self.add_comparison(key, tuple(self.raw_features[key][0]), tuple(self.raw_features[key][1]))