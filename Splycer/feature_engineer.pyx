#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
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
                        BooleanMatch, AbsDistance

class FeatureEngineer(FeatureBase):
    """This class generates an array of similarity/distance scores by building
       a pipeline of comparisons. Currently, it crucially assumes that the two
       record sets that you are comparing have similarly named columns.
    """
    def __init__(self):
        self.compares_avail = {"jw": JW(),
                               "abs dist": AbsDistance(),
                               "euclidean dist": EuclideanDistance(),
                               "geo dist": GeoDistance(),
                               "bigram": BiGram(),
                               "trigram": TriGram(),
                               "ngram": NGram(),
                               "exact match": BooleanMatch()}
        self.raw_compares = []
        self.rec_columns = set()
        self.ncompares = 0
        self.pipeline = []

    def show_available_compares(self):
        """Output the currently implemented comparisons to the console."""
        print(self.compares_avail)
    
    def add_comparison(self, record_col, compare_type, extra_args=None):
        """Add a similarity/distance score to the feature engineer.
           use show_available_compares() to show which compares are implemented.
           Parameters:
               record_col (string or tuple): the column(s) that you wish to compare.
               compare_type (string): the comparison you want to do on the columns.
                                      the name corresponds to the key in self.compares_avail.
               extra_args (dict): optional. Pass additional arguments to a comparison,
                                  such as commonality weights.
           Returns:
               Nothing. Updates pipeline with comparison.
        """
        if extra_args is None:
            extra_args = {}
        self.raw_compares.append([record_col, compare_type, extra_args])
        if type(record_col) == list:
            for i in record_col:
                self.rec_columns.add(i)
        elif type(record_col) == str:
            self.rec_columns.add(record_col)
        else:
            raise TypeError("record column is neither a string nor a list")
        self.ncompares += 1
        try:
            comp_func = copy.deepcopy(self.compares_avail[compare_type])
        except KeyError:
            print(f"{compare_type} is not a currently implemented comparison")
        comp_func.__init__(**extra_args)
        if type(record_col) == str or (type(record_col) == list and len(record_col) == 1):
            comp_func.col = record_col
        else:
            comp_func.col = list(record_col)
        self.pipeline.append(comp_func)
        
    def rm_comparison(self, record_col): #FIXME this does not account for a feature col that has multiple comparisons.
        warnings.warn("rm_comparison is still in alpha. It may have unintended consequences.")
        self.pipeline.pop(record_col)
        for i in record_col:
            self.rec_columns.remove(record_col)
        self.ncompares -= 1
        
    def check_pipeline(self, example_rec):
        """check_pipeline currently checks for two things:
           1. It warns the user if some columns from the example record array
              are not being used. Some columns will obviously not want a comparison
              (e.g. commonality weights), but this will catch any major columns
              that the user will likely want to compare.
           2. It raises an exception if there is a comparison done on a column
              that is not in the example record array. You cannot run the pipeline
              with this type of error.
           check_pipeline should be run on both records sets to ensure that both
           have the correct column names.
        """
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
        """Run the comparison pipeline on the two given records.
           Returns:
               A float32 numpy array of the pipeline's comparison scores.
        """
        comp_vec = np.ndarray((rec1.shape[0],self.ncompares), dtype=np.float32)
        i = 0
        for compare in self.pipeline:
            comp_vec[:,i] = compare.transform(rec1, rec2)
            i += 1
        return comp_vec

    def save(self, file_path):
        """Save the feature engineer at the specified file path."""
        with open(file_path, 'w') as f:
            json.dump(OrderedDict((str(k),v) for k,v in self.raw_compares.items()), f)

    def load(self, file_path):
        """Load the feature engineer from the specified file path."""
        self.__init__()
        with open(file_path, 'r') as f:
            self.raw_compares = json.load(f, object_pairs_hook=OrderedDict)
        self.raw_compares = {literal_eval(k):v for k, v in self.raw_compares.items()}
        for key in self.raw_compares:
            self.add_comparison(key, tuple(self.raw_compares[key][0]), tuple(self.raw_compares[key][1]))
