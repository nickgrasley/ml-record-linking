#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
"""
Created on Tue Jul 23 22:24:00 2019

@author: thegrasley
"""
from itertools import chain
import math
import numpy as np
cimport numpy as np
import jellyfish
import geopy.distance as gd
from base import WeightedCompareBase

class JW(WeightedCompareBase):
    def __init__(self, col="pr_name_gn", comm_weight=None, comm_col=None):
        """Generate any string distance for a column of strings
        Parameters:
            string_col (list): a list of the columns that you want string distance for. This should be the base name without the year.
            dist_metric (string): the name of the string distance metric that you want to use.
                                  Valid metrics are jw, levenshtein.
        """
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.dist_type = np.vectorize(jellyfish.jaro_winkler)

    def compare(self, rec1, rec2):
        return self.dist_type(rec1[self.col], rec2[self.col])

class AbsDistance(WeightedCompareBase):
    def __init__(self, col="immigration", comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
    def compare(self, rec1, rec2):
        return rec1[self.col] - rec2[self.col]

class EuclideanDistance(WeightedCompareBase):
    def __init__(self, col=[f"first_vec{i}" for i in range(200)], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)

    def compare(self, rec1, rec2):
        return np.linalg.norm(rec1[self.col] - rec2[self.col], axis=1)

class GeoDistance(WeightedCompareBase):
    def __init__(self, col=["event_lat", "event_lon"], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)

    def compare(self, rec1, rec2):
        return gd.distance(rec1[self.col], rec2[self.col]).km

class NGram2(WeightedCompareBase):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None, n=3):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.n = n
        self.create_ngram = np.vectorize(self.create_ngram)
        self.jaccard_similarity = np.vectorize(self.jaccard_similarity)

    def jaccard_similarity(self, x, y):
        intersection_cardinality = len(set.intersection(*[x, y]))
        union_cardinality = len(set.union(*[x, y]))
        return intersection_cardinality / float(union_cardinality)

    def create_ngram(self, sequence, pad_left=False, pad_right=False, pad_symbol=None):
        if pad_left:
            sequence = chain((pad_symbol,) * (self.n-1), sequence)
        if pad_right:
            sequence = chain(sequence, (pad_symbol,) * (self.n-1))
        sequence = list(sequence)
        count = max(0, len(sequence) - self.n + 1)
        return {tuple(sequence[i:i+self.n]) for i in range(count)}

    def compare(self, rec1, rec2):
        return self.jaccard_similarity(self.create_ngram(rec1[self.col]), self.create_ngram(rec2[self.col]))

#for testing purposes
import ngram
class NGram(WeightedCompareBase):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None, n=3):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.ngram = np.vectorize(ngram.NGram.compare)
        self.n = n
    def compare(self, rec1, rec2):
        return self.ngram(rec1[self.col], rec2[self.col], N=self.n)[0]

class BiGram(NGram):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight, comm_col, 2)

class TriGram(NGram):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight, comm_col, 3)

class BooleanMatch(WeightedCompareBase):
    def __init__(self, col="race", comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight, comm_col)
    def compare(self, rec1, rec2):
        return (rec1[self.col] == rec2[self.col])
