#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
"""
Created on Tue Jul 23 22:24:00 2019

@author: thegrasley
"""
import warnings
from itertools import chain
import math
import numpy as np
cimport numpy as np
import jellyfish
from base import WeightedCompareBase

class BadJaroWinklerValues(RuntimeWarning):
    pass
class BadNGramValues(RuntimeWarning):
    pass

class JW(WeightedCompareBase):
    def __init__(self, col="pr_name_gn", comm_weight=None, comm_col=None):
        """Generate any string distance for a column of strings
        Parameters:
            string_col (list): a list of the columns that you want string distance for. This should be the base name without the year.
            dist_metric (string): the name of the string distance metric that you want to use.
                                  Valid metrics are jw, levenshtein.
        """
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.dist_type = np.vectorize(self.jw)

    def jw(self, str1, str2):
        bad_vals = ["", None, np.nan]
        if str1 not in bad_vals and str2 not in bad_vals:
            return jellyfish.jaro_winkler(str1, str2)
        warnings.warn("Encountered missing string value(s) for jw", BadJaroWinklerValues)
        return np.nan
    
    def compare(self, rec1, rec2):
        return self.dist_type(rec1[self.col], rec2[self.col])

class AbsDistance(WeightedCompareBase):
    def __init__(self, col="immigration", comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
    def compare(self, rec1, rec2):
        return abs(rec1[self.col] - rec2[self.col])

class EuclideanDistance(WeightedCompareBase):
    def __init__(self, col=[f"first_vec{i}" for i in range(200)], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)

    def compare(self, rec1, rec2):
        return np.linalg.norm(rec1[self.col] - rec2[self.col], axis=1)

class GeoDistance(WeightedCompareBase):
    def __init__(self, col=["event_lat", "event_lon"], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)

    def haversine(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
        dlat = lat2 - lat1
        dlon = lon2 - lon1
    
        a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        km = 6371 * c
        return km
    
    def compare(self, rec1, rec2):
        return self.haversine(rec1[self.col[0]], rec1[self.col[1]], rec2[self.col[0]], rec2[self.col[1]])


class NGram(WeightedCompareBase):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None, n=3):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.n = n
        self.jaccard_similarity = np.vectorize(self.jaccard_similarity)
        self.bad_vals = ["", None, np.nan]

    def jaccard_similarity(self, x, y):
        if x not in self.bad_vals and y not in self.bad_vals:
            if x == y:
                return 1.
            x = self.create_ngram(x)
            y = self.create_ngram(y)
            intersection_cardinality = len(x.intersection(y))
            union_cardinality = len(x.union(y))
            if union_cardinality != 0:
                return intersection_cardinality / float(union_cardinality)
        warnings.warn("Encountered missing string value(s) for ngram", BadNGramValues)
        return np.nan

    def create_ngram(self, sequence): #, pad_left=False, pad_right=False, pad_symbol=None):
        """
        if pad_left:
            sequence = chain((pad_symbol,) * (self.n-1), sequence)
        if pad_right:
            sequence = chain(sequence, (pad_symbol,) * (self.n-1))
        """
        sequence = list(sequence.lower())
        count = max(0, len(sequence) - self.n + 1)
        return {tuple(sequence[i:i+self.n]) for i in range(count)}

    def compare(self, rec1, rec2):
        return self.jaccard_similarity(rec1[self.col], rec2[self.col])

#for testing purposes. This does not give a jaccard similarity score, and I don't know what it's doing.
"""
import ngram
class NGram(WeightedCompareBase):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None, n=3):
        super().__init__(col, comm_weight=comm_weight, comm_col=comm_col)
        self.ngram = np.vectorize(ngram.NGram.compare)
        self.n = n
    def compare(self, rec1, rec2):
        return self.ngram(rec1[self.col], rec2[self.col], N=self.n)
"""

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
        return (rec1[self.col] == rec2[self.col]).mask(rec1[self.col].isna() | rec2[self.col].isna(), np.nan)
        
