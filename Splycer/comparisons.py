#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 22:24:00 2019

@author: thegrasley
"""
import numpy as np
import jellyfish
import geopy.distance as gd
import ngram
from base import WeightedCompareBase

class JW(WeightedCompareBase):
    def __init__(self, col="pr_name_gn", comm_weight=None, comm_col=None):
        """Generate any string distance for a column of strings
        Parameters:
            string_col (list): a list of the columns that you want string distance for. This should be the base name without the year.
            dist_metric (string): the name of the string distance metric that you want to use.
                                  Valid metrics are jw, levenshtein.
        """
        super().__init__(col, comm_weight=None, comm_col=None)
        self.dist_type = np.vectorize(jellyfish.jaro_winkler)
        
    def compare(self, rec1, rec2):
        return self.dist_type(rec1[self.col], rec2[self.col])
    
class EuclideanDistance(WeightedCompareBase):
    def __init__(self, col=[f"first_vec{i}" for i in range(200)], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=None, comm_col=None)
    
    def compare(self, rec1, rec2):
        return np.linalg.norm(rec1[self.col].view(np.float32) - rec2[self.col].view(np.float32))
        
        
class GeoDistance(WeightedCompareBase):
    def __init__(self, col=["event_lat", "event_lon"], comm_weight=None, comm_col=None):
        super().__init__(col, comm_weight=None, comm_col=None)
        
    def compare(self, rec1, rec2):
        return gd.distance(rec1[self.col][0], rec2[self.col][0]).km
    
class NGram(WeightedCompareBase):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None, n=3):
        super().__init__(col, comm_weight=None, comm_col=None)
        self.ngram = np.vectorize(ngram.NGram.compare)
        self.n = n
    def compare(self, rec1, rec2):
        return self.ngram(rec1[self.col], rec2[self.col], N=self.n)

class BiGram(NGram):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None):
        super().__init__(col, 2)
        
class TriGram(NGram):
    def __init__(self, col="first_name", comm_weight=None, comm_col=None):
        super().__init__(col, 3)

class BooleanMatch(WeightedCompareBase):
    def __init__(self, col="race", comm_weight=None, comm_col=None):
        super().__init__(col)
    def compare(self, rec1, rec2):
        return (rec1[self.col] == rec2[self.col])[0]