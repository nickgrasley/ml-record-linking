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
from base import CompareBase

class JW(CompareBase):
    def __init__(self, col="pr_name_gn"):
        """Generate any string distance for a column of strings
        Parameters:
            string_col (list): a list of the columns that you want string distance for. This should be the base name without the year.
            dist_metric (string): the name of the string distance metric that you want to use.
                                  Valid metrics are jw, levenshtein.
        """
        super().__init__(col)
        self.dist_type = np.vectorize(jellyfish.jaro_winkler)
        
    def transform(self, rec1, rec2):
        return [self.dist_type(rec1[self.col], rec2[self.col]), rec1, rec2]
    
class EuclideanDistance(CompareBase):
    def __init__(self, col=[f"first_vec{i}" for i in range(200)]):
        super().__init__(col)
    
    def transform(self, rec1, rec2):
        return [np.linalg.norm(rec1[self.col].view(np.float32) - rec2[self.col].view(np.float32)), rec1, rec2]
        
        
class GeoDistance(CompareBase):
    def __init__(self, col=["event_lat", "event_lon"]):
        super().__init__(col)
        
    def transform(self, rec1, rec2):
        return [gd.distance(rec1[self.col][0], rec2[self.col][0]).km, rec1, rec2]
    
class NGram(CompareBase):
    def __init__(self, col="first_name", n=3):
        super().__init__(col)
        self.ngram = np.vectorize(ngram.NGram.compare)
        self.n = n
    def transform(self, rec1, rec2):
        return [self.ngram(rec1[self.col], rec2[self.col], N=self.n), rec1, rec2]

class BiGram(NGram):
    def __init__(self, col="first_name"):
        super().__init__(col, 2)
        
class TriGram(NGram):
    def __init__(self, col="first_name"):
        super().__init__(col, 3)

class BooleanMatch(CompareBase):
    def __init__(self, col="race"):
        super().__init__(col)
    def transform(self, rec1, rec2):
        return [(rec1[self.col] == rec2[self.col])[0], rec1, rec2]
    
class CommonalityWeight(CompareBase):
    def __init__(self, col="first_name", comm_col=["first_comm"], divide=True):
        super().__init__(col)
        self.comm_col
        self.divide = divide
    def transform(self, comp_val, rec1, rec2):
        if self.divide:
            return [comp_val / ((np.log1p(rec1[self.comm_col]) + np.log1p(rec2[self.comm_col])) / 2)]
        else:
            return [comp_val * ((np.log1p(rec1[self.comm_col]) + np.log1p(rec2[self.comm_col])) / 2)]
        