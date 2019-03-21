# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 16:48:57 2019

@author: ngrasley
"""
from sklearn.base import BaseEstimator, TransformerMixin


class Pruner(BaseEstimator, TransformerMixin):
    def __init__(self, proba_threshold=0.95, pred_proba_col="pred_proba", match_csv_file):
        self.proba_threshold = proba_threshold
        self.pred_proba_col= pred_proba_col
        self.match_csv_file = match_csv_file
        
    def prune(self, data):
        good_matches = data[data[self.pred_proba_col] >= self.proba_threshold].compute()
        good_matches.to_csv(match_csv_file, mode="a", index=None)
        data = data[data[self.pred_proba_col] < self.proba_threshold]
        return data.compute()