# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:36 2019

@author: ngrasley
"""
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
import sys
sys.path.append("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/preprocessing")
from Splycer.preprocessing import EuclideanDistance, PhoneticCode, StringDistance, Bigram, DropVars, BooleanMatch, FuzzyBoolean, ColumnImputer, CommonalityWeight

class FeatureEngineer(BaseEstimator, TransformerMixin):
    """This class generates features from the merged census data. Implemented
       features are listed in self.features_avail. The values in self.features_avail
       are functors in preprocessing.py. Look there for the needed parameters
       for each. Use add_feature() to add all the features that you need, and
       then use transfrom() to generate the features that were appended to self.features.
       transform() will call the functors in self.features sequentially, which
       is important if some of your features depend on the creation of other features
       first.
    """
    def __init__(self):
        self.features_avail = {"distance": EuclideanDistance(variables=[], new_cols=[]),
                               "SDX": PhoneticCode(encoding="SDX"),
                               "NYSIIS": PhoneticCode(encoding="NYSIIS"),
                               "dmetaphone": PhoneticCode(),
                               "jaro-winkler": StringDistance(dist_metric="jw"),
                               "levenshtein": StringDistance(),
                               "ngram": Bigram(),
                               "drop": DropVars(cols_to_drop=[]),
                               "bool match": BooleanMatch(vars_to_match=[]),
                               "fuzzy bool match": FuzzyBoolean(vars_fuzzy=[]),
                               "euclidean distance": EuclideanDistance(variables=[], new_cols=[]),
                               "imputer": ColumnImputer(cols=[]),
                               "commonality weight": CommonalityWeight(cols=[], comm_cols=[])} #FIXME add the other preprocessing functors
        self.features = []
        
    def add_feature(self, feature_name, param_dict):
        feat = self.features_avail[feature_name].__init__(**param_dict)
        self.features.append((feature_name, feat)) #FIXME can this take same feature names, or do I have to check for that?
     
    def fit(self, X, y=None):
        return self
    
    def transform(self, data):
        pipe = Pipeline(self.features)
        pipe.fit_transform(data)
        return data