#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:36 2019

@author: ngrasley
"""
import pandas as pd
from sklearn.pipeline import Pipeline
from Splycer.preprocessing import EuclideanDistance, PhoneticCode,\
                                  StringDistance, Bigram, DropVars,\
                                  BooleanMatch, FuzzyBoolean,\
                                  ColumnImputer, CommonalityWeight
from Splycer.base import FeatureBase

"""TODO the whole pipeline should be redesigned. First, pandas dataframes should
be scrapped for some numpy array representation. Second, the comparison vector
should be separate from the records info so that record columns don't accidentally
end up in the comparison vector. This is also slower since the two record vectors
have to be wrangled into one dataframe with different names for each record vector.
"""
class FeatureEngineer(FeatureBase):
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
        self.raw_feature_attributes = {}
        self.pipeline = None

    def add_feature(self, feature_name, param_dict):
        feat = self.features_avail[feature_name]
        feat.__init__(**param_dict)
        count = 0
        while f"{feature_name}_{count}" in dict(self.features):
            count += 1
        self.raw_feature_attributes[f"{feature_name}_{count}"] = param_dict
        self.features.append((f"{feature_name}_{count}", feat)) #FIXME can this take same feature names, or do I have to check for that?

    def build(self):
        """Build the pipeline"""
        self.pipeline = Pipeline(self.features)

    def compare(self, rec1, rec2):
        rec1.columns = [f"{i}_1" for i in rec1.columns]
        rec2.columns = [f"{i}_2" for i in rec2.columns]
        X = pd.concat([rec1, rec2], axis=1)
        return self.pipeline.transform(X)


    def save(self, path):
        with open(path, "w") as file:
            for feat in self.raw_feature_attributes:
                file.write(feat + "|" + self.raw_feature_attributes[feat])

    def load(self, path):
        with open(path, "r") as file:
            for line in file.readlines():
                self.add_feature(line.split("|")) #FIXME
