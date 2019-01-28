#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:49:19 2019

@author: thegrasley
"""
import numpy  as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
import jellyfish
import sqlite3

class DataFrameSelector(BaseEstimator, TransformerMixin):
    def __init__(self, attribute_names, is_string=False, df_out=True):
        self.attribute_names = attribute_names
        self.is_string = is_string
        self.df_out = df_out
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if self.df_out:
            return X[self.attribute_names]
        return X[self.attribute_names].values


class Substring(BaseEstimator, TransformerMixin):
    def __init__(self, str_len=1, col="first_name", start=0, col_idx=[0], drop=False):
        self.str_len = str_len
        self.col = col
        self.start = start
        self.col_idx = col_idx
        self.drop = drop #FIXME add this
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.series.Series:
            return X.str[self.start:self.str_len]
        elif type(X) == np.ndarray:
            return np.c_[X, X[:,self.col_idx].astype(f"<U{self.str_len}")] #FIXME allow for diff. starting index
        elif type(X) == pd.core.frame.DataFrame:
            return X[self.col].str[self.start:self.start + self.str_len] #FIXME
        else:
            raise Exception("{} data type input to Substring".format(type(X)))

class SDX(BaseEstimator, TransformerMixin):
    def __init__(self, sdx_col_name="soundex", string_col="name"):
        self.sdx_col_name = sdx_col_name
        self.string_col = string_col
        self.sdx = np.vectorize(jellyfish.soundex())
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            X[self.sdx_col_name] = self.sdx(X[self.sdx_col_name])
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, self.sdx(X[:,self.sdx_col_name])]
        elif type(X) == pd.core.series.Series:
            return self.sdx(X)

class JW(BaseEstimator, TransformerMixin):
    def __init__(self, jw_col_name="jaro_winkler",
                 string1_col="name1", string2_col="name2"):
        self.jw = np.vectorize(jellyfish.jaro_winkler())
        self.jw_col_name = jw_col_name
        self.string1_col = string1_col
        self.string2_col = string2_col
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            X[self.jw_col_name] = self.jw(X[self.string1_col], X[self.string2_col])
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, self.jw(X[:,self.string1_col], X[:,self.string2_col])]

class DropVars(BaseEstimator, TransformerMixin):
    def __init__(self, cols_to_drop):
        self.cols_to_drop = cols_to_drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            return X.drop(self.cols_to_drop, axis=1)
        elif type(X) == np.ndarray:
            return np.delete(X, self.cols_to_drop, 1)

class DummyGender(BaseEstimator, TransformerMixin):
    def __init__(self, sex_col="pr_sex_code", drop=True):
        self.sex_col = sex_col
        self.drop = drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            X.loc[X[self.sex_col] == "Unknown", self.sex_col] = np.nan
            X[self.sex_col] = pd.Series(np.where(X[self.sex_col] == "Female", 1, 0),
                                        X[self.sex_col].index)
            return X
        elif type(X) == np.ndarray:
            sex_dummy = np.zeros(shape = X[:, self.sex_col].shape)
            sex_dummy[X[:, self.sex_col] == "Female"] = 1
            sex_dummy[X[:, self.sex_col] == "Unknown"] = np.nan #FIXME gen unknown gender using name and relationship code
            return np.c_[np.delete(X, self.sex_col, 1), sex_dummy]

class DummyRace(BaseEstimator, TransformerMixin):
    #FIXME condense race to black, white, latino, indian, asian
    def __init__(self, race_col="pr_race_or_color", drop=True):
        self.race_col = race_col
        self.drop = drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            return pd.get_dummies(X, columns=["pr_race_or_color"], drop_first=True)
        elif type(X) == np.ndarray:
            return #FIXME

class BooleanMatch(BaseEstimator, TransformerMixin):
    def __init__(self, vars_to_match, years=["1910", "1920"]):
        self.vars_to_match = vars_to_match
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for var in self.var_to_match:
                X[f"{var}_match"] = X[f"{var}_{self.years[0]}"] == \
                                                  X[f"{var}_{self.years[1]}"]
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, X[:, self.var_to_match[0]] == X[:, self.var_to_match[1]]]

class CrosswalkMerge(BaseEstimator, TransformerMixin): #FIXME
    def __init__(self, crosswalk_file):
        self.crosswalk_file = crosswalk_file
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        return self
    
class Bin(BaseEstimator, TransformerMixin): #FIXME
    def __init__(self, bin_vars, index):
        self.bin_vars = bin_vars
        self.index = index
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        #calculate bins
        #merge censuses
        return self

def load_data(state="Delaware", year="1910", variables=["*"]):
    census_files = "R:/JoePriceResearch/record_linking/data/census.db"
    conn = sqlite3.connect(census_files)
    variables = ", ".join(variables)
    return pd.read_sql_query(f"SELECT {variables} FROM basic{year} LIMIT 1000;", conn) #FIXME remove LIMIT

class LoadData(BaseEstimator, TransformerMixin):
    def __init__(self, state="Delaware", year="1910", variables=["*"]):
        self.census_files = "R:/JoePriceResearch/record_linking/data/census.db"
        self.conn = sqlite3.connect(self.census_files)
        self.variables = variables
        self.year = year
    def fit(self, X=None, y=None):
        return self
    def transform(self, X=None):
        query = f"SELECT {self.variables} FROM basic{self.year} LIMIT 1000;"
        return pd.read_sql_query(query, self.conn) #FIXME remove LIMIT