# -*- coding: utf-8 -*-
"""
Created on Fri May 2 15:26:57 2019

@author: ngrasley
"""
import sys
sys.path.append("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/Splycer")
from Splycer.Splycer         import Splycer
from FeatureEngineer import FeatureEngineer
from XGBoostMatch    import XGBoostMatch

import pandas as pd
import pyodbc
import traceback

def main():
    #Create the object that finds all of the raw data from various files
    try:
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "dmetaphone", "levenshtein", "drop", "commonality weight", "drop"] #FIXME add immigration imputer to FeatureEngineer
        first_vec = [f"first_vec{i}" for i in range(2, 202)]
        last_vec = [f"last_vec{i}" for i in range(2,202)]
        cols_to_drop = ["marstat", "birth_year", "household", "immigration", "race", "rel", "female",
                        "bp", "mbp", "fbp", "state", "county", "midinit", "cohort1", 
                        "cohort2", "last_sdxn", "first_sdxn", "last_init",
                        "first_init", "bp_comm", "first_comm", "last_comm", "bp_lat", "bp_lon",
                        "res_lat", "res_lon", "full_name", "first_dmetaphone", "last_dmetaphone"]
        cols_to_drop.extend(first_vec)
        cols_to_drop.extend(last_vec)
        feature_params = [{"cols": ["first_comm", "last_comm"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "female",  "mbp", "fbp", "first_sdxn", "last_sdxn", "bp", "county", "state"]},
                          {"variables": [["immigration"], ["birth_year"], ["res_lat", "res_lon"], ["bp_lat", "bp_lon"], [f"first_vec{i}" for i in range(2, 202)], [f"last_vec{i}" for i in range(2,202)]], "new_cols": ["immigration_dist", "birth_year_dist", "geodist", "bp_geodist", "first_vec_dist", "last_vec_dist"]},
                          {"string_col": ["first", "last"], "dist_metric": "jw"},
                          {"string_col": ["first", "last"], "n": 2},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first_dmetaphone", "last_dmetaphone"]},
                          {"cols_to_drop": ["first", "last"], "both_years": True},
                          {"cols": ["first_jw", "last_jw", "first_sdxn_match", "first_dmetaphone_levenshtein", "last_sdxn_match", "last_dmetaphone_levenshtein", "bp_match"], "comm_cols": ["first", "last", "first", "first", "last", "last", "bp"]}]
    
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        #Create the object that trains the model
        trainer_predictor = XGBoostMatch()
        hyper_params = {"max_depth": [3], "learning_rate": [0.01], "n_estimators": [2500]}
        trainer_predictor.set_hyper_params(hyper_params)
        
        record_linker = Splycer(feature_engineer=feature_engineer, xgboost=trainer_predictor)
        conn = pyodbc.connect("DSN=rec_db")
        record_linker.candidate_pairs = pd.read_sql("SELECT * FROM Price.dbo.training_data", conn)
        record_linker.create_features()
        record_linker.candidate_pairs = record_linker.candidate_pairs.drop([f"{i}_1910" for i in cols_to_drop], axis=1).drop([f"{i}_1920" for i in cols_to_drop], axis=1)
        record_linker.arks = record_linker.candidate_pairs.loc[:, ["index_1910", "index_1920"]]
        record_linker.labels = (record_linker.candidate_pairs.true_index_1920 == record_linker.candidate_pairs.index_1920)
        record_linker.candidate_pairs = record_linker.candidate_pairs.drop(["index_1910", "index_1920", "true_index_1920"], axis=1)
        record_linker.train()
        return record_linker
    except Exception:
        print(traceback.format_exc())
        try:
            return record_linker
        except:
            return None
    
if __name__ == "__main__":
    record_linker = main()