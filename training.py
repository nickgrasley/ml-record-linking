# -*- coding: utf-8 -*-
"""
Created on Fri May  3 10:40:55 2019

@author: ngrasley
"""

import sys
sys.path.append("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/Splycer")
from Splycer.Splycer         import Splycer
from FeatureEngineer import FeatureEngineer
from XGBoostMatch    import XGBoostMatch

import pickle as pkl
import pandas as pd
import pyodbc
import traceback

def main():
    #Create the object that finds all of the raw data from various files
    try:
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "drop", "commonality weight"] #FIXME add immigration imputer to FeatureEngineer
        first_vec = [f"first_vec{i}" for i in range(2, 202)]
        last_vec = [f"last_vec{i}" for i in range(2,202)]
        cols_to_drop = ["marstat", "birth_year", "household", "immigration", "race", "rel", "female",
                        "bp", "mbp", "fbp", "state", "county", "midinit", "cohort1", 
                        "cohort2", "last_sdxn", "first_sdxn", "last_init",
                        "first_init", "bp_comm", "first_comm", "last_comm", "bp_lat", "bp_lon",
                        "res_lat", "res_lon"]
        #cols_to_drop.extend(first_vec)
        #cols_to_drop.extend(last_vec)
        feature_params = [{"cols": ["first_comm", "last_comm"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "mbp", "fbp", "first_sdxn", "last_sdxn", "bp", "county"]},
                          {"variables": [["immigration"], ["birth_year"], ["res_lat", "res_lon"], ["bp_lat", "bp_lon"], first_vec, last_vec], "new_cols": ["immigration_dist", "birth_year_dist", "geodist", "bp_geodist", "first_vec_dist", "last_vec_dist"]},
                          {"string_col": ["first", "last"], "dist_metric": "jw"},
                          {"string_col": ["first", "last"], "n": 2},
                          {"cols_to_drop": ["first", "last"], "both_years": True},
                          {"cols": ["first_jw", "last_jw", "first_sdxn_match", "last_sdxn_match", "bp_match"], "comm_cols": ["first", "last", "first", "last", "bp"]}]
    
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        #Create the object that trains the model
        trainer_predictor = XGBoostMatch()
        hyper_params = {"max_depth": [3], "learning_rate": [0.01], "n_estimators": [2500]}
        trainer_predictor.set_hyper_params(hyper_params)
        
        record_linker = Splycer(feature_engineer=feature_engineer, xgboost=trainer_predictor)
        conn = pyodbc.connect("DSN=rec_db")
        print("Loading data...")
        record_linker.candidate_pairs = pd.read_sql("Select top 500000 * FROM Price.dbo.training_data3", conn)
        print("Fixing nicknames...")
        record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1910.isnull(), "first_1910"] = record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1910.isnull(), "full_name_1910"]
        record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1920.isnull(), "first_1920"] = record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1920.isnull(), "full_name_1920"]
        record_linker.candidate_pairs = record_linker.candidate_pairs.drop(["full_name_1910", "full_name_1920"], axis=1)
        print("Creating features...")
        record_linker.create_features()
        print("Dropping unneeded vars...")
        record_linker.candidate_pairs = record_linker.candidate_pairs.drop([f"{i}_1910" for i in cols_to_drop], axis=1).drop([f"{i}_1920" for i in cols_to_drop], axis=1)
        print("Getting indices...")
        record_linker.arks = record_linker.candidate_pairs.loc[:, ["index_1910", "index_1920"]]
        print("Creating truth labels...")
        record_linker.labels = record_linker.candidate_pairs.index_1920 == record_linker.candidate_pairs.true_index_1920
        print("Dropping indices...")
        record_linker.candidate_pairs = record_linker.candidate_pairs.drop(["index_1910", "index_1920", "true_index_1920"], axis=1) #Add true_index_1920 back in if training and also generate true matches by comparing 1920 indices
        return record_linker
    except Exception:
        print(traceback.format_exc())
        try:
            return record_linker
        except:
            return None
    
if __name__ == "__main__":
    record_linker = main()