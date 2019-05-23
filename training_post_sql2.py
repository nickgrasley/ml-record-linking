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
from time import time

def main():
    #Create the object that finds all of the raw data from various files
    try:
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "drop", "commonality weight"] #FIXME add immigration imputer to FeatureEngineer
        first_vec = [f"first_vec{i}" for i in range(2, 202)]
        #last_vec = [f"last_vec{i}" for i in range(2,202)]
        cols_to_drop = ["marstat", "birth_year", "household", "immigration", "race", "rel", "female",
                        "bp", "mbp", "fbp", "state", "county", "cohort1", 
                        "cohort2", "last_sdxn", "first_sdxn", "last_init",
                        "first_init", "bp_comm", "first_comm", "last_comm", "bp_lat", "bp_lon",
                        "res_lat", "res_lon", "full_name"]
        cols_to_drop.extend(first_vec)
        #cols_to_drop.extend(last_vec)
        feature_params = [{"cols": ["first_comm", "last_comm"], "years": ["1900", "1910"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "mbp", "fbp", "first_sdxn", "last_sdxn", "bp", "county"], "years": ["1900", "1910"]},
                          {"variables": [["immigration"], ["birth_year"], ["res_lat", "res_lon"], ["bp_lat", "bp_lon"], [f"first_vec{i}" for i in range(2, 202)], [f"last_vec{i}" for i in range(2,202)]], "new_cols": ["immigration_dist", "birth_year_dist", "geodist", "bp_geodist", "first_vec_dist", "last_vec_dist"], "years": ["1900", "1910"]},
                          {"string_col": ["first", "last"], "dist_metric": "jw", "years": ["1900", "1910"]},
                          {"string_col": ["first", "last"], "n": 2, "years": ["1900", "1910"]},
                          {"cols_to_drop": ["first", "last"], "both_years": True, "years": ["1900", "1910"]},
                          {"cols": ["first_jw", "last_jw", "first_sdxn_match", "last_sdxn_match", "bp_match"], "comm_cols": ["first", "last", "first", "last", "bp"], "years": ["1900", "1910"]}]
    
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        #Create the object that trains the model
        trainer_predictor = XGBoostMatch()
        hyper_params = {"max_depth": [3], "learning_rate": [0.01], "n_estimators": [2500]}
        trainer_predictor.set_hyper_params(hyper_params)
        
        record_linker = Splycer(feature_engineer=feature_engineer, xgboost=trainer_predictor)
        conn = pyodbc.connect("DSN=rec_db")
        with open("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/model_post_sql3.xgboost", "rb") as file:
            model = pkl.load(file)
        percent_completed = 0
        print("Running predictions...")
        start = time()
        for chunk in pd.read_sql("Select * FROM Price.dbo.compares_1900_1910 where index_1900 >= 0 and index_1900 < 100000", conn, chunksize = 100000):
            print(f"{percent_completed}% completed")
            percent_completed += 10
            record_linker.candidate_pairs = chunk
            #record_linker.candidate_pairs = record_linker.candidate_pairs.drop(["true_index_1920"], axis = 1)
            record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1900.isnull(), "first_1900"] = record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1900.isnull(), "full_name_1900"]
            record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1910.isnull(), "first_1910"] = record_linker.candidate_pairs.loc[~record_linker.candidate_pairs.full_name_1910.isnull(), "full_name_1910"]
            record_linker.create_features()
            record_linker.candidate_pairs = record_linker.candidate_pairs.drop([f"{i}_1900" for i in cols_to_drop], axis=1).drop([f"{i}_1910" for i in cols_to_drop], axis=1) #FIXME remove this for later batches
            record_linker.arks = record_linker.candidate_pairs.loc[:, ["index_1900", "index_1910"]]
            record_linker.candidate_pairs = record_linker.candidate_pairs.drop(["index_1900", "index_1910"], axis=1) #Add true_index_1920 back in if training and also generate true matches by comparing 1920 indices
            preds = pd.DataFrame(model.predict(record_linker.candidate_pairs), columns=["is_match"])
            preds = pd.concat([record_linker.arks, preds], axis=1)
            preds.to_csv("R:/JoePriceResearch/record_linking/data/preds/preds_1900_1910.csv", index=None, mode='a')
        end = time()
        print(f"time taken: {end - start} seconds")
        return end - start
    except Exception:
        print(traceback.format_exc())
        try:
            return record_linker
        except:
            return None
    
if __name__ == "__main__":
    time_taken = main()