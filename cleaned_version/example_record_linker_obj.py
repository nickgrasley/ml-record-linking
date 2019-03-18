# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 15:26:57 2019

@author: ngrasley
"""

from Splycer         import Splycer
from CensusCompiler  import CensusCompiler
from FeatureEngineer import FeatureEngineer
from XGBoostMatch    import XGBoostMatch

import traceback

def main():
    #Create the object that finds all of the raw data from various files
    try:
        census_compiler = CensusCompiler()
        vars_to_add = ["census", "name strings", "name commonality", "residence geo-coordinates", "birth place commonality", "birth place geo-coordinates"]
        for var in vars_to_add:
            census_compiler.add_variable(var)
        
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "dmetaphone", "levenshtein", "drop", "commonality weight", "drop"] #FIXME add immigration imputer to FeatureEngineer
        feature_params = [{"cols": ["first_name_comm", "last_name_comm"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "female",  "mbp", "fbp", "first_sdxn", "last_sdxn", "bp", "county", "state"]},
                          {"variables": [["immigration"], ["birth_year"], ["event_lat", "event_lon"], ["bplace_lat", "bplace_lon"]], "new_cols": ["immigration_dist", "birth_year_dist", "geodist", "bp_geodist"]},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first", "last"], "n": 2},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first_dmetaphone", "last_dmetaphone"]},
                          {"cols_to_drop": ["first", "last"], "both_years": True},
                          {"cols": ["first_jw", "last_jw", "first_sdxn_match", "first_dmetaphone_levenshtein", "last_sdxn_match", "last_dmetaphone_levenshtein", "bp_match"], "comm_cols": ["first", "last", "first", "first", "last", "last", "bp"]}, #FIXME add event place commonality
                          {"cols_to_drop": ["marstat", "birth_year", "immigration",
                        "race", "rel", "female", "mbp", "fbp", "first_sdxn", "last_sdxn", 
                        "first_init", "last_init", "event_lat", "event_lon", "county", 
                        "state", "bp", "bplace_lat", "bplace_lon", "first_dmetaphone", 
                        "last_dmetaphone", "first_comm", "last_comm", "bp_comm", "type", "fs_fam", "random_gen_type"], "both_years": True}]
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        #Create the object that trains the model
        trainer_predictor = XGBoostMatch()
        hyper_params = {"max_depth": [3], "learning_rate": [0.01], "n_estimators": [2500]}
        trainer_predictor.set_hyper_params(hyper_params)
        
        record_linker = Splycer(census_compiler=census_compiler, feature_engineer=feature_engineer, trainer_predictor=trainer_predictor)
        record_linker.get_labels("y")
        record_linker.add_census_data()
        record_linker.create_features()
        record_linker.get_arks()
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