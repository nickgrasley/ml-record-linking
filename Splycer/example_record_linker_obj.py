# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 15:26:57 2019

@author: ngrasley
"""

from Splycer import Splycer
from CensusCompiler  import CensusCompiler
from FeatureEngineer import FeatureEngineer
from XGBoostMatch    import XGBoostMatch

import traceback
import dask.dataframe as dd
from dask.distributed import Client

def main():
    #Create the object that finds all of the raw data from various files
    try:
        census_compiler = CensusCompiler()
        vars_to_add = ["census", "name strings", "nicknames", "name commonality", "residence geo-coordinates", "birth place commonality", "birth place geo-coordinates"]
        for var in vars_to_add:
            census_compiler.add_variable(var)
        
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "dmetaphone", "levenshtein", "commonality weight", "drop"] #FIXME add immigration imputer to FeatureEngineer
        feature_params = [{"cols": ["first_name_comm", "last_name_comm"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "female",  "mbp", "fbp", "first_sdxn", "last_sdxn", "bp", "county", "state"]},
                          {"variables": [["immigration"], ["birth_year"], ["lat", "lon"], ["bplace_lat", "bplace_lon"], [f"first_vec{i}" for i in range(200)], [f"last_vec{i}" for i in range(200)]], "new_cols": ["immigration_dist", "birth_year_dist", "geodist", "bp_geodist", "first_vec_dist", "last_vec_dist"]},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first", "last"], "n": 2},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first_dmetaphone", "last_dmetaphone"]},
                          {"cols": ["first_jw", "last_jw", "first_sdxn_match", "first_dmetaphone_levenshtein", "last_sdxn_match", "last_dmetaphone_levenshtein", "bp_match"], "comm_cols": ["first", "last", "first", "first", "last", "last", "bp"]}, #FIXME add event place commonality
                          {"cols_to_drop": ['marstat', 'birth_year', 'household',
                                            'immigration', 'race', 'rel', 'female',
                                            'bp', 'mbp', 'fbp', 'state', 'county',
                                            'midinit', 'cohort1', 'cohort2', 'last_sdxn',
                                            'first_sdxn', 'first_init', 'last_init',
                                            'first', 'last', 'first_name_comm',
                                            'last_name_comm', 'event_place', 'lat',
                                            'lon', 'county_string', 'state_string',
                                            'bp_comm', 'bplace_lat', 'bplace_lon'], "both_years": True}]
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        #Create the object that trains the model
        trainer_predictor = XGBoostMatch()
        hyper_params = {"max_depth": [3], "learning_rate": [0.1, 0.01], "n_estimators": [2500, 4000, 5000]}
        trainer_predictor.set_hyper_params(hyper_params)
        
        record_linker = Splycer(census_compiler=census_compiler, feature_engineer=feature_engineer, xgboost=trainer_predictor)
        #record_linker.get_labels("y")
        #return record_linker
        #record_linker.candidate_pairs = dd.from_pandas(record_linker.candidate_pairs, chunksize=100000)
        return record_linker
        record_linker.add_census_data()
        return record_linker
        record_linker.create_features()
        record_linker.get_arks()
        record_linker.get_labels()
        return record_linker
        record_linker.train()
        return record_linker
    except Exception:
        print(traceback.format_exc())
        try:
            return record_linker
        except:
            return None
    
if __name__ == "__main__":
    #client = Client() # VERY IMPORTANT. ALWAYS RUN THIS BEFORE ANY DASK CODE, OR IT WILL BE SLOW AS MOLASSES.
    record_linker = main()