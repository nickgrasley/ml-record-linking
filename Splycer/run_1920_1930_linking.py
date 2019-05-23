# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 21:24:57 2019

@author: ngrasley
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 15:26:57 2019

@author: ngrasley
"""
import sys
from Splycer import Splycer
from Binner import Binner
from CensusCompiler  import CensusCompiler
from FeatureEngineer import FeatureEngineer
from XGBoostMatch    import XGBoostMatch

import traceback
import dask.dataframe as dd
from dask.distributed import Client

def main(chunk_num):
    #Create the object that finds all of the raw data from various files
    try:
        binner = Binner(years=["1920", "1930"], chunk_num=chunk_num)
        census_compiler = CensusCompiler()
        vars_to_add = ["census", "name strings", "nicknames", "name commonality", "residence geo-coordinates", "birth place commonality"]
        for var in vars_to_add:
            census_compiler.add_variable(var)
        
        #Create the object that handles generating features
        feature_engineer = FeatureEngineer()
        features = ["imputer", "bool match", "euclidean distance", "jaro-winkler", 
                    "ngram", "dmetaphone", "levenshtein", "commonality weight", "drop"] #FIXME add immigration imputer to FeatureEngineer
        feature_params = [{"cols": ["first_name_comm", "last_name_comm", "immigration"]}, 
                          {"vars_to_match": ["marstat", "race", "rel", "female",  "mbp", "fbp", "first_sdxn", "last_sdxn", "bp"]},
                          {"variables": [["immigration"], ["birth_year"], ["lat", "lon"]], "new_cols": ["immigration_dist", "birth_year_dist", "geodist"]},
                          {"string_col": ["first", "last"]},
                          {"string_col": ["first", "last"], "n": 3},
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
                                            'bp_comm'], "both_years": True}]
        for i, j in zip(features, feature_params):
            feature_engineer.add_feature(i, j)
        
        record_linker = Splycer(binner=binner, census_compiler=census_compiler, feature_engineer=feature_engineer)
        #record_linker.get_labels("y")
        #return record_linker
        #record_linker.candidate_pairs = dd.from_pandas(record_linker.candidate_pairs, chunksize=100000)
        record_linker.create_pairs()
        record_linker.add_census_data()
        record_linker.create_features()
        record_linker.get_arks()
        record_linker.get_labels()
        return record_linker
        record_linker.predict()
    except Exception:
        print(traceback.format_exc())
        try:
            return record_linker
        except:
            return None
    
if __name__ == "__main__":
    client = Client(processes=False) # VERY IMPORTANT. ALWAYS RUN THIS BEFORE ANY DASK CODE, OR IT WILL BE SLOW AS MOLASSES.
    record_linker = main(0)