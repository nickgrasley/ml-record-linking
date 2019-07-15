#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 16:40:00 2019

Author: Nick Grasley
Date Created: 1/28/19
Date Modified: 2/14/19

This file handles the training and prediction of models using xgboost.
"""

from sklearn.pipeline import Pipeline
from preprocessing import DropVars, BooleanMatch, JW, EuclideanDistance, Bigram, PhoneticCode, StringDistance, ColumnImputer, CommonalityWeight
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, GridSearchCV

import pandas as pd
from time import time
import pickle as pkl

#geodist location
# record_linking/data/crosswalks/event_lat_lon_1910.dta
# "                                           "1920.dta

#city matches
#record_linking/data/crosswalks/cities/matched_1910_1920

#name commonality
#record_linking/data/census_1910/data/obj/names_crosswalk.dta

#FIXME include commonality, geodist, larger sample, etc.
#FIXME include birth place centroid geodist, last name features
#FIXME change bplace match to geodist
#FIXME bin on residence city

#FIXME 2/6 Things to still do: bplace geodist, bin on residence city (merge not working)
#FIXME to set feature names, use xgboost_model.get_booster().feature_names

def predict(file_name, model_file, years):
    """Create predictions for a constructed census dataset.
    Parameters:
        file_name (string): The constructed census file from the chosen arks.
        model_file (string): The file path of the model you want to use to predict.
        years (list): The two years that you are comparing.
    Return:
        Dataframe of compact census indices, arks, predicted match, probability.
    """
    X = feature_eng(file_name)
    try:
        X.drop(["ismatch"], axis=1, inplace=True)
    except:
        pass
    index_arks_df = X[[f"ark{years[0]}1900", f"ark{years[1]}1900", f"index{years[0]}", f"index{years[1]}"]]
    X.drop([f"ark{years[0]}1910", f"ark{years[1]}1910", f"index{years[0]}", f"index{years[1]}"], axis=1, inplace=True)
    with open(model_file, "rb") as file:
        model = pkl.load(file)
    if len(X.columns) != len(model.get_booster().feature_names):
        raise Exception("columns of prediction dataset do not match feature names of model")
    y_pred = pd.DataFrame(model.predict(X), columns=["ismatch"])
    y_pred_proba = pd.DataFrame(model.predict_proba(X), columns=["junk", "ismatch_proba"])
    return pd.concat([index_arks_df, y_pred, y_pred_proba.ismatch_proba], axis=1)

def train(file_name, hyper_params, do_grid_search=False):
    """Train a new model for a constructed census dataset.
    Parameters:
        file_name: The constructed census file from the chosen arks.
        hyper_params: A dictionary of the hyper parameters for the xgboost model.
        do_grid_search: If true, it will perform a grid search over hyper parameters.
                        Hyper params that you want to search must be a list of values.
    Return:
        the trained model, test samples, full sample, total time to train.
    """
    start = time()
    X = feature_eng(file_name)
    y = X.ismatch
    X.drop(["ismatch"], axis=1, inplace=True)
    X_train, X_test, Y_train, Y_test = train_test_split(X.values, y.values, test_size=0.20, random_state=94)
    clf = XGBClassifier(max_depth=3, learning_rate=0.01, n_estimators=2500, n_jobs=4, random_state=94) #FIXME have variable parameters
    if do_grid_search:
        gs = GridSearchCV(clf, hyper_params, cv=5, n_jobs=4, scoring="f1_weighted")
        model = gs.fit(X_train, Y_train)
    else:
        model = clf.fit(X_train, Y_train)
    #model.booster.feature_names = X.columns
    end = time()
    total_time = end - start
    return model, X_test, Y_test, X, total_time
    

def feature_eng(file_name):
    """Create the desired features for training. For now, you have to manually
       edit this code to change feature engineering. In the future, we should
       fix this to allow someone to specify the features that they want instead
       of hard coding the features.
    Parameters:
        file_name (string): The location of the constructed census dataset.
    Return:
        Feature engineered dataframe of sample.
    """
    pairs_df = pd.read_stata(file_name)
    #pairs_df.drop(["type1910", "fs_fam1910", "random_gen_type1910"], axis=1, inplace=True)
    '''Impute immigration. If immigration is missing, we set it to zero. Not the best way, but it works.'''
    pairs_df.loc[pairs_df["immigration1900"].isnull(), "immigration1900"] = 0
    pairs_df.loc[pairs_df["immigration1920"].isnull(), "immigration1920"] = 0
    rename_dict = {"first_name_comm1910": "first_comm1910", "first_name_comm1920": "first_comm1920",
                     "last_name_comm1910": "last_comm1910", "last_name_comm1920": "last_comm1920"}
    pairs_df.rename(rename_dict, axis=1, inplace=True)
    #initial_drop = DropVars(["household", "cohort1", "cohort2", "ark", "index"], both_years=True)
    col_imp = ColumnImputer(["first_comm", "last_comm"], years=["1900", "1920"])
    
    '''Create bool if vars match'''
    bool_vars = ["marstat", "race", "rel", "female",  "mbp", "fbp", "first_sdxn", "last_sdxn", "bp"]
    boolean_match = BooleanMatch(vars_to_match=bool_vars, years=["1900", "1920"])
    
    '''Calculate distance'''
    distance_vars = [["immigration"], ["birth_year"], ["event_lat", "event_lon"]]
    new_distance_cols = ["immigration_dist", "birth_year_dist", "geodist"]
    dist = EuclideanDistance(variables=distance_vars, new_cols=new_distance_cols, years=["1900", "1920"])
    
    '''String Functions'''
    jaro_wink = JW(jw_col_name=["first_jw", "last_jw"], 
                   string1_col=["first1900", "last1900"],
                   string2_col=["first1920", "last1920"])
    bigram = Bigram(string_col=["first", "last"], years=["1900", "1920"])
    dmetaphone = PhoneticCode(string_col=["first", "last"], years=["1900", "1920"])
    levenshtein = StringDistance(string_col=["first_dmetaphone", "last_dmetaphone"], years=["1900", "1920"])
    drop_names = DropVars(cols_to_drop=["first", "last"], both_years=True, years=["1900", "1920"])
    
    '''Commonality weighting'''
    columns_to_weight = ["first_jw", "last_jw", "first_sdxn_match", 
                         "first_dmetaphone_levenshtein", "last_sdxn_match", 
                         "last_dmetaphone_levenshtein", "bp_match"]
    weights_cols = ["first", "last", "first", "first", "last", "last", "bp"]
    comm_weight = CommonalityWeight(cols=columns_to_weight, comm_cols=weights_cols, years=["1900", "1920"])
    
    '''Drop vars, run pipeline, and return'''
    vars_to_drop = ["marstat", "birth_year", "immigration", #FIXME reinsert index, ark
                    "race", "rel", "female", "mbp", "fbp", "first_sdxn", "last_sdxn", 
                    "first_init", "last_init", "event_lat", "event_lon", "county", 
                    "state", "bp", "first_dmetaphone", "city_string",
                    "last_dmetaphone", "first_comm", "last_comm"]
    final_drop = DropVars(vars_to_drop, both_years=True, years=["1900", "1920"])
    
    pipe = Pipeline([("jw", jaro_wink), ("col_imp", col_imp),
                     ("bigram", bigram), ("dmetaphone", dmetaphone), 
                     ("levenshtein", levenshtein), ("drop_names", drop_names), 
                     ("bool", boolean_match), ("dist", dist),
                     ("comm_weight", comm_weight), ("final_drop", final_drop)])
    pipe.fit_transform(pairs_df)
    return pairs_df.dropna()

#xgboost on random_training.dta took 22561 seconds to train