#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 16:40:00 2019

@author: thegrasley
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
    X = feature_eng(file_name) #FIXME
    try:
        X.drop(["ismatch"], axis=1, inplace=True)
    except:
        pass
    index_arks_df = X[[f"ark{years[0]}", f"ark{years[1]}", f"index{years[0]}", f"index{years[1]}"]]
    X.drop([f"ark{years[0]}", f"ark{years[1]}", f"index{years[0]}", f"index{years[1]}"], axis=1, inplace=True)
    with open(model_file, "rb") as file:
        model = pkl.load(file)
    if X.columns != model.get_booster().feature_names:
        raise Exception("columns of prediction dataset do not match feature names of model")
    y_pred = pd.DataFrame(model.predict(X.values), columns=["ismatch"])
    return pd.concat([index_arks_df, y_pred], axis=1)

def train(file_name, hyper_params, do_grid_search=False):
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
    model.booster.feature_names = X.columns
    end = time()
    total_time = end - start
    return model, X_test, Y_test, X, total_time
    

def feature_eng(file_name):
    pairs_df = pd.read_stata(file_name)
    pairs_df.loc[pairs_df["immigration1910"].isnull(), "immigration1910"] = 0
    pairs_df.loc[pairs_df["immigration1920"].isnull(), "immigration1920"] = 0
    pairs_df.rename({"first_name_comm1910": "first_comm1910", "first_name_comm1920": "first_comm1920",
                     "last_name_comm1910": "last_comm1910", "last_name_comm1920": "last_comm1920"}, axis=1, inplace=True)
    #initial_drop = DropVars(["household", "cohort1", "cohort2", "ark", "index"], both_years=True)
    col_imp = ColumnImputer(["first_comm", "last_comm"])
    
    boolean_match = BooleanMatch(vars_to_match=["marstat", "race", "rel", "female", 
                                                "mbp", "fbp", "first_sdxn", "last_sdxn", "bp"])
    
    dist = EuclideanDistance([["immigration"], ["birth_year"], ["event_lat", "event_lon"]],
                             ["immigration_dist", "birth_year_dist", "geodist"])
    
    jaro_wink = JW(jw_col_name=["first_jw", "last_jw"], 
                   string1_col=["first1910", "last1910"],
                   string2_col=["first1920", "last1920"])
    
    bigram = Bigram(string_col=["first", "last"])
    dmetaphone = PhoneticCode(string_col=["first", "last"])
    levenshtein = StringDistance(string_col=["first_dmetaphone", "last_dmetaphone"])
    drop_names = DropVars(["first", "last"], both_years=True)
    
    comm_weight = CommonalityWeight(["first_jw", "last_jw", "first_sdxn_match",
                                     "first_dmetaphone_levenshtein", "last_sdxn_match",
                                     "last_dmetaphone_levenshtein", "bp_match"],
                                    ["first", "last", "first", "first", "last",
                                     "last", "bp"])
    
    final_drop = DropVars(["marstat", "birth_year", "immigration",              #FIXME add index back into drop vars.
                           "race", "rel", "female", "mbp", "fbp", "first_sdxn",
                           "last_sdxn", "first_init", "last_init", "event_lat",
                           "event_lon", "county", "state", "bp",
                           "first_dmetaphone", "last_dmetaphone", "first_comm", "last_comm"], both_years=True)
    pipe = Pipeline([("jw", jaro_wink), ("col_imp", col_imp),
                     ("bigram", bigram), ("dmetaphone", dmetaphone), 
                     ("levenshtein", levenshtein), ("drop_names", drop_names), 
                     ("bool", boolean_match), ("dist", dist),
                     ("comm_weight", comm_weight), ("final_drop", final_drop)])
    pipe.fit_transform(pairs_df)
    return pairs_df.dropna()

#xgboost on random_training.dta took 22561 seconds