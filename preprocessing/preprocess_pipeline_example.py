#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 16:40:00 2019

@author: thegrasley
"""

from sklearn.pipeline import Pipeline
from preprocessing import DropVars, BooleanMatch, JW, FuzzyBoolean
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
import pandas as pd
from time import time

def main(file_name):
    pairs_df = pd.read_stata(file_name)
    #pairs_df.rename({"first_init": "first_init1920", "last_init": "last_init1920"}, axis="columns", inplace=True) #for small_training and training_20k
    #y = pairs_df["ismatch1910"]
    #y.columns = ["ismatch"]
    #pairs_df.drop(["ismatch1910"], axis=1, inplace=True)
    y = pairs_df["ismatch"]
    pairs_df.drop(["ismatch"], axis=1, inplace=True)
    
    initial_drop = DropVars(["household", "cohort1", "cohort2", "ark", "index"], both_years=True)
    boolean_match = BooleanMatch(vars_to_match=["race", "rel", "female", "bp",
                                                "mbp", "fbp", "state", "county",
                                                "last_sdxn", "first_sdxn",
                                                "first_init", "last_init", "marstat",
                                                "birth_year", "immigration"])
    fuzzy_match = FuzzyBoolean(vars_fuzzy=["birth_year"])
    jaro_wink = JW(jw_col_name=["first_jw", "last_jw"], string1_col=["name_gn1910", "name_surn1910"], string2_col=["name_gn1920", "name_surn1920"])
    drop_names = DropVars(["name_gn", "name_surn"], both_years=True)
    final_drop = DropVars(["race", "rel", "female", "bp", "mbp", "fbp", "state",
                           "county", "last_sdxn", "first_sdxn", "first_init",
                           "last_init", "marstat", "birth_year", "immigration",
                           "midinit"], both_years=True)
    clf = XGBClassifier(n_jobs=4, random_stata=94)
    #hyper_params = {"n_estimators": [100, 2500, 5000], "max_depth": [1, 3, 5, 10], "class_weight": ["balanced", None]} #rand forest
    hyper_params = {"max_depth": [1, 3, 5, 10], "learning_rate": [0.1, 0.01, 0.001], "n_estimators": [100, 2500, 5000]} #xgboost
    #hyper_params = {"class_weight": ["balanced", None]}
    gs = GridSearchCV(clf, hyper_params, cv=5, n_jobs=4, scoring="f1_macro")
    pipe = Pipeline([("initial_drop", initial_drop), ("jw", jaro_wink),
                     ("drop_names", drop_names), ("bool", boolean_match),
                     ("fuzzy_match", fuzzy_match), ("final_drop", final_drop)])
    pipe.fit_transform(pairs_df)
    X_train, X_test, Y_train, Y_test = train_test_split(pairs_df.values, y.values, test_size=0.33, random_state=94)
    del pairs_df
    model = gs.fit(X_train, Y_train)
    return model, X_train, X_test, Y_train, Y_test

#xgboost on random_training.dta took 22561 seconds
if __name__ == "__main__":
    start = time()
    #pairs_df = main("R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/random_training.dta")
    model, X_train, X_test, Y_train, Y_test = main("R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/random_training.dta")
    end = time()
    print(f"time in seconds: {end - start}")