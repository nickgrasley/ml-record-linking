# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 17:09:24 2019

@author: ngrasley
"""

import pickle
import pandas as pd
from sklearn.pipeline import Pipeline
from preprocessing import BooleanMatch, FuzzyBoolean, JW, DropVars


def main():
    """Create predictions for a given trained model"""
    #Load data
    pairs_df = pd.read_stata("R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/prediction_pairs.dta")    
    #Generate features
    boolean_match = BooleanMatch(vars_to_match=["race", "rel", "female", "bp",
                                                "mbp", "fbp", "state", "county",
                                                "last_sdxn", "first_sdxn",
                                                "first_init", "last_init", "marstat",
                                                "birth_year", "immigration"])
    fuzzy_match = FuzzyBoolean(vars_fuzzy=["birth_year"])
    jaro_wink = JW(jw_col_name=["first_jw", "last_jw"], string1_col=["first1910", "last1910"], string2_col=["first1920", "last1920"])
    drop_names = DropVars(["first", "last"], both_years=True)
    final_drop = DropVars(["race", "rel", "female", "bp", "mbp", "fbp", "state",
                           "county", "last_sdxn", "first_sdxn", "first_init",
                           "last_init", "marstat", "birth_year", "immigration",
                           "midinit"], both_years=True)
    pipe = Pipeline([("jw", jaro_wink), ("drop_names", drop_names),
                     ("bool", boolean_match), ("fuzzy_match", fuzzy_match),
                     ("final_drop", final_drop)])
    pipe.fit_transform(pairs_df)
    return pairs_df
    #Run predictions

if __name__ == "__main__":
    pairs_df = main()