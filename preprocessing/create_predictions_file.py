# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 15:43:01 2019

@author: ngrasley
"""
from subprocess import call


def create_prediction_file(predictions_df, out_file, has_arks=True, ark_pairs_file=None, years=["1910", "1920"]):
    """Create a csv file of pid, ark1, ark2, is_match using generated predictions.
       Calls a Stata file which does all of the merging.
    Parameters:
        predictions_df (dataframe): The dataframe of predictions. Must at least have indices in R:/JoePriceResearch/record_linking/data/census_compact/census{year}.
        out_file (string): The name of the final dta file of predictions.
        has_arks (bool): True if predictions_df already has arks
        ark_pairs_file (string): The file where the arks are located if arks are missing from predictions_df
        years (list): The years of the linked censuses.
    """
    pred_file = "R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/data/temp.csv"
    predictions_df.to_csv(pred_file, index=None)
    if has_arks:
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", 
              pred_file, years[0], years[1], out_file, 1, ark_pairs_file])
    else:
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", 
              pred_file, years[0], years[1], out_file, 0, ark_pairs_file])