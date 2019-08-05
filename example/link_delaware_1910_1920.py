# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 12:36:54 2019

@author: ngrasley
"""
import os
import pandas as pd

from Splycer.record_set import RecordDataFrame
from Splycer.compare_set import CompareMatrix
from Splycer.feature_engineer import FeatureEngineer
from Splycer.xgboost_match import XGBoostMatch

def main():
    """This example links Delaware 1910-1920 censuses."""
    os.chdir("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/example")
    #Load in the record sets
    delaware_1910 = pd.read_csv("delaware_1910.csv", index_col="index_1910")
    delaware_1920 = pd.read_csv("delaware_1920.csv", index_col="index_1920")
    #Construct the record sets
    