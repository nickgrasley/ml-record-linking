# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 17:09:24 2019

@author: ngrasley
"""

import pickle
import pandas as pd
from sklearn.pipeline import Pipeline

Pipeline()

def main(model, census1, census2, ltr):
    """Create predictions for a given trained model"""
    with open(model, "rb") as file:
        model = pickle.load(file)
    #Load data
    census1 = pd.read_stata(census1)
    census2 = pd.read_stata(census2)
    
    #Create bins
    
    #Generate features
    
    #Run predictions