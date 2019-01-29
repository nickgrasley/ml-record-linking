#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 16:40:00 2019

@author: thegrasley
"""

from sklearn.pipeline import Pipeline
from preprocessing import load_data

def main(years=["1910", "1920"]):
    census = load_data(year=years[0])
    index = census.shape[0]
    census = pd.concat([census, load_data(year=years[1])], axis=0)
    Pipeline(steps=[""])
    
if __name__ == "__main__":
    main()