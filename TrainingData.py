# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 17:42:38 2019

@author: ngrasley
"""

class TrainingData():
    def __init__(self, state):
        self.c1910 = pd.read_stata(r'R:/JoePriceResearch/record_linking/)
    
    def process_census_files(self):
        '''Choose columns to keep and generate some features
        '''
        
    def create_bins(self, *args):
        '''Choose what you want to bin on.
        '''
        
    def create_true_false_matches(self):
        '''Generate the true and false matches in the training data.
        '''
        
    def train_test_split(self):
        '''Split the data
        '''
        
    def soundex(self):
        '''Create soundex
        '''
        
    def jw(self):
        '''Create jw scores.
        '''
    
    def match(self):
        '''Generate boolean for whether censuses match.
        '''

class Models():
    def __init__(self):
        '''Class for the various models that we are going to run.
        '''
    def pipeline(self):
        
    def fit(self):
        
    def score(self):