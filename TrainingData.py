# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 17:42:38 2019

@author: ngrasley
"""
import pandas as pd
class TrainingData():
    '''This class will handle the creation of training data for record linking
       ML models.
    '''
    def __init__(self, state):
        '''Initialize TrainingData with the raw census data
        Inputs:
            state -- The state in the census that you want to consider as a
                     subsample. FIXME allow selecting multiple states
        '''
        self.data_direc = "R:/JoePriceResearch/record_linking/data"
        self.c1910 = pd.read_stata("{}/census_1910/data/state_files/{}".format(data_direc,
                                                                               state.title()))
        self.c1920 = pd.read_stata("{}/census_1920/data/state_files/{}".format(data_direc,
                                                                               state.lower()))
        self.columns = self._common_elements(self.c1910.columns,
                                             self.c1920.columns)


    
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
    def _common_elements(self, list1, list2):
        return list(set(list1).intersection(set(list2)))

class Models():
    def __init__(self):
        '''Class for the various models that we are going to run.
        '''
    def pipeline(self):
        
    def fit(self):
        
    def score(self):
