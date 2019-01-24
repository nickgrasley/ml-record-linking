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


    
    def process_census_files(self, *args):
        '''Choose columns to keep and generate some features
        '''

        for census in [(self.c1910, '1910'), (self.c1920, '1920')]:
            for variable in [('first_init', 'pr_name_gn'), ('last_init', 'last'), ('first_sdx', 'pr_name_gn')]:
                census[0]['{}{}'].format(variable[0], census[1]) = [census[0]['{}{}'].format(variable[1], census[1]).str.get(0) if census[0]['{}{}'].format(variable[1], census[1]).notnull() else '']
            for variable in ['pr_age', 'pr_imm_year']:
                census[0]['{}{}'].format(variable, census[1]) = [pd.to_numeric(census[0]['{}{}'].format(variable, census[1])) if census[0]['{}{}'].format(variable, census[1]).notnull() else '']
            census[0]['last_sdx{}'].format(census[1]) = [sdx(x) for x in census[0]['last{}'].format(census[1])]

        c1920 = c1920[c1920['pr_age1920']>9]
        keep = [bool(1-(x>1911 and x<1921)) for x in c1920['pr_imm_year1920']]
        c1920 = c1920[keep]
        del keep

        
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
