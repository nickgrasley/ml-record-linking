# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:22:08 2019

@author: ngrasley
"""
import pandas as pd
import dask.dataframe as dd
import sys
sys.path.append("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/cleaned_version")
import pickle as pkl
from preprocessing.stata_dask import dask_read_stata_delayed_group
from Binner import Binner
from CensusCompiler   import CensusCompiler
from FeatureEngineer  import FeatureEngineer
from TrainerPredictor import TrainerPredictor
from sklearn.pipeline import Pipeline

class Splycer():
    """This class handles the entire record linking process from start to finish.
       The intended use of this class is to write a separate file that creates
       Binner, CensusCompiler, FeatureEngineer, and TrainerPredictor specific to
       your model. The functions of RecordLinker is a wrapper for each of the
       objects. After finishing training, save (using the module pickle) the 
       RecordLinker object somewhere for later predictions. This has the advantage
       that you won't have to keep a script of how you created each class.
       Additional features can be added to each respective class according to
       the structure of the class.
       Parameters:
           binner: A Binner object
           census_compiler: A CensusCompiler object
           feature_engineer: A FeatureEngineer object
           trainer_predictor: A TrainerPredictor object
           candidate_pairs: If candidate pairs already exist, this is the file
                            path to them. It is later overridden as the actual
                            candidate pairs data.
           labels: The column that indicates whether something is a match.
                   Created using get_labels() and not necessary for prediction
           model: The model used to train. I need to create a function that loads in the model
    """
    def __init__(self, binner=Binner(), census_compiler=CensusCompiler(),
                 feature_engineer=FeatureEngineer(), trainer_predictor=TrainerPredictor(),
                 candidate_pairs="R:/JoePriceResearch/record_linking/data/census_tree/training_data/training.dta",
                 years=["1910", "1920"]):
        '''The 4 classes'''
        self.binner = binner
        self.census_compiler = census_compiler
        self.feature_engineer = feature_engineer
        self.trainer_predictor = trainer_predictor
        
        self.years = years
        self.candidate_pairs = candidate_pairs
        self.model = None
        self.labels = None
        self.arks = None
        self.pipe = None
        if type(candidate_pairs) == str:
            self.candidate_pairs = dask_read_stata_delayed_group([candidate_pairs])

    """Remove the labels column from the rest of the data and assign to self.labels
       Parameters:
           label_col (string): the name of the label column in the candidate_pairs dataframe
    """
    def get_labels(self, label_col):
        self.labels = self.candidate_pairs[label_col]
        self.candidate_pairs = self.candidate_pairs.drop(label_col, axis=1).compute()
    
    """Run this after feature engineering to get the arks."""
    def get_arks(self, has_compact_indices=True):
        if has_compact_indices:
            ark_cols = [f"ark{self.years[0]}", f"ark{self.years[1]}", f"index{self.years[0]}", f"index{self.years[1]}"]
            self.arks = self.candidate_pairs[ark_cols]
            self.candidate_pairs.drop(ark_cols, axis=1).compute()
        else:
            ark_cols = [f"ark{self.years[0]}", f"ark{self.years[1]}"]
            self.arks = self.candidate_pairs[ark_cols]
            self.candidate_pairs.drop(ark_cols, axis=1).compute()
   
    """Load in the pickled model from the given directory
       Parameters:
           model_file (string): the path to the model that you want to load
    """
    def get_model(self, model_file):
        with open(model_file, "rb") as file:
            self.model = pkl.load(file)

    """Create candidate pairs using the bins of the binner object."""
    def create_pairs(self):
        self.candidate_pairs = self.binner.makePairs()
    
    """Merge all of the data from census and other sources onto the candidate pairs"""
    def add_census_data(self):
        self.candidate_pairs = self.census_compiler.compile_census(self.candidate_pairs)
        
    """From the census data, generate the desired features listed in the feature engineer object"""
    def create_features(self):
        self.candidate_pairs = self.feature_engineer.transform(self.candidate_pairs)
        
    def train(self):
        self.model = self.trainer_predictor.train(self.candidate_pairs, self.labels)
        
    def predict(self):
        self.trainer_predictor.predict(self.candidate_pairs)