# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:22:08 2019

@author: ngrasley
"""
import sys
import os
import warnings
import pandas as pd

import pickle as pkl
import json

from FeatureEngineer  import FeatureEngineer
from XGBoostMatch import XGBoostMatch
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
    def __init__(self, blocking_sql_file="", feature_engineer=FeatureEngineer(), xgboost = XGBoostMatch(),
                 candidate_pairs="R:/JoePriceResearch/record_linking/data/census_tree/training_data/training.dta",
                 years=["1910", "1920"], outfile="R:/JoePriceResearch/record_linking/deep_learning/data/predictions/xgboost"):
        self.years = years
        self.candidate_pairs = candidate_pairs
        self.labels = None
        self.arks = None
        self.pipe = []
        self.indices = ["index1910", "index1920"]
        self.outfile = outfile

    """Remove the labels column from the rest of the data and assign to self.labels
       Parameters:
           label_col (string): the name of the label column in the candidate_pairs dataframe
    """
    def get_labels(self, label_col):
        self.labels = self.candidate_pairs[label_col]
        self.candidate_pairs = self.candidate_pairs.drop(label_col, axis=1)
    
    """Run this after feature engineering to get the arks."""
    def get_arks(self, has_compact_indices=True):
        if has_compact_indices:
            ark_cols = [f"ark{self.years[0]}", f"ark{self.years[1]}", f"index{self.years[0]}", f"index{self.years[1]}"]
            self.arks = self.candidate_pairs[ark_cols]
            self.candidate_pairs = self.candidate_pairs.drop(ark_cols, axis=1)
        else:
            ark_cols = [f"ark{self.years[0]}", f"ark{self.years[1]}"]
            self.arks = self.candidate_pairs[ark_cols]
            self.candidate_pairs = self.candidate_pairs.drop(ark_cols, axis=1)
        
    def predict(self):
        preds = self.xgboost.predict(self.candidate_pairs)
        if type(self.arks) == pd.core.frame.DataFrame:
            pd.concat([self.arks, preds], axis=1).to_csv(self.outfile, index=False)
        elif type(self.arks) == dd.core.DataFrame:
            pd.concat([self.arks.compute().reset_index(), preds], axis=1).to_csv(self.outfile, index=False)
        
    def fit(self):
        self.pipe.fit()
    
    def fit_transform(self):
        self.pipe.fit_transform()
        
    def transform(self):
        self.pipe.transform()
        
    """Append either a preprocessing object or a linker object to the pipeline"""
    def add_to_pipeline(self, name, linker_obj):
        self.pipe.append((name, linker_obj)) #FIXME create a way of implementing parallel parts in pipeline
        
    """This creates the actual pipeline (you can't add new elements to a pipeline, so you have to create it in one go)"""
    def set_pipeline(self):
        self.pipe = Pipeline(self.pipe)
        
    def save(self, file_path, folder_name, training_data=False):
        os.mkdir(folder_name)
        path = f"{file_path}/{folder_name}"
        if training_data:
            self.candidate_pairs.to_csv(f"{path}/candidate_pairs.csv", index=False)
            self.labels.to_csv(f"{path}/labels.csv", index=False)
            self.arks.to_csv(f"{path}/arks.csv", index=False)
            
    def load(self, file_path, training_data=False):
        if training_data:
            self.candidate_pairs = pd.read_csv(f"{file_path}/candidate_pairs.csv")
            self.labels = pd.read_csv(f"{file_path}/labels.csv")
            self.arks = pd.read_csv(f"{file_path}/arks.csv")