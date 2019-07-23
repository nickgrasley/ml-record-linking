#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:22:25 2019

@author: thegrasley
"""

import pickle as pkl
import os
import json
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from time import time
from Splycer.base import LinkerBase

class XGBoostMatch(LinkerBase):
    """This class either trains a new model given the data or generates predictions
       using a previously trained model.
    """
    def __init__(self, recordset1, recordset2, compareset, comp_eng, model):
        super().__init__(recordset1, recordset2, compareset)
        self.comp_eng = comp_eng
        self.model = model

    def create_training_set(self):
        """Used in the train function. Generate comparison vectors."""
        comp_array = np.zeros((self.compareset.ncompares, self.comp_eng.nfeats), dtype=np.float32) #FIXME add shape features
        labels = np.zeros(self.compareset.shape[0], dtype=np.uint8)
        i = 0
        for uid1, uid2, label in self.compareset:
            labels[i] = label
            comp_array[i] = self.comp_eng.compare(self.recordset1.get_record(uid1), 
                                                  self.recordset2.get_record(uid2)) #FIXME implement to_array(float32) in FeatureEngineer
        return comp_array, labels
    def train(self, test_size=0.2, random_state=94):
        """Train the xgboost model. This is agnostic to a grid search, but you
        have to set up the grid search in your own script.
        """
        print("creating training set...")
        X, y = self.create_training_set()
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
        print("fitting model...")
        start = time()
        self.model.fit(X, y)
        end = time()
        y_pred = self.model.predict(X_test)
        self.confusion_mat = confusion_matrix(y_test, y_pred)
        self.test_precision = precision_score(y_test, y_pred)
        self.test_recall = recall_score(y_test, y_pred)
        print(f"number of training obs: {X.shape[0]}, training time: {end - start},\n\
                precision: {self.test_precision}, recall: {self.test_recall}")
        
    def is_link(self, candidate_pair):
        """Use the model's builtin cutoff to say whether a comparison pair is a link. This
           does not account for duplicates since it only predicts one pair at a time.
        """
        rec1 = self.recordset1.get_record(candidate_pair[0])
        rec2 = self.recordset2.get_record(candidate_pair[1])
        comp_vec = self.comp_eng.compare(rec1, rec2)
        return self.model.predict(comp_vec)
    
    def link_proba(self, candidate_pair):
        """Generate probability prediction for a comparison pair."""
        rec1 = self.recordset1.get_record(candidate_pair[0])
        rec2 = self.recordset2.get_record(candidate_pair[1])
        comp_vec = self.comp_eng.compare(rec1, rec2)
        return self.model.predict_proba(comp_vec)[0][0] #FIXME check that this is the correct probability
    
    def run(self, outfile):
        """Run the model on the full compare set, writing results to file."""
        outfile = open(outfile, "w")
        for candidate_pair in self.compareset.get_pairs():
            match_proba = self.link_proba(candidate_pair)
            if self.above_thresh(match_proba):
                outfile.write(f"{candidate_pair[0]},{candidate_pair[1]},{match_proba}")
        #FIXME should I implement remove_duplicates in run itself?
            
    def remove_duplicates(self):
        raise NotImplementedError()
        
    def save(self, path): #FIXME this isn't all that I want to save
        if not os.path.isdir(path):
            os.mkdir(path)
        with open(f"{path}/model.xgboost", "w") as file:
            pkl.dump(self.model, file)
        with open(f"{path}/model_features.json", "w") as file:
            file.write(f"\{'training_time': {self.time_taken}, 'confusion_mat': {self.confusion_mat},\
                           'precision': {self.test_precision}, 'recall': {self.test_recall},\
                           'hyper_params': {self.hyper_params}\}")
                  
    def load(self, path): #FIXME this isn't all that I want to load
        with open(f"{path}/model.xgboost", "r") as file:
            self.model = pkl.load(file)
        with open(f"{path}/model_features.json", "r") as file:
            json_data = json.load(file)
            params = {'training_time': self.time_taken, 'confusion_mat': self.confusion_mat,
                      'precision': self.test_precision, 'recall': self.test_recall,
                      'hyper_params': self.hyper_params}
            for key in params:
                params[key] = json_data[key]
