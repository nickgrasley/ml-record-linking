#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
"""
Created on Thu Jul 18 11:22:25 2019

@author: thegrasley
"""

import pickle as pkl
import os
import json
from itertools import zip_longest
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from time import time
from base import LinkerBase
from tqdm import tqdm

class XGBoostMatch(LinkerBase):
    """This class either trains a new model given the data or generates predictions
       using a previously trained model.
    """
    def __init__(self, recordset1, recordset2, compareset, comp_eng, model):
        super().__init__(recordset1, recordset2, compareset)
        self.comp_eng = comp_eng
        self.model = model

    def create_training_set(self, maxsize=1000000):
        """Used in the train function. Generate comparison vectors."""
        nrows = self.compareset.ncompares
        if maxsize < self.compareset.ncompares:
            nrows = maxsize
        uids1 = []
        uids2 = []
        labels = []
        for uid1, uid2, label in self.compareset:
            uids1.append(uid1)
            uids2.append(uid2)
            labels.append(label)
        comp_array = self.comp_eng.compare(self.recordset1.get_records(uids1), 
                                           self.recordset2.get_records(uids2))
        return comp_array, np.array(labels)
    
    def train(self, test_size=0.2, random_state=94, maxsize=1000000):
        """Train the xgboost model. This is agnostic to a grid search, but you
        have to set up the grid search in your own script.
        """
        print("creating training set...")
        X, y = self.create_training_set(maxsize)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
        print("fitting model...")
        tic = time()
        self.model.fit(X, y)
        toc = time()
        y_pred = self.model.predict(X_test)
        self.confusion_mat = confusion_matrix(y_test, y_pred)
        self.test_precision = precision_score(y_test, y_pred)
        self.test_recall = recall_score(y_test, y_pred)
        print(f"number of training obs: {X.shape[0]}, training time: {toc - tic},\n\
                precision: {self.test_precision}, recall: {self.test_recall}")
        
    def is_link(self, candidate_pair1, candidate_pair2):
        """Use the model's builtin cutoff to say whether a comparison pair is a link. This
           does not account for duplicates since it only predicts one pair at a time.
        """
        rec1 = self.recordset1.get_record(candidate_pair1)
        rec2 = self.recordset2.get_record(candidate_pair2)
        comp_vec = self.comp_eng.compare(rec1, rec2)
        return self.model.predict(comp_vec)
    
    def link_proba(self, comp_mat):
        """Generate probability prediction for a comparison pair."""
        return self.model.predict_proba(comp_mat) #FIXME check that this is the correct probability
    
    def run(self, outfile, chunksize=100000): #FIXME output progress stats
        """Run the model on the full compare set, writing results to file."""
        comp_mat = np.ndarray((chunksize, self.comp_eng.ncompares), dtype=np.float32)
        with tqdm(total=self.compareset.ncompares) as pbar:
            pbar.set_description("Predicting links...")
            with open(outfile, "wb") as f:
                for cand_mat in self.compareset.get_pairs(chunksize=chunksize):
                    rec1 = self.recordset1.get_records(cand_mat[0])
                    rec2 = self.recordset2.get_records(cand_mat[1])
                    comp_mat = self.comp_eng.compare(rec1, rec2)
                    preds = self.link_proba(comp_mat)
                    np.savetxt(f, np.concatenate((np.array(cand_mat).T, preds), axis=1)[:,[0,1,4]], fmt="%i %i %1.4f") #FIXME make saving more clear what's what.
                    pbar.update(chunksize)
        print("Linking completed")
 
    def rm_duplicates(self):
        raise NotImplementedError()
        
    def save(self, path): #FIXME this isn't all that I want to save
        if not os.path.isdir(path):
            os.mkdir(path)
        with open(f"{path}/model.xgboost", "w") as file:
            pkl.dump(self.model, file)
        with open(f"{path}/model_features.json", "w") as file:
            file.write(f"'training_time': {self.time_taken}, 'confusion_mat': {self.confusion_mat},'precision': {self.test_precision}, 'recall': {self.test_recall}, 'hyper_params': {self.hyper_params}")
                  
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
