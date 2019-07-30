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
cimport numpy as np
np.import_array()
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from time import time

cdef class XGBoostMatch():
    """This class either trains a new model given the data or generates predictions
       using a previously trained model.
    """
    cdef double proba_threshold

    def __init__(self, recordset1, recordset2, compareset, comp_eng, model, proba_threshold=0.5):
        self.recordset1 = recordset1
        self.recordset2 = recordset2
        self.compareset = compareset
        self.comp_eng = comp_eng
        self.model = model
        self.proba_thresh = proba_threshold

    def create_training_set(self, maxsize=1000000):
        """Used in the train function. Generate comparison vectors."""
        nrows = self.compareset.ncompares
        if maxsize < self.compareset.ncompares:
            nrows = maxsize
        comp_array = np.zeros((nrows, self.comp_eng.ncompares), dtype=np.float32)
        labels = np.zeros(nrows, dtype=np.uint8)
        i = 0
        for uid1, uid2, label in self.compareset:
            labels[i] = label
            comp_array[i] = self.comp_eng.compare(self.recordset1.get_record(uid1), 
                                                  self.recordset2.get_record(uid2))
        return (comp_array, labels)

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
        return self.model.predict_proba(comp_mat)[0] #FIXME check that this is the correct probability
 
    cpdef void run(self, str outfile, int chunksize=100000):
        """Run the model on the full compare set, writing results to file."""
        cdef (int, int) candidate_pair
        cdef np.ndarray rec1
        cdef np.ndarray rec2

        for candidate_pair in self.compareset.get_pairs():
            pairs_array = np.ndarray((chunksize, 2), dtype=np.uint32)
            comp_mat = np.ndarray((chunksize, self.comp_eng.ncompares), dtype=np.float32) 
            for i in range(chunksize):
                rec1 = self.recordset1.get_record(candidate_pair[0])
                rec2 = self.recordset2.get_record(candidate_pair[1])
                comp_mat[i] = self.comp_eng.compare(rec1, rec2)
            match_proba = self.link_proba(comp_mat)
            np.concatenate((pairs_array, match_proba), axis=1).savetxt(outfile)

    def rm_duplicates(self):
        raise NotImplementedError()
        
    def save(self):
        pass
    def load(self):
        pass
