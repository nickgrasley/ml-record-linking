# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:55:12 2019

@author: ngrasley
"""

import pickle as pkl
import os
import json
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from sklearn.base import BaseEstimator, TransformerMixin
from time import time

class XGBoostMatch(BaseEstimator, TransformerMixin):
    """This class either trains a new model given the data or generates predictions
       using a previously trained model. Make sure to use set_hyper_params() if
       you are training a new model.
    """
    def __init__(self, model_file="R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/model.xgboost"):
        self.model = None
        if model_file is not None:
            with open(model_file, "rb") as file:
                self.model = pkl.load(file)
        self.time_taken     = -1
        self.confusion_mat = np.array(0)
        self.test_precision = -1
        self.test_recall    = -1
        self.hyper_params = {}
        
    """Set the hyper_params
       Parameters:
           params (dict): keys are the parameter names for an xgboost model while
               values are either singular values or a list of values. A list of
               values will perform a grid search to choose the optimal hyper parameter.
    """
    def set_hyper_params(self, params):
        self.hyper_params = params
        
    def fit(self, data, Y, test_size=0.20, random_state=94, cv=5, n_jobs=8):
        X_train, X_test, Y_train, Y_test = train_test_split(data, Y, test_size=test_size, random_state=random_state)
        start = time()
        clf = XGBClassifier()
        gs = GridSearchCV(clf, self.hyper_params, cv=cv, n_jobs=n_jobs, scoring="f1_weighted")
        gs.fit(X_train, Y_train)
        end = time()
        self.time_taken = end - start
        Y_pred = gs.predict(X_test)
        self.confusion_mat = confusion_matrix(Y_test, Y_pred)
        self.test_precision = precision_score(Y_test, Y_pred)
        self.test_recall = recall_score(Y_test, Y_pred)
        gs.best_estimator_.get_booster().feature_names = data.columns
        self.model = gs
        return self
    
    def predict(self, data, drop_duplicates=False):
        Y_pred = pd.Series(self.model.predict(data), name=["Y_pred"])
        Y_pred_proba = pd.Series(self.model.predict_proba(data)[:,1], columns=["Y_pred_proba"])
        return pd.concat([Y_pred, Y_pred_proba], axis=1) #FIXME where do I add in arks?
    
    def transform(self, data):
        return self.predict(data)
    
    def save(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)
        with open(f"{path}/model.xgboost", "w") as file:
            pkl.dump(self.model, file)
        with open(f"{path}/model_features.json", "w") as file:
            file.write(f"\{'training_time': {self.time_taken}, 'confusion_mat': {self.confusion_mat},\
                           'precision': {self.test_precision}, 'recall': {self.test_recall},\
                           'hyper_params': {self.hyper_params}\}")
                  
    def load(self, path):
        with open(f"{path}/model.xgboost", "r") as file:
            self.model = pkl.load(file)
        with open(f"{path}/model_features.json", "r") as file:
            json_data = json.load(file)
            params = {'training_time': self.time_taken, 'confusion_mat': self.confusion_mat,
                      'precision': self.test_precision, 'recall': self.test_recall,
                      'hyper_params': self.hyper_params}
            for key in params:
                params[key] = json_data[key]