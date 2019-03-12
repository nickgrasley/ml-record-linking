# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:55:12 2019

@author: ngrasley
"""
import pickle as pkl
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from time import time

class TrainerPredictor():
    """This class either trains a new model given the data or generates predictions
       using a previously trained model. Make sure to use set_hyper_params() if
       you are training a new model.
    """
    def __init__(self, model_file="R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/models/xgboost_more_features.p"):
        self.model_file = model_file
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
        
    def train(self, data, Y): #FIXME I'm not handling labels correctly
        X_train, X_test, Y_train, Y_test = train_test_split(data, Y, test_size=0.20, random_state=94)
        start = time()
        clf = XGBClassifier()
        gs = GridSearchCV(clf, self.hyper_params, cv=5, n_jobs=4, scoring="f1_weighted")
        model = gs.fit(X_train, Y_train)
        end = time()
        self.time_taken = end - start
        Y_pred = model.predict(X_test)
        self.confusion_mat = confusion_matrix(Y_test, Y_pred)
        self.test_precision = precision_score(Y_test, Y_pred)
        self.test_recall = recall_score(Y_test, Y_pred)
        return model
        
    def predict(self, data):
        with open(self.model_file, "rb") as file:
            model = pkl.load(file)
        Y_pred = pd.DataFrame(model.predict(data), columns=["Y_pred"])
        Y_pred_proba = pd.DataFrame(model.predict_proba(data)[:,1], columns=["Y_pred_proba"])
        return pd.concat([Y_pred, Y_pred_proba], axis=1) #FIXME where do I add in arks?