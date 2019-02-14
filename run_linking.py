# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 16:36:08 2019

@author: ngrasley

This is the master file to run a machine algorithm for linking. You can specify
whether you want to train or whether you want to predict.
"""
import sys
import pickle as pkl
from train_predict import predict, train, feature_eng

def run_linking(is_train, model_pickle_file, candidate_pairs_file, census_out_file, features):
    """Run the linking process from start to finish
    Parameters:
        is_train (bool): whether you want to train or predict
        model_pickle_file (string): if predicting, file path to model
        candidate_pairs_file (string): where candidate pairs are located
        census_out_file (string): file path to where merged census data is stored
        features: Don't implement this yet. Soon we'll let people choose features, but it's not necessary right now
    Output:
        if predicting:
            a table of predictions with pid, arks, ismatch
        if training:
            model with precision and recall of test sets
    """
    if is_train:
        return 0 #FIXME train model and pickle.
    else:
        with open(model_pickle_file, "rb") as file:
            model = pkl.load(file)
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", "merge_datasets.do", candidate_pairs_file, out_file]) #FIXME add file args
        #load data
        #run train_predict.py
        #run create_predictions_file.py

if __name__ == "__main__":
    if len(sys.argv) == 1: #FIXME complete this
        train_or_predict = input("Do you want to train or predict?")
        if is_train:
            raise Exception
        else:
            model_pickle_file = input("Where is the model that you want to use for prediction?")
            candidate_pairs_file = input("Where is the candidate pairs file that you want to use for prediction?")
            run_linking(False, model_pickle_file, candidate_pairs_file)
            
    else:
        run_linking(sys.) #FIXME