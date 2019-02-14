# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 16:36:08 2019

@author: ngrasley

This is the master file to run a machine algorithm for linking. You can specify
whether you want to train or whether you want to predict.
"""
import sys
import pickle as pkl

def run_linking(is_train, model_pickle_file, candidate_pairs, features):
    """
    """
    if is_train:
        return 0 #FIXME add this functionality
    else:
        with open(model_pickle_file, "rb") as file:
            model = pkl.load(file)
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", "merge_datasets.do"]) #FIXME add file args
        #load data
        #run preprocess_pipeline_example.py
        #run create_predictions_file.py

if __name__ == "__main__":
    if len(sys.argv) == 1:
        train_or_predict = input("Do you want to train or predict?")
        if is_train:
            raise Exception
        else:
            model_pickle_file = input("Where is the model that you want to use for prediction?")
            candidate_pairs_file = input("Where is the candidate pairs file that you want to use for prediction?")
            run_linking(False, model_pickle_file, candidate_pairs_file)