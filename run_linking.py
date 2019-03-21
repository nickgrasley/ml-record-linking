# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 16:36:08 2019

@author: ngrasley

This is the master file to run a machine algorithm for linking. You can specify
whether you want to train or whether you want to predict.
"""
import sys
import pickle as pkl
from sklearn.metrics import confusion_matrix
import preprocessing.train_predict, preprocessing.create_predictions_file
from subprocess import call

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
    merge_file = "R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/preprocessing/merge_datasets.do"
    if is_train:
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", "do", merge_file, candidate_pairs_file, census_out_file])
        model, X_test, Y_test, X, total_time = train_predict.train(census_out_file)
        with open(model_pickle_file, "wb") as file:
            pkl.dump(model, file)
        con_matrix = confusion_matrix(Y_test, X_test)

        return  model, con_matrix, total_time
    else:
        call(["C:/Program Files (x86)/Stata15/StataSE-64.exe", "do", merge_file, candidate_pairs_file, census_out_file, "False"])
        #load data
        preds = train_predict.predict(census_out_file, model_pickle_file, ["1910", "1920"]) #FIXME make years a function parameter
        #run train_predict.py
        create_predictions_file.create_prediction_file(preds, "predictions.csv")
        #run create_predictions_file.py
        return None, None, None

if __name__ == "__main__":
    if len(sys.argv) == 1: #FIXME complete this
        train_or_predict = input("Do you want to train or predict?")
        if train_or_predict:
            raise Exception
        else:
            model_pickle_file = input("Where is the model that you want to use for prediction?")
            candidate_pairs_file = input("Where is the candidate pairs file that you want to use for prediction?")
            run_linking(False, model_pickle_file, candidate_pairs_file)
            
    else:
        if sys.argv[1] == "True":
            is_train = True
        elif sys.argv[1] == "False":
            is_train = False
        model, con_matrix, total_time = run_linking(is_train, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
       #FIXME output precision recall time