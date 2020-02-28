#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True

import pickle as pkl
import os
import json
from itertools import zip_longest
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from time import time
from splycer.base import LinkerBase
from tqdm import tqdm

class XGBoostMatch(LinkerBase):
    """This class either trains a new model given the data or generates predictions
       using a previously trained model.
    """
    def __init__(self, recordset1, recordset2, compareset, comp_eng, model=None):
        super().__init__(recordset1, recordset2, compareset)
        self.comp_eng = comp_eng
        self.model = model
        self.confusion_mat,self.test_precision,self.test_recall,self.train_time=[],0,0,0

    def create_training_set(self, maxsize=1000000):
        """Used in the train function. Generate comparison vectors."""
        nrows = self.compareset.ncompares
        if maxsize < self.compareset.ncompares:
            nrows = maxsize
        uids1 = []
        uids2 = []
        labels = []
        i = 1
        for uid1, uid2, label in self.compareset:
            uids1.append(uid1)
            uids2.append(uid2)
            labels.append(label)
            if i > maxsize:
                break
            i += 1
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
        self.train_time = toc-tic
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
    
    def run(self, outfile,chunksize=100000,logfile='log.txt'):
        """Run the model on the full compare set, writing results to file."""
        comp_mat = np.ndarray((chunksize, self.comp_eng.ncompares), dtype=np.float32)
        with tqdm(total=self.compareset.ncompares) as pbar:
            pbar.set_description("Predicting links...")
            with open(outfile, "wb") as f:
                for cand_mat in self.compareset.get_pairs(chunksize=chunksize):
                    cand_mat = list(cand_mat)
                    rec1 = self.recordset1.get_records(cand_mat[0])
                    mask1 = pd.Series(cand_mat[0]).isin(rec1[self.recordset1.idx_name])
                    cand_mat[1]=cand_mat[1][mask1]
                    rec2 = self.recordset2.get_records(cand_mat[1])
                    mask2 = pd.Series(cand_mat[1]).isin(rec2[self.recordset2.idx_name])
                    rec1 = rec1[mask2].reset_index(drop=True)
                    rec2 = rec2.reset_index(drop=True)
                    
                    try:
                        comp_mat = self.comp_eng.compare(rec1, rec2)
                    except:
                        with open(logfile,"a") as l:
                            l.write(f'failed to compile data for indices {cand_mat[0][0]} to {cand_mat[0][-1]}\n')
                        continue
                    preds = self.link_proba(comp_mat)
					#import pdb; pdb.set_trace()
					#print("Concatenated matrix " + np.concatenate((np.array(cand_mat).T, preds), axis=1))
					#print("Matrix to print " + np.concatenate((np.array(cand_mat).T, preds), axis=1)[:,[0,1,3]])
                    cand_mat = np.vstack([rec1[self.recordset1.idx_name].to_numpy(),rec2[self.recordset1.idx_name].to_numpy()]).T
                    np.savetxt(f, np.concatenate((np.array(cand_mat), preds), axis=1)[:,[0,1,3]], fmt="%i %i %1.4f") #FIXME make saving more clear what's what.
                    pbar.update(chunksize)
        print("Linking completed")
 
    def rm_duplicates(self, preds_file):
        data = pd.read_csv(preds_file, header=None, names=["index1", "index2", "prob_match"])
        #separate duplicates from non-duplicates
        no_dup = data.drop_duplicates(subset=["index1", "index2"], keep=False)
        dup = data.loc[data.duplicated(subset=["index1", "index2"], keep=False), :]
        #apply rule to duplicates
        nlargest = dup.groupby("index1").prob_match.nlargest(n=2)
        max_prob = nlargest.groupby(level=0).max().rename("max_prob")
        second_max_prob = nlargest.groupby(level=0).min().rename("second_max_prob")
        dup = dup.merge(max_prob, how="left", left_on="index1", right_index=True).merge(second_max_prob, how="left", left_on="index1", right_index=True)
        dup = ( (dup.max_prob > 0.96) & 
                (abs(dup.max_prob - dup.second_max_prob) > .05) & 
                (dup.prob_match == dup.max_prob) )

        data = pd.concat([no_dup, dup])
        data.to_csv(preds_file[:-4] + "_deduped.csv", header=None, index=None)
        
    def save(self, path): #FIXME this isn't all that I want to save
        if not os.path.isdir(path):
            os.mkdir(path)
        exists = os.path.isfile(f"{path}/model.xgboost")    
        if exists:
            while True:
                x=input('Model exists in specified path. Are you sure you want to override old model? [Y/N]')
                if x=='Y': 
                    exists=False
                    break
                elif x=='N': break
                else: print('invalid input.')
        if not exists:
            with open(f"{path}/model.xgboost", 'wb') as file: #FIXME add a prompt if the user will override an old model.
                pkl.dump(self.model, file)
            with open(f"{path}/model_scores.json", 'w') as file:
                file.write(f'''{{"training_time": {self.train_time}, "confusion_mat": {list(self.confusion_mat)},"precision": {self.test_precision}, "recall": {self.test_recall}}}''')
            self.comp_eng.save(f"{path}/fe.json")
        
                  
    def load(self, path): #FIXME this isn't all that I want to load
        with open(f"{path}/model.xgboost", "rb") as file:
            self.model = pkl.load(file)
        with open(f"{path}/model_scores.json", "r") as file:
            json_data = json.load(file)
            params = {'training_time': self.train_time, 'confusion_mat': self.confusion_mat,
                      'precision': self.test_precision, 'recall': self.test_recall,
                      }
            for key in params:
                params[key] = json_data[key]
        self.comp_eng.load(f"{path}/fe.json")

if __name__=='__main__':
    import sys
    from record_set import RecordDataFrame
    from pairs_set import PairsCOO, PairsMatrix
    from feature_engineer import FeatureEngineer
    from xgboost_match import XGBoostMatch
    from record_set import RecordDB
    