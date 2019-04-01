# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 13:18:39 2019

@author: jacobrvl
"""

from Splycer.XGBoostMatch import XGBoostMatch
from Splycer.stata_dask import dask_read_stata_delayed_group

test_data = dask_read_stata_delayed_group(["R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/data/more_feat_sample_small.dta"])
test_data = test_data.loc[1:10000,:]
test_Y = test_data['ismatch']
test_X = test_data.drop(['ismatch'], axis = 1)


def test_fit(test_in_X, test_in_Y, test_out): # test_in_X and test_in_Y are a train-test split, and test_out is a previously trained model
    test_in = XGBoostMatch()
    test_in.set_hyper_params(test_in, test_out.hyper_params)
    test_in = XGBoostMatch.fit(test_in, test_in_X, test_in_Y)
    
    assert test_in.test_precision == test_out.test_precision
    assert test_in.test_recall == test_out.test_recall
    assert test_in.hyper_params == test_out.hyper_params
    
def test_predict(test_in, test_out): # test_in is a previously trained model, and test_out is the set of predictions from a model
    assert test_in.predict(test_in, test_X).values == test_out.values


test_out = XGBoostMatch()
test_out.set_hyper_params(test_out, {"verbose": 1})
test_out = XGBoostMatch.fit(test_out, test_X, test_Y)
test_fit(test_X, test_Y, test_out)
    