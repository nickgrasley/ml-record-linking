#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 10:15:09 2019

@author: thegrasley
"""
import sys
sys.path.append('/Users/thegrasley/Documents/price_ra/ml-record-linking/Splycer')
import pytest
import numpy as np
from comparisons import JW, GeoDistance, BooleanMatch
from feature_engineer import FeatureEngineer

def create_arrays():
    rec_type = np.dtype([("first", "U10"), ("middle", "U10"), ("last", "U10"),
                              ("state", np.uint8), ("race", np.uint8), ("bp_lat", np.float32),
                              ("bp_lon", np.float32), ("first_vec1", np.float32),
                              ("first_vec2", np.float32), ("first_vec3", np.float32),
                              ("first_comm", int)])
    rec1 = np.array([("Billy", "Bob", "Joe", 1, 2, 45.31, 122.40, 1.21, 3.2, 0.87, 112)],
                          dtype=rec_type)
    rec2 = np.array([("Bill", "Bob", "Smith", 1, 1, 40.14, 111.39, 1.21, 3.34, 1.01, 87)],
                          dtype=rec_type)
    return rec1, rec2

def add_comp_wo_args(self):
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw")
    assert fe.raw_compares == [["first", "jw", {}]]
    assert fe.rec_columns == set("first")
    assert fe.ncompares == 1
    assert fe.pipeline == [JW("first")]

def add_comp_w_args():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"})
    assert fe.raw_compares == [["first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"}]]
    assert fe.rec_columns == set("first")
    assert fe.ncompares == 1
    assert fe.pipeline == [JW("first", 'd', "first_comm")]

def add_two_comps():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw")
    fe.add_comparision("race", "exact match")
    assert fe.raw_compares == [["first", "jw", {}], ["race", "exact match", {}]]
    assert fe.rec_columns == {"first", "race"}
    assert fe.ncompares == 2
    assert fe.pipeline == [JW("first"), BooleanMatch("race")]

def add_mult_rec_col_comp():
    fe = FeatureEngineer()
    fe.add_comparison(["bp_lat", "bp_lon"], "geo dist")
    assert fe.raw_compares == [[["bp_lat", "bp_lon"], "geo dist", {}]]
    assert fe.rec_columns == {"bp_lat", "bp_lon"}
    assert fe.ncompares == 1
    assert fe.pipeline == [GeoDistance(["bp_lat", "bp_lon"])]

def throw_nonexist_comp_error():
    fe = FeatureEngineer()
    with pytest.raises(KeyError):
        fe.add_comparison("first", "jq")

def rm_comp():
    pass #also check to make sure ncompares, pipeline, etc. are correctly changed

def rm_nonexist_comp_error():
    pass

def rm_feature_w_multiple_comp():
    pass

def check_pipeline_unused_cols():
    pass

def check_pipeline_extra_cols():
    pass

def compare_pipeline1(): #One compare
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw")
    assert fe.compare(rec1, rec2) == np.array([0.96], dtype=np.float32)

def compare_pipeline2(): #Multiple compares
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw")
    fe.add_comparison("middle", "bigram")
    fe.add_comparison("last", "trigram")
    fe.add_comparison("state", "exact match")
    fe.add_comparison("race", "exact match")
    fe.add_comparison(["bp_lat", "bp_lon"], "geo dist")
    fe.add_comparison(["first_vec1", "first_vec2", "first_vec3"], "euclidean dist")
    assert fe.compare(rec1, rec2) == np.array([0.96, 1., 0., 1., 0., 1067.6585766467526, 0.1979898987322331], dtype=np.float32)

def compare_pipeline3(): #Commonality Weight
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"})
    assert fe.compare(rec1, rec2) == np.array([0.96 / np.log1p((rec1["first_comm"] + rec2["first_comm"]) / 2)], dtype=np.float32)
    
def save():
    pass

def load():
    pass