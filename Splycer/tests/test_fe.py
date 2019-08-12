#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 10:15:09 2019

@author: thegrasley
"""
import pytest
import numpy as np
import pandas as pd
from splycer.feature_engineer import FeatureEngineer

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
    rec1 = pd.DataFrame().from_records(rec1)
    rec2 = pd.DataFrame().from_records(rec2)
    return rec1, rec2

def test_add_comp_wo_args():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw")
    assert fe.raw_compares == [["first", "jw", {}]]
    assert fe.rec_columns == {"first"}
    assert fe.ncompares == 1
    assert fe.pipeline[0].col == "first"

def test_add_comp_w_args():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"})
    assert fe.raw_compares == [["first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"}]]
    assert fe.rec_columns == {"first"}
    assert fe.ncompares == 1
    assert fe.pipeline[0].comm_col == "first_comm"

def test_add_two_comps():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw")
    fe.add_comparison("race", "exact match")
    assert fe.raw_compares == [["first", "jw", {}], ["race", "exact match", {}]]
    assert fe.rec_columns == {"first", "race"}
    assert fe.ncompares == 2
    assert fe.pipeline[1].col == "race"

def test_add_mult_rec_col_comp():
    fe = FeatureEngineer()
    fe.add_comparison(["bp_lat", "bp_lon"], "geo dist")
    assert fe.raw_compares == [[["bp_lat", "bp_lon"], "geo dist", {}]]
    assert fe.rec_columns == {"bp_lat", "bp_lon"}
    assert fe.ncompares == 1
    assert fe.pipeline[0].col == ["bp_lat", "bp_lon"]

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

def test_compare_pipeline1(): #One compare
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw")
    assert fe.compare(rec1, rec2) == np.array([0.96], dtype=np.float32)

def test_compare_pipeline2(): #Multiple compares
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw")
    fe.add_comparison("middle", "bigram")
    fe.add_comparison("last", "trigram")
    fe.add_comparison("state", "exact match")
    fe.add_comparison("race", "exact match")
    fe.add_comparison(["bp_lat", "bp_lon"], "geo dist")
    fe.add_comparison(["first_vec1", "first_vec2", "first_vec3"], "euclidean dist")
    correct_ans = np.array([[0.96, 1., 0., 1., 0., 1.065943e+03, 0.1979898987322331]], dtype=np.float32)
    np.testing.assert_array_almost_equal(fe.compare(rec1, rec2), correct_ans, 3)

def test_compare_pipeline3(): #Commonality Weight
    fe = FeatureEngineer()
    rec1, rec2 = create_arrays()
    fe.add_comparison("first", "jw", {"comm_weight": 'd', "comm_col": "first_comm"})
    assert fe.compare(rec1, rec2) == np.array([0.96 / np.log1p((rec1["first_comm"] + rec2["first_comm"]) / 2)], dtype=np.float32)

def test_compare_mult_recs():
    fe = FeatureEngineer()
    fe.add_comparison("first", "jw")
    rec1, rec2 = create_arrays()
    rec3 = pd.concat([rec1, rec2])
    rec4 = pd.concat([rec2, rec1])
    np.testing.assert_array_almost_equal(fe.compare(rec3, rec4), np.array([[0.96], [0.96]], dtype=np.float32), 3)

    
def save():
    pass

def load():
    pass