# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 10:34:36 2019

@author: ngrasley
"""
import sys
sys.path.append('R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/build/lib.win-amd64-3.7')
import numpy as np
import numpy.testing as npt
import pandas as pd
from haversine import haversine
from nltk.util import ngrams
from comparisons import JW, AbsDistance, EuclideanDistance, GeoDistance, BiGram,\
                        TriGram, NGram, BooleanMatch

def create_dfs():
    df1 = pd.DataFrame({"first": ["Billy", "Bob", "Joe", np.nan], "state": [1, 1, 2, np.nan],
                        "bp_lat": [45.31, 40.14, 38.55, np.nan], "bp_lon": [122.40, 111.39, 100.77, np.nan], 
                        "first_vec1": [1.21, 3.2, 0.87, np.nan], "first_vec2": [0.98, 2.11, 1.41, np.nan],
                        "byear": [1901, 1889, 1910, np.nan], "first_comm": [100, 20, 500, np.nan]})
    df2 = pd.DataFrame({"first": ["Bill", "William", "Joe", "Jim"], "state": [2, 5, 2, 3],
                        "bp_lat": [43.31, 10.14, 38.55, 88.23], "bp_lon": [121.40, 55.39, 100.77, 101.21], 
                        "first_vec1": [1.1, 0.5, 0.87, 0.01], "first_vec2": [1.1, 0.67, 1.41, 0.34],
                        "byear": [1900, 1930, 1910, 1920], "first_comm": [98, 300, 500, 110]})
    return df1, df2

def test_jw():
    df1, df2 = create_dfs()
    jw = JW("first")
    npt.assert_array_equal(jw.compare(df1, df2), np.array([0.96, 0.0, 1.0, np.nan], dtype=np.float64))
    
def test_abs_distance():
    df1, df2 = create_dfs()
    abs_dist = AbsDistance("state")
    npt.assert_array_equal(abs_dist.compare(df1, df2).values, np.array([1, 4, 0, np.nan]))

def test_euclidean_distance():
    df1, df2 = create_dfs()
    euc_dist = EuclideanDistance(["first_vec1", "first_vec2"])
    val1 = np.linalg.norm(np.array([1.21, 0.98]) - np.array([1.1, 1.1]))
    val2 = np.linalg.norm(np.array([3.2, 2.11]) - np.array([0.5, 0.67]))
    val3 = 0.
    npt.assert_array_equal(euc_dist.compare(df1, df2), np.array([val1, val2, val3, np.nan]))
    
def test_geo_distance():
    df1, df2 = create_dfs()
    geo_dist = GeoDistance(["bp_lat", "bp_lon"])
    val1 = haversine((45.31, 122.40), (43.31, 121.40))
    val2 = haversine((40.14, 111.39), (10.14, 55.39))
    val3 = 0.
    npt.assert_allclose(geo_dist.compare(df1, df2), np.array([val1, val2, val3, np.nan]), rtol=1e-03)
    
def test_ngram():
    df1, df2 = create_dfs()
    bg = BiGram("first")
    tg = TriGram("first")
    ng = NGram("first", n=4)

    bg_billy = set(ngrams("Billy", 2)) 
    tg_billy = set(ngrams("Billy", 3))
    ng_billy = set(ngrams("Billy", 4))
    
    bg_bill = set(ngrams("Bill", 2)) 
    tg_bill = set(ngrams("Bill", 3))
    ng_bill = set(ngrams("Bill", 4))
    
    bg_bob  = set(ngrams("Bob", 2))
    tg_bob  = set(ngrams("Bob", 3))
    ng_bob  = set(ngrams("Bob", 4))
    
    bg_will = set(ngrams("William", 2))
    tg_will = set(ngrams("William", 3))
    ng_will = set(ngrams("William", 4))

    bg_billy_bill = len(bg_billy.intersection(bg_bill)) / float(len(bg_billy.union(bg_bill)))
    tg_billy_bill = len(tg_billy.intersection(tg_bill)) / float(len(tg_billy.union(tg_bill)))
    ng_billy_bill = len(ng_billy.intersection(ng_bill)) / float(len(ng_billy.union(ng_bill)))
    
    bg_bob_will = len(bg_bob.intersection(bg_will)) / float(len(bg_bob.union(bg_will)))
    tg_bob_will = len(tg_bob.intersection(tg_will)) / float(len(tg_bob.union(tg_will)))
    ng_bob_will = len(ng_bob.intersection(ng_will)) / float(len(ng_bob.union(ng_will)))
    
    npt.assert_array_equal(bg.compare(df1, df2), np.array([bg_billy_bill, bg_bob_will, 1., np.nan]))
    npt.assert_array_equal(tg.compare(df1, df2), np.array([tg_billy_bill, tg_bob_will, 1., np.nan]))
    npt.assert_array_equal(ng.compare(df1, df2), np.array([ng_billy_bill, ng_bob_will, 1., np.nan]))
    
def test_bool_match():
    df1, df2 = create_dfs()
    bm = BooleanMatch("state")
    npt.assert_array_equal(bm.compare(df1, df2).values, np.array([False, False, True, np.nan]))