# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 16:15:11 2019

@author: ngrasley
"""
import sys
sys.path.append('R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/build/lib.win-amd64-3.7')
from record_set import RecordDict, RecordDB
from pairs_set import PairsCSR, PairsDB, PairsCOO, PairsMatrix
import numpy as np
import pandas as pd

def test_RecordDict():
    rec_id = 1
    uids = [1,2,3]
    features = [np.array([(1,2)], dtype=[('a', int), ('b', int)]), np.array([(3,4)], dtype=[('a', int), ('b', int)]), np.array([(5, 6)], dtype=[('a', int), ('b', int)])]
    rd = RecordDict(rec_id, uids, features)
    dummy_frame = pd.DataFrame({'a': [1,3], 'b': [2,4]}, dtype=np.int32)
    pd.testing.assert_frame_equal(rd.get_records([1,2]), dummy_frame)
        
def test_RecordDB():
    rec_id = 1
    table_name = "compact1910_census"
    idx_name = "index_1910"
    conn_str = "rec_db"
    rec_db = RecordDB(rec_id, table_name, idx_name, conn_str)
    data = rec_db.get_records([0,1,2])
    dummy_frame = pd.DataFrame([[0,0,1906,80957,np.nan,0,3,1,3,3,3,14,17,0,26,26,689,1090,2,3],
                                [1,0,1900,80957,np.nan,0,3,1,3,3,3,14,17,0,25,24,689,1123,16,3],
                                [2,0,1881,80958,np.nan,0,0,0,3,3,3,14,17,13,20,19,83,323,5,16]],
                               columns=["index_1910", "marstat_1910", "birth_year_1910", 
                                        "household_1910", "immigration_1910", "race_1910", 
                                        "rel_1910", "female_1910", "bp_1910", "mbp_1910", 
                                        "fbp_1910", "state_1910", "county_1910", "midinit_1910",
                                        "cohort1_1910", "cohort2_1910", "last_sdxn_1910", 
                                        "first_sdxn_1910", "first_init_1910", "last_init_1910"])
    dummy_frame["immigration_1910"] = dummy_frame["immigration_1910"].astype(object)
    pd.testing.assert_frame_equal(data, dummy_frame)

def build_PairsDB():
    rec_id1 = 1
    rec_id2 = 2
    table_name = "training_indices_test"
    conn_str = "rec_db"
    idx_cols = ["index_1900", "index_1910"]
    return PairsDB(rec_id1, rec_id2, table_name, conn_str, idx_cols)

def test_PairsDB_get_pairs():
    pairs_db = build_PairsDB()
    for i in pairs_db.get_pairs():
        assert len(i) == 2
        assert type(i) == tuple
        assert type(i[0]) == np.ndarray
   

def build_CompareCSR():
    rec_id1 = 1
    rec_id2 = 2
    uids1 = np.array([0,1,1,2,2,2,3,3,3,3,4,4,4,4,4,5,5,5,5,5,5,6,6,6,6,6,6,6,7,7])
    uids2 = np.arange(30, dtype=np.uint32)
    data = np.tile([1,2], 15)
    return PairsCSR(rec_id1, rec_id2, uids1, uids2, data)

def test_PairsCSR_getitem():
    comp_csr = build_CompareCSR()
    assert comp_csr[5, 20] == 1
    assert comp_csr[5, 17] == 2
    assert np.isnan(comp_csr[5, 22])
    assert np.isnan(comp_csr[5, 13])

def test_PairsCSR_data():
    comp_csr = build_CompareCSR()
    assert comp_csr.record_id1 == 1
    assert comp_csr.record_id2 == 2
    assert np.all(comp_csr.indptr == np.array([0,1,3,6,10,15,21,28,30]))
    assert np.all(comp_csr.indices == np.arange(30))
    assert np.all(comp_csr.data == np.tile([1,2], 15))

def test_PairsCSR_iter():
    comp_csr = build_CompareCSR()
    test_i = 0
    test_j = 0
    test_k = 1
    it = 0
    for i, j, k in comp_csr:
        assert i == test_i
        assert j == test_j
        assert k[0] == test_k
        if it == test_i:
            test_i += 1
            it = -1
        test_j += 1
        test_k = (k[0] * 2) % 3
        it += 1

def build_PairsCOO():
    rec_id1 = 1
    rec_id2 = 2
    uids1 = np.array([0,1,1,2,2,2,3,3,3,3,4,4,4,4,4,5,5,5,5,5,5,6,6,6,6,6,6,6,7,7])
    uids2 = np.arange(30)
    data = np.tile([1,2], 15)
    return PairsCOO(rec_id1, rec_id2, uids1, uids2, data)    

def test_PairsCOO_get_pairs():
    pairs_coo = build_PairsCOO()
    for i in pairs_coo.get_pairs():
        assert len(i) == 2
        assert type(i) == tuple
        assert type(i[0]) == list

def test_PairsCOO_iter():
    pairs_coo = build_PairsCOO()
    for i in pairs_coo:
        assert len(i) == 3
        assert type(i) == tuple
        assert type(i[0]) == np.int32
        assert i[2] in [1,2]
        
def build_PairsMatrix():
    rec_id1 = 1
    rec_id2 = 2
    mat = np.array([[0,1,0], [1,0,1], [0,1,0]])
    return PairsMatrix(rec_id1, rec_id2, mat)

def test_PairsMatrix_get_pairs():
    pairs_mat = build_PairsMatrix()
    for i in pairs_mat.get_pairs():
        assert len(i) == 2
        assert type(i) == tuple
        assert type(i[0]) == list

def test_PairsMatrix_iter():
    pairs_mat = build_PairsMatrix()
    for i in pairs_mat:
        assert len(i) == 3
        assert type(i) == tuple
        assert i[2] in [0,1]