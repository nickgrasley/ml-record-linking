# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 16:15:11 2019

@author: ngrasley
"""
from Splycer.Base import RecordDict, CompareCSR
import numpy as np
def test_RecordDict():
    rec_id = 1
    uids = np.arange(30, dtype=np.uint32)
    features = [np.arange(i, i + 20) for i in range(30)]
    rec_dict = RecordDict(rec_id, uids, features)
    for i in range(30):
        assert np.all(rec_dict.get_record(i) == np.arange(i, i + 20))

def build_CompareCSR():
    rec_id1 = 1
    rec_id2 = 2
    uids1 = np.array([0,1,1,2,2,2,3,3,3,3,4,4,4,4,4,5,5,5,5,5,5,6,6,6,6,6,6,6,7,7])
    uids2 = np.arange(30, dtype=np.uint32)
    data = np.tile([1,2], 15)
    return CompareCSR(rec_id1, rec_id2, uids1, uids2, data)

def test_CompareCSR_getitem():
    comp_csr = build_CompareCSR()
    assert comp_csr[5, 20] == 1
    assert comp_csr[5, 17] == 2
    assert np.isnan(comp_csr[5, 22])
    assert np.isnan(comp_csr[5, 13])

def test_CompareCSR_data():
    comp_csr = build_CompareCSR()
    assert comp_csr.record_id1 == 1
    assert comp_csr.record_id2 == 2
    assert np.all(comp_csr.indptr == np.array([0,1,3,6,10,15,21,28,30]))
    assert np.all(comp_csr.indices == np.arange(30))
    assert np.all(comp_csr.data == np.tile([1,2], 15))

def test_CompareCSR_iter():
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
    
