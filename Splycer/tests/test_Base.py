# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 16:15:11 2019

@author: ngrasley
"""
from Splycer.Base import RecordDict, CompareCSR
import numpy as np
from time import time
def test_RecordDict():
    rec_id = 1
    uids = np.arange(300, dtype=np.uint32)
    features = [np.arange(i, i + 20)]
    rec_dict = RecordDict(rec_id, uids, features)
    start = time()
    for i in range(300):
        rec_dict.get_record(i)
    end = time()

#test CompareCSR()
def test_CompareCSR():
    rec_id1 = 1
    rec_id2 = 2
    