# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 16:15:11 2019

@author: ngrasley
"""
from Splycer.Base import RecordDict
import numpy as np
def test_RecordDict():
    rec_id = 1
    uids = np.arange(300, dtype=np.uint32)
    features = [np.arange(i, i + 20) for i in range(300)]
    rec_dict = RecordDict(rec_id, uids, features)
    for i in range(300):
        assert np.all(rec_dict.get_record(i) == np.arange(i, i + 20))
