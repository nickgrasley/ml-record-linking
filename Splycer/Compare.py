# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:19:28 2019

@author: ngrasley
"""

import numpy as np

from numba import jitclass
from numba import uint32, uint8, float64

record_spec  = [("pid", uint32), ("record", uint8), ("features", float64[:])]
@jitclass(record_spec)
class Record(object):
    def __init__(self, uid, record, features):
        self.uid = uid
        self.record = record
        self.features = features


csr_spec = [("rows", uint32[:]), ("columns", uint32[:]), ("values", uint8[:])]
@jitclass(csr_spec)
class csr_matrix(object):
    def __init__(self, uids1, uids2, data):
        self.rows = np.bincounts(uids1)
        self.columns = uids2
        self.values = data

compare_spec = [("record1", uint8), ("record2", uint8), ("matrix", csr_matrix.class_type.instance_type)]
@jitclass(compare_spec)
class Compares(object):
    """Create a sparse matrix of the comparison pairs of records. data contains
       blocking and match information: 0 = not compared; 1 = compared; 
       2 = compared but false match; 3 = compared and true match. Likely, you'll
       block first and get 0s and 1s, and then modify the 1s to 2s and 3s as you
       go.
       
       To initialize Compares, specify the record ids for both records; uids for
       both sets; and the data. For index i, data[i] is the compare value for uids1[i]
       and uids2[i]. See the csr_matrix documentation for more info on this.
    """
    def __init__(self, record1, record2, uids1, uids2, data):
        self.record1 = record1
        self.record2 = record2
        self.matrix = csr_matrix((data, uids1, uids2))