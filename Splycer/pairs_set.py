#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 14:06:40 2019

@author Nick Grasley (ngrasley@stanford.edu)
"""

import numpy as np
from scipy.sparse import csr_matrix
import turbodbc
from base import PairsBase

class PairsCSR(PairsBase):
    """Comparisons are stored in a Compressed Sparse Row matrix. This is optimal
       for large datasets that restric the number of compares significantly
       through blocking
    """
    def __init__(self, record_ids, uids1, uids2, data):
        self.record_ids = record_ids
        matrix = csr_matrix((data, (uids1, uids2)))
        self.indptr = matrix.indptr
        self.indices = matrix.indices
        self.data = matrix.data
        super().__init__(self.data.shape[0])

    def __iter__(self):
        for i in range(np.shape(self.indptr)[0]-1):
            for j in range(self.indptr[i], self.indptr[i+1]):
                yield (i, self.indices[j], self.data[j:j+1]) #this returns data as a view to allow modification of it.

    def __getitem__(self, uids):
        uid1, uid2 = uids
        uid1_start = self.indptr[uid1]
        uid1_end = self.indptr[uid1 + 1]
        i = 0
        uid2_tmp = -1
        while i < (uid1_end - uid1_start):
            uid2_tmp = self.indices[uid1_start + i]
            i += 1
            if uid2_tmp == uid2:
                return self.data[i]
        return np.nan

    def get_pairs(self):
        for i in range(np.shape(self.indptr)[0]-1):
            for j in range(self.indptr[i], self.indptr[i+1]):
                yield (i, self.indices[j])

class PairsDB(PairsBase):
    """Comparisons are stored in a sql database. This object assumes that it is
       stored as a table of unique identifier pairs.
    """
    def __init__(self, record_ids, table_name, conn_str, idx_cols):
        self.record_ids = record_ids
        self.table_name = table_name
        self.idx_cols = idx_cols
        self.conn = turbodbc.connect(conn_str)
        self.cursor = self.conn.cursor()
        ncompares = self.cursor.execute(f"select count(*) from {self.table_name}").fetchone()
        super().__init__(ncompares)

    def __iter__(self):
        self.cursor.execute(f"select * from {self.table_name}")
        row = True
        while row:
            row = self.cursor.fetchone()
            yield row #FIXME turn to numpy array
    def __getitem__(self, uids):
        uid1, uid2 = uids
        self.cursor.execute(f"select * from {self.table_name} \
                              where {self.idx_cols[0]} = {uid1} and {self.idx_cols[1]} = {uid2}")
        return self.cursor.fetchall() #FIXME turn to numpy array

    def get_pairs(self):
        self.cursor.execute(f"select {self.idx_cols[0]}, {self.idx_cols[1]} from {self.table_name}")
        row = True
        while row:
            row = self.cursor.fetchone()
            yield row #FIXME turn to numpy array

class PairsMatrix(PairsBase):
    """Comparisons are stored in a matrix, where matrix[i, j] = 1 if the two
       records are valid comparisons. This is best for small datasets since
       you can quickly look up any compare. However, it takes more space than the
       other Compare objects.
    """
    def __init__(self, record_ids, compare_matrix):
        self.record_ids = record_ids
        self.matrix = compare_matrix
        super().__init__(np.count_nonzero(compare_matrix))

    def __iter__(self):
        indices = np.nonzero(self.matrix)
        for i, j in indices:
            yield (i, j, self.matrix[i:i+1, j:j+1])

    def __getitem__(self, uids):
        return self.matrix[uids[0], uids[1]]

    def get_pairs(self):
        indices = np.nonzero(self.matrix)
        for i in range(indices[0].shape[0]):
            yield (indices[0][i], indices[1][i])
