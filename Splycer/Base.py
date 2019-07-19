#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 20:13:29 2019

@author: thegrasley
"""
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
import pyodbc
import abc

class RecordBase(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_record(self, uid):
        pass
    @abc.abstractmethod
    def __get_item__(self, uid):
        pass

class CompareBase(object, metaclass=abc.ABCMeta):
    def __init__(self, shape):
        self.shape = shape
    @abc.abstractmethod
    def __iter__(self):
        pass
    @abc.abstractmethod
    def __getitem__(self, uid1, uid2):
        pass
    @abc.abstractmethod
    def get_pairs(self):
        pass

class RecordDict(dict, RecordBase):
    """Records are organized in a dictionary with the key as a unique identifier
       and the value as the record information. Since dictionary lookup scales
       at a constant rate with the number of records, this object is most
       efficient when you merely have to grab record information.
    """
    def __init__(self, record_id, uids, features):
        self.record_id = record_id
        super().__init__(zip(uids, features))
    def get_record(self, uid):
        return self.get(uid)
        
class RecordDB(RecordBase): #FIXME implement this
    """Records are stored in a sql database. If you need to do any blocking,
       sql handles a lot of the hard work of building data structures for efficient
       merges. You have to pay the upfront cost of setting up the database though.
    """
    def __init__(self, record_id, table_name, idx_name, conn_str, var_list=None):
        self.record_id = record_id
        self.table_name = table_name
        self.idx_name = idx_name
        dsn = pyodbc.connect(conn_str)
        self.cursor = dsn.cursor()
    
    def __get_item__(self, uid):
        return self.get_record(uid)
    
    def get_record(self, uid):
        if self.var_list is None:
            data = self.cursor.execute(f"select * from {self.table_name} where {self.idx_name} = {uid}").fetchall()
        else:
            data = self.cursor.execute(f"select {self.var_list} from {self.table_name} where {self.idx_name} = {uid}").fetchall()
        return data

class RecordDataFrame(RecordBase):
    """Records are stored in a Pandas DataFrame. This is best for small datasets
       with a limited number of string features, or if you need to block and don't
       want to set up a sql server. However, it has slow lookup, slow merges, and
       is a memory hog, so don't use this for large datasets.
    """
    def __init__(self, record_id, records, uid_col):
        self.record_id = record_id
        self.df = pd.DataFrame(records, index_col = uid_col)
    def __get_item__(self, uid):
        self.df.loc[uid, :]
    def get_record(self, uid):
        self.df.loc[uid, :]

class CompareCSR(CompareBase):
    """Comparisons are stored in a Compressed Sparse Row matrix. This is optimal
       for large datasets that restric the number of compares significantly
       through blocking
    """
    def __init__(self, record_id1, record_id2, uids1, uids2, data):
        self.record_id1 = record_id1
        self.record_id2 = record_id2
        matrix = csr_matrix((data, (uids1, uids2)))
        self.indptr = matrix.indptr
        self.indices = matrix.indices
        self.data = matrix.data
        super().__init__(matrix.shape)
        
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
                
class CompareDB(CompareBase): #FIXME implement this
    """Comparisons are stored in a sql database. This object assumes that it is
       stored as a table of unique identifier pairs.
    """
    def __init__(self):
        pass
class CompareMatrix(CompareBase):
    """Comparisons are stored in a matrix, where matrix[i, j] = 1 if the two
       records are valid comparisons. This is best for small datasets since
       you can quickly look up any compare. However, it takes more space than the
       other Compare objects.
    """
    pass