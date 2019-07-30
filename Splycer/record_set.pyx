#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 14:02:20 2019

@author: Nick Grasley (ngrasley@stanford.edu)
"""
import numpy as np

import pandas as pd
import turbodbc
from base import RecordBase

class RecordDict(RecordBase):
    """Records are organized in a dictionary with the key as a unique identifier
       and the value as a numpy structured array of record info. Since dictionary
       lookup scales at a constant rate with the number of records, this object
       is most efficient when you merely have to grab record information. This
       assumes the record arrays are numpy structured arrays.
    """
    
    def __init__(self, record_id, uids, features):
        self.record_id = record_id
        self.data = dict(zip(uids, features))
        
    def get_record(self, int uid, list var_list=[]):
        if len(var_list) == 0:
            return self.data.get(uid)
        else:
            return self.data.get(uid)[var_list]
        
    def __getitem__(self, int uid):
        return self.get_record(uid)

class RecordDB(RecordBase):
    """Records are stored in a sql database. If you need to do any blocking,
       sql handles a lot of the hard work of building data structures for efficient
       merges. You have to pay the upfront cost of setting up the database though.
    """
    def __init__(self, record_id, table_name, idx_name, conn_str):

        self.record_id = record_id
        self.table_name = table_name
        self.idx_name = idx_name
        options = turbodbc.make_options(prefer_unicode=True) #Apparently necessary for MS sql servers.
        self.conn = turbodbc.connect(conn_str, turbodbc_options=options)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"select column_name from information_schema.columns\
                              where table_name = '{self.table_name}'")
        cols = self.cursor.fetchall()
        self.cols = np.ndarray(len(cols), dtype="U50")
        for i in range(len(cols)):
            self.cols[i] = cols[i][0]

    def __getitem__(self, uid):
        return self.get_record(uid)

    def get_record(self, uid, var_list=None):
        if var_list is None:
            data = self.cursor.execute(f"select * from {self.table_name} \
                                         where {self.idx_name} = {uid}").fetchallnumpy()
        else:
            data = self.cursor.execute(f"select {var_list} from {self.table_name}\
                                         where {self.idx_name} = {uid}").fetchallnumpy()
        return data

class RecordDataFrame(RecordBase):
    """Records are stored in a Pandas DataFrame. This is best for small datasets
       with a limited number of string features, or if you need to block and don't
       want to set up a sql server. However, it has slow lookup, slow merges, and
       is a memory hog, so don't use this for large datasets.
    """
    def __init__(self, record_id, records, uid_col=None):
        self.record_id = record_id
        if type(records) == pd.core.frame.DataFrame:
            self.df = records
        else:
            self.df = pd.DataFrame(records, index=uid_col)
    def __getitem__(self, uid):
        return self.df.loc[[uid], :].to_records(index=False)
    def get_record(self, uid, var_list=None):
        if var_list is None:
            return self.df.loc[[uid], :].to_records(index=False)
        return self.df.loc[[uid], var_list].to_records(index=False)

