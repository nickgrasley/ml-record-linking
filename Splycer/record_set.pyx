#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
"""
Created on Mon Jul 22 14:02:20 2019

@author: Nick Grasley (ngrasley@stanford.edu)
"""
import numpy as np
import pandas as pd
import turbodbc
from base import RecordBase

class RecordDict(dict, RecordBase): #FIXME update the output format of data from this class. It should be a Pandas DataFrame.
    """Records are organized in a dictionary with the key as a unique identifier
       and the value as a numpy structured array of record info. Since dictionary
       lookup scales at a constant rate with the number of records, this object
       is most efficient when you merely have to grab record information. This
       assumes the record arrays are numpy structured arrays.
    """
    def __init__(self, record_id, uids, features):
        self.record_id = record_id
        self.var_list = None
        super().__init__(zip(uids, features))
    
    def set_var_list(self, var_list):
        self.var_list = var_list
    
    def get_record(self, uid):
        if self.var_list is None:
            return pd.DataFrame.from_records(self.get(uid))
        return pd.DataFrame.from_records(self.get(uid)[self.var_list])

    def get_records(self, uids):
        rec_array = np.concatenate(tuple(self.get(i) for i in uids))
        if self.var_list is None:
            return pd.DataFrame.from_records(rec_array)
        return pd.DataFrame.from_records(rec_array)[self.var_list]

class RecordDB(RecordBase): #FIXME this class assumes a standardized naming convention for all comparable record sets, i.e. I need to remove years from variable names.
    """Records are stored in a sql database. If you need to do any blocking,
       sql handles a lot of the hard work of building data structures for efficient
       merges. You have to pay the upfront cost of setting up the database though.
    """
    def __init__(self, record_id, table_name, idx_name, dsn):
        self.record_id = record_id
        self.var_list = None
        self.table_name = table_name
        self.idx_name = idx_name
        options = turbodbc.make_options(prefer_unicode=True) #Apparently necessary for MS sql servers.
        self.conn = turbodbc.connect(dsn=dsn, turbodbc_options=options)
        self.cursor = self.conn.cursor()
        self.extra_joins = ""
        """
        self.cursor.execute(f"select column_name from information_schema.columns where table_name = '{self.table_name}'")
        cols = self.cursor.fetchall()
        self.cols = np.ndarray(len(cols), dtype="U50")
        for i in range(len(cols)):
            self.cols[i] = cols[i][0]
        """

    def set_var_list(self, var_list):
        self.var_list = var_list
        
    def set_joins(self, join_str):
        self.extra_joins = join_str

    def __getitem__(self, uid):
        return self.get_record(uid)

    def get_record(self, uid):
        if self.var_list is None:
            data = pd.read_sql(f"select * from {self.table_name} where {self.idx_name} = {uid} {self.extra_joins}", self.conn)
        else:
            data = pd.read_sql(f"select {self.var_list} from {self.table_name} where {self.idx_name} = {uid} {self.extra_joins}", self.conn)
        return data
#FIXME return records from get_records in same order as uid list
    def get_records(self, uids):
        if self.var_list is None:
            data = pd.read_sql(f"select * from {self.table_name} where {self.idx_name} in {tuple(uids)} {self.extra_joins}", self.conn)
        else:
            data = pd.read_sql(f"select {self.var_list} from {self.table_name} where {self.idx_name} in {tuple(uids)} {self.extra_joins}", self.conn)
        return data.set_index('index')[uids].reset_index().rename(columns={'index':self.idx_name})

class RecordDataFrame(RecordBase):
    """Records are stored in a Pandas DataFrame. This is best for small datasets
       with a limited number of string features, or if you need to block and don't
       want to set up a sql server. However, it has slow lookup, slow merges, and
       is a memory hog, so don't use this for large datasets.
    """
    def __init__(self, record_id, records, uid_col=None):
        self.record_id = record_id
        self.var_list = None
        if type(records) == pd.core.frame.DataFrame:
            self.df = records
        else:
            self.df = pd.DataFrame(records, index=uid_col)

    def set_var_list(self, var_list):
        self.var_list = var_list

    def __getitem__(self, uid):
        return self.df.loc[uid, :]

    def get_record(self, uid):
        if self.var_list is None:
            return self.df.loc[[uid], :].reset_index(drop=True)
        return self.df.loc[[uid], self.var_list].reset_index(drop=True)

    def get_records(self, uids):
        if self.var_list is None:
            return self.df.loc[uids, :].reset_index(drop=True)
        return self.df.loc[uids, self.var_list].reset_index(drop=True)

