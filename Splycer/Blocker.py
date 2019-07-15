# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 12:08:04 2019

@author: ngrasley
"""
import pyodbc
import pandas as pd

class Blocker(BaseEstimator, TransformerMixin):
    """Handles the blocking. Would be cool if this could build the sql script for
       blocking after the user defines what blocks they want, but that's a tall 
       order. For now, the user points blocker to where the sql script is, and 
       it runs the sql script.
    """
    def __init__(self, sql_script_file_path, dsn="rec_db"):
        with open(sql_script_file_path, "r") as file:
            self.sql_script = file
        self.conn = pyodbc.connect(f"DSN={dsn}") #TODO maybe expand this functionality to allow for non-DSN connection strings?
        
    def fit(self, X=None):
        return self
        
    def transform(self, X=None, Y=None):
        return pd.read_sql(self.sql_script, self.conn) #FIXME I need to allow it to specify the chunk of the census to do.
    
    def save(self): #FIXME code this up
        return self
    def load(self): #FIXME code this up
        return self