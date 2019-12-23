# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 12:09:36 2019

@author: ngrasley
"""
import numpy as np
import pandas as pd
import turbodbc
from pairs_set import PairsDB, PairsCOO

class BlockDB(PairsDB):
    """Here's a prototype of a clever way of blocking that I came up with.
       The idea is that the blocks parameter of the block() function is a list
       of different blocks. For example, blocks could be 
       [["first_soundex", "birth_year], ["last_soundex", "bp"]]. ["first_soundex", "birth_year"]
       is a block, meaning if two records are compared, they must match on both
       of those variables. A compare can show up if it satisfies either of the blocks.
       Be warned, I haven't tested this.
    """
    def __init__(self, record_id1, record_id2, table_name, dsn_str, idx_cols, table1, table2):
        conn = turbodbc.connect(dsn=dsn_str)
        cursor = conn.cursor()
        table_exists = cursor.execute(f"if object_id('dbo.{table_name}', 'U') is not null select 1 else select 0").fetchone()[0]
        if not table_exists:
            self.create_table()
        super().__init__(record_id1, record_id2, table_name, dsn_str, idx_cols)
        self.table1 = table1
        self.table2 = table2
        self.blocks = None

    def create_table(self):
        sql_str = f"create table {self.table_name} ({self.idx_cols[0]} int primary key not null, {self.idx_cols[1]} int not null)"
        self.cursor.execute(sql_str)
        self.conn.commit()

    def set_blocks(self, blocks):
        self.blocks = blocks

    def block(self): #FIXME add a chunksize in case sql can't handle all the blocking at once.
        """Execute a block for a pair of years and a chosen sample"""
        sql_str = ""
        for block in self.blocks:
            sql_str += f"""select t1.[index] as {self.idx_cols[0]}, t2.[index] as {self.idx_cols[1]}
                           into {self.table_name}
                           from self.table1 as t1
                           left join self.table2 as t2 on """
            for block_var in block:
                sql_str += f"t1.{block_var} = t2.{block_var} and "
            sql_str = sql_str[:-4] + f"where t2.[index] is not null union "
        sql_str = sql_str[:-7]
        self.cursor.execute(sql_str)
        self.conn.commit()
        
def block_dataframe(record_df1, record_df2, blocks):
    pairs = []
    index_name1 = record_df1.index.name
    index_name2 = record_df2.index.name
    record_df1 = record_df1.reset_index().rename({index_name1: f"index_{record_df1.record_id}"})
    record_df2 = record_df2.reset_index().rename({index_name2: f"index_{record_df2.record_id}"})
    for b in blocks:
        pairs.append(pd.merge(record_df1, record_df2, how="left", on=blocks)[[f"index_{record_df1.record_id}", f"index_{record_df2.record_id}"]])
    pairs = pd.concat(pairs).drop_duplicates()
    return PairsCOO(record_df1.record_id, record_df2.record_id,
                    pairs["index_{record_df1.record_id}"].values, 
                    pairs["index_{record_df2.record_id}"].values, 
                    np.ones(pairs.shape[0]))