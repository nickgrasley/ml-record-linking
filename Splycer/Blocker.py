# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 12:09:36 2019

@author: ngrasley
"""
from pairs_set import PairsDB

class BlockDB(PairsDB):
    """Here's a prototype of a clever way of blocking that I came up with.
       The idea is that the blocks parameter of the block() function is a list
       of different blocks. For example, blocks could be 
       [["first_soundex", "birth_year], ["last_soundex", "bp"]]. ["first_soundex", "birth_year"]
       is a block, meaning if two records are compared, they must match on both
       of those variables. A compare can show up if it satisfies either of the blocks.
    """
    def __init__(self, record_id1, record_id2, table_name, conn_str, idx_cols, table1, table2):
        super().__init__(record_id1, record_id2, table_name, conn_str, idx_cols)
        self.table1 = table1
        self.table2 = table2
        self.blocks = None
    
    def set_blocks(self, blocks):
        self.blocks = blocks
    
    def block(self):
        """Execute a block for a pair of years and a chosen sample"""
        sql_str = ""
        for block in blocks:
            sql_str += f"""select t1.{self.idx_cols[0]}, t2.{self.idx_cols[1]}
                           into {self.table_name}
                           from self.table1 as t1
                           left join self.table2 as t2 on """
            for block_var in block:
                sql_str += f"t1.{block_var} = t2.{block_var} and "
            sql_str = sql_str[:-4] + f"where t2.{self.idx_cols[1]} is not null union "
        sql_str = sql_str[:-7]
        self.cursor.execute(sql_str)
        self.conn.commit()