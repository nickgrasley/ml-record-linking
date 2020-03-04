# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 12:09:36 2019

@author: Ben Busath
bbusath5@gmail.com
"""
import numpy as np
import pandas as pd
import turbodbc
import re
from splycer.pairs_set import PairsDB, PairsCOO


class BlockDB(PairsDB):
    """Here's a prototype of a clever way of blocking that I came up with.
       The idea is that the blocks parameter of the block() function is a list
       of different blocks. For example, blocks could be
       [["first_soundex", "birth_year <= 4"], ["last_soundex", "bp"]]. ["first_soundex", "birth_year"]
       is a block, meaning if two records are compared, they must match on both
       of those variables. A compare can show up if it satisfies either of the blocks.
       Be warned, I haven't tested this.
    """

    def __init__(self, record_id1, record_id2, table_name, dsn_str, idx_cols, table1, table2):
        self.conn = turbodbc.connect(dsn=dsn_str)
        self.cursor = self.conn.cursor()

        self.table_name = table_name
        self.idx_cols = idx_cols
        if not self.table_exists(table_name):
            self.create_table()

        super().__init__(record_id1, record_id2, table_name, dsn_str, idx_cols)

        self.table1 = table1
        self.table2 = table2
        self.extra_joins = None
        self.blocks = None

    def table_exists(self, table_name):
        # check if a table exists in our sql database
        return self.cursor.execute(f"if object_id('dbo.{table_name}', 'U') is not null select 1 else select 0").fetchone()[0]

    def create_table(self):
        # create table that blocked indices will be uploaded to
        sql_str = f'''CREATE TABLE {self.table_name} 
        ({self.idx_cols[0]} int not null, 
        {self.idx_cols[1]} int not null, 
        CONSTRAINT pk_{self.table_name} PRIMARY KEY ({self.idx_cols[0]},{self.idx_cols[1]}))'''
        self.cursor.execute(sql_str)
        self.conn.commit()

    def set_extra_joins(self, extra_joins):
        # error handling
        if self.blocks is None:
            raise ValueError('You must set blocks before you can set extra_joins')

        if len(extra_joins) != len(self.blocks):
            raise AssertionError(f'extra_joins length must be the same as block length ({len(extra_joins)} != {len(blocks)})')

        # format joins into proper format for SQL where clause
        self.extra_joins = [' and ' + extra_join for extra_join in extra_joins]
        self.extra_joins = [extra_join + existing_join for extra_join, existing_join in zip(extra_joins,self.extra_joins)]

    def set_blocks(self, blocks):
        # instantiate extra_joins
        self.extra_joins = ['']*len(blocks)

        # search for birth year blocks
        # format birth year block as a sql-ready extra join to be placed in where clause
        for i, block in enumerate(blocks):
            for j, feature in enumerate(block):
                if 'birth_year' in feature:
                    bound = re.findall(r'\d+|$', feature)[0]
                    self.extra_joins[i] += f' and ABS(t1.birth_year-t2.birth_year)<={bound}'
                    del blocks[i][j]
        self.blocks = blocks

    def get_row_count(self, table_name):
        # efficient row_count getter
        sql_str = f"""
        SELECT count(*) FROM {table_name}
        """
        return pd.read_sql(sql_str, self.conn).values[0][0]

    def block(self, chunksize=1000000, print_query=False):
        # Execute a block for a pair of years and a chosen sample
        # Create temp table storing index IDs with order variable
        # this allows us to have a chunksize blocking option

        if self.table_exists('training_temp'):
            self.cursor.execute('drop table training_temp')

        sql_str = f'''
        Create Table training_temp(
            ID      int identity(1,1),
            [index] int not null,
            CONSTRAINT PK_training_temp PRIMARY KEY ([index])
            )
        '''
        print('creating temp SQL table...')
        self.cursor.execute(sql_str)
        self.conn.commit()

        # insert table1 index data into temp table
        print(f'uploading {self.table1} indices to temp table...')
        self.cursor.execute(f'INSERT INTO training_temp ([index]) SELECT [index] FROM {self.table1}')
        self.conn.commit()

        row_count = self.get_row_count('training_temp')
        print(f'total indices to block: {row_count}')
        for batch_min in range(0, row_count, chunksize):
            batch_max = batch_min + chunksize
            print(f'blocking from {batch_min} to {batch_max}...')
            sql_str = f"INSERT INTO {self.table_name}"
            for block, extra_join in zip(self.blocks, self.extra_joins):
                sql_str += f"""
                               SELECT t1.[index] as {self.idx_cols[0]}, t2.[index] as {self.idx_cols[1]}
                               FROM {self.table1} as t1
                               LEFT JOIN training_temp as train
                                   ON train.[index] = t1.[index] 
                               LEFT JOIN {self.table2} as t2 on """
                for block_var in block:
                    sql_str += f"t1.{block_var} = t2.{block_var} and "
                sql_str = sql_str[:-4] + f'''WHERE t2.[index] is not null 
                                                and train.ID < {batch_max} 
                                                and train.ID > {batch_min}
                                                and t1.[index] is not null {extra_join} UNION '''
            sql_str = sql_str[:-7]
            # FIXME: below line gives primary key error, don't know why
            #sql_str += ' ORDER BY t1.[index]'
            if print_query:
                print(sql_str)

            self.cursor.execute(sql_str)


        #self.cursor.execute('DROP TABLE training_temp')
        self.conn.commit()


def block_dataframe(record_df1, record_df2, blocks):
    pairs = []
    index_name1 = record_df1.index.name
    index_name2 = record_df2.index.name
    record_df1 = record_df1.reset_index().rename({index_name1: f"index_{record_df1.record_id}"})
    record_df2 = record_df2.reset_index().rename({index_name2: f"index_{record_df2.record_id}"})
    for b in blocks:
        pairs.append(pd.merge(record_df1, record_df2, how="left", on=blocks)[
                         [f"index_{record_df1.record_id}", f"index_{record_df2.record_id}"]])
    pairs = pd.concat(pairs).drop_duplicates()
    return PairsCOO(record_df1.record_id, record_df2.record_id,
                    pairs["index_{record_df1.record_id}"].values,
                    pairs["index_{record_df2.record_id}"].values,
                    np.ones(pairs.shape[0]))


if __name__ == '__main__':
    bdb = BlockDB(1920, 1930, 'training_indices_1920_1930_test', 'rec_db',
                  ['index1920', 'index1930'], 'compact1920_census', 'compact1930_census')

    blocks = [['birth_year < 3', 'mbp', 'fbp', 'race', 'female', 'bp', 'first_sdxn', 'last_sdxn','first_init'],['birth_year < 3', 'county', 'race', 'female', 'bp', 'first_sdxn', 'last_sdxn','first_init']]
    bdb.set_blocks(blocks)
    bdb.block(1000000,print_query=True)