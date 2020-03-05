# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 13:19:27 2019

This code compiles a census
"""
import turbodbc
import pandas as pd

class CensusCompiler():
    def __init__(self,year,dsn_str):
        self.conn = turbodbc.connect(dsn=dsn_str)
        self.cursor = self.conn.cursor()
        self.year=year
        self.table_name=f'compiled_{year}'
        self.table_len=self.get_row_count(f'compact{self.year}_census')
        
        table_exists = self.cursor.execute(f"if object_id('dbo.{self.table_name}', 'U') is not null select 1 else select 0").fetchone()[0]
        if not table_exists:
            self.create_table()
            
    def create_table(self):
        '''
        Create table that compiled data will be inserted into
        '''
        sql_str=f'''
        create table {self.table_name}(
            [index]         int not null primary key,
            marstat      [smallint],
            birth_year    [smallint],
            household     [int],
            immigration   [smallint],
            race          [smallint],
            rel           [smallint],
            female        [smallint],
            bp            [smallint],
            mbp           [smallint],
            fbp           [smallint],
            state        [smallint],
            county        [smallint],
            cohort1       [smallint],
            cohort2       [smallint],
            last_sdxn     [smallint],
            first_sdxn    [smallint],
            last_init     [smallint],
            first_init    [smallint],
            bp_comm       [int],
            first_comm    [float],
            last_comm     [float],
            first         nvarchar(75),
            last          nvarchar(90),
            bp_lat        [float],
            bp_lon        [float],
            res_lat       [float],
            res_lon       [float],
            full_name     nvarchar(75)
            )
        '''
        self.cursor.execute(sql_str)
        self.conn.commit()
        
    def get_row_count(self,table_name):
        # efficient row_count getter 
        sql_str=f"""
        SELECT CONVERT(bigint, rows)
        FROM sysindexes
        WHERE id = OBJECT_ID('{table_name}')
        AND indid < 2
        """
        return pd.read_sql(sql_str,self.conn).values[0][0]
    
    def compile_census(self,chunksize=10000000):
        '''
        Compiles census data for use in splycer feature engineer
        '''
        for chunk_start in range(0,self.table_len,chunksize):
            chunk_end=chunk_start + chunksize
            print(f'''compiling rows {chunk_start} to {chunk_end}...''')
            sql_str=f'''
            INSERT INTO {self.table_name}
            SELECT
              census.[index],
              census.marstat,
              census.birth_year,
              census.household,
              census.immigration,
              census.race,
              census.rel,
              census.female,
              census.bp,
              census.mbp,
              census.fbp,
              census.state,
              census.county,
              census.cohort1,
              census.cohort2,
              census.last_sdxn,
              census.first_sdxn,
              census.last_init,
              census.first_init,
              bp_comm.bp_comm_1910 as bp_comm,
              f_comm.first_comm,
              l_comm.last_comm,
              snames.first,
              snames.last,
              g_bp.bp_lat as bp_lat,
              g_bp.bp_lon as bp_lon,
              gr.res_lat as res_lat,
              gr.res_lon as res_lon,
              nickname.full_name as full_name
            FROM
              compact{self.year}_census AS census
              LEFT JOIN compact{self.year}_stringnames as snames ON census.[index] = snames.[index]
              LEFT JOIN compact{self.year}_place as place on census.[index] = place.[index]
              LEFT JOIN compact{self.year}_first_comm as f_comm on snames.first COLLATE SQL_Latin1_General_CP1_CI_AS = f_comm.first COLLATE SQL_Latin1_General_CP1_CI_AS
              LEFT JOIN compact{self.year}_last_comm as l_comm on snames.last COLLATE SQL_Latin1_General_CP1_CI_AS = l_comm.last COLLATE SQL_Latin1_General_CP1_CI_AS
              LEFT JOIN crosswalk_geo_bplace as g_bp on census.bp = g_bp.int_place
              LEFT JOIN crosswalk_geo_res as gr on place.township = gr.city
              and place.county = gr.county
              and place.state = gr.state
              LEFT JOIN crosswalk_nickname as nickname on snames.first COLLATE SQL_Latin1_General_CP1_CI_AS  = nickname.nickname COLLATE SQL_Latin1_General_CP1_CI_AS 
              LEFT JOIN compact1910_bplace_comm as bp_comm on census.bp = bp_comm.int_place
            WHERE
              census.[index] >= {chunk_start}
              and census.[index] < {chunk_end}
              '''
            self.cursor.execute(sql_str)
            self.conn.commit()
        print('Finished!')

if __name__=='__main__':
    cc=CensusCompiler(1930,'rec_db')
    cc.compile_census()
        
        
            