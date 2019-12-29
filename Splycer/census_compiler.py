# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 13:19:27 2019

@author: bbusa
"""
#import turbodbc

class CensusCompiler():
    def __init__(self,year,dsn_str):
 #       self.conn = turbodbc.connect(dsn=dsn_str)
  #      self.cursor = self.conn.cursor()
        self.year=year
        self.table_name=f'compiled_{year}'
        
    def create_table(self):
        '''
        Create table that compiled data will be inserted into
        '''
        sql_str=f'''
        create table {self.table_name}(
            index         int not null primary key,
            marstat      smallint,
            birth_year    smallint,
            household     int,
            immigration   smallint,
            race          smallint,
            rel           smallint,
            female        smallint,
            bp            smalllint,
            mbp           smallint,
            fbp           smallint,
            state        smallint,
            county        smallint,
            cohort1       smallint,
            cohort2       smallint,
            last_sdxn     smallint,
            first_sdxn    smallint,
            last_init     smallint,
            first_init    smallint,
            bp_comm       int,
            first_comm    float,
            last_comm     float,
            first         nvarchar(50),
            last          nvarchar(90),
            bp_lat        float,
            bp_lon        float,
            res_lat       float,
            res_lon       float,
            full_name     nvarchar(50)
            )
        '''
        self.cursor.execute(sql_str)
        self.conn.commit()
        
    def compile_census(self,chunksize=10000000):
        #self.create_table()
        chunk_start=0
        chunk_end=1000
        sql_str=f'''
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
          nicknames.full_name as full_name
        FROM
          compact{self.year}_census AS census
          LEFT JOIN compact{self.year}_stringnames as snames ON census.[index] = snames.[index]
          LEFT JOIN compact{self.year}_place as place on census.[index] = place.[index]
          LEFT JOIN compact{self.year}_first_comm as f_comm on snames.first = f_comm.first
          LEFT JOIN compact{self.year}_last_comm as l_comm on snames.last = l_comm.last
          LEFT JOIN crosswalk_geo_bplace as g_bp on census.bp = g_bp.int_place
          LEFT JOIN crosswalk_geo_res as gr on place.township = gr.city
          and place.county = gr.county
          and place.state = gr.state
          LEFT JOIN crosswalk_nickname as nickname on snames.first = nickname.nickname
          LEFT JOIN compact1910_bplace_comm as bp_comm on census.bp = bp_comm.int_place
        WHERE
          census.[index] >= 0
          and census.[index] < 1000
  
          '''
          
        print(sql_str)
        
if __name__=='__main__':
    cc=CensusCompiler(1930,'howdy')
    cc.compile_census()
        
        
            