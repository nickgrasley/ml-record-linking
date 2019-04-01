# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:07 2019

@author: ngrasley
"""
from sklearn.base import BaseEstimator, TransformerMixin
import sys
import pandas as pd
import dask.dataframe as dd
sys.path.append("R:/JoePriceReseearch/record_linking/projects/deep_learning/ml-record-linking/preprocessing")
from stata_dask import dask_read_stata_delayed_group
from time import time

def bins():
    return [['cohort1','bp','county','fbp','female','first_init','last_sdxn','race'],
            ['cohort1','bp','county','fbp','female','first_sdxn','last_init','mbp'],
            ['cohort1','bp','county','fbp','female','first_sdxn','last_init','race'],
             ['cohort1', 'bp', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn'],
             ['cohort1', 'bp', 'county', 'fbp', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort1','bp','county','female','first_init','last_sdxn','mbp','race'],
             ['cohort1','bp','county','female','first_sdxn','last_init','mbp','race'],
             ['cohort1', 'bp', 'county', 'female', 'first_sdxn', 'last_sdxn', 'mbp'],
             ['cohort1', 'bp', 'county', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort1', 'bp', 'county', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1','bp','fbp','female','first_init','last_sdxn','race','state'],
             ['cohort1', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort1','bp','fbp','female','first_sdxn','last_sdxn','race','state'],
             ['cohort1', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'state'],
             ['cohort1', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1','bp','female','first_init','last_sdxn','mbp','race','state'],
             ['cohort1', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
             ['cohort1', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'race', 'state'],
             ['cohort1', 'bp', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
             ['cohort1','county','fbp','female','first_init','last_sdxn','mbp','race'],
             ['cohort1','county','fbp','female','first_sdxn','last_init','mbp','race'],
             ['cohort1', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp'],
             ['cohort1', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort1', 'county', 'fbp', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1', 'county', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1','fbp','female','first_init','last_sdxn','mbp','race','state'],
             ['cohort1', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort1', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
             ['cohort1', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race', 'state'],
             ['cohort1', 'fbp', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
             ['cohort1', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
             ['cohort2','bp','county','fbp','female','first_init','last_sdxn','race'],
             ['cohort2','bp','county','fbp','female','first_sdxn','last_init','mbp'],
             ['cohort2','bp','county','fbp','female','first_sdxn','last_init','race'],
             ['cohort2', 'bp', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn'],
             ['cohort2', 'bp', 'county', 'fbp', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort2','bp','county','female','first_init','last_sdxn','mbp','race'],
             ['cohort2','bp','county','female','first_sdxn','last_init','mbp','race'],
             ['cohort2', 'bp', 'county', 'female', 'first_sdxn', 'last_sdxn', 'mbp'],
             ['cohort2', 'bp', 'county', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort2', 'bp', 'county', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2','bp','fbp','female','first_init','last_sdxn','race','state'],
             ['cohort2', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort2','bp','fbp','female','first_sdxn','last_sdxn','race','state'],
             ['cohort2', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'state'],
             ['cohort2', 'bp', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2','bp','female','first_init','last_sdxn','mbp','race','state'],
             ['cohort2', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
             ['cohort2', 'bp', 'female', 'first_sdxn', 'last_sdxn', 'race', 'state'],
             ['cohort2', 'bp', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
             ['cohort2','county','fbp','female','first_init','last_sdxn','mbp','race'],
             ['cohort2','county','fbp','female','first_sdxn','last_init','mbp','race'],
             ['cohort2', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp'],
             ['cohort2', 'county', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race'],
             ['cohort2', 'county', 'fbp', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2', 'county', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2','fbp','female','first_init','last_sdxn','mbp','race','state'],
             ['cohort2', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race'],
             ['cohort2', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
             ['cohort2', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'race', 'state'],
             ['cohort2', 'fbp', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
             ['cohort2', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state']]

class Binner(BaseEstimator, TransformerMixin):
    """This handles creating candidate pairs from the compact census. Isaac Riley 
       created the functions, so contact him if you have questions.
    """
    def __init__(self, bins=bins(), years=["1910", "1920"], chunk_size=100000, chunk_num=0):
        self.bins = bins
        self.years = years
        self.chunk_size = chunk_size
        self.chunk_num = chunk_num
        self.census1 = f"R:/JoePriceResearch/record_linking/data/census_compact/{years[0]}/census{years[0]}.dta"
        self.census2 = f"R:/JoePriceResearch/record_linking/data/census_compact/{years[1]}/census{years[1]}.dta"
        self.time_taken = -1
    
    def load_data_(self):
        self.census1 = dask_read_stata_delayed_group([self.census1])
        df_tmp = pd.read_stata(self.census2, chunksize=self.chunk_size)
        df_tmp._lines_read = self.chunk_num * self.chunk_size
        self.census2 = dd.from_pandas(df_tmp.get_chunk(), chunksize=25000)
        return
        
    def delete_data_(self):
        del self.census1, self.census2
        return
        
    def filterUnmatchables(self, df, isyear2):
        if isyear2:
            df =  df[(df['immigration']<int(self.years[0]))|(df['immigration'].isna())]
        keep = (df['marstat']==0)|(df['female']==0)
        return df[keep].persist()
    
    def getArks(self, df):
        #index_pairs = index_pairs.rename(columns = {})
        cw = dask_read_stata_delayed_group(['R:/JoePriceResearch/record_linking/data/census_compact/{0}/int_ark{0}.dta'.format(self.years[0])])
        df = dd.merge(df, cw, how='inner', on='index'+self.years[0])
        del cw
        cw = dask_read_stata_delayed_group(['R:/JoePriceResearch/record_linking/data/census_compact/{0}/int_ark{0}.dta'.format(self.years[1])])
        df = dd.merge(df, cw, how='inner', on='index'+self.years[1])
        del cw
        df = df[['ark'+self.years[0],'ark'+self.years[1],'index'+self.years[0],'index'+self.years[1]]]
        return df.persist()
    
    def makePairs(self):
        """
        Parameters:
            df1: census1
            df2: census2
        Returns:
            Candidate pairs file
        """
        start = time()
        self.load_data_()
        print(self.census1.shape[0], self.census2.shape[0])
        print('Filtering unmatchables...')
        self.census1 = self.filterUnmatchables(self.census1, False).rename(columns={'index':'index'+self.years[0]})
        self.census2 = self.filterUnmatchables(self.census2, True).rename(columns={'index':'index'+self.years[1]})
        print(self.census1.shape[0], self.census2.shape[0])
        # use blockMergeCap, blockMerge, tightenList
        outpairs = dd.from_pandas(pd.DataFrame(columns=['index'+self.years[0], 'index'+self.years[1]]), npartitions=100)
        #problems1, problems2 = set([]), set([])
        print('Binning...')
        chunky_bins = []
        for b in self.bins:
            b.sort()
            list_tracker = []
            if b not in list_tracker:
                new = dd.merge(self.census1, self.census2, on=b, how='inner')[['index'+self.year[0], 'index'+self.year[1]]]
                #outpairs = outpairs.append(new).drop_duplicates()
                chunky_bins.append(new)
                list_tracker.append(b)
        outpairs = dd.concat(chunky_bins, interleave_partitions=True).drop_duplicates() #FIXME check if interleave_partitions=True gives what we want. It gives an error otherwise.
        print('Concatenating...')
        outpairs.persist()
        for c in outpairs.columns:
            outpairs[c] = outpairs[c].astype(int)
        print('Getting arks...')
        outpairs = self.getArks(outpairs)
        end = time()
        print(f'time: {end - start}')
        self.time_taken = end - start
        self.delete_data_()
        return outpairs

    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        return self.makePairs()