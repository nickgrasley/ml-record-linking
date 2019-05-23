# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 12:58:10 2019

@author: iriley
"""


import pandas as pd
import dask.dataframe as dd
#from preprocessing.stata_dask import dask_read_stata_delayed_group
from time import time

binlists = [['cohort1','bp','county','fbp','female','first_init','last_sdxn','race'],
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

def filterUnmatchables(df, isyear2, year1, year2):
    if isyear2:
        df =  df[(df['immigration']<int(year1))|(df['immigration'].isna())]
    keep = (df['marstat']==0)|(df['female']==0)
    return df[keep]


def getArks(df, year1, year2):
    #index_pairs = index_pairs.rename(columns = {})
    cw = pd.read_stata('R:/JoePriceResearch/record_linking/data/census_compact/{0}/ind_ark{0}.dta'.format(year1))
    df = pd.merge(df, cw, how='inner', on='index'+year1)
    del cw
    cw = pd.read_stata('R:/JoePriceResearch/record_linking/data/census_compact/{0}/ind_ark{0}.dta'.format(year2))
    df = pd.merge(df, cw, how='inner', on='index'+year2)
    del cw
    df = df[['ark'+year1,'ark'+year2,'index'+year1,'index'+year2]]
    return df

def createBins(df1, df2, year1, year2, binlist):
    return pd.merge(df1, df2, on=binlist, how='inner')[[f'index{year1}', f'index{year2}']].drop_duplicates()

def makePairs(df1, df2, year1, year2, binlists):
    """
    Parameters:
        df1: census1
        df2: census2
    Returns:
        Candidate pairs file
    """
    start = time()
    print(df1.shape[0], df2.shape[0])
    print('Filtering unmatchables...')
    df1 = filterUnmatchables(df1, False, year1, year2).rename(columns={'index':'index'+year1})
    df2 = filterUnmatchables(df2, True, year1, year2).rename(columns={'index':'index'+year2})
    print(df1.shape[0], df2.shape[0])
    # use blockMergeCap, blockMerge, tightenList
    outpairs = pd.DataFrame(columns=['index'+year1, 'index'+year2])
    #problems1, problems2 = set([]), set([])
    print('Binning...')
    chunky_bins = []
    for b in binlists:
        b.sort()
        list_tracker = []
        if b not in list_tracker:
            new = dd.merge(df1, df2, on=b, how='inner')[['index'+year1, 'index'+year2]]
            #outpairs = outpairs.append(new).drop_duplicates()
            chunky_bins.append(new)
            list_tracker.append(b)
    del df1, df2
    outpairs = pd.concat(chunky_bins).drop_duplicates()
    print('Concatenating...')
    for c in outpairs.columns:
        outpairs[c] = outpairs[c].astype(int)
    print('Getting arks...')
    outpairs = getArks(outpairs, year1, year2)
    end = time()
    print(f'time: {end - start}')
    return outpairs


