# -*- coding: utf-8 -*-
"""
Created on Wed Feb 20 16:35:37 2019

@author: ngrasley
"""
import pandas as pd
import dask.dataframe as dd
from candidate_creator import makePairs
from run_linking import run_linking

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

CEN_DIREC = "R:/JoePriceResearch/record_linking/data/census_compact"
PAIR_FILE = "R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/data/pairs_ben"
MODEL_PKL = "R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/models/xgboost_more_features_ben.p"
CEN_OUT_FILE = "R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/data/temp_ben.dta"
def main(chunk_index):
    census1 = dd.from_pandas(pd.read_stata(f"{CEN_DIREC}/1910/census1910.dta"), npartitions=100)
    census2 = pd.read_stata(f"{CEN_DIREC}/1920/census1920.dta", chunksize=100000)
    census2._lines_read = chunk_index * 100000
    census2 = dd.from_pandas(census2.get_chunk(), npartitions=100)
    binlists = [['cohort1', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
				['cohort2', 'bp', 'first_sdxn', 'last_sdxn', 'mbp', 'race', 'state'],
				['cohort2', 'fbp', 'female', 'first_sdxn', 'last_sdxn', 'mbp', 'state'],
				['cohort1','bp','female','first_init','last_sdxn','mbp','race','state']]
    makePairs(census1, census2, "1910", "1920", binlists).to_stata(f"{PAIR_FILE}_{chunk_index}.dta")
    run_linking("True", MODEL_PKL, f"{PAIR_FILE}_{chunk_index}.dta", CEN_OUT_FILE, None)
    return 0

if __name__ == "__main__":
    main(0)