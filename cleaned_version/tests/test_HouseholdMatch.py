# -*- coding: utf-8 -*-
"""
This file runs unit test for the file HouseholdMatch.py in the 
family search class.

Author: Ben Busath
March 20, 2019
"""

import os
import pandas as pd
import numpy as np



def test_init(obj, *args):
    
    assert obj.df1.equals(args[0]), 'census frame 1 not properly assigned'
    assert obj.df2.equals(args[1]), 'census frame 2 not properly assigned'
    assert obj.anchors.equals(args[2]),'anchors not properly assigned'
    assert obj.year1 == args[3], 'year 1 not properly assigned'
    assert obj.year2 == args[4], 'year 2 not properly assigned'
    
    if len(args)>5:
        assert obj.iterlist==args[5], 'given iterlist not properly assigned'
        assert obj.stepsize==args[5][1]-args[5][0],'stepsize not properly assigned'
    else:
        assert obj.stepsize==None, 'stepsize is not None'
        assert obj.iterlist==[(0,args[2].shape[0]+1)], 'iterlist not properly assigned (no iterlist given)'
    
    

def test_make_hhdict(obj, df):
    assert obj.make_hhdict(df)=={k:{i:{c:v[c][i] for c in v.columns} for i in v['index']} for \
                k,v in dict(list(df[compact_cols].groupby('household'))).items()}, 'nested household dictionary not properly created'
    
    
    
#def test_fish_for_pairs():

#def test_filter_households_on():

#def test_evalutate():

#def test_run():
    


if __name__=='__main__':
    exec(open(r'R:\JoePriceResearch\record_linking\projects\deep_learning\ml-record-linking\cleaned_version\HouseholdMatch.py').read())
    
    compact_cols=['index','household','rel','female','bp','mbp','fbp','race','first_sdxn','last_sdxn','cohort1','cohort2']
    
    test_args1=[
            [(pd.DataFrame({'col1':[1,2]})),(pd.DataFrame({'col1':[2,3]})),(pd.DataFrame({'col1':[5,6],'col2':[7,8]})),1920,1940],
            [(pd.DataFrame({'col1':[1,2]})),(pd.DataFrame({'col1':[2,3]})),(pd.DataFrame({'col1':[5,6],'col2':[7,8]})),1920,1940,[0,1]]
            ]
    
    
    #test arguments for make_hhdict 
    test_args2=[
            pd.DataFrame(data=np.array([[x for x in range(len(compact_cols))]]),columns=compact_cols)
            ]


    for x in test_args1:  
        hhmatch=HouseholdByAnchors(*x)
        test_init(hhmatch,*x)
        test_make_hhdict(hhmatch,*test_args2)
        
      
 
      
        
    


   
    