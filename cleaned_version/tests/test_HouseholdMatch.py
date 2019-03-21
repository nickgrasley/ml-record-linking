# -*- coding: utf-8 -*-
"""
This file runs unit test for the file HouseholdMatch.py in the 
family search class.

Author: Ben Busath
March 20, 2019
"""

import os

def test_init(obj, *args):
    assert obj.df1 == args[0], 'census frame 1 not properly assigned'
    assert obj.df2 == args[1], 'census frame 2 not properly assigned'
    assert obj.anchors == range(args[2].shape[0]), 'anchors not properly assigned'
    assert obj.year1 == args[3], 'year 1 not properly assigned'
    assert obj.year2 == args[4], 'year 2 not properly assigned'
    
    if args[5]:
        assert obj.stepsize==args[5][1]-args[5][0],'stepsize not properly assigned'
    else:
        assert obj.stepsize==None, 'stepsize is not None'
        assert obj.iterlist==[(0,args[2].shape[0]+1)], 'iterlist not properly assigned (no iterlist given)'
    
    
    
if __name__=='__main__':
    exec(open(r'R:\JoePriceResearch\record_linking\projects\deep_learning\ml-record-linking\cleaned_version/HouseholdMatch.py').read())
    
    df1=pd.DataFrame(daa)
    run
    classif=HouseholdByAnchors()
    




