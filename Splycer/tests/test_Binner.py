"""
This file runs unit test for the file Binner.py in the 
family search class.

Author: Ben Branchflower
Created: 19 March 2019
Last edited: 19 March 2019

Notes:
19 mar - Just getting started
21 
"""

# importing the functions to be tested
from Splycer.Binner import bins
from Splycer.Binner import Binner


def test_bins():
    assert bins() == [['cohort1','bp','county','fbp','female','first_init','last_sdxn','race'],
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
               
    
def test_init(obj, *args):
    assert obj.bins == args[0], 'bins not properly assigned'
    assert obj.years == args[1], 'years not properly assigned'
    assert obj.chunk_size == args[2], 'chunk_size not properly assigned'
    assert obj.chunk_num == args[3], 'chunk_num not properly assigned'
    assert obj.census1 is None, 'census1 is not None'
    assert obj.census2 is None, 'census2 is not None'
    assert obj.time_taken == -1, 'time taken is not -1'
    
  
def test_load_data():
    return None


def test_delete_data():
    return None
    

def test_filter_unmatchables():
    return None
    
    
def test_get_arks():
    return None
    
    
def test_make_pairs():
    return None
    
    
def test_fit():
    return None
    
    
def test_transform():
    return None
    
    
    
if __name__ == '__main__':
    # defining the functions
    # exec(open('../Binner.py').read())
    
    # running the tests
    test_bins()    
    
    test_args = [(bins(),['1910','1920'],100000,0),
                 ([['cohort','female','mbp'],['female','first_sdxn','state']],
                      ['1880','1940'],100,5),
                 (bins(),['1900','1880'],10000000000,200000)]
    for x in test_args:
        _binner = Binner(x[0],x[1],x[2],x[3])
        test_init(_binner, *x)