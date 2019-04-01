# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 16:35:07 2019

@author: ngrasley
"""

from Splycer.CensusCompiler import CensusCompiler
from Splycer.stata_dask import dask_read_stata_delayed_group

test_data = dask_read_stata_delayed_group(["R:/JoePriceResearch/record_linking/data/census_tree/training_data/training.dta"])
test_data = test_data.loc[1:10000,:]
census_compiler = CensusCompiler()
test_data = census_compiler.add_census(test_data, "1910")
test_data = census_compiler.add_bplace_comm(test_data, "1910")
test_data = census_compiler.add_bplace_geo(test_data, "1910")

def load_test_data():
    return None

def test_add_census(test_in, test_out):
    test_in = census_compiler.add_census(test_in, "1910")
    test_in = census_compiler.add_census(test_in, "1920")
    assert test_in.columns == test_out.columns, f"For columns of add_census, expected {test_out.columns}, got {test_in.columns}"
    assert test_in.shape == test_out.shape, f"For shape of add_census, expected {test_out.shape}. got {test_in.shape}"
    assert test_in.values == test_out.values, f"Incorrect values after running add_census"
    
def test_add_res_geo(test_in, test_out):
    test_in = census_compiler.add_res_geo(test_in, "1910")
    test_in = census_compiler.add_res_geo(test_in, "1920")
    assert test_in.columns == test_out.columns, f"For columns of add_res_geo, expected {test_out.columns}, got {test_in.columns}"
    assert test_in.shape == test_out.shape, f"For shape of add_res_geo, expected {test_out.shape}. got {test_in.shape}"
    assert test_in.values == test_out.values, f"Incorrect values after running add_res_geo"
    
def test_add_bplace_comm(test_in, test_out):
    test_in = census_compiler.add_bplace_comm(test_in, "1910")
    test_in = census_compiler.add_bplace_comm(test_in, "1920")
    assert test_in.columns == test_out.columns, f"For columns of add_bplace_geo, expected {test_out.columns}, got {test_in.columns}"
    assert test_in.shape == test_out.shape, f"For shape of add_bplace_geo, expected {test_out.shape}. got {test_in.shape}"
    assert test_in.values == test_out.values, f"Incorrect values after running add_bplace_geo"
    
def test_add_bplace_geo(test_in, test_out):
    test_in = census_compiler.add_bplace_geo(test_in, "1910")
    test_in = census_compiler.add_bplace_comm(test_in, "1920")
    assert test_in.columns == test_out.columns, f"For columns of add_bplace_geo, expected {test_out.columns}, got {test_in.columns}"
    assert test_in.shape == test_out.shape, f"For shape of add_bplace_geo, expected {test_out.shape}. got {test_in.shape}"
    assert test_in.values == test_out.values, f"Incorrect values after running add_bplace_geo"