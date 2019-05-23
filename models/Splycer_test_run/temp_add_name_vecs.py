# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 16:07:27 2019

@author: ngrasley
"""

import sys
sys.path.append("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/Splycer")
from Splycer.CensusCompiler import CensusCompiler
from Splycer.preprocessing import EuclideanDistance, DropVars
import dask.dataframe as dd


test = dd.read_parquet("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/models/Splycer_test_run/temp2")
census_compiler = CensusCompiler()
test = test.set_index("first1910", drop=False)
test = census_compiler.add_name_vectors(test, "1910")
test = test.set_index("first1920", drop=False)
test = census_compiler.add_name_vectors(test, "1920")

euclid = EuclideanDistance(variables=[[f"first_vec{i}" for i in range(200)], [f"last_vec{i}" for i in range(200)]], new_cols=["first_vec_dist", "last_vec_dist"])
test = euclid.transform(test)
cols = [f"first_vec{i}" for i in range(200)]
cols.extend([f"last_vec{i}" for i in range(200)])
drop = DropVars(cols_to_drop=cols, both_years=True)
test = drop.transform(test)
test = test.persist()