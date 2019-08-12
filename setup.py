#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 15:46:53 2019

@author: thegrasley
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

ext_modules=[
             Extension("splycer.base", ["splycer/base.pyx"]),
             Extension("splycer.comparisons", ["splycer/comparisons.pyx"]),
             Extension("splycer.feature_engineer", ["splycer/feature_engineer.pyx"]),
             Extension("splycer.pairs_set", ["splycer/pairs_set.pyx"]),
             Extension("splycer.record_set", ["splycer/record_set.pyx"]),
             Extension("splycer.xgboost_match", ["splycer/xgboost_match.pyx"])]

setup(name="splycer",
      version="0.1.2",
      packages=["splycer",],
      ext_modules=cythonize(ext_modules),
      include_dirs = [np.get_include()],
      long_description=open("README.md").read()
)