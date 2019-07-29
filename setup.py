#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 15:46:53 2019

@author: thegrasley
"""

from distutils.core import setup
from Cython.Build import cythonize
import numpy as np

setup(name="Splycer",
      version="0.1dev",
      packages=["Splycer",],
      ext_modules=cythonize("Splycer/*.pyx"),
      include_dirs=[np.get_include()],
      long_description=open("README.md").read()
)