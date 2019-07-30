#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 15:46:53 2019

@author: thegrasley
"""

from distutils.core import setup
<<<<<<< HEAD
from Cython.Build import cythonize
import numpy as np
=======
>>>>>>> parent of d8776e3... working code

setup(name="Splycer",
      version="0.1dev",
      packages=["Splycer",],
<<<<<<< HEAD
      ext_modules=cythonize("Splycer/*.pyx"),
      include_dirs=[np.get_include()],
=======
>>>>>>> parent of d8776e3... working code
      long_description=open("README.md").read()
)