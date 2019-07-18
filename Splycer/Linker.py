#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 20:17:51 2019

@author: thegrasley
"""
import abc


class Linker(object, metaclass=abc.ABCMeta):
    def __init__(self, recordset1, recordset2, compareset):
        self.recordset1 = recordset1
        self.recordset2 = recordset2
        self.compareset = compareset
        
    @abc.abstractmethod
    def is_link(self, candidate_pair):
        pass
    
    @abc.abstractmethod
    def run(self, outfile):
        pass
    
    @abc.abstractmethod
    def save(self):
        pass
    
    @abc.abstractmethod
    def load(self):
        pass