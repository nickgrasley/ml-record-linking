#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 20:13:29 2019

@author: Nick Grasley (ngrasley@stanford.edu)
"""
import abc

class RecordBase(metaclass=abc.ABCMeta):
    """Abstract container for any record set. Other classes
       expect some way to query records using get_record and/or
       __getitem__().
    """
    @abc.abstractmethod
    def get_record(self, uid, var_list=None):
        """Query record info from its unique identifier (uid).
           Record must be return as a Pandas Series/DataFrame.
        """
    @abc.abstractmethod
    def __getitem__(self, uid):
        pass

class CompareBase(metaclass=abc.ABCMeta):
    """Abstract container for the comparisons between two record
       sets.
    """
    def __init__(self, ncompares):
        self.ncompares = ncompares
    @abc.abstractmethod
    def __iter__(self):
        pass
    @abc.abstractmethod
    def __getitem__(self, uids):
        pass
    @abc.abstractmethod
    def get_pairs(self):
        """Query all record pairs and create a generator object."""

class FeatureBase(metaclass=abc.ABCMeta):
    """Abstract class that handles how records are compared."""
    @abc.abstractmethod
    def compare(self, uid1, uid2):
        """Compare two uids."""

    @abc.abstractmethod
    def save(self, path):
        """Save object."""

    @abc.abstractmethod
    def load(self, path):
        """Load object."""

class LinkerBase(metaclass=abc.ABCMeta):
    """Abstract class for defining how to structure a linking
       algorithm into a Python object
    """
    def __init__(self, recordset1, recordset2, compareset):
        self.recordset1 = recordset1
        self.recordset2 = recordset2
        self.compareset = compareset

    @abc.abstractmethod
    def is_link(self, candidate_pair):
        """Given a pair of uids, return True/False for whether the pair is a link"""

    @abc.abstractmethod
    def run(self, outfile):
        """Run the entire model from start to finish."""

    @abc.abstractmethod
    def save(self):
        """Save the model."""

    @abc.abstractmethod
    def load(self):
        """Load a model"""
