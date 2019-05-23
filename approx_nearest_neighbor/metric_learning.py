# -*- coding: utf-8 -*-
"""
Created on Fri May 17 13:58:25 2019

@author: ngrasley

calculate weights for each feature of the metric space. Using the weights W and
feature vector x, calculate new vector W^(1/2) @ x.
"""

import numpy as np
from scipy.spatial.distance import minkowski
from scipy.optimize import fsolve

def dist_metric(x, y, A):
    return minkowski(x, y, w=A)

dist_metric = np.vectorize(dist_metric)

def opt_func(A, S, D):
    return np.sum(dist_metric(S[:,0], S[:,1])**2, A) - np.log(np.sum(dist_metric(D[:,0], D[:,1], A)))


def main():
    A0 = np.ones(20) #FIXME choose the right dimensions
    S = np.loadtxt("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/approx_nearest_neighbor/true_matches.csv") #FIXME add true matches here
    D = np.loadtxt("R:/JoePriceResearch/record_linking/projects/deep_learning/ml-record-linking/approx_nearest_neighbor/false_matches.csv") #FIXME add false matches here 
    return fsolve(opt_func, A0, args=(S, D,)) #FIXME ensure the weights are positive. The paper said something about changing the Newton-Raphson method.
