# -*- coding: utf-8 -*-
"""
Spyder Editor

Author: Isaac Riley
March 13th, 2019
"""

import pandas as pd
from itertools import product

###################################################################################################################################

class HouseholdByAnchors():
    """
    Takes two census frames and a frame of household ID pairs with at least 1 
        match already made between their members
    Outputs additional matches made within the household pairs
    """
    def __init__(self, df1, df2, anchors, year1, year2, iterlist=[]):
        """ Parameters:
        df1, df2: record dataframes
        anchors: dataframe with two columns containing already-merged households
        year1, year2: census years """
        self.df1 = df1
        self.df2 = df2
        anchors.index = range(anchors.shape[0])
        self.anchors = anchors
        self.year1 = year1
        self.year2 = year2
        self.iterlist = iterlist
        if iterlist:
            self.stepsize = iterlist[1]-iterlist[0]
        else:
            self.stepsize = None
            self.iterlist = [(0,anchors.shape[0]+1)]
    
        
    def make_hhdict(self, df):
        """ Creates dictionary with household IDs as key and nested 
            dictionaries with individual information as values. """
        collist = ['index','household','rel','female','bp','mbp','fbp',
                   'race','first_sdxn','last_sdxn','cohort1','cohort2']
        return {k:{i:{c:v[c][i] for c in v.columns} for i in v['index']} for \
                k,v in dict(list(df[collist].groupby('household'))).items()}
    
    def fish_for_pairs(self, dict1, dict2, pairs):
        """ Iterates over household pairs to identify individuals matching on 
            soundex and cohort. """
        hh1,hh2 = 'hh'+self.year1,'hh'+self.year2
        pairset = set([])
        for hh1,hh2 in zip(pairs[hh1],pairs[hh2]):
            famcount = 0
            famset= set([])
            for id1,id2 in product(dict1.get(hh1,{}).keys(),dict2.get(hh2,{}).keys()):
                d1,d2 = dict1[hh1][id1],dict2[hh2][id2]
                if d1['first_sdxn']==d2['first_sdxn'] and (d1['cohort1']==d2['cohort1'] \
                     or d1['cohort2']==d2['cohort2']):
                    famcount += 1
                    famset.add((id1,id2,hh1,hh2))
                if famcount>1:
                    pairset.update(famset)
        cols = ['index'+self.year1,'index'+self.year2,'hh'+self.year1,'hh'+self.year2]
        return pd.DataFrame(list(pairset), columns=cols)
    
    def filter_households_on(self, df, filter_val, filter_col='bp'):
        """ Keeps only households with at least one member having 'filter_val'
            in 'filter_col'. """
        orig = df.copy()
        df['keeper'] = df[filter_col]==filter_val
        df['keep'] = df[['household','keeper']].groupby(
                'household').transform(max).fillna(False)
        df = df[df.keep].dropna()[['household']].drop_duplicates()
        return pd.merge(orig, df)
        
    def evaluate(self, matches):
        """ Takes the set of matches and creates new variables reflecting 
            match quality. """   
        hhcols = ['hh'+str(self.year1), 'hh'+str(self.year2)]
        matches['hhmatches'] = matches[hhcols+[
                'index'+str(self.year1)]].groupby(hhcols).transform(len)
        ind1m,ind2m = ['index{}matches'.format(x) for x in [
                self.year1,self.year2]]
        indices = ['index'+str(self.year1), 'index'+str(self.year2)]
        matches[ind1m] = matches[indices].groupby(indices[0]).transform(len)
        matches[ind1m] = matches[indices].groupby(indices[1]).transform(len)
        return matches
        
    def run(self, dest_folder):
        """ Runs the entire process and returns a set of matches. """
        hh1,hh2 = 'hh'+self.year1,'hh'+self.year2
        for start,stop in self.iterlist:
            hhp = self.anchors[(self.anchors.index>=start)&(self.anchors.index<stop)]
            hhset1 = set(hhp[hh1])
            hhset2 = set(hhp[hh2])
            hhd1 = self.make_hhdict(self.df1[self.df1.household.isin(hhset1)])
            hhd2 = self.make_hhdict(self.df2[self.df2.household.isin(hhset2)])
            pairs = self.fish_for_pairs(hhd1,hhd2,hhp)
            pairs.to_stata('{0}\\hh_pairs_{1}thr{2}.dta'.format(dest_folder,start,stop))
        


class HouseholdByRelPairs():
    """ 
    Takes two dataframes with a household ID and relationship to household 
        head 
    Returns a frame containing new matches 
        
    """
    def __init__(self, df1, df2, year1, year2, ):
        """ Parameters:
        df1, df2: record dataframes
        year1, year2: census years """
        self.df1 = df1
        self.df2 = df2
        self.year1 = year1
        self.year2 = year2
        self.rels, self.pairrels = self.get_dicts()
        bps = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\dict_place.dta')
        self.bps = dict(zip(bps['int_place'],bps['gen_place']))
        self.bps_inv = {v:k for k,v in self.bps.items()}
    
    def get_dicts(self):
        rels = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\rel_dict.dta')
        rels = dict(zip(rels.rel,rels.rel_to_head))
        rels.update({-1:'_'})
        pairrels = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\pairrel_dict.dta')
        pairrels = dict(zip(pairrels.relpair,pairrels.pairrel))
        return rels, pairrels
              
    def open_census(self, year, otheryear, hhset_to_remove=set([])):
        """ Opens census files and removes a priori unmatchables.
            Optionally removes households that have already been linked. """
        df = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\{0}\census{0}.dta'.format(year))    
        if year>otheryear:
            try:
                df =  df[(df['immigration']<int(otheryear))|(df['immigration'].isna())]
            except:
                df =  df[(df['immigration']<int(otheryear))|(df['immigration'].isnull())]
        if hhset_to_remove:
            df = df[~df['household'].isin(hhset_to_remove)]
        df = df.groupby('household').filter(lambda x: len(x) > 1)
        return df              
        
    def get_rel_pairs(self, df, relationship=None):
        """ Creates a data frame containing all within-household pairs
            and their relationship to one another. """
        m = pd.merge(df, df, on='household')
        m = m[m.index_x!=m.index_y]
        
        m['pairrel'] = (m['rel_x'].apply(lambda x:self.rels[x])+m['rel_y'].apply(
                lambda x:self.rels[x])).apply(lambda x:self.pairrels.get(x,'_'))
        if relationship is None:
            return m
        else:
            return m[m['pairrel']==relationship]
        
    def pairmerge(self, m1, m2):
        """ Merges between years on within-household pairs. """
        df = pd.merge(m1, m2, on=['cohort1_x','cohort1_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])
        df = df.append(pd.merge(m1, m2, on=['cohort2_x','cohort2_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        df = df.append(pd.merge(m1, m2, on=['cohort1_x','cohort2_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        df = df.append(pd.merge(m1, m2, on=['cohort2_x','cohort1_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        return df
     
    def filter_households_on(self, df, filter_val, filter_col='bp'):
        """ Keeps only households with at least one member having 'filter_val'
            in 'filter_col' and households with size >1. """
        orig = df.copy()
        df['keeper'] = df[filter_col]==filter_val
        df['keep'] = df[['household','keeper']].groupby('household').transform(max).fillna(False)
        df = df[df.keep].dropna()[['household']].drop_duplicates()
        df = df.groupby('household').filter(lambda x: len(x) > 1)
        return pd.merge(orig, df)
                
    def evaluate(self, matches):
        """ Takes the set of matches and creates new variables reflecting 
            match quality. """  
        hhcols = ['hh'+str(self.year1), 'hh'+str(self.year2)]
        matches['hhmatches'] = matches[hhcols+['index'+str(self.year1)]].groupby(hhcols).transform(len)
        ind1m,ind2m = ['index{}matches'.format(x) for x in [self.year1,self.year2]]
        indices = ['index'+str(self.year1), 'index'+str(self.year2)]
        matches[ind1m] = matches[indices].groupby(indices[0]).transform(len)
        matches[ind1m] = matches[indices].groupby(indices[1]).transform(len)
        return matches
            
    def run(self, bp, dest_folder):
        """ Runs the entire process and returns a set of matches. """
        pairs1 = self.filter_households_on(self.df1, self.year1, bp)
        pairs2 = self.filter_households_on(self.df2, self.year2, bp)
        merge = self.pairmerge(pairs1, pairs2)  
        merge[['index_x_x','index_y_x','index_x_y','index_y_y','pairrel_x',
               'pairrel_y','household_x','household_y']].to_stata(
               '{0}\\relpair_merge_{1}.dta'.format(dest_folder,self.bps[bp]))  
        merged1 = set(merge['index_x_x'])
        merged2 = set(merge['index_x_y'])
        del merge
        pairs1[~pairs1['index_x'].isin(merged1)][['index_x']].to_stata(
            '{0}\\relpair_unmerged_{1}_{2}.dta'.format(dest_folder,self.bps[bp],self.year1))
        del pairs1
        pairs2[~pairs2['index_x'].isin(merged2)][['index_x']].to_stata(
            '{0}\\relpair_unmerged_{1}_{2}.dta'.format(dest_folder,self.bps[bp],self.year2))
        del pairs2  
        print('{} saved.'.format(bp))





        
#    def get_arks(self, index_pairs, year1, year2):
#        cw = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\{0}\int_ark{0}.dta'.format(year1))
#        df = pd.merge(index_pairs, cw.rename(columns={'index'+year1:'index_x_x'}), how='inner', on='index_x_x')
#        df = pd.merge(df, cw.rename(columns={'index'+year1:'index_y_x'}), how='inner', on='index_y_x')
#        df = df.rename(columns={'ark'+year1+'_x':'ark1_'+year1,'ark'+year1+'_y':'ark2_'+year1})
#        del cw
#        cw = pd.read_stata(r'R:\JoePriceResearch\record_linking\data\census_compact\{0}\int_ark{0}.dta'.format(year2))
#        df = pd.merge(df, cw.rename(columns={'index'+year2:'index_x_y'}), how='inner', on='index_x_y')
#        df = pd.merge(df, cw.rename(columns={'index'+year2:'index_y_y'}), how='inner', on='index_y_y')
#        df = df.rename(columns={'ark'+year2+'_x':'ark1_'+year2,'ark'+year2+'_y':'ark2_'+year2})
#        del cw
#        df = df[['ark1_'+year1,'ark2_'+year1,'ark1_'+year2,'ark2_'+year2,'pairrel_x','pairrel_y']]
#        return df

 