# -*- coding: utf-8 -*-
"""
Spyder Editor

Author: Isaac Riley
March 13th, 2019
"""

import pandas as pd
from itertools import product

###################################################################################################################################

            

class Household():
    """
    Takes two census frames and a frame of household ID pairs with at least 1 
        match already made between their members
    Outputs additional matches made within the household pairs
    """
    def __init__(self, anchors, year1, year2, SC=True, iterlist_step_size=None, anchors_are_hh=True, print_=True):
        """ Parameters:
        df1, df2: record dataframes
        anchors: dataframe with two columns containing already-merged households
        year1, year2: census years
        iterlist_step_size: how much of the data to work with at a time 
        anchors_are_hh: whether the anchors are already household IDs, or they need
            to be recovered from indices/arks """
        if SC:
            self.CENSUS_PATH = lambda year:'/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/{0}/census{0}.dta'.format(year)
            self.CW_PATH = lambda year:'/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/{0}/ind_ark{0}.dta'.format(year)
            self.PLACE_DICT_PATH = '/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/utils/dict_place.dta'
            self.REL_DICT_PATH = '/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/utils/rel_dict.dta'
            self.PAIRREL_DICT_PATH = '/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/utils/pairrel_dict.dta'
            self.STRINGNAMES = lambda year:'/fslgroup/fslg_JoePriceResearch/compute/census/f_search/census_compact/{0}/stringnames{0}.dta'.format(year)
        else:
            self.CENSUS_PATH = lambda year: r'R:\JoePriceResearch\record_linking\data\census_compact\{0}\census{0}.dta'.format(year)
            self.CW_PATH = lambda year:r'R:\JoePriceResearch\record_linking\data\census_compact\{0}\ind_ark{0}.dta'.format(year)
            self.PLACE_DICT_PATH = r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\dict_place.dta'
            self.REL_DICT_PATH = r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\rel_dict.dta'
            self.PAIRREL_DICT_PATH = r'R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\pairrel_dict.dta'
            self.STRINGNAMES = lambda year:r'R:\JoePriceResearch\record_linking\data\census_compact\1920\stringnames1920.dta'.format(year)
        self.year1 = year1
        self.year2 = year2
        self.ark1 = 'ark'+str(year1)
        self.ark2 = 'ark'+str(year2)
        self.index1 = 'index'+str(year1)
        self.index2 = 'index'+str(year2)
        self.hh1 = 'household'+str(year1)
        self.hh2 = 'household'+str(year2)
        if print_:
            print('Parameters set up:')
            print(self.year1)
            print(self.year1)
            print(self.ark1)
            print(self.ark2)
            print(self.index1)
            print(self.index2)
            print(self.hh1)
            print(self.hh2)
        
        # open and set up dictionaries
        rels = pd.read_stata(self.REL_DICT_PATH)
        rels = dict(zip(rels.rel,rels.rel_to_head))
        rels.update({-1:'_'})
        self.rels = rels
        pairrels = pd.read_stata(self.PAIRREL_DICT_PATH)
        self.pairrels = dict(zip(pairrels.relpair,pairrels.pairrel))
        bps = pd.read_stata(self.PLACE_DICT_PATH)
        self.bps = dict(zip(bps['int_place'],bps['gen_place']))
        self.bps_inv = {v:k for k,v in self.bps.items()}
        if print_:
            print('Dictionaries setup. Lengths:', len(self.rels), len(self.pairrels), len(self.bps))
        
        self.df1 = self.open_census(year1, year2)
        self.df2 = self.open_census(year2, year1)
        if print_:
            print('Censuses opened:', self.df1.shape, self.df2.shape)
        
        # open census files, if necessary
#        if df1==None:
#            self.df1 = pd.read_stata(self.CENSUS_PATH(year1))
#        else:
#            self.df1 = df1
#        if df2==None:
#            self.df2 = pd.read_stata(self.CENSUS_PATH(year2))
#        else:
#            self.df2 = df2
        
        # fix variable names
#        if 'household' in self.df1.columns:
#            self.df1 = self.df1.rename(columns={'household':self.hh1})
#        if 'household' in self.df2.columns:
#            self.df2 = self.df2.rename(columns={'household':self.hh2})
#        if 'index' in df1.columns:
#            self.df1 = self.df1.rename(columns={'index':self.index1})
#        if 'index' in self.df2.columns:
#            self.df2 = self.df2.rename(columns={'index':self.index2})
        
        # sort out anchors
        if anchors==None:
            self.anchors = pd.DataFrame([])
        else:
            anchors.index = range(anchors.shape[0])
            if anchors_are_hh:
                self.anchors = anchors[[self.hh1, self.hh2]]
            else: 
                if not self.index1 in anchors.columns:
                    self.cw1 = pd.read_stata(self.CW_PATH(self.year1))
                    anchors = pd.merge(anchors, self.cw1, on=self.ark1)
                if not self.index2 in anchors.columns:
                    self.cw2 = pd.read_stata(self.CW_PATH(self.year2)) 
                    anchors = pd.merge(anchors, self.cw2, on=self.ark2)
                anchors = pd.merge(anchors, self.df1[[self.index1, self.hh1]], on=self.index1)
                anchors = pd.merge(anchors, self.df2[[self.index2, self.hh2]], on=self.index2)
                self.anchors = anchors[[self.hh1, self.hh2]]
        if print_:
            print('Anchors set up:', self.anchors.shape, self.anchors.columns)
        
        # set up iterlist
        if iterlist_step_size is None:
            self.iterlist = [(0, anchors.shape[0])]
        else:
            iterlist = list(range(0, self.anchors.shape[0], iterlist_step_size))+[self.anchors.shape[0]]
            self.iterlist = list(zip(iterlist[:-1],iterlist[1:]))
        if print_:
            print('Iterlist made.')
        
    
    def open_census(self, year, otheryear, hhset_to_remove=set([])): #hhbrp
        """ Opens census files and removes a priori unmatchables.
            Optionally removes households that have already been linked. """
        df = pd.read_stata(self.CENSUS_PATH(year))   
        cols = [c for c in df.columns if not c=='ark'+str(year)]
        df = df[cols]
        hh = 'household'+str(year)
        index = 'index'+str(year)
        if hh not in cols:
            df = df.rename(columns={'household':hh})
        if index not in cols:
            df = df.rename(columns={'index':index})
        if year>otheryear:
            try:
                df =  df[(df['immigration']<int(otheryear))|(df['immigration'].isna())]
            except:
                df =  df[(df['immigration']<int(otheryear))|(df['immigration'].isnull())]
        if hhset_to_remove:
            df = df[~df[hh].isin(hhset_to_remove)]
        df = df.groupby(hh).filter(lambda x: len(x) > 1)
        
        return df      
    
    
    def make_hhdict(self, df, year):
        """ Creates dictionary with household IDs as key and nested 
            dictionaries with individual information as values. """
        index = 'index'+str(year)
        hh = 'household'+str(year)
        collist = [index, hh,'rel','female','bp','mbp','fbp',
                   'race','first_sdxn','last_sdxn','cohort1','cohort2']
        return {k:{i:{c:v[c][i] for c in v.columns} for i in v[index]} for \
                k,v in dict(list(df[collist].groupby(hh))).items()}
        
    
    def fish_for_pairs(self, dict1, dict2, pairs):
        """ Iterates over household pairs to identify individuals matching on 
            soundex and cohort. """
        pairset = set([])
        for hh1,hh2 in zip(pairs[self.hh1],pairs[self.hh2]):
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
        cols = [self.index1,self.index2,self.hh1,self.hh2]
        return pd.DataFrame(list(pairset), columns=cols)
    
            
    def fit(self):
        return self
    
    def predict(self, dest_folder):
        """ Runs the entire process and returns a set of matches. """
        for start,stop in self.iterlist:
            hhp = self.anchors[(self.anchors.index>=start)&(self.anchors.index<stop)]
            hhset1 = set(hhp[self.hh1])
            hhset2 = set(hhp[self.hh2])
            hhd1 = self.make_hhdict(self.df1[self.df1[self.hh1].isin(hhset1)], self.year1)
            hhd2 = self.make_hhdict(self.df2[self.df2[self.hh2].isin(hhset2)], self.year2)
            pairs = self.fish_for_pairs(hhd1,hhd2,hhp)
            pairs.to_stata('{0}\\hh_pairs_{1}thr{2}.dta'.format(dest_folder,start,stop))
            
        
    def transform(self):
        return self
            
        
    def get_rel_pairs(self, df, one_two, relationship=None): #hhbrp
        """ Creates a data frame containing all within-household pairs
            and their relationship to one another. """
        if one_two==1:
            hh,year = self.hh1,self.year1
        elif one_two==2:
            hh,year = self.hh2,self.year2
        print(df.columns)
        m = pd.merge(df, df, on=hh)
        print('\tRelpairs size 1:', m.shape[0]/1000000)
        indx = 'index{}_x'.format(year)
        indy = 'index{}_y'.format(year)
        print(m.columns)
        print(one_two, hh, indx, indy)
        m = m[m[indx]!=m[indy]]
        print('\tRelpairs size 2:', m.shape[0]/1000000)
        m['pairrel'] = (m['rel_x'].apply(lambda x:self.rels[x])+m['rel_y'].apply(
                lambda x:self.rels[x])).apply(lambda x:self.pairrels.get(x,'_'))
        if relationship==None:
            return m
        else:
            return m[m['pairrel']==relationship]
            print('\tRelpairs size 3:', m.shape[0])
        
    def pairmerge(self, m1, m2): 
        """ Merges between years on within-household pairs. """
        df = pd.merge(m1, m2, on=['cohort1_x','cohort1_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])
        df = df.append(pd.merge(m1, m2, on=['cohort2_x','cohort2_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        df = df.append(pd.merge(m1, m2, on=['cohort1_x','cohort2_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        df = df.append(pd.merge(m1, m2, on=['cohort2_x','cohort1_y','first_sdxn_x','first_sdxn_y','last_sdxn_x','last_sdxn_y'])).drop_duplicates()
        return df
 

    def filter_households_on(self, df_, one_two, filter_val, filter_col='bp'): #hhbrp
        """ Keeps only households with at least one member having 'filter_val'
            in 'filter_col' and households with size >1. """
        print(filter_val, filter_col, one_two)
        df = df_.copy()
        if one_two==1:
            hh = self.hh1
        elif one_two==2:
            hh = self.hh2
        df = df[df.rel!=-1]
        print('\tFilter 1:', df.shape[0]/1000000)
        df = df[~df[hh].isna()]
        print('\tFilter 2:', df.shape[0]/1000000)
        df['keeper'] = df[filter_col]==filter_val
        print('\tFilter 3:', df.shape[0]/1000000)
        df['keep'] = df[[hh,'keeper']].groupby(hh).transform(max).fillna(False)
        print('\tFilter 4:', df.shape[0]/1000000)
        df = df[df.keep]
        print('\tFilter 5:', df.shape[0]/1000000) 
        df = df.groupby(hh).filter(lambda x: (len(x) > 1) and (len(x) < 16))
        print('\tFilter 6:', df.shape[0]/1000000)
        return df
                
    def evaluate(self, matches): #hhbrp
        """ Takes the set of matches and creates new variables reflecting 
            match quality. """  
        hhcols = [self.hh1, self.hh2]
        matches['hhmatches'] = matches[hhcols+[self.index1]].groupby(hhcols).transform(len)
        ind1m,ind2m = ['index{}matches'.format(x) for x in [self.year1,self.year2]]
        indices = [self.index1, self.index2]
        matches[ind1m] = matches[indices].groupby(indices[0]).transform(len)
        matches[ind1m] = matches[indices].groupby(indices[1]).transform(len)
        return matches
    
    def predict_by_relpair(self, bp_list, dest_folder): #hhbrp
        """ Runs the entire process and returns a set of matches. """
        print(len(self.rels), len(self.pairrels))
        for bp in bp_list:
            print('*********\n{}\n*********'.format(bp))
            pairs1 = self.filter_households_on(self.df1, 1, bp) 
            print('pairs1 initial size:', pairs1.shape[0]/1000000)
            pairs2 = self.filter_households_on(self.df2, 2, bp)
            print('pairs2 initial size:', pairs2.shape[0]/1000000)
            if pairs1.shape[0]==0 or pairs2.shape[0]==0:
                print('Size 0')
                continue
            pairs1 = self.get_rel_pairs(pairs1, 1)
            print('pairs1 final size:', pairs1.shape[0]/1000000)
            pairs2 = self.get_rel_pairs(pairs2, 2)
            print('pairs2 final size:', pairs2.shape[0]/1000000)
            merge = self.pairmerge(pairs1, pairs2)
            print('Final merge size:', merge.shape[0]/1000000)
            ind1x = 'index{}_x'.format(self.year1)
            ind2x = 'index{}_x'.format(self.year2)
            ind1y = 'index{}_y'.format(self.year1)
            ind2y = 'index{}_y'.format(self.year2)
            print(ind1x,ind1y,ind2x,ind2y)
            merge[[ind1x,ind1y,ind2x,ind2y,'pairrel_x',
                   'pairrel_y',self.hh1,self.hh2]].to_stata(
                   '{0}\\relpair_merge_{1}.dta'.format(dest_folder,self.bps[bp]))  
            print('d')
            del merge


			
			
			
	