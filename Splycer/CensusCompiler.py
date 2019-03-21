# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:24 2019

@author: ngrasley
"""
import dask.dataframe as dd
from stata_dask import dask_read_stata_delayed_group
from sklearn.base import BaseEstimator, TransformerMixin
import warnings

def R_DRIVE_DIRECS(): #FIXME. Write documentation for what is in each of these files.
    BASE = "R:/JoePriceResearch/record_linking/data"
    return {"census1900"        : f"{BASE}/census_compact/1900/census1900.dta", #1900 COMPACT NOT IMPLEMENTED
            "census1910"        : f"{BASE}/census_compact/1910/census1910.dta",
            "census1920"        : f"{BASE}/census_compact/1920/census1920.dta",
            "census1930"        : f"{BASE}/census_compact/1930/census1930.dta",
            "census1940"        : f"{BASE}/census_compact/1940/census1940.dta",
            "stringnames1900"   : f"{BASE}/census_compact/1900/stringnames1900.dta",
            "stringnames1910"   : f"{BASE}/census_compact/1910/stringnames1910.dta",
            "stringnames1920"   : f"{BASE}/census_compact/1920/stringnames1920.dta",
            "stringnames1930"   : f"{BASE}/census_compact/1930/stringnames1930.dta",
            "stringnames1940"   : f"{BASE}/census_compact/1940/stringnames1940.dta",
            "gn_name_comm1910"  : f"{BASE}/census_1910/data/obj/names_crosswalk.dta",
            "gn_name_comm1920"  : f"{BASE}/census_1910/data/obj/names_crosswalk.dta", #NO DEDICATED 1920 NAME CROSSWALK
            "surn_name_comm1910": f"{BASE}/census_1910/data/obj/surn_comm.dta",
            "surn_name_comm1920": f"{BASE}/census_1920/data/obj/surn_comm.dta",
            "place_names1910"   : f"{BASE}/census_compact/1910/place1910.dta",
            "place_names1920"   : f"{BASE}/census_compact/1920/place1920.dta",
            "event_lat_lon_1910": f"{BASE}/crosswalks/event_lat_lon_1910.dta",
            "event_lat_lon_1920": f"{BASE}/crosswalks/event_lat_lon_1920.dta",
            "dict_place_group"  : f"{BASE}/census_compact/dictionaries/dict_place_group.dta",
            "state_dict"        : f"{BASE}/census_compact/dictionaries/state_dict.dta",
            "county_lat_lon_all": f"{BASE}/crosswalks/county_lat_lon_all.dta",
            "bplace_comm1910"   : f"{BASE}/census_compact/1910/bplace_comm1910.dta",
            "bplace_comm1920"   : f"{BASE}/census_compact/1910/bplace_comm1910.dta",
            "bplace_lat_lon"    : f"{BASE}/census_compact/dictionaries/bplace_lat_lon.dta"} #NO DEDICATED 1920 BPLACE COMM
#FIXME ADD R:\JoePriceResearch\record_linking\data\crosswalks\census_towns_coor_v6.dta. It's an updated county lat lon

class CensusCompiler(BaseEstimator, TransformerMixin):
    """This class grabs all the data from the census and accompanying crosswalks.
       If you are not working on the R drive, you need to create a dictionary
       similar to the one above. Your dictionary must contain each key (or at
       least a key for each variable you will be using) and the value will be
       the path to the data on your machine. My code assumes that your file is
       a dta file, so you should edit this class or resave your data as a dta
       file if you are using any other data format.
       
       This code borrowed heavily from merge_datasets.do in the preprocessing
       folder of this same repo. I believe merge_datasets.do runs faster on small
       samples, so quick results are better when using that. However, this code
       is more readable, scalable, and safe than the Stata do file.
    """
    def __init__(self, years=["1910", "1920"], data_direc=R_DRIVE_DIRECS()):
        self.years = years
        self.available_variables = {"census": self.add_census,
                                    "name strings": self.add_names,
                                    "name commonality": self.add_name_comm,
                                    "residence geo-coordinates": self.add_res_geo,
                                    "birth place commonality": self.add_bplace_comm,
                                    "birth place geo-coordinates": self.add_bplace_geo}
        self.chosen_variables = []
        self.data_directory = data_direc
    
    def add_variable(self, var):
        try:
            if self.available_variables[var] not in self.chosen_variables:
                self.chosen_variables.append(self.available_variables[var])
            else:
                print("Variable already included")
        except:
            print("Throw some error")
    
    def compile_census(self, ark_pairs):
        warnings.warn("compile_census deprecated: run transform instead", FutureWarning)
        #FIXME there's some functions that must run before others. Ensure that those occur if the later functions are called.
        for func in self.chosen_variables:
            for y in self.years:
                ark_pairs = func(ark_pairs, y)
        #FIXME find out best way to drop variables that shouldn't be included.
        return ark_pairs
            
    def add_census(self, data, year):
        census = dask_read_stata_delayed_group([self.data_directory[f"census{year}"]])
        census.columns = [f"{i}{year}" for i in census.columns]
        return dd.merge(data, census, how="left", on=f"index{year}").compute()
        
    def add_names(self, data, year):
        string_names_df = dask_read_stata_delayed_group([self.data_directory[f"stringnames{year}"]])[[f"index{year}", f"first{year}", f"last{year}"]]
        return dd.merge(data, string_names_df, how="left", on=f"index{year}").compute()
        
    def add_name_comm(self, data, year):
        gn_name_comm = dask_read_stata_delayed_group([self.data_directory[f"gn_name_comm{year}"]])
        gn_name_comm.columns = [f"first{year}", f"first_name_comm{year}"]
        surn_name_comm = dask_read_stata_delayed_group([self.data_directory[f"surn_name_comm{year}"]])
        surn_name_comm.columns = [f"last{year}", f"last_name_comm{year}"]
        data = dd.merge(data, gn_name_comm, how="left", on=f"first{year}")
        return dd.merge(data, surn_name_comm, how="left", on=f"last{year}").compute()
        
        
    def add_res_geo(self, data, year):
        place_names_df = dask_read_stata_delayed_group([self.data_directory[f"place_names{year}"]])
        event_geo_coord_df = dask_read_stata_delayed_group([self.data_directory[f"event_lat_lon_{year}"]])
        event_geo_coord_df = event_geo_coord_df.drop(["county_imputed"], axis=1)
        place_names_df.columns = [f"index{year}", f"event_place{year}"]
        event_geo_coord_df.columns = [f"event_place{year}", f"lat{year}", f"lon{year}"]
        data = dd.merge(data, place_names_df, how="left", on=f"index{year}")
        data = dd.merge(data, event_geo_coord_df, how="left", on=f"event_place{year}")
        del place_names_df, event_geo_coord_df
        
        dc = "Washington, District of Columbia, United States"
        richmond = "Richmond (Independent City), Virginia, United states"
        data[f"lat{year}"] = data[f"lat{year}"].mask(data[f"event_place{year}"] == dc, 38.904722) #FIXME check that mask is working as intended
        data[f"lon{year}"] = data[f"lon{year}"].mask(data[f"event_place{year}"] == dc, -77.016389)
        data[f"lat{year}"] = data[f"lat{year}"].mask(data[f"event_place{year}"] == richmond, 37.533333)
        data[f"lon{year}"] = data[f"lon{year}"].mask(data[f"event_place{year}"] == richmond, -77.466667)
        ''' This does not work in dask like it does in Pandas. You need to use where() or mask()
        data.loc[data[f"event_place{year}"] == dc, f"lat{year}"] = 38.904722
        data.loc[data[f"event_place{year}"] == dc, f"lon{year}"] = -77.016389
        data.loc[data[f"event_place{year}"] == richmond, f"lat{year}"] = 37.533333
        data.loc[data[f"event_place{year}"] == richmond, f"lon{year}"] = -77.466667
        '''
    
        dict_place_group = dask_read_stata_delayed_group([self.data_directory["dict_place_group"]])[["county", "county_state"]]
        dict_place_group = dict_place_group.drop_duplicates(subset="county_state")
        dict_place_group.columns = [f"county_string{year}", f"county{year}"]
        data = dd.merge(data, dict_place_group, how="left", on=f"county{year}") #Removed compute
        
        state_dict = dask_read_stata_delayed_group([self.data_directory[f"state_dict"]])
        state_dict = state_dict.loc[state_dict["state"].str.len == 2, :]
        state_dict = state_dict.loc[~state_dict["state"].isin(["New Mexico Territory", "Hawaii Territory", "Consular Service"]), :]
        state_dict.columns = [f"state_string{year}", f"state{year}"]
        data = dd.merge(data, state_dict, how="left", on=f"state{year}") #Removed compute
        
        county_lat_lon_all = dask_read_stata_delayed_group([self.data_directory["county_lat_lon_all"]])
        county_lat_lon_all = county_lat_lon_all.drop_duplicates(subset=["county", "state"])
        print(data.info())
        data = dd.merge(data, county_lat_lon_all, how="left", left_on=[f"county_string{year}", f"state_string{year}"], right_on=["county", "state"]) #Removed compute
        
        data[f"lat{year}"] = data[f"lat{year}"].mask(data[f"lat{year}"].isnull(), data.latitude)
        data[f"lon{year}"] = data[f"lon{year}"].mask(data[f"lon{year}"].isnull(), data.longitude)
        #data = dd.from_pandas(data.drop(["latitude", "longitude", "county", "state"], axis=1), chunksize=10000) #TODO this will likely cause memory issues since drop converts dask to pandas. Find a different way to do this.
        return data.compute()
        
    def add_bplace_comm(self, data, year):
        bplace_comm = dask_read_stata_delayed_group([self.data_directory[f"bplace_comm{year}"]])[["int_place", "comm"]]
        bplace_comm.columns = [f"bp{year}", f"bp_comm{year}"]
        return dd.merge(data, bplace_comm, how="left", on=f"bp{year}").compute()
        
        
    def add_bplace_geo(self, data, year):
        bplace_geo_df = dask_read_stata_delayed_group([self.data_directory[f"bplace_lat_lon"]])
        bplace_geo_df.columns = [f"bplace_lat{year}", f"bplace_lon{year}", f"bp{year}"]
        return dd.merge(data, bplace_geo_df, how="left", on=f"bp{year}").compute()
        
    def delete_vars(self, data, vars_to_drop):
        return data.drop(vars_to_drop, axis=1)
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        #FIXME there's some functions that must run before others. Ensure that those occur if the later functions are called.
        for func in self.chosen_variables:
            for y in self.years:
                X = func(X, y)
        #FIXME find out best way to drop variables that shouldn't be included.
        return X