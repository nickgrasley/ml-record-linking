# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:54:24 2019

@author: ngrasley
"""
import dask.dataframe as dd
from dask.distributed import wait
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
            "nicknames"         : f"{BASE}/crosswalks/nickname_crosswalk.dta",
            "gn_name_comm1910"  : f"{BASE}/census_1910/data/obj/names_crosswalk.dta",
            "gn_name_comm1920"  : f"{BASE}/census_1910/data/obj/names_crosswalk.dta", #NO DEDICATED 1920 NAME CROSSWALK
            "gn_name_comm1930"  : f"{BASE}/census_1930/data/obj/gn_comm.dta",
            "surn_name_comm1910": f"{BASE}/census_1910/data/obj/surn_comm.dta",
            "surn_name_comm1920": f"{BASE}/census_1920/data/obj/surn_comm.dta",
            "surn_name_comm1930": f"{BASE}/census_1930/data/obj/surn_comm.dta",
            "place_names1910"   : f"{BASE}/census_compact/1910/place1910.dta", #FIXME CHANGE TO places1910_v2
            "place_names1920"   : f"{BASE}/census_compact/1920/place1920.dta", #FIXME CHANGE TO places1920_v2
            "place_names1930"   : f"{BASE}/census_compact/1930/places1930.dta",
            "event_lat_lon_1910": f"{BASE}/crosswalks/event_lat_lon_1910.dta", #FIXME CHANGE TO census_towns_coor_v6.dta
            "event_lat_lon_1920": f"{BASE}/crosswalks/event_lat_lon_1920.dta", #FIXME CHANGE TO census_towns_coor_v6.dta
            "event_lat_lon_1930": f"{BASE}/crosswalks/event_lat_lon_1920.dta", #FIXME CHANGE TO census_towns_coor_v6.dta 
            "dict_place_group"  : f"{BASE}/census_compact/dictionaries/dict_place_group.dta",
            "state_dict"        : f"{BASE}/census_compact/dictionaries/state_dict.dta",
            "county_lat_lon_all": f"{BASE}/crosswalks/census_towns_coor_v6.dta", # "{BASE}/crosswalks/county_lat_lon_all.dta" old county lat lon
            "bplace_comm1910"   : f"{BASE}/census_compact/1910/bplace_comm1910.dta",
            "bplace_comm1920"   : f"{BASE}/census_compact/1910/bplace_comm1910.dta", #NO DEDICATED 1920 BPLACE COMM
            "bplace_comm1930"   : f"{BASE}/census_compact/1910/bplace_comm1910.dta", #NO DEDICATED 1930 BPLACE COMM
            "bplace_lat_lon"    : f"{BASE}/census_compact/dictionaries/bplace_lat_lon.dta",
            "gn_name_vectors"   : f"R:/JoePriceResearch/record_linking/projects/fixing_lastnames/firstnames/firstnametovec.dta",
            "surn_name_vectors" : f"R:/JoePriceResearch/record_linking/projects/fixing_lastnames/mltest/nametovec.dta"}

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
                                    "nicknames": self.correct_nicknames,
                                    "name commonality": self.add_name_comm,
                                    "name vectors": self.add_name_vectors,
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
        for y in self.years:
            for func in self.chosen_variables:
                ark_pairs = func(ark_pairs, y)
        #FIXME find out best way to drop variables that shouldn't be included.
        return ark_pairs.persist()
            
    def add_census(self, data, year):
        print(f"adding census {year}")
        data = data.set_index(f"index{year}", drop=False)
        census = dask_read_stata_delayed_group([self.data_directory[f"census{year}"]])
        census.columns = [f"{i}{year}" for i in census.columns]
        census = census.set_index(f"index{year}", sorted=True)
        data = data.join(census, how="left")
        del census
        try:
            wait(data)
        except:
            pass
        return data
        
    def add_names(self, data, year):
        print(f"adding names {year}")
        string_names_df = dask_read_stata_delayed_group([self.data_directory[f"stringnames{year}"]])[[f"index{year}", f"first{year}", f"last{year}"]].set_index(f"index{year}") #slow
        data = data.join(string_names_df, how="left")
        del string_names_df
        return data
        
    def correct_nicknames(self, data, year):
        print(f"correcting nicknames {year}")
        data = data.set_index(f"first{year}", drop=False)
        nickname_df = dask_read_stata_delayed_group([self.data_directory[f"nicknames"]]).set_index("first_name")
        tmp =  data.join(nickname_df, how="left")
        data[f"first{year}"] = data[f"first{year}"].mask(tmp["full_name"].isnull(), tmp["full_name"])
        del nickname_df, tmp
        return data

    def add_name_comm(self, data, year):
        print(f"adding name commonality {year}")
        gn_name_comm = dask_read_stata_delayed_group([self.data_directory[f"gn_name_comm{year}"]])
        gn_name_comm.columns = [f"first{year}", f"first_name_comm{year}"]
        gn_name_comm = gn_name_comm.set_index(f"first{year}").persist()
        surn_name_comm = dask_read_stata_delayed_group([self.data_directory[f"surn_name_comm{year}"]])
        surn_name_comm.columns = [f"last{year}", f"last_name_comm{year}"]
        data = data.join(gn_name_comm, how="left")
        data = dd.merge(data, surn_name_comm, how="left", on=f"last{year}")
        del gn_name_comm, surn_name_comm
        return data
    
    def add_name_vectors(self, data, year):
        print(f"adding name vectors {year}")
        gn_name_vec_df = dask_read_stata_delayed_group([self.data_directory[f"gn_name_vectors"]]).set_index("pr_name_gn")
        gn_name_vec_df.columns = [f"first_vec{i}{year}" for i in range(len(gn_name_vec_df.columns))]
        surn_name_vec_df = dask_read_stata_delayed_group([self.data_directory[f"surn_name_vectors"]])
        cols = ["pr_name_surn"]
        cols.extend([f"last_vec{i}{year}" for i in range(len(surn_name_vec_df.columns) - 1)])
        surn_name_vec_df.columns = cols
        data = data.join(gn_name_vec_df, how="left")
        data = dd.merge(data, surn_name_vec_df, how="left", left_on=f"last{year}", right_on=f"pr_name_surn")
        del gn_name_vec_df, surn_name_vec_df
        return data
        
    def add_res_geo(self, data, year):
        print(f"adding residence geo-coordinates {year}")
        place_names_df = dask_read_stata_delayed_group([self.data_directory[f"place_names{year}"]])
        event_geo_coord_df = dask_read_stata_delayed_group([self.data_directory[f"event_lat_lon_{year}"]])
        event_geo_coord_df = event_geo_coord_df.drop(["county_imputed"], axis=1)
        place_names_df.columns = [f"index{year}", f"event_place{year}"]
        event_geo_coord_df.columns = [f"event_place{year}", f"lat{year}", f"lon{year}"]
        data = dd.merge(data, place_names_df, how="left", on=f"index{year}")
        data = dd.merge(data, event_geo_coord_df, how="left", on=f"event_place{year}").persist()
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
        del dict_place_group
        
        state_dict = dask_read_stata_delayed_group([self.data_directory[f"state_dict"]])
        state_dict = state_dict.loc[state_dict["state"].str.len == 2, :]
        state_dict = state_dict.loc[~state_dict["state"].isin(["New Mexico Territory", "Hawaii Territory", "Consular Service"]), :]
        state_dict.columns = [f"state_string{year}", f"state{year}"]
        data = dd.merge(data, state_dict, how="left", on=f"state{year}") #Removed compute
        del state_dict
        
        county_lat_lon_all = dask_read_stata_delayed_group([self.data_directory["county_lat_lon_all"]])
        county_lat_lon_all = county_lat_lon_all.drop_duplicates(subset=["county", "state"])
        print(data.info())
        tmp = dd.merge(data, county_lat_lon_all, how="left", left_on=[f"county_string{year}", f"state_string{year}"], right_on=["county", "state"]).persist() #Removed compute
        
        data[f"lat{year}"] = data[f"lat{year}"].mask(data[f"lat{year}"].isnull(), tmp.lat)
        data[f"lon{year}"] = data[f"lon{year}"].mask(data[f"lon{year}"].isnull(), tmp.lon)
        del tmp
        #data = dd.from_pandas(data.drop(["latitude", "longitude", "county", "state"], axis=1), chunksize=10000) #TODO this will likely cause memory issues since drop converts dask to pandas. Find a different way to do this.
        return data
        
    def add_bplace_comm(self, data, year):
        print(f"adding birth place commonality {year}")
        data = data.set_index(f"bp{year}", drop=False)
        bplace_comm = dask_read_stata_delayed_group([self.data_directory[f"bplace_comm{year}"]])[["int_place", "comm"]]
        bplace_comm.columns = [f"bp{year}", f"bp_comm{year}"]
        bplace_comm = bplace_comm.set_index(f"bp{year}")
        data = data.join(bplace_comm, how="left")
        del bplace_comm
        return data
        
        
    def add_bplace_geo(self, data, year):
        print(f"adding birth place geo-coordinates {year}")
        bplace_geo_df = dask_read_stata_delayed_group([self.data_directory[f"bplace_lat_lon"]])
        bplace_geo_df.columns = [f"bplace_lat{year}", f"bplace_lon{year}", f"bp{year}"]
        bplace_geo_df = bplace_geo_df.set_index(f"bp{year}")
        data = data.join(bplace_geo_df, how="left")
        del bplace_geo_df
        return data
        
    def delete_vars(self, data, vars_to_drop):
        return data.drop(vars_to_drop, axis=1)
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        #FIXME there's some functions that must run before others. Ensure that those occur if the later functions are called.
        for y in self.years:
            for func in self.chosen_variables:
                X = func(X, y)
        #FIXME find out best way to drop variables that shouldn't be included.
        return X.persist()