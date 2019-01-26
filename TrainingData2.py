
import sqlite3
import pandas as pd
import numpy  as np

class TrainingData():
    def __init__(self, ark_file, years=['1910', '1920'], model=None):
        '''Initialize with a DataFrame of arks to either train or predict
        '''
        census_files = "R:/JoePriceResearch/record_linking/data/census.db"
        self.conn = sqlite3.connect(ark_file)
        self.cursor = sqlite3.Cursor()
        self.years = years
        self.cursor.execute("ATTACH {} as arks_db".format(ark_file))
        self.cursor.execute("ATTACH {} as census_db".format(census_files))
        self.censuses = []
        self.final_data = pd.DataFrame()
        self.model = model

    def create_bins(self, bin_vars):
        bins = pd.concat([self.censuses[0][bin_vars],
                          self.censuses[1][bin_vars]], axis=0)
        bins.groupby(bin_vars).grouper.groupinfo[0]
        '''FIXME finish this function '''

    def get_census(self, census_year, cols=["*"], features, subset=False):
        query = "SELECT {} FROM census_db.basic{} WHERE event_state = '{}'".format(cols.join(", "), year, self.state)
        self.censuses.append((census_year, pd.read_sql_query(query,
                                                             self.conn)))
        if "pr_age" in cols:
            self.censuses[-1].pr_age = self.censuses[-1].pr_age.to_numeric()
        if "pr_immigration_year" in cols:
            self.censuses[-1].pr_immigration_year = self.censuses[-1].pr_immigration_year.to_numeric()
        if "pr_sex_code" in cols:
            self.censuses[-1].sex = pd.Series(np.where(self.censuses[-1].pr_sex_code == "Female", 1, 0), self.censuses[-1].index)
            self.censuses[-1].loc[self.censuses[-1].pr_sex_code == "Unknown", "sex"] = np.nan
            del self.censuses[-1].pr_sex_code
        if "pr_race_or_color" in cols:
            '''FIXME do some prelim binning of race (e.g. [mulatto, black, negro] block)'''
        if "pr_relationship_code" in cols:
            self.censuses[-1] = pd.concat([self.censuses[-1], pd.get_dummies(self.censuses[-1].pr_relationship_code)], axis=1)
            del self.censuses[-1].pr_relationship_code
        if "pr_marital_status" in cols:
            self.censuses[-1] = pd.concat([self.censuses[-1], pd.get_dummies(self.censuses[-1].pr_marital_status)], axis=1)
            del self.censuses[-1].pr_marital_status

        self.process_census_file(-1, features)


    def merge_censuses(self):
        self.final_data = pd.read_sql_query("SELECT * FROM arks_db.bins")
        count = 0
        for i in self.censuses:
            self.final_data = self.final_data.merge(i[1],
                                                    on="ark{}".format(i[0]),
                                                    how="left")
            del self.censuses[count]
            count += 1
        return 0

    def process_census_file(self, census_idx, features):
        function_dict = {"jw": jw(), "sdx": sdx(), "initial":
                                             initial, ""}
        for feat in features:
            function_dict[feat]
