#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:49:19 2019

@author: thegrasley
"""
import numpy  as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
import jellyfish
from metaphone import doublemetaphone
import sqlite3
from ngram import NGram

class DataFrameSelector(BaseEstimator, TransformerMixin): #works
    """Select variables for certain transformations
    Parameters:
        attribute_names (list): The names of the columns you want to select
        is_string (bool): Whether the columns you are selecting are strings (if np array, implement this to return unicode arrays.)
        df_out (bool): if true, return a pandas dataframe
    Returns:
        A dataframe or array of the selected data.
    """
    def __init__(self, attribute_names, is_string=False, df_out=True): #FIXME implement choosing string and numeric columns
        self.attribute_names = attribute_names
        self.is_string = is_string
        self.df_out = df_out
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if self.df_out:
            return X[self.attribute_names]
        return X[self.attribute_names].values


class Substring(BaseEstimator, TransformerMixin): #works
    """Select a substring of a specified column
    Parameters:
        str_len (int): The length of the substring
        col (string): The name of the column that you want a substring of
        start (int): The first character (indexed from 0) of the substring
        col_idx (list): used for np array. Still needs testing
        drop (bool): if true, drop the original column
    Returns:
        dataframe or array with the new substring column
    """
    def __init__(self, str_len=1, col="pr_name_gn", start=0, col_idx=[0], drop=False):
        self.str_len = str_len
        self.col = col
        self.start = start
        self.col_idx = col_idx
        self.drop = drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.series.Series:
            return X.str[self.start:self.str_len]
        elif type(X) == np.ndarray:
            return np.c_[X, X[:,self.col_idx].astype(f"<U{self.str_len}")] #FIXME allow for diff. starting index
        elif type(X) == pd.core.frame.DataFrame:
            if self.drop:
                return X[self.col].str[self.start:self.start + self.str_len] #FIXME only drop self.col
            else:
                X[self.col + "_initial"] = X[self.col].str[self.start:self.start + self.str_len]
                return X
        else:
            raise Exception("{} data type input to Substring".format(type(X)))

class SplitString(BaseEstimator, TransformerMixin): #FIXME test
    """Create new columns for the number of splits found in a string.
       Often used for splitting names into first and middle.
    Parameters:
        col (string): The name of the column to split. Default is "pr_name_gn".
        split_char (string): The character on which to split (typically whitespace). Default is " "
        num_splits (int): The number of splits to perform. -1 is all possible. Default is 2.
        drop (bool): if true, drops the original column. Default is False.
    Returns:
        DataFrame with the split columns
    """
    def __init__(self, col="pr_name_gn", split_char=" ", num_splits=2, drop=False):
        self.col = col
        self.split_char = split_char
        self.drop = drop
        self.num_splits = num_splits
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            new_cols = X[self.col].str.split(pat=self.split_char, n=self.num_splits, expand=True)
            new_cols.columns = [f"{self.col}{i}" for i in range(self.num_splits)]
            X = pd.concat([X, new_cols], axis=1)
            if self.drop:
                X.drop(self.col, axis=1, inplace=True)
            return X

class SDX(BaseEstimator, TransformerMixin): #works
    """Generate the soundex code for a column of strings. (DEPRECATED)
    Parameters:
        string_col (string): the name of the old column of strings
    Returns:
        returns a pandas DataFrame with the soundex code for strings 
        (in one column and the original strings in the other?)
    """
    def __init__(self, string_col="name"):
        self.string_col = string_col
        self.sdx = np.vectorize(jellyfish.soundex)
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            X[f"{self.string_col}_sdx"] = self.sdx(X[self.string_col])
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, self.sdx(X[:,self.sdx_col_name])]
        elif type(X) == pd.core.series.Series:
            return self.sdx(X)

class NYSIIS(BaseEstimator, TransformerMixin):
    """Generate the NYSIIS code for a column of strings (DEPRECATED)
    Parameters:
        
    Returns:
    """
    def __init__(self, string_col=["pr_name_gn"]):
        self.string_col = string_col
        self.nysiis = np.vectorize(jellyfish.nysiis)
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for col in self.string_col:
                X[f"{col}_NYSIIS"] = self.nysiis(X[col])
            return X
        
class PhoneticCode(BaseEstimator, TransformerMixin):
    """Generate any phonetic code for a column of strings
    Parameters:
        string_col (list): a list of the columns that you want phonetic codes for.
                           This should be the base name without year. Default is ["pr_name_gn"]
        encoding (string): the type of encoding that you want to run.
                           Valid encodings are dmetaphone, NYSIIS, SDX. Default is "dmetaphone".
        years (list): the two years that are in your dataset. Default is ["1910", "1920"]
    Returns:
        The desired phonetic code for the desired columns concatenated with the original dataframe.
    """
    def __init__(self, string_col=["pr_name_gn"], encoding="dmetaphone", years=["1910", "1920"]):
        self.string_col = string_col
        if encoding == "dmetaphone":
            self.encoding = np.vectorize(doublemetaphone)
        elif encoding == "NYSIIS":
            self.encoding = np.vectorize(jellyfish.nysiis)
        elif encoding == "SDX":
            self.encoding = np.vectorize(jellyfish.soundex)
        else:
            raise Exception(f"{encoding} is not a valid phonetic encoding")
        self.encoding_name = encoding
        self.years = years
        #FIXME refactor the other phonetic encodings here.
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for col in self.string_col:
                if self.years is not None:
                    X[f"{col}_{self.encoding_name}{self.years[0]}"] = self.encoding(X[f"{col}{self.years[0]}"].astype(str))[0]
                    X[f"{col}_{self.encoding_name}{self.years[1]}"] = self.encoding(X[f"{col}{self.years[1]}"].astype(str))[0]
                else:
                    X[f"{col}_{self.encoding_name}"] = self.encoding(X[col])[0]
        return X

class JW(BaseEstimator, TransformerMixin): #works
    """Takes in two columns of strings and gives you a column of the Jaro-Winkler
        score. (DEPRECATED)
    Parameters:
        jw_col_name (string): the column name of the jaro-winkler score for the strings that will be returned.
        string1_col (string): the first column of strings that you want calculate distance for
        string2_col (string): the column of strings you want to compare the first column to.
    Returns:
        returns a numpy with the two columns of strings compared and one column with
        the J-W scores of the strings in that row. 
    """
    def __init__(self, jw_col_name=["name_gn_jw"],
                 string1_col=["pr_name_gn1910"], string2_col=["pr_name_gn1920"]):
        self.jw = np.vectorize(jellyfish.jaro_winkler)
        self.jw_col_name = jw_col_name
        self.string1_col = string1_col
        self.string2_col = string2_col
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for new_col, name1, name2 in zip(self.jw_col_name, self.string1_col, self.string2_col):
                X[new_col] = self.jw(X[name1], X[name2])
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, self.jw(X[:,self.string1_col], X[:,self.string2_col])]
        
class StringDistance(BaseEstimator, TransformerMixin):
    def __init__(self, string_col=["pr_name_gn"],
                 dist_metric="levenshtein",
                 years=["1910", "1920"]):
        """Generate any string distance for a column of strings
        Parameters:
            string_col (list): a list of the columns that you want string distance for. This should be the base name without the year.
            dist_metric (string): the name of the string distance metric that you want to use.
                                  Valid metrics are jw, levenshtein.
            years (list): a list of the two years in your dataset.
        """
        self.string_col = string_col
        self.years = years
        self.dist_metric = np.vectorize(jellyfish.levenshtein_distance)
        self.dist_metric_name = dist_metric
        if dist_metric == "jw":
            self.dist_metric = np.vectorize(jellyfish.jaro_winkler)
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for col in self.string_col:
                X[f"{col}_{self.dist_metric_name}"] = \
                    self.dist_metric(X[f"{col}{self.years[0]}"],
                                     X[f"{col}{self.years[1]}"])
            return X

class Bigram(BaseEstimator, TransformerMixin):
    """Creates a bigram representation of string(s)
    Parameters:
        string_col (list): The columns on which you want to perform the ngram.
                           These should be base names without the year.
        n (int): The number of characters per gram.
        years (list): A list of the two years in you dataset. Default is ["1910", "1920"]
    Returns:
        An ngram similarity score for the columns.
    """
    def __init__(self, string_col=["pr_name_gn"], n=3, years=["1910", "1920"]):
        self.string_col = string_col
        self.ngram = np.vectorize(NGram.compare)
        self.n = n
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        for col in self.string_col:
            X[f"{col}_ngram"] = self.ngram(X[f"{col}{self.years[0]}"].astype(str), X[f"{col}{self.years[1]}"].astype(str))
        return X

class DropVars(BaseEstimator, TransformerMixin): #works
    """Takes a dataFrame and returns a dataframe with the columns deleted that you wanted to drop
    Parameters:
        cols_to_drop (list): the names of the columns that you want to drop from a dataframe
    Returns:
        a pandas DataFrame that is copy of the one you had to begin with but with the columns
        you wanted to get rid of removed.
    """
    def __init__(self, cols_to_drop=[], both_years=False, years=["1910", "1920"]):
        self.cols_to_drop = cols_to_drop
        self.both_years = both_years
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            if self.both_years:
                drop_cols = [f"{i}{self.years[0]}" for i in self.cols_to_drop]
                drop_cols.extend([f"{i}{self.years[1]}" for i in self.cols_to_drop])
                X.drop(drop_cols, axis=1, inplace=True)
            else:
                X.drop(self.cols_to_drop, axis=1, inplace=True)
            return X
        elif type(X) == np.ndarray:
            return np.delete(X, self.cols_to_drop, 1)

class DummyGender(BaseEstimator, TransformerMixin): #works but replaces original df as well
    """Converts a categorical sex column into a column with a 0 for Female and 1 for Male
    If gender is unkown, enter "nan" in that row.
    Parameters:
        sex_col (str): the name of the sex column in the original dataframe
        drop (bool): if True, drop the original sex column 
    Returns:
        A dataframe with the original data, but with a column that converts sex into a dummy 
        instead of categorical variable.
    """
    def __init__(self, sex_col="pr_sex_code", drop=True):
        self.sex_col = sex_col
        self.drop = drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            X.loc[X[self.sex_col] == "Unknown", self.sex_col] = np.nan
            X[self.sex_col] = pd.Series(np.where(X[self.sex_col] == "Female", 1, 0),
                                        X[self.sex_col].index)
            return X
        elif type(X) == np.ndarray:
            sex_dummy = np.zeros(shape = X[:, self.sex_col].shape)
            sex_dummy[X[:, self.sex_col] == "Female"] = 1
            sex_dummy[X[:, self.sex_col] == "Unknown"] = np.nan #FIXME gen unknown gender using name and relationship code
            return np.c_[np.delete(X, self.sex_col, 1), sex_dummy]

class DummyRace(BaseEstimator, TransformerMixin): #works, but needs more testing
    #FIXME condense race to black, white, latino, indian, asian
    """Convert race into several dummy variables for race: white, latino, black, indian, asian, etc.
        Still in progress.
    Parameters:
        race_col (string): the name of the race column in the dataset
        drop (bool): if True, drop the original race column
    Returns:
        The dataset with several new dummy race variables.
    """
    def __init__(self, race_col="pr_race_or_color", drop=True):
        self.race_col = race_col
        self.drop = drop
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            black = ["Colored", "Mulatto", "Negro", "Dark", "Brown", "Creole",
                     "1/2 Black", "1/4 Black", "1/8 Black", "Colored &amp; White",
                     "African"]
            white = ["Italian", "Portuguese", "Russian", "Spanish", "Polish",
                     "German", "Slavic", "1/2 White", "Scottish", "English",
                     "Australian", "Jewish", "French", "Hungarian", "Greek",
                     "1/4 White", "Swedish", "Part Caucasian", "3/4 White",
                     "Canadian", "Finnish", "Swiss", "Yugoslavian", "2/3 White",
                     "White 3/4 Indian", "English &amp; Canadian", "American",
                     "Texan"] #FIXME visualize labelled race data to check if these bins are true.
            indian = ["1/2 Indian","1/4 Indian", "Red", "Hindu", "American Indian",
                      "Sioux Indian", "Cippewa Indian", "Cherokee Indian",
                      "Indian &amp; White", "Seminole Indian", "Part Indian",
                      "1/8 Indian", "3/4 Indian", "Indian &amp; Black",
                      "Cree Indian", "White &amp; Indian", "East Indian",
                      "Atquanachuke Indian", "Brule Sioux Indian",
                      "1/2 Indian 1/2 Black", "Indian &amp; Mexican",
                      "Oneida Indian", "1/2 Cheyenne Indian"]
            latino = ["Mexican", "Mexican &amp; White", "Mestizo", "Cuban",
                      "Puerto Rican", "Brazilian"]
            islander = ["Hawaiian", "Part Hawaiian", "South Sea Islander", "Polynesian",
                        "Filipino", "Polynesian &amp; Caucasian",
                        "Polynesian &amp; Japanese", "Guamanian",
                        "Polynesian &amp; Chinese", "New Zealander"]
            asian = ["Japanese", "Chinese", "Yellow", "Oriental", 
                     "Japanese &amp; Hawaiian", "Mongolian", "Vietnamese",
                     "Part Chinese", "Korean", "1/2 Japanese",
                     "Chinese &amp; Hawaiian", "Malaysian",
                     "Asian &amp; Hawaiian", "Siamese", "Japanese &amp; White",
                     "White &amp; Chinese", "Japanese &amp; Caucasian",
                     "French &amp; Chinese", "1/8 Japanese"]
            arab = ["Syrian", "Arabian", "Turkish"]
            for i in [(black, "Black"), (white, "White"), (indian, "Indian"),
                      (latino, "Latino"), (islander, "Islander"),
                      (asian, "Asian"), (arab, "Arab")]:
                X.loc[X[self.race_col].isin(i[0]), self.race_col] = i[1]
            X = pd.get_dummies(X, columns=["pr_race_or_color"], drop_first=True)
            return X
        elif type(X) == np.ndarray:
            return #FIXME

class BooleanMatch(BaseEstimator, TransformerMixin):
    """Compare two columns, generating True if they match, False if they don't.
    Parameters:
        vars_to_match (list): The variables that you want to compare across years. These should be base names without the year.
        years (list): The list of years you are comparing. Default is ["1910", "1920"]
    Returns:
        Data with bool columns appended.
    """
    def __init__(self, vars_to_match=[], years=["1910", "1920"]):
        self.vars_to_match = vars_to_match
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        if type(X) == pd.core.frame.DataFrame:
            for var in self.vars_to_match:
                try:
                    X[f"{var}_match"] = X[f"{var}_{self.years[0]}"] == \
                                        X[f"{var}_{self.years[1]}"]
                except:
                    X[f"{var}_match"] = X[f"{var}{self.years[0]}"] == \
                                        X[f"{var}{self.years[1]}"]
            return X
        elif type(X) == np.ndarray:
            return np.c_[X, X[:, self.var_to_match[0]] == X[:, self.var_to_match[1]]]

class FuzzyBoolean(BaseEstimator, TransformerMixin):
    """Run a fuzzy compare of two values. Better to use absolute distance where possible.
    """
    def __init__(self, vars_fuzzy=[], years=["1910", "1920"], year_diff=2):
        self.vars_fuzzy = vars_fuzzy
        self.years = years
        self.year_diff = year_diff
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        for var in self.vars_fuzzy:
            try:
                X[f"{var}_fuzzy"] = X[f"{var}_{self.years[0]}"] == X[f"{var}_{self.years[1]}"]
            except:
                X[f"{var}_fuzzy"] = X[f"{var}{self.years[0]}"] == X[f"{var}{self.years[1]}"]
        return X

class EuclideanDistance(BaseEstimator, TransformerMixin):
    """Calculate the Euclidean distance between variables in each year.
    Parameters:
        variables (list): The variables that you want to calculate Euclidean distance for. These should be base names without the year.
        new_cols  (list): The names of the columns of the generated Euclidean distances.
        years     (list): The two years in your dataset. Default is ["1910", "1920"]
    Return:
        Dataset with euclidean distance columns appended.
    """
    def __init__(self, variables=[], new_cols=[], years=["1910", "1920"]):
        self.variables = variables
        self.new_cols = new_cols
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        for var, col in zip(self.variables, self.new_cols):
            try:
                #X.dropna(subset=var, inplace=True)
                var_list_year1 = []
                var_list_year2 = []
                for i in var:
                    var_list_year1.append(f"{i}{self.years[0]}")
                    var_list_year2.append(f"{i}{self.years[1]}")
                X[col] = np.linalg.norm(X[var_list_year1].values - X[var_list_year2].values, axis=1)
            except:
                print("There was an error")
                #X.dropna(subset=var, inplace=True)
                var_list_year1 = []
                var_list_year2 = []
                for i in var:
                    var_list_year1.append(f"{i}_{self.years[0]}")
                    var_list_year2.append(f"{i}_{self.years[1]}")
                X[col] = np.linalg.norm(X[var_list_year1].values - X[var_list_year2].values, axis=1)
        return X

class ColumnImputer(BaseEstimator, TransformerMixin):
    """Impute missing values for columns. Only median is currently implemented.
    Parameters:
        cols (list): The list of columns that you want to impute. These are names without the year.
                     All columns will have the same imputation.
        years (list): A list of the two years in the dataset. Default is ["1910", "1920"]
    Return:
        Dataset with corrected missing values for the specified columns.
    """
    def __init__(self, cols, years=["1910", "1920"]):
        self.cols = cols
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        for c in self.cols:
            if c == "immigration":
                X[X[f"{c}{self.years[0]}"].isnan()] = 0
                X[X[f"{c}{self.years[1]}"].isnan()] = 0
            X[X[f"{c}{self.years[0]}"].isnull()] = X[f"{c}{self.years[0]}"].median()
            X[X[f"{c}{self.years[1]}"].isnull()] = X[f"{c}{self.years[1]}"].median()
        return X

class CommonalityWeight(BaseEstimator, TransformerMixin):
    """Divide the desired columns by the log commonality of the observation's value
    Parameters:
        cols (list): The names of the columns that you want to transform. These are base names without the year.
        comm_cols (list): The names of the columns that contain commonality weights.
                          Index i of this list should be the commonality of index i in cols.
        years (list): A list of the two years in the dataset. Default is ["1910", "1920"]
    Returns:
        Dataset with desired columns divded by the log of the commonality score.
    """
    def __init__(self, cols=[], comm_cols=[], years=["1910", "1920"]):
        self.cols = cols
        self.comm_cols = comm_cols
        self.years = years
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        for col, comm_col in zip(self.cols, self.comm_cols):
            X[col] = X[col] / np.log1p(X[f"{comm_col}_comm{self.years[0]}"].values)
            X[col] = X[col] / np.log1p(X[f"{comm_col}_comm{self.years[1]}"].values)
        return X

class CrosswalkMerge(BaseEstimator, TransformerMixin): #works
    """
    Parameters:
        crosswalk_file (string): the filename of the crosswalk file you need to access or that you
                                 have already been using.
        sql_table_name (string): The name of the SQL table of the census data that you want to pull
        years (list): the list of census years that you want to pull from
        index:
    Returns:
        A dataframe of labeled ARK lines that has census data merged on from the years we care about.
    """
    def __init__(self, crosswalk_file, sql_table_name="None", years=["1910", "1920"], index=1):
        self.crosswalk_file = crosswalk_file
        self.sql_table_name = sql_table_name
        self.years = years
        self.index = index
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        cw_df = pd.DataFrame()
        if self.crosswalk_file[-3:] == "dta":
            cw_df = pd.read_stata(self.crosswalk_file)
        elif self.crosswalk_file[-3:] == "csv":
            cw_df = pd.read_csv(self.crosswalk_file)
        elif self.crosswalk_file[-3:] == "sql":
            new_conn = sqlite3.connect(self.crosswalk_file)
            cw_df = pd.read_sql_query(f"SELECT * FROM {self.sql_table_name}", new_conn)
        census1 = X.iloc[0:self.index - 1, :]
        census1.drop(f"ark{self.years[1]}", axis=1, inplace=True)
        census2 = X.iloc[self.index:X.shape[0] - 1, :]
        census2.drop(f"ark{self.years[0]}", axis=1, inplace=True)
        census1 = census1.merge(cw_df, how="inner", on=f"ark{self.years[0]}")
        X = census1.merge(census2, how="inner", on=f"ark{self.years[1]}")
        return X
 
#TODO Function to run update merge
class UpdateMerge(BaseEstimator, TransformerMixin):
    """This is a new update merge function
    It's not quite done yet.
    Parameters:
        on (string): the name of the column index that you want to merge on.
        how (string): The merge method that you want to use
        (unused now I think:)
        left_on (string): The name of the column index you want to merge on in "X"
        right_on (string): The name of the column index you want to merge on in "df2"
    Returns:
        a merged pandas DataFrame.
    """
    def __init__(self, on, how, left_on, right_on):
        self.on = on
        self.how = how
        self.left_on = left_on
        self.right_on = right_on
        self.df2 = pd.DataFrame()
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        X = X.merge(self.df2, on=self.on, how=self.how)
        for i in X.columns:
            if i[-2:] == "_x":
                X[i[-2:]] = X[f"{i[-2:]}_y"].fillna(X[f"{i}"])
    
class Bin(BaseEstimator, TransformerMixin): #FIXME
    def __init__(self, bin_vars, index):
        """
        Parameters:
            bin_vars (list): list of variable names to bin on
            index (int): index starting the second census

        Returns:
            (dataframe): binned observations across two censuses
        """
        self.bin_vars = bin_vars
        self.index = index
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        #Generate "bin" variable that indexes unique bins for given bin variables
        X = X.merge(X[self.bin_vars].drop_duplicates(self.bin_vars).reset_index(), on=self.bin_vars)
        X = X.rename(columns={"index":"bin"})
        #merge censuses on bins
        #Rename every column to be census-unique except "bin"
        orig_columns = X.columns.values[:-1]
        first_columns = [column_name + "_1" for column_name in orig_columns]
        second_columns = [column_name + "_2" for column_name in orig_columns]
        first_census = X.iloc[:self.index].rename(columns=dict(zip(orig_columns, first_columns)))
        second_census = X.iloc[self.index:].rename(columns=dict(zip(orig_columns, second_columns)))
        #how="left" keeps obs. from the first census that don't match any from second census
        #   how="inner" doesn't keep those observations, but I believe both of these
        #   perform the join-by type of operation we want
        return first_census.merge(second_census, how='inner', on="bin")

class AddData(BaseEstimator, TransformerMixin):
    def __init__(self, file_name, merge_col, use_cols=[]):
        self.file_name = file_name
        self.merge_col = merge_col
        self.use_cols = use_cols
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        new_col = pd.DataFrame()
        if self.file_name[-3:] == "dta":
            new_col = pd.read_stata(self.file_name)
        elif self.file_name[-3:] == "csv":
            new_col = pd.read_csv(self.file_name)
        if len(self.use_cols) > 0:
            new_col = new_col[self.use_cols]
        X = X.merge(new_col, on=self.merge_col, how="left")
        return X

def load_data(state="Delaware", year="1910", variables=["*"]): #works
    census_files = "R:/JoePriceResearch/record_linking/data/census.db"
    conn = sqlite3.connect(census_files)
    variables = ", ".join(variables)
    return pd.read_sql_query(f"SELECT {variables} FROM basic{year} LIMIT 1000;", conn) #FIXME remove LIMIT

class LoadData(BaseEstimator, TransformerMixin): #FIXME test
    """I think this one is not done yet
    Parameters:
        state (list): The states that you want to pull data for.
        year (list): The census year that you want to pull data for.
        variables (list): The variables that you want to pull.
    Returns:
        
    """
    def __init__(self, state="Delaware", year="1910", variables=["*"]):
        self.census_files = "R:/JoePriceResearch/record_linking/data/census.db"
        self.conn = sqlite3.connect(self.census_files)
        self.variables = variables
        self.year = year
    def fit(self, X=None, y=None):
        return self
    def transform(self, X=None):
        query = f"SELECT {self.variables} FROM basic{self.year} LIMIT 1000;"
        return pd.read_sql_query(query, self.conn) #FIXME remove LIMIT
