class CrosswalkMerge(BaseEstimator, TransformerMixin): #FIXME
    def __init__(self, crosswalk_file):
        self.crosswalk_file = crosswalk_file
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        return self

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
        X = X.merge(X[bin_vars].drop_duplicates(bin_vars).reset_index(), on=bin_vars)
        X = X.rename(columns={"index":"bin"})
        #merge censuses on bins
        #Rename every column to be census-unique except "bin"
        orig_columns = X.columns.values[:-1]
        first_columns = [column_name + "_1" for column_name in orig_columns]
        second_columns = [column_name + "_2" for column_name in orig_columns]
        first_census = X.iloc[:index].rename(columns=dict(zip(orig_columns, first_columns)))
        second_census = X.iloc[index:].rename(columns=dict(zip(orig_columns, second_columns)))
        #how="left" keeps obs. from the first census that don't match any from second census
        #   how="inner" doesn't keep those observations, but I believe both of these
        #   perform the join-by type of operation we want
        return first_census.merge(second_census, how='inner', on="bin")
