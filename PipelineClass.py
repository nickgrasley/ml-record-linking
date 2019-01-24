"""
January 23, 2019
Ben Christensen
"""

class ModelTrainer():
    """This class will allow for the testing of different models on the same
    training data"""
    def __init__(self, models, hyper_prms, prm_vals, cv, scoring):
        """
        #Maybe just have param_grid be one of the paramters???
        Parameters:
            models (list): a list of actual scikitlearn models on which
                to train the data (i.e. LogisticRegression after importing
                it from sklearn.linear_model)
#fix this   hyper_prms (list): a list of lists containing the names of the
                hyperparameters to use in each model
                (i.e. {"max_depth": [4, 5, 6, 7], "n_estimators":[100, 1000]})
#fix this   prm_vals (list): a list of
                References:
                GradientBoostingClassifier from sklearn.ensemble
                    n_estimators (int)
                    learning_rate(float)
                    max_depth(int)
                    max_features(float)
                    random_state(int)
                RandomForestClassifier from sklearn.ensemble
                    n_estimators(int)
                    max_features(float)
                    max_depth(int)
                    n_jobs(int)
                LogisticRegression from sklearn.linear_model

                XGBClassifier from xgboost
                    max_depth(int)
                    learning_rate(float)
                    n_estimators(int)
            cv (int): the number of partitions to use in cross validation
            scoring (str): scoring type for cross validation (i.e. "f1")
                See scikit-learn.org/stable/modules/model_evaluation.html
                for possible scoring types
        """
        self.models = models
        self.hyper_prms = hyper_prms
        self.cv = cv
        self.scoring = scoring
        self.times = list()
        self.pipe = Pipeline([("classifier": model) for model in self.models])
        self.param_grid =
            dict(zip())
        [
            {"classifier": [model],
             "classifier__" +

    def fit(self):
        """Something here"""
    def predict(self):
        """Something here"""
    def best_params(self):
        """Something here"""
    def best_score(self):
        """Something here"""
    def all_scores(self):
        """Something here"""
    def confusion_matrix(self):
        """Something here"""
