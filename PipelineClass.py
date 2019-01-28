"""
January 23, 2019
Ben Christensen
"""
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier
from sklearn.pipeline import Pipeline

class ModelTrainer():
    """This class will allow for the testing of different models on the same
    training data"""
    def __init__(self, models, hyper_prms, cv, scoring, n_jobs=-1):
        """
        Parameters:
            models (list): a list of actual scikitlearn models on which
                to train the data (i.e. LogisticRegression() after importing
                it from sklearn.linear_model)
            hyper_prms (list): a list of dicts containing the names of the
                hyperparameters to use in each model and their values
                (e.g. {"max_depth": [4, 5, 6, 7], "n_estimators":[100, 1000]})
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
            scoring (str): scoring type for cross validation (e.g. "f1")
                See scikit-learn.org/stable/modules/model_evaluation.html
                for possible scoring types
            n_jobs (int): Number of processors to use. Defaults to -1 (max)
        """
        self.models = models
        self.hyper_prms = hyper_prms
        self.cv = cv
        self.scoring = scoring
        self.times = list()
        self.pipe = Pipeline([("classifier": model) for model in self.models])
        self.n_jobs = n_jobs
        #Create the parameter grid for the Grid Search
        self.param_grid = [dict(zip(["classifier"] + ["classifier__"+param for param in self.hyper_prms[i].keys()], [[self.models[i]]] + [self.hyper_prms[i][param] for param in self.hyper_prms[i].keys()])) for i in range(len(self.models))]
        self.gs = None


    def fit(self, X_train, y_train):
        """Fit every given model to the given data; cross-validate
        Parameters:
            X_train: Data
            y_train: target data (labels)
        """
        self.gs = GridSearchCV(self.pipe, self.param_grid, self.cv, self.scoring, self.n_jobs).fit(X_train, y_train)

    def best_params(self):
        """Give the model and its parameters that performed the best over the
           cross validation"""
        return self.gs.best_params_

    def best_model(self):
        """After fitting, return the best model. This can be then be fit and
           used for predictions."""
        return self.best_params()["classifier"]

    def best_score(self):
        """Return the best score over all model scores"""
        return self.gs.best_score_

    def results(self):
        """Return scores for all models and hyperparameters as pandas dataframe
        """
        return pd.DataFrame(self.gs.cv_results_)

    def predict(self, X_test):
        """Predict classification of X_test given the best model with the best
           parameters
        Parameters:
            X_test(ndarray(n,D)): Data to classify
        Returns:
            ndarray(n,): Predicted classifications
        """
        return self.gs.predict(X_test)

    def report(self, X_test, y_test):
        """Show classification report given the labels of the test data
        Parameters:
            X_test(ndarray(n,D)): Test data
            y_test(ndarray(n,)): labels of test data
        """
        print(classification_report(y_test, self.predict(X_test)))
