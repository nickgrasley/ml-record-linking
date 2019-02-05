# ml-record-linking
This package handles the automation of machine learning models for record linking.

## Files
- preprocessing/preprocessing.py: Contains sklearn-style transformers to be run in a pipeline. Performs operations such as dropping data,
computing Jaro-Winkler scores, boolean matching, etc.
- preprocessing/preprocess_pipeline_example.py: Runs a single simple model using the transformers of preprocessing.py
- PipelineClass.py: Automates the pipeline for running multiple models.
- Cleaned_Nebraska_XGBOOST.ipynb: Original feature engineering and model prediction by Amanda Marsden. Used as a reference.

##TODO List
- [ ] Fix PipelineClass.py and make it into an estimator to use in other pipelines
- [ ] Add name vectors to preprocessing
- [ ] Create bigram/trigram transformer
- [ ] Add geodistance
- [ ] Add feature interactions parameter to xgboost
- [ ] Implement supercomputer capabilities to run models faster (does a GPU have a significant performance gain for xgboost?)
- [ ] Implement a stacking model
- [ ] Create a test file to make sure all elements of the model continue to work after changes.
