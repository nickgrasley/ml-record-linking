# Splycer Bug Report

#### xgboost_match.py
* **Line 97**: index out-of-bounds error, [0,1,4] should be [0,1,3]
* **Line 47-48**: `create_training_set` does not work properly when record_sets are of record_db type. More on this in  othernotes.
#### pairs_set.py
* **Line  109**: SQL error; backslash should be removed
#### record_set.py
* **Line 78-83**: `get_records` method should return records in order of the uid list passed in as . If returned unordered, then records will be compared randomly when running feature engineer.
#### blocker.pyx
* multiple issues, I coded up a stable working version located in AA_model folder (however, I haven't implemented the chunksize feature yet, which we will need to do blocking at a census wide scale)
#

### other notes
training sets are not being created correctly when using a database as the internal record_set structure. Following error is thrown: `Can only compare identically-labeled Series objects`

Error occurs when trying to perform a boolean match between two record sets. The problem is one of the record dataframes has more rows than the other. Why they have different row sizes is beyond me.
