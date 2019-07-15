Author: Nick Grasley (nicholas.a.grasley@gmail.com)

This is a guide for how to run the xgboost pipeline from start to finish
This is a guide for how to run the xgboost pipeline from start to finish.

# Data

First, you need to get all of the data on the SQL server.
1. open Microsoft SQL Server Management Studio
2. Select:
	Server type: Database Engine
	Server name: fhss-sql17research2\research
	Authentication: Windows Authentication (the other ones do not work
			for some reason. You have to have permission to access
			the SQL server on your account for this to work.)
3. Open Databases > Price > Tables

You'll see all the tables there. For each census, the name, variables, data types,
and keys have to be *exactly* the same. After you upload all of the data, you need
to compile all of the data using the compile_census.sql script. This will give you
dbo.compiled_year. This isn't completely necessary, but it makes the code run faster
by doing a lot of the binning up front. You then only have to merge in the name vectors.

## Location of Data Files

(Replace 'year' with the year you are uploading to the SQL server)
1. compact_census - R:\JoePriceResearch\record_linking\data\census_compact\year\censusyear.dta
2. compact_ark - R:\JoePriceResearch\record_linking\data\census_compact\year\ind_arkyear.dta
3. compact_first_comm - R:\JoePriceResearch\record_linking\data\census_year\data\obj\gn_comm.dta
4. compact_last_comm - R:\JoePriceResearch\record_linking\data\census_year\data\obj\surn_comm.dta
5. compact_place - R:\JoePriceResearch\record_linking\data\census_compact\year\places_v2.dta (use v2 when available)
6. compact_stringnames - R:\JoePriceResearch\record_linking\data\census_compact\year\stringnamesyear.dta
7. crosswalk_wide_pid_ark - R:\JoePriceResearch\record_linking\data\census_tree\pid_ark.dta (have to reshape it. DO NOT MODIFY THE ORIGINAL FILE)
8. crosswalk_first_vec - R:\JoePriceResearch\record_linking\projects\fixing_lastnames\firstnametovec.dta
9. crosswalk_last_vec - R:\JoePriceResearch\record_linking\projects\fixing_lastnames\lastnametovec.dta
10. crosswalk_geo_bplace - R:\JoePriceResearch\record_linking\data\census_compact\dictionaries\bplace_lat_lon.dta
11. crosswalk_geo_res - R:\JoePriceResearch\record_linking\data\crosswalks\census_towns_coor_v6_small.dta
12. crosswalk_nickname - R:\JoePriceResearch\record_linking\data\crosswalks\nickname_crosswalk.dta

## SQL Server Performance Tips

### Creating keys

The SQL server only runs fast if you construct your data correctly. The most important
part is that every table should have a primary key. If you have a primary key, the
SQL server organizes your data into what is known as a B-tree which sorts your data
on your primary key and has fast lookup of elements. This enables fast merging on
primary keys. To set up a primary key:
1. Open a new query script
2. Type `alter table table_name
	 add constraint constraint_name primary key (ID);'
3. Hit run
It's more ideal to create your table with the primary key in the first place. You
can do this either when you are importing the data or when you create a blank table.

If you aren't merging on a primary key, you should set up a foreign key between the
columns you are merging. This ensures that your non-primary key column is properly
indexed so it can search your table quickly. The syntax for creating a foreign key
is `alter table table_name add constraint constraint_name foreign key (ID) references table2_name(ID2);`.
See 1900-1920 censuses for all the primary and foreign keys that you need.

### Dropping Duplicates

Some tables might have duplicates which will prevent you from creating a primary key.
To drop duplicates, use this code I got from a stack overflow page:
`WITH CTE AS(
   SELECT [col1], [col2], [col3], [col4], [col5], [col6], [col7],
       RN = ROW_NUMBER()OVER(PARTITION BY col1 ORDER BY col1)
   FROM dbo.Table1
)
DELETE FROM CTE WHERE RN > 1`.

Replace col1 with the primary key column that you are trying to create and the
other cols are just the other columns of your table.

### Large Query Tips

Before you run a large query, it's helpful to check if you set up your data correctly.
You can see this if you click on the 'Display Estimated Execution Plan' button, which
is the button to the right of the checkmark with three squares and a triangle. This
will show you what SQL is going to do under the hood. If you set up the data correctly,
you should see lots of hash merges and clustered index scans. If you see any sorting,
you are probably missing a key in one of the tables.

If you are saving large tables to the server after running a query, make sure to do
your query in chunks. If you don't, you may get an error about a temp log file being full.
The SQL server buffers your data in a log file before saving, and this can fill up
quickly. I've found that batches of 10 million are typically fine. To run batches,
I usually use a where clause on index, like so:
`where census.index_year >= chunk_start and census.index_year < chunk_end`.

### Transaction Log Full Error

If you start getting errors about the transaction log file being full, this is 
because IT has set up our database with a full recovery model. This means that
it can restore our data if things go haywire. Unfortunately, this also means
that the transaction log has to be cleared occaisonally. I don't know how to
do this, so you should email Bruce (bruce@byu.edu) if it happens again.

## Setting up a DSN

DSN (Data Source Name) is the only way that I could get Python to interact with
the SQL server. Here's how to set it up.

1. Search for Administrative Tools on your local computer.
2. Click ODBC Data Sources (64-bit).
3. Enter your password.
4. Make sure you have User DSN selected from the tabs at the top.
5. Click Add...
6. Select ODBC Driver 13 for SQL Server.
7. Enter
	Name: rec_db
	Description: (don't need)
	Server: fhss-sql17research2\research
   and click next
8. Click next again (With Integrated Windows authentication should be selected)
9. Check Change the default database to: and select Price. Click next.
10. Click Finish
11. Click the Test button to make sure everything is set up correctly.
12. Press finish/OK/Apply until you exit out of administrative tools.

# Quick Note on Splycer Class

The Splycer class is used throughout the training and predicting, but it was only
halfway completed. You'll notice that some of the functionality that should be in
the Splycer class is done explicitly in the code. You have to live with this for
now, but hopefully we get this cleaned up eventually. Also, make sure to only
pickle your xgboost model and not the full Splycer class. If any changes are
made to the Splycer class, you cannot unpickle your previously saved objects
because pickle uses the current module to load in the data. The xgboost class
is much more stable, so you can save it and expect it to load again.

# Training

You will need to train a new model for any linking involving the 1940 census because
there is not immigration year in the 1940 census unfortunately. The script I used to
train the current model is called training.py. There are a couple elements of this
script that might break because the code under the hood had some changes. Specifically,
you need to delete variables like in link_census.py line 118 and also the exact variables
dropped on line 44. Notice last name vectors are not dropped here because they are deleted
within the EuclideanDistance class (I couldn't get first name vectors to do the same for some reason).
This is to reduce the memory consumption as fast as possible. You can also implement the
SQL code of link_censuses.py (line 80) if you don't want to compile the entire census for
all of the arks. Also, remember to drop immigration from the script wherever it's mentioned.

First, you need to create the training data. You can do that by uploading an ark/index crosswalk
to the SQL server. Then feed those the earlier census year of those arks into the binner
with the true ark pair still attached. Finally, compare the true ark to the binned ark to get
0s and 1s for whether the arks match or not. Feed this data into your training script, and you
should have a new model afterwards. Just make sure to call it something different so you don't
save over the old model.

To reiterate:
1. training.py handles training of a new model.
2. change the way variables are dropped to that of link_censuses.py (or fix it, but I don't 
   recommend this since it already runs)
3. upload ark/index crosswalk (similar to wide_pid_ark_1910_1920 already on the SQL server).
4. Run the early census arks through the Binner, keeping the true ark (you'll have to rename
   the true ark and copy paste DynamicBinner.sql to a new file to modify it to keep the true ark)
5. Update the way training.py interfaces with the SQL server so that it's like link_censuses.py
   (or create a table like training_data on the SQL server)
6. Run the training, saving the model as a new name (something to indicate it's for 1940).

I would also recommend adding a test to make sure that the columns are the features you
expect to train on. Dropping variables can often be a tricky business, and some might
linger longer than expected and make it into your model.

## 'Module Not Found' Error

Sometimes it can't find the Splycer class or its pieces. To fix this, you either have
to use `sys.path.append()` with the path to the Splycer class or do some weird
combination of `from Splycer.Splycer import Splycer`.

# Blocking

Blocking is done by Splycer/DynamicBinner.sql. All you need to do to this script is
change the years and chunk numbers. This works in 10 million person batches, so you
need to calculate how many chunks you need to cover everyone in the first census
(e.g. there are ~75 million people in the 1900 census, so you need chunks 0-7 for
any linking involving the 1900 census.). This creates dbo.training_indices_year1_year2_num
for each chunk. If you need extra room, just delete old ones.
Note: I don't believe I blocked on immigration, but it could break if I did and
you're linking 1940. You can also add/delete blocks if the blocking method for
later censuses is too strict/relaxed. You must retrain a new model if you do this.


# Predicting

After blocking is completed, you can begin prediction. The link_censuses.py script handles
all of the prediction. All you have to do is change the years (line 33), the model you're
using (line 69), and enter the batch number that you want to run (function argument).
Remember that you have to set up a DSN on each computer that you're running this script on.
So far, I have run link_censuses.py on up to 8 computers at once with no errors from the
SQL server. However, I try to space out when I start the script so that they are not all
pinging the server at once. The output will be index_year1, index_year2, prob_match.

# Handling Duplicates

I tested how to drop duplicates in R:/JoePriceResearch/record_linking/data/preds3/duplicates_rule_handling.ipynb.
I would put this code into a dedicated Python function outside of a notebook so it's easier
to use. The most important part of this notebook is that we keep a match if it's probability
of being a match is above 0.96 and all of the other duplicates have a probability that is
0.05 less than the max probability. I then combine the results of dropping duplicates from both census
years and drop any lingering duplicates from the merge to get the final result. Dropping duplicates
isn't the most pressing part of this project, so if it's not working out, just wait for me to get
back to help out with it.