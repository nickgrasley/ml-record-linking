Author: Nick Grasley (nicholas.a.grasley@gmail.com)

This is a guide for how to run the xgboost pipeline from start to finish

# Splycer

There have been some major redesigns of the Splycer framework. Here is the new structure of it.

## Base.py

This handles the root problem of record linking. The root problem is that you have 
two record sets and you need to compare records between them. 

RecordBase is the [interface](https://dzone.com/articles/when-to-use-abstract-class-and-intreface) 
that defines how all record linking methods will access a record set. It must have 
get_record() (and __get_item__()), which accesses the record set by a unique id
for each record. This allows the Splycer package to be agnostic to how the record
data is stored for a user. I have defined a couple common record sets as examples.
(note: the abc package used here allows Python to define abstract classes.)

The first example is RecordDict. This implements a record set as a dictionary.
A dictionary is great if you only have to look up uids because its inner structure
is built for extremely fast lookup. RecordDB will be coded up for working on sql
servers. RecordDataFrame will be coded up for working with DataFrames.

CompareBase is the interface that defines how record sets are compared. Since
comparing a record in record set 1 to every record in record set 2 is often
computationally infeasible, CompareBase defines a container for record pairs
that will be compared by a record linking model. Once again, record linking
models are programmed to be agnostic to the way that compares are stored as
long as you implement the methods in CompareBase. A Compressed Sparse Row
(CSR) matrix and a sql server implementation are the most suitable for our
purposes.

## FeatureEngineer.py

This class hasn't changed much since the original Splycer package. Its
intended use is to build a pipeline of comparison functions to modify
the original data. All of the available features are listed in the class.
To build a pipeline, use the add_feature() method for each feature you want, 
and then transform() to run the pipeline.

## preprocessing.py

These are all the available comparisons and feature modifications that I've
coded up. I'm not a fan of the preprocessing name, but I'll keep it for now.
If you want to implement another comparison, you code up a new class (technically
a functor) that inherits from BaseEstimator and TransformerMixin. These are
scikit-learn classes that allow you class to be included in a pipeline. Then,
you must write a fit() and transform() method. If you don't have to fit() your
comparison, you can just write `return self`. (note: I'll put a link to the
scikit-learn documentation on this)

## Linker.py

This defines the interface for a linking algorithm. Originally, I had a Splycer
class that attempted to be an all-in-one stop for every single linking method.
However, this essentially turned Splycer into a [god object](https://en.wikipedia.org/wiki/God_object). 
Instead, I've defined a Linker interface to standardize linking algorithms but
still allow some flexibility to each algorithm. It might be useful to code up
these linkers in some sort of defined pipeline, but that is for another day.

## XGBoostMatch.py

This serves the same function as the previous XGBoostMatch class, but it's been
modified to inherit from Linker. This one is also more agnostic to the type
of model you specify, so you can do grid searching or not. In fact, it's
so agnostic that it will likely work with any classifier a la scikit-learn.
Maybe I'll rename it SupervisedLinker.py. (note: I also need to implement
the duplicate removal part of it.)

## HouseholdMatch.py

Issac Riley coded this up. It still needs to be implemented in the Linker interface,
but that is for another day. It could also be implemented as a supervised learning
approach and coded up as a model in the style of a scikit-learn classifier.

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
