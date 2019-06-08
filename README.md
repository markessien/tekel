# TekelLib - Alpha 0.1

The Tekel Library is a list manipulation library for machine learning. It provides a bunch of wrapper function around pandas.

Features:
- Easy use of a pandas list
- loading of data from db, csv or json and corresponding persistence back to any of these

There are three classes in Tekel:

TekelList: It represents a list of objects.
TekelObject: Contains the data for a list item
TekelFeature: Contains the information about each feature of an object (equivalent to the columns in the table)

## TekelList


*DB Functions*

* commit_db
* close_db
* set_db_details_from_env
* load_from_db
* save_to_db
* save_item_to_db
* set_db_details
* add_column

*List Manipulation*
* rename_column
* rename_columns
* add
* update

*Data Retrieval*
* get_as_json
* total
* total_from_db
* get_count
* find_first_item
* comma_features
* feature_names
* tabulate
* first

*Loading Data*
* load_from_json
* add_from_json
* load_from_csv
* load_from_db
* set_s3_details

*Special Functions*
* generate_descriptions
* match_with


