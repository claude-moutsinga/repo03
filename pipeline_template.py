# -*- coding: utf-8 -*-
"""
Data Pipeline for ETL

In this code-along, we'll focus on extracting data from flat-files. 
A flat file might be something like a .csv or a .json file. 
The two files that we'll be extracting data from are the apps_data.csv and 
the review_data.csv file. To do this, we'll used pandas. Let's take a closer look!

"""

# Import Pandas

import pandas as pd
#import os
#os.path.exists()

# Ingest these datasets into memory using read_csv and save as apps and reviews variable
apps = pd.read_csv("apps_data.csv")
reviews = pd.read_csv("apps_data.csv")

# Take peak at the two data sets with print function or view in variable explorer


# View the columns, shape and data types of the data sets
print(apps.columns)
print(apps.shape)
print(apps.dtypes)
# Is there a single pandas method that does this?
print(apps.info())
"""
The code above works perfectly well, but this time let's try using DRY-principles to build a function to extract data.

Create a function called extract, with a single parameter of name file_path.
Sprint the number of rows and columns in the DataFrame, as well as the data type of each column. Provide instructions about how to use the value that will eventually be returned by this function.
Return the variable data.
Call the extract function twice, once passing in the apps_data.csv file path, and another time with the review_data.csv file path. Output the first few rows of the apps_data DataFrame.

Extracting is one of the most tricky things to do in a data pipeline, always try to know much as you can about the source system, here its just a flat file which is quite simple.
"""

# Extract Function
def extract(file_path):
    # Read the file into memory
    data = pd.read_csv(file_path)
    # Now, print the details about the file
    print(data.info())
    # Print the type of each column
    
    # Finally, print a message before returning the DataFrame
    print("Data has been ingested")
    
    return data

# Call the function (create apps_data and reviews_data)
apps_data = extract("apps_data.csv")
reviews_data = extract("review_data.csv")
# Take a peek at one of the DataFrames


"""
We have extracted the data and now we want to transform them. 
Now we are going to use the food and drink category. 
So we are going to write a function that provides a top apps view for food and drink. 
So we will write a function that takes in 5 parameters, drop some duplicates
find positive reviews and filter columns. Then only keep a few columns. 
Then join it by min_rating and min_reviews, order it and check for min rating 
of 4 stars with at least 1000 reviews.

"""
print(apps_data["Category"].unique())

category = "FOOD_AND_DRINK"
min_rating = 4.0
min_reviews = 1000

reviews_data = reviews_data.drop_duplicates()
apps_data = apps_data.drop_duplicates(subset=["App"])

apps_series = apps_data["Category"] == "FOOD_AND_DRINK"

print(apps_series.describe()) 

# Transform Function
def transform(apps, reviews, category, min_rating, min_reviews):
    
    # Drop any duplicates from both DataFrames
    reviews_data = reviews.drop_duplicates()
    apps_data = apps.drop_duplicates(subset=["App"])
    
    # Find all of the apps and reviews in the food and drink category
    apps_series = apps_data["Category"] == category

    subset_apps = apps_data[apps_series]

    reviews_series = reviews_data["App"].isin(subset_apps["App"])

    subset_reviews = reviews_data[reviews_series]
    
    
    # Aggregate the subset_reviews DataFrame
    aggregated_reviews = subset_reviews.groupby("App")["Sentiment_Polarity"].mean().reset_index()
    
    
    # Join it back to the subset_apps table
    joined_apps_reviews = subset_apps.merge(aggregated_reviews, on="App", how="left")
 
    # Keep only the needed columns
    filtered_apps_reviews = joined_apps_reviews[["App", "Rating", "Reviews",
                                                 "Installs", 
                                                 "Sentiment_Polarity"]]  
    # Convert reviews to int
    filtered_apps_reviews["Reviews"] = filtered_apps_reviews["Reviews"].astype(int)
 
    # Create series for min rating and filter dataframe with it
    rating_series = filtered_apps_reviews["Rating"] >= min_rating
    
    apps_with_min_rating = filtered_apps_reviews[rating_series]

    # Create series for min reviews and filter dataframe with it
    reviews_series = apps_with_min_rating["Reviews"] >= min_reviews
    
    top_apps = apps_with_min_rating[reviews_series]
    
    # Sort dataframe
    top_apps = top_apps.sort_values(by=["Rating","Reviews"]
                                    ,ascending=False).reset_index(drop=True)
    
    # Return the transformed DataFrame
    print("data transformed")
    return top_apps
    
# Call the function
top_apps_data = transform(
    apps=apps_data, 
    reviews=reviews_data,
    category="FOOD_AND_DRINK",
    min_rating=4.0,
    min_reviews=1000)

# Show the data
print(top_apps_data)




"""
Ok so last step is to load data, now you can save and keep it as csv but for it is 
a better practice to load it into sqlite DB or similar if its quite a large file.
...what advantages are there of loading into a SQL DB?
"""

# Import sqlite
import sqlite3
# Load Function
def load(dataframe, database_name, table_name):
    # Create a connection object
    con = sqlite3.connect(database_name)
    
    # Write the data to the specified table (table_name)
    dataframe.to_sql(name=table_name, con=con, if_exists="replace", index=False)
    print("OriginalDataframe has been loaded to sqlite\n")
    
    # Read the data, and return the result (it is to be used)
    loaded_dataframe = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=con)
    print("The loaded dataframe has been read from sqlite\n")
    
    # Add try/except to handle error handling and assert to check for conditions
    print(dataframe.shape)
    print(loaded_dataframe.shape)
    """
    
    """
# Call the function
load(
     dataframe=top_apps_data,
     database_name="market_research",
     table_name="top_apps") 
    

    
    
    
    