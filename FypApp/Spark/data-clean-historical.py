import pandas as pd
import numpy as np

# Read the dataset
df = pd.read_csv("D:\CSIT321P\Project\Changes-master\FypApp\DataStorage\data\combined_csv.csv")

# Dropping unrelated columns
if 'Unnamed: 0' in df.columns:
    df = df.drop(columns=['Unnamed: 0'])


###################################### DUPLICATES ############################################
# Check for duplicates
print(f"Checking duplicates in the dataset...")
duplicates = df.duplicated()

# Count the number of duplicates
num_duplicates = duplicates.sum()

print(f"There are {num_duplicates} duplicates in the dataset.")

# Drop the duplicates
if num_duplicates > 0:
    print(f"Cleaning duplicates...")
    df = df.drop_duplicates()
    print(f"Clean.")
else:
    print(f"Done.")


###################################### MISSING ############################################
#Checking missing columns
print(f"Checking missing values in the dataset...")
missing_values = df.isnull().sum()

if missing_values.any():
    cat_cols = df.select_dtypes(include=['object']).columns
    num_cols = df.select_dtypes(exclude=['object']).columns
    df[cat_cols] = df[cat_cols].fillna(df[cat_cols].mode().iloc[0])
    df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
else:
    print(f"No Missing Values.")


##############################################################################################
# Cleaned datasets
df = pd.concat([df[num_cols], df[cat_cols]], axis=1)
check = df.isnull().sum()
if check.any():
    print(f"Failed to clean datasets.")
else:
    print(f"Successfully cleaning data.")

print("...")