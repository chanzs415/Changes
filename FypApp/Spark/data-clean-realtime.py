# "D:\CSIT321P\Project\Changes-master\FypApp\DataStorage\data\weather_test3.csv"
import pandas as pd
import numpy as np
from datetime import datetime

# read the dataset into a DataFrame
df = pd.read_csv("D:\CSIT321P\Project\Changes-master\FypApp\DataStorage\data\weather_test3.csv")

# convert the 'time_epoch' column to a datetime object
df['time'] = df['time_epoch'].apply(lambda x: datetime.fromtimestamp(x))

# drop the 'time_epoch' column
df.drop(columns=['time_epoch'], inplace=True)

# filter the rows with inconsistent 'is_day' values
inconsistent_rows = df.loc[(df['is_day'] == 1) & ((df['hour'] < 6) | (df['hour'] > 18)) | 
                           (df['is_day'] == 0) & ((df['hour'] >= 6) & (df['hour'] <= 18))]

# update the inconsistent 'is_day' values
df.loc[inconsistent_rows.index, 'is_day'] = 1 - df.loc[inconsistent_rows.index, 'is_day']

# Use string manipulation to extract the 'text' value from 'condition' column
clear_values = df['condition'].str.split("'text': '").str[1].str.split("',").str[0]
df['condition'] = clear_values

# Check the 'humidity' column for values outside the range of 0-100%, and correct any values that are out of range.
df.loc[df['humidity'] < 0, 'humidity'] = 0
df.loc[df['humidity'] > 100, 'humidity'] = 100

# Check the 'uv' column for values outside the range of 0-12, and correct any values that are out of range.
df.loc[df['uv'] < 0, 'uv'] = 0
df.loc[df['uv'] > 12, 'uv'] = 12

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
# check for missing values
print(f"Checking missing values in the dataset...")
missing_values = df.isnull().sum()

if missing_values.any():
    print(f"There are missing values.")
    print(f"Checking missing values...")
    # Check if any dates are missing
    daily_data = pd.DataFrame(pd.date_range(start=df['time'].min(),end=df['time'].max()))
    daily_data.rename(columns={ daily_data.columns[0]: "time" }, inplace = True)
    daily_data.describe()
    # Add the missing dates
    print(f"Filling missing values...")
    df1 = pd.merge(df,daily_data,on='time',how='outer')
    df1 = df1.sort_values(by=['time'])
    # After adding missing dates there will be null values for the data
    # Use linear interpolation to fill up nulls (forward fill & backward fill for time-series analysis)
    df2 = df1.interpolate(method='linear', axis=0).ffill().bfill()
    df2.head(10)
    print(f"Done.")
else:
    print(f"No missing values in the datasets.")


###################################### OUTLIERS ############################################
# Outlier Detection using Inter Quartile Range
def out_iqr(s, k=1.5, return_thresholds=False):
    # calculate interquartile range
    q25, q75 = np.percentile(s, 25), np.percentile(s, 75)
    iqr = q75 - q25
    # calculate the outlier cutoff
    cut_off = iqr * k
    lower, upper = q25 - cut_off, q75 + cut_off
    if return_thresholds:
        return lower, upper
    else: # identify outliers
        return [True if x < lower or x > upper else False for x in s]
    

# For comparison, make one array each at varying values of k.
df3 = df2.drop(columns=['time','location','condition','wind_dir'],axis=1)
iqr1 = df3.apply(out_iqr, k=1.5)
iqr1.head(10)

if df3.any().any():
    print(f"Checking outliers...")
    # Treat the outliers
    for column in df3:
        df3[column] = np.where(iqr1[column] == True,'NaN',df3[column])
    print(f"Removing outliers...")
    cols = df3.columns
    df3[cols] = df3[cols].apply(pd.to_numeric, errors='coerce')
    # Use linear interpolation to fill up nulls
    df = df3.interpolate(method='linear', axis=0).bfill().ffill()
    df2['time'] = pd.to_datetime(df2['time'])
    df = pd.concat([df2[['time','location','condition','wind_dir']],df], axis=1)
    df.head(10)
    print(f"Done.")
else:
    print(f"There are no outliers in the datasets.")

# define columns to exclude from renaming
exclude_cols = ['location','time']

# filter possible categorical data (rename)
print(f"Filtering categorical data..")
for col in df.columns:
    if col not in exclude_cols and df[col].dtype == 'object':
        new_col_name = col + '(categorical)'
        df = df.rename(columns={col: new_col_name})
        df.head()


print(f"Done.")

print(f"Data cleaning done.")

print("...")