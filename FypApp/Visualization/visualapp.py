import streamlit as st
import pandas as pd
import glob
import plotly.express as px
from PIL import Image, ImageDraw


st.set_page_config(page_title='Weather data Visualization')
st.header('Weather data visualization')
st.subheader('Info gathered from API call')

#read csv file
csv_files = glob.glob('/usr/local/output/*.csv')

dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file, usecols=lambda x: x != 'Unnamed: 0')
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)

st.dataframe(df)

#put column names into a tuple
columns = tuple(df.columns)

#let user select column to analyze in boxplot
option = st.selectbox('Which column boxplot would you like to observe', columns)
st.write('You selected:', option)
fig = px.box(df, y=option)
st.plotly_chart(fig)

option2 = st.selectbox('Which column piechart would you like to observe', columns)
st.write('You selected:', option2)
new_df = df.groupby([option2])[option2].count().reset_index(name='count')
st.dataframe(new_df)
fig = px.pie(new_df, values='count', names=option2, title=option2)
st.plotly_chart(fig)