import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import time
import logging

# read csv from a URL
@st.experimental_memo
def get_data() -> pd.DataFrame:
    df = pd.read_json('data/TSXData.json')
    df["bar_color"] = np.where(df["Direction"]=='NBFToExchange', 'green', 'red')
    # Make OrderPrice negative if Direction is NBFToExchange and OrderPrice is not NaN
    df['adjusted_price'] = np.where((df['Direction'] == 'NBFToExchange') & (df['OrderPrice'].notna()), -df['OrderPrice'], df['OrderPrice'])
    # Get the time difference between the first and last row, in seconds
    return df



start = time.time()
@st.experimental_memo
def update_data(window=4):
    global start, simulation_length, dataframe
    diff = start - time.time()
    index = int(dataframe.shape[0]*((diff/simulation_length)/simulation_length))
    start = 0 if index - 100 < 0 else index - 100
    logging.info(f'Setting df range to [{start:,}:{index:,}]')
    return dataframe.iloc[start:index]


st.set_page_config(
        page_title="Real-Time Order Book Dashboard",
        page_icon="✅",
        layout="wide",
    )

dataframe = get_data()
simulation_length = time_diff = (dataframe['TimeStamp'].iloc[-1] - dataframe['TimeStamp'].iloc[0]).total_seconds()

placeholder = st.empty()

while True:
    df = update_data()
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)

        with fig_col1:
            st.markdown("### Demand and Supply")

            fig = px.bar(df[df.OrderPrice.notna()], x="TimeStamp", y="adjusted_price", color="bar_color", labels={"adjusted_price": "Order Price", "TimeStamp": "Time"})
            fig.update_traces(marker_color=df["bar_color"])
            st.write(fig)

        # with fig_col2:
        #     st.write("### Second Chart")
        #     fig = px.scatter(df[df.MessageType=='NewOrderAcknowledged'], x="TimeStamp", y="OrderPrice")


        st.header(f"Order Book: {df.shape[0]:,} row(s) from {df.index[0]:,} to {df.index[-1]:,}")
        st.dataframe(df)
    time.sleep(0.5)