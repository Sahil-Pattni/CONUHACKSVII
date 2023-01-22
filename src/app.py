import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import time
import logging
from datastreamer import DataStreamer


st.set_page_config(
        page_title="Real-Time Order Book Dashboard",
        page_icon="✅",
        layout="wide",
)


# read csv from a URL
@st.experimental_memo
def get_streamer():
    """Generates a streamer"""
    streamer = DataStreamer('data/TSXData.json')
    return streamer


# Globals
streamer = get_streamer()
placeholder = st.empty()



while True:
    df = streamer.stream()
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)

        with fig_col1:
            st.markdown(f"### Demand and Supply")

            fig = px.scatter(
                df[df.OrderPrice.notna()],
                x="TimeStamp",
                y="OrderPrice",
                color='Symbol',
                labels={"OrderPrice": "Order Price", "TimeStamp": "Time"})
            st.write(fig)
            st.write(f'### Open Orders: {streamer.get_nopen_orders():,}')

        with fig_col2:
            cancelled_orders = streamer.get_cancelled_orders()
            executed_trades = streamer.get_executed_trades()
            open_orders = streamer.get_nopen_orders()
            


        st.header(f"Order Book: {df.shape[0]:,} row(s) from {df.index[0]:,} to {df.index[-1]:,}")
        st.dataframe(df)
    time.sleep(0.5)