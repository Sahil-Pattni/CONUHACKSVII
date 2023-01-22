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


last_refresh_time = 1
no_refresh = 0
last_row = None
while True:
    start_time = time.time()
    df = streamer.stream()
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)

        with fig_col1:
            refresh_time_padded = 1 if last_refresh_time < 1 else last_refresh_time
            st.markdown(f"### Refresh Time: {refresh_time_padded:.3f} second(s) | Open Orders: {streamer.get_nopen_orders():,}")

            fig = px.scatter(
                df[df.OrderPrice.notna()],
                x="TimeStamp",
                y="OrderPrice",
                color='Symbol',
                labels={"OrderPrice": "Order Price", "TimeStamp": "Time"},
                title = "# of orders per second"
            )
            st.write(fig)

        with fig_col2:
            cancelled_orders = streamer.get_cancelled_orders()
            executed_trades = streamer.get_executed_trades()
            open_orders = streamer.get_nopen_orders()

            equitymeans = df.groupby('Symbol').mean()
            fig = px.bar(
                equitymeans,
                x=equitymeans.index,
                y='OrderPrice',
                color=equitymeans.index,
                title='Avg Price per second'
            )
            st.write(fig)
        st.metric(label='Avg Standard Deviation per second', value=df.groupby('Symbol').std().mean())
        st.header(f"Order Book: {df.shape[0]:,} row(s) from {df.index[0]:,} to {df.index[-1]:,}")

        # Sleep for remaining time (up to 1 second)
        elapsed_time = time.time() - start_time
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)


        last_refresh_time = abs(elapsed_time)
        if last_row is None:
                last_row = df.index[-1]
        elif last_row == df.index[-1]:
            logging.info(f'No update to df found.')
            no_refresh += 1
            if no_refresh > 3:
                break
        last_row = df.index[-1]

        st.dataframe(df.tail(10))