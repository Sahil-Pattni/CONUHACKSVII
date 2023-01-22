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
    df = streamer.box_stream()
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)

        refresh_time_padded = 1 if last_refresh_time < 1 else last_refresh_time
        st.markdown(f"### Live Orders: {df.RoundedTimeStamp.iloc[0]} to {df.RoundedTimeStamp.iloc[-1]}")

        fig = px.scatter(
            df[df.MessageType.isin(['Trade', 'NewOrderAcknowledged'])],
            x="TimeStamp",
            y="OrderPrice",
            color="Symbol",
            color_discrete_map=streamer.get_ticker_color_map(),
            title=f"Refresh Time: {refresh_time_padded:.3f} second(s) | Open Orders: {streamer.get_nopen_orders():,}",
            labels={"OrderPrice": "Order Price", "TimeStamp": "Time"})
        # Set category order
        fig.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig)
        

        st.markdown(f"### Anomalies Detected (Incorrect Flow)")
        st.write(streamer.get_anomalies())

        # Plot percentage of cancelled orders and executed trades
        fig = px.pie(
            values=[streamer.get_cancelled_orders(), streamer.get_executed_trades()],
            names=['Cancelled Orders', 'Executed Trades'],
            title=f"Cancelled Orders: {streamer.get_cancelled_orders():,} | Executed Trades: {streamer.get_executed_trades():,}"
        )
        st.plotly_chart(fig)


        # # with fig_col2:
        # fig = px.treemap(
        #     df,
        #     path=['MessageType'],
        #     values=pd.Series(np.ones(df.shape[0])),
        #     color='MessageType',
        #     color_discrete_map=streamer.get_message_color_map(),
        # )
        # st.plotly_chart(fig)


        # cancelled_orders = streamer.get_cancelled_orders()
        # executed_trades = streamer.get_executed_trades()
        # open_orders = streamer.get_nopen_orders()

        # equitymeans = df.groupby('Symbol').mean()
        # fig = px.bar(
        #     equitymeans,
        #     x=equitymeans.index,
        #     y='OrderPrice',
        #     color=equitymeans.index,
        #     title='$x^2$'
        # )
        # fig.update_xaxes(categoryorder='category ascending')
        # st.plotly_chart(fig)
            
        st.header(f"Order Book: {df.shape[0]:,} row(s) from {df.index[0]:,} to {df.index[-1]:,}")
        st.dataframe(df.tail(10))

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