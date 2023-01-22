import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

option = st.sidebar.selectbox(
                'Select a simulation speed',
                options=['Real Time', 'Sped Up'],
                index=0,
                key='speed'
)

while True:
    start_time = time.time()
    df = streamer.stream()
    with placeholder.container():
        refresh_time_padded = 1 if last_refresh_time < 1 else last_refresh_time

        st.markdown(f"### Simulated Live Stream\n{df.RoundedTimeStamp.iloc[0]} to {df.RoundedTimeStamp.iloc[-1]}")
            
        
        # Filter df to top 10 symbols
        top_symbols = df.Symbol.value_counts().index[:10]
        
        # Plot acknowledged/executed orders
        fig = px.scatter(
            df[(df.MessageType.isin(['Trade', 'NewOrderAcknowledged'])) & (df.Symbol.isin(top_symbols))].sort_values('Symbol'),
            x="TimeStamp",
            y="OrderPrice",
            color="Symbol",
            color_discrete_map=streamer.get_ticker_color_map(),
            title=f"Trades/Acknowledged Orders (10 most active symbols)",
            labels={"OrderPrice": "Order Price", "TimeStamp": "Time"})
        # Set category order
        fig.update_xaxes(categoryorder='category ascending')
        fig.update_traces(marker={'size': 15})
        # Change width of the plot
        st.plotly_chart(fig, use_container_width=True)

        st.metric(label='Mean Std. Dev across all tickers', value=round(df.groupby('Symbol').std().mean(), 4))


        # Plot mean order price by ticker
        equitymeans = df.sort_values('Symbol').groupby('Symbol').mean()
        fig = px.bar(
            equitymeans,
            x=equitymeans.index,
            y='OrderPrice',
            color=equitymeans.index,
            title='Average Order Price by Ticker'
        )
        fig.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig, use_container_width=True)
        

        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            # Plot the distribution of New Order (Req. vs Ack.)
            fig = px.violin(
                df[~df.OrderPrice.isna()],
                y='OrderPrice',
                x='MessageType',
                color='MessageType',
                box=False,
                points='outliers',
                title='New Order (Req. vs Ack.)'
            )
            st.plotly_chart(fig, use_container_width=True)


        with fig_col2:
            # Plot the distribution of Acknowledged Orders (New vs Cancel)
            fig = px.violin(
                df[df['MessageType'].isin(['NewOrderAcknowledged','CancelAcknowledged'])].sort_values('MessageType'),
                x='MessageType',
                title='Acknowledged Orders (New vs Cancel)'
                )
            st.plotly_chart(fig, use_container_width=True)
        

        fig = px.violin(df.sort_values('MessageType'), x='MessageType', points='outliers')
        st.plotly_chart(fig, use_container_width=True)

        # Anomalies
        st.header(f"Anomalies Detected (Incorrect Flow)")
        anomalies = streamer.get_anomalies()
        st.write(anomalies)

        fig1, fig2 = st.columns(2)
        with fig1:
            # Plot the distribution of Direction
            fig = px.pie(
                anomalies,
                names='Direction',
                title='Direction Distribution',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with fig2:
            # Plot the distribution of Type
            fig = px.pie(
                anomalies,
                names='MessageType',
                title='Message Type Distribution',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)

        # Order Book            
        st.header(f"Order Book: {df.shape[0]:,} row(s) from {df.index[-10]:,} to {df.index[-1]:,}")
        st.dataframe(df.tail(10))


        # Plot refresh rate
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = int(refresh_time_padded),
            gauge = {
                'axis': {'range': [None, 2], 'tickwidth': 1},
                'bar': {'color': "black"},
                'steps': [
                    {'range': [0, 1], 'color': 'green'},
                    {'range': [1, 1.5], 'color': 'orange'},
                    {'range': [1.5, 2], 'color': 'red'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 1.8}    
            },
            title = {'text': "Refresh Rate (seconds)"}))
            fig.update_layout(height=300)
            st.plotly_chart(fig, height=200, use_container_width=True)
        
        # Plot processing time
        with fig_col2:
            fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = last_refresh_time,
            gauge = {
                'axis': {'range': [None, 2], 'tickwidth': 1},
                'bar': {'color': "black"},
                'steps': [
                    {'range': [0, 0.5], 'color': 'green'},
                    {'range': [0.5, 1], 'color': 'yellow'},
                    {'range': [1, 1.5], 'color': 'orange'},
                    {'range': [1.5, 2], 'color': 'red'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 1.8}    
            },
            title = {'text': "Processing Time (seconds)"}))
            fig.update_layout(height=300)
            st.plotly_chart(fig, height=200, use_container_width=True)

        # Sleep for remaining time (up to 1 second)
        if option == 'Real Time':
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
