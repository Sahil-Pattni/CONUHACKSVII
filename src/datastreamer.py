import pandas as pd
import numpy as np
import logging
import time
import plotly.express as px
import streamlit as st

class DataStreamer:
    def __init__(self, datapath: str, time_col='TimeStamp') -> None:
        """
        Simulates a data stream from a file.

        Args:
            datapath (str): Path to the JSON data file.
            time_col (str, optional): Name of the column containing the timestamp. Defaults to 'TimeStamp'.
        """
        self.df = pd.read_json(datapath)
        # Sort by timestamp (just-in-case)
        self.df.sort_values(by=[time_col], inplace=True)
        # Add rounded timestamp
        self.df['RoundedTimeStamp'] = self.df[time_col].dt.round('S')
        # Color maps
        self.color_map = {symbol: color for symbol, color in zip(self.df['Symbol'].unique(),  px.colors.qualitative.Plotly)}
        self.message_color_map = {message: color for message, color in zip(self.df['MessageType'].unique(),  px.colors.qualitative.Plotly)}
        self.orders = {}
        self.cancelled_orders = 0
        self.executed_trades = 0
        self.anomalies = None

        # Bounds for time stream
        self.lower_bound, self.upper_bound = None, None

        # Index tracker
        self.last_index_seen = 0

    
    def __flow_error(self, order_id, actual, expected, prefix=''):
        logging.error(f"{prefix} ERROR on Order ID `{order_id}`: Expected {expected}, but got `{actual}`. DELETING ORDER")
        if order_id in self.orders:
            self.orders.pop(order_id, None)
            self.cancelled_orders += 1
    
    
    
    def stream(self, window=1, width=5):
        """
        Stream data from the file.

        Args:
            window (int, optional): Time window in seconds. Defaults to 1.

        Returns:
            pd.DataFrame: Dataframe containing the data in the time window.
        """
        if self.lower_bound is None or self.upper_bound is None:
            # If lower bound is None, set it to the first timestamp
            self.lower_bound = self.df['RoundedTimeStamp'].iloc[0]
            # If upper bound is None, set it to the first timestamp + width
            self.upper_bound = self.lower_bound + pd.Timedelta(seconds=width)
        else:
            # Update the bounds
            self.lower_bound += pd.Timedelta(seconds=window)
            self.upper_bound += pd.Timedelta(seconds=window)

            # If upper bound is greater than the last timestamp, return the full dataframe
            if self.upper_bound > self.df['RoundedTimeStamp'].iloc[-1]:
                return self.df

        # Get the data in the time window
        df = self.df[(self.df['RoundedTimeStamp'] >= self.lower_bound) & (self.df['RoundedTimeStamp'] < self.upper_bound)]
        logging.info(f"Streaming data from {df.index[0]:,} to {df.index[1]:,} ({df.shape[0]:,} row(s))")
        # Update the order status
        self.update_order_status(df)
        return df
    

    def box_stream(self, window=100):
        if self.lower_bound is None or self.upper_bound is None:
            # If lower bound is None, set it to the first timestamp
            self.lower_bound = 0
            # If upper bound is None, set it to the first timestamp + width
            self.upper_bound = window
        else:
            # Update the bounds
            self.lower_bound += window
            self.upper_bound += window

            # If upper bound is greater than the last timestamp, return the full dataframe
            if self.upper_bound > self.df.shape[0]:
                return self.df
        
        # Get the data in the time window
        df = self.df.iloc[self.lower_bound:self.upper_bound]
        logging.info(f"Streaming data from {df.index[0]:,} to {df.index[-1]:,} ({df.shape[0]:,} row(s))")
        # Update the order status
        self.update_order_status(df)
        return df

    

    def update_order_status(self, df):
        """
        Update the `self.orders` dictionary with the order status.

        Args:
            df (pd.DataFrame): Dataframe containing the data in the time window.
        """
        for idx, row in df.iterrows():
            if idx <= self.last_index_seen:
                continue
            self.last_index_seen = idx
            order_id = row['OrderID']
            order_status = row['MessageType']

            if order_id not in self.orders:
                if order_status != 'NewOrderRequest':
                    self.__flow_error(order_id, order_status, 'NewOrderRequest', prefix='[NEW ORDER]')
                    self.add_anomaly(row)
                else:
                    # If order does not exist, add it
                    self.orders[order_id] = order_status
                    # logging.info(f"[SUCCESS NEW ORDER] Order `{order_id}` has been created with status `{order_status}`")
            else:
                previous_status = self.orders[order_id]
                if order_status == 'NewOrderAcknowledged' and previous_status != 'NewOrderRequest':
                    self.__flow_error(order_id, previous_status, 'NewOrderRequest')
                    self.add_anomaly(row)
                    # Add to anomalies
                elif order_status == 'CancelRequest' and previous_status != 'NewOrderAcknowledged':
                    self.__flow_error(order_id, previous_status, 'NewOrderAcknowledged')
                    self.add_anomaly(row)
                elif order_status == 'CancelAcknowledged' and previous_status != 'CancelRequest':
                    self.__flow_error(order_id, previous_status, 'CancelRequest')
                    self.add_anomaly(row)
                elif order_status == 'Cancelled':
                    if previous_status != 'CancelAcknowledged':
                        self.__flow_error(order_id, previous_status, 'CancelAcknowledged')
                        self.add_anomaly(row)
                    else:
                        self.orders.pop(order_id)
                        self.cancelled_orders += 1
                        # logging.info(f"[CLEAR] Order `{order_id}` has been cancelled")
                elif order_status == 'Trade':
                    if previous_status != 'NewOrderAcknowledged':
                        self.__flow_error(order_id, previous_status, 'NewOrderAcknowledged')
                        self.add_anomaly(row)
                    else:
                        self.orders.pop(order_id)
                        self.executed_trades += 1
                        # logging.info(f"[CLEAR] Order `{order_id}` has been traded")
                # Finally, if order flow is correct, remove the order from the dictionary
                else:
                    self.orders[order_id] = order_status
    

    def add_anomaly(self, row):
        """
        Add an anomaly to the `self.anomalies` dataframe.

        Args:
            row (pd.Series): Row containing the anomaly.
        """
        if self.anomalies is None:
            self.anomalies = pd.DataFrame(columns=row.index)
        self.anomalies = pd.concat([self.anomalies, row.to_frame().T], axis=0)
    
    
    def get_nopen_orders(self):
        """
        Get the number of open orders.

        Returns:
            int: Number of open orders.
        """
        return len(self.orders)
    

    def get_cancelled_orders(self):
        """
        Get the number of cancelled orders.

        Returns:
            int: Number of cancelled orders.
        """
        return self.cancelled_orders
    

    def get_executed_trades(self):
        """
        Get the number of executed trades.

        Returns:
            int: Number of executed trades.
        """
        return self.executed_trades
    
    
    def get_ticker_color_map(self):
        """
        Get a color map for each unique symbol.

        Returns:
            dict: Dictionary containing the color map.
        """
        return self.color_map
    

    def get_message_color_map(self):
        """
        Get a color map for each unique message type.

        Returns:
            dict: Dictionary containing the color map.
        """
        return self.message_color_map

    
    def get_anomalies(self):
        """
        Get the anomalies dataframe.

        Returns:
            pd.DataFrame: Dataframe containing the anomalies.
        """
        return self.anomalies[['TimeStamp', 'Direction', 'OrderID', 'MessageType', 'Symbol']]



    

