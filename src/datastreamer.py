import pandas as pd
import numpy as np
import logging
import time


class DataStreamer:
    def __init__(self, datapath: str, time_col='TimeStamp') -> None:
        """
        Simulates a data stream from a file.

        Args:
            datapath (str): Path to the JSON data file.
            time_col (str, optional): Name of the column containing the timestamp. Defaults to 'TimeStamp'.
        """
        self.df = pd.read_json(datapath)
        # Sort by timestamp
        self.df.sort_values(by=[time_col], inplace=True)
        self.last_time = None
        self.orders = {}

    
    def __flow_error(self, order_id, actual, expected, prefix=''):
        logging.error(f"{prefix} ERROR on Order ID `{order_id}`: Expected {expected}, but got `{actual}`. DELETING ORDER")
        if order_id in self.orders:
            self.orders.pop(order_id, None)

    
    def stream(self, window=1):
        """
        Stream data from the file.

        Args:
            window (int, optional): Time window in seconds. Defaults to 1.

        Returns:
            pd.DataFrame: Dataframe containing the data in the time window.
        """
        half_window = False
        if self.last_time is None:
            self.last_time = self.df['TimeStamp'].iloc[0]
            half_window = True
        # Get the data for a `window` seconds time window
        one_second_ahead = self.last_time + pd.Timedelta(seconds=window)

        # If the window exceeds the last timestamp, loop back to the beginning
        if one_second_ahead > self.df['TimeStamp'].iloc[-1]:
            one_second_ahead = self.df['TimeStamp'].iloc[0]
            half_window = True

        if half_window:
            df = self.df[(self.df['TimeStamp'] <= one_second_ahead)]
        else:
            df = self.df[(self.df['TimeStamp'] > self.last_time) & (self.df['TimeStamp'] <= one_second_ahead)]
        # Update the last time
        self.last_time = one_second_ahead
        # Log the data
        logging.info(f"Streaming {df.shape[0]:,} row(s) from {df.index[0]:,} to {df.index[-1]:,}")
        # Update the order status
        self.update_order_status(df)
        return df
    

    def update_order_status(self, df):
        """
        Update the `self.orders` dictionary with the order status.

        Args:
            df (pd.DataFrame): Dataframe containing the data in the time window.
        """
        for _, row in df.iterrows():
            order_id = row['OrderID']
            order_status = row['MessageType']

            if order_id not in self.orders:
                if order_status != 'NewOrderRequest':
                    self.__flow_error(order_id, order_status, 'NewOrderRequest', prefix='[NEW ORDER]')
                else:
                    # If order does not exist, add it
                    self.orders[order_id] = order_status
                    logging.info(f"[SUCCESS NEW ORDER] Order `{order_id}` has been created with status `{order_status}`")
            else:
                previous_status = self.orders[order_id]
                if order_status == 'NewOrderAcknowledged' and previous_status != 'NewOrderRequest':
                    self.__flow_error(order_id, previous_status, 'NewOrderRequest')
                elif order_status == 'CancelRequest' and previous_status != 'NewOrderAcknowledged':
                    self.__flow_error(order_id, previous_status, 'NewOrderAcknowledged')
                elif order_status == 'CancelAcknowledged' and previous_status != 'CancelRequest':
                    self.__flow_error(order_id, previous_status, 'CancelRequest')
                elif order_status == 'Cancelled':
                    if previous_status != 'CancelAcknowledged':
                        self.__flow_error(order_id, previous_status, 'CancelAcknowledged')
                    else:
                        self.orders.pop(order_id)
                        logging.info(f"[CLEAR] Order `{order_id}` has been cancelled")
                elif order_status == 'Trade':
                    if previous_status != 'NewOrderAcknowledged':
                        self.__flow_error(order_id, previous_status, 'NewOrderAcknowledged')
                    else:
                        self.orders.pop(order_id)
                        logging.info(f"[CLEAR] Order `{order_id}` has been traded")
                # Finally, if order flow is correct, remove the order from the dictionary
                else:
                    self.orders[order_id] = order_status
    

    def get_open_orders(self):
        """
        Get the number of open orders.

        Returns:
            int: Number of open orders.
        """
        return len(self.orders)



    

