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
        self.last_time = self.df[time_col].iloc[0]
        self.orders = {}

    
    def __flow_error(self, order_id, actual, expected):
        logging.error(f"ERROR on Order ID `{order_id}`: Expected {expected}, but got `{actual}`")

    
    def stream(self, window=1):
        """
        Stream data from the file.

        Args:
            window (int, optional): Time window in seconds. Defaults to 1.

        Returns:
            pd.DataFrame: Dataframe containing the data in the time window.
        """
        # Increase the last time by the window
        self.last_time = self.last_time + pd.Timedelta(seconds=window)
        # Get the data in the time window
        df = self.df[(self.df['TimeStamp'] <= self.last_time)&(self.df['TimeStamp'] > self.last_time - pd.Timedelta(seconds=window))]
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
                    self.__flow_error(order_id, order_status, 'NewOrderRequest')
                    # logging.error(f"ERROR on Order ID `{order_id}`: [NEW ORDER] Expected `NewOrderRequest`, but got `{order_status}`")
                else:
                    # If order does not exist, add it
                    self.orders[order_id] = order_status
            else:
                previous_status = self.orders[order_id]
                if order_status == 'NewOrderAcknowledged' and previous_status != 'NewOrderRequest':
                    self.__flow_error(order_id, previous_status, 'NewOrderRequest')
                elif order_status == 'CancelRequest' and previous_status != 'NewOrderAcknowledged':
                    self.__flow_error(order_id, previous_status, 'NewOrderAcknowledged')
                elif order_status == 'CancelRequestAcknowledged' and previous_status != 'CancelRequest':
                    self.__flow_error(order_id, previous_status, 'CancelRequest')
                elif order_status == 'Cancelled':
                    if previous_status != 'CancelRequestAcknowledged':
                        self.__flow_error(order_id, previous_status, 'CancelRequestAcknowledged')
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



    

