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
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `NewOrderRequest`, but got `{order_status}`")
                else:
                    # If order does not exist, add it
                    self.orders[order_id] = order_status
            else:
                if order_status == 'NewOrderAcknowledged' and self.orders[order_id] != 'NewOrderRequest':
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `NewOrderRequest`, but got `{order_status}`")
                elif order_status == 'CancelRequest' and self.orders[order_id] != 'NewOrderAcknowledged':
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `NewOrderAcknowledged`, but got `{order_status}`")
                elif order_status == 'CancelRequestAcknowledged' and self.orders[order_id] != 'CancelRequest':
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `CancelRequest`, but got `{order_status}`")
                elif order_status == 'Cancelled' and self.orders[order_id] != 'CancelRequestAcknowledged':
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `CancelRequestAcknowledged`, but got `{order_status}`")
                elif order_status == 'Trade' and self.orders[order_id] != 'NewOrderAcknowledged':
                    logging.error(f"ERROR on Order ID `{order_id}`: Expected `NewOrderAcknowledged`, but got `{order_status}`")
                # Finally, if order flow is correct, remove the order from the dictionary
                else:
                    self.orders.pop(order_id)
    

    def get_open_orders(self):
        """
        Get the number of open orders.

        Returns:
            int: Number of open orders.
        """
        return len(self.orders)



    

