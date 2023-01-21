import streamlit as st
import pandas as pd
import plotly.express as px
import time

start = time.time()
# read csv from a URL
@st.experimental_memo
def get_data() -> pd.DataFrame:
    return pd.read_json('data/TSXData.json')

def update_data():
    diff = start - time.time()
    index = int(len(df)*((diff % 4)/4))
    print(f'Index: {index} | {index}/{len(df)}')
    return df.iloc[:]


st.set_page_config(
        page_title="Real-Time Order Book Dashboard",
        page_icon="✅",
        layout="wide",
    )

df = get_data()

placeholder = st.empty()

while True:
    df = update_data()
    with placeholder.container():
        fig_col1, fig_col2 = st.columns(2)

        with fig_col1:
            st.markdown("### First Chart")
            fig = px.line(df[df.MessageType=='NewOrderAcknowledged'], x="TimeStamp", y="OrderPrice")


        st.header("Order Book")
        st.dataframe(df)