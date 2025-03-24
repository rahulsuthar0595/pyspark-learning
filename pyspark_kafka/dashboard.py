import time

import pandas as pd
import psycopg2
import streamlit as st

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="salaries_pyspark",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

st.title("🚨 Real-Time Fraud Detection Dashboard")


# Fetch fraud transactions
def fetch_fraud_transactions():
    query = "SELECT * FROM salaries LIMIT 10"
    df = pd.read_sql(query, conn)
    return df


# Auto-refresh every 5 seconds
refresh_interval = 5  # in seconds
st.write(f"🔄 Auto-refreshing every {refresh_interval} seconds...")
placeholder = st.empty()

while True:
    st.empty()
    df = fetch_fraud_transactions()
    placeholder.dataframe(df)
    time.sleep(refresh_interval)  # Refresh data
