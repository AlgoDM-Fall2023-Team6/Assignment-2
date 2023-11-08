import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px


add_selectbox = st.sidebar.selectbox(
    "Which one do you like to see",
    ("Forecast Graph", "Actual vs Forecast Graph", "Anomaly Detection")
)


import os
os.environ["SNOWFLAKE_DISABLE_ARROW"] = "1"



#from urllib import parse
from sqlalchemy import create_engine 
from snowflake.sqlalchemy import URL


engine = create_engine(URL(
    account = 'qjkryhh-cu10455',
    user = 'PRANEETH',
    password = 'Praneeth12345',
    database = 'AD_FORECAST_DEMO',
    schema = 'DEMO',
    warehouse = 'AD_FORECAST_DEMO_WH',
    role='ACCOUNTADMIN',
))

connection = engine.connect()

# Streamlit app
st.title('Assignment2: forecasting and anomaly')
#st.set_page_config(page_title='Assignment1: Snowflake Queries')



if add_selectbox == "Forecast Graph":
    st.subheader("Forecast Graph")

    query= f"CALL impressions_forecast!FORECAST(FORECASTING_PERIODS => 14);"
    results = connection.execute(query)  

    df = pd.DataFrame(results.fetchall())

    # Create the time series forecast plot
    plt.figure(figsize=(12, 6))

    # Plot the forecasted values
    plt.plot(df['ts'], df['forecast'], label='forecast', color='blue')
    # Fill between the lower and upper bounds
    plt.fill_between(df.index, df['lower_bound'], df['upper_bound'], color='lightblue', alpha=0.5, label='95% Prediction Interval')

    plt.xlim(pd.Timestamp('2022-12-06'), pd.Timestamp('2022-12-19'))
    # Customize the plot
    plt.title('Time Series Forecast')
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.grid(True)

    # Show the plot or save it to a file
    plt.tight_layout()
    

    st.write('Forecast of the impressions over the next two weeks')
    #st.divider()
    # Display as table
    st.dataframe(df)
    st.pyplot(plt)



elif add_selectbox == "Actual vs Forecast Graph":
    st.subheader("Actual Impressions vs. Forecast Impressions")
    query_temp= f"CALL impressions_forecast!FORECAST(FORECASTING_PERIODS => 14);"
    query_2 = f" SELECT day AS ts, impression_count AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound FROM daily_impressions UNION ALL SELECT ts, NULL AS actual, forecast, lower_bound, upper_bound FROM TABLE(RESULT_SCAN(-1));"
    results_temp = connection.execute(query_temp)  
    results_2 = connection.execute(query_2) 
    forecast_df = pd.DataFrame(results_2.fetchall(), columns= results_2.keys())
    fig = px.line(forecast_df, x="ts", y=["actual", "forecast"], labels={"value": "Impression Count"}, title="Actual Impressions vs. Forecast Impressions",
                        color_discrete_map={"actual": "blue", "forecast": "yellow"})
    fig.update_traces (line=dict (width=3))

    st.dataframe(forecast_df)
    st.plotly_chart(fig)


elif add_selectbox == "Anomaly Detection":
    st.subheader("Anomaly Detection")  
    query_main = f'''  CALL impression_anomaly_detector!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('select ''2022-12-06''::timestamp as day, 12000 as impressions'), TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'impressions');'''
    results_main = connection.execute(query_main)
    query_3=fquery = f'''CALL impression_anomaly_detector!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('select ''2022-12-06''::timestamp as day, 12000 as impressions'), TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'impressions');'''
    results_3 = connection.execute(query_3)
    df_3 = pd.DataFrame(results_3.fetchall())

    st.dataframe(df_3)
    if df_3["is_anomaly"].any():
        st.write("Anomaly is Detected based om the given conditions")
    else:
        st.write("No anomalies detected")

    

    query_4=f'''CALL impression_anomaly_detector!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('select ''2022-12-06''::timestamp as day, 120000 as impressions'), TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'impressions');'''
    results_4 = connection.execute(query_4)
    df_4 = pd.DataFrame(results_4.fetchall())

    st.dataframe(df_4) 
    if df_4["is_anomaly"].any():
        st.write("Anomaly is Detected based om the given conditions")
    else:
        st.write("No anomalies detected")

    query_5=f'''CALL impression_anomaly_detector!DETECT_ANOMALIES(INPUT_DATA => SYSTEM$QUERY_REFERENCE('select ''2022-12-06''::timestamp as day, 60000 as impressions'),TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'impressions');'''
    results_5 = connection.execute(query_5) 
    df_5 = pd.DataFrame(results_5.fetchall()) 

    st.dataframe(df_5)
    if df_5["is_anomaly"].any():
        st.write("Anomaly is Detected based om the given conditions")
    else:
        st.write("No anomalies detected")
