import pandas as pd
import datetime as dt
import numpy as np
import os
from flask import Flask
from google.cloud import storage
from google.cloud import bigquery
from pandas_gbq import to_gbq

app = Flask(__name__)

@app.route('/',methods=["GET"])
def rfm_func():

    # storage_client = storage.Client()
    # bucket_name = 'appdeployee'
    # file_name = 'online_retail.csv'


    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(file_name)
    # blob.download_to_filename(file_name)
    df = pd.read_csv('gs://appdeployee/online_retail.csv')
    # Calculate RFM metrics
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
    reference_date = df['InvoiceDate'].max() + pd.DateOffset(1)
    df['Recency'] = (reference_date - df['InvoiceDate']).dt.days
    rfm = df.groupby(['CustomerID','UnitPrice','InvoiceDate']).agg({
        'Recency': 'min',
        'InvoiceNo': 'count',
        'TotalAmount': 'sum'
    }).reset_index()

    # Calculate RFM scores
    quantiles = rfm.quantile(q=[0.25, 0.50, 0.75,])
    rfm['R_Score'] = rfm['Recency'].apply(lambda x: 4 if x <= quantiles['Recency'][0.25] else 3 if x <= quantiles['Recency'][0.50] else 2 if x <= quantiles['Recency'][0.75] else 1)
    rfm['F_Score'] = rfm['InvoiceNo'].apply(lambda x: 4 if x <= quantiles['InvoiceNo'][0.25] else 3 if x <= quantiles['InvoiceNo'][0.50] else 2 if x <= quantiles['InvoiceNo'][0.75] else 1)
    rfm['M_Score'] = rfm['TotalAmount'].apply(lambda x: 4 if x <= quantiles['TotalAmount'][0.25] else 3 if x <= quantiles['TotalAmount'][0.50] else 2 if x <= quantiles['TotalAmount'][0.75] else 1)

    # Calculate RFM Group and RFM Score
    rfm['RFM_Group'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)
    rfm['RFM_Score'] = rfm['R_Score'] + rfm['F_Score'] + rfm['M_Score']
    # Merge with customer description and country
    rfm_df = rfm.merge(df[['Description', 'Country','CustomerID']], on='CustomerID', how='inner')
    bigquery.Client().load_table_from_dataframe(rfm_df, 'prj-gradient-kshama.kshama.result', if_exists='replace')

    return "Rfm analysis completed"

if __name__ == "__main__":
    app.debug = True
    app.host='0.0.0.0'
    app.port=int(os.environ.get('PORT', 8080))
    app.run()
