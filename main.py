import pandas as pd
import datetime as dt
import numpy as np
import os 
from flask import Flask
from google.cloud import storage
from google.cloud import bigquery

app = Flask(__name__)

@app.route('/',methods=["GET"])
def rfm_func():

    storage_client = storage.Client()
    bucket_name = 'appdeployee'
    file_name = 'rfm - online_retail (2).csv'


    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(file_name)
    df = pd.read_csv(file_name)
    print(len(df))

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

    reference_date = df['InvoiceDate'].max() + pd.DateOffset(1)  
    df['Recency'] = (reference_date - df['InvoiceDate']).dt.days
    rfm = df.groupby(['CustomerID','UnitPrice','InvoiceDate']).agg({
        'Recency': 'min',           
        'InvoiceNo': 'count',       
        'TotalAmount': 'sum' 
    }).reset_index()
    print("rfm")

    str_df=df[['Description', 'Country','CustomerID']]


    quantiles = rfm.quantile(q=[0.25, 0.50, 0.75,])

    def rfm_score(x, p, d): 
        if x <= d[p][0.25]:
            return 4
        elif x <= d[p][0.50]:
            return 3
        elif x <= d[p][0.75]:
            return 2
        else:
            return 16
        

    rfm['R_Score'] = rfm['Recency'].apply(rfm_score, args=('Recency', quantiles))
    rfm['F_Score'] = rfm['InvoiceNo'].apply(rfm_score, args=('InvoiceNo', quantiles))
    rfm['M_Score'] = rfm['TotalAmount'].apply(rfm_score, args=('TotalAmount', quantiles))

    rfm['RFM_Group'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)

    rfm['RFM_Score'] = rfm['R_Score'] + rfm['F_Score'] + rfm['M_Score']
    rfm['CustomerID'] =rfm['CustomerID'].astype("int64")

    # final_df= rfm.merge(str_df,on='CustomerID',how='inner')
    client = bigquery.Client()

    table_id = "prj-gradient-kshama.kshama.newww"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("No","INT64"),
            bigquery.SchemaField("Unnamed","INT64"),
            bigquery.SchemaField("CustomerID", "INT64"),
            bigquery.SchemaField("UnitPrice", "FLOAT64"),
            bigquery.SchemaField("InvoiceDate", "STRING"),
        bigquery.SchemaField("InvoiceNo", "INT64"),
        bigquery.SchemaField("Recency","INT64"),
        bigquery.SchemaField("TotalAmount", "FLOAT64"),
        bigquery.SchemaField("R_Score", "INT64"),
        bigquery.SchemaField("F_Score", "INT64"),
        bigquery.SchemaField("M_Score", "INT64"),
        bigquery.SchemaField("RFM_Group", "INT64"),
        bigquery.SchemaField("RFM_Score", "INT64"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Country", "STRING")

        ],
        skip_leading_rows=1,
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.DAY,
        #     field="date",  # Name of the column to use for partitioning.
        #     expiration_ms=7776000000,  # 90 days.
        # ),
    )
    uri = "gs://appdeployee/updated.csv"  

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  

    table = client.get_table("prj-gradient-kshama.kshama.newww")
    print("Loaded {} rows to table {}".format(table.num_rows,"prj-gradient-kshama.kshama.newww")) 

    return "Rfm analysis completed"
     #table_id
if __name__ == "__main__":
    app.debug = True
    app.host='0.0.0.0'
    app.port=int(os.environ.get('PORT', 8080))
    app.run()
    app.run(host="127.0.0.1", port=8080, debug=True)
    # app.run(debug=True,host="0.0.0.0", port=8080)
# print("RFM analysis is completed")
