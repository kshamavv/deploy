import pandas as pd
import datetime as dt
import numpy as np
from google.cloud import storage

storage_client = storage.Client()
bucket_name = 'appdeployee'
file_name = 'rfm - online_retail.csv'

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_name)
blob.download_to_filename(file_name)
df = pd.read_csv(file_name)

df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

reference_date = df['InvoiceDate'].max() + pd.DateOffset(1)  
df['Recency'] = (reference_date - df['InvoiceDate']).dt.days
rfm = df.groupby(['CustomerID','UnitPrice','InvoiceDate']).agg({
    'Recency': 'min',           
    'InvoiceNo': 'count',       
    'TotalAmount': 'sum' 
}).reset_index()

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
        return 1
    

rfm['R_Score'] = rfm['Recency'].apply(rfm_score, args=('Recency', quantiles))
rfm['F_Score'] = rfm['InvoiceNo'].apply(rfm_score, args=('InvoiceNo', quantiles))
rfm['M_Score'] = rfm['TotalAmount'].apply(rfm_score, args=('TotalAmount', quantiles))

rfm['RFM_Group'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)

rfm['RFM_Score'] = rfm['R_Score'] + rfm['F_Score'] + rfm['M_Score']
rfm['CustomerID'] =rfm['CustomerID'].astype("int64")

final_df= rfm.merge(str_df,on='CustomerID',how='inner')
print("RFM analysis is completed")