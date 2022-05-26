import pandas as pd
import boto3
import numpy as np
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

bucket = boto3.resource('s3').Bucket(bucket_name)
objects = bucket.objects.all()

for object in objects:
    if object.key.startswith('demo/CLIENT_SW_PP_DEMO') and object.key.endswith('.txt'):
        demog = client.get_object(Bucket=bucket_name,
                                  Key=object.key)
        df2 = pd.read_csv(demog['Body'], sep="|")
        df2 = df2.set_index('Rel_ID')
        # print (df2.columns)
    ##check Cust_mstr file
    if object.key.startswith('demo/DM_CUST_MSTR') and object.key.endswith('.csv'):
        custmstr = client.get_object(Bucket=bucket_name,
                                     Key=object.key)
        df1 = pd.read_csv(custmstr['Body'], sep=",")
        df1 = df1.set_index('Source_ID')
        df = pd.concat([df1, df2[~df2.index.isin(df1.index)]])
        df.update(df2)
        df['Cust_ID'] = df['Cust_ID'].astype(str).replace('\.0', '', regex=True)
        df['CS_Provider_AMA_Check_Digit'] = df['CS_Provider_AMA_Check_Digit'].astype(str).replace('\.0', '', regex=True)
        df['Zip_Code'] = df['Zip_Code'].astype(str).replace('\.0', '', regex=True)
        df['AMA_No_Contact'] = df['AMA_No_Contact'].astype(str).replace('\.0', '', regex=True)
        df['PDRP_Indicator'] = df['PDRP_Indicator'].astype(str).replace('\.0', '', regex=True)
        df['PDRP_Date'] = df['PDRP_Date'].astype(str).replace('\.0', '', regex=True)
        df['CS_Provider_AMA_ID'] = df['CS_Provider_AMA_ID'].astype(str).replace('\.0', '', regex=True)
        df['Territory_ID'] = df['Territory_ID'].astype(str).replace('\.0', '', regex=True)
        df['Call_Status_Code'] = df['Call_Status_Code'].astype(str).replace('\.0', '', regex=True)
        df = df.drop(['Cust_ID'], axis=1)
        df['Cust_ID'] = np.arange(1, len(df) + 1)
        df.index.name = 'Source_ID'
        # print (df.dtypes)
        print (df)
        FileName = 'cust_mstr.csv'
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        response = client.put_object(
            ACL='private',
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=FileName
        )
    # else:
    # df2 = df.rename(columns = {'Rel_ID': 'Source_ID'})
    #   df2.index = np.arange(1,len(df2)+1)
    #    df2.index.name = 'Cust_Id'
    #    FileName = 'cust_mstr.csv'
    #   csv_buffer = StringIO()
    #  df2.to_csv(csv_buffer)
    # response = client.put_object(
    #    ACL = 'private',
    #   Body = csv_buffer.getvalue(),
    #  Bucket=bucket_name,
    # Key=FileName
    # )
