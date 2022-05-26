import logging
import boto3
import pandas as pd
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime
client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

terr_mstr = client.get_object(Bucket=bucket_name,
                     Key='prodmasters/DM_TERR_MSTR.csv')
terr_mstr_df = pd.read_csv(terr_mstr['Body'], sep=",")
terr_mstr_df.columns = terr_mstr_df.columns.map(lambda x: str(x) + '_TerrFile')

ziptoterr_mstr = client.get_object(Bucket=bucket_name,
                     Key='prodmasters/DM_Zip_To_Terr.csv')
ziptoterr_mstr_df = pd.read_csv(ziptoterr_mstr['Body'], sep=",")
ziptoterr_mstr_df.columns = ziptoterr_mstr_df.columns.map(lambda x: str(x) + '_ZipFile')

cust_mstr = client.get_object(Bucket=bucket_name,
                     Key='prodmasters/DM_CUST_MSTR.csv')
cust_mstr_df = pd.read_csv(cust_mstr['Body'], sep=",")
cust_mstr_df.columns = cust_mstr_df.columns.map(lambda x: str(x) + '_CustFile')

#cust_mstr_df = cust_mstr_df.drop(['Unnamed: 0_CustFile'],axis=1)

zipp_cust_df = cust_mstr_df.merge(ziptoterr_mstr_df, how='inner', left_on=['City_CustFile','State_CustFile','Zip_Code_CustFile'], right_on=['City_ZipFile','State_ZipFile','Zip_ZipFile']).drop(['Provider_ID_CustFile',
       'Writer_Type_CustFile', 'First_Name_CustFile', 'Middle_Name_CustFile',
       'Last_Name_CustFile', 'Title_CustFile', 'Specialty_Code_CustFile',
       'Specialty_Description_CustFile', 'Address_CustFile','AMA_No_Contact_CustFile',
       'PDRP_Indicator_CustFile', 'PDRP_Date_CustFile', 'DEA_Number_CustFile',
       'CS_Provider_AMA_ID_CustFile', 'CS_Provider_AMA_Check_Digit_CustFile',
       'NPI_Number_CustFile', 'Territory_ID_CustFile',
       'Call_Status_Code_CustFile','ID_ZipFile','Zip_ZipFile', 'City_ZipFile', 'State_ZipFile',
       'Area_ZipFile', 'Area_Name_ZipFile'], axis=1)
zipp_cust_df['Territory_Name_ZipFile'] = zipp_cust_df['Territory_Name_ZipFile'].str.upper()
terr_mstr_df['Territory_Name_TerrFile']= terr_mstr_df['Territory_Name_TerrFile'].str.upper()

zipp_cust_terrmstr_df= zipp_cust_df.merge(terr_mstr_df, how='inner', left_on=['Territory_Name_ZipFile'], right_on=['Territory_Name_TerrFile']).drop(['Territory_NMBR_TerrFile', 'Territory_Name_TerrFile',
       'Territory_Level_TerrFile', 'ParentTerritory_Id_TerrFile','Rel_ID_CustFile', 'City_CustFile', 'State_CustFile',
'Zip_Code_CustFile'],axis=1)

zipp_cust_terrmstr_df.index.name = 'Cust_Terr_Id'
zipp_cust_terrmstr_df = zipp_cust_terrmstr_df.rename(columns = {'ID_CustFile': 'Cust_Id', 'Territory_ZipFile': 'Territory_NMBR','Territory_Name_ZipFile': 'Territory_Name',
                                                'ID_TerrFile': 'Territory_ID'
                                                })
zipp_cust_terrmstr_df['Cust_Id'] = zipp_cust_terrmstr_df['Cust_Id'].astype(str).replace('\.0', '', regex=True)
##print (zipp_cust_terrmstr_df.convert_dtypes().dtypes)
##print (zipp_cust_terrmstr_df)
zipp_cust_terrmstr_df.index += 1
FileName = 'prodmasters/DM_CUST_TO_TERR.csv'
csv_buffer = StringIO()
zipp_cust_terrmstr_df.to_csv(csv_buffer)
response = client.put_object(
        ACL = 'private',
     Body = csv_buffer.getvalue(),
     Bucket=bucket_name,
     Key=FileName
)

