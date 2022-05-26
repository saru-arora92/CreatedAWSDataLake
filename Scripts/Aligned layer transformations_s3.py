import logging
import boto3
import pandas as pd
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

today = date.today()
folder_date = today.strftime("%m-%d-%y")
now = datetime.now()
current_time = now.strftime("%H_%M_%S")

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

bucket = boto3.resource('s3').Bucket(bucket_name)
objects = bucket.objects.all()
for object in objects:
    ##Unaligned file    
    if object.key.startswith('unaligned/UnalignedFile') and object.key.endswith('.csv'):
        unaligned = client.get_object(Bucket=bucket_name,
                                      Key=object.key)
        unaligned_df = pd.read_csv(unaligned['Body'], sep=",")
        unaligned_df.columns = unaligned_df.columns.map(lambda x: str(x) + '_unaligned')
        unaligned_df = unaligned_df.drop(['2_unaligned', 'MarketId_unaligned',
                                          'MonthEndingDate_unaligned', '1_unaligned'], axis=1)
        unaligned_df = unaligned_df.rename(columns={'RelId_unaligned': 'SourceId_unaligned'})
        unaligned_df['SourceId_unaligned'] = unaligned_df['SourceId_unaligned'].astype(str).replace('\.0', '',
                                                                                                    regex=True)
        unaligned_df['productId_unaligned'] = unaligned_df['productId_unaligned'].astype(str).replace('\.0', '',
                                                                                                      regex=True)
        unaligned_df['WeekEndingDate_unaligned'] = unaligned_df['WeekEndingDate_unaligned'].astype(str)
    ##CustMstr file    
    if object.key.startswith('prodmasters/DM_CUST_MSTR') and object.key.endswith('.csv'):
        cust_mstr = client.get_object(Bucket=bucket_name,
                                      Key=object.key)
        cust_mstr_df = pd.read_csv(cust_mstr['Body'], sep=",")
        cust_mstr_df.columns = cust_mstr_df.columns.map(lambda x: str(x) + '_CustFile')
        cust_mstr_df = cust_mstr_df.drop(['Provider_ID_CustFile',
                                          'Writer_Type_CustFile', 'First_Name_CustFile', 'Middle_Name_CustFile',
                                          'Last_Name_CustFile', 'Title_CustFile', 'Specialty_Code_CustFile',
                                          'Specialty_Description_CustFile', 'Address_CustFile', 'City_CustFile',
                                          'State_CustFile', 'Zip_Code_CustFile', 'AMA_No_Contact_CustFile',
                                          'PDRP_Indicator_CustFile', 'PDRP_Date_CustFile', 'DEA_Number_CustFile',
                                          'CS_Provider_AMA_ID_CustFile', 'CS_Provider_AMA_Check_Digit_CustFile',
                                          'NPI_Number_CustFile', 'Territory_ID_CustFile',
                                          'Call_Status_Code_CustFile'], axis=1)
        cust_mstr_df['Rel_ID_CustFile'] = cust_mstr_df['Rel_ID_CustFile'].astype(str).replace('\.0', '', regex=True)

    ## CusttoTerr File
    if object.key.startswith('prodmasters/DM_CUST_TO_TERR') and object.key.endswith('.csv'):
        cust_to_terr = client.get_object(Bucket=bucket_name,
                                         Key=object.key)
        cust_to_terr_df = pd.read_csv(cust_to_terr['Body'], sep=",")
        cust_to_terr_df.columns = cust_to_terr_df.columns.map(lambda x: str(x) + '_CustToTerrFile')
        cust_to_terr_df = cust_to_terr_df.drop(['Territory_NMBR_CustToTerrFile', 'Territory_Name_CustToTerrFile',
                                                'Territory_ID_CustToTerrFile'], axis=1)
    ##ClntMrktDef File
    if object.key.startswith('prodmasters/DM_CLNT_MARKET_DEFINITION') and object.key.endswith('.csv'):
        clnt_market_def = client.get_object(Bucket=bucket_name,
                                            Key=object.key)
        clnt_market_def_df = pd.read_csv(clnt_market_def['Body'], sep=",")
        clnt_market_def_df.columns = clnt_market_def_df.columns.map(lambda x: str(x) + '_MrktDefFile')
        clnt_market_def_df = clnt_market_def_df.drop(['Veeva_product_Name_MrktDefFile', 'Market_MrktDefFile',
                                                      'Company_Product_MrktDefFile', 'Product_Group_MrktDefFile',
                                                      'Description_MrktDefFile'], axis=1)
        clnt_market_def_df['product_id_MrktDefFile'] = clnt_market_def_df['product_id_MrktDefFile'].astype(str).replace(
            '\.0', '', regex=True)

    ##DMTime File
    if object.key.startswith('prodmasters/DM_TIME') and object.key.endswith('.csv'):
        time = client.get_object(Bucket=bucket_name,
                                 Key=object.key)
        time_df = pd.read_csv(time['Body'], sep=",")
        time_df['END_DT'] = pd.to_datetime(time_df['END_DT'])
        time_df['END_DT'] = time_df['END_DT'].astype(str).str.replace('-', '').astype(str)
        time_df.columns = time_df.columns.map(lambda x: str(x) + '_TimeFile')
        time_df = time_df.drop(['ID_TimeFile', 'TIME_VALUE_TimeFile', 'START_DT_TimeFile', 'MONTH_START_DT_TimeFile',
                                'QUARTER_START_DT_TimeFile',
                                'YEAR_START_DT_TimeFile', 'YEAR_NBR_TimeFile', 'QUARTER_NBR_TimeFile',
                                'MONTH_NBR_TimeFile', 'DAY_NBR_TimeFile', 'PREV_TIME_ID_TimeFile'], axis=1)

custmstr_unaligned_df = unaligned_df.merge(cust_mstr_df, how='inner', left_on=['SourceId_unaligned'],
                                           right_on=['Rel_ID_CustFile']).drop(['Rel_ID_CustFile'], axis=1)
custmstr_unaligned_df = custmstr_unaligned_df.rename(columns={'ID_CustFile': 'Cust_Id_Custmstrfile'})

custmstr_unaligned_custtoterr_df = custmstr_unaligned_df.merge(cust_to_terr_df, how='inner',
                                                               left_on=['Cust_Id_Custmstrfile'],
                                                               right_on=['Cust_Id_CustToTerrFile']).drop(
    ['Cust_Id_CustToTerrFile'], axis=1)

cust_un_custtoterr_mrkt_df = custmstr_unaligned_custtoterr_df.merge(clnt_market_def_df, how='inner',
                                                                    left_on=['productId_unaligned'],
                                                                    right_on=['product_id_MrktDefFile']).drop(
    ['product_id_MrktDefFile'], axis=1)
df = cust_un_custtoterr_mrkt_df.merge(time_df, how='inner', left_on=['WeekEndingDate_unaligned'],
                                      right_on=['END_DT_TimeFile']).drop(['END_DT_TimeFile'], axis=1)
df.loc[df['TIME_TYPE_TimeFile'] == 'W']
df['SourceId_unaligned'] = df['SourceId_unaligned'].apply(lambda x: '{0:0>9}'.format(x))
df = df.drop(['SourceId_unaligned', 'WeekEndingDate_unaligned', 'ID_MrktDefFile', 'TIME_TYPE_TimeFile'], axis=1)
df['newrxcount_unaligned'] = df['newrxcount_unaligned'].astype(float)
df['refillrxcount_unaligned'] = df['refillrxcount_unaligned'].astype(float)
df['totalrxcount_unaligned'] = df['totalrxcount_unaligned'].astype(float)
df['newrxquantity_unaligned'] = df['newrxquantity_unaligned'].astype(float)
df['refillrxquantity_unaligned'] = df['refillrxquantity_unaligned'].astype(float)
df['totalrxquantity_unaligned'] = df['totalrxquantity_unaligned'].astype(float)
df['newrxcost_unaligned'] = df['newrxcost_unaligned'].astype(float)
df['refillrxcost_unaligned'] = df['refillrxcost_unaligned'].astype(float)
df['totalrxcost_unaligned'] = df['totalrxcost_unaligned'].astype(float)
df['Cust_Id_Custmstrfile'] = df['Cust_Id_Custmstrfile'].astype(int)
df['Cust_Terr_Id_CustToTerrFile'] = df['Cust_Terr_Id_CustToTerrFile'].astype(int)
df['TIME_ID_TimeFile'] = df['TIME_ID_TimeFile'].astype(int)
df['productId_unaligned'] = df['productId_unaligned'].astype(int)
df["1"] = ""import logging
import boto3
import pandas as pd
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

today = date.today()
folder_date = today.strftime("%m-%d-%y")
now = datetime.now()
current_time = now.strftime("%H_%M_%S")

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

bucket = boto3.resource('s3').Bucket(bucket_name)
objects = bucket.objects.all()
for object in objects:
##Unaligned file    
    if object.key.startswith('unaligned/UnalignedFile') and object.key.endswith('.csv'):
        unaligned = client.get_object(Bucket=bucket_name,
                    Key=object.key) 
        unaligned_df = pd.read_csv(unaligned['Body'], sep=",")
        unaligned_df.columns = unaligned_df.columns.map(lambda x: str(x) + '_unaligned')
        unaligned_df= unaligned_df.drop(['2_unaligned', 'MarketId_unaligned',
                                         'MonthEndingDate_unaligned','1_unaligned'],axis=1)
        unaligned_df = unaligned_df.rename(columns = {'RelId_unaligned': 'SourceId_unaligned'})
        unaligned_df['SourceId_unaligned'] = unaligned_df['SourceId_unaligned'].astype(str).replace('\.0', '', regex=True)
        unaligned_df['productId_unaligned'] = unaligned_df['productId_unaligned'].astype(str).replace('\.0', '', regex=True)
        unaligned_df['WeekEndingDate_unaligned'] = unaligned_df['WeekEndingDate_unaligned'].astype(str)
##CustMstr file    
    if object.key.startswith('prodmasters/DM_CUST_MSTR') and object.key.endswith('.csv'):
        cust_mstr = client.get_object(Bucket=bucket_name,
                     Key=object.key)        
        cust_mstr_df = pd.read_csv(cust_mstr['Body'], sep=",")
        cust_mstr_df.columns = cust_mstr_df.columns.map(lambda x: str(x) + '_CustFile')
        cust_mstr_df = cust_mstr_df.drop(['Provider_ID_CustFile',
                                          'Writer_Type_CustFile', 'First_Name_CustFile', 'Middle_Name_CustFile',
                                          'Last_Name_CustFile', 'Title_CustFile', 'Specialty_Code_CustFile',
                                          'Specialty_Description_CustFile', 'Address_CustFile', 'City_CustFile',
                                          'State_CustFile', 'Zip_Code_CustFile', 'AMA_No_Contact_CustFile',
                                          'PDRP_Indicator_CustFile', 'PDRP_Date_CustFile', 'DEA_Number_CustFile',
                                          'CS_Provider_AMA_ID_CustFile', 'CS_Provider_AMA_Check_Digit_CustFile',
                                          'NPI_Number_CustFile', 'Territory_ID_CustFile',
                                          'Call_Status_Code_CustFile'],axis=1)
        cust_mstr_df['Rel_ID_CustFile'] = cust_mstr_df['Rel_ID_CustFile'].astype(str).replace('\.0', '', regex=True)

## CusttoTerr File
    if object.key.startswith('prodmasters/DM_CUST_TO_TERR') and object.key.endswith('.csv'):
        cust_to_terr = client.get_object(Bucket=bucket_name,
                                         Key=object.key)
        cust_to_terr_df = pd.read_csv(cust_to_terr['Body'], sep=",")
        cust_to_terr_df.columns = cust_to_terr_df.columns.map(lambda x: str(x) + '_CustToTerrFile')
        cust_to_terr_df = cust_to_terr_df.drop(['Territory_NMBR_CustToTerrFile', 'Territory_Name_CustToTerrFile',
                                                'Territory_ID_CustToTerrFile'],axis=1)
##ClntMrktDef File
    if object.key.startswith('prodmasters/DM_CLNT_MARKET_DEFINITION') and object.key.endswith('.csv'):
        clnt_market_def = client.get_object(Bucket=bucket_name,
                                            Key= object.key)
        clnt_market_def_df = pd.read_csv(clnt_market_def['Body'], sep=",")
        clnt_market_def_df.columns = clnt_market_def_df.columns.map(lambda x: str(x) + '_MrktDefFile')
        clnt_market_def_df = clnt_market_def_df.drop(['Veeva_product_Name_MrktDefFile', 'Market_MrktDefFile',
                                                      'Company_Product_MrktDefFile', 'Product_Group_MrktDefFile',
                                                      'Description_MrktDefFile'],axis=1)
        clnt_market_def_df['product_id_MrktDefFile'] = clnt_market_def_df['product_id_MrktDefFile'].astype(str).replace('\.0', '', regex=True)

##DMTime File
    if object.key.startswith('prodmasters/DM_TIME') and object.key.endswith('.csv'): 
        time = client.get_object(Bucket=bucket_name,
                                 Key=object.key)
        time_df = pd.read_csv(time['Body'], sep=",")
        time_df['END_DT'] = pd.to_datetime(time_df['END_DT'])
        time_df['END_DT'] = time_df['END_DT'].astype(str).str.replace('-','').astype(str)
        time_df.columns = time_df.columns.map(lambda x: str(x) + '_TimeFile')
        time_df = time_df.drop(['ID_TimeFile','TIME_VALUE_TimeFile', 'START_DT_TimeFile','MONTH_START_DT_TimeFile', 'QUARTER_START_DT_TimeFile',
                                'YEAR_START_DT_TimeFile', 'YEAR_NBR_TimeFile', 'QUARTER_NBR_TimeFile',
                                'MONTH_NBR_TimeFile', 'DAY_NBR_TimeFile', 'PREV_TIME_ID_TimeFile'],axis=1)
        
custmstr_unaligned_df= unaligned_df.merge(cust_mstr_df, how='inner', left_on=['SourceId_unaligned'], right_on=['Rel_ID_CustFile']).drop(['Rel_ID_CustFile'],axis=1)
custmstr_unaligned_df = custmstr_unaligned_df.rename(columns = {'ID_CustFile': 'Cust_Id_Custmstrfile'})

custmstr_unaligned_custtoterr_df= custmstr_unaligned_df.merge(cust_to_terr_df,how='inner', left_on=['Cust_Id_Custmstrfile'], right_on=['Cust_Id_CustToTerrFile']).drop(['Cust_Id_CustToTerrFile'],axis=1)

cust_un_custtoterr_mrkt_df= custmstr_unaligned_custtoterr_df.merge(clnt_market_def_df,how='inner', left_on=['productId_unaligned'], right_on=['product_id_MrktDefFile']).drop(['product_id_MrktDefFile'],axis=1)
df=cust_un_custtoterr_mrkt_df.merge(time_df,how='inner',left_on=['WeekEndingDate_unaligned'], right_on=['END_DT_TimeFile']).drop(['END_DT_TimeFile'],axis=1) 
df.loc[df['TIME_TYPE_TimeFile'] == 'W']
df['SourceId_unaligned'] = df['SourceId_unaligned'].apply(lambda x: '{0:0>9}'.format(x))
df = df.drop(['SourceId_unaligned','WeekEndingDate_unaligned', 'ID_MrktDefFile','TIME_TYPE_TimeFile'],axis=1)
df['newrxcount_unaligned'] = df['newrxcount_unaligned'].astype(float)
df['refillrxcount_unaligned'] = df['refillrxcount_unaligned'].astype(float)
df['totalrxcount_unaligned'] = df['totalrxcount_unaligned'].astype(float)
df['newrxquantity_unaligned'] = df['newrxquantity_unaligned'].astype(float)
df['refillrxquantity_unaligned'] = df['refillrxquantity_unaligned'].astype(float)
df['totalrxquantity_unaligned'] = df['totalrxquantity_unaligned'].astype(float)
df['newrxcost_unaligned'] = df['newrxcost_unaligned'].astype(float)
df['refillrxcost_unaligned'] = df['refillrxcost_unaligned'].astype(float)
df['totalrxcost_unaligned'] = df['totalrxcost_unaligned'].astype(float)
df['Cust_Id_Custmstrfile'] = df['Cust_Id_Custmstrfile'].astype(int)
df['Cust_Terr_Id_CustToTerrFile'] = df['Cust_Terr_Id_CustToTerrFile'].astype(int)
df['TIME_ID_TimeFile'] = df['TIME_ID_TimeFile'].astype(int)
df['productId_unaligned'] = df['productId_unaligned'].astype(int)
df["1"] = ""
df.index.name = 2
FileName = "aligned/aligned_cust_weekly_salesdata_"+folder_date+"_"+current_time+".csv"
csv_buffer = StringIO()
df.to_csv(csv_buffer)
response = client.put_object(
        ACL = 'private',
     Body = csv_buffer.getvalue(),
     Bucket=bucket_name,
     Key=FileName
)

##df = df.drop(['Cust_Id_Custmstrfile'],axis=1)
#df["1"] = ""
#df.index.name = 2
#FileName = "aligned/aligned_terr_weekly_salesdata_"+folder_date+"_"+current_time+".csv"
#csv_buffer = StringIO()
#df.to_csv(csv_buffer)
#response = client.put_object(
#        ACL = 'private',
#     Body = csv_buffer.getvalue(),
#     Bucket=bucket_name,
#     Key=FileName
#)

df.index.name = 2
FileName = "aligned/aligned_cust_weekly_salesdata_" + folder_date + "_" + current_time + ".csv"
csv_buffer = StringIO()
df.to_csv(csv_buffer)
response = client.put_object(
    ACL='private',
    Body=csv_buffer.getvalue(),
    Bucket=bucket_name,
    Key=FileName
)

##df = df.drop(['Cust_Id_Custmstrfile'],axis=1)
# df["1"] = ""
# df.index.name = 2
# FileName = "aligned/aligned_terr_weekly_salesdata_"+folder_date+"_"+current_time+".csv"
# csv_buffer = StringIO()
# df.to_csv(csv_buffer)
# response = client.put_object(
#        ACL = 'private',
#     Body = csv_buffer.getvalue(),
#     Bucket=bucket_name,
#     Key=FileName
# )
