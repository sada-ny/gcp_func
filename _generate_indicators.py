import pandas as pd
from google.cloud import storage
from io import StringIO
import os
from fredapi import Fred
import ssl

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']


def load_SP_Indices():
    ssl._create_default_https_context = ssl._create_unverified_context

    folder_name = 'SP_Indices'
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)
    src_blob = bucket.get_blob(folder_name + '/Config/' + folder_name + '.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))

    for index, row in universe_df.iterrows():
        url_string = 'https://us.spindices.com/idsexport/file.xls?hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797&selectedModule=' + row['Risk'] + 'GraphView&selectedSubModule=Graph&yearFlag=tenYearFlag&indexId=' + str(row['Index'])
        hist_df = pd.read_excel(url_string, skiprows=8, skipfooter=4, encoding = 'utf-8')
        blob = bucket.blob(folder_name + '/Historical-Prices/' + row['Ticker'] + '.csv')
        blob.upload_from_string(hist_df.to_csv(encoding="UTF-8", index=False))
        print('Ticker: ' + row['Ticker'])
        print(hist_df.shape)


def load_FRED_indicators():

    ssl._create_default_https_context = ssl._create_unverified_context
    fred = Fred(api_key='ce3f4e77e012db41750e39bd818336e6')
    folder_name = 'FRED_Indicators'
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)
    src_blob = bucket.get_blob(folder_name + '/Config/' + folder_name + '.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))

    for index, row in universe_df.iterrows():
        hist_ds = fred.get_series(row['Index'])
        hist_ds.rename(row['Ticker'], inplace=True)
        blob = bucket.blob(folder_name + '/Historical-Prices/' + row['Ticker'] + '.csv')
        blob.upload_from_string(hist_ds.to_csv(encoding="UTF-8", index_label='Date'))
        print('Ticker: ' + row['Index'])
        print(hist_ds.shape)
