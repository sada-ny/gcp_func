import pandas as pd
from google.cloud import storage
from io import StringIO
import os

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
FILE_CHUNK_SIZE = os.environ['FILE_CHUNK_SIZE']
ROW_CHUNK = 200000


def save_large_df(inp_df, folder_name, bucket):
    i = 0
    df_size = len(inp_df.index)
    while i < df_size:
        if i+ROW_CHUNK < df_size:
            slice_df = inp_df.iloc[:, i:i+ROW_CHUNK]
            slice_df.to_csv('/tmp/test.csv', index=False)
            blob = bucket.blob(folder_name + '/Historical-Returns/daily_return_' + str(i) + '.csv', chunk_size=int(FILE_CHUNK_SIZE))
            blob.upload_from_filename('/tmp/test.csv')
        else:
            slice_df = inp_df.iloc[:, i:df_size]
            slice_df.to_csv('/tmp/test.csv', index=False)
            blob = bucket.blob(folder_name + '/Historical-Returns/daily_return_' + str(i) + '.csv', chunk_size=int(FILE_CHUNK_SIZE))
            blob.upload_from_filename('/tmp/test.csv')
        i = i + ROW_CHUNK


def save_df(df, relative_path, index_flag=True):
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    df.to_csv('/tmp/test.csv', index=index_flag)
    blob = bucket.blob(relative_path, chunk_size=int(FILE_CHUNK_SIZE))
    blob.upload_from_filename('/tmp/test.csv')


def read_df(relative_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.get_blob(relative_path)
    blob.download_to_filename('/tmp/test.csv')
    df = pd.read_csv('/tmp/test.csv')
    return df

def save_sm_df(df, relative_path, index_flag=True):
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.blob(relative_path)
    blob.upload_from_string(df.to_csv(encoding="UTF-8", index=index_flag))


def read_sm_df(relative_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.get_blob(relative_path)
    str_data = str(blob.download_as_string(), 'utf-8')
    df = pd.read_csv(StringIO(str_data))

    return df
