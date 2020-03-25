import yfinance as yf
import pandas as pd
import datetime
from google.cloud import storage
from io import StringIO
import os
import ssl

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']


def get_stock_info():
    ssl._create_default_https_context = ssl._create_unverified_context

    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    src_blob = bucket.get_blob('SP500/Config/SP500.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    stock_df = pd.read_csv(StringIO(str_data))

    src_blob = bucket.get_blob('SP500/Config/stock_info.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    info_df = pd.read_csv(StringIO(str_data))

    super_dict = {}

    for index, row in stock_df.iterrows():
        ticker = yf.Ticker(row['Ticker'])
        try:
            info_table = ticker.info
            super_dict[row['Ticker']] = info_table
        except:
            print ("**********Cannot get info for: " + row['Ticker'])

    stock_info = pd.DataFrame(super_dict)
    stock_info = stock_info.T
    #stock_info = stock_info.filter(items=info_df['Fields'].tolist())
    stock_info = stock_info.reset_index()
    stock_info.rename(columns={"index": "Ticker"}, inplace=True)

    info_blob = bucket.blob('SP500/Info/' + datetime.date.today().strftime("%Y%m%d") + '_SP500_Enriched.csv')
    info_blob.upload_from_string(stock_info.to_csv(encoding="UTF-8", index=False))

