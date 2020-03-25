import yfinance as yf
import pandas as pd
from google.cloud import storage
from io import StringIO
import os

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

def list_blobs_with_prefix(bucket_name, prefix=None, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        a/1.txt
        a/b/2.txt
    If you just specify prefix = 'a', you'll get back:
        a/1.txt
        a/b/2.txt
    However, if you specify prefix='a' and delimiter='/', you'll get back:
        a/1.txt
    Additionally, the same request will return blobs.prefixes populated with:
        a/b/
    """

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )

    print("Blobs:")
    for blob in blobs:
        print(blob.name)

    if delimiter:
        print("Prefixes:")
        for prefix in blobs.prefixes:
            print(prefix)




def load_historical_data(folder_name, period_length='20y'):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)
    src_blob = bucket.get_blob(folder_name + '/Config/' + folder_name + '.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))

    use_symbol = False
    if 'Symbol' in universe_df.columns:
        use_symbol = True

    type_exists = False
    if 'Type' in universe_df:
        print ('Type found???')
        type_exists = True

    for index, row in universe_df.iterrows():
        symbol = row['Ticker']
        if use_symbol:
            symbol = row['Symbol']

        print (symbol)
        ticker = yf.Ticker(symbol)
        try:
            hist_df = ticker.history(period=period_length, auto_adjust=False)
        except Exception as e:
            print ("Could not get history for " + period_length)
            hist_df = ticker.history(period="20y", auto_adjust=False)
        if type_exists:
            if row['Type'] == 'NAV':
                cef_blob = bucket.get_blob(folder_name + '/Historical-Prices/' + row['Ticker'][1:-1] + '.csv')
                str_data = str(cef_blob.download_as_string(), 'utf-8')
                cef_df = pd.read_csv(StringIO(str_data))
                cef_df['Date'] = pd.to_datetime(cef_df['Date'])
                hist_df = hist_df.drop(columns=['Dividends'])
                hist_df = pd.merge(hist_df, cef_df[['Date', 'Dividends']], on='Date', how='left')
                hist_df = hist_df[~pd.isnull(hist_df['Dividends'])]
        blob = bucket.blob(folder_name + '/Historical-Prices/' + row['Ticker'] + '.csv')
        blob.upload_from_string(hist_df.to_csv(encoding="UTF-8"))
