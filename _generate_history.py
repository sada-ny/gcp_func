import yfinance as yf
import pandas as pd
import numpy as np
from google.cloud import storage
from io import StringIO
import os
import datetime
from _generate_daily_and_weekly import convert_history_to_return
import _cloud_utils
from _holiday_util import USTradingCalendar
from pandas.tseries.offsets import BDay
from _cloud_utils import save_large_df

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
FILE_CHUNK_SIZE = os.environ['FILE_CHUNK_SIZE']

def create_history(folder_name) :
    print ("Folder name: " + folder_name)

    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.get_blob(folder_name + '/Config/' + folder_name + '.csv')
    str_data = str(blob.download_as_string(), 'utf-8')
    constituents_df = pd.read_csv(StringIO(str_data))

    dly_df_list = []
    wkly_df_list = []

    tickers_list = []
    inception_date_list = []
    dividend_list = []
    close_price_list = []
    
    for index, row in constituents_df.iterrows():
        print (row['Ticker'])
        ticker = row['Ticker']
        file_name = row['Ticker']
        if folder_name == 'nonUSD_Universe':
            file_name = row['Ticker'] + '_mod'
        blob = bucket.get_blob(folder_name + '/Historical-Prices/' + file_name + '.csv')
        str_data = str(blob.download_as_string(), 'utf-8')
        inp_df = pd.read_csv(StringIO(str_data))

        if len(inp_df.index) > 0:

            inp_df = inp_df[['Date', 'Close', 'Adj Close', 'Dividends']]
            inp_df['Date'] = pd.to_datetime(inp_df['Date'])
            inp_df.sort_values(by='Date', inplace=True)

            df1, df2 = convert_history_to_return(inp_df, ticker)

            last_date = inp_df['Date'].max()
            year_ago = datetime.datetime(last_date.year - 1, last_date.month, last_date.day)

            tickers_list.append(ticker)
            inception_date_list.append(df1['Date'].min())
            dividend_list.append(df1[df1['Date'] > year_ago]['Dividends'].sum())
            close_price_list.append(df1.at[df1['Date'].idxmax(), 'Close'])

            dly_df_list.append(df1)
            wkly_df_list.append(df2)
        else:
            print('No data for : ' + ticker)

    daily_df = pd.concat(dly_df_list)
    weekly_df = pd.concat(wkly_df_list)

    print(daily_df.shape)

    # clean up daily price history
    # remove weekends and holidays

    daily_df['Date'] = pd.to_datetime(daily_df['Date'])
    daily_df.sort_values(by=['Ticker', 'Date'], inplace=True, ignore_index=True)

    print ('Remove duplicates')
    daily_df.drop_duplicates(subset=['Ticker', 'Date'], keep='first', ignore_index=True, inplace=True)

    print ('Remove today')
    daily_df = daily_df[daily_df['Date'].dt.date < datetime.date.today()]

    print('Remove holidays')
    cal = USTradingCalendar()
    holidays = cal.holidays(start=daily_df['Date'].min(), end = daily_df['Date'].max())

    daily_df = daily_df[daily_df['Date'].dt.dayofweek < 5]
    daily_df = daily_df[~daily_df['Date'].isin(holidays)]

    print('About to save weekly file')

    weekly_df.to_csv('/tmp/test.csv', index=False)
    blob = bucket.blob(folder_name + '/Historical-Returns/weekly_return.csv', chunk_size=int(FILE_CHUNK_SIZE))
    blob.upload_from_filename('/tmp/test.csv')

    print('About to save daily file')

    daily_df.to_csv('/tmp/test.csv', index=False)
    blob = bucket.blob(folder_name + '/Historical-Returns/daily_return.csv', chunk_size=int(FILE_CHUNK_SIZE))
    blob.upload_from_filename('/tmp/test.csv')

    #save_large_df(daily_df, folder_name, bucket)

    print('Done saving large input')

    stats_df = pd.DataFrame(data={'Ticker' : tickers_list,
                                  'InceptionDate' : inception_date_list,
                                  'Dividends' : dividend_list,
                                  'ClosePrice' : close_price_list})

    constituents_df = constituents_df.merge(stats_df, how='left', on='Ticker')

    blob = bucket.blob(folder_name + '/' + folder_name + '_Enriched.csv')
    blob.upload_from_string(constituents_df.to_csv(encoding="UTF-8", index=False))

    
def create_non_usd_history():
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.get_blob('Macro/Historical-Prices/GBP.csv')
    str_data = str(blob.download_as_string(), 'utf-8')
    gbp_df = pd.read_csv(StringIO(str_data))

    gbp_df['Date'] = pd.to_datetime(gbp_df['Date'], dayfirst=True)

    today_date = datetime.date.today()
    prev_business_day = today_date - BDay(1)

    if (len(gbp_df[gbp_df['Date'].dt.date == today_date].index) > 0) & \
            (len(gbp_df[gbp_df['Date'].dt.date == prev_business_day].index) == 0):

        gbp_df.loc[gbp_df['Date'].dt.date == today_date, 'Date'] = prev_business_day
        blob = bucket.blob('Macro/Historical-Prices/GBP.csv')
        blob.upload_from_string(gbp_df.to_csv(encoding="UTF-8", index=False))
        print (gbp_df.tail())

    non_usd_list = ['IBTG', 'VUKE']

    for non_usd_ticker in non_usd_list:
        blob = bucket.get_blob('nonUSD_Universe/Historical-Prices/' + non_usd_ticker + '.csv')
        str_data = str(blob.download_as_string(), 'utf-8')
        ibtg_df = pd.read_csv(StringIO(str_data))

        print ('Raw')
        print (ibtg_df.tail())

        ibtg_df['Date'] = pd.to_datetime(ibtg_df['Date'], dayfirst=True)
        if (len(ibtg_df[ibtg_df['Date'].dt.date == today_date].index) > 0) & \
                (len(ibtg_df[ibtg_df['Date'].dt.date == prev_business_day].index) == 0):
            ibtg_df.loc[ibtg_df['Date'].dt.date == today_date, 'Date'] = prev_business_day

        ibtg_df['Adj Close'] = np.where(ibtg_df['Adj Close'] > 100, ibtg_df['Adj Close']/100, ibtg_df['Adj Close'])
        ibtg_df['Close'] = np.where(ibtg_df['Close'] > 100, ibtg_df['Close']/100, ibtg_df['Close'])
        ibtg_df['High'] = np.where(ibtg_df['High'] > 100, ibtg_df['High']/100, ibtg_df['High'])
        ibtg_df['Low'] = np.where(ibtg_df['Low'] > 100, ibtg_df['Low']/100, ibtg_df['Low'])
        ibtg_df['Open'] = np.where(ibtg_df['Open'] > 100, ibtg_df['Open']/100, ibtg_df['Open'])

        ibtg_df = pd.merge(ibtg_df, gbp_df[['Date', 'Close']], how='left', on='Date', suffixes=('', '_gbp'))
        ibtg_df['Dividends'] = ibtg_df['Dividends'] * ibtg_df['Close_gbp']
        ibtg_df['Close'] = ibtg_df['Close'] * ibtg_df['Close_gbp']
        ibtg_df['High'] = ibtg_df['High'] * ibtg_df['Close_gbp']
        ibtg_df['Low'] = ibtg_df['Low'] * ibtg_df['Close_gbp']
        ibtg_df['Open'] = ibtg_df['Open'] * ibtg_df['Close_gbp']
        ibtg_df['Adj Close'] = ibtg_df['Adj Close'] * ibtg_df['Close_gbp']

        print ('Adjusted')
        print (ibtg_df.tail())

        ibtg_df = ibtg_df.drop(columns=['Close_gbp'])
        blob = bucket.blob('nonUSD_Universe/Historical-Prices/' + non_usd_ticker + '_mod.csv')
        blob.upload_from_string(ibtg_df.to_csv(encoding="UTF-8", index=False))

    create_history('nonUSD_Universe')
