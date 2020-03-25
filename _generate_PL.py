import pandas as pd
import numpy as np
import datetime
from pandas.tseries.offsets import CDay
from google.cloud import storage
from io import StringIO
import os
from _holiday_util import USTradingCalendar

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']


def create_holdings_pl():

    #######################################################################
    start_date = datetime.datetime(2020, 1, 1)
    #######################################################################

    print("Starting PL")
    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    blob = bucket.get_blob('Holdings/Config/Holdings.csv')
    str_data = str(blob.download_as_string(), 'utf-8')
    hld_df = pd.read_csv(StringIO(str_data))

    # retain Investment and Retirement
    hld_df = hld_df[(hld_df['Category'] == 'INVESTMENT') | (hld_df['Category'] == 'RETIREMENT')]

    # if Inception Date is missing, make it start_date
    hld_df.loc[pd.isnull(hld_df['Inception Date']), 'Inception Date'] = start_date

    hld_df['Inception Date'] = pd.to_datetime(hld_df['Inception Date'], dayfirst=True)
    hld_df['Expiration Date'] = pd.to_datetime(hld_df['Expiration Date'], dayfirst=True)

    # add cash entry for new trades and change sign for sell
    # columns impacted : Ticker, Direction, Trade Type, Cost Price and Quantity

    new_trades_df = hld_df[hld_df['Inception Date'] > start_date].copy()

    new_trades_df['Quantity'] = new_trades_df['Quantity'] * new_trades_df['Cost Price']
    new_trades_df['Cost Price'] = np.nan
    new_trades_df['Direction'] = np.where(new_trades_df['Direction'] == 'B', 'S', 'B')
    new_trades_df['Trade Type'] = 'CASH'
    new_trades_df['Ticker'] = new_trades_df['Asset Currency']

    hld_df = pd.concat([hld_df, new_trades_df], ignore_index=True)
    hld_df['Quantity'] = np.where(hld_df['Direction'] == 'B', hld_df['Quantity'], -hld_df['Quantity'])

    print('created holdings')

    blob = bucket.get_blob('Universe/Historical-Returns/daily_return.csv')
    blob.download_to_filename('/tmp/test.csv')
    dly_df = pd.read_csv('/tmp/test.csv')
    dly_df['Date'] = pd.to_datetime(dly_df['Date'], dayfirst=True)

    blob = bucket.get_blob('Macro/Historical-Returns/daily_return.csv')
    blob.download_to_filename('/tmp/test.csv')
    macro_df = pd.read_csv('/tmp/test.csv')
    macro_df['Date'] = pd.to_datetime(macro_df['Date'], dayfirst=True)

    blob = bucket.get_blob('nonUSD_Universe/Historical-Returns/daily_return.csv')
    blob.download_to_filename('/tmp/test.csv')
    non_usd_dly_df = pd.read_csv('/tmp/test.csv')
    non_usd_dly_df['Date'] = pd.to_datetime(non_usd_dly_df['Date'], dayfirst=True)

    dly_df = pd.concat([dly_df, macro_df, non_usd_dly_df])

    dly_df = dly_df[dly_df['Date'] >= start_date]

    print('created price history')
    # add USD
    usd_df = pd.DataFrame({'Ticker' : 'USD',
                           'Date' : dly_df[dly_df['Date'] >= start_date]['Date'].unique(),
                           'Close' : 1.0,
                           'Dividends' : 0.0,
                           '1D_Prc_Return' : 0.0,
                           '1D_Div_Return' : 0.0,
                           '1D_Return' : 0.0,
                           'Index' : 1.0})

    dly_df = pd.concat([dly_df, usd_df])

    print('Added USD')

    hld_df = pd.merge(dly_df, hld_df, on='Ticker', how='right')

    print ('First Holdings merge - Close')
    hld_df = pd.merge(hld_df, dly_df[['Date', 'Ticker', 'Close']], left_on=['Asset Currency', 'Date'], right_on=['Ticker', 'Date'],
                      how='left', suffixes=('', '_FX'))
    print ('Second Holdings merge - FX')
    hld_df = hld_df.sort_values(by='Date')
    dly_df = dly_df.sort_values(by='Date')

    hld_df = pd.merge_asof(hld_df, dly_df[['Date', 'Ticker', 'Close']], on='Date', by='Ticker',
                           direction='backward', allow_exact_matches=False, suffixes=('', '_prev'))

    print ('Third Holdings merge - Prev Close')
    hld_df = hld_df[hld_df['Date'] >= hld_df['Inception Date']]
    print(hld_df['Ticker'].unique())
    print('D')

    hld_df = hld_df[hld_df['Date'] >= start_date]

    hld_df['is_alive'] = np.select([(hld_df['Trade Type'] == 'BOND') & ((hld_df['Date'] < hld_df['Inception Date']) | (hld_df['Date'] > hld_df['Expiration Date'])),
                                    (hld_df['Trade Type'] == 'OPTIONS') & (hld_df['Date'] > hld_df['Expiration Date'])],
                                   [0, 0],
                                   1)

    hld_df['Post_tax'] = np.where(hld_df['Description'] == 'DEFERRED', 0.5, 1)

    hld_df['USD_Div_CF'] = np.select([hld_df['Trade Type'] == 'STOCK', hld_df['Trade Type'] == 'BOND'],
                                     [hld_df['Quantity'] * hld_df['Dividends'],
                                      hld_df['Quantity'] * hld_df['Coupon'] * (hld_df['Date'] - (hld_df['Date'] - CDay(1, calendar=USTradingCalendar()))).dt.days/365]) * hld_df['is_alive'] * hld_df['Post_tax']

    hld_df['LC_Div_CF'] = hld_df['USD_Div_CF'] / hld_df['Close_FX']

    hld_df['USD_MV'] = np.select([(hld_df['Trade Type'] == 'OPTIONS') & (hld_df['Option Type'] == 'C'),
                                  (hld_df['Trade Type'] == 'OPTIONS') & (hld_df['Option Type'] == 'P')],
                                 [np.maximum(hld_df['Close'] - hld_df['Strike Price'], 0) * hld_df['Quantity'],
                                  np.maximum(hld_df['Strike Price'] - hld_df['Close'], 0) * hld_df['Quantity']],
                                 hld_df['Quantity'] * hld_df['Close']) * hld_df['is_alive'] * hld_df['Post_tax']

    hld_df['LC_MV'] = hld_df['USD_MV'] / hld_df['Close_FX']

    hld_df['USD_Capital_Gain'] = np.select([(hld_df['Trade Type'] == 'OPTIONS') & (hld_df['Option Type'] == 'C'),
                                            (hld_df['Trade Type'] == 'OPTIONS') & (hld_df['Option Type'] == 'P')],
                                           [(np.maximum(hld_df['Close'] - hld_df['Strike Price'], 0) - np.maximum(hld_df['Close_prev'] - hld_df['Strike Price'], 0)) * hld_df['Quantity'],
                                            (np.maximum(hld_df['Strike Price'] - hld_df['Close'], 0) - np.maximum(hld_df['Strike Price'] - hld_df['Close_prev'], 0)) * hld_df['Quantity']],
                                            hld_df['Quantity'] * (hld_df['Close'] - hld_df['Close_prev'])) * hld_df['is_alive'] * hld_df['Post_tax']



    # Dividends have to increase the cash balance in RETIREMENT and INVESTMENT
    # all dividends in a given account for a given day should be added to USD and GBP balance
    print (hld_df.columns)
    temp_df = hld_df.groupby(['Account', 'Date', 'Asset Currency'])['LC_Div_CF'].sum().reset_index()
    div_df = hld_df.groupby(['Account', 'Date', 'Asset Currency']).agg({'LC_Div_CF' : 'sum',
                                                                        'USD_Div_CF' : 'sum'}).reset_index()

    #div_df = hld_df.groupby(['Account', 'Date', 'Asset Currency'])['LC_Div_CF', 'USD_Div_CF'].sum().reset_index()
    div_df['C_LC_Div_CF'] = div_df.groupby(['Account','Asset Currency'])['LC_Div_CF'].cumsum()
    div_df['C_USD_Div_CF'] = div_df.groupby(['Account','Asset Currency'])['USD_Div_CF'].cumsum()
    div_df['Description'] = 'DIV_CF'

    new_hld_df = pd.merge(hld_df, div_df, left_on=['Description', 'Account', 'Date', 'Ticker'],
                          right_on=['Description', 'Account', 'Date', 'Asset Currency'], how='left', suffixes=('', '_y'))

    new_hld_df['USD_MV'] = np.where(pd.isnull(new_hld_df['C_USD_Div_CF']),
                                    new_hld_df['USD_MV'],
                                    new_hld_df['USD_MV'] + new_hld_df['C_USD_Div_CF'])

    new_hld_df['LC_MV'] = np.where(pd.isnull(new_hld_df['C_LC_Div_CF']),
                                   new_hld_df['LC_MV'],
                                   new_hld_df['LC_MV'] + new_hld_df['C_LC_Div_CF'])


    # clean up
    new_hld_df = new_hld_df.drop(columns=['C_USD_Div_CF', 'C_LC_Div_CF', 'USD_Div_CF_y', 'LC_Div_CF_y', 'Asset Currency_y'])

    blob = bucket.blob('Holdings/Historical-Returns/daily_return.csv', chunk_size=10 * 1024 * 1024)
    hld_df.to_csv('/tmp/test.csv', index=False)
    blob.upload_from_filename('/tmp/test.csv')

    blob = bucket.get_blob('Universe/Summary.csv')
    str_data = str(blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))
    columns_to_keep = ['Ticker','Name','Type','Asset','Region','Strategy','Sector','SubSector']
    universe_df = universe_df[columns_to_keep]
    new_row_df = universe_df[universe_df['Ticker'] == 'GBP'].copy()
    new_row_df['Name'] = 'US Dollar'
    new_row_df['Sector'] = 'GBP'
    new_row_df['Ticker'] = 'USD'
    universe_df = pd.concat([universe_df, new_row_df], ignore_index=True)

    new_hld_df = pd.merge(new_hld_df, universe_df, on='Ticker', how='left')

    blob = bucket.blob('Holdings/Historical-Returns/new_daily_return.csv', chunk_size=10 * 1024 * 1024)
    new_hld_df.to_csv('/tmp/test.csv', index=False)
    blob.upload_from_filename('/tmp/test.csv')

    # for each row, add total PL, dividend, capital gain

    print('Finished PL')

