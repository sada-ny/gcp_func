import pandas as pd
from google.cloud import storage
from io import StringIO
import datetime
import math
from scipy import stats

import os
import numpy as np
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import CDay
from _holiday_util import USTradingCalendar

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

def zs_6m(series):
    if series.size >= 123:
        return stats.zscore(series.tail(123).to_numpy())[-1]
    else:
        return np.nan

def zs_1y(series):
    if series.size >= 252:
        return stats.zscore(series.tail(252).to_numpy())[-1]
    else:
        return np.nan

def zs_3y(series):
    if series.size >= 252*3:
        return stats.zscore(series.tail(252*3).to_numpy())[-1]
    else:
        return np.nan

def zs_all(series):
    if series.size > 0:
        return stats.zscore(series.to_numpy())[-1]
    else:
        return np.nan

def avg_6m(series):
    if series.size >= 123:
        return series.tail(123).mean()
    else:
        return np.nan

def avg_1y(series):
    if series.size >= 252:
        return series.tail(252).mean()
    else:
        return np.nan

def min_1y(series):
    return series.tail(252).min()

def max_1y(series):
    return series.tail(252).max()

def avg_3y(series):
    if series.size >= 252*3:
        return series.tail(252*3).mean()
    else:
        return np.nan

def avg_all(series):
    if series.size > 0:
        return series.mean()
    else:
        return np.nan

def process_sharpe_ratio(ret_df):
    ret_df = ret_df[['Ticker', 'Date', '1D_Return', 'Quarter', 'Year']]
    stat_df = ret_df.groupby(['Ticker', 'Quarter', 'Year'])['1D_Return'].agg(['mean', 'std', 'count']).reset_index()
    stat_df['SR'] = stat_df['mean'] / stat_df['std'] * math.sqrt(252)
    stat_df = stat_df[stat_df['count'] > 40]
    return stat_df

def process_stress_matrix(str_data):
    recent_date = datetime.date.today() - CDay(1, calendar=USTradingCalendar())
    stress_period_df = pd.read_csv(StringIO(str_data))
    stress_period_df.loc[pd.isnull(stress_period_df['EndDate']), 'EndDate'] = recent_date

    for index, row in stress_period_df.iterrows():
        if row['Type'] == 'Relative':
            if row['Units'] == 'D':
                stress_period_df.at[index, 'StartDate'] = recent_date - CDay(row['Offset'], calendar=USTradingCalendar())
            elif row['Units'] == 'W':
                stress_period_df.at[index, 'StartDate'] = recent_date - relativedelta(weeks=row['Offset'])
            elif row['Units'] == 'M':
                stress_period_df.at[index, 'StartDate'] = recent_date - relativedelta(months=row['Offset'])
            elif row['Units'] == 'Y':
                stress_period_df.at[index, 'StartDate'] = recent_date - relativedelta(years=row['Offset'])

            stress_period_df.at[index, 'EndDate'] = recent_date

    stress_period_df['StartDate'] = pd.to_datetime(stress_period_df['StartDate'])
    stress_period_df['EndDate'] = pd.to_datetime(stress_period_df['EndDate'])
    stress_period_df = stress_period_df[['Label', 'StartDate', 'EndDate']]
    return stress_period_df

def create_summary(folder_name):

    #############################################################################
    data_year = 2019
    data_quarter = 4
    regression_style = 'Asset_Factor'
    #############################################################################

    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)


    src_blob = bucket.get_blob('Universe/Config/Return_Periods.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    stress_period_df = process_stress_matrix(str_data)

    src_blob = bucket.get_blob(folder_name + '/' + folder_name + '_Enriched.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))
    universe_df['Quarter'] = data_quarter
    universe_df['Year']  = data_year

    src_blob = bucket.get_blob(folder_name + '/Regression-Results/' + regression_style + '_lr_results.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    lr_df = pd.read_csv(StringIO(str_data))

    blob = bucket.get_blob(folder_name + '/Historical-Returns/daily_return.csv')
    blob.download_to_filename('/tmp/test.csv')
    dly_df = pd.read_csv('/tmp/test.csv')
    dly_df['Date'] = pd.to_datetime(dly_df['Date'], dayfirst=True)
    dly_df['Quarter'] = dly_df['Date'].dt.quarter
    dly_df['Year'] = dly_df['Date'].dt.year

    sr_df = process_sharpe_ratio(dly_df)

    universe_df = pd.merge(universe_df, sr_df[['Ticker', 'Quarter', 'Year', 'SR']], how='left', on=['Ticker', 'Quarter', 'Year'])
    universe_df = pd.merge(universe_df, lr_df, how='left', on=['Ticker', 'Quarter', 'Year'])

    stress_period_df['Tmp'] = 1
    universe_df['Tmp'] = 1

    stress_df = pd.merge(universe_df, stress_period_df[['Label', 'StartDate', 'EndDate', 'Tmp']], on='Tmp', how='outer')

    dly_df = dly_df.sort_values(['Date', 'Ticker'])

    stress_df = stress_df.sort_values('StartDate')
    stress_df = pd.merge_asof(stress_df, dly_df[['Ticker', 'Date', 'Index', 'Close']],
                              by='Ticker', left_on='StartDate', right_on='Date',
                              tolerance=pd.Timedelta('3 days'), direction='nearest')

    stress_df = stress_df.sort_values('EndDate')
    stress_df = pd.merge_asof(stress_df, dly_df[['Ticker', 'Date', 'Index', 'Close']],
                              by='Ticker', left_on='EndDate', right_on='Date', suffixes=('_start', '_end'),
                              tolerance=pd.Timedelta('3 days'), direction='nearest')

    stress_df['Perf'] = (stress_df['Index_end'] - stress_df['Index_start'])/stress_df['Index_start']
    stress_df['Price_Perf'] = (stress_df['Close_end'] - stress_df['Close_start'])/stress_df['Close_start']
    stress_df['Div_Perf'] = stress_df['Perf'] - stress_df['Price_Perf']

    stress_df = pd.merge(universe_df, stress_df[['Ticker', 'Label', 'Perf', 'Price_Perf', 'Div_Perf']], on='Ticker', how='left')


    stress_total_df = stress_df.pivot_table(values='Perf', columns='Label', index='Ticker').reset_index()

    stress_prc_df = stress_df.pivot_table(values='Price_Perf', columns='Label', index='Ticker').reset_index()
    stress_prc_df.columns = stress_prc_df.columns.str.replace('Ret', 'Prc_Ret')
    stress_prc_df.columns = stress_prc_df.columns.str.replace('Scen', 'Prc_Scen')

    stress_div_df = stress_df.pivot_table(values='Div_Perf', columns='Label', index='Ticker').reset_index()
    stress_div_df.columns = stress_div_df.columns.str.replace('Ret', 'Div_Ret')
    stress_div_df.columns = stress_div_df.columns.str.replace('Scen', 'Div_Scen')

    print('Before any merging')
    print(universe_df.shape)
    universe_df = pd.merge(universe_df, stress_total_df, how='left', on='Ticker')
    print('After total')
    print(universe_df.shape)
    universe_df = pd.merge(universe_df, stress_prc_df, how='left', on='Ticker')
    print('After prc')
    print(universe_df.shape)
    universe_df = pd.merge(universe_df, stress_div_df, how='left', on='Ticker')
    print('After div')
    print(universe_df.shape)
    universe_df = universe_df.drop(columns=['Tmp', 'Count'])

    dly_df = dly_df.sort_values(['Ticker', 'Date'])

    div_df = dly_df[['Ticker', 'Date', 'Year', 'Dividends', 'Close']]
    div_df = div_df.groupby(['Ticker', 'Year']).agg({'Dividends' : 'sum',
                                        'Close' : 'mean'}).reset_index()

    div_df['Div_Yield'] = div_df['Dividends'] / div_df['Close']

    div_df = div_df[div_df['Year'] > 2006]
    div_df = div_df.sort_values(['Ticker', 'Year'], ascending=[True, False])
    div_df = pd.pivot_table(div_df, values = ['Dividends', 'Div_Yield'], index=['Ticker'], columns=['Year'])
    div_df.columns = [' '.join(str(col)).strip() for col in div_df.columns.values]

    universe_df = pd.merge(universe_df, div_df, on='Ticker', how='left')

    blob = bucket.blob(folder_name + '/' + folder_name + '_Summary.csv')
    blob.upload_from_string(universe_df.to_csv(encoding="UTF-8", index=False))
    print ("Done with Summary")

    src_blob = bucket.blob(folder_name + '/Config/RV_Tracker.csv')
    if src_blob.exists():
        src_blob = bucket.get_blob(folder_name + '/Config/RV_Tracker.csv')
        str_data = str(src_blob.download_as_string(), 'utf-8')
        tables_df = pd.read_csv(StringIO(str_data))

        filter_col = [col for col in universe_df.columns if col.startswith('Ret ')]

        tables_df = pd.merge(tables_df, universe_df[['Ticker'] + filter_col], how='left', on='Ticker')
        tables_df = pd.merge(tables_df, universe_df[['Ticker'] + filter_col], how='left', right_on='Ticker', left_on='Benchmark',
                             suffixes=('_self', '_bnch'))


        for col in filter_col:
            new_col = col.replace('Ret ', 'RV Move ')
            tables_df[new_col] = tables_df[col + '_self'] - tables_df[col + '_bnch']

        tables_df = tables_df.loc[:,~tables_df.columns.str.endswith('_bnch')]
        blob = bucket.blob(folder_name + '/' + folder_name + '_RV_Table.csv')
        blob.upload_from_string(tables_df.to_csv(encoding="UTF-8", index=False))

    print("Done with RV Tables")

    dly_df = pd.merge(dly_df, universe_df[['Ticker', 'Type', 'Name']], on='Ticker', how='left')
    dly_df = dly_df[(dly_df['Type'] == 'CEF') | (dly_df['Type'] == 'NAV')]
    if len(dly_df.index) > 0:
        print('inside if statement')
        dly_df['NAV_Ticker'] = 'X' + dly_df['Ticker'] + 'X'

        dly_df = pd.merge(dly_df, dly_df[['Ticker', 'Date', 'Close']],
                          left_on=['NAV_Ticker', 'Date'], right_on=['Ticker', 'Date'],
                          how='inner', suffixes=('', '_nav'))

        dly_df['Discount_Premium'] = dly_df['Close'] / dly_df['Close_nav'] - 1
        dly_df = dly_df.groupby(['Ticker', 'Name']).agg({'Close' : 'last',
                                                         'Close_nav': 'last',
                                                         'Discount_Premium' : ['last', zs_6m, zs_1y, zs_3y, zs_all]})
        dly_df.columns = ['|'.join(col).strip() for col in dly_df.columns.values]
        dly_df = dly_df.reset_index()

        print ("About to write")

        blob = bucket.blob(folder_name + '/' + folder_name + '_CEF_Table.csv')
        blob.upload_from_string(dly_df.to_csv(encoding="UTF-8", index=False))


def create_master_summary():

    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    src_blob = bucket.get_blob('Universe/Universe_Summary.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    universe_df = pd.read_csv(StringIO(str_data))

    src_blob = bucket.get_blob('nonUSD_Universe/nonUSD_Universe_Summary.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    non_usd_universe_df = pd.read_csv(StringIO(str_data))

    src_blob = bucket.get_blob('Macro/Macro_Summary.csv')
    str_data = str(src_blob.download_as_string(), 'utf-8')
    macro_df = pd.read_csv(StringIO(str_data))

    universe_df = pd.concat([universe_df, non_usd_universe_df, macro_df], ignore_index=True)

    blob = bucket.blob('Universe/Summary.csv')
    blob.upload_from_string(universe_df.to_csv(encoding="UTF-8", index=False))
    print ("Done with Summary")
