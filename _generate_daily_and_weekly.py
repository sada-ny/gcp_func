import pandas as pd
import numpy as np

def convert_history_to_return(inp_df, ticker):

    inp_df['Week'] = inp_df['Date'].dt.week
    inp_df['Quarter'] = inp_df['Date'].dt.quarter
    inp_df['Year'] = inp_df['Date'].dt.year
    inp_df['Year'] = np.where((inp_df['Week'] == 1) & (inp_df['Date'].dt.month == 12),
                              inp_df['Year'] + 1, inp_df['Year'])
    inp_df['Ticker'] = ticker
    inp_df['1D_Prc_Return'] = inp_df['Close']/inp_df['Close'].shift(1) - 1
    inp_df['1D_Prc_Return'].fillna(0, inplace=True)
    inp_df['1D_Div_Return'] = inp_df['Dividends']/inp_df['Close'].shift(1)
    inp_df['1D_Div_Return'].fillna(0, inplace=True)
    inp_df['1D_Return'] = inp_df['1D_Div_Return'] + inp_df['1D_Prc_Return']
    inp_df['Index'] = (1+ inp_df['1D_Return']).cumprod()

    out_df = inp_df.groupby(['Ticker', 'Year', 'Quarter', 'Week']).agg({'1D_Prc_Return' : ['sum', 'count'],
                                                                        '1D_Div_Return' : ['sum', 'count'],
                                                                        '1D_Return' : ['sum', 'count']})
    out_df.columns = out_df.columns.map('_'.join)
    out_df = out_df.reset_index()

    out_df = out_df[out_df['1D_Prc_Return_count'] > 1]
    out_df = out_df[['Ticker', 'Year', 'Quarter', 'Week', '1D_Prc_Return_sum', '1D_Div_Return_sum', '1D_Return_sum']]
    out_df.rename(columns={'1D_Prc_Return_sum': 'Prc_Return',
                           '1D_Div_Return_sum': 'Div_return',
                           '1D_Return_sum': 'Return'}, inplace=True)

    return inp_df[['Ticker', 'Date', 'Close', 'Dividends', '1D_Prc_Return', '1D_Div_Return', '1D_Return', 'Index']], out_df


def convert_history_to_cum_return(inp_df):

    inp_df['Date'] = pd.to_datetime(inp_df['Date'])
    inp_df.sort_values(by='Date', inplace=True)

    out_df = inp_df.groupby(['Ticker']).agg({'1D_Prc_Return' : ['sum', 'count'],
                                             '1D_Div_Return' : ['sum', 'count'],
                                             '1D_Return' : ['sum', 'count']})
    out_df.columns = out_df.columns.map('_'.join)
    out_df = out_df.reset_index()

    out_df = out_df[['Ticker', '1D_Prc_Return_sum', '1D_Div_Return_sum', '1D_Return_sum']]
    out_df.rename(columns={'1D_Prc_Return_sum': 'Prc_Return',
                           '1D_Div_Return_sum': 'Div_return',
                           '1D_Return_sum': 'Return'}, inplace=True)

    return out_df


