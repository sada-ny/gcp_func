import pandas as pd
from google.cloud import storage
from io import StringIO
import os
from sklearn.linear_model import Ridge
from sklearn.model_selection import GridSearchCV
from _cloud_utils import read_df

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
FILE_CHUNK_SIZE = os.environ['FILE_CHUNK_SIZE']

# cash : MINT ETF
# economic growth : EQ (IVV) + REIR(IYR) - Cash (MINT)
# real rates : TIP - MINT
# inflation " nominal rtes (IEF) - real rates (TIP)
# credit " LQD + HYG - MINt
# emerging market : em eq (EEM) + em cr (EMB) - dm eq (IVV) - dm cr(LQD + HYG)
# liquidity  small cap (IWM) - large cap (IVV)
# alt fixed income : MBB + MUB

def generate_LR_Coefficients():

    storage_client = storage.Client()
    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)

    folder_name = 'Universe'
    non_usd_folder_name = 'nonUSD_Universe'
    macro_folder_name = 'Macro'
    print ("started process")
    ######Only run regression for 2010 and later##############
    LR_start_year = 2018
    ##########################################################

    blob = bucket.get_blob(folder_name + '/Config/LR_Features.csv')
    str_data = str(blob.download_as_string(), 'utf-8')
    lr_df = pd.read_csv(StringIO(str_data))
    lr_df.set_index('Components', inplace=True)

    universe_df = read_df(folder_name + '/' + folder_name + '_Enriched.csv')
    non_usd_universe_df = read_df(non_usd_folder_name + '/' + non_usd_folder_name + '_Enriched.csv')
    macro_df = read_df(macro_folder_name + '/' + macro_folder_name + '_Enriched.csv')

    wkly_ret_df = read_df(folder_name + '/Historical-Returns/weekly_return.csv')
    wkly_ret_df = wkly_ret_df[wkly_ret_df['Year'] >= LR_start_year]

    dly_ret_df = read_df(folder_name + '/Historical-Returns/daily_return.csv')
    dly_ret_df['Date'] = pd.to_datetime(dly_ret_df['Date'])
    dly_ret_df['Year'] = dly_ret_df['Date'].dt.year
    dly_ret_df['Quarter'] = dly_ret_df['Date'].dt.quarter
    dly_ret_df = dly_ret_df[dly_ret_df['Year'] >= LR_start_year]

    non_usd_dly_ret_df = read_df(non_usd_folder_name + '/Historical-Returns/daily_return.csv')
    non_usd_dly_ret_df['Date'] = pd.to_datetime(non_usd_dly_ret_df['Date'])
    non_usd_dly_ret_df['Year'] = non_usd_dly_ret_df['Date'].dt.year
    non_usd_dly_ret_df['Quarter'] = non_usd_dly_ret_df['Date'].dt.quarter
    non_usd_dly_ret_df = non_usd_dly_ret_df[non_usd_dly_ret_df['Year'] >= LR_start_year]

    macro_dly_ret_df = read_df(macro_folder_name + '/Historical-Returns/daily_return.csv')
    macro_dly_ret_df['Date'] = pd.to_datetime(macro_dly_ret_df['Date'])
    macro_dly_ret_df['Year'] = macro_dly_ret_df['Date'].dt.year
    macro_dly_ret_df['Quarter'] = macro_dly_ret_df['Date'].dt.quarter
    macro_dly_ret_df = macro_dly_ret_df[macro_dly_ret_df['Year'] >= LR_start_year]


    for feature_set in lr_df.columns:
        print ("Running : " + feature_set)

        feature_etf = lr_df[lr_df[feature_set] == 1].index.tolist()

        wkly_feature_df = wkly_ret_df[wkly_ret_df['Ticker'].isin(feature_etf)]
        dly_feature_df = dly_ret_df[dly_ret_df['Ticker'].isin(feature_etf)]

        wkly_feature_df = wkly_feature_df[['Ticker', 'Year', 'Quarter', 'Week', 'Return']]
        dly_feature_df = dly_feature_df[['Ticker', 'Date', '1D_Return']]

        wkly_feature_df = pd.pivot_table(data=wkly_feature_df, index=['Year', 'Quarter', 'Week'], columns='Ticker')
        dly_feature_df = pd.pivot_table(data=dly_feature_df, index=['Date'], columns='Ticker')

        dly_feature_df.to_csv('/tmp/test.csv', index=True)
        blob = bucket.blob(folder_name + '/Historical-Returns/' + feature_set + '_daily_return.csv', chunk_size=int(FILE_CHUNK_SIZE))
        blob.upload_from_filename('/tmp/test.csv')

        wkly_feature_df.to_csv('/tmp/test.csv', index=True)
        blob = bucket.blob(folder_name + '/Historical-Returns/' + feature_set + '_weekly_return.csv', chunk_size=int(FILE_CHUNK_SIZE))
        blob.upload_from_filename('/tmp/test.csv')

        print ("Done generating feature vectors")

        lr_results = calc_LR_metrics(universe_df, dly_ret_df, dly_feature_df)
        non_usd_lr_results = calc_LR_metrics(non_usd_universe_df, non_usd_dly_ret_df, dly_feature_df)
        macro_lr_results = calc_LR_metrics(macro_df, macro_dly_ret_df, dly_feature_df)

        print ("Received Results")

        lr_results.to_csv('/tmp/test.csv', index=False)
        blob = bucket.blob(folder_name + '/Regression-Results/' + feature_set + '_lr_results.csv', chunk_size=int(FILE_CHUNK_SIZE))
        blob.upload_from_filename('/tmp/test.csv')

        non_usd_lr_results.to_csv('/tmp/test.csv', index=False)
        blob = bucket.blob(non_usd_folder_name + '/Regression-Results/' + feature_set + '_lr_results.csv', chunk_size=int(FILE_CHUNK_SIZE))
        blob.upload_from_filename('/tmp/test.csv')

        macro_lr_results.to_csv('/tmp/test.csv', index=False)
        blob = bucket.blob(macro_folder_name + '/Regression-Results/' + feature_set + '_lr_results.csv', chunk_size=int(FILE_CHUNK_SIZE))
        blob.upload_from_filename('/tmp/test.csv')

        print ("Done calculating LR Coeff")


def calc_LR_metrics(universe_df, ret_df, features_df_orig):

    features_df = features_df_orig.copy()
    features_df.columns = features_df.columns.map('|'.join).str.strip('|')
    features_df.index = pd.to_datetime(features_df.index)
    features_df = features_df.dropna()
    feature_hdr = features_df.columns.values.tolist()
    feature_hdr = [w.replace('1D_Return', 'Coeff') for w in feature_hdr]

    lr_results = pd.DataFrame(columns=['Ticker', 'Year', 'Quarter', 'Count', 'R2','Intercept'] + feature_hdr)

    for index, row in universe_df.iterrows():
        Y_df = ret_df[ret_df['Ticker'] == row['Ticker']]
        Y_df = Y_df.dropna()

        df = pd.merge(Y_df[['Year', 'Quarter', 'Date', '1D_Return']], features_df, left_on=['Date'], right_index=True, how='inner')
        df['KEY'] = df['Year'].astype(str) + ' ' + df['Quarter'].astype(str)
        for key in df['KEY'].unique():
            year = int(key.split(' ')[0])
            quarter = int(key.split(' ')[1])

            lr_df = pd.DataFrame()
            if quarter == 1:
                lr_df = df[((df['Year'] == year) & (df['Quarter'] == 1)) |
                           ((df['Year'] == year - 1) & (df['Quarter'] == 4))]
            else:
                lr_df = df[((df['Year'] == year) & (df['Quarter'] == quarter-1)) |
                           ((df['Year'] == year) & (df['Quarter'] == quarter))]

            if len(lr_df.index) > 100 :
                X = lr_df[features_df.columns.values.tolist()].to_numpy()
                Y = lr_df['1D_Return'].to_numpy()

                parameters = {'alpha' : [1e-15, 1e-10, 1e-8, 1e-4, 1e-3, 1e-2, 1, 5, 10, 20]}
                ridge = Ridge(max_iter=1e6)
                ridge_regressor = GridSearchCV(ridge, parameters, scoring='neg_mean_squared_error', cv=5)
                ridge_regressor.fit(X, Y)

                ridge_run = Ridge(ridge_regressor.best_params_['alpha'], max_iter=10e6)
                ridge_run.fit(X, Y)
                Y_pred = ridge_run.predict(X)

                result_dict = {'Ticker' : row['Ticker'],
                                                'Year' : str(year),
                                                'Quarter' : str(quarter),
                                                'Count' : str(len(X)),
                                                'R2' : ridge_run.score(X, Y),
                                                'Intercept' : ridge_run.intercept_}

                coeff_dict = dict(zip(feature_hdr,ridge_run.coef_))
                result_dict.update(coeff_dict)
                lr_results = lr_results.append(result_dict, ignore_index=True)
    return lr_results
