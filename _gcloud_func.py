from _get_historical_prices import load_historical_data
from _get_stock_info import get_stock_info
from _generate_history import create_history, create_non_usd_history
from _generate_features import generate_LR_Coefficients
from _generate_summary import create_summary, create_master_summary
from _generate_PL import create_holdings_pl
from _generate_indicators import load_FRED_indicators, load_SP_Indices

def gcf_runbatch():
    load_historical_data('Universe')
    load_historical_data('Macro')
    load_historical_data('SP500', period_length='1y')
    load_historical_data(('nonUSD_Universe'), period_length='9mo')
    return 'Data Loaded'


def gcf_stockinfo():
    get_stock_info()
    return 'Stock Info Generated'


def gcf_createhistory():
    create_history('Universe')
    create_non_usd_history()
    create_history('Macro')
    create_history('SP500')
    return 'History Created'


def gcf_runregression():
    generate_LR_Coefficients()
    return 'Regression Finished'


def gcf_runsummary():
    print("in summary")
    create_summary('Universe')
    create_summary('nonUSD_Universe')
    create_summary('Macro')
    create_master_summary()
    return 'Summary Finished'


def gcf_runPL():
    print ("in PL")
    create_holdings_pl()
    return 'PL Finished'

def gcf_getIndic():
    print ("Getting Indicators")
    load_FRED_indicators()
    load_SP_Indices()
    return 'Indicators loaded'

