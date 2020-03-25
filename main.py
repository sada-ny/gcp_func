# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_flex_storage_app]
import logging
import os

from flask import Flask, request
from google.cloud import storage
from _get_historical_prices import load_historical_data
from _get_stock_info import get_stock_info
from _generate_history import create_history, create_non_usd_history
from _generate_features import generate_LR_Coefficients
from _generate_summary import create_summary, create_master_summary
from _generate_PL import create_holdings_pl
from _generate_indicators import load_FRED_indicators, load_SP_Indices

app = Flask(__name__)

# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']


@app.route('/')
def root():
    """serves index.html"""
    return "Batch Server Up and Running"


@app.route('/runbatch', methods=['GET', 'POST'])
def runbatch():
    load_historical_data('Universe')
    load_historical_data('Macro')
    load_historical_data('SP500', period_length='1y')
    load_historical_data(('nonUSD_Universe'), period_length='9mo')
    return 'Data Loaded'


@app.route('/stockinfo', methods=['GET', 'POST'])
def stockinfo():
    get_stock_info()
    return 'Stock Info Generated'


@app.route('/createhistory', methods=['GET', 'POST'])
def createhistory():
    create_history('Universe')
    create_non_usd_history()
    create_history('Macro')
    create_history('SP500')
    return 'History Created'


@app.route('/runregression', methods=['GET', 'POST'])
def runregression():
    generate_LR_Coefficients()
    return 'Regression Finished'


@app.route('/runsummary', methods=['GET', 'POST'])
def runsummary():
    print("in summary")
    create_summary('Universe')
    create_summary('nonUSD_Universe')
    create_summary('Macro')
    create_master_summary()
    return 'Summary Finished'


@app.route('/runPL', methods=['GET', 'POST'])
def runPL():
    print ("in PL")
    create_holdings_pl()
    return 'PL Finished'

@app.route('/getIndic', methods=['GET', 'POST'])
def getIndic():
    print ("Getting Indicators")
    load_FRED_indicators()
    load_SP_Indices()
    return 'Indicators loaded'



@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8081, debug=True)
# [END gae_flex_storage_app]
