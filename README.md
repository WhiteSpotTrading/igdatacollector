# igdatacollector
Package that enables downloading, updating and structuring financial data from the IG markets REST API and stores it locally

# Features
* Simple extraction of financial timeseries data for assets traded on IG markets.
* Reformat IG response to a flat pandas.dataframe that is simple to wotk with.
* Reformat dataframe to integrate with pyalgotrade class barfeed
* Store Dataframe as CSV or pickle
* Easily read latest update date for a given instrument
* Update and append data to an existing CSV or Pickle.


# Requirements
## Technical requirements
* Python (2.7.13)
* Pandas (0.19.2)
* Numpy (1.11.3)
* trading_ig (0.0.6)

## Other requirements
* Account at IG Markets and a API-key

# Setup`
Install using pip in commandline

``` pip install git+git://github.com/WhiteSpotTrading/igdatacollector ```

# Use
In order to be able to use it you must have a file called "trading_ig_config.py" in your project root.
The file should contain a class declared as below:

~~~~~
class config(object):
    username = "IG_MARKETS_USERNAME"
    password = "IG_MARKETS_PASSWORD"
    api_key = "IG_MARKETS_API_KEY"
    acc_type = "DEMO" # LIVE / DEMO
    acc_number = "IG_MARKETS_ACC_NUM"
~~~~~

# Acknowledgement
All api calls performed by the ig_trading lib. Check it out here: https://github.com/ig-python/ig-markets-api-python-library
