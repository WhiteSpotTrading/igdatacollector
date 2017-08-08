import os

ROOTDIR = os.path.dirname(__file__)
TEST_DATA_STORE_ROOT = '/Users/Carlwestman/PycharmProjects/WhiteSpotTrading/test-data-store'
TEST_DATA_STORE_RAW = '/raw'
TEST_DATA_STORE_PICKLE = '/pickles'

class config(object):
    username = "USERNAME"
    password = "PASSWORD"
    api_key = "API-KEY"
    acc_type = "DEMO" # LIVE / DEMO
    acc_number = "ACC_NR"