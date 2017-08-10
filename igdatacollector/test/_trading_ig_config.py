import os
from datetime import datetime, timedelta

ROOTDIR = os.path.dirname(__file__)
TEST_DATA_STORE = ROOTDIR+'/test_data'
TEST_DATA_STORE_CSV = TEST_DATA_STORE+'/csv'
TEST_DATA_STORE_PICKLE = TEST_DATA_STORE+'/pickles'
TEST_SQL_PATH = TEST_DATA_STORE+'/sqlite/sql'

class config(object):
    username = "USERNAME"
    password = "PASSWORD"
    api_key = "API-KEY"
    acc_type = "DEMO" # LIVE / DEMO
    acc_number = "ACC_NR"

TEST_EPIC = 'IX.D.OMX.IFD.IP'
TEST_RESOLUTION = 'D'

TEST_START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=10)
TEST_END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  - timedelta(days=6)
TEST_APPEND_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  - timedelta(days=1)


