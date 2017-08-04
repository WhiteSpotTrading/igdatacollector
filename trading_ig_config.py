import os

ROOTDIR = os.path.dirname(__file__)
TEST_DATA_STORE_ROOT = '/Users/Carlwestman/PycharmProjects/WhiteSpotTrading/test-data-store'
TEST_DATA_STORE_RAW = '/raw'
TEST_DATA_STORE_PICKLE = '/pickles'

class config(object):
    username = "cwestman-test"
    password = "cwb-dLM88ar-test"
    api_key = "1426ebf7579c3e0b24d65de73d0112dd7759eedf"
    acc_type = "DEMO" # LIVE / DEMO
    acc_number = "X95UN"


EPICS = {
    "IX.D.OMX.IFD.IP":
        {
            "resolutions":{
                "daily":"D",
                "hourly":"1H"}
        },
    "IX.D.FTSE.CFD.IP":
        {
            "resolutions": {
                "daily": "D",
                "hourly": "1H"}
        },
    "IX.D.SPTRD.IFD.IP":
        {
            "resolutions": {
                "daily": "D",
                "hourly": "1H"}
        },
    "IX.D.DOW.IFD.IP":
        {
            "resolutions": {
                "daily": "D",
                "hourly": "1H"}
        },
    "IX.D.DAX.IFD.IP":
        {
            "resolutions": {
                "daily": "D",
                "hourly": "1H"}
        },
    "CS.D.USDSEK.CFD.IP":{
            "resolutions": {
                "daily": "D",
                "hourly": "1H"}
        }
    }

RESOLUTIONS = {"D":
                   {"file_append":""},
               "1H":
                   {"file_append":"_hourly"}
               }