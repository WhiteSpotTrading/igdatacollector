import pandas as pd
import numpy as np
from datetime import datetime
import numpy as np
import types

def cleanDates(df):
    """

    :param df: dataframe containing atleast col called DateTime
    :return: Same DF but with formated DateTime
    """
    df_date = pd.DataFrame()
    df_date['year'] = df['DateTime'].str.slice(0,4)
    df_date['month'] = df['DateTime'].str.slice(5,7)
    df_date['day'] = df['DateTime'].str.slice(8,10)
    df_date['hour'] = df['DateTime'].str.slice(11,13)
    df_date['min'] = df['DateTime'].str.slice(14,16)
    df_date['second'] = df['DateTime'].str.slice(17,109)
    df['DateTime'] = pd.to_datetime(df_date['year']+'-'+df_date['month']+'-'+df_date['day']
                                    +' '+df_date['hour']+':'+df_date['min']+':'+df_date['second'],
                                    yearfirst=True)
    return df

def cleanDate(ig_date_str):

    return str(ig_date_str[0:4]+'-'+\
               ig_date_str[5:7]+'-'+\
               ig_date_str[8:10]+' '+\
               ig_date_str[11:13]+':'+\
               ig_date_str[14:16]+':'+\
               ig_date_str[17:])

def get_df_from_raw(path):
    df = pd.read_csv(filepath_or_buffer=path, header=[0, 1, 2])

    for i, col in enumerate(df.columns.levels):
        columns = np.where(col.str.contains('Unnamed'), '', col)
        df.columns.set_levels(columns, level=i, inplace=True)

    vol = df['last']
    df = df[[0]].join(df['ask']).join(vol['Volume'])
    df.columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']

    return cleanDates(df)


def get_last_update(df):
    """

    :param df: df containing atleast a col called DateTime
    :return: Max DateTime
    """
    return max(df['DateTime'])


def flatten_df(df):
    cols = df.columns.values
    index = df.index.values
    rows = df.values

    columns = []
    for c in cols:
        columns.append(c[0] + '_' + c[1])

    index_list = []
    for i in index:
        index_list.append(cleanDate(i))
    df = pd.DataFrame(data=rows, index=index_list, columns=columns)
    df.index.name = 'DateTime'
    return df


def return_datetime(date_time):
    if np.issubdtype(type(date_time), np.datetime64):
        ts = (date_time - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
        return datetime.fromtimestamp(ts)
    if not isinstance(date_time, datetime) and isinstance(date_time, types.StringType):
        return datetime.strptime(date_time,'%Y-%m-%d %H:%M:%S')
    else:
        return date_time

def num_trading_days(start, end):
    return np.busday_count(start, end) + 1
