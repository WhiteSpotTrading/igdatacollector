from igdatacollector import Timeseries, utils
from trading_ig_config import *
import sqlalchemy as sql
import unittest
import os


class TestTimeSeriesSetup(unittest.TestCase):
    def setUp(self):
        self.instance_a = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        self.instance_a.getTimeseries(start_date=TEST_START_DATE, end_date=TEST_END_DATE)
        self.e_cols = ['bid_Open', 'bid_High', 'bid_Low', 'bid_Close',
                  'ask_Open', 'ask_High', 'ask_Low', 'ask_Close',
                  'spread_Open', 'spread_High', 'spread_Low', 'spread_Close',
                  'last_Open', 'last_High', 'last_Low', 'last_Close', 'last_Volume']


class TestTimeSeriesGet(TestTimeSeriesSetup):

    def test_init(self):
        self.assertEquals(self.instance_a.epic, TEST_EPIC)
        self.assertEquals(self.instance_a.resolution, TEST_RESOLUTION)

    def test_getTimeseries(self):
        self.assertEquals(len(self.instance_a.dataframe), utils.num_trading_days(TEST_START_DATE, TEST_END_DATE))
        self.assertEquals(list(self.instance_a.dataframe.columns.values), self.e_cols)
        self.assertEquals(self.instance_a.dataframe.index.name, 'DateTime')


class TestTimeSeriesStoreRead(TestTimeSeriesSetup):

    def test_StoreReadPickle(self):
        path = self.instance_a.store_to_file(file_type='pickle', path=TEST_DATA_STORE_PICKLE+'/'+self.instance_a.epic)
        instance_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_b.getTimeseries_from_file(file_type='pickle', path=path)
        self.assertEquals(len(instance_b.dataframe), utils.num_trading_days(TEST_START_DATE, TEST_END_DATE))
        self.assertEquals(list(instance_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_b.dataframe.index.name, 'DateTime')
        os.remove(path)

    def test_StoreReadCSV(self):
        path = self.instance_a.store_to_file(file_type='csv',
                                             path=TEST_DATA_STORE_CSV + '/' + self.instance_a.epic+'.csv')
        instance_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_b.getTimeseries_from_file(file_type='csv', path=path)
        self.assertEquals(len(instance_b.dataframe), utils.num_trading_days(TEST_START_DATE, TEST_END_DATE))
        self.assertEquals(list(instance_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_b.dataframe.index.name, 'DateTime')
        os.remove(path)

    def test_StoreReadsql(self):
        engine = sql.create_engine('sqlite:///'+TEST_SQL_PATH)
        self.instance_a.store_to_psql(engine)
        instance_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_b.getTimeseries_from_sql(engine=engine)
        self.assertEquals(len(instance_b.dataframe), utils.num_trading_days(TEST_START_DATE, TEST_END_DATE))
        self.assertEquals(list(instance_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_b.dataframe.index.name, 'DateTime')
        os.remove(TEST_SQL_PATH)

    def test_Append_CSV(self):
        # Create ins. get data, store as CSV
        instance_csv = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_csv.getTimeseries(start_date=TEST_START_DATE, end_date=TEST_END_DATE)
        instance_csv.store_to_file(file_type='csv', path=TEST_DATA_STORE_CSV + '/' + instance_csv.epic + '.csv')
        orig_len_csv = len(instance_csv.dataframe)
        # New instance,  Get last update from CSV file
        instance_csv_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        last_update = instance_csv_b.get_last_update(file_type='csv', path=instance_csv.path)
        next_date = last_update + timedelta(days=1)
        # Get Data from last_update + 1 day

        instance_csv_b.getTimeseries(start_date=next_date, end_date=TEST_APPEND_DATE)
        new_len_csv = orig_len_csv + len(instance_csv_b.dataframe)
        instance_csv_b.append_data(file_type='csv', path=instance_csv.path)

        self.assertEquals(len(instance_csv_b.dataframe), new_len_csv)
        self.assertEquals(list(instance_csv_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_csv_b.dataframe.index.name, 'DateTime')
        os.remove(instance_csv.path)

    def test_Append_Pickle(self):
        # Create ins. get data, store as Pickle
        instance_pickle = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_pickle.getTimeseries(start_date=TEST_START_DATE, end_date=TEST_END_DATE)
        instance_pickle.store_to_file(file_type='pickle', path=TEST_DATA_STORE_PICKLE + '/' + instance_pickle.epic)
        orig_len_pickle = len(instance_pickle.dataframe)

        # New instance,  Get last update from Pickle file
        instance_pickle_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        last_update = instance_pickle_b.get_last_update(file_type='pickle', path=instance_pickle.path)
        next_date = last_update + timedelta(days=1)
        # Get Data from last_update + 1 day

        instance_pickle_b.getTimeseries(start_date=next_date, end_date=TEST_APPEND_DATE)
        new_len_csv = orig_len_pickle + len(instance_pickle_b.dataframe)
        instance_pickle_b.append_data(file_type='pickle', path=instance_pickle.path)
        self.assertEquals(len(instance_pickle_b.dataframe), new_len_csv)
        self.assertEquals(list(instance_pickle_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_pickle_b.dataframe.index.name, 'DateTime')
        os.remove(instance_pickle.path)

    def test_Append_Sql(self):
        # Create ins. get data, store in SQL
        instance_sql = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        instance_sql.getTimeseries(start_date=TEST_START_DATE, end_date=TEST_END_DATE)
        engine = sql.create_engine('sqlite:///' + TEST_SQL_PATH)
        instance_sql.store_to_psql(engine=engine, name='table2')
        orig_len_sql = len(instance_sql.dataframe)

        # New instance,  Get last update from sql
        instance_sql_b = Timeseries(epic=TEST_EPIC, resolution=TEST_RESOLUTION)
        last_update = instance_sql_b.get_last_update(engine=engine)
        next_date = last_update + timedelta(days=1)
        # Get Data from last_update + 1 day

        instance_sql_b.getTimeseries(start_date=next_date, end_date=TEST_APPEND_DATE)
        new_len_sql = orig_len_sql + len(instance_sql_b.dataframe)
        instance_sql_b.append_data(engine=engine, name='table2')
        self.assertEquals(len(instance_sql_b.dataframe), new_len_sql)
        self.assertEquals(list(instance_sql_b.dataframe.columns.values), self.e_cols)
        self.assertEquals(instance_sql_b.dataframe.index.name, 'DateTime')
        os.remove(TEST_SQL_PATH)


if __name__ == '__main__':
    unittest.main()
