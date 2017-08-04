from trading_ig import IGService
from trading_ig.config import config
from utils import flatten_df
import pandas as pd


class Timeseries:

    def __init__(self, epic, resolution, file_path=None, last_updates=None, path=None, start_date=None, end_date=None):
        '''

        :param epic: IG Markets Epic
        :param resolution:
        :param file_path:
        :param last_updates:
        :param path:
        :param start_date:
        :param end_date:
        '''
        self.epic = epic
        self.resolution = resolution
        self.file_path = file_path
        self.last_updates = last_updates
        self.path = path
        self.start_date = start_date
        self.end_date = end_date
        self.ig_reponse = None
        self.dataframe = None
        self.last_entry_date = None

    def getTimeseries(self):
        '''
        # Gets data from IG markets API and structures into flattend df
        :return: flat df
        '''
        ig_service = IGService(config.username, config.password, config.api_key, config.acc_type)
        ig_service.create_session()
        self.ig_reponse = ig_service.fetch_historical_prices_by_epic_and_date_range(epic=self.epic,
                                                                                    resolution=self.resolution,
                                                                                    start_date=self.start_date,
                                                                                    end_date=self.end_date)
        self.dataframe = flatten_df(self.ig_reponse['prices'])

        return self.dataframe

    def store_as_csv(self, path):
        """
        Stores self.dataframe as csv file at supplied path
        :param path:
        :return:
        """

        self.path = path
        if not isinstance(self.dataframe, pd.DataFrame):
            raise ValueError('dataframe argument is not a Pandas.DataFrame')
        self.dataframe.to_csv(path_or_buf=path)
        return self.path

    def store_as_pickle(self, path):
        """
            Stores self.dataframe as pickle file at supplied path
            :param path:
            :return:
        """
        self.path = path
        if not isinstance(self.dataframe, pd.DataFrame):
            raise ValueError('dataframe argument is not a Pandas.DataFrame')
        self.dataframe.to_pickle(path=self.path)
        return self.path

    def get_last_update_from_file(self, path, pickle=False, csv=False):
        """
        Opens file at path and gets datetime for last entry
        :param path:
        :param pickle:
        :param csv:
        :return:
        """
        self.path = path
        if not pickle and not csv:
            raise ValueError('Either pickle or csv must be true.')
        if pickle:
            self.last_entry_date = pd.read_pickle(self.path).index.values[-1]
            return self.last_entry_date
        if csv:
            self.last_entry_date = pd.read_csv(self.path, index_col=0).index.values[-1]
            return self.last_entry_date

    def append_data_to_file(self, path, pickle=False, csv=False):
        """
        Appends new data from dataframe to end of existing file
        :param path:
        :param pickle:
        :param csv:
        :return:
        """
        self.path = path

        if not pickle and not csv:
            raise ValueError('Either pickle or csv must be true.')

        if pickle:
            base = pd.read_pickle(self.path)
            cols = base.columns.values
        else:
            base = pd.read_csv(self.path, index_col=0)
            cols = base.columns.values
        if not (set(cols) == set(self.dataframe.columns.values) and len(cols) == len(self.dataframe.columns.values)):
            raise IOError('Columns in dataframe do not match file structure.')

        self.dataframe = base.append(self.dataframe)

        if pickle:
            self.store_as_pickle(self.path)
        else:
            self.store_as_csv(self.path)

        return self.dataframe

    def pyalgoformatdate(self, price='ask'):
        '''
        Formats self.dataframe for a BarFeed in pyalgotrade library
        :param price: either ask or bid
        :return:
        '''
        get_cols = [price+'_Open', price+'_High',
                    price+'_Low', price+'_Close',
                    'last_Volume', price+'_Close']

        new_df = self.dataframe[get_cols]
        new_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        new_df.index.name = 'DateTime'
        self.dataframe = new_df
        return new_df




