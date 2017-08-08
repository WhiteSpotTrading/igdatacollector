from trading_ig import IGService
from trading_ig.config import config
from utils import flatten_df, cleanDates
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

    def getTimeseries(self, start_date, end_date):
        '''
        # Gets data from IG markets API and structures into flattend df
        :return: flat df
        '''
        self.start_date = start_date
        self.end_date = end_date
        ig_service = IGService(config.username, config.password, config.api_key, config.acc_type)
        ig_service.create_session()
        self.ig_reponse = ig_service.fetch_historical_prices_by_epic_and_date_range(epic=self.epic,
                                                                                    resolution=self.resolution,
                                                                                    start_date=self.start_date,
                                                                                    end_date=self.end_date)
        self.dataframe = flatten_df(self.ig_reponse['prices'])
        self.sort_df()
        return self.dataframe

    def getTimesseries_from_file(self, path, pickle=False, csv=False):
        """
        Gets data from file and puts in dataframe
        :param path:
        :param pickle:
        :param csv:
        :return:
        """
        self.path = path
        if not pickle and not csv:
            raise ValueError('Either pickle or csv must be true.')
        if pickle:
            temp_dataframe = pd.read_pickle(self.path)

        if csv:
            temp_dataframe = pd.read_csv(self.path, index_col=0)
        self.dataframe = cleanDates(temp_dataframe.reset_index())
        self.dataframe.set_index(['DateTime'], inplace=True)
        self.sort_df()
        self.last_entry_date = self.dataframe.index.values[-1]
        return self.dataframe

    def getTimeseries_from_sql(self, engine, name=None, maxLength=None):
        """

        :param engine:
        :param name:
        :param maxLength:
        :return:
        """
        if not name:
            name = self.epic+'_'+self.resolution
        if not maxLength:
            self.dataframe = pd.read_sql_table(table_name=name, con=engine, parse_dates='DateTime', index_col='DateTime')
            self.last_entry_date = self.dataframe.index.values[-1]
        else:
            params = {'limit':maxLength}
            self.dataframe = pd.read_sql(sql=name, con=engine, parse_dates='DateTime',
                                               index_col='DateTime', params=params)
            self.last_entry_date = self.dataframe.index.values[-1]
        self.sort_df()
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
        self.sort_df()
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
        self.sort_df()
        self.dataframe.to_pickle(path=self.path)
        return self.path

    def store_to_psql(self, engine, name=None, if_exists='fail'):
        self.sort_df()
        if not name:
            name = self.epic+'_'+self.resolution
        self.dataframe.to_sql(name=name, con=engine, if_exists=if_exists)


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
            df = pd.read_pickle(self.path)
        else:
            df = pd.read_csv(self.path, index_col=0)
        self.last_entry_date = df.sort_index().index.values[-1]
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

    def sort_df(self, ascending=True):
        self.dataframe.sort_index(ascending=ascending, inplace=True)



