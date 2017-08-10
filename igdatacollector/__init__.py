from trading_ig import IGService
from trading_ig.config import config
from utils import flatten_df, cleanDates, return_datetime
import pandas as pd


class Timeseries:

    def __init__(self, epic, resolution, file_path=None, last_updates=None, path=None, table=None, start_date=None, end_date=None):
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
        self.table = table
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

    def getTimeseries_from_file(self, path, file_type):
        """
        Gets data from file and returns Dataframe
        :param path:
        :param file_type:
        :return:
        """
        self.path = path
        if not file_type or file_type.lower() not in ['csv', 'pickle']:
            raise ValueError('Either pickle or csv must be true.')
        elif file_type=='pickle':
            temp_dataframe = pd.read_pickle(self.path)
        else:
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

    def store_to_file(self, file_type, path):
        """
        Stores Dataframe to file at given path
        :param file_type:
        :param path:
        :return:
        """
        if not isinstance(self.dataframe, pd.DataFrame):
            raise ValueError('dataframe argument is not a Pandas.DataFrame')
        elif file_type.lower() not in ['csv', 'pickle']:
            raise ValueError('File_type arg must be either pickle or csv.')
        elif file_type.lower() == 'pickle':
            self.sort_df()
            self.dataframe.to_pickle(path=path)
        else:
            self.sort_df()
            self.dataframe.to_csv(path_or_buf=path)
        self.path = path
        return self.path

    def store_to_psql(self, engine, name=None, if_exists='fail'):
        """
        Stores Dataframe to sql and returns table name
        :param engine:
        :param name: table name, optional, default epic + resolution
        :param if_exists:
        :return:
        """
        self.sort_df()
        self.table_name(name=name)
        self.dataframe.to_sql(name=self.table, con=engine, if_exists=if_exists)
        return self.table

    def get_last_update(self, engine=None, name=None, file_type=None, path=None):
        """
        Gets last update for given epic from sql, pickle or csv
        :param engine:
        :param name:
        :param file_type:
        :param path:
        :return:
        """
        if engine==None and file_type == None:
            return self.last_entry_date
        elif engine:
            self.table_name(name=name)
            df = pd.read_sql_table(table_name=self.table, con=engine,
                                   parse_dates='DateTime', index_col='DateTime')
        else:
            if file_type.lower() not in ['pickle', 'csv']:
                raise ValueError('Incorrect file_type input, must be pickle or csv.')
            elif file_type.lower()=='pickle':
                df = pd.read_pickle(path=path)
            else:
                df = pd.read_csv(path, index_col=0)

        self.last_entry_date = return_datetime(df.sort_index().index.values[-1])
        return self.last_entry_date

    def append_data(self, engine=None, name=None, file_type=None, path=None):
        """
        Appends data to end of sql table, pickled DF or csv
        :param engine:
        :param name:
        :param file_type:
        :param path:
        :return:
        """
        if engine == None and file_type == None:
            raise ValueError('Neither file_type nor engine given.')
        elif engine:
            if not name:
                name = self.epic
            self.dataframe.to_sql(name=name, con=engine, if_exists='append')
            self.getTimeseries_from_sql(engine=engine, name=name)
        else:
            if file_type.lower() == 'pickle':
                base = pd.read_pickle(path)
                cols = base.columns.values
            elif file_type.lower() == 'csv':
                base = pd.read_csv(path, index_col=0)
                cols = base.columns.values
            else:
                raise ValueError('Incorrect file_type input, must be pickle or csv.')

            if not (set(cols) == set(self.dataframe.columns.values) and len(cols) == len(self.dataframe.columns.values)):
                raise IOError('Columns in dataframe do not match file structure.')
            else:
                self.dataframe = base.append(self.dataframe)

                self.store_to_file(file_type=file_type, path=path)
                self.getTimeseries_from_file(file_type=file_type, path=path)

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

    def table_name(self, name=None):
        if name:
            self.table = name
        elif not self.table:
            self.table = self.epic+'_'+self.resolution

