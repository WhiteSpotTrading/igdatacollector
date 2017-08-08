from igdatacollector import Timeseries
import sqlalchemy


a = Timeseries(epic='IX.D.OMX.IFD.IP', resolution='D')
#a.getTimeseries(start_date='2017-08-01', end_date='2017-08-05')




a.getTimesseries_from_file(path='/Users/Carlwestman/PycharmProjects/WhiteSpotTrading/test-data-store/raw/CS.D.USDSEK.CFD.IP_hourly.csv',csv=True)
a.store_as_csv('/Users/Carlwestman/PycharmProjects/WhiteSpotTrading/test-data-store/pyalgotradeformatted/test.csv')
engine = sqlalchemy.create_engine('postgresql+psycopg2://data_manager:localdevpassword@localhost/postgres')

a.store_to_psql(engine, if_exists='replace')
#print a.dataframe.head()
#print a.last_updates
b = Timeseries(epic="IX.D.OMX.IFD.IP", resolution='D')
b.getTimeseries_from_sql(engine=engine, maxLength=10)

print b.dataframe
print type(a.last_entry_date)