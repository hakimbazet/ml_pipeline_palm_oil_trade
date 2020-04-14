import pandas as pd 
import xgboost as xgb
import joblib

from datetime import timedelta
from sklearn.model_selection import train_test_split,GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgresdev@10.0.75.1:5432/')


def monthly_forecast_data_cleanup():
    '''
    Data cleanup for monthly forecast
    
    '''
    
    # Load palm oil data
    df = pd.read_sql('crude_palm_oil_fob_spot_monthly',con=engine)

    ## Fixed the date
    df.date = df.date.map(lambda x:'{}-{}-01'.format(x.year,x.month))
    df.date = pd.to_datetime(df.date)
    df = df.set_index('date').asfreq('MS')
    df = df.iloc[:,0]
    
    return df

def SARIMAX_model(ts,order, forecast_period=3):
    '''
    SARIMAX model Params
    
    ts: Time series data
    order: seasonal order for SARIMAX 
    return: pd.Series of 3 months forecast
    
    '''
    sarimax = SARIMAX(ts, seasonal_order=order)
    sarimax_results = sarimax.fit()
    forecast = sarimax_results.forecast(forecast_period)
    
    
    return forecast


def load_into_pg_db_monthly(series_name,forecast_series):
    '''
    series_name: Name of our stored forecasted table
    forecast_series: Results of our forecasted series
    con: SQLAlchemy DB engine    
    '''
    
    forecast_series.to_sql(series_name,con=engine,if_exists='replace') 
    

def predict_monthly():
    
    print('Prediction on monthly basis:')

    table_name ='palm_oil_3_month'
    ts = monthly_forecast_data_cleanup()
    forecast_result = SARIMAX_model(ts,(1,1,1,3))
    load_into_pg_db_monthly(table_name,forecast_result)
    print('Monthly prediction updated in postgres table name {}'.\
            format(table_name))

if __name__=='__main__':

    predict_monthly()