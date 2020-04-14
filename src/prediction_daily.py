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



def daily_forecast_data_cleanup():
    
    '''
    Data cleanup for daily % change forecast
    
    '''
    df_bo1_comb = pd.read_sql('bo1_comb_comdty_daily',con=engine).set_index('date')
    df_co1_comb = pd.read_sql('co1_comb_comdty_daily',con=engine).set_index('date')
    df_fbmklci = pd.read_sql('fbmklci_index_daily',con=engine).set_index('date')
    df_klpln = pd.read_sql('klpln_index_daily',con=engine).set_index('date')
    df_palm_oil = pd.read_sql('pal2maly_index_daily',con=engine).set_index('date')
    df_qs1 = pd.read_sql('qs1_comdty_daily',con=engine).set_index('date')
    df_usdmyr = pd.read_sql('usdmyr_curncy_daily',con=engine).set_index('date')

    df = df_bo1_comb.join(df_co1_comb).join(df_fbmklci).join(df_klpln).join(df_qs1).join(df_usdmyr).join(df_palm_oil)

    ## Convert data to weekday frequency (Mon - Friday)

    df = df.asfreq('D').asfreq('B') 

    for column in df.columns:

        df.loc[:,column] = df.loc[:,column].fillna(method='ffill')

    ## Skip first week due to null value at first date

    df = df.iloc[5:]

    df = df.pct_change().dropna()
    
    return df


def model_training_daily(df):
    '''
    df: pd.DataFrame training data
    
    '''

    X_train,X_test,y_train,y_test = train_test_split(df.drop('pal2maly_index'
                ,axis=1),df.loc[:,'pal2maly_index'],test_size=0.20,random_state=0)


    pipe = Pipeline([("Regressor", RandomForestRegressor())])
    # Hyperparameter grid search 
    grid_param = [
                    {"Regressor": [RandomForestRegressor(random_state=0,
                                    n_jobs=-1)],
                     "Regressor__n_estimators": [10,50, 100,200,500,1000],
                     "Regressor__max_depth":[2,4,8,None]
                    },
                    {"Regressor": [xgb.XGBRegressor(objective ='reg:squarederror')],
                     "Regressor__n_estimators": [10,50, 100,200,500,1000],
                     "Regressor__learning_rate":[0.01,0.1],
                     "Regressor__max_depth":[2,4,8,None]
                    }]
    # Fit grid search
    gridsearch = GridSearchCV(pipe, grid_param, cv=10, verbose=0,n_jobs=-1) 
    best_model = gridsearch.fit(X_train,y_train)

    joblib.dump(best_model.best_estimator_,'daily_model.sav')

def load_into_pg_db_daily(df):
    '''
    df: pd.DataFrame load data frame into db for daily forecast
    '''


    loaded_model = joblib.load('daily_model.sav')
    ## Prediction

    tomorrow_value = loaded_model.predict(df.iloc[-1].to_frame().T\
                    .drop('pal2maly_index',axis=1))


    pd.Series(tomorrow_value,index=[df.index[-1].date()+timedelta(days=1)]).\
                to_sql('palm_oil_daily_percentage_forecast',
                con=engine,if_exists='replace') 

def predict_daily():

    df = daily_forecast_data_cleanup()
    print('Training on new dataset:')
    model_training_daily(df)
    print('Done')
    print('Load predicition into Postgres DB table name \
        palm_oil_daily_percentage_forecast')
    load_into_pg_db_daily(df)
    print('Done')


if __name__ == '__main__':

    predict_daily()