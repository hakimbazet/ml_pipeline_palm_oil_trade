from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:postgresdev@10.0.75.1:5432/')

def etl_daily():

    '''
    ETL Job for daily frequency data

    '''

    print('ETL Daily Data from Raw -> Postgres: ')


    df_daily = pd.read_csv('/apps/src/dataset/Assignment-dailyexcerpt.csv')
    df_daily.drop(['Unnamed: 2','Unnamed: 5','Unnamed: 8','Unnamed: 11',
                'Unnamed: 14','Unnamed: 17','Unnamed: 20'],axis=1,inplace=True)

    for i in range(0,df_daily.columns.shape[0],2):
        
        df_ = df_daily.iloc[:,i:i+2].dropna()
        df_ = df_.drop(0)
        
        df_.columns = ['date',df_.columns[0]]
        df_.date = pd.to_datetime(df_.date,format='%d-%m-%y').\
                    map(lambda x:x.date())
        df_ = df_.set_index('date')
        df_.iloc[:,0] = pd.to_numeric(df_.iloc[:,0])
        
        # Remove unnecssary character
        df_ = df_.rename(columns={df_.columns[0]:df_.columns[0].lower().\
                replace(' ','_').replace('/','_')})
        
        # Reset Frequency as daily
        df_ = df_.asfreq('D')

        df_.to_sql('{}_daily'.format(df_.columns[0]),con=engine,
                if_exists='append')

    print('Done')



if __name__ == '__main__':

    etl_daily()
    


