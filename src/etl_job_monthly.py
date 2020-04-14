from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:postgresdev@10.0.75.1:5432/')

def etl_monthly():
    
    '''
    ETL Job for monthly frequency data

    '''
    
    print('ETL Monthly Data from Raw -> Postgres: ')


    df_monthly = pd.read_csv('/apps/src/dataset/Assignment-monthlyexcerpt.csv')
    df_monthly.drop(['Unnamed: 2','Unnamed: 5','Unnamed: 8','Unnamed: 11',
                    'Unnamed: 14','Unnamed: 17','Unnamed: 20'],
                    axis=1,inplace=True)

    for i in range(0,df_monthly.columns.shape[0],2):
        
        df_ = df_monthly.iloc[:,i:i+2].dropna()
        df_ = df_.drop(0)


        df_.columns = ['date',df_.columns[1]]
        df_.date = pd.to_datetime(df_.date).map(lambda x:x.date())
        df_ = df_.set_index('date')
        df_.iloc[:,0] = pd.to_numeric(df_.iloc[:,0])
        
        df_ = df_.rename(columns={df_.columns[0]:df_.columns[0].lower().\
                        replace(' ','_').replace('/','_')})

        df_.to_sql('{}_monthly'.format(df_.columns[0]),con=engine,
                    if_exists='append')

    print('Done')



if __name__ == '__main__':

    etl_monthly()

    