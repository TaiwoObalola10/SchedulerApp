import os
import pandas as pd
import pymysql
import requests
from sqlalchemy import create_engine

EXT_URL = os.environ.get('EXT_URL')
HOST = os.environ.get('HOST')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')
DATABASE = os.environ.get('DATABASE')
PORT = os.environ.get('PORT')



def update_my_db():
    #make a request for the data
    response = requests.get(EXT_URL)
    data = response.json()

    print("gotten response")
    # expand columns and rename some columns
    covid_df = pd.DataFrame(data)
    covid_df = pd.concat([covid_df.drop(['countryInfo'], axis=1), covid_df['countryInfo'].apply(pd.Series)], axis=1)
    covid_df.rename(columns={"iso3": "countryCode"}, inplace=True)

    print("fixing data")
    #connect to db
    db_data = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}"
    engine = create_engine(db_data)
    # send the data to db. 
    # NOTE: This db is a MySQL dtabase hosted on AWS
    covid_df.to_sql(con=engine, name='covid', if_exists='replace')

    print("done")

if __name__ == "__main__":
    update_my_db()