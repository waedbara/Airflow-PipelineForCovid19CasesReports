import csv
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
import json
import subprocess
import time

try:
    from faker import Faker
except:
    subprocess.check_call(['pip3', 'install', 'faker'])
try:
    from sqlalchemy import create_engine
except:
    subprocess.check_call(['pip3', 'install', 'sqlalchemy'])
try:
    import psycopg2
except:
    subprocess.check_call(['pip3', 'install', 'psycopg2-binary'])
try:
    import matplotlib
except:
    subprocess.check_call(['pip3', 'install', 'matplotlib'])
try:
    import sklearn
except:
    subprocess.check_call(['pip3', 'install', 'sklearn'])


# config varabiles
host = Variable.set("host", "postgres")
user = Variable.set("user", "airflow")
password = Variable.set("password", "airflow")
port = Variable.set("port", '5432')
database = Variable.set("database", 'reports')
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def Get_Data_Rang():
    List_of_days = []
    for year in range(2020, 2022):
        for month in range(1, 13):
            for day in range(1, 32):
                month = int(month)
                if day <= 9:
                    day = f'0{day}'

                if month <= 9:
                    month = f'0{month}'
                List_of_days.append(f'{month}-{day}-{year}')
    return List_of_days


def Get_DF_i(Day):
    DF_i = None
    try:
        URL_Day = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day = pd.read_csv(URL_Day)
        DF_day['Day'] = Day
        cond = (DF_day.Country_Region == 'Jordan')
        Selec_columns = ['Day', 'Country_Region', 'Last_Update',
                         'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                         'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i = DF_day[cond][Selec_columns].reset_index(drop=True)
    except:
        pass
    return DF_i


def Get_All_Data():
    Start = time.time()
    DF_all = []
    for Day in Get_Data_Rang():
        DF_all.append(Get_DF_i(Day))
    End = time.time()
    Time_in_sec = round((End-Start)/60, 2)
    print(f'It took {Time_in_sec} minutes to get all data')
    DF_Jordan = pd.concat(DF_all).reset_index(drop=True)
    # Create DateTime for Last_Update
    DF_Jordan['Last_Update'] = pd.to_datetime(
        DF_Jordan.Last_Update, infer_datetime_format=True)
    DF_Jordan['Day'] = pd.to_datetime(
        DF_Jordan.Day, infer_datetime_format=True)

    DF_Jordan['Case_Fatality_Ratio'] = DF_Jordan['Case_Fatality_Ratio'].astype(
        float)
    DF_Jordan.to_csv(AIRFLOW_HOME + '/dags/alldata.csv', index=False)


def scoringReport():
    import matplotlib.pyplot as plt
    DF_Jordan = pd.read_csv(AIRFLOW_HOME + '/dags/alldata.csv')
    DF_Jordan.index = DF_Jordan.Day
    Selec_Columns = ['Confirmed', 'Deaths', 'Recovered',
                     'Active', 'Incident_Rate', 'Case_Fatality_Ratio']
    DF_Jordan_2 = DF_Jordan[Selec_Columns]

    DF_Jordan_2

    from sklearn.preprocessing import MinMaxScaler

    min_max_scaler = MinMaxScaler()

    DF_Jordan_3 = pd.DataFrame(min_max_scaler.fit_transform(
        DF_Jordan_2[Selec_Columns]), columns=Selec_Columns)
    DF_Jordan_3.index = DF_Jordan_2.index
    DF_Jordan_3['Day'] = DF_Jordan.Day
    DF_Jordan_3[Selec_Columns].plot(figsize=(20, 10))
    plt.savefig(AIRFLOW_HOME + '/dags/Jordan_scoring_report.png')
    DF_Jordan_3.to_csv(AIRFLOW_HOME + 'Jordan_scoring_report.csv')
    DF_Jordan_2.to_csv(AIRFLOW_HOME + 'Jordan_scoring_report_NotScaled.csv')


def SaveCsvToPostgres():
    host = Variable.get('host')
    user = Variable.get('user')
    password = Variable.get('password')
    port = Variable.get('port')
    database = Variable.get('database')
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}')
    print("Database Tables :- ", engine.table_names())
    DF = pd.read_csv(AIRFLOW_HOME + '/dags/Jordan_scoring_report.csv')
    DF_notscaled = pd.read_csv(
        AIRFLOW_HOME + '/dags/Jordan_scoring_report_NotScaled.csv')
    # push table
    DF.to_sql('Jordan_scoring_report', engine,
              if_exists='replace', index=False)
    DF_notscaled.to_sql('Jordan_scoring_report_NotScaled',
                        engine, if_exists='replace', index=False)


default_args = {
    'owner': 'waed',
    'start_date': dt.datetime(2020, 5, 15),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('covid19_cases_Jordan',
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False,
         ) as dag:
    Get_All_Data = PythonOperator(task_id='Get_COVID19_Cases_For_Jordan_2020ToNow',
                                  python_callable=Get_All_Data)

    ScoringReport = PythonOperator(task_id='Scoring_Report',
                                   python_callable=scoringReport)
    SaveCsvToPostgres = PythonOperator(task_id='Save_Report_InPostgres',
                                       python_callable=SaveCsvToPostgres)


Get_All_Data >> ScoringReport >> SaveCsvToPostgres
