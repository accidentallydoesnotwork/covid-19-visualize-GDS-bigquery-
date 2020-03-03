import pandas as pd 
import datetime
from google.cloud import bigquery
from datetime import timedelta
import os


client = bigquery.Client()
#master_table
master_table_path = './master_table.csv'
dataset_id = 'covid_19'
table_id_master = 'master_table'

#today_table
today_table_path = './today_table.csv'
table_id_today= 'today_table'

yesterday = (datetime.date.today() - timedelta(days=1)).strftime("%-m/%-d/%y")
today = datetime.date.today().strftime("%-m/%-d/%y")

def get_data():
    try: 
        death_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"
        confirm_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
        recover_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"

        death = pd.read_csv(death_url, error_bad_lines=False)
        confirm = pd.read_csv(confirm_url, error_bad_lines=False)
        recover = pd.read_csv(recover_url, error_bad_lines=False)
        return death, confirm, recover
    
    except:
        print('Cannot get the data. Please check the url')
        return -1

def get_today_data(master): 
    global yesterday, today
    # master = pd.read_csv('master_table.csv')
    if len(master['date'] == yesterday)>0:
        master[master['date'] == yesterday].to_csv('today_table.csv')
    else:
        master[master['date'] == today].to_csv('today_table.csv')

def melted_data(df, date_range):
    data = df.copy()
    data = data.reset_index()
    data.drop(columns=['Province/State', 'Country/Region', 'Lat', 'Long'], inplace=True)
    df_melted = pd.melt(data, id_vars=["index"], value_vars=date_range)
    df_melted = df_melted.rename(columns={'variable':'date', 'value':'people'})
    return df_melted


def load_to_bigquery(filename, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))



def create_final_table():
    global master_table_path, today_table_path

    death, confirm, recover = get_data()
    date_range = confirm.columns[4:]
    
    df_loc = confirm[['Province/State', 'Country/Region', 'Lat', 'Long',]].reset_index().rename(columns={'index':'loc_index'})
    df_confirm = melted_data(confirm, date_range)
    df_death = melted_data(death, date_range)
    df_recover = melted_data(recover, date_range)
    df_death['state']= 'D'
    df_confirm['state']= 'C'
    df_recover['state']= 'R'
    
    master_table = pd.concat([df_death, df_confirm, df_recover], join="inner")
    master_table.rename(columns={'index':'loc_index'}, inplace=True)
    ultimate_table = df_loc.merge(master_table, left_on='loc_index', right_on='loc_index')
    ultimate_table.to_csv('master_table.csv', index=False)
    print('Finish Create master_table')
 
    get_today_data(ultimate_table)
    print('Finish Create today table')

    load_to_bigquery(master_table_path, dataset_id, table_id_master)
    load_to_bigquery(today_table_path, dataset_id, table_id_today)




if __name__ == "__main__":
    create_final_table()
