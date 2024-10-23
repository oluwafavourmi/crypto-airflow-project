from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import requests
import bs4
import pandas as pd
import json
    

def load_web_page():
    page = requests.get("https://coincodex.com/crypto/bitcoin/historical-data/")
    

    
def ET_data():
    url = 'https://coincodex.com/crypto/bitcoin/historical-data/'
    req = requests.get(url)
    soup = bs4.BeautifulSoup(req.content, 'html.parser')
    table = soup.find('table')
    table_rows = soup.find_all('tr')
    rows = table.find_all('tr')

    table_data = []
    for row in table_rows:
        cells = row.find_all("td")  # Or "th" for header cells
        row_data = [cell.text.strip() for cell in cells]
        table_data.append(row_data)
    
    col = ['Date Start', 'Date End', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']
    df = pd.DataFrame(table_data, columns=col)

    df = df.iloc[1:]
    df2 = df.drop('Date End', axis='columns')
    df3 = df2.reindex(index=df2.index[::-1])
    df3['Date'] = pd.to_datetime(df['Date Start'], format='%b %d, %Y')
    df4 = df3.drop(['Date Start'], axis='columns')
    df5 = df4.reset_index()
    df6 = df5.drop('index', axis='columns')

    open = pd.DataFrame(df6['Open'].str.replace('$', ''))
    high = pd.DataFrame(df6['High'].str.replace('$', ''))
    low = pd.DataFrame(df6['Low'].str.replace('$', ''))
    close = pd.DataFrame(df6['Close'].str.replace('$', ''))
    volume = pd.DataFrame(df6['Volume'].str.replace('$', ''))
    market = pd.DataFrame(df6['Market Cap'].str.replace('$', ''))
    date = df6['Date']

    columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap', 'Date']
    df7 = pd.concat([date, open, high, low, close, volume, market ], axis='columns')
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'BTCUSDPrices' + dt_string
    df7.to_csv(f"{dt_string}.csv", index = False)


    
"""
def ETL_data2():
    period = tonight.find(class_="period-name").get_text()
    short_desc = tonight.find(class_="short-desc").get_text()
    temp = tonight.find(class_="temp").get_text()
    img = tonight.find("img")
    desc = img['title']
    period_tags = seven_day.select(".tombstone-container .period-name")
    periods = [pt.get_text() for pt in period_tags]
    short_descs = [sd.get_text() for sd in seven_day.select(".tombstone-container .short-desc")]
    temps = [t.get_text() for t in seven_day.select(".tombstone-container .temp")]
    descs = [d["title"] for d in seven_day.select(".tombstone-container img")]
"""

"""
soup = BeautifulSoup(load_web_page().content, 'html.parser')
seven_day = soup.find(id="seven-day-forecast")


def preprocess_pipeline():
    tonight = available_data()[0]
    period = tonight.find(class_="period-name").get_text()
    short_desc = tonight.find(class_="short-desc").get_text()
    temp = tonight.find(class_="temp").get_text()
    img = tonight.find("img")
    desc = img['title']
    period_tags = seven_day.select(".tombstone-container .period-name")
    periods = [pt.get_text() for pt in period_tags]
    short_descs = [sd.get_text() for sd in seven_day.select(".tombstone-container .short-desc")]
    temps = [t.get_text() for t in seven_day.select(".tombstone-container .temp")]
    descs = [d["title"] for d in seven_day.select(".tombstone-container img")]
    
    return periods, short_descs, temps, descs


def output_save_test():
    periods = preprocess_pipeline()[0]
    short_descs = preprocess_pipeline()[1]
    temps = preprocess_pipeline()[2]
    descs = preprocess_pipeline()[3]
    
    weather = pd.DataFrame({
    "period": periods,
    "short_desc": short_descs,
    "temp": temps,
    "desc":descs
    })
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'los_angeles_weather' + dt_string
    weather.to_csv(f"{dt_string}.csv", index=False)
    
"""

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 10, 17),

    'email' : ['akomolafekenny7@gmail.com'],
    'email_on_failure' : True,
    'retries' : 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG('crypto_dag',
         default_args = default_args,
         schedule =  '@daily',
         catchup=False
         ) as dag:
    
    is_web_page_available = PythonOperator(
        task_id = 'is_web_page_available',
        python_callable = load_web_page)
    
    
    extract_transform = PythonOperator(
      task_id = 'extract_transform_data',
       python_callable = ET_data)
    
    """
    
    preprocess_pipeline_1 = PythonOperator(
        task_id = 'preprocess_pipeline',
        python_callable = preprocess_pipeline
    )
    
    output_and_save_pipeline = PythonOperator(
        task_id = 'output_and_save',
        python_callable = output_save_test
    ) 
    """
    is_web_page_available >> extract_transform
    
    
    