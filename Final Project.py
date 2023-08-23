'''
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
 https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv
'''

# prompt> python -m pip install pandas
# prompt> python -m pip install requests

# Q1
dataframe = pd.read_csv('exchange_rates.csv')
dataframe.columns = ['Symbol', 'Rates']
criteria = dataframe['Symbol'] == 'GBP'
exchange_rate = dataframe[criteria]['Rates']
exchange_rate


#Q2
import glob
import pandas as pd
from datetime import datetime

def extract():
    df = pd.read_json('bank_market_cap_1.json')
    return df

extracted_dt = extract()

#Q3
def transform(extracted_dt, exchange_rate):
    extracted_dt['Market Cap (GBP$ Billion)'] = round(extracted_dt['Market Cap (US$ Billion)'] * exchange_rate, 3)
    return extracted_dt

def load(extracted_dt):
    extracted_dt.to_csv('bank_market_cap_gbp.csv', index = False)

def log(message):
    # Write your code here
    tmstamp_fmt = '%Y-%m-%d %H:%M:%S' # Year-Month-Day Hour:Minute:Second
    now = datetime.now() # current timestamp
    tmstamp = now.strftime(tmstamp_fmt)
    with open("logfile.txt","a") as f:
        f.write(tmstamp + ',' + message + '\n')


log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
log("ETL Job Started")

log("Extract phase Started")
## Start of EXtract
# function call
extracted_dt = extract()
# extracted_dt # view
log("Extract phase Ended")
## End of EXtract

## Start of Transform
log("Transform phase Started")
# function call
transformed_dt = transform(extracted_dt, exchange_rate)
# data
# transformed_dt # view
log("Transform phase Ended")
## End of Transform

log("Load phase Started")
load(transformed_dt)
log("Load phase Ended")

log("ETL Job Ended")
log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")