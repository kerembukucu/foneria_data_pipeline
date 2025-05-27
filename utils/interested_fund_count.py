# INTERESTED_FUND_COUNT kolonu, ay bazinda musterinin open, aksiyonu aldigi fon sayisini hesaplar. fund_actions-20241206.csv dosyasi as an input

import pandas as pd

date_format = "%H.%M.%S-%d.%m.%y"

def interested_fund_count(inputdf):

    inputdf["YEAR"] = pd.to_datetime(inputdf["timestamp"], format=date_format, errors='coerce').dt.year
    inputdf["MONTH"] = pd.to_datetime(inputdf["timestamp"], format=date_format, errors='coerce').dt.month

    inputdf = inputdf[inputdf["action_type"] == "open"]

    #count the number of funds opened by each customer
    inputdf = inputdf.groupby(["YEAR", "MONTH", "action_type"])["fund_code"].count().reset_index()

    #rename columns
    inputdf.columns = ["YEAR", "MONTH", "CUST_ID", "INTERESTED_FUND_COUNT"]


    return inputdf