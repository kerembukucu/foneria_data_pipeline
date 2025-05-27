import pandas as pd

date_format = "%H.%M.%S-%d.%m.%y"

def fund_clicks(df):
    df["YEAR"] = pd.to_datetime(df["timestamp"], format= date_format, errors="coerce").dt.year
    df["MONTH"] = pd.to_datetime(df["timestamp"], format= date_format, errors="coerce").dt.month

    df = df.groupby(['customer_id', "YEAR", "MONTH", "fund_code"]).size().reset_index(name="FUND_CLICKS")

    df.columns = ['CUST_ID', "YEAR", "MONTH", "FUND_CODE", "FUND_CLICKS"]

    return df

if __name__ == "__main__":
    input = "../input-data/fund_actions-20241206.csv"
    print(fund_clicks(input))