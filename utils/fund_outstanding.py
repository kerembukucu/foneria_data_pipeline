import pandas as pd 

def fund_outstanding(df):
    df['YEAR'] = pd.to_datetime(df['year_month'], format="%Y/%m").dt.year
    df['MONTH'] = pd.to_datetime(df['year_month'], format="%Y/%m").dt.month
    df = df.drop(columns=["year_month"])

    df = df[["customer_id", "YEAR", "MONTH", "fund_code", "size"]]

    # Correct renaming of columns
    df = df.rename(columns={"customer_id": "CUST_ID", "fund_code": "FUND_CODE", "size": "FUND_OUTSTANDING"})

    return df

if __name__ == "__main__":
    input = "../input-data/monthly_outstanding_funds.csv"
    print(fund_outstanding(input))
