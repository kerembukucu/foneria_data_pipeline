import pandas as pd

def fund_transaction_volume(df):
    df = df[df['action_type'].isin(["Fon Alma", "Fon Satma"])]

    df["YEAR"] = pd.to_datetime(df["timestamp"], format="%H.%M-%d.%m.%y").dt.year
    df["MONTH"] = pd.to_datetime(df["timestamp"], format="%H.%M-%d.%m.%y").dt.month
    df= df.groupby(["customer_id", "YEAR", "MONTH", "fund_code"])["amount"].sum().reset_index(name="FUND_TRANSACTION_VOLUME")
    df = df.rename(columns={"customer_id": "CUST_ID", "fund_code": "FUND_CODE"})

    return df

if __name__ == "__main__":
    input = "../input-data/actions.csv"
    print(fund_transaction_volume(input))