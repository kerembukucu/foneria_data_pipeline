# INTERESTED_FUND_RISK kolonu, ay bazinda musterinin open, aksiyonu aldigi fon'larin risk degerlerini de goz onunde bulundurarak agirlikli bir risk hesabi yapar. fund_actions-20241206.csv dosyasi as an input.

import pandas as pd

date_format = "%H.%M.%S-%d.%m.%y"

def interested_fund_risk(df, df2):

    df['YEAR'] = pd.to_datetime(df['timestamp'], format = date_format, errors="coerce").dt.year
    df['MONTH'] = pd.to_datetime(df['timestamp'], format = date_format, errors="coerce").dt.month

    risk_dict = dict(zip(df2['code'], df2['risk_value']))

    df['RISK'] = df['fund_code'].map(risk_dict)

    #filtering the dataframe for only open transactions
    df = df[df["action_type"]=="open"]
    

    df1 = df.groupby(["customer_id", "YEAR", "MONTH"])["RISK"].sum()
    df2 = df.groupby(["customer_id", "YEAR", "MONTH"])["action_type"].count()
    
    df = pd.concat([df1, df2], axis=1).reset_index()

    df["INTERESTED_FUND_RISK"] = df["RISK"] / df["action_type"]

    df.drop(columns=["RISK", "action_type"], inplace=True)

    # rename
    df.columns = ["CUST_ID", "YEAR", "MONTH", "INTERESTED_FUND_RISK"]
    df['INTERESTED_FUND_RISK'] = df['INTERESTED_FUND_RISK'].round(2)

    return df

if __name__ == '__main__':
    input_file = '../input-data/fund_actions-20241206.csv'
    input_file_2 = '../input-data/Fon_Detay_Tanımları.xlsx'
    print(interested_fund_risk(input_file, input_file_2))