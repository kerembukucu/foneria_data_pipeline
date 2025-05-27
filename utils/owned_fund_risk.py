import pandas as pd

def owned_fund_risk(df, df2):
    df['YEAR'] = pd.to_datetime(df['year_month'], format = "%Y/%m").dt.year
    df['MONTH'] = pd.to_datetime(df['year_month'], format = "%Y/%m").dt.month

    # create a dictionary for 'Fon Kodu' and 'Risk Değeri' columns
    risk_dict = dict(zip(df2['code'], df2['risk_value']))

    # create a new column named 'RISK' and assign the risk value of the 'Fon Kodu' to that column
    df['RISK'] = df['fund_code'].map(risk_dict)

    # calculate the weighted average risk value for each customer for each month considering the risk and amount of all the funds they have for each month
    df['WEIGHTED_RISK'] = df['RISK'] * df['size']
    df = df.groupby(['customer_id', 'YEAR', 'MONTH']).agg({'WEIGHTED_RISK': 'sum', 'size': 'sum'}).reset_index()
    df['OWNED_FUND_RISK'] = df['WEIGHTED_RISK'] / df['size']

    df.drop(columns=['WEIGHTED_RISK', 'size'], inplace=True)

    # Rename columns
    df.columns = ['CUST_ID', 'YEAR', 'MONTH', 'OWNED_FUND_RISK']
    df['OWNED_FUND_RISK'] = df['OWNED_FUND_RISK'].round(2)
    return df

if __name__ == '__main__':
    input_file = '../input-data/monthly_outstanding_funds.csv'
    input_file2 = '../input-data/Fon_Detay_Tanımları.xlsx'
    print(owned_fund_risk(input_file, input_file2))