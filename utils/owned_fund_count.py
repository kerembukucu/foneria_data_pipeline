import pandas as pd

def owned_fund_count(df):
    df['YEAR'] = pd.to_datetime(df['year_month'], format = "%Y/%m").dt.year
    df['MONTH'] = pd.to_datetime(df['year_month'], format = "%Y/%m").dt.month

    # count how many different 'Fund Code' each customer has for each month. creqate column for that count. name it to 'OWNED_FUND_COUNT' column
    df = df.groupby(['customer_id', 'YEAR', 'MONTH']).size().reset_index(name='OWNED_FUND_COUNT')

    # Rename columns
    df.columns = ['CUST_ID', 'YEAR', 'MONTH', 'OWNED_FUND_COUNT']

    return df

if __name__ == '__main__':
    input_file = '../input-data/monthly_outstanding_funds.csv'
    print(owned_fund_count(input_file))