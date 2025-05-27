import pandas as pd 

def transaction_volume(df):
    # Filter rows where "İşlem Tipi" column is equal to 'Fon Alma' or 'Fon Satma'
    df = df[df['action_type'].isin(['Fon Alma', 'Fon Satma'])]

    # calculate YEAR and MONTH based getting the date data from Timestamp column
    df['YEAR'] = pd.to_datetime(df['timestamp'], format = "%H.%M-%d.%m.%y").dt.year
    df['MONTH'] = pd.to_datetime(df['timestamp'], format = "%H.%M-%d.%m.%y").dt.month

    # Calculate the total amount of transactions for each customer for each month getting the amount data from 'TRANSACTION_VOLUME' column
    df = df.groupby(['customer_id', 'YEAR', 'MONTH']).agg({'amount': 'sum'}).reset_index()


    # Rename columns
    df.columns = ['CUST_ID', 'YEAR', 'MONTH', 'TRANSACTION_VOLUME']

    return df

if __name__ == '__main__':
    input_file = '../input-data/actions.csv'
    result = transaction_volume(input_file)
    print(result)
