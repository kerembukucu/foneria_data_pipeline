import pandas as pd

def average_outstanding(df):
    # Convert 'date' to datetime and extract YEAR, MONTH, and DAY
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d", errors="coerce")
    df['YEAR'] = df['date'].dt.year
    df['MONTH'] = df['date'].dt.month

    # Group by 'Müşteri ID', 'YEAR', and 'MONTH' and calculate the monthly average outstanding
    df = df.groupby(['customer_id', 'YEAR', 'MONTH'])['account_volume'].mean().reset_index()

    # Rename the column to reflect it's the average
    df.rename(columns={'account_volume': 'AVERAGE_OUTSTANDING', 'customer_id': 'CUST_ID'}, inplace=True)

    return df

if __name__ == "__main__":
    inputfile = "../input-data/daily_outstandings.csv"
    print(average_outstanding(inputfile))
