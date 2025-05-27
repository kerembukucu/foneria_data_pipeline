import pandas as pd

def outstanding_trend(df):
    # Convert 'date' to datetime and extract YEAR, MONTH, and DAY
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d", errors="coerce")
    df['YEAR'] = df['date'].dt.year
    df['MONTH'] = df['date'].dt.month
    df['DAY'] = df['date'].dt.day

    # Calculate daily balance change (positive or negative) for each customer for each day
    df['Balance Change'] = df.groupby(['customer_id', 'YEAR', 'MONTH'])['account_volume'].diff().fillna(0)

    # Calculate overall balance trend for each month (1 if increasing, -1 if decreasing, 0 if no change)
    df['Monthly Balance Trend'] = df.groupby(['customer_id', 'YEAR', 'MONTH'])['Balance Change'].transform(lambda x: 1 if x.sum() > 0 else (-1 if x.sum() < 0 else 0))

    # Keep only unique rows for each customer and month
    df= df.drop_duplicates(subset=['customer_id', 'YEAR', 'MONTH'])[['customer_id', 'YEAR', 'MONTH', 'Monthly Balance Trend']]

    df.rename(columns={"customer_id":"CUST_ID", 'Monthly Balance Trend':'OUTSTANDING_TREND'}, inplace=True)

    return df

if __name__ == "__main__":
    inputfile = "../input-data/daily_outstandings.csv"
    # Call the function and save the resulting DataFrame to a new CSV file
    outstanding_trend(inputfile).to_csv("output2.csv")
    print("zen")