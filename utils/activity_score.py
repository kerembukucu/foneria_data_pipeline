import pandas as pd 

def activity_score(df):
    df['ACTIVITY_SCORE'] = 0

    # Convert Timestamp to datetime, handling invalid entries
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%H.%M-%d.%m.%y", errors='coerce')

    # If the conversion resulted in NaT for any row, you can handle that here if needed
    df['YEAR'] = df['timestamp'].dt.year
    df['MONTH'] = df['timestamp'].dt.month

    # Remove rows where Timestamp is NaT if necessary
    df = df.dropna(subset=['timestamp'])

    # Counting the number of actions for each customer in each month
    df = df.groupby(['customer_id', 'YEAR', 'MONTH']).size().reset_index(name='COUNT')

    # Calculate the average of count column
    min_val = df['COUNT'].min()
    max_val = df['COUNT'].max()
    df['ACTIVITY_SCORE'] = [(x - min_val) / (max_val - min_val) * 100 for x in df['COUNT']]

    # Rounding the values
    df['ACTIVITY_SCORE'] = df['ACTIVITY_SCORE'].round(2)

    df = df.drop(columns=['COUNT'])
    df = df.rename(columns={'customer_id': 'CUST_ID'})

    return df

if __name__ == "__main__":
    inputfile = "../input-data/actions.csv"
    df = pd.read_csv(inputfile)  # Read the input data
    activity_score_df = activity_score(df)
    activity_score_df.to_excel("zen.xlsx")
