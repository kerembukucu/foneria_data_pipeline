import pandas as pd

def daily_click_seperator(df):
    # Convert timestamp to datetime and extract components
    df['YEAR'] = pd.to_datetime(df['timestamp'], format="%H.%M-%d.%m.%y").dt.year
    df['MONTH'] = pd.to_datetime(df['timestamp'], format="%H.%M-%d.%m.%y").dt.month
    df['HOUR'] = pd.to_datetime(df['timestamp'], format="%H.%M-%d.%m.%y").dt.hour

    def categorize_hour(hour):
        if 4 < hour <= 10:
            return "early_morning"
        elif 10 < hour <= 16:
            return "work_time"
        elif 16 < hour <= 22:
            return "evening"
        elif (0 <= hour <= 4) or (22 < hour < 24):
            return "night"
        return hour  
    
    df['HOUR'] = df['HOUR'].apply(categorize_hour)

    # Group by and count
    df = df.groupby(['customer_id', "YEAR", "MONTH", "HOUR"]).size().reset_index(name='COUNT')

    # Ensure all time periods exist before pivot
    all_periods = ["early_morning", "work_time", "evening", "night"]
    if not all(period in df['HOUR'].unique() for period in all_periods):
        # Add missing periods with count 0
        missing_periods = []
        for cust in df['customer_id'].unique():
            for year in df['YEAR'].unique():
                for month in df['MONTH'].unique():
                    for period in all_periods:
                        if period not in df[df['customer_id'] == cust]['HOUR'].values:
                            missing_periods.append({
                                'customer_id': cust,
                                'YEAR': year,
                                'MONTH': month,
                                'HOUR': period,
                                'COUNT': 0
                            })
        if missing_periods:
            df = pd.concat([df, pd.DataFrame(missing_periods)], ignore_index=True)

    # Pivot table
    df = df.pivot_table(
        index=["customer_id", "YEAR", "MONTH"],
        columns="HOUR",
        values="COUNT",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    df.columns.name = None

    # Calculate total and percentages
    df["total"] = df["early_morning"] + df["work_time"] + df["evening"] + df["night"]
    
    # Handle division by zero
    for col in ["early_morning", "work_time", "evening", "night"]:
        df[col] = df.apply(lambda x: "0%" if x['total'] == 0 else f"{(x[col]/x['total']*100):.2f}%", axis=1)

    # Reorder columns
    new_order = ["customer_id", "YEAR", "MONTH", "early_morning", "work_time", "evening", "night"]
    df = df[new_order]

    # Rename columns
    df.columns = ['CUST_ID', 'YEAR', 'MONTH', 
                 "EARLY_MORNING_ACTIVITY_PERCENTAGE", 
                 "WORK_TIME_ACTIVITY_PERCENTAGE", 
                 "EVENING_ACTIVITY_PERCENTAGE", 
                 "NIGHT_ACTIVITY_PERCENTAGE"]

    return df

if __name__ == "__main__":
    inputfile1 = "../input-data/actions.csv"
    print(daily_click_seperator(inputfile1))