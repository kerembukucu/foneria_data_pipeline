# bu dosya musterinin fon/hesap oranini hesaplayan fonksiyonun tanimini icerir.
import pandas as pd

def fund_balance_ratio(inputdf):
    # Parse the date column into year and month
    inputdf['Yil'] = pd.to_datetime(inputdf['year_month'], format="%Y/%m").dt.year
    inputdf['Ay'] = pd.to_datetime(inputdf['year_month'], format="%Y/%m").dt.month

    inputdf['Fon/Hesap Oranı'] = inputdf['fund_volume'] / inputdf['account_volume']

    #sort the data by Customer ID, Year, and Month to ensure proper ordering for calculations
    inputdf.sort_values(['customer_id', 'Yil', 'Ay'], inplace=True)

    output_df = inputdf[['customer_id', 'Yil', 'Ay', 'Fon/Hesap Oranı']]

    # Rename columns to match the desired output format
    output_df.rename(columns={
        'customer_id': 'CUST_ID',
        'Yil': 'YEAR',
        'Ay': 'MONTH',
        'Fon/Hesap Oranı': 'FUND_BALANCE_RATIO'
    }, inplace=True)
    return output_df